#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "predex/utils/spsc_queue.hpp"
#include "predex/control/control_types.hpp"
#include "predex/operator/operator_commands.hpp"

namespace predex::core::control::kalshi {

/*
ControlPlane is responsible for:
 - managing the overall lifecycle of predex
 - operator control (e.g. start/stop/refresh/reconfigure/etc)
 - orchestrating desync recovery, crash recovery, and cross-thread coordination
*/

struct IoQueues {
    utils::SPSCQueue<ControlIoCommand>& control_io_queue;
    utils::SPSCQueue<IoControlStatus>& io_control_status;
};

struct OperatorQueues {
    utils::SPSCQueue<operator_commands::OperatorCommand>& operator_command_queue;
    utils::SPSCQueue<operator_commands::OperatorResponse>& operator_response_queue;
};

using StatusProvider = std::function<operator_commands::OperatorStatusSnapshot()>;

class ControlPlane {
  public:
    ControlPlane(IoQueues queues, OperatorQueues operator_queues, StatusProvider status_provider)
        : control_io_queue_(queues.control_io_queue),
          io_control_status_queue_(queues.io_control_status),
          operator_command_queue_(operator_queues.operator_command_queue),
          operator_response_queue_(operator_queues.operator_response_queue),
          status_provider_(std::move(status_provider)) {
        process_state_ = ProcessState{ProcessStatus::kBooting};
    }

    [[nodiscard]] ProcessState process_state() const { return process_state_; }

    void set_process_state(ProcessState new_state) { process_state_ = new_state; }

    std::size_t pump_operator_commands() {
        operator_commands::OperatorCommand cmd{};
        std::size_t processed = 0;
        while (operator_command_queue_.try_pop(cmd)) {
            switch (cmd.type) {
            case operator_commands::OperatorCommandType::kStatus:
                (void)push_operator_response(operator_commands::OperatorResponse::completed_status(
                    cmd.request_id, status_provider_()));
                break;
            case operator_commands::OperatorCommandType::kBeginShutdown:
                process_state_.status = ProcessStatus::kShuttingDown;
                (void)push_operator_response(operator_commands::OperatorResponse::accepted(
                    cmd.type, cmd.request_id, "shutdown requested"));
                break;
            case operator_commands::OperatorCommandType::kLoadUniverse:
            case operator_commands::OperatorCommandType::kRefreshUniverse:
            case operator_commands::OperatorCommandType::kSetTradingEnabled:
            case operator_commands::OperatorCommandType::kHaltTrading:
            case operator_commands::OperatorCommandType::kResumeTrading:
            case operator_commands::OperatorCommandType::kExitAll:
                (void)push_operator_response(operator_commands::OperatorResponse::rejected(
                    cmd.type, cmd.request_id, "command not implemented yet"));
                break;
            }
            ++processed;
        }
        return processed;
    }

    std::size_t pump_io_status() {
        IoControlStatus status{};
        std::size_t processed = 0;
        while (io_control_status_queue_.try_pop(status)) {
            switch (status.type) {
            case IoControlStatusType::kConnected:
                io_state_.current_state = IoControlStateType::kConnected;
                if (io_state_.target_state == IoControlStateType::kConnected) {
                    io_state_.transition_in_flight = false;
                }
                if (process_state_.status == ProcessStatus::kBooting ||
                    process_state_.status == ProcessStatus::kWaitingForIo) {
                    set_process_state(ProcessState{ProcessStatus::kIoConnected});
                }
                break;
            case IoControlStatusType::kDisconnected:
                io_state_.current_state = IoControlStateType::kDisconnected;
                if (io_state_.target_state == IoControlStateType::kDisconnected) {
                    io_state_.transition_in_flight = false;
                    if (process_state_.status == ProcessStatus::kShuttingDown) {
                        set_process_state(ProcessState{ProcessStatus::kStopped});
                    } else if (process_state_.status == ProcessStatus::kBooting ||
                               process_state_.status == ProcessStatus::kWaitingForIo) {
                        set_process_state(ProcessState{ProcessStatus::kWaitingForIo});
                    }
                    break;
                }

                io_state_.transition_in_flight = false;
                if (process_state_.status == ProcessStatus::kBooting ||
                    process_state_.status == ProcessStatus::kWaitingForIo ||
                    process_state_.status == ProcessStatus::kIoConnected ||
                    process_state_.status == ProcessStatus::kReady ||
                    process_state_.status == ProcessStatus::kLive) {
                    set_process_state(ProcessState{ProcessStatus::kWaitingForIo});
                }
                break;
            case IoControlStatusType::kUniverseSnapshotReceived:
            case IoControlStatusType::kSubscriptionStarted:
            case IoControlStatusType::kSubscriptionReady:
            case IoControlStatusType::kSubscriptionFailed:
            case IoControlStatusType::kUniverseApplied:
                break;
            }
            ++processed;
        }
        return processed;
    }

    bool send_io_command(const ControlIoCommand& command) {
        if (control_io_queue_.try_push(command)) {
            io_state_.last_cmd_sent = command.type;
            io_state_.transition_in_flight = true;
            switch (command.type) {
            case ControlIoCommandType::kDisconnect:
                io_state_.target_state = IoControlStateType::kDisconnected;
                break;
            case ControlIoCommandType::kReconnect:
            case ControlIoCommandType::kRefresh:
                io_state_.target_state = IoControlStateType::kConnected;
                io_state_.current_state = IoControlStateType::kReconnecting;
                break;
            case ControlIoCommandType::kRecoverMarket:
            case ControlIoCommandType::kApplyUniverseSnapshot:
                break;
            }
            return true;
        }
        return false;
    }

    [[nodiscard]] std::uint64_t update_market_route_universe(MarketRouteUniverse new_universe) {
        new_universe.version = market_route_universe_.version + 1;
        market_route_universe_ = std::move(new_universe);
        return market_route_universe_.version;
    }

    void publish_io_subscription_universe() {
        auto next_snapshot = std::make_shared<IoSubscriptionUniverse>();
        next_snapshot->version = market_route_universe_.version;
        next_snapshot->markets.reserve(market_route_universe_.routes.size());
        for (const auto& route : market_route_universe_.routes) {
            next_snapshot->markets.push_back(IoMarketSubscription{
                .id = route.market_id,
                .kalshi_ticker = route.kalshi_ticker,
            });
        }
        published_io_subscription_universe_ = next_snapshot;
    }

    [[nodiscard]] std::shared_ptr<const IoSubscriptionUniverse> io_subscription_universe()
        const noexcept {
        return published_io_subscription_universe_;
    }

    [[nodiscard]] bool send_io_subscription_universe() {
        if (published_io_subscription_universe_ == nullptr) {
            publish_io_subscription_universe();
        }
        return send_io_command(
            ControlIoCommand::apply_universe_snapshot(published_io_subscription_universe_));
    }

    [[nodiscard]] bool recover_market(internal::MarketId market_id) {
        return send_io_command(ControlIoCommand::recover_market(market_id));
    }

  private:
    bool push_operator_response(operator_commands::OperatorResponse response) {
        return operator_response_queue_.try_push(std::move(response));
    }

    ProcessState process_state_;
    IoControlState io_state_;

    utils::SPSCQueue<ControlIoCommand>& control_io_queue_;
    utils::SPSCQueue<IoControlStatus>& io_control_status_queue_;

    utils::SPSCQueue<operator_commands::OperatorCommand>& operator_command_queue_;
    utils::SPSCQueue<operator_commands::OperatorResponse>& operator_response_queue_;

    StatusProvider status_provider_;
    MarketRouteUniverse market_route_universe_;
    std::shared_ptr<const IoSubscriptionUniverse> published_io_subscription_universe_;
};

} // namespace predex::core::control::kalshi
