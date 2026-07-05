#pragma once
#include <stop_token>
#include <vector>
#include <utility>
#include <memory>
#include <string>
#include <chrono>
#include <cstdint>
#include <optional>
#include <unordered_map>

#include "predex/exchange/kalshi/websocket_session.hpp"
#include "predex/exchange/kalshi/adapters/order_data_handler.hpp"
#include "predex/ingest/kalshi/order_data/order_parser.hpp"
#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc.hpp"

namespace predex::ingest::kalshi::order_data{

    inline constexpr std::chrono::milliseconds kPRIVATE_ORDER_FEED_TELEMETRY_INTERVAL{250};

    enum class OrderSubscriptionPhase : std::uint8_t{
        kIDLE = 0,
        kSUBSCRIBE_PENDING = 1,
        kSUBSCRIBED = 2,
        kUNSUBSCRIBE_PENDING = 3,
        kFAULTED = 4,
    };

    enum class OrderWsCommandKind : std::uint8_t{
        kSUBSCRIBE = 1,
        kUNSUBSCRIBE = 2,
    };

    struct ActiveOrderSubscription{
        exchange::kalshi::KalshiOrderDataChannel channel{};
        OrderSubscriptionPhase phase{OrderSubscriptionPhase::kIDLE};
        std::optional<std::int64_t> sid;
        std::string last_error;
    };

    struct PendingOrderWsCommand{
        std::uint64_t ws_command_id{};
        OrderWsCommandKind kind{};
        exchange::kalshi::KalshiOrderDataChannel channel{};
    };

    struct OrderSessionControlQueues{
        utils::SPSCQueue<core::control::ControlToPrivateOrderFeedCommand>& control_to_order_session_queue;
        utils::SPSCQueue<core::control::PrivateOrderFeedToControlStatus>& order_session_to_control_queue;
    };

    struct OrderSessionOmsQueues{
        utils::SPSCQueue<oms::KalshiToOmsEvent>& private_ws_to_oms_queue;
    };

    struct OrderSessionDeps{
        exchange::kalshi::KalshiOrderDataHandler order_data_handler;
        std::vector<exchange::kalshi::KalshiOrderDataChannel> desired_channels;
        // to & from control plane queues
        OrderSessionControlQueues control_queues;
        // to OMS queues
        OrderSessionOmsQueues oms_queues;
    };

    struct OrderSessionState{
        bool connected{false};
        std::string last_error;
    };

    class KalshiOrderSession{
        public:
            KalshiOrderSession(OrderSessionDeps deps)
                : order_data_handler_(std::move(deps.order_data_handler)), 
                  ws_session_(order_data_handler_),
                  control_queues_(deps.control_queues),
                  oms_queues_(deps.oms_queues),
                  desired_channels_(std::move(deps.desired_channels)) {}

            void run(const std::stop_token& stop_token);

            [[nodiscard]] OrderSessionState state() const{
                return status_;
            }
        private:

            void drain_control_commands() noexcept;
            void handle_control_command(const core::control::ControlToPrivateOrderFeedCommand& cmd);

            void apply_universe_snapshot(const std::shared_ptr<const core::control::OrderRouteUniverse>& snapshot);

            [[nodiscard]] bool connect();

            void disconnect(std::string reason = {});
            void clear_transport_subscription_state();

            [[nodiscard]] bool subscribe_active_universe();
            [[nodiscard]] bool subscribe_channel(exchange::kalshi::KalshiOrderDataChannel channel);
            [[nodiscard]] bool unsubscribe_channel(exchange::kalshi::KalshiOrderDataChannel channel);

            [[nodiscard]] bool push_control_status(core::control::PrivateOrderFeedToControlStatus status) noexcept;

            void maybe_send_telemetry() noexcept;

            void pump_socket_once() noexcept;

            [[nodiscard]] bool send_oms_event(oms::KalshiToOmsEvent event) noexcept;

            [[nodiscard]] std::uint64_t next_ws_command_id() noexcept {
                return next_ws_command_id_++;
            }


            exchange::kalshi::KalshiOrderDataHandler order_data_handler_;
            exchange::kalshi::WebSocketSession ws_session_;
            OrderSessionControlQueues control_queues_;
            OrderSessionOmsQueues oms_queues_;
            OrderSessionState status_;

            std::shared_ptr<const core::control::OrderRouteUniverse> desired_universe_;
            std::unordered_map<std::string, core::control::OrderMarketRoute> route_by_ticker_;
            std::vector<exchange::kalshi::KalshiOrderDataChannel> desired_channels_;
            std::unordered_map<exchange::kalshi::KalshiOrderDataChannel, ActiveOrderSubscription> active_subscriptions_;
            std::unordered_map<std::uint64_t, PendingOrderWsCommand> pending_ws_commands_;

            OrderParser order_parser_;

            core::control::PrivateOrderFeedTelemetrySnapshot telemetry_;
            std::chrono::steady_clock::time_point next_telemetry_send_{std::chrono::steady_clock::now() + kPRIVATE_ORDER_FEED_TELEMETRY_INTERVAL};
            std::uint64_t next_ws_command_id_{1};
    };

}
