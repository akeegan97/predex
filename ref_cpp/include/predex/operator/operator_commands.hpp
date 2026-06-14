#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "predex/control/control_types.hpp"

namespace predex::core::operator_commands {

using OperatorRequestId = std::uint64_t;

enum class OperatorCommandType : std::uint8_t {
    kStatus,
    kLoadUniverse,
    kRefreshUniverse,
    kSetTradingEnabled,
    kBeginShutdown,
    kHaltTrading,
    kResumeTrading,
    kExitAll,
};

struct OperatorCommand {
    OperatorRequestId request_id{0};
    OperatorCommandType type{OperatorCommandType::kStatus};

    static OperatorCommand status(OperatorRequestId request_id_in) {
        return OperatorCommand{
            .request_id = request_id_in,
            .type = OperatorCommandType::kStatus,
        };
    }

    static OperatorCommand begin_shutdown(OperatorRequestId request_id_in) {
        return OperatorCommand{
            .request_id = request_id_in,
            .type = OperatorCommandType::kBeginShutdown,
        };
    }
};

enum class OperatorResponseType : std::uint8_t {
    kAccepted,
    kRejected,
    kInProgress,
    kCompleted,
    kFaulted,
};

struct OperatorStatusSnapshot {
    control::ProcessState process_state{};
    bool app_running{false};
    bool trading_halted{false};
    std::size_t live_orders{0};
    std::uint64_t public_ws_generation{0};
    std::string last_error;
};

struct OperatorResponse {
    OperatorRequestId request_id{0};
    OperatorCommandType command_type{OperatorCommandType::kStatus};
    OperatorResponseType type{OperatorResponseType::kAccepted};
    std::string message;
    std::optional<OperatorStatusSnapshot> status_snapshot;

    static OperatorResponse completed_status(OperatorRequestId request_id_in,
                                             OperatorStatusSnapshot snapshot) {
        OperatorResponse response{};
        response.request_id = request_id_in;
        response.command_type = OperatorCommandType::kStatus;
        response.type = OperatorResponseType::kCompleted;
        response.message = "status snapshot ready";
        response.status_snapshot = std::move(snapshot);
        return response;
    }

    static OperatorResponse accepted(OperatorCommandType command_type_in,
                                     OperatorRequestId request_id_in,
                                     std::string message_in) {
        OperatorResponse response{};
        response.request_id = request_id_in;
        response.command_type = command_type_in;
        response.type = OperatorResponseType::kAccepted;
        response.message = std::move(message_in);
        return response;
    }

    static OperatorResponse rejected(OperatorCommandType command_type_in,
                                     OperatorRequestId request_id_in,
                                     std::string message_in) {
        OperatorResponse response{};
        response.request_id = request_id_in;
        response.command_type = command_type_in;
        response.type = OperatorResponseType::kRejected;
        response.message = std::move(message_in);
        return response;
    }
};

} // namespace predex::core::operator_commands
