#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "predex/control/control_lifecycle.hpp"

namespace predex::operator_admin {

enum class OperatorCommandType : std::uint8_t {
    kUNKNOWN = 0,
    kSTATUS = 1,
    kSHUTDOWN_GRACEFUL = 2,
    kSHUTDOWN_FORCEFUL = 3,
};

using OperatorCommandId = std::uint64_t;

struct OperatorCommand {
    OperatorCommandType type{OperatorCommandType::kUNKNOWN};
    OperatorCommandId request_id{0};
};

enum class OperatorResponseType : std::uint8_t {
    kACK = 0,
    kERROR = 1,
    kSTATUS = 2,
};



struct OperatorStatusSnapshot {
    predex::core::control::LifecyclePhase lifecycle{predex::core::control::LifecyclePhase::kBOOTING};
    bool trading_enabled{false};
    bool shutdown_requested{false};
};

struct AckPayload {};

struct ErrorPayload {
    std::string message;
};

using OperatorResponsePayload =
    std::variant<AckPayload, ErrorPayload, OperatorStatusSnapshot>;

struct OperatorResponse {
    OperatorCommandId request_id{0};
    OperatorResponseType type{OperatorResponseType::kACK};
    OperatorResponsePayload payload{AckPayload{}};
};

}  // namespace predex::operator_admin