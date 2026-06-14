#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "predex/internal/market_types.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/oms/transport/kalshi_rest_adapter.hpp"

namespace predex::core::oms::kalshi::gateway {

[[nodiscard]] inline internal::TimestampNs gateway_now_ns() noexcept {
    return static_cast<internal::TimestampNs>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

using LineageId = std::uint64_t;
using DispatchItemId = std::uint64_t;
using DispatchRequestId = std::uint64_t;

enum class DispatchClass : std::uint8_t {
    kHot = 1,
    kRecovery = 2,
    kReconcile = 3,
};

enum class DispatchRequestState : std::uint8_t {
    kQueued = 1,
    kAdmitted = 2,
    kDispatchedPreWrite = 3,
    kPostWriteUnknown = 4,
    kCompleted = 5,
    kAbandonedToRecovery = 6,
};

enum class DispatchItemState : std::uint8_t {
    kPending = 1,
    kAcked = 2,
    kRejected = 3,
    kUnknown = 4,
    kAdoptedFromTruth = 5,
};

enum class DispatchOperation : std::uint8_t {
    kSubmit = 1,
    kCancel = 2,
    kModify = 3,
};

enum class DispatchBatchKind : std::uint8_t {
    kSingleton = 1,
    kGroupedSubmit = 2,
    kCoordinatedCancel = 3,
};

enum class ConnectionStartResult : std::uint8_t {
    kStarted = 1,
    kBusy = 2,
    kInvalidRequest = 3,
    kConnectionUnavailable = 4,
};

enum class ConnectionPollStatus : std::uint8_t {
    kIdle = 1,
    kInFlight = 2,
    kCompleted = 3,
};

enum class SessionPoolSubmitResult : std::uint8_t {
    kAccepted = 1,
    kNoIdleConnection = 2,
    kInvalidRequest = 3,
    kPoolUnavailable = 4,
};

struct GroupKey {
    GroupIntentId group_intent_id{0};
    std::uint16_t expected_leg_count{0};

    [[nodiscard]] bool valid() const noexcept {
        return group_intent_id != 0 && expected_leg_count > 0;
    }
};

struct BatchGroupMetadata {
    GroupKey group_key{};
    GroupExecutionPolicy execution_policy{GroupExecutionPolicy::kAbortRemaining};
    std::uint16_t leg_index{0};

    [[nodiscard]] bool valid() const noexcept {
        return group_key.valid() && leg_index < group_key.expected_leg_count;
    }
};

struct BatchGroupRequestMetadata {
    GroupKey group_key{};
    GroupExecutionPolicy execution_policy{GroupExecutionPolicy::kAbortRemaining};

    [[nodiscard]] bool valid() const noexcept { return group_key.valid(); }
};

struct LineageKey {
    OmsRequestId oms_request_id{0};
    ClientOrderId client_order_id{};
    std::optional<ExchangeOrderId> exchange_order_id;

    [[nodiscard]] bool valid() const noexcept {
        return oms_request_id != 0 || !client_order_id.empty() ||
               (exchange_order_id.has_value() && !exchange_order_id->empty());
    }
};

struct DispatchBudgetCost {
    std::uint32_t transaction_units{0};

    [[nodiscard]] bool zero() const noexcept { return transaction_units == 0; }
};

struct CommandEnvelope {
    DispatchItemId dispatch_item_id{0};
    LineageId lineage_id{0};
    LineageKey lineage_key{};
    std::optional<GroupKey> group_key;
    std::optional<BatchGroupMetadata> batch_group;
    DispatchOperation operation{DispatchOperation::kSubmit};
    DispatchClass dispatch_class{DispatchClass::kHot};
    internal::TimestampNs ingress_ts_ns{0};
    OmsToKalshiCommand command;
};

struct DispatchItem {
    DispatchItemId dispatch_item_id{0};
    LineageId lineage_id{0};
    LineageKey lineage_key{};
    std::optional<GroupKey> group_key;
    std::optional<BatchGroupMetadata> batch_group;
    DispatchOperation operation{DispatchOperation::kSubmit};
    DispatchClass dispatch_class{DispatchClass::kHot};
    DispatchItemState state{DispatchItemState::kPending};
    internal::TimestampNs ingress_ts_ns{0};
    internal::TimestampNs sequenced_ts_ns{0};
    OmsToKalshiCommand command;
};

struct DispatchRequest {
    DispatchRequestId dispatch_request_id{0};
    DispatchClass dispatch_class{DispatchClass::kHot};
    DispatchBatchKind batch_kind{DispatchBatchKind::kSingleton};
    DispatchRequestState state{DispatchRequestState::kQueued};
    DispatchBudgetCost budget_cost{};
    internal::TimestampNs queued_ts_ns{0};
    internal::TimestampNs planned_ts_ns{0};
    internal::TimestampNs admitted_ts_ns{0};
    internal::TimestampNs session_submit_ts_ns{0};
    internal::TimestampNs connection_start_ts_ns{0};
    std::optional<GroupKey> group_key;
    std::optional<BatchGroupRequestMetadata> batch_group;
    std::vector<DispatchItem> items;

    [[nodiscard]] bool empty() const noexcept { return items.empty(); }
    [[nodiscard]] std::size_t item_count() const noexcept { return items.size(); }
};

struct DispatchCompletion {
    DispatchRequest request;
    std::vector<KalshiToOmsEvent> emitted_events;
    std::optional<transport::RestTraceInfo> trace;
    DispatchRequestState terminal_state{DispatchRequestState::kCompleted};
    internal::TimestampNs completed_ts_ns{0};
    std::string error_message;
};

struct ConnectionPollResult {
    ConnectionPollStatus status{ConnectionPollStatus::kIdle};
    std::optional<DispatchCompletion> completion;
};

struct SessionPoolCompletion {
    std::size_t connection_index{0};
    DispatchCompletion completion;
};

[[nodiscard]] inline std::optional<GroupKey> group_key_for_context(const IntentContext& context) {
    if (context.group_intent_id == 0 || context.leg_count == 0) {
        return std::nullopt;
    }
    return GroupKey{
        .group_intent_id = context.group_intent_id,
        .expected_leg_count = context.leg_count,
    };
}

[[nodiscard]] inline DispatchOperation
operation_for_command(const OmsToKalshiCommand& command) {
    return std::visit(
        [](const auto& typed_command) noexcept {
            using T = std::decay_t<decltype(typed_command)>;
            if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                return DispatchOperation::kSubmit;
            }
            if constexpr (std::is_same_v<T, CancelOrderCmd>) {
                return DispatchOperation::kCancel;
            }
            return DispatchOperation::kModify;
        },
        command);
}

[[nodiscard]] inline LineageKey lineage_key_for_command(const OmsToKalshiCommand& command) {
    return std::visit(
        [](const auto& typed_command) -> LineageKey {
            using T = std::decay_t<decltype(typed_command)>;
            if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                return LineageKey{
                    .oms_request_id = typed_command.order.oms_request_id,
                    .client_order_id = typed_command.order.client_order_id,
                    .exchange_order_id = typed_command.order.exchange_order_id,
                };
            } else {
                return LineageKey{
                    .oms_request_id = typed_command.corr.order.oms_request_id,
                    .client_order_id = typed_command.corr.order.client_order_id,
                    .exchange_order_id = typed_command.corr.order.exchange_order_id,
                };
            }
        },
        command);
}

[[nodiscard]] inline std::optional<GroupKey>
group_key_for_command(const OmsToKalshiCommand& command) {
    return std::visit(
        [](const auto& typed_command) -> std::optional<GroupKey> {
            using T = std::decay_t<decltype(typed_command)>;
            if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                return group_key_for_context(typed_command.intent.context);
            } else {
                return group_key_for_context(typed_command.corr.context);
            }
        },
        command);
}

[[nodiscard]] inline std::optional<BatchGroupMetadata>
batch_group_metadata_for_command(const OmsToKalshiCommand& command) {
    return std::visit(
        [](const auto& typed_command) -> std::optional<BatchGroupMetadata> {
            using T = std::decay_t<decltype(typed_command)>;
            if constexpr (!std::is_same_v<T, SubmitOrderCmd>) {
                return std::nullopt;
            } else {
                const auto group_key = group_key_for_context(typed_command.intent.context);
                if (!group_key.has_value() || !typed_command.group_execution_policy.has_value()) {
                    return std::nullopt;
                }
                return BatchGroupMetadata{
                    .group_key = *group_key,
                    .execution_policy = *typed_command.group_execution_policy,
                    .leg_index = typed_command.intent.context.leg_index,
                };
            }
        },
        command);
}

[[nodiscard]] inline std::string_view dispatch_class_name(DispatchClass dispatch_class) noexcept {
    switch (dispatch_class) {
    case DispatchClass::kHot:
        return "hot";
    case DispatchClass::kRecovery:
        return "recovery";
    case DispatchClass::kReconcile:
        return "reconcile";
    }
    return "unknown";
}

} // namespace predex::core::oms::kalshi::gateway
