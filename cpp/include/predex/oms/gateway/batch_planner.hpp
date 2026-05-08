#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "predex/oms/gateway/gateway_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::gateway {

struct BatchPlannerQueues {
    utils::SPSCQueue<DispatchItem>* hot_input_queue{nullptr};
    utils::SPSCQueue<DispatchItem>* recovery_input_queue{nullptr};
    utils::SPSCQueue<DispatchItem>* reconcile_input_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* hot_output_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* recovery_output_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* reconcile_output_queue{nullptr};
};

struct BatchPlannerTelemetry {
    std::uint64_t emitted_singleton_requests{0};
    std::uint64_t emitted_grouped_requests{0};
    std::uint64_t buffered_group_items{0};
    std::uint64_t invalid_group_items{0};
    std::uint64_t output_backpressure{0};
};

struct BatchPlannerConfig {
    std::size_t max_group_legs{kMaxGroupOrderLegs};
};

// Converts sequenced dispatch items into transport-ready requests.
//
// Responsibilities:
// - emit singleton requests for non-grouped work
// - accumulate grouped submit legs until all expected members arrive
// - preserve in-group leg order using IntentContext.leg_index
// - emit one grouped DispatchRequest once the full set is present
//
// Non-responsibilities:
// - lineage sequencing
// - rate-limit admission
// - session assignment
// - recovery policy
class BatchPlanner {
  public:
    explicit BatchPlanner(BatchPlannerQueues queues = {}, BatchPlannerConfig config = {})
        : queues_(queues), config_(config) {}

    [[nodiscard]] bool drain_one() {
        if (flush_ready_request()) {
            return true;
        }

        DispatchItem item;
        if (!try_take_item(item)) {
            return false;
        }

        if (item.batch_group.has_value() && item.operation == DispatchOperation::kSubmit) {
            return buffer_group_item(std::move(item));
        }

        return emit_or_buffer(make_singleton_request(std::move(item)));
    }

    [[nodiscard]] const BatchPlannerTelemetry& telemetry() const noexcept { return telemetry_; }

  private:
    struct PendingGroup {
        DispatchClass dispatch_class{DispatchClass::kHot};
        GroupKey group_key{};
        GroupExecutionPolicy execution_policy{GroupExecutionPolicy::kAbortRemaining};
        internal::TimestampNs earliest_ingress_ts_ns{0};
        std::vector<std::optional<DispatchItem>> legs;
        std::size_t filled_legs{0};
    };

    BatchPlannerQueues queues_{};
    BatchPlannerConfig config_{};
    BatchPlannerTelemetry telemetry_{};
    DispatchRequestId next_dispatch_request_id_{1};
    std::unordered_map<GroupIntentId, PendingGroup> pending_groups_;
    std::deque<DispatchRequest> ready_hot_;
    std::deque<DispatchRequest> ready_recovery_;
    std::deque<DispatchRequest> ready_reconcile_;

    [[nodiscard]] bool try_take_item(DispatchItem& out_item) {
        if (queues_.hot_input_queue != nullptr && queues_.hot_input_queue->try_pop(out_item)) {
            return true;
        }
        if (queues_.recovery_input_queue != nullptr &&
            queues_.recovery_input_queue->try_pop(out_item)) {
            return true;
        }
        if (queues_.reconcile_input_queue != nullptr &&
            queues_.reconcile_input_queue->try_pop(out_item)) {
            return true;
        }
        return false;
    }

    [[nodiscard]] bool flush_ready_request() {
        return flush_ready_request_for_class(DispatchClass::kHot) ||
               flush_ready_request_for_class(DispatchClass::kRecovery) ||
               flush_ready_request_for_class(DispatchClass::kReconcile);
    }

    [[nodiscard]] bool flush_ready_request_for_class(DispatchClass dispatch_class) {
        auto& ready = ready_queue_for_class(dispatch_class);
        if (ready.empty()) {
            return false;
        }

        auto* queue = output_queue_for_class(dispatch_class);
        if (queue == nullptr) {
            return false;
        }
        if (!queue->try_push(std::move(ready.front()))) {
            ++telemetry_.output_backpressure;
            return false;
        }

        ready.pop_front();
        return true;
    }

    [[nodiscard]] bool emit_or_buffer(DispatchRequest request) {
        auto* queue = output_queue_for_class(request.dispatch_class);
        if (queue != nullptr && queue->try_push(request)) {
            if (request.batch_kind == DispatchBatchKind::kGroupedSubmit) {
                ++telemetry_.emitted_grouped_requests;
            } else {
                ++telemetry_.emitted_singleton_requests;
            }
            return true;
        }

        ready_queue_for_class(request.dispatch_class).push_back(std::move(request));
        ++telemetry_.output_backpressure;
        return false;
    }

    [[nodiscard]] bool buffer_group_item(DispatchItem item) {
        const auto& batch_group = *item.batch_group;
        if (!batch_group.valid() ||
            batch_group.group_key.expected_leg_count > config_.max_group_legs) {
            ++telemetry_.invalid_group_items;
            return emit_or_buffer(make_singleton_request(std::move(item)));
        }

        const GroupIntentId group_id = batch_group.group_key.group_intent_id;
        auto& pending = pending_groups_[group_id];
        if (pending.legs.empty()) {
            pending.dispatch_class = item.dispatch_class;
            pending.group_key = batch_group.group_key;
            pending.execution_policy = batch_group.execution_policy;
            pending.earliest_ingress_ts_ns = item.ingress_ts_ns;
            pending.legs.resize(batch_group.group_key.expected_leg_count);
        } else if (pending.group_key.expected_leg_count !=
                       batch_group.group_key.expected_leg_count ||
                   pending.execution_policy != batch_group.execution_policy ||
                   pending.dispatch_class != item.dispatch_class ||
                   batch_group.leg_index >= pending.legs.size()) {
            ++telemetry_.invalid_group_items;
            return emit_or_buffer(make_singleton_request(std::move(item)));
        }

        pending.earliest_ingress_ts_ns =
            pending.earliest_ingress_ts_ns == 0
                ? item.ingress_ts_ns
                : std::min(pending.earliest_ingress_ts_ns, item.ingress_ts_ns);

        if (!pending.legs[batch_group.leg_index].has_value()) {
            ++pending.filled_legs;
        }
        pending.legs[batch_group.leg_index] = std::move(item);
        ++telemetry_.buffered_group_items;

        if (pending.filled_legs != pending.legs.size()) {
            return true;
        }

        DispatchRequest request{
            .dispatch_request_id = next_dispatch_request_id_++,
            .dispatch_class = pending.dispatch_class,
            .batch_kind = DispatchBatchKind::kGroupedSubmit,
            .state = DispatchRequestState::kQueued,
            .budget_cost = DispatchBudgetCost{.transaction_units =
                                                  static_cast<std::uint32_t>(pending.legs.size())},
            .queued_ts_ns = pending.earliest_ingress_ts_ns,
            .planned_ts_ns = gateway_now_ns(),
            .admitted_ts_ns = 0,
            .session_submit_ts_ns = 0,
            .connection_start_ts_ns = 0,
            .group_key = pending.group_key,
            .batch_group =
                BatchGroupRequestMetadata{
                    .group_key = pending.group_key,
                    .execution_policy = pending.execution_policy,
                },
        };

        request.items.reserve(pending.legs.size());
        for (auto& leg : pending.legs) {
            if (!leg.has_value()) {
                ++telemetry_.invalid_group_items;
                pending_groups_.erase(group_id);
                return false;
            }
            request.items.push_back(std::move(*leg));
        }
        pending_groups_.erase(group_id);
        return emit_or_buffer(std::move(request));
    }

    [[nodiscard]] DispatchRequest make_singleton_request(DispatchItem item) {
        DispatchRequest request{
            .dispatch_request_id = next_dispatch_request_id_++,
            .dispatch_class = item.dispatch_class,
            .batch_kind = DispatchBatchKind::kSingleton,
            .state = DispatchRequestState::kQueued,
            .budget_cost = DispatchBudgetCost{.transaction_units = 1},
            .queued_ts_ns = item.ingress_ts_ns,
            .planned_ts_ns = gateway_now_ns(),
            .admitted_ts_ns = 0,
            .session_submit_ts_ns = 0,
            .connection_start_ts_ns = 0,
            .group_key = item.group_key,
            .batch_group = item.batch_group.has_value()
                               ? std::optional<BatchGroupRequestMetadata>(BatchGroupRequestMetadata{
                                     .group_key = item.batch_group->group_key,
                                     .execution_policy = item.batch_group->execution_policy,
                                 })
                               : std::nullopt,
        };
        request.items.push_back(std::move(item));
        return request;
    }

    [[nodiscard]] std::deque<DispatchRequest>& ready_queue_for_class(DispatchClass dispatch_class) {
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return ready_hot_;
        case DispatchClass::kRecovery:
            return ready_recovery_;
        case DispatchClass::kReconcile:
            return ready_reconcile_;
        }
        return ready_hot_;
    }

    [[nodiscard]] utils::SPSCQueue<DispatchRequest>*
    output_queue_for_class(DispatchClass dispatch_class) noexcept {
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return queues_.hot_output_queue;
        case DispatchClass::kRecovery:
            return queues_.recovery_output_queue;
        case DispatchClass::kReconcile:
            return queues_.reconcile_output_queue;
        }
        return nullptr;
    }
};

} // namespace predex::core::oms::kalshi::gateway
