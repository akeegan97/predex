#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "predex/oms/gateway/gateway_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::gateway {

struct OrderSequencerQueues {
    utils::SPSCQueue<CommandEnvelope>* hot_input_queue{nullptr};
    utils::SPSCQueue<CommandEnvelope>* recovery_input_queue{nullptr};
    utils::SPSCQueue<CommandEnvelope>* reconcile_input_queue{nullptr};
    utils::SPSCQueue<DispatchItem>* hot_output_queue{nullptr};
    utils::SPSCQueue<DispatchItem>* recovery_output_queue{nullptr};
    utils::SPSCQueue<DispatchItem>* reconcile_output_queue{nullptr};
};

struct OrderSequencerTelemetry {
    std::uint64_t admitted_items{0};
    std::uint64_t blocked_items{0};
    std::uint64_t released_lineages{0};
    std::uint64_t output_backpressure{0};
};

class OrderSequencer {
  public:
    explicit OrderSequencer(OrderSequencerQueues queues = {}) : queues_(queues) {}

    // Drains one ingress envelope and either:
    // - admits it immediately when the lineage is currently free, or
    // - parks it behind the active lineage so transport completion can release it later.
    [[nodiscard]] bool drain_one() {
        CommandEnvelope envelope;
        DispatchClass dispatch_class = DispatchClass::kHot;
        if (!try_take_envelope(dispatch_class, envelope)) {
            return false;
        }

        DispatchItem item{
            .dispatch_item_id = envelope.dispatch_item_id,
            .lineage_id = envelope.lineage_id,
            .lineage_key = std::move(envelope.lineage_key),
            .group_key = std::move(envelope.group_key),
            .batch_group = std::move(envelope.batch_group),
            .operation = envelope.operation,
            .dispatch_class = envelope.dispatch_class,
            .state = DispatchItemState::kPending,
            .ingress_ts_ns = envelope.ingress_ts_ns,
            .command = std::move(envelope.command),
        };

        if (lineage_blocked_.contains(item.lineage_id)) {
            pending_by_lineage_[item.lineage_id].push_back(PendingSequencedItem{
                .dispatch_class = dispatch_class,
                .item = std::move(item),
            });
            ++telemetry_.blocked_items;
            return true;
        }

        if (!emit_ready_item(dispatch_class, item)) {
            pending_by_lineage_[item.lineage_id].push_front(PendingSequencedItem{
                .dispatch_class = dispatch_class,
                .item = std::move(item),
            });
            ++telemetry_.output_backpressure;
            return false;
        }

        lineage_blocked_.insert(item.lineage_id);
        ++telemetry_.admitted_items;
        return true;
    }

    // Called by Gateway when transport has reached a terminal state for the current
    // lineage head, allowing the next queued item for that lineage to advance.
    void note_transport_complete(LineageId lineage_id) {
        if (lineage_id == 0) {
            return;
        }
        lineage_blocked_.erase(lineage_id);
        ++telemetry_.released_lineages;

        auto pending_it = pending_by_lineage_.find(lineage_id);
        if (pending_it == pending_by_lineage_.end() || pending_it->second.empty()) {
            return;
        }

        PendingSequencedItem pending = std::move(pending_it->second.front());
        pending_it->second.pop_front();
        if (pending_it->second.empty()) {
            pending_by_lineage_.erase(pending_it);
        }

        if (emit_ready_item(pending.dispatch_class, pending.item)) {
            lineage_blocked_.insert(lineage_id);
            ++telemetry_.admitted_items;
            return;
        }

        pending_by_lineage_[lineage_id].push_front(std::move(pending));
        ++telemetry_.output_backpressure;
    }

    [[nodiscard]] const OrderSequencerTelemetry& telemetry() const noexcept {
        return telemetry_;
    }

  private:
    struct PendingSequencedItem {
        DispatchClass dispatch_class{DispatchClass::kHot};
        DispatchItem item;
    };

    OrderSequencerQueues queues_{};
    std::unordered_set<LineageId> lineage_blocked_;
    std::unordered_map<LineageId, std::deque<PendingSequencedItem>> pending_by_lineage_;
    OrderSequencerTelemetry telemetry_{};

    [[nodiscard]] bool try_take_envelope(DispatchClass& dispatch_class,
                                         CommandEnvelope& out_envelope) {
        if (queues_.hot_input_queue != nullptr && queues_.hot_input_queue->try_pop(out_envelope)) {
            dispatch_class = DispatchClass::kHot;
            return true;
        }
        if (queues_.recovery_input_queue != nullptr &&
            queues_.recovery_input_queue->try_pop(out_envelope)) {
            dispatch_class = DispatchClass::kRecovery;
            return true;
        }
        if (queues_.reconcile_input_queue != nullptr &&
            queues_.reconcile_input_queue->try_pop(out_envelope)) {
            dispatch_class = DispatchClass::kReconcile;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool emit_ready_item(DispatchClass dispatch_class, DispatchItem& item) {
        auto* queue = output_queue_for_class(dispatch_class);
        if (queue == nullptr) {
            return false;
        }
        return queue->try_push(std::move(item));
    }

    [[nodiscard]] utils::SPSCQueue<DispatchItem>* output_queue_for_class(
        DispatchClass dispatch_class) noexcept {
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
