#pragma once

#include <chrono>
#include <cstddef>
#include <utility>
#include <vector>

#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi {

struct ExecutionTransportQueues {
    std::vector<utils::SPSCQueue<OmsToKalshiCommand>*> command_queues;
    std::vector<utils::SPSCQueue<KalshiToOmsEvent>*> rest_event_queues;
    utils::SPSCQueue<KalshiToOmsEvent>* ws_event_queue{nullptr};
};

// Minimal queue-based OMS transport seam.
//
// OMS remains single-writer and single-consumer here:
// - outbound commands are pushed toward the transport runtime
// - sourced venue events are drained back into OMS
//
// The higher-level gateway/runtime can evolve behind these queues without
// forcing OMS command/event churn.
class ExecutionTransport {
  public:
    explicit ExecutionTransport(ExecutionTransportQueues queues = {})
        : queues_(std::move(queues)) {}

    [[nodiscard]] bool try_send(const OmsToKalshiCommand& command) {
        if (queues_.command_queues.empty()) {
            return false;
        }

        OmsToKalshiCommand stamped_command = command;
        stamp_transport_enqueue_ts(stamped_command);

        const std::size_t queue_count = queues_.command_queues.size();
        for (std::size_t offset = 0; offset < queue_count; ++offset) {
            const std::size_t index = (next_command_queue_index_ + offset) % queue_count;
            auto* queue = queues_.command_queues[index];
            if (queue == nullptr) {
                continue;
            }
            if (!queue->try_push(stamped_command)) {
                continue;
            }
            next_command_queue_index_ = (index + 1U) % queue_count;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool try_pop_event(SourcedKalshiEvent& out_event) noexcept {
        KalshiToOmsEvent raw_event;
        if (queues_.ws_event_queue != nullptr && queues_.ws_event_queue->try_pop(raw_event)) {
            out_event = SourcedKalshiEvent{
                .source = KalshiEventSource::kPrivateWs,
                .event = std::move(raw_event),
            };
            return true;
        }

        if (queues_.rest_event_queues.empty()) {
            return false;
        }

        const std::size_t queue_count = queues_.rest_event_queues.size();
        for (std::size_t offset = 0; offset < queue_count; ++offset) {
            const std::size_t index = (next_event_queue_index_ + offset) % queue_count;
            auto* queue = queues_.rest_event_queues[index];
            if (queue == nullptr) {
                continue;
            }
            if (!queue->try_pop(raw_event)) {
                continue;
            }
            next_event_queue_index_ = (index + 1U) % queue_count;
            out_event = SourcedKalshiEvent{
                .source = KalshiEventSource::kRest,
                .event = std::move(raw_event),
            };
            return true;
        }
        return false;
    }

  private:
    ExecutionTransportQueues queues_{};
    std::size_t next_command_queue_index_{0};
    std::size_t next_event_queue_index_{0};

    [[nodiscard]] static internal::TimestampNs now_ns() noexcept {
        return static_cast<internal::TimestampNs>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }

    static void stamp_transport_enqueue_ts(OmsToKalshiCommand& command) {
        const auto enqueue_ts_ns = now_ns();
        std::visit(
            [&](auto& typed_command) {
                typed_command.transport_enqueue_ts_ns = enqueue_ts_ns;
            },
            command);
    }
};

} // namespace predex::core::oms::kalshi
