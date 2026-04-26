#pragma once

#include <cstddef>
#include <utility>
#include <vector>

#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi {

// OMS owns this adapter, while the actual REST/private-WS worker threads own the
// opposite ends of the queues. Commands flow OMS -> REST thread. Venue truth
// flows REST/private-WS threads -> OMS via separate inbound queues to preserve
// SPSC invariants for each producer.
struct ExecutionTransportQueues {
    std::vector<utils::SPSCQueue<OmsToKalshiCommand>*> command_queues;
    std::vector<utils::SPSCQueue<KalshiToOmsEvent>*> rest_event_queues;
    utils::SPSCQueue<KalshiToOmsEvent>* ws_event_queue{nullptr};
};

class ExecutionTransport {
  public:
    explicit ExecutionTransport(ExecutionTransportQueues queues = {}) : queues_(queues) {}
//NOLINTNEXTLINE
    [[nodiscard]] bool try_send(const OmsToKalshiCommand& command) noexcept {
        auto* command_queue = select_command_queue(command);
        return command_queue != nullptr && command_queue->try_push(command);
    }

    [[nodiscard]] bool try_pop_event(SourcedKalshiEvent& event_out) noexcept {
        if (drain_rest_next_) {
            if (try_pop_rest_event(event_out)) {
                drain_rest_next_ = false;
                return true;
            }
            KalshiToOmsEvent raw_event{};
            if (queues_.ws_event_queue != nullptr &&
                queues_.ws_event_queue->try_pop(raw_event)) {
                event_out = SourcedKalshiEvent{
                    .source = KalshiEventSource::kPrivateWs,
                    .event = std::move(raw_event),
                };
                drain_rest_next_ = true;
                return true;
            }
            return false;
        }

        KalshiToOmsEvent raw_event{};
        if (queues_.ws_event_queue != nullptr && queues_.ws_event_queue->try_pop(raw_event)) {
            event_out = SourcedKalshiEvent{
                .source = KalshiEventSource::kPrivateWs,
                .event = std::move(raw_event),
            };
            drain_rest_next_ = true;
            return true;
        }
        if (try_pop_rest_event(event_out)) {
            drain_rest_next_ = false;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool command_available() const noexcept {
        for (const auto* queue : queues_.command_queues) {
            if (queue != nullptr) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool inbound_available() const noexcept {
        if (queues_.ws_event_queue != nullptr) {
            return true;
        }
        for (const auto* queue : queues_.rest_event_queues) {
            if (queue != nullptr) {
                return true;
            }
        }
        return false;
    }

  private:
    ExecutionTransportQueues queues_{};
    bool drain_rest_next_{true};
    std::size_t next_rest_event_queue_index_{0};
    std::size_t next_unkeyed_command_queue_index_{0};

    [[nodiscard]] static OmsRequestId command_request_id(
        const OmsToKalshiCommand& command) noexcept {
        return std::visit(
            [](const auto& typed_command) noexcept -> OmsRequestId {
                using T = std::decay_t<decltype(typed_command)>;
                if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                    return typed_command.order.oms_request_id;
                } else {
                    return typed_command.corr.order.oms_request_id;
                }
            },
            command);
    }

    [[nodiscard]] utils::SPSCQueue<OmsToKalshiCommand>* select_command_queue(
        const OmsToKalshiCommand& command) noexcept {
        const std::size_t queue_count = queues_.command_queues.size();
        if (queue_count == 0) {
            return nullptr;
        }

        const auto request_id = command_request_id(command);
        std::size_t start_index = 0;
        if (request_id == 0) {
            start_index = next_unkeyed_command_queue_index_ % queue_count;
            next_unkeyed_command_queue_index_ = (start_index + 1U) % queue_count;
        } else {
            start_index = static_cast<std::size_t>((request_id - 1U) % queue_count);
        }

        for (std::size_t offset = 0; offset < queue_count; ++offset) {
            const std::size_t index = (start_index + offset) % queue_count;
            auto* queue = queues_.command_queues[index];
            if (queue != nullptr) {
                return queue;
            }
        }
        return nullptr;
    }

    [[nodiscard]] bool try_pop_rest_event(SourcedKalshiEvent& event_out) noexcept {
        const std::size_t queue_count = queues_.rest_event_queues.size();
        if (queue_count == 0) {
            return false;
        }

        KalshiToOmsEvent raw_event{};
        for (std::size_t offset = 0; offset < queue_count; ++offset) {
            const std::size_t index = (next_rest_event_queue_index_ + offset) % queue_count;
            auto* queue = queues_.rest_event_queues[index];
            if (queue == nullptr || !queue->try_pop(raw_event)) {
                continue;
            }

            next_rest_event_queue_index_ = (index + 1U) % queue_count;
            const auto source = std::holds_alternative<ReconcileOpenOrderSnapshot>(raw_event)
                ? KalshiEventSource::kReconcile
                : KalshiEventSource::kRest;
            event_out = SourcedKalshiEvent{
                .source = source,
                .event = std::move(raw_event),
            };
            return true;
        }
        return false;
    }
};

} // namespace predex::core::oms::kalshi
