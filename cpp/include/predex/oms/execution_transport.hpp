#pragma once

#include <utility>

#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi {

// OMS owns this adapter, while the actual REST/private-WS worker threads own the
// opposite ends of the queues. Commands flow OMS -> REST thread. Venue truth
// flows REST/private-WS threads -> OMS via separate inbound queues to preserve
// SPSC invariants for each producer.
struct ExecutionTransportQueues {
    utils::SPSCQueue<OmsToKalshiCommand>* command_queue{nullptr};
    utils::SPSCQueue<KalshiToOmsEvent>* rest_event_queue{nullptr};
    utils::SPSCQueue<KalshiToOmsEvent>* ws_event_queue{nullptr};
};

class ExecutionTransport {
  public:
    explicit ExecutionTransport(ExecutionTransportQueues queues = {}) : queues_(queues) {}
//NOLINTNEXTLINE
    [[nodiscard]] bool try_send(const OmsToKalshiCommand& command) noexcept {
        return queues_.command_queue != nullptr && queues_.command_queue->try_push(command);
    }

    [[nodiscard]] bool try_pop_event(SourcedKalshiEvent& event_out) noexcept {
        if (drain_rest_next_) {
            KalshiToOmsEvent raw_event{};
            if (queues_.rest_event_queue != nullptr &&
                queues_.rest_event_queue->try_pop(raw_event)) {
                const KalshiEventSource source =
                    std::holds_alternative<ReconcileOpenOrderSnapshot>(raw_event)
                        ? KalshiEventSource::kReconcile
                        : KalshiEventSource::kRest;
                event_out = SourcedKalshiEvent{
                    .source = source,
                    .event = std::move(raw_event),
                };
                drain_rest_next_ = false;
                return true;
            }
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
        if (queues_.rest_event_queue != nullptr && queues_.rest_event_queue->try_pop(raw_event)) {
            const KalshiEventSource source =
                std::holds_alternative<ReconcileOpenOrderSnapshot>(raw_event)
                    ? KalshiEventSource::kReconcile
                    : KalshiEventSource::kRest;
            event_out = SourcedKalshiEvent{
                .source = source,
                .event = std::move(raw_event),
            };
            drain_rest_next_ = false;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool command_available() const noexcept {
        return queues_.command_queue != nullptr;
    }

    [[nodiscard]] bool inbound_available() const noexcept {
        return queues_.rest_event_queue != nullptr || queues_.ws_event_queue != nullptr;
    }

  private:
    ExecutionTransportQueues queues_{};
    bool drain_rest_next_{true};
};

} // namespace predex::core::oms::kalshi
