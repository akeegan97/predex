#pragma once

#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi {

// Queue handles wiring the OMS coordinator to the REST and private WS threads.
// Constructed in App and passed to Oms — pointers must outlive both.
struct OmsTransportQueues {
    utils::SPSCQueue<SubmitOrderCmd>* submit_queue{nullptr};
    utils::SPSCQueue<CancelOrderCmd>* cancel_queue{nullptr};
    utils::SPSCQueue<ModifyOrderCmd>* modify_queue{nullptr};
    utils::SPSCQueue<OrderLifecycleEvent>* inbound_update_queue{nullptr};
};

// Thin adapter over OmsTransportQueues. Encapsulates all outbound command
// dispatch and inbound lifecycle event ingestion. No business logic.
class ExecutionTransport {
  public:
    explicit ExecutionTransport(OmsTransportQueues queues = {});

    [[nodiscard]] bool try_submit(const SubmitOrderCmd& cmd) noexcept;

    [[nodiscard]] bool try_cancel(const CancelOrderCmd& cmd) noexcept;

    [[nodiscard]] bool try_modify(const ModifyOrderCmd& cmd) noexcept;

    // Pops one inbound lifecycle event. Returns false if none available.
    [[nodiscard]] bool try_pop_lifecycle_event(OrderLifecycleEvent& event_out) noexcept;

    [[nodiscard]] bool submit_available() const noexcept;

    [[nodiscard]] bool cancel_available() const noexcept;

    [[nodiscard]] bool modify_available() const noexcept;

    [[nodiscard]] bool inbound_available() const noexcept;

  private:
    OmsTransportQueues queues_{};
};

} // namespace predex::core::oms::kalshi
