#include "predex/oms/execution_transport.hpp"

namespace predex::core::oms::kalshi {

ExecutionTransport::ExecutionTransport(OmsTransportQueues queues)
    : queues_(std::move(queues)) {}

bool ExecutionTransport::try_submit(const SubmitOrderCmd& cmd) noexcept {
    return queues_.submit_queue != nullptr && queues_.submit_queue->try_push(cmd);
}

bool ExecutionTransport::try_cancel(const CancelOrderCmd& cmd) noexcept {
    return queues_.cancel_queue != nullptr && queues_.cancel_queue->try_push(cmd);
}

bool ExecutionTransport::try_modify(const ModifyOrderCmd& cmd) noexcept {
    return queues_.modify_queue != nullptr && queues_.modify_queue->try_push(cmd);
}

bool ExecutionTransport::try_pop_lifecycle_event(OrderLifecycleEvent& event_out) noexcept {
    return queues_.inbound_update_queue != nullptr &&
           queues_.inbound_update_queue->try_pop(event_out);
}

bool ExecutionTransport::submit_available() const noexcept {
    return queues_.submit_queue != nullptr;
}

bool ExecutionTransport::cancel_available() const noexcept {
    return queues_.cancel_queue != nullptr;
}

bool ExecutionTransport::modify_available() const noexcept {
    return queues_.modify_queue != nullptr;
}

bool ExecutionTransport::inbound_available() const noexcept {
    return queues_.inbound_update_queue != nullptr;
}

} // namespace predex::core::oms::kalshi
