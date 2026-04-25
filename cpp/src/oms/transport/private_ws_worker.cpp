#include "predex/oms/transport/private_ws_worker.hpp"

#include <algorithm>
#include <chrono>
#include <thread>
#include <utility>
namespace predex::core::oms::kalshi::transport {
namespace {

constexpr auto kIdleSleep = std::chrono::milliseconds{1};

} // namespace

PrivateWsWorker::PrivateWsWorker(PrivateWsWorkerQueues queues,
                                 PrivateWsWorkerConfig config,
                                 KalshiPrivateWsAdapter adapter,
                                 predex::websocket::BoostBeastWsTransport transport)
    : queues_(queues),
      config_(std::move(config)),
      adapter_(std::move(adapter)),
      transport_(std::move(transport)),
      session_(transport_, adapter_.ws_adapter()) {}

void PrivateWsWorker::run(const std::stop_token& stop_token) {
    std::uint32_t reconnect_attempts = 0;

    while (!stop_token.stop_requested()) {
        if (!ensure_connected_and_subscribed()) {
            const auto backoff_ms =
                std::min<std::uint32_t>(5000U, 100U * (1U << std::min(reconnect_attempts, 5U)));
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            ++reconnect_attempts;
            continue;
        }

        reconnect_attempts = 0;
        if (process_one_message()) {
            continue;
        }

        std::this_thread::sleep_for(kIdleSleep);
    }

    close();
}

void PrivateWsWorker::close() {
    session_.close();
    connected_ = false;
    adapter_.reset_sequence_tracking();
}

std::optional<PrivateWsReconcileRequest> PrivateWsWorker::reconciliation_request() const noexcept {
    return reconcile_request_;
}

std::optional<PrivateWsReconcileRequest> PrivateWsWorker::take_reconciliation_request() noexcept {
    auto request = std::exchange(reconcile_request_, std::nullopt);
    if (request.has_value() && request->reason == PrivateWsReconcileReason::kSeqGap) {
        awaiting_seq_gap_reconcile_ = false;
    }
    return request;
}

bool PrivateWsWorker::ensure_connected_and_subscribed() {
    if (connected_) {
        return true;
    }
    if (awaiting_seq_gap_reconcile_) {
        return false;
    }

    adapter_.reset_sequence_tracking();
    if (!session_.connect()) {
        return false;
    }

    for (const auto& channel : config_.channels) {
        if (!session_.subscribe(channel)) {
            session_.close();
            return false;
        }
    }

    connected_ = true;
    mark_reconnect_needed_(PrivateWsReconcileReason::kReconnect);
    return true;
}

bool PrivateWsWorker::process_one_message() {
    const auto recv_result = session_.recv_text(config_.recv_timeout);
    if (recv_result.status == predex::websocket::RecvStatus::kTimeout) {
        return false;
    }
    if (recv_result.status == predex::websocket::RecvStatus::kClosed) {
        session_.close();
        connected_ = false;
        adapter_.reset_sequence_tracking();
        mark_reconnect_needed_(PrivateWsReconcileReason::kReconnect);
        return false;
    }
    if (recv_result.status == predex::websocket::RecvStatus::kError) {
        session_.close();
        connected_ = false;
        adapter_.reset_sequence_tracking();
        return false;
    }

    auto parse_result = adapter_.parse_message(recv_result.payload);
    if (!parse_result.error_message.empty()) {
        return false;
    }

    if (parse_result.reconcile_request.has_value()) {
        reconcile_request_ = parse_result.reconcile_request;
        if (parse_result.reconcile_request->reason == PrivateWsReconcileReason::kSeqGap) {
            awaiting_seq_gap_reconcile_ = true;
            session_.close();
            connected_ = false;
            adapter_.reset_sequence_tracking();
            return false;
        }
    }

    for (const auto& event : parse_result.events) {
        if (!emit_event(event)) {
            return false;
        }
    }
    return !parse_result.events.empty();
}

bool PrivateWsWorker::emit_event(const KalshiToOmsEvent& event) {
    if (queues_.event_queue == nullptr) {
        return false;
    }

    while (!queues_.event_queue->try_push(event)) {
        std::this_thread::sleep_for(kIdleSleep);
    }
    return true;
}

void PrivateWsWorker::mark_reconnect_needed_(PrivateWsReconcileReason reason) noexcept {
    reconcile_request_ = PrivateWsReconcileRequest{.reason = reason};
}

} // namespace predex::core::oms::kalshi::transport
