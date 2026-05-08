#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <utility>

#include "predex/oms/gateway/gateway_types.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::gateway {

struct RateLimiterQueues {
    utils::SPSCQueue<DispatchRequest>* hot_input_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* recovery_input_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* reconcile_input_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* hot_output_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* recovery_output_queue{nullptr};
    utils::SPSCQueue<DispatchRequest>* reconcile_output_queue{nullptr};
};

struct RateLimiterConfig {
    std::uint32_t initial_transaction_units{100};
    std::uint32_t refill_transaction_units{100};
    std::chrono::nanoseconds refill_interval{std::chrono::seconds{1}};
};

struct RateLimiterTelemetry {
    std::uint64_t admitted_requests{0};
    std::uint64_t deferred_requests{0};
    std::uint64_t rejected_invalid_requests{0};
    std::uint64_t output_backpressure{0};
    std::uint64_t refill_count{0};
};

// Request-level admission control between BatchPlanner and SessionPool.
//
// Responsibilities:
// - admit dispatch requests by transaction-unit budget
// - defer requests when the budget is temporarily exhausted
// - preserve dispatch class while forwarding admitted requests
//
// Non-responsibilities:
// - batching
// - sequencing
// - session assignment
// - recovery decisions
class RateLimiter {
  public:
    explicit RateLimiter(RateLimiterQueues queues = {}, RateLimiterConfig config = {})
        : queues_(queues), config_(config),
          available_transaction_units_(config.initial_transaction_units),
          last_refill_tp_(std::chrono::steady_clock::now()) {}

    [[nodiscard]] bool drain_one() {
        refill_budget_if_needed();

        if (flush_pending_request()) {
            return true;
        }

        DispatchRequest request;
        if (!try_take_request(request)) {
            return false;
        }

        if (!request_is_valid(request)) {
            ++telemetry_.rejected_invalid_requests;
            return true;
        }

        const auto required_units = required_transaction_units(request);
        if (required_units > available_transaction_units_) {
            pending_for_class(request.dispatch_class).push_back(std::move(request));
            ++telemetry_.deferred_requests;
            return true;
        }

        request.admitted_ts_ns = gateway_now_ns();
        request.state = DispatchRequestState::kAdmitted;
        available_transaction_units_ -= required_units;
        return emit_admitted_request(std::move(request));
    }

    [[nodiscard]] const RateLimiterTelemetry& telemetry() const noexcept { return telemetry_; }

    [[nodiscard]] std::uint32_t available_transaction_units() const noexcept {
        return available_transaction_units_;
    }

  private:
    RateLimiterQueues queues_{};
    RateLimiterConfig config_{};
    RateLimiterTelemetry telemetry_{};
    std::uint32_t available_transaction_units_{0};
    std::chrono::steady_clock::time_point last_refill_tp_{};
    std::deque<DispatchRequest> pending_hot_;
    std::deque<DispatchRequest> pending_recovery_;
    std::deque<DispatchRequest> pending_reconcile_;

    void refill_budget_if_needed() {
        const auto now = std::chrono::steady_clock::now();
        if (config_.refill_interval.count() <= 0) {
            return;
        }

        while ((now - last_refill_tp_) >= config_.refill_interval) {
            available_transaction_units_ += config_.refill_transaction_units;
            last_refill_tp_ += config_.refill_interval;
            ++telemetry_.refill_count;
        }
    }

    [[nodiscard]] bool flush_pending_request() {
        return flush_pending_request_for_class(DispatchClass::kRecovery) ||
               flush_pending_request_for_class(DispatchClass::kHot) ||
               flush_pending_request_for_class(DispatchClass::kReconcile);
    }

    [[nodiscard]] bool flush_pending_request_for_class(DispatchClass dispatch_class) {
        auto& pending = pending_for_class(dispatch_class);
        if (pending.empty()) {
            return false;
        }

        const auto required_units = required_transaction_units(pending.front());
        if (required_units > available_transaction_units_) {
            return false;
        }

        DispatchRequest request = std::move(pending.front());
        pending.pop_front();
        request.admitted_ts_ns = gateway_now_ns();
        request.state = DispatchRequestState::kAdmitted;
        available_transaction_units_ -= required_units;
        return emit_admitted_request(std::move(request));
    }

    [[nodiscard]] bool emit_admitted_request(DispatchRequest request) {
        auto* queue = output_queue_for_class(request.dispatch_class);
        if (queue == nullptr || !queue->try_push(request)) {
            pending_for_class(request.dispatch_class).push_front(std::move(request));
            ++telemetry_.output_backpressure;
            return false;
        }

        ++telemetry_.admitted_requests;
        return true;
    }

    [[nodiscard]] bool try_take_request(DispatchRequest& out_request) {
        if (queues_.hot_input_queue != nullptr && queues_.hot_input_queue->try_pop(out_request)) {
            return true;
        }
        if (queues_.recovery_input_queue != nullptr &&
            queues_.recovery_input_queue->try_pop(out_request)) {
            return true;
        }
        if (queues_.reconcile_input_queue != nullptr &&
            queues_.reconcile_input_queue->try_pop(out_request)) {
            return true;
        }
        return false;
    }

    [[nodiscard]] static bool request_is_valid(const DispatchRequest& request) {
        return !request.empty() && required_transaction_units(request) > 0;
    }

    [[nodiscard]] static std::uint32_t required_transaction_units(const DispatchRequest& request) {
        if (!request.budget_cost.zero()) {
            return request.budget_cost.transaction_units;
        }
        return static_cast<std::uint32_t>(request.item_count());
    }

    [[nodiscard]] std::deque<DispatchRequest>& pending_for_class(DispatchClass dispatch_class) {
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return pending_hot_;
        case DispatchClass::kRecovery:
            return pending_recovery_;
        case DispatchClass::kReconcile:
            return pending_reconcile_;
        }
        return pending_hot_;
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
