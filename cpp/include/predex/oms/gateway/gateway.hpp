#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <utility>

#include "predex/oms/gateway/batch_planner.hpp"
#include "predex/oms/gateway/command_ingress.hpp"
#include "predex/oms/gateway/gateway_types.hpp"
#include "predex/oms/gateway/order_sequencer.hpp"
#include "predex/oms/gateway/rate_limiter.hpp"
#include "predex/oms/gateway/session_pool.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::gateway {

// Top-level transport/gateway orchestrator between OMS and venue I/O resources.
//
// Ownership:
// - owned by app/runtime wiring
// - owns transport-internal policy components by composition
//
// Responsibilities:
// - drain OMS command ingress
// - transform OMS commands into gateway dispatch objects
// - sequence by lineage
// - plan singleton/batched dispatch requests
// - apply rate-limit admission
// - hand admitted requests to SessionPool
// - merge REST/private-WS/recovery truth
// - emit normalized venue events back toward OMS
struct GatewayQueues {
    utils::SPSCQueue<OmsToKalshiCommand>* oms_command_queue{nullptr};
    utils::SPSCQueue<KalshiToOmsEvent>* venue_event_queue{nullptr};
};

struct GatewayConfig {
    std::size_t hot_queue_capacity{1024};
    std::size_t recovery_queue_capacity{256};
    std::size_t reconcile_queue_capacity{256};
};

struct GatewayTelemetry {
    std::uint64_t sequenced_items{0};
    std::uint64_t rate_limited_requests{0};
    std::uint64_t submitted_to_session_pool{0};
    std::uint64_t session_pool_backpressure{0};
    std::uint64_t emitted_venue_events{0};
    std::uint64_t venue_event_backpressure{0};
    std::uint64_t completed_requests{0};
    std::uint64_t uncertain_requests{0};
    std::uint64_t total_completion_latency_ns{0};
    std::uint64_t last_completion_latency_ns{0};
};

class Gateway {
  public:
    explicit Gateway(GatewayQueues queues,
                     SessionPool session_pool,
                     GatewayConfig config = {})
        : queues_(queues),
          hot_envelope_queue_(std::make_unique<EnvelopeQueue>(config.hot_queue_capacity)),
          recovery_envelope_queue_(std::make_unique<EnvelopeQueue>(config.recovery_queue_capacity)),
          reconcile_envelope_queue_(std::make_unique<EnvelopeQueue>(config.reconcile_queue_capacity)),
          hot_item_queue_(std::make_unique<ItemQueue>(config.hot_queue_capacity)),
          recovery_item_queue_(std::make_unique<ItemQueue>(config.recovery_queue_capacity)),
          reconcile_item_queue_(std::make_unique<ItemQueue>(config.reconcile_queue_capacity)),
          hot_request_queue_(std::make_unique<RequestQueue>(config.hot_queue_capacity)),
          recovery_request_queue_(std::make_unique<RequestQueue>(config.recovery_queue_capacity)),
          reconcile_request_queue_(std::make_unique<RequestQueue>(config.reconcile_queue_capacity)),
          admitted_hot_request_queue_(std::make_unique<RequestQueue>(config.hot_queue_capacity)),
          admitted_recovery_request_queue_(
              std::make_unique<RequestQueue>(config.recovery_queue_capacity)),
          admitted_reconcile_request_queue_(
              std::make_unique<RequestQueue>(config.reconcile_queue_capacity)),
          command_ingress_(
              CommandIngressQueues{
                  .oms_command_queue = queues_.oms_command_queue,
                  .session_class_queues =
                      SessionClassQueues{
                          .hot_queue = hot_envelope_queue_.get(),
                          .recovery_queue = recovery_envelope_queue_.get(),
                          .reconcile_queue = reconcile_envelope_queue_.get(),
                      },
              }),
          order_sequencer_(
              OrderSequencerQueues{
                  .hot_input_queue = hot_envelope_queue_.get(),
                  .recovery_input_queue = recovery_envelope_queue_.get(),
                  .reconcile_input_queue = reconcile_envelope_queue_.get(),
                  .hot_output_queue = hot_item_queue_.get(),
                  .recovery_output_queue = recovery_item_queue_.get(),
                  .reconcile_output_queue = reconcile_item_queue_.get(),
              }),
          batch_planner_(
              BatchPlannerQueues{
                  .hot_input_queue = hot_item_queue_.get(),
                  .recovery_input_queue = recovery_item_queue_.get(),
                  .reconcile_input_queue = reconcile_item_queue_.get(),
                  .hot_output_queue = hot_request_queue_.get(),
                  .recovery_output_queue = recovery_request_queue_.get(),
                  .reconcile_output_queue = reconcile_request_queue_.get(),
              }),
          rate_limiter_(
              RateLimiterQueues{
                  .hot_input_queue = hot_request_queue_.get(),
                  .recovery_input_queue = recovery_request_queue_.get(),
                  .reconcile_input_queue = reconcile_request_queue_.get(),
                  .hot_output_queue = admitted_hot_request_queue_.get(),
                  .recovery_output_queue = admitted_recovery_request_queue_.get(),
                  .reconcile_output_queue = admitted_reconcile_request_queue_.get(),
              }),
          session_pool_(std::move(session_pool)),
          config_(config) {}

    [[nodiscard]] bool pump_once() {
        bool made_progress = false;
        made_progress = session_pool_.drain_completions() || made_progress;
        made_progress = flush_session_pool_completions() || made_progress;
        made_progress = command_ingress_.drain_one() || made_progress;
        made_progress = order_sequencer_.drain_one() || made_progress;
        made_progress = batch_planner_.drain_one() || made_progress;
        made_progress = rate_limiter_.drain_one() || made_progress;
        made_progress = dispatch_one() || made_progress;
        made_progress = flush_venue_events() || made_progress;
        return made_progress;
    }

    [[nodiscard]] CommandIngress& command_ingress() noexcept { return command_ingress_; }
    [[nodiscard]] OrderSequencer& order_sequencer() noexcept { return order_sequencer_; }
    [[nodiscard]] BatchPlanner& batch_planner() noexcept { return batch_planner_; }
    [[nodiscard]] RateLimiter& rate_limiter() noexcept { return rate_limiter_; }
    [[nodiscard]] SessionPool& session_pool() noexcept { return session_pool_; }
    [[nodiscard]] const GatewayTelemetry& telemetry() const noexcept { return telemetry_; }

  private:
    using EnvelopeQueue = utils::SPSCQueue<CommandEnvelope>;
    using ItemQueue = utils::SPSCQueue<DispatchItem>;
    using RequestQueue = utils::SPSCQueue<DispatchRequest>;

    GatewayQueues queues_{};
    std::unique_ptr<EnvelopeQueue> hot_envelope_queue_;
    std::unique_ptr<EnvelopeQueue> recovery_envelope_queue_;
    std::unique_ptr<EnvelopeQueue> reconcile_envelope_queue_;
    std::unique_ptr<ItemQueue> hot_item_queue_;
    std::unique_ptr<ItemQueue> recovery_item_queue_;
    std::unique_ptr<ItemQueue> reconcile_item_queue_;
    std::unique_ptr<RequestQueue> hot_request_queue_;
    std::unique_ptr<RequestQueue> recovery_request_queue_;
    std::unique_ptr<RequestQueue> reconcile_request_queue_;
    std::unique_ptr<RequestQueue> admitted_hot_request_queue_;
    std::unique_ptr<RequestQueue> admitted_recovery_request_queue_;
    std::unique_ptr<RequestQueue> admitted_reconcile_request_queue_;
    CommandIngress command_ingress_;
    OrderSequencer order_sequencer_;
    BatchPlanner batch_planner_;
    RateLimiter rate_limiter_;
    SessionPool session_pool_;
    GatewayConfig config_{};
    GatewayTelemetry telemetry_{};

    std::deque<DispatchRequest> pending_hot_;
    std::deque<DispatchRequest> pending_recovery_;
    std::deque<DispatchRequest> pending_reconcile_;
    std::deque<SessionPoolCompletion> pending_completions_;
    std::deque<KalshiToOmsEvent> pending_venue_events_;

    [[nodiscard]] bool dispatch_one() {
        DispatchRequest request;
        if (try_take_admitted_request(DispatchClass::kHot, request) ||
            try_take_admitted_request(DispatchClass::kRecovery, request) ||
            try_take_admitted_request(DispatchClass::kReconcile, request)) {
            ++telemetry_.rate_limited_requests;
            switch (session_pool_.submit(request)) {
                case SessionPoolSubmitResult::kAccepted:
                    telemetry_.sequenced_items += request.item_count();
                    ++telemetry_.submitted_to_session_pool;
                    return true;
                case SessionPoolSubmitResult::kNoIdleConnection:
                case SessionPoolSubmitResult::kPoolUnavailable:
                    requeue_request(std::move(request));
                    ++telemetry_.session_pool_backpressure;
                    return false;
                case SessionPoolSubmitResult::kInvalidRequest:
                    release_request_lineages(request);
                    return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool flush_session_pool_completions() {
        bool made_progress = false;
        while (auto completion = session_pool_.poll_completion()) {
            pending_completions_.push_back(std::move(*completion));
            made_progress = true;
        }

        while (!pending_completions_.empty()) {
            auto& completion = pending_completions_.front();
            release_request_lineages(completion.completion.request);
            for (auto& event : completion.completion.emitted_events) {
                pending_venue_events_.push_back(std::move(event));
            }
            ++telemetry_.completed_requests;
            if (completion.completion.terminal_state == DispatchRequestState::kPostWriteUnknown) {
                ++telemetry_.uncertain_requests;
            }
            const auto completion_latency_ns =
                completion.completion.completed_ts_ns >= completion.completion.request.queued_ts_ns
                    ? completion.completion.completed_ts_ns -
                          completion.completion.request.queued_ts_ns
                    : 0;
            telemetry_.last_completion_latency_ns = completion_latency_ns;
            telemetry_.total_completion_latency_ns += completion_latency_ns;
            pending_completions_.pop_front();
            made_progress = true;
        }
        return made_progress;
    }

    [[nodiscard]] bool flush_venue_events() {
        if (queues_.venue_event_queue == nullptr) {
            pending_venue_events_.clear();
            return false;
        }

        bool made_progress = false;
        while (!pending_venue_events_.empty()) {
            if (!queues_.venue_event_queue->try_push(std::move(pending_venue_events_.front()))) {
                ++telemetry_.venue_event_backpressure;
                return made_progress;
            }
            pending_venue_events_.pop_front();
            ++telemetry_.emitted_venue_events;
            made_progress = true;
        }
        return made_progress;
    }

    [[nodiscard]] bool try_take_admitted_request(DispatchClass dispatch_class,
                                                 DispatchRequest& out_request) {
        auto& pending = pending_for_class(dispatch_class);
        if (!pending.empty()) {
            out_request = std::move(pending.front());
            pending.pop_front();
            return true;
        }

        auto* queue = admitted_queue_for_class(dispatch_class);
        if (queue == nullptr) {
            return false;
        }
        return queue->try_pop(out_request);
    }

    void requeue_request(DispatchRequest request) {
        pending_for_class(request.dispatch_class).push_front(std::move(request));
    }

    void release_request_lineages(const DispatchRequest& request) {
        for (const auto& item : request.items) {
            order_sequencer_.note_transport_complete(item.lineage_id);
        }
    }

    [[nodiscard]] std::deque<DispatchRequest>& pending_for_class(
        DispatchClass dispatch_class) {
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

    [[nodiscard]] RequestQueue* admitted_queue_for_class(DispatchClass dispatch_class) noexcept {
        switch (dispatch_class) {
            case DispatchClass::kHot:
                return admitted_hot_request_queue_.get();
            case DispatchClass::kRecovery:
                return admitted_recovery_request_queue_.get();
            case DispatchClass::kReconcile:
                return admitted_reconcile_request_queue_.get();
        }
        return nullptr;
    }
};

} // namespace predex::core::oms::kalshi::gateway
