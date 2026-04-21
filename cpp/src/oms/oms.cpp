#include "predex/oms/oms.hpp"
#include "predex/oms/oms_types.hpp"

#include <chrono>
#include <cstdio>
#include <limits>

namespace predex::core::oms::kalshi {

namespace {

[[nodiscard]] std::int64_t latency_delta_ns(internal::TimestampNs end_ts_ns,
                                             internal::TimestampNs start_ts_ns) {
    if (end_ts_ns <= start_ts_ns) {
        return 0;
    }
    const auto raw_delta = end_ts_ns - start_ts_ns;
    constexpr auto max_i64 =
        static_cast<internal::TimestampNs>(std::numeric_limits<std::int64_t>::max());
    if (raw_delta > max_i64) {
        return std::numeric_limits<std::int64_t>::max();
    }
    return static_cast<std::int64_t>(raw_delta);
}

[[nodiscard]] internal::TimestampNs monotonic_now_ns() {
    return static_cast<internal::TimestampNs>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] OmsRequestId extract_oms_request_id(const IntentDecision& decision) {
    if (const auto* accepted = std::get_if<AcceptedIntent>(&decision.data)) {
        return accepted->oms_request_id;
    }
    if (const auto* modified = std::get_if<ModifiedIntent>(&decision.data)) {
        return modified->oms_request_id;
    }
    return 0;
}

[[nodiscard]] std::uint8_t extract_reject_reason(const IntentDecision& decision) {
    if (const auto* rejected = std::get_if<RejectedIntent>(&decision.data)) {
        return static_cast<std::uint8_t>(rejected->reason);
    }
    if (const auto* modified = std::get_if<ModifiedIntent>(&decision.data)) {
        return static_cast<std::uint8_t>(modified->reason);
    }
    return 0;
}

} // namespace

Oms::Oms(std::vector<SubmissionQueue*> shard_intent_queues,
         std::vector<DecisionQueue*> shard_decision_queues,
         std::vector<LifecycleQueue*> shard_lifecycle_queues,
         OmsTransportQueues transport_queues,
         GlobalRiskManager global_risk,
         AuditQueue* audit_queue,
         std::int64_t max_session_loss_ticks)
    //NOLINTNEXTLINE
    : risk_engine_(std::move(global_risk)),
      transport_(std::move(transport_queues)),
      shard_intent_queues_(std::move(shard_intent_queues)),
      shard_decision_queues_(std::move(shard_decision_queues)),
      shard_lifecycle_queues_(std::move(shard_lifecycle_queues)),
      audit_queue_(audit_queue),
      max_session_loss_ticks_(max_session_loss_ticks) {}

//NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
OmsPumpResult Oms::pump(std::size_t max_transport_updates,
                         std::size_t max_shard_intents) noexcept {
    if (halt_mode_.load(std::memory_order_acquire) >=
            static_cast<std::uint8_t>(HaltMode::kHard) &&
        !hard_halt_cancel_triggered_) {
        if (!cancel_all_live_orders()) {
            return OmsPumpResult{
                .code = OmsProcessCode::kTransportBackpressure,
                .processed_intents = 0,
                .processed_transport_updates = 0,
            };
        }
        hard_halt_cancel_triggered_ = true;
    }

    OmsPumpResult result{};

    for (std::size_t i = 0; i < max_transport_updates; i++) {
        const OmsProcessCode code = process_one_transport_update();
        if (code == OmsProcessCode::kIdle) {
            break;
        }
        result.code = code;
        if (code == OmsProcessCode::kProcessedTransportUpdate) {
            ++result.processed_transport_updates;
            continue;
        }
        return result;
    }

    for (std::size_t i = 0; i < max_shard_intents; i++) {
        const OmsProcessCode code = process_one_shard_intent();
        if (code == OmsProcessCode::kIdle) {
            if (result.processed_transport_updates > 0) {
                result.code = OmsProcessCode::kProcessedTransportUpdate;
            }
            break;
        }
        result.code = code;
        if (code == OmsProcessCode::kProcessedIntent) {
            ++result.processed_intents;
            continue;
        }
        return result;
    }

    if (result.processed_intents > 0 && result.code == OmsProcessCode::kIdle) {
        result.code = OmsProcessCode::kProcessedIntent;
    } else if (result.processed_transport_updates > 0 &&
               result.code == OmsProcessCode::kIdle) {
        result.code = OmsProcessCode::kProcessedTransportUpdate;
    }
    return result;
}

const GlobalRiskState& Oms::global_risk_state() const noexcept {
    return risk_engine_.global_state();
}

std::size_t Oms::live_order_count() const noexcept {
    return order_store_.live_order_count();
}

std::uint64_t Oms::processed_intent_count() const noexcept {
    return processed_intent_count_;
}

std::uint64_t Oms::processed_transport_update_count() const noexcept {
    return processed_transport_update_count_;
}

std::uint64_t Oms::rejected_intent_count() const noexcept {
    return rejected_intent_count_;
}

std::uint64_t Oms::unknown_fill_side_count() const noexcept {
    return unknown_fill_side_count_;
}

OmsRequestId Oms::seed_orphaned_order(OrderState state,
                                      internal::Side side,
                                      Outcome outcome) noexcept {
    const OmsRequestId request_id = next_oms_request_id_++;
    state.oms_request_id = request_id;
    const AcceptedIntent accepted{
        .intent = OrderIntent{
            .origin = state.origin,
            .exchange = internal::ExchangeId::kKalshi,
            .side = side,
            .outcome = outcome,
            .qty_lots = state.live_qty_lots,
            .time_in_force = OmsTimeInForce::kGtc,
        },
        .oms_request_id = request_id,
        .client_order_id = state.client_order_id,
    };
    risk_engine_.on_intent_accepted(accepted);
    order_store_.adopt_orphaned(std::move(state));
    return request_id;
}

void Oms::request_soft_halt() noexcept {
    std::uint8_t expected = static_cast<std::uint8_t>(HaltMode::kNone);
    halt_mode_.compare_exchange_strong(expected,
                                       static_cast<std::uint8_t>(HaltMode::kSoft),
                                       std::memory_order_release,
                                       std::memory_order_relaxed);
}

void Oms::request_hard_halt() noexcept {
    halt_mode_.store(static_cast<std::uint8_t>(HaltMode::kHard), std::memory_order_release);
}

bool Oms::cancel_all_live_orders() noexcept {
    const internal::TimestampNs now_ns = monotonic_now_ns();
    const auto cmds = order_store_.build_cancel_all_cmds(now_ns);
    bool all_enqueued = true;
    for (const auto& cmd : cmds) {
        if (!transport_.try_cancel(cmd)) {
            all_enqueued = false;
        }
    }
    return all_enqueued;
}

bool Oms::is_halted() const noexcept {
    return halt_mode_.load(std::memory_order_acquire) !=
           static_cast<std::uint8_t>(HaltMode::kNone);
}

std::int64_t Oms::session_net_ticks() const noexcept {
    return session_net_ticks_;
}

OmsProcessCode Oms::process_one_transport_update() noexcept {
    OrderLifecycleEvent event{};
    if (!transport_.try_pop_lifecycle_event(event)) {
        return OmsProcessCode::kIdle;
    }

    OrderState* const tracked = order_store_.find_order_state(event);
    if (tracked == nullptr) {
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kOmsLifecycle,
            .ts_ns = event.recv_ts_ns,
            .shard_id = event.origin.shard_id,
            .signal_id = event.origin.signal_id,
            .group_id = event.origin.group_id,
            .local_intent_id = event.origin.local_intent_id,
            .oms_request_id = event.oms_request_id,
            .exchange = internal::ExchangeId::kKalshi,
            .event_id = event.origin.event_id,
            .market_id = event.origin.market_id,
            .side = internal::Side::kUnknown,
            .leg_index = event.origin.leg_index,
            .leg_count = event.origin.leg_count,
            .lifecycle_kind = static_cast<std::uint8_t>(event.kind),
            .order_status = static_cast<std::uint8_t>(event.status),
        });
        ++processed_transport_update_count_;
        return OmsProcessCode::kProcessedTransportUpdate;
    }

    // Snapshot fields before apply_lifecycle_event may erase the order on terminal events.
    const IntentOrigin origin = tracked->origin;
    const OmsRequestId resolved_request_id = tracked->oms_request_id;
    const internal::TimestampNs decision_ts_ns = tracked->oms_decision_ts_ns;
    const internal::TimestampNs transport_ts_ns = tracked->transport_submit_ts_ns;
    const std::optional<internal::PriceTicks> tracked_limit_price_ticks =
        tracked->live_limit_price_ticks;

    const auto apply_result = order_store_.apply_lifecycle_event(event);
    if (!apply_result.ok) {
        return OmsProcessCode::kError;
    }

    if (apply_result.fill_qty_lots > 0) {
        risk_engine_.on_fill(apply_result.event_id, apply_result.fill_qty_lots);
        if (tracked_limit_price_ticks.has_value() && *tracked_limit_price_ticks > 0) {
            const std::int64_t released_capital_ticks =
                static_cast<std::int64_t>(apply_result.fill_qty_lots) *
                static_cast<std::int64_t>(*tracked_limit_price_ticks);
            risk_engine_.on_capital_released(released_capital_ticks);
        }
        if (const auto* fill = std::get_if<OrderFill>(&event.data)) {
            const std::int64_t fill_value =
                static_cast<std::int64_t>(fill->fill_price_ticks) *
                static_cast<std::int64_t>(apply_result.fill_qty_lots);
            if (fill->side == internal::Side::kBuy || fill->side == internal::Side::kBid) {
                session_net_ticks_ -= fill_value;
            } else if (fill->side == internal::Side::kSell || fill->side == internal::Side::kAsk) {
                session_net_ticks_ += fill_value;
            } else {
                ++unknown_fill_side_count_;
                emit_audit(predex::core::audit::AuditEvent{
                    .kind = predex::core::audit::AuditKind::kPipelineProbe,
                    .ts_ns = event.recv_ts_ns,
                    .shard_id = origin.shard_id,
                    .signal_id = origin.signal_id,
                    .group_id = origin.group_id,
                    .local_intent_id = origin.local_intent_id,
                    .oms_request_id = resolved_request_id,
                    .exchange = internal::ExchangeId::kKalshi,
                    .event_id = origin.event_id,
                    .market_id = origin.market_id,
                    .side = internal::Side::kUnknown,
                    .leg_index = origin.leg_index,
                    .leg_count = origin.leg_count,
                    .qty_lots = apply_result.fill_qty_lots,
                    .price_ticks = fill->fill_price_ticks,
                    .score = static_cast<std::int64_t>(unknown_fill_side_count_),
                    .lifecycle_kind = static_cast<std::uint8_t>(event.kind),
                    .order_status = static_cast<std::uint8_t>(event.status),
                });
                std::fprintf(stderr,
                             "[oms] unknown fill side (count=%llu, oms_request_id=%llu, "
                             "action=%s, side=%s, is_yes=%d)\n",
                             static_cast<unsigned long long>(unknown_fill_side_count_),
                             static_cast<unsigned long long>(resolved_request_id),
                             fill->raw_action.c_str(),
                             fill->raw_side.c_str(),
                             static_cast<int>(fill->raw_is_yes));
            }
            if (max_session_loss_ticks_ > 0 &&
                session_net_ticks_ < -max_session_loss_ticks_ &&
                halt_mode_.load(std::memory_order_acquire) ==
                    static_cast<std::uint8_t>(HaltMode::kNone)) {
                request_soft_halt();
            }
        }
    }
    if (apply_result.is_terminal) {
        if (tracked_limit_price_ticks.has_value() && *tracked_limit_price_ticks > 0 &&
            apply_result.remaining_open_qty_lots > 0) {
            const std::int64_t released_capital_ticks =
                static_cast<std::int64_t>(apply_result.remaining_open_qty_lots) *
                static_cast<std::int64_t>(*tracked_limit_price_ticks);
            risk_engine_.on_capital_released(released_capital_ticks);
        }
        risk_engine_.on_order_terminal(apply_result.event_id,
                                       apply_result.remaining_open_qty_lots);
    }

    emit_audit(predex::core::audit::AuditEvent{
        .kind = predex::core::audit::AuditKind::kOmsLifecycle,
        .ts_ns = event.recv_ts_ns,
        .shard_id = origin.shard_id,
        .signal_id = origin.signal_id,
        .group_id = origin.group_id,
        .local_intent_id = origin.local_intent_id,
        .oms_request_id = resolved_request_id,
        .tick_recv_ns = origin.tick_recv_ns,
        .signal_ts_ns = origin.signal_ts_ns,
        .submission_enqueued_ns = origin.submission_enqueued_ns,
        .oms_decision_ts_ns = decision_ts_ns,
        .transport_submit_ts_ns = transport_ts_ns,
        .first_fill_recv_ns = apply_result.first_fill_recv_ns,
        .terminal_recv_ns = apply_result.is_terminal ? event.recv_ts_ns : 0,
        .decision_to_transport_ns = latency_delta_ns(transport_ts_ns, decision_ts_ns),
        .transport_to_first_fill_ns =
            apply_result.first_fill_recv_ns > 0
                ? latency_delta_ns(apply_result.first_fill_recv_ns, transport_ts_ns)
                : 0,
        .tick_to_first_fill_ns =
            apply_result.first_fill_recv_ns > 0
                ? latency_delta_ns(apply_result.first_fill_recv_ns, origin.tick_recv_ns)
                : 0,
        .tick_to_terminal_ns =
            apply_result.is_terminal
                ? latency_delta_ns(event.recv_ts_ns, origin.tick_recv_ns)
                : 0,
        .exchange = internal::ExchangeId::kKalshi,
        .event_id = origin.event_id,
        .market_id = origin.market_id,
        .side = internal::Side::kUnknown,
        .leg_index = origin.leg_index,
        .leg_count = origin.leg_count,
        .lifecycle_kind = static_cast<std::uint8_t>(event.kind),
        .order_status = static_cast<std::uint8_t>(event.status),
    });

    if (!emit_lifecycle_event(event)) {
        return OmsProcessCode::kShardBackpressure;
    }
    ++processed_transport_update_count_;
    return OmsProcessCode::kProcessedTransportUpdate;
}

OmsProcessCode Oms::process_one_shard_intent() noexcept {
    if (shard_intent_queues_.empty()) {
        return OmsProcessCode::kIdle;
    }

    for (std::size_t scanned = 0; scanned < shard_intent_queues_.size(); ++scanned) {
        const std::size_t shard_index =
            (next_shard_index_ + scanned) % shard_intent_queues_.size();
        SubmissionQueue* const queue = shard_intent_queues_[shard_index];
        if (queue == nullptr) {
            continue;
        }

        OmsSubmission submission{};
        if (!queue->try_pop(submission)) {
            continue;
        }

        next_shard_index_ = (shard_index + 1) % shard_intent_queues_.size();
        //NOLINTNEXTLINE
        const OmsProcessCode code = process_submission(std::move(submission));
        if (code == OmsProcessCode::kProcessedIntent) {
            ++processed_intent_count_;
        }
        return code;
    }

    return OmsProcessCode::kIdle;
}

OmsProcessCode Oms::process_submission(OmsSubmission submission) noexcept {
    if (auto* intent = std::get_if<OrderIntent>(&submission)) {
        //NOLINTNEXTLINE
        return process_intent(std::move(*intent));
    }
    if (auto* cancel_intent = std::get_if<CancelIntent>(&submission)) {
        return process_cancel_intent(std::move(*cancel_intent));
    }
    if (auto* modify_intent = std::get_if<ModifyIntent>(&submission)) {
        return process_modify_intent(std::move(*modify_intent));
    }

    auto* group = std::get_if<GroupOrderIntent>(&submission);
    if (group == nullptr || group->leg_count == 0 ||
        group->leg_count > group->legs.size()) {
        return OmsProcessCode::kError;
    }

    for (std::size_t leg_index = 0; leg_index < group->leg_count; ++leg_index) {
        OrderIntent leg = group->legs[leg_index];
        leg.origin.group_id = group->group_id;
        leg.origin.leg_index = static_cast<std::uint16_t>(leg_index);
        leg.origin.leg_count = static_cast<std::uint16_t>(group->leg_count);

        const std::uint64_t rejected_before = rejected_intent_count_;
        //NOLINTNEXTLINE
        const OmsProcessCode code = process_intent(std::move(leg));
        if (code != OmsProcessCode::kProcessedIntent) {
            return code;
        }
        if (group->execution_policy == GroupExecutionPolicy::kAbortRemainingOnReject &&
            rejected_intent_count_ > rejected_before) {
            break;
        }
    }

    return OmsProcessCode::kProcessedIntent;
}

OmsProcessCode Oms::process_intent(OrderIntent intent) noexcept {
    const OmsRequestId oms_request_id = next_oms_request_id_++;
    const ClientOrderId client_order_id = make_client_order_id(oms_request_id);
    const internal::TimestampNs signal_ts_ns =
        intent.origin.signal_ts_ns != 0 ? intent.origin.signal_ts_ns : intent.intent_ts_ns;
    const internal::TimestampNs submission_ts_ns =
        intent.origin.submission_enqueued_ns != 0 ? intent.origin.submission_enqueued_ns
                                                   : signal_ts_ns;
    const internal::TimestampNs tick_recv_ts_ns =
        intent.origin.tick_recv_ns != 0 ? intent.origin.tick_recv_ns : signal_ts_ns;
    const internal::TimestampNs decision_ts_ns = monotonic_now_ns();
    const std::int64_t submission_to_decision_ns =
        latency_delta_ns(decision_ts_ns, submission_ts_ns);

    IntentDecision decision =
        halt_mode_.load(std::memory_order_acquire) != static_cast<std::uint8_t>(HaltMode::kNone)
            ? IntentDecision{
                  .code = IntentDecisionCode::kRejected,
                  .data = RejectedIntent{
                      .intent = intent,
                      .reason = IntentRejectReason::kHalted,
                  },
                  .decision_ts_ns = decision_ts_ns,
              }
            : risk_engine_.evaluate(intent, oms_request_id, client_order_id, decision_ts_ns);

    if (decision.code == IntentDecisionCode::kAccepted) {
        const auto* accepted_intent = std::get_if<AcceptedIntent>(&decision.data);
        if (accepted_intent == nullptr) {
            return OmsProcessCode::kError;
        }

        const SubmitOrderCmd submit_cmd{
            .oms_request_id = accepted_intent->oms_request_id,
            .intent = accepted_intent->intent,
            .client_order_id = accepted_intent->client_order_id,
        };

        if (!transport_.try_submit(submit_cmd)) {
            decision = make_transport_reject(intent, decision_ts_ns);
        } else {
            const internal::TimestampNs transport_submit_ts_ns = monotonic_now_ns();
            risk_engine_.on_intent_accepted(*accepted_intent);
            order_store_.insert_live_order(*accepted_intent, decision_ts_ns);
            order_store_.set_transport_submit_ts(accepted_intent->oms_request_id,
                                                  transport_submit_ts_ns);
            emit_audit(predex::core::audit::AuditEvent{
                .kind = predex::core::audit::AuditKind::kOmsTransport,
                .ts_ns = transport_submit_ts_ns,
                .shard_id = accepted_intent->intent.origin.shard_id,
                .signal_id = accepted_intent->intent.origin.signal_id,
                .group_id = accepted_intent->intent.origin.group_id,
                .local_intent_id = accepted_intent->intent.origin.local_intent_id,
                .oms_request_id = accepted_intent->oms_request_id,
                .tick_recv_ns = tick_recv_ts_ns,
                .signal_ts_ns = signal_ts_ns,
                .submission_enqueued_ns = submission_ts_ns,
                .oms_decision_ts_ns = decision_ts_ns,
                .transport_submit_ts_ns = transport_submit_ts_ns,
                .submission_to_decision_ns =
                    latency_delta_ns(decision_ts_ns, submission_ts_ns),
                .decision_to_transport_ns =
                    latency_delta_ns(transport_submit_ts_ns, decision_ts_ns),
                .exchange = accepted_intent->intent.exchange,
                .event_id = accepted_intent->intent.origin.event_id,
                .market_id = accepted_intent->intent.origin.market_id,
                .side = accepted_intent->intent.side,
                .leg_index = accepted_intent->intent.origin.leg_index,
                .leg_count = accepted_intent->intent.origin.leg_count,
                .qty_lots = accepted_intent->intent.qty_lots,
                .price_ticks = accepted_intent->intent.limit_price_ticks.value_or(0),
            });
        }
    }

    const IntentOrigin audit_origin = extract_origin(decision);
    emit_audit(predex::core::audit::AuditEvent{
        .kind = predex::core::audit::AuditKind::kOmsDecision,
        .ts_ns = decision.decision_ts_ns,
        .shard_id = audit_origin.shard_id,
        .signal_id = audit_origin.signal_id,
        .group_id = audit_origin.group_id,
        .local_intent_id = audit_origin.local_intent_id,
        .oms_request_id = extract_oms_request_id(decision),
        .tick_recv_ns = tick_recv_ts_ns,
        .signal_ts_ns = signal_ts_ns,
        .submission_enqueued_ns = submission_ts_ns,
        .oms_decision_ts_ns = decision.decision_ts_ns,
        .submission_to_decision_ns = submission_to_decision_ns,
        .exchange = intent.exchange,
        .event_id = audit_origin.event_id,
        .market_id = audit_origin.market_id,
        .side = intent.side,
        .leg_index = audit_origin.leg_index,
        .leg_count = audit_origin.leg_count,
        .qty_lots = intent.qty_lots,
        .price_ticks = intent.limit_price_ticks.value_or(0),
        .decision_code = static_cast<std::uint8_t>(decision.code),
        .reject_reason = extract_reject_reason(decision),
    });

    if (!emit_intent_decision(decision)) {
        return OmsProcessCode::kShardBackpressure;
    }

    if (decision.code == IntentDecisionCode::kRejected) {
        ++rejected_intent_count_;
    }

    return OmsProcessCode::kProcessedIntent;
}

OmsProcessCode Oms::process_cancel_intent(CancelIntent intent) noexcept {
    const internal::TimestampNs now_ns = monotonic_now_ns();
    OrderState* const target = order_store_.find_order_for_action(
        intent.target_oms_request_id,
        intent.target_client_order_id,
        intent.target_exchange_order_id);

    if (target == nullptr) {
        OrderLifecycleEvent reject{
            .origin = intent.origin,
            .oms_request_id = intent.target_oms_request_id.value_or(0),
            .kind = OrderLifecycleEventKind::kCancelReject,
            .status = OmsOrderStatus::kUnknown,
            .client_order_id = intent.target_client_order_id,
            .exchange_order_id = intent.target_exchange_order_id,
            .recv_ts_ns = now_ns,
            .data = CancelReject{
                .reason_code = "unknown_order",
                .reason_message = "cancel target order was not found",
            },
        };
        if (!emit_lifecycle_event(reject)) {
            return OmsProcessCode::kShardBackpressure;
        }
        ++processed_transport_update_count_;
        return OmsProcessCode::kProcessedIntent;
    }

    const CancelOrderCmd cmd{
        .oms_request_id = target->oms_request_id,
        .origin = target->origin,
        .client_order_id = target->client_order_id,
        .exchange_order_id = target->exchange_order_id,
        .cmd_ts_ns = now_ns,
    };
    if (!transport_.try_cancel(cmd)) {
        OrderLifecycleEvent reject{
            .origin = target->origin,
            .oms_request_id = target->oms_request_id,
            .kind = OrderLifecycleEventKind::kCancelReject,
            .status = target->status,
            .client_order_id = target->client_order_id,
            .exchange_order_id = target->exchange_order_id,
            .recv_ts_ns = now_ns,
            .data = CancelReject{
                .reason_code = "transport_unavailable",
                .reason_message = "cancel command enqueue failed",
            },
        };
        if (!emit_lifecycle_event(reject)) {
            return OmsProcessCode::kShardBackpressure;
        }
        ++processed_transport_update_count_;
        return OmsProcessCode::kProcessedIntent;
    }

    target->status = OmsOrderStatus::kPendingCancel;
    target->last_update_ts_ns = now_ns;
    emit_audit(predex::core::audit::AuditEvent{
        .kind = predex::core::audit::AuditKind::kOmsTransport,
        .ts_ns = now_ns,
        .shard_id = target->origin.shard_id,
        .signal_id = target->origin.signal_id,
        .group_id = target->origin.group_id,
        .local_intent_id = target->origin.local_intent_id,
        .oms_request_id = target->oms_request_id,
        .exchange = internal::ExchangeId::kKalshi,
        .event_id = target->origin.event_id,
        .market_id = target->origin.market_id,
        .leg_index = target->origin.leg_index,
        .leg_count = target->origin.leg_count,
        .order_status = static_cast<std::uint8_t>(target->status),
    });
    return OmsProcessCode::kProcessedIntent;
}

OmsProcessCode Oms::process_modify_intent(ModifyIntent intent) noexcept {
    const internal::TimestampNs now_ns = monotonic_now_ns();
    OrderState* const target = order_store_.find_order_for_action(
        intent.target_oms_request_id,
        intent.target_client_order_id,
        intent.target_exchange_order_id);

    if (target == nullptr) {
        OrderLifecycleEvent reject{
            .origin = intent.origin,
            .oms_request_id = intent.target_oms_request_id.value_or(0),
            .kind = OrderLifecycleEventKind::kReplaceReject,
            .status = OmsOrderStatus::kUnknown,
            .client_order_id = intent.target_client_order_id,
            .exchange_order_id = intent.target_exchange_order_id,
            .recv_ts_ns = now_ns,
            .data = ReplaceReject{
                .reason_code = "unknown_order",
                .reason_message = "modify target order was not found",
            },
        };
        if (!emit_lifecycle_event(reject)) {
            return OmsProcessCode::kShardBackpressure;
        }
        ++processed_transport_update_count_;
        return OmsProcessCode::kProcessedIntent;
    }

    const ModifyOrderCmd cmd{
        .oms_request_id = target->oms_request_id,
        .replacement_intent = intent.replacement_intent,
        .client_order_id = target->client_order_id,
        .exchange_order_id = target->exchange_order_id,
    };
    if (!transport_.try_modify(cmd)) {
        OrderLifecycleEvent reject{
            .origin = target->origin,
            .oms_request_id = target->oms_request_id,
            .kind = OrderLifecycleEventKind::kReplaceReject,
            .status = target->status,
            .client_order_id = target->client_order_id,
            .exchange_order_id = target->exchange_order_id,
            .recv_ts_ns = now_ns,
            .data = ReplaceReject{
                .reason_code = "transport_unavailable",
                .reason_message = "modify command enqueue failed",
            },
        };
        if (!emit_lifecycle_event(reject)) {
            return OmsProcessCode::kShardBackpressure;
        }
        ++processed_transport_update_count_;
        return OmsProcessCode::kProcessedIntent;
    }

    target->status = OmsOrderStatus::kPendingModify;
    target->last_update_ts_ns = now_ns;
    emit_audit(predex::core::audit::AuditEvent{
        .kind = predex::core::audit::AuditKind::kOmsTransport,
        .ts_ns = now_ns,
        .shard_id = target->origin.shard_id,
        .signal_id = target->origin.signal_id,
        .group_id = target->origin.group_id,
        .local_intent_id = target->origin.local_intent_id,
        .oms_request_id = target->oms_request_id,
        .exchange = intent.replacement_intent.exchange == internal::ExchangeId::kUnknown
            ? internal::ExchangeId::kKalshi
            : intent.replacement_intent.exchange,
        .event_id = target->origin.event_id,
        .market_id = target->origin.market_id,
        .side = intent.replacement_intent.side,
        .leg_index = target->origin.leg_index,
        .leg_count = target->origin.leg_count,
        .qty_lots = intent.replacement_intent.qty_lots,
        .price_ticks = intent.replacement_intent.limit_price_ticks.value_or(0),
        .order_status = static_cast<std::uint8_t>(target->status),
    });
    return OmsProcessCode::kProcessedIntent;
}

//NOLINTNEXTLINE
IntentDecision Oms::make_transport_reject(const OrderIntent& intent,
                                           internal::TimestampNs decision_ts_ns) const {
    return IntentDecision{
        .code = IntentDecisionCode::kRejected,
        .data = RejectedIntent{
            .intent = intent,
            .reason = IntentRejectReason::kVenueUnavailable,
        },
        .decision_ts_ns = decision_ts_ns,
    };
}

bool Oms::emit_intent_decision(const IntentDecision& decision) noexcept {
    const auto shard_index = shard_index_for_origin(extract_origin(decision));
    if (!shard_index.has_value()) {
        return false;
    }
    DecisionQueue* const queue = shard_decision_queues_[*shard_index];
    return queue != nullptr && queue->try_push(decision);
}

bool Oms::emit_lifecycle_event(const OrderLifecycleEvent& event) noexcept {
    const auto shard_index = shard_index_for_origin(event.origin);
    if (!shard_index.has_value()) {
        return false;
    }
    LifecycleQueue* const queue = shard_lifecycle_queues_[*shard_index];
    return queue != nullptr && queue->try_push(event);
}

void Oms::emit_audit(const predex::core::audit::AuditEvent& event) noexcept {
    if (audit_queue_ == nullptr) {
        return;
    }
    static_cast<void>(audit_queue_->try_push(event));
}

IntentOrigin Oms::extract_origin(const IntentDecision& decision) {
    if (const auto* accepted = std::get_if<AcceptedIntent>(&decision.data)) {
        return accepted->intent.origin;
    }
    if (const auto* rejected = std::get_if<RejectedIntent>(&decision.data)) {
        return rejected->intent.origin;
    }
    if (const auto* modified = std::get_if<ModifiedIntent>(&decision.data)) {
        return modified->modified_intent.origin;
    }
    return {};
}

std::optional<std::size_t> Oms::shard_index_for_origin(
    const IntentOrigin& origin) const noexcept {
    if (origin.shard_id >= shard_decision_queues_.size() ||
        origin.shard_id >= shard_lifecycle_queues_.size()) {
        return std::nullopt;
    }
    return static_cast<std::size_t>(origin.shard_id);
}

ClientOrderId Oms::make_client_order_id(OmsRequestId oms_request_id) {
    return "predex-" + std::to_string(oms_request_id) + "-" +
           std::to_string(next_client_order_seq_++);
}

} // namespace predex::core::oms::kalshi
