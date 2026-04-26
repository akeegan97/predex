#include "predex/oms/oms.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

namespace predex::core::oms::kalshi {
namespace {

[[nodiscard]] internal::TimestampNs monotonic_now_ns() noexcept {
    return static_cast<internal::TimestampNs>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] std::int64_t latency_delta_ns(internal::TimestampNs end_ts_ns,
                                            internal::TimestampNs start_ts_ns) noexcept {
    if (end_ts_ns <= start_ts_ns || start_ts_ns == 0) {
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

enum class DecisionAuditCode : std::uint8_t {
    kAccepted = 1,
    kRejected = 2,
    kModified = 3,
};

enum class LifecycleAuditCode : std::uint8_t {
    kWorking = 1,
    kPartiallyFilled = 2,
    kFilled = 3,
    kCanceled = 4,
    kUncertain = 5,
    kVenueRejected = 6,
};

struct TransportAuditSample {
    internal::TimestampNs submit_ts_ns{0};
    internal::TimestampNs response_ts_ns{0};
    std::uint8_t decision_code{0};
    internal::QtyLots qty_lots{0};
    internal::PriceTicks price_ticks{0};
};

[[nodiscard]] std::optional<TransportAuditSample> transport_audit_sample_for(
    const KalshiEventSource source,
    const KalshiToOmsEvent& event,
    internal::QtyLots previous_working_qty,
    std::optional<internal::PriceTicks> previous_working_price) noexcept {
    if (source != KalshiEventSource::kRest) {
        return std::nullopt;
    }

    return std::visit(
        [&](const auto& typed_event) -> std::optional<TransportAuditSample> {
            using T = std::decay_t<decltype(typed_event)>;
            if constexpr (std::is_same_v<T, VenueOrderAck>) {
                return TransportAuditSample{
                    .submit_ts_ns = typed_event.transport_submit_ts_ns,
                    .response_ts_ns = typed_event.recv_ts_ns,
                    .decision_code = static_cast<std::uint8_t>(DecisionAuditCode::kAccepted),
                    .qty_lots = typed_event.accepted_qty_lots,
                    .price_ticks = previous_working_price.value_or(0),
                };
            } else if constexpr (std::is_same_v<T, VenueOrderReject>) {
                return TransportAuditSample{
                    .submit_ts_ns = typed_event.transport_submit_ts_ns,
                    .response_ts_ns = typed_event.recv_ts_ns,
                    .decision_code = static_cast<std::uint8_t>(DecisionAuditCode::kAccepted),
                    .qty_lots = previous_working_qty,
                    .price_ticks = previous_working_price.value_or(0),
                };
            } else if constexpr (std::is_same_v<T, VenueCancelAck> ||
                                 std::is_same_v<T, VenueCancelReject>) {
                return TransportAuditSample{
                    .submit_ts_ns = typed_event.transport_submit_ts_ns,
                    .response_ts_ns = typed_event.recv_ts_ns,
                    .decision_code = static_cast<std::uint8_t>(DecisionAuditCode::kModified),
                    .qty_lots = previous_working_qty,
                    .price_ticks = previous_working_price.value_or(0),
                };
            } else if constexpr (std::is_same_v<T, VenueModifyAck>) {
                return TransportAuditSample{
                    .submit_ts_ns = typed_event.transport_submit_ts_ns,
                    .response_ts_ns = typed_event.recv_ts_ns,
                    .decision_code = static_cast<std::uint8_t>(DecisionAuditCode::kModified),
                    .qty_lots = typed_event.working_qty_lots,
                    .price_ticks = typed_event.working_price_ticks.value_or(
                        previous_working_price.value_or(0)),
                };
            } else if constexpr (std::is_same_v<T, VenueModifyReject>) {
                return TransportAuditSample{
                    .submit_ts_ns = typed_event.transport_submit_ts_ns,
                    .response_ts_ns = typed_event.recv_ts_ns,
                    .decision_code = static_cast<std::uint8_t>(DecisionAuditCode::kModified),
                    .qty_lots = previous_working_qty,
                    .price_ticks = previous_working_price.value_or(0),
                };
            }
            return std::nullopt;
        },
        event);
}

} // namespace

Oms::Oms(std::vector<ShardRequestQueue*> shard_request_queues,
         std::vector<ShardDecisionQueue*> shard_decision_queues,
         std::vector<ShardLifecycleQueue*> shard_lifecycle_queues,
         ExecutionTransportQueues transport_queues,
         GlobalRiskLimits global_risk_limits,
         utils::SPSCQueue<predex::core::audit::AuditEvent>* audit_queue,
         std::function<std::optional<std::string>(internal::MarketId)> market_ticker_resolver)
    : global_risk_(std::move(global_risk_limits)),
      order_store_(),
      transport_(std::move(transport_queues)),
      audit_queue_(audit_queue),
      market_ticker_resolver_(std::move(market_ticker_resolver)),
      shard_request_queues_(std::move(shard_request_queues)),
      shard_decision_queues_(std::move(shard_decision_queues)),
      shard_lifecycle_queues_(std::move(shard_lifecycle_queues)) {}

OmsPumpResult Oms::pump(std::size_t max_kalshi_events,
                        std::size_t max_shard_requests) noexcept {
    OmsPumpResult result{};

    for (std::size_t i = 0; i < max_kalshi_events; ++i) {
        const auto code = process_one_kalshi_event();
        if (code == OmsProcessCode::kIdle) {
            break;
        }
        result.code = code;
        if (code == OmsProcessCode::kProcessedKalshiEvent) {
            ++result.processed_kalshi_events;
            continue;
        }
        return result;
    }

    for (std::size_t i = 0; i < max_shard_requests; ++i) {
        const auto code = process_one_shard_request();
        if (code == OmsProcessCode::kIdle) {
            break;
        }
        result.code = code;
        if (code == OmsProcessCode::kProcessedShardRequest) {
            ++result.processed_shard_requests;
            continue;
        }
        return result;
    }

    if (result.processed_shard_requests > 0 && result.code == OmsProcessCode::kIdle) {
        result.code = OmsProcessCode::kProcessedShardRequest;
    } else if (result.processed_kalshi_events > 0 && result.code == OmsProcessCode::kIdle) {
        result.code = OmsProcessCode::kProcessedKalshiEvent;
    }
    return result;
}

OmsRequestId Oms::seed_reconciled_order(OrderState state) noexcept {
    if (state.order.oms_request_id == 0) {
        state.order.oms_request_id = next_oms_request_id_++;
    } else {
        next_oms_request_id_ = std::max(next_oms_request_id_, state.order.oms_request_id + 1);
    }
    if (state.order.client_order_id.value.empty()) {
        state.order.client_order_id = make_client_order_id(state.order.oms_request_id);
    }
    order_store_.adopt_reconciled_order(state);

    NewOrderIntent synthetic_intent{
        .context = state.context,
        .exchange = state.exchange,
        .side = state.side,
        .outcome = state.outcome,
        .qty_lots = state.working_qty_lots,
        .limit_price_ticks = state.working_limit_price_ticks,
        .time_in_force = state.time_in_force,
        .liquidity_intent = state.liquidity_intent,
        .order_type_intent = state.order_type_intent,
        .intent_ts_ns = state.intent_ts_ns,
    };
    global_risk_.on_new_order_accepted(synthetic_intent, 0);
    return state.order.oms_request_id;
}

void Oms::request_soft_halt() noexcept {
    halt_mode_.store(static_cast<std::uint8_t>(HaltMode::kSoft), std::memory_order_release);
}

void Oms::request_hard_halt() noexcept {
    halt_mode_.store(static_cast<std::uint8_t>(HaltMode::kHard), std::memory_order_release);
}

bool Oms::is_halted() const noexcept {
    return halt_mode_.load(std::memory_order_acquire) !=
           static_cast<std::uint8_t>(HaltMode::kNone);
}

std::size_t Oms::live_order_count() const noexcept {
    return order_store_.live_order_count();
}

std::uint64_t Oms::processed_shard_request_count() const noexcept {
    return processed_shard_request_count_;
}

std::uint64_t Oms::processed_kalshi_event_count() const noexcept {
    return processed_kalshi_event_count_;
}

std::uint64_t Oms::emitted_decision_count() const noexcept {
    return emitted_decision_count_;
}

std::uint64_t Oms::emitted_transport_count() const noexcept {
    return emitted_transport_count_;
}

std::uint64_t Oms::emitted_lifecycle_count() const noexcept {
    return emitted_lifecycle_count_;
}

std::uint64_t Oms::rejected_decision_count() const noexcept {
    return rejected_decision_count_;
}

OmsProcessCode Oms::process_one_shard_request() noexcept {
    if (shard_request_queues_.empty()) {
        return OmsProcessCode::kIdle;
    }

    const std::size_t queue_count = shard_request_queues_.size();
    for (std::size_t offset = 0; offset < queue_count; ++offset) {
        const std::size_t index = (next_shard_index_ + offset) % queue_count;
        auto* queue = shard_request_queues_[index];
        if (queue == nullptr) {
            continue;
        }
        ShardOmsRequest request{};
        if (!queue->try_pop(request)) {
            continue;
        }
        next_shard_index_ = (index + 1U) % queue_count;
        ++processed_shard_request_count_;
        return handle_shard_request(request);
    }
    return OmsProcessCode::kIdle;
}

OmsProcessCode Oms::process_one_kalshi_event() noexcept {
    SourcedKalshiEvent event{};
    if (!transport_.try_pop_event(event)) {
        return OmsProcessCode::kIdle;
    }
    ++processed_kalshi_event_count_;
    return handle_kalshi_event(event);
}

OmsProcessCode Oms::handle_new_order_intent(const NewOrderIntent& intent) noexcept {
    if (halt_mode_.load(std::memory_order_acquire) >= static_cast<std::uint8_t>(HaltMode::kSoft)) {
        const auto now_ns = monotonic_now_ns();
        emit_decision_audit(
            intent.context,
            0,
            now_ns,
            static_cast<std::uint8_t>(DecisionAuditCode::kRejected),
            static_cast<std::uint8_t>(IntentRejectReason::kSoftHalt),
            intent.qty_lots);
        return emit_shard_decision(
                   OmsToShardDecision{IntentRejected{
                       .context = intent.context,
                       .reason = IntentRejectReason::kSoftHalt,
                   }},
                   intent.context.shard_id)
            ? OmsProcessCode::kProcessedShardRequest
            : OmsProcessCode::kShardBackpressure;
    }

    const auto risk_decision = global_risk_.evaluate_new_order(intent);
    if (const auto* rejected = std::get_if<RiskRejected>(&risk_decision)) {
        const auto now_ns = monotonic_now_ns();
        emit_decision_audit(
            intent.context,
            0,
            now_ns,
            static_cast<std::uint8_t>(DecisionAuditCode::kRejected),
            static_cast<std::uint8_t>(rejected->reason),
            intent.qty_lots);
        return emit_shard_decision(
                   OmsToShardDecision{IntentRejected{
                       .context = intent.context,
                       .reason = rejected->reason,
                   }},
                   intent.context.shard_id)
            ? OmsProcessCode::kProcessedShardRequest
            : OmsProcessCode::kShardBackpressure;
    }

    const auto now_ns = monotonic_now_ns();
    const auto order = make_order_ref();
    const ShardOrderCorrelation corr{
        .context = intent.context,
        .order = order,
    };

    order_store_.insert_pending_submit(BeginSubmitTransition{
        .corr = corr,
        .intent = intent,
        .oms_decision_ts_ns = now_ns,
    });
    global_risk_.on_new_order_accepted(
        intent, std::get<RiskApproved>(risk_decision).capital_reserved_ticks);
    emit_decision_audit(
        intent.context,
        order.oms_request_id,
        now_ns,
        static_cast<std::uint8_t>(DecisionAuditCode::kAccepted),
        static_cast<std::uint8_t>(IntentRejectReason::kNone),
        intent.qty_lots);

    if (!emit_shard_decision(OmsToShardDecision{IntentAccepted{.corr = corr}},
                             intent.context.shard_id)) {
        return OmsProcessCode::kShardBackpressure;
    }

    const std::string market_ticker =
        market_ticker_resolver_ != nullptr
            ? market_ticker_resolver_(intent.context.market_id).value_or(std::string{})
            : std::string{};
    if (!emit_kalshi_command(OmsToKalshiCommand{SubmitOrderCmd{
            .order = order,
            .intent = intent,
            .market_ticker = market_ticker,
        }})) {
        return OmsProcessCode::kVenueBackpressure;
    }
    order_store_.mark_submitted(MarkSubmittedTransition{
        .order = order,
        .venue_submit_ts_ns = now_ns,
    });
    return OmsProcessCode::kProcessedShardRequest;
}

OmsProcessCode Oms::handle_group_order_intent(const GroupOrderIntent& intent) noexcept {
    for (std::size_t leg_index = 0; leg_index < intent.leg_count; ++leg_index) {
        const auto code = handle_new_order_intent(intent.legs[leg_index]);
        if (code != OmsProcessCode::kProcessedShardRequest &&
            code != OmsProcessCode::kIdle) {
            return code;
        }
    }
    return OmsProcessCode::kProcessedShardRequest;
}

OmsProcessCode Oms::handle_cancel_order_intent(const CancelOrderIntent& intent) noexcept {
    const auto key = make_lookup_key(intent);
    if (!key.has_value()) {
        const auto now_ns = monotonic_now_ns();
        emit_decision_audit(
            intent.context,
            0,
            now_ns,
            static_cast<std::uint8_t>(DecisionAuditCode::kRejected),
            static_cast<std::uint8_t>(IntentRejectReason::kInvalidParams),
            0);
        return emit_shard_decision(
                   OmsToShardDecision{IntentRejected{
                       .context = intent.context,
                       .reason = IntentRejectReason::kInvalidParams,
                   }},
                   intent.context.shard_id)
            ? OmsProcessCode::kProcessedShardRequest
            : OmsProcessCode::kShardBackpressure;
    }
    auto* state = order_store_.find(*key);
    if (state == nullptr) {
        const auto now_ns = monotonic_now_ns();
        emit_decision_audit(
            intent.context,
            0,
            now_ns,
            static_cast<std::uint8_t>(DecisionAuditCode::kRejected),
            static_cast<std::uint8_t>(IntentRejectReason::kInvalidParams),
            0);
        return emit_shard_decision(
                   OmsToShardDecision{IntentRejected{
                       .context = intent.context,
                       .reason = IntentRejectReason::kInvalidParams,
                   }},
                   intent.context.shard_id)
            ? OmsProcessCode::kProcessedShardRequest
            : OmsProcessCode::kShardBackpressure;
    }

    const ShardOrderCorrelation corr{
        .context = state->context,
        .order = state->order,
    };
    if (!order_store_.mark_pending_cancel(BeginCancelTransition{
            .corr = corr,
            .ts_ns = monotonic_now_ns(),
        })) {
        return OmsProcessCode::kError;
    }
    if (!emit_kalshi_command(OmsToKalshiCommand{CancelOrderCmd{
            .corr = corr,
            .cmd_ts_ns = monotonic_now_ns(),
        }})) {
        return OmsProcessCode::kVenueBackpressure;
    }
    return OmsProcessCode::kProcessedShardRequest;
}

OmsProcessCode Oms::handle_modify_order_intent(const ModifyOrderIntent& intent) noexcept {
    const auto key = make_lookup_key(intent);
    if (!key.has_value()) {
        const auto now_ns = monotonic_now_ns();
        emit_decision_audit(
            intent.context,
            0,
            now_ns,
            static_cast<std::uint8_t>(DecisionAuditCode::kRejected),
            static_cast<std::uint8_t>(IntentRejectReason::kInvalidParams),
            intent.replacement.qty_lots);
        return emit_shard_decision(
                   OmsToShardDecision{IntentRejected{
                       .context = intent.context,
                       .reason = IntentRejectReason::kInvalidParams,
                   }},
                   intent.context.shard_id)
            ? OmsProcessCode::kProcessedShardRequest
            : OmsProcessCode::kShardBackpressure;
    }
    auto* state = order_store_.find(*key);
    if (state == nullptr || !modify_preserves_immutable_fields(*state, intent.replacement)) {
        const auto now_ns = monotonic_now_ns();
        emit_decision_audit(
            intent.context,
            0,
            now_ns,
            static_cast<std::uint8_t>(DecisionAuditCode::kRejected),
            static_cast<std::uint8_t>(IntentRejectReason::kInvalidParams),
            intent.replacement.qty_lots);
        return emit_shard_decision(
                   OmsToShardDecision{IntentRejected{
                       .context = intent.context,
                       .reason = IntentRejectReason::kInvalidParams,
                   }},
                   intent.context.shard_id)
            ? OmsProcessCode::kProcessedShardRequest
            : OmsProcessCode::kShardBackpressure;
    }

    const auto risk_decision = global_risk_.evaluate_modify(
        intent.replacement,
        state->working_qty_lots,
        state->working_limit_price_ticks,
        state->context.event_id);
    if (const auto* rejected = std::get_if<RiskRejected>(&risk_decision)) {
        const auto now_ns = monotonic_now_ns();
        emit_decision_audit(
            intent.context,
            state->order.oms_request_id,
            now_ns,
            static_cast<std::uint8_t>(DecisionAuditCode::kRejected),
            static_cast<std::uint8_t>(rejected->reason),
            intent.replacement.qty_lots);
        return emit_shard_decision(
                   OmsToShardDecision{IntentRejected{
                       .context = intent.context,
                       .reason = rejected->reason,
                   }},
                   intent.context.shard_id)
            ? OmsProcessCode::kProcessedShardRequest
            : OmsProcessCode::kShardBackpressure;
    }

    const ShardOrderCorrelation corr{
        .context = state->context,
        .order = state->order,
    };
    if (!order_store_.mark_pending_modify(BeginModifyTransition{
            .corr = corr,
            .replacement = intent.replacement,
            .ts_ns = monotonic_now_ns(),
        })) {
        return OmsProcessCode::kError;
    }
    emit_decision_audit(
        corr.context,
        corr.order.oms_request_id,
        monotonic_now_ns(),
        static_cast<std::uint8_t>(DecisionAuditCode::kModified),
        static_cast<std::uint8_t>(IntentRejectReason::kNone),
        intent.replacement.qty_lots);
    if (!emit_shard_decision(OmsToShardDecision{IntentModified{
            .corr = corr,
            .replacement = intent.replacement,
        }},
                             state->context.shard_id)) {
        return OmsProcessCode::kShardBackpressure;
    }

    if (!emit_kalshi_command(OmsToKalshiCommand{ModifyOrderCmd{
            .corr = corr,
            .updated_client_order_id = make_client_order_id(corr.order.oms_request_id),
            .replacement = intent.replacement,
            .cmd_ts_ns = monotonic_now_ns(),
        }})) {
        return OmsProcessCode::kVenueBackpressure;
    }
    return OmsProcessCode::kProcessedShardRequest;
}

bool Oms::modify_preserves_immutable_fields(const OrderState& order_state,
                                            const NewOrderIntent& replacement) const noexcept {
    return replacement.context.event_id == order_state.context.event_id &&
           replacement.context.market_id == order_state.context.market_id &&
           replacement.side == order_state.side &&
           replacement.outcome == order_state.outcome &&
           replacement.exchange == order_state.exchange;
}

OmsProcessCode Oms::handle_kalshi_event(const SourcedKalshiEvent& event) noexcept {
    const auto* pre_state = std::visit(
        [this](const auto& typed_event) -> const OrderState* { return order_store_.find(typed_event.order); },
        event.event);
    const internal::EventId event_id = pre_state != nullptr ? pre_state->context.event_id : 0;
    const internal::QtyLots previous_working_qty = pre_state != nullptr ? pre_state->working_qty_lots : 0;
    const auto previous_working_price =
        pre_state != nullptr ? pre_state->working_limit_price_ticks : std::optional<internal::PriceTicks>{};
    const std::optional<ShardOrderCorrelation> pre_corr =
        pre_state != nullptr
            ? std::optional<ShardOrderCorrelation>{ShardOrderCorrelation{
                  .context = pre_state->context,
                  .order = pre_state->order,
              }}
            : std::nullopt;

    const auto apply_result = order_store_.apply_venue_event(event);
    if (!apply_result.ok) {
        return OmsProcessCode::kError;
    }

    if (apply_result.fill_qty_lots > 0) {
        const std::int64_t released_ticks =
            previous_working_price.has_value()
                ? internal::scale_ticks_by_qty_ceil(*previous_working_price,
                                                    apply_result.fill_qty_lots)
                : 0;
        global_risk_.on_fill(event_id, apply_result.fill_qty_lots, released_ticks);
    }
    if (apply_result.became_terminal) {
        const std::int64_t released_ticks =
            previous_working_price.has_value()
                ? internal::scale_ticks_by_qty_ceil(*previous_working_price,
                                                    apply_result.remaining_open_qty_lots)
                : 0;
        global_risk_.on_order_terminal(event_id, apply_result.remaining_open_qty_lots, released_ticks);
    }

    if (pre_state != nullptr && std::holds_alternative<VenueModifyAck>(event.event)) {
        const auto* post_state = order_store_.find(pre_state->order);
        if (post_state != nullptr) {
            global_risk_.on_modify_accepted(
                event_id,
                previous_working_qty,
                post_state->working_qty_lots,
                0);
        }
    }

    if (auto corr = pre_corr.has_value()
                        ? pre_corr
                        : resolve_correlation(std::visit(
                              [](const auto& typed_event) -> const OmsOrderRef& {
                                  return typed_event.order;
                              },
                              event.event));
        corr.has_value()) {
        if (pre_state != nullptr) {
            if (const auto transport_sample = transport_audit_sample_for(
                    event.source, event.event, previous_working_qty, previous_working_price);
                transport_sample.has_value()) {
                emit_transport_audit(
                    *corr,
                    pre_state->oms_decision_ts_ns,
                    transport_sample->submit_ts_ns != 0 ? transport_sample->submit_ts_ns
                                                        : pre_state->venue_submit_ts_ns,
                    transport_sample->response_ts_ns,
                    transport_sample->decision_code,
                    transport_sample->qty_lots,
                    transport_sample->price_ticks);
            }
        }
        OmsToShardLifecycleEvent lifecycle{};
        bool emit = true;
        std::visit(
            [&](const auto& typed_event) {
                using T = std::decay_t<decltype(typed_event)>;
                if constexpr (std::is_same_v<T, VenueOrderAck>) {
                    lifecycle = OrderWorking{
                        .corr = *corr,
                        .working_qty_lots = typed_event.accepted_qty_lots,
                        .working_price_ticks = previous_working_price.value_or(0),
                    };
                } else if constexpr (std::is_same_v<T, VenueOrderPartialFill>) {
                    lifecycle = OrderPartiallyFilled{
                        .corr = *corr,
                        .filled_qty_lots = typed_event.fill_qty_lots,
                        .remaining_qty_lots = apply_result.remaining_open_qty_lots,
                        .fill_price_ticks = typed_event.fill_price_ticks,
                    };
                } else if constexpr (std::is_same_v<T, VenueOrderFill>) {
                    lifecycle = OrderFilled{
                        .corr = *corr,
                        .filled_qty_lots = typed_event.fill_qty_lots,
                        .fill_price_ticks = typed_event.fill_price_ticks,
                    };
                } else if constexpr (std::is_same_v<T, VenueOrderCanceled>) {
                    lifecycle = OrderCanceled{.corr = *corr};
                } else if constexpr (std::is_same_v<T, VenueOrderUncertain>) {
                    lifecycle = OrderUncertain{.corr = *corr};
                } else if constexpr (std::is_same_v<T, VenueOrderReject> ||
                                     std::is_same_v<T, VenueCancelReject> ||
                                     std::is_same_v<T, VenueModifyReject>) {
                    lifecycle = OrderVenueRejected{
                        .corr = *corr,
                        .reason = typed_event.reason,
                    };
                } else if constexpr (std::is_same_v<T, VenueModifyAck>) {
                    lifecycle = OrderWorking{
                        .corr = *corr,
                        .working_qty_lots = typed_event.working_qty_lots,
                        .working_price_ticks = typed_event.working_price_ticks.value_or(0),
                    };
                } else {
                    emit = false;
                }
            },
            event.event);
        if (emit && !emit_shard_lifecycle(lifecycle, corr->context.shard_id)) {
            return OmsProcessCode::kShardBackpressure;
        }
        if (emit) {
            const auto* post_state = order_store_.find(corr->order);
            const auto status =
                post_state != nullptr ? static_cast<std::uint8_t>(post_state->status) : 0;
            const auto first_fill_ts =
                post_state != nullptr ? post_state->first_fill_ts_ns : 0;
            const auto terminal_ts =
                post_state != nullptr ? post_state->terminal_ts_ns : 0;
            std::visit(
                [&](const auto& typed_lifecycle) {
                    using T = std::decay_t<decltype(typed_lifecycle)>;
                    if constexpr (std::is_same_v<T, OrderWorking>) {
                        emit_lifecycle_audit(
                            typed_lifecycle.corr,
                            monotonic_now_ns(),
                            static_cast<std::uint8_t>(LifecycleAuditCode::kWorking),
                            status,
                            static_cast<std::uint8_t>(VenueRejectReason::kNone),
                            typed_lifecycle.working_qty_lots,
                            typed_lifecycle.working_price_ticks,
                            first_fill_ts,
                            terminal_ts);
                    } else if constexpr (std::is_same_v<T, OrderPartiallyFilled>) {
                        emit_lifecycle_audit(
                            typed_lifecycle.corr,
                            monotonic_now_ns(),
                            static_cast<std::uint8_t>(LifecycleAuditCode::kPartiallyFilled),
                            status,
                            static_cast<std::uint8_t>(VenueRejectReason::kNone),
                            typed_lifecycle.remaining_qty_lots,
                            typed_lifecycle.fill_price_ticks,
                            first_fill_ts,
                            terminal_ts);
                    } else if constexpr (std::is_same_v<T, OrderFilled>) {
                        emit_lifecycle_audit(
                            typed_lifecycle.corr,
                            monotonic_now_ns(),
                            static_cast<std::uint8_t>(LifecycleAuditCode::kFilled),
                            status,
                            static_cast<std::uint8_t>(VenueRejectReason::kNone),
                            typed_lifecycle.filled_qty_lots,
                            typed_lifecycle.fill_price_ticks,
                            first_fill_ts,
                            terminal_ts);
                    } else if constexpr (std::is_same_v<T, OrderCanceled>) {
                        emit_lifecycle_audit(
                            typed_lifecycle.corr,
                            monotonic_now_ns(),
                            static_cast<std::uint8_t>(LifecycleAuditCode::kCanceled),
                            status,
                            static_cast<std::uint8_t>(VenueRejectReason::kNone),
                            0,
                            0,
                            first_fill_ts,
                            terminal_ts);
                    } else if constexpr (std::is_same_v<T, OrderUncertain>) {
                        emit_lifecycle_audit(
                            typed_lifecycle.corr,
                            monotonic_now_ns(),
                            static_cast<std::uint8_t>(LifecycleAuditCode::kUncertain),
                            status,
                            static_cast<std::uint8_t>(VenueRejectReason::kNone),
                            0,
                            0,
                            first_fill_ts,
                            terminal_ts);
                    } else {
                        emit_lifecycle_audit(
                            typed_lifecycle.corr,
                            monotonic_now_ns(),
                            static_cast<std::uint8_t>(LifecycleAuditCode::kVenueRejected),
                            status,
                            static_cast<std::uint8_t>(typed_lifecycle.reason),
                            0,
                            0,
                            first_fill_ts,
                            terminal_ts);
                    }
                },
                lifecycle);
        }
    }

    return OmsProcessCode::kProcessedKalshiEvent;
}

std::optional<ShardOrderCorrelation> Oms::resolve_correlation(const OmsOrderRef& order) const noexcept {
    const auto* state = order_store_.find(order);
    if (state == nullptr) {
        return std::nullopt;
    }
    return ShardOrderCorrelation{
        .context = state->context,
        .order = state->order,
    };
}

std::optional<OrderStore::LookupKey> Oms::make_lookup_key(const CancelOrderIntent& intent) const noexcept {
    if (intent.target_oms_request_id.has_value() || !intent.target_client_order_id.value.empty() ||
        intent.target_exchange_order_id.has_value()) {
        return OrderStore::LookupKey{
            .oms_request_id = intent.target_oms_request_id,
            .client_order_id = intent.target_client_order_id,
            .exchange_order_id = intent.target_exchange_order_id,
        };
    }
    return std::nullopt;
}

std::optional<OrderStore::LookupKey> Oms::make_lookup_key(const ModifyOrderIntent& intent) const noexcept {
    if (intent.target_oms_request_id.has_value() || !intent.target_client_order_id.value.empty() ||
        intent.target_exchange_order_id.has_value()) {
        return OrderStore::LookupKey{
            .oms_request_id = intent.target_oms_request_id,
            .client_order_id = intent.target_client_order_id,
            .exchange_order_id = intent.target_exchange_order_id,
        };
    }
    return std::nullopt;
}

OmsOrderRef Oms::make_order_ref() {
    const auto oms_request_id = next_oms_request_id_++;
    return OmsOrderRef{
        .oms_request_id = oms_request_id,
        .client_order_id = make_client_order_id(oms_request_id),
        .exchange_order_id = std::nullopt,
    };
}

ClientOrderId Oms::make_client_order_id(OmsRequestId oms_request_id) {
    return ClientOrderId{
        .value = "oms-" + std::to_string(oms_request_id) + "-" +
            std::to_string(next_client_order_seq_++),
    };
}

bool Oms::emit_kalshi_command(const OmsToKalshiCommand& command) noexcept {
    return transport_.try_send(command);
}

bool Oms::emit_shard_decision(const OmsToShardDecision& decision, std::uint16_t shard_id) noexcept {
    const auto shard_index = shard_index_for(shard_id);
    return shard_index.has_value() && shard_decision_queues_[*shard_index] != nullptr &&
           shard_decision_queues_[*shard_index]->try_push(decision);
}

bool Oms::emit_shard_lifecycle(const OmsToShardLifecycleEvent& event, std::uint16_t shard_id) noexcept {
    const auto shard_index = shard_index_for(shard_id);
    return shard_index.has_value() && shard_lifecycle_queues_[*shard_index] != nullptr &&
           shard_lifecycle_queues_[*shard_index]->try_push(event);
}

std::optional<std::size_t> Oms::shard_index_for(std::uint16_t shard_id) const noexcept {
    if (static_cast<std::size_t>(shard_id) >= shard_request_queues_.size()) {
        return std::nullopt;
    }
    return static_cast<std::size_t>(shard_id);
}

void Oms::emit_audit(const predex::core::audit::AuditEvent& event) noexcept {
    if (audit_queue_ == nullptr) {
        return;
    }
    static_cast<void>(audit_queue_->try_push(event));
}

void Oms::emit_decision_audit(const IntentContext& context,
                              OmsRequestId oms_request_id,
                              internal::TimestampNs decision_ts_ns,
                              std::uint8_t decision_code,
                              std::uint8_t reject_reason,
                              internal::QtyLots qty_lots) noexcept {
    if (decision_code == static_cast<std::uint8_t>(DecisionAuditCode::kRejected)) {
        ++rejected_decision_count_;
    }
    ++emitted_decision_count_;
    emit_audit(predex::core::audit::AuditEvent{
        .kind = predex::core::audit::AuditKind::kOmsDecision,
        .ts_ns = decision_ts_ns,
        .shard_id = context.shard_id,
        .signal_id = context.signal_id,
        .group_id = context.group_intent_id,
        .local_intent_id = context.local_intent_id,
        .oms_request_id = oms_request_id,
        .tick_recv_ns = context.tick_recv_ns,
        .signal_ts_ns = context.signal_ts_ns,
        .submission_enqueued_ns = context.submission_enqueued_ns,
        .oms_decision_ts_ns = decision_ts_ns,
        .signal_to_submission_ns = latency_delta_ns(context.submission_enqueued_ns, context.signal_ts_ns),
        .submission_to_decision_ns = latency_delta_ns(decision_ts_ns, context.submission_enqueued_ns),
        .event_id = context.event_id,
        .market_id = context.market_id,
        .leg_index = context.leg_index,
        .leg_count = context.leg_count,
        .qty_lots = qty_lots,
        .decision_code = decision_code,
        .reject_reason = reject_reason,
    });
}

void Oms::emit_transport_audit(const ShardOrderCorrelation& corr,
                               internal::TimestampNs oms_decision_ts_ns,
                               internal::TimestampNs transport_submit_ts_ns,
                               internal::TimestampNs transport_response_ts_ns,
                               std::uint8_t decision_code,
                               internal::QtyLots qty_lots,
                               internal::PriceTicks price_ticks) noexcept {
    ++emitted_transport_count_;
    emit_audit(predex::core::audit::AuditEvent{
        .kind = predex::core::audit::AuditKind::kOmsTransport,
        .ts_ns = transport_response_ts_ns,
        .shard_id = corr.context.shard_id,
        .signal_id = corr.context.signal_id,
        .group_id = corr.context.group_intent_id,
        .local_intent_id = corr.context.local_intent_id,
        .oms_request_id = corr.order.oms_request_id,
        .tick_recv_ns = corr.context.tick_recv_ns,
        .signal_ts_ns = corr.context.signal_ts_ns,
        .submission_enqueued_ns = corr.context.submission_enqueued_ns,
        .oms_decision_ts_ns = oms_decision_ts_ns,
        .transport_submit_ts_ns = transport_submit_ts_ns,
        .transport_response_recv_ns = transport_response_ts_ns,
        .signal_to_submission_ns =
            latency_delta_ns(corr.context.submission_enqueued_ns, corr.context.signal_ts_ns),
        .submission_to_decision_ns =
            latency_delta_ns(oms_decision_ts_ns, corr.context.submission_enqueued_ns),
        .decision_to_transport_ns =
            latency_delta_ns(transport_submit_ts_ns, oms_decision_ts_ns),
        .tick_to_transport_submit_ns =
            latency_delta_ns(transport_submit_ts_ns, corr.context.tick_recv_ns),
        .transport_submit_to_response_ns =
            latency_delta_ns(transport_response_ts_ns, transport_submit_ts_ns),
        .tick_to_transport_response_ns =
            latency_delta_ns(transport_response_ts_ns, corr.context.tick_recv_ns),
        .event_id = corr.context.event_id,
        .market_id = corr.context.market_id,
        .leg_index = corr.context.leg_index,
        .leg_count = corr.context.leg_count,
        .qty_lots = qty_lots,
        .price_ticks = price_ticks,
        .decision_code = decision_code,
    });
}

void Oms::emit_lifecycle_audit(const ShardOrderCorrelation& corr,
                               internal::TimestampNs lifecycle_ts_ns,
                               std::uint8_t lifecycle_kind,
                               std::uint8_t order_status,
                               std::uint8_t reject_reason,
                               internal::QtyLots qty_lots,
                               internal::PriceTicks price_ticks,
                               internal::TimestampNs first_fill_ts_ns,
                               internal::TimestampNs terminal_ts_ns) noexcept {
    ++emitted_lifecycle_count_;
    emit_audit(predex::core::audit::AuditEvent{
        .kind = predex::core::audit::AuditKind::kOmsLifecycle,
        .ts_ns = lifecycle_ts_ns,
        .shard_id = corr.context.shard_id,
        .signal_id = corr.context.signal_id,
        .group_id = corr.context.group_intent_id,
        .local_intent_id = corr.context.local_intent_id,
        .oms_request_id = corr.order.oms_request_id,
        .tick_recv_ns = corr.context.tick_recv_ns,
        .signal_ts_ns = corr.context.signal_ts_ns,
        .submission_enqueued_ns = corr.context.submission_enqueued_ns,
        .first_fill_recv_ns = first_fill_ts_ns,
        .terminal_recv_ns = terminal_ts_ns,
        .tick_to_first_fill_ns = latency_delta_ns(first_fill_ts_ns, corr.context.tick_recv_ns),
        .tick_to_terminal_ns = latency_delta_ns(terminal_ts_ns, corr.context.tick_recv_ns),
        .event_id = corr.context.event_id,
        .market_id = corr.context.market_id,
        .leg_index = corr.context.leg_index,
        .leg_count = corr.context.leg_count,
        .qty_lots = qty_lots,
        .price_ticks = price_ticks,
        .reject_reason = reject_reason,
        .lifecycle_kind = lifecycle_kind,
        .order_status = order_status,
    });
}

OmsProcessCode Oms::handle_shard_request(const ShardOmsRequest& request) noexcept {
    return std::visit(
        [this](const auto& typed_request) -> OmsProcessCode {
            using T = std::decay_t<decltype(typed_request)>;
            if constexpr (std::is_same_v<T, NewOrderIntent>) {
                return handle_new_order_intent(typed_request);
            } else if constexpr (std::is_same_v<T, GroupOrderIntent>) {
                return handle_group_order_intent(typed_request);
            } else if constexpr (std::is_same_v<T, CancelOrderIntent>) {
                return handle_cancel_order_intent(typed_request);
            } else {
                return handle_modify_order_intent(typed_request);
            }
        },
        request);
}

} // namespace predex::core::oms::kalshi
