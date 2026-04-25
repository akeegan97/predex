#include "predex/oms/oms.hpp"

#include <algorithm>
#include <chrono>
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

} // namespace

Oms::Oms(std::vector<ShardRequestQueue*> shard_request_queues,
         std::vector<ShardDecisionQueue*> shard_decision_queues,
         std::vector<ShardLifecycleQueue*> shard_lifecycle_queues,
         ExecutionTransportQueues transport_queues,
         GlobalRiskLimits global_risk_limits,
         std::function<std::optional<std::string>(internal::MarketId)> market_ticker_resolver)
    : global_risk_(std::move(global_risk_limits)),
      order_store_(),
      transport_(std::move(transport_queues)),
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
                ? static_cast<std::int64_t>(apply_result.fill_qty_lots) *
                      static_cast<std::int64_t>(*previous_working_price)
                : 0;
        global_risk_.on_fill(event_id, apply_result.fill_qty_lots, released_ticks);
    }
    if (apply_result.became_terminal) {
        const std::int64_t released_ticks =
            previous_working_price.has_value()
                ? static_cast<std::int64_t>(apply_result.remaining_open_qty_lots) *
                      static_cast<std::int64_t>(*previous_working_price)
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
