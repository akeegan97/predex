#include "predex/oms/order_store.hpp"

#include <algorithm>
#include <type_traits>
#include <utility>

namespace predex::core::oms::kalshi {
namespace {

[[nodiscard]] OrderStatus restored_working_status(const OrderState& state) noexcept {
    if (state.status_before_pending_transition.has_value()) {
        return *state.status_before_pending_transition;
    }
    if (state.cumulative_filled_qty_lots > 0) {
        return OrderStatus::kPartiallyFilled;
    }
    return OrderStatus::kWorking;
}

[[nodiscard]] bool is_terminal_status(OrderStatus status) noexcept {
    return status == OrderStatus::kRejected || status == OrderStatus::kFilled ||
           status == OrderStatus::kCanceled;
}

} // namespace

OrderState* OrderStore::find(const LookupKey& key) noexcept {
    if (key.oms_request_id.has_value()) {
        if (auto* state = find_by_request_id(*key.oms_request_id); state != nullptr) {
            return state;
        }
    }
    if (!key.client_order_id.value.empty()) {
        if (auto* state = find_by_client_order_id(key.client_order_id); state != nullptr) {
            return state;
        }
    }
    if (key.exchange_order_id.has_value()) {
        return find_by_exchange_order_id(*key.exchange_order_id);
    }
    return nullptr;
}

const OrderState* OrderStore::find(const LookupKey& key) const noexcept {
    return const_cast<OrderStore*>(this)->find(key);
}

OrderState* OrderStore::find(const OmsOrderRef& order) noexcept {
    return find(LookupKey{
        .oms_request_id = order.oms_request_id,
        .client_order_id = order.client_order_id,
        .exchange_order_id = order.exchange_order_id,
    });
}

const OrderState* OrderStore::find(const OmsOrderRef& order) const noexcept {
    return const_cast<OrderStore*>(this)->find(order);
}

void OrderStore::insert_pending_submit(const BeginSubmitTransition& transition) {
    OrderState state{
        .context = transition.intent.context,
        .order = transition.corr.order,
        .exchange = transition.intent.exchange,
        .side = transition.intent.side,
        .outcome = transition.intent.outcome,
        .initial_qty_lots = transition.intent.qty_lots,
        .working_qty_lots = transition.intent.qty_lots,
        .cumulative_filled_qty_lots = 0,
        .initial_limit_price_ticks = transition.intent.limit_price_ticks,
        .working_limit_price_ticks = transition.intent.limit_price_ticks,
        .time_in_force = transition.intent.time_in_force,
        .liquidity_intent = transition.intent.liquidity_intent,
        .order_type_intent = transition.intent.order_type_intent,
        .status = OrderStatus::kPendingSubmit,
        .last_venue_reject_reason = VenueRejectReason::kNone,
        .intent_ts_ns = transition.intent.intent_ts_ns,
        .oms_decision_ts_ns = transition.oms_decision_ts_ns,
        .venue_submit_ts_ns = 0,
        .venue_ack_ts_ns = 0,
        .first_fill_ts_ns = 0,
        .last_fill_ts_ns = 0,
        .terminal_ts_ns = 0,
        .last_update_ts_ns = transition.oms_decision_ts_ns,
    };

    bind_client_order_id(state.order.client_order_id, state.order.oms_request_id);
    if (state.order.exchange_order_id.has_value()) {
        bind_exchange_order_id(*state.order.exchange_order_id, state.order.oms_request_id);
    }
    orders_by_request_id_[state.order.oms_request_id] = std::move(state);
}

void OrderStore::mark_submitted(const MarkSubmittedTransition& transition) noexcept {
    if (auto* state = find(transition.order); state != nullptr) {
        state->venue_submit_ts_ns = transition.venue_submit_ts_ns;
        state->last_update_ts_ns = transition.venue_submit_ts_ns;
    }
}

bool OrderStore::mark_pending_cancel(const BeginCancelTransition& transition) noexcept {
    auto* state = find(transition.corr.order);
    if (state == nullptr || is_terminal_status(state->status)) {
        return false;
    }
    state->status_before_pending_transition = state->status;
    state->status = OrderStatus::kPendingCancel;
    state->last_update_ts_ns = transition.ts_ns;
    return true;
}

bool OrderStore::mark_pending_modify(const BeginModifyTransition& transition) noexcept {
    auto* state = find(transition.corr.order);
    if (state == nullptr || is_terminal_status(state->status)) {
        return false;
    }
    state->status_before_pending_transition = state->status;
    state->status = OrderStatus::kPendingModify;
    state->pending_replacement = PendingReplacement{
        .requested_working_qty_lots = transition.replacement.qty_lots,
        .requested_working_limit_price_ticks = transition.replacement.limit_price_ticks,
        .request_ts_ns = transition.ts_ns,
    };
    state->last_update_ts_ns = transition.ts_ns;
    return true;
}

OrderStore::VenueApplyResult OrderStore::apply_venue_event(const SourcedKalshiEvent& sourced_event) {
    const auto* ref = std::visit(
        [](const auto& typed_event) -> const OmsOrderRef* { return &typed_event.order; },
        sourced_event.event);
    OrderState* state = find(*ref);
    if (state == nullptr) {
        if (const auto* snapshot = std::get_if<ReconcileOpenOrderSnapshot>(&sourced_event.event)) {
            adopt_reconciled_order(OrderState{
                .context = snapshot->context,
                .order = snapshot->order,
                .exchange = snapshot->exchange,
                .side = snapshot->side,
                .outcome = snapshot->outcome,
                .initial_qty_lots = snapshot->initial_qty_lots,
                .working_qty_lots = snapshot->working_qty_lots,
                .cumulative_filled_qty_lots = snapshot->cumulative_filled_qty_lots,
                .initial_limit_price_ticks = snapshot->working_limit_price_ticks,
                .working_limit_price_ticks = snapshot->working_limit_price_ticks,
                .status = snapshot->working_qty_lots > 0
                    ? (snapshot->cumulative_filled_qty_lots > 0 ? OrderStatus::kPartiallyFilled
                                                                 : OrderStatus::kWorking)
                    : OrderStatus::kFilled,
                .intent_ts_ns = snapshot->context.signal_ts_ns,
                .venue_ack_ts_ns = snapshot->recv_ts_ns,
                .last_update_ts_ns = snapshot->recv_ts_ns,
            });
            return {
                .ok = true,
                .order_found = false,
            };
        }
        return {};
    }

    VenueApplyResult result{
        .ok = true,
        .order_found = true,
        .first_fill_observed = false,
        .became_terminal = false,
        .fill_qty_lots = 0,
        .remaining_open_qty_lots = state->working_qty_lots,
    };

    const auto recv_ts_ns = std::visit(
        [](const auto& typed_event) { return typed_event.recv_ts_ns; }, sourced_event.event);
    state->last_update_ts_ns = recv_ts_ns;

    auto update_identity = [&](const OmsOrderRef& order) {
        if (order.client_order_id != state->order.client_order_id &&
            !order.client_order_id.value.empty()) {
            request_by_client_order_id_.erase(state->order.client_order_id);
            state->order.client_order_id = order.client_order_id;
            bind_client_order_id(state->order.client_order_id, state->order.oms_request_id);
        }
        if (order.exchange_order_id.has_value() &&
            (!state->order.exchange_order_id.has_value() ||
             *state->order.exchange_order_id != *order.exchange_order_id)) {
            if (state->order.exchange_order_id.has_value()) {
                request_by_exchange_order_id_.erase(*state->order.exchange_order_id);
            }
            state->order.exchange_order_id = order.exchange_order_id;
            bind_exchange_order_id(*state->order.exchange_order_id, state->order.oms_request_id);
        }
    };

    std::visit(
        [&](const auto& event) {
            using T = std::decay_t<decltype(event)>;
            update_identity(event.order);

            if constexpr (std::is_same_v<T, VenueOrderAck>) {
                state->venue_ack_ts_ns = event.recv_ts_ns;
                state->status = OrderStatus::kWorking;
                state->working_qty_lots = event.accepted_qty_lots > 0 ? event.accepted_qty_lots
                                                                      : state->working_qty_lots;
                result.remaining_open_qty_lots = state->working_qty_lots;
            } else if constexpr (std::is_same_v<T, VenueOrderReject>) {
                state->venue_ack_ts_ns = event.recv_ts_ns;
                state->status = OrderStatus::kRejected;
                state->last_venue_reject_reason = event.reason;
                state->terminal_ts_ns = event.recv_ts_ns;
                result.became_terminal = true;
                result.remaining_open_qty_lots = state->working_qty_lots;
            } else if constexpr (std::is_same_v<T, VenueOrderPartialFill>) {
                const internal::QtyLots applied_fill =
                    std::min(state->working_qty_lots, event.fill_qty_lots);
                state->cumulative_filled_qty_lots += applied_fill;
                state->working_qty_lots -= applied_fill;
                state->status = state->working_qty_lots > 0 ? OrderStatus::kPartiallyFilled
                                                            : OrderStatus::kFilled;
                if (state->first_fill_ts_ns == 0 && applied_fill > 0) {
                    state->first_fill_ts_ns = event.recv_ts_ns;
                    result.first_fill_observed = true;
                }
                if (applied_fill > 0) {
                    state->last_fill_ts_ns = event.recv_ts_ns;
                }
                result.fill_qty_lots = applied_fill;
                result.remaining_open_qty_lots = state->working_qty_lots;
            } else if constexpr (std::is_same_v<T, VenueOrderFill>) {
                const internal::QtyLots applied_fill =
                    std::min(state->working_qty_lots, event.fill_qty_lots > 0
                                                          ? event.fill_qty_lots
                                                          : state->working_qty_lots);
                state->cumulative_filled_qty_lots += applied_fill;
                state->working_qty_lots -= applied_fill;
                if (state->first_fill_ts_ns == 0 && applied_fill > 0) {
                    state->first_fill_ts_ns = event.recv_ts_ns;
                    result.first_fill_observed = true;
                }
                if (applied_fill > 0) {
                    state->last_fill_ts_ns = event.recv_ts_ns;
                }
                state->status = OrderStatus::kFilled;
                state->terminal_ts_ns = event.recv_ts_ns;
                result.fill_qty_lots = applied_fill;
                result.remaining_open_qty_lots = state->working_qty_lots;
                result.became_terminal = true;
            } else if constexpr (std::is_same_v<T, VenueCancelAck>) {
                state->status = OrderStatus::kPendingCancel;
            } else if constexpr (std::is_same_v<T, VenueCancelReject>) {
                state->status = restored_working_status(*state);
                state->status_before_pending_transition.reset();
                state->last_venue_reject_reason = event.reason;
            } else if constexpr (std::is_same_v<T, VenueModifyAck>) {
                state->working_qty_lots = event.working_qty_lots;
                state->working_limit_price_ticks = event.working_price_ticks;
                state->status = restored_working_status(*state);
                state->status_before_pending_transition.reset();
                state->pending_replacement.reset();
                result.remaining_open_qty_lots = state->working_qty_lots;
            } else if constexpr (std::is_same_v<T, VenueModifyReject>) {
                state->status = restored_working_status(*state);
                state->status_before_pending_transition.reset();
                state->pending_replacement.reset();
                state->last_venue_reject_reason = event.reason;
            } else if constexpr (std::is_same_v<T, VenueOrderCanceled>) {
                state->status = OrderStatus::kCanceled;
                state->terminal_ts_ns = event.recv_ts_ns;
                result.remaining_open_qty_lots = state->working_qty_lots;
                result.became_terminal = true;
            } else if constexpr (std::is_same_v<T, VenueOrderUncertain>) {
                state->status = OrderStatus::kUncertain;
            } else if constexpr (std::is_same_v<T, ReconcileOpenOrderSnapshot>) {
                state->context = event.context;
                state->exchange = event.exchange;
                state->side = event.side;
                state->outcome = event.outcome;
                state->initial_qty_lots = event.initial_qty_lots;
                state->working_qty_lots = event.working_qty_lots;
                state->cumulative_filled_qty_lots = event.cumulative_filled_qty_lots;
                state->working_limit_price_ticks = event.working_limit_price_ticks;
                state->last_update_ts_ns = event.recv_ts_ns;
                state->status = event.working_qty_lots > 0
                    ? (event.cumulative_filled_qty_lots > 0 ? OrderStatus::kPartiallyFilled
                                                            : OrderStatus::kWorking)
                    : OrderStatus::kFilled;
                result.remaining_open_qty_lots = state->working_qty_lots;
            }
        },
        sourced_event.event);

    if (result.became_terminal) {
        erase_indices_for(*state);
        orders_by_request_id_.erase(state->order.oms_request_id);
    }

    return result;
}

void OrderStore::adopt_reconciled_order(OrderState state) {
    bind_client_order_id(state.order.client_order_id, state.order.oms_request_id);
    if (state.order.exchange_order_id.has_value()) {
        bind_exchange_order_id(*state.order.exchange_order_id, state.order.oms_request_id);
    }
    orders_by_request_id_[state.order.oms_request_id] = std::move(state);
}

std::vector<CancelOrderCmd> OrderStore::build_cancel_all_cmds(internal::TimestampNs ts_ns) const {
    std::vector<CancelOrderCmd> cmds;
    cmds.reserve(orders_by_request_id_.size());
    for (const auto& [request_id, state] : orders_by_request_id_) {
        if (is_terminal_status(state.status) || state.status == OrderStatus::kPendingCancel) {
            continue;
        }
        cmds.push_back(CancelOrderCmd{
            .corr = ShardOrderCorrelation{
                .context = state.context,
                .order = state.order,
            },
            .cmd_ts_ns = ts_ns,
        });
    }
    return cmds;
}

std::size_t OrderStore::live_order_count() const noexcept {
    return orders_by_request_id_.size();
}

OrderState* OrderStore::find_by_request_id(OmsRequestId oms_request_id) noexcept {
    const auto it = orders_by_request_id_.find(oms_request_id);
    return it != orders_by_request_id_.end() ? &it->second : nullptr;
}

const OrderState* OrderStore::find_by_request_id(OmsRequestId oms_request_id) const noexcept {
    return const_cast<OrderStore*>(this)->find_by_request_id(oms_request_id);
}

OrderState* OrderStore::find_by_client_order_id(const ClientOrderId& client_order_id) noexcept {
    const auto it = request_by_client_order_id_.find(client_order_id);
    return it != request_by_client_order_id_.end() ? find_by_request_id(it->second) : nullptr;
}

const OrderState* OrderStore::find_by_client_order_id(
    const ClientOrderId& client_order_id) const noexcept {
    return const_cast<OrderStore*>(this)->find_by_client_order_id(client_order_id);
}

OrderState* OrderStore::find_by_exchange_order_id(
    const ExchangeOrderId& exchange_order_id) noexcept {
    const auto it = request_by_exchange_order_id_.find(exchange_order_id);
    return it != request_by_exchange_order_id_.end() ? find_by_request_id(it->second) : nullptr;
}

const OrderState* OrderStore::find_by_exchange_order_id(
    const ExchangeOrderId& exchange_order_id) const noexcept {
    return const_cast<OrderStore*>(this)->find_by_exchange_order_id(exchange_order_id);
}

void OrderStore::bind_client_order_id(const ClientOrderId& client_order_id,
                                      OmsRequestId oms_request_id) {
    if (!client_order_id.value.empty()) {
        request_by_client_order_id_[client_order_id] = oms_request_id;
    }
}

void OrderStore::bind_exchange_order_id(const ExchangeOrderId& exchange_order_id,
                                        OmsRequestId oms_request_id) {
    if (!exchange_order_id.value.empty()) {
        request_by_exchange_order_id_[exchange_order_id] = oms_request_id;
    }
}

void OrderStore::erase_indices_for(const OrderState& state) {
    if (!state.order.client_order_id.value.empty()) {
        request_by_client_order_id_.erase(state.order.client_order_id);
    }
    if (state.order.exchange_order_id.has_value()) {
        request_by_exchange_order_id_.erase(*state.order.exchange_order_id);
    }
}

} // namespace predex::core::oms::kalshi
