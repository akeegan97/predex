#include "predex/oms/order_store.hpp"

namespace predex::core::oms::kalshi {

void OrderStore::insert_live_order(const AcceptedIntent& accepted_intent,
                                    internal::TimestampNs decision_ts_ns) {
    OrderState order_state{
        .origin = accepted_intent.intent.origin,
        .oms_request_id = accepted_intent.oms_request_id,
        .status = OmsOrderStatus::kPendingSubmit,
        .client_order_id = accepted_intent.client_order_id,
        .exchange_order_id = std::nullopt,
        .original_qty_lots = accepted_intent.intent.qty_lots,
        .live_qty_lots = accepted_intent.intent.qty_lots,
        .cum_fill_qty_lots = 0,
        .live_limit_price_ticks = accepted_intent.intent.limit_price_ticks,
        .last_update_ts_ns = decision_ts_ns,
        .oms_decision_ts_ns = decision_ts_ns,
        .transport_submit_ts_ns = 0,
        .first_fill_recv_ns = 0,
        .terminal_recv_ns = 0,
    };
    request_by_client_order_id_[order_state.client_order_id] = order_state.oms_request_id;
    orders_by_request_id_[order_state.oms_request_id] = std::move(order_state);
}

void OrderStore::set_transport_submit_ts(OmsRequestId oms_request_id,
                                          internal::TimestampNs ts_ns) noexcept {
    const auto it = orders_by_request_id_.find(oms_request_id);
    if (it != orders_by_request_id_.end()) {
        it->second.transport_submit_ts_ns = ts_ns;
    }
}

OrderState* OrderStore::find_order_state(const OrderLifecycleEvent& event) {
    {
        auto it = orders_by_request_id_.find(event.oms_request_id);
        if (it != orders_by_request_id_.end()) {
            return &it->second;
        }
    }
    if (!event.client_order_id.empty()) {
        const auto by_client = request_by_client_order_id_.find(event.client_order_id);
        if (by_client != request_by_client_order_id_.end()) {
            auto it = orders_by_request_id_.find(by_client->second);
            if (it != orders_by_request_id_.end()) {
                return &it->second;
            }
        }
    }
    if (event.exchange_order_id.has_value()) {
        const auto by_exchange =
            request_by_exchange_order_id_.find(*event.exchange_order_id);
        if (by_exchange != request_by_exchange_order_id_.end()) {
            auto it = orders_by_request_id_.find(by_exchange->second);
            if (it != orders_by_request_id_.end()) {
                return &it->second;
            }
        }
    }
    return nullptr;
}

OrderState* OrderStore::find_order_for_action(
    std::optional<OmsRequestId> oms_request_id,
    const ClientOrderId& client_order_id,
    const std::optional<ExchangeOrderId>& exchange_order_id) {
    if (oms_request_id.has_value()) {
        auto it = orders_by_request_id_.find(*oms_request_id);
        if (it != orders_by_request_id_.end()) {
            return &it->second;
        }
    }
    if (!client_order_id.empty()) {
        const auto by_client = request_by_client_order_id_.find(client_order_id);
        if (by_client != request_by_client_order_id_.end()) {
            auto it = orders_by_request_id_.find(by_client->second);
            if (it != orders_by_request_id_.end()) {
                return &it->second;
            }
        }
    }
    if (exchange_order_id.has_value()) {
        const auto by_exchange = request_by_exchange_order_id_.find(*exchange_order_id);
        if (by_exchange != request_by_exchange_order_id_.end()) {
            auto it = orders_by_request_id_.find(by_exchange->second);
            if (it != orders_by_request_id_.end()) {
                return &it->second;
            }
        }
    }
    return nullptr;
}

OrderStore::LifecycleApplyResult OrderStore::apply_lifecycle_event(
    const OrderLifecycleEvent& event) {
    OrderState* const order_state = find_order_state(event);
    if (order_state == nullptr) {
        return {};
    }

    const internal::EventId event_id = order_state->origin.event_id;

    order_state->status = event.status;
    order_state->last_update_ts_ns = event.recv_ts_ns;

    if (!event.client_order_id.empty() &&
        event.client_order_id != order_state->client_order_id) {
        request_by_client_order_id_.erase(order_state->client_order_id);
        order_state->client_order_id = event.client_order_id;
        request_by_client_order_id_[order_state->client_order_id] = order_state->oms_request_id;
    }
    if (event.exchange_order_id.has_value()) {
        order_state->exchange_order_id = event.exchange_order_id;
        request_by_exchange_order_id_[*event.exchange_order_id] = order_state->oms_request_id;
    }

    internal::QtyLots fill_qty_lots = 0;
    if (const auto* fill = std::get_if<OrderFill>(&event.data)) {
        const internal::QtyLots fill_reduction =
            fill->fill_qty_lots > order_state->live_qty_lots ? order_state->live_qty_lots
                                                              : fill->fill_qty_lots;
        order_state->cum_fill_qty_lots += fill_reduction;
        order_state->live_qty_lots -= fill_reduction;
        if (order_state->first_fill_recv_ns == 0) {
            order_state->first_fill_recv_ns = event.recv_ts_ns;
        }
        fill_qty_lots = fill_reduction;
    }

    if (const auto* replace_ack = std::get_if<ReplaceAck>(&event.data)) {
        order_state->live_qty_lots = replace_ack->replaced_qty_lots;
        order_state->live_limit_price_ticks = replace_ack->replaced_limit_price_ticks;
    }

    const bool is_terminal =
        event.kind == OrderLifecycleEventKind::kReject ||
        event.kind == OrderLifecycleEventKind::kCanceled ||
        event.kind == OrderLifecycleEventKind::kFill;

    if (!is_terminal) {
        return LifecycleApplyResult{
            .ok = true,
            .event_id = event_id,
            .fill_qty_lots = fill_qty_lots,
            .first_fill_recv_ns = order_state->first_fill_recv_ns,
            .is_terminal = false,
            .remaining_open_qty_lots = 0,
        };
    }

    const internal::QtyLots remaining_open_qty_lots = order_state->live_qty_lots;
    const internal::TimestampNs first_fill_recv_ns = order_state->first_fill_recv_ns;
    order_state->live_qty_lots = 0;
    order_state->terminal_recv_ns = event.recv_ts_ns;

    const OmsRequestId request_id = order_state->oms_request_id;
    const ClientOrderId client_order_id = order_state->client_order_id;
    const std::optional<ExchangeOrderId> exchange_order_id = order_state->exchange_order_id;

    request_by_client_order_id_.erase(client_order_id);
    if (exchange_order_id.has_value()) {
        request_by_exchange_order_id_.erase(*exchange_order_id);
    }
    orders_by_request_id_.erase(request_id);

    return LifecycleApplyResult{
        .ok = true,
        .event_id = event_id,
        .fill_qty_lots = fill_qty_lots,
        .first_fill_recv_ns = first_fill_recv_ns,
        .is_terminal = true,
        .remaining_open_qty_lots = remaining_open_qty_lots,
    };
}

void OrderStore::adopt_orphaned(OrderState state) {
    const OmsRequestId request_id = state.oms_request_id;
    if (!state.client_order_id.empty()) {
        request_by_client_order_id_[state.client_order_id] = request_id;
    }
    if (state.exchange_order_id.has_value() && !state.exchange_order_id->empty()) {
        request_by_exchange_order_id_[*state.exchange_order_id] = request_id;
    }
    orders_by_request_id_.emplace(request_id, std::move(state));
}

std::vector<CancelOrderCmd> OrderStore::build_cancel_all_cmds(
    internal::TimestampNs ts_ns) const {
    std::vector<CancelOrderCmd> cmds;
    cmds.reserve(orders_by_request_id_.size());
    for (const auto& [request_id, state] : orders_by_request_id_) {
        if (state.status == OmsOrderStatus::kPendingCancel) {
            continue;
        }
        cmds.push_back(CancelOrderCmd{
            .oms_request_id = state.oms_request_id,
            .origin = state.origin,
            .client_order_id = state.client_order_id,
            .exchange_order_id = state.exchange_order_id,
            .cmd_ts_ns = ts_ns,
        });
    }
    return cmds;
}

std::size_t OrderStore::live_order_count() const noexcept {
    return orders_by_request_id_.size();
}

} // namespace predex::core::oms::kalshi
