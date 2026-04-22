#include "predex/oms/risk_engine.hpp"

namespace predex::core::oms::kalshi {

RiskEngine::RiskEngine(GlobalRiskManager manager) : manager_(std::move(manager)) {}

IntentDecision RiskEngine::evaluate(const OrderIntent& intent,
                                     OmsRequestId oms_request_id,
                                     ClientOrderId client_order_id,
                                     internal::TimestampNs decision_ts_ns) noexcept {
    return manager_.evaluate(intent,
                             make_state_for_event(intent.origin.event_id),
                             oms_request_id,
                             std::move(client_order_id),
                             decision_ts_ns);
}

void RiskEngine::on_intent_accepted(const AcceptedIntent& accepted_intent) noexcept {
    GlobalRiskState state = make_state_for_event(accepted_intent.intent.origin.event_id);
    GlobalRiskManager::on_intent_accepted(accepted_intent, state);
    global_state_.open_orders_global = state.open_orders_global;
    global_state_.global_exposure_lots = state.global_exposure_lots;
    global_state_.locked_capital_ticks = state.locked_capital_ticks;
    auto& event_risk = event_state_[accepted_intent.intent.origin.event_id];
    event_risk.open_orders = state.open_orders_for_target_event;
    event_risk.exposure_lots = state.target_event_exposure_lots;
}

void RiskEngine::on_fill(internal::EventId event_id,
                          internal::QtyLots fill_qty_lots) noexcept {
    GlobalRiskState state = make_state_for_event(event_id);
    GlobalRiskManager::on_fill(fill_qty_lots, state);
    global_state_.global_exposure_lots = state.global_exposure_lots;
    auto& event_risk = event_state_[event_id];
    event_risk.open_orders = state.open_orders_for_target_event;
    event_risk.exposure_lots = state.target_event_exposure_lots;
}

void RiskEngine::on_capital_released(std::int64_t released_capital_ticks) noexcept {
    GlobalRiskManager::on_capital_released(released_capital_ticks, global_state_);
}

void RiskEngine::on_order_terminal(internal::EventId event_id,
                                    internal::QtyLots remaining_open_qty_lots) noexcept {
    GlobalRiskState state = make_state_for_event(event_id);
    GlobalRiskManager::on_order_terminal(remaining_open_qty_lots, state);
    global_state_.open_orders_global = state.open_orders_global;
    global_state_.global_exposure_lots = state.global_exposure_lots;

    if (state.open_orders_for_target_event == 0 && state.target_event_exposure_lots == 0) {
        event_state_.erase(event_id);
        return;
    }

    auto& event_risk = event_state_[event_id];
    event_risk.open_orders = state.open_orders_for_target_event;
    event_risk.exposure_lots = state.target_event_exposure_lots;
}

GlobalRiskState RiskEngine::make_state_for_event(
    internal::EventId event_id) const noexcept {
    GlobalRiskState state{
        .open_orders_global = global_state_.open_orders_global,
        .open_orders_for_target_event = 0,
        .global_exposure_lots = global_state_.global_exposure_lots,
        .target_event_exposure_lots = 0,
        .locked_capital_ticks = global_state_.locked_capital_ticks,
    };
    const auto it = event_state_.find(event_id);
    if (it != event_state_.end()) {
        state.open_orders_for_target_event = it->second.open_orders;
        state.target_event_exposure_lots = it->second.exposure_lots;
    }
    return state;
}

const GlobalRiskState& RiskEngine::global_state() const noexcept {
    return global_state_;
}

const GlobalRiskLimits& RiskEngine::limits() const noexcept {
    return manager_.limits();
}

} // namespace predex::core::oms::kalshi
