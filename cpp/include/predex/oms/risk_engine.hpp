#pragma once

#include <cstddef>
#include <unordered_map>

#include "predex/internal/market_types.hpp"
#include "predex/oms/global_risk.hpp"
#include "predex/oms/oms_types.hpp"

namespace predex::core::oms::kalshi {

// Owns the mutable risk state (global + per-event) and delegates evaluation
// to GlobalRiskManager. Single writer: OMS coordinator thread.
class RiskEngine {
  public:
    explicit RiskEngine(GlobalRiskManager manager = GlobalRiskManager{});

    // Assembles the correct GlobalRiskState for intent.origin.event_id, then
    // delegates to GlobalRiskManager::evaluate(). Returns a fully-formed
    // IntentDecision (accepted or rejected).
    [[nodiscard]] IntentDecision evaluate(const OrderIntent& intent,
                                          OmsRequestId oms_request_id,
                                          ClientOrderId client_order_id,
                                          internal::TimestampNs decision_ts_ns) noexcept;

    void on_intent_accepted(const AcceptedIntent& accepted_intent) noexcept;

    void on_fill(internal::EventId event_id, internal::QtyLots fill_qty_lots) noexcept;

    void on_capital_released(std::int64_t released_capital_ticks) noexcept;

    void on_order_terminal(internal::EventId event_id,
                           internal::QtyLots remaining_open_qty_lots) noexcept;

    [[nodiscard]] GlobalRiskState make_state_for_event(
        internal::EventId event_id) const noexcept;

    [[nodiscard]] const GlobalRiskState& global_state() const noexcept;

    [[nodiscard]] const GlobalRiskLimits& limits() const noexcept;

  private:
    struct EventRiskState {
        std::size_t open_orders{0};
        internal::QtyLots exposure_lots{0};
    };

    GlobalRiskManager manager_;
    GlobalRiskState global_state_{};
    std::unordered_map<internal::EventId, EventRiskState> event_state_;
};

} // namespace predex::core::oms::kalshi
