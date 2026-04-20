#pragma once

#include <cstddef>
#include <limits>


#include "predex/internal/market_types.hpp"
#include "predex/oms/oms_types.hpp"

namespace predex::core::oms::kalshi {

constexpr std::size_t kDefaultMaxOpenOrdersGlobal = 1'000'000;
constexpr std::size_t kDefaultMaxOpenOrdersPerEvent = 1'000'000;
constexpr internal::QtyLots kDefaultMaxGlobalExposureLots =
    std::numeric_limits<internal::QtyLots>::max() / 4;
constexpr internal::QtyLots kDefaultMaxEventExposureLots =
    std::numeric_limits<internal::QtyLots>::max() / 4;

struct GlobalRiskLimits {
    std::size_t max_open_orders_global{kDefaultMaxOpenOrdersGlobal};
    std::size_t max_open_orders_per_event{kDefaultMaxOpenOrdersPerEvent};
    internal::QtyLots max_global_exposure_lots{kDefaultMaxGlobalExposureLots};
    internal::QtyLots max_event_exposure_lots{kDefaultMaxEventExposureLots};
    bool trading_enabled{true};
};

struct GlobalRiskState {
    std::size_t open_orders_global{0};
    std::size_t open_orders_for_target_event{0};
    internal::QtyLots global_exposure_lots{0};
    internal::QtyLots target_event_exposure_lots{0};
};

class GlobalRiskManager {
  public:
    explicit GlobalRiskManager(GlobalRiskLimits limits = {}) : limits_(limits) {}

    [[nodiscard]] const GlobalRiskLimits& limits() const noexcept { return limits_; }

    [[nodiscard]] IntentDecision evaluate(const OrderIntent& intent,
                                          const GlobalRiskState& state,
                                          OmsRequestId oms_request_id,
                                          ClientOrderId client_order_id,
                                          internal::TimestampNs decision_ts_ns) const {
        if (!limits_.trading_enabled) {
            return IntentDecision{
                .code = IntentDecisionCode::kRejected,
                .data = RejectedIntent{
                    .intent = intent,
                    .reason = IntentRejectReason::kOmsDisabled,
                },
                .decision_ts_ns = decision_ts_ns,
            };
        }

        if (intent.origin.event_id == 0 || intent.origin.market_id == 0 ||
            intent.qty_lots <= 0 || intent.side == internal::Side::kUnknown) {
            return IntentDecision{
                .code = IntentDecisionCode::kRejected,
                .data = RejectedIntent{
                    .intent = intent,
                    .reason = IntentRejectReason::kInvalidIntent,
                },
                .decision_ts_ns = decision_ts_ns,
            };
        }

        if (state.open_orders_global >= limits_.max_open_orders_global) {
            return IntentDecision{
                .code = IntentDecisionCode::kRejected,
                .data = RejectedIntent{
                    .intent = intent,
                    .reason = IntentRejectReason::kGlobalRisk,
                },
                .decision_ts_ns = decision_ts_ns,
            };
        }

        if (state.open_orders_for_target_event >= limits_.max_open_orders_per_event) {
            return IntentDecision{
                .code = IntentDecisionCode::kRejected,
                .data = RejectedIntent{
                    .intent = intent,
                    .reason = IntentRejectReason::kGlobalRisk,
                },
                .decision_ts_ns = decision_ts_ns,
            };
        }

        if (state.global_exposure_lots + intent.qty_lots > limits_.max_global_exposure_lots) {
            return IntentDecision{
                .code = IntentDecisionCode::kRejected,
                .data = RejectedIntent{
                    .intent = intent,
                    .reason = IntentRejectReason::kGlobalRisk,
                },
                .decision_ts_ns = decision_ts_ns,
            };
        }

        if (state.target_event_exposure_lots + intent.qty_lots >
            limits_.max_event_exposure_lots) {
            return IntentDecision{
                .code = IntentDecisionCode::kRejected,
                .data = RejectedIntent{
                    .intent = intent,
                    .reason = IntentRejectReason::kGlobalRisk,
                },
                .decision_ts_ns = decision_ts_ns,
            };
        }

        return IntentDecision{
            .code = IntentDecisionCode::kAccepted,
            .data = AcceptedIntent{
                .intent = intent,
                .oms_request_id = oms_request_id,
                .client_order_id = std::move(client_order_id),
            },
            .decision_ts_ns = decision_ts_ns,
        };
    }

    static void on_intent_accepted(const AcceptedIntent& accepted_intent,
                                   GlobalRiskState& state) noexcept {
        ++state.open_orders_global;
        ++state.open_orders_for_target_event;
        state.global_exposure_lots += accepted_intent.intent.qty_lots;
        state.target_event_exposure_lots += accepted_intent.intent.qty_lots;
    }

    static void on_order_terminal(internal::QtyLots remaining_open_qty_lots,
                                  GlobalRiskState& state) noexcept {
        if (state.open_orders_for_target_event > 0) {
            --state.open_orders_for_target_event;
        }
        if (state.open_orders_global > 0) {
            --state.open_orders_global;
        }

        if (remaining_open_qty_lots <= 0) {
            return;
        }

        state.global_exposure_lots =
            saturating_subtract(state.global_exposure_lots, remaining_open_qty_lots);
        state.target_event_exposure_lots =
            saturating_subtract(state.target_event_exposure_lots, remaining_open_qty_lots);
    }

    static void on_fill(internal::QtyLots fill_qty_lots, GlobalRiskState& state) noexcept {
        if (fill_qty_lots <= 0) {
            return;
        }
        state.global_exposure_lots =
            saturating_subtract(state.global_exposure_lots, fill_qty_lots);
        state.target_event_exposure_lots =
            saturating_subtract(state.target_event_exposure_lots, fill_qty_lots);
    }

  private:
    GlobalRiskLimits limits_{};
    static internal::QtyLots saturating_subtract(internal::QtyLots value,
                                                 internal::QtyLots delta) noexcept {
        if (delta <= 0) {
            return value;
        }
        if (delta >= value) {
            return 0;
        }
        return value - delta;
    }
};

} // namespace predex::core::oms::kalshi
