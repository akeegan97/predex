#pragma once

#include <cstddef>
#include <optional>
#include <string>

#include "predex/oms/oms_types.hpp"

namespace predex::core::oms::kalshi {

constexpr std::size_t kDefaultMaxOpenOrdersGlobal = 128;
constexpr std::size_t kDefaultMaxOpenOrdersPerEvent = 16;
constexpr internal::QtyLots kDefaultMaxGlobalExposureLots = 1000;
constexpr internal::QtyLots kDefaultMaxEventExposureLots = 200;

struct GlobalRiskLimits {
    std::size_t max_open_orders_global{kDefaultMaxOpenOrdersGlobal};
    std::size_t max_open_orders_per_event{kDefaultMaxOpenOrdersPerEvent};
    internal::QtyLots max_global_exposure_lots{kDefaultMaxGlobalExposureLots};
    internal::QtyLots max_event_exposure_lots{kDefaultMaxEventExposureLots};
    bool trading_enabled{true};
};

struct GlobalRiskState {
    std::size_t open_orders_global{0};
    std::size_t open_orders_for_event{0};
    internal::QtyLots global_exposure_lots{0};
    internal::QtyLots event_exposure_lots{0};
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

        if (state.open_orders_for_event >= limits_.max_open_orders_per_event) {
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

        if (state.event_exposure_lots + intent.qty_lots > limits_.max_event_exposure_lots) {
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

    void on_intent_accepted(const AcceptedIntent& accepted_intent,
                            GlobalRiskState& state) const noexcept {
        ++state.open_orders_global;
        ++state.open_orders_for_event;
        state.global_exposure_lots += accepted_intent.intent.qty_lots;
        state.event_exposure_lots += accepted_intent.intent.qty_lots;
    }

  private:
    GlobalRiskLimits limits_{};
};

} // namespace predex::core::oms::kalshi
