#pragma once

#include <cstddef>
#include <limits>

#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi {
    constexpr std::size_t kDefaultMaxOpenIntentsPerEvent = 1'000'000;
    constexpr std::size_t kDefaultMaxOpenIntentsPerMarket = 1'000'000;
    constexpr internal::QtyLots kDefaultMaxEventExposureLots =
        std::numeric_limits<internal::QtyLots>::max() / 4;
    constexpr internal::QtyLots kDefaultMaxMarketExposureLots =
        std::numeric_limits<internal::QtyLots>::max() / 4;

struct LocalRiskLimits {
    std::size_t max_open_intents_per_event{kDefaultMaxOpenIntentsPerEvent};
    std::size_t max_open_intents_per_market{kDefaultMaxOpenIntentsPerMarket};
    internal::QtyLots max_event_exposure_lots{kDefaultMaxEventExposureLots};
    internal::QtyLots max_market_exposure_lots{kDefaultMaxMarketExposureLots};
    bool trading_enabled{true};
};

struct LocalRiskState {
    std::size_t open_intents_for_event{0};
    std::size_t open_intents_for_market{0};
    internal::QtyLots event_exposure_lots{0};
    internal::QtyLots market_exposure_lots{0};
};

class LocalRiskManager {
  public:
    explicit LocalRiskManager(LocalRiskLimits limits = {}) : limits_(limits) {}

    [[nodiscard]] const LocalRiskLimits& limits() const noexcept { return limits_; }

    [[nodiscard]] RiskDecision evaluate(const AppliedEventUpdate& update,
                                        const OmsOrderIntent& intent,
                                        const LocalRiskState& state) const noexcept {
        static_cast<void>(update);
        if (!limits_.trading_enabled) {
            return RiskDecision{
                .code = RiskDecisionCode::kDisabled,
                .reason = RiskRejectReason::kStrategyDisabled,
            };
        }
        if (intent.origin.market_id == 0 || intent.qty_lots <= 0 ||
            intent.side == internal::Side::kUnknown) {
            return RiskDecision{
                .code = RiskDecisionCode::kRejected,
                .reason = RiskRejectReason::kInvalidIntent,
            };
        }
        if (state.open_intents_for_event >= limits_.max_open_intents_per_event) {
            return RiskDecision{
                .code = RiskDecisionCode::kRejected,
                .reason = RiskRejectReason::kMaxOpenIntents,
            };
        }
        if (state.open_intents_for_market >= limits_.max_open_intents_per_market) {
            return RiskDecision{
                .code = RiskDecisionCode::kRejected,
                .reason = RiskRejectReason::kMaxOpenIntents,
            };
        }
        if (state.event_exposure_lots + intent.qty_lots > limits_.max_event_exposure_lots) {
            return RiskDecision{
                .code = RiskDecisionCode::kRejected,
                .reason = RiskRejectReason::kEventExposureLimit,
            };
        }
        if (state.market_exposure_lots + intent.qty_lots > limits_.max_market_exposure_lots) {
            return RiskDecision{
                .code = RiskDecisionCode::kRejected,
                .reason = RiskRejectReason::kMarketExposureLimit,
            };
        }
        return RiskDecision{
            .code = RiskDecisionCode::kAccepted,
            .reason = RiskRejectReason::kNone,
            .accepted_intent = intent,
        };
    }

  private:
    LocalRiskLimits limits_{};
};

}  // namespace predex::core::shards::kalshi
