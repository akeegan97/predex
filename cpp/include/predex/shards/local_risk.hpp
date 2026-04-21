#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <unordered_map>

#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi {
    constexpr std::size_t kDefaultMaxOpenIntentsPerEvent = 1'000'000;
    constexpr std::size_t kDefaultMaxOpenIntentsPerMarket = 1'000'000;
    constexpr internal::QtyLots kDefaultMaxEventExposureLots =
        std::numeric_limits<internal::QtyLots>::max() / 4;
    constexpr internal::QtyLots kDefaultMaxMarketExposureLots =
        std::numeric_limits<internal::QtyLots>::max() / 4;
    constexpr std::int64_t kDefaultMaxNetPositionLotsPerMarket =
        std::numeric_limits<std::int64_t>::max() / 4;

struct LocalRiskLimits {
    std::size_t max_open_intents_per_event{kDefaultMaxOpenIntentsPerEvent};
    std::size_t max_open_intents_per_market{kDefaultMaxOpenIntentsPerMarket};
    internal::QtyLots max_event_exposure_lots{kDefaultMaxEventExposureLots};
    internal::QtyLots max_market_exposure_lots{kDefaultMaxMarketExposureLots};
    // Maximum absolute net filled position per market (long or short). 0 = disabled.
    std::int64_t max_net_position_lots_per_market{kDefaultMaxNetPositionLotsPerMarket};
    // Reject intents for markets closing within this many seconds. 0 = disabled.
    std::uint64_t min_seconds_to_close{0};
    bool trading_enabled{true};
};

struct LocalRiskState {
    std::size_t open_intents_for_event{0};
    std::size_t open_intents_for_market{0};
    internal::QtyLots event_exposure_lots{0};
    internal::QtyLots market_exposure_lots{0};
    std::unordered_map<internal::MarketId, std::int64_t> net_position_lots_by_market;
};

class LocalRiskManager {
  public:
    explicit LocalRiskManager(LocalRiskLimits limits = {}) : limits_(limits) {}

    [[nodiscard]] const LocalRiskLimits& limits() const noexcept { return limits_; }

    [[nodiscard]] RiskDecision evaluate(const AppliedEventUpdate& update,
                                        const OmsOrderIntent& intent,
                                        const LocalRiskState& state) const noexcept {
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
        // Kalshi emits no WS event at natural close_time, so we cannot wait for a
        // deactivation message — every intent must be compared against wall-clock now.
        // The hard post-close reject fires unconditionally; the margin reject is only
        // active when min_seconds_to_close is configured > 0.
        {
            const auto* market_view = update.event.find_market_view(intent.origin.market_id);
            if (market_view != nullptr) {
                const auto now_s = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count());
                if (!market_view->lifecycle.is_tradeable_at(now_s)) {
                    return RiskDecision{
                        .code = RiskDecisionCode::kRejected,
                        .reason = RiskRejectReason::kMarketClosed,
                    };
                }
                if (limits_.min_seconds_to_close > 0 &&
                    market_view->lifecycle.close_ts_s > 0 &&
                    market_view->lifecycle.close_ts_s <= now_s + limits_.min_seconds_to_close) {
                    return RiskDecision{
                        .code = RiskDecisionCode::kRejected,
                        .reason = RiskRejectReason::kMarketCloseSoon,
                    };
                }
            }
        }

        if (limits_.max_net_position_lots_per_market < kDefaultMaxNetPositionLotsPerMarket) {
            const auto pos_it =
                state.net_position_lots_by_market.find(intent.origin.market_id);
            const std::int64_t current_net =
                pos_it != state.net_position_lots_by_market.end() ? pos_it->second : 0;
            const std::int64_t delta =
                (intent.side == internal::Side::kBuy || intent.side == internal::Side::kBid)
                    ? static_cast<std::int64_t>(intent.qty_lots)
                    : -static_cast<std::int64_t>(intent.qty_lots);
            const std::int64_t projected = current_net + delta;
            if (projected > limits_.max_net_position_lots_per_market ||
                projected < -limits_.max_net_position_lots_per_market) {
                return RiskDecision{
                    .code = RiskDecisionCode::kRejected,
                    .reason = RiskRejectReason::kNetPositionLimit,
                };
            }
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
