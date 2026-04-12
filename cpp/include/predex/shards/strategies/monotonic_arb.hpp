#pragma once

#include <cmath>

#include "predex/internal/market_types.hpp"
#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi::strategies {
inline constexpr double kPriceScale = 10000.0;
inline constexpr double kCentScale = 100.0;
inline constexpr internal::PriceTicks kTicksPerCent = 100;
inline constexpr double kTakerFeeRate = 0.07;
inline constexpr double kMakerFeeRate = 0.0175;
struct MonotonicArbConfig {
    std::int64_t min_edge_ticks{1};
    internal::QtyLots default_order_qty_lots{1};
    bool enabled{true};
};

class MonotonicArbStrategy {
  public:
    explicit MonotonicArbStrategy(MonotonicArbConfig config = {}) : config_(config) {}

    template <typename SignalSink>
    void on_event(const AppliedEventUpdate& update,
                  SignalSink& out_signals) noexcept {
        if (!config_.enabled ||
            update.event.topology_kind != internal::EventTopologyKind::kMonotonicChain) {
            return;
        }
        static_cast<void>(out_signals);
    }

  private:
    MonotonicArbConfig config_{};
    [[nodiscard]] static internal::PriceTicks fee_ticks_(
        double fee_rate,
        internal::PriceTicks price_ticks,
        internal::QtyLots qty) noexcept {
        if (qty <= 0 || price_ticks <= 0 ||
            price_ticks >= static_cast<internal::PriceTicks>(kPriceScale)) {
            return 0;
        }

        const double p_dollars = static_cast<double>(price_ticks) / kPriceScale;
        const double fee_dollars =
            fee_rate * static_cast<double>(qty) * p_dollars * (1.0 - p_dollars);
        const double fee_cents = std::ceil(fee_dollars * kCentScale);
        return static_cast<internal::PriceTicks>(fee_cents) * kTicksPerCent;
    }

    [[nodiscard]] static internal::PriceTicks taker_fee_ticks_(
        internal::PriceTicks price_ticks,
        internal::QtyLots qty) noexcept {
        return fee_ticks_(kTakerFeeRate, price_ticks, qty);
    }

    [[nodiscard]] static internal::PriceTicks maker_fee_ticks_(
        internal::PriceTicks price_ticks,
        internal::QtyLots qty) noexcept {
        return fee_ticks_(kMakerFeeRate, price_ticks, qty);
    }
};

}  // namespace predex::core::shards::kalshi::strategies
