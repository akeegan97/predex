#pragma once

#include "predex/shards/applied_event_update.hpp"


namespace predex::core::shards::kalshi::strategies {

struct MarketMakingConfig {
    internal::QtyLots default_quote_qty_lots{internal::kOneContractQtyLots};
    std::int64_t min_spread_ticks{1};
    bool enabled{true};
};

class MarketMakingStrategy {
  public:
    explicit MarketMakingStrategy(MarketMakingConfig config = {}) : config_(config) {}

    template <typename SignalSink>
    void on_event(const AppliedEventUpdate& update, SignalSink& out_signals) noexcept {
        if (!config_.enabled ||
            update.event.topology_kind == internal::EventTopologyKind::kUnknown) {
            return;
        }
        static_cast<void>(out_signals);
    }

  private:
    MarketMakingConfig config_{};
};

} // namespace predex::core::shards::kalshi::strategies
