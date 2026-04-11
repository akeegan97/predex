#pragma once

#include <vector>

#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi::strategies {

struct MarketMakingConfig {
    internal::QtyLots default_quote_qty_lots{1};
    std::int64_t min_spread_ticks{1};
    bool enabled{true};
};

class MarketMakingStrategy {
  public:
    explicit MarketMakingStrategy(MarketMakingConfig config = {}) : config_(config) {}

    void on_event(const AppliedEventUpdate& update,
                  std::vector<Signal>& out_signals) noexcept {
        if (!config_.enabled || update.event.topology_kind == internal::EventTopologyKind::kUnknown) {
            return;
        }
        static_cast<void>(out_signals);
    }

  private:
    MarketMakingConfig config_{};
};

}  // namespace predex::core::shards::kalshi::strategies
