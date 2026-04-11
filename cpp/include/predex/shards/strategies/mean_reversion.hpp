#pragma once

#include <vector>

#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi::strategies {

struct MeanReversionConfig {
    std::int64_t min_reversion_score{1};
    internal::QtyLots default_order_qty_lots{1};
    bool enabled{true};
};

class MeanReversionStrategy {
  public:
    explicit MeanReversionStrategy(MeanReversionConfig config = {}) : config_(config) {}

    void on_event(const AppliedEventUpdate& update,
                  std::vector<Signal>& out_signals) noexcept {
        if (!config_.enabled || update.event.topology_kind == internal::EventTopologyKind::kUnknown) {
            return;
        }
        static_cast<void>(out_signals);
    }

  private:
    MeanReversionConfig config_{};
};

}  // namespace predex::core::shards::kalshi::strategies
