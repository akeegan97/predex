#pragma once

#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi::strategies {

struct CdfViolationConfig {
    std::int64_t min_violation_ticks{1};
    internal::QtyLots default_order_qty_lots{1};
    bool enabled{true};
};

class CdfViolationStrategy {
  public:
    explicit CdfViolationStrategy(CdfViolationConfig config = {}) : config_(config) {}

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
    CdfViolationConfig config_{};
};

}  // namespace predex::core::shards::kalshi::strategies
