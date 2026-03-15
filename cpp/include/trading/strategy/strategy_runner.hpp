#pragma once

#include <cstdint>
#include <string>

#include "trading/metrics/latency_histogram.hpp"
#include "trading/strategy/order_intent_sink.hpp"
#include "trading/strategy/shard_risk_gate.hpp"
#include "trading/strategy/shard_risk_snapshot_provider.hpp"
#include "trading/strategy/strategy.hpp"

namespace trading::strategy {

struct StrategyRunnerConfig {
    ShardRiskConfig shard_risk{};
    const IShardRiskSnapshotProvider* risk_snapshot_provider{nullptr};
};

struct StrategyRunnerStats {
    std::uint64_t events_processed_count{0};
    std::uint64_t intents_emitted_count{0};
    std::uint64_t intents_submitted_count{0};
    std::uint64_t risk_reject_count{0};
    std::uint64_t sink_reject_count{0};
    std::uint64_t strategy_error_count{0};
    metrics::LatencyPercentiles event_to_submit_latency{};
};

class StrategyRunner final {
  public:
    StrategyRunner(IStrategy& strategy, IOrderIntentSink& intent_sink, StrategyRunnerConfig config = {});

    [[nodiscard]] bool on_event(const internal::NormalizedEvent& event);
    [[nodiscard]] StrategyRunnerStats stats() const;
    [[nodiscard]] std::string last_error() const;

  private:
    void set_error(std::string error_message);

    IStrategy& strategy_;
    IOrderIntentSink& intent_sink_;
    StrategyRunnerConfig config_;
    ShardRiskGate shard_risk_gate_;
    metrics::AtomicLatencyHistogram event_to_submit_latency_hist_;
    StrategyRunnerStats stats_{};
    std::string last_error_;
};

} // namespace trading::strategy
