#pragma once

#include <string>

#include "trading/shards/event_handler.hpp"
#include "trading/strategy/strategy_runner.hpp"

namespace trading::strategy {

class StrategyEventHandler final : public shards::IShardEventHandler {
  public:
    StrategyEventHandler(IStrategy& strategy, IOrderIntentSink& intent_sink,
                         StrategyRunnerConfig config = {});

    [[nodiscard]] bool on_event(const internal::NormalizedEvent& event) override;
    [[nodiscard]] StrategyRunnerStats stats() const;
    [[nodiscard]] std::string last_error() const;

  private:
    StrategyRunner runner_;
};

} // namespace trading::strategy
