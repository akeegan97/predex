#pragma once

#include "trading/strategy/strategy.hpp"

namespace trading::strategy {

class NoopStrategy final : public IStrategy {
  public:
    [[nodiscard]] StrategyDecision on_event(const internal::NormalizedEvent& event) override {
        (void)event;
        return StrategyDecision{};
    }
};

} // namespace trading::strategy
