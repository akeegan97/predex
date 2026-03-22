#pragma once

#include <vector>

#include "trading/internal/normalized_event.hpp"
#include "trading/internal/oms_types.hpp"

namespace trading::strategy {

struct StrategyDecision {
    std::vector<internal::OrderIntent> intents;
};

class IStrategy {
  public:
    virtual ~IStrategy() = default;

    [[nodiscard]] virtual StrategyDecision on_event(const internal::NormalizedEvent& event) = 0;
};

} // namespace trading::strategy
