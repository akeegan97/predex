#pragma once

#include <string_view>

#include "trading/internal/market_types.hpp"
#include "trading/strategy/shard_risk_gate.hpp"

namespace trading::strategy {

class IShardRiskSnapshotProvider {
  public:
    virtual ~IShardRiskSnapshotProvider() = default;

    [[nodiscard]] virtual ShardRiskSnapshot snapshot_for(internal::ExchangeId exchange,
                                                         std::string_view market_ticker) const = 0;
};

} // namespace trading::strategy
