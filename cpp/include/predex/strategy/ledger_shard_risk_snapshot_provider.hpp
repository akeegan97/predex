#pragma once

#include <string_view>

#include "trading/oms/portfolio_risk_snapshot_provider.hpp"
#include "trading/strategy/shard_risk_snapshot_provider.hpp"

namespace trading::strategy {

class LedgerShardRiskSnapshotProvider final : public IShardRiskSnapshotProvider {
  public:
    explicit LedgerShardRiskSnapshotProvider(
        const oms::IPortfolioRiskSnapshotProvider& portfolio_snapshot_provider)
        : portfolio_snapshot_provider_(portfolio_snapshot_provider) {}

    [[nodiscard]] ShardRiskSnapshot
    snapshot_for(internal::ExchangeId exchange, std::string_view market_ticker) const override {
        const auto portfolio_snapshot =
            portfolio_snapshot_provider_.snapshot_for(exchange, market_ticker);
        return ShardRiskSnapshot{
            .open_orders_market = 0,
            .working_qty_market = 0,
            .net_position_market = portfolio_snapshot.net_position_market,
        };
    }

  private:
    const oms::IPortfolioRiskSnapshotProvider& portfolio_snapshot_provider_;
};

} // namespace trading::strategy
