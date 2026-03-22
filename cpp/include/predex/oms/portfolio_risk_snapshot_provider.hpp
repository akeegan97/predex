#pragma once

#include <string_view>

//#include "trading/internal/oms_types.hpp"
#include "trading/oms/portfolio_risk_gate.hpp"

namespace trading::oms {

class IPortfolioRiskSnapshotProvider {
  public:
    virtual ~IPortfolioRiskSnapshotProvider() = default;

    [[nodiscard]] virtual PortfolioRiskSnapshot snapshot_for(internal::ExchangeId exchange,
                                                             std::string_view market_ticker) const = 0;
};

} // namespace trading::oms
