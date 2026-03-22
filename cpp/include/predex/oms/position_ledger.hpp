#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

#include "trading/oms/order_event_sink.hpp"
#include "trading/oms/portfolio_risk_snapshot_provider.hpp"
#include "trading/oms/position_ledger_core.hpp"

namespace trading::oms {

class PositionLedger final : public IOrderEventSink, public IPortfolioRiskSnapshotProvider {
  public:
    bool on_order_update(const internal::OrderStateUpdate& update) override;
    [[nodiscard]] PortfolioRiskSnapshot snapshot_for(internal::ExchangeId exchange,
                                                     std::string_view market_ticker) const override;

    [[nodiscard]] std::optional<PositionSnapshot>
    market_position(internal::ExchangeId exchange, std::string_view market_ticker) const;
    [[nodiscard]] PositionLedgerStats stats() const;
    [[nodiscard]] std::string last_error() const;

  private:
    mutable std::mutex mutex_;
    PositionLedgerCore core_;
    std::string last_error_;
};

} // namespace trading::oms
