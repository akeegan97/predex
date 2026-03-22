#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "trading/internal/oms_types.hpp"
#include "trading/oms/portfolio_risk_gate.hpp"

namespace trading::oms {

struct PositionSnapshot {
    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    std::string market_ticker;
    internal::QtyLots net_qty_lots{0};
    std::optional<internal::PriceTicks> avg_open_price_ticks;
    internal::QtyLots bought_qty_lots{0};
    internal::QtyLots sold_qty_lots{0};
    internal::QtyLots closed_qty_lots{0};
    std::int64_t realized_pnl_ticks{0};
    std::uint64_t fill_count{0};
    internal::TimestampNs last_fill_ts_ns{0};
};

struct PositionLedgerStats {
    std::uint64_t processed_fill_count{0};
    std::uint64_t ignored_update_count{0};
    std::uint64_t rejected_fill_count{0};
    std::size_t tracked_market_count{0};
    std::size_t open_position_count{0};
    std::size_t closed_position_count{0};
    std::int64_t realized_pnl_ticks_total{0};
};

class PositionLedgerCore final {
  public:
    [[nodiscard]] bool on_order_update(const internal::OrderStateUpdate& update,
                                       std::string& error_message);
    [[nodiscard]] std::optional<PositionSnapshot>
    market_position(internal::ExchangeId exchange, std::string_view market_ticker) const;
    [[nodiscard]] PortfolioRiskSnapshot
    portfolio_risk_snapshot(internal::ExchangeId exchange, std::string_view market_ticker) const;
    [[nodiscard]] PositionLedgerStats stats() const;

  private:
    struct MarketKey {
        internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
        std::string market_ticker;

        [[nodiscard]] bool operator==(const MarketKey& other) const {
            return exchange == other.exchange && market_ticker == other.market_ticker;
        }
    };

    struct MarketKeyHash {
        [[nodiscard]] std::size_t operator()(const MarketKey& key) const {
            const std::size_t exchange_hash =
                std::hash<std::uint8_t>{}(static_cast<std::uint8_t>(key.exchange));
            const std::size_t ticker_hash = std::hash<std::string>{}(key.market_ticker);
            return exchange_hash ^ (ticker_hash << 1U);
        }
    };

    struct MarketPositionState {
        internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
        std::string market_ticker;
        internal::QtyLots net_qty_lots{0};
        internal::PriceTicks avg_open_price_ticks{0};
        bool has_open_price{false};
        internal::QtyLots bought_qty_lots{0};
        internal::QtyLots sold_qty_lots{0};
        internal::QtyLots closed_qty_lots{0};
        std::int64_t realized_pnl_ticks{0};
        std::uint64_t fill_count{0};
        internal::TimestampNs last_fill_ts_ns{0};
    };

    [[nodiscard]] bool apply_fill(const internal::OrderStateUpdate& update,
                                  const internal::OrderFill& fill, std::string& error_message);
    [[nodiscard]] static internal::QtyLots abs_qty(internal::QtyLots value);
    [[nodiscard]] static internal::QtyLots side_to_signed_qty(internal::Side side,
                                                              internal::QtyLots qty_lots);

    std::unordered_map<MarketKey, MarketPositionState, MarketKeyHash> positions_;
    std::uint64_t processed_fill_count_{0};
    std::uint64_t ignored_update_count_{0};
    std::uint64_t rejected_fill_count_{0};
    std::size_t closed_position_count_{0};
    std::int64_t realized_pnl_ticks_total_{0};
};

} // namespace trading::oms
