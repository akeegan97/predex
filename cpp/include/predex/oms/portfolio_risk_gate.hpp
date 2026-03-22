#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "trading/internal/oms_types.hpp"

namespace trading::oms {

struct PortfolioRiskConfig {
    internal::QtyLots max_abs_net_position_per_market{0};
    internal::QtyLots max_abs_position_gross_global{0};
    std::int64_t min_realized_pnl_ticks{0};
    bool enforce_realized_pnl_floor{false};
};

struct PortfolioRiskSnapshot {
    internal::QtyLots net_position_market{0};
    internal::QtyLots gross_position_global{0};
    std::int64_t realized_pnl_ticks_total{0};
};

enum class PortfolioRiskRejectCode : std::uint8_t {
    kNone = 0,
    kInvalidIntent = 1,
    kAbsNetPositionMarketLimit = 2,
    kAbsPositionGrossGlobalLimit = 3,
    kRealizedPnlFloor = 4,
};

struct PortfolioRiskDecision {
    bool allow{true};
    PortfolioRiskRejectCode code{PortfolioRiskRejectCode::kNone};
    std::string reason_message;

    [[nodiscard]] static PortfolioRiskDecision allow_decision();
    [[nodiscard]] static PortfolioRiskDecision reject(PortfolioRiskRejectCode code,
                                                      std::string reason_message);
};

class PortfolioRiskGate final {
  public:
    explicit PortfolioRiskGate(PortfolioRiskConfig config = {});

    [[nodiscard]] const PortfolioRiskConfig& config() const;
    [[nodiscard]] PortfolioRiskDecision evaluate(const internal::OrderIntent& intent,
                                                 const PortfolioRiskSnapshot& snapshot) const;
    [[nodiscard]] static std::string_view reject_code_name(PortfolioRiskRejectCode code);

  private:
    PortfolioRiskConfig config_;
};

} // namespace trading::oms
