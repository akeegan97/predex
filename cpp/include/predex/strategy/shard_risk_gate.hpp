#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "trading/internal/oms_types.hpp"

namespace trading::strategy {

struct ShardRiskConfig {
    std::size_t max_open_orders_per_market{0};
    internal::QtyLots max_working_qty_per_market{0};
    internal::QtyLots max_abs_net_position_per_market{0};
};

struct ShardRiskSnapshot {
    std::size_t open_orders_market{0};
    internal::QtyLots working_qty_market{0};
    internal::QtyLots net_position_market{0};
};

enum class ShardRiskRejectCode : std::uint8_t {
    kNone = 0,
    kInvalidIntent = 1,
    kOpenOrdersPerMarketLimit = 2,
    kWorkingQtyPerMarketLimit = 3,
    kAbsNetPositionPerMarketLimit = 4,
};

struct ShardRiskDecision {
    bool allow{true};
    ShardRiskRejectCode code{ShardRiskRejectCode::kNone};
    std::string reason_message;

    [[nodiscard]] static ShardRiskDecision allow_decision();
    [[nodiscard]] static ShardRiskDecision reject(ShardRiskRejectCode code, std::string reason_message);
};

class ShardRiskGate final {
  public:
    explicit ShardRiskGate(ShardRiskConfig config = {});

    [[nodiscard]] const ShardRiskConfig& config() const;
    [[nodiscard]] ShardRiskDecision evaluate(const internal::OrderIntent& intent,
                                             const ShardRiskSnapshot& snapshot) const;
    [[nodiscard]] static std::string_view reject_code_name(ShardRiskRejectCode code);

  private:
    ShardRiskConfig config_;
};

} // namespace trading::strategy
