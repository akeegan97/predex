#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "trading/internal/oms_types.hpp"

namespace trading::oms {

struct GlobalRiskConfig {
    std::size_t max_active_orders_global{0};
    std::size_t max_active_orders_per_market{0};
    internal::QtyLots max_outstanding_qty_global{0};
    internal::QtyLots max_outstanding_qty_per_market{0};
};

struct GlobalRiskSnapshot {
    std::size_t active_orders_global{0};
    std::size_t active_orders_market{0};
    internal::QtyLots outstanding_qty_global{0};
    internal::QtyLots outstanding_qty_market{0};
};

enum class GlobalRiskRejectCode : std::uint8_t {
    kNone = 0,
    kActiveOrdersGlobalLimit = 1,
    kActiveOrdersMarketLimit = 2,
    kOutstandingQtyGlobalLimit = 3,
    kOutstandingQtyMarketLimit = 4,
};

struct GlobalRiskDecision {
    bool allow{true};
    GlobalRiskRejectCode code{GlobalRiskRejectCode::kNone};
    std::string reason_message;

    [[nodiscard]] static GlobalRiskDecision allow_decision();
    [[nodiscard]] static GlobalRiskDecision reject(GlobalRiskRejectCode code,
                                                   std::string reason_message);
};

class GlobalRiskGate final {
  public:
    explicit GlobalRiskGate(GlobalRiskConfig config = {});

    [[nodiscard]] const GlobalRiskConfig& config() const;
    [[nodiscard]] GlobalRiskDecision evaluate(const internal::OrderIntent& intent,
                                              const GlobalRiskSnapshot& snapshot) const;
    [[nodiscard]] static std::string_view reject_code_name(GlobalRiskRejectCode code);

  private:
    GlobalRiskConfig config_;
};

} // namespace trading::oms
