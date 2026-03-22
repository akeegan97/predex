#pragma once

#include <string>
#include <string_view>

#include "trading/internal/oms_types.hpp"
#include "trading/oms/parse_result.hpp"

namespace trading::oms {

class IExchangeOmsAdapter {
  public:
    virtual ~IExchangeOmsAdapter() = default;

    [[nodiscard]] virtual internal::ExchangeId exchange_id() const = 0;
    [[nodiscard]] virtual std::string name() const = 0;
    [[nodiscard]] virtual bool supports(const internal::OrderIntent& intent) const = 0;

    [[nodiscard]] virtual std::string build_request(const internal::OrderIntent& intent) const = 0;
    [[nodiscard]] virtual ParseResult<internal::OrderStateUpdate>
    parse_update(std::string_view payload) const = 0;
};

} // namespace trading::oms
