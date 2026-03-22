#pragma once

#include <string>
#include <string_view>

#include "trading/internal/oms_types.hpp"
#include "trading/oms/exchange_adapter.hpp"

namespace trading::adapters::exchanges::kalshi {

class OmsAdapter final : public oms::IExchangeOmsAdapter {
  public:
    [[nodiscard]] internal::ExchangeId exchange_id() const override;
    [[nodiscard]] std::string name() const override;
    [[nodiscard]] bool supports(const internal::OrderIntent& intent) const override;

    [[nodiscard]] std::string build_request(const internal::OrderIntent& intent) const override;
    [[nodiscard]] oms::ParseResult<internal::OrderStateUpdate>
    parse_update(std::string_view payload) const override;

  private:
    [[nodiscard]] static std::string build_place_request(const internal::OrderIntent& intent);
    [[nodiscard]] static std::string build_cancel_request(const internal::OrderIntent& intent);
    [[nodiscard]] static std::string build_replace_request(const internal::OrderIntent& intent);
};

} // namespace trading::adapters::exchanges::kalshi
