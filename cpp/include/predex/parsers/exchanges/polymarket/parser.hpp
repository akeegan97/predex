#pragma once

#include "trading/internal/normalized_event.hpp"
#include "trading/internal/router_frame.hpp"
#include "trading/parsers/parse_result.hpp"

namespace trading::parsers::exchanges::polymarket {

class Parser final {
  public:
    static constexpr auto kExchangeId = internal::ExchangeId::kPolymarket;

    [[nodiscard]] ParseResult<internal::NormalizedEvent> parse(
        const internal::RouterFrame& frame) const;

  private:
    bool strict_field_validation_{true};
};

} // namespace trading::parsers::exchanges::polymarket
