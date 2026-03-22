#pragma once

#include "trading/internal/normalized_event.hpp"
#include "trading/parsers/parse_result.hpp"
#include "trading/router/shard_dispatch.hpp"

namespace trading::shards {

class IExchangeMessageParser {
  public:
    virtual ~IExchangeMessageParser() = default;
    [[nodiscard]] virtual parsers::ParseResult<internal::NormalizedEvent>
    parse(const router::RoutedEvent& routed_event) const = 0;
};

class RoutedEventParser final : public IExchangeMessageParser {
  public:
    [[nodiscard]] parsers::ParseResult<internal::NormalizedEvent>
    parse(const router::RoutedEvent& routed_event) const override;
};

} // namespace trading::shards
