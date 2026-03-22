#pragma once

#include <concepts>

#include "trading/internal/normalized_event.hpp"
#include "trading/internal/router_frame.hpp"
#include "trading/parsers/parse_result.hpp"

namespace trading::parsers {

template <typename T>
concept ExchangeParser = requires(const T& parser, const internal::RouterFrame& frame) {
    { T::kExchangeId } -> std::convertible_to<internal::ExchangeId>;
    { parser.parse(frame) } -> std::same_as<ParseResult<internal::NormalizedEvent>>;
};

} // namespace trading::parsers
