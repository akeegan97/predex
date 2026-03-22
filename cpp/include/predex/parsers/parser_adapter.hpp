#pragma once

#include <utility>

#include "trading/parsers/parser_concept.hpp"

namespace trading::parsers {

template <ExchangeParser ParserT>
class ParserAdapter {
  public:
    ParserAdapter() = default;
    explicit ParserAdapter(ParserT parser) : parser_(std::move(parser)) {}

    [[nodiscard]] ParseResult<internal::NormalizedEvent> parse(
        const internal::RouterFrame& frame) const {
        return parser_.parse(frame);
    }

    [[nodiscard]] static constexpr internal::ExchangeId exchange_id() {
        return ParserT::kExchangeId;
    }

  private:
    ParserT parser_{};
};

} // namespace trading::parsers
