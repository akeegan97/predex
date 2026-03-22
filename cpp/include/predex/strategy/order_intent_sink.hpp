#pragma once

#include <string_view>

#include "trading/internal/oms_types.hpp"

namespace trading::strategy {

class IOrderIntentSink {
  public:
    virtual ~IOrderIntentSink() = default;

    [[nodiscard]] virtual bool submit_intent(internal::OrderIntent intent) = 0;
    [[nodiscard]] virtual std::string_view last_error() const = 0;
};

} // namespace trading::strategy
