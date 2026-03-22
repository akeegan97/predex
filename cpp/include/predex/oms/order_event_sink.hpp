#pragma once

#include "trading/internal/oms_types.hpp"

namespace trading::oms {

class IOrderEventSink {
  public:
    virtual ~IOrderEventSink() = default;
    [[nodiscard]] virtual bool on_order_update(const internal::OrderStateUpdate& update) = 0;
};

} // namespace trading::oms
