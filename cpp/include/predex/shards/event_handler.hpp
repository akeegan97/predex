#pragma once

#include "trading/internal/normalized_event.hpp"

namespace trading::shards {

class IShardEventHandler {
  public:
    virtual ~IShardEventHandler() = default;

    [[nodiscard]] virtual bool on_event(const internal::NormalizedEvent& event) = 0;
};

} // namespace trading::shards
