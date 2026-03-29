#pragma once

#include "predex/internal/normalized_event.hpp"
namespace predex::core::kalshi::shard {

class IShardEventHandler {
  public:
    virtual ~IShardEventHandler() = default;

    [[nodiscard]] virtual bool on_event(const internal::NormalizedEvent& event) = 0;
};

} // namespace predex::core::kalshi::shard