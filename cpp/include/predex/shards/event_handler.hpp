#pragma once

#include "predex/internal/normalized_event.hpp"
#include "predex/shards/event_store.hpp"

namespace predex::core::shards::kalshi {

enum class HandlerDecisionCode: std::uint8_t{
    kAccepted=1,
    kDeclined=2,
    kError=3,
};

struct AppliedEventUpdate{
  const internal::NormalizedEvent& update;
  const Event& event;
};

class IShardEventHandler {
  public:
    virtual ~IShardEventHandler() = default;

    [[nodiscard]] virtual HandlerDecisionCode on_event(const AppliedEventUpdate& update) = 0;
};

} // namespace predex::core::shards::kalshi
