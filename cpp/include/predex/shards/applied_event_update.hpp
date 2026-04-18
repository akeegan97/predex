#pragma once

#include "predex/internal/normalized_event.hpp"
#include "predex/shards/event_store.hpp"

namespace predex::core::shards::kalshi {

struct AppliedEventUpdate {
    const internal::NormalizedEvent& update;
    const Event& event;
};

} // namespace predex::core::shards::kalshi
