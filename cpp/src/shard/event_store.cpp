

#include "predex/shard/event_store.hpp"

namespace predex::shard{

    bool EventStore::initialize(std::vector<KalshiEvent> events){
        events_.clear();
        events_.reserve(events.size());
        for(auto& event : events){
            events_.emplace_back(std::move(event));
        }
        return true;
    }

    EventApplyResult EventStore::apply(const ingest::kalshi::FrameHandle& handle, const KalshiParsedEvent& parsed_event) noexcept{
        if(handle.shard_event_index >= events_.size()){
            return EventApplyResult{EventApplyCode::kREJECTED, EventDesyncReason::kINVALID_MARKET_INDEX};
        }
        Event& event = events_[handle.shard_event_index];
        EventApplyResult result = event.apply(handle.event_market_index, parsed_event);
        return result;
    }

    Event* EventStore::get_event(std::uint32_t shard_event_index) noexcept{
        if(shard_event_index >= events_.size()){
            return nullptr;
        }
        return &events_[shard_event_index];
    }

    const Event* EventStore::get_event(std::uint32_t shard_event_index) const noexcept{
        if(shard_event_index >= events_.size()){
            return nullptr;
        }
        return &events_[shard_event_index];
    }

    std::size_t EventStore::size() const noexcept{
        return events_.size();
    }

}
