

#include "predex/shard/event_store.hpp"
#include "predex/shard/event.hpp"

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
            return EventApplyResult{
                .disposition = ApplyDisposition::kREJECTED,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = MarketApplyReason::kINVALID_MARKET_INDEX
            };
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

    BookInvalidationResult EventStore::invalidate_market(
        //NOLINTNEXTLINE -- easily swapped parameter suppressed
        std::uint32_t shard_event_index,
        std::uint32_t event_market_index,
        MarketId expected_market_id,
        predex::ingest::kalshi::BookInvalidationReason reason) noexcept{
        if(shard_event_index >= events_.size()){
            return BookInvalidationResult{
                .target_found = false,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = reason,
                .reject_reason = InvalidationRejectReason::kINVALID_EVENT_INDEX
            };
        }
        Event& event = events_[shard_event_index];
        const KalshiMarket* market = event.get_market(event_market_index);
        if(market == nullptr){
            return BookInvalidationResult{
                .target_found = false,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = reason,
                .reject_reason = InvalidationRejectReason::kINVALID_MARKET_INDEX
            };
        }
        if(market->market_id != expected_market_id){
            return BookInvalidationResult{
                .target_found = false,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = reason,
                .reject_reason = InvalidationRejectReason::kMARKET_ID_MISMATCH
            };
        }
        return event.invalidate_market(event_market_index, reason);
    }
    BookInvalidationSummary EventStore::invalidate_all_markets(predex::ingest::kalshi::BookInvalidationReason reason) noexcept{
        BookInvalidationSummary summary{};
        for(std::size_t idx = 0; idx < events_.size(); ++idx){ //NOLINT -- suppressing range-based loop
            const auto result = events_[idx].invalidate_all_markets(reason);
            summary.targets_found += result.targets_found;
            summary.targets_became_unusable += result.targets_became_unusable;
            summary.targets_recovery_required += result.targets_recovery_required;
            summary.targets_already_awaiting_recovery += result.targets_already_awaiting_recovery;
        }
        return summary;
    }


}
