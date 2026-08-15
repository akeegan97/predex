#pragma once 
#include <vector>

#include "predex/shard/event.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
namespace predex::shard{


    class EventStore{
        public:
            [[nodiscard]] bool initialize(std::vector<KalshiEvent> events);

            [[nodiscard]] EventApplyResult apply(const ingest::kalshi::FrameHandle& handle, const KalshiParsedEvent& parsed_event) noexcept;

            [[nodiscard]] Event* get_event(std::uint32_t shard_event_index) noexcept;
            [[nodiscard]] const Event* get_event(std::uint32_t shard_event_index) const noexcept;

            [[nodiscard]] std::size_t size() const noexcept;

            [[nodiscard]] BookInvalidationResult invalidate_market(
                std::uint32_t shard_event_index,
                std::uint32_t event_market_index,
                MarketId expected_market_id,
                predex::ingest::kalshi::BookInvalidationReason reason) noexcept;

            [[nodiscard]] BookInvalidationSummary invalidate_all_markets(predex::ingest::kalshi::BookInvalidationReason reason) noexcept;
        private:
            std::vector<Event> events_;

    };
}