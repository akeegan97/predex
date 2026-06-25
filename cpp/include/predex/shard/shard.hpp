#pragma once 

#include <cstdint>
#include <vector>

#include "predex/shard/models.hpp"
#include "predex/shard/market_parser.hpp"
#include "predex/shard/event_store.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"

namespace predex::shard{

    struct ShardQueues{
        predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& router_to_shard_queue;
        predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& shard_to_logger_queue;
        predex::utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>& last_resort_recycle_queue;
    };

    enum class ShardPumpCode : std::uint8_t{
        kIDLE = 0,
        kAPPLIED = 1,
        kPARSE_REJECTED = 2,
        kEVENT_REJECTED = 3,
        kEVENT_DESYNCED = 4,
        kMISSING_FRAME = 5,
        kLOGGER_BACKPRESSURE = 6,
        kHANDLE_LEAK = 7,
    };

    struct ShardPumpResult{
        ShardPumpCode code{ShardPumpCode::kIDLE};
        ParseResult parse_result{};
        EventApplyResult event_result{};
    };

    struct ShardStats{
        std::uint64_t frames_seen{0};
        std::uint64_t frames_applied{0};
        std::uint64_t parse_rejects{0};
        std::uint64_t event_rejects{0};
        std::uint64_t event_desyncs{0};
        std::uint64_t frames_to_logger{0};
        std::uint64_t frames_recycled{0};
        std::uint64_t leaked_handles{0};
        std::uint64_t missed_frames_to_logger{0};
    };

    class Shard{
        public:
            Shard(
                std::uint32_t shard_index,
                ShardQueues queues,
                predex::ingest::kalshi::FramePool& frame_pool
            );

            [[nodiscard]] bool initialize(std::vector<KalshiEvent> events);

            [[nodiscard]] ShardPumpResult pump_once() noexcept;
            [[nodiscard]] std::uint32_t shard_index() const noexcept;
            [[nodiscard]] const ShardStats& stats() const noexcept;

        private:
            [[nodiscard]] bool terminal_handoff(const predex::ingest::kalshi::FrameHandle& handle) noexcept;

            std::uint32_t shard_index_{0};
            ShardQueues queues_;
            predex::ingest::kalshi::FramePool& frame_pool_;

            EventStore event_store_;
            MarketParser market_parser_;
            ShardStats stats_;
    };

}
