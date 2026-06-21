#pragma once 
#include <cstdint>
#include <variant>

namespace predex::router{
    
    using AffinityKey = std::uint64_t;
    using MarketId = std::uint32_t;
    using EventId = std::uint32_t;
    
    struct ShardBackpressure{
        std::uint64_t shard_index;
        AffinityKey affinity_key;
        MarketId market_id;
        EventId event_id;
    };

    struct OutOfSequenceFrame{
        std::uint64_t sid;
        std::uint64_t sequence;
        MarketId market_id;
        EventId event_id;
        std::uint64_t shard_index;
    };

    struct RouterTelemetry{
        std::uint64_t total_frames_seen{0};
        std::uint64_t frames_to_shards{0};
        std::uint64_t frames_to_logger{0};
        std::uint64_t frames_recycled{0};
    };

    struct RouterHandleLeak{
        std::uint64_t universe_version{0};
        std::uint64_t sid{0};
        std::uint64_t sequence{0};
        std::uint32_t pool_index{0};
        std::uint32_t pool_generation{0};
        std::uint64_t shard_index{0};
        MarketId market_id{0};
        EventId event_id{0};
    };

    using RouterToControl = std::variant<ShardBackpressure, RouterTelemetry, OutOfSequenceFrame, RouterHandleLeak>;

}
