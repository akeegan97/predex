#pragma once 
#include <cstdint>
#include <variant>

#include "predex/ingest/kalshi/market_data/integrity_messages.hpp"

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
        std::uint64_t market_barriers_received{0};
        std::uint64_t market_barriers_delivered{0};
        std::uint64_t subscription_barriers_received{0};
        std::uint64_t subscription_barriers_delivered{0};
        std::uint64_t barriers_deferred{0};
        std::uint64_t subscription_recovery_facts_deferred{0};
        std::uint64_t shard_queue_depth_high_water{0};
        predex::core::control::MarketDataChannelTelemetry channel_stats{
            predex::core::control::make_market_data_channel_telemetry()};
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

    struct OrderBookSubscriptionBarrierDelivered{
        std::uint64_t universe_version{};
        predex::ingest::kalshi::IntegrityIncidentKey incident{};
        std::uint32_t sid{};
        std::uint64_t expected_sequence{};
        std::uint64_t observed_sequence{};
        predex::ingest::kalshi::BookInvalidationReason reason{
            predex::ingest::kalshi::BookInvalidationReason::kNONE};
    };

    using RouterToControl = std::variant<
        ShardBackpressure,
        RouterTelemetry,
        OutOfSequenceFrame,
        RouterHandleLeak,
        OrderBookSubscriptionBarrierDelivered>;

}
