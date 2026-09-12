#pragma once 
#include <cstdint>
#include "predex/control/control_types.hpp"

namespace predex::ingest::kalshi{
    enum class BookInvalidationReason : std::uint8_t {
        kNONE,
        kEXCHANGE_SEQUENCE_GAP,
        kWIRE_POOL_EXHAUSTION,
        kWIRE_TO_ROUTER_DELIVERY_LOSS,
        kROUTER_TO_SHARD_DELIVERY_LOSS,
        kSHARD_FRAME_MISSING,
        kSHARD_PARSE_FAILURE,
    };

    enum class IntegrityIncidentOrigin : std::uint8_t{
        kUNKNOWN = 0,
        kWIRE_SESSION = 1,
        kROUTER = 2,
        kSHARD = 3,
    };

    struct IntegrityIncidentKey{
        IntegrityIncidentOrigin origin{IntegrityIncidentOrigin::kUNKNOWN};
        std::uint32_t producer_index{0};
        std::uint64_t incident_id{0};

        friend bool operator==(const IntegrityIncidentKey&, const IntegrityIncidentKey&) = default;
    };

    struct MarketInvalidationBarrier{
        std::uint64_t universe_version{};
        IntegrityIncidentKey incident{};

        std::uint32_t sid{};
        std::uint64_t sequence{};

        core::control::MarketId market_id{};
        core::control::EventId event_id{};

        std::uint32_t shard_index{};
        std::uint32_t shard_event_index{};
        std::uint32_t event_market_index{};

        BookInvalidationReason reason{BookInvalidationReason::kNONE};
    }; // used to notify shard that a particular market has been invalidated due to framepool exhaustion or router->shard SPSC queue fail 

    struct OrderBookSubscriptionInvalidationBarrier{
        std::uint64_t universe_version{};
        IntegrityIncidentKey incident{};

        std::uint32_t sid{};

        std::uint64_t expected_sequence{};
        std::uint64_t observed_sequence{};

        BookInvalidationReason reason{BookInvalidationReason::kNONE};
    };  // general notification to all shards that the order book subscription has been invalidated (globally missing SEQ where MarketID is unknown)
   


}