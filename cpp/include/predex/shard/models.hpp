#pragma once 
#include <cstdint>
#include <array>
#include <variant>
#include <vector>
#include "predex/control/control_types.hpp"

namespace predex::shard{
    
    constexpr std::uint64_t kMAX_TICKS = 1000; // kalshi prices are in fp with 4 decimals, converted to int 
    constexpr std::uint64_t kQTY_SCALE = 100; // kalshi quantities are in fp with 2 decimals, converted to int

    using MarketId = predex::core::control::MarketId;
    using EventId = predex::core::control::EventId;

    using EventTopology = predex::core::control::EventTopology;
    
    using PriceTicks = std::uint64_t;
    using QtyLots = std::uint64_t;

    using DeltaQtyLots = std::int64_t;

    enum class Side : std::uint8_t{
        kUNKNOWN = 0,
        kBID = 1,
        kASK = 2
    };

    struct KalshiBook{
        std::array<QtyLots, kMAX_TICKS + 1> bids{};
        std::array<QtyLots, kMAX_TICKS + 1> asks{};

        bool has_snapshot{false};
        bool desynced{false};
    };

    struct KalshiMarket{
        MarketId market_id{};
        std::uint32_t event_market_index{};
        bool tradeable{false};
        KalshiBook book{};
    };

    struct SingleMarketState{};
    struct MutuallyExclusiveState{};
    struct MonotonicChainState{};
    struct UnorderedGroupState{};

    using EventDerivedState = std::variant<std::monostate, SingleMarketState, MutuallyExclusiveState, MonotonicChainState, UnorderedGroupState>;

    struct KalshiEvent{
        EventId event_id{};
        EventTopology topology{EventTopology::kUNKNOWN};
        std::uint32_t shard_event_index{};
        std::vector<KalshiMarket> markets;
        EventDerivedState derived_state;
        bool desynced{false};
    };

    //Normalized Parsed Market Data Events

    struct Level{
        PriceTicks price_ticks{0};
        QtyLots qty_lots{0};
    };

    struct KalshiSnapshotEvent{
        std::vector<Level> bids;
        std::vector<Level> asks;
    };

    struct KalshiDeltaData{
        Side side{Side::kUNKNOWN};
        PriceTicks price_ticks{0};
        QtyLots delta_qty_lots{0};
    };

    struct KalshiTradeData{
        PriceTicks price_ticks{0};
        DeltaQtyLots qty_lots{0};
        Side aggressor{Side::kUNKNOWN};
        Side book_side{Side::kUNKNOWN};
    };

    struct KalshiLifecycleData{};

    using KalshiEventData = std::variant<std::monostate, KalshiSnapshotEvent, KalshiDeltaData, KalshiTradeData, KalshiLifecycleData>;

    enum class KalshiEventType : std::uint8_t{
        kUNKNOWN = 0,
        kSNAPSHOT = 1,
        kDELTA = 2,
        kTRADE = 3,
        kLIFECYCLE = 4
    };
    struct KalshiParsedEvent{
        KalshiEventType type{KalshiEventType::kUNKNOWN};
        KalshiEventData data;
    };

}