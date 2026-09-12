#pragma once 

#include <cstdint>
#include <array>
#include <cstddef>
#include <variant>

namespace predex::strategy{

    using MarketId = std::uint32_t;
    using EventId = std::uint32_t;
    using PriceTicks = std::uint64_t;
    using QtyLots = std::uint64_t;
    
    inline constexpr PriceTicks kPriceTicksPerDollar = 10'000;
    inline constexpr PriceTicks kPriceTicksPerCent = 100;
    inline constexpr QtyLots kQuantityLotsPerContract = 100;

    inline constexpr std::size_t kStrategyBookDepth = 4;


    struct StrategyObservationContext {
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
        std::uint64_t ingress_timestamp_ns{};
        std::uint64_t book_apply_timestamp_ns{};
        std::uint64_t publish_timestamp_ns{};
    };
    
    struct StrategyBookLevel{
        PriceTicks price_ticks{};
        QtyLots quantity_lots{};
    };

    struct StrategyMarketView{
        MarketId market_id{};
        std::uint32_t event_market_index{};
        std::int64_t strike_key{};
        bool tradeable{false};

        std::uint64_t market_time_s{};
        std::uint64_t market_close_time_s{};
        std::uint64_t market_expected_expiration_time_s{};
        std::uint64_t market_expiration_time_s{};

        std::array<StrategyBookLevel, kStrategyBookDepth> bids{};
        std::array<StrategyBookLevel, kStrategyBookDepth> asks{};

        std::uint8_t bid_count{};
        std::uint8_t ask_count{};

    };

    struct MonotonicPairObservation{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
        EventId event_id{};
        std::uint64_t event_revision{};

        std::uint64_t ingress_timestamp_ns{};
        std::uint64_t book_apply_timestamp_ns{};
        std::uint64_t publish_timestamp_ns{};

        StrategyMarketView easier;
        StrategyMarketView harder;
    };

    struct StrategyEventUnavailable{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
        EventId event_id{};
        MarketId affected_market_id{};
        std::uint64_t event_revision{};
    };

    struct StrategyShardUnavailable{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
    };


    using ShardToStrategyMessage = std::variant<
        MonotonicPairObservation,
        StrategyEventUnavailable,
        StrategyShardUnavailable
    >;


}