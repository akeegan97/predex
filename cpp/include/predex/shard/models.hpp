#pragma once 
#include <cstdint>
#include <variant>
#include <vector>
#include <optional>
#include <limits>
#include "predex/control/control_types.hpp"

namespace predex::shard{
    
    constexpr std::uint64_t kTICKSCALE = 10000; // kalshi prices are in fp with 4 decimals, converted to int 
    constexpr std::uint64_t kQTY_SCALE = 100; // kalshi quantities are in fp with 2 decimals, converted to int

    constexpr std::uint64_t kTICK_TO_LINEAR_CENTS_DIVISOR = 100;
    constexpr std::uint64_t kTICK_TO_TAPERED_DECI_CENTS_DIVISOR = 10;
    constexpr std::uint64_t kTICK_TO_DECI_CENTS_DIVISOR = 10;

    constexpr std::uint64_t kLEVEL_COUNT_LINEAR_CENTS = 101;
    constexpr std::uint64_t kLEVEL_COUNT_TAPERED_DECI_CENTS = 1001;
    constexpr std::uint64_t kLEVEL_COUNT_DECI_CENTS = 1001;

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

    enum class AggressorSide : std::uint8_t{
        kUNKNOWN = 0,
        kBUY = 1,
        kSELL = 2
    };

    enum class MarketScale : std::uint8_t{
        kUNKNOWN = 0,
        kLINEAR_CENTS = 1,
        kTAPERED_DECI_CENTS = 2,
        kDECI_CENTS = 3
    };
    
    static constexpr std::uint16_t kInvalidBookIndex = std::numeric_limits<std::uint16_t>::max();
    
    struct KalshiBook{
        /*
            Since Kalshi markets can be either linear cents, tapered_deci_cents, or deci_cents. 
            in interest of saving memory store bids/asks in vectors initialized to the entire scale but trimmed to the actual scale a particular market 
            uses, and internal ticks -> index done by helper function. 

            for now tapered & deci are treated the same per scale/vector size. 
        */
        std::vector<std::uint16_t> index_by_tick;

        std::uint16_t invalid_index{kInvalidBookIndex};
        std::vector<QtyLots> bids; 
        std::vector<QtyLots> asks;

        bool has_snapshot{false};
        bool desynced{false};

        MarketScale scale{MarketScale::kUNKNOWN};

        [[nodiscard]] std::optional<std::size_t> get_index(PriceTicks price_ticks) const noexcept {
            if (price_ticks >= index_by_tick.size()) {
                return std::nullopt;
            }

            const auto index = index_by_tick[price_ticks];
            if (index == invalid_index) {
                return std::nullopt;
            }

            return index;
        }
        void set_index_grid() {
            index_by_tick.assign(kTICKSCALE + 1, invalid_index);

            std::uint64_t divisor = 0;
            std::size_t level_count = 0;

            switch (scale) {
                case MarketScale::kLINEAR_CENTS:
                    divisor = kTICK_TO_LINEAR_CENTS_DIVISOR;
                    level_count = kLEVEL_COUNT_LINEAR_CENTS;
                    break;
                case MarketScale::kTAPERED_DECI_CENTS:
                    divisor = kTICK_TO_TAPERED_DECI_CENTS_DIVISOR;
                    level_count = kLEVEL_COUNT_TAPERED_DECI_CENTS;
                    break;
                case MarketScale::kDECI_CENTS:
                    divisor = kTICK_TO_DECI_CENTS_DIVISOR;
                    level_count = kLEVEL_COUNT_DECI_CENTS;
                    break;
                default:
                    bids.clear();
                    asks.clear();
                    return;
            }

            for (std::uint64_t tick = 0; tick <= kTICKSCALE; ++tick) {
                if (tick % divisor == 0) {
                    index_by_tick[tick] = static_cast<std::uint16_t>(tick / divisor);
                }
            }

            bids.assign(level_count, QtyLots{});
            asks.assign(level_count, QtyLots{});
        }
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
        DeltaQtyLots delta_qty_lots{0};
    };

    struct KalshiTradeData{
        PriceTicks price_ticks{0};
        QtyLots qty_lots{0};
        AggressorSide aggressor{AggressorSide::kUNKNOWN};
        Side book_side{Side::kUNKNOWN};
    };

    struct KalshiLifecycleData{};

    using KalshiParsedEvent = std::variant<std::monostate, KalshiSnapshotEvent, KalshiDeltaData, KalshiTradeData, KalshiLifecycleData>;

}