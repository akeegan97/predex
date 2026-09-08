#pragma once 
#include <cstdint>
#include <optional>


#include "predex/strategy/strategy_types.hpp"


namespace predex::strategy {

enum class MonotonicArbRejectReason : std::uint8_t{
    kNONE,
    kMARKET_NOT_TRADEABLE,
    kMARKET_CLOSED,
    kINVALID_PAIR_ORDER,
    kMISSING_BID_DEPTH,
    kMISSING_ASK_DEPTH,
    kNO_GROSS_EDGE,
    kINSUFFICIENT_NET_EDGE,
    kDEPTH_DISCONTINUITY,
    kINSUFFICIENT_QUANTITY,
    kPRICE_AGGRESSION_EXCEEDED,
    kARITHMETIC_OVERFLOW,
    kINVALID_CONFIG,
    kINVALID_OBSERVATION,
    kFEES_ERASE_EDGE,
    kCOUNT,
};

struct MonotonicArbConfig {
    QtyLots order_quantity_lots{100}; //NOLINT
    std::uint64_t minimum_net_edge_ticks{200};//NOLINT
    std::uint64_t edge_cushion_ticks{0};

    bool require_top_gap_continuity{true};
    PriceTicks maximum_top_gap_ticks{200};//NOLINT

    bool require_near_top_multilevel_support{true};
    PriceTicks near_top_depth_window_ticks{200};//NOLINT
    std::uint8_t minimum_near_top_levels{2};

    bool bounded_easier_aggression_enabled{true};
    bool bounded_harder_aggression_enabled{true};
    PriceTicks maximum_easier_aggression_ticks{300};//NOLINT
    PriceTicks maximum_harder_aggression_ticks{300};//NOLINT

    std::uint8_t maximum_easier_book_levels{3};
    std::uint8_t maximum_harder_book_levels{3};

    bool require_full_easier_depth_for_quantity{true};
    bool require_full_harder_depth_for_quantity{true};

    std::uint32_t taker_fee_rate_numerator{7}; //NOLINT
    std::uint32_t taker_fee_rate_denominator{100}; //NOLINT
    PriceTicks execution_rounding_reserve_ticks_per_leg{};
};

struct FeeCalculation {
    std::uint64_t total_fee_ticks{};
    bool overflow{false};
};


[[nodiscard]] bool valid_monotonic_arb_config(
    const MonotonicArbConfig& config) noexcept;

enum class ArbAction : std::uint8_t{
    kBUY_YES,
    kSELL_YES,
};

struct MonotonicArbLeg{
    MarketId market_id{};
    ArbAction action{};
    PriceTicks limit_price_ticks{};
    QtyLots quantity_lots{};
};

struct MonotonicArbCandidate{
    EventId event_id{};
    std::uint64_t event_revision{};

    MonotonicArbLeg easier_leg;
    MonotonicArbLeg harder_leg;

    std::uint64_t gross_edge_ticks{};
    std::uint64_t estimated_fee_ticks{};
    std::uint64_t net_edge_ticks{};

    std::uint64_t universe_version{};
    std::uint32_t shard_index{};
    std::uint64_t ingress_timestamp_ns{};
    std::uint64_t book_apply_timestamp_ns{};
    std::uint64_t observation_publish_timestamp_ns{};

};

[[nodiscard]] FeeCalculation calculate_fee(
    const MonotonicArbConfig& config,
    const MonotonicArbLeg& candidate_leg) noexcept;

struct MonotonicArbEvaluation{
    MonotonicArbRejectReason reason{
        MonotonicArbRejectReason::kNONE
    };
    
    std::optional<MonotonicArbCandidate> candidate;

    [[nodiscard]] bool accepted() const noexcept{
        return candidate.has_value();
    }
};

[[nodiscard]] MonotonicArbEvaluation evaluate_monotonic_arb(
    const MonotonicArbConfig& config,
    const MonotonicPairObservation& observation,
    std::uint64_t wall_clock_now_s
) noexcept;



}// namespace predex::strategy