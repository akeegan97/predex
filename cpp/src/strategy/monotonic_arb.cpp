#include "predex/strategy/monotonic_arb.hpp"

#include <cassert>
#include <limits>
namespace {
using predex::strategy::MonotonicArbEvaluation;
using predex::strategy::MonotonicArbRejectReason;

struct LegSelection {
    predex::strategy::PriceTicks observed_top_ticks{};
    predex::strategy::PriceTicks chosen_limit_ticks{};
    std::uint8_t scanned_levels{};
    predex::strategy::QtyLots cumulative_quantity_lots{};
    bool bounded_aggression_applied{false};
};

struct LegCandidateSet {
    std::array<LegSelection, predex::strategy::kStrategyBookDepth> selections{};

    std::size_t count{0};

    predex::strategy::MonotonicArbRejectReason failure{
        predex::strategy::MonotonicArbRejectReason::kNONE};
};

struct PairedSelection {
    LegSelection easier;
    LegSelection harder;

    std::uint64_t gross_edge_ticks{};
    std::uint64_t estimated_fee_ticks{};
    std::uint64_t net_edge_ticks{};
};

struct PairedSelectionResult {
    PairedSelection selection{};
    bool found{false};

    MonotonicArbRejectReason failure{MonotonicArbRejectReason::kNONE};
};

[[nodiscard]] MonotonicArbEvaluation reject(MonotonicArbRejectReason reason) noexcept {
    return MonotonicArbEvaluation{
        .reason = reason,
        .candidate = std::nullopt,
    };
}
using predex::strategy::kStrategyBookDepth;
using predex::strategy::MonotonicPairObservation;

[[nodiscard]] bool valid_pair_header(const MonotonicPairObservation& observation) noexcept {

    if (observation.event_id == 0 || observation.easier.market_id == 0 ||
        observation.harder.market_id == 0) {
        return false;
    }

    if (observation.easier.market_id == observation.harder.market_id) {
        return false;
    }

    if (observation.easier.strike_key >= observation.harder.strike_key) {
        return false;
    }

    if (observation.easier.event_market_index >= observation.harder.event_market_index) {
        return false;
    }

    if (observation.harder.event_market_index - observation.easier.event_market_index != 1) {
        return false;
    }

    if (observation.easier.ask_count > kStrategyBookDepth ||
        observation.harder.bid_count > kStrategyBookDepth) {
        return false;
    }

    return true;
}
using predex::strategy::kPriceTicksPerDollar;
using predex::strategy::MonotonicArbConfig;
using predex::strategy::PriceTicks;
using predex::strategy::QtyLots;
using predex::strategy::StrategyMarketView;

[[nodiscard]] LegCandidateSet candidate_failure(MonotonicArbRejectReason reason) noexcept {
    LegCandidateSet result{};
    result.failure = reason;
    return result;
}

[[nodiscard]] QtyLots add_up_to_required(QtyLots current, QtyLots additional, // NOLINT
                                         QtyLots required) noexcept {

    if (current >= required) {
        return required;
    }

    const QtyLots remaining = required - current;

    if (additional >= remaining) {
        return required;
    }

    return current + additional;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- complex as all logic is inline
[[nodiscard]] LegCandidateSet collect_easier_candidates(const StrategyMarketView& market,
                                                        const MonotonicArbConfig& config) noexcept {

    if (market.ask_count == 0 || market.ask_count > predex::strategy::kStrategyBookDepth) {
        return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
    }

    const auto& top = market.asks[0];

    if (top.price_ticks == 0 || top.price_ticks >= kPriceTicksPerDollar || top.quantity_lots == 0) {
        return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
    }

    LegCandidateSet result{};

    QtyLots candidate_quantity{};
    QtyLots near_top_quantity{};
    std::uint8_t near_top_levels{};

    bool inside_near_top_window{true};
    bool candidate_scan_active{true};
    MonotonicArbRejectReason candidate_stop_reason{MonotonicArbRejectReason::kNONE};

    bool observed_second_level{false};
    bool top_gap_valid{false};

    PriceTicks previous_price = top.price_ticks;

    for (std::size_t index = 0; index < market.ask_count; ++index) {

        const auto& level = market.asks[index];

        if (level.price_ticks == 0 || level.price_ticks >= kPriceTicksPerDollar ||
            level.quantity_lots == 0) {
            return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
        }

        PriceTicks adjacent_gap{};

        if (index > 0) {
            if (level.price_ticks <= previous_price) {
                return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
            }

            adjacent_gap = level.price_ticks - previous_price;

            if (index == 1) {
                observed_second_level = true;
                top_gap_valid = adjacent_gap <= config.maximum_top_gap_ticks;
            }
        }

        const PriceTicks distance_from_top = level.price_ticks - top.price_ticks;

        if (inside_near_top_window && distance_from_top <= config.near_top_depth_window_ticks) {
            near_top_quantity += level.quantity_lots;
            ++near_top_levels;
        } else {
            inside_near_top_window = false;
        }

        const bool within_candidate_scan = index < config.maximum_easier_book_levels;

        if (within_candidate_scan && candidate_scan_active) {
            if (index > 0 && config.require_top_gap_continuity &&
                adjacent_gap > config.maximum_top_gap_ticks) {

                candidate_scan_active = false;
                candidate_stop_reason = MonotonicArbRejectReason::kDEPTH_DISCONTINUITY;
            }

            const PriceTicks aggression = level.price_ticks - top.price_ticks;

            if (candidate_scan_active &&
                ((!config.bounded_easier_aggression_enabled && aggression > 0) ||
                 aggression > config.maximum_easier_aggression_ticks)) {

                candidate_scan_active = false;
                candidate_stop_reason = MonotonicArbRejectReason::kPRICE_AGGRESSION_EXCEEDED;
            }

            if (candidate_scan_active) {

                candidate_quantity = add_up_to_required(candidate_quantity, level.quantity_lots,
                                                        config.order_quantity_lots);

                if (!config.require_full_easier_depth_for_quantity ||
                    candidate_quantity >= config.order_quantity_lots) {

                    result.selections[result.count++] = LegSelection{
                        .observed_top_ticks = top.price_ticks,
                        .chosen_limit_ticks = level.price_ticks,
                        .scanned_levels = static_cast<std::uint8_t>(index + 1),
                        .cumulative_quantity_lots = candidate_quantity,
                        .bounded_aggression_applied = level.price_ticks != top.price_ticks,
                    };
                }
            }
        }

        previous_price = level.price_ticks;
    }

    if (config.require_top_gap_continuity && (!observed_second_level || !top_gap_valid)) {
        return candidate_failure(MonotonicArbRejectReason::kDEPTH_DISCONTINUITY);
    }

    if (config.require_near_top_multilevel_support) {
        if (near_top_levels < config.minimum_near_top_levels) {
            return candidate_failure(MonotonicArbRejectReason::kDEPTH_DISCONTINUITY);
        }

        if (near_top_quantity < config.order_quantity_lots) {
            return candidate_failure(MonotonicArbRejectReason::kINSUFFICIENT_QUANTITY);
        }
    }

    if (result.count == 0) {
        if (candidate_stop_reason != MonotonicArbRejectReason::kNONE) {
            return candidate_failure(candidate_stop_reason);
        }

        return candidate_failure(MonotonicArbRejectReason::kINSUFFICIENT_QUANTITY);
    }

    return result;
}
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
[[nodiscard]] LegCandidateSet collect_harder_candidates(const StrategyMarketView& market,
                                                        const MonotonicArbConfig& config) noexcept {
    if (market.bid_count == 0 || market.bid_count > predex::strategy::kStrategyBookDepth) {
        return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
    }

    const auto& top = market.bids[0];

    if (top.price_ticks == 0 || top.price_ticks >= kPriceTicksPerDollar || top.quantity_lots == 0) {
        return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
    }

    LegCandidateSet result{};

    QtyLots candidate_quantity{};
    QtyLots near_top_quantity{};
    std::uint8_t near_top_levels{};

    bool inside_near_top_window{true};
    bool candidate_scan_active{true};
    MonotonicArbRejectReason candidate_stop_reason{MonotonicArbRejectReason::kNONE};

    bool observed_second_level{false};
    bool top_gap_valid{false};

    PriceTicks previous_price = top.price_ticks;

    for (std::size_t index = 0; index < market.bid_count; ++index) {

        const auto& level = market.bids[index];

        if (level.price_ticks == 0 || level.price_ticks >= kPriceTicksPerDollar ||
            level.quantity_lots == 0) {
            return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
        }

        PriceTicks adjacent_gap{};

        if (index > 0) {
            if (level.price_ticks >= previous_price) {
                return candidate_failure(MonotonicArbRejectReason::kINVALID_OBSERVATION);
            }

            adjacent_gap = previous_price - level.price_ticks;

            if (index == 1) {
                observed_second_level = true;
                top_gap_valid = adjacent_gap <= config.maximum_top_gap_ticks;
            }
        }

        const PriceTicks distance_from_top = top.price_ticks - level.price_ticks;

        if (inside_near_top_window && distance_from_top <= config.near_top_depth_window_ticks) {

            near_top_quantity += level.quantity_lots;
            ++near_top_levels;
        } else {
            inside_near_top_window = false;
        }

        const bool within_candidate_scan = index < config.maximum_harder_book_levels;

        if (within_candidate_scan && candidate_scan_active) {
            if (index > 0 && config.require_top_gap_continuity &&
                adjacent_gap > config.maximum_top_gap_ticks) {

                candidate_scan_active = false;
                candidate_stop_reason = MonotonicArbRejectReason::kDEPTH_DISCONTINUITY;
            }

            const PriceTicks aggression = top.price_ticks - level.price_ticks;

            if (candidate_scan_active &&
                ((!config.bounded_harder_aggression_enabled && aggression > 0) ||
                 aggression > config.maximum_harder_aggression_ticks)) {

                candidate_scan_active = false;
                candidate_stop_reason = MonotonicArbRejectReason::kPRICE_AGGRESSION_EXCEEDED;
            }

            if (candidate_scan_active) {

                candidate_quantity = add_up_to_required(candidate_quantity, level.quantity_lots,
                                                        config.order_quantity_lots);

                if (!config.require_full_harder_depth_for_quantity ||
                    candidate_quantity >= config.order_quantity_lots) {

                    result.selections[result.count++] = LegSelection{
                        .observed_top_ticks = top.price_ticks,
                        .chosen_limit_ticks = level.price_ticks,
                        .scanned_levels = static_cast<std::uint8_t>(index + 1),
                        .cumulative_quantity_lots = candidate_quantity,
                        .bounded_aggression_applied = level.price_ticks != top.price_ticks,
                    };
                }
            }
        }

        previous_price = level.price_ticks;
    }

    if (config.require_top_gap_continuity && (!observed_second_level || !top_gap_valid)) {
        return candidate_failure(MonotonicArbRejectReason::kDEPTH_DISCONTINUITY);
    }

    if (config.require_near_top_multilevel_support) {
        if (near_top_levels < config.minimum_near_top_levels) {
            return candidate_failure(MonotonicArbRejectReason::kDEPTH_DISCONTINUITY);
        }

        if (near_top_quantity < config.order_quantity_lots) {
            return candidate_failure(MonotonicArbRejectReason::kINSUFFICIENT_QUANTITY);
        }
    }

    if (result.count == 0) {
        if (candidate_stop_reason != MonotonicArbRejectReason::kNONE) {
            return candidate_failure(candidate_stop_reason);
        }

        return candidate_failure(MonotonicArbRejectReason::kINSUFFICIENT_QUANTITY);
    }

    return result;
}

[[nodiscard]] std::uint64_t calculate_gross_edge_ticks(PriceTicks easier_limit,
                                                       PriceTicks harder_limit, // NOLINT
                                                       QtyLots quantity_lots) noexcept {

    if (harder_limit <= easier_limit) {
        return 0;
    }

    const PriceTicks edge_per_contract = harder_limit - easier_limit;

    return (edge_per_contract * quantity_lots) / predex::strategy::kQuantityLotsPerContract;
}

inline constexpr QtyLots kMaximumSafeGrossQuantityLots =
    std::numeric_limits<std::uint64_t>::max() / (kPriceTicksPerDollar - 1);

using predex::strategy::ArbAction;
using predex::strategy::FeeCalculation;
using predex::strategy::MonotonicArbLeg;

[[nodiscard]] PairedSelectionResult // NOLINTNEXTLINE
select_paired_limits(const LegCandidateSet& easier_candidates,
                     const LegCandidateSet& harder_candidates,
                     const MonotonicArbConfig& config) noexcept {

    PairedSelectionResult result{};

    bool saw_gross_edge{false};
    bool saw_fee_surviving_edge{false};
    std::uint16_t best_depth_score{};

    const std::uint64_t required_net_edge =
        config.minimum_net_edge_ticks + config.edge_cushion_ticks;

    for (std::size_t easier_index = 0; easier_index < easier_candidates.count; ++easier_index) {

        const auto& easier = easier_candidates.selections[easier_index];

        const MonotonicArbLeg easier_leg{
            .market_id = 0,
            .action = ArbAction::kBUY_YES,
            .limit_price_ticks = easier.chosen_limit_ticks,
            .quantity_lots = config.order_quantity_lots,
        };

        const FeeCalculation easier_fee = calculate_fee(config, easier_leg);

        if (easier_fee.overflow) {
            return PairedSelectionResult{
                .failure = MonotonicArbRejectReason::kARITHMETIC_OVERFLOW,
            };
        }

        for (std::size_t harder_index = 0; harder_index < harder_candidates.count; ++harder_index) {

            const auto& harder = harder_candidates.selections[harder_index];

            const std::uint64_t gross_edge = calculate_gross_edge_ticks(
                easier.chosen_limit_ticks, harder.chosen_limit_ticks, config.order_quantity_lots);

            if (gross_edge == 0) {
                continue;
            }

            saw_gross_edge = true;

            const MonotonicArbLeg harder_leg{
                .market_id = 0,
                .action = ArbAction::kSELL_YES,
                .limit_price_ticks = harder.chosen_limit_ticks,
                .quantity_lots = config.order_quantity_lots,
            };

            const FeeCalculation harder_fee = calculate_fee(config, harder_leg);

            if (harder_fee.overflow) {
                return PairedSelectionResult{
                    .failure = MonotonicArbRejectReason::kARITHMETIC_OVERFLOW,
                };
            }

            if (easier_fee.total_fee_ticks >
                std::numeric_limits<std::uint64_t>::max() - harder_fee.total_fee_ticks) {
                return PairedSelectionResult{
                    .failure = MonotonicArbRejectReason::kARITHMETIC_OVERFLOW,
                };
            }

            const std::uint64_t total_fees =
                easier_fee.total_fee_ticks + harder_fee.total_fee_ticks;

            if (gross_edge <= total_fees) {
                continue;
            }

            saw_fee_surviving_edge = true;

            const std::uint64_t net_edge = gross_edge - total_fees;

            if (net_edge < required_net_edge) {
                continue;
            }

            const std::uint16_t depth_score = static_cast<std::uint16_t>(easier.scanned_levels) +
                                              static_cast<std::uint16_t>(harder.scanned_levels);

            const bool is_better =
                !result.found || depth_score > best_depth_score ||
                (depth_score == best_depth_score && net_edge > result.selection.net_edge_ticks);

            if (!is_better) {
                continue;
            }

            result.selection = PairedSelection{
                .easier = easier,
                .harder = harder,
                .gross_edge_ticks = gross_edge,
                .estimated_fee_ticks = total_fees,
                .net_edge_ticks = net_edge,
            };

            result.found = true;
            best_depth_score = depth_score;
        }
    }

    if (result.found) {
        return result;
    }

    if (!saw_gross_edge) {
        result.failure = MonotonicArbRejectReason::kNO_GROSS_EDGE;
    } else if (!saw_fee_surviving_edge) {
        result.failure = MonotonicArbRejectReason::kFEES_ERASE_EDGE;
    } else {
        result.failure = MonotonicArbRejectReason::kINSUFFICIENT_NET_EDGE;
    }

    return result;
}

} // namespace

namespace predex::strategy {

bool valid_monotonic_arb_config(const MonotonicArbConfig& config) noexcept {

    if (config.order_quantity_lots == 0) {
        return false;
    }

    if (config.maximum_easier_book_levels == 0 ||
        config.maximum_easier_book_levels > kStrategyBookDepth) {
        return false;
    }

    if (config.maximum_harder_book_levels == 0 ||
        config.maximum_harder_book_levels > kStrategyBookDepth) {
        return false;
    }

    if (config.require_near_top_multilevel_support &&
        (config.minimum_near_top_levels == 0 ||
         config.minimum_near_top_levels > kStrategyBookDepth)) {
        return false;
    }

    if (config.require_top_gap_continuity && config.maximum_top_gap_ticks == 0) {
        return false;
    }

    if (config.minimum_net_edge_ticks >
        std::numeric_limits<std::uint64_t>::max() - config.edge_cushion_ticks) {
        return false;
    }

    if (config.order_quantity_lots > kMaximumSafeGrossQuantityLots) {
        return false;
    }

    if (config.taker_fee_rate_denominator == 0) {
        return false;
    }

    if (config.taker_fee_rate_numerator > config.taker_fee_rate_denominator) {
        return false;
    }

    return true;
}

FeeCalculation calculate_fee(const MonotonicArbConfig& config,
                             const MonotonicArbLeg& candidate_leg) noexcept {

    FeeCalculation result{};

    if (candidate_leg.quantity_lots == 0) {
        return result;
    }

    if (config.taker_fee_rate_denominator == 0 || candidate_leg.limit_price_ticks == 0 ||
        candidate_leg.limit_price_ticks >= kPriceTicksPerDollar) {
        result.overflow = true;
        return result;
    }

    using WideUInt = unsigned __int128; // NOLINT

    const WideUInt price = candidate_leg.limit_price_ticks;

    const WideUInt complement = kPriceTicksPerDollar - price;

    const WideUInt numerator = static_cast<WideUInt>(config.taker_fee_rate_numerator) *
                               candidate_leg.quantity_lots * price * complement;

    const WideUInt denominator = static_cast<WideUInt>(config.taker_fee_rate_denominator) *
                                 kQuantityLotsPerContract * kPriceTicksPerDollar;

    // Ceiling division.
    const WideUInt base_fee_ticks = (numerator + denominator - 1) / denominator;

    const WideUInt total_fee_ticks =
        base_fee_ticks + config.execution_rounding_reserve_ticks_per_leg;

    if (total_fee_ticks > std::numeric_limits<std::uint64_t>::max()) {
        result.overflow = true;
        return result;
    }

    result.total_fee_ticks = static_cast<std::uint64_t>(total_fee_ticks);

    return result;
}

MonotonicArbEvaluation evaluate_monotonic_arb(const MonotonicArbConfig& config,
                                              const MonotonicPairObservation& observation,
                                              std::uint64_t wall_clock_now_s) noexcept {

    assert(valid_monotonic_arb_config(config));

    if (!valid_pair_header(observation)) {
        return reject(MonotonicArbRejectReason::kINVALID_OBSERVATION);
    }

    if (!observation.easier.tradeable || !observation.harder.tradeable) {
        return reject(MonotonicArbRejectReason::kMARKET_NOT_TRADEABLE);
    }

    const bool easier_closed = observation.easier.market_close_time_s != 0 &&
                               wall_clock_now_s >= observation.easier.market_close_time_s;

    const bool harder_closed = observation.harder.market_close_time_s != 0 &&
                               wall_clock_now_s >= observation.harder.market_close_time_s;

    if (easier_closed || harder_closed) {
        return reject(MonotonicArbRejectReason::kMARKET_CLOSED);
    }

    if (observation.easier.ask_count == 0) {
        return reject(MonotonicArbRejectReason::kMISSING_ASK_DEPTH);
    }

    if (observation.harder.bid_count == 0) {
        return reject(MonotonicArbRejectReason::kMISSING_BID_DEPTH);
    }
    const LegCandidateSet easier_candidates = collect_easier_candidates(observation.easier, config);

    if (easier_candidates.count == 0) {
        return reject(easier_candidates.failure);
    }

    const LegCandidateSet harder_candidates = collect_harder_candidates(observation.harder, config);

    if (harder_candidates.count == 0) {
        return reject(harder_candidates.failure);
    }

    const PairedSelectionResult paired =
        select_paired_limits(easier_candidates, harder_candidates, config);

    if (!paired.found) {
        return reject(paired.failure);
    }

    const auto& selection = paired.selection;

    return MonotonicArbEvaluation{
        .reason = MonotonicArbRejectReason::kNONE,
        .candidate =
            MonotonicArbCandidate{
                .event_id = observation.event_id,
                .event_revision = observation.event_revision,

                .easier_leg =
                    MonotonicArbLeg{
                        .market_id = observation.easier.market_id,
                        .action = ArbAction::kBUY_YES,
                        .limit_price_ticks = selection.easier.chosen_limit_ticks,
                        .quantity_lots = config.order_quantity_lots,
                    },

                .harder_leg =
                    MonotonicArbLeg{
                        .market_id = observation.harder.market_id,
                        .action = ArbAction::kSELL_YES,
                        .limit_price_ticks = selection.harder.chosen_limit_ticks,
                        .quantity_lots = config.order_quantity_lots,
                    },

                .gross_edge_ticks = selection.gross_edge_ticks,
                .estimated_fee_ticks = selection.estimated_fee_ticks,
                .net_edge_ticks = selection.net_edge_ticks,

                .universe_version = observation.universe_version,
                .shard_index = observation.shard_index,
                .ingress_timestamp_ns = observation.ingress_timestamp_ns,
                .book_apply_timestamp_ns = observation.book_apply_timestamp_ns,
                .observation_publish_timestamp_ns = observation.publish_timestamp_ns,
            },
    };
}
} // namespace predex::strategy
