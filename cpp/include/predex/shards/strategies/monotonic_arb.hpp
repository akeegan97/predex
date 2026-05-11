#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

#include "predex/internal/market_types.hpp"
#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi::strategies {
constexpr double kPriceScale = static_cast<double>(internal::kPriceTicksPerDollar);
constexpr double kCentScale = 100.0;
constexpr internal::PriceTicks kTicksPerCent = internal::kTicksPerCent;
constexpr double kTakerFeeRate = 0.07;
constexpr double kMakerFeeRate = 0.0175;
constexpr std::int64_t kMinEdgeTicks = 20;
struct MonotonicArbConfig {
    constexpr static std::int64_t kMaxTopGapTicks = 20;
    constexpr static std::int64_t kMaxEasierAggressionTicks = 30;
    constexpr static std::int64_t kMaxHarderAggressionTicks = 30;
    constexpr static std::uint16_t kMinNearTopLevels = 2;
    constexpr static std::int64_t kTopDepthWindowTicks = 20;
    std::int64_t min_net_edge_ticks{kMinEdgeTicks};
    internal::QtyLots default_order_qty_lots{internal::kOneContractQtyLots};
    bool bounded_harder_aggression_enabled{true};
    bool bounded_easier_aggression_enabled{true};
    bool require_top_gap_continuity{true};
    internal::PriceTicks max_top_gap_ticks{kMaxTopGapTicks};
    bool require_near_top_multilevel_support{true};
    internal::PriceTicks near_top_depth_window_ticks{kTopDepthWindowTicks};
    std::uint16_t min_near_top_levels{kMinNearTopLevels};
    internal::PriceTicks max_easier_aggression_ticks{kMaxEasierAggressionTicks};
    internal::PriceTicks max_harder_aggression_ticks{kMaxHarderAggressionTicks};
    std::size_t max_easier_book_levels{3};
    std::size_t max_harder_book_levels{3};
    bool require_full_easier_depth_for_qty{true};
    bool require_full_harder_depth_for_qty{true};
    bool enabled{true};
};

class MonotonicArbStrategy {
  public:
    explicit MonotonicArbStrategy(MonotonicArbConfig config = {}) : config_(config) {}

    template <typename SignalSink>
    void on_event(const AppliedEventUpdate& update, SignalSink& out_signals) noexcept {
        if (!config_.enabled ||
            update.event.topology_kind != internal::EventTopologyKind::kMonotonicChain) {
            return;
        }

        const auto* chain = std::get_if<MonotonicChainState>(&update.event.derived_state);
        if (chain == nullptr || !chain->complete || chain->desynced) {
            return;
        }

        const auto market_index = chain->market_index_by_id.find(update.update.meta.market_id);
        if (market_index == chain->market_index_by_id.end()) {
            return;
        }

        // Kalshi emits no WebSocket event at natural close_time, so markets can transition out
        // of tradeable state without notification. A live half-fill incident at near-close
        // motivated adding strategy-time tradeability checks rather than relying on
        // venue-driven deactivation.
        //
        // recv_ns is intentionally stamped from steady_clock for latency measurement, so it
        // cannot be compared against venue close_time. Tradeability must use wall-clock epoch
        // seconds even though the rest of the pipeline uses monotonic timestamps.
        const auto now_s = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

        std::optional<GroupSignal> best_signal;
        const std::size_t index = market_index->second;
        if (index > 0) {
            best_signal =
                evaluate_pair(chain->markets[index - 1], chain->markets[index], update, now_s);
        }
        if (index + 1 < chain->markets.size()) {
            auto candidate =
                evaluate_pair(chain->markets[index], chain->markets[index + 1], update, now_s);
            if (candidate.has_value() &&
                (!best_signal.has_value() || candidate->score > best_signal->score)) {
                // NOLINTNEXTLINE(performance-move-const-arg)
                best_signal = std::move(candidate);
            }
        }

        if (best_signal.has_value()) {
            static_cast<void>(out_signals.try_push_group_signal(*best_signal));
        }
    }

  private:
    struct LegSelection {
        internal::PriceTicks observed_top_ticks{0};
        internal::PriceTicks chosen_limit_ticks{0};
        std::uint16_t scanned_levels{0};
        internal::QtyLots cumulative_qty_lots{0};
        bool bounded_aggression_applied{false};
    };
    template <std::size_t Depth> struct LegSelectionBuffer {
        std::array<LegSelection, Depth> entries{};
        std::size_t count{0};

        void push(const LegSelection& selection) noexcept {
            if (count < Depth) {
                entries[count++] = selection;
            }
        }

        [[nodiscard]] bool empty() const noexcept { return count == 0; }
        [[nodiscard]] std::size_t size() const noexcept { return count; }
        [[nodiscard]] const LegSelection* begin() const noexcept { return entries.data(); }
        [[nodiscard]] const LegSelection* end() const noexcept { return entries.data() + count; }
    };
    static constexpr std::size_t kMaxBookLevelScan = 10;

    MonotonicArbConfig config_{};
    std::uint64_t next_signal_id_{1};

    [[nodiscard]] static internal::PriceTicks fee_ticks_(
        // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
        double fee_rate, internal::PriceTicks price_ticks, internal::QtyLots qty) noexcept {
        if (qty <= 0 || price_ticks <= 0 ||
            price_ticks >= static_cast<internal::PriceTicks>(kPriceScale)) {
            return 0;
        }

        const double p_dollars = static_cast<double>(price_ticks) / kPriceScale;
        const double fee_dollars =
            fee_rate * internal::qty_to_contracts(qty) * p_dollars * (1.0 - p_dollars);
        const double fee_cents = std::ceil(fee_dollars * kCentScale);
        return static_cast<internal::PriceTicks>(fee_cents) * kTicksPerCent;
    }

    [[nodiscard]] static internal::PriceTicks taker_fee_ticks_(internal::PriceTicks price_ticks,
                                                               internal::QtyLots qty) noexcept {
        return fee_ticks_(kTakerFeeRate, price_ticks, qty);
    }

    [[nodiscard]] static internal::PriceTicks maker_fee_ticks_(internal::PriceTicks price_ticks,
                                                               internal::QtyLots qty) noexcept {
        return fee_ticks_(kMakerFeeRate, price_ticks, qty);
    }

    [[nodiscard]] static std::optional<internal::PriceTicks>
    best_bid_ticks_(const EventMarketView& market) noexcept {
        return market.depth.bids[0].price_ticks;
    }

    [[nodiscard]] static std::optional<internal::QtyLots>
    best_bid_qty_(const EventMarketView& market) noexcept {
        return market.depth.bids[0].qty_lots;
    }

    [[nodiscard]] static std::optional<internal::PriceTicks>
    best_ask_ticks_(const EventMarketView& market) noexcept {
        return market.depth.asks[0].price_ticks;
    }

    [[nodiscard]] static std::optional<internal::QtyLots>
    best_ask_qty_(const EventMarketView& market) noexcept {
        return market.depth.asks[0].qty_lots;
    }

    [[nodiscard]] internal::PriceTicks
    net_edge_ticks_for_pair_(internal::PriceTicks easier_buy_ticks,
                             internal::PriceTicks harder_sell_ticks,
                             internal::QtyLots executable_qty) const noexcept {
        if (easier_buy_ticks <= 0 || harder_sell_ticks <= easier_buy_ticks || executable_qty <= 0) {
            return std::numeric_limits<internal::PriceTicks>::min();
        }

        const internal::PriceTicks gross_edge_per_contract = harder_sell_ticks - easier_buy_ticks;
        const auto gross_edge_ticks = static_cast<internal::PriceTicks>(
            internal::scale_ticks_by_qty_floor(gross_edge_per_contract, executable_qty));
        const internal::PriceTicks total_fees_ticks =
            taker_fee_ticks_(easier_buy_ticks, executable_qty) +
            taker_fee_ticks_(harder_sell_ticks, executable_qty);
        return gross_edge_ticks - total_fees_ticks;
    }

    template <std::size_t Depth>
    [[nodiscard]] std::pair<std::uint16_t, internal::QtyLots>
    near_top_support_(const std::array<SideDepthLevel, Depth>& levels,
                      bool descending_prices) const noexcept {
        std::optional<internal::PriceTicks> top_price_ticks;
        std::uint16_t level_count = 0;
        internal::QtyLots cumulative_qty_lots = 0;

        for (std::size_t level_index = 0; level_index < levels.size(); ++level_index) {
            const auto& level = levels[level_index];
            if (!level.price_ticks.has_value() || !level.qty_lots.has_value() ||
                *level.qty_lots <= 0) {
                continue;
            }

            const internal::PriceTicks price_ticks = *level.price_ticks;
            if (!top_price_ticks.has_value()) {
                top_price_ticks = price_ticks;
            } else {
                const internal::PriceTicks distance_ticks = descending_prices
                                                                ? (*top_price_ticks - price_ticks)
                                                                : (price_ticks - *top_price_ticks);
                if (distance_ticks < 0 || distance_ticks > config_.near_top_depth_window_ticks) {
                    break;
                }
            }

            ++level_count;
            cumulative_qty_lots += *level.qty_lots;
        }

        return {level_count, cumulative_qty_lots};
    }

    template <std::size_t Depth>
    [[nodiscard]] bool has_near_top_support_(const std::array<SideDepthLevel, Depth>& levels,
                                             bool descending_prices,
                                             internal::QtyLots executable_qty) const noexcept {
        if (!config_.require_near_top_multilevel_support) {
            return true;
        }

        const auto [level_count, cumulative_qty_lots] =
            near_top_support_(levels, descending_prices);
        return level_count >= config_.min_near_top_levels && cumulative_qty_lots >= executable_qty;
    }

    template <std::size_t Depth>
    [[nodiscard]] bool top_gap_within_limit_(const std::array<SideDepthLevel, Depth>& levels,
                                             bool descending_prices) const noexcept {
        if (!config_.require_top_gap_continuity || config_.max_top_gap_ticks <= 0) {
            return true;
        }

        std::optional<internal::PriceTicks> first_price;
        std::optional<internal::PriceTicks> second_price;
        for (std::size_t level_index = 0; level_index < levels.size(); ++level_index) {
            const auto& level = levels[level_index];
            if (!level.price_ticks.has_value() || !level.qty_lots.has_value() ||
                *level.qty_lots <= 0) {
                continue;
            }

            if (!first_price.has_value()) {
                first_price = level.price_ticks;
                continue;
            }

            second_price = level.price_ticks;
            break;
        }

        if (!first_price.has_value() || !second_price.has_value()) {
            return false;
        }

        const internal::PriceTicks gap_ticks =
            descending_prices ? (*first_price - *second_price) : (*second_price - *first_price);
        return gap_ticks >= 0 && gap_ticks <= config_.max_top_gap_ticks;
    }

    [[nodiscard]] LegSelectionBuffer<kMaxBookLevelScan>
    collect_easier_buy_candidates_(const EventMarketView& easier_market,
                                   internal::QtyLots executable_qty) const noexcept {
        const auto easier_ask = best_ask_ticks_(easier_market);
        if (!easier_ask.has_value()) {
            return {};
        }
        LegSelectionBuffer<kMaxBookLevelScan> candidates{};
        LegSelection selection{
            .observed_top_ticks = *easier_ask,
            .chosen_limit_ticks = *easier_ask,
            .scanned_levels = 0,
            .cumulative_qty_lots = 0,
            .bounded_aggression_applied = false,
        };

        const std::size_t max_levels =
            std::min<std::size_t>(config_.max_easier_book_levels,
                                  std::min(easier_market.depth.asks.size(), kMaxBookLevelScan));

        internal::PriceTicks previous_price_ticks = *easier_ask;
        for (std::size_t level_index = 0; level_index < max_levels; ++level_index) {
            const auto& level = easier_market.depth.asks[level_index];
            if (!level.price_ticks.has_value() || !level.qty_lots.has_value() ||
                *level.qty_lots <= 0) {
                break;
            }

            const internal::PriceTicks candidate_ticks = *level.price_ticks;
            if (level_index > 0 && config_.require_top_gap_continuity) {
                const internal::PriceTicks gap_ticks = candidate_ticks - previous_price_ticks;
                if (gap_ticks < 0 || gap_ticks > config_.max_top_gap_ticks) {
                    break;
                }
            }
            selection.scanned_levels = static_cast<std::uint16_t>(level_index + 1);
            selection.cumulative_qty_lots += *level.qty_lots;
            previous_price_ticks = candidate_ticks;

            const internal::PriceTicks aggression_ticks = candidate_ticks - *easier_ask;
            if (aggression_ticks < 0 ||
                (!config_.bounded_easier_aggression_enabled && aggression_ticks > 0) ||
                aggression_ticks > config_.max_easier_aggression_ticks) {
                break;
            }
            if (config_.require_full_easier_depth_for_qty &&
                selection.cumulative_qty_lots < executable_qty) {
                continue;
            }
            selection.chosen_limit_ticks = candidate_ticks;
            selection.bounded_aggression_applied =
                selection.chosen_limit_ticks != selection.observed_top_ticks;
            candidates.push(selection);
        }
        return candidates;
    }

    [[nodiscard]] LegSelectionBuffer<kMaxBookLevelScan>
    collect_harder_sell_candidates_(const EventMarketView& harder_market,
                                    internal::QtyLots executable_qty) const noexcept {
        const auto harder_bid = best_bid_ticks_(harder_market);
        if (!harder_bid.has_value()) {
            return {};
        }
        LegSelectionBuffer<kMaxBookLevelScan> candidates{};
        LegSelection selection{
            .observed_top_ticks = *harder_bid,
            .chosen_limit_ticks = *harder_bid,
            .scanned_levels = 0,
            .cumulative_qty_lots = 0,
            .bounded_aggression_applied = false,
        };
        const std::size_t max_levels =
            std::min<std::size_t>(config_.max_harder_book_levels,
                                  std::min(harder_market.depth.bids.size(), kMaxBookLevelScan));
        internal::PriceTicks previous_price_ticks = *harder_bid;
        for (std::size_t level_index = 0; level_index < max_levels; ++level_index) {
            const auto& level = harder_market.depth.bids[level_index];
            if (!level.price_ticks.has_value() || !level.qty_lots.has_value() ||
                *level.qty_lots <= 0) {
                break;
            }
            const internal::PriceTicks candidate_ticks = *level.price_ticks;
            if (level_index > 0 && config_.require_top_gap_continuity) {
                const internal::PriceTicks gap_ticks = previous_price_ticks - candidate_ticks;
                if (gap_ticks < 0 || gap_ticks > config_.max_top_gap_ticks) {
                    break;
                }
            }
            selection.scanned_levels = static_cast<std::uint16_t>(level_index + 1);
            selection.cumulative_qty_lots += *level.qty_lots;
            previous_price_ticks = candidate_ticks;
            const internal::PriceTicks aggression_ticks = *harder_bid - candidate_ticks;
            if (aggression_ticks < 0 ||
                (!config_.bounded_harder_aggression_enabled && aggression_ticks > 0) ||
                aggression_ticks > config_.max_harder_aggression_ticks) {
                break;
            }
            if (config_.require_full_harder_depth_for_qty &&
                selection.cumulative_qty_lots < executable_qty) {
                continue;
            }
            selection.chosen_limit_ticks = candidate_ticks;
            selection.bounded_aggression_applied =
                selection.chosen_limit_ticks != selection.observed_top_ticks;
            candidates.push(selection);
        }
        return candidates;
    };

    [[nodiscard]] std::optional<std::pair<LegSelection, LegSelection>>
    select_paired_limits_(const EventMarketView& easier_market,
                          const EventMarketView& harder_market,
                          internal::QtyLots executable_qty) const noexcept {
        const auto easier_ask = best_ask_ticks_(easier_market);
        const auto harder_bid = best_bid_ticks_(harder_market);
        if (!easier_ask.has_value() || !harder_bid.has_value()) {
            return std::nullopt;
        }

        if (!top_gap_within_limit_(easier_market.depth.asks, false) ||
            !top_gap_within_limit_(harder_market.depth.bids, true) ||
            !has_near_top_support_(easier_market.depth.asks, false, executable_qty) ||
            !has_near_top_support_(harder_market.depth.bids, true, executable_qty)) {
            return std::nullopt;
        }

        const auto easier_candidates =
            collect_easier_buy_candidates_(easier_market, executable_qty);
        const auto harder_candidates =
            collect_harder_sell_candidates_(harder_market, executable_qty);
        if (easier_candidates.empty() || harder_candidates.empty()) {
            return std::nullopt;
        }

        std::optional<std::pair<LegSelection, LegSelection>> best_selection;
        internal::PriceTicks best_net_edge_ticks = std::numeric_limits<internal::PriceTicks>::min();
        std::uint32_t best_depth_score = 0;

        for (const auto& easier_selection : easier_candidates) {
            for (const auto& harder_selection : harder_candidates) {
                const internal::PriceTicks candidate_net_edge_ticks =
                    net_edge_ticks_for_pair_(easier_selection.chosen_limit_ticks,
                                             harder_selection.chosen_limit_ticks, executable_qty);
                if (candidate_net_edge_ticks < config_.min_net_edge_ticks) {
                    continue;
                }

                const std::uint32_t depth_score =
                    static_cast<std::uint32_t>(easier_selection.scanned_levels) +
                    static_cast<std::uint32_t>(harder_selection.scanned_levels);
                if (!best_selection.has_value() || depth_score > best_depth_score ||
                    (depth_score == best_depth_score &&
                     candidate_net_edge_ticks > best_net_edge_ticks)) {
                    best_selection = std::make_pair(easier_selection, harder_selection);
                    best_depth_score = depth_score;
                    best_net_edge_ticks = candidate_net_edge_ticks;
                }
            }
        }

        return best_selection;
    }

    [[nodiscard]] std::optional<GroupSignal> evaluate_pair(const ChainEntry& easier,
                                                           const ChainEntry& harder,
                                                           const AppliedEventUpdate& update,
                                                           std::uint64_t now_s) noexcept {
        if (!easier.market.has_book || !harder.market.has_book || easier.market.desynced ||
            harder.market.desynced || !easier.market.lifecycle.is_tradeable_at(now_s) ||
            !harder.market.lifecycle.is_tradeable_at(now_s)) {
            return std::nullopt;
        }

        const auto easier_ask = best_ask_ticks_(easier.market);
        const auto easier_ask_qty = best_ask_qty_(easier.market);
        const auto harder_bid = best_bid_ticks_(harder.market);
        const auto harder_bid_qty = best_bid_qty_(harder.market);
        if (!easier_ask.has_value() || !easier_ask_qty.has_value() || !harder_bid.has_value() ||
            !harder_bid_qty.has_value()) {
            return std::nullopt;
        }

        const internal::QtyLots executable_qty = config_.default_order_qty_lots;
        if (executable_qty <= 0) {
            return std::nullopt;
        }

        const auto paired_selection =
            select_paired_limits_(easier.market, harder.market, executable_qty);
        if (!paired_selection.has_value()) {
            return std::nullopt;
        }
        const auto& easier_selection = paired_selection->first;
        const auto& harder_selection = paired_selection->second;
        const auto [easier_near_top_levels, easier_near_top_qty_lots] =
            near_top_support_(easier.market.depth.asks, false);
        const auto [harder_near_top_levels, harder_near_top_qty_lots] =
            near_top_support_(harder.market.depth.bids, true);

        const internal::PriceTicks net_edge_ticks =
            net_edge_ticks_for_pair_(easier_selection.chosen_limit_ticks,
                                     harder_selection.chosen_limit_ticks, executable_qty);
        if (net_edge_ticks < config_.min_net_edge_ticks) {
            return std::nullopt;
        }

        GroupSignal signal{};
        signal.signal_id = next_signal_id_++;
        signal.exchange = update.update.meta.exchange;
        signal.event_id = update.event.event_id;
        signal.kind = SignalKind::kMonotonicViolation;
        signal.execution_policy =
            predex::core::oms::kalshi::GroupExecutionPolicy::kAbortRemainingOnReject;
        signal.leg_count = 2;
        signal.reference_price_ticks = easier_ask;
        signal.aux_reference_price_ticks = harder_bid;
        signal.reference_depth_levels = easier_near_top_levels;
        signal.aux_reference_depth_levels = harder_near_top_levels;
        signal.reference_depth_qty_lots = easier_near_top_qty_lots;
        signal.aux_reference_depth_qty_lots = harder_near_top_qty_lots;
        signal.paired_frontier_qty_lots =
            std::min(easier_near_top_qty_lots, harder_near_top_qty_lots);
        signal.signal_ts_ns = update.update.meta.recv_ns;
        signal.edge_ticks = net_edge_ticks;
        signal.score = net_edge_ticks;

        // Both legs trade the YES contract: we buy YES on the easier (underpriced) market at
        // its ask, and sell YES on the harder (overpriced) market at its bid. easier_ask and
        // harder_bid are both YES-side quotes, so the Outcome here must be kYes — encoding
        // the sell leg as an NO order (the prior Side-only mistranslation) would change the
        // trade entirely and get 400-rejected by Kalshi.
        signal.legs[0] = SubmissionLeg{
            .market_id = easier.market.market_id,
            .side = internal::Side::kBuy,
            .outcome = predex::core::oms::kalshi::Outcome::kYes,
            .qty_lots = executable_qty,
            .limit_price_ticks = easier_selection.chosen_limit_ticks,
            .time_in_force = predex::core::oms::kalshi::OmsTimeInForce::kIoc,
        };
        signal.legs[1] = SubmissionLeg{
            .market_id = harder.market.market_id,
            .side = internal::Side::kSell,
            .outcome = predex::core::oms::kalshi::Outcome::kYes,
            .qty_lots = executable_qty,
            .limit_price_ticks = harder_selection.chosen_limit_ticks,
            .time_in_force = predex::core::oms::kalshi::OmsTimeInForce::kIoc,
        };
        return signal;
    }
};

} // namespace predex::core::shards::kalshi::strategies
