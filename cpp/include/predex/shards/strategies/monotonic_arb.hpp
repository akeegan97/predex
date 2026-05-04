#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>

#include "predex/internal/market_types.hpp"
#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi::strategies {
inline constexpr double kPriceScale = static_cast<double>(internal::kPriceTicksPerDollar);
inline constexpr double kCentScale = 100.0;
inline constexpr internal::PriceTicks kTicksPerCent = internal::kTicksPerCent;
inline constexpr double kTakerFeeRate = 0.07;
inline constexpr double kMakerFeeRate = 0.0175;
inline constexpr std::int64_t kMinEdgeTicks = 20;

struct MonotonicArbConfig {
    std::int64_t min_net_edge_ticks{kMinEdgeTicks};
    internal::QtyLots default_order_qty_lots{internal::kOneContractQtyLots};
    // Phase-2 bounded aggression scaffold. Disabled by default until we are ready
    // to convert the harder-leg limit from a pure top-of-book quote into an
    // EV-constrained deeper-through-book IOC price.
    bool bounded_harder_aggression_enabled{true};
    internal::PriceTicks max_harder_aggression_ticks{20};
    std::size_t max_harder_book_levels{3};
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

        // Kalshi emits no WS event at natural close_time; tradeable checks must compare
        // against wall-clock now, not wait for a deactivation message that never arrives.
        const auto now_s = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

        std::optional<GroupSignal> best_signal;
        const std::size_t index = market_index->second;
        if (index > 0) {
            best_signal = evaluate_pair(chain->markets[index - 1], chain->markets[index],
                                        update, now_s);
        }
        if (index + 1 < chain->markets.size()) {
            auto candidate = evaluate_pair(chain->markets[index], chain->markets[index + 1],
                                           update, now_s);
            if (candidate.has_value() &&
                (!best_signal.has_value() || candidate->score > best_signal->score)) {
                //NOLINTNEXTLINE(performance-move-const-arg)
                best_signal = std::move(candidate);
            }
        }

        if (best_signal.has_value()) {
            static_cast<void>(out_signals.try_push_group_signal(*best_signal));
        }
    }

  private:
    struct HarderLegSelection {
        internal::PriceTicks observed_top_bid_ticks{0};
        internal::PriceTicks chosen_limit_ticks{0};
        std::uint16_t scanned_bid_levels{0};
        bool bounded_aggression_applied{false};
    };

    MonotonicArbConfig config_{};
    std::uint64_t next_signal_id_{1};

    [[nodiscard]] static internal::PriceTicks fee_ticks_(
        //NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
        double fee_rate,
        internal::PriceTicks price_ticks,
        internal::QtyLots qty) noexcept {
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

    [[nodiscard]] static internal::PriceTicks taker_fee_ticks_(
        internal::PriceTicks price_ticks,
        internal::QtyLots qty) noexcept {
        return fee_ticks_(kTakerFeeRate, price_ticks, qty);
    }

    [[nodiscard]] static internal::PriceTicks maker_fee_ticks_(
        internal::PriceTicks price_ticks,
        internal::QtyLots qty) noexcept {
        return fee_ticks_(kMakerFeeRate, price_ticks, qty);
    }

    [[nodiscard]] static std::optional<internal::PriceTicks> best_bid_ticks_(
        const EventMarketView& market) noexcept {
        return market.depth.bids[0].price_ticks;
    }

    [[nodiscard]] static std::optional<internal::QtyLots> best_bid_qty_(
        const EventMarketView& market) noexcept {
        return market.depth.bids[0].qty_lots;
    }

    [[nodiscard]] static std::optional<internal::PriceTicks> best_ask_ticks_(
        const EventMarketView& market) noexcept {
        return market.depth.asks[0].price_ticks;
    }

    [[nodiscard]] static std::optional<internal::QtyLots> best_ask_qty_(
        const EventMarketView& market) noexcept {
        return market.depth.asks[0].qty_lots;
    }

    [[nodiscard]] internal::PriceTicks net_edge_ticks_for_pair_(
        internal::PriceTicks easier_buy_ticks,
        internal::PriceTicks harder_sell_ticks,
        internal::QtyLots executable_qty) const noexcept {
        if (easier_buy_ticks <= 0 || harder_sell_ticks <= easier_buy_ticks || executable_qty <= 0) {
            return std::numeric_limits<internal::PriceTicks>::min();
        }

        const internal::PriceTicks gross_edge_per_contract = harder_sell_ticks - easier_buy_ticks;
        const internal::PriceTicks gross_edge_ticks =
            static_cast<internal::PriceTicks>(
                internal::scale_ticks_by_qty_floor(gross_edge_per_contract, executable_qty));
        const internal::PriceTicks total_fees_ticks =
            taker_fee_ticks_(easier_buy_ticks, executable_qty) +
            taker_fee_ticks_(harder_sell_ticks, executable_qty);
        return gross_edge_ticks - total_fees_ticks;
    }

    [[nodiscard]] HarderLegSelection select_harder_sell_limit_(
        internal::PriceTicks easier_ask_ticks,
        const EventMarketView& harder_market,
        internal::PriceTicks top_bid_ticks,
        internal::QtyLots executable_qty) const noexcept {
        HarderLegSelection selection{
            .observed_top_bid_ticks = top_bid_ticks,
            .chosen_limit_ticks = top_bid_ticks,
            .scanned_bid_levels = 1,
            .bounded_aggression_applied = false,
        };

        if (!config_.bounded_harder_aggression_enabled || config_.max_harder_book_levels <= 1 ||
            config_.max_harder_aggression_ticks <= 0) {
            return selection;
        }

        internal::QtyLots cumulative_qty = 0;
        const std::size_t max_levels = std::min<std::size_t>(
            config_.max_harder_book_levels,
            harder_market.depth.bids.size());
        for (std::size_t level_index = 0; level_index < max_levels; ++level_index) {
            const auto& level = harder_market.depth.bids[level_index];
            if (!level.price_ticks.has_value() || !level.qty_lots.has_value() ||
                *level.qty_lots <= 0) {
                break;
            }

            selection.scanned_bid_levels =
                static_cast<std::uint16_t>(level_index + 1);
            cumulative_qty += *level.qty_lots;

            const internal::PriceTicks candidate_ticks = *level.price_ticks;
            const internal::PriceTicks aggression_ticks = top_bid_ticks - candidate_ticks;
            if (aggression_ticks < 0 ||
                aggression_ticks > config_.max_harder_aggression_ticks) {
                break;
            }

            if (config_.require_full_harder_depth_for_qty &&
                cumulative_qty < executable_qty) {
                continue;
            }

            const internal::PriceTicks candidate_net_edge_ticks = net_edge_ticks_for_pair_(
                easier_ask_ticks,
                candidate_ticks,
                executable_qty);
            if (candidate_net_edge_ticks < config_.min_net_edge_ticks) {
                continue;
            }

            selection.chosen_limit_ticks = candidate_ticks;
            selection.bounded_aggression_applied =
                selection.chosen_limit_ticks != selection.observed_top_bid_ticks;
        }

        return selection;
    }

    [[nodiscard]] std::optional<GroupSignal> evaluate_pair(
        const ChainEntry& easier,
        const ChainEntry& harder,
        const AppliedEventUpdate& update,
        std::uint64_t now_s) noexcept {
        if (!easier.market.has_book || !harder.market.has_book ||
            easier.market.desynced || harder.market.desynced ||
            !easier.market.lifecycle.is_tradeable_at(now_s) ||
            !harder.market.lifecycle.is_tradeable_at(now_s)) {
            return std::nullopt;
        }

        const auto easier_ask = best_ask_ticks_(easier.market);
        const auto easier_ask_qty = best_ask_qty_(easier.market);
        const auto harder_bid = best_bid_ticks_(harder.market);
        const auto harder_bid_qty = best_bid_qty_(harder.market);
        if (!easier_ask.has_value() || !easier_ask_qty.has_value() ||
            !harder_bid.has_value() || !harder_bid_qty.has_value()) {
            return std::nullopt;
        }

        const internal::QtyLots executable_qty = std::min(
            {config_.default_order_qty_lots, *easier_ask_qty, *harder_bid_qty});
        if (executable_qty <= 0) {
            return std::nullopt;
        }

        const auto harder_selection =
            select_harder_sell_limit_(*easier_ask, harder.market, *harder_bid, executable_qty);
        const internal::PriceTicks net_edge_ticks = net_edge_ticks_for_pair_(
            *easier_ask,
            harder_selection.chosen_limit_ticks,
            executable_qty);
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
        signal.reference_depth_levels = 1;
        signal.aux_reference_depth_levels = harder_selection.scanned_bid_levels;
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
            .limit_price_ticks = easier_ask,
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

}  // namespace predex::core::shards::kalshi::strategies
