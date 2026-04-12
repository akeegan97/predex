#pragma once

#include <algorithm>
#include <cmath>
#include <optional>

#include "predex/internal/market_types.hpp"
#include "predex/shards/applied_event_update.hpp"
#include "predex/shards/signal_types.hpp"

namespace predex::core::shards::kalshi::strategies {
inline constexpr double kPriceScale = 10000.0;
inline constexpr double kCentScale = 100.0;
inline constexpr internal::PriceTicks kTicksPerCent = 100;
inline constexpr double kTakerFeeRate = 0.07;
inline constexpr double kMakerFeeRate = 0.0175;

struct MonotonicArbConfig {
    std::int64_t min_net_edge_ticks{200};
    internal::QtyLots default_order_qty_lots{1};
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

        std::optional<GroupSignal> best_signal;
        const std::size_t index = market_index->second;
        if (index > 0) {
            best_signal = evaluate_pair(chain->markets[index - 1], chain->markets[index], update);
        }
        if (index + 1 < chain->markets.size()) {
            auto candidate = evaluate_pair(chain->markets[index], chain->markets[index + 1], update);
            if (candidate.has_value() &&
                (!best_signal.has_value() || candidate->score > best_signal->score)) {
                best_signal = std::move(candidate);
            }
        }

        if (best_signal.has_value()) {
            static_cast<void>(out_signals.try_push_group_signal(*best_signal));
        }
    }

  private:
    MonotonicArbConfig config_{};
    std::uint64_t next_signal_id_{1};

    [[nodiscard]] static internal::PriceTicks fee_ticks_(
        double fee_rate,
        internal::PriceTicks price_ticks,
        internal::QtyLots qty) noexcept {
        if (qty <= 0 || price_ticks <= 0 ||
            price_ticks >= static_cast<internal::PriceTicks>(kPriceScale)) {
            return 0;
        }

        const double p_dollars = static_cast<double>(price_ticks) / kPriceScale;
        const double fee_dollars =
            fee_rate * static_cast<double>(qty) * p_dollars * (1.0 - p_dollars);
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

    [[nodiscard]] std::optional<GroupSignal> evaluate_pair(
        const ChainEntry& easier,
        const ChainEntry& harder,
        const AppliedEventUpdate& update) noexcept {
        if (!easier.market.has_book || !harder.market.has_book ||
            easier.market.desynced || harder.market.desynced) {
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

        const internal::PriceTicks gross_edge_per_contract = *harder_bid - *easier_ask;
        if (gross_edge_per_contract <= 0) {
            return std::nullopt;
        }

        const internal::PriceTicks gross_edge_ticks =
            gross_edge_per_contract * executable_qty;
        const internal::PriceTicks total_fees_ticks =
            taker_fee_ticks_(*easier_ask, executable_qty) +
            taker_fee_ticks_(*harder_bid, executable_qty);
        const internal::PriceTicks net_edge_ticks =
            gross_edge_ticks - total_fees_ticks;
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
        signal.signal_ts_ns = update.update.meta.recv_ns;
        signal.edge_ticks = net_edge_ticks;
        signal.score = net_edge_ticks;

        signal.legs[0] = SubmissionLeg{
            .market_id = easier.market.market_id,
            .side = internal::Side::kBuy,
            .qty_lots = executable_qty,
            .limit_price_ticks = easier_ask,
            .time_in_force = predex::core::oms::kalshi::OmsTimeInForce::kIoc,
        };
        signal.legs[1] = SubmissionLeg{
            .market_id = harder.market.market_id,
            .side = internal::Side::kSell,
            .qty_lots = executable_qty,
            .limit_price_ticks = harder_bid,
            .time_in_force = predex::core::oms::kalshi::OmsTimeInForce::kIoc,
        };
        return signal;
    }
};

}  // namespace predex::core::shards::kalshi::strategies
