#include <gtest/gtest.h>

#include "predex/research/activity_features.hpp"

#include <array>
#include <cstdint>

namespace {

using predex::research::ActivitySignal;
using predex::research::ActivityBookSide;
using predex::research::DepthRemovalMatchConfig;
using predex::research::DepthRemovalReconciler;
using predex::research::activity_acceleration_proxy;
using predex::research::activity_rate_per_second;
using predex::research::summarize_activity_windows;

TEST(ResearchActivityFeatures, SummarizesNestedCausalWindows) {
    constexpr std::uint64_t now = 30'000'000'000ULL;
    const std::array signals{
        ActivitySignal{
            .recv_ts_ns = now - 50'000'000ULL,
            .market_index = 2,
            .trade_qty_lots = 100,
            .signed_yes_trade_qty_lots = 100,
            .bid_quote_delta_qty_lots = -40,
        },
        ActivitySignal{
            .recv_ts_ns = now - 500'000'000ULL,
            .market_index = 1,
            .trade_qty_lots = 200,
            .signed_yes_trade_qty_lots = -200,
            .ask_quote_delta_qty_lots = -60,
        },
        ActivitySignal{
            .recv_ts_ns = now - 4'000'000'000ULL,
            .market_index = 4,
            .trade_qty_lots = 300,
            .signed_yes_trade_qty_lots = 300,
            .bid_quote_delta_qty_lots = 75,
        },
    };

    const auto summary = summarize_activity_windows(signals, 2, now);

    EXPECT_EQ(summary[0].chain.trade_qty_lots, 100);
    EXPECT_EQ(summary[0].target.signed_yes_trade_qty_lots, 100);
    EXPECT_EQ(summary[0].target.bid_quote_delta_qty_lots, -40);
    EXPECT_EQ(summary[0].neighbor.trade_qty_lots, 0);

    EXPECT_EQ(summary[1].chain.trade_qty_lots, 300);
    EXPECT_EQ(summary[1].target.trade_qty_lots, 100);
    EXPECT_EQ(summary[1].neighbor.trade_qty_lots, 200);
    EXPECT_EQ(summary[1].neighbor.ask_quote_delta_qty_lots, -60);

    EXPECT_EQ(summary[2].chain.trade_qty_lots, 600);
    EXPECT_EQ(summary[2].target.trade_qty_lots, 100);
    EXPECT_EQ(summary[2].neighbor.trade_qty_lots, 200);
}

TEST(ResearchActivityFeatures, ExcludesFutureAndExpiredSignals) {
    constexpr std::uint64_t now = 50'000'000'000ULL;
    const std::array signals{
        ActivitySignal{
            .recv_ts_ns = now + 1,
            .market_index = 0,
            .trade_qty_lots = 100,
        },
        ActivitySignal{
            .recv_ts_ns = now - 30'000'000'001ULL,
            .market_index = 0,
            .trade_qty_lots = 200,
        },
    };

    const auto summary = summarize_activity_windows(signals, 0, now);

    for(const auto& window : summary){
        EXPECT_EQ(window.chain.trade_qty_lots, 0);
        EXPECT_EQ(window.target.trade_qty_lots, 0);
    }
}

TEST(ResearchActivityFeatures, ComputesRateAndAccelerationFromNestedWindows) {
    EXPECT_DOUBLE_EQ(activity_rate_per_second(100, 100'000'000ULL), 1000.0);
    EXPECT_DOUBLE_EQ(activity_rate_per_second(200, 1'000'000'000ULL), 200.0);
    EXPECT_DOUBLE_EQ(
        activity_acceleration_proxy(
            100,
            100'000'000ULL,
            200,
            1'000'000'000ULL),
        800.0);
}

TEST(ResearchActivityFeatures, ReconcilesPartialTradeAndExpiresResidualRemoval) {
    DepthRemovalReconciler reconciler{DepthRemovalMatchConfig{
        .max_record_gap = 8,
        .max_time_gap_ns = 100'000'000ULL,
    }};
    reconciler.observe_removal(
        3,
        ActivityBookSide::kAsk,
        61,
        100,
        10,
        1'000'000'000ULL);

    auto pending = reconciler.pending_signals();
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].ask_pending_depletion_qty_lots, 100);

    const auto confirmed = reconciler.match_trade(
        3,
        ActivityBookSide::kAsk,
        61,
        40);
    ASSERT_EQ(confirmed.size(), 1);
    EXPECT_EQ(confirmed[0].recv_ts_ns, 1'000'000'000ULL);
    EXPECT_EQ(confirmed[0].ask_confirmed_execution_depletion_qty_lots, 40);

    pending = reconciler.pending_signals();
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].ask_pending_depletion_qty_lots, 60);

    EXPECT_TRUE(reconciler.expire_before(18, 1'100'000'000ULL).empty());
    const auto inferred = reconciler.expire_before(19, 1'100'000'001ULL);
    ASSERT_EQ(inferred.size(), 1);
    EXPECT_EQ(inferred[0].ask_inferred_nontrade_depletion_qty_lots, 60);
    EXPECT_TRUE(reconciler.pending_signals().empty());
}

TEST(ResearchActivityFeatures, MatchesOnlyExactMarketSideAndPrice) {
    DepthRemovalReconciler reconciler;
    reconciler.observe_removal(
        2,
        ActivityBookSide::kBid,
        40,
        25,
        100,
        2'000'000'000ULL);

    EXPECT_TRUE(reconciler.match_trade(
        3, ActivityBookSide::kBid, 40, 25).empty());
    EXPECT_TRUE(reconciler.match_trade(
        2, ActivityBookSide::kAsk, 40, 25).empty());
    EXPECT_TRUE(reconciler.match_trade(
        2, ActivityBookSide::kBid, 41, 25).empty());

    const auto confirmed = reconciler.match_trade(
        2, ActivityBookSide::kBid, 40, 25);
    ASSERT_EQ(confirmed.size(), 1);
    EXPECT_EQ(confirmed[0].bid_confirmed_execution_depletion_qty_lots, 25);
    EXPECT_TRUE(reconciler.pending_signals().empty());
}

TEST(ResearchActivityFeatures, DiscardsOnlySnapshotMarketPendingRemoval) {
    DepthRemovalReconciler reconciler;
    reconciler.observe_removal(
        1, ActivityBookSide::kBid, 39, 10, 1, 1'000ULL);
    reconciler.observe_removal(
        2, ActivityBookSide::kAsk, 61, 20, 2, 2'000ULL);

    reconciler.discard_market(1);

    const auto pending = reconciler.pending_signals();
    ASSERT_EQ(pending.size(), 1);
    EXPECT_EQ(pending[0].market_index, 2);
    EXPECT_EQ(pending[0].ask_pending_depletion_qty_lots, 20);
}

} // namespace
