#include <gtest/gtest.h>

#include "predex/research/outcome_tracker.hpp"

namespace {

using namespace predex::research;

FillAccounting bid_fill() {
    return FillAccounting{
        .fill = SimulatedFill{
            .order_id = "order",
            .event_id = 1,
            .market_id = 2,
            .quote_side = QuoteSide::kBidYes,
            .stamp = ReplayStamp{.record_index = 10, .recv_ts_ns = 1'000'000'000},
            .yes_price_ticks = 4'500,
            .filled_qty_lots = 100,
        },
        .position_before = {},
        .position_after = MarketPosition{.signed_yes_contracts = 1, .cash_ticks = -4'500},
    };
}

TEST(ResearchOutcomeTracker, CapturesFirstValidPostHorizonMark) {
    OutcomeTracker tracker;
    tracker.add(bid_fill());
    tracker.observe(
        2,
        ReplayStamp{.record_index = 11, .recv_ts_ns = 1'100'000'001},
        MarketBbo{.bid_ticks = 4'700, .ask_ticks = 4'900});
    tracker.finish();
    const auto completed = tracker.drain_completed();
    ASSERT_EQ(completed.size(), 1U);
    const auto& markout = completed.front().markouts.front();
    EXPECT_TRUE(markout.available);
    EXPECT_EQ(markout.elapsed_ns, 100'000'001U);
    EXPECT_DOUBLE_EQ(markout.midpoint_markout_ticks, 300.0);
    EXPECT_DOUBLE_EQ(markout.executable_unwind_ticks, 200.0);
}

TEST(ResearchOutcomeTracker, EndOfRunLeavesUnobservedHorizonUnavailable) {
    OutcomeTracker tracker;
    tracker.add(bid_fill());
    tracker.finish();
    const auto completed = tracker.drain_completed();
    ASSERT_EQ(completed.size(), 1U);
    EXPECT_FALSE(completed.front().markouts.front().available);
}

} // namespace
