#include <gtest/gtest.h>

#include "predex/research/simulated_venue.hpp"

namespace {

using predex::research::OrderTerminalReason;
using predex::research::QuoteSide;
using predex::research::ReplayStamp;
using predex::research::SimulatedVenue;
using predex::research::SimulatedVenueConfig;

constexpr std::uint32_t kEventId = 1;
constexpr std::uint32_t kMarketId = 2;
constexpr std::int64_t kBidPriceTicks = 4'500;
constexpr std::int64_t kAskPriceTicks = 5'500;
constexpr std::int64_t kOneContractLots = 100;
constexpr std::int64_t kTwoContractLots = 200;
constexpr std::int64_t kTradeLots = 150;
constexpr std::int64_t kCancellationLots = 60;
constexpr std::int64_t kLargeQueueAheadLots = 50'000;
constexpr std::uint64_t kEntryRecordIndex = 10;
constexpr std::uint64_t kFirstTradeRecordIndex = 11;
constexpr std::uint64_t kSecondTradeRecordIndex = 12;
constexpr std::uint64_t kEntryRecvTsNs = 1'000'000'000ULL;
constexpr std::uint64_t kCancelLatencyNs = 100'000'000ULL;

ReplayStamp stamp(std::uint64_t record_index, std::uint64_t recv_ts_ns) {
    return ReplayStamp{
        .record_index = record_index,
        .recv_ts_ns = recv_ts_ns,
    };
}

SimulatedVenue venue(double cancel_ahead_weight = 0.0) {
    return SimulatedVenue{SimulatedVenueConfig{
        .order_qty_lots = kOneContractLots,
        .cancel_ahead_weight = cancel_ahead_weight,
        .cancel_latency_ns = kCancelLatencyNs,
    }};
}

TEST(ResearchSimulatedVenue, EqualPriceTradeConsumesQueueAheadBeforeOrder) {
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        kTwoContractLots, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    EXPECT_TRUE(simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kTradeLots,
        stamp(kFirstTradeRecordIndex, kEntryRecvTsNs + 10)).empty());
    EXPECT_DOUBLE_EQ(order.queue_ahead_lots, 50.0);
    EXPECT_DOUBLE_EQ(order.filled_qty_lots, 0.0);

    const auto fills = simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kTradeLots,
        stamp(kSecondTradeRecordIndex, kEntryRecvTsNs + 20));
    ASSERT_EQ(fills.size(), 1U);
    EXPECT_DOUBLE_EQ(fills.front().filled_qty_lots, kOneContractLots);
    EXPECT_EQ(fills.front().yes_price_ticks, kBidPriceTicks);
    EXPECT_EQ(fills.front().trigger_trade_yes_price_ticks, kBidPriceTicks);
    EXPECT_TRUE(order.fully_filled());
    EXPECT_EQ(order.terminal_reason, OrderTerminalReason::kFilled);
}

TEST(ResearchSimulatedVenue, SamePriceAddJoinsBehindOurOrder) {
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        kOneContractLots, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    simulated_venue.apply_same_price_add(
        kMarketId, QuoteSide::kBidYes, kBidPriceTicks, kTwoContractLots);
    EXPECT_DOUBLE_EQ(order.queue_behind_lots, kTwoContractLots);
}

TEST(ResearchSimulatedVenue, StrictCancellationRemovesBehindBeforeQueueAhead) {
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        kOneContractLots, stamp(kEntryRecordIndex, kEntryRecvTsNs));
    simulated_venue.apply_same_price_add(
        kMarketId, QuoteSide::kBidYes, kBidPriceTicks, kOneContractLots);

    simulated_venue.apply_inferred_cancellation(
        kMarketId, QuoteSide::kBidYes, kBidPriceTicks, kOneContractLots);
    EXPECT_DOUBLE_EQ(order.queue_ahead_lots, kOneContractLots);
    EXPECT_DOUBLE_EQ(order.queue_behind_lots, 0.0);
}

TEST(ResearchSimulatedVenue, CancellationAdvancesWhenThereIsNothingBehind) {
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        kOneContractLots, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    simulated_venue.apply_inferred_cancellation(
        kMarketId, QuoteSide::kBidYes, kBidPriceTicks, kCancellationLots);
    EXPECT_DOUBLE_EQ(order.queue_ahead_lots, 40.0);
}

TEST(ResearchSimulatedVenue, TradeThroughFullyFillsBidRegardlessOfQueue) {
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        kLargeQueueAheadLots, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    const auto fills = simulated_venue.apply_trade(
        kMarketId, 4'400, 10,
        stamp(kFirstTradeRecordIndex, kEntryRecvTsNs + 10));
    ASSERT_EQ(fills.size(), 1U);
    EXPECT_DOUBLE_EQ(fills.front().filled_qty_lots, kOneContractLots);
    EXPECT_EQ(fills.front().yes_price_ticks, kBidPriceTicks);
    EXPECT_EQ(fills.front().trigger_trade_yes_price_ticks, 4'400);
    EXPECT_TRUE(order.fully_filled());
    ASSERT_TRUE(order.first_fill_yes_price_ticks.has_value());
    EXPECT_EQ(*order.first_fill_yes_price_ticks, 4'400);
}

TEST(ResearchSimulatedVenue, CancelLeavesOrderFillableUntilEffectiveTime) {
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        0, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    ASSERT_TRUE(simulated_venue.request_cancel(
        kMarketId, QuoteSide::kBidYes, kEntryRecvTsNs));
    simulated_venue.apply_cancel_timers(kEntryRecvTsNs + kCancelLatencyNs - 1);
    EXPECT_TRUE(order.active());

    const auto fills = simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kOneContractLots,
        stamp(kFirstTradeRecordIndex, kEntryRecvTsNs + kCancelLatencyNs - 1));
    ASSERT_EQ(fills.size(), 1U);
    EXPECT_TRUE(order.fully_filled());
}

TEST(ResearchSimulatedVenue, CancelPullsOrderAtEffectiveTime) {
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        0, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    ASSERT_TRUE(simulated_venue.request_cancel(
        kMarketId, QuoteSide::kBidYes, kEntryRecvTsNs));
    simulated_venue.apply_cancel_timers(kEntryRecvTsNs + kCancelLatencyNs);
    EXPECT_EQ(order.terminal_reason, OrderTerminalReason::kCancelled);
    EXPECT_TRUE(simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kOneContractLots,
        stamp(kFirstTradeRecordIndex, kEntryRecvTsNs + kCancelLatencyNs + 1)).empty());
}

TEST(ResearchSimulatedVenue, RejectsASecondLiveOrderOnTheSameMarketSide) {
    auto simulated_venue = venue();
    simulated_venue.place_order(
        "first", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        0, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    EXPECT_THROW(
        simulated_venue.place_order(
            "second", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
            0, stamp(kFirstTradeRecordIndex, kEntryRecvTsNs + 1)),
        std::logic_error);
}

TEST(ResearchSimulatedVenue, DrainingTerminalOrderPreservesLifecycleBeforeReplacement) {
    auto simulated_venue = venue();
    simulated_venue.place_order(
        "first", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        0, stamp(kEntryRecordIndex, kEntryRecvTsNs));
    const auto fills = simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kOneContractLots,
        stamp(kFirstTradeRecordIndex, kEntryRecvTsNs + 1));
    ASSERT_EQ(fills.size(), 1U);

    auto terminal = simulated_venue.drain_terminal_orders();
    ASSERT_EQ(terminal.size(), 1U);
    EXPECT_EQ(terminal.front().order_id, "first");
    EXPECT_EQ(terminal.front().terminal_reason, OrderTerminalReason::kFilled);
    EXPECT_EQ(simulated_venue.order(kMarketId, QuoteSide::kBidYes), nullptr);

    simulated_venue.place_order(
        "second", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        0, stamp(kSecondTradeRecordIndex, kEntryRecvTsNs + 2));
    EXPECT_EQ(simulated_venue.total_order_count(), 2U);
}

TEST(ResearchSimulatedVenue, AllowsOpposingOrdersToFillFromOneTradeThrough) {
    auto simulated_venue = venue();
    simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        0, stamp(kEntryRecordIndex, kEntryRecvTsNs));
    simulated_venue.place_order(
        "ask", kEventId, kMarketId, QuoteSide::kAskYes, kAskPriceTicks,
        0, stamp(kEntryRecordIndex, kEntryRecvTsNs));

    const auto fills = simulated_venue.apply_trade(
        kMarketId, 4'400, kOneContractLots,
        stamp(kFirstTradeRecordIndex, kEntryRecvTsNs + 10));
    ASSERT_EQ(fills.size(), 1U);
    const auto* bid = simulated_venue.order(kMarketId, QuoteSide::kBidYes);
    const auto* ask = simulated_venue.order(kMarketId, QuoteSide::kAskYes);
    ASSERT_NE(bid, nullptr);
    ASSERT_NE(ask, nullptr);
    EXPECT_TRUE(bid->fully_filled());
    EXPECT_TRUE(ask->active());
}

} // namespace
