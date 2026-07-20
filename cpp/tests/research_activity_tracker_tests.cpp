#include <gtest/gtest.h>

#include "predex/research/activity_tracker.hpp"
#include "predex/research/simulated_venue.hpp"

namespace {

using predex::research::ActivityBookSide;
using predex::research::ActivityTracker;
using predex::research::BookDelta;
using predex::research::DepthRemovalMatchConfig;
using predex::research::PublicTrade;
using predex::research::QuoteSide;
using predex::research::ReplayStamp;
using predex::research::SimulatedVenue;
using predex::research::SimulatedVenueConfig;

constexpr std::uint32_t kEventId = 1;
constexpr std::uint32_t kMarketId = 2;
constexpr std::uint64_t kEntryRecordIndex = 10;
constexpr std::uint64_t kRemovalRecordIndex = 11;
constexpr std::uint64_t kTradeRecordIndex = 12;
constexpr std::uint64_t kExpiryRecordIndex = 20;
constexpr std::uint64_t kEntryTimeNs = 1'000;
constexpr std::uint64_t kExpiryTimeNs = 2'000;
constexpr std::uint64_t kRemovalOffsetNs = 10;
constexpr std::uint64_t kTradeOffsetNs = 20;
constexpr std::uint64_t kMaxRecordGap = 8;
constexpr std::uint64_t kMaxTimeGapNs = 100;
constexpr std::int64_t kBidPriceTicks = 4'500;
constexpr std::int64_t kOneContractLots = 100;

ReplayStamp stamp(std::uint64_t record_index, std::uint64_t recv_ts_ns) {
    return ReplayStamp{
        .record_index = record_index,
        .recv_ts_ns = recv_ts_ns,
    };
}

BookDelta removal() {
    return BookDelta{
        .stamp = stamp(kRemovalRecordIndex, kEntryTimeNs + kRemovalOffsetNs),
        .run_id = "fixture",
        .sequence = kRemovalRecordIndex,
        .event_id = kEventId,
        .market_id = kMarketId,
        .side = "bid",
        .price_ticks = kBidPriceTicks,
        .delta_qty_lots = -kOneContractLots,
    };
}

PublicTrade no_trade() {
    return PublicTrade{
        .stamp = stamp(kTradeRecordIndex, kEntryTimeNs + kTradeOffsetNs),
        .run_id = "fixture",
        .sequence = kTradeRecordIndex,
        .event_id = kEventId,
        .market_id = kMarketId,
        .yes_price_ticks = kBidPriceTicks,
        .qty_lots = kOneContractLots,
        .aggressor = "no",
    };
}

SimulatedVenue venue() {
    return SimulatedVenue{SimulatedVenueConfig{
        .order_qty_lots = kOneContractLots,
        .cancel_ahead_weight = 0.0,
    }};
}

TEST(ResearchActivityTracker, MatchedRemovalDoesNotAdvanceQueueTwice) {
    ActivityTracker tracker{DepthRemovalMatchConfig{
        .max_record_gap = kMaxRecordGap,
        .max_time_gap_ns = kMaxTimeGapNs,
    }};
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        kOneContractLots, stamp(kEntryRecordIndex, kEntryTimeNs));

    tracker.observe_delta_removal(removal());
    const auto matched = tracker.match_trade(no_trade());
    ASSERT_EQ(matched.size(), 1U);
    EXPECT_TRUE(matched.front().confirmed_execution);
    EXPECT_EQ(matched.front().key.side, ActivityBookSide::kBid);
    EXPECT_EQ(matched.front().key.price_ticks, kBidPriceTicks);

    EXPECT_TRUE(simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kOneContractLots, no_trade().stamp).empty());
    EXPECT_DOUBLE_EQ(order.queue_ahead_lots, 0.0);
    EXPECT_TRUE(tracker.expire_before(
        stamp(kExpiryRecordIndex, kExpiryTimeNs)).empty());

    const auto fills = simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kOneContractLots,
        stamp(kExpiryRecordIndex + 1, kExpiryTimeNs + 1));
    ASSERT_EQ(fills.size(), 1U);
}

TEST(ResearchActivityTracker, ExpiredRemovalAdvancesOnlyMatchingQueue) {
    ActivityTracker tracker{DepthRemovalMatchConfig{
        .max_record_gap = kMaxRecordGap,
        .max_time_gap_ns = kMaxTimeGapNs,
    }};
    auto simulated_venue = venue();
    auto& order = simulated_venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        kOneContractLots, stamp(kEntryRecordIndex, kEntryTimeNs));

    tracker.observe_delta_removal(removal());
    const auto expired = tracker.expire_before(
        stamp(kExpiryRecordIndex, kExpiryTimeNs));
    ASSERT_EQ(expired.size(), 1U);
    EXPECT_FALSE(expired.front().confirmed_execution);
    EXPECT_EQ(expired.front().key.market_index, kMarketId);

    simulated_venue.apply_inferred_cancellation(
        expired.front().key.market_index,
        QuoteSide::kBidYes,
        expired.front().key.price_ticks,
        expired.front().qty_lots);
    EXPECT_DOUBLE_EQ(order.queue_ahead_lots, 0.0);
    const auto fills = simulated_venue.apply_trade(
        kMarketId, kBidPriceTicks, kOneContractLots,
        stamp(kTradeRecordIndex, kExpiryTimeNs + 1));
    ASSERT_EQ(fills.size(), 1U);
}

TEST(ResearchActivityTracker, SnapshotResetDiscardsPendingMarketRemoval) {
    ActivityTracker tracker;
    tracker.observe_delta_removal(removal());

    tracker.discard_market(kMarketId);
    EXPECT_TRUE(tracker.expire_before(
        stamp(kExpiryRecordIndex, kExpiryTimeNs), true).empty());
}

} // namespace
