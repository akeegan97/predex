#include <gtest/gtest.h>

#include "predex/research/market_state.hpp"

namespace {

using predex::research::BookDelta;
using predex::research::BookLevel;
using predex::research::BookSnapshot;
using predex::research::MarketAvailability;
using predex::research::MarketState;
using predex::research::ReplayStamp;

constexpr std::uint32_t kEventId = 1;
constexpr std::uint32_t kMarketId = 2;
constexpr std::uint64_t kSnapshotSequence = 10;
constexpr std::uint64_t kDeltaSequence = 11;
constexpr std::uint64_t kTimestampScaleNs = 1'000;
constexpr std::int64_t kBidPriceTicks = 4'500;
constexpr std::int64_t kBetterBidPriceTicks = 4'600;
constexpr std::int64_t kAskPriceTicks = 5'500;
constexpr std::int64_t kWorseAskPriceTicks = 5'600;
constexpr std::int64_t kOneContractLots = 100;

BookSnapshot snapshot(std::uint64_t sequence = kSnapshotSequence) {
    return BookSnapshot{
        .stamp = ReplayStamp{
            .record_index = sequence,
            .recv_ts_ns = sequence * kTimestampScaleNs,
        },
        .run_id = "fixture",
        .sequence = sequence,
        .event_id = kEventId,
        .market_id = kMarketId,
        .bid_levels = {{.price_ticks = kBidPriceTicks, .qty_lots = kOneContractLots}},
        .ask_levels = {{.price_ticks = kAskPriceTicks, .qty_lots = kOneContractLots}},
    };
}

BookDelta delta(std::uint64_t sequence,
                std::string side,
                std::int64_t price_ticks,
                std::int64_t delta_qty_lots) {
    return BookDelta{
        .stamp = ReplayStamp{
            .record_index = sequence,
            .recv_ts_ns = sequence * kTimestampScaleNs,
        },
        .run_id = "fixture",
        .sequence = sequence,
        .event_id = kEventId,
        .market_id = kMarketId,
        .side = std::move(side),
        .price_ticks = price_ticks,
        .delta_qty_lots = delta_qty_lots,
    };
}

TEST(ResearchMarketState, SnapshotEstablishesBboAndQuoteEligibility) {
    MarketState state;

    const auto update = state.apply_snapshot(snapshot());
    EXPECT_EQ(update.availability, MarketAvailability::kAvailable);
    EXPECT_FALSE(update.snapshot_replaced);
    EXPECT_TRUE(state.quote_eligible(kMarketId));

    const auto bbo = state.bbo(kMarketId);
    ASSERT_TRUE(bbo.has_value());
    EXPECT_EQ(bbo->bid_ticks, kBidPriceTicks);
    EXPECT_EQ(bbo->ask_ticks, kAskPriceTicks);
    EXPECT_EQ(bbo->bid_qty_lots, kOneContractLots);
    EXPECT_EQ(bbo->ask_qty_lots, kOneContractLots);
    EXPECT_EQ(bbo->midpoint_ticks(), (kBidPriceTicks + kAskPriceTicks) / 2);
}

TEST(ResearchMarketState, DeltaRecalculatesChangedTouch) {
    MarketState state;
    state.apply_snapshot(snapshot());

    const auto update = state.apply_delta(delta(
        kDeltaSequence, "bid", kBetterBidPriceTicks, kOneContractLots));
    EXPECT_EQ(update.availability, MarketAvailability::kAvailable);

    const auto bbo = state.bbo(kMarketId);
    ASSERT_TRUE(bbo.has_value());
    EXPECT_EQ(bbo->bid_ticks, kBetterBidPriceTicks);
    EXPECT_EQ(bbo->ask_ticks, kAskPriceTicks);
}

TEST(ResearchMarketState, SnapshotReplacesOldDisplayedDepth) {
    MarketState state;
    state.apply_snapshot(snapshot());
    auto replacement = snapshot(kDeltaSequence);
    replacement.bid_levels = {{
        .price_ticks = kBetterBidPriceTicks,
        .qty_lots = kOneContractLots,
    }};
    replacement.ask_levels = {{
        .price_ticks = kWorseAskPriceTicks,
        .qty_lots = kOneContractLots,
    }};

    const auto update = state.apply_snapshot(replacement);
    EXPECT_TRUE(update.snapshot_replaced);
    const auto bbo = state.bbo(kMarketId);
    ASSERT_TRUE(bbo.has_value());
    EXPECT_EQ(bbo->bid_ticks, kBetterBidPriceTicks);
    EXPECT_EQ(bbo->ask_ticks, kWorseAskPriceTicks);
}

TEST(ResearchMarketState, DeltaBeforeSnapshotFailsClosed) {
    MarketState state;

    const auto update = state.apply_delta(delta(
        kDeltaSequence, "bid", kBidPriceTicks, kOneContractLots));
    EXPECT_EQ(update.availability, MarketAvailability::kDesynced);
    EXPECT_TRUE(update.became_desynced);
    EXPECT_FALSE(state.quote_eligible(kMarketId));
    EXPECT_FALSE(state.bbo(kMarketId).has_value());
}

TEST(ResearchMarketState, NegativeDepthUnderflowMarksOnlyMarketDesynced) {
    MarketState state;
    state.apply_snapshot(snapshot());

    const auto update = state.apply_delta(delta(
        kDeltaSequence, "bid", kBidPriceTicks, -2 * kOneContractLots));
    EXPECT_EQ(update.availability, MarketAvailability::kDesynced);
    EXPECT_TRUE(update.became_desynced);
    EXPECT_FALSE(state.quote_eligible(kMarketId));
}

TEST(ResearchMarketState, NonIncreasingDeltaSequenceMarksMarketDesynced) {
    MarketState state;
    state.apply_snapshot(snapshot());

    const auto update = state.apply_delta(delta(
        kSnapshotSequence, "bid", kBidPriceTicks, kOneContractLots));
    EXPECT_EQ(update.availability, MarketAvailability::kDesynced);
    EXPECT_TRUE(update.became_desynced);
}

TEST(ResearchMarketState, SnapshotRestoresEligibilityAfterDesync) {
    MarketState state;
    state.apply_delta(delta(
        kDeltaSequence, "bid", kBidPriceTicks, kOneContractLots));
    ASSERT_EQ(state.availability(kMarketId), MarketAvailability::kDesynced);

    const auto update = state.apply_snapshot(snapshot());
    EXPECT_EQ(update.availability, MarketAvailability::kAvailable);
    EXPECT_TRUE(state.quote_eligible(kMarketId));
}

TEST(ResearchMarketState, CloseRemovesDisplayedBookAndEligibility) {
    MarketState state;
    state.apply_snapshot(snapshot());

    const auto update = state.close_market(kMarketId);
    EXPECT_EQ(update.availability, MarketAvailability::kClosed);
    EXPECT_FALSE(state.quote_eligible(kMarketId));
    EXPECT_FALSE(state.bbo(kMarketId).has_value());
}

} // namespace
