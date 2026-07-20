#include <gtest/gtest.h>

#include "predex/research/replay_engine.hpp"

namespace {

using predex::research::ActivityTracker;
using predex::research::BookDelta;
using predex::research::BookLevel;
using predex::research::BookSnapshot;
using predex::research::FeedEvent;
using predex::research::MarketState;
using predex::research::OrderTerminalReason;
using predex::research::Portfolio;
using predex::research::PublicTrade;
using predex::research::QuoteSide;
using predex::research::ReplayEngine;
using predex::research::ReplayStamp;
using predex::research::SimulatedVenue;
using predex::research::SimulatedVenueConfig;
using predex::research::IResearchStrategy;
using predex::research::DecisionContext;
using predex::research::ActionBuffer;
using predex::research::CancelQuote;
using predex::research::MarketLifecycle;

constexpr std::uint32_t kEventId = 1;
constexpr std::uint32_t kMarketId = 2;
constexpr std::uint64_t kSnapshotSequence = 10;
constexpr std::uint64_t kRemovalSequence = 11;
constexpr std::uint64_t kTradeSequence = 12;
constexpr std::uint64_t kEntryTimeNs = 1'000;
constexpr std::uint64_t kCancelLatencyNs = 100;
constexpr std::uint64_t kNextEventOffsetNs = 10;
constexpr std::int64_t kBidPriceTicks = 4'500;
constexpr std::int64_t kAskPriceTicks = 5'500;
constexpr std::int64_t kOneContractLots = 100;
constexpr std::int64_t kTwoContractLots = 200;

ReplayStamp stamp(std::uint64_t sequence, std::uint64_t recv_ts_ns) {
    return ReplayStamp{
        .record_index = sequence,
        .recv_ts_ns = recv_ts_ns,
    };
}

BookSnapshot snapshot(std::uint64_t sequence = kSnapshotSequence) {
    return BookSnapshot{
        .stamp = stamp(sequence, kEntryTimeNs),
        .run_id = "fixture",
        .sequence = sequence,
        .event_id = kEventId,
        .market_id = kMarketId,
        .bid_levels = {{.price_ticks = kBidPriceTicks, .qty_lots = kOneContractLots}},
        .ask_levels = {{.price_ticks = kAskPriceTicks, .qty_lots = kOneContractLots}},
    };
}

BookDelta removal() {
    return BookDelta{
        .stamp = stamp(kRemovalSequence, kEntryTimeNs + kNextEventOffsetNs),
        .run_id = "fixture",
        .sequence = kRemovalSequence,
        .event_id = kEventId,
        .market_id = kMarketId,
        .side = "bid",
        .price_ticks = kBidPriceTicks,
        .delta_qty_lots = -kOneContractLots,
    };
}

PublicTrade no_trade(std::uint64_t sequence,
                     std::uint64_t recv_ts_ns,
                     std::int64_t qty_lots = kOneContractLots) {
    return PublicTrade{
        .stamp = stamp(sequence, recv_ts_ns),
        .run_id = "fixture",
        .sequence = sequence,
        .event_id = kEventId,
        .market_id = kMarketId,
        .yes_price_ticks = kBidPriceTicks,
        .qty_lots = qty_lots,
        .aggressor = "no",
    };
}

struct EngineFixture {
    MarketState market_state;
    ActivityTracker activity_tracker;
    SimulatedVenue venue{SimulatedVenueConfig{
        .order_qty_lots = kOneContractLots,
        .cancel_ahead_weight = 0.0,
        .cancel_latency_ns = kCancelLatencyNs,
    }};
    Portfolio portfolio;
    ReplayEngine engine{market_state, activity_tracker, venue, portfolio};

    void initialize_and_quote() {
        engine.process(FeedEvent{snapshot()});
        venue.place_order(
            "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
            kOneContractLots, stamp(kSnapshotSequence, kEntryTimeNs));
    }
};

TEST(ResearchReplayEngine, MatchedRemovalDoesNotDoubleAdvanceQueue) {
    EngineFixture fixture;
    fixture.initialize_and_quote();

    fixture.engine.process(FeedEvent{removal()});
    const auto first = fixture.engine.process(FeedEvent{
        no_trade(kTradeSequence, kEntryTimeNs + 20)});
    EXPECT_TRUE(first.fills.empty());

    const auto second = fixture.engine.process(FeedEvent{
        no_trade(kTradeSequence + 1, kEntryTimeNs + 30)});
    ASSERT_EQ(second.fills.size(), 1U);
    EXPECT_DOUBLE_EQ(
        fixture.portfolio.position(kMarketId).signed_yes_contracts, 1.0);
    EXPECT_DOUBLE_EQ(
        fixture.portfolio.position(kMarketId).cash_ticks, -kBidPriceTicks);
}

TEST(ResearchReplayEngine, ReplacementSnapshotInvalidatesLiveQueuePosition) {
    EngineFixture fixture;
    fixture.initialize_and_quote();

    auto replacement = snapshot(kRemovalSequence);
    replacement.stamp.recv_ts_ns = kEntryTimeNs + kNextEventOffsetNs;
    fixture.engine.process(FeedEvent{replacement});

    const auto* order = fixture.venue.order(kMarketId, QuoteSide::kBidYes);
    ASSERT_NE(order, nullptr);
    EXPECT_EQ(order->terminal_reason, OrderTerminalReason::kQueueUnknownSnapshot);
}

TEST(ResearchReplayEngine, TradeAtCancelEffectiveTimeFillsBeforePull) {
    EngineFixture fixture;
    fixture.initialize_and_quote();
    ASSERT_TRUE(fixture.venue.request_cancel(
        kMarketId, QuoteSide::kBidYes, kEntryTimeNs));

    const auto result = fixture.engine.process(FeedEvent{
        no_trade(
            kTradeSequence,
            kEntryTimeNs + kCancelLatencyNs,
            kTwoContractLots)});
    ASSERT_EQ(result.fills.size(), 1U);
    const auto* order = fixture.venue.order(kMarketId, QuoteSide::kBidYes);
    ASSERT_NE(order, nullptr);
    EXPECT_TRUE(order->fully_filled());
}

class CancelBidStrategy final : public IResearchStrategy {
public:
    void on_step(const DecisionContext& context, ActionBuffer& actions) override {
        if(context.bid_order != nullptr && context.bid_order->active() &&
           !context.bid_order->cancel_requested_recv_ts_ns.has_value()) {
            actions.emplace_back(CancelQuote{.quote_side = QuoteSide::kBidYes});
        }
    }
};

TEST(ResearchReplayEngine, StrategyCanRequestDelayedCancellation) {
    MarketState market_state;
    ActivityTracker activity_tracker;
    SimulatedVenue venue{SimulatedVenueConfig{
        .order_qty_lots = kOneContractLots,
        .cancel_latency_ns = kCancelLatencyNs,
    }};
    Portfolio portfolio;
    CancelBidStrategy strategy;
    ReplayEngine engine{market_state, activity_tracker, venue, portfolio, &strategy};
    market_state.apply_snapshot(snapshot());
    venue.place_order(
        "bid", kEventId, kMarketId, QuoteSide::kBidYes, kBidPriceTicks,
        0, stamp(kSnapshotSequence, kEntryTimeNs));

    const auto result = engine.process(FeedEvent{removal()});
    ASSERT_EQ(result.cancellations.size(), 1U);
    const auto* order = venue.order(kMarketId, QuoteSide::kBidYes);
    ASSERT_NE(order, nullptr);
    EXPECT_TRUE(order->cancel_effective_recv_ts_ns.has_value());
}

TEST(ResearchReplayEngine, DeterminedLifecycleClosesMarketAndOrders) {
    EngineFixture fixture;
    fixture.initialize_and_quote();
    const MarketLifecycle lifecycle{
        .stamp = stamp(kTradeSequence, kEntryTimeNs + 20),
        .run_id = "fixture",
        .sequence = kTradeSequence,
        .event_id = kEventId,
        .market_id = kMarketId,
        .msg_json = "{\"event_type\":\"determined\"}",
    };

    fixture.engine.process(FeedEvent{lifecycle});
    EXPECT_FALSE(fixture.market_state.quote_eligible(kMarketId));
    const auto* order = fixture.venue.order(kMarketId, QuoteSide::kBidYes);
    ASSERT_NE(order, nullptr);
    EXPECT_EQ(order->terminal_reason, OrderTerminalReason::kMarketClosed);
}

} // namespace
