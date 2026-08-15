#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "predex/shard/event.hpp"
#include "predex/shard/event_store.hpp"

namespace {

using predex::shard::ApplyDisposition;
using predex::shard::BookSyncState;
using predex::shard::BookSyncTransition;
using predex::shard::Event;
using predex::shard::EventStore;
using predex::shard::InvalidationRejectReason;
using predex::shard::KalshiDeltaData;
using predex::shard::KalshiEvent;
using predex::shard::KalshiMarket;
using predex::shard::KalshiParsedEvent;
using predex::shard::KalshiSnapshotEvent;
using predex::shard::Level;
using predex::shard::MarketApplyReason;
using predex::shard::MarketScale;
using predex::shard::QtyLots;
using predex::shard::Side;
using predex::ingest::kalshi::BookInvalidationReason;

constexpr std::uint32_t kMarketIndex = 0;
constexpr predex::shard::PriceTicks kBidPrice = 4'000;
constexpr predex::shard::PriceTicks kAskPrice = 6'000;

KalshiMarket linear_market(std::uint32_t market_id, std::uint32_t market_index) {
    KalshiMarket market{};
    market.market_id = market_id;
    market.event_market_index = market_index;
    market.book.scale = MarketScale::kLINEAR_CENTS;
    return market;
}

Event single_market_event(MarketScale scale = MarketScale::kLINEAR_CENTS) {
    auto market = linear_market(101, kMarketIndex);
    market.book.scale = scale;

    KalshiEvent event{};
    event.event_id = 11;
    event.shard_event_index = 0;
    event.markets.push_back(std::move(market));
    return Event{std::move(event)};
}

KalshiSnapshotEvent snapshot(
    std::vector<Level> bids = {{kBidPrice, 10}},
    std::vector<Level> asks = {{kAskPrice, 12}}) {
    return KalshiSnapshotEvent{
        .bids = std::move(bids),
        .asks = std::move(asks),
    };
}

const predex::shard::KalshiBook& book(const Event& event, std::uint32_t market_index = 0) {
    const auto* market = event.get_market(market_index);
    EXPECT_NE(market, nullptr);
    return market->book;
}

QtyLots quantity_at(
    const predex::shard::KalshiBook& target,
    Side side,
    predex::shard::PriceTicks price) {
    const auto index = target.get_index(price);
    EXPECT_TRUE(index.has_value());
    if (!index.has_value()) {
        return 0;
    }
    return side == Side::kBID ? target.bids[*index] : target.asks[*index];
}

TEST(EventBookSyncTest, InitialAndReplacementSnapshotsInstallCompleteBooks) {
    auto event = single_market_event();

    const auto initial = event.apply(kMarketIndex, KalshiParsedEvent{snapshot()});
    EXPECT_EQ(initial.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(initial.book_sync_transition, BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED);
    EXPECT_EQ(initial.reason, MarketApplyReason::kNONE);
    EXPECT_TRUE(event.usable());
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 10U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 12U);

    const auto replacement = event.apply(
        kMarketIndex,
        KalshiParsedEvent{snapshot({{5'000, 7}}, {})});
    EXPECT_EQ(replacement.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(replacement.book_sync_transition, BookSyncTransition::kNONE);
    EXPECT_TRUE(event.usable());
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 0U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 0U);
    EXPECT_EQ(quantity_at(book(event), Side::kBID, 5'000), 7U);
}

TEST(EventBookSyncTest, DeltasAreIgnoredUntilInitialSnapshotArrives) {
    auto event = single_market_event();

    const auto result = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{
            .side = Side::kBID,
            .price_ticks = kBidPrice,
            .delta_qty_lots = 5,
        }});

    EXPECT_EQ(result.disposition, ApplyDisposition::kIGNORED);
    EXPECT_EQ(result.book_sync_transition, BookSyncTransition::kNONE);
    EXPECT_EQ(result.reason, MarketApplyReason::kMISSING_INITIAL_SNAPSHOT);
    EXPECT_FALSE(event.usable());
    EXPECT_EQ(book(event).sync_state, BookSyncState::kAWAITING_INITIAL_SNAPSHOT);
}

TEST(EventBookSyncTest, InvalidInitialSnapshotRequiresRecoveryAndReplacementClosesIt) {
    auto event = single_market_event();

    const auto rejected = event.apply(
        kMarketIndex,
        KalshiParsedEvent{snapshot({{5'050, 1}}, {})});
    EXPECT_EQ(rejected.disposition, ApplyDisposition::kREJECTED);
    EXPECT_EQ(rejected.book_sync_transition, BookSyncTransition::kRECOVERY_REQUIRED);
    EXPECT_EQ(rejected.reason, MarketApplyReason::kINVALID_PRICE);
    EXPECT_FALSE(event.usable());
    EXPECT_EQ(book(event).sync_state, BookSyncState::kAWAITING_RECOVERY_SNAPSHOT);

    const auto recovered = event.apply(
        kMarketIndex,
        KalshiParsedEvent{snapshot({{kBidPrice, 6}}, {{kAskPrice, 8}})});
    EXPECT_EQ(recovered.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(recovered.book_sync_transition, BookSyncTransition::kRECOVERED);
    EXPECT_EQ(recovered.reason, MarketApplyReason::kNONE);
    EXPECT_TRUE(event.usable());
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 6U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 8U);
}

TEST(EventBookSyncTest, BidAndAskDeltasApplySignedQuantityChanges) {
    auto event = single_market_event();
    ASSERT_EQ(
        event.apply(kMarketIndex, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);

    const auto add_bid = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{Side::kBID, kBidPrice, 4}});
    const auto remove_bid = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{Side::kBID, kBidPrice, -3}});
    const auto add_ask = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{Side::kASK, kAskPrice, 5}});
    const auto remove_ask = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{Side::kASK, kAskPrice, -2}});

    EXPECT_EQ(add_bid.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(remove_bid.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(add_ask.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(remove_ask.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 11U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 15U);
    EXPECT_TRUE(event.usable());
}

TEST(EventBookSyncTest, CorruptDeltaInvalidatesOnceAndRecoverySnapshotRestoresBook) {
    auto event = single_market_event();
    ASSERT_EQ(
        event.apply(kMarketIndex, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);

    const auto rejected = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{Side::kBID, kBidPrice, -11}});
    EXPECT_EQ(rejected.disposition, ApplyDisposition::kREJECTED);
    EXPECT_EQ(rejected.book_sync_transition, BookSyncTransition::kBECAME_UNUSABLE);
    EXPECT_EQ(rejected.reason, MarketApplyReason::kNEGATIVE_LEVEL);
    EXPECT_FALSE(event.usable());
    EXPECT_EQ(book(event).sync_state, BookSyncState::kAWAITING_RECOVERY_SNAPSHOT);
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 0U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 0U);

    const auto ignored = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{Side::kBID, kBidPrice, 1}});
    EXPECT_EQ(ignored.disposition, ApplyDisposition::kIGNORED);
    EXPECT_EQ(ignored.book_sync_transition, BookSyncTransition::kNONE);
    EXPECT_EQ(ignored.reason, MarketApplyReason::kMISSING_RECOVERY_SNAPSHOT);

    const auto recovered = event.apply(
        kMarketIndex,
        KalshiParsedEvent{snapshot({{kBidPrice, 3}}, {{kAskPrice, 4}})});
    EXPECT_EQ(recovered.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(recovered.book_sync_transition, BookSyncTransition::kRECOVERED);
    EXPECT_TRUE(event.usable());
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 3U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 4U);
}

TEST(EventBookSyncTest, InvalidSnapshotDoesNotPartiallyReplaceLiveBook) {
    auto event = single_market_event();
    ASSERT_EQ(
        event.apply(kMarketIndex, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);

    const auto rejected = event.apply(
        kMarketIndex,
        KalshiParsedEvent{snapshot({{5'000, 99}, {5'050, 1}}, {})});

    EXPECT_EQ(rejected.disposition, ApplyDisposition::kREJECTED);
    EXPECT_EQ(rejected.book_sync_transition, BookSyncTransition::kBECAME_UNUSABLE);
    EXPECT_EQ(rejected.reason, MarketApplyReason::kINVALID_PRICE);
    EXPECT_FALSE(event.usable());
    EXPECT_EQ(quantity_at(book(event), Side::kBID, 5'000), 0U);
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 0U);
}

TEST(EventBookSyncTest, QuantityOverflowInvalidatesTheBook) {
    auto event = single_market_event();
    ASSERT_EQ(
        event.apply(
            kMarketIndex,
            KalshiParsedEvent{snapshot(
                {{kBidPrice, std::numeric_limits<QtyLots>::max()}},
                {})})
            .disposition,
        ApplyDisposition::kAPPLIED);

    const auto overflow = event.apply(
        kMarketIndex,
        KalshiParsedEvent{KalshiDeltaData{Side::kBID, kBidPrice, 1}});

    EXPECT_EQ(overflow.disposition, ApplyDisposition::kREJECTED);
    EXPECT_EQ(overflow.book_sync_transition, BookSyncTransition::kBECAME_UNUSABLE);
    EXPECT_EQ(overflow.reason, MarketApplyReason::kOVERFLOW);
    EXPECT_FALSE(event.usable());
}

TEST(EventBookSyncTest, EventIsUsableOnlyWhenEveryMarketHasASnapshot) {
    KalshiEvent state{};
    state.event_id = 12;
    state.markets.push_back(linear_market(101, 0));
    state.markets.push_back(linear_market(102, 1));
    Event event{std::move(state)};

    const auto first = event.apply(0, KalshiParsedEvent{snapshot()});
    EXPECT_EQ(first.book_sync_transition, BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED);
    EXPECT_FALSE(event.usable());

    const auto second = event.apply(1, KalshiParsedEvent{snapshot()});
    EXPECT_EQ(second.book_sync_transition, BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED);
    EXPECT_TRUE(event.usable());
}

TEST(EventBookInvalidationTest, DefaultInvalidationReasonIsNone) {
    EXPECT_EQ(BookInvalidationReason{}, BookInvalidationReason::kNONE);
}

TEST(EventBookInvalidationTest, ExternalInvalidationIsIdempotentAndSnapshotRecovers) {
    auto event = single_market_event();
    ASSERT_EQ(
        event.apply(kMarketIndex, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);

    const auto invalidated = event.invalidate_market(
        kMarketIndex,
        BookInvalidationReason::kWIRE_TO_ROUTER_DELIVERY_LOSS);

    EXPECT_TRUE(invalidated.target_found);
    EXPECT_EQ(invalidated.book_sync_transition, BookSyncTransition::kBECAME_UNUSABLE);
    EXPECT_EQ(invalidated.reason, BookInvalidationReason::kWIRE_TO_ROUTER_DELIVERY_LOSS);
    EXPECT_EQ(invalidated.reject_reason, InvalidationRejectReason::kNONE);
    EXPECT_FALSE(event.usable());
    EXPECT_EQ(book(event).sync_state, BookSyncState::kAWAITING_RECOVERY_SNAPSHOT);
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 0U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 0U);

    const auto repeated = event.invalidate_market(
        kMarketIndex,
        BookInvalidationReason::kROUTER_TO_SHARD_DELIVERY_LOSS);

    EXPECT_TRUE(repeated.target_found);
    EXPECT_EQ(repeated.book_sync_transition, BookSyncTransition::kNONE);
    EXPECT_EQ(repeated.reason, BookInvalidationReason::kROUTER_TO_SHARD_DELIVERY_LOSS);
    EXPECT_EQ(repeated.reject_reason, InvalidationRejectReason::kNONE);

    const auto recovered = event.apply(
        kMarketIndex,
        KalshiParsedEvent{snapshot({{kBidPrice, 3}}, {{kAskPrice, 4}})});

    EXPECT_EQ(recovered.disposition, ApplyDisposition::kAPPLIED);
    EXPECT_EQ(recovered.book_sync_transition, BookSyncTransition::kRECOVERED);
    EXPECT_TRUE(event.usable());
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 3U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 4U);
}

TEST(EventBookInvalidationTest, InvalidMarketIndexDoesNotMutateTheEvent) {
    auto event = single_market_event();
    ASSERT_EQ(
        event.apply(kMarketIndex, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);

    const auto result = event.invalidate_market(
        kMarketIndex + 1,
        BookInvalidationReason::kSHARD_PARSE_FAILURE);

    EXPECT_FALSE(result.target_found);
    EXPECT_EQ(result.book_sync_transition, BookSyncTransition::kNONE);
    EXPECT_EQ(result.reason, BookInvalidationReason::kSHARD_PARSE_FAILURE);
    EXPECT_EQ(result.reject_reason, InvalidationRejectReason::kINVALID_MARKET_INDEX);
    EXPECT_TRUE(event.usable());
    EXPECT_EQ(quantity_at(book(event), Side::kBID, kBidPrice), 10U);
    EXPECT_EQ(quantity_at(book(event), Side::kASK, kAskPrice), 12U);
}

TEST(EventStoreBookInvalidationTest, RoutingIdentityMustMatchBeforeMutation) {
    KalshiEvent state{};
    state.event_id = 11;
    state.shard_event_index = 0;
    state.markets.push_back(linear_market(101, 0));

    EventStore store;
    ASSERT_TRUE(store.initialize({std::move(state)}));
    auto* event = store.get_event(0);
    ASSERT_NE(event, nullptr);
    ASSERT_EQ(
        event->apply(0, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);

    const auto invalid_event = store.invalidate_market(
        1,
        0,
        101,
        BookInvalidationReason::kSHARD_FRAME_MISSING);
    EXPECT_FALSE(invalid_event.target_found);
    EXPECT_EQ(invalid_event.reject_reason, InvalidationRejectReason::kINVALID_EVENT_INDEX);
    EXPECT_EQ(invalid_event.reason, BookInvalidationReason::kSHARD_FRAME_MISSING);

    const auto invalid_market = store.invalidate_market(
        0,
        1,
        101,
        BookInvalidationReason::kSHARD_FRAME_MISSING);
    EXPECT_FALSE(invalid_market.target_found);
    EXPECT_EQ(invalid_market.reject_reason, InvalidationRejectReason::kINVALID_MARKET_INDEX);

    const auto identity_mismatch = store.invalidate_market(
        0,
        0,
        999,
        BookInvalidationReason::kSHARD_FRAME_MISSING);
    EXPECT_FALSE(identity_mismatch.target_found);
    EXPECT_EQ(identity_mismatch.reject_reason, InvalidationRejectReason::kMARKET_ID_MISMATCH);

    EXPECT_TRUE(event->usable());
    EXPECT_EQ(quantity_at(book(*event), Side::kBID, kBidPrice), 10U);
    EXPECT_EQ(quantity_at(book(*event), Side::kASK, kAskPrice), 12U);

    const auto invalidated = store.invalidate_market(
        0,
        0,
        101,
        BookInvalidationReason::kSHARD_FRAME_MISSING);
    EXPECT_TRUE(invalidated.target_found);
    EXPECT_EQ(invalidated.reject_reason, InvalidationRejectReason::kNONE);
    EXPECT_EQ(invalidated.book_sync_transition, BookSyncTransition::kBECAME_UNUSABLE);
    EXPECT_FALSE(event->usable());

    const auto recovered = event->apply(0, KalshiParsedEvent{snapshot()});
    EXPECT_EQ(recovered.book_sync_transition, BookSyncTransition::kRECOVERED);
    EXPECT_TRUE(event->usable());
}

TEST(EventStoreBookInvalidationTest, ShardWideInvalidationAggregatesEveryPriorState) {
    KalshiEvent first_state{};
    first_state.event_id = 11;
    first_state.shard_event_index = 0;
    first_state.markets.push_back(linear_market(101, 0));
    first_state.markets.push_back(linear_market(102, 1));

    KalshiEvent second_state{};
    second_state.event_id = 12;
    second_state.shard_event_index = 1;
    second_state.markets.push_back(linear_market(103, 0));

    EventStore store;
    std::vector<KalshiEvent> states;
    states.push_back(std::move(first_state));
    states.push_back(std::move(second_state));
    ASSERT_TRUE(store.initialize(std::move(states)));

    auto* first = store.get_event(0);
    auto* second = store.get_event(1);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_EQ(
        first->apply(0, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);
    ASSERT_EQ(
        second->apply(0, KalshiParsedEvent{snapshot()}).disposition,
        ApplyDisposition::kAPPLIED);
    ASSERT_EQ(
        second->invalidate_market(0, BookInvalidationReason::kSHARD_PARSE_FAILURE)
            .book_sync_transition,
        BookSyncTransition::kBECAME_UNUSABLE);

    const auto summary = store.invalidate_all_markets(
        BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP);

    EXPECT_EQ(summary.targets_found, 3U);
    EXPECT_EQ(summary.targets_became_unusable, 1U);
    EXPECT_EQ(summary.targets_recovery_required, 1U);
    EXPECT_EQ(summary.targets_already_awaiting_recovery, 1U);
    EXPECT_EQ(book(*first, 0).sync_state, BookSyncState::kAWAITING_RECOVERY_SNAPSHOT);
    EXPECT_EQ(book(*first, 1).sync_state, BookSyncState::kAWAITING_RECOVERY_SNAPSHOT);
    EXPECT_EQ(book(*second, 0).sync_state, BookSyncState::kAWAITING_RECOVERY_SNAPSHOT);
    EXPECT_FALSE(first->usable());
    EXPECT_FALSE(second->usable());
}

} // namespace
