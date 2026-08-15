#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

#include "predex/ingest/kalshi/market_data/wire_session.hpp"

namespace {

namespace kalshi = predex::exchange::kalshi;
namespace market_data = predex::ingest::kalshi::market_data;

TEST(WireSequenceObserverTest, DistinguishesInactiveFirstAndContiguousSid){
    market_data::SidSequenceObserver observer;

    EXPECT_EQ(
        observer.observe(7, 10).code,
        market_data::SequenceObservationCode::kINACTIVE_SID);

    observer.activate(7, kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA);
    const auto first = observer.observe(7, 10);
    EXPECT_EQ(first.code, market_data::SequenceObservationCode::kFIRST);
    EXPECT_EQ(first.expected_sequence, 10U);
    EXPECT_EQ(
        first.channel,
        kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA);

    const auto contiguous = observer.observe(7, 11);
    EXPECT_EQ(
        contiguous.code,
        market_data::SequenceObservationCode::kCONTIGUOUS);
    EXPECT_EQ(contiguous.expected_sequence, 11U);
}

TEST(WireSequenceObserverTest, GapAdvancesBaselineWithoutPoisoningSid){
    market_data::SidSequenceObserver observer;
    observer.activate(9, kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA);
    ASSERT_EQ(
        observer.observe(9, 20).code,
        market_data::SequenceObservationCode::kFIRST);

    const auto gap = observer.observe(9, 24);
    EXPECT_EQ(gap.code, market_data::SequenceObservationCode::kGAP);
    EXPECT_EQ(gap.expected_sequence, 21U);
    EXPECT_EQ(gap.observed_sequence, 24U);
    EXPECT_EQ(
        observer.observe(9, 25).code,
        market_data::SequenceObservationCode::kCONTIGUOUS);
}

TEST(WireSequenceObserverTest, DuplicateAndStaleDoNotMoveBaseline){
    market_data::SidSequenceObserver observer;
    observer.activate(11, kalshi::KalshiMarketDataChannel::kTRADE);
    ASSERT_EQ(
        observer.observe(11, 30).code,
        market_data::SequenceObservationCode::kFIRST);
    EXPECT_EQ(
        observer.observe(11, 30).code,
        market_data::SequenceObservationCode::kDUPLICATE);
    EXPECT_EQ(
        observer.observe(11, 29).code,
        market_data::SequenceObservationCode::kSTALE);
    EXPECT_EQ(
        observer.observe(11, 31).code,
        market_data::SequenceObservationCode::kCONTIGUOUS);
}

TEST(WireSequenceObserverTest, ReactivationStartsFreshBaseline){
    market_data::SidSequenceObserver observer;
    observer.activate(13, kalshi::KalshiMarketDataChannel::kMARKET_LIFECYCLE);
    ASSERT_EQ(
        observer.observe(13, 100).code,
        market_data::SequenceObservationCode::kFIRST);
    observer.deactivate(13);
    EXPECT_EQ(
        observer.observe(13, 101).code,
        market_data::SequenceObservationCode::kINACTIVE_SID);
    observer.activate(13, kalshi::KalshiMarketDataChannel::kMARKET_LIFECYCLE);
    EXPECT_EQ(
        observer.observe(13, 900).code,
        market_data::SequenceObservationCode::kFIRST);
}

TEST(WireSequenceObserverTest, MaximumSequenceDoesNotWrapExpectedValue){
    market_data::SidSequenceObserver observer;
    observer.activate(15, kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA);
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    ASSERT_EQ(
        observer.observe(15, maximum).code,
        market_data::SequenceObservationCode::kFIRST);
    const auto duplicate = observer.observe(15, maximum);
    EXPECT_EQ(
        duplicate.code,
        market_data::SequenceObservationCode::kDUPLICATE);
    EXPECT_EQ(duplicate.expected_sequence, maximum);
}

} // namespace
