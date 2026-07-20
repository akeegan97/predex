#include <gtest/gtest.h>

#include "../apps/research/historical_feed.hpp"

#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using predex::research::BookDelta;
using predex::research::BookSnapshot;
using predex::research::FeedEvent;
using predex::research::HistoricalFeed;
using predex::research::IFeedSource;
using predex::research::ReplayStamp;

constexpr std::uint64_t kFirstRecordIndex = 10;
constexpr std::uint64_t kSecondRecordIndex = 11;
constexpr std::uint64_t kEarlierRecordIndex = 9;
constexpr std::uint64_t kReceiveTimeNs = 1'000;
constexpr std::uint64_t kLaterReceiveTimeNs = 1'001;
constexpr std::int64_t kBidPriceTicks = 4'500;
constexpr std::int64_t kOneContractLots = 100;

class ScriptedFeedSource final : public IFeedSource {
public:
    explicit ScriptedFeedSource(std::vector<FeedEvent> events):
        events_{std::move(events)} {}

    bool next(FeedEvent& event) override {
        if(index_ == events_.size()) {
            return false;
        }
        event = events_[index_++];
        return true;
    }

    [[nodiscard]] std::string_view source_name() const override {
        return "scripted";
    }

private:
    std::vector<FeedEvent> events_;
    std::size_t index_{};
};

BookSnapshot snapshot(std::uint64_t record_index, std::uint64_t recv_ts_ns) {
    return BookSnapshot{
        .stamp = ReplayStamp{
            .record_index = record_index,
            .recv_ts_ns = recv_ts_ns,
        },
        .run_id = "fixture",
        .sequence = record_index,
        .event_id = 1,
        .market_id = 2,
    };
}

BookDelta delta(std::uint64_t record_index, std::uint64_t recv_ts_ns) {
    return BookDelta{
        .stamp = ReplayStamp{
            .record_index = record_index,
            .recv_ts_ns = recv_ts_ns,
        },
        .run_id = "fixture",
        .sequence = record_index,
        .event_id = 1,
        .market_id = 2,
        .side = "bid",
        .price_ticks = kBidPriceTicks,
        .delta_qty_lots = kOneContractLots,
    };
}

TEST(ResearchHistoricalFeed, PreservesRecordOrderAtEqualReceiveTime) {
    HistoricalFeed feed{std::make_unique<ScriptedFeedSource>(
        std::vector<FeedEvent>{
            snapshot(kFirstRecordIndex, kReceiveTimeNs),
            delta(kSecondRecordIndex, kReceiveTimeNs)})};
    FeedEvent event;

    ASSERT_TRUE(feed.next(event));
    EXPECT_EQ(predex::research::event_stamp(event),
              (ReplayStamp{
                  .record_index = kFirstRecordIndex,
                  .recv_ts_ns = kReceiveTimeNs}));
    ASSERT_TRUE(feed.next(event));
    EXPECT_EQ(predex::research::event_stamp(event),
              (ReplayStamp{
                  .record_index = kSecondRecordIndex,
                  .recv_ts_ns = kReceiveTimeNs}));
    EXPECT_FALSE(feed.next(event));
}

TEST(ResearchHistoricalFeed, RejectsNonIncreasingRecordIndex) {
    HistoricalFeed feed{std::make_unique<ScriptedFeedSource>(
        std::vector<FeedEvent>{
            snapshot(kFirstRecordIndex, kReceiveTimeNs),
            delta(kEarlierRecordIndex, kLaterReceiveTimeNs)})};
    FeedEvent event;

    ASSERT_TRUE(feed.next(event));
    EXPECT_THROW(feed.next(event), std::runtime_error);
}

TEST(ResearchHistoricalFeed, RejectsInvalidEventIdentity) {
    auto invalid = snapshot(kFirstRecordIndex, kReceiveTimeNs);
    invalid.market_id = 0;
    HistoricalFeed feed{std::make_unique<ScriptedFeedSource>(
        std::vector<FeedEvent>{std::move(invalid)})};
    FeedEvent event;

    EXPECT_THROW(feed.next(event), std::runtime_error);
}

TEST(ResearchHistoricalFeed, RejectsZeroReceiveTime) {
    HistoricalFeed feed{std::make_unique<ScriptedFeedSource>(
        std::vector<FeedEvent>{snapshot(kFirstRecordIndex, 0)})};
    FeedEvent event;

    EXPECT_THROW(feed.next(event), std::runtime_error);
}

} // namespace
