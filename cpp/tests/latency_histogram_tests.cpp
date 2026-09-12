#include <cstdint>

#include <gtest/gtest.h>

#include "predex/utils/latency_histogram.hpp"

namespace {

namespace utils = predex::utils;

TEST(LatencyHistogramTest, RecordsBoundedAndOverflowSamples){
    utils::LatencyHistogram histogram{};

    histogram.record(100);
    histogram.record(101);
    histogram.record(1'000'000'001);

    EXPECT_EQ(histogram.count, 3U);
    EXPECT_EQ(histogram.sum_ns, 1'000'000'202U);
    EXPECT_EQ(histogram.max_ns, 1'000'000'001U);
    EXPECT_EQ(histogram.buckets[0], 1U);
    EXPECT_EQ(histogram.buckets[1], 1U);
    EXPECT_EQ(histogram.buckets[utils::kLatencyOverflowBucket], 1U);
    EXPECT_EQ(histogram.percentile_upper_bound(5'000), 250U);
    EXPECT_EQ(histogram.percentile_upper_bound(9'500), 1'000'000'001U);
}

TEST(LatencyHistogramTest, MergesCumulativeThreadOwnedSnapshots){
    utils::LatencyHistogram first{};
    first.record(500);
    utils::LatencyHistogram second{};
    second.record(2'500);
    second.record(5'000);

    first.merge(second);

    EXPECT_EQ(first.count, 3U);
    EXPECT_EQ(first.sum_ns, 8'000U);
    EXPECT_EQ(first.mean_ns(), 2'666U);
    EXPECT_EQ(first.max_ns, 5'000U);
    EXPECT_EQ(first.percentile_upper_bound(5'000), 2'500U);
}

TEST(LatencyHistogramTest, ElapsedRecordingRejectsMissingAndBackwardStamps){
    utils::LatencyHistogram histogram{};

    utils::record_elapsed_ns(histogram, 0, 10);
    utils::record_elapsed_ns(histogram, 20, 10);
    utils::record_elapsed_ns(histogram, 10, 35);

    EXPECT_EQ(histogram.count, 1U);
    EXPECT_EQ(histogram.sum_ns, 25U);
    EXPECT_EQ(histogram.max_ns, 25U);
}

} // namespace
