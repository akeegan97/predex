#include <cstddef>

#include <gtest/gtest.h>

#include "predex/ingest/kalshi/market_data/frame_pool.hpp"

namespace {

TEST(FramePoolTest, RecyclesSlotsAndRejectsStaleHandles) {
    predex::ingest::kalshi::FramePool pool{2};

    predex::ingest::kalshi::FrameHandle first{};
    predex::ingest::kalshi::FrameHandle second{};
    predex::ingest::kalshi::FrameHandle overflow{};

    EXPECT_EQ(pool.capacity(), 2U);
    EXPECT_EQ(pool.available(), 2U);
    EXPECT_TRUE(pool.try_acquire(first));
    EXPECT_TRUE(pool.try_acquire(second));
    EXPECT_FALSE(pool.try_acquire(overflow));

    auto* frame = pool.writable_frame(first);
    ASSERT_NE(frame, nullptr);
    frame->payload[0] = std::byte{'{'};
    frame->len = 1U;

    const auto* read_frame = pool.frame(first);
    ASSERT_NE(read_frame, nullptr);
    EXPECT_EQ(read_frame->len, 1U);
    EXPECT_EQ(read_frame->payload[0], std::byte{'{'});

    EXPECT_TRUE(pool.recycle(first));
    EXPECT_EQ(pool.frame(first), nullptr);
    EXPECT_FALSE(pool.recycle(first));

    predex::ingest::kalshi::FrameHandle reused{};
    EXPECT_TRUE(pool.try_acquire(reused));
    EXPECT_EQ(reused.pool_index, first.pool_index);
    EXPECT_NE(reused.pool_generation, first.pool_generation);

    EXPECT_TRUE(pool.recycle(second));
    EXPECT_TRUE(pool.recycle(reused));
    EXPECT_EQ(pool.available(), 2U);
}

} // namespace
