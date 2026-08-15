#include <cstdint>
#include <cstddef>
#include <variant>

#include <gtest/gtest.h>

#include "predex/router/router.hpp"

namespace {

namespace ingest = predex::ingest::kalshi;
namespace router = predex::router;
namespace utils = predex::utils;

ingest::OrderBookSubscriptionInvalidationBarrier subscription_barrier(){
    return ingest::OrderBookSubscriptionInvalidationBarrier{
        .universe_version = 7,
        .incident = {
            .origin = ingest::IntegrityIncidentOrigin::kWIRE_SESSION,
            .producer_index = 0,
            .incident_id = 91,
        },
        .sid = 13,
        .expected_sequence = 20,
        .observed_sequence = 24,
        .reason = ingest::BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP,
    };
}

TEST(RouterIntegrityTest, SubscriptionBarrierNotifiesControlAfterEveryShard){
    utils::SPSCQueue<ingest::MarketDataPathMessage> shard_zero{2};
    utils::SPSCQueue<ingest::MarketDataPathMessage> shard_one{2};
    utils::SPSCQueue<ingest::FrameHandle> logger{2};
    utils::SPSCQueue<router::RouterToControl> control{2};
    utils::SPSCQueue<ingest::FrameHandle> recycle{2};
    router::Router instance{router::RouterQueues{
        .router_to_shard_queues = {&shard_zero, &shard_one},
        .router_to_logger_queue = &logger,
        .router_to_control_queue = &control,
        .last_resort_recycle_queue = &recycle,
    }};

    const auto barrier = subscription_barrier();
    EXPECT_EQ(
        instance.route_message(ingest::MarketDataPathMessage{barrier}),
        router::RouterRouteResult::kCOMPLETED);

    ingest::MarketDataPathMessage shard_message{};
    ASSERT_TRUE(shard_zero.try_pop(shard_message));
    EXPECT_TRUE(std::holds_alternative<
        ingest::OrderBookSubscriptionInvalidationBarrier>(shard_message));
    ASSERT_TRUE(shard_one.try_pop(shard_message));
    EXPECT_TRUE(std::holds_alternative<
        ingest::OrderBookSubscriptionInvalidationBarrier>(shard_message));

    router::RouterToControl control_message{};
    ASSERT_TRUE(control.try_pop(control_message));
    ASSERT_TRUE(std::holds_alternative<
        router::OrderBookSubscriptionBarrierDelivered>(control_message));
    const auto& delivered = std::get<
        router::OrderBookSubscriptionBarrierDelivered>(control_message);
    EXPECT_EQ(delivered.incident, barrier.incident);
    EXPECT_EQ(delivered.sid, barrier.sid);
}

TEST(RouterIntegrityTest, PartialFanoutBlocksLaterMessagesUntilComplete){
    utils::SPSCQueue<ingest::MarketDataPathMessage> shard_zero{1};
    utils::SPSCQueue<ingest::MarketDataPathMessage> shard_one{1};
    utils::SPSCQueue<ingest::FrameHandle> logger{1};
    utils::SPSCQueue<router::RouterToControl> control{2};
    utils::SPSCQueue<ingest::FrameHandle> recycle{1};
    router::Router instance{router::RouterQueues{
        .router_to_shard_queues = {&shard_zero, &shard_one},
        .router_to_logger_queue = &logger,
        .router_to_control_queue = &control,
        .last_resort_recycle_queue = &recycle,
    }};

    const ingest::MarketDataPathMessage filler{ingest::FrameHandle{}};
    ASSERT_TRUE(shard_one.try_push(filler));
    const auto barrier = subscription_barrier();
    EXPECT_EQ(
        instance.route_message(ingest::MarketDataPathMessage{barrier}),
        router::RouterRouteResult::kBLOCKED);
    EXPECT_EQ(
        instance.route_message(ingest::MarketDataPathMessage{ingest::FrameHandle{}}),
        router::RouterRouteResult::kBLOCKED);

    router::RouterToControl control_message{};
    EXPECT_FALSE(control.try_pop(control_message));
    ingest::MarketDataPathMessage shard_message{};
    ASSERT_TRUE(shard_zero.try_pop(shard_message));
    ASSERT_TRUE(shard_one.try_pop(shard_message));

    EXPECT_EQ(
        instance.flush_pending_barrier(),
        router::RouterRouteResult::kCOMPLETED);
    ASSERT_TRUE(shard_one.try_pop(shard_message));
    EXPECT_TRUE(std::holds_alternative<
        ingest::OrderBookSubscriptionInvalidationBarrier>(shard_message));
    ASSERT_TRUE(control.try_pop(control_message));
    EXPECT_TRUE(std::holds_alternative<
        router::OrderBookSubscriptionBarrierDelivered>(control_message));
    EXPECT_FALSE(shard_zero.try_pop(shard_message));
}

TEST(RouterIntegrityTest, ControlBackpressureRetainsSubscriptionRecoveryFact){
    utils::SPSCQueue<ingest::MarketDataPathMessage> shard{1};
    utils::SPSCQueue<ingest::FrameHandle> logger{1};
    utils::SPSCQueue<router::RouterToControl> control{1};
    utils::SPSCQueue<ingest::FrameHandle> recycle{1};
    router::Router instance{router::RouterQueues{
        .router_to_shard_queues = {&shard},
        .router_to_logger_queue = &logger,
        .router_to_control_queue = &control,
        .last_resort_recycle_queue = &recycle,
    }};

    ASSERT_TRUE(control.try_push(router::RouterToControl{
        router::RouterTelemetry{}}));
    EXPECT_EQ(
        instance.route_message(ingest::MarketDataPathMessage{
            subscription_barrier()}),
        router::RouterRouteResult::kBLOCKED);

    router::RouterToControl control_message{};
    ASSERT_TRUE(control.try_pop(control_message));
    EXPECT_TRUE(std::holds_alternative<router::RouterTelemetry>(control_message));
    EXPECT_EQ(
        instance.flush_pending_barrier(),
        router::RouterRouteResult::kCOMPLETED);
    ASSERT_TRUE(control.try_pop(control_message));
    EXPECT_TRUE(std::holds_alternative<
        router::OrderBookSubscriptionBarrierDelivered>(control_message));
}

TEST(RouterIntegrityTest, PeriodicTelemetryIncludesPerChannelAndQueueHighWater){
    utils::SPSCQueue<ingest::MarketDataPathMessage> shard{2};
    utils::SPSCQueue<ingest::FrameHandle> logger{1};
    utils::SPSCQueue<router::RouterToControl> control{2};
    utils::SPSCQueue<ingest::FrameHandle> recycle{1};
    router::Router instance{router::RouterQueues{
        .router_to_shard_queues = {&shard},
        .router_to_logger_queue = &logger,
        .router_to_control_queue = &control,
        .last_resort_recycle_queue = &recycle,
    }};

    ingest::FrameHandle handle{
        .shard_index = 0,
        .kind = ingest::FrameKind::kTRADE,
    };
    ingest::MarketDataPathMessage routed{};
    for(std::size_t index = 0; index < router::kFRAMETHRESHOLD; ++index){
        ASSERT_EQ(
            instance.route_message(ingest::MarketDataPathMessage{handle}),
            router::RouterRouteResult::kCOMPLETED);
        ASSERT_TRUE(shard.try_pop(routed));
    }

    router::RouterToControl control_message{};
    ASSERT_TRUE(control.try_pop(control_message));
    ASSERT_TRUE(std::holds_alternative<router::RouterTelemetry>(control_message));
    const auto& telemetry = std::get<router::RouterTelemetry>(control_message);
    EXPECT_EQ(telemetry.total_frames_seen, router::kFRAMETHRESHOLD);
    EXPECT_EQ(telemetry.channel_stats[1].frames_observed, router::kFRAMETHRESHOLD);
    EXPECT_EQ(telemetry.shard_queue_depth_high_water, 1U);
}

} // namespace
