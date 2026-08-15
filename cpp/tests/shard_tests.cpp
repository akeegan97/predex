#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/ingest/kalshi/market_data/integrity_messages.hpp"
#include "predex/shard/shard.hpp"

namespace {

namespace ingest = predex::ingest::kalshi;
namespace shard = predex::shard;
namespace utils = predex::utils;

constexpr std::uint64_t kUniverseVersion = 1;
constexpr std::uint32_t kShardIndex = 0;
constexpr std::uint32_t kEventIndex = 0;
constexpr std::uint32_t kEventId = 11;
constexpr std::uint32_t kMarketIndex = 0;
constexpr std::uint32_t kMarketId = 101;

shard::KalshiMarket market(std::uint32_t market_id, std::uint32_t market_index) {
    shard::KalshiMarket result{};
    result.market_id = market_id;
    result.event_market_index = market_index;
    result.book.scale = shard::MarketScale::kLINEAR_CENTS;
    return result;
}

shard::KalshiEvent event_with_markets(std::size_t market_count = 1) {
    shard::KalshiEvent event{};
    event.event_id = kEventId;
    event.shard_event_index = kEventIndex;
    for (std::size_t index = 0; index < market_count; ++index) {
        event.markets.push_back(market(
            kMarketId + static_cast<std::uint32_t>(index),
            static_cast<std::uint32_t>(index)));
    }
    return event;
}

class ShardTest : public testing::Test {
protected:
    using PathQueue = utils::SPSCQueue<ingest::MarketDataPathMessage>;
    using HandleQueue = utils::SPSCQueue<ingest::FrameHandle>;
    using StatusQueue = utils::SPSCQueue<shard::ShardToControlMessage>;
    using CommandQueue = utils::SPSCQueue<shard::ControlToShardCommand>;

    ShardTest()
        : shard_(
              kShardIndex,
              shard::ShardQueues{
                  .router_to_shard_queue = router_to_shard_,
                  .shard_to_logger_queue = shard_to_logger_,
                  .last_resort_recycle_queue = recycle_queue_,
                  .shard_to_control_queue = shard_to_control_,
                  .control_to_shard_queue = control_to_shard_,
              },
              frame_pool_) {}

    void install(std::vector<shard::KalshiEvent> events = {event_with_markets()}) {
        ASSERT_TRUE(control_to_shard_.try_push(shard::ControlToShardCommand{
            shard::InstallShardUniverse{
                .universe_version = kUniverseVersion,
                .shard_index = kShardIndex,
                .events = std::move(events),
            }
        }));
        ASSERT_TRUE(shard_.process_one_control_command());

        shard::ShardToControlMessage status{};
        ASSERT_TRUE(shard_to_control_.try_pop(status));
        ASSERT_TRUE(std::holds_alternative<shard::ShardUniverseInstalled>(status));
    }

    void push_command(shard::ControlToShardCommand command) {
        ASSERT_TRUE(control_to_shard_.try_push(std::move(command)));
        ASSERT_TRUE(shard_.process_one_control_command());
    }

    void push_path_message(ingest::MarketDataPathMessage message) {
        ASSERT_TRUE(router_to_shard_.try_push(std::move(message)));
    }

    ingest::FrameHandle make_frame(ingest::FrameKind kind, std::string_view payload) {
        ingest::FrameHandle handle{};
        if (!frame_pool_.try_acquire(handle)) {
            ADD_FAILURE() << "Failed to acquire frame-pool slot";
            return handle;
        }
        auto* frame = frame_pool_.writable_frame(handle);
        if (frame == nullptr) {
            ADD_FAILURE() << "Acquired frame handle was not writable";
            return handle;
        }
        if (payload.size() > ingest::kMaxFrameBytes) {
            ADD_FAILURE() << "Test payload exceeded frame capacity";
            return handle;
        }
        std::memcpy(frame->payload.data(), payload.data(), payload.size());
        frame->len = static_cast<std::uint32_t>(payload.size());

        stamp_handle(handle, kind);
        return handle;
    }

    static void stamp_handle(ingest::FrameHandle& handle, ingest::FrameKind kind) {
        handle.universe_version = kUniverseVersion;
        handle.sequence = next_sequence_++;
        handle.sid = 7;
        handle.market_id = kMarketId;
        handle.event_id = kEventId;
        handle.affinity_key = 0;
        handle.shard_index = kShardIndex;
        handle.shard_event_index = kEventIndex;
        handle.event_market_index = kMarketIndex;
        handle.kind = kind;
    }

    void recycle_logged_frame() {
        ingest::FrameHandle handle{};
        ASSERT_TRUE(shard_to_logger_.try_pop(handle));
        ASSERT_TRUE(frame_pool_.recycle(handle));
    }

    shard::ShardToControlMessage pop_status() {
        shard::ShardToControlMessage status{};
        EXPECT_TRUE(shard_to_control_.try_pop(status));
        return status;
    }

    static ingest::MarketInvalidationBarrier market_barrier(
        std::uint64_t incident_id = 41) {
        return ingest::MarketInvalidationBarrier{
            .universe_version = kUniverseVersion,
            .incident = {.origin = predex::ingest::kalshi::IntegrityIncidentOrigin::kROUTER,
                         .producer_index = 0,
                         .incident_id = incident_id
                        },
            .sid = 7,
            .sequence = 10,
            .market_id = kMarketId,
            .event_id = kEventId,
            .shard_index = kShardIndex,
            .shard_event_index = kEventIndex,
            .event_market_index = kMarketIndex,
            .reason = ingest::BookInvalidationReason::kROUTER_TO_SHARD_DELIVERY_LOSS,
        };
    }

    static constexpr std::string_view kSnapshot =
        R"({"msg":{"yes_dollars_fp":[["0.4000","10.00"]],"no_dollars_fp":[["0.4000","12.00"]]}})";
    static constexpr std::string_view kDelta =
        R"({"msg":{"side":"yes","price_dollars":"0.4000","delta_fp":"1.00"}})";

    inline static std::uint64_t next_sequence_ = 1;

    ingest::FramePool frame_pool_{16};
    PathQueue router_to_shard_{16};
    HandleQueue shard_to_logger_{16};
    HandleQueue recycle_queue_{16};
    StatusQueue shard_to_control_{16};
    CommandQueue control_to_shard_{16};
    shard::Shard shard_;
};

TEST_F(ShardTest, InvalidLifecycleTransitionsFaultClosed) {
    install();

    push_command(shard::ControlToShardCommand{
        shard::DrainShardUniverse{
            .universe_version = kUniverseVersion,
            .shard_index = kShardIndex,
        }
    });

    const auto fault = pop_status();
    ASSERT_TRUE(std::holds_alternative<shard::ShardFaulted>(fault));
    EXPECT_EQ(std::get<shard::ShardFaulted>(fault).shard_index, kShardIndex);

    push_path_message(ingest::MarketDataPathMessage{market_barrier()});
    EXPECT_EQ(shard_.pump_once().code, shard::ShardPumpCode::kIDLE);
}

TEST_F(ShardTest, EmptyShardCanPrepareAndResume) {
    install({});

    push_command(shard::ControlToShardCommand{
        shard::PrepareStopUniverse{kUniverseVersion, kShardIndex}
    });
    EXPECT_TRUE(std::holds_alternative<shard::ShardSafeToStopUniverse>(pop_status()));

    push_command(shard::ControlToShardCommand{
        shard::ResumeShardUniverse{kUniverseVersion, kShardIndex}
    });

    push_command(shard::ControlToShardCommand{
        shard::PrepareStopUniverse{kUniverseVersion, kShardIndex}
    });
    EXPECT_TRUE(std::holds_alternative<shard::ShardSafeToStopUniverse>(pop_status()));
}

TEST_F(ShardTest, DrainedShardCannotResumeWithoutReinstallation) {
    install();
    push_command(shard::ControlToShardCommand{
        shard::PrepareStopUniverse{kUniverseVersion, kShardIndex}
    });
    EXPECT_TRUE(std::holds_alternative<shard::ShardSafeToStopUniverse>(pop_status()));

    push_command(shard::ControlToShardCommand{
        shard::DrainShardUniverse{kUniverseVersion, kShardIndex}
    });
    EXPECT_EQ(shard_.pump_once().code, shard::ShardPumpCode::kDRAIN_COMPLETE);
    EXPECT_TRUE(std::holds_alternative<shard::ShardDrainComplete>(pop_status()));

    push_command(shard::ControlToShardCommand{
        shard::ResumeShardUniverse{kUniverseVersion, kShardIndex}
    });
    EXPECT_TRUE(std::holds_alternative<shard::ShardFaulted>(pop_status()));
}

TEST_F(ShardTest, MarketBarrierIsTargetedAndIdempotent) {
    install();

    push_path_message(ingest::MarketDataPathMessage{market_barrier(51)});
    const auto first = shard_.pump_once();
    ASSERT_EQ(first.code, shard::ShardPumpCode::kMARKET_BARRIER_HANDLED);
    EXPECT_EQ(first.incident.incident_id, 51U);
    EXPECT_TRUE(first.market_invalidation.target_found);
    EXPECT_EQ(
        first.market_invalidation.book_sync_transition,
        shard::BookSyncTransition::kRECOVERY_REQUIRED);

    const auto recovery_required_status = pop_status();
    ASSERT_TRUE(std::holds_alternative<
        shard::ShardMarketRecoveryRequired>(recovery_required_status));
    const auto& recovery_required = std::get<
        shard::ShardMarketRecoveryRequired>(recovery_required_status);
    EXPECT_EQ(recovery_required.universe_version, kUniverseVersion);
    EXPECT_EQ(recovery_required.shard_index, kShardIndex);
    EXPECT_EQ(recovery_required.incident.incident_id, 51U);
    EXPECT_EQ(recovery_required.sid, 7U);
    EXPECT_EQ(recovery_required.sequence, 10U);
    EXPECT_EQ(recovery_required.market_id, kMarketId);
    EXPECT_EQ(recovery_required.event_id, kEventId);
    EXPECT_EQ(
        recovery_required.reason,
        ingest::BookInvalidationReason::kROUTER_TO_SHARD_DELIVERY_LOSS);

    push_path_message(ingest::MarketDataPathMessage{market_barrier(51)});
    const auto repeated = shard_.pump_once();
    ASSERT_EQ(repeated.code, shard::ShardPumpCode::kMARKET_BARRIER_HANDLED);
    EXPECT_EQ(
        repeated.market_invalidation.book_sync_transition,
        shard::BookSyncTransition::kNONE);
    EXPECT_EQ(shard_.stats().market_barriers_seen, 2U);
    EXPECT_EQ(shard_.stats().markets_recovery_required, 1U);
    EXPECT_EQ(shard_.stats().markets_already_awaiting_recovery, 1U);

    shard::ShardToControlMessage duplicate_status{};
    EXPECT_FALSE(shard_to_control_.try_pop(duplicate_status));

    ingest::FrameHandle unused{};
    EXPECT_FALSE(shard_to_logger_.try_pop(unused));
    EXPECT_FALSE(recycle_queue_.try_pop(unused));
}

TEST_F(ShardTest, MarketBarrierIdentityMismatchFaultsShard) {
    install();

    auto barrier = market_barrier(56);
    barrier.event_id = kEventId + 1;
    push_path_message(ingest::MarketDataPathMessage{barrier});

    const auto result = shard_.pump_once();
    EXPECT_EQ(result.code, shard::ShardPumpCode::kINTEGRITY_BARRIER_REJECTED);
    EXPECT_EQ(result.incident.origin, ingest::IntegrityIncidentOrigin::kROUTER);
    EXPECT_EQ(result.incident.incident_id, 56U);
    EXPECT_TRUE(std::holds_alternative<shard::ShardFaulted>(pop_status()));
}

TEST_F(ShardTest, SubscriptionBarrierInvalidatesEveryLocalMarket) {
    install({event_with_markets(2)});
    const ingest::OrderBookSubscriptionInvalidationBarrier barrier{
        .universe_version = kUniverseVersion,
        .incident = {.origin = predex::ingest::kalshi::IntegrityIncidentOrigin::kROUTER,
                     .producer_index = 0,
                     .incident_id = 61},
        .sid = 7,
        .expected_sequence = 9,
        .observed_sequence = 11,
        .reason = ingest::BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP,
    };
    push_path_message(ingest::MarketDataPathMessage{barrier});

    const auto result = shard_.pump_once();
    ASSERT_EQ(result.code, shard::ShardPumpCode::kSUBSCRIPTION_BARRIER_HANDLED);
    EXPECT_EQ(result.incident.incident_id, 61U);
    EXPECT_EQ(result.subscription_invalidation.targets_found, 2U);
    EXPECT_EQ(result.subscription_invalidation.targets_recovery_required, 2U);
}

TEST_F(ShardTest, DrainingPathStillAppliesOrderedBarriers) {
    install();
    push_command(shard::ControlToShardCommand{
        shard::PrepareStopUniverse{kUniverseVersion, kShardIndex}
    });
    EXPECT_TRUE(std::holds_alternative<shard::ShardSafeToStopUniverse>(pop_status()));

    push_path_message(ingest::MarketDataPathMessage{market_barrier(66)});
    push_command(shard::ControlToShardCommand{
        shard::DrainShardUniverse{kUniverseVersion, kShardIndex}
    });

    const auto barrier_result = shard_.pump_once();
    EXPECT_EQ(barrier_result.code, shard::ShardPumpCode::kMARKET_BARRIER_HANDLED);
    EXPECT_EQ(barrier_result.incident.incident_id, 66U);
    EXPECT_EQ(barrier_result.incident.origin, ingest::IntegrityIncidentOrigin::kROUTER);
    EXPECT_TRUE(std::holds_alternative<shard::ShardMarketRecoveryRequired>(
        pop_status()));
    EXPECT_EQ(shard_.pump_once().code, shard::ShardPumpCode::kDRAIN_COMPLETE);
    EXPECT_TRUE(std::holds_alternative<shard::ShardDrainComplete>(pop_status()));
}

TEST_F(ShardTest, OrderedBarrierBlocksDeltasUntilReplacementSnapshot) {
    install();

    push_path_message(ingest::MarketDataPathMessage{
        make_frame(ingest::FrameKind::kORDERBOOK_SNAPSHOT, kSnapshot)
    });
    const auto initial = shard_.pump_once();
    ASSERT_EQ(initial.code, shard::ShardPumpCode::kAPPLIED);
    EXPECT_EQ(
        initial.event_result.book_sync_transition,
        shard::BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED);
    recycle_logged_frame();

    push_path_message(ingest::MarketDataPathMessage{market_barrier(71)});
    const auto invalidated = shard_.pump_once();
    ASSERT_EQ(invalidated.code, shard::ShardPumpCode::kMARKET_BARRIER_HANDLED);
    EXPECT_EQ(
        invalidated.market_invalidation.book_sync_transition,
        shard::BookSyncTransition::kBECAME_UNUSABLE);
    EXPECT_TRUE(std::holds_alternative<shard::ShardMarketRecoveryRequired>(
        pop_status()));

    push_path_message(ingest::MarketDataPathMessage{
        make_frame(ingest::FrameKind::kORDERBOOK_DELTA, kDelta)
    });
    const auto ignored = shard_.pump_once();
    ASSERT_EQ(ignored.code, shard::ShardPumpCode::kEVENT_IGNORED);
    EXPECT_EQ(ignored.event_result.reason, shard::MarketApplyReason::kMISSING_RECOVERY_SNAPSHOT);
    recycle_logged_frame();

    auto recovery_snapshot =
        make_frame(ingest::FrameKind::kORDERBOOK_SNAPSHOT, kSnapshot);
    recovery_snapshot.recovery_id = 501;
    push_path_message(ingest::MarketDataPathMessage{recovery_snapshot});
    const auto recovered = shard_.pump_once();
    ASSERT_EQ(recovered.code, shard::ShardPumpCode::kAPPLIED);
    EXPECT_EQ(recovered.event_result.book_sync_transition, shard::BookSyncTransition::kRECOVERED);

    const auto applied_status = pop_status();
    ASSERT_TRUE(std::holds_alternative<
        shard::ShardRecoverySnapshotApplied>(applied_status));
    const auto& applied =
        std::get<shard::ShardRecoverySnapshotApplied>(applied_status);
    EXPECT_EQ(applied.recovery_id, 501U);
    EXPECT_EQ(applied.universe_version, kUniverseVersion);
    EXPECT_EQ(applied.shard_index, kShardIndex);
    EXPECT_EQ(applied.sid, recovery_snapshot.sid);
    EXPECT_EQ(applied.sequence, recovery_snapshot.sequence);
    EXPECT_EQ(applied.market_id, kMarketId);
    EXPECT_EQ(applied.transition, shard::BookSyncTransition::kRECOVERED);
    recycle_logged_frame();
}

TEST_F(ShardTest, MissingOrderBookFrameInvalidatesMarketBeforeNextDelta) {
    install();

    push_path_message(ingest::MarketDataPathMessage{
        make_frame(ingest::FrameKind::kORDERBOOK_SNAPSHOT, kSnapshot)
    });
    ASSERT_EQ(shard_.pump_once().code, shard::ShardPumpCode::kAPPLIED);
    recycle_logged_frame();

    ingest::FrameHandle missing{};
    stamp_handle(missing, ingest::FrameKind::kORDERBOOK_DELTA);
    missing.pool_index = 15;
    missing.pool_generation = 999;
    push_path_message(ingest::MarketDataPathMessage{missing});

    const auto missing_result = shard_.pump_once();
    ASSERT_EQ(missing_result.code, shard::ShardPumpCode::kMISSING_FRAME);
    EXPECT_TRUE(missing_result.market_invalidation.target_found);
    EXPECT_EQ(
        missing_result.market_invalidation.reason,
        ingest::BookInvalidationReason::kSHARD_FRAME_MISSING);
    EXPECT_EQ(
        missing_result.market_invalidation.book_sync_transition,
        shard::BookSyncTransition::kBECAME_UNUSABLE);
    EXPECT_EQ(shard_.stats().markets_became_unusable, 1U);

    const auto recovery_status = pop_status();
    ASSERT_TRUE(std::holds_alternative<
        shard::ShardMarketRecoveryRequired>(recovery_status));
    const auto& recovery_required =
        std::get<shard::ShardMarketRecoveryRequired>(recovery_status);
    EXPECT_EQ(
        recovery_required.incident.origin,
        ingest::IntegrityIncidentOrigin::kSHARD);
    EXPECT_EQ(recovery_required.incident.producer_index, kShardIndex);
    EXPECT_NE(recovery_required.incident.incident_id, 0U);
    EXPECT_EQ(
        recovery_required.reason,
        ingest::BookInvalidationReason::kSHARD_FRAME_MISSING);

    ingest::FrameHandle logged_missing{};
    ASSERT_TRUE(shard_to_logger_.try_pop(logged_missing));

    push_path_message(ingest::MarketDataPathMessage{
        make_frame(ingest::FrameKind::kORDERBOOK_DELTA, kDelta)
    });
    const auto ignored = shard_.pump_once();
    EXPECT_EQ(ignored.code, shard::ShardPumpCode::kEVENT_IGNORED);
    EXPECT_EQ(ignored.event_result.reason, shard::MarketApplyReason::kMISSING_RECOVERY_SNAPSHOT);
    recycle_logged_frame();
}

TEST_F(ShardTest, ParseFailureInvalidatesOrderBookMarket) {
    install();

    push_path_message(ingest::MarketDataPathMessage{
        make_frame(
            ingest::FrameKind::kORDERBOOK_SNAPSHOT,
            R"({"msg":{"yes_dollars_fp":"invalid"}})")
    });
    const auto rejected = shard_.pump_once();
    ASSERT_EQ(rejected.code, shard::ShardPumpCode::kPARSE_REJECTED);
    EXPECT_TRUE(rejected.market_invalidation.target_found);
    EXPECT_EQ(
        rejected.market_invalidation.reason,
        ingest::BookInvalidationReason::kSHARD_PARSE_FAILURE);
    EXPECT_EQ(
        rejected.market_invalidation.book_sync_transition,
        shard::BookSyncTransition::kRECOVERY_REQUIRED);
    EXPECT_EQ(shard_.stats().markets_recovery_required, 1U);
    const auto recovery_status = pop_status();
    ASSERT_TRUE(std::holds_alternative<
        shard::ShardMarketRecoveryRequired>(recovery_status));
    const auto& recovery_required =
        std::get<shard::ShardMarketRecoveryRequired>(recovery_status);
    EXPECT_EQ(
        recovery_required.incident.origin,
        ingest::IntegrityIncidentOrigin::kSHARD);
    EXPECT_EQ(
        recovery_required.reason,
        ingest::BookInvalidationReason::kSHARD_PARSE_FAILURE);
    recycle_logged_frame();
}

TEST_F(ShardTest, RecoveryStatusBackpressureBlocksLaterMarketDataUntilDelivered) {
    install();

    for(std::size_t index = 0; index < 16; ++index){
        ASSERT_TRUE(shard_to_control_.try_push(shard::ShardToControlMessage{
            shard::ShardTelemetry{
                .shard_index = kShardIndex,
                .universe_version = kUniverseVersion,
            }
        }));
    }

    push_path_message(ingest::MarketDataPathMessage{market_barrier(81)});
    ASSERT_EQ(
        shard_.pump_once().code,
        shard::ShardPumpCode::kMARKET_BARRIER_HANDLED);

    push_path_message(ingest::MarketDataPathMessage{
        make_frame(ingest::FrameKind::kORDERBOOK_DELTA, kDelta)
    });

    EXPECT_EQ(shard_.pump_once().code, shard::ShardPumpCode::kIDLE);
    EXPECT_EQ(shard_.stats().frames_seen, 0U);

    shard::ShardToControlMessage discarded{};
    ASSERT_TRUE(shard_to_control_.try_pop(discarded));

    EXPECT_EQ(shard_.pump_once().code, shard::ShardPumpCode::kEVENT_IGNORED);
    EXPECT_EQ(shard_.stats().frames_seen, 1U);
    recycle_logged_frame();

    bool found_recovery_status = false;
    shard::ShardToControlMessage status{};
    while(shard_to_control_.try_pop(status)){
        if(const auto* recovery =
            std::get_if<shard::ShardMarketRecoveryRequired>(&status)){
            found_recovery_status = recovery->incident.incident_id == 81;
        }
    }
    EXPECT_TRUE(found_recovery_status);
}

TEST_F(ShardTest, FrameIdentityMismatchFaultsBeforeParsingOrApplying) {
    install();

    auto handle = make_frame(ingest::FrameKind::kORDERBOOK_SNAPSHOT, kSnapshot);
    handle.event_id = kEventId + 1;
    push_path_message(ingest::MarketDataPathMessage{handle});

    const auto result = shard_.pump_once();
    EXPECT_EQ(result.code, shard::ShardPumpCode::kFRAME_ROUTE_REJECTED);
    EXPECT_TRUE(std::holds_alternative<shard::ShardFaulted>(pop_status()));
    EXPECT_EQ(shard_.stats().frames_applied, 0U);
    recycle_logged_frame();
}

} // namespace
