#include <chrono>
#include <cstdint>

#include <gtest/gtest.h>

#include "predex/control/recovery_coordinator.hpp"

namespace {

namespace control = predex::core::control;
namespace ingest = predex::ingest::kalshi;
namespace shard = predex::shard;

constexpr std::uint64_t kUniverseVersion = 7;

control::UniverseSnapshot universe(){
    control::UniverseSnapshot result{};
    result.version = kUniverseVersion;
    result.market_routes = {
        control::UniverseMarketRoute{
            .market_id = 101,
            .event_id = 11,
            .shard_index = 0,
        },
        control::UniverseMarketRoute{
            .market_id = 205,
            .event_id = 22,
            .shard_index = 1,
        },
    };
    return result;
}

shard::ShardMarketRecoveryRequired router_fact(
    std::uint64_t incident_id,
    control::MarketId market_id = 101,
    control::EventId event_id = 11,
    std::uint32_t shard_index = 0){
    return shard::ShardMarketRecoveryRequired{
        .universe_version = kUniverseVersion,
        .shard_index = shard_index,
        .incident = {
            .origin = ingest::IntegrityIncidentOrigin::kROUTER,
            .producer_index = 0,
            .incident_id = incident_id,
        },
        .sid = 9,
        .sequence = 50,
        .market_id = market_id,
        .event_id = event_id,
        .reason = ingest::BookInvalidationReason::kROUTER_TO_SHARD_DELIVERY_LOSS,
    };
}

predex::router::OrderBookSubscriptionBarrierDelivered subscription_fact(
    std::uint64_t incident_id){
    return predex::router::OrderBookSubscriptionBarrierDelivered{
        .universe_version = kUniverseVersion,
        .incident = {
            .origin = ingest::IntegrityIncidentOrigin::kWIRE_SESSION,
            .producer_index = 0,
            .incident_id = incident_id,
        },
        .sid = 17,
        .expected_sequence = 50,
        .observed_sequence = 54,
        .reason = ingest::BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP,
    };
}

control::IoRecoveryRequestAccepted accepted_fact(
    const control::RecoverMarketIo& command){
    return control::IoRecoveryRequestAccepted{
        .recovery_id = command.recovery_id,
        .universe_version = command.universe_version,
        .market_id = command.market_id,
        .request_attempt = command.request_attempt,
    };
}

control::IoRecoveryRequestFailed failed_fact(
    const control::RecoverMarketIo& command){
    return control::IoRecoveryRequestFailed{
        .recovery_id = command.recovery_id,
        .universe_version = command.universe_version,
        .market_id = command.market_id,
        .request_attempt = command.request_attempt,
        .reason = "snapshot request failed",
    };
}

shard::ShardRecoverySnapshotApplied snapshot_fact(
    const control::RecoverMarketIo& command,
    shard::BookSyncTransition transition = shard::BookSyncTransition::kRECOVERED){
    return shard::ShardRecoverySnapshotApplied{
        .recovery_id = command.recovery_id,
        .universe_version = command.universe_version,
        .shard_index = 0,
        .sid = 9,
        .sequence = 51,
        .market_id = command.market_id,
        .transition = transition,
    };
}

TEST(RecoveryCoordinatorTest, CreatesDeduplicatesAndLatchesActiveMarket){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{std::chrono::seconds{1}};

    const auto created = coordinator.observe(router_fact(41), active_universe, now);
    ASSERT_EQ(created.code, control::RecoveryObservationCode::kCREATED);
    EXPECT_NE(created.recovery_id, 0U);
    EXPECT_EQ(created.markets_affected, 1U);

    const auto duplicate = coordinator.observe(router_fact(41), active_universe, now);
    EXPECT_EQ(duplicate.code, control::RecoveryObservationCode::kDUPLICATE);
    EXPECT_EQ(duplicate.recovery_id, created.recovery_id);
    EXPECT_EQ(duplicate.markets_affected, 0U);

    const auto already_recovering = coordinator.observe(router_fact(42), active_universe, now);
    EXPECT_EQ(
        already_recovering.code,
        control::RecoveryObservationCode::kALREADY_RECOVERING);
    EXPECT_EQ(already_recovering.recovery_id, created.recovery_id);

    const auto second_market = coordinator.observe(
        router_fact(43, 205, 22, 1),
        active_universe,
        now);
    EXPECT_EQ(second_market.code, control::RecoveryObservationCode::kCREATED);
    EXPECT_NE(second_market.recovery_id, created.recovery_id);
}

TEST(RecoveryCoordinatorTest, RejectsInvalidVersionIncidentReasonAndRoute){
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{};

    {
        control::RecoveryCoordinator coordinator;
        auto fact = router_fact(51);
        fact.universe_version = kUniverseVersion + 1;
        const auto result = coordinator.observe(fact, active_universe, now);
        EXPECT_EQ(result.code, control::RecoveryObservationCode::kSTALE_UNIVERSE);
        EXPECT_EQ(result.recovery_id, 0U);
    }
    {
        control::RecoveryCoordinator coordinator;
        auto fact = router_fact(51);
        fact.incident.origin = ingest::IntegrityIncidentOrigin::kUNKNOWN;
        EXPECT_EQ(
            coordinator.observe(fact, active_universe, now).code,
            control::RecoveryObservationCode::kINVALID_INCIDENT);
    }
    {
        control::RecoveryCoordinator coordinator;
        auto fact = router_fact(51);
        fact.incident.incident_id = 0;
        EXPECT_EQ(
            coordinator.observe(fact, active_universe, now).code,
            control::RecoveryObservationCode::kINVALID_INCIDENT);
    }
    {
        control::RecoveryCoordinator coordinator;
        auto fact = router_fact(51);
        fact.reason = ingest::BookInvalidationReason::kNONE;
        EXPECT_EQ(
            coordinator.observe(fact, active_universe, now).code,
            control::RecoveryObservationCode::kINVALID_REASON);
    }
    {
        control::RecoveryCoordinator coordinator;
        auto fact = router_fact(51);
        fact.reason = ingest::BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP;
        fact.incident.origin = ingest::IntegrityIncidentOrigin::kWIRE_SESSION;
        EXPECT_EQ(
            coordinator.observe(fact, active_universe, now).code,
            control::RecoveryObservationCode::kINVALID_REASON);
    }
    {
        control::RecoveryCoordinator coordinator;
        const auto result = coordinator.observe(
            router_fact(51, 999, 11, 0),
            active_universe,
            now);
        EXPECT_EQ(result.code, control::RecoveryObservationCode::kUNKNOWN_MARKET);
    }
    {
        control::RecoveryCoordinator coordinator;
        const auto result = coordinator.observe(
            router_fact(51, 101, 12, 0),
            active_universe,
            now);
        EXPECT_EQ(result.code, control::RecoveryObservationCode::kROUTE_MISMATCH);
    }
    {
        control::RecoveryCoordinator coordinator;
        const auto result = coordinator.observe(
            router_fact(51, 101, 11, 1),
            active_universe,
            now);
        EXPECT_EQ(result.code, control::RecoveryObservationCode::kROUTE_MISMATCH);
    }
}

TEST(RecoveryCoordinatorTest, AcceptsValidWireAndShardTargetedSources){
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{};

    control::RecoveryCoordinator wire_coordinator;
    auto wire_fact = router_fact(61);
    wire_fact.incident.origin = ingest::IntegrityIncidentOrigin::kWIRE_SESSION;
    wire_fact.reason = ingest::BookInvalidationReason::kWIRE_POOL_EXHAUSTION;
    EXPECT_EQ(
        wire_coordinator.observe(wire_fact, active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);

    control::RecoveryCoordinator shard_coordinator;
    auto shard_fact = router_fact(62);
    shard_fact.incident.origin = ingest::IntegrityIncidentOrigin::kSHARD;
    shard_fact.incident.producer_index = shard_fact.shard_index;
    shard_fact.reason = ingest::BookInvalidationReason::kSHARD_PARSE_FAILURE;
    EXPECT_EQ(
        shard_coordinator.observe(shard_fact, active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);

    control::RecoveryCoordinator mismatched_shard_coordinator;
    shard_fact.incident.incident_id = 63;
    shard_fact.incident.producer_index = shard_fact.shard_index + 1;
    EXPECT_EQ(
        mismatched_shard_coordinator.observe(shard_fact, active_universe, now).code,
        control::RecoveryObservationCode::kINVALID_REASON);
}

TEST(RecoveryCoordinatorTest, PendingCommandIsStableUntilSuccessfullyMarked){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{std::chrono::seconds{5}};

    const auto created = coordinator.observe(router_fact(71), active_universe, now);
    ASSERT_EQ(created.code, control::RecoveryObservationCode::kCREATED);

    const auto before_eligible = coordinator.next_pending_command(
        now - std::chrono::nanoseconds{1});
    EXPECT_FALSE(before_eligible.has_value());

    const auto first = coordinator.next_pending_command(now);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->recovery_id, created.recovery_id);
    EXPECT_EQ(first->universe_version, kUniverseVersion);
    EXPECT_EQ(first->market_id, 101U);
    EXPECT_EQ(first->request_attempt, 1U);

    const auto retry_after_queue_failure = coordinator.next_pending_command(now);
    ASSERT_TRUE(retry_after_queue_failure.has_value());
    EXPECT_EQ(retry_after_queue_failure->recovery_id, first->recovery_id);
    EXPECT_EQ(retry_after_queue_failure->universe_version, first->universe_version);
    EXPECT_EQ(retry_after_queue_failure->market_id, first->market_id);
    EXPECT_EQ(retry_after_queue_failure->request_attempt, first->request_attempt);

    auto invalid = *first;
    ++invalid.recovery_id;
    EXPECT_FALSE(coordinator.mark_command_enqueued(invalid, now));

    invalid = *first;
    ++invalid.universe_version;
    EXPECT_FALSE(coordinator.mark_command_enqueued(invalid, now));

    invalid = *first;
    ++invalid.market_id;
    EXPECT_FALSE(coordinator.mark_command_enqueued(invalid, now));

    invalid = *first;
    ++invalid.request_attempt;
    EXPECT_FALSE(coordinator.mark_command_enqueued(invalid, now));

    EXPECT_FALSE(coordinator.mark_command_enqueued(
        *first,
        now - std::chrono::nanoseconds{1}));

    EXPECT_TRUE(coordinator.mark_command_enqueued(*first, now));
    EXPECT_FALSE(coordinator.next_pending_command(now).has_value());
    EXPECT_FALSE(coordinator.mark_command_enqueued(*first, now));
}

TEST(RecoveryCoordinatorTest, SelectsPendingCommandsByRecoveryId){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{std::chrono::seconds{9}};

    const auto first_created = coordinator.observe(
        router_fact(81, 205, 22, 1),
        active_universe,
        now);
    const auto second_created = coordinator.observe(
        router_fact(82, 101, 11, 0),
        active_universe,
        now);
    ASSERT_EQ(first_created.code, control::RecoveryObservationCode::kCREATED);
    ASSERT_EQ(second_created.code, control::RecoveryObservationCode::kCREATED);
    ASSERT_LT(first_created.recovery_id, second_created.recovery_id);

    const auto first_command = coordinator.next_pending_command(now);
    ASSERT_TRUE(first_command.has_value());
    EXPECT_EQ(first_command->recovery_id, first_created.recovery_id);
    EXPECT_EQ(first_command->market_id, 205U);
    ASSERT_TRUE(coordinator.mark_command_enqueued(*first_command, now));

    const auto second_command = coordinator.next_pending_command(now);
    ASSERT_TRUE(second_command.has_value());
    EXPECT_EQ(second_command->recovery_id, second_created.recovery_id);
    EXPECT_EQ(second_command->market_id, 101U);
    ASSERT_TRUE(coordinator.mark_command_enqueued(*second_command, now));

    EXPECT_FALSE(coordinator.next_pending_command(now).has_value());
}

TEST(RecoveryCoordinatorTest, AcceptsOnlyTheCurrentEnqueuedRequestAttempt){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{11}};

    ASSERT_EQ(
        coordinator.observe(router_fact(91), active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto command = coordinator.next_pending_command(now);
    ASSERT_TRUE(command.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*command, now));

    auto future = accepted_fact(*command);
    ++future.request_attempt;
    const auto future_result = coordinator.handle(future, now);
    EXPECT_EQ(
        future_result.disposition,
        control::RecoveryFactDisposition::kREJECTED);

    auto stale = accepted_fact(*command);
    --stale.request_attempt;
    const auto stale_result = coordinator.handle(stale, now);
    EXPECT_EQ(
        stale_result.disposition,
        control::RecoveryFactDisposition::kIGNORED);

    const auto accepted = coordinator.handle(
        accepted_fact(*command),
        now + std::chrono::milliseconds{1});
    EXPECT_EQ(
        accepted.disposition,
        control::RecoveryFactDisposition::kAPPLIED);
    EXPECT_EQ(
        accepted.effect,
        control::RecoveryFactEffect::kREQUEST_ACCEPTED);
    EXPECT_FALSE(accepted.incident_completed);

    const auto duplicate = coordinator.handle(
        accepted_fact(*command),
        now + std::chrono::milliseconds{2});
    EXPECT_EQ(
        duplicate.disposition,
        control::RecoveryFactDisposition::kIGNORED);
    EXPECT_EQ(duplicate.effect, control::RecoveryFactEffect::kNONE);
}

TEST(RecoveryCoordinatorTest, FailedRequestRetriesWithCappedExponentialBackoff){
    control::RecoveryCoordinator coordinator{
        control::RecoveryCoordinatorConfig{
            .max_attempts = 3,
            .initial_backoff = std::chrono::milliseconds{100},
            .max_backoff = std::chrono::milliseconds{150},
        }};
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{13}};

    ASSERT_EQ(
        coordinator.observe(router_fact(92), active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto first = coordinator.next_pending_command(now);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*first, now));

    const auto first_failure_time = now + std::chrono::milliseconds{1};
    const auto first_failure = coordinator.handle(
        failed_fact(*first),
        first_failure_time);
    EXPECT_EQ(
        first_failure.disposition,
        control::RecoveryFactDisposition::kAPPLIED);
    EXPECT_EQ(
        first_failure.effect,
        control::RecoveryFactEffect::kRETRY_SCHEDULED);

    const auto duplicate_failure = coordinator.handle(
        failed_fact(*first),
        first_failure_time + std::chrono::milliseconds{50});
    EXPECT_EQ(
        duplicate_failure.disposition,
        control::RecoveryFactDisposition::kIGNORED);

    EXPECT_FALSE(coordinator.next_pending_command(
        first_failure_time + std::chrono::milliseconds{99}).has_value());
    const auto second = coordinator.next_pending_command(
        first_failure_time + std::chrono::milliseconds{100});
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->request_attempt, 2U);
    ASSERT_TRUE(coordinator.mark_command_enqueued(
        *second,
        first_failure_time + std::chrono::milliseconds{100}));

    const auto second_failure_time =
        first_failure_time + std::chrono::milliseconds{101};
    EXPECT_EQ(
        coordinator.handle(failed_fact(*second), second_failure_time).effect,
        control::RecoveryFactEffect::kRETRY_SCHEDULED);
    EXPECT_FALSE(coordinator.next_pending_command(
        second_failure_time + std::chrono::milliseconds{149}).has_value());
    const auto third = coordinator.next_pending_command(
        second_failure_time + std::chrono::milliseconds{150});
    ASSERT_TRUE(third.has_value());
    EXPECT_EQ(third->request_attempt, 3U);
}

TEST(RecoveryCoordinatorTest, ExhaustedRequestRemainsLatchedUntilSnapshotApplies){
    control::RecoveryCoordinator coordinator{
        control::RecoveryCoordinatorConfig{
            .max_attempts = 1,
            .initial_backoff = std::chrono::milliseconds{100},
            .max_backoff = std::chrono::milliseconds{100},
        }};
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{15}};

    const auto created = coordinator.observe(
        router_fact(93),
        active_universe,
        now);
    ASSERT_EQ(created.code, control::RecoveryObservationCode::kCREATED);
    const auto command = coordinator.next_pending_command(now);
    ASSERT_TRUE(command.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*command, now));

    const auto failed = coordinator.handle(
        failed_fact(*command),
        now + std::chrono::milliseconds{1});
    EXPECT_EQ(failed.disposition, control::RecoveryFactDisposition::kAPPLIED);
    EXPECT_EQ(failed.effect, control::RecoveryFactEffect::kMARKET_FAILED);
    EXPECT_FALSE(coordinator.next_pending_command(
        now + std::chrono::hours{1}).has_value());

    const auto still_latched = coordinator.observe(
        router_fact(94),
        active_universe,
        now + std::chrono::milliseconds{2});
    EXPECT_EQ(
        still_latched.code,
        control::RecoveryObservationCode::kALREADY_RECOVERING);
    EXPECT_EQ(still_latched.recovery_id, created.recovery_id);

    const auto recovered = coordinator.handle(
        snapshot_fact(*command),
        active_universe,
        now + std::chrono::milliseconds{3});
    EXPECT_EQ(
        recovered.disposition,
        control::RecoveryFactDisposition::kAPPLIED);
    EXPECT_EQ(
        recovered.effect,
        control::RecoveryFactEffect::kMARKET_RECOVERED);
    EXPECT_TRUE(recovered.incident_completed);

    EXPECT_EQ(
        coordinator.observe(
            router_fact(95),
            active_universe,
            now + std::chrono::milliseconds{4}).code,
        control::RecoveryObservationCode::kCREATED);
}

TEST(RecoveryCoordinatorTest, SnapshotApplicationWinsRaceWithIoAcceptance){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{17}};

    const auto created = coordinator.observe(
        router_fact(96),
        active_universe,
        now);
    ASSERT_EQ(created.code, control::RecoveryObservationCode::kCREATED);
    const auto command = coordinator.next_pending_command(now);
    ASSERT_TRUE(command.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*command, now));

    const auto applied = coordinator.handle(
        snapshot_fact(*command),
        active_universe,
        now + std::chrono::milliseconds{1});
    EXPECT_EQ(applied.disposition, control::RecoveryFactDisposition::kAPPLIED);
    EXPECT_EQ(applied.effect, control::RecoveryFactEffect::kMARKET_RECOVERED);
    EXPECT_TRUE(applied.incident_completed);

    const auto late_acceptance = coordinator.handle(
        accepted_fact(*command),
        now + std::chrono::milliseconds{2});
    EXPECT_EQ(
        late_acceptance.disposition,
        control::RecoveryFactDisposition::kIGNORED);

    const auto duplicate_source = coordinator.observe(
        router_fact(96),
        active_universe,
        now + std::chrono::milliseconds{3});
    EXPECT_EQ(
        duplicate_source.code,
        control::RecoveryObservationCode::kDUPLICATE);
    EXPECT_EQ(duplicate_source.recovery_id, created.recovery_id);
}

TEST(RecoveryCoordinatorTest, SnapshotApplicationCancelsScheduledRetry){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{19}};

    ASSERT_EQ(
        coordinator.observe(router_fact(97), active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto command = coordinator.next_pending_command(now);
    ASSERT_TRUE(command.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*command, now));

    const auto failure_time = now + std::chrono::milliseconds{1};
    ASSERT_EQ(
        coordinator.handle(failed_fact(*command), failure_time).effect,
        control::RecoveryFactEffect::kRETRY_SCHEDULED);

    const auto applied = coordinator.handle(
        snapshot_fact(*command, shard::BookSyncTransition::kNONE),
        active_universe,
        failure_time + std::chrono::milliseconds{1});
    EXPECT_EQ(applied.disposition, control::RecoveryFactDisposition::kAPPLIED);
    EXPECT_EQ(applied.effect, control::RecoveryFactEffect::kMARKET_RECOVERED);
    EXPECT_TRUE(applied.incident_completed);
    EXPECT_FALSE(coordinator.next_pending_command(
        failure_time + std::chrono::hours{1}).has_value());
}

TEST(RecoveryCoordinatorTest, RejectsUncorrelatedOrInvalidSnapshotCompletion){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{21}};

    ASSERT_EQ(
        coordinator.observe(router_fact(98), active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto command = coordinator.next_pending_command(now);
    ASSERT_TRUE(command.has_value());

    const auto before_enqueue = coordinator.handle(
        snapshot_fact(*command),
        active_universe,
        now);
    EXPECT_EQ(
        before_enqueue.disposition,
        control::RecoveryFactDisposition::kREJECTED);

    ASSERT_TRUE(coordinator.mark_command_enqueued(*command, now));

    auto wrong_shard = snapshot_fact(*command);
    ++wrong_shard.shard_index;
    EXPECT_EQ(
        coordinator.handle(wrong_shard, active_universe, now).disposition,
        control::RecoveryFactDisposition::kREJECTED);

    auto invalid_transition = snapshot_fact(*command);
    invalid_transition.transition = shard::BookSyncTransition::kRECOVERY_REQUIRED;
    EXPECT_EQ(
        coordinator.handle(
            invalid_transition,
            active_universe,
            now).disposition,
        control::RecoveryFactDisposition::kREJECTED);

    auto stale_universe = snapshot_fact(*command);
    ++stale_universe.universe_version;
    EXPECT_EQ(
        coordinator.handle(
            stale_universe,
            active_universe,
            now).disposition,
        control::RecoveryFactDisposition::kIGNORED);

    EXPECT_EQ(
        coordinator.handle(
            snapshot_fact(*command),
            active_universe,
            now).disposition,
        control::RecoveryFactDisposition::kAPPLIED);
}

TEST(RecoveryCoordinatorTest, UniverseSupersessionStopsOldWorkAndScopesSourceDeduplication){
    control::RecoveryCoordinator coordinator;
    const auto old_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{23}};

    const auto old_fact = router_fact(99);
    const auto old_recovery = coordinator.observe(
        old_fact,
        old_universe,
        now);
    ASSERT_EQ(
        old_recovery.code,
        control::RecoveryObservationCode::kCREATED);

    const auto old_command = coordinator.next_pending_command(now);
    ASSERT_TRUE(old_command.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*old_command, now));

    constexpr std::uint64_t kNextUniverseVersion = kUniverseVersion + 1;
    const auto supersession = coordinator.supersede_before_universe(
        kNextUniverseVersion,
        now + std::chrono::milliseconds{1});
    EXPECT_EQ(supersession.incidents_superseded, 1U);
    EXPECT_EQ(supersession.markets_superseded, 1U);
    EXPECT_EQ(supersession.active_latches_released, 1U);
    EXPECT_FALSE(coordinator.next_pending_command(
        now + std::chrono::hours{1}).has_value());

    const auto late_acceptance = coordinator.handle(
        accepted_fact(*old_command),
        now + std::chrono::milliseconds{2});
    EXPECT_EQ(
        late_acceptance.disposition,
        control::RecoveryFactDisposition::kIGNORED);

    auto next_universe = old_universe;
    next_universe.version = kNextUniverseVersion;

    const auto late_snapshot = coordinator.handle(
        snapshot_fact(*old_command),
        next_universe,
        now + std::chrono::milliseconds{3});
    EXPECT_EQ(
        late_snapshot.disposition,
        control::RecoveryFactDisposition::kIGNORED);

    auto next_fact = old_fact;
    next_fact.universe_version = kNextUniverseVersion;
    const auto next_recovery = coordinator.observe(
        next_fact,
        next_universe,
        now + std::chrono::milliseconds{4});
    EXPECT_EQ(
        next_recovery.code,
        control::RecoveryObservationCode::kCREATED);
    EXPECT_NE(next_recovery.recovery_id, old_recovery.recovery_id);

    const auto duplicate = coordinator.observe(
        next_fact,
        next_universe,
        now + std::chrono::milliseconds{5});
    EXPECT_EQ(
        duplicate.code,
        control::RecoveryObservationCode::kDUPLICATE);
    EXPECT_EQ(duplicate.recovery_id, next_recovery.recovery_id);
}

TEST(RecoveryCoordinatorTest, UniverseSupersessionReleasesFailedMarketLatch){
    control::RecoveryCoordinator coordinator{
        control::RecoveryCoordinatorConfig{
            .max_attempts = 1,
            .initial_backoff = std::chrono::milliseconds{100},
            .max_backoff = std::chrono::milliseconds{100},
        }};
    const auto old_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{
        std::chrono::seconds{25}};

    ASSERT_EQ(
        coordinator.observe(router_fact(100), old_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto command = coordinator.next_pending_command(now);
    ASSERT_TRUE(command.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*command, now));
    ASSERT_EQ(
        coordinator.handle(failed_fact(*command), now).effect,
        control::RecoveryFactEffect::kMARKET_FAILED);

    constexpr std::uint64_t kNextUniverseVersion = kUniverseVersion + 1;
    const auto supersession = coordinator.supersede_before_universe(
        kNextUniverseVersion,
        now + std::chrono::milliseconds{1});
    EXPECT_EQ(supersession.incidents_superseded, 1U);
    EXPECT_EQ(supersession.markets_superseded, 1U);
    EXPECT_EQ(supersession.active_latches_released, 1U);

    auto next_universe = old_universe;
    next_universe.version = kNextUniverseVersion;
    auto next_fact = router_fact(101);
    next_fact.universe_version = kNextUniverseVersion;
    EXPECT_EQ(
        coordinator.observe(
            next_fact,
            next_universe,
            now + std::chrono::milliseconds{2}).code,
        control::RecoveryObservationCode::kCREATED);
}

TEST(RecoveryCoordinatorTest, SubscriptionIncidentSchedulesEveryUnlatchedMarket){
    control::RecoveryCoordinator coordinator;
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{std::chrono::seconds{1}};

    const auto observed = coordinator.observe(
        subscription_fact(501),
        active_universe,
        now);
    ASSERT_EQ(observed.code, control::RecoveryObservationCode::kCREATED);
    EXPECT_EQ(observed.markets_affected, 2U);
    EXPECT_EQ(coordinator.active_incident_count(), 1U);
    EXPECT_EQ(coordinator.active_market_count(), 2U);

    const auto first = coordinator.next_pending_command(now);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->market_id, 101U);
    EXPECT_EQ(first->recovery_id, observed.recovery_id);
    ASSERT_TRUE(coordinator.mark_command_enqueued(*first, now));

    const auto second = coordinator.next_pending_command(now);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->market_id, 205U);
    EXPECT_EQ(second->recovery_id, observed.recovery_id);

    const auto duplicate = coordinator.observe(
        subscription_fact(501),
        active_universe,
        now);
    EXPECT_EQ(duplicate.code, control::RecoveryObservationCode::kDUPLICATE);
    EXPECT_EQ(duplicate.recovery_id, observed.recovery_id);
}

TEST(RecoveryCoordinatorTest, RequestAckTimeoutSchedulesRetry){
    control::RecoveryCoordinator coordinator{control::RecoveryCoordinatorConfig{
        .max_attempts = 3,
        .initial_backoff = std::chrono::milliseconds{10},
        .max_backoff = std::chrono::milliseconds{100},
        .request_ack_timeout = std::chrono::milliseconds{20},
        .snapshot_timeout = std::chrono::milliseconds{50},
    }};
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{std::chrono::seconds{1}};
    ASSERT_EQ(
        coordinator.observe(router_fact(601), active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto first = coordinator.next_pending_command(now);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*first, now));

    const auto before = coordinator.expire_timeouts(
        now + std::chrono::milliseconds{19});
    EXPECT_EQ(before.request_ack_timeouts, 0U);

    const auto expired = coordinator.expire_timeouts(
        now + std::chrono::milliseconds{20});
    EXPECT_EQ(expired.request_ack_timeouts, 1U);
    EXPECT_EQ(expired.retries_scheduled, 1U);
    EXPECT_FALSE(coordinator.next_pending_command(
        now + std::chrono::milliseconds{29}).has_value());
    const auto retry = coordinator.next_pending_command(
        now + std::chrono::milliseconds{30});
    ASSERT_TRUE(retry.has_value());
    EXPECT_EQ(retry->request_attempt, 2U);
}

TEST(RecoveryCoordinatorTest, AcceptedFailureAndSnapshotTimeoutBothRetry){
    control::RecoveryCoordinator coordinator{control::RecoveryCoordinatorConfig{
        .max_attempts = 4,
        .initial_backoff = std::chrono::milliseconds{10},
        .max_backoff = std::chrono::milliseconds{100},
        .request_ack_timeout = std::chrono::milliseconds{20},
        .snapshot_timeout = std::chrono::milliseconds{30},
    }};
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{std::chrono::seconds{1}};
    ASSERT_EQ(
        coordinator.observe(router_fact(701), active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto first = coordinator.next_pending_command(now);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*first, now));
    ASSERT_EQ(
        coordinator.handle(
            accepted_fact(*first),
            now + std::chrono::milliseconds{5}).effect,
        control::RecoveryFactEffect::kREQUEST_ACCEPTED);

    const auto failed = coordinator.handle(
        failed_fact(*first),
        now + std::chrono::milliseconds{6});
    EXPECT_EQ(failed.effect, control::RecoveryFactEffect::kRETRY_SCHEDULED);

    const auto retry_time = now + std::chrono::milliseconds{16};
    const auto second = coordinator.next_pending_command(retry_time);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->request_attempt, 2U);
    ASSERT_TRUE(coordinator.mark_command_enqueued(*second, retry_time));
    ASSERT_EQ(
        coordinator.handle(
            accepted_fact(*second),
            retry_time + std::chrono::milliseconds{1}).effect,
        control::RecoveryFactEffect::kREQUEST_ACCEPTED);

    const auto expired = coordinator.expire_timeouts(
        retry_time + std::chrono::milliseconds{31});
    EXPECT_EQ(expired.snapshot_timeouts, 1U);
    EXPECT_EQ(expired.retries_scheduled, 1U);
}

TEST(RecoveryCoordinatorTest, FinalSnapshotTimeoutFailsAndRetainsMarketLatch){
    control::RecoveryCoordinator coordinator{control::RecoveryCoordinatorConfig{
        .max_attempts = 1,
        .initial_backoff = std::chrono::milliseconds{10},
        .max_backoff = std::chrono::milliseconds{10},
        .request_ack_timeout = std::chrono::milliseconds{20},
        .snapshot_timeout = std::chrono::milliseconds{30},
    }};
    const auto active_universe = universe();
    const auto now = control::RecoveryCoordinator::TimePoint{std::chrono::seconds{1}};
    ASSERT_EQ(
        coordinator.observe(router_fact(801), active_universe, now).code,
        control::RecoveryObservationCode::kCREATED);
    const auto command = coordinator.next_pending_command(now);
    ASSERT_TRUE(command.has_value());
    ASSERT_TRUE(coordinator.mark_command_enqueued(*command, now));
    ASSERT_EQ(
        coordinator.handle(accepted_fact(*command), now).effect,
        control::RecoveryFactEffect::kREQUEST_ACCEPTED);

    const auto expired = coordinator.expire_timeouts(
        now + std::chrono::milliseconds{30});
    EXPECT_EQ(expired.snapshot_timeouts, 1U);
    EXPECT_EQ(expired.markets_failed, 1U);
    EXPECT_EQ(expired.retries_scheduled, 0U);
    EXPECT_EQ(coordinator.active_market_count(), 1U);
    EXPECT_EQ(coordinator.active_incident_count(), 1U);
    EXPECT_FALSE(coordinator.next_pending_command(
        now + std::chrono::seconds{1}).has_value());

    const auto duplicate_expiration = coordinator.expire_timeouts(
        now + std::chrono::seconds{2});
    EXPECT_EQ(duplicate_expiration.snapshot_timeouts, 0U);
    EXPECT_EQ(duplicate_expiration.markets_failed, 0U);
}

} // namespace
