#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <variant>

#include <gtest/gtest.h>

#include "predex/ingest/kalshi/market_data/wire_session.hpp"

namespace predex::ingest::kalshi::market_data {

class KalshiWireSessionTestPeer{
    public:
        static void install(
            KalshiWireSession& session,
            std::shared_ptr<const core::control::UniverseSnapshot> universe,
            std::uint32_t sid){
            session.desired_universe_ = std::move(universe);
            session.market_route_by_ticker_.clear();
            session.market_route_by_id_.clear();
            for(const auto& route : session.desired_universe_->market_routes){
                session.market_route_by_ticker_.emplace(
                    route.kalshi_ticker,
                    route);
                session.market_route_by_id_.emplace(route.market_id, route);
            }
            const auto channel =
                exchange::kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA;
            session.sequence_observer_.activate(sid, channel);
            auto& subscription = session.active_subscriptions_[channel];
            subscription.channel = channel;
            subscription.phase = SubscriptionPhase::kSUBSCRIBED;
            subscription.sid = sid;
            for(const auto& route : session.desired_universe_->market_routes){
                subscription.market_ids.insert(route.market_id);
            }
        }

        static SequenceObservation observe(
            KalshiWireSession& session,
            std::uint32_t sid,
            std::uint64_t sequence){
            return session.observe_sequence(sid, sequence);
        }

        static void publish(KalshiWireSession& session, std::string_view json){
            session.publish_market_data_frame(std::span<const std::byte>{
                reinterpret_cast<const std::byte*>(json.data()),
                json.size(),
            });
        }

        static bool flush_integrity_barrier(KalshiWireSession& session){
            return session.flush_pending_integrity_barrier();
        }

        static bool has_pending_integrity_barrier(
            const KalshiWireSession& session){
            return session.pending_integrity_barrier_.has_value();
        }

        static void seed_recovery(
            KalshiWireSession& session,
            std::uint64_t ws_command_id,
            const core::control::RecoverMarketIo& command){
            const RecoveryCommandContext context{
                .recovery_id = command.recovery_id,
                .universe_version = command.universe_version,
                .market_id = command.market_id,
                .request_attempt = command.request_attempt,
            };
            session.pending_recovery_by_market_.insert_or_assign(
                command.market_id,
                PendingRecoveryTag{
                    .recovery_id = command.recovery_id,
                    .universe_version = command.universe_version,
                    .request_attempt = command.request_attempt,
                });
            session.pending_ws_commands_.insert_or_assign(
                ws_command_id,
                PendingWsCommand{
                    .ws_command_id = ws_command_id,
                    .kind = WsCommandKind::kGET_SNAPSHOT,
                    .channel = exchange::kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA,
                    .market_ids = {command.market_id},
                    .recovery_context = context,
                });
        }

        static void control_response(
            KalshiWireSession& session,
            std::string_view json){
            session.handle_ws_control_response(std::span<const std::byte>{
                reinterpret_cast<const std::byte*>(json.data()),
                json.size(),
            });
        }

        static bool has_recovery_tag(
            const KalshiWireSession& session,
            core::control::MarketId market_id){
            return session.pending_recovery_by_market_.find(market_id) !=
                   session.pending_recovery_by_market_.end();
        }

        static const core::control::IoTelemetrySnapshot& telemetry(
            const KalshiWireSession& session){
            return session.telemetry_;
        }
};

} // namespace predex::ingest::kalshi::market_data

namespace {

namespace control = predex::core::control;
namespace ingest = predex::ingest::kalshi;
namespace kalshi = predex::exchange::kalshi;
namespace market_data = predex::ingest::kalshi::market_data;
namespace utils = predex::utils;

constexpr std::string_view kTestPrivateKey = R"pem(-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgWM3GNrJc3+t87NPD
vanECAuygFkY/vCBbXUkquXO7QWhRANCAATsl9Mk/2MV6FG+gylbu1CYFlc4SESW
qXZ1pcKi7gCICqdYeugyFTZY20LTmhJAd7rFpvIKe+KLJhU9VDYyxFnm
-----END PRIVATE KEY-----
)pem";

std::shared_ptr<const control::UniverseSnapshot> universe(){
    auto result = std::make_shared<control::UniverseSnapshot>();
    result->version = 7;
    result->market_routes = {
        control::UniverseMarketRoute{
            .kalshi_ticker = "KNOWN",
            .market_id = 101,
            .event_id = 11,
            .shard_index = 0,
            .shard_event_index = 0,
            .event_market_index = 0,
        },
    };
    return result;
}

struct WireHarness{
    ingest::FramePool pool;
    utils::SPSCQueue<control::ControlToIoCommand> control_to_io{8};
    utils::SPSCQueue<control::IoToControlStatus> io_to_control{8};
    utils::SPSCQueue<ingest::MarketDataPathMessage> router{2};
    utils::SPSCQueue<ingest::FrameHandle> logger{2};
    market_data::KalshiWireSession session;

    explicit WireHarness(std::size_t pool_capacity)
        : pool(pool_capacity),
          session(market_data::KalshiWireSessionDeps{
              .frame_pool = pool,
              .control_queues = market_data::ControlQueues{
                  control_to_io,
                  io_to_control,
              },
              .recycle_queues = {},
              .router_queue = router,
              .logger_queue = logger,
              .market_data_handler = kalshi::KalshiMarketDataHandler{
                  kalshi::AuthSigner{kalshi::Credentials{
                      .key_id = "test",
                      .private_key_pem = std::string{kTestPrivateKey},
                  }}},
              .desired_channels = {
                  kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA,
              },
          }){
        market_data::KalshiWireSessionTestPeer::install(
            session,
            universe(),
            9);
    }
};

TEST(WireSessionIngressTest, SequenceGapPrecedesUnknownMarketFiltering){
    WireHarness harness{2};
    ASSERT_EQ(
        market_data::KalshiWireSessionTestPeer::observe(
            harness.session,
            9,
            10).code,
        market_data::SequenceObservationCode::kFIRST);

    market_data::KalshiWireSessionTestPeer::publish(
        harness.session,
        R"json({"sid":9,"seq":12,"type":"orderbook_delta","msg":{"market_ticker":"UNKNOWN"}})json");

    ingest::MarketDataPathMessage message{};
    ASSERT_TRUE(harness.router.try_pop(message));
    ASSERT_TRUE(std::holds_alternative<
        ingest::OrderBookSubscriptionInvalidationBarrier>(message));
    const auto& barrier = std::get<
        ingest::OrderBookSubscriptionInvalidationBarrier>(message);
    EXPECT_EQ(barrier.expected_sequence, 11U);
    EXPECT_EQ(barrier.observed_sequence, 12U);

    const auto& telemetry =
        market_data::KalshiWireSessionTestPeer::telemetry(harness.session);
    EXPECT_EQ(telemetry.channel_stats[0].sequence_gaps, 1U);
    EXPECT_EQ(telemetry.channel_stats[0].intentionally_filtered, 1U);
    EXPECT_EQ(telemetry.pool_exhausted, 0U);
}

TEST(WireSessionIngressTest, PoolExhaustionEmitsTargetedBarrierBeforeAllocation){
    WireHarness harness{1};
    ingest::FrameHandle reserved{};
    ASSERT_TRUE(harness.pool.try_acquire(reserved));

    market_data::KalshiWireSessionTestPeer::publish(
        harness.session,
        R"json({"sid":9,"seq":1,"type":"orderbook_snapshot","msg":{"market_ticker":"KNOWN"}})json");

    ingest::MarketDataPathMessage message{};
    ASSERT_TRUE(harness.router.try_pop(message));
    ASSERT_TRUE(std::holds_alternative<ingest::MarketInvalidationBarrier>(message));
    EXPECT_EQ(
        std::get<ingest::MarketInvalidationBarrier>(message).reason,
        ingest::BookInvalidationReason::kWIRE_POOL_EXHAUSTION);
    EXPECT_EQ(
        market_data::KalshiWireSessionTestPeer::telemetry(
            harness.session).pool_exhausted,
        1U);
    EXPECT_TRUE(harness.pool.recycle(reserved));
}

TEST(WireSessionIngressTest, RouterLossFallsBackToLoggerAndRetainsBarrier){
    WireHarness harness{2};
    ASSERT_TRUE(harness.router.try_push(ingest::MarketDataPathMessage{
        ingest::FrameHandle{}}));
    ASSERT_TRUE(harness.router.try_push(ingest::MarketDataPathMessage{
        ingest::FrameHandle{}}));

    market_data::KalshiWireSessionTestPeer::publish(
        harness.session,
        R"json({"sid":9,"seq":1,"type":"orderbook_delta","msg":{"market_ticker":"KNOWN"}})json");
    EXPECT_TRUE(
        market_data::KalshiWireSessionTestPeer::has_pending_integrity_barrier(
            harness.session));

    ingest::FrameHandle logged{};
    ASSERT_TRUE(harness.logger.try_pop(logged));
    ingest::MarketDataPathMessage message{};
    ASSERT_TRUE(harness.router.try_pop(message));
    ASSERT_TRUE(harness.router.try_pop(message));
    ASSERT_TRUE(
        market_data::KalshiWireSessionTestPeer::flush_integrity_barrier(
            harness.session));
    ASSERT_TRUE(harness.router.try_pop(message));
    ASSERT_TRUE(std::holds_alternative<ingest::MarketInvalidationBarrier>(message));
    EXPECT_EQ(
        std::get<ingest::MarketInvalidationBarrier>(message).reason,
        ingest::BookInvalidationReason::kWIRE_TO_ROUTER_DELIVERY_LOSS);
    EXPECT_TRUE(harness.pool.recycle(logged));

    const auto& channel =
        market_data::KalshiWireSessionTestPeer::telemetry(
            harness.session).channel_stats[0];
    EXPECT_EQ(channel.logger_only_frames, 1U);
    EXPECT_EQ(channel.downstream_delivery_losses, 1U);
}

TEST(WireSessionRecoveryTest, AckAndErrorPreserveCurrentCorrelationRules){
    WireHarness harness{2};
    const control::RecoverMarketIo accepted_command{
        .recovery_id = 44,
        .universe_version = 7,
        .market_id = 101,
        .request_attempt = 1,
    };
    market_data::KalshiWireSessionTestPeer::seed_recovery(
        harness.session,
        41,
        accepted_command);
    market_data::KalshiWireSessionTestPeer::control_response(
        harness.session,
        R"json({"id":41,"type":"ok"})json");

    control::IoToControlStatus status{};
    ASSERT_TRUE(harness.io_to_control.try_pop(status));
    ASSERT_TRUE(std::holds_alternative<
        control::IoRecoveryRequestAccepted>(status));
    EXPECT_TRUE(market_data::KalshiWireSessionTestPeer::has_recovery_tag(
        harness.session,
        101));

    const control::RecoverMarketIo failed_command{
        .recovery_id = 44,
        .universe_version = 7,
        .market_id = 101,
        .request_attempt = 2,
    };
    market_data::KalshiWireSessionTestPeer::seed_recovery(
        harness.session,
        42,
        failed_command);
    market_data::KalshiWireSessionTestPeer::control_response(
        harness.session,
        R"json({"id":42,"type":"error"})json");
    ASSERT_TRUE(harness.io_to_control.try_pop(status));
    ASSERT_TRUE(std::holds_alternative<control::IoRecoveryRequestFailed>(status));
    EXPECT_FALSE(market_data::KalshiWireSessionTestPeer::has_recovery_tag(
        harness.session,
        101));
}

TEST(WireSessionRecoveryTest, SnapshotIsStampedAndTagClearsAfterRouterHandoff){
    WireHarness harness{2};
    const control::RecoverMarketIo command{
        .recovery_id = 55,
        .universe_version = 7,
        .market_id = 101,
        .request_attempt = 1,
    };
    market_data::KalshiWireSessionTestPeer::seed_recovery(
        harness.session,
        51,
        command);

    market_data::KalshiWireSessionTestPeer::publish(
        harness.session,
        R"json({"sid":9,"seq":1,"type":"orderbook_snapshot","msg":{"market_ticker":"KNOWN"}})json");

    ingest::MarketDataPathMessage message{};
    ASSERT_TRUE(harness.router.try_pop(message));
    ASSERT_TRUE(std::holds_alternative<ingest::FrameHandle>(message));
    const auto handle = std::get<ingest::FrameHandle>(message);
    EXPECT_EQ(handle.recovery_id, command.recovery_id);
    EXPECT_FALSE(market_data::KalshiWireSessionTestPeer::has_recovery_tag(
        harness.session,
        command.market_id));
    EXPECT_TRUE(harness.pool.recycle(handle));
}

} // namespace
