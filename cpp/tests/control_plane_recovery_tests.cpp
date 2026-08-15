#include <chrono>
#include <cstdint>
#include <variant>

#include <gtest/gtest.h>

#include "predex/control/control_plane.hpp"

namespace {

namespace control = predex::core::control;
namespace ingest = predex::ingest::kalshi;
namespace operator_admin = predex::operator_admin;
namespace router = predex::router;
namespace utils = predex::utils;

TEST(ControlPlaneRecoveryTest, DeliveredSubscriptionBarrierExpandsIntoMarketCommands){
    utils::SPSCQueue<operator_admin::OperatorCommand> operator_commands{4};
    utils::SPSCQueue<operator_admin::OperatorResponse> operator_responses{4};
    utils::SPSCQueue<control::ControlToIoCommand> control_to_io{8};
    utils::SPSCQueue<control::IoToControlStatus> io_to_control{8};
    utils::SPSCQueue<router::RouterToControl> router_to_control{8};

    control::ControlPlane control_plane{
        control::OperatorQueues{operator_commands, operator_responses},
        control::ControlIoQueues{control_to_io, io_to_control},
        control::RouterQueue{router_to_control},
        {},
        {},
        {},
        {},
        {},
        control::RequiredComponents{
            .market_data = true,
            .shards = false,
            .logger = false,
        }};

    control::UniverseSnapshot universe{};
    universe.market_routes = {
        control::UniverseMarketRoute{
            .kalshi_ticker = "MKT-A",
            .market_id = 101,
            .event_id = 11,
            .shard_index = 0,
        },
        control::UniverseMarketRoute{
            .kalshi_ticker = "MKT-B",
            .market_id = 205,
            .event_id = 22,
            .shard_index = 1,
        },
    };
    const auto universe_version = control_plane.install_universe(
        std::move(universe));

    ASSERT_TRUE(io_to_control.try_push(control::IoToControlStatus{
        control::IoConnected{}}));
    ASSERT_TRUE(io_to_control.try_push(control::IoToControlStatus{
        control::IoUniverseSnapshotApplied{.version = universe_version}}));
    ASSERT_TRUE(io_to_control.try_push(control::IoToControlStatus{
        control::IoSubscriptionReady{.version = universe_version}}));
    EXPECT_EQ(control_plane.process_io_status().statuses_processed, 3U);

    ASSERT_TRUE(router_to_control.try_push(router::RouterToControl{
        router::OrderBookSubscriptionBarrierDelivered{
            .universe_version = universe_version,
            .incident = {
                .origin = ingest::IntegrityIncidentOrigin::kWIRE_SESSION,
                .producer_index = 0,
                .incident_id = 77,
            },
            .sid = 9,
            .expected_sequence = 30,
            .observed_sequence = 34,
            .reason = ingest::BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP,
        }}));
    EXPECT_TRUE(control_plane.process_router_messages());

    const auto pump = control_plane.process_recovery(
        std::chrono::steady_clock::now());
    EXPECT_EQ(pump.commands_pushed_success, 2U);
    EXPECT_EQ(pump.commands_pushed_failure, 0U);

    control::ControlToIoCommand command{};
    ASSERT_TRUE(control_to_io.try_pop(command));
    ASSERT_TRUE(std::holds_alternative<control::RecoverMarketIo>(command));
    const auto first = std::get<control::RecoverMarketIo>(command);
    ASSERT_TRUE(control_to_io.try_pop(command));
    ASSERT_TRUE(std::holds_alternative<control::RecoverMarketIo>(command));
    const auto second = std::get<control::RecoverMarketIo>(command);
    EXPECT_EQ(first.recovery_id, second.recovery_id);
    EXPECT_NE(first.market_id, second.market_id);

    const auto state = control_plane.process_state();
    EXPECT_EQ(state.recovery_telemetry.incidents_created, 1U);
    EXPECT_EQ(state.recovery_telemetry.markets_scheduled, 2U);
    EXPECT_EQ(state.recovery_telemetry.requests_enqueued, 2U);
    EXPECT_EQ(state.recovery_telemetry.active_incidents, 1U);
    EXPECT_EQ(state.recovery_telemetry.active_markets, 2U);
}

} // namespace
