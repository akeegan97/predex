#include "predex/control/control_plane.hpp"

#include <iostream>

namespace {

int fail(const char* message) {
    std::cerr << "control_plane_operator_test: " << message << '\n';
    return 1;
}

} // namespace

int main() {
    predex::utils::SPSCQueue<predex::core::control::ControlIoCommand> control_io_queue{8};
    predex::utils::SPSCQueue<predex::core::control::IoControlStatus> io_status_queue{8};
    predex::utils::SPSCQueue<predex::core::operator_commands::OperatorCommand> command_queue{8};
    predex::utils::SPSCQueue<predex::core::operator_commands::OperatorResponse> response_queue{8};

    predex::core::control::kalshi::ControlPlane control_plane{
        predex::core::control::kalshi::IoQueues{
            .control_io_queue = control_io_queue,
            .io_control_status = io_status_queue,
        },
        predex::core::control::kalshi::OperatorQueues{
            .operator_command_queue = command_queue,
            .operator_response_queue = response_queue,
        },
        []() {
            predex::core::operator_commands::OperatorStatusSnapshot snapshot{};
            snapshot.process_state.status = predex::core::control::ProcessStatus::kLive;
            snapshot.app_running = true;
            snapshot.live_orders = 7;
            snapshot.public_ws_generation = 3;
            snapshot.last_error = "";
            return snapshot;
        },
    };

    if (!command_queue.try_push(predex::core::operator_commands::OperatorCommand::status(101))) {
        return fail("failed to enqueue status command");
    }
    if (control_plane.pump_operator_commands() != 1U) {
        return fail("expected one processed status command");
    }

    predex::core::operator_commands::OperatorResponse status_response{};
    if (!response_queue.try_pop(status_response)) {
        return fail("missing status response");
    }
    if (status_response.request_id != 101U ||
        status_response.command_type != predex::core::operator_commands::OperatorCommandType::kStatus ||
        status_response.type != predex::core::operator_commands::OperatorResponseType::kCompleted) {
        return fail("unexpected status response envelope");
    }
    if (!status_response.status_snapshot.has_value() ||
        !status_response.status_snapshot->app_running ||
        status_response.status_snapshot->live_orders != 7U) {
        return fail("unexpected status payload");
    }

    if (!command_queue.try_push(
            predex::core::operator_commands::OperatorCommand::begin_shutdown(202))) {
        return fail("failed to enqueue shutdown command");
    }
    if (control_plane.pump_operator_commands() != 1U) {
        return fail("expected one processed shutdown command");
    }

    predex::core::operator_commands::OperatorResponse shutdown_response{};
    if (!response_queue.try_pop(shutdown_response)) {
        return fail("missing shutdown response");
    }
    if (shutdown_response.request_id != 202U ||
        shutdown_response.command_type !=
            predex::core::operator_commands::OperatorCommandType::kBeginShutdown ||
        shutdown_response.type != predex::core::operator_commands::OperatorResponseType::kAccepted) {
        return fail("unexpected shutdown response envelope");
    }
    if (control_plane.process_state().status != predex::core::control::ProcessStatus::kShuttingDown) {
        return fail("control plane did not enter shutting down state");
    }

    return 0;
}
