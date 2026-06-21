#include <atomic>
#include <csignal>
#include <iostream>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>

#include "predex/control/control_plane.hpp"
#include "predex/operator/operator_command_handler.hpp"
#include "predex/socket/unix_command_server.hpp"
#include "predex/utils/spsc.hpp"

namespace {

std::atomic<bool> g_signal_stop_requested{false};
constexpr std::size_t kOperatorQueueCapacity = 64;

void signal_handler(int /*signal_number*/) {
    g_signal_stop_requested.store(true);
}

}  // namespace

int main() {
    using namespace std::chrono_literals;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    predex::utils::SPSCQueue<predex::operator_admin::OperatorCommand>
        server_to_control_queue{kOperatorQueueCapacity};
    predex::utils::SPSCQueue<predex::operator_admin::OperatorResponse>
        control_to_server_queue{kOperatorQueueCapacity};
    predex::utils::SPSCQueue<predex::core::control::ControlToIoCommand>
        control_to_io_queue{kOperatorQueueCapacity};
    predex::utils::SPSCQueue<predex::core::control::IoToControlStatus>
        io_to_control_status_queue{kOperatorQueueCapacity};
    predex::utils::SPSCQueue<predex::router::RouterToControl>
        router_to_control_queue{kOperatorQueueCapacity};

    predex::core::control::OperatorQueues control_queues{
        .operator_command_queue = server_to_control_queue,
        .operator_response_queue = control_to_server_queue,
    };
    predex::core::control::ControlIoQueues io_queues{
        .control_to_io_queue = control_to_io_queue,
        .io_to_control_status_queue = io_to_control_status_queue,
    };
    predex::core::control::RouterQueue router_queue{
        .router_to_control_queue = router_to_control_queue,
    };
    predex::operator_admin::ControlQueues handler_queues{
        .server_to_control_queue = server_to_control_queue,
        .control_to_server_queue = control_to_server_queue,
    };

    predex::core::control::ControlPlane control_plane{control_queues, io_queues, router_queue};
    predex::operator_admin::OperatorCommandHandler command_handler{
        handler_queues};
    predex::socket::UnixCommandServer server{
        predex::socket::OperatorSocketConfig{}, &command_handler};

    std::mutex server_error_mutex;
    std::string server_error;
    std::atomic<bool> server_failed{false};

    std::jthread server_thread([&](const std::stop_token& stop_token) {
        std::string local_error;
        server.run(stop_token, local_error);
        if (!local_error.empty()) {
            std::lock_guard<std::mutex> lock(server_error_mutex);
            server_error = local_error;
            server_failed.store(true);
        }
    });

    while (!g_signal_stop_requested.load()) {
        const auto pump_result = control_plane.process_operator_commands();
        (void)control_plane.process_io_status();
        (void)control_plane.process_router_messages();
        const auto process_state = control_plane.process_state();

        if (server_failed.load()) {
            break;
        }

        if (process_state.shutdown_requested) {
            break;
        }

        if (pump_result.commands_processed == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    server_thread.request_stop();
    server.stop();
    server_thread.join();

    if (server_failed.load()) {
        std::lock_guard<std::mutex> lock(server_error_mutex);
        std::cerr << "predex server error: " << server_error << '\n';
        return 1;
    }

    return 0;
}
