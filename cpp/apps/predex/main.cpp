#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>

#include "predex/control/control_plane.hpp"
#include "predex/operator/operator_command_handler.hpp"
#include "predex/shard/shard.hpp"
#include "predex/socket/unix_command_server.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/router/router.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"

namespace {

namespace control = predex::core::control;
namespace operator_admin = predex::operator_admin;
namespace router = predex::router;
namespace shard = predex::shard;
namespace socket = predex::socket;
namespace utils = predex::utils;

std::atomic<bool> g_signal_stop_requested{false};

constexpr std::size_t kOperatorQueueCapacity = 64;
constexpr std::size_t kShardCount = 4;
constexpr std::size_t kShardQueueCapacity = 8192;
constexpr std::size_t kFramePoolCapacity = 8192;
constexpr std::size_t kRouterQueueCapacity = 8192;
constexpr std::size_t kTerminalQueueCapacity = 8192;

constexpr std::size_t kDefaultSleepMs = 10;

using OperatorCommandQueue = utils::SPSCQueue<operator_admin::OperatorCommand>;
using OperatorResponseQueue = utils::SPSCQueue<operator_admin::OperatorResponse>;
using ControlToIoQueue = utils::SPSCQueue<control::ControlToIoCommand>;
using IoToControlStatusQueue = utils::SPSCQueue<control::IoToControlStatus>;
using RouterToControlQueue = utils::SPSCQueue<router::RouterToControl>;
using ControlToShardQueue = utils::SPSCQueue<shard::ControlToShardCommand>;
using ShardToControlQueue = utils::SPSCQueue<shard::ShardToControlMessage>;
using RouterToShardQueue = utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>;
using FrameHandle = predex::ingest::kalshi::FrameHandle;
using FrameHandleQueue = utils::SPSCQueue<FrameHandle>;
using FramePool = predex::ingest::kalshi::FramePool;

void signal_handler(int /*signal_number*/) {
    g_signal_stop_requested.store(true);
}

void install_signal_handlers() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
}

struct AppQueues {
    OperatorCommandQueue server_to_control{kOperatorQueueCapacity};
    OperatorResponseQueue control_to_server{kOperatorQueueCapacity};
    ControlToIoQueue control_to_io{kOperatorQueueCapacity};
    IoToControlStatusQueue io_to_control_status{kOperatorQueueCapacity};
    RouterToControlQueue router_to_control{kOperatorQueueCapacity};
    FrameHandleQueue wire_to_router{kRouterQueueCapacity};
    FrameHandleQueue router_to_logger{kTerminalQueueCapacity};
    FrameHandleQueue router_recycle{kTerminalQueueCapacity};

    std::array<std::unique_ptr<ControlToShardQueue>, kShardCount> control_to_shard;
    std::array<std::unique_ptr<ShardToControlQueue>, kShardCount> shard_to_control;

    std::array<std::unique_ptr<RouterToShardQueue>, kShardCount> router_to_shard;
    std::array<std::unique_ptr<FrameHandleQueue>, kShardCount> shard_to_logger;
    std::array<std::unique_ptr<FrameHandleQueue>, kShardCount> shard_recycle;

    AppQueues() {
        for (std::size_t shard_index = 0; shard_index < kShardCount; ++shard_index) {
            control_to_shard[shard_index] = std::make_unique<ControlToShardQueue>(kShardQueueCapacity);
            shard_to_control[shard_index] = std::make_unique<ShardToControlQueue>(kShardQueueCapacity);
            router_to_shard[shard_index] = std::make_unique<RouterToShardQueue>(kShardQueueCapacity);
            shard_to_logger[shard_index] = std::make_unique<FrameHandleQueue>(kTerminalQueueCapacity);
            shard_recycle[shard_index] = std::make_unique<FrameHandleQueue>(kTerminalQueueCapacity);
        }
    }
};

control::OperatorQueues make_operator_queues(AppQueues& queues) {
    return control::OperatorQueues{
        .operator_command_queue = queues.server_to_control,
        .operator_response_queue = queues.control_to_server,
    };
}

control::ControlIoQueues make_io_queues(AppQueues& queues) {
    return control::ControlIoQueues{
        .control_to_io_queue = queues.control_to_io,
        .io_to_control_status_queue = queues.io_to_control_status,
    };
}

control::RouterQueue make_router_queue(AppQueues& queues) {
    return control::RouterQueue{
        .router_to_control_queue = queues.router_to_control,
    };
}

control::ControlShardQueues make_shard_queues(AppQueues& queues) {
    control::ControlShardQueues shard_queues;
    shard_queues.control_to_shard_queues.reserve(kShardCount);
    shard_queues.shard_to_control_queues.reserve(kShardCount);

    for (std::size_t shard_index = 0; shard_index < kShardCount; ++shard_index) {
        shard_queues.control_to_shard_queues.push_back(queues.control_to_shard[shard_index].get());
        shard_queues.shard_to_control_queues.push_back(queues.shard_to_control[shard_index].get());
    }

    return shard_queues;
}

operator_admin::ControlQueues make_operator_handler_queues(AppQueues& queues) {
    return operator_admin::ControlQueues{
        .server_to_control_queue = queues.server_to_control,
        .control_to_server_queue = queues.control_to_server,
    };
}

predex::router::RouterQueues make_router_queues(AppQueues& queues){
    predex::router::RouterQueues router_queues;
    router_queues.router_to_control_queue = &queues.router_to_control;
    router_queues.router_to_logger_queue = &queues.router_to_logger;
    router_queues.last_resort_recycle_queue = &queues.router_recycle;

    for (std::size_t shard_index = 0; shard_index < kShardCount; ++shard_index) {
        router_queues.router_to_shard_queues.push_back(queues.router_to_shard[shard_index].get());
    }

    return router_queues;
}

shard::ShardQueues make_single_shard_queues(AppQueues& queues, std::size_t shard_index) {
    return shard::ShardQueues{
        .router_to_shard_queue = *queues.router_to_shard[shard_index],
        .shard_to_logger_queue = *queues.shard_to_logger[shard_index],
        .last_resort_recycle_queue = *queues.shard_recycle[shard_index],
        .shard_to_control_queue = *queues.shard_to_control[shard_index],
        .control_to_shard_queue = *queues.control_to_shard[shard_index],
    };
}

struct ServerThreadState {
    std::mutex error_mutex;
    std::string error;
    std::atomic<bool> failed{false};
};

std::jthread start_server_thread(socket::UnixCommandServer& server, ServerThreadState& state) {
    return std::jthread([&server, &state](const std::stop_token& stop_token) {
        std::string local_error;
        server.run(stop_token, local_error);
        if (!local_error.empty()) {
            std::lock_guard<std::mutex> lock(state.error_mutex);
            state.error = local_error;
            state.failed.store(true);
        }
    });
}

std::jthread start_router_thread(router::Router& router_instance, FrameHandleQueue& input_queue) {
    return std::jthread([&router_instance, &input_queue](const std::stop_token& stop_token) {
        FrameHandle handle{};
        while (!stop_token.stop_requested()) {
            if (input_queue.try_pop(handle)) {
                router_instance.route_frame(handle);
            } else {
                std::this_thread::yield();
            }
        }
    });
}

std::jthread start_shard_thread(shard::Shard& shard_instance) {
    return std::jthread([&shard_instance](const std::stop_token& stop_token) {
        while (!stop_token.stop_requested()) {
            (void)shard_instance.drain_control_commands(64); //NOLINT: arbitrary max commands to process per iteration
            const auto result = shard_instance.pump_once();

            if (result.code == shard::ShardPumpCode::kIDLE) {
                std::this_thread::yield();
            }
        }
    });
}

bool drain_frame_queue(FrameHandleQueue& queue, FramePool& frame_pool) {
    bool did_work = false;
    FrameHandle handle{};
    while (queue.try_pop(handle)) {
        (void)frame_pool.recycle(handle);
        did_work = true;
    }
    return did_work;
}

bool drain_terminal_frame_queues(AppQueues& queues, FramePool& frame_pool) {
    bool did_work = false;

    did_work = drain_frame_queue(queues.router_to_logger, frame_pool) || did_work;
    did_work = drain_frame_queue(queues.router_recycle, frame_pool) || did_work;

    for (std::size_t shard_index = 0; shard_index < kShardCount; ++shard_index) {
        did_work = drain_frame_queue(*queues.shard_to_logger[shard_index], frame_pool) || did_work;
        did_work = drain_frame_queue(*queues.shard_recycle[shard_index], frame_pool) || did_work;
    }

    return did_work;
}

std::jthread start_terminal_recycler_thread(AppQueues& queues, FramePool& frame_pool) {
    return std::jthread([&queues, &frame_pool](const std::stop_token& stop_token) {
        while (!stop_token.stop_requested()) {
            if (!drain_terminal_frame_queues(queues, frame_pool)) {
                std::this_thread::yield();
            }
        }

        while (drain_terminal_frame_queues(queues, frame_pool)) {}
    });
}

bool pump_control_plane_once(control::ControlPlane& control_plane) {
    const auto operator_result = control_plane.process_operator_commands();
    const auto io_result = control_plane.process_io_status();
    const bool processed_router_messages = control_plane.process_router_messages();
    const bool processed_shard_messages = control_plane.process_shard_messages();

    return operator_result.commands_processed > 0 ||
           io_result.statuses_processed > 0 ||
           processed_router_messages ||
           processed_shard_messages;
}

}  // namespace

int main() {
    using namespace std::chrono_literals;

    install_signal_handlers();

    AppQueues app_queues;

    auto operator_queues = make_operator_queues(app_queues);
    auto io_queues = make_io_queues(app_queues);
    auto router_queue = make_router_queue(app_queues);
    auto shard_queues = make_shard_queues(app_queues);
    auto handler_queues = make_operator_handler_queues(app_queues);
    auto router_queues = make_router_queues(app_queues);

    control::ControlPlane control_plane{operator_queues, io_queues, router_queue, shard_queues};
    operator_admin::OperatorCommandHandler command_handler{handler_queues};
    socket::UnixCommandServer server{socket::OperatorSocketConfig{}, &command_handler};


    FramePool frame_pool{kFramePoolCapacity};
    router::Router router_instance{router_queues};

    std::array<std::unique_ptr<shard::Shard>, kShardCount> shards;
    for (std::size_t i = 0; i < kShardCount; ++i) {
        auto shard_queues = make_single_shard_queues(app_queues, i);
        shards[i] = std::make_unique<shard::Shard>(
            static_cast<std::uint32_t>(i),
            shard_queues,
            frame_pool
        );
    }

    std::array<std::jthread, kShardCount> shard_threads;
    for (std::size_t i = 0; i < kShardCount; ++i) {
        shard_threads[i] = start_shard_thread(*shards[i]);
    }

    ServerThreadState server_state;
    std::jthread server_thread = start_server_thread(server, server_state);
    std::jthread router_thread = start_router_thread(router_instance, app_queues.wire_to_router);
    std::jthread terminal_recycler_thread = start_terminal_recycler_thread(app_queues, frame_pool);

    while (!g_signal_stop_requested.load()) {
        const bool did_work = pump_control_plane_once(control_plane);
        const auto process_state = control_plane.process_state();

        if (server_state.failed.load()) {
            break;
        }

        if (process_state.shutdown_requested) {
            break;
        }

        if (!did_work) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kDefaultSleepMs));
        }
    }

    router_thread.request_stop();
    for(auto& shard_thread : shard_threads){
        shard_thread.request_stop();
    }
    server_thread.request_stop();

    server.stop();

    router_thread.join();
    for(auto& shard_thread : shard_threads){
        shard_thread.join();
    }
    terminal_recycler_thread.request_stop();
    terminal_recycler_thread.join();
    server_thread.join();

    if (server_state.failed.load()) {
        std::lock_guard<std::mutex> lock(server_state.error_mutex);
        std::cerr << "predex server error: " << server_state.error << '\n';
        return 1;
    }

    return 0;
}
