#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <cstdlib>
#include <optional>
#include <ranges>

#include "predex/config/app_config.hpp"
#include "predex/config/universe_builder.hpp"
#include "predex/control/control_plane.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/operator/operator_command_handler.hpp"
#include "predex/router/router.hpp"
#include "predex/shard/shard.hpp"
#include "predex/socket/unix_command_server.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/logging/market_data_logger.hpp"
#include "predex/exchange/kalshi/adapters/auth_signer.hpp"
#include "predex/exchange/kalshi/adapters/market_data_handler.hpp"
#include "predex/ingest/kalshi/market_data/wire_session.hpp"

namespace {

namespace config = predex::config;
namespace control = predex::core::control;
namespace operator_admin = predex::operator_admin;
namespace router = predex::router;
namespace shard = predex::shard;
namespace socket = predex::socket;
namespace utils = predex::utils;
namespace logging = predex::logging;
namespace kalshi_exchange = predex::exchange::kalshi;
namespace market_data = predex::ingest::kalshi::market_data;

std::atomic<bool> g_signal_stop_requested{false};

constexpr std::size_t kDefaultSleepMs = 10;
constexpr std::string_view kDefaultConfigPath = "docs/app_config.example.json";

constexpr std::size_t kLoggerPumpBatchSize = 256;

using OperatorCommandQueue = utils::SPSCQueue<operator_admin::OperatorCommand>;
using OperatorResponseQueue = utils::SPSCQueue<operator_admin::OperatorResponse>;
using ControlToIoQueue = utils::SPSCQueue<control::ControlToIoCommand>;
using IoToControlStatusQueue = utils::SPSCQueue<control::IoToControlStatus>;
using LoggerToControlStatusQueue = utils::SPSCQueue<control::LoggerToControlStatus>;
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

std::string_view config_path_from_args(int argc, char** argv) {
    if (argc > 1 && argv[1] != nullptr) {
        return argv[1];
    }
    return kDefaultConfigPath;
}

std::uint32_t checked_shard_count(std::size_t shard_count) {
    if (shard_count == 0 || shard_count > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("Configured shard_count is outside uint32_t range");
    }
    return static_cast<std::uint32_t>(shard_count);
}
std::string required_env_value(const std::string& env_name) {
    const char* value = std::getenv(env_name.c_str());
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error("Missing required environment variable: " + env_name);
    }
    return std::string{value};
}

kalshi_exchange::KalshiMarketDataHandler make_market_data_handler(
    const config::KalshiAuthConfig& auth_config
) {
    kalshi_exchange::Credentials credentials{
        .key_id = required_env_value(auth_config.key_id_env),
        .private_key_pem = required_env_value(auth_config.private_key_pem_env),
    };

    return kalshi_exchange::KalshiMarketDataHandler{
        kalshi_exchange::AuthSigner{std::move(credentials)}
    };
}
struct AppQueues {
    explicit AppQueues(const config::RuntimeConfig& runtime_config)
        : server_to_control(runtime_config.operator_queue_capacity),
          control_to_server(runtime_config.operator_queue_capacity),
          control_to_io(runtime_config.operator_queue_capacity),
          io_to_control_status(runtime_config.operator_queue_capacity),
          logger_to_control_status(runtime_config.operator_queue_capacity),
          router_to_control(runtime_config.operator_queue_capacity),
          wire_to_router(runtime_config.router_queue_capacity),
          router_to_logger(runtime_config.router_queue_capacity),
          router_recycle(runtime_config.router_queue_capacity),
          logger_recycle(runtime_config.router_queue_capacity),
          wire_to_logger(runtime_config.router_queue_capacity) {
        control_to_shard.reserve(runtime_config.shard_count);
        shard_to_control.reserve(runtime_config.shard_count);
        router_to_shard.reserve(runtime_config.shard_count);
        shard_to_logger.reserve(runtime_config.shard_count);
        shard_recycle.reserve(runtime_config.shard_count);

        for (std::size_t shard_index = 0; shard_index < runtime_config.shard_count; ++shard_index) {
            control_to_shard.push_back(std::make_unique<ControlToShardQueue>(runtime_config.shard_queue_capacity));
            shard_to_control.push_back(std::make_unique<ShardToControlQueue>(runtime_config.shard_queue_capacity));
            router_to_shard.push_back(std::make_unique<RouterToShardQueue>(runtime_config.shard_queue_capacity));
            shard_to_logger.push_back(std::make_unique<FrameHandleQueue>(runtime_config.router_queue_capacity));
            shard_recycle.push_back(std::make_unique<FrameHandleQueue>(runtime_config.router_queue_capacity));
        }
    }

    OperatorCommandQueue server_to_control;
    OperatorResponseQueue control_to_server;
    ControlToIoQueue control_to_io;
    IoToControlStatusQueue io_to_control_status;
    LoggerToControlStatusQueue logger_to_control_status;
    RouterToControlQueue router_to_control;
    FrameHandleQueue wire_to_router;
    FrameHandleQueue router_to_logger;
    FrameHandleQueue router_recycle;
    FrameHandleQueue logger_recycle;
    FrameHandleQueue wire_to_logger;

    std::vector<std::unique_ptr<ControlToShardQueue>> control_to_shard;
    std::vector<std::unique_ptr<ShardToControlQueue>> shard_to_control;

    std::vector<std::unique_ptr<RouterToShardQueue>> router_to_shard;
    std::vector<std::unique_ptr<FrameHandleQueue>> shard_to_logger;
    std::vector<std::unique_ptr<FrameHandleQueue>> shard_recycle;
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

control::ControlLoggerQueue make_logger_queue(AppQueues& queues) {
    return control::ControlLoggerQueue{
        .logger_to_control_status_queue = &queues.logger_to_control_status,
    };
}

control::ControlShardQueues make_shard_queues(AppQueues& queues) {
    control::ControlShardQueues shard_queues;
    shard_queues.control_to_shard_queues.reserve(queues.control_to_shard.size());
    shard_queues.shard_to_control_queues.reserve(queues.shard_to_control.size());

    for (std::size_t shard_index = 0; shard_index < queues.control_to_shard.size(); ++shard_index) {
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

    for (const auto& shard_index : std::views::iota(std::size_t{0}, queues.router_to_shard.size())) {
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

std::vector<FrameHandleQueue*> make_logger_input_queues(AppQueues& queues) {
    std::vector<FrameHandleQueue*> input_queues;
    input_queues.reserve(1 + queues.shard_to_logger.size());
    input_queues.push_back(&queues.router_to_logger);
    input_queues.push_back(&queues.wire_to_logger);
    for (auto& shard_logger_queue : queues.shard_to_logger) {
        input_queues.push_back(shard_logger_queue.get());
    }
    return input_queues;
}

market_data::RecycleQueues make_wire_recycle_queues(AppQueues& queues) {
    market_data::RecycleQueues recycle_queues;
    recycle_queues.reserve(2 + queues.shard_recycle.size());

    recycle_queues.push_back(&queues.router_recycle);
    recycle_queues.push_back(&queues.logger_recycle);

    for (auto& shard_recycle_queue : queues.shard_recycle) {
        recycle_queues.push_back(shard_recycle_queue.get());
    }

    return recycle_queues;
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

std::jthread start_wire_session_thread(market_data::KalshiWireSession& wire_session) {
    return std::jthread([&wire_session](const std::stop_token& stop_token) {
        wire_session.run(stop_token);
    });
}


std::jthread start_market_data_logger_thread(logging::MarketDataLogger& market_data_logger) {
    return std::jthread([&market_data_logger](const std::stop_token& stop_token) {
        while (!stop_token.stop_requested()) {
            if (market_data_logger.pump(kLoggerPumpBatchSize) == 0) {
                std::this_thread::yield();
            }
        }

        while (market_data_logger.pump(kLoggerPumpBatchSize) > 0) {}
    });
}

bool pump_control_plane_once(control::ControlPlane& control_plane) {
    const auto operator_result = control_plane.process_operator_commands();
    const auto io_result = control_plane.process_io_status();
    const bool processed_router_messages = control_plane.process_router_messages();
    const bool processed_shard_messages = control_plane.process_shard_messages();
    const bool processed_logger_messages = control_plane.process_logger_messages();

    return operator_result.commands_processed > 0 ||
           io_result.statuses_processed > 0 ||
           processed_router_messages ||
           processed_shard_messages ||
           processed_logger_messages;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace std::chrono_literals;

    config::AppConfig app_config;
    try {
        app_config = config::load_app_config(config_path_from_args(argc, argv));
    } catch (const std::exception& e) {
        std::cerr << "predex config error: " << e.what() << '\n';
        return 1;
    }

    install_signal_handlers();

    std::uint32_t shard_count{0};
    try {
        shard_count = checked_shard_count(app_config.runtime.shard_count);
    } catch (const std::exception& e) {
        std::cerr << "predex config error: " << e.what() << '\n';
        return 1;
    }
    AppQueues app_queues{app_config.runtime};

    auto operator_queues = make_operator_queues(app_queues);
    auto io_queues = make_io_queues(app_queues);
    auto router_queue = make_router_queue(app_queues);
    auto shard_queues = make_shard_queues(app_queues);
    auto logger_queue = make_logger_queue(app_queues);
    auto handler_queues = make_operator_handler_queues(app_queues);
    auto router_queues = make_router_queues(app_queues);

    control::ControlPlane control_plane{operator_queues, io_queues, router_queue, shard_queues, logger_queue};
    operator_admin::OperatorCommandHandler command_handler{handler_queues};
    socket::OperatorSocketConfig socket_config{};
    socket_config.socket_path = app_config.runtime.operator_socket_path;
    socket::UnixCommandServer server{std::move(socket_config), &command_handler};


    FramePool frame_pool{app_config.runtime.frame_pool_capacity};
    router::Router router_instance{router_queues};

    std::vector<std::unique_ptr<shard::Shard>> shards;
    shards.reserve(app_config.runtime.shard_count);
    for (std::size_t i = 0; i < app_config.runtime.shard_count; ++i) {
        auto shard_queues = make_single_shard_queues(app_queues, i);
        shards.push_back(std::make_unique<shard::Shard>(
            static_cast<std::uint32_t>(i),
            shard_queues,
            frame_pool
        ));
    }

    try {
        auto universe = config::build_universe_snapshot(app_config, shard_count);
        (void)control_plane.install_universe(std::move(universe));
        const auto shard_install_result = control_plane.send_active_universe_to_shards();
        if (shard_install_result.commands_pushed_failure != 0) {
            throw std::runtime_error("Failed to enqueue universe install command to one or more shards");
        }
    } catch (const std::exception& e) {
        std::cerr << "predex universe init error: " << e.what() << '\n';
        return 1;
    }

    std::vector<std::jthread> shard_threads;
    shard_threads.reserve(shards.size());
    for (const auto& shard_index : std::views::iota(std::size_t{0}, shards.size())) {
        shard_threads.push_back(start_shard_thread(*shards[shard_index]));
    }

    ServerThreadState server_state;
    std::jthread server_thread = start_server_thread(server, server_state);
    std::jthread router_thread = start_router_thread(router_instance, app_queues.wire_to_router);
    logging::MarketDataLogger market_data_logger{
        logging::MarketDataLoggerDeps{
            .input_queues = make_logger_input_queues(app_queues),
            .frame_pool = frame_pool,
            .recycle_queue = app_queues.logger_recycle,
            .logger_to_control_status_queue = &app_queues.logger_to_control_status,
            .output_file_path = app_config.runtime.market_data_tape_path,
        }
    };
    std::jthread market_data_logger_thread = start_market_data_logger_thread(market_data_logger);

    std::optional<market_data::KalshiWireSession> wire_session;
    std::optional<std::jthread> wire_session_thread;

    if (app_config.kalshi.market_data.enable_market_data) {
        try {
            wire_session.emplace(market_data::KalshiWireSessionDeps{
                .frame_pool = frame_pool,
                .control_queues = market_data::ControlQueues{
                    .control_to_io_queue = app_queues.control_to_io,
                    .io_to_control_status_queue = app_queues.io_to_control_status,
                },
                .recycle_queues = make_wire_recycle_queues(app_queues),
                .router_queue = app_queues.wire_to_router,
                .logger_queue = app_queues.wire_to_logger,
                .market_data_handler = make_market_data_handler(app_config.kalshi.auth),
                .desired_channels = app_config.kalshi.market_data.channels,
            });

            wire_session_thread.emplace(start_wire_session_thread(*wire_session));

            if (!control_plane.send_active_universe_to_io()) {
                throw std::runtime_error("Failed to enqueue active universe to IO");
            }
            if (!control_plane.push_io_command(control::ControlToIoCommand{control::ConnectIo{}})) {
                throw std::runtime_error("Failed to enqueue IO connect command");
            }
        } catch (const std::exception& e) {
            std::cerr << "predex market data init error: " << e.what() << '\n';
            return 1;
        }
    }
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

    if (wire_session_thread.has_value()) {
        wire_session_thread->request_stop();
    }
    router_thread.request_stop();
    for(auto& shard_thread : shard_threads){
        shard_thread.request_stop();
    }
    server_thread.request_stop();

    server.stop();

    if (wire_session_thread.has_value()) {
        wire_session_thread->join();
    }
    router_thread.join();
    for(auto& shard_thread : shard_threads){
        shard_thread.join();
    }
    market_data_logger_thread.request_stop();
    market_data_logger_thread.join();
    server_thread.join();

    if (server_state.failed.load()) {
        std::lock_guard<std::mutex> lock(server_state.error_mutex);
        std::cerr << "predex server error: " << server_state.error << '\n';
        return 1;
    }

    return 0;
}
