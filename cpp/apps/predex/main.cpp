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

#include <curl/curl.h>

#include "predex/config/app_config.hpp"
#include "predex/config/universe_builder.hpp"
#include "predex/control/control_plane.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/ingest/kalshi/order_data/order_session.hpp"
#include "predex/operator/operator_command_handler.hpp"
#include "predex/oms/oms.hpp"
#include "predex/router/router.hpp"
#include "predex/shard/shard.hpp"
#include "predex/socket/unix_command_server.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/utils/idle_backoff.hpp"
#include "predex/logging/market_data_logger.hpp"
#include "predex/exchange/kalshi/adapters/auth_signer.hpp"
#include "predex/exchange/kalshi/adapters/market_data_handler.hpp"
#include "predex/exchange/kalshi/adapters/order_data_handler.hpp"
#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"
#include "predex/exchange/kalshi/http2_session.hpp"
#include "predex/exchange/kalshi/order_rest_session.hpp"
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
namespace order_data = predex::ingest::kalshi::order_data;
namespace oms = predex::oms;

std::atomic<bool> g_signal_stop_requested{false};

constexpr std::size_t kDefaultSleepMs = 10;
constexpr std::string_view kDefaultConfigPath = "docs/app_config.example.json";

constexpr std::size_t kLoggerPumpBatchSize = 256;
using MarketDataPathMessage = predex::ingest::kalshi::MarketDataPathMessage;
using OperatorCommandQueue = utils::SPSCQueue<operator_admin::OperatorCommand>;
using OperatorResponseQueue = utils::SPSCQueue<operator_admin::OperatorResponse>;
using ControlToIoQueue = utils::SPSCQueue<control::ControlToIoCommand>;
using IoToControlStatusQueue = utils::SPSCQueue<control::IoToControlStatus>;
using LoggerToControlStatusQueue = utils::SPSCQueue<control::LoggerToControlStatus>;
using RouterToControlQueue = utils::SPSCQueue<router::RouterToControl>;
using ControlToShardQueue = utils::SPSCQueue<shard::ControlToShardCommand>;
using ShardToControlQueue = utils::SPSCQueue<shard::ShardToControlMessage>;
using RouterToShardQueue = utils::SPSCQueue<MarketDataPathMessage>;
using ControlToOmsQueue = utils::SPSCQueue<control::ControlToOmsCommand>;
using OmsToControlStatusQueue = utils::SPSCQueue<control::OmsToControlStatus>;
using StrategyIntentQueue = utils::SPSCQueue<oms::intent::StrategyIntent>;
using OmsToStrategyQueue = utils::SPSCQueue<oms::OmsToStrategyMessage>;
using OmsToKalshiQueue = utils::SPSCQueue<oms::OmsToKalshiCommand>;
using KalshiToOmsQueue = utils::SPSCQueue<oms::KalshiToOmsEvent>;
using ControlToOrderRestQueue = utils::SPSCQueue<control::ControlToOrderRestCommand>;
using OrderRestToControlStatusQueue = utils::SPSCQueue<control::OrderRestToControlStatus>;
using ControlToPrivateOrderFeedQueue = utils::SPSCQueue<control::ControlToPrivateOrderFeedCommand>;
using PrivateOrderFeedToControlStatusQueue = utils::SPSCQueue<control::PrivateOrderFeedToControlStatus>;
using FrameHandle = predex::ingest::kalshi::FrameHandle;

using FrameHandleQueue = utils::SPSCQueue<FrameHandle>;
using MarketDataPathMessageQueue = utils::SPSCQueue<MarketDataPathMessage>;
using FramePool = predex::ingest::kalshi::FramePool;

struct CurlGlobalGuard {
    CurlGlobalGuard() {
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
            throw std::runtime_error("curl_global_init failed");
        }
    }

    ~CurlGlobalGuard() {
        curl_global_cleanup();
    }

    CurlGlobalGuard(const CurlGlobalGuard&) = delete;
    CurlGlobalGuard& operator=(const CurlGlobalGuard&) = delete;
};

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

kalshi_exchange::Credentials make_kalshi_credentials(const config::KalshiAuthConfig& auth_config) {
    return kalshi_exchange::Credentials{
        .key_id = required_env_value(auth_config.key_id_env),
        .private_key_pem = required_env_value(auth_config.private_key_pem_env),
    };
}

kalshi_exchange::KalshiMarketDataHandler make_market_data_handler(
    const config::KalshiAuthConfig& auth_config
) {
    kalshi_exchange::Credentials credentials = make_kalshi_credentials(auth_config);

    return kalshi_exchange::KalshiMarketDataHandler{
        kalshi_exchange::AuthSigner{std::move(credentials)}
    };
}

kalshi_exchange::KalshiOrderDataHandler make_order_data_handler(
    const config::KalshiAuthConfig& auth_config
) {
    kalshi_exchange::Credentials credentials = make_kalshi_credentials(auth_config);

    return kalshi_exchange::KalshiOrderDataHandler{
        kalshi_exchange::AuthSigner{std::move(credentials)}
    };
}

kalshi_exchange::Http2Session make_http_session(
    const config::KalshiAuthConfig& auth_config,
    const config::KalshiOrderRestConfig& order_rest_config
) {
    kalshi_exchange::Http2SessionConfig http_config{};
    if(!order_rest_config.endpoint.empty()){
        http_config.endpoint = order_rest_config.endpoint;
    }
    http_config.max_concurrent_streams = static_cast<std::uint16_t>(order_rest_config.max_concurrent_streams);

    return kalshi_exchange::Http2Session{
        kalshi_exchange::AuthSigner{make_kalshi_credentials(auth_config)},
        std::move(http_config),
    };
}

control::RequiredComponents make_required_components(const config::AppConfig& app_config) {
    const bool order_graph_enabled =
        app_config.kalshi.order_rest.enable_order_rest ||
        app_config.kalshi.private_order_feed.enable_private_order_feed;

    return control::RequiredComponents{
        .market_data = app_config.kalshi.market_data.enable_market_data,
        .shards = true,
        .logger = true,
        .oms = order_graph_enabled,
        .private_order_feed = app_config.kalshi.private_order_feed.enable_private_order_feed,
        .order_rest = app_config.kalshi.order_rest.enable_order_rest,
    };
}

control::SyntheticTradingSessionConfig make_synthetic_session_config(const config::RuntimeConfig& runtime_config) {
    constexpr std::uint64_t kNsPerSecond = 1'000'000'000ULL;
    return control::SyntheticTradingSessionConfig{
        .enabled = runtime_config.synthetic_trading_session_enabled,
        .reduce_only_after_ns = runtime_config.reduce_only_after_seconds * kNsPerSecond,
        .flatten_to_zero_after_ns = runtime_config.flatten_to_zero_after_seconds * kNsPerSecond,
        .stopped_after_ns = runtime_config.stopped_after_seconds * kNsPerSecond,
    };
}

struct AppQueues {
    explicit AppQueues(const config::RuntimeConfig& runtime_config)
        : server_to_control(runtime_config.operator_queue_capacity),
          control_to_server(runtime_config.operator_queue_capacity),
          control_to_io(runtime_config.operator_queue_capacity),
          io_to_control_status(runtime_config.operator_queue_capacity),
          logger_to_control_status(runtime_config.operator_queue_capacity),
          control_to_oms(runtime_config.operator_queue_capacity),
          oms_to_control_status(runtime_config.operator_queue_capacity),
          oms_to_order_rest(runtime_config.shard_queue_capacity),
          order_rest_to_oms(runtime_config.shard_queue_capacity),
          private_order_feed_to_oms(runtime_config.shard_queue_capacity),
          control_to_order_rest(runtime_config.operator_queue_capacity),
          order_rest_to_control_status(runtime_config.operator_queue_capacity),
          control_to_private_order_feed(runtime_config.operator_queue_capacity),
          private_order_feed_to_control_status(runtime_config.operator_queue_capacity),
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
        strategy_to_oms.reserve(runtime_config.shard_count);
        oms_to_strategy.reserve(runtime_config.shard_count);

        for (std::size_t shard_index = 0; shard_index < runtime_config.shard_count; ++shard_index) {
            control_to_shard.push_back(std::make_unique<ControlToShardQueue>(runtime_config.shard_queue_capacity));
            shard_to_control.push_back(std::make_unique<ShardToControlQueue>(runtime_config.shard_queue_capacity));
            router_to_shard.push_back(std::make_unique<RouterToShardQueue>(runtime_config.shard_queue_capacity));
            shard_to_logger.push_back(std::make_unique<FrameHandleQueue>(runtime_config.router_queue_capacity));
            shard_recycle.push_back(std::make_unique<FrameHandleQueue>(runtime_config.router_queue_capacity));
            strategy_to_oms.push_back(std::make_unique<StrategyIntentQueue>(runtime_config.shard_queue_capacity));
            oms_to_strategy.push_back(std::make_unique<OmsToStrategyQueue>(runtime_config.shard_queue_capacity));
        }
    }

    OperatorCommandQueue server_to_control;
    OperatorResponseQueue control_to_server;
    ControlToIoQueue control_to_io;
    IoToControlStatusQueue io_to_control_status;
    LoggerToControlStatusQueue logger_to_control_status;
    ControlToOmsQueue control_to_oms;
    OmsToControlStatusQueue oms_to_control_status;
    OmsToKalshiQueue oms_to_order_rest;
    KalshiToOmsQueue order_rest_to_oms;
    KalshiToOmsQueue private_order_feed_to_oms;
    ControlToOrderRestQueue control_to_order_rest;
    OrderRestToControlStatusQueue order_rest_to_control_status;
    ControlToPrivateOrderFeedQueue control_to_private_order_feed;
    PrivateOrderFeedToControlStatusQueue private_order_feed_to_control_status;
    RouterToControlQueue router_to_control;
    MarketDataPathMessageQueue wire_to_router;
    FrameHandleQueue router_to_logger;
    FrameHandleQueue router_recycle;
    FrameHandleQueue logger_recycle;
    FrameHandleQueue wire_to_logger;

    std::vector<std::unique_ptr<ControlToShardQueue>> control_to_shard;
    std::vector<std::unique_ptr<ShardToControlQueue>> shard_to_control;
    std::vector<std::unique_ptr<StrategyIntentQueue>> strategy_to_oms;
    std::vector<std::unique_ptr<OmsToStrategyQueue>> oms_to_strategy;

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

control::ControlOmsQueues make_control_oms_queues(AppQueues& queues) {
    return control::ControlOmsQueues{
        .control_to_oms_queue = &queues.control_to_oms,
        .oms_to_control_status_queue = &queues.oms_to_control_status,
    };
}

control::ControlPrivateOrderFeedQueues make_control_private_order_feed_queues(AppQueues& queues) {
    return control::ControlPrivateOrderFeedQueues{
        .control_to_private_order_feed_queue = &queues.control_to_private_order_feed,
        .private_order_feed_to_control_status_queue = &queues.private_order_feed_to_control_status,
    };
}

control::ControlOrderRestQueues make_control_order_rest_queues(AppQueues& queues) {
    return control::ControlOrderRestQueues{
        .control_to_order_rest_queue = &queues.control_to_order_rest,
        .order_rest_to_control_status_queue = &queues.order_rest_to_control_status,
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

oms::OmsQueues make_oms_queues(AppQueues& queues){
    oms::OmsQueues oms_queues{
        .control_command_queue = queues.control_to_oms,
        .oms_status_queue = queues.oms_to_control_status,
        .kalshi_command_queue = queues.oms_to_order_rest,
    };

    oms_queues.strategy_intent_queues.reserve(queues.strategy_to_oms.size());
    oms_queues.strategy_response_queues.reserve(queues.oms_to_strategy.size());
    for(const auto& strategy_queue : queues.strategy_to_oms){
        oms_queues.strategy_intent_queues.push_back(strategy_queue.get());
    }
    for(const auto& response_queue : queues.oms_to_strategy){
        oms_queues.strategy_response_queues.push_back(response_queue.get());
    }

    oms_queues.venue_event_queues.push_back(&queues.order_rest_to_oms);
    oms_queues.venue_event_queues.push_back(&queues.private_order_feed_to_oms);

    return oms_queues;
}

kalshi_exchange::OrderRestControlQueues make_order_rest_control_queues(AppQueues& queues){
    return kalshi_exchange::OrderRestControlQueues{
        .control_to_order_rest_queue = queues.control_to_order_rest,
        .order_rest_to_control_queue = queues.order_rest_to_control_status,
    };
}

kalshi_exchange::OrderRestOmsQueues make_order_rest_oms_queues(AppQueues& queues){
    return kalshi_exchange::OrderRestOmsQueues{
        .oms_to_order_rest_queue = queues.oms_to_order_rest,
        .order_rest_to_oms_queue = queues.order_rest_to_oms,
    };
}

order_data::OrderSessionControlQueues make_order_session_control_queues(AppQueues& queues){
    return order_data::OrderSessionControlQueues{
        .control_to_order_session_queue = queues.control_to_private_order_feed,
        .order_session_to_control_queue = queues.private_order_feed_to_control_status,
    };
}

order_data::OrderSessionOmsQueues make_order_session_oms_queues(AppQueues& queues){
    return order_data::OrderSessionOmsQueues{
        .private_ws_to_oms_queue = queues.private_order_feed_to_oms,
    };
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

std::jthread start_router_thread(
    router::Router& router_instance,
    MarketDataPathMessageQueue& input_queue,
    utils::IdleBackoffConfig polling_config) {
    return std::jthread([&router_instance, &input_queue, polling_config](const std::stop_token& stop_token) {
        utils::IdleBackoff idle_backoff{polling_config};
        MarketDataPathMessage message{};
        while (!stop_token.stop_requested()) {
            const auto pending_result = router_instance.flush_pending_barrier();
            if (pending_result == router::RouterRouteResult::kFAULTED) {
                break;
            }
            if (pending_result == router::RouterRouteResult::kBLOCKED) {
                idle_backoff.idle();
                continue;
            }
            if (input_queue.try_pop(message)) {
                idle_backoff.reset();
                const auto route_result = router_instance.route_message(message);
                if (route_result == router::RouterRouteResult::kFAULTED) {
                    break;
                }
            } else {
                idle_backoff.idle();
            }
        }
    });
}

std::jthread start_shard_thread(
    shard::Shard& shard_instance,
    utils::IdleBackoffConfig polling_config) {
    return std::jthread([&shard_instance, polling_config](const std::stop_token& stop_token) {
        utils::IdleBackoff idle_backoff{polling_config};
        while (!stop_token.stop_requested()) {
            const auto commands_processed =
                shard_instance.drain_control_commands(64); //NOLINT: arbitrary max commands to process per iteration
            shard_instance.maybe_send_telemetry();
            const auto result = shard_instance.pump_once();
            switch(result.code){
                case shard::ShardPumpCode::kIDLE:
                case shard::ShardPumpCode::kAPPLIED:
                case shard::ShardPumpCode::kEVENT_IGNORED:
                case shard::ShardPumpCode::kMARKET_BARRIER_HANDLED:
                case shard::ShardPumpCode::kSUBSCRIPTION_BARRIER_HANDLED:
                case shard::ShardPumpCode::kDRAINED_FRAME:
                case shard::ShardPumpCode::kDRAIN_COMPLETE:
                    break;

                case shard::ShardPumpCode::kPARSE_REJECTED:
                    std::cerr << "Shard " << shard_instance.shard_index()
                            << " parse failure: "
                            << static_cast<std::uint32_t>(result.parse_result.reason)
                            << '\n';
                    break;

                case shard::ShardPumpCode::kEVENT_REJECTED:
                case shard::ShardPumpCode::kEVENT_DESYNCED:
                    std::cerr << "Shard " << shard_instance.shard_index()
                            << " event apply failure: "
                            << static_cast<std::uint32_t>(result.event_result.reason)
                            << '\n';
                    break;
                case shard::ShardPumpCode::kINTEGRITY_BARRIER_REJECTED:
                    std::cerr << "Shard " << shard_instance.shard_index()
                            << " integrity barrier apply failure: "
                            << static_cast<std::uint32_t>(result.event_result.reason)
                            << '\n';
                    break;
                case shard::ShardPumpCode::kFRAME_ROUTE_REJECTED:
                    std::cerr << "Shard " << shard_instance.shard_index()
                            << " frame routing identity rejected\n";
                    break;
                case shard::ShardPumpCode::kMISSING_FRAME:
                    std::cerr << "Shard " << shard_instance.shard_index()
                            << " missing frame: "
                            << static_cast<std::uint32_t>(result.event_result.reason)
                            << '\n';
                    break;
                case shard::ShardPumpCode::kHANDLE_LEAK:
                    std::cerr << "Shard " << shard_instance.shard_index()
                            << " handle leak: "
                            << static_cast<std::uint32_t>(result.event_result.reason)
                            << '\n';
                    break;
                default:
                    std::cerr << "Shard " << shard_instance.shard_index()
                            << " pump result: "
                            << static_cast<std::uint32_t>(result.code)
                            << '\n';
                    break;
            }

            if (result.code == shard::ShardPumpCode::kIDLE &&
                commands_processed == 0) {
                idle_backoff.idle();
            }else{
                idle_backoff.reset();
            }
        }
    });
}

std::jthread start_wire_session_thread(market_data::KalshiWireSession& wire_session) {
    return std::jthread([&wire_session](const std::stop_token& stop_token) {
        wire_session.run(stop_token);
    });
}

std::jthread start_private_order_feed_thread(order_data::KalshiOrderSession& order_session) {
    return std::jthread([&order_session](const std::stop_token& stop_token) {
        order_session.run(stop_token);
    });
}

std::jthread start_order_rest_thread(kalshi_exchange::OrderRestSession& order_rest_session) {
    return std::jthread([&order_rest_session](const std::stop_token& stop_token) {
        order_rest_session.run(stop_token);
    });
}

std::jthread start_oms_thread(
    oms::Oms& oms_instance,
    utils::IdleBackoffConfig polling_config) {
    return std::jthread([&oms_instance, polling_config](const std::stop_token& stop_token) {
        utils::IdleBackoff idle_backoff{polling_config};
        while (!stop_token.stop_requested()) {
            if (oms_instance.pump_once() == oms::OmsPumpResult::kNoWork) {
                idle_backoff.idle();
            }else{
                idle_backoff.reset();
            }
        }
    });
}

std::jthread start_market_data_logger_thread(
    logging::MarketDataLogger& market_data_logger,
    utils::IdleBackoffConfig polling_config) {
    return std::jthread([&market_data_logger, polling_config](const std::stop_token& stop_token) {
        utils::IdleBackoff idle_backoff{polling_config};
        while (!stop_token.stop_requested()) {
            if (market_data_logger.pump(kLoggerPumpBatchSize) == 0) {
                idle_backoff.idle();
            }else{
                idle_backoff.reset();
            }
        }

        while (market_data_logger.pump(kLoggerPumpBatchSize) > 0) {}
    });
}

bool pump_control_plane_once(control::ControlPlane& control_plane) {
    const bool phase_changed = control_plane.update_trading_session_phase();
    const auto operator_result = control_plane.process_operator_commands();
    const auto io_result = control_plane.process_io_status();
    const bool processed_router_messages = control_plane.process_router_messages();
    const bool processed_shard_messages = control_plane.process_shard_messages();
    const auto recovery_result = control_plane.process_recovery(std::chrono::steady_clock::now());
    const bool processed_logger_messages = control_plane.process_logger_messages();
    const bool processed_oms_messages = control_plane.process_oms_status();
    const bool processed_order_rest_messages = control_plane.process_order_rest_status();
    const bool processed_private_order_feed_messages = control_plane.process_private_order_feed_status();

    return phase_changed ||
           operator_result.commands_processed > 0 ||
           io_result.statuses_processed > 0 ||
           processed_router_messages ||
           processed_shard_messages ||
           recovery_result.commands_pushed_success > 0 ||
           processed_logger_messages ||
           processed_oms_messages ||
           processed_order_rest_messages ||
           processed_private_order_feed_messages;
}

}  // namespace
//NOLINTNEXTLINE
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

    std::optional<CurlGlobalGuard> curl_global;
    if(app_config.kalshi.order_rest.enable_order_rest){
        try {
            curl_global.emplace();
        } catch (const std::exception& e) {
            std::cerr << "predex curl init error: " << e.what() << '\n';
            return 1;
        }
    }

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
    auto control_oms_queues = make_control_oms_queues(app_queues);
    auto control_private_order_feed_queues = make_control_private_order_feed_queues(app_queues);
    auto control_order_rest_queues = make_control_order_rest_queues(app_queues);
    auto handler_queues = make_operator_handler_queues(app_queues);
    auto router_queues = make_router_queues(app_queues);
    auto oms_queues = make_oms_queues(app_queues);

    control::ControlPlane control_plane{
        operator_queues,
        io_queues,
        router_queue,
        shard_queues,
        logger_queue,
        control_oms_queues,
        control_private_order_feed_queues,
        control_order_rest_queues,
        make_required_components(app_config),
        make_synthetic_session_config(app_config.runtime),
    };
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
        shard_threads.push_back(start_shard_thread(
            *shards[shard_index],
            app_config.runtime.thread_polling));
    }

    ServerThreadState server_state;
    std::jthread server_thread = start_server_thread(server, server_state);
    std::jthread router_thread = start_router_thread(
        router_instance,
        app_queues.wire_to_router,
        app_config.runtime.thread_polling);
    logging::MarketDataLogger market_data_logger{
        logging::MarketDataLoggerDeps{
            .input_queues = make_logger_input_queues(app_queues),
            .frame_pool = frame_pool,
            .recycle_queue = app_queues.logger_recycle,
            .logger_to_control_status_queue = &app_queues.logger_to_control_status,
            .output_file_path = app_config.runtime.market_data_tape_path,
        }
    };
    std::jthread market_data_logger_thread = start_market_data_logger_thread(
        market_data_logger,
        app_config.runtime.thread_polling);

    const bool order_graph_enabled =
        app_config.kalshi.order_rest.enable_order_rest ||
        app_config.kalshi.private_order_feed.enable_private_order_feed;

    std::optional<oms::Oms> oms_instance;
    std::optional<std::jthread> oms_thread;
    if(order_graph_enabled){
        oms_instance.emplace(oms_queues);
        oms_thread.emplace(start_oms_thread(
            *oms_instance,
            app_config.runtime.thread_polling));
        if(!control_plane.send_active_order_universe_to_oms()){
            std::cerr << "predex OMS init error: failed to enqueue active order universe to OMS\n";
            return 1;
        }
    }

    std::optional<kalshi_exchange::OrderRestSession> order_rest_session;
    std::optional<std::jthread> order_rest_thread;
    if(app_config.kalshi.order_rest.enable_order_rest){
        try{
            order_rest_session.emplace(kalshi_exchange::OrderRestSessionDeps{
                .http_session = make_http_session(app_config.kalshi.auth, app_config.kalshi.order_rest),
                .order_rest_adapter = kalshi_exchange::KalshiOrderRestAdapter{},
                .control_queues = make_order_rest_control_queues(app_queues),
                .oms_queues = make_order_rest_oms_queues(app_queues),
            });
            order_rest_thread.emplace(start_order_rest_thread(*order_rest_session));

            if(!control_plane.send_active_order_universe_to_order_rest()){
                throw std::runtime_error("Failed to enqueue active order universe to order REST");
            }
            if(!control_plane.push_order_rest_command(control::ControlToOrderRestCommand{control::EnableOrderRest{}})){
                throw std::runtime_error("Failed to enqueue order REST enable command");
            }
        } catch(const std::exception& e){
            std::cerr << "predex order REST init error: " << e.what() << '\n';
            return 1;
        }
    }

    std::optional<order_data::KalshiOrderSession> private_order_feed_session;
    std::optional<std::jthread> private_order_feed_thread;
    if(app_config.kalshi.private_order_feed.enable_private_order_feed){
        try{
            private_order_feed_session.emplace(order_data::OrderSessionDeps{
                .order_data_handler = make_order_data_handler(app_config.kalshi.auth),
                .desired_channels = app_config.kalshi.private_order_feed.channels,
                .control_queues = make_order_session_control_queues(app_queues),
                .oms_queues = make_order_session_oms_queues(app_queues),
            });
            private_order_feed_thread.emplace(start_private_order_feed_thread(*private_order_feed_session));

            if(!control_plane.send_active_order_universe_to_private_order_feed()){
                throw std::runtime_error("Failed to enqueue active order universe to private order feed");
            }
            if(!control_plane.push_private_order_feed_command(control::ControlToPrivateOrderFeedCommand{control::ConnectPrivateOrderFeed{}})){
                throw std::runtime_error("Failed to enqueue private order feed connect command");
            }
        } catch(const std::exception& e){
            std::cerr << "predex private order feed init error: " << e.what() << '\n';
            return 1;
        }
    }

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
    if (private_order_feed_thread.has_value()) {
        private_order_feed_thread->request_stop();
    }
    if (order_rest_thread.has_value()) {
        order_rest_thread->request_stop();
    }
    if (oms_thread.has_value()) {
        oms_thread->request_stop();
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
    if (private_order_feed_thread.has_value()) {
        private_order_feed_thread->join();
    }
    if (order_rest_thread.has_value()) {
        order_rest_thread->join();
    }
    if (oms_thread.has_value()) {
        oms_thread->join();
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
