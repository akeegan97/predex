#include "predex/app.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

namespace {

std::atomic<bool> g_shutdown_requested{false};

constexpr int kExitSuccess = 0;
constexpr int kExitArgsFailure = 2;
constexpr int kExitConfigFailure = 3;
constexpr int kExitStartupFailure = 4;
constexpr int kExitRuntimeFailure = 5;
constexpr std::int64_t kDefaultSleepMs = 100;

void handle_shutdown_signal(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        g_shutdown_requested.store(true, std::memory_order_relaxed);
    }
}

std::optional<std::string> get_env(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return std::nullopt;
    }
    return std::string(value);
}

std::optional<std::string> resolve_config_path(int argc, char** argv) {
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            continue;
        }

        const std::string_view arg{argv[index]};
        if (arg == "--config") {
            if (index + 1 >= argc || argv[index + 1] == nullptr ||
                std::string_view{argv[index + 1]}.empty()) {
                return std::nullopt;
            }
            return std::string{argv[index + 1]};
        }

        if (!arg.starts_with("--")) {
            return std::string{arg};
        }
    }

    return get_env("TRADING_CONFIG_PATH");
}

std::vector<std::string> read_string_array(const nlohmann::json& parent, std::string_view key) {
    std::vector<std::string> values;
    const auto iterator = parent.find(std::string(key));
    if (iterator == parent.end() || !iterator->is_array()) {
        return values;
    }

    values.reserve(iterator->size());
    for (const auto& value : *iterator) {
        if (value.is_string()) {
            values.push_back(value.get<std::string>());
        }
    }
    return values;
}

std::size_t read_size(const nlohmann::json& parent,
                      std::string_view key,
                      std::size_t fallback) {
    const auto iterator = parent.find(std::string(key));
    if (iterator == parent.end() || !iterator->is_number_unsigned()) {
        return fallback;
    }
    return iterator->get<std::size_t>();
}

std::int64_t read_int64(const nlohmann::json& parent,
                        std::string_view key,
                        std::int64_t fallback) {
    const auto iterator = parent.find(std::string(key));
    if (iterator == parent.end() || !iterator->is_number_integer()) {
        return fallback;
    }
    return iterator->get<std::int64_t>();
}

bool read_bool(const nlohmann::json& parent, std::string_view key, bool fallback) {
    const auto iterator = parent.find(std::string(key));
    if (iterator == parent.end() || !iterator->is_boolean()) {
        return fallback;
    }
    return iterator->get<bool>();
}

std::string read_string(const nlohmann::json& parent,
                        std::string_view key,
                        std::string fallback = {}) {
    const auto iterator = parent.find(std::string(key));
    if (iterator == parent.end() || !iterator->is_string()) {
        return fallback;
    }
    return iterator->get<std::string>();
}

std::optional<predex::internal::EventTopologyKind> parse_topology_kind(
    std::string_view value) {
    if (value == "monotonic_chain" || value == "monotonic-chain") {
        return predex::internal::EventTopologyKind::kMonotonicChain;
    }
    if (value == "mutually_exclusive" || value == "mutually-exclusive") {
        return predex::internal::EventTopologyKind::kMutuallyExclusive;
    }
    if (value == "unordered_group" || value == "unordered-group") {
        return predex::internal::EventTopologyKind::kUnorderedGroup;
    }
    if (value == "single_market" || value == "single-market") {
        return predex::internal::EventTopologyKind::kSingleMarket;
    }
    return std::nullopt;
}

std::optional<std::vector<predex::MarketRouteConfig>> build_market_routes(
    const nlohmann::json& root,
    std::string& error_out) {
    std::vector<predex::MarketRouteConfig> routes;

    const auto explicit_routes_it = root.find("market_routes");
    if (explicit_routes_it == root.end() || !explicit_routes_it->is_array()) {
        error_out = "market_routes must be explicitly configured for event-centric routing";
        return std::nullopt;
    }

    routes.reserve(explicit_routes_it->size());
    for (const auto& route_json : *explicit_routes_it) {
        if (!route_json.is_object()) {
            continue;
        }

        predex::MarketRouteConfig route{};
        route.market_ticker = read_string(route_json, "market_ticker");
        route.market_id = read_size(route_json, "market_id", 0);
        route.event_id = read_size(route_json, "event_id", 0);
        route.affinity_key = read_size(route_json, "affinity_key", 0);
        route.strike_key = read_int64(route_json, "strike_key", 0);
        route.close_time_s = read_size(route_json, "close_time_s", 0);
        route.tradeable = read_bool(route_json, "tradeable", false);

        const auto topology_value = read_string(route_json, "topology_kind");
        if (!topology_value.empty()) {
            const auto topology_kind = parse_topology_kind(topology_value);
            if (!topology_kind.has_value()) {
                error_out = "market_routes contains an invalid topology_kind";
                return std::nullopt;
            }
            route.topology_kind = *topology_kind;
        }

        if (route.market_ticker.empty() || route.market_id == 0 || route.event_id == 0 ||
            route.topology_kind == predex::internal::EventTopologyKind::kUnknown) {
            error_out =
                "each market_routes entry must define market_ticker, market_id, event_id, and topology_kind";
            return std::nullopt;
        }

        routes.push_back(std::move(route));
    }
    return routes;
}
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::optional<predex::AppConfig> build_app_config(const nlohmann::json& root,
                                                  std::string& error_out) {
    predex::AppConfig config{};

    const auto kalshi_it = root.find("kalshi");
    if (kalshi_it == root.end() || !kalshi_it->is_object()) {
        error_out = "Missing required object: kalshi";
        return std::nullopt;
    }
    const auto& kalshi = *kalshi_it;

    config.kalshi_ws.endpoint = read_string(
        kalshi, "endpoint", "wss://api.elections.kalshi.com/trade-api/ws/v2");
    config.kalshi_ws.channels = read_string_array(kalshi, "channels");
    config.kalshi_ws.lifecycle_channels = read_string_array(kalshi, "lifecycle_channels");

    config.kalshi_ws.market_tickers = read_string_array(kalshi, "market_tickers");
    if (config.kalshi_ws.market_tickers.empty()) {
        const auto universe_it = root.find("market_universe");
        if (universe_it != root.end() && universe_it->is_object()) {
            config.kalshi_ws.market_tickers = read_string_array(*universe_it, "tickers");
        }
    }

    const auto credentials_it = kalshi.find("credentials");
    if (credentials_it != kalshi.end() && credentials_it->is_object()) {
        const auto& credentials = *credentials_it;

        config.kalshi_ws.key_id = read_string(credentials, "key_id");
        if (config.kalshi_ws.key_id.empty()) {
            const auto env_name = read_string(credentials, "key_id_env");
            if (!env_name.empty()) {
                config.kalshi_ws.key_id = get_env(env_name.c_str()).value_or("");
            }
        }

        config.kalshi_ws.private_key_pem = read_string(credentials, "private_key_pem");
        if (config.kalshi_ws.private_key_pem.empty()) {
            const auto env_name = read_string(credentials, "private_key_pem_env");
            if (!env_name.empty()) {
                config.kalshi_ws.private_key_pem = get_env(env_name.c_str()).value_or("");
            }
        }
    }

    if (config.kalshi_ws.key_id.empty()) {
        config.kalshi_ws.key_id = get_env("KALSHI_KEY_ID").value_or("");
    }
    if (config.kalshi_ws.private_key_pem.empty()) {
        config.kalshi_ws.private_key_pem = get_env("KALSHI_PRIVATE_KEY_PEM").value_or("");
    }

    const auto pipeline_it = root.find("pipeline");
    if (pipeline_it != root.end() && pipeline_it->is_object()) {
        const auto& pipeline = *pipeline_it;
        config.pipeline.frame_pool_capacity =
            read_size(pipeline, "frame_pool_capacity", config.pipeline.frame_pool_capacity);
        config.pipeline.shard_count =
            read_size(pipeline, "shard_count", config.pipeline.shard_count);

        const std::size_t frame_queue_capacity =
            read_size(pipeline, "frame_queue_capacity", predex::kDefaultCapacity);
        const std::size_t per_shard_queue_capacity =
            read_size(pipeline, "per_shard_queue_capacity", predex::kDefaultCapacity);

        config.pipeline.io_to_router_capacity =
            read_size(pipeline, "io_to_router_capacity", frame_queue_capacity);
        config.pipeline.router_to_logger_capacity =
            read_size(pipeline, "router_to_logger_capacity", frame_queue_capacity);
        config.pipeline.shard_input_capacity =
            read_size(pipeline, "shard_input_capacity", per_shard_queue_capacity);
        config.pipeline.shard_to_logger_capacity =
            read_size(pipeline, "shard_to_logger_capacity", per_shard_queue_capacity);
    }

    const auto tape_it = root.find("tape");
    if (tape_it != root.end() && tape_it->is_object()) {
        config.tape.output_path = read_string(*tape_it, "output_path", config.tape.output_path);
    }

    const auto oms_transport_it = root.find("oms_transport");
    if (oms_transport_it != root.end() && oms_transport_it->is_object()) {
        const auto& oms_transport = *oms_transport_it;
        config.oms_transport.enabled =
            read_bool(oms_transport, "enabled", config.oms_transport.enabled);
        config.oms_transport.rest_endpoint = read_string(
            oms_transport, "rest_endpoint", config.oms_transport.rest_endpoint);
        config.oms_transport.private_ws_endpoint = read_string(
            oms_transport, "private_ws_endpoint", config.oms_transport.private_ws_endpoint);
        config.oms_transport.private_ws_channels =
            read_string_array(oms_transport, "private_ws_channels");
        if (config.oms_transport.enabled &&
            config.oms_transport.private_ws_channels.empty()) {
            config.oms_transport.private_ws_channels = {"user_orders"};
        }
        config.oms_transport.max_session_loss_ticks =
            read_int64(oms_transport, "max_session_loss_ticks",
                       config.oms_transport.max_session_loss_ticks);
        config.oms_transport.available_capital_ticks =
            read_int64(oms_transport, "available_capital_ticks",
                       config.oms_transport.available_capital_ticks);
    }

    const auto local_risk_it = root.find("local_risk");
    if (local_risk_it != root.end() && local_risk_it->is_object()) {
        const auto& local_risk = *local_risk_it;
        config.local_risk.max_net_position_lots_per_market = read_int64(
            local_risk, "max_net_position_lots_per_market",
            config.local_risk.max_net_position_lots_per_market);
        config.local_risk.min_seconds_to_close = read_size(
            local_risk, "min_seconds_to_close",
            config.local_risk.min_seconds_to_close);
        config.local_risk.trading_enabled =
            read_bool(local_risk, "trading_enabled", config.local_risk.trading_enabled);
    }

    if (config.kalshi_ws.channels.empty()) {
        error_out = "kalshi.channels must contain at least one subscription channel";
        return std::nullopt;
    }
    if (config.kalshi_ws.key_id.empty() || config.kalshi_ws.private_key_pem.empty()) {
        error_out = "Missing Kalshi credentials (inline or env-backed)";
        return std::nullopt;
    }
    if (config.pipeline.shard_count == 0) {
        error_out = "pipeline.shard_count must be greater than zero";
        return std::nullopt;
    }

    auto market_routes = build_market_routes(root, error_out);
    if (!market_routes.has_value()) {
        return std::nullopt;
    }
    config.market_routes = std::move(*market_routes);

    if (config.kalshi_ws.market_tickers.empty()) {
        config.kalshi_ws.market_tickers.reserve(config.market_routes.size());
        for (const auto& route : config.market_routes) {
            config.kalshi_ws.market_tickers.push_back(route.market_ticker);
        }
    }
    if (config.kalshi_ws.market_tickers.empty()) {
        error_out = "No market tickers configured under kalshi.market_tickers, market_universe.tickers, or market_routes";
        return std::nullopt;
    }

    return config;
}

} // namespace
//NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
    const auto config_path = resolve_config_path(argc, argv);
    if (!config_path.has_value()) {
        std::cerr << "Missing trader config path. Use --config <path> or set TRADING_CONFIG_PATH.\n";
        return kExitArgsFailure;
    }

    std::ifstream config_stream(*config_path);
    if (!config_stream) {
        std::cerr << "Failed to open trader config: " << *config_path << '\n';
        return kExitConfigFailure;
    }

    nlohmann::json root;
    try {
        config_stream >> root;
    } catch (const std::exception& exception) {
        std::cerr << "Failed to parse trader config: " << exception.what() << '\n';
        return kExitConfigFailure;
    }

    std::string config_error;
    auto app_config = build_app_config(root, config_error);
    if (!app_config.has_value()) {
        std::cerr << "Invalid trader config: " << config_error << '\n';
        return kExitConfigFailure;
    }

    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);

    predex::App app{std::move(*app_config)};
    if (!app.start()) {
        std::cerr << "Failed to start trader app: " << app.last_error() << '\n';
        return kExitStartupFailure;
    }

    std::atomic<bool> app_finished{false};
    std::jthread app_thread([&app, &app_finished](const std::stop_token&) {
        app.run();
        app_finished.store(true, std::memory_order_release);
    });

    while (!g_shutdown_requested.load(std::memory_order_relaxed) &&
           !app_finished.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kDefaultSleepMs));
    }

    if (g_shutdown_requested.load(std::memory_order_relaxed)) {
        app.stop();
    }

    if (app_thread.joinable()) {
        app_thread.join();
    }

    const auto final_error = app.last_error();
    if (!final_error.empty() && !g_shutdown_requested.load(std::memory_order_relaxed)) {
        std::cerr << "Trader app exited with error: " << final_error << '\n';
        return kExitRuntimeFailure;
    }

    return kExitSuccess;
}
