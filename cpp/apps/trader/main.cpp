#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>

#include "trading/adapters/exchanges/kalshi/auth_signer.hpp"
#include "trading/adapters/exchanges/kalshi/oms_adapter.hpp"
#include "trading/adapters/exchanges/kalshi/ws_adapter.hpp"
#include "trading/adapters/logging/logger.hpp"
#include "trading/adapters/ws/client.hpp"
#include "trading/adapters/ws/feed_runner.hpp"
#include "trading/adapters/ws/session.hpp"
#include "trading/config/trader_config.hpp"
#include "trading/engine/runtime.hpp"
#include "trading/oms/order_manager.hpp"
#include "trading/oms/paper_order_transport.hpp"
#include "trading/oms/position_ledger.hpp"
#include "trading/oms/ws_order_transport.hpp"
#include "trading/pipeline/live_pipeline.hpp"
#include "trading/strategy/dropping_order_intent_sink.hpp"
#include "trading/strategy/ledger_shard_risk_snapshot_provider.hpp"
#include "trading/strategy/market_filter_event_handler.hpp"
#include "trading/strategy/noop_strategy.hpp"
#include "trading/strategy/order_manager_intent_sink.hpp"
#include "trading/strategy/order_intent_sink.hpp"
#include "trading/strategy/strategy_event_handler.hpp"

namespace {

std::atomic<bool> g_shutdown_requested{false};
constexpr int kExitMissingCredentials = 3;
constexpr int kExitPipelineStartFailure = 4;
constexpr int kExitFeedRunnerStartFailure = 5;
constexpr int kExitConfigLoadFailure = 6;
constexpr int kExitOmsStartFailure = 7;

std::optional<std::string> get_env(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return std::nullopt;
    }
    return std::string(value);
}

void handle_shutdown_signal(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        // Signal-only flag: no dependent shared state needs ordering.
        g_shutdown_requested.store(true, std::memory_order_relaxed);
    }
}

std::optional<std::string> resolve_config_path(int argc, char** argv) {
    if (argc >= 3 && std::string_view{argv[1]} == "--config") {
        if (argv[2] != nullptr && !std::string_view{argv[2]}.empty()) {
            return std::string{argv[2]};
        }
    } else if (argc >= 2) {
        if (argv[1] != nullptr && !std::string_view{argv[1]}.empty()) {
            return std::string{argv[1]};
        }
    }
    return get_env("TRADING_CONFIG_PATH");
}

} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
int main(int argc, char** argv) {
    trading::config::TraderRuntimeConfig runtime_config{};
    const auto config_path = resolve_config_path(argc, argv);
    if (config_path.has_value()) {
        const auto loaded = trading::config::load_trader_config_from_file(*config_path);
        if (!loaded.ok) {
            trading::adapters::logging::log_startup("trader.config.error", loaded.error);
            return kExitConfigLoadFailure;
        }
        runtime_config = loaded.config;
        trading::adapters::logging::log_startup("trader.config",
                                                "loaded config file: " + *config_path);
    }

    const auto startup = trading::engine::build_trader_startup_payload(runtime_config.mode);
    const bool run_live_oms = runtime_config.execution_mode == trading::config::TraderExecutionMode::kLive;
    const bool run_paper_oms =
        runtime_config.execution_mode == trading::config::TraderExecutionMode::kPaper;
    const bool run_oms = run_live_oms || run_paper_oms;

    const auto key_id = runtime_config.kalshi.credentials.key_id.empty()
                            ? get_env(runtime_config.kalshi.credentials.key_id_env.c_str())
                            : std::optional<std::string>{runtime_config.kalshi.credentials.key_id};
    const auto private_key_pem =
        runtime_config.kalshi.credentials.private_key_pem.empty()
            ? get_env(runtime_config.kalshi.credentials.private_key_pem_env.c_str())
            : std::optional<std::string>{runtime_config.kalshi.credentials.private_key_pem};
    if (!key_id || !private_key_pem) {
        trading::adapters::logging::log_startup("trader",
                                                "missing Kalshi credentials (inline or env)");
        return kExitMissingCredentials;
    }

    const trading::adapters::exchanges::kalshi::Credentials kalshi_credentials{
        .key_id = *key_id,
        .private_key_pem = *private_key_pem,
    };
    trading::adapters::exchanges::kalshi::WsAdapter kalshi_adapter{
        trading::adapters::exchanges::kalshi::AuthSigner{kalshi_credentials},
        runtime_config.kalshi.endpoint,
    };

    trading::adapters::exchanges::kalshi::OmsAdapter oms_adapter;
    trading::oms::PositionLedger position_ledger;
    trading::strategy::LedgerShardRiskSnapshotProvider shard_risk_snapshot_provider{
        position_ledger};
    std::unique_ptr<trading::adapters::ws::BoostBeastWsTransport> oms_ws_transport;
    std::unique_ptr<trading::oms::IOrderTransport> oms_transport;
    std::unique_ptr<trading::oms::OrderManager> order_manager;
    std::unique_ptr<trading::strategy::OrderManagerIntentSink> order_manager_intent_sink;
    std::unique_ptr<trading::strategy::DroppingOrderIntentSink> dropping_intent_sink;
    trading::strategy::IOrderIntentSink* strategy_intent_sink{nullptr};

    if (run_oms) {
        std::map<std::string, std::string> oms_headers;
        std::string oms_endpoint = "paper://oms";
        if (run_live_oms) {
            const auto oms_auth_headers =
                trading::adapters::exchanges::kalshi::AuthSigner{kalshi_credentials}.make_ws_headers();
            oms_headers = {
                {"KALSHI-ACCESS-KEY", oms_auth_headers.key_id},
                {"KALSHI-ACCESS-TIMESTAMP", oms_auth_headers.timestamp_ms},
                {"KALSHI-ACCESS-SIGNATURE", oms_auth_headers.signature_base64},
            };
            oms_endpoint = runtime_config.kalshi.endpoint;
            oms_ws_transport = std::make_unique<trading::adapters::ws::BoostBeastWsTransport>();
            oms_transport = std::make_unique<trading::oms::WsOrderTransport>(*oms_ws_transport);
        } else {
            oms_transport = std::make_unique<trading::oms::PaperOrderTransport>();
        }

        order_manager = std::make_unique<trading::oms::OrderManager>(
            oms_adapter, *oms_transport, position_ledger,
            trading::oms::OrderManagerConfig{
                .transport =
                    trading::oms::OrderTransportConfig{
                        .endpoint = std::move(oms_endpoint),
                        .headers = std::move(oms_headers),
                    },
                .global_risk = runtime_config.risk.oms_global,
                .portfolio_risk = runtime_config.risk.oms_portfolio,
                .portfolio_snapshot_provider = &position_ledger,
                .loop_idle_sleep = std::chrono::milliseconds{1},
            });
        if (!order_manager->start()) {
            trading::adapters::logging::log_startup(
                "trader.error", "failed to start order manager: " + order_manager->last_error());
            return kExitOmsStartFailure;
        }
        order_manager_intent_sink =
            std::make_unique<trading::strategy::OrderManagerIntentSink>(*order_manager);
        strategy_intent_sink = order_manager_intent_sink.get();
    } else {
        dropping_intent_sink = std::make_unique<trading::strategy::DroppingOrderIntentSink>();
        strategy_intent_sink = dropping_intent_sink.get();
    }

    const std::size_t strategy_shard_count = std::max<std::size_t>(runtime_config.pipeline.shard_count, 1U);
    const std::unordered_set<std::string> allowed_strategy_markets{
        runtime_config.market_universe.tickers.begin(),
        runtime_config.market_universe.tickers.end(),
    };
    std::vector<std::unique_ptr<trading::strategy::NoopStrategy>> shard_strategies;
    shard_strategies.reserve(strategy_shard_count);
    for (std::size_t shard_id = 0; shard_id < strategy_shard_count; ++shard_id) {
        (void)shard_id;
        shard_strategies.push_back(std::make_unique<trading::strategy::NoopStrategy>());
    }
    runtime_config.pipeline.shard_event_handler_factory =
        [strategy_intent_sink, &shard_strategies, &runtime_config, &shard_risk_snapshot_provider,
         allowed_strategy_markets](std::size_t shard_id)
        -> std::unique_ptr<trading::shards::IShardEventHandler> {
        if (strategy_intent_sink == nullptr) {
            return nullptr;
        }
        if (shard_id >= shard_strategies.size()) {
            return nullptr;
        }
        if (shard_strategies[shard_id] == nullptr) {
            return nullptr;
        }
        auto strategy_handler = std::make_unique<trading::strategy::StrategyEventHandler>(
            *shard_strategies[shard_id], *strategy_intent_sink,
            trading::strategy::StrategyRunnerConfig{
                .shard_risk = runtime_config.risk.shard,
                .risk_snapshot_provider = &shard_risk_snapshot_provider,
            });
        if (allowed_strategy_markets.empty()) {
            return strategy_handler;
        }
        return std::make_unique<trading::strategy::MarketFilterEventHandler>(
            allowed_strategy_markets, std::move(strategy_handler));
    };

    trading::pipeline::LivePipeline pipeline{runtime_config.pipeline};
    if (!pipeline.start()) {
        if (order_manager != nullptr) {
            order_manager->stop();
        }
        trading::adapters::logging::log_startup("trader.error", "failed to start pipeline");
        return kExitPipelineStartFailure;
    }

    trading::adapters::ws::BoostBeastWsTransport ws_transport;
    trading::adapters::ws::WsSession session{ws_transport, kalshi_adapter};

    trading::adapters::ws::WsFeedRunner feed_runner{
        session,
        pipeline.message_sink(),
        trading::adapters::ws::WsFeedRunnerConfig{
            .channels = runtime_config.kalshi.channels,
            .market_tickers = runtime_config.market_universe.tickers,
        },
    };
    if (!feed_runner.start()) {
        pipeline.stop();
        if (order_manager != nullptr) {
            order_manager->stop();
        }
        trading::adapters::logging::log_startup("trader.error", "failed to start ws feed runner");
        return kExitFeedRunnerStartFailure;
    }

    trading::adapters::logging::log_startup("trader", startup);
    trading::adapters::logging::log_startup(
        "trader.execution_mode",
        std::string{trading::config::execution_mode_name(runtime_config.execution_mode)});
    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);

    // Poll stop flag only; relaxed is sufficient for this shutdown check.
    while (!g_shutdown_requested.load(std::memory_order_relaxed)) {
        const std::size_t pumped = pipeline.pump_ingest(runtime_config.pump_batch_size);
        if (pumped == 0) {
            std::this_thread::sleep_for(runtime_config.pump_idle_sleep);
        }
    }

    feed_runner.stop();
    pipeline.stop();
    if (order_manager != nullptr) {
        order_manager->stop();
    }

    const auto pipeline_stats = pipeline.stats();
    const auto feed_received = feed_runner.received_count();
    const auto feed_dropped = feed_runner.dropped_count();
    trading::oms::OrderManagerStats oms_stats{};
    if (order_manager != nullptr) {
        oms_stats = order_manager->stats();
    }
    const std::string summary =
        "frames_pumped=" + std::to_string(pipeline_stats.ingest_frames_pumped) +
        ", routed=" + std::to_string(pipeline_stats.route_success) +
        ", route_drop=" + std::to_string(pipeline_stats.route_drop) +
        ", sink_drop=" + std::to_string(pipeline_stats.ingest_sink_dropped) +
        ", shard_drop=" + std::to_string(pipeline_stats.shard_dispatch_dropped) +
        ", parsed=" + std::to_string(pipeline_stats.shard_parsed) +
        ", parser_rejects=" + std::to_string(pipeline_stats.shard_parser_rejects) +
        ", book_apply_rejects=" + std::to_string(pipeline_stats.shard_apply_rejects) +
        ", parse_errors_total=" + std::to_string(pipeline_stats.shard_parse_errors) +
        ", parse_invalid_json=" + std::to_string(pipeline_stats.parse_error_invalid_json) +
        ", parse_missing_field=" + std::to_string(pipeline_stats.parse_error_missing_field) +
        ", parse_invalid_field=" + std::to_string(pipeline_stats.parse_error_invalid_field) +
        ", parse_unsupported_type=" + std::to_string(pipeline_stats.parse_error_unsupported_type) +
        ", parsed_snapshot=" + std::to_string(pipeline_stats.parsed_snapshots) +
        ", parsed_delta=" + std::to_string(pipeline_stats.parsed_deltas) +
        ", parsed_trade=" + std::to_string(pipeline_stats.parsed_trades) +
        ", parsed_other=" + std::to_string(pipeline_stats.parsed_other) +
        ", oms_submitted=" + std::to_string(oms_stats.submitted_count) +
        ", oms_sent=" + std::to_string(oms_stats.sent_count) +
        ", oms_rejects=" +
        std::to_string(oms_stats.risk_reject_count + oms_stats.portfolio_risk_reject_count) +
        ", ws_received=" + std::to_string(feed_received) +
        ", ws_dropped=" + std::to_string(feed_dropped);
    trading::adapters::logging::log_startup("trader.pipeline", summary);
    return 0;
}
