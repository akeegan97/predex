#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

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
#include "trading/strategy/paper_trade_probe_strategy.hpp"
#include "trading/strategy/strategy.hpp"
#include "trading/strategy/strategy_event_handler.hpp"

namespace {

std::atomic<bool> g_shutdown_requested{false};
constexpr int kExitMissingCredentials = 3;
constexpr int kExitPipelineStartFailure = 4;
constexpr int kExitFeedRunnerStartFailure = 5;
constexpr int kExitConfigLoadFailure = 6;
constexpr int kExitOmsStartFailure = 7;
constexpr int kExitArgsFailure = 8;
constexpr int kExitTapeOpenFailure = 9;

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

struct FlagValue {
    bool found{false};
    std::optional<std::string> value;
};

FlagValue find_flag_value(int argc, char** argv, std::string_view flag) {
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            continue;
        }
        if (std::string_view{argv[index]} != flag) {
            continue;
        }
        if (index + 1 < argc && argv[index + 1] != nullptr &&
            !std::string_view{argv[index + 1]}.empty()) {
            return FlagValue{
                .found = true,
                .value = std::string{argv[index + 1]},
            };
        }
        return FlagValue{
            .found = true,
            .value = std::nullopt,
        };
    }
    return FlagValue{};
}

std::optional<std::string> resolve_positional_config_path(int argc, char** argv) {
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr || std::string_view{argv[index]}.empty()) {
            continue;
        }
        const std::string_view arg{argv[index]};
        if (arg == "--config" || arg == "--record-jsonl") {
            ++index;
            continue;
        }
        if (arg.starts_with("--")) {
            continue;
        }
        return std::string{arg};
    }
    return get_env("TRADING_CONFIG_PATH");
}

class RecordingWsMessageSink final : public trading::adapters::ws::IWsMessageSink {
  public:
    RecordingWsMessageSink(trading::adapters::ws::IWsMessageSink& downstream, std::string tape_path)
        : downstream_(downstream), tape_path_(std::move(tape_path)),
          tape_(tape_path_, std::ios::out | std::ios::trunc) {}

    [[nodiscard]] bool ready() const { return tape_.is_open(); }
    [[nodiscard]] bool write_failed() const { return write_failed_; }
    [[nodiscard]] const std::string& tape_path() const { return tape_path_; }

    bool push_message(std::string message) override {
        if (tape_.is_open()) {
            tape_ << message << '\n';
            if (!tape_) {
                write_failed_ = true;
            }
        } else {
            write_failed_ = true;
        }
        return downstream_.push_message(std::move(message));
    }

  private:
    trading::adapters::ws::IWsMessageSink& downstream_;
    std::string tape_path_;
    std::ofstream tape_;
    bool write_failed_{false};
};

void merge_latency_max(trading::metrics::LatencyPercentiles& aggregate,
                       const trading::metrics::LatencyPercentiles& sample) {
    aggregate.count += sample.count;
    aggregate.p50_ns = std::max(aggregate.p50_ns, sample.p50_ns);
    aggregate.p95_ns = std::max(aggregate.p95_ns, sample.p95_ns);
    aggregate.p99_ns = std::max(aggregate.p99_ns, sample.p99_ns);
    aggregate.max_ns = std::max(aggregate.max_ns, sample.max_ns);
}

trading::strategy::StrategyRunnerStats aggregate_strategy_stats(
    const std::vector<trading::strategy::StrategyEventHandler*>& strategy_handlers) {
    trading::strategy::StrategyRunnerStats strategy_stats{};
    for (const auto* strategy_handler : strategy_handlers) {
        if (strategy_handler == nullptr) {
            continue;
        }
        const auto handler_stats = strategy_handler->stats();
        strategy_stats.events_processed_count += handler_stats.events_processed_count;
        strategy_stats.intents_emitted_count += handler_stats.intents_emitted_count;
        strategy_stats.intents_submitted_count += handler_stats.intents_submitted_count;
        strategy_stats.risk_reject_count += handler_stats.risk_reject_count;
        strategy_stats.sink_reject_count += handler_stats.sink_reject_count;
        strategy_stats.strategy_error_count += handler_stats.strategy_error_count;
        merge_latency_max(strategy_stats.event_to_submit_latency,
                          handler_stats.event_to_submit_latency);
    }
    return strategy_stats;
}

std::string build_runtime_summary(const trading::pipeline::LivePipelineStats& pipeline_stats,
                                  const trading::strategy::StrategyRunnerStats& strategy_stats,
                                  const trading::oms::OrderManagerStats& oms_stats,
                                  std::size_t ws_received, std::size_t ws_dropped) {
    return "frames_pumped=" + std::to_string(pipeline_stats.ingest_frames_pumped) +
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
           ", ingest_q_depth=" + std::to_string(pipeline_stats.ingest_queue_depth) +
           ", ingest_q_hwm=" + std::to_string(pipeline_stats.ingest_queue_high_watermark) +
           ", shard_q_depth=" + std::to_string(pipeline_stats.shard_queue_depth) +
           ", shard_q_hwm_total=" +
           std::to_string(pipeline_stats.shard_queue_high_watermark_total) +
           ", shard_q_hwm_max=" + std::to_string(pipeline_stats.shard_queue_high_watermark_max) +
           ", parse_latency_count=" + std::to_string(pipeline_stats.recv_to_parse_latency.count) +
           ", parse_latency_p50_ns=" + std::to_string(pipeline_stats.recv_to_parse_latency.p50_ns) +
           ", parse_latency_p95_ns=" + std::to_string(pipeline_stats.recv_to_parse_latency.p95_ns) +
           ", parse_latency_p99_ns=" + std::to_string(pipeline_stats.recv_to_parse_latency.p99_ns) +
           ", parse_latency_max_ns=" + std::to_string(pipeline_stats.recv_to_parse_latency.max_ns) +
           ", strategy_events=" + std::to_string(strategy_stats.events_processed_count) +
           ", strategy_intents_emitted=" + std::to_string(strategy_stats.intents_emitted_count) +
           ", strategy_intents_submitted=" + std::to_string(strategy_stats.intents_submitted_count) +
           ", strategy_risk_rejects=" + std::to_string(strategy_stats.risk_reject_count) +
           ", strategy_sink_rejects=" + std::to_string(strategy_stats.sink_reject_count) +
           ", strategy_errors=" + std::to_string(strategy_stats.strategy_error_count) +
           ", strategy_latency_count=" + std::to_string(strategy_stats.event_to_submit_latency.count) +
           ", strategy_latency_p50_ns=" +
           std::to_string(strategy_stats.event_to_submit_latency.p50_ns) +
           ", strategy_latency_p95_ns=" +
           std::to_string(strategy_stats.event_to_submit_latency.p95_ns) +
           ", strategy_latency_p99_ns=" +
           std::to_string(strategy_stats.event_to_submit_latency.p99_ns) +
           ", strategy_latency_max_ns=" +
           std::to_string(strategy_stats.event_to_submit_latency.max_ns) +
           ", oms_submitted=" + std::to_string(oms_stats.submitted_count) +
           ", oms_sent=" + std::to_string(oms_stats.sent_count) +
           ", oms_rejects=" +
           std::to_string(oms_stats.risk_reject_count + oms_stats.portfolio_risk_reject_count) +
           ", oms_pending_q_depth=" + std::to_string(oms_stats.pending_intent_count) +
           ", oms_pending_q_hwm=" + std::to_string(oms_stats.pending_intent_high_watermark) +
           ", oms_latency_count=" + std::to_string(oms_stats.submit_to_send_latency.count) +
           ", oms_latency_p50_ns=" + std::to_string(oms_stats.submit_to_send_latency.p50_ns) +
           ", oms_latency_p95_ns=" + std::to_string(oms_stats.submit_to_send_latency.p95_ns) +
           ", oms_latency_p99_ns=" + std::to_string(oms_stats.submit_to_send_latency.p99_ns) +
           ", oms_latency_max_ns=" + std::to_string(oms_stats.submit_to_send_latency.max_ns) +
           ", ws_received=" + std::to_string(ws_received) +
           ", ws_dropped=" + std::to_string(ws_dropped);
}

} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
int main(int argc, char** argv) {
    trading::config::TraderRuntimeConfig runtime_config{};
    const auto config_arg = find_flag_value(argc, argv, "--config");
    if (config_arg.found && !config_arg.value.has_value()) {
        trading::adapters::logging::log_startup("trader.config.error", "missing value for --config");
        return kExitArgsFailure;
    }
    const auto record_jsonl_arg = find_flag_value(argc, argv, "--record-jsonl");
    if (record_jsonl_arg.found && !record_jsonl_arg.value.has_value()) {
        trading::adapters::logging::log_startup("trader.config.error",
                                                "missing value for --record-jsonl");
        return kExitArgsFailure;
    }
    const auto config_path =
        config_arg.value.has_value() ? config_arg.value : resolve_positional_config_path(argc, argv);
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
            oms_transport = std::make_unique<trading::oms::PaperOrderTransport>(
                trading::oms::PaperOrderTransportConfig{
                    .auto_fill_on_place = runtime_config.paper_oms.auto_fill_on_place,
                    .fill_parts = runtime_config.paper_oms.fill_parts,
                    .place_reject_bps = runtime_config.paper_oms.place_reject_bps,
                    .ack_delay = runtime_config.paper_oms.ack_delay,
                    .fill_delay = runtime_config.paper_oms.fill_delay,
                    .fill_interval = runtime_config.paper_oms.fill_interval,
                    .reject_delay = runtime_config.paper_oms.reject_delay,
                });
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
    std::vector<std::unique_ptr<trading::strategy::IStrategy>> shard_strategies;
    std::vector<trading::strategy::StrategyEventHandler*> strategy_handlers;
    shard_strategies.reserve(strategy_shard_count);
    strategy_handlers.resize(strategy_shard_count, nullptr);
    for (std::size_t shard_id = 0; shard_id < strategy_shard_count; ++shard_id) {
        if (runtime_config.strategy.mode == trading::config::TraderStrategyMode::kPaperTradeProbe) {
            shard_strategies.push_back(std::make_unique<trading::strategy::PaperTradeProbeStrategy>(
                trading::strategy::PaperTradeProbeStrategyConfig{
                    .client_order_id_prefix = runtime_config.strategy.client_order_id_prefix,
                    .side = runtime_config.strategy.side,
                    .qty_lots = runtime_config.strategy.qty_lots,
                    .time_in_force = trading::internal::OmsTimeInForce::kGtc,
                    .max_orders = runtime_config.strategy.max_orders_per_shard,
                    .limit_price_ticks_override = runtime_config.strategy.limit_price_ticks_override,
                },
                shard_id));
        } else {
            shard_strategies.push_back(std::make_unique<trading::strategy::NoopStrategy>());
        }
    }
    runtime_config.pipeline.shard_event_handler_factory =
        [strategy_intent_sink, &shard_strategies, &runtime_config, &shard_risk_snapshot_provider,
         &strategy_handlers, allowed_strategy_markets](std::size_t shard_id)
        -> std::unique_ptr<trading::shards::IShardEventHandler> {
        if (strategy_intent_sink == nullptr) {
            return nullptr;
        }
        if (shard_id >= shard_strategies.size()) {
            return nullptr;
        }
        if (shard_id >= strategy_handlers.size()) {
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
        strategy_handlers[shard_id] = strategy_handler.get();
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
    trading::adapters::ws::IWsMessageSink* feed_sink = &pipeline.message_sink();
    std::unique_ptr<RecordingWsMessageSink> recording_sink;
    if (record_jsonl_arg.value.has_value()) {
        recording_sink = std::make_unique<RecordingWsMessageSink>(pipeline.message_sink(),
                                                                  *record_jsonl_arg.value);
        if (!recording_sink->ready()) {
            pipeline.stop();
            if (order_manager != nullptr) {
                order_manager->stop();
            }
            trading::adapters::logging::log_startup(
                "trader.error", "failed to open recording tape: " + *record_jsonl_arg.value);
            return kExitTapeOpenFailure;
        }
        feed_sink = recording_sink.get();
        trading::adapters::logging::log_startup(
            "trader.recording", "capturing ws payloads to: " + recording_sink->tape_path());
    }

    trading::adapters::ws::WsFeedRunner feed_runner{
        session,
        *feed_sink,
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
    trading::adapters::logging::log_startup(
        "trader.strategy_mode", std::string{trading::config::strategy_mode_name(runtime_config.strategy.mode)});
    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);

    const bool periodic_stats_enabled = runtime_config.stats_log_interval.count() > 0;
    auto next_stats_log_at = std::chrono::steady_clock::now();
    if (periodic_stats_enabled) {
        next_stats_log_at += runtime_config.stats_log_interval;
    }

    // Poll stop flag only; relaxed is sufficient for this shutdown check.
    while (!g_shutdown_requested.load(std::memory_order_relaxed)) {
        const std::size_t pumped = pipeline.pump_ingest(runtime_config.pump_batch_size);
        if (pumped == 0) {
            std::this_thread::sleep_for(runtime_config.pump_idle_sleep);
        }

        if (!periodic_stats_enabled) {
            continue;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now < next_stats_log_at) {
            continue;
        }

        const auto pipeline_stats = pipeline.stats();
        const auto feed_received = feed_runner.received_count();
        const auto feed_dropped = feed_runner.dropped_count();
        trading::oms::OrderManagerStats oms_stats{};
        if (order_manager != nullptr) {
            oms_stats = order_manager->stats();
        }
        const auto strategy_stats = aggregate_strategy_stats(strategy_handlers);

        trading::adapters::logging::log_startup(
            "trader.runtime",
            build_runtime_summary(pipeline_stats, strategy_stats, oms_stats, feed_received,
                                  feed_dropped));
        next_stats_log_at = now + runtime_config.stats_log_interval;
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
    const auto strategy_stats = aggregate_strategy_stats(strategy_handlers);
    const std::string summary =
        build_runtime_summary(pipeline_stats, strategy_stats, oms_stats, feed_received,
                              feed_dropped);
    trading::adapters::logging::log_startup("trader.pipeline", summary);
    if (recording_sink != nullptr && recording_sink->write_failed()) {
        trading::adapters::logging::log_startup(
            "trader.recording.error",
            "one or more payload writes failed for tape: " + recording_sink->tape_path());
    }
    return 0;
}
