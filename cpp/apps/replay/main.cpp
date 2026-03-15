#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

#include "trading/adapters/exchanges/kalshi/oms_adapter.hpp"
#include "trading/adapters/logging/logger.hpp"
#include "trading/config/trader_config.hpp"
#include "trading/metrics/latency_histogram.hpp"
#include "trading/oms/order_manager.hpp"
#include "trading/oms/paper_order_transport.hpp"
#include "trading/oms/position_ledger.hpp"
#include "trading/pipeline/live_pipeline.hpp"
#include "trading/pipeline/replay_harness.hpp"
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

constexpr int kExitConfigLoadFailure = 2;
constexpr int kExitArgsFailure = 3;
constexpr int kExitOmsStartFailure = 4;
constexpr int kExitPipelineStartFailure = 5;
constexpr int kExitReplayLoadFailure = 6;

std::optional<std::string> get_arg_value(int argc, char** argv, std::string_view flag) {
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            continue;
        }
        if (std::string_view{argv[index]} != flag) {
            continue;
        }
        if (index + 1 >= argc || argv[index + 1] == nullptr) {
            return std::nullopt;
        }
        return std::string{argv[index + 1]};
    }
    return std::nullopt;
}

bool parse_size_arg(std::string_view value, std::size_t& out) {
    if (value.empty()) {
        return false;
    }
    try {
        const auto parsed = std::stoull(std::string{value});
        if (parsed > std::numeric_limits<std::size_t>::max()) {
            return false;
        }
        out = static_cast<std::size_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

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

void merge_replay_stats(trading::pipeline::ReplayHarnessStats& aggregate,
                        const trading::pipeline::ReplayHarnessStats& sample) {
    aggregate.attempted_messages += sample.attempted_messages;
    aggregate.pushed_messages += sample.pushed_messages;
    aggregate.sink_rejected_messages += sample.sink_rejected_messages;
    aggregate.pump_calls += sample.pump_calls;
    aggregate.frames_pumped += sample.frames_pumped;
    aggregate.drain_completed = aggregate.drain_completed && sample.drain_completed;
    aggregate.final_ingest_queue_depth = sample.final_ingest_queue_depth;
    aggregate.final_shard_queue_depth = sample.final_shard_queue_depth;
}

std::string build_summary(std::size_t iterations, const trading::pipeline::ReplayHarnessStats& replay_stats,
                          const trading::pipeline::LivePipelineStats& pipeline_stats,
                          const trading::strategy::StrategyRunnerStats& strategy_stats,
                          const trading::oms::OrderManagerStats& oms_stats) {
    return "iterations=" + std::to_string(iterations) +
           ", replay_attempted=" + std::to_string(replay_stats.attempted_messages) +
           ", replay_pushed=" + std::to_string(replay_stats.pushed_messages) +
           ", replay_sink_rejects=" + std::to_string(replay_stats.sink_rejected_messages) +
           ", replay_pump_calls=" + std::to_string(replay_stats.pump_calls) +
           ", replay_frames_pumped=" + std::to_string(replay_stats.frames_pumped) +
           ", replay_drain_completed=" + std::string{replay_stats.drain_completed ? "true" : "false"} +
           ", replay_final_ingest_depth=" + std::to_string(replay_stats.final_ingest_queue_depth) +
           ", replay_final_shard_depth=" + std::to_string(replay_stats.final_shard_queue_depth) +
           ", frames_pumped=" + std::to_string(pipeline_stats.ingest_frames_pumped) +
           ", routed=" + std::to_string(pipeline_stats.route_success) +
           ", route_drop=" + std::to_string(pipeline_stats.route_drop) +
           ", parsed=" + std::to_string(pipeline_stats.shard_parsed) +
           ", parser_rejects=" + std::to_string(pipeline_stats.shard_parser_rejects) +
           ", book_apply_rejects=" + std::to_string(pipeline_stats.shard_apply_rejects) +
           ", parse_errors_total=" + std::to_string(pipeline_stats.shard_parse_errors) +
           ", ingest_q_hwm=" + std::to_string(pipeline_stats.ingest_queue_high_watermark) +
           ", shard_q_hwm_total=" + std::to_string(pipeline_stats.shard_queue_high_watermark_total) +
           ", shard_q_hwm_max=" + std::to_string(pipeline_stats.shard_queue_high_watermark_max) +
           ", parse_latency_p95_ns=" + std::to_string(pipeline_stats.recv_to_parse_latency.p95_ns) +
           ", parse_latency_p99_ns=" + std::to_string(pipeline_stats.recv_to_parse_latency.p99_ns) +
           ", strategy_events=" + std::to_string(strategy_stats.events_processed_count) +
           ", strategy_intents_emitted=" + std::to_string(strategy_stats.intents_emitted_count) +
           ", strategy_intents_submitted=" + std::to_string(strategy_stats.intents_submitted_count) +
           ", strategy_risk_rejects=" + std::to_string(strategy_stats.risk_reject_count) +
           ", oms_submitted=" + std::to_string(oms_stats.submitted_count) +
           ", oms_sent=" + std::to_string(oms_stats.sent_count) +
           ", oms_rejects=" +
           std::to_string(oms_stats.risk_reject_count + oms_stats.portfolio_risk_reject_count) +
           ", oms_pending_q_hwm=" + std::to_string(oms_stats.pending_intent_high_watermark) +
           ", oms_latency_p95_ns=" + std::to_string(oms_stats.submit_to_send_latency.p95_ns) +
           ", oms_latency_p99_ns=" + std::to_string(oms_stats.submit_to_send_latency.p99_ns);
}

} // namespace

int main(int argc, char** argv) {
    const auto config_path = get_arg_value(argc, argv, "--config");
    const auto payloads_path = get_arg_value(argc, argv, "--payloads");
    if (!config_path.has_value() || !payloads_path.has_value()) {
        trading::adapters::logging::log_startup(
            "replay.error",
            "usage: replay_app --config <path> --payloads <jsonl> [--repeat N] [--push-batch N] [--max-drain N]");
        return kExitArgsFailure;
    }

    std::size_t repeat_count = 1;
    if (const auto repeat_arg = get_arg_value(argc, argv, "--repeat"); repeat_arg.has_value()) {
        if (!parse_size_arg(*repeat_arg, repeat_count) || repeat_count == 0) {
            trading::adapters::logging::log_startup("replay.error", "--repeat must be a positive integer");
            return kExitArgsFailure;
        }
    }

    std::size_t push_batch_size = 1;
    if (const auto push_batch_arg = get_arg_value(argc, argv, "--push-batch");
        push_batch_arg.has_value()) {
        if (!parse_size_arg(*push_batch_arg, push_batch_size) || push_batch_size == 0) {
            trading::adapters::logging::log_startup("replay.error",
                                                    "--push-batch must be a positive integer");
            return kExitArgsFailure;
        }
    }

    std::size_t max_drain_iterations = 20'000;
    if (const auto max_drain_arg = get_arg_value(argc, argv, "--max-drain");
        max_drain_arg.has_value()) {
        if (!parse_size_arg(*max_drain_arg, max_drain_iterations) || max_drain_iterations == 0) {
            trading::adapters::logging::log_startup("replay.error",
                                                    "--max-drain must be a positive integer");
            return kExitArgsFailure;
        }
    }

    const auto loaded = trading::config::load_trader_config_from_file(*config_path);
    if (!loaded.ok) {
        trading::adapters::logging::log_startup("replay.config.error", loaded.error);
        return kExitConfigLoadFailure;
    }
    auto runtime_config = loaded.config;

    if (runtime_config.execution_mode == trading::config::TraderExecutionMode::kLive) {
        trading::adapters::logging::log_startup(
            "replay.error", "live OMS mode is not supported in replay_app; use paper or md-only");
        return kExitArgsFailure;
    }

    std::vector<std::string> replay_payloads;
    std::string replay_load_error;
    if (!trading::pipeline::ReplayHarness::load_jsonl_file(*payloads_path, replay_payloads,
                                                            replay_load_error)) {
        trading::adapters::logging::log_startup("replay.error", replay_load_error);
        return kExitReplayLoadFailure;
    }
    if (replay_payloads.empty()) {
        trading::adapters::logging::log_startup("replay.error", "payload file has no replayable lines");
        return kExitReplayLoadFailure;
    }

    trading::adapters::exchanges::kalshi::OmsAdapter oms_adapter;
    trading::oms::PositionLedger position_ledger;
    trading::strategy::LedgerShardRiskSnapshotProvider shard_risk_snapshot_provider{
        position_ledger};
    std::unique_ptr<trading::oms::IOrderTransport> oms_transport;
    std::unique_ptr<trading::oms::OrderManager> order_manager;
    std::unique_ptr<trading::strategy::OrderManagerIntentSink> order_manager_intent_sink;
    std::unique_ptr<trading::strategy::DroppingOrderIntentSink> dropping_intent_sink;
    trading::strategy::IOrderIntentSink* strategy_intent_sink{nullptr};

    if (runtime_config.execution_mode == trading::config::TraderExecutionMode::kPaper) {
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
        order_manager = std::make_unique<trading::oms::OrderManager>(
            oms_adapter, *oms_transport, position_ledger,
            trading::oms::OrderManagerConfig{
                .transport =
                    trading::oms::OrderTransportConfig{
                        .endpoint = "paper://oms",
                        .headers = {},
                    },
                .global_risk = runtime_config.risk.oms_global,
                .portfolio_risk = runtime_config.risk.oms_portfolio,
                .portfolio_snapshot_provider = &position_ledger,
                .loop_idle_sleep = std::chrono::milliseconds{1},
            });
        if (!order_manager->start()) {
            trading::adapters::logging::log_startup(
                "replay.error", "failed to start order manager: " + order_manager->last_error());
            return kExitOmsStartFailure;
        }
        order_manager_intent_sink =
            std::make_unique<trading::strategy::OrderManagerIntentSink>(*order_manager);
        strategy_intent_sink = order_manager_intent_sink.get();
    } else {
        dropping_intent_sink = std::make_unique<trading::strategy::DroppingOrderIntentSink>();
        strategy_intent_sink = dropping_intent_sink.get();
    }

    const std::size_t strategy_shard_count =
        std::max<std::size_t>(runtime_config.pipeline.shard_count, 1U);
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
        if (strategy_intent_sink == nullptr || shard_id >= shard_strategies.size() ||
            shard_id >= strategy_handlers.size() || shard_strategies[shard_id] == nullptr) {
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
        trading::adapters::logging::log_startup("replay.error", "failed to start pipeline");
        return kExitPipelineStartFailure;
    }

    trading::pipeline::ReplayHarness harness{
        pipeline,
        trading::pipeline::ReplayHarnessConfig{
            .push_batch_size = push_batch_size,
            .pump_batch_size = runtime_config.pump_batch_size,
            .max_drain_iterations = max_drain_iterations,
            .drain_sleep = runtime_config.pump_idle_sleep,
        },
    };

    trading::pipeline::ReplayHarnessStats replay_stats{};
    replay_stats.drain_completed = true;
    for (std::size_t iteration = 0; iteration < repeat_count; ++iteration) {
        const auto run_stats = harness.replay(replay_payloads);
        merge_replay_stats(replay_stats, run_stats);
    }

    if (order_manager != nullptr) {
        constexpr auto kOmsDrainBudget = std::chrono::milliseconds{2000};
        const auto deadline = std::chrono::steady_clock::now() + kOmsDrainBudget;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto oms_stats = order_manager->stats();
            if (oms_stats.pending_intent_count == 0) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }

    const auto pipeline_stats = pipeline.stats();
    trading::oms::OrderManagerStats oms_stats{};
    if (order_manager != nullptr) {
        oms_stats = order_manager->stats();
    }
    const auto strategy_stats = aggregate_strategy_stats(strategy_handlers);

    pipeline.stop();
    if (order_manager != nullptr) {
        order_manager->stop();
    }

    trading::adapters::logging::log_startup("replay.summary",
                                            build_summary(repeat_count, replay_stats, pipeline_stats,
                                                          strategy_stats, oms_stats));
    return 0;
}
