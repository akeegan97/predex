#include "predex/app.hpp"
#include "predex/audit/audit_logger.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/ingest/io_writer.hpp"
#include "predex/oms/oms.hpp"
#include "predex/oms/kalshi/private_ws_parser.hpp"
#include "predex/router/market_registry.hpp"
#include "predex/router/router.hpp"
#include "predex/router/shard_dispatch.hpp"
#include "predex/shards/shard.hpp"
#include "predex/shards/local_risk.hpp"
#include "predex/shards/shard_pipeline.hpp"
#include "predex/shards/strategies/cdf_violation.hpp"
#include "predex/shards/strategies/market_making.hpp"
#include "predex/shards/strategies/mean_reversion.hpp"
#include "predex/shards/strategies/monotonic_arb.hpp"
#include "predex/tape/logger.hpp"
#include "predex/websocket/client.hpp"
#include "predex/websocket/session.hpp"
#include "predex/websocket/kalshi/auth_signer.hpp"
#include "predex/websocket/kalshi/rest_client.hpp"
#include "predex/websocket/kalshi/ws_adapter.hpp"
#include "predex/utils/spsc_queue.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <memory>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <string_view>
#include <system_error>


namespace predex {
    constexpr std::int64_t kDefaultSleepMs = 100;
    namespace {
        [[nodiscard]] predex::internal::TimestampNs monotonic_now_ns() {
            return static_cast<predex::internal::TimestampNs>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count());
        }

        [[nodiscard]] internal::QtyLots parse_count_fp_to_lots(std::string_view value) {
            if (value.empty()) {
                return 0;
            }
            const std::size_t dot_pos = value.find('.');
            const std::string_view integer_part =
                dot_pos == std::string_view::npos ? value : value.substr(0, dot_pos);
            if (integer_part.empty()) {
                return 0;
            }
            std::int64_t parsed = 0;
            const auto [ptr, ec] =
                std::from_chars(integer_part.data(), integer_part.data() + integer_part.size(), parsed);
            if (ec != std::errc() || ptr != integer_part.data() + integer_part.size() || parsed < 0) {
                return 0;
            }
            return static_cast<internal::QtyLots>(parsed);
        }

        [[nodiscard]] std::optional<std::uint64_t> read_u64_field(
            const nlohmann::json& object,
            const char* key) {
            if (!object.contains(key)) {
                return std::nullopt;
            }
            const auto& value = object[key];
            if (value.is_number_unsigned()) {
                return value.get<std::uint64_t>();
            }
            if (value.is_number_integer()) {
                const auto as_i64 = value.get<std::int64_t>();
                if (as_i64 >= 0) {
                    return static_cast<std::uint64_t>(as_i64);
                }
            }
            if (value.is_string()) {
                try {
                    return static_cast<std::uint64_t>(std::stoull(value.get<std::string>()));
                } catch (...) {
                    return std::nullopt;
                }
            }
            return std::nullopt;
        }
    } // namespace

    struct App::Runtime {
        using FrameHandle = predex::core::ingest::kalshi::FrameHandle;
        using FrameQueue = predex::utils::SPSCQueue<FrameHandle>;
        using LocalRiskManager = predex::core::shards::kalshi::LocalRiskManager;
        using MonotonicArbStrategy =
            predex::core::shards::kalshi::strategies::MonotonicArbStrategy;
        using CdfViolationStrategy =
            predex::core::shards::kalshi::strategies::CdfViolationStrategy;
        using MarketMakingStrategy =
            predex::core::shards::kalshi::strategies::MarketMakingStrategy;
        using MeanReversionStrategy =
            predex::core::shards::kalshi::strategies::MeanReversionStrategy;
        using ShardPipeline = predex::core::shards::kalshi::DefaultShardPipeline<
            LocalRiskManager,
            MonotonicArbStrategy,
            CdfViolationStrategy,
            MarketMakingStrategy,
            MeanReversionStrategy>;
        using Shard = predex::core::shards::kalshi::Shard<ShardPipeline>;
        using Oms = predex::core::oms::kalshi::Oms;
        using OmsIntentQueue = predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsSubmission>;
        using OmsDecisionQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::IntentDecision>;
        using OmsLifecycleQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::OrderLifecycleEvent>;
        using AuditQueue =
            predex::utils::SPSCQueue<predex::core::audit::AuditEvent>;
        using SubmitOrderQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::SubmitOrderCmd>;
        using CancelOrderQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::CancelOrderCmd>;
        using ModifyOrderQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::ModifyOrderCmd>;
        using ReconcileRequestQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::ReconcileRequest>;
        using AuditLogger = predex::core::audit::AuditLogger;

        explicit Runtime(AppConfig config_in);

        AppConfig config;
        bool init_ok{true};
        mutable std::mutex error_mutex;
        std::string last_error;
        std::atomic<bool> running{false};
        std::vector<core::routing::kalshi::MarketRegistryEntry> market_registry_entries;
        std::unordered_map<internal::MarketId, std::string> market_ticker_by_id_;
        std::unordered_map<std::string, std::size_t> registry_index_by_ticker_;
        std::unordered_map<std::uint64_t, std::uint64_t> oms_ws_last_seq_by_sid_;
        static std::vector<predex::core::routing::kalshi::MarketRegistryEntry>
        build_market_registry_entries(const AppConfig& config) {
            std::vector<predex::core::routing::kalshi::MarketRegistryEntry> entries;
            entries.reserve(config.market_routes.size());
            for (const auto& route : config.market_routes) {
                entries.push_back({
                    .ticker_ = route.market_ticker,
                    .market_id_ = static_cast<uint32_t>(route.market_id),
                    .event_id_ = static_cast<uint32_t>(route.event_id),
                    .affinity_key_ = static_cast<uint16_t>(route.affinity_key),
                    .topology_kind_ = route.topology_kind,
                    .strike_key_ = route.strike_key,
                });
            }
            return entries;
        }
        static bool
        build_event_definitions_by_shard(
            const std::vector<MarketRouteConfig>& routes,
            std::size_t shard_count,
            std::vector<std::vector<core::shards::kalshi::EventDefinition>>& definitions_by_shard,
            std::string& error_out) {
            definitions_by_shard.clear();
            definitions_by_shard.resize(shard_count);
            if (shard_count == 0) {
                error_out = "pipeline.shard_count must be greater than zero";
                return false;
            }

            struct EventAccumulator {
                predex::internal::EventTopologyKind topology_kind{
                    predex::internal::EventTopologyKind::kUnknown};
                std::uint16_t affinity_key{0};
                bool affinity_initialized{false};
                std::vector<core::shards::kalshi::EventMarketDefinition> markets;
            };

            std::unordered_map<std::uint32_t, EventAccumulator> grouped_events;
            grouped_events.reserve(routes.size());

            for (const auto& route : routes) {
                if (route.market_id == 0 || route.event_id == 0) {
                    error_out = "market route entries must define non-zero market_id and event_id";
                    return false;
                }
                if (route.topology_kind == predex::internal::EventTopologyKind::kUnknown) {
                    error_out = "market route entries must define a non-unknown topology_kind";
                    return false;
                }
                const auto event_id32 = static_cast<std::uint32_t>(route.event_id);
                const auto affinity_key16 = static_cast<std::uint16_t>(route.affinity_key);
                auto& accumulator = grouped_events[event_id32];
                if (!accumulator.affinity_initialized) {
                    accumulator.affinity_key = affinity_key16;
                    accumulator.affinity_initialized = true;
                    accumulator.topology_kind = route.topology_kind;
                } else if (accumulator.affinity_key != affinity_key16) {
                    error_out = "event " + std::to_string(event_id32) +
                        " has inconsistent affinity_key values in config";
                    return false;
                } else if (accumulator.topology_kind != route.topology_kind) {
                    error_out = "event " + std::to_string(event_id32) +
                        " has inconsistent topology_kind values in config";
                    return false;
                }
                const auto market_id32 = static_cast<std::uint32_t>(route.market_id);
                const auto duplicate_market = std::find_if(
                    accumulator.markets.begin(),
                    accumulator.markets.end(),
                    [market_id32](const core::shards::kalshi::EventMarketDefinition& market) {
                        return market.market_id == market_id32;
                    });
                if (duplicate_market != accumulator.markets.end()) {
                    error_out = "event " + std::to_string(event_id32) +
                        " contains duplicate market_id " + std::to_string(market_id32) +
                        " in config";
                    return false;
                }
                accumulator.markets.push_back(core::shards::kalshi::EventMarketDefinition{
                    .market_id = market_id32,
                    .strike_key = route.strike_key,
                    .close_time_s = route.close_time_s,
                    .tradeable = route.tradeable,
                });
            }

            for (const auto& [event_id, accumulator] : grouped_events) {
                const std::size_t shard_id = accumulator.affinity_key % shard_count;
                definitions_by_shard[shard_id].push_back(core::shards::kalshi::EventDefinition{
                    .event_id = event_id,
                    .topology_kind = accumulator.topology_kind,
                    .expected_market_count = accumulator.markets.size(),
                    .markets = accumulator.markets,
                });
            }

            return true;
        }

        websocket::kalshi::AuthSigner auth_signer;
        websocket::kalshi::WsAdapter ws_adapter;
        websocket::BoostBeastWsTransport ws_transport;
        websocket::WsSession ws_session;
        websocket::kalshi::RestClient oms_rest_client;
        websocket::kalshi::WsAdapter oms_ws_adapter;
        websocket::BoostBeastWsTransport oms_ws_transport;
        websocket::WsSession oms_ws_session;
        core::oms::kalshi::PrivateWsParser oms_private_ws_parser;

        core::ingest::kalshi::FramePool frame_pool;
        std::unique_ptr<FrameQueue> io_to_router_queue;
        std::unique_ptr<FrameQueue> router_to_logger_queue;
        // Per-producer SPSC recycle queues fanning into IOWriter. Each producer thread owns
        // exactly one of these; IOWriter is the sole consumer across all of them.
        std::unique_ptr<FrameQueue> recycle_from_logger;
        std::unique_ptr<FrameQueue> recycle_from_router;
        std::vector<std::unique_ptr<FrameQueue>> recycle_from_shards;
        std::vector<FrameQueue*> recycle_input_queue_ptrs;
        std::vector<std::unique_ptr<FrameQueue>> shard_input_queues;
        std::vector<std::unique_ptr<FrameQueue>> shard_to_logger_queues;
        std::vector<std::unique_ptr<OmsIntentQueue>> shard_to_oms_intent_queues;
        std::vector<std::unique_ptr<OmsDecisionQueue>> oms_to_shard_decision_queues;
        std::vector<std::unique_ptr<OmsLifecycleQueue>> oms_to_shard_lifecycle_queues;
        std::vector<std::unique_ptr<AuditQueue>> shard_audit_queues;
        std::unique_ptr<SubmitOrderQueue> oms_submit_queue;
        std::unique_ptr<CancelOrderQueue> oms_cancel_queue;
        std::unique_ptr<ModifyOrderQueue> oms_modify_queue;
        std::unique_ptr<OmsLifecycleQueue> oms_rest_update_queue;
        std::unique_ptr<OmsLifecycleQueue> oms_ws_update_queue;
        // Private-WS thread is the sole producer; REST thread is the sole consumer.
        // Preserves the SPSC invariant while keeping all RestClient mutations on one thread.
        std::unique_ptr<ReconcileRequestQueue> oms_reconcile_request_queue;
        std::unique_ptr<AuditQueue> oms_audit_queue;

        core::routing::kalshi::MarketRegistry market_registry;
        std::vector<FrameQueue*> shard_input_queue_ptrs;
        std::vector<FrameQueue*> logger_input_queue_ptrs;
        std::vector<OmsIntentQueue*> shard_to_oms_intent_queue_ptrs;
        std::vector<OmsDecisionQueue*> oms_to_shard_decision_queue_ptrs;
        std::vector<OmsLifecycleQueue*> oms_to_shard_lifecycle_queue_ptrs;
        std::vector<AuditQueue*> audit_input_queue_ptrs;

        std::unique_ptr<core::routing::kalshi::ShardDispatch> shard_dispatch;
        std::unique_ptr<core::routing::kalshi::Router> router;
        std::unique_ptr<core::ingest::kalshi::IOWriter> io_writer;
        std::unique_ptr<core::tape::kalshi::Logger> tape_logger;
        std::unique_ptr<AuditLogger> audit_logger;
        std::unique_ptr<Oms> oms;

        std::vector<core::shards::kalshi::EventStore> event_stores;
        std::vector<std::unique_ptr<Shard>> shards;
        std::jthread io_thread;
        std::jthread router_thread;
        std::vector<std::jthread> shard_threads; // will house strategy & risk eventually on the same thread as shard 
        std::jthread oms_thread;
        std::jthread oms_rest_thread;
        std::jthread oms_private_ws_thread;
        std::jthread logger_thread;
        std::jthread audit_thread;

        
        

        bool start();
        void run();
        void stop();

        void io_loop(const std::stop_token& stop_token);
        void oms_rest_loop(const std::stop_token& stop_token);
        void oms_private_ws_loop(const std::stop_token& stop_token);
        [[nodiscard]] bool reconcile_open_orders_from_rest(bool is_startup);
        void router_loop(const std::stop_token& stop_token) const;
        void shard_loop(std::size_t shard_index, const std::stop_token& stop_token) const;
        void oms_loop(const std::stop_token& stop_token);
        void logger_loop(const std::stop_token& stop_token) const;
        void audit_loop(const std::stop_token& stop_token) const;
        void set_error(std::string message);
        std::string_view last_error_view() const noexcept;
        void print_health_status() const noexcept;
    };
    App::Runtime::Runtime(AppConfig config_in)
        : config(std::move(config_in)),
        market_registry_entries(build_market_registry_entries(config)),
        market_ticker_by_id_(),
        auth_signer(websocket::kalshi::Credentials{
            .key_id = config.kalshi_ws.key_id,
            .private_key_pem = config.kalshi_ws.private_key_pem,
        }),
        ws_adapter(auth_signer, config.kalshi_ws.endpoint),
        ws_session(ws_transport, ws_adapter),
        oms_rest_client(auth_signer, config.oms_transport.rest_endpoint),
        oms_ws_adapter(auth_signer, config.oms_transport.private_ws_endpoint),
        oms_ws_session(oms_ws_transport, oms_ws_adapter),
        frame_pool(config.pipeline.frame_pool_capacity),
        market_registry(market_registry_entries) {
        market_ticker_by_id_.reserve(market_registry_entries.size());
        registry_index_by_ticker_.reserve(market_registry_entries.size());
        for (std::size_t i = 0; i < market_registry_entries.size(); ++i) {
            const auto& entry = market_registry_entries[i];
            market_ticker_by_id_[entry.market_id_] = entry.ticker_;
            registry_index_by_ticker_[entry.ticker_] = i;
        }

        io_to_router_queue =
            std::make_unique<FrameQueue>(config.pipeline.io_to_router_capacity);
        router_to_logger_queue =
            std::make_unique<FrameQueue>(config.pipeline.router_to_logger_capacity);
        recycle_from_logger =
            std::make_unique<FrameQueue>(config.pipeline.frame_pool_capacity);
        recycle_from_router =
            std::make_unique<FrameQueue>(config.pipeline.frame_pool_capacity);
        recycle_from_shards.reserve(config.pipeline.shard_count);
        recycle_input_queue_ptrs.reserve(config.pipeline.shard_count + 2);

        shard_input_queues.reserve(config.pipeline.shard_count);
        shard_to_logger_queues.reserve(config.pipeline.shard_count);
        shard_to_oms_intent_queues.reserve(config.pipeline.shard_count);
        oms_to_shard_decision_queues.reserve(config.pipeline.shard_count);
        oms_to_shard_lifecycle_queues.reserve(config.pipeline.shard_count);
        shard_audit_queues.reserve(config.pipeline.shard_count);
        shard_input_queue_ptrs.reserve(config.pipeline.shard_count);
        logger_input_queue_ptrs.reserve(config.pipeline.shard_count + 1);
        shard_to_oms_intent_queue_ptrs.reserve(config.pipeline.shard_count);
        oms_to_shard_decision_queue_ptrs.reserve(config.pipeline.shard_count);
        oms_to_shard_lifecycle_queue_ptrs.reserve(config.pipeline.shard_count);
        audit_input_queue_ptrs.reserve(config.pipeline.shard_count + 1);
        event_stores.reserve(config.pipeline.shard_count);
        shards.reserve(config.pipeline.shard_count);

        for (std::size_t i = 0; i < config.pipeline.shard_count; ++i) {
            shard_input_queues.push_back(
                std::make_unique<FrameQueue>(config.pipeline.shard_input_capacity));
            shard_to_logger_queues.push_back(
                std::make_unique<FrameQueue>(config.pipeline.shard_to_logger_capacity));
            shard_to_oms_intent_queues.push_back(
                std::make_unique<OmsIntentQueue>(config.pipeline.shard_input_capacity));
            oms_to_shard_decision_queues.push_back(
                std::make_unique<OmsDecisionQueue>(config.pipeline.shard_input_capacity));
            oms_to_shard_lifecycle_queues.push_back(
                std::make_unique<OmsLifecycleQueue>(config.pipeline.shard_input_capacity));
            shard_audit_queues.push_back(
                std::make_unique<AuditQueue>(config.pipeline.shard_input_capacity));
            recycle_from_shards.push_back(
                std::make_unique<FrameQueue>(config.pipeline.frame_pool_capacity));
            event_stores.emplace_back();
        }

        oms_submit_queue =
            std::make_unique<SubmitOrderQueue>(config.pipeline.shard_input_capacity);
        oms_cancel_queue =
            std::make_unique<CancelOrderQueue>(config.pipeline.shard_input_capacity);
        oms_modify_queue =
            std::make_unique<ModifyOrderQueue>(config.pipeline.shard_input_capacity);
        oms_rest_update_queue =
            std::make_unique<OmsLifecycleQueue>(config.pipeline.shard_input_capacity);
        oms_ws_update_queue =
            std::make_unique<OmsLifecycleQueue>(config.pipeline.shard_input_capacity);
        // Small queue: reconcile requests coalesce — one in-flight covers any that would
        // have followed it. Capacity 2 so a WS-thread push never blocks the reconnect path.
        oms_reconcile_request_queue = std::make_unique<ReconcileRequestQueue>(2);
        oms_audit_queue =
            std::make_unique<AuditQueue>(config.pipeline.shard_input_capacity);

        std::vector<std::vector<core::shards::kalshi::EventDefinition>> event_definitions_by_shard;
        std::string event_definition_error;
        if (!build_event_definitions_by_shard(
                config.market_routes,
                config.pipeline.shard_count,
                event_definitions_by_shard,
                event_definition_error)) {
            init_ok = false;
            set_error(std::move(event_definition_error));
            return;
        }
        for (std::size_t shard_index = 0; shard_index < config.pipeline.shard_count; ++shard_index) {
            if (!event_stores[shard_index].initialize(event_definitions_by_shard[shard_index])) {
                init_ok = false;
                set_error("Failed to initialize event store for shard " +
                    std::to_string(shard_index));
                return;
            }
        }

        for (const auto& queue : shard_input_queues) {
            shard_input_queue_ptrs.push_back(queue.get());
        }
        for (const auto& queue : shard_to_oms_intent_queues) {
            shard_to_oms_intent_queue_ptrs.push_back(queue.get());
        }
        for (const auto& queue : oms_to_shard_decision_queues) {
            oms_to_shard_decision_queue_ptrs.push_back(queue.get());
        }
        for (const auto& queue : oms_to_shard_lifecycle_queues) {
            oms_to_shard_lifecycle_queue_ptrs.push_back(queue.get());
        }
        for (const auto& queue : shard_audit_queues) {
            audit_input_queue_ptrs.push_back(queue.get());
        }
        audit_input_queue_ptrs.push_back(oms_audit_queue.get());

        logger_input_queue_ptrs.push_back(router_to_logger_queue.get());
        for (const auto& queue : shard_to_logger_queues) {
            logger_input_queue_ptrs.push_back(queue.get());
        }

        // Assemble the per-producer recycle fan-in for IOWriter. Order here does not matter —
        // IOWriter round-robins, but we list logger first since it is the highest-volume
        // producer (every tape-written frame).
        recycle_input_queue_ptrs.push_back(recycle_from_logger.get());
        recycle_input_queue_ptrs.push_back(recycle_from_router.get());
        for (const auto& queue : recycle_from_shards) {
            recycle_input_queue_ptrs.push_back(queue.get());
        }

        shard_dispatch =
            std::make_unique<core::routing::kalshi::ShardDispatch>(shard_input_queue_ptrs);

        router = std::make_unique<core::routing::kalshi::Router>(
            *io_to_router_queue,
            frame_pool,
            market_registry,
            *shard_dispatch,
            *router_to_logger_queue,
            *recycle_from_router);

        io_writer = std::make_unique<core::ingest::kalshi::IOWriter>(
            frame_pool,
            *io_to_router_queue,
            recycle_input_queue_ptrs);

        tape_logger = std::make_unique<core::tape::kalshi::Logger>(
            logger_input_queue_ptrs,
            frame_pool,
            *recycle_from_logger,
            config.tape.output_path);
        audit_logger = std::make_unique<AuditLogger>(
            audit_input_queue_ptrs,
            config.audit.output_path);

        predex::core::oms::kalshi::GlobalRiskLimits global_risk_limits{};
        global_risk_limits.available_capital_ticks = config.oms_transport.available_capital_ticks;
        global_risk_limits.trading_enabled = config.local_risk.trading_enabled;

        oms = std::make_unique<Oms>(
            shard_to_oms_intent_queue_ptrs,
            oms_to_shard_decision_queue_ptrs,
            oms_to_shard_lifecycle_queue_ptrs,
            predex::core::oms::kalshi::OmsTransportQueues{
                .submit_queue = oms_submit_queue.get(),
                .cancel_queue = oms_cancel_queue.get(),
                .modify_queue = oms_modify_queue.get(),
                .rest_update_queue = oms_rest_update_queue.get(),
                .ws_update_queue = oms_ws_update_queue.get(),
            },
            predex::core::oms::kalshi::GlobalRiskManager{global_risk_limits},
            oms_audit_queue.get(),
            config.oms_transport.max_session_loss_ticks);

        predex::core::shards::kalshi::LocalRiskLimits local_risk_limits{};
        if (config.local_risk.max_net_position_lots_per_market > 0) {
            local_risk_limits.max_net_position_lots_per_market =
                config.local_risk.max_net_position_lots_per_market;
        }
        local_risk_limits.min_seconds_to_close = config.local_risk.min_seconds_to_close;
        local_risk_limits.trading_enabled = config.local_risk.trading_enabled;

        for (std::size_t i = 0; i < config.pipeline.shard_count; ++i) {
            shards.push_back(std::make_unique<Shard>(
                *shard_input_queues[i],
                frame_pool,
                *shard_to_logger_queues[i],
                *recycle_from_shards[i],
                event_stores[i],
                ShardPipeline{
                    static_cast<std::uint16_t>(i),
                    LocalRiskManager{local_risk_limits},
                    shard_to_oms_intent_queues[i].get(),
                    oms_to_shard_decision_queues[i].get(),
                    oms_to_shard_lifecycle_queues[i].get(),
                    shard_audit_queues[i].get(),
                    MonotonicArbStrategy{},
                    CdfViolationStrategy{},
                    MarketMakingStrategy{},
                    MeanReversionStrategy{},
                }));
        }
    }
    void App::Runtime::set_error(std::string message){
        std::lock_guard lock(error_mutex);
        last_error = std::move(message);
    }

    bool App::Runtime::start(){
        if (!init_ok) {
            return false;
        }
        if(running.load(std::memory_order_acquire)){
            return true;
        }
        if(!ws_session.connect()){
            ws_session.close();
            set_error(ws_session.last_error());
            return false;
        }

        for(const auto& channel : config.kalshi_ws.channels){
            if(!ws_session.subscribe(channel, config.kalshi_ws.market_tickers)){
                ws_session.close();
                set_error(ws_session.last_error());
                return false;
            }
        }
        for (const auto& channel : config.kalshi_ws.lifecycle_channels) {
            if (!ws_session.subscribe(channel, {})) {
                ws_session.close();
                set_error(ws_session.last_error());
                return false;
            }
        }

        if (config.oms_transport.enabled) {
            if (!oms_ws_session.connect()) {
                oms_ws_session.close();
                ws_session.close();
                set_error(oms_ws_session.last_error());
                return false;
            }
            oms_ws_last_seq_by_sid_.clear();

            for (const auto& channel : config.oms_transport.private_ws_channels) {
                if (!oms_ws_session.subscribe(channel)) {
                    oms_ws_session.close();
                    ws_session.close();
                    set_error(oms_ws_session.last_error());
                    return false;
                }
            }
            if (!reconcile_open_orders_from_rest(/*is_startup=*/true)) {
                oms_ws_session.close();
                ws_session.close();
                return false;
            }
        }
        set_error("");
        running.store(true, std::memory_order_release);
        
        io_thread = std::jthread([this](const std::stop_token& stop_token){
            io_loop(stop_token);
        });
        router_thread = std::jthread([this](const std::stop_token& stop_token){
            router_loop(stop_token);
        });
        shard_threads.clear();
        shard_threads.reserve(config.pipeline.shard_count);
        for(std::size_t i = 0; i < config.pipeline.shard_count; ++i){
            shard_threads.emplace_back([this, i](const std::stop_token& stop_token){
                shard_loop(i, stop_token);
            });
        }
        oms_thread = std::jthread([this](const std::stop_token& stop_token){
            oms_loop(stop_token);
        });
        oms_rest_thread = std::jthread([this](const std::stop_token& stop_token){
            oms_rest_loop(stop_token);
        });
        if (config.oms_transport.enabled) {
            oms_private_ws_thread = std::jthread([this](const std::stop_token& stop_token) {
                oms_private_ws_loop(stop_token);
            });
        }
        logger_thread = std::jthread([this](const std::stop_token& stop_token){
            logger_loop(stop_token);
        });
        audit_thread = std::jthread([this](const std::stop_token& stop_token){
            audit_loop(stop_token);
        });
        return true;
    }

    void App::Runtime::io_loop(const std::stop_token& stop_token) {
        std::uint32_t reconnect_attempts = 0;
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            const auto recv_result = ws_session.recv_text(std::chrono::milliseconds{50});

            if (recv_result.status == websocket::RecvStatus::kTimeout) {
                continue;
            }

            if (recv_result.status == websocket::RecvStatus::kClosed) {
                if (!running.load(std::memory_order_acquire) || stop_token.stop_requested()) {
                    break;
                }
                // Reconnect with exponential backoff.
                const std::uint32_t backoff_ms = std::min<std::uint32_t>(
                    5000U, 100U * (1U << std::min(reconnect_attempts, 5U)));
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                ++reconnect_attempts;

                if (!ws_session.connect()) {
                    continue;
                }
                bool subscribe_ok = true;
                for (const auto& channel : config.kalshi_ws.channels) {
                    if (!ws_session.subscribe(channel, config.kalshi_ws.market_tickers)) {
                        subscribe_ok = false;
                        break;
                    }
                }
                for (const auto& channel : config.kalshi_ws.lifecycle_channels) {
                    if (!ws_session.subscribe(channel, {})) {
                        subscribe_ok = false;
                        break;
                    }
                }
                if (!subscribe_ok) {
                    ws_session.close();
                    continue;
                }
                // Reset sequence state so fresh SIDs from the new session are accepted,
                // and signal each shard to drop its stale book state.
                router->reset_sequence_state();
                for (const auto& shard : shards) {
                    shard->request_reset();
                }
                reconnect_attempts = 0;
                continue;
            }

            if (recv_result.status == websocket::RecvStatus::kError) {
                set_error(ws_session.last_error());
                running.store(false, std::memory_order_release);
                break;
            }

            reconnect_attempts = 0;
            if (!io_writer->on_wire_message(recv_result.payload)) {
                set_error("Failed to enqueue message into IOWriter");
                running.store(false, std::memory_order_release);
                break;
            }
        }

        ws_session.close();
    }

    void App::Runtime::oms_rest_loop(const std::stop_token& stop_token) {
        std::uint32_t idle_iters = 0;
        const bool live_transport_enabled = config.oms_transport.enabled;
        // Connection keep-warm cadence. 30s is well inside the typical AWS keep-alive idle
        // cap (~60s) and avoids the ~100ms TLS handshake tail when the next order lands
        // after a quiet period. check_and_keep_warm() is a no-op if we haven't actually
        // been idle long enough.
        constexpr std::uint64_t kKeepWarmThresholdSeconds = 30;
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            bool processed = false;

            // Reconcile requests arrive here from the private-WS thread on reconnect /
            // seq-gap. Running the multi-page fetch on this thread keeps the persistent
            // RestClient single-owner — any cross-thread use of the stream would be UB now
            // that it is no longer a per-call scratch connection.
            predex::core::oms::kalshi::ReconcileRequest reconcile_req{};
            if (oms_reconcile_request_queue != nullptr &&
                oms_reconcile_request_queue->try_pop(reconcile_req)) {
                processed = true;
                if (live_transport_enabled) {
                    if (!reconcile_open_orders_from_rest(/*is_startup=*/false)) {
                        running.store(false, std::memory_order_release);
                        break;
                    }
                }
            }

            predex::core::oms::kalshi::SubmitOrderCmd submit_cmd{};
            if (oms_submit_queue != nullptr && oms_submit_queue->try_pop(submit_cmd)) {
                processed = true;
                predex::core::oms::kalshi::OrderLifecycleEvent event{};
                if (live_transport_enabled) {
                    const auto ticker_it =
                        market_ticker_by_id_.find(submit_cmd.intent.origin.market_id);
                    const std::string market_ticker =
                        ticker_it != market_ticker_by_id_.end()
                        ? ticker_it->second
                        : std::string{};
                    const auto result = oms_rest_client.submit_order(submit_cmd, market_ticker);
                    event = predex::core::oms::kalshi::OrderLifecycleEvent{
                        .origin = submit_cmd.intent.origin,
                        .oms_request_id = submit_cmd.oms_request_id,
                        .kind = result.ok
                            ? predex::core::oms::kalshi::OrderLifecycleEventKind::kAck
                            : predex::core::oms::kalshi::OrderLifecycleEventKind::kReject,
                        .status = result.ok
                            ? predex::core::oms::kalshi::OmsOrderStatus::kLive
                            : predex::core::oms::kalshi::OmsOrderStatus::kRejected,
                        .client_order_id = submit_cmd.client_order_id,
                        .exchange_order_id = result.exchange_order_id,
                        .recv_ts_ns = monotonic_now_ns(),
                    };
                    if (result.ok) {
                        event.data = predex::core::oms::kalshi::OrderAck{
                            .accepted_qty_lots = submit_cmd.intent.qty_lots,
                        };
                    } else {
                        event.data = predex::core::oms::kalshi::OrderReject{
                            .reason_code = "rest_submit_failed",
                            .reason_message = result.error,
                        };
                    }
                } else {
                    event = predex::core::oms::kalshi::OrderLifecycleEvent{
                        .origin = submit_cmd.intent.origin,
                        .oms_request_id = submit_cmd.oms_request_id,
                        .kind = predex::core::oms::kalshi::OrderLifecycleEventKind::kReject,
                        .status = predex::core::oms::kalshi::OmsOrderStatus::kRejected,
                        .client_order_id = submit_cmd.client_order_id,
                        .exchange_order_id = std::nullopt,
                        .recv_ts_ns = monotonic_now_ns(),
                        .data = predex::core::oms::kalshi::OrderReject{
                            .reason_code = "transport_disabled",
                            .reason_message = "OMS transport disabled: submit dropped",
                        },
                    };
                }
                if (!oms_rest_update_queue->try_push(std::move(event))) {
                    set_error("OMS transport update queue backpressured on submit result");
                    running.store(false, std::memory_order_release);
                    break;
                }
            }

            predex::core::oms::kalshi::CancelOrderCmd cancel_cmd{};
            if (oms_cancel_queue != nullptr && oms_cancel_queue->try_pop(cancel_cmd)) {
                processed = true;
                if (live_transport_enabled) {
                    const auto result = oms_rest_client.cancel_order(cancel_cmd);
                    if (!result.ok) {
                        predex::core::oms::kalshi::OrderLifecycleEvent event{
                            .origin = cancel_cmd.origin,
                            .oms_request_id = cancel_cmd.oms_request_id,
                            .kind =
                                predex::core::oms::kalshi::OrderLifecycleEventKind::kCancelReject,
                            .status = predex::core::oms::kalshi::OmsOrderStatus::kPendingCancel,
                            .client_order_id = cancel_cmd.client_order_id,
                            .exchange_order_id = cancel_cmd.exchange_order_id,
                            .recv_ts_ns = monotonic_now_ns(),
                            .data = predex::core::oms::kalshi::CancelReject{
                                .reason_code = "rest_cancel_failed",
                                .reason_message = result.error,
                            },
                        };
                        if (!oms_rest_update_queue->try_push(std::move(event))) {
                            set_error(
                                "OMS transport update queue backpressured on cancel result");
                            running.store(false, std::memory_order_release);
                            break;
                        }
                    }
                } else {
                    predex::core::oms::kalshi::OrderLifecycleEvent event{
                        .origin = cancel_cmd.origin,
                        .oms_request_id = cancel_cmd.oms_request_id,
                        .kind = predex::core::oms::kalshi::OrderLifecycleEventKind::kCancelReject,
                        .status = predex::core::oms::kalshi::OmsOrderStatus::kPendingCancel,
                        .client_order_id = cancel_cmd.client_order_id,
                        .exchange_order_id = cancel_cmd.exchange_order_id,
                        .recv_ts_ns = monotonic_now_ns(),
                        .data = predex::core::oms::kalshi::CancelReject{
                            .reason_code = "transport_disabled",
                            .reason_message = "OMS transport disabled: cancel dropped",
                        },
                    };
                    if (!oms_rest_update_queue->try_push(std::move(event))) {
                        set_error(
                            "OMS transport update queue backpressured on cancel result");
                        running.store(false, std::memory_order_release);
                        break;
                    }
                }
            }

            predex::core::oms::kalshi::ModifyOrderCmd modify_cmd{};
            if (oms_modify_queue != nullptr && oms_modify_queue->try_pop(modify_cmd)) {
                processed = true;
                if (live_transport_enabled) {
                    const auto result = oms_rest_client.modify_order(modify_cmd);
                    if (!result.ok) {
                        predex::core::oms::kalshi::OrderLifecycleEvent event{
                            .origin = modify_cmd.replacement_intent.origin,
                            .oms_request_id = modify_cmd.oms_request_id,
                            .kind =
                                predex::core::oms::kalshi::OrderLifecycleEventKind::kReplaceReject,
                            .status = predex::core::oms::kalshi::OmsOrderStatus::kPendingModify,
                            .client_order_id = modify_cmd.client_order_id,
                            .exchange_order_id = modify_cmd.exchange_order_id,
                            .recv_ts_ns = monotonic_now_ns(),
                            .data = predex::core::oms::kalshi::ReplaceReject{
                                .reason_code = "rest_modify_failed",
                                .reason_message = result.error,
                            },
                        };
                        if (!oms_rest_update_queue->try_push(std::move(event))) {
                            set_error(
                                "OMS transport update queue backpressured on modify result");
                            running.store(false, std::memory_order_release);
                            break;
                        }
                    }
                } else {
                    predex::core::oms::kalshi::OrderLifecycleEvent event{
                        .origin = modify_cmd.replacement_intent.origin,
                        .oms_request_id = modify_cmd.oms_request_id,
                        .kind = predex::core::oms::kalshi::OrderLifecycleEventKind::kReplaceReject,
                        .status = predex::core::oms::kalshi::OmsOrderStatus::kPendingModify,
                        .client_order_id = modify_cmd.client_order_id,
                        .exchange_order_id = modify_cmd.exchange_order_id,
                        .recv_ts_ns = monotonic_now_ns(),
                        .data = predex::core::oms::kalshi::ReplaceReject{
                            .reason_code = "transport_disabled",
                            .reason_message = "OMS transport disabled: modify dropped",
                        },
                    };
                    if (!oms_rest_update_queue->try_push(std::move(event))) {
                        set_error(
                            "OMS transport update queue backpressured on modify result");
                        running.store(false, std::memory_order_release);
                        break;
                    }
                }
            }

            if (processed) {
                idle_iters = 0;
                continue;
            }

            ++idle_iters;
            if (idle_iters <= config.pipeline.idle_policy.spin_iters_oms) {
                continue;
            }
            if (live_transport_enabled) {
                oms_rest_client.check_and_keep_warm(kKeepWarmThresholdSeconds);
            }
            if (config.pipeline.idle_policy.yield_every > 0U &&
                (idle_iters % config.pipeline.idle_policy.yield_every) == 0U) {
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::oms_private_ws_loop(const std::stop_token& stop_token) {
        std::vector<predex::core::oms::kalshi::OrderLifecycleEvent> parsed_events;
        parsed_events.reserve(predex::core::oms::kalshi::kDefaultMaxPrivateWsEventsPerMessage);
        std::uint32_t reconnect_attempts = 0;
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            const auto recv_result = oms_ws_session.recv_text(std::chrono::milliseconds{50});
            if (recv_result.status == websocket::RecvStatus::kTimeout) {
                continue;
            }
            if (recv_result.status == websocket::RecvStatus::kClosed) {
                if (!running.load(std::memory_order_acquire)) {
                    break;
                }
                const std::uint32_t backoff_ms =
                    std::min<std::uint32_t>(5000, 100 * (1U << std::min(reconnect_attempts, 5U)));
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                ++reconnect_attempts;
                if (!oms_ws_session.connect()) {
                    continue;
                }
                oms_ws_last_seq_by_sid_.clear();
                bool subscribe_ok = true;
                for (const auto& channel : config.oms_transport.private_ws_channels) {
                    if (!oms_ws_session.subscribe(channel)) {
                        subscribe_ok = false;
                        break;
                    }
                }
                if (!subscribe_ok) {
                    oms_ws_session.close();
                    continue;
                }
                // Hand reconciliation to the REST thread: it owns the persistent
                // RestClient, and a direct fetch_open_orders() call from here would race
                // the REST thread on the shared TLS stream.
                if (oms_reconcile_request_queue != nullptr) {
                    (void)oms_reconcile_request_queue->try_push(
                        predex::core::oms::kalshi::ReconcileRequest{
                            .reason = predex::core::oms::kalshi::ReconcileReason::kReconnect,
                            .requested_ts_ns = monotonic_now_ns(),
                        });
                }
                reconnect_attempts = 0;
                continue;
            }
            if (recv_result.status == websocket::RecvStatus::kError) {
                oms_ws_session.close();
                continue;
            }

            reconnect_attempts = 0;
            try {
                const auto envelope = nlohmann::json::parse(recv_result.payload);
                if (envelope.is_object()) {
                    const auto sid = read_u64_field(envelope, "sid");
                    const auto seq = read_u64_field(envelope, "seq");
                    if (sid.has_value() && seq.has_value()) {
                        auto [it, inserted] = oms_ws_last_seq_by_sid_.emplace(*sid, *seq);
                        if (!inserted) {
                            const auto last_seq = it->second;
                            if (*seq <= last_seq) {
                                continue;
                            }
                            if (*seq != last_seq + 1) {
                                // Same rationale as the reconnect branch: reconcile runs
                                // on the REST thread to keep the RestClient single-owner.
                                if (oms_reconcile_request_queue != nullptr) {
                                    (void)oms_reconcile_request_queue->try_push(
                                        predex::core::oms::kalshi::ReconcileRequest{
                                            .reason = predex::core::oms::kalshi::
                                                ReconcileReason::kSeqGap,
                                            .requested_ts_ns = monotonic_now_ns(),
                                        });
                                }
                            }
                            it->second = *seq;
                        }
                    }
                }
            } catch (const std::exception&) {
                // Let the parser handle invalid payload errors consistently.
            }
            const auto parse_status =
                oms_private_ws_parser.parse_message(recv_result.payload, parsed_events);
            if (parse_status == predex::core::oms::kalshi::PrivateWsParseStatus::kInvalidJson) {
                continue;
            }
            if (parse_status ==
                predex::core::oms::kalshi::PrivateWsParseStatus::kTooManyEvents) {
                set_error("OMS private websocket parser exceeded per-message event cap");
                running.store(false, std::memory_order_release);
                continue;
            }
            for (auto& event : parsed_events) {
                if (!oms_ws_update_queue->try_push(std::move(event))) {
                    set_error("OMS transport update queue backpressured from private websocket");
                    running.store(false, std::memory_order_release);
                    break;
                }
            }
        }
        oms_ws_session.close();
    }

    [[nodiscard]] bool App::Runtime::reconcile_open_orders_from_rest(bool is_startup) {
        std::optional<std::string> cursor;
        constexpr std::size_t kMaxOpenOrdersPages = 128;
        std::size_t pages_fetched = 0;

        while (pages_fetched < kMaxOpenOrdersPages) {
            const auto snapshot = oms_rest_client.fetch_open_orders(
                predex::websocket::kalshi::kOpenOrderFetchLimit,
                cursor);
            if (!snapshot.ok) {
                set_error("Failed to reconcile open orders from REST: " + snapshot.error);
                return false;
            }

            const auto now_ns = static_cast<predex::internal::TimestampNs>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count());

            for (const auto& order : snapshot.orders) {
                // fetch_open_orders returns only open orders, but guard against terminal status.
                if (order.status == "canceled" || order.status == "executed") {
                    continue;
                }

                if (is_startup) {
                    // Startup path: orders are from a prior session. Adopt them into OMS with
                    // synthetic tracking so the kill switch can reach them and fills are
                    // accounted for in session P&L.
                    const auto reg_it = registry_index_by_ticker_.find(order.ticker);
                    if (reg_it == registry_index_by_ticker_.end()) {
                        std::fprintf(stdout,
                            "[reconcile] orphaned order on unrecognized ticker=%s, "
                            "skipping: client_order_id=%s exchange_order_id=%s\n",
                            order.ticker.c_str(),
                            order.client_order_id.c_str(), order.order_id.c_str());
                        std::fflush(stdout);
                        continue;
                    }
                    const auto& reg_entry = market_registry_entries[reg_it->second];
                    const auto shard_id = static_cast<std::uint16_t>(
                        reg_entry.affinity_key_ % config.pipeline.shard_count);
                    predex::core::oms::kalshi::OrderState state{
                        .origin = predex::core::oms::kalshi::IntentOrigin{
                            .shard_id = shard_id,
                            .affinity_key = reg_entry.affinity_key_,
                            .event_id = reg_entry.event_id_,
                            .market_id = reg_entry.market_id_,
                        },
                        .oms_request_id = 0,
                        .status = predex::core::oms::kalshi::OmsOrderStatus::kLive,
                        .client_order_id = order.client_order_id,
                        .exchange_order_id = order.order_id,
                        .original_qty_lots = parse_count_fp_to_lots(order.initial_count_fp),
                        .live_qty_lots = parse_count_fp_to_lots(order.remaining_count_fp),
                        .cum_fill_qty_lots = parse_count_fp_to_lots(order.fill_count_fp),
                        .last_update_ts_ns = now_ns,
                    };
                    // Kalshi reports two orthogonal fields: `action` = {buy,sell} (direction)
                    // and `side` = {yes,no} (binary-contract outcome). The prior code collapsed
                    // them into a single Side, which mistranslated sell-YES as sell-NO on
                    // reconciliation — the exact bug the new Outcome field exists to prevent.
                    const auto side = order.action == "buy"  ? predex::internal::Side::kBuy
                                    : order.action == "sell" ? predex::internal::Side::kSell
                                                             : predex::internal::Side::kUnknown;
                    const auto outcome =
                        order.side == "yes" ? predex::core::oms::kalshi::Outcome::kYes
                      : order.side == "no"  ? predex::core::oms::kalshi::Outcome::kNo
                                            : predex::core::oms::kalshi::Outcome::kUnknown;
                    if (side == predex::internal::Side::kUnknown ||
                        outcome == predex::core::oms::kalshi::Outcome::kUnknown) {
                        std::fprintf(stderr,
                            "[reconcile] skipping orphaned order with unparseable "
                            "action/side: ticker=%s client_order_id=%s "
                            "exchange_order_id=%s action=%s side=%s\n",
                            order.ticker.c_str(), order.client_order_id.c_str(),
                            order.order_id.c_str(), order.action.c_str(),
                            order.side.c_str());
                        std::fflush(stderr);
                        continue;
                    }
                    const auto assigned_id =
                        oms->seed_orphaned_order(std::move(state), side, outcome);
                    std::fprintf(stdout,
                        "[reconcile] adopted orphaned order: ticker=%s "
                        "client_order_id=%s exchange_order_id=%s "
                        "assigned_oms_request_id=%llu\n",
                        order.ticker.c_str(),
                        order.client_order_id.c_str(), order.order_id.c_str(),
                        static_cast<unsigned long long>(assigned_id));
                    std::fflush(stdout);
                    continue;
                }

                // Reconnect path: orders may belong to the current session and are already tracked
                // in the OMS by client_order_id. Push a lifecycle event so the OMS can reconcile.
                predex::core::oms::kalshi::OrderLifecycleEvent event{
                    .origin = {},
                    .oms_request_id = 0,
                    .kind = predex::core::oms::kalshi::OrderLifecycleEventKind::kAck,
                    .status = predex::core::oms::kalshi::OmsOrderStatus::kLive,
                    .client_order_id = order.client_order_id,
                    .exchange_order_id = order.order_id,
                    .recv_ts_ns = now_ns,
                    .data = predex::core::oms::kalshi::OrderAck{
                        .accepted_qty_lots = parse_count_fp_to_lots(order.initial_count_fp),
                    },
                };
                // Post-startup reconciliation runs on the REST thread (the sole producer
                // of oms_rest_update_queue), which preserves the SPSC invariant now that
                // the RestClient is only touched from one thread.
                if (!oms_rest_update_queue->try_push(std::move(event))) {
                    set_error("OMS transport update queue backpressured during reconciliation");
                    return false;
                }
            }

            ++pages_fetched;
            if (!snapshot.next_cursor.has_value() || snapshot.next_cursor->empty() ||
                (cursor.has_value() && *snapshot.next_cursor == *cursor)) {
                return true;
            }
            cursor = snapshot.next_cursor;
        }

        set_error("Open-orders reconciliation exceeded pagination safety limit");
        return false;
    }

    void App::Runtime::router_loop(const std::stop_token& stop_token) const {
        std::uint32_t idle_iters = 0;
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            const auto routed = router->pump(config.pipeline.io_to_router_capacity);
            if (routed > 0U) {
                idle_iters = 0;
                continue;
            }

            ++idle_iters;
            if (idle_iters <= config.pipeline.idle_policy.spin_iters_router) {
                continue;
            }

            if (config.pipeline.idle_policy.yield_every > 0U &&
                (idle_iters % config.pipeline.idle_policy.yield_every) == 0U) {
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::shard_loop(std::size_t shard_index, const std::stop_token& stop_token) const {
        const auto& shard = shards[shard_index];
        std::uint32_t idle_iters = 0;
        while(running.load(std::memory_order_acquire) && !stop_token.stop_requested()){
            const auto processed = shard->pump(config.pipeline.shard_input_capacity);
            if(processed > 0U){
                idle_iters = 0;
                continue;
            }

            ++idle_iters;
            if (idle_iters <= config.pipeline.idle_policy.spin_iters_shard) {
                continue;
            }

            if (config.pipeline.idle_policy.yield_every > 0U &&
                (idle_iters % config.pipeline.idle_policy.yield_every) == 0U) {
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::oms_loop(const std::stop_token& stop_token) {
        std::uint32_t idle_iters = 0;
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            const auto result = oms->pump(
                config.pipeline.shard_input_capacity,
                config.pipeline.shard_input_capacity);
            if (result.code == predex::core::oms::kalshi::OmsProcessCode::kError) {
                set_error("OMS encountered an unrecoverable processing error");
                running.store(false, std::memory_order_release);
                break;
            }
            if (result.code == predex::core::oms::kalshi::OmsProcessCode::kShardBackpressure) {
                set_error("OMS backpressured while routing decisions/lifecycle updates to shards");
                running.store(false, std::memory_order_release);
                break;
            }
            if (result.code == predex::core::oms::kalshi::OmsProcessCode::kTransportBackpressure) {
                set_error("OMS transport backpressured while enqueueing outbound commands");
                running.store(false, std::memory_order_release);
                break;
            }
            if (result.code != predex::core::oms::kalshi::OmsProcessCode::kIdle) {
                idle_iters = 0;
                continue;
            }
            if (result.code == predex::core::oms::kalshi::OmsProcessCode::kIdle) {
                ++idle_iters;
                if (idle_iters <= config.pipeline.idle_policy.spin_iters_oms) {
                    continue;
                }

                if (config.pipeline.idle_policy.yield_every > 0U &&
                    (idle_iters % config.pipeline.idle_policy.yield_every) == 0U) {
                    std::this_thread::yield();
                }
            }
        }
        // Tail drain: cancel all live orders (enqueue cancel cmds), then drain remaining
        // intents — halted_ rejects them so no new orders are submitted.
        static_cast<void>(oms->cancel_all_live_orders());
        predex::core::oms::kalshi::OmsProcessCode tail_code{};
        do {
            const auto result = oms->pump(
                config.pipeline.shard_input_capacity,
                config.pipeline.shard_input_capacity);
            tail_code = result.code;
        } while (tail_code != predex::core::oms::kalshi::OmsProcessCode::kIdle &&
                 tail_code != predex::core::oms::kalshi::OmsProcessCode::kError);
    }

    void App::Runtime::logger_loop(const std::stop_token& stop_token) const {
        std::uint32_t idle_iters = 0;
        while(running.load(std::memory_order_acquire) && !stop_token.stop_requested()){
            const auto logged = tape_logger->pump(config.pipeline.router_to_logger_capacity);
            if(logged > 0U){
                idle_iters = 0;
                continue;
            }

            ++idle_iters;
            if (idle_iters <= config.pipeline.idle_policy.spin_iters_logger) {
                continue;
            }

            if (idle_iters >= config.pipeline.idle_policy.sleep_after_idle_iters) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(config.pipeline.idle_policy.sleep_micros));
                continue;
            }

            if (config.pipeline.idle_policy.yield_every > 0U &&
                (idle_iters % config.pipeline.idle_policy.yield_every) == 0U) {
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::audit_loop(const std::stop_token& stop_token) const {
        std::uint32_t idle_iters = 0;
        while(running.load(std::memory_order_acquire) && !stop_token.stop_requested()){
            const auto logged = audit_logger->pump(config.pipeline.router_to_logger_capacity);
            if(logged > 0U){
                idle_iters = 0;
                continue;
            }
            ++idle_iters;
            if (idle_iters <= config.pipeline.idle_policy.spin_iters_audit) {
                continue;
            }

            if (idle_iters >= config.pipeline.idle_policy.sleep_after_idle_iters) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(config.pipeline.idle_policy.sleep_micros));
                continue;
            }

            if (config.pipeline.idle_policy.yield_every > 0U &&
                (idle_iters % config.pipeline.idle_policy.yield_every) == 0U) {
                std::this_thread::yield();
            }
        }
    }

    void App::Runtime::run(){
        if(!running.load(std::memory_order_acquire)){
            set_error("Attempted to run App that is not started");
            return;
        }
        constexpr std::int64_t kHealthDumpIntervalMs = 30'000;
        std::int64_t ms_since_last_dump = kHealthDumpIntervalMs; // dump immediately on start
        while(running.load(std::memory_order_acquire)){
            std::this_thread::sleep_for(std::chrono::milliseconds(kDefaultSleepMs));
            ms_since_last_dump += kDefaultSleepMs;
            if (ms_since_last_dump >= kHealthDumpIntervalMs) {
                ms_since_last_dump = 0;
                print_health_status();
            }
        }
    }

    void App::Runtime::print_health_status() const noexcept {
        const auto now_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        char time_buf[32];
        std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S UTC",
                      std::gmtime(&now_t));

        const bool halted = oms && oms->is_halted();
        const std::int64_t net_ticks = oms ? oms->session_net_ticks() : 0;
        const std::size_t live_orders = oms ? oms->live_order_count() : 0;
        const std::uint64_t intents = oms ? oms->processed_intent_count() : 0;
        const std::uint64_t transport_updates = oms ? oms->processed_transport_update_count() : 0;
        const std::uint64_t rejected = oms ? oms->rejected_intent_count() : 0;
        const std::uint64_t unknown_fill_side = oms ? oms->unknown_fill_side_count() : 0;

        std::size_t desynced_events = 0;
        for (const auto& event_store : event_stores) {
            desynced_events += event_store.desynced_event_count();
        }

        const auto& telem = router ? router->telemetry()
                                   : predex::core::routing::kalshi::RouterTelemetry{};

        std::fprintf(stdout,
            "[%s] STATUS | halted=%s | pnl_ticks=%+lld"
            " | live_orders=%zu | intents=%llu | rejected=%llu | transport_updates=%llu"
            " | unknown_fill_side=%llu"
            " | router_frames=%zu | router_drop_bp=%zu | router_drop_lifecycle=%zu"
            " | router_drop_invalid=%zu | desynced_events=%zu\n",
            time_buf,
            halted ? "true" : "false",
            static_cast<long long>(net_ticks),
            live_orders,
            static_cast<unsigned long long>(intents),
            static_cast<unsigned long long>(rejected),
            static_cast<unsigned long long>(transport_updates),
            static_cast<unsigned long long>(unknown_fill_side),
            telem.processed_frames_,
            telem.dropped_backpressure_,
            telem.dropped_unknown_ticker_lifecycle_,
            telem.dropped_invalid_,
            desynced_events);
        std::fflush(stdout);
    }

    void App::Runtime::stop(){
        // Hard halt: blocks new submission and schedules cancel-all on the OMS thread.
        if (oms) {
            oms->request_hard_halt();
        }
        running.store(false, std::memory_order_release);

        // Stop inbound data flow.
        if(io_thread.joinable()){
            io_thread.request_stop();
            io_thread.join();
        }

        // Stop and join router, then drain remaining frames into shard queues.
        if(router_thread.joinable()){
            router_thread.request_stop();
            router_thread.join();
        }
        if (router) {
            while (router->pump(config.pipeline.io_to_router_capacity) > 0) {}
        }

        // Stop and join shards, then drain remaining frames from shard input queues.
        for(auto& thread : shard_threads){
            if(thread.joinable()){
                thread.request_stop();
                thread.join();
            }
        }
        shard_threads.clear();
        for (const auto& shard : shards) {
            while (shard->pump(config.pipeline.shard_input_capacity) > 0) {}
        }

        // Stop OMS thread: its tail drain calls cancel_all_live_orders + pumps remaining.
        if(oms_thread.joinable()){
            oms_thread.request_stop();
            oms_thread.join();
        }

        // Join the REST thread BEFORE the main thread touches oms_rest_client. The
        // RestClient owns a persistent TLS stream that is not safe to share across
        // threads; prior to this join, oms_rest_loop may still be mid-request.
        if (oms_rest_thread.joinable()) {
            oms_rest_thread.request_stop();
            oms_rest_thread.join();
        }

        // After OMS tail drain + REST thread join, flush any cancel commands directly
        // via the now-idle RestClient.
        {
            predex::core::oms::kalshi::CancelOrderCmd cancel_cmd{};
            while (oms_cancel_queue != nullptr && oms_cancel_queue->try_pop(cancel_cmd)) {
                if (config.oms_transport.enabled) {
                    constexpr std::uint32_t kCancelRetryCount = 3;
                    bool canceled = false;
                    std::string last_cancel_error;
                    for (std::uint32_t attempt = 0; attempt < kCancelRetryCount; ++attempt) {
                        const auto cancel_result = oms_rest_client.cancel_order(cancel_cmd);
                        if (cancel_result.ok) {
                            canceled = true;
                            break;
                        }
                        last_cancel_error = cancel_result.error;
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                    if (!canceled) {
                        std::fprintf(stderr,
                                     "[stop] failed to cancel order (oms_request_id=%llu, client_order_id=%s): %s\n",
                                     static_cast<unsigned long long>(cancel_cmd.oms_request_id),
                                     cancel_cmd.client_order_id.c_str(),
                                     last_cancel_error.c_str());
                    }
                }
            }
        }

        if (oms_private_ws_thread.joinable()) {
            oms_private_ws_thread.request_stop();
            oms_private_ws_thread.join();
        }

        // Drain tape logger and audit queues before joining those threads.
        if (tape_logger) {
            while (tape_logger->pump(config.pipeline.router_to_logger_capacity) > 0) {}
        }
        if(logger_thread.joinable()){
            logger_thread.request_stop();
            logger_thread.join();
        }

        if (audit_logger) {
            while (audit_logger->pump(config.pipeline.router_to_logger_capacity) > 0) {}
        }
        if(audit_thread.joinable()){
            audit_thread.request_stop();
            audit_thread.join();
        }
    }

    App::App(AppConfig config)
        : runtime_(std::make_unique<Runtime>(std::move(config))) {}

    App::~App() = default;
    bool App::start() {
        return runtime_->start(); 
    }


    void App::run() {
        runtime_->run();
    }

    void App::stop() {
        runtime_->stop();
    }

    std::string App::last_error() const noexcept {
        std::scoped_lock lock(runtime_->error_mutex);
        return runtime_->last_error;
    }
}
