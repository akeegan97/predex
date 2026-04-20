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
#include <algorithm>
#include <charconv>
#include <chrono>
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
        using AuditLogger = predex::core::audit::AuditLogger;

        explicit Runtime(AppConfig config_in);

        AppConfig config;
        bool init_ok{true};
        mutable std::mutex error_mutex;
        std::string last_error;
        std::atomic<bool> running{false};
        std::vector<core::routing::kalshi::MarketRegistryEntry> market_registry_entries;
        std::unordered_map<internal::MarketId, std::string> market_ticker_by_id_;
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
            const std::vector<predex::core::routing::kalshi::MarketRegistryEntry>& entries,
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
            grouped_events.reserve(entries.size());

            for (const auto& entry : entries) {
                if (entry.market_id_ == 0 || entry.event_id_ == 0) {
                    error_out = "market route entries must define non-zero market_id and event_id";
                    return false;
                }
                if (entry.topology_kind_ == predex::internal::EventTopologyKind::kUnknown) {
                    error_out = "market route entries must define a non-unknown topology_kind";
                    return false;
                }
                auto& accumulator = grouped_events[entry.event_id_];
                if (!accumulator.affinity_initialized) {
                    accumulator.affinity_key = entry.affinity_key_;
                    accumulator.affinity_initialized = true;
                    accumulator.topology_kind = entry.topology_kind_;
                } else if (accumulator.affinity_key != entry.affinity_key_) {
                    error_out = "event " + std::to_string(entry.event_id_) +
                        " has inconsistent affinity_key values in config";
                    return false;
                } else if (accumulator.topology_kind != entry.topology_kind_) {
                    error_out = "event " + std::to_string(entry.event_id_) +
                        " has inconsistent topology_kind values in config";
                    return false;
                }
                const auto duplicate_market = std::find_if(
                    accumulator.markets.begin(),
                    accumulator.markets.end(),
                    [&entry](const core::shards::kalshi::EventMarketDefinition& market) {
                        return market.market_id == entry.market_id_;
                    });
                if (duplicate_market != accumulator.markets.end()) {
                    error_out = "event " + std::to_string(entry.event_id_) +
                        " contains duplicate market_id " + std::to_string(entry.market_id_) +
                        " in config";
                    return false;
                }
                accumulator.markets.push_back(core::shards::kalshi::EventMarketDefinition{
                    .market_id = entry.market_id_,
                    .strike_key = entry.strike_key_,
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
        std::unique_ptr<FrameQueue> recycle_queue;
        std::vector<std::unique_ptr<FrameQueue>> shard_input_queues;
        std::vector<std::unique_ptr<FrameQueue>> shard_to_logger_queues;
        std::vector<std::unique_ptr<OmsIntentQueue>> shard_to_oms_intent_queues;
        std::vector<std::unique_ptr<OmsDecisionQueue>> oms_to_shard_decision_queues;
        std::vector<std::unique_ptr<OmsLifecycleQueue>> oms_to_shard_lifecycle_queues;
        std::vector<std::unique_ptr<AuditQueue>> shard_audit_queues;
        std::unique_ptr<SubmitOrderQueue> oms_submit_queue;
        std::unique_ptr<CancelOrderQueue> oms_cancel_queue;
        std::unique_ptr<ModifyOrderQueue> oms_modify_queue;
        std::unique_ptr<OmsLifecycleQueue> oms_transport_update_queue;
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
        [[nodiscard]] bool reconcile_open_orders_from_rest();
        void router_loop(const std::stop_token& stop_token) const;
        void shard_loop(std::size_t shard_index, const std::stop_token& stop_token) const;
        void oms_loop(const std::stop_token& stop_token);
        void logger_loop(const std::stop_token& stop_token) const;
        void audit_loop(const std::stop_token& stop_token) const;
        void set_error(std::string message);
        std::string_view last_error_view() const noexcept;
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
        for (const auto& entry : market_registry_entries) {
            market_ticker_by_id_[entry.market_id_] = entry.ticker_;
        }

        io_to_router_queue =
            std::make_unique<FrameQueue>(config.pipeline.io_to_router_capacity);
        router_to_logger_queue =
            std::make_unique<FrameQueue>(config.pipeline.router_to_logger_capacity);
        recycle_queue =
            std::make_unique<FrameQueue>(config.pipeline.frame_pool_capacity);

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
            event_stores.emplace_back();
        }

        oms_submit_queue =
            std::make_unique<SubmitOrderQueue>(config.pipeline.shard_input_capacity);
        oms_cancel_queue =
            std::make_unique<CancelOrderQueue>(config.pipeline.shard_input_capacity);
        oms_modify_queue =
            std::make_unique<ModifyOrderQueue>(config.pipeline.shard_input_capacity);
        oms_transport_update_queue =
            std::make_unique<OmsLifecycleQueue>(config.pipeline.shard_input_capacity);
        oms_audit_queue =
            std::make_unique<AuditQueue>(config.pipeline.shard_input_capacity);

        std::vector<std::vector<core::shards::kalshi::EventDefinition>> event_definitions_by_shard;
        std::string event_definition_error;
        if (!build_event_definitions_by_shard(
                market_registry_entries,
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

        shard_dispatch =
            std::make_unique<core::routing::kalshi::ShardDispatch>(shard_input_queue_ptrs);

        router = std::make_unique<core::routing::kalshi::Router>(
            *io_to_router_queue,
            frame_pool,
            market_registry,
            *shard_dispatch,
            *router_to_logger_queue);

        io_writer = std::make_unique<core::ingest::kalshi::IOWriter>(
            frame_pool,
            *io_to_router_queue,
            *recycle_queue);

        tape_logger = std::make_unique<core::tape::kalshi::Logger>(
            logger_input_queue_ptrs,
            frame_pool,
            *recycle_queue,
            config.tape.output_path);
        audit_logger = std::make_unique<AuditLogger>(
            audit_input_queue_ptrs,
            config.audit.output_path);

        oms = std::make_unique<Oms>(
            shard_to_oms_intent_queue_ptrs,
            oms_to_shard_decision_queue_ptrs,
            oms_to_shard_lifecycle_queue_ptrs,
            predex::core::oms::kalshi::OmsTransportQueues{
                .submit_queue = oms_submit_queue.get(),
                .cancel_queue = oms_cancel_queue.get(),
                .modify_queue = oms_modify_queue.get(),
                .inbound_update_queue = oms_transport_update_queue.get(),
            },
            predex::core::oms::kalshi::GlobalRiskManager{},
            oms_audit_queue.get());

        for (std::size_t i = 0; i < config.pipeline.shard_count; ++i) {
            shards.push_back(std::make_unique<Shard>(
                *shard_input_queues[i],
                frame_pool,
                *shard_to_logger_queues[i],
                event_stores[i],
                ShardPipeline{
                    static_cast<std::uint16_t>(i),
                    LocalRiskManager{},
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

        if (config.oms_transport.enabled) {
            if (!oms_ws_session.connect()) {
                oms_ws_session.close();
                ws_session.close();
                set_error(oms_ws_session.last_error());
                return false;
            }

            for (const auto& channel : config.oms_transport.private_ws_channels) {
                if (!oms_ws_session.subscribe(channel)) {
                    oms_ws_session.close();
                    ws_session.close();
                    set_error(oms_ws_session.last_error());
                    return false;
                }
            }
            if (!reconcile_open_orders_from_rest()) {
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
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            const auto recv_result = ws_session.recv_text(std::chrono::milliseconds{50});

            if (recv_result.status == websocket::RecvStatus::kTimeout) {
                continue;
            }

            if (recv_result.status == websocket::RecvStatus::kClosed) {
                if (running.load(std::memory_order_acquire)) {
                    set_error("Websocket connection closed unexpectedly");
                    running.store(false, std::memory_order_release);
                }
                break;
            }

            if (recv_result.status == websocket::RecvStatus::kError) {
                set_error(ws_session.last_error());
                running.store(false, std::memory_order_release);
                break;
            }

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
        while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
            bool processed = false;

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
                if (!oms_transport_update_queue->try_push(std::move(event))) {
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
                        if (!oms_transport_update_queue->try_push(std::move(event))) {
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
                    if (!oms_transport_update_queue->try_push(std::move(event))) {
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
                        if (!oms_transport_update_queue->try_push(std::move(event))) {
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
                    if (!oms_transport_update_queue->try_push(std::move(event))) {
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
                if (!reconcile_open_orders_from_rest()) {
                    running.store(false, std::memory_order_release);
                    break;
                }
                reconnect_attempts = 0;
                continue;
            }
            if (recv_result.status == websocket::RecvStatus::kError) {
                oms_ws_session.close();
                continue;
            }

            reconnect_attempts = 0;
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
                if (!oms_transport_update_queue->try_push(std::move(event))) {
                    set_error("OMS transport update queue backpressured from private websocket");
                    running.store(false, std::memory_order_release);
                    break;
                }
            }
        }
        oms_ws_session.close();
    }

    [[nodiscard]] bool App::Runtime::reconcile_open_orders_from_rest() {
        const auto snapshot = oms_rest_client.fetch_open_orders();
        if (!snapshot.ok) {
            set_error("Failed to reconcile open orders from REST: " + snapshot.error);
            return false;
        }

        for (const auto& order : snapshot.orders) {
            const auto now_ns = static_cast<predex::internal::TimestampNs>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count());
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
            if (order.status == "canceled") {
                event.kind = predex::core::oms::kalshi::OrderLifecycleEventKind::kCanceled;
                event.status = predex::core::oms::kalshi::OmsOrderStatus::kCanceled;
            } else if (order.status == "executed") {
                event.kind = predex::core::oms::kalshi::OrderLifecycleEventKind::kFill;
                event.status = predex::core::oms::kalshi::OmsOrderStatus::kFilled;
            }
            if (!oms_transport_update_queue->try_push(std::move(event))) {
                set_error("OMS transport update queue backpressured during reconciliation");
                return false;
            }
        }

        return true;
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
        while(running.load(std::memory_order_acquire)){
            std::this_thread::sleep_for(std::chrono::milliseconds(kDefaultSleepMs));
        }
        // stop();
    }

    void App::Runtime::stop(){
        running.store(false, std::memory_order_release);
        if(io_thread.joinable()){
            io_thread.request_stop();
            io_thread.join();
        }
        if(router_thread.joinable()){
            router_thread.request_stop();
            router_thread.join();
        }
        for(auto& thread : shard_threads){
            if(thread.joinable()){
                thread.request_stop();
                thread.join();
            }
        }
        shard_threads.clear();
        if(oms_thread.joinable()){
            oms_thread.request_stop();
            oms_thread.join();
        }
        if (oms_rest_thread.joinable()) {
            oms_rest_thread.request_stop();
            oms_rest_thread.join();
        }
        if (oms_private_ws_thread.joinable()) {
            oms_private_ws_thread.request_stop();
            oms_private_ws_thread.join();
        }
        if(logger_thread.joinable()){
            logger_thread.request_stop();
            logger_thread.join();
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
