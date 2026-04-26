#include "predex/app.hpp"
#include "predex/audit/audit_logger.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/ingest/io_writer.hpp"
#include "predex/oms/oms.hpp"
#include "predex/oms/transport/kalshi_private_ws_adapter.hpp"
#include "predex/oms/transport/kalshi_rest_adapter.hpp"
#include "predex/oms/transport/persistent_http_session.hpp"
#include "predex/oms/transport/private_ws_worker.hpp"
#include "predex/oms/transport/rest_worker.hpp"
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
#include <limits>
#include <string>
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

        void format_utc_timestamp(char* buffer, std::size_t buffer_size) noexcept {
            const std::time_t now = std::time(nullptr);
            std::tm utc_tm{};
#if defined(_WIN32)
            gmtime_s(&utc_tm, &now);
#else
            gmtime_r(&now, &utc_tm);
#endif
            if (std::strftime(buffer, buffer_size, "%Y-%m-%d %H:%M:%S UTC", &utc_tm) == 0U) {
                std::snprintf(buffer, buffer_size, "unknown-time");
            }
        }

        void log_public_ws_event(const char* phase,
                                 std::uint64_t generation,
                                 const char* detail = nullptr) noexcept {
            char time_buf[32];
            format_utc_timestamp(time_buf, sizeof(time_buf));
            std::fprintf(stdout,
                         "[%s] WS | scope=public | generation=%llu | phase=%s%s%s\n",
                         time_buf,
                         static_cast<unsigned long long>(generation),
                         phase,
                         detail == nullptr ? "" : " | detail=",
                         detail == nullptr ? "" : detail);
            std::fflush(stdout);
        }

        [[nodiscard]] internal::QtyLots parse_count_fp_to_lots(std::string_view value) {
            internal::QtyLots qty_lots = 0;
            if (!internal::parse_non_negative_quantity_fp(value, qty_lots)) {
                return 0;
            }
            return qty_lots;
        }

        [[nodiscard]] std::string rest_trace_output_path(std::string_view base_path,
                                                         std::size_t worker_index,
                                                         std::size_t worker_count) {
            if (base_path.empty() || worker_count <= 1) {
                return std::string{base_path};
            }

            const auto dot_pos = base_path.rfind('.');
            if (dot_pos == std::string_view::npos) {
                return std::string{base_path} + ".worker" + std::to_string(worker_index);
            }

            return std::string{base_path.substr(0, dot_pos)} +
                ".worker" + std::to_string(worker_index) +
                std::string{base_path.substr(dot_pos)};
        }

        [[nodiscard]] bool parse_non_negative_dollars_to_ticks(
            std::string_view value,
            internal::PriceTicks& out_ticks) {
            constexpr std::uint64_t kDollarToTicksScale =
                static_cast<std::uint64_t>(internal::kPriceTicksPerDollar);
            constexpr auto kI64MaxAsU64 =
                static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());

            if (value.empty() || value.front() == '-' || value.front() == '+') {
                return false;
            }
            const std::size_t dot_pos = value.find('.');
            const std::string_view int_part =
                dot_pos == std::string_view::npos ? value : value.substr(0, dot_pos);
            const std::string_view frac_part =
                dot_pos == std::string_view::npos ? std::string_view{} : value.substr(dot_pos + 1);
            if (int_part.empty()) {
                return false;
            }
            for (char digit_char : int_part) {
                if (digit_char < '0' || digit_char > '9') {
                    return false;
                }
            }
            for (char frac_char : frac_part) {
                if (frac_char < '0' || frac_char > '9') {
                    return false;
                }
            }

            std::uint64_t dollars = 0;
            const auto [ptr, ec] =
                std::from_chars(int_part.data(), int_part.data() + int_part.size(), dollars);
            if (ec != std::errc{} || ptr != int_part.data() + int_part.size()) {
                return false;
            }

            std::uint64_t subcent_units = 0;
            const std::size_t digits_to_take =
                std::min<std::size_t>(frac_part.size(), internal::kPriceDecimalPlaces);
            for (std::size_t index = 0; index < digits_to_take; ++index) {
                subcent_units = subcent_units * 10U +
                    static_cast<std::uint64_t>(frac_part[index] - '0');
            }
            for (std::size_t index = digits_to_take; index < internal::kPriceDecimalPlaces;
                 ++index) {
                subcent_units *= 10U;
            }
            if (frac_part.size() > internal::kPriceDecimalPlaces &&
                frac_part[internal::kPriceDecimalPlaces] >= '5') {
                ++subcent_units;
                if (subcent_units == kDollarToTicksScale) {
                    subcent_units = 0U;
                    ++dollars;
                }
            }

            if (dollars > (kI64MaxAsU64 - subcent_units) / kDollarToTicksScale) {
                return false;
            }

            out_ticks = static_cast<internal::PriceTicks>(
                dollars * kDollarToTicksScale + subcent_units);
            return true;
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
        using OmsIntentQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsSubmission>;
        using OmsDecisionQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsToShardDecision>;
        using OmsLifecycleQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsToShardLifecycleEvent>;
        using AuditQueue =
            predex::utils::SPSCQueue<predex::core::audit::AuditEvent>;
        using OmsCommandQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsToKalshiCommand>;
        using KalshiEventQueue =
            predex::utils::SPSCQueue<predex::core::oms::kalshi::KalshiToOmsEvent>;
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
        std::vector<std::unique_ptr<predex::core::oms::kalshi::transport::RestWorker>>
            oms_rest_workers;
        std::unique_ptr<predex::core::oms::kalshi::transport::PrivateWsWorker> oms_private_ws_worker;

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
        std::unique_ptr<AuditQueue> router_audit_queue;
        std::vector<std::unique_ptr<OmsCommandQueue>> oms_command_queues;
        std::vector<std::unique_ptr<KalshiEventQueue>> oms_rest_event_queues;
        std::unique_ptr<KalshiEventQueue> oms_ws_event_queue;
        std::unique_ptr<AuditQueue> oms_audit_queue;

        core::routing::kalshi::MarketRegistry market_registry;
        std::vector<FrameQueue*> shard_input_queue_ptrs;
        std::vector<FrameQueue*> logger_input_queue_ptrs;
        std::vector<OmsIntentQueue*> shard_to_oms_intent_queue_ptrs;
        std::vector<OmsDecisionQueue*> oms_to_shard_decision_queue_ptrs;
        std::vector<OmsLifecycleQueue*> oms_to_shard_lifecycle_queue_ptrs;
        std::vector<OmsCommandQueue*> oms_command_queue_ptrs;
        std::vector<KalshiEventQueue*> oms_rest_event_queue_ptrs;
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
        std::vector<std::jthread> oms_rest_threads;
        std::jthread oms_private_ws_thread;
        std::jthread logger_thread;
        std::jthread audit_thread;
        std::atomic<std::uint64_t> public_ws_generation_{0};

        
        

        bool start();
        void run();
        void stop();

        void io_loop(const std::stop_token& stop_token);
        void oms_rest_loop(std::size_t worker_index, const std::stop_token& stop_token);
        void oms_private_ws_loop(const std::stop_token& stop_token);
        [[nodiscard]] bool reconcile_open_orders_from_rest(bool is_startup);
        [[nodiscard]] predex::core::oms::kalshi::transport::RestWorkerConfig
        build_oms_rest_worker_config(std::size_t worker_index,
                         std::size_t worker_count) const;
        [[nodiscard]] std::optional<
            predex::core::oms::kalshi::transport::RestWorkerConfig::ReconcileOrderSeed>
        resolve_oms_reconcile_seed(std::string_view ticker) const;
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

        const std::size_t rest_worker_count =
            std::max<std::size_t>(1, config.oms_transport.rest_worker_count);
        oms_command_queues.reserve(rest_worker_count);
        oms_rest_event_queues.reserve(rest_worker_count);
        oms_command_queue_ptrs.reserve(rest_worker_count);
        oms_rest_event_queue_ptrs.reserve(rest_worker_count);
        oms_rest_workers.reserve(rest_worker_count);

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

        for (std::size_t worker_index = 0; worker_index < rest_worker_count; ++worker_index) {
            oms_command_queues.push_back(
                std::make_unique<OmsCommandQueue>(config.pipeline.shard_input_capacity));
            oms_rest_event_queues.push_back(
                std::make_unique<KalshiEventQueue>(config.pipeline.shard_input_capacity));
            oms_command_queue_ptrs.push_back(oms_command_queues.back().get());
            oms_rest_event_queue_ptrs.push_back(oms_rest_event_queues.back().get());
        }
        oms_ws_event_queue =
            std::make_unique<KalshiEventQueue>(config.pipeline.shard_input_capacity);
        oms_audit_queue =
            std::make_unique<AuditQueue>(config.pipeline.shard_input_capacity);
        router_audit_queue =
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
        audit_input_queue_ptrs.push_back(router_audit_queue.get());

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
            router_audit_queue.get(),
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

        for (std::size_t worker_index = 0; worker_index < rest_worker_count; ++worker_index) {
            auto worker_config = build_oms_rest_worker_config(worker_index, rest_worker_count);
            oms_rest_workers.push_back(
                std::make_unique<predex::core::oms::kalshi::transport::RestWorker>(
                    predex::core::oms::kalshi::transport::RestWorkerQueues{
                        .command_queue = oms_command_queues[worker_index].get(),
                        .event_queue = oms_rest_event_queues[worker_index].get(),
                    },
                    predex::core::oms::kalshi::transport::KalshiRestAdapter{
                        predex::core::oms::kalshi::transport::PersistentHttpSession{
                            websocket::kalshi::AuthSigner{auth_signer},
                            config.oms_transport.rest_endpoint}},
                    std::move(worker_config)));
        }
        oms_private_ws_worker =
            std::make_unique<predex::core::oms::kalshi::transport::PrivateWsWorker>(
                predex::core::oms::kalshi::transport::PrivateWsWorkerQueues{
                    .event_queue = oms_ws_event_queue.get(),
                },
                predex::core::oms::kalshi::transport::PrivateWsWorkerConfig{
                    .channels = {"orders"},
                    .recv_timeout = std::chrono::milliseconds{50},
                },
                predex::core::oms::kalshi::transport::KalshiPrivateWsAdapter{
                    websocket::kalshi::WsAdapter{
                        websocket::kalshi::AuthSigner{auth_signer},
                        config.oms_transport.private_ws_endpoint}});

        oms = std::make_unique<Oms>(
            shard_to_oms_intent_queue_ptrs,
            oms_to_shard_decision_queue_ptrs,
            oms_to_shard_lifecycle_queue_ptrs,
            predex::core::oms::kalshi::ExecutionTransportQueues{
                .command_queues = oms_command_queue_ptrs,
                .rest_event_queues = oms_rest_event_queue_ptrs,
                .ws_event_queue = oms_ws_event_queue.get(),
            },
            global_risk_limits,
            oms_audit_queue.get(),
            [this](internal::MarketId market_id) -> std::optional<std::string> {
                const auto it = market_ticker_by_id_.find(market_id);
                if (it == market_ticker_by_id_.end()) {
                    return std::nullopt;
                }
                return it->second;
            });

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

    predex::core::oms::kalshi::transport::RestWorkerConfig
    App::Runtime::build_oms_rest_worker_config(std::size_t worker_index,
                                               std::size_t worker_count) const {
        using RestWorkerConfig = predex::core::oms::kalshi::transport::RestWorkerConfig;

        return RestWorkerConfig{
            .ticker_seed_resolver =
                [this](std::string_view ticker)
                    -> std::optional<RestWorkerConfig::ReconcileOrderSeed> {
                    return resolve_oms_reconcile_seed(ticker);
                },
            .trace_output_path = rest_trace_output_path(
                RestWorkerConfig{}.trace_output_path, worker_index, worker_count),
        };
    }

    std::optional<predex::core::oms::kalshi::transport::RestWorkerConfig::ReconcileOrderSeed>
    App::Runtime::resolve_oms_reconcile_seed(std::string_view ticker) const {
        using ReconcileOrderSeed =
            predex::core::oms::kalshi::transport::RestWorkerConfig::ReconcileOrderSeed;

        const auto reg_it = registry_index_by_ticker_.find(std::string{ticker});
        if (reg_it == registry_index_by_ticker_.end()) {
            return std::nullopt;
        }

        const auto& reg_entry = market_registry_entries[reg_it->second];
        const auto shard_id = static_cast<std::uint16_t>(
            reg_entry.affinity_key_ % config.pipeline.shard_count);

        return ReconcileOrderSeed{
            .context = predex::core::oms::kalshi::IntentContext{
                .shard_id = shard_id,
                .affinity_key = reg_entry.affinity_key_,
                .event_id = reg_entry.event_id_,
                .market_id = reg_entry.market_id_,
            },
            .exchange = predex::internal::ExchangeId::kKalshi,
            .side = predex::internal::Side::kUnknown,
            .outcome = predex::core::oms::kalshi::Outcome::kYes,
        };
    }

    bool App::Runtime::start(){
        if (!init_ok) {
            return false;
        }
        if(running.load(std::memory_order_acquire)){
            return true;
        }
        log_public_ws_event("connect_start", 0, config.kalshi_ws.endpoint.c_str());
        if(!ws_session.connect()){
            log_public_ws_event("connect_failed", 0, ws_session.last_error().c_str());
            ws_session.close();
            set_error(ws_session.last_error());
            return false;
        }
        log_public_ws_event("connect_ok", 0);

        for(const auto& channel : config.kalshi_ws.channels){
            if(!ws_session.subscribe(channel, config.kalshi_ws.market_tickers)){
                log_public_ws_event("subscribe_failed", 0, ws_session.last_error().c_str());
                ws_session.close();
                set_error(ws_session.last_error());
                return false;
            }
            log_public_ws_event("subscribe_ok", 0, channel.c_str());
        }
        for (const auto& channel : config.kalshi_ws.lifecycle_channels) {
            if (!ws_session.subscribe(channel, {})) {
                log_public_ws_event("subscribe_failed", 0, ws_session.last_error().c_str());
                ws_session.close();
                set_error(ws_session.last_error());
                return false;
            }
            log_public_ws_event("subscribe_ok", 0, channel.c_str());
        }

        if (config.oms_transport.enabled) {
            if (!reconcile_open_orders_from_rest(/*is_startup=*/true)) {
                ws_session.close();
                return false;
            }
        }
        set_error("");
        public_ws_generation_.store(0, std::memory_order_release);
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
        oms_rest_threads.clear();
        oms_rest_threads.reserve(oms_rest_workers.size());
        for (std::size_t worker_index = 0; worker_index < oms_rest_workers.size(); ++worker_index) {
            oms_rest_threads.emplace_back([this, worker_index](const std::stop_token& stop_token){
                oms_rest_loop(worker_index, stop_token);
            });
        }
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
                const auto next_generation =
                    public_ws_generation_.load(std::memory_order_acquire) + 1U;
                char reconnect_detail[256];
                std::snprintf(reconnect_detail,
                              sizeof(reconnect_detail),
                              "backoff_ms=%u attempt=%u",
                              backoff_ms,
                              reconnect_attempts + 1U);
                log_public_ws_event("closed", next_generation, reconnect_detail);
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                ++reconnect_attempts;

                if (!ws_session.connect()) {
                    log_public_ws_event("reconnect_failed",
                                        next_generation,
                                        ws_session.last_error().c_str());
                    continue;
                }
                log_public_ws_event("reconnect_ok", next_generation);
                bool subscribe_ok = true;
                for (const auto& channel : config.kalshi_ws.channels) {
                    if (!ws_session.subscribe(channel, config.kalshi_ws.market_tickers)) {
                        log_public_ws_event("resubscribe_failed",
                                            next_generation,
                                            ws_session.last_error().c_str());
                        subscribe_ok = false;
                        break;
                    }
                    log_public_ws_event("resubscribe_ok", next_generation, channel.c_str());
                }
                for (const auto& channel : config.kalshi_ws.lifecycle_channels) {
                    if (!ws_session.subscribe(channel, {})) {
                        log_public_ws_event("resubscribe_failed",
                                            next_generation,
                                            ws_session.last_error().c_str());
                        subscribe_ok = false;
                        break;
                    }
                    log_public_ws_event("resubscribe_ok", next_generation, channel.c_str());
                }
                if (!subscribe_ok) {
                    log_public_ws_event("reconnect_close", next_generation);
                    ws_session.close();
                    continue;
                }
                // Reset sequence state so fresh SIDs from the new session are accepted,
                // and signal each shard to drop its stale book state.
                router->reset_sequence_state();
                for (const auto& shard : shards) {
                    shard->request_reset();
                }
                public_ws_generation_.store(next_generation, std::memory_order_release);
                log_public_ws_event("reconnect_epoch_activated", next_generation);
                reconnect_attempts = 0;
                continue;
            }

            if (recv_result.status == websocket::RecvStatus::kError) {
                log_public_ws_event("recv_error",
                                    public_ws_generation_.load(std::memory_order_acquire),
                                    ws_session.last_error().c_str());
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

    void App::Runtime::oms_rest_loop(std::size_t worker_index, const std::stop_token& stop_token) {
        if (!config.oms_transport.enabled || worker_index >= oms_rest_workers.size() ||
            oms_rest_workers[worker_index] == nullptr) {
            while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
                std::this_thread::sleep_for(std::chrono::milliseconds{kDefaultSleepMs});
            }
            return;
        }
        oms_rest_workers[worker_index]->run(stop_token);
    }

    void App::Runtime::oms_private_ws_loop(const std::stop_token& stop_token) {
        if (!config.oms_transport.enabled || oms_private_ws_worker == nullptr) {
            while (running.load(std::memory_order_acquire) && !stop_token.stop_requested()) {
                std::this_thread::sleep_for(std::chrono::milliseconds{kDefaultSleepMs});
            }
            return;
        }
        oms_private_ws_worker->run(stop_token);
    }

    [[nodiscard]] bool App::Runtime::reconcile_open_orders_from_rest(bool is_startup) {
        if (!config.oms_transport.enabled || oms_rest_workers.empty() ||
            oms_rest_workers.front() == nullptr) {
            return true;
        }
        if (!oms_rest_workers.front()->reconcile_open_orders()) {
            set_error("Failed to reconcile open orders from REST");
            return false;
        }
        if (is_startup && oms != nullptr) {
            for (;;) {
                const auto result = oms->pump(
                    config.pipeline.shard_input_capacity,
                    config.pipeline.shard_input_capacity);
                if (result.code == predex::core::oms::kalshi::OmsProcessCode::kIdle) {
                    break;
                }
                if (result.code == predex::core::oms::kalshi::OmsProcessCode::kError) {
                    set_error("OMS failed while applying startup reconciliation");
                    return false;
                }
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
            if (oms_private_ws_worker != nullptr) {
                const auto reconcile_request =
                    oms_private_ws_worker->take_reconciliation_request();
                if (reconcile_request.has_value() && !oms_rest_workers.empty() &&
                    oms_rest_workers.front() != nullptr) {
                    oms_rest_workers.front()->request_reconcile();
                }
            }
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
            if (result.code == predex::core::oms::kalshi::OmsProcessCode::kVenueBackpressure) {
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
        const std::size_t live_orders = oms ? oms->live_order_count() : 0;
        const std::uint64_t oms_processed_shard_requests =
            oms ? oms->processed_shard_request_count() : 0;
        const std::uint64_t oms_processed_kalshi_events =
            oms ? oms->processed_kalshi_event_count() : 0;
        const std::uint64_t oms_emitted_decisions =
            oms ? oms->emitted_decision_count() : 0;
        const std::uint64_t oms_emitted_transport =
            oms ? oms->emitted_transport_count() : 0;
        const std::uint64_t oms_emitted_lifecycle =
            oms ? oms->emitted_lifecycle_count() : 0;
        const std::uint64_t oms_rejected_decisions =
            oms ? oms->rejected_decision_count() : 0;

        std::size_t desynced_events = 0;
        for (const auto& event_store : event_stores) {
            desynced_events += event_store.desynced_event_count();
        }

        const auto& telem = router ? router->telemetry()
                                   : predex::core::routing::kalshi::RouterTelemetry{};

        std::fprintf(stdout,
            "[%s] STATUS | halted=%s"
            " | live_orders=%zu"
            " | oms_shard_requests=%llu | oms_kalshi_events=%llu"
            " | oms_decisions=%llu | oms_transport=%llu"
            " | oms_lifecycle=%llu | oms_rejected=%llu"
            " | router_frames=%zu | router_drop_bp=%zu | router_shard_bp=%zu | router_drop_lifecycle=%zu"
            " | router_drop_invalid=%zu | router_seq_rejects=%zu | desynced_events=%zu\n",
            time_buf,
            halted ? "true" : "false",
            live_orders,
            static_cast<unsigned long long>(oms_processed_shard_requests),
            static_cast<unsigned long long>(oms_processed_kalshi_events),
            static_cast<unsigned long long>(oms_emitted_decisions),
            static_cast<unsigned long long>(oms_emitted_transport),
            static_cast<unsigned long long>(oms_emitted_lifecycle),
            static_cast<unsigned long long>(oms_rejected_decisions),
            telem.processed_frames_,
            telem.dropped_backpressure_,
            telem.shard_backpressure_to_logger_,
            telem.dropped_unknown_ticker_lifecycle_,
            telem.dropped_invalid_,
            telem.sequence_rejects_,
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

        // Stop OMS thread after upstream shard/router activity has quiesced, then let it
        // tail-drain any remaining shard requests and venue events.
        if(oms_thread.joinable()){
            oms_thread.request_stop();
            oms_thread.join();
        }

        for (auto& thread : oms_rest_threads) {
            if (thread.joinable()) {
                thread.request_stop();
                thread.join();
            }
        }
        oms_rest_threads.clear();

        if (oms_private_ws_thread.joinable()) {
            oms_private_ws_thread.request_stop();
            if (oms_private_ws_worker != nullptr) {
                oms_private_ws_worker->close();
            }
            oms_private_ws_thread.join();
        }

        if(logger_thread.joinable()){
            logger_thread.request_stop();
            logger_thread.join();
        }
        // Single-threaded final drain after logger thread has stopped.
        if (tape_logger) {
            while (tape_logger->pump(config.pipeline.router_to_logger_capacity) > 0) {}
        }

        if(audit_thread.joinable()){
            audit_thread.request_stop();
            audit_thread.join();
        }
        // Single-threaded final drain after audit thread has stopped.
        if (audit_logger) {
            while (audit_logger->pump(config.pipeline.router_to_logger_capacity) > 0) {}
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
