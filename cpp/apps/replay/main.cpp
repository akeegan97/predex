#include "../common/config.hpp"

#include "predex/audit/audit_logger.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/oms/execution_transport.hpp"
#include "predex/oms/global_risk.hpp"
#include "predex/oms/oms.hpp"
#include "predex/router/market_registry.hpp"
#include "predex/router/router.hpp"
#include "predex/router/shard_dispatch.hpp"
#include "predex/shards/event_store.hpp"
#include "predex/shards/local_risk.hpp"
#include "predex/shards/shard.hpp"
#include "predex/shards/shard_pipeline.hpp"
#include "predex/shards/strategies/cdf_violation.hpp"
#include "predex/shards/strategies/market_making.hpp"
#include "predex/shards/strategies/mean_reversion.hpp"
#include "predex/shards/strategies/monotonic_arb.hpp"
#include "predex/utils/spsc_queue.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

constexpr int kExitSuccess = 0;
constexpr int kExitArgsFailure = 2;
constexpr int kExitConfigFailure = 3;
constexpr int kExitRuntimeFailure = 5;
constexpr std::size_t kPumpBatchSize = 1024;
constexpr auto kStatusInterval = std::chrono::seconds(30);

using FrameHandle = predex::core::ingest::kalshi::FrameHandle;
using FramePool = predex::core::ingest::kalshi::FramePool;
using FrameQueue = predex::utils::SPSCQueue<FrameHandle>;
using Router = predex::core::routing::kalshi::Router;
using MarketRegistry = predex::core::routing::kalshi::MarketRegistry;
using MarketRegistryEntry = predex::core::routing::kalshi::MarketRegistryEntry;
using ShardDispatch = predex::core::routing::kalshi::ShardDispatch;
using EventStore = predex::core::shards::kalshi::EventStore;
using EventDefinition = predex::core::shards::kalshi::EventDefinition;
using EventMarketDefinition = predex::core::shards::kalshi::EventMarketDefinition;
using LocalRiskManager = predex::core::shards::kalshi::LocalRiskManager;
using LocalRiskLimits = predex::core::shards::kalshi::LocalRiskLimits;
using MonotonicArbStrategy = predex::core::shards::kalshi::strategies::MonotonicArbStrategy;
using CdfViolationStrategy = predex::core::shards::kalshi::strategies::CdfViolationStrategy;
using MarketMakingStrategy = predex::core::shards::kalshi::strategies::MarketMakingStrategy;
using MeanReversionStrategy = predex::core::shards::kalshi::strategies::MeanReversionStrategy;
using ShardPipeline =
    predex::core::shards::kalshi::DefaultShardPipeline<LocalRiskManager, MonotonicArbStrategy,
                                                       CdfViolationStrategy, MarketMakingStrategy,
                                                       MeanReversionStrategy>;
using Shard = predex::core::shards::kalshi::Shard<ShardPipeline>;
using Oms = predex::core::oms::kalshi::Oms;
using OmsIntentQueue = predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsSubmission>;
using OmsDecisionQueue = predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsToShardDecision>;
using OmsLifecycleQueue =
    predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsToShardLifecycleEvent>;
using AuditQueue = predex::utils::SPSCQueue<predex::core::audit::AuditEvent>;
using OmsCommandQueue = predex::utils::SPSCQueue<predex::core::oms::kalshi::OmsToKalshiCommand>;
using KalshiEventQueue = predex::utils::SPSCQueue<predex::core::oms::kalshi::KalshiToOmsEvent>;

struct ReplayOptions {
    std::string config_path;
    std::string tape_path;
    std::string audit_path;
};

std::optional<std::string> find_arg_value(int argc, char** argv, std::string_view flag) {
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr || std::string_view{argv[index]} != flag) {
            continue;
        }
        if (index + 1 >= argc || argv[index + 1] == nullptr ||
            std::string_view{argv[index + 1]}.empty()) {
            return std::nullopt;
        }
        return std::string{argv[index + 1]};
    }
    return std::nullopt;
}

std::string derive_replay_audit_path(std::string base_path) {
    if (base_path.empty()) {
        return "predex_audit.replay.jsonl";
    }
    const std::size_t dot_pos = base_path.rfind('.');
    if (dot_pos == std::string::npos) {
        return base_path + ".replay";
    }
    return base_path.substr(0, dot_pos) + ".replay" + base_path.substr(dot_pos);
}

std::vector<MarketRegistryEntry> build_market_registry_entries(const predex::AppConfig& config) {
    std::vector<MarketRegistryEntry> entries;
    entries.reserve(config.market_routes.size());
    for (const auto& route : config.market_routes) {
        entries.push_back({
            .ticker_ = route.market_ticker,
            .market_id_ = static_cast<std::uint32_t>(route.market_id),
            .event_id_ = static_cast<std::uint32_t>(route.event_id),
            .affinity_key_ = static_cast<std::uint16_t>(route.affinity_key),
            .topology_kind_ = route.topology_kind,
            .strike_key_ = route.strike_key,
        });
    }
    return entries;
}

bool build_event_definitions_by_shard(
    const std::vector<predex::MarketRouteConfig>& routes, std::size_t shard_count,
    std::vector<std::vector<EventDefinition>>& definitions_by_shard, std::string& error_out) {
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
        std::vector<EventMarketDefinition> markets;
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
        const auto duplicate_market =
            std::find_if(accumulator.markets.begin(), accumulator.markets.end(),
                         [market_id32](const EventMarketDefinition& market) {
                             return market.market_id == market_id32;
                         });
        if (duplicate_market != accumulator.markets.end()) {
            error_out = "event " + std::to_string(event_id32) + " contains duplicate market_id " +
                        std::to_string(market_id32) + " in config";
            return false;
        }

        accumulator.markets.push_back(EventMarketDefinition{
            .market_id = market_id32,
            .strike_key = route.strike_key,
            .close_time_s = route.close_time_s,
            .tradeable = route.tradeable,
        });
    }

    for (const auto& [event_id, accumulator] : grouped_events) {
        const std::size_t shard_id = accumulator.affinity_key % shard_count;
        definitions_by_shard[shard_id].push_back(EventDefinition{
            .event_id = event_id,
            .topology_kind = accumulator.topology_kind,
            .expected_market_count = accumulator.markets.size(),
            .markets = accumulator.markets,
        });
    }

    return true;
}

class TapeReader {
  public:
    explicit TapeReader(const std::string& path) : path_(path), input_(path, std::ios::binary) {}

    [[nodiscard]] bool ok() const noexcept { return input_.is_open(); }
    [[nodiscard]] std::string error() const { return error_; }

    bool open_and_validate() {
        if (!input_.is_open()) {
            error_ = "Failed to open tape: " + path_;
            return false;
        }

        std::array<char, 4> magic{};
        std::uint16_t version = 0;
        std::uint16_t flags = 0;
        input_.read(magic.data(), static_cast<std::streamsize>(magic.size()));
        input_.read(reinterpret_cast<char*>(&version), sizeof(version));
        input_.read(reinterpret_cast<char*>(&flags), sizeof(flags));
        if (!input_) {
            error_ = "Failed to read tape header";
            return false;
        }
        if (magic != std::array<char, 4>{'P', 'D', 'T', '2'}) {
            error_ = "Unsupported tape magic";
            return false;
        }
        if (version != 2) {
            error_ = "Unsupported tape version: " + std::to_string(version);
            return false;
        }
        (void)flags;
        return true;
    }

    [[nodiscard]] std::optional<std::uint64_t> count_records() const {
        std::ifstream input(path_, std::ios::binary);
        if (!input.is_open()) {
            return std::nullopt;
        }

        std::array<char, 4> magic{};
        std::uint16_t version = 0;
        std::uint16_t flags = 0;
        input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
        input.read(reinterpret_cast<char*>(&version), sizeof(version));
        input.read(reinterpret_cast<char*>(&flags), sizeof(flags));
        if (!input || magic != std::array<char, 4>{'P', 'D', 'T', '2'} || version != 2) {
            return std::nullopt;
        }

        std::uint64_t count = 0;
        while (true) {
            std::uint64_t recv_ts_ns = 0;
            std::uint32_t len = 0;
            input.read(reinterpret_cast<char*>(&recv_ts_ns), sizeof(recv_ts_ns));
            if (input.eof()) {
                break;
            }
            input.read(reinterpret_cast<char*>(&len), sizeof(len));
            if (!input || len > predex::core::ingest::kalshi::kMaxFrameBytes) {
                return std::nullopt;
            }
            input.seekg(static_cast<std::streamoff>(len), std::ios::cur);
            if (!input) {
                return std::nullopt;
            }
            ++count;
        }
        return count;
    }

    bool read_next(FramePool& frame_pool, FrameHandle& handle_out) {
        std::uint64_t recv_ts_ns = 0;
        std::uint32_t len = 0;
        input_.read(reinterpret_cast<char*>(&recv_ts_ns), sizeof(recv_ts_ns));
        if (input_.eof()) {
            return false;
        }
        input_.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!input_) {
            error_ = "Tape record truncated while reading length";
            return false;
        }
        if (len > predex::core::ingest::kalshi::kMaxFrameBytes) {
            error_ = "Tape frame exceeds max frame size: " + std::to_string(len);
            return false;
        }

        FrameHandle handle{};
        if (!frame_pool.try_acquire(handle)) {
            error_ = "Frame pool exhausted while replaying tape";
            return false;
        }

        auto* frame = frame_pool.writable_frame(handle);
        if (frame == nullptr) {
            error_ = "Failed to acquire writable replay frame";
            return false;
        }

        frame->recv_ts_ns_ = recv_ts_ns;
        frame->len_ = len;
        frame->flags_ = 0;
        input_.read(reinterpret_cast<char*>(frame->payload.data()), len);
        if (!input_) {
            error_ = "Tape record truncated while reading payload";
            return false;
        }

        handle_out = handle;
        ++records_read_;
        return true;
    }

    [[nodiscard]] std::uint64_t records_read() const noexcept { return records_read_; }

  private:
    std::string path_{};
    std::ifstream input_;
    std::string error_{};
    std::uint64_t records_read_{0};
};

class SyntheticRejectTransport {
  public:
    SyntheticRejectTransport(OmsCommandQueue& command_queue, KalshiEventQueue& event_queue)
        : command_queue_(command_queue), event_queue_(event_queue) {}

    [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept {
        std::size_t processed = 0;
        for (; processed < max_batch_size; ++processed) {
            predex::core::oms::kalshi::OmsToKalshiCommand command;
            if (!command_queue_.try_pop(command)) {
                break;
            }

            const auto pushed = std::visit(
                [this](const auto& typed_command) noexcept {
                    using T = std::decay_t<decltype(typed_command)>;
                    const auto recv_ts_ns = typed_command.transport_enqueue_ts_ns + 1;
                    if constexpr (std::is_same_v<T, predex::core::oms::kalshi::SubmitOrderCmd>) {
                        return event_queue_.try_push(predex::core::oms::kalshi::KalshiToOmsEvent{
                            predex::core::oms::kalshi::VenueOrderReject{
                                .order = typed_command.order,
                                .transport_submit_ts_ns = typed_command.transport_enqueue_ts_ns,
                                .recv_ts_ns = recv_ts_ns,
                                .http_status_code = 403,
                                .retry_count = 0,
                                .reason = predex::core::oms::kalshi::VenueRejectReason::kUnknown,
                                .raw_reason_code = "replay_read_only",
                                .raw_reason_message = "synthetic replay reject",
                            }});
                    } else if constexpr (std::is_same_v<
                                             T, predex::core::oms::kalshi::CancelOrderCmd>) {
                        return event_queue_.try_push(predex::core::oms::kalshi::KalshiToOmsEvent{
                            predex::core::oms::kalshi::VenueCancelReject{
                                .order = typed_command.corr.order,
                                .transport_submit_ts_ns = typed_command.transport_enqueue_ts_ns,
                                .recv_ts_ns = recv_ts_ns,
                                .http_status_code = 403,
                                .retry_count = 0,
                                .reason = predex::core::oms::kalshi::VenueRejectReason::kUnknown,
                                .raw_reason_code = "replay_read_only",
                                .raw_reason_message = "synthetic replay reject",
                            }});
                    } else {
                        return event_queue_.try_push(predex::core::oms::kalshi::KalshiToOmsEvent{
                            predex::core::oms::kalshi::VenueModifyReject{
                                .order = typed_command.corr.order,
                                .transport_submit_ts_ns = typed_command.transport_enqueue_ts_ns,
                                .recv_ts_ns = recv_ts_ns,
                                .http_status_code = 403,
                                .retry_count = 0,
                                .reason = predex::core::oms::kalshi::VenueRejectReason::kUnknown,
                                .raw_reason_code = "replay_read_only",
                                .raw_reason_message = "synthetic replay reject",
                            }});
                    }
                },
                command);

            if (!pushed) {
                break;
            }
        }
        return processed;
    }

  private:
    OmsCommandQueue& command_queue_;
    KalshiEventQueue& event_queue_;
};

class ReplayRuntime {
  public:
    ReplayRuntime(predex::AppConfig config, std::string tape_path, std::string audit_path)
        : config_(std::move(config)), tape_path_(std::move(tape_path)),
          audit_path_(std::move(audit_path)),
          market_registry_entries_(build_market_registry_entries(config_)),
          frame_pool_(config_.pipeline.frame_pool_capacity),
          io_to_router_queue_(config_.pipeline.io_to_router_capacity),
          router_to_logger_queue_(config_.pipeline.router_to_logger_capacity),
          recycle_from_router_(config_.pipeline.frame_pool_capacity),
          market_registry_(market_registry_entries_), tape_reader_(tape_path_),
          synthetic_transport_(oms_command_queue_, oms_rest_event_queue_) {
        init();
    }

    [[nodiscard]] const std::string& error() const noexcept { return error_; }

    bool run() {
        if (!init_ok_) {
            return false;
        }
        if (!tape_reader_.open_and_validate()) {
            error_ = tape_reader_.error();
            return false;
        }
        total_records_ = tape_reader_.count_records();
        if (!total_records_.has_value()) {
            error_ = "Failed to count replay tape records";
            return false;
        }

        start_time_ = std::chrono::steady_clock::now();
        next_status_time_ = *start_time_ + kStatusInterval;
        print_status_line("start");

        FrameHandle handle{};
        while (tape_reader_.read_next(frame_pool_, handle)) {
            if (!io_to_router_queue_.try_push(handle)) {
                error_ = "Replay ingress queue overflow";
                return false;
            }
            if (!drain_to_quiescence()) {
                return false;
            }
            maybe_print_status_line();
        }

        if (!tape_reader_.error().empty()) {
            error_ = tape_reader_.error();
            return false;
        }

        if (!drain_to_quiescence()) {
            return false;
        }
        print_status_line("finalizing");
        while (audit_logger_ != nullptr && audit_logger_->pump(kPumpBatchSize) > 0) {
        }

        std::cout << "Replay complete | frames=" << tape_reader_.records_read()
                  << " | total_frames=" << total_records_.value_or(0)
                  << " | router_frames=" << router_->telemetry().processed_frames_
                  << " | router_seq_rejects=" << router_->telemetry().sequence_rejects_
                  << " | router_drop_bp=" << router_->telemetry().dropped_backpressure_
                  << " | oms_requests=" << oms_->processed_shard_request_count()
                  << " | oms_events=" << oms_->processed_kalshi_event_count()
                  << " | audit=" << audit_path_ << '\n';
        return true;
    }

  private:
    predex::AppConfig config_;
    std::string tape_path_;
    std::string audit_path_;
    bool init_ok_{true};
    std::string error_{};

    std::vector<MarketRegistryEntry> market_registry_entries_;
    std::unordered_map<predex::internal::MarketId, std::string> market_ticker_by_id_;

    FramePool frame_pool_;
    FrameQueue io_to_router_queue_;
    FrameQueue router_to_logger_queue_;
    FrameQueue recycle_from_router_;
    std::vector<std::unique_ptr<FrameQueue>> shard_input_queues_;
    std::vector<std::unique_ptr<FrameQueue>> shard_to_logger_queues_;
    std::vector<std::unique_ptr<FrameQueue>> recycle_from_shards_;
    std::vector<FrameQueue*> shard_input_queue_ptrs_;
    std::vector<std::unique_ptr<OmsIntentQueue>> shard_to_oms_intent_queues_;
    std::vector<std::unique_ptr<OmsDecisionQueue>> oms_to_shard_decision_queues_;
    std::vector<std::unique_ptr<OmsLifecycleQueue>> oms_to_shard_lifecycle_queues_;
    std::vector<std::unique_ptr<AuditQueue>> shard_audit_queues_;
    std::unique_ptr<AuditQueue> router_audit_queue_;
    std::unique_ptr<AuditQueue> oms_audit_queue_;
    OmsCommandQueue oms_command_queue_{config_.pipeline.shard_input_capacity};
    KalshiEventQueue oms_rest_event_queue_{config_.pipeline.shard_input_capacity};

    MarketRegistry market_registry_;
    std::unique_ptr<ShardDispatch> shard_dispatch_;
    std::unique_ptr<Router> router_;
    std::unique_ptr<Oms> oms_;
    std::unique_ptr<predex::core::audit::AuditLogger> audit_logger_;
    std::vector<EventStore> event_stores_;
    std::vector<std::unique_ptr<Shard>> shards_;

    TapeReader tape_reader_;
    SyntheticRejectTransport synthetic_transport_;
    std::optional<std::uint64_t> total_records_;
    std::optional<std::chrono::steady_clock::time_point> start_time_;
    std::optional<std::chrono::steady_clock::time_point> next_status_time_;

    void init() {
        for (const auto& entry : market_registry_entries_) {
            market_ticker_by_id_[entry.market_id_] = entry.ticker_;
        }

        shard_input_queues_.reserve(config_.pipeline.shard_count);
        shard_to_logger_queues_.reserve(config_.pipeline.shard_count);
        recycle_from_shards_.reserve(config_.pipeline.shard_count);
        shard_to_oms_intent_queues_.reserve(config_.pipeline.shard_count);
        oms_to_shard_decision_queues_.reserve(config_.pipeline.shard_count);
        oms_to_shard_lifecycle_queues_.reserve(config_.pipeline.shard_count);
        shard_audit_queues_.reserve(config_.pipeline.shard_count);
        event_stores_.reserve(config_.pipeline.shard_count);
        shards_.reserve(config_.pipeline.shard_count);
        shard_input_queue_ptrs_.reserve(config_.pipeline.shard_count);

        for (std::size_t i = 0; i < config_.pipeline.shard_count; ++i) {
            shard_input_queues_.push_back(
                std::make_unique<FrameQueue>(config_.pipeline.shard_input_capacity));
            shard_to_logger_queues_.push_back(
                std::make_unique<FrameQueue>(config_.pipeline.shard_to_logger_capacity));
            recycle_from_shards_.push_back(
                std::make_unique<FrameQueue>(config_.pipeline.frame_pool_capacity));
            shard_to_oms_intent_queues_.push_back(
                std::make_unique<OmsIntentQueue>(config_.pipeline.shard_input_capacity));
            oms_to_shard_decision_queues_.push_back(
                std::make_unique<OmsDecisionQueue>(config_.pipeline.shard_input_capacity));
            oms_to_shard_lifecycle_queues_.push_back(
                std::make_unique<OmsLifecycleQueue>(config_.pipeline.shard_input_capacity));
            shard_audit_queues_.push_back(
                std::make_unique<AuditQueue>(config_.pipeline.shard_input_capacity));
            event_stores_.emplace_back();
        }

        router_audit_queue_ = std::make_unique<AuditQueue>(config_.pipeline.shard_input_capacity);
        oms_audit_queue_ = std::make_unique<AuditQueue>(config_.pipeline.shard_input_capacity);

        std::vector<std::vector<EventDefinition>> event_definitions_by_shard;
        std::string definition_error;
        if (!build_event_definitions_by_shard(config_.market_routes, config_.pipeline.shard_count,
                                              event_definitions_by_shard, definition_error)) {
            init_ok_ = false;
            error_ = std::move(definition_error);
            return;
        }

        for (std::size_t shard_index = 0; shard_index < config_.pipeline.shard_count;
             ++shard_index) {
            if (!event_stores_[shard_index].initialize(event_definitions_by_shard[shard_index])) {
                init_ok_ = false;
                error_ =
                    "Failed to initialize event store for shard " + std::to_string(shard_index);
                return;
            }
        }

        for (const auto& queue : shard_input_queues_) {
            shard_input_queue_ptrs_.push_back(queue.get());
        }

        shard_dispatch_ = std::make_unique<ShardDispatch>(shard_input_queue_ptrs_);
        router_ = std::make_unique<Router>(io_to_router_queue_, frame_pool_, market_registry_,
                                           *shard_dispatch_, router_to_logger_queue_,
                                           router_audit_queue_.get(), recycle_from_router_, false);

        predex::core::oms::kalshi::GlobalRiskLimits global_risk_limits{};
        global_risk_limits.available_capital_ticks = config_.oms_transport.available_capital_ticks;
        global_risk_limits.trading_enabled = config_.local_risk.trading_enabled;

        std::vector<OmsIntentQueue*> shard_to_oms_intent_queue_ptrs;
        std::vector<OmsDecisionQueue*> oms_to_shard_decision_queue_ptrs;
        std::vector<OmsLifecycleQueue*> oms_to_shard_lifecycle_queue_ptrs;
        std::vector<AuditQueue*> audit_input_queue_ptrs;
        shard_to_oms_intent_queue_ptrs.reserve(config_.pipeline.shard_count);
        oms_to_shard_decision_queue_ptrs.reserve(config_.pipeline.shard_count);
        oms_to_shard_lifecycle_queue_ptrs.reserve(config_.pipeline.shard_count);
        audit_input_queue_ptrs.reserve(config_.pipeline.shard_count + 2);

        for (std::size_t i = 0; i < config_.pipeline.shard_count; ++i) {
            shard_to_oms_intent_queue_ptrs.push_back(shard_to_oms_intent_queues_[i].get());
            oms_to_shard_decision_queue_ptrs.push_back(oms_to_shard_decision_queues_[i].get());
            oms_to_shard_lifecycle_queue_ptrs.push_back(oms_to_shard_lifecycle_queues_[i].get());
            audit_input_queue_ptrs.push_back(shard_audit_queues_[i].get());
        }
        audit_input_queue_ptrs.push_back(oms_audit_queue_.get());
        audit_input_queue_ptrs.push_back(router_audit_queue_.get());

        oms_ = std::make_unique<Oms>(
            shard_to_oms_intent_queue_ptrs, oms_to_shard_decision_queue_ptrs,
            oms_to_shard_lifecycle_queue_ptrs,
            predex::core::oms::kalshi::ExecutionTransportQueues{
                .command_queues = {&oms_command_queue_},
                .rest_event_queues = {&oms_rest_event_queue_},
                .ws_event_queue = nullptr,
            },
            global_risk_limits, oms_audit_queue_.get(),
            [this](predex::internal::MarketId market_id) -> std::optional<std::string> {
                const auto it = market_ticker_by_id_.find(market_id);
                if (it == market_ticker_by_id_.end()) {
                    return std::nullopt;
                }
                return it->second;
            });

        LocalRiskLimits local_risk_limits{};
        if (config_.local_risk.max_net_position_lots_per_market > 0) {
            local_risk_limits.max_net_position_lots_per_market =
                config_.local_risk.max_net_position_lots_per_market;
        }
        local_risk_limits.min_seconds_to_close = config_.local_risk.min_seconds_to_close;
        local_risk_limits.trading_enabled = config_.local_risk.trading_enabled;

        for (std::size_t i = 0; i < config_.pipeline.shard_count; ++i) {
            shards_.push_back(std::make_unique<Shard>(*shard_input_queues_[i], frame_pool_,
                                                      *shard_to_logger_queues_[i],
                                                      *recycle_from_shards_[i], event_stores_[i],
                                                      ShardPipeline{
                                                          static_cast<std::uint16_t>(i),
                                                          LocalRiskManager{local_risk_limits},
                                                          shard_to_oms_intent_queues_[i].get(),
                                                          oms_to_shard_decision_queues_[i].get(),
                                                          oms_to_shard_lifecycle_queues_[i].get(),
                                                          shard_audit_queues_[i].get(),
                                                          MonotonicArbStrategy{},
                                                          CdfViolationStrategy{},
                                                          MarketMakingStrategy{},
                                                          MeanReversionStrategy{},
                                                      }));
        }

        audit_logger_ =
            std::make_unique<predex::core::audit::AuditLogger>(audit_input_queue_ptrs, audit_path_);
    }

    [[nodiscard]] bool drain_to_quiescence() {
        for (std::size_t guard = 0; guard < 100000; ++guard) {
            std::size_t progress = 0;
            progress += router_->pump(kPumpBatchSize);
            for (auto& shard : shards_) {
                progress += shard->pump(kPumpBatchSize);
            }

            const auto oms_result = oms_->pump(kPumpBatchSize, kPumpBatchSize);
            progress += oms_result.processed_kalshi_events;
            progress += oms_result.processed_shard_requests;
            progress += synthetic_transport_.pump(kPumpBatchSize);
            progress += drain_logger_queue(router_to_logger_queue_);
            progress += drain_recycle_queue(recycle_from_router_);
            for (auto& queue : shard_to_logger_queues_) {
                progress += drain_logger_queue(*queue);
            }
            for (auto& queue : recycle_from_shards_) {
                progress += drain_recycle_queue(*queue);
            }
            if (audit_logger_ != nullptr) {
                progress += audit_logger_->pump(kPumpBatchSize);
            }

            if (progress == 0) {
                return true;
            }
        }

        error_ = "Replay pipeline failed to reach quiescence";
        return false;
    }

    [[nodiscard]] std::size_t drain_logger_queue(FrameQueue& queue) {
        std::size_t recycled = 0;
        FrameHandle handle{};
        while (queue.try_pop(handle)) {
            if (!frame_pool_.recycle(handle)) {
                error_ = "Failed to recycle replay frame from logger queue";
                return recycled;
            }
            ++recycled;
        }
        return recycled;
    }

    [[nodiscard]] std::size_t drain_recycle_queue(FrameQueue& queue) {
        std::size_t recycled = 0;
        FrameHandle handle{};
        while (queue.try_pop(handle)) {
            if (!frame_pool_.recycle(handle)) {
                error_ = "Failed to recycle replay frame from recycle queue";
                return recycled;
            }
            ++recycled;
        }
        return recycled;
    }

    void maybe_print_status_line() {
        if (!next_status_time_.has_value()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now < *next_status_time_) {
            return;
        }
        print_status_line("running");
        next_status_time_ = now + kStatusInterval;
    }

    void print_status_line(std::string_view phase) const {
        const std::uint64_t processed_frames = tape_reader_.records_read();
        const std::uint64_t total_frames = total_records_.value_or(0);
        const std::uint64_t remaining_frames =
            total_frames >= processed_frames ? total_frames - processed_frames : 0;
        const auto elapsed_seconds = start_time_.has_value()
                                         ? std::chrono::duration_cast<std::chrono::seconds>(
                                               std::chrono::steady_clock::now() - *start_time_)
                                               .count()
                                         : 0;
        std::cout << "Replay status" << " | phase=" << phase << " | elapsed_s=" << elapsed_seconds
                  << " | frames_processed=" << processed_frames
                  << " | frames_remaining=" << remaining_frames
                  << " | total_frames=" << total_frames
                  << " | router_frames=" << router_->telemetry().processed_frames_
                  << " | oms_requests=" << oms_->processed_shard_request_count()
                  << " | oms_events=" << oms_->processed_kalshi_event_count() << '\n';
        std::cout.flush();
    }
};

std::optional<ReplayOptions> parse_options(int argc, char** argv, std::string& error_out) {
    const auto config_path = predex::apps::resolve_config_path(argc, argv);
    if (!config_path.has_value()) {
        error_out = "Missing replay config path. Use --config <path> or set TRADING_CONFIG_PATH.";
        return std::nullopt;
    }

    std::string config_error;
    auto app_config = predex::apps::load_app_config(*config_path, config_error,
                                                    predex::apps::AppConfigParseOptions{
                                                        .require_credentials = false,
                                                        .require_public_channels = false,
                                                    });
    if (!app_config.has_value()) {
        error_out = "Invalid replay config: " + config_error;
        return std::nullopt;
    }

    ReplayOptions options{
        .config_path = *config_path,
        .tape_path = find_arg_value(argc, argv, "--tape").value_or(app_config->tape.output_path),
        .audit_path = find_arg_value(argc, argv, "--audit")
                          .value_or(derive_replay_audit_path(app_config->audit.output_path)),
    };
    if (options.tape_path.empty()) {
        error_out = "Replay tape path is empty. Use --tape <path> or configure tape.output_path.";
        return std::nullopt;
    }
    return options;
}

} // namespace

int main(int argc, char** argv) {
    std::string option_error;
    auto options = parse_options(argc, argv, option_error);
    if (!options.has_value()) {
        std::cerr << option_error << '\n';
        return kExitArgsFailure;
    }

    std::string config_error;
    auto app_config = predex::apps::load_app_config(options->config_path, config_error,
                                                    predex::apps::AppConfigParseOptions{
                                                        .require_credentials = false,
                                                        .require_public_channels = false,
                                                    });
    if (!app_config.has_value()) {
        std::cerr << "Invalid replay config: " << config_error << '\n';
        return kExitConfigFailure;
    }

    ReplayRuntime runtime{std::move(*app_config), options->tape_path, options->audit_path};
    if (!runtime.run()) {
        std::cerr << "Replay failed: " << runtime.error() << '\n';
        return kExitRuntimeFailure;
    }
    return kExitSuccess;
}
