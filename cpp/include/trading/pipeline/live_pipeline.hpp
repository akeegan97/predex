#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "trading/adapters/ws/feed_runner.hpp"
#include "trading/decode/dispatch.hpp"
#include "trading/ingest/frame_pool.hpp"
#include "trading/ingest/spsc_frame_queue.hpp"
#include "trading/ingest/ws_message_sink.hpp"
#include "trading/internal/market_types.hpp"
#include "trading/metrics/latency_histogram.hpp"
#include "trading/router/router.hpp"
#include "trading/router/shard_dispatch.hpp"
#include "trading/shards/book_store.hpp"
#include "trading/shards/event_handler.hpp"
#include "trading/shards/message_parser.hpp"
#include "trading/shards/shard.hpp"

namespace trading::pipeline {

struct LivePipelineConfig {
    using ShardEventHandlerFactory =
        std::function<std::unique_ptr<shards::IShardEventHandler>(std::size_t shard_id)>;

    static constexpr std::size_t kDefaultFramePoolCapacity = 8192;
    static constexpr std::size_t kDefaultFrameQueueCapacity = 8192;
    static constexpr std::size_t kDefaultShardCount = 4;
    static constexpr std::size_t kDefaultPerShardQueueCapacity = 1024;
    static constexpr auto kDefaultShardIdleSleep = std::chrono::milliseconds{1};

    std::string source{"kalshi"};
    decode::ExchangeId decode_exchange{decode::ExchangeId::kKalshi};
    internal::ExchangeId router_exchange{internal::ExchangeId::kKalshi};
    std::size_t frame_pool_capacity{kDefaultFramePoolCapacity};
    std::size_t frame_queue_capacity{kDefaultFrameQueueCapacity};
    std::size_t shard_count{kDefaultShardCount};
    std::size_t per_shard_queue_capacity{kDefaultPerShardQueueCapacity};
    std::chrono::milliseconds shard_idle_sleep{kDefaultShardIdleSleep};
    ShardEventHandlerFactory shard_event_handler_factory;
};

struct LivePipelineStats {
    std::uint64_t ingest_frames_pumped{0};
    std::uint64_t route_success{0};
    std::uint64_t route_drop{0};
    std::uint64_t frame_release_failures{0};
    std::uint64_t ingest_sink_dropped{0};
    std::uint64_t shard_dispatch_dropped{0};
    std::uint64_t shard_consumed{0};
    std::uint64_t shard_parsed{0};
    std::uint64_t shard_parse_errors{0};
    std::uint64_t shard_parser_rejects{0};
    std::uint64_t shard_apply_rejects{0};
    std::uint64_t shard_applied{0};
    std::uint64_t parsed_snapshots{0};
    std::uint64_t parsed_deltas{0};
    std::uint64_t parsed_trades{0};
    std::uint64_t parsed_other{0};
    std::uint64_t parse_error_invalid_json{0};
    std::uint64_t parse_error_missing_field{0};
    std::uint64_t parse_error_invalid_field{0};
    std::uint64_t parse_error_unsupported_type{0};
    std::size_t ingest_queue_depth{0};
    std::size_t ingest_queue_high_watermark{0};
    std::size_t shard_queue_depth{0};
    std::size_t shard_queue_high_watermark_total{0};
    std::size_t shard_queue_high_watermark_max{0};
    metrics::LatencyPercentiles recv_to_parse_latency{};
};

class LivePipeline final {
  public:
    static constexpr std::size_t kDefaultPumpBatchSize = 1024;

    explicit LivePipeline(LivePipelineConfig config = {});
    ~LivePipeline();

    LivePipeline(const LivePipeline&) = delete;
    LivePipeline& operator=(const LivePipeline&) = delete;
    LivePipeline(LivePipeline&&) = delete;
    LivePipeline& operator=(LivePipeline&&) = delete;

    [[nodiscard]] bool start();
    void stop();

    [[nodiscard]] bool running() const;
    [[nodiscard]] adapters::ws::IWsMessageSink& message_sink();
    [[nodiscard]] std::size_t pump_ingest(std::size_t max_frames = kDefaultPumpBatchSize) noexcept;

    [[nodiscard]] LivePipelineStats stats() const;
    [[nodiscard]] std::size_t shard_count() const;
    [[nodiscard]] const shards::BookStore* book_store(std::size_t shard_id) const;

  private:
    LivePipelineConfig config_;

    ingest::FramePool frame_pool_;
    ingest::SpscFrameQueue frame_queue_;
    ingest::FramePoolMessageSink sink_;
    router::ShardedEventDispatch shard_dispatch_;
    router::Router router_;
    shards::RoutedEventParser parser_;
    std::vector<std::unique_ptr<shards::BookStore>> book_stores_;
    std::vector<std::unique_ptr<shards::IShardEventHandler>> shard_event_handlers_;
    std::vector<std::unique_ptr<shards::Shard>> shards_;

    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> ingest_frames_pumped_{0};
    std::atomic<std::uint64_t> route_success_{0};
    std::atomic<std::uint64_t> route_drop_{0};
    std::atomic<std::uint64_t> frame_release_failures_{0};
};

} // namespace trading::pipeline
