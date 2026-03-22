#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include "trading/shards/event_handler.hpp"
#include "trading/metrics/latency_histogram.hpp"
#include "trading/router/shard_dispatch.hpp"
#include "trading/shards/book_store.hpp"
#include "trading/shards/message_parser.hpp"

namespace trading::shards {

struct ShardConfig {
    static constexpr auto kDefaultIdleSleep = std::chrono::milliseconds{1};

    std::size_t shard_id{0};
    std::chrono::milliseconds idle_sleep{kDefaultIdleSleep};
};

struct ShardStats {
    std::uint64_t consumed{0};
    std::uint64_t parsed{0};
    std::uint64_t parse_errors{0};
    std::uint64_t parser_rejects{0};
    std::uint64_t apply_rejects{0};
    std::uint64_t parsed_snapshots{0};
    std::uint64_t parsed_deltas{0};
    std::uint64_t parsed_trades{0};
    std::uint64_t parsed_other{0};
    std::uint64_t parse_error_invalid_json{0};
    std::uint64_t parse_error_missing_field{0};
    std::uint64_t parse_error_invalid_field{0};
    std::uint64_t parse_error_unsupported_type{0};
    std::uint64_t applied{0};
    std::uint64_t handler_invoked{0};
    std::uint64_t handler_errors{0};
    metrics::LatencyPercentiles recv_to_parse_latency{};
};

class Shard final {
  public:
    Shard(ShardConfig config,
          router::ShardedEventDispatch& dispatch,
          IExchangeMessageParser& parser,
          BookStore& books,
          IShardEventHandler* event_handler = nullptr);
    ~Shard();

    Shard(const Shard&) = delete;
    Shard& operator=(const Shard&) = delete;
    Shard(Shard&&) = delete;
    Shard& operator=(Shard&&) = delete;

    [[nodiscard]] bool start();
    void stop();

    [[nodiscard]] bool running() const;
    [[nodiscard]] ShardStats stats() const;

  private:
    void run(const std::stop_token& stop_token);

    ShardConfig config_;
    router::ShardedEventDispatch& dispatch_;
    IExchangeMessageParser& parser_;
    BookStore& books_;
    IShardEventHandler* event_handler_{nullptr};

    std::jthread worker_;
    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> consumed_{0};
    std::atomic<std::uint64_t> parsed_{0};
    std::atomic<std::uint64_t> parse_errors_{0};
    std::atomic<std::uint64_t> parser_rejects_{0};
    std::atomic<std::uint64_t> apply_rejects_{0};
    std::atomic<std::uint64_t> parsed_snapshots_{0};
    std::atomic<std::uint64_t> parsed_deltas_{0};
    std::atomic<std::uint64_t> parsed_trades_{0};
    std::atomic<std::uint64_t> parsed_other_{0};
    std::atomic<std::uint64_t> parse_error_invalid_json_{0};
    std::atomic<std::uint64_t> parse_error_missing_field_{0};
    std::atomic<std::uint64_t> parse_error_invalid_field_{0};
    std::atomic<std::uint64_t> parse_error_unsupported_type_{0};
    std::atomic<std::uint64_t> applied_{0};
    std::atomic<std::uint64_t> handler_invoked_{0};
    std::atomic<std::uint64_t> handler_errors_{0};
    metrics::AtomicLatencyHistogram recv_to_parse_latency_hist_;
};

} // namespace trading::shards
