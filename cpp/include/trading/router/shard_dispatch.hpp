#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "trading/internal/market_types.hpp"
#include "trading/router/route_key.hpp"

namespace trading::router {

struct RoutedFrame {
    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    std::string market_ticker;
    std::optional<internal::SequenceId> sequence_id;
    internal::TimestampNs recv_ns{0};
    std::string raw_payload;
};

class IShardDispatch {
  public:
    virtual ~IShardDispatch() = default;

    virtual bool dispatch(const RouteKey& route_key, const RoutedFrame& frame) = 0;
};

struct RoutedEvent {
    RouteKey route_key;
    RoutedFrame frame;
};

class SpscRoutedEventQueue {
  public:
    explicit SpscRoutedEventQueue(std::size_t capacity);

    [[nodiscard]] bool try_push(const RoutedEvent& event) noexcept;
    [[nodiscard]] bool try_pop(RoutedEvent& event_out) noexcept;
    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] std::size_t size_approx() const noexcept;
    [[nodiscard]] std::size_t high_watermark() const noexcept;

  private:
    [[nodiscard]] std::size_t increment(std::size_t index) const noexcept;
    [[nodiscard]] std::size_t used_slots(std::size_t head, std::size_t tail) const noexcept;
    void update_high_watermark(std::size_t size) noexcept;

    std::vector<RoutedEvent> buffer_;
    std::size_t capacity_;
    std::atomic<std::size_t> head_{0};
    std::atomic<std::size_t> tail_{0};
    std::atomic<std::size_t> high_watermark_{0};
};

struct ShardedEventDispatchConfig {
    static constexpr std::size_t kDefaultPerShardQueueCapacity = 1024;

    std::size_t shard_count{1};
    std::size_t per_shard_queue_capacity{kDefaultPerShardQueueCapacity};
};

struct ShardDispatchQueueStats {
    std::size_t pending_count_total{0};
    std::size_t high_watermark_total{0};
    std::size_t high_watermark_max{0};
};

class ShardedEventDispatch final : public IShardDispatch {
  public:
    explicit ShardedEventDispatch(ShardedEventDispatchConfig config);

    bool dispatch(const RouteKey& route_key, const RoutedFrame& frame) override;
    [[nodiscard]] bool try_pop(std::size_t shard_id, RoutedEvent& event_out);
    [[nodiscard]] std::size_t shard_count() const;
    [[nodiscard]] std::size_t dropped_count() const;
    [[nodiscard]] ShardDispatchQueueStats queue_stats() const;

  private:
    std::vector<std::unique_ptr<SpscRoutedEventQueue>> shard_queues_;
    std::atomic<std::size_t> dropped_count_{0};
};

class NoopShardDispatch final : public IShardDispatch {
  public:
    bool dispatch(const RouteKey& route_key, const RoutedFrame& frame) override;
    [[nodiscard]] std::size_t dispatched_count() const;

  private:
    std::atomic<std::size_t> dispatched_count_{0};
};

} // namespace trading::router
