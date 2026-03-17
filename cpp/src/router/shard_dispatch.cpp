#include "trading/router/shard_dispatch.hpp"

#include <utility>

namespace trading::router {
namespace {

constexpr std::size_t kMinQueueCapacity = 2;
constexpr std::size_t kMinShardCount = 1;

} // namespace

SpscRoutedEventQueue::SpscRoutedEventQueue(std::size_t capacity)
    : buffer_(capacity < kMinQueueCapacity ? kMinQueueCapacity : capacity),
      capacity_(buffer_.size()) {}

bool SpscRoutedEventQueue::try_push(const RoutedEvent& event) noexcept {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t next_head = increment(head);
    // Acquire pairs with consumer tail release for correct full/empty state.
    if (next_head == tail_.load(std::memory_order_acquire)) {
        return false;
    }

    buffer_[head] = event;
    // Publish event payload before publishing updated head.
    head_.store(next_head, std::memory_order_release);
    update_high_watermark(used_slots(head, tail_.load(std::memory_order_relaxed)) + 1U);
    return true;
}

bool SpscRoutedEventQueue::try_pop(RoutedEvent& event_out) noexcept {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    // Acquire pairs with producer head release so event fields are visible.
    if (tail == head_.load(std::memory_order_acquire)) {
        return false;
    }

    event_out = std::move(buffer_[tail]);
    // Publish consumed slot after move from buffer_ is complete.
    tail_.store(increment(tail), std::memory_order_release);
    return true;
}

std::size_t SpscRoutedEventQueue::capacity() const noexcept { return capacity_; }

std::size_t SpscRoutedEventQueue::size_approx() const noexcept {
    return used_slots(head_.load(std::memory_order_relaxed), tail_.load(std::memory_order_relaxed));
}

std::size_t SpscRoutedEventQueue::high_watermark() const noexcept {
    return high_watermark_.load(std::memory_order_relaxed);
}

std::size_t SpscRoutedEventQueue::increment(std::size_t index) const noexcept {
    return (index + 1) % capacity_;
}

std::size_t SpscRoutedEventQueue::used_slots(std::size_t head, std::size_t tail) const noexcept {
    if (head >= tail) {
        return head - tail;
    }
    return capacity_ - (tail - head);
}

void SpscRoutedEventQueue::update_high_watermark(std::size_t size) noexcept {
    std::size_t previous = high_watermark_.load(std::memory_order_relaxed);
    while (size > previous &&
           !high_watermark_.compare_exchange_weak(previous, size, std::memory_order_relaxed,
                                                  std::memory_order_relaxed)) {
    }
}

ShardedEventDispatch::ShardedEventDispatch(ShardedEventDispatchConfig config) {
    const std::size_t resolved_shards =
        config.shard_count == 0 ? kMinShardCount : config.shard_count;
    shard_queues_.reserve(resolved_shards);
    for (std::size_t i = 0; i < resolved_shards; ++i) {
        shard_queues_.push_back(
            std::make_unique<SpscRoutedEventQueue>(config.per_shard_queue_capacity));
    }
}

bool ShardedEventDispatch::dispatch(const RouteKey& route_key, const RoutedFrame& frame) {
    if (route_key.shard_id >= shard_queues_.size()) {
        dropped_count_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    RoutedEvent routed{
        .route_key = route_key,
        .frame = frame,
    };
    if (!shard_queues_[route_key.shard_id]->try_push(routed)) {
        dropped_count_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

bool ShardedEventDispatch::try_pop(std::size_t shard_id, RoutedEvent& event_out) {
    if (shard_id >= shard_queues_.size()) {
        return false;
    }
    return shard_queues_[shard_id]->try_pop(event_out);
}

std::size_t ShardedEventDispatch::shard_count() const { return shard_queues_.size(); }

std::size_t ShardedEventDispatch::dropped_count() const {
    return dropped_count_.load(std::memory_order_relaxed);
}

ShardDispatchQueueStats ShardedEventDispatch::queue_stats() const {
    ShardDispatchQueueStats stats{};
    for (const auto& queue : shard_queues_) {
        if (queue == nullptr) {
            continue;
        }
        const std::size_t pending = queue->size_approx();
        const std::size_t high_watermark = queue->high_watermark();
        stats.pending_count_total += pending;
        stats.high_watermark_total += high_watermark;
        if (high_watermark > stats.high_watermark_max) {
            stats.high_watermark_max = high_watermark;
        }
    }
    return stats;
}

bool NoopShardDispatch::dispatch(const RouteKey& route_key, const RoutedFrame& frame) {
    (void)route_key;
    (void)frame;
    dispatched_count_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

std::size_t NoopShardDispatch::dispatched_count() const {
    return dispatched_count_.load(std::memory_order_relaxed);
}

} // namespace trading::router
