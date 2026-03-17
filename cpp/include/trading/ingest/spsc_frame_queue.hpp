#pragma once

#include "trading/ingest/frame_handle.hpp"
#include <atomic>
#include <cstddef>
#include <vector>

#if defined(__cpp_lib_hardware_interference_size)
#include <new>
constexpr std::size_t kCacheLine = std::hardware_constructive_interference_size;
#else
constexpr std::size_t kCacheLine = 64;
#endif

namespace trading::ingest {

class SpscFrameQueue {
  public:
    explicit SpscFrameQueue(std::size_t capacity)
        : capacity_(capacity < kMinCapacity ? kMinCapacity : capacity), buffer_(capacity_) {}

    [[nodiscard]] bool try_push(const FrameRef& ref) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next_head = increment(head);
        // Acquire pairs with consumer's tail release so fullness is observed correctly.
        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        buffer_[head] = ref;
        // Publish slot contents before making the new head visible.
        head_.store(next_head, std::memory_order_release);
        update_high_watermark(used_slots(head, tail_.load(std::memory_order_relaxed)) + 1U);
        return true;
    }

    [[nodiscard]] bool try_pop(FrameRef& ref_out) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        // Acquire pairs with producer's head release so slot data is visible.
        if (tail == head_.load(std::memory_order_acquire)) {
            return false;
        }

        ref_out = buffer_[tail];
        // Publish consumed slot after the read from buffer_ is complete.
        tail_.store(increment(tail), std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] std::size_t size_approx() const noexcept {
        return used_slots(head_.load(std::memory_order_relaxed), tail_.load(std::memory_order_relaxed));
    }
    [[nodiscard]] std::size_t high_watermark() const noexcept {
        return high_watermark_.load(std::memory_order_relaxed);
    }

  private:
    static constexpr std::size_t kMinCapacity = 2;

    [[nodiscard]] std::size_t increment(std::size_t index) const noexcept {
        return (index + 1) % capacity_;
    }

    [[nodiscard]] std::size_t used_slots(std::size_t head, std::size_t tail) const noexcept {
        if (head >= tail) {
            return head - tail;
        }
        return capacity_ - (tail - head);
    }

    void update_high_watermark(std::size_t size) noexcept {
        std::size_t previous = high_watermark_.load(std::memory_order_relaxed);
        while (size > previous &&
               !high_watermark_.compare_exchange_weak(previous, size, std::memory_order_relaxed,
                                                      std::memory_order_relaxed)) {
        }
    }

    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    std::size_t capacity_;
    std::vector<FrameRef> buffer_;
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
    std::atomic<std::size_t> high_watermark_{0};
};

} // namespace trading::ingest
