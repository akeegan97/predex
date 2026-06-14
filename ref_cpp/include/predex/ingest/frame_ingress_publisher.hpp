#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <span>

#include "predex/ingest/frame_pool.hpp"
#include "predex/utils/spsc_queue.hpp"


namespace predex::core::ingest::kalshi {
constexpr std::size_t kMaxBatchSize =
    64; // max number of frames to process in one batch, can be tuned for performance
struct IngestionTelemetry{
    std::size_t received_count{0};
    std::size_t dropped_count{0};
    std::size_t oversized_count{0};
    std::uint64_t recycle_failed_count{0};
};

class FrameIngressPublisher {
  public:
    // recycle_queues is a fan-in of per-producer SPSC queues (one per producer thread)
    // logger, router, each shard). FrameIngressPublisher is the single consumer across all of them.
    // Passing a vector of explicit SPSCs rather than a single shared queue preserves the
    // SPSC contract on every edge.
    explicit FrameIngressPublisher(
        predex::core::ingest::kalshi::FramePool& frame_pool,
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& router_queue,
        std::vector<predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>*>
            recycle_queues) noexcept;

    [[nodiscard]] bool on_wire_message(std::span<const std::byte> payload) noexcept;

    [[nodiscard]] IngestionTelemetry get_telemetry() const noexcept {
        return IngestionTelemetry{
            .received_count = received_count_.load(std::memory_order_relaxed),
            .dropped_count = dropped_count_.load(std::memory_order_relaxed),
            .oversized_count = oversized_count_.load(std::memory_order_relaxed),
            .recycle_failed_count = recycle_failed_count_.load(std::memory_order_relaxed),
        };
    }

  private:
    predex::core::ingest::kalshi::FramePool& frame_pool_;
    predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>&
        router_queue_; // FrameIngressPublisher producer, Router consumer
    std::vector<predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>*>
        recycle_queues_;                // per-producer SPSC fan-in, FrameIngressPublisher consumer
    std::size_t next_recycle_queue_{0}; // round-robin cursor across recycle_queues_

    std::atomic<std::size_t> received_count_{0};
    std::atomic<std::size_t> dropped_count_{0};
    std::atomic<std::size_t> oversized_count_{0};
    std::atomic<std::uint64_t> recycle_failed_count_{0};


    std::size_t max_batch_size_{kMaxBatchSize};

    std::size_t
    drain_recycled(std::size_t max_batch_size) noexcept; // returns the number of frames recycled
    static std::uint64_t
    monotonic_now_ns() noexcept; // convenience method for getting current time in nanoseconds, used
                                 // for telemetry and logging
};
} // namespace predex::core::ingest::kalshi