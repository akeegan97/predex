#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "predex/ingest/frame_pool.hpp"

#include "predex/utils/spsc_queue.hpp"

namespace predex::core::ingest::io{
  constexpr std::size_t kMaxBatchSize = 64; // max number of frames to process in one batch, can be tuned for performance
  class IOWriter{
    public:
      explicit IOWriter(predex::core::ingest::kalshi::FramePool& frame_pool, 
                        predex::utils::SPSCQueue<kalshi::FrameHandle>& router_queue,
                        predex::utils::SPSCQueue<kalshi::FrameHandle>& recycle_queue) noexcept;

        [[nodiscard]] bool on_wire_message(std::string_view payload) noexcept;


    private:
      predex::core::ingest::kalshi::FramePool& frame_pool_;
      predex::utils::SPSCQueue<kalshi::FrameHandle>& router_queue_; // IOWriter producer, Router consumer
      predex::utils::SPSCQueue<kalshi::FrameHandle>& recycle_queue_; // logger producer, IOWriter consumer
      std::atomic<std::size_t> received_count_{0};
      std::atomic<std::size_t> dropped_count_{0};
      std::atomic<std::size_t> oversized_count_{0};
      std::atomic<std::uint64_t> recycle_failed_count_{0};
      std::size_t max_batch_size_{kMaxBatchSize};

      std::size_t drain_recycled(std::size_t max_batch_size) noexcept; //returns the number of frames recycled
      static std::uint64_t monotonic_now_ns() noexcept; //convenience method for getting current time in nanoseconds, used for telemetry and logging

  };   
}