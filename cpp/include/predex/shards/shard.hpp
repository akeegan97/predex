#pragma once 
#include <cstddef>
#include <cstdint>
#include "predex/ingest/frame_pool.hpp"
#include "predex/utils/spsc_queue.hpp"


namespace predex::core::kalshi::shard{
  class Shard{
    //drains spsc queue of frame handles (router owned), processes frames -> decodes actual raw json-> updates internal book state 
    //enqueues 2 things to logger -> framehandle once done with decoding, and book states 
    
    public:
      explicit Shard(predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& input_queue, 
        predex::core::ingest::kalshi::FramePool& frame_pool, 
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue) noexcept;

      [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept;
    private:
      predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& input_queue_; // Router producer, Shard consumer
      predex::core::ingest::kalshi::FramePool& frame_pool_; // shared frame pool for zero copy access to frames
      predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue_; // Shard producer, Logger consumer

      std::uint64_t processed_count_{0};
      std::uint64_t failed_count_{0};

      [[nodiscard]] bool process_one() noexcept; //processes one frame from input_queue_, returns false if no more frames to process or if processing failed
      [[nodiscard]] bool forward_to_logger(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept; //forwards the frame to logger for persistence, returns false

  };
}