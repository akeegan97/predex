#pragma once 
#include <cstddef>
#include <cstdint>

#include "predex/ingest/frame_pool.hpp"
#include "predex/shards/event_store.hpp"
#include "predex/utils/spsc_queue.hpp"
#include "predex/parsers/kalshi/parser.hpp"
#include "predex/shards/event_handler.hpp"

namespace predex::core::shards::kalshi {

  enum class ProcessCode:std::uint8_t{
    kIdle=0, // no more frames to process
    kProcessed=1, // successfully processed frame and applied event update
    kParseFail=2, // failed to parse frame into event
    kApplyFail=3, // failed to apply event update to book store
    kLoggerBackpressure=4, // failed to forward frame to logger due to logger queue backpressure/full
    kFrameMissing=5,
    kHandlerError=6,
  };

  struct ProcessOneResult{
    ProcessCode code{ProcessCode::kIdle};
    EventApplyCode apply_code{EventApplyCode::kRejected};
    HandlerDecisionCode handler_code{HandlerDecisionCode::kDeclined};
  };

  class Shard{
    //drains spsc queue of frame handles (router owned), processes frames -> decodes actual raw json-> updates internal book state 
    //enqueues 2 things to logger -> framehandle once done with decoding, and book states 
    
    public:
      explicit Shard(
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& input_queue, 
        predex::core::ingest::kalshi::FramePool& frame_pool, 
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue,
        predex::core::parsers::kalshi::Parser parser,
        predex::core::shards::kalshi::EventStore& event_store,
        predex::core::shards::kalshi::IShardEventHandler* event_handler
      ); 

      [[nodiscard]] std::size_t pump(std::size_t max_batch_size) noexcept;
    private:
      predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& input_queue_; // Router producer, Shard consumer
      predex::core::ingest::kalshi::FramePool& frame_pool_; // shared frame pool for zero copy access to frames
      predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue_; // Shard producer, Logger consumer
      predex::core::parsers::kalshi::Parser parser_; 
      predex::core::shards::kalshi::EventStore& event_store_; 
      predex::core::shards::kalshi::IShardEventHandler* event_handler_{nullptr}; // used to invoke/hook strategy invocations/callbacks based on events processed in the shard

      std::uint64_t processed_count_{0};
      std::uint64_t failed_count_{0};
      std::uint64_t parse_fail_count_{0};
      std::uint64_t apply_fail_count_{0};
      std::uint64_t logger_fail_count_{0};

      [[nodiscard]] ProcessOneResult process_one() noexcept;
      [[nodiscard]] bool forward_to_logger(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept; //forwards the frame to logger for persistence, returns false
      [[nodiscard]] const predex::core::ingest::kalshi::KalshiFrame* get_frame(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept; //helper to get frame ptr from frame handle
      [[nodiscard]] ProcessOneResult apply_event(
        const predex::core::ingest::kalshi::FrameHandle& handle, 
        const predex::core::ingest::kalshi::KalshiFrame& frame
      ) noexcept;

  };
}// namespace predex::core::shards::kalshi
