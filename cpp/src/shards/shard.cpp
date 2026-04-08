#include "predex/shards/shard.hpp"


namespace predex::core::shards::kalshi{

    Shard::Shard(predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& input_queue, 
        predex::core::ingest::kalshi::FramePool& frame_pool, 
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue,
        predex::core::parsers::kalshi::Parser parser,
        predex::core::shards::kalshi::BookStore& book_store,
        predex::core::shards::kalshi::IShardEventHandler* event_handler) :
        input_queue_(input_queue),
        frame_pool_(frame_pool),
        logger_queue_(logger_queue),
        parser_(parser),
        book_store_(book_store),
        event_handler_(event_handler)
    {}

    std::size_t Shard::pump(std::size_t max_batch_size) noexcept{
        std::size_t processed = 0;
        while (processed < max_batch_size && process_one()) {
            ++processed;
        }
        return processed;
    }

    bool Shard::process_one() noexcept{
        //might want to convert this to enum code instead of bool for better failure mode visibility since 
        //false can be both for no more frames to process or processing failures, but for now keeping it simple with bool
        predex::core::ingest::kalshi::FrameHandle handle{};
        if(!input_queue_.try_pop(handle)){
            return false; // no more frames to process
        }
        const predex::core::ingest::kalshi::KalshiFrame* frame = get_frame(handle);
        if(frame == nullptr){
            ++failed_count_;
            return false; // failed to get frame from handle
        }
        ApplyResult result = apply_event(handle, *frame);
        if(!forward_to_logger(handle)){
            ++logger_fail_count_;
            return false; // failed to forward to logger, could extend this to have a retry mechanism if Logger SPSC queue is temporarily full,
        }
        switch(result){
            case ApplyResult::kApplied:
            case ApplyResult::kHandlerAccept:
                ++processed_count_;
                return true;
            case ApplyResult::kRejected:
                return false;
            case ApplyResult::kHandlerReject:
                ++processed_count_;
                return true;
            case ApplyResult::kParseFail:
                return false;
            default:
                // Handle unexpected enum values
                ++failed_count_;
                return false;
        }
        return true;
    }

    ApplyResult Shard::apply_event(const predex::core::ingest::kalshi::FrameHandle& handle, 
      const predex::core::ingest::kalshi::KalshiFrame& frame) noexcept{
        auto parse_result = parser_.parse(handle,frame);
        if(!parse_result.ok()){
            ++parse_fail_count_;
            return ApplyResult::kParseFail; // failed to parse frame
        }
        if(!parse_result.value().has_value()){
            ++parse_fail_count_;
            return ApplyResult::kParseFail; // parsed frame but no event produced, could be due to unsupported message type or other parsing issues
        }
        //NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        const auto& event = *parse_result.value();
        auto apply_result = book_store_.apply_with_result(event);
        if(!apply_result.applied){
            ++apply_fail_count_;
            return ApplyResult::kRejected; // failed to apply event to book store
        }
        if(event_handler_!= nullptr){
            return event_handler_->on_event(event) ? ApplyResult::kHandlerAccept : ApplyResult::kHandlerReject; 

        }
        return ApplyResult::kApplied;
    }
    
     bool Shard::forward_to_logger(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        return logger_queue_.try_push(handle);
    }
    const predex::core::ingest::kalshi::KalshiFrame* Shard::get_frame(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        return frame_pool_.frame(handle);
    }


}