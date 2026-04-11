#include "predex/shards/shard.hpp"
#include "predex/shards/event_store.hpp"


namespace predex::core::shards::kalshi{

    Shard::Shard(predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& input_queue, 
        predex::core::ingest::kalshi::FramePool& frame_pool, 
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue,
        predex::core::parsers::kalshi::Parser parser,
        predex::core::shards::kalshi::EventStore& event_store,
        predex::core::shards::kalshi::IShardEventHandler* event_handler) :
        input_queue_(input_queue),
        frame_pool_(frame_pool),
        logger_queue_(logger_queue),
        parser_(parser),
        event_store_(event_store),
        event_handler_(event_handler)
    {}

    std::size_t Shard::pump(std::size_t max_batch_size) noexcept{
        std::size_t processed = 0;
        while (processed < max_batch_size) {
            const ProcessOneResult result = process_one();
            switch (result.code) {
                case ProcessCode::kIdle:
                    return processed;
                case ProcessCode::kProcessed:
                    ++processed;
                    break;
                case ProcessCode::kParseFail:
                case ProcessCode::kApplyFail:
                case ProcessCode::kFrameMissing:
                case ProcessCode::kHandlerError:
                    break;
                case ProcessCode::kLoggerBackpressure:
                    return processed;
                default:
                    ++failed_count_;
                    return processed;
            }
        }
        return processed;
    }

    ProcessOneResult Shard::process_one() noexcept{
        predex::core::ingest::kalshi::FrameHandle handle{};
        if(!input_queue_.try_pop(handle)){
            return ProcessOneResult{};
        }
        const predex::core::ingest::kalshi::KalshiFrame* frame = get_frame(handle);
        if(frame == nullptr){
            ++failed_count_;
            return ProcessOneResult{
                .code = ProcessCode::kFrameMissing,
                .apply_code = EventApplyCode::kRejected,
                .handler_code = HandlerDecisionCode::kDeclined,
            };
        }
        ProcessOneResult result = apply_event(handle, *frame);
        if(!forward_to_logger(handle)){
            ++logger_fail_count_;
            result.code = ProcessCode::kLoggerBackpressure;
            return result;
        }

        if (result.code == ProcessCode::kProcessed) {
            ++processed_count_;
        }
        return result;
    }

    ProcessOneResult Shard::apply_event(const predex::core::ingest::kalshi::FrameHandle& handle, 
      const predex::core::ingest::kalshi::KalshiFrame& frame) noexcept{
        auto parse_result = parser_.parse(handle,frame);
        if(!parse_result.ok()){
            ++parse_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kParseFail,
                .apply_code = EventApplyCode::kParseFail,
                .handler_code = HandlerDecisionCode::kDeclined,
            };
        }
        if(!parse_result.value().has_value()){
            ++parse_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kParseFail,
                .apply_code = EventApplyCode::kParseFail,
                .handler_code = HandlerDecisionCode::kDeclined,
            };
        }

        const auto& event = *parse_result.value();
        auto* stored_event = event_store_.find(event.meta.event_id);
        if(stored_event == nullptr){
            ++apply_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kApplyFail,
                .apply_code = EventApplyCode::kRejected,
                .handler_code = HandlerDecisionCode::kDeclined,
            };
        }

        const EventApplyCode apply_result = stored_event->apply_market_update(event);
        if(apply_result != EventApplyCode::kApplied){
            ++apply_fail_count_;
            return ProcessOneResult{
                .code = ProcessCode::kApplyFail,
                .apply_code = apply_result,
                .handler_code = HandlerDecisionCode::kDeclined,
            };
        }

        if(event_handler_!= nullptr){
            const AppliedEventUpdate update{event, *stored_event};
            const HandlerDecisionCode handler_decision = event_handler_->on_event(update);
            if (handler_decision == HandlerDecisionCode::kError) {
                ++failed_count_;
                return ProcessOneResult{
                    .code = ProcessCode::kHandlerError,
                    .apply_code = apply_result,
                    .handler_code = handler_decision,
                };
            }
            return ProcessOneResult{
                .code = ProcessCode::kProcessed,
                .apply_code = apply_result,
                .handler_code = handler_decision,
            };
        }

        return ProcessOneResult{
            .code = ProcessCode::kProcessed,
            .apply_code = apply_result,
            .handler_code = HandlerDecisionCode::kDeclined,
        };
    }
    
     bool Shard::forward_to_logger(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        return logger_queue_.try_push(handle);
    }
    const predex::core::ingest::kalshi::KalshiFrame* Shard::get_frame(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        return frame_pool_.frame(handle);
    }


}
