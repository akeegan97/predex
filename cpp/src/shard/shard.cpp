#include "predex/shard/shard.hpp"


namespace predex::shard{

    Shard::Shard(
        std::uint32_t shard_index,
        ShardQueues queues,
        predex::ingest::kalshi::FramePool& frame_pool
    ) : shard_index_(shard_index), queues_(queues), frame_pool_(frame_pool){}

    bool Shard::initialize(std::vector<KalshiEvent> events){
        return event_store_.initialize(std::move(events));
    }

    std::uint32_t Shard::shard_index() const noexcept{
        return shard_index_;
    }

    const ShardStats& Shard::stats() const noexcept{
        return stats_;
    }

    bool Shard::terminal_handoff(const predex::ingest::kalshi::FrameHandle& handle) noexcept{
        if(!queues_.shard_to_logger_queue.try_push(handle)){
            if(!queues_.last_resort_recycle_queue.try_push(handle)){
                ++stats_.leaked_handles;
                return false;
            }
            ++stats_.missed_frames_to_logger;
            return true;
        }
        ++stats_.frames_to_logger;
        return true;
    }

    ShardPumpResult Shard::pump_once() noexcept{
        ShardPumpResult result{};
        predex::ingest::kalshi::FrameHandle handle{};

        if(!queues_.router_to_shard_queue.try_pop(handle)){
            result.code = ShardPumpCode::kIDLE;
            return result;
        }
        ++stats_.frames_seen;

        const predex::ingest::kalshi::KalshiFrame* frame = frame_pool_.frame(handle);

        if(frame == nullptr){
            result.code = ShardPumpCode::kMISSING_FRAME;
            return result;
        }

        KalshiParsedEvent parsed_event{};
        ParseResult parse_result = market_parser_.parse(handle, *frame, parsed_event);
        
        if(!parse_result.success){
            ++stats_.parse_rejects;
            result.code = ShardPumpCode::kPARSE_REJECTED;
            result.parse_result = parse_result;
            if(!terminal_handoff(handle)){
                result.code = ShardPumpCode::kHANDLE_LEAK;
            }
            return result;
        }

        EventApplyResult event_result = event_store_.apply(handle, parsed_event);
        switch(event_result.code){
            case EventApplyCode::kAPPLIED:
                ++stats_.frames_applied;
                result.code = ShardPumpCode::kAPPLIED;
                break;
            case EventApplyCode::kREJECTED:
                ++stats_.event_rejects;
                result.code = ShardPumpCode::kEVENT_REJECTED;
                result.event_result = event_result;
                if(!terminal_handoff(handle)){
                    result.code = ShardPumpCode::kHANDLE_LEAK;
                }
                return result;
            case EventApplyCode::kDESYNCED:
                ++stats_.event_desyncs;
                result.code = ShardPumpCode::kEVENT_DESYNCED;
                result.event_result = event_result;
                if(!terminal_handoff(handle)){
                    result.code = ShardPumpCode::kHANDLE_LEAK;
                }
                return result;
        }
        if(!terminal_handoff(handle)){
            result.code = ShardPumpCode::kHANDLE_LEAK;
            return result;
        }
        return result;
    }

}