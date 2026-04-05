#include "predex/router/shard_dispatch.hpp"
#include <atomic>

namespace predex::core::routing::kalshi{
    ShardDispatch::ShardDispatch(std::vector<Queue*> shard_queues) : shard_queues_(std::move(shard_queues)) {}

    bool ShardDispatch::try_dispatch(std::size_t shard_id, const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        if(shard_id >= shard_queues_.size()){
            ++dropped_count_;
            return false; //invalid shard id
        }
        if(shard_queues_[shard_id] == nullptr){
            ++dropped_count_;
            return false; //no queue for this shard
        }
        if(!shard_queues_[shard_id]->try_push(handle)){
            ++dropped_count_;
            return false; //failed to push to shard queue, could be temporarily full
        }
        ++dispatched_count_;
        return true;
    }

    ShardDispatchStats ShardDispatch::stats() const noexcept{
        return ShardDispatchStats{dispatched_count_.load(std::memory_order_relaxed), dropped_count_.load(std::memory_order_relaxed)};
    }

    std::size_t ShardDispatch::shard_count() const noexcept{
        return shard_queues_.size();
    }
}