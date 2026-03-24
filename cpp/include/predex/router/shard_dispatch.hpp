#pragma once 

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "predex/ingest/frame_pool.hpp"
#include "predex/utils/spsc_queue.hpp"


namespace predex::core::routing{
    struct ShardDispatchStats{
        std::uint64_t dispatched_{0};
        std::uint64_t dropped_{0};
    };
    class ShardDispatch{
        public:
        using Queue = predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>;
        explicit ShardDispatch(std::vector<Queue*> shard_queues);

        ShardDispatch(const ShardDispatch&) = delete;
        ShardDispatch& operator=(const ShardDispatch&) = delete;
        ShardDispatch(ShardDispatch&&) = delete;
        ShardDispatch& operator=(ShardDispatch&&) = delete;

        [[nodiscard]] bool try_dispatch(std::size_t shard_id, const predex::core::ingest::kalshi::FrameHandle& handle) noexcept;
        [[nodiscard]] ShardDispatchStats stats() const noexcept;
        [[nodiscard]] std::size_t shard_count() const noexcept;

        private:
        std::vector<Queue*> shard_queues_;
        std::atomic<std::uint64_t> dispatched_count_{0};
        std::atomic<std::uint64_t> dropped_count_{0};
    };
}