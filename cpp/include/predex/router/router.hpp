#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include "predex/utils/spsc_queue.hpp"
#include "predex/ingest/frame_pool.hpp"





namespace predex::core::routing{
    struct RouterTelemetry{
        std::size_t processed_frames_{0};
        std::size_t dropped_frames_{0};
    };
    class Router{
        public:
            explicit Router(predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle> router_queue,
                predex::core::ingest::kalshi::FramePool& frame_pool,
                predex::utils::SPSCQueue<RouterTelemetry>& telemetry_queue) noexcept;

            [[nodiscard]] bool pump(size_t max_batch_size) noexcept;


        private:
            predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle> router_queue_;
            predex::core::ingest::kalshi::FramePool& frame_pool_;
            predex::utils::SPSCQueue<RouterTelemetry>& telemetry_queue_;
    };
}