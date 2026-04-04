#include "predex/ingest/io_writer.hpp"
#include "predex/ingest/frame_pool.hpp"
#include <cstring>
#include <chrono>
namespace predex::core::ingest::io{
  IOWriter::IOWriter(predex::core::ingest::kalshi::FramePool& frame_pool, 
                     predex::utils::SPSCQueue<kalshi::FrameHandle>& router_queue,
                     predex::utils::SPSCQueue<kalshi::FrameHandle>& recycle_queue) noexcept
    : frame_pool_(frame_pool), router_queue_(router_queue), recycle_queue_(recycle_queue) {}

    bool IOWriter::on_wire_message(std::string_view payload) noexcept{
        ++received_count_;
        if(payload.size() > predex::core::ingest::kalshi::kMaxFrameBytes){
            ++oversized_count_;
            return false; // drop frame if payload is too large to fit in a frame, could extend this to have a separate queue for oversized frames if we want to keep them for analysis
        }
        //try and acquire handle from pool 
        kalshi::FrameHandle handle{};
        if(!frame_pool_.try_acquire(handle)){
            //try and drain recycle pool to free up handles
            drain_recycled(max_batch_size_);
            if(!frame_pool_.try_acquire(handle)){
                ++dropped_count_;
                return false; // drop frame if we can't acquire a handle even after draining recycled frames,
            }
        }
        //copy payload into frame
        if(auto* frame = frame_pool_.writable_frame(handle)){
            frame->recv_ts_ns_ = monotonic_now_ns();
            frame->len_ = static_cast<std::uint32_t>(payload.size());
            frame->flags_ = 0; // can set flags based on message type or other
            std::memcpy(frame->payload, payload.data(), payload.size());
            //enqueue handle for router to process
            if(!router_queue_.try_push(handle)){
                // if router queue is full, we return false, catastrophic backpressure we must fail.
                ++dropped_count_;
                // TODO would be to add retry/yield but for now we want to fail fast
                if(!frame_pool_.recycle(handle)){
                    ++recycle_failed_count_; // failed to recycle frame back into pool, could be due to invalid handle or double recycle, log this for analysis
                }
                return false;
            }
            return true;
        }
        if(!frame_pool_.recycle(handle)){
            ++recycle_failed_count_; // failed to recycle frame back into pool, could be due to invalid handle or double recycle, log this for analysis
        }
        dropped_count_++;
        return false;
        
    }

    std::size_t IOWriter::drain_recycled(std::size_t max_batch_size) noexcept{
        std::size_t recycled_count = 0;
        kalshi::FrameHandle handle{};
        while(recycled_count < max_batch_size && recycle_queue_.try_pop(handle)){
            if(frame_pool_.recycle(handle)){
                ++recycled_count;
            } else {
                ++recycle_failed_count_; // failed to recycle frame back into pool, could be due to invalid handle or double recycle, log this for analysis
            }
        }
        return recycled_count;
    }
    std::uint64_t IOWriter::monotonic_now_ns() noexcept{
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }
}