#include "predex/ingest/frame_pool.hpp"
#include <cassert>
#include <limits>


namespace predex::core::ingest::kalshi{
    FramePool::FramePool(std::size_t capacity):
        capacity_(capacity)
        {
            assert(capacity <= std::numeric_limits<std::uint32_t>::max() && "FramePool capacity exceeds maximum supported size due to uint32_t indexing");
            //initialize vectors once with reserve capacity and then use push_back to maintain size invariants with pool
            free_slots_.reserve(capacity);
            generations_.reserve(capacity);
            in_use_.reserve(capacity);

            //warm up vectors with initial values
            for(std::uint32_t i=0; i<capacity; ++i){
                free_slots_.push_back(i);
                generations_.push_back(1);
                in_use_.push_back(0);
            }
            pool_ = new KalshiFrame[capacity_];

        }
    
    FramePool::~FramePool(){
        delete[] pool_;
    }
    bool FramePool::try_acquire(FrameHandle& handle_out)noexcept{
        if(free_slots_.empty()){
            return false;
        }
        std::uint32_t idx = free_slots_.back();
        if(idx >= capacity_){
            assert(false && "FramePool invariant violated: free slot index out of bounds");
            return false;
        }
        if(in_use_[idx]!=0){
            assert(false && "FramePool invariant violated: free slot marked in use");
            return false;
        }

        free_slots_.pop_back();
        in_use_[idx] = 1;
        pool_[idx] = KalshiFrame{}; //zero out frame memory for safety, might want to optimize this later by only zeroing out relevant fields or relying on caller to zero out after acquiring
        handle_out = FrameHandle{};
        handle_out.idx_ = idx;
        handle_out.gen_ = generations_[idx];
        

        return true;
    }
    KalshiFrame* FramePool::writable_frame(const FrameHandle& handle) noexcept{
        if(handle.idx_ >= capacity_){
            assert(false && "FramePool invariant violated: handle index out of bounds");
            return nullptr;
        }
        if(in_use_[handle.idx_] == 0){
            assert(false && "FramePool invariant violated: handle index not in use");
            return nullptr;
        }
        if(generations_[handle.idx_] != handle.gen_){
            assert(false && "FramePool invariant violated: handle generation mismatch, possible use after free");
            return nullptr;
        }
        return &pool_[handle.idx_];
    }
    const KalshiFrame* FramePool::frame(const FrameHandle& handle) const noexcept{
        if(handle.idx_ >= capacity_){
            assert(false && "FramePool invariant violated: handle index out of bounds");
            return nullptr;
        }
        if(in_use_[handle.idx_] == 0){
            assert(false && "FramePool invariant violated: handle index not in use");
            return nullptr;
        }
        if(generations_[handle.idx_] != handle.gen_){
            assert(false && "FramePool invariant violated: handle generation mismatch, possible use after free");
            return nullptr;
        }
        return &pool_[handle.idx_];
    }
    bool FramePool::recycle(const FrameHandle& handle) noexcept{
        if(handle.idx_ >= capacity_){
            assert(false && "FramePool invariant violated: handle index out of bounds");
            return false;
        }
        if(in_use_[handle.idx_] == 0){
            assert(false && "FramePool invariant violated: handle index not in use");
            return false;
        }
        if(generations_[handle.idx_] != handle.gen_){
            assert(false && "FramePool invariant violated: handle generation mismatch, possible double free");
            return false;
        }

        in_use_[handle.idx_] = 0;

        ++generations_[handle.idx_];
        if (generations_[handle.idx_] == 0) {
            generations_[handle.idx_] = 1;
        }
        free_slots_.push_back(handle.idx_);
        return true;
    }
}