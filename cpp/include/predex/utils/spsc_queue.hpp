#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <new>
#include <utility>
#include <cstdint>
#include <stdexcept>
namespace predex::utils{
    // A single-producer, single-consumer lock-free queue implementation.
    // T must be move-assignable for try_pop to work, and must be constructible with the provided arguments for try_emplace to work.
constexpr std::size_t CACHE_LINE_SIZE = 64;

struct alignas(CACHE_LINE_SIZE) PaddedIndex{
    std::atomic<std::uint64_t> v{0};
    char padding[CACHE_LINE_SIZE - sizeof(std::atomic<std::uint64_t>)];
};


template<typename T>
class SPSCQueue{
    public:
        explicit SPSCQueue(std::size_t capacity):
            capacity_(capacity),
            mask_(capacity - 1),
            buffer_(nullptr){
                if((capacity == 0) || (capacity & (capacity - 1)) != 0) {
                    throw std::invalid_argument("Capacity must be a power of 2 and greater than 0");
                }
                buffer_ = static_cast<T*>(
                    ::operator new[](capacity * sizeof(T), std::align_val_t(alignof(T)))
                );
            }
        ~SPSCQueue(){
            std::uint64_t curr_head = head_.v.load(std::memory_order_relaxed);
            std::uint64_t curr_tail = tail_.v.load(std::memory_order_relaxed);
            while(curr_head != curr_tail){
                (buffer_ + (curr_head & mask_))->~T();
                curr_head++;
            }
            ::operator delete[](buffer_, std::align_val_t(alignof(T)));
        }
        SPSCQueue& operator=(const SPSCQueue&) = delete;
        SPSCQueue(const SPSCQueue&) = delete;
        SPSCQueue& operator=(SPSCQueue&&) = delete;
        SPSCQueue(SPSCQueue&&) = delete;

        template<typename...Args>
        bool try_emplace(Args&&...args){
            std::uint64_t curr_tail = tail_.v.load(std::memory_order_relaxed);
            if(curr_tail - head_.v.load(std::memory_order_acquire)==capacity_){
                return false;
            }
            
            T* slot = buffer_ + (curr_tail & mask_);
            void* memory = static_cast<void*>(slot);

            ::new (memory) T(std::forward<Args>(args)...);
            tail_.v.store(curr_tail + 1, std::memory_order_release);
            return true;
        }
        bool try_pop(T& item){
            std::uint64_t curr_head = head_.v.load(std::memory_order_relaxed);
            if(curr_head == tail_.v.load(std::memory_order_acquire)){
                return false;
            }
            T* slot = buffer_ + (curr_head & mask_);
            item = std::move(*slot);
            slot->~T();
            head_.v.store(curr_head + 1, std::memory_order_release);
            return true;
        }
        bool try_push(const T& item){
            return try_emplace(item);
        }
        bool try_push(T&& item){
            return try_emplace(std::move(item));
        }

    private: 
        const std::size_t capacity_;
        const std::size_t mask_;

        T* buffer_;
        PaddedIndex tail_;
        PaddedIndex head_;
};
}// namespace predex::utils