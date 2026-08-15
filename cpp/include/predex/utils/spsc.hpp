#pragma once 
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <utility>
#include <type_traits>

namespace predex::utils{
    /*
        SPSC lock-free implementation. 

        T must be move-assignable for try_pop to work, and must be constructible with the provided
        arguments for try_emplace to work.
    
    */

#if defined(__cpp_lib_hardware_interference_size) && __cpp_lib_hardware_interference_size >= 201703L
inline constexpr std::size_t k_destructive_interference_size =
    std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t k_destructive_interference_size = 64;
#endif

    struct alignas(k_destructive_interference_size) PaddedIndex {
        std::atomic<std::uint64_t> v{0};
        std::array<char, k_destructive_interference_size - sizeof(std::atomic<std::uint64_t>)> padding{};
    };

    template <typename T>
    class SPSCQueue{
        public:
        static_assert(std::is_move_assignable_v<T>, "T must be move-assignable");
            explicit SPSCQueue(std::size_t capacity):
                capacity_(capacity),
                mask_(capacity - 1),
                buffer_(nullptr)
            {
                if((capacity == 0) || (capacity & (capacity - 1)) !=0){
                    throw std::invalid_argument("Capacity must be a power of 2 and greater than 0");
                }

                buffer_ = 
                    static_cast<T*>(
                        ::operator new[](capacity * sizeof(T), std::align_val_t{k_buffer_alignment})
                    );
            }

            ~SPSCQueue(){
                std::uint64_t curr_head = head_.v.load(std::memory_order_relaxed);
                std::uint64_t curr_tail = tail_.v.load(std::memory_order_relaxed);

                while(curr_head != curr_tail){
                    (buffer_ + (curr_head & mask_))->~T();
                    curr_head++;
                }

                ::operator delete[](buffer_, std::align_val_t{k_buffer_alignment});
            }
            //copy and move operators & constructors are deleted
            SPSCQueue& operator=(const SPSCQueue&) = delete;
            SPSCQueue(const SPSCQueue&) = delete;
            
            SPSCQueue& operator=(SPSCQueue&&) = delete;
            SPSCQueue(SPSCQueue&&) = delete;

        template <typename... Args> 
        bool try_emplace(Args&&... args) {
            const std::uint64_t tail = producer_tail_;

            if (tail - producer_cached_head_ == capacity_) {
                producer_cached_head_ = head_.v.load(std::memory_order_acquire);

                if (tail - producer_cached_head_ == capacity_) {
                    return false;
                }
            }

            T* slot = buffer_ + (tail & mask_);
            ::new (static_cast<void*>(slot)) T(std::forward<Args>(args)...);

            const std::uint64_t next_tail = tail + 1;
            producer_tail_ = next_tail;
            tail_.v.store(next_tail, std::memory_order_release);

            return true;
        }

        bool try_pop(T& item) {
            const std::uint64_t head = consumer_head_;

            if (head == consumer_cached_tail_) {
                consumer_cached_tail_ = tail_.v.load(std::memory_order_acquire);

                if (head == consumer_cached_tail_) {
                    return false;
                }
            }

            T* slot = buffer_ + (head & mask_);
            item = std::move(*slot);
            slot->~T();

            const std::uint64_t next_head = head + 1;
            consumer_head_ = next_head;
            head_.v.store(next_head, std::memory_order_release);

            return true;
        }

            bool try_push(const T& item){
                return try_emplace(item);
            }
        bool try_push(T&& item){
            return try_emplace(std::move(item));
        }

        [[nodiscard]] std::size_t capacity() const noexcept{
            return capacity_;
        }

        // Producer-side diagnostic. The acquire load makes this an exact
        // snapshot at the instant the consumer head is observed.
        [[nodiscard]] std::size_t producer_size() const noexcept{
            const std::uint64_t head = head_.v.load(std::memory_order_acquire);
            return static_cast<std::size_t>(producer_tail_ - head);
        }

        private:
            const std::size_t capacity_;
            const std::size_t mask_;
            
            PaddedIndex tail_;
            PaddedIndex head_;
            
            alignas(k_destructive_interference_size) std::uint64_t consumer_head_{0};
            std::uint64_t consumer_cached_tail_{0};

            alignas(k_destructive_interference_size) std::uint64_t producer_tail_{0};
            std::uint64_t producer_cached_head_{0};
            

            T* buffer_;
            static constexpr std::size_t k_buffer_alignment = k_destructive_interference_size > alignof(T)
                ? k_destructive_interference_size
                : alignof(T);

    };

}
