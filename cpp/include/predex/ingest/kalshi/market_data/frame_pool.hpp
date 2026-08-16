#pragma once 

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <limits>
#include <simdjson.h>
#include <vector>
#include <variant>

#include "predex/control/control_types.hpp"
#include "predex/ingest/kalshi/market_data/integrity_messages.hpp"

namespace predex::ingest::kalshi{

    inline constexpr std::size_t kMaxFrameBytes = 8192;
    inline constexpr std::size_t kFrameParserPadding = simdjson::SIMDJSON_PADDING;

    enum class FrameKind : std::uint8_t {
        kUNKNOWN = 0,
        kORDERBOOK_SNAPSHOT = 1,
        kORDERBOOK_DELTA = 2,
        kTRADE = 3,
        kSUBSCRIPTION_ACK = 4,
        kUNSUBSCRIBED = 5,
        kLIFECYCLE = 6,
        kHEARTBEAT = 7,
    };

    [[nodiscard]] inline constexpr std::size_t market_data_channel_index(
        FrameKind kind) noexcept{
        switch(kind){
            case FrameKind::kORDERBOOK_SNAPSHOT:
            case FrameKind::kORDERBOOK_DELTA:
                return 0;
            case FrameKind::kTRADE:
                return 1;
            case FrameKind::kLIFECYCLE:
                return 2;
            case FrameKind::kUNKNOWN:
            case FrameKind::kSUBSCRIPTION_ACK:
            case FrameKind::kUNSUBSCRIBED:
            case FrameKind::kHEARTBEAT:
                return predex::core::control::kMarketDataChannelCount;
        }
        return predex::core::control::kMarketDataChannelCount;
    }

    struct KalshiFrame {
        std::uint64_t recv_ts_ns{};
        std::uint32_t len{};
        std::uint32_t flags{};
        std::array<std::byte, kMaxFrameBytes + kFrameParserPadding> payload{};
    };
    
    struct FrameHandle{
        std::uint64_t universe_version{};
        std::uint64_t sequence{};
        predex::core::control::RecoveryId recovery_id{};
        std::uint64_t ingress_ts_ns{};
        std::uint64_t wire_publish_ts_ns{};
        std::uint64_t router_publish_ts_ns{};
        std::uint64_t shard_dequeue_ts_ns{};
        std::uint64_t shard_publish_ts_ns{};

        std::uint32_t sid{};
        std::uint32_t pool_index{};
        std::uint32_t pool_generation{};

        predex::core::control::MarketId market_id{};
        predex::core::control::EventId event_id{};
        predex::core::control::AffinityKey affinity_key{};
        predex::core::control::EventTopology topology{predex::core::control::EventTopology::kUNKNOWN};
        std::uint32_t shard_index{};
        std::uint32_t shard_event_index{};
        std::uint32_t event_market_index{};

        FrameKind kind{FrameKind::kUNKNOWN};
    };

    using MarketDataPathMessage = std::variant<FrameHandle, predex::ingest::kalshi::MarketInvalidationBarrier, predex::ingest::kalshi::OrderBookSubscriptionInvalidationBarrier>;
    
    
    class FramePool{
        public:
            explicit FramePool(std::size_t capacity)
                : capacity_(capacity),
                  frames_(capacity),
                  generations_(capacity, 1U),
                  in_use_(capacity, 0U) {
                if (capacity_ > std::numeric_limits<std::uint32_t>::max()) {
                    throw std::invalid_argument("FramePool capacity exceeds uint32_t handle range");
                }
                free_slots_.reserve(capacity_);
                for (std::size_t slot = capacity_; slot > 0; --slot) {
                    free_slots_.push_back(static_cast<std::uint32_t>(slot - 1U));
                }
            }

            FramePool(const FramePool&) = delete;
            FramePool& operator=(const FramePool&) = delete;
            FramePool(FramePool&&) = delete;
            FramePool& operator=(FramePool&&) = delete;

            [[nodiscard]] std::size_t capacity() const noexcept {
                return capacity_;
            }

            [[nodiscard]] std::size_t available() const noexcept {
                return free_slots_.size();
            }

            [[nodiscard]] bool try_acquire(FrameHandle& handle_out) noexcept {
                if (free_slots_.empty()) {
                    return false;
                }

                const std::uint32_t slot = free_slots_.back();
                free_slots_.pop_back();
                assert(slot < in_use_.size());
                assert(in_use_[slot] == 0U);

                in_use_[slot] = 1U;
                frames_[slot] = KalshiFrame{};

                handle_out = FrameHandle{};
                handle_out.pool_index = slot;
                handle_out.pool_generation = generations_[slot];
                return true;
            }

            [[nodiscard]] KalshiFrame* writable_frame(const FrameHandle& handle) noexcept {
                if (!is_live_handle(handle)) {
                    return nullptr;
                }
                return &frames_[handle.pool_index];
            }

            [[nodiscard]] const KalshiFrame* frame(const FrameHandle& handle) const noexcept {
                if (!is_live_handle(handle)) {
                    return nullptr;
                }
                return &frames_[handle.pool_index];
            }

            [[nodiscard]] bool recycle(const FrameHandle& handle) noexcept {
                if (!is_live_handle(handle)) {
                    return false;
                }

                const std::uint32_t slot = handle.pool_index;
                in_use_[slot] = 0U;
                ++generations_[slot];
                if (generations_[slot] == 0U) {
                    generations_[slot] = 1U;
                }
                free_slots_.push_back(slot);
                return true;
            }

        private:
            [[nodiscard]] bool is_live_handle(const FrameHandle& handle) const noexcept {
                if (handle.pool_index >= frames_.size()) {
                    return false;
                }
                if (in_use_[handle.pool_index] == 0U) {
                    return false;
                }
                return generations_[handle.pool_index] == handle.pool_generation;
            }

            std::size_t capacity_{};
            std::vector<KalshiFrame> frames_;
            std::vector<std::uint32_t> free_slots_;
            std::vector<std::uint32_t> generations_;
            std::vector<std::uint8_t> in_use_;

    };
}
