#pragma once

#include "predex/internal/event_topology.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <simdjson.h>
#include <vector>

namespace predex::core::ingest::kalshi {
constexpr std::size_t kMaxFrameBytes = 8192;
constexpr std::size_t kSimdJsonPadding = simdjson::SIMDJSON_PADDING;
struct KalshiFrame {
    std::uint64_t recv_ts_ns_;
    std::uint32_t len_;
    std::uint32_t flags_;
    std::array<std::byte, kMaxFrameBytes + kSimdJsonPadding> payload;
};

enum class KalshiEventType : std::uint8_t {
    kTrade = 0,
    kDelta = 1,
    kSnapshot = 2,
    kSubscribed = 3,
    kLifecycle = 4,
    kUnknown = 255
};

struct FrameHandle {
    std::uint64_t seq_{0};
    std::uint32_t sid_{0};
    std::uint32_t idx_{0};
    std::uint32_t gen_{0};
    std::uint32_t market_id_{0};
    std::uint32_t event_id_{0};
    std::uint16_t affinity_key_{0};
    internal::EventTopologyKind topology_kind_{internal::EventTopologyKind::kUnknown};
    KalshiEventType event_type_{KalshiEventType::kUnknown};
};

class FramePool {
  public:
    explicit FramePool(std::size_t capacity);
    ~FramePool();

    // never want to copy/move framepool
    const FramePool& operator=(const FramePool&) = delete;
    FramePool(const FramePool&) = delete;
    FramePool(FramePool&&) = delete;
    FramePool& operator=(FramePool&&) = delete;
    // producer (IO) side:
    [[nodiscard]] bool try_acquire(FrameHandle& handle_out) noexcept;
    [[nodiscard]] KalshiFrame* writable_frame(const FrameHandle& handle) noexcept;

    [[nodiscard]] bool recycle(const FrameHandle& handle) noexcept;

    // consumer side (Router/Shard/Logger):
    [[nodiscard]] const KalshiFrame* frame(const FrameHandle& handle) const noexcept;

  private:
    const std::size_t capacity_;
    KalshiFrame* pool_{nullptr};
    std::vector<std::uint32_t> free_slots_;
    std::vector<std::uint32_t> generations_;
    std::vector<std::uint8_t> in_use_;
};

} // namespace predex::core::ingest::kalshi