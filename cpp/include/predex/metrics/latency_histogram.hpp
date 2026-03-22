#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>

namespace trading::metrics {

struct LatencyPercentiles {
    std::uint64_t count{0};
    std::uint64_t p50_ns{0};
    std::uint64_t p95_ns{0};
    std::uint64_t p99_ns{0};
    std::uint64_t max_ns{0};
};

class AtomicLatencyHistogram {
  public:
    static constexpr std::size_t kBucketCount = 64;

    void observe(std::uint64_t latency_ns) noexcept {
        const std::size_t bucket_index = bucket_for(latency_ns);
        buckets_[bucket_index].fetch_add(1, std::memory_order_relaxed);

        std::uint64_t max_seen = max_ns_.load(std::memory_order_relaxed);
        while (latency_ns > max_seen &&
               !max_ns_.compare_exchange_weak(max_seen, latency_ns, std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
        }
    }

    [[nodiscard]] LatencyPercentiles snapshot() const noexcept {
        std::array<std::uint64_t, kBucketCount> bucket_counts{};
        std::uint64_t total_count = 0;
        for (std::size_t index = 0; index < kBucketCount; ++index) {
            bucket_counts[index] = buckets_[index].load(std::memory_order_relaxed);
            total_count += bucket_counts[index];
        }

        if (total_count == 0) {
            return LatencyPercentiles{};
        }

        const std::uint64_t p50_rank = percentile_rank(total_count, 50U);
        const std::uint64_t p95_rank = percentile_rank(total_count, 95U);
        const std::uint64_t p99_rank = percentile_rank(total_count, 99U);

        std::uint64_t cumulative = 0;
        std::uint64_t p50_ns = 0;
        std::uint64_t p95_ns = 0;
        std::uint64_t p99_ns = 0;
        bool p50_set = false;
        bool p95_set = false;
        bool p99_set = false;
        for (std::size_t index = 0; index < kBucketCount; ++index) {
            cumulative += bucket_counts[index];
            const std::uint64_t bucket_upper_ns = bucket_upper_bound_ns(index);
            if (!p50_set && cumulative >= p50_rank) {
                p50_ns = bucket_upper_ns;
                p50_set = true;
            }
            if (!p95_set && cumulative >= p95_rank) {
                p95_ns = bucket_upper_ns;
                p95_set = true;
            }
            if (!p99_set && cumulative >= p99_rank) {
                p99_ns = bucket_upper_ns;
                p99_set = true;
                break;
            }
        }

        return LatencyPercentiles{
            .count = total_count,
            .p50_ns = p50_ns,
            .p95_ns = p95_ns,
            .p99_ns = p99_ns,
            .max_ns = max_ns_.load(std::memory_order_relaxed),
        };
    }

    void reset() noexcept {
        for (auto& bucket : buckets_) {
            bucket.store(0, std::memory_order_relaxed);
        }
        max_ns_.store(0, std::memory_order_relaxed);
    }

  private:
    static constexpr std::uint64_t kOne = 1ULL;

    static std::size_t bucket_for(std::uint64_t latency_ns) noexcept {
        if (latency_ns == 0) {
            return 0;
        }
        const std::size_t width = std::bit_width(latency_ns);
        return width >= kBucketCount ? (kBucketCount - 1) : width;
    }

    static std::uint64_t bucket_upper_bound_ns(std::size_t bucket_index) noexcept {
        if (bucket_index == 0) {
            return 0;
        }
        if (bucket_index >= (kBucketCount - 1)) {
            return UINT64_MAX;
        }
        return (kOne << bucket_index) - 1ULL;
    }

    static std::uint64_t percentile_rank(std::uint64_t count, std::uint64_t percentile) noexcept {
        const std::uint64_t numerator = count * percentile + 99ULL;
        const std::uint64_t rank = numerator / 100ULL;
        return rank == 0 ? 1ULL : rank;
    }

    std::array<std::atomic<std::uint64_t>, kBucketCount> buckets_{};
    std::atomic<std::uint64_t> max_ns_{0};
};

} // namespace trading::metrics
