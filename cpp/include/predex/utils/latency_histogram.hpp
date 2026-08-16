#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace predex::utils{

    inline constexpr std::array<std::uint64_t, 22> kLatencyBucketUpperBoundsNs{
        100,
        250,
        500,
        1'000,
        2'500,
        5'000,
        10'000,
        25'000,
        50'000,
        100'000,
        250'000,
        500'000,
        1'000'000,
        2'500'000,
        5'000'000,
        10'000'000,
        25'000'000,
        50'000'000,
        100'000'000,
        250'000'000,
        500'000'000,
        1'000'000'000,
    };

    inline constexpr std::size_t kLatencyOverflowBucket =
        kLatencyBucketUpperBoundsNs.size();
    inline constexpr std::size_t kLatencyBucketCount =
        kLatencyBucketUpperBoundsNs.size() + 1U;

    struct LatencyHistogram{
        std::uint64_t count{};
        std::uint64_t sum_ns{};
        std::uint64_t max_ns{};
        std::array<std::uint64_t, kLatencyBucketCount> buckets{};

        void record(std::uint64_t latency_ns) noexcept{
            if(count != std::numeric_limits<std::uint64_t>::max()){
                ++count;
            }
            sum_ns = saturating_add(sum_ns, latency_ns);
            max_ns = std::max(max_ns, latency_ns);

            const auto iterator = std::lower_bound(
                kLatencyBucketUpperBoundsNs.begin(),
                kLatencyBucketUpperBoundsNs.end(),
                latency_ns);
            const auto index = iterator == kLatencyBucketUpperBoundsNs.end()
                ? kLatencyOverflowBucket
                : static_cast<std::size_t>(
                    iterator - kLatencyBucketUpperBoundsNs.begin());
            if(buckets[index] != std::numeric_limits<std::uint64_t>::max()){
                ++buckets[index];
            }
        }

        void merge(const LatencyHistogram& other) noexcept{
            count = saturating_add(count, other.count);
            sum_ns = saturating_add(sum_ns, other.sum_ns);
            max_ns = std::max(max_ns, other.max_ns);
            for(std::size_t index = 0; index < buckets.size(); ++index){
                buckets[index] = saturating_add(
                    buckets[index],
                    other.buckets[index]);
            }
        }

        [[nodiscard]] std::uint64_t mean_ns() const noexcept{
            return count == 0 ? 0 : sum_ns / count;
        }

        [[nodiscard]] std::uint64_t percentile_upper_bound(
            std::uint32_t basis_points) const noexcept{
            if(count == 0 || basis_points == 0){
                return 0;
            }
            const auto bounded_basis_points =
                std::min<std::uint32_t>(basis_points, 10'000U);
            const std::uint64_t quotient = count / 10'000U;
            const std::uint64_t remainder = count % 10'000U;
            const std::uint64_t target =
                quotient * bounded_basis_points +
                (remainder * bounded_basis_points + 9'999U) / 10'000U;

            std::uint64_t cumulative{};
            for(std::size_t index = 0;
                index < kLatencyBucketUpperBoundsNs.size();
                ++index){
                cumulative = saturating_add(cumulative, buckets[index]);
                if(cumulative >= target){
                    return kLatencyBucketUpperBoundsNs[index];
                }
            }
            return max_ns;
        }

        private:
            [[nodiscard]] static std::uint64_t saturating_add(
                std::uint64_t lhs,
                std::uint64_t rhs) noexcept{
                const auto maximum = std::numeric_limits<std::uint64_t>::max();
                return rhs > maximum - lhs ? maximum : lhs + rhs;
            }
    };

    inline void record_elapsed_ns(
        LatencyHistogram& histogram,
        std::uint64_t start_ns,
        std::uint64_t end_ns) noexcept{
        if(start_ns == 0 || end_ns < start_ns){
            return;
        }
        histogram.record(end_ns - start_ns);
    }

}
