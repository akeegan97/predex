#pragma once

#include <chrono>
#include <cstdint>

namespace predex::utils{

    static_assert(std::chrono::steady_clock::is_steady);

    [[nodiscard]] inline std::uint64_t monotonic_now_ns() noexcept{
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }

}
