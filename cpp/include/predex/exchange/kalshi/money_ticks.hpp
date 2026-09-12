#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>

namespace predex::exchange::kalshi {

enum class MoneyTickRounding : std::uint8_t {
    kTOWARD_ZERO,
    kAWAY_FROM_ZERO,
    kFLOOR,
};

// Kalshi portfolio dollar fields may contain up to six fractional digits,
// while PredEx stores money in $0.0001 ticks. Convert at the venue boundary
// with a caller-selected rounding policy rather than changing the internal
// monetary scale.
[[nodiscard]] inline std::optional<std::int64_t> parse_money_ticks(
    std::string_view value,
    MoneyTickRounding rounding) noexcept {

    constexpr std::uint64_t kMONEY_TICKS_PER_DOLLAR = 10'000;
    constexpr std::size_t kMONEY_TICK_DECIMAL_PLACES = 4;
    constexpr std::size_t kMAX_WIRE_DECIMAL_PLACES = 6;

    if(value.empty()) {
        return std::nullopt;
    }

    bool negative = false;
    if(value.front() == '-' || value.front() == '+') {
        negative = value.front() == '-';
        value.remove_prefix(1);
    }
    if(value.empty()) {
        return std::nullopt;
    }

    const std::size_t decimal_position = value.find('.');
    const std::string_view whole_text =
        decimal_position == std::string_view::npos
            ? value
            : value.substr(0, decimal_position);
    const std::string_view fractional_text =
        decimal_position == std::string_view::npos
            ? std::string_view{}
            : value.substr(decimal_position + 1);

    if(whole_text.empty() ||
    fractional_text.size() > kMAX_WIRE_DECIMAL_PLACES) {
        return std::nullopt;
    }

    std::uint64_t whole{};
    for(const char ch : whole_text) { //NOLINT
        if(ch < '0' || ch > '9') {
            return std::nullopt;
        }
        const auto digit = static_cast<std::uint64_t>(ch - '0');
        if(whole >
        (std::numeric_limits<std::uint64_t>::max() - digit) / 10) { //NOLINT
            return std::nullopt;
        }
        whole = whole * 10 + digit; //NOLINT
    }

    std::uint64_t fractional_ticks{};
    bool discarded_nonzero = false;
    for(std::size_t index = 0; index < fractional_text.size(); ++index) {
        const char ch = fractional_text[index]; //NOLINT
        if(ch < '0' || ch > '9') {
            return std::nullopt;
        }
        if(index < kMONEY_TICK_DECIMAL_PLACES) {
            fractional_ticks =
                fractional_ticks * 10 + //NOLINT
                static_cast<std::uint64_t>(ch - '0');
        } else if(ch != '0') {
            discarded_nonzero = true;
        }
    }
    for(std::size_t index = fractional_text.size();
        index < kMONEY_TICK_DECIMAL_PLACES;
        ++index) {
        fractional_ticks *= 10; //NOLINT
    }

    const auto max_magnitude = static_cast<std::uint64_t>(
        std::numeric_limits<std::int64_t>::max());
    if(whole >
    (max_magnitude - fractional_ticks) / kMONEY_TICKS_PER_DOLLAR) {
        return std::nullopt;
    }

    std::uint64_t magnitude =
        whole * kMONEY_TICKS_PER_DOLLAR + fractional_ticks;
    const bool increment_magnitude = discarded_nonzero &&
        (rounding == MoneyTickRounding::kAWAY_FROM_ZERO ||
         (rounding == MoneyTickRounding::kFLOOR && negative));
    if(increment_magnitude) {
        if(magnitude == max_magnitude) {
            return std::nullopt;
        }
        ++magnitude;
    }

    const auto signed_magnitude = static_cast<std::int64_t>(magnitude);
    return negative ? -signed_magnitude : signed_magnitude;
}

} // namespace predex::exchange::kalshi
