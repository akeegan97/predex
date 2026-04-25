#pragma once

#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>

namespace predex::internal {

using EventId = std::uint32_t;
using MarketId = std::uint32_t;
using SequenceId = std::uint64_t;
using PriceTicks = std::int64_t;
using QtyLots = std::int64_t;
using TimestampNs = std::uint64_t;
using AffinityKey = std::uint64_t;

inline constexpr QtyLots kQtyScale = 100;
inline constexpr QtyLots kOneContractQtyLots = kQtyScale;
inline constexpr PriceTicks kPriceTicksPerDollar = 1000;
inline constexpr PriceTicks kMaxPriceTicks = kPriceTicksPerDollar;
inline constexpr PriceTicks kTicksPerCent = kPriceTicksPerDollar / 100;
inline constexpr std::size_t kPriceDecimalPlaces = 3;

enum class ExchangeId : std::uint8_t {
    kUnknown = 0,
    kKalshi = 1,
    kPolymarket = 2,
};

enum class EventType : std::uint8_t {
    kUnknown = 0,
    kSnapshot = 1,
    kDelta = 2,
    kTrade = 3,
    kHeartbeat = 4,
    kStatus = 5,
    kLifecycle = 6,
};

enum class MarketLifecycleStatus : std::uint8_t {
    kUnknown = 0,
    kCreated = 1,
    kActivated = 2,
    kDeactivated = 3,
    kCloseDateUpdated = 4,
    kDetermined = 5,
    kSettled = 6,
    kFractionalTradingUpdated = 7,
    kPriceLevelStructureUpdated = 8,
};

enum class Side : std::uint8_t {
    kUnknown = 0,
    kBid = 1,
    kAsk = 2,
    kBuy = 3,
    kSell = 4,
};

[[nodiscard]] inline constexpr QtyLots contracts_to_qty(std::int64_t contracts) noexcept {
    return static_cast<QtyLots>(contracts) * kQtyScale;
}

[[nodiscard]] inline constexpr double qty_to_contracts(QtyLots qty_lots) noexcept {
    return static_cast<double>(qty_lots) / static_cast<double>(kQtyScale);
}

[[nodiscard]] inline bool parse_quantity_fp(std::string_view value,
                                            QtyLots& out_qty_lots,
                                            bool allow_negative = true) noexcept {
    if (value.empty()) {
        return false;
    }

    bool negative = false;
    if (value.front() == '+' || value.front() == '-') {
        negative = value.front() == '-';
        if (negative && !allow_negative) {
            return false;
        }
        value.remove_prefix(1);
    }
    if (value.empty()) {
        return false;
    }

    const std::size_t dot_pos = value.find('.');
    const std::string_view int_part =
        dot_pos == std::string_view::npos ? value : value.substr(0, dot_pos);
    const std::string_view frac_part =
        dot_pos == std::string_view::npos ? std::string_view{} : value.substr(dot_pos + 1);
    if (int_part.empty()) {
        return false;
    }
    for (char digit_char : int_part) {
        if (digit_char < '0' || digit_char > '9') {
            return false;
        }
    }
    if (frac_part.size() > 2U) {
        return false;
    }

    std::int64_t whole = 0;
    const auto [ptr, ec] = std::from_chars(int_part.data(), int_part.data() + int_part.size(), whole);
    if (ec != std::errc{} || ptr != int_part.data() + int_part.size()) {
        return false;
    }

    std::int64_t fractional = 0;
    for (char digit_char : frac_part) {
        if (digit_char < '0' || digit_char > '9') {
            return false;
        }
        fractional = fractional * 10 + static_cast<std::int64_t>(digit_char - '0');
    }
    if (frac_part.size() == 1U) {
        fractional *= 10;
    }

    if (whole > (std::numeric_limits<QtyLots>::max() - fractional) / kQtyScale) {
        return false;
    }
    QtyLots scaled = whole * kQtyScale + fractional;
    if (!allow_negative && scaled < 0) {
        return false;
    }
    if (!negative) {
        out_qty_lots = scaled;
        return true;
    }
    out_qty_lots = -scaled;
    return true;
}

[[nodiscard]] inline bool parse_non_negative_quantity_fp(std::string_view value,
                                                         QtyLots& out_qty_lots) noexcept {
    return parse_quantity_fp(value, out_qty_lots, false);
}

[[nodiscard]] inline std::string format_quantity_fp(QtyLots qty_lots) {
    const bool negative = qty_lots < 0;
    using UnsignedQtyLots = std::uint64_t;
    const auto magnitude = negative
        ? static_cast<UnsignedQtyLots>(-(qty_lots + 1)) + 1U
        : static_cast<UnsignedQtyLots>(qty_lots);
    const auto whole = magnitude / static_cast<UnsignedQtyLots>(kQtyScale);
    const auto fractional = magnitude % static_cast<UnsignedQtyLots>(kQtyScale);

    std::string formatted;
    if (negative) {
        formatted.push_back('-');
    }
    formatted.append(std::to_string(whole));
    formatted.push_back('.');
    formatted.push_back(static_cast<char>('0' + (fractional / 10U)));
    formatted.push_back(static_cast<char>('0' + (fractional % 10U)));
    return formatted;
}

[[nodiscard]] inline constexpr std::int64_t scale_ticks_by_qty_floor(PriceTicks ticks_per_contract,
                                                                     QtyLots qty_lots) noexcept {
    return (static_cast<std::int64_t>(ticks_per_contract) * static_cast<std::int64_t>(qty_lots)) /
           static_cast<std::int64_t>(kQtyScale);
}

[[nodiscard]] inline constexpr std::int64_t scale_ticks_by_qty_ceil(PriceTicks ticks_per_contract,
                                                                    QtyLots qty_lots) noexcept {
    if (ticks_per_contract <= 0 || qty_lots <= 0) {
        return 0;
    }
    const auto numerator =
        static_cast<std::int64_t>(ticks_per_contract) * static_cast<std::int64_t>(qty_lots);
    const auto scale = static_cast<std::int64_t>(kQtyScale);
    return (numerator + scale - 1) / scale;
}

} // namespace predex::internal
