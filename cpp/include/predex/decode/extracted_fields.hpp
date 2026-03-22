#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace trading::decode {

enum class DecodeStatus : std::uint8_t {
    kOk,
    kMissingField,
    kUnsupportedMessage,
};

struct ExtractedFields {
    DecodeStatus status{DecodeStatus::kUnsupportedMessage};
    std::string market_ticker;
    std::optional<std::uint64_t> seq_id;
    std::chrono::steady_clock::time_point recv_timestamp;
    std::string source;

    [[nodiscard]] bool ok() const { return status == DecodeStatus::kOk; }
};

} // namespace trading::decode
