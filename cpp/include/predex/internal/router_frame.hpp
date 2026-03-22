#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "trading/internal/market_types.hpp"

namespace trading::internal {

// Lightweight handoff from router to parser. raw_payload_view must reference
// storage that outlives parse invocation.
struct RouterFrame {
    ExchangeId exchange{ExchangeId::kUnknown};
    std::string market_ticker;
    std::optional<SequenceId> sequence_id;
    TimestampNs recv_ns{0};
    std::string_view raw_payload_view;
};

} // namespace trading::internal
