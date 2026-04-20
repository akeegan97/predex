#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "predex/oms/oms_types.hpp"

namespace predex::core::oms::kalshi {

enum class PrivateWsParseStatus : std::uint8_t {
    kOk = 0,
    kInvalidJson = 1,
    kTooManyEvents = 2,
};

inline constexpr std::size_t kDefaultMaxPrivateWsEventsPerMessage = 32;

class PrivateWsParser {
  public:
    [[nodiscard]] PrivateWsParseStatus parse_message(
        std::string_view payload,
        std::vector<OrderLifecycleEvent>& out_events) const;
};

} // namespace predex::core::oms::kalshi
