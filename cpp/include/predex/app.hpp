#pragma once

#include <string>
#include <string_view>

#include "trading/core/app.hpp"

namespace trading {

inline std::string build_heartbeat_json(std::string_view mode) {
    return core::build_heartbeat_json(mode);
}

} // namespace trading
