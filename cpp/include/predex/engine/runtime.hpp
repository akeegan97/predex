#pragma once

#include <string>
#include <string_view>

namespace trading::engine {

std::string build_trader_startup_payload(std::string_view mode);
std::string build_logger_startup_payload(std::string_view mode);

} // namespace trading::engine
