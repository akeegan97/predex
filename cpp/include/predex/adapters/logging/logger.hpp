#pragma once

#include <string_view>

namespace trading::adapters::logging {

void log_startup(std::string_view component, std::string_view payload);

} // namespace trading::adapters::logging
