#pragma once

#include <optional>
#include <string>

#include "predex/app.hpp"

namespace predex::apps {

struct AppConfigParseOptions {
    bool require_credentials{true};
    bool require_public_channels{true};
};

[[nodiscard]] std::optional<std::string> resolve_config_path(int argc, char** argv);

[[nodiscard]] std::optional<AppConfig> load_app_config(
    const std::string& config_path,
    std::string& error_out,
    AppConfigParseOptions options = {});

} // namespace predex::apps
