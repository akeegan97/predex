#pragma once 
#include "predex/config/app_config.hpp"
#include "predex/control/control_types.hpp"


namespace predex::config{
    [[nodiscard]] predex::core::control::UniverseSnapshot build_universe_snapshot(const AppConfig& config, std::uint32_t shard_count);
}