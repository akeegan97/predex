#pragma once

#include "predex/control/control_lifecycle.hpp"
namespace predex::core::control {


struct ProcessState {
    LifecyclePhase lifecycle{LifecyclePhase::kBOOTING};
    bool trading_enabled{false};
    bool shutdown_requested{false};
    //other fields as needed
};



}  // namespace predex::core::control