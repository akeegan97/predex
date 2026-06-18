#pragma once 

#include <cstdint>

namespace predex::core::control{
    enum class LifecyclePhase : std::uint8_t{
        kBOOTING = 0,
        kWAITING_FOR_IO = 1,
        kIO_CONNECTED = 2, 
        kREADY = 3, 
        kLIVE_TRADING = 4,
        kSHUTTING_DOWN = 5, 
        kSTOPPED = 6,
    };

}