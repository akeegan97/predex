#pragma once 

#include <cstdint>

namespace predex::exchange::kalshi{
        enum class KalshiMarketDataChannel : std::uint8_t {
        kORDERBOOK_DELTA = 1,
        kTRADE = 2,
        kMARKET_LIFECYCLE = 3,
    };

}