#pragma once 

#include <cstdint>

namespace predex::exchange::kalshi{
    enum class KalshiMarketDataChannel : std::uint8_t {
        kORDERBOOK_DELTA = 1,
        kTRADE = 2,
        kMARKET_LIFECYCLE = 3,
    };

    enum class KalshiOrderDataChannel : std::uint8_t{
        kFILL = 1, // Tracking your trading activity, Updates sent immediately when your orders are filled
        kMARKET_POSITIONS = 2, // Portfolio tracking, position monitoring, P&L calculations
        kUSER_ORDERS = 3, // Tracking your resting orders, fills, and cancellations in real time
    };

}