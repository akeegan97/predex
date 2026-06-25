#pragma once 

#include <cstdint>

#include "predex/shard/models.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"

namespace predex::shard{
    
    enum class KalshiParseFailureReason : std::uint8_t {
        kNONE = 0,
        kUNSUPPORTED_FRAME_KIND,
        kINVALID_JSON,
        kMISSING_FIELD,
        kINVALID_SIDE,
        kINVALID_PRICE,
        kINVALID_QUANTITY,
    };
    
    struct ParseResult{
        bool success{false};
        KalshiParseFailureReason reason{KalshiParseFailureReason::kNONE};
    };
    class MarketParser{
        public:
            [[nodiscard]] ParseResult parse(const ingest::kalshi::FrameHandle& handle, const ingest::kalshi::KalshiFrame& frame, KalshiParsedEvent& parsed_event) noexcept;
    };

}