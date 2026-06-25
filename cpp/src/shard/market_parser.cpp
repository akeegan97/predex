#include "predex/shard/market_parser.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"


namespace predex::shard{

    ParseResult MarketParser::parse(const ingest::kalshi::FrameHandle& handle, const ingest::kalshi::KalshiFrame& frame, KalshiParsedEvent& parsed_event) noexcept{
        ParseResult result{};
        switch(handle.kind){
            case ingest::kalshi::FrameKind::kORDERBOOK_SNAPSHOT:
                // parse snapshot event
                // ...
                result.success = true;
                break;
            case ingest::kalshi::FrameKind::kORDERBOOK_DELTA:
                // parse delta data
                // ...
                result.success = true;
                break;
            case ingest::kalshi::FrameKind::kTRADE:
                // parse trade data
                // ...
                result.success = true;
                break;
            case ingest::kalshi::FrameKind::kLIFECYCLE:
                // parse lifecycle data
                // ...
                result.success = true;
                break;
            default:
                result.reason = KalshiParseFailureReason::kUNSUPPORTED_FRAME_KIND;
                break;
        }
        return result;
    }
}

