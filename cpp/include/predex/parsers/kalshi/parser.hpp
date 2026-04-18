#pragma once

#include "predex/internal/normalized_event.hpp"
#include "predex/parsers/parse_result.hpp"
#include "predex/ingest/frame_pool.hpp"
#include <simdjson.h>

namespace predex::core::parsers::kalshi {

class Parser{
  //takes a FrameHandle & Frame Ref and returns a ParseResult containing a NormalizedEvent
  public:
    Parser() = default;
    ~Parser() = default;
      [[nodiscard]] predex::parsers::ParseResult<predex::internal::NormalizedEvent> parse(const predex::core::ingest::kalshi::FrameHandle& handle, 
      const predex::core::ingest::kalshi::KalshiFrame& frame);

  private:
        simdjson::ondemand::parser parser_;

};

} // namespace predex::core::parsers::kalshi
