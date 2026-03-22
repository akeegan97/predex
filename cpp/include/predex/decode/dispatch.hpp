#pragma once

#include <cstdint>

#include "trading/decode/extracted_fields.hpp"
#include "trading/ingest/raw_frame.hpp"

namespace trading::decode {

enum class ExchangeId : std::uint8_t {
    kKalshi,
    kPolymarket,
};

using DecodeFn = ExtractedFields (*)(const ingest::RawFrame& frame);

[[nodiscard]] DecodeFn resolve_decoder(ExchangeId exchange_id);

} // namespace trading::decode
