#pragma once

#include "trading/decode/extracted_fields.hpp"
#include "trading/ingest/raw_frame.hpp"

namespace trading::decode::exchanges::kalshi {

[[nodiscard]] ExtractedFields extract(const ingest::RawFrame& frame);

} // namespace trading::decode::exchanges::kalshi
