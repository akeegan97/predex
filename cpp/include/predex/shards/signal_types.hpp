#pragma once

#include <cstdint>
#include <optional>

#include "predex/internal/market_types.hpp"
#include "predex/oms/oms_types.hpp"

namespace predex::core::shards::kalshi {

using OmsOrderIntent = predex::core::oms::kalshi::OrderIntent;
using OmsSubmission = predex::core::oms::kalshi::OmsSubmission;

enum class SignalKind : std::uint8_t {
    kUnknown = 0,
    kMonotonicViolation = 1,
    kMutuallyExclusiveDislocation = 2,
    kUnorderedGroupDislocation = 3,
    kMarketMaking = 4,
    kMeanReversion = 5,
};

struct Signal {
    std::uint64_t signal_id{0};
    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    internal::EventId event_id{0};
    internal::MarketId market_id{0};
    SignalKind kind{SignalKind::kUnknown};
    internal::Side side{internal::Side::kUnknown};
    internal::QtyLots target_qty_lots{0};
    std::optional<internal::PriceTicks> reference_price_ticks;
    internal::TimestampNs signal_ts_ns{0};
    std::int64_t edge_ticks{0};
    std::int64_t score{0};
};

enum class RiskDecisionCode : std::uint8_t {
    kAccepted = 1,
    kRejected = 2,
    kClipped = 3,
    kDisabled = 4,
    kError = 5,
};

enum class RiskRejectReason : std::uint8_t {
    kNone = 0,
    kEventExposureLimit = 1,
    kMarketExposureLimit = 2,
    kMaxOpenIntents = 3,
    kStrategyDisabled = 4,
    kInvalidSignal = 5,
    kInvalidIntent = 6,
};

struct RiskDecision {
    RiskDecisionCode code{RiskDecisionCode::kRejected};
    RiskRejectReason reason{RiskRejectReason::kNone};
    std::optional<OmsOrderIntent> accepted_intent;
};

}  // namespace predex::core::shards::kalshi
