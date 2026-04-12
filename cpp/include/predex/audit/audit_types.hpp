#pragma once

#include <cstdint>

#include "predex/internal/market_types.hpp"

namespace predex::core::audit {

enum class AuditKind : std::uint8_t {
    kSignal = 1,
    kGroupSignal = 2,
    kLocalRisk = 3,
    kSubmission = 4,
    kOmsDecision = 5,
    kOmsTransport = 6,
    kOmsLifecycle = 7,
    kShardReconcile = 8,
};

struct AuditEvent {
    AuditKind kind{AuditKind::kSignal};
    internal::TimestampNs ts_ns{0};
    std::uint16_t shard_id{0};

    std::uint64_t signal_id{0};
    std::uint64_t group_id{0};
    std::uint64_t local_intent_id{0};
    std::uint64_t oms_request_id{0};

    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    internal::EventId event_id{0};
    internal::MarketId market_id{0};
    internal::Side side{internal::Side::kUnknown};

    std::uint16_t leg_index{0};
    std::uint16_t leg_count{0};

    internal::QtyLots qty_lots{0};
    internal::QtyLots aux_qty_lots{0};
    internal::PriceTicks price_ticks{0};
    internal::PriceTicks aux_price_ticks{0};

    std::int64_t edge_ticks{0};
    std::int64_t score{0};

    std::uint8_t decision_code{0};
    std::uint8_t reject_reason{0};
    std::uint8_t lifecycle_kind{0};
    std::uint8_t order_status{0};

    internal::QtyLots event_exposure_lots{0};
    internal::QtyLots market_exposure_lots{0};
};

} // namespace predex::core::audit
