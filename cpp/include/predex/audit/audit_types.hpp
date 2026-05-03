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
    kPipelineProbe = 9,
    kShardDesync = 10,
    kRouterShardBackpressure = 11,
};

struct AuditEvent {
    AuditKind kind{AuditKind::kSignal};
    internal::TimestampNs ts_ns{0};
    std::uint16_t shard_id{0};

    std::uint64_t signal_id{0};
    std::uint64_t group_id{0};
    std::uint64_t local_intent_id{0};
    std::uint64_t oms_request_id{0};
    std::uint64_t frame_seq{0};
    std::uint32_t frame_sid{0};
    std::uint16_t transport_http_status{0};
    std::uint16_t transport_retry_count{0};
    /*
    Latency tracking fields
    */
    internal::TimestampNs tick_recv_ns{0};
    internal::TimestampNs signal_ts_ns{0};
    internal::TimestampNs submission_enqueued_ns{0};
    internal::TimestampNs oms_decision_ts_ns{0};
    internal::TimestampNs transport_submit_ts_ns{0};
    internal::TimestampNs transport_response_recv_ns{0};
    internal::TimestampNs first_fill_recv_ns{0};
    internal::TimestampNs terminal_recv_ns{0};
    /*
    computed span fields
    */
    std::int64_t tick_to_signal_ns{0};
    std::int64_t signal_to_submission_ns{0};
    std::int64_t submission_to_decision_ns{0};
    std::int64_t decision_to_transport_ns{0};
    std::int64_t tick_to_transport_submit_ns{0};
    std::int64_t transport_submit_to_response_ns{0};
    std::int64_t tick_to_transport_response_ns{0};
    std::int64_t transport_to_first_fill_ns{0};
    std::int64_t tick_to_first_fill_ns{0};
    std::int64_t tick_to_terminal_ns{0};

    internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
    internal::EventId event_id{0};
    internal::MarketId market_id{0};
    internal::MarketId aux_market_id{0};
    internal::Side side{internal::Side::kUnknown};
    internal::Side aux_side{internal::Side::kUnknown};

    std::uint16_t leg_index{0};
    std::uint16_t leg_count{0};

    internal::QtyLots qty_lots{0};
    internal::QtyLots aux_qty_lots{0};
    internal::PriceTicks reference_price_ticks{0};
    internal::PriceTicks aux_reference_price_ticks{0};
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
