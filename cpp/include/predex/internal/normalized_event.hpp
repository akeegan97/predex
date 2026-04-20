#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "predex/internal/event_topology.hpp"
#include "predex/internal/market_types.hpp"

namespace predex::internal {

struct EventMeta {
    EventId event_id{0};
    ExchangeId exchange{ExchangeId::kUnknown};
    EventTopologyKind topology_kind{EventTopologyKind::kUnknown};
    AffinityKey affinity_key{0};
    MarketId market_id{0};
    SequenceId sequence_id{0};
    TimestampNs recv_ns{0};
    TimestampNs exchange_ts_ns{0};

    [[nodiscard]] bool has_routing_identity() const noexcept {
        return event_id != 0U && market_id != 0U;
    }
};

struct Level {
    PriceTicks price_ticks{0};
    QtyLots qty_lots{0};
};

struct SnapshotData {
    std::vector<Level> bids;
    std::vector<Level> asks;
};

struct DeltaData {
    Side side{Side::kUnknown};
    PriceTicks price_ticks{0};
    QtyLots delta_qty_lots{0};
};

struct TradeData {
    PriceTicks price_ticks{0};
    QtyLots qty_lots{0};
    Side aggressor{Side::kUnknown};
    Side book_side{Side::kUnknown};
    std::optional<std::string> trade_id;
};

struct MarketLifecycleData {
    MarketLifecycleStatus status{MarketLifecycleStatus::kUnknown};
    std::uint64_t open_ts_s{0};
    std::uint64_t close_ts_s{0};
};

using EventData = std::variant<std::monostate, SnapshotData, DeltaData, TradeData, MarketLifecycleData>;

struct NormalizedEvent {
    EventType type{EventType::kUnknown};
    EventMeta meta{};
    std::optional<SequenceId> raw_sequence_id;
    EventData data;

    [[nodiscard]] std::optional<SequenceId> effective_sequence_id() const {
        if (raw_sequence_id.has_value()) {
            return raw_sequence_id;
        }
        if (meta.sequence_id != 0U) {
            return meta.sequence_id;
        }
        return std::nullopt;
    }

    [[nodiscard]] bool has_event_context() const noexcept {
        return meta.has_routing_identity();
    }
};

} // namespace predex::internal
