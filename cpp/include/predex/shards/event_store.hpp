#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

#include "predex/internal/market_types.hpp"
#include "predex/shards/book_store.hpp"

#include "predex/internal/event_topology.hpp"

namespace predex::core::shards::kalshi {

// EventStore owns several Events on a single shard.
// Each Event owns the raw per-market BookStore plus a derived topology-specific view.
inline constexpr std::size_t kMaxDepth = 5;

enum class EventApplyCode: std::uint8_t{
    kApplied = 1,
    kRejected = 2,
    kParseFail=3
};

struct SideDepthLevel {
    std::optional<internal::PriceTicks> price_ticks;
    std::optional<internal::QtyLots> qty_lots;
};

template <std::size_t Depth>
struct DepthView {
    std::array<SideDepthLevel, Depth> bids{};
    std::array<SideDepthLevel, Depth> asks{};
};

struct EventMarketView {
    internal::MarketId market_id{0};
    bool has_book{false};
    bool desynced{false};
    DepthView<kMaxDepth> depth{};
    std::optional<internal::TradeData> last_trade;
};

struct ChainEntry {
    EventMarketView market{};
    std::int64_t strike_key{0};
};

struct EventMarketDefinition {
    internal::MarketId market_id{0};
    std::int64_t strike_key{0};
};

struct EventDefinition {
    internal::EventId event_id{0};
    internal::EventTopologyKind topology_kind{internal::EventTopologyKind::kUnknown};
    std::size_t expected_market_count{0};
    std::vector<EventMarketDefinition> markets;
};

struct MonotonicChainState {
    std::vector<ChainEntry> markets;  // ordered by strike_key, easiest -> hardest
    std::unordered_map<internal::MarketId, std::size_t> market_index_by_id;
    bool complete{false};
    bool desynced{false};
    internal::TimestampNs last_update_ns{0};

    [[nodiscard]] std::size_t size() const noexcept { return markets.size(); }

    [[nodiscard]] const ChainEntry* find(internal::MarketId market_id) const noexcept {
        const auto iterator = market_index_by_id.find(market_id);
        if (iterator == market_index_by_id.end()) {
            return nullptr;
        }
        return &markets[iterator->second];
    }

    [[nodiscard]] ChainEntry* find(internal::MarketId market_id) noexcept {
        const auto iterator = market_index_by_id.find(market_id);
        if (iterator == market_index_by_id.end()) {
            return nullptr;
        }
        return &markets[iterator->second];
    }
};

struct MutuallyExclusiveState {
    std::vector<EventMarketView> markets;
    std::unordered_map<internal::MarketId, std::size_t> market_index_by_id;
    bool complete{false};
    bool desynced{false};
    internal::TimestampNs last_update_ns{0};

    [[nodiscard]] std::size_t size() const noexcept { return markets.size(); }
};

struct UnorderedGroupState {
    std::vector<EventMarketView> markets;
    std::unordered_map<internal::MarketId, std::size_t> market_index_by_id;
    bool complete{false};
    bool desynced{false};
    internal::TimestampNs last_update_ns{0};

    [[nodiscard]] std::size_t size() const noexcept { return markets.size(); }
};

struct SingleMarketState {
    std::optional<EventMarketView> market;
    bool desynced{false};
    internal::TimestampNs last_update_ns{0};
};



using EventDerivedState = std::variant<std::monostate,
                                       MonotonicChainState,
                                       MutuallyExclusiveState,
                                       UnorderedGroupState,
                                       SingleMarketState>;

struct Event {
    internal::EventId event_id{0};
    internal::EventTopologyKind topology_kind{internal::EventTopologyKind::kUnknown};
    std::size_t expected_market_count{0};
    BookStore book_store;
    EventDerivedState derived_state;
    bool desynced{false};
    internal::TimestampNs last_update_ns{0};

    EventApplyCode apply_market_update(const internal::NormalizedEvent& event);
};

class EventStore {
  public:
    [[nodiscard]] bool initialize(const std::vector<EventDefinition>& definitions);

    [[nodiscard]] Event* find(internal::EventId event_id) noexcept {
        const auto iterator = events_.find(event_id);
        if (iterator == events_.end()) {
            return nullptr;
        }
        return &iterator->second;
    }

    [[nodiscard]] const Event* find(internal::EventId event_id) const noexcept {
        const auto iterator = events_.find(event_id);
        if (iterator == events_.end()) {
            return nullptr;
        }
        return &iterator->second;
    }

    [[nodiscard]] std::size_t size() const noexcept { return events_.size(); }
    [[nodiscard]] bool empty() const noexcept { return events_.empty(); }

  private:
    std::unordered_map<internal::EventId, Event> events_;
};

}  // namespace predex::core::shards::kalshi
