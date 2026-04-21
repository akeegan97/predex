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

struct MarketLifecycleState {
    std::uint64_t open_ts_s{0};
    std::uint64_t close_ts_s{0};
    bool tradeable{false};

    // Kalshi emits no WS event at the natural active → closed transition that happens
    // when close_ts_s passes in wall-clock time; the `tradeable` flag on its own will
    // stay true past close_ts_s forever. Callers must route all "is this market
    // tradeable right now?" checks through this helper, not a direct .tradeable read.
    [[nodiscard]] bool is_tradeable_at(std::uint64_t now_s) const noexcept {
        return tradeable
            && (open_ts_s == 0 || now_s >= open_ts_s)
            && (close_ts_s == 0 || now_s < close_ts_s);
    }
};

struct EventMarketView {
    internal::MarketId market_id{0};
    bool has_book{false};
    bool desynced{false};
    DepthView<kMaxDepth> depth{};
    std::optional<internal::TradeData> last_trade;
    MarketLifecycleState lifecycle{};
};

struct ChainEntry {
    EventMarketView market{};
    std::int64_t strike_key{0};
};

struct EventMarketDefinition {
    internal::MarketId market_id{0};
    std::int64_t strike_key{0};
    std::uint64_t close_time_s{0};
    bool tradeable{false};
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

    [[nodiscard]] const EventMarketView* find_market_view(
        internal::MarketId market_id) const noexcept;
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

    // Resets all BookState entries so that the next snapshot from a reconnected
    // session is treated as the first snapshot. Call from the shard thread only.
    void reset_all_books() noexcept;

    // Approximate count of events whose derived state is currently desynced.
    // Safe to read from any thread as a best-effort monitoring value.
    [[nodiscard]] std::size_t desynced_event_count() const noexcept;

  private:
    std::unordered_map<internal::EventId, Event> events_;
};

}  // namespace predex::core::shards::kalshi
