#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <unordered_map>

#include "predex/internal/market_types.hpp"
#include "predex/internal/normalized_event.hpp"

namespace predex::core::shards::kalshi {

struct BookState {
    using BidLevels = std::map<internal::PriceTicks, internal::QtyLots, std::greater<>>;
    using AskLevels = std::map<internal::PriceTicks, internal::QtyLots, std::less<>>;

    internal::MarketId market_id{0};
    std::optional<internal::SequenceId> last_seq_id;
    bool has_snapshot{false};
    bool desynced{false};
    BidLevels bids;
    AskLevels asks;
    std::deque<internal::NormalizedEvent> pending_deltas;
    std::optional<internal::TradeData> last_trade;
    std::uint64_t snapshot_count{0};
    std::uint64_t delta_count{0};
    std::uint64_t trade_count{0};
    std::uint64_t buffered_delta_count{0};
    std::uint64_t replayed_delta_count{0};
    std::uint64_t dropped_pending_delta_count{0};
    std::uint64_t desync_count{0};
    std::uint64_t stale_sequence_count{0};
    std::uint64_t apply_reject_count{0};
    std::uint64_t invalid_negative_level_count{0};
    std::uint64_t invalid_side_count{0};
    std::uint64_t invalid_seq_count{0};
};

enum class BookApplyRejectReason : unsigned char {
    kNone = 0,
    kMissingMarketOrUnknownEvent,
    kInvalidSnapshotData,
    kReplayPendingDeltaFailure,
    kMissingDeltaData,
    kDeltaWhileDesynced,
    kPendingDeltaOverflowDesync,
    kDeltaSequence,
    kInvalidSide,
    kTradeInvalidOrDesynced,
    kUnsupportedEventType,
    kNegativeQuantity,
    kBufferedDelta,
    kTradeBeforeSnap,
    kInvalidSeq,
    kUnexpectedSnapshotAfterInit,
};
enum class DeltaApplyResult: unsigned int{
    kSuccess = 0,
    kStaleSequence,
    kInvalidSide,
    kNegativeQuantityDesync,
    kInvalidSeq,
};

struct BookApplyResult {
    bool applied{false};
    BookApplyRejectReason reason{BookApplyRejectReason::kNone};
};

class BookStore {
  public:
    static constexpr std::size_t kMaxPendingDeltas = 512;

    [[nodiscard]] BookApplyResult apply_with_result(const internal::NormalizedEvent& event);
    [[nodiscard]] const BookState* find(internal::MarketId market_id) const;
    [[nodiscard]] std::size_t size() const;

  private:
    std::unordered_map<internal::MarketId, BookState> books_;
};

} // namespace predex::core::shards::kalshi
