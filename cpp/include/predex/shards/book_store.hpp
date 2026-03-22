#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "trading/internal/normalized_event.hpp"

namespace trading::shards {

struct BookState {
    struct PendingDelta {
        std::optional<internal::SequenceId> sequence_id;
        internal::DeltaData delta;
    };

    using BidLevels = std::map<internal::PriceTicks, internal::QtyLots, std::greater<>>;
    using AskLevels = std::map<internal::PriceTicks, internal::QtyLots, std::less<>>;

    std::string market_ticker;
    std::optional<internal::SequenceId> last_seq_id;
    bool has_snapshot{false};
    bool desynced{false};
    BidLevels bids;
    AskLevels asks;
    std::deque<PendingDelta> pending_deltas;
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
};

enum class BookApplyRejectReason : unsigned char {
    kNone = 0,
    kMissingMarketOrUnknownEvent,
    kInvalidSnapshotData,
    kReplayPendingDeltaFailure,
    kMissingDeltaData,
    kDeltaWhileDesynced,
    kPendingDeltaOverflowDesync,
    kDeltaSequenceOrLevelInvalid,
    kTradeInvalidOrDesynced,
    kUnsupportedEventType,
};

struct BookApplyResult {
    bool applied{false};
    BookApplyRejectReason reason{BookApplyRejectReason::kNone};
};

class BookStore {
  public:
    static constexpr std::size_t kMaxPendingDeltas = 512;

    [[nodiscard]] BookApplyResult apply_with_result(const internal::NormalizedEvent& event);
    [[nodiscard]] bool apply(const internal::NormalizedEvent& event);
    [[nodiscard]] const BookState* find(std::string_view market_ticker) const;
    [[nodiscard]] std::size_t size() const;

  private:
    std::unordered_map<std::string, BookState> books_;
};

} // namespace trading::shards
