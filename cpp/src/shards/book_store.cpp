#include "predex/shards/book_store.hpp"
#include "predex/internal/market_types.hpp"
#include "predex/internal/normalized_event.hpp"

namespace predex::core::shards::kalshi {
namespace {
DeltaApplyResult apply_delta(const internal::NormalizedEvent& event, BookState& book_state);

void append_recent_update(BookState& book_state, const internal::NormalizedEvent& event,
                          internal::Side side = internal::Side::kUnknown,
                          internal::PriceTicks price_ticks = 0, internal::QtyLots qty_lots = 0) {
    if (book_state.recent_updates.size() >= kRecentBookUpdateHistory) {
        book_state.recent_updates.pop_front();
    }
    book_state.recent_updates.push_back(BookUpdateTrace{
        .type = event.type,
        .seq_id = event.effective_sequence_id(),
        .side = side,
        .price_ticks = price_ticks,
        .qty_lots = qty_lots,
    });
}

void set_failure_trace(BookState& book_state, const internal::NormalizedEvent& event,
                       internal::Side side, internal::PriceTicks price_ticks,
                       internal::QtyLots delta_qty_lots, internal::QtyLots existing_qty_lots,
                       internal::QtyLots updated_qty_lots) {
    book_state.last_failure = BookFailureTrace{
        .type = event.type,
        .seq_id = event.effective_sequence_id(),
        .side = side,
        .price_ticks = price_ticks,
        .delta_qty_lots = delta_qty_lots,
        .existing_qty_lots = existing_qty_lots,
        .updated_qty_lots = updated_qty_lots,
        .pending_delta_count = book_state.pending_deltas.size(),
    };
}

void clear_book_levels(BookState& book_state) {
    book_state.bids.fill(0);
    book_state.asks.fill(0);
}

BookApplyResult apply_snapshot(const internal::NormalizedEvent& event, BookState& book_state,
                               bool recovery_snapshot) {
    if (!event.effective_sequence_id().has_value()) {
        return BookApplyResult{.applied = false,
                               .reason = BookApplyRejectReason::kInvalidSnapshotData};
    }
    clear_book_levels(book_state);
    const auto& snapshot_data = std::get<internal::SnapshotData>(event.data);
    for (const auto& bid : snapshot_data.bids) {
        if (bid.price_ticks > kMaxPriceTicks) {
            book_state.desynced = true;
            set_failure_trace(book_state, event, internal::Side::kBid, bid.price_ticks,
                              bid.qty_lots, 0, bid.qty_lots);
            return BookApplyResult{.applied = false,
                                   .reason = BookApplyRejectReason::kInvalidSnapshotData};
        }
        book_state.bids[bid.price_ticks] = bid.qty_lots;
    }
    for (const auto& ask : snapshot_data.asks) {
        if (ask.price_ticks > kMaxPriceTicks) {
            book_state.desynced = true;
            set_failure_trace(book_state, event, internal::Side::kAsk, ask.price_ticks,
                              ask.qty_lots, 0, ask.qty_lots);
            return BookApplyResult{.applied = false,
                                   .reason = BookApplyRejectReason::kInvalidSnapshotData};
        }
        book_state.asks[ask.price_ticks] = ask.qty_lots;
    }
    book_state.has_snapshot = true;
    book_state.desynced = false;
    book_state.last_seq_id = event.effective_sequence_id();
    book_state.snapshot_count++;
    if (recovery_snapshot) {
        book_state.recovery_snapshot_count++;
    }
    append_recent_update(book_state, event);

    if (!book_state.pending_deltas.empty()) {
        for (const auto& pending_delta : book_state.pending_deltas) {
            if (apply_delta(pending_delta, book_state) != DeltaApplyResult::kSuccess) {
                book_state.desynced = true;
                return BookApplyResult{.applied = false,
                                       .reason = BookApplyRejectReason::kReplayPendingDeltaFailure};
            }
            book_state.replayed_delta_count++;
        }
        book_state.pending_deltas.clear();
    }
    return BookApplyResult{.applied = true};
}

DeltaApplyResult apply_delta(const internal::NormalizedEvent& event, BookState& book_state) {
    if (event.effective_sequence_id().has_value() && book_state.last_seq_id.has_value()) {
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        if (event.effective_sequence_id().value() <= book_state.last_seq_id.value()) {
            set_failure_trace(book_state, event, internal::Side::kUnknown, 0, 0, 0, 0);
            return DeltaApplyResult::kStaleSequence;
        }
        const auto& delta_data = std::get<internal::DeltaData>(event.data);
        // handle both sides
        if (delta_data.side == internal::Side::kBid) {
            // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
            if (delta_data.price_ticks > kMaxPriceTicks || delta_data.price_ticks < 0) {
                book_state.desynced = true;
                set_failure_trace(book_state, event, delta_data.side, delta_data.price_ticks,
                                  delta_data.delta_qty_lots, 0, 0);
                return DeltaApplyResult::kInvalidSeq; // might want to expand enum to
                                                      // kInvalidPriceTicks, leaving it as invalid
                                                      // seq for now
            }
            const auto existing_qty = book_state.bids[delta_data.price_ticks];
            const auto updated_qty = existing_qty + delta_data.delta_qty_lots;
            if (updated_qty < 0) {
                book_state.desynced = true;
                set_failure_trace(book_state, event, delta_data.side, delta_data.price_ticks,
                                  delta_data.delta_qty_lots, existing_qty, updated_qty);
                append_recent_update(book_state, event, delta_data.side, delta_data.price_ticks,
                                     delta_data.delta_qty_lots);
                return DeltaApplyResult::kNegativeQuantityDesync;
            }
            book_state.bids[delta_data.price_ticks] = updated_qty;
            book_state.last_seq_id = event.effective_sequence_id();
            append_recent_update(book_state, event, delta_data.side, delta_data.price_ticks,
                                 delta_data.delta_qty_lots);
            return DeltaApplyResult::kSuccess;
        }
        if (delta_data.side == internal::Side::kAsk) {
            if (delta_data.price_ticks > kMaxPriceTicks || delta_data.price_ticks < 0) {
                book_state.desynced = true;
                set_failure_trace(book_state, event, delta_data.side, delta_data.price_ticks,
                                  delta_data.delta_qty_lots, 0, 0);
                return DeltaApplyResult::kInvalidSeq; // might want to expand enum to
                                                      // kInvalidPriceTicks, leaving it as invalid
                                                      // seq for now
            }
            const auto existing_qty = book_state.asks[delta_data.price_ticks];
            const auto updated_qty = existing_qty + delta_data.delta_qty_lots;
            if (updated_qty < 0) {
                book_state.desynced = true;
                set_failure_trace(book_state, event, delta_data.side, delta_data.price_ticks,
                                  delta_data.delta_qty_lots, existing_qty, updated_qty);
                append_recent_update(book_state, event, delta_data.side, delta_data.price_ticks,
                                     delta_data.delta_qty_lots);
                return DeltaApplyResult::kNegativeQuantityDesync;
            }
            book_state.asks[delta_data.price_ticks] = updated_qty;
            book_state.last_seq_id = event.effective_sequence_id();
            append_recent_update(book_state, event, delta_data.side, delta_data.price_ticks,
                                 delta_data.delta_qty_lots);
            return DeltaApplyResult::kSuccess;
        }
        set_failure_trace(book_state, event, delta_data.side, delta_data.price_ticks,
                          delta_data.delta_qty_lots, 0, 0);
        return DeltaApplyResult::kInvalidSide;
    }
    set_failure_trace(book_state, event, internal::Side::kUnknown, 0, 0, 0, 0);
    return DeltaApplyResult::kInvalidSeq;
}
} // namespace
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
BookApplyResult BookStore::apply_with_result(const internal::NormalizedEvent& event) {
    /*
    check if we are initialized with has_snapshot & market_id
    if not initialized, only apply snapshot events, buffer deltas and ignore trades, if we get a
    delta and buffer is full mark desynced if initialized apply deltas and trades, if delta fails to
    apply mark desynced, ignore unsupported event types (heartbeat/status)
    */

    if (event.type == internal::EventType::kHeartbeat ||
        event.type == internal::EventType::kStatus) {
        return BookApplyResult{.applied = false,
                               .reason = BookApplyRejectReason::kUnsupportedEventType};
    }

    if (event.meta.market_id == 0) {
        // market_id of 0 is invalid and means we either malformed the json decode/router didn't
        // attach market_id or market is not in our map
        return BookApplyResult{.applied = false,
                               .reason = BookApplyRejectReason::kMissingMarketOrUnknownEvent};
    }
    // try to emplace book state for market if it doesn't exist, if it does exist we will just get
    // an iterator to it
    auto [iterator, inserted] = books_.try_emplace(event.meta.market_id);
    BookState& book_state = iterator->second;
    if (inserted) {
        book_state.market_id = event.meta.market_id;
    }
    // check book_state to see if we have a snapshot for this market && sequence id, if we are
    // uninitialized we buffer deltas, disregard trades and apply snapshot only.

    // unitialized check
    if (!book_state.has_snapshot) {
        if (event.type == internal::EventType::kSnapshot) {
            return apply_snapshot(event, book_state, false);
        }
        // handle case delta shows up before snapshot
        if (event.type == internal::EventType::kDelta) {
            if (book_state.pending_deltas.size() >= kMaxPendingDeltas) {
                book_state.desynced = true;
                set_failure_trace(book_state, event, std::get<internal::DeltaData>(event.data).side,
                                  std::get<internal::DeltaData>(event.data).price_ticks,
                                  std::get<internal::DeltaData>(event.data).delta_qty_lots, 0, 0);
                return BookApplyResult{
                    .applied = false, .reason = BookApplyRejectReason::kPendingDeltaOverflowDesync};
            }
            book_state.pending_deltas.push_back(event);
            book_state.buffered_delta_count++;
            append_recent_update(book_state, event, std::get<internal::DeltaData>(event.data).side,
                                 std::get<internal::DeltaData>(event.data).price_ticks,
                                 std::get<internal::DeltaData>(event.data).delta_qty_lots);
            return BookApplyResult{.applied = false,
                                   .reason = BookApplyRejectReason::kBufferedDelta};
        }
        if (event.type == internal::EventType::kTrade) {
            // we don't really want to do anything with trades that come in before snapshots
            return BookApplyResult{.applied = false,
                                   .reason = BookApplyRejectReason::kTradeBeforeSnap};
        }
    }
    // initialized state we can apply deltas/track trades
    if (event.type == internal::EventType::kSnapshot) {
        // Treat later full snapshots as authoritative recovery points.
        book_state.pending_deltas.clear();
        return apply_snapshot(event, book_state, true);
    }
    if (event.type == internal::EventType::kDelta) {
        if (!book_state.desynced) {
            DeltaApplyResult result = apply_delta(event, book_state);
            if (result == DeltaApplyResult::kSuccess) {
                book_state.delta_count++;
                return BookApplyResult{.applied = true};
            }
            switch (result) {
            case DeltaApplyResult::kStaleSequence:
                book_state.stale_sequence_count++;
                return BookApplyResult{.applied = false,
                                       .reason = BookApplyRejectReason::kDeltaSequence};
                break;
            case DeltaApplyResult::kInvalidSide:
                book_state.invalid_side_count++;
                return BookApplyResult{.applied = false,
                                       .reason = BookApplyRejectReason::kInvalidSide};
                break;
            case DeltaApplyResult::kNegativeQuantityDesync:
                book_state.invalid_negative_level_count++;
                return BookApplyResult{.applied = false,
                                       .reason = BookApplyRejectReason::kNegativeQuantity};
            case DeltaApplyResult::kInvalidSeq:
                book_state.invalid_seq_count++;
                return BookApplyResult{.applied = false,
                                       .reason = BookApplyRejectReason::kInvalidSeq};
            default:
                break;
            }
        }
        return BookApplyResult{.applied = false,
                               .reason = BookApplyRejectReason::kDeltaWhileDesynced};
    }
    if (event.type == internal::EventType::kTrade) {
        const auto& trade_data = std::get<internal::TradeData>(event.data);
        if (trade_data.price_ticks == 0 || trade_data.qty_lots == 0) {
            book_state.apply_reject_count++;
            set_failure_trace(book_state, event, trade_data.book_side, trade_data.price_ticks,
                              trade_data.qty_lots, 0, 0);
            return BookApplyResult{.applied = false,
                                   .reason = BookApplyRejectReason::kTradeInvalidOrDesynced};
        }
        book_state.last_trade = trade_data;
        book_state.trade_count++;
        append_recent_update(book_state, event, trade_data.book_side, trade_data.price_ticks,
                             trade_data.qty_lots);
    }
    // trades for kalshi follow their own session id and therefore we don't want to update
    // last_seq_id since it only applies to book updates
    return BookApplyResult{.applied = true};
}

const BookState* BookStore::find(internal::MarketId market_id) const {
    const auto iterator = books_.find(market_id);
    if (iterator != books_.end()) {
        return &iterator->second;
    }
    return nullptr;
}

std::size_t BookStore::size() const { return books_.size(); }

void BookStore::reset_all() noexcept {
    for (auto& [market_id, book] : books_) {
        book = BookState{.market_id = market_id};
    }
}

} // namespace predex::core::shards::kalshi
