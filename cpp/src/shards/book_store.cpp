#include "trading/shards/book_store.hpp"

namespace trading::shards {
namespace {

void mark_desynced(BookState& book, std::uint64_t dropped_pending_count = 0) {
    if (!book.desynced) {
        ++book.desync_count;
    }
    book.desynced = true;
    book.has_snapshot = false;
    book.last_seq_id.reset();
    book.bids.clear();
    book.asks.clear();
    book.pending_deltas.clear();
    book.dropped_pending_delta_count += dropped_pending_count;
}

bool advance_sequence_if_present(BookState& book,
                                 const std::optional<internal::SequenceId>& sequence_id) {
    if (!sequence_id.has_value()) {
        return true;
    }

    if (book.last_seq_id.has_value()) {
        const auto last_seq = book.last_seq_id.value();
        const auto next_seq = sequence_id.value();
        // Per-market books allow sequence gaps because some exchanges emit connection-global
        // sequence ids, but regressions/stale values are rejected.
        if (next_seq <= last_seq) {
            ++book.stale_sequence_count;
            return false;
        }
    }

    book.last_seq_id = sequence_id;
    return true;
}

bool apply_snapshot_data(BookState& book, const internal::SnapshotData& snapshot) {
    book.bids.clear();
    book.asks.clear();

    for (const auto& level : snapshot.bids) {
        if (level.price_ticks < 0 || level.qty_lots <= 0) {
            return false;
        }
        book.bids[level.price_ticks] = level.qty_lots;
    }
    for (const auto& level : snapshot.asks) {
        if (level.price_ticks < 0 || level.qty_lots <= 0) {
            return false;
        }
        book.asks[level.price_ticks] = level.qty_lots;
    }
    return true;
}

bool apply_delta_data(BookState& book, const internal::DeltaData& delta) {
    if (delta.price_ticks < 0) {
        return false;
    }

    BookState::BidLevels* bids = &book.bids;
    BookState::AskLevels* asks = &book.asks;
    switch (delta.side) {
    case internal::Side::kBid:
    case internal::Side::kBuy: {
        const auto existing_it = bids->find(delta.price_ticks);
        const internal::QtyLots existing_qty = existing_it == bids->end() ? 0 : existing_it->second;
        const internal::QtyLots updated_qty = existing_qty + delta.delta_qty_lots;
        if (updated_qty <= 0) {
            bids->erase(delta.price_ticks);
        } else {
            (*bids)[delta.price_ticks] = updated_qty;
        }
        return true;
    }
    case internal::Side::kAsk:
    case internal::Side::kSell: {
        const auto existing_it = asks->find(delta.price_ticks);
        const internal::QtyLots existing_qty = existing_it == asks->end() ? 0 : existing_it->second;
        const internal::QtyLots updated_qty = existing_qty + delta.delta_qty_lots;
        if (updated_qty <= 0) {
            asks->erase(delta.price_ticks);
        } else {
            (*asks)[delta.price_ticks] = updated_qty;
        }
        return true;
    }
    case internal::Side::kUnknown:
        return false;
    }
    return false;
}

bool replay_pending_deltas(BookState& book) {
    while (!book.pending_deltas.empty()) {
        BookState::PendingDelta pending = book.pending_deltas.front();
        book.pending_deltas.pop_front();

        if (!advance_sequence_if_present(book, pending.sequence_id)) {
            ++book.dropped_pending_delta_count;
            continue;
        }
        if (!apply_delta_data(book, pending.delta)) {
            const auto remaining = static_cast<std::uint64_t>(book.pending_deltas.size());
            mark_desynced(book, remaining + 1U);
            return false;
        }

        ++book.delta_count;
        ++book.replayed_delta_count;
    }
    return true;
}

} // namespace

BookApplyResult BookStore::apply_with_result(const internal::NormalizedEvent& event) {
    auto reject = [](BookApplyRejectReason reason) {
        return BookApplyResult{
            .applied = false,
            .reason = reason,
        };
    };

    if (event.market_ticker.empty() || event.type == internal::EventType::kUnknown) {
        return reject(BookApplyRejectReason::kMissingMarketOrUnknownEvent);
    }

    auto [it, inserted] = books_.try_emplace(event.market_ticker);
    if (inserted) {
        it->second.market_ticker = event.market_ticker;
    }
    BookState& book = it->second;

    switch (event.type) {
    case internal::EventType::kSnapshot: {
        const auto* snapshot = std::get_if<internal::SnapshotData>(&event.data);
        if (snapshot == nullptr || !apply_snapshot_data(book, *snapshot)) {
            ++book.apply_reject_count;
            return reject(BookApplyRejectReason::kInvalidSnapshotData);
        }
        book.has_snapshot = true;
        book.desynced = false;
        book.last_seq_id = event.effective_sequence_id();
        ++book.snapshot_count;
        if (!replay_pending_deltas(book)) {
            ++book.apply_reject_count;
            return reject(BookApplyRejectReason::kReplayPendingDeltaFailure);
        }
        break;
    }
    case internal::EventType::kDelta: {
        const auto* delta = std::get_if<internal::DeltaData>(&event.data);
        if (delta == nullptr) {
            ++book.apply_reject_count;
            return reject(BookApplyRejectReason::kMissingDeltaData);
        }

        if (book.desynced) {
            ++book.apply_reject_count;
            return reject(BookApplyRejectReason::kDeltaWhileDesynced);
        }
        if (!book.has_snapshot) {
            if (book.pending_deltas.size() >= BookStore::kMaxPendingDeltas) {
                const std::uint64_t dropped_pending =
                    static_cast<std::uint64_t>(book.pending_deltas.size()) + 1U;
                mark_desynced(book, dropped_pending);
                ++book.apply_reject_count;
                return reject(BookApplyRejectReason::kPendingDeltaOverflowDesync);
            }

            book.pending_deltas.push_back(BookState::PendingDelta{
                .sequence_id = event.effective_sequence_id(),
                .delta = *delta,
            });
            ++book.buffered_delta_count;
            return BookApplyResult{
                .applied = true,
                .reason = BookApplyRejectReason::kNone,
            };
        }
        if (!advance_sequence_if_present(book, event.effective_sequence_id()) ||
            !apply_delta_data(book, *delta)) {
            ++book.apply_reject_count;
            return reject(BookApplyRejectReason::kDeltaSequenceOrLevelInvalid);
        }

        ++book.delta_count;
        break;
    }
    case internal::EventType::kTrade: {
        const auto* trade = std::get_if<internal::TradeData>(&event.data);
        if (trade == nullptr || book.desynced) {
            ++book.apply_reject_count;
            return reject(BookApplyRejectReason::kTradeInvalidOrDesynced);
        }
        book.last_trade = *trade;
        ++book.trade_count;
        break;
    }
    case internal::EventType::kHeartbeat:
    case internal::EventType::kStatus:
    case internal::EventType::kUnknown:
        ++book.apply_reject_count;
        return reject(BookApplyRejectReason::kUnsupportedEventType);
    }
    return BookApplyResult{
        .applied = true,
        .reason = BookApplyRejectReason::kNone,
    };
}

bool BookStore::apply(const internal::NormalizedEvent& event) {
    return apply_with_result(event).applied;
}

const BookState* BookStore::find(std::string_view market_ticker) const {
    const auto book_it = books_.find(std::string(market_ticker));
    if (book_it == books_.end()) {
        return nullptr;
    }
    return &book_it->second;
}

std::size_t BookStore::size() const { return books_.size(); }

} // namespace trading::shards
