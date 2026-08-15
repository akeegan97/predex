#include "predex/shard/event.hpp"
#include <algorithm>
#include <cstddef>

namespace predex::shard{

    Event::Event(KalshiEvent state) : state_(std::move(state)){}

    EventId Event::event_id() const noexcept{
        return state_.event_id;
    }

    EventTopology Event::event_topology() const noexcept{
        return state_.topology;
    }

    std::uint32_t Event::shard_event_index() const noexcept{
        return state_.shard_event_index;
    }

    bool Event::usable() const noexcept{
        return !state_.markets.empty() && 
            std::ranges::all_of(state_.markets, [](const KalshiMarket& market){
                return market.book.usable();
            });
    }

    EventApplyResult Event::apply(std::uint32_t event_market_index, const KalshiParsedEvent& parsed_event) noexcept{
        if(event_market_index >= state_.markets.size()){
            return EventApplyResult{
                .disposition = ApplyDisposition::kREJECTED,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = MarketApplyReason::kINVALID_MARKET_INDEX
            };
        }
        auto& market = state_.markets[event_market_index];
        const auto result = apply_to_market(market, parsed_event);

        if(result.book_sync_transition == BookSyncTransition::kBECAME_UNUSABLE){
            //future plug in: make sure all strategy derived features are cleared and communicated to strategy to avoid stale data.
        }

        if(result.disposition == ApplyDisposition::kAPPLIED && usable()){
            update_derived_state_after_market_update(event_market_index);
        }
        return EventApplyResult{
            .disposition = result.disposition,
            .book_sync_transition = result.book_sync_transition,
            .reason = result.reason,
        };
    }

    const KalshiMarket* Event::get_market(std::uint32_t event_market_index) const noexcept{
        if(event_market_index >= state_.markets.size()){
            return nullptr;
        }
        return &state_.markets[event_market_index];
    }

    MarketApplyResult Event::apply_to_market(KalshiMarket& market, const KalshiParsedEvent& parsed_event) noexcept{
        return std::visit([&](const auto& event)->MarketApplyResult{
            using T = std::decay_t<decltype(event)>;
            if constexpr(std::is_same_v<T, KalshiSnapshotEvent>){
                return apply_snapshot(market, event);
            }else if constexpr(std::is_same_v<T, KalshiDeltaData>){
                return apply_delta(market, event);
            }else if constexpr(std::is_same_v<T, KalshiTradeData>){
                return apply_trade(market, event);
            }else if constexpr(std::is_same_v<T, KalshiLifecycleData>){
                return apply_lifecycle(market, event);
            }else{
                return MarketApplyResult{
                    .disposition = ApplyDisposition::kREJECTED,
                    .book_sync_transition = BookSyncTransition::kNONE,
                    .reason = MarketApplyReason::kUNKNOWN_EVENT_TYPE
                };
            }
        }, parsed_event);
    }
    //TODO: make this actually "noexcept" since set_index_grid() could theoretically throw with the vector allocation.
    MarketApplyResult Event::apply_snapshot(KalshiMarket& market, const KalshiSnapshotEvent& parsed_event) noexcept{
        KalshiBook candidate{};
        candidate.scale = market.book.scale;
        if(!candidate.set_index_grid()){
            return reject_snapshot(market, MarketApplyReason::kINVALID_BOOK_SCALE);
        }
        for(const auto& bid : parsed_event.bids){
            if(!install_level(candidate, candidate.bids, bid)){
                return reject_snapshot(market, MarketApplyReason::kINVALID_PRICE);
            }
        }
        for(const auto& ask : parsed_event.asks){
            if(!install_level(candidate, candidate.asks, ask)){
                return reject_snapshot(market, MarketApplyReason::kINVALID_PRICE);
            }
        }
        candidate.sync_state = BookSyncState::kSYNCHRONIZED;
        const bool became_usable = !market.book.usable();
        const auto prev_sync_state = market.book.sync_state;
        market.book = std::move(candidate);
        return MarketApplyResult{
            .disposition = ApplyDisposition::kAPPLIED,
            .book_sync_transition = became_usable ? 
            (prev_sync_state == BookSyncState::kAWAITING_INITIAL_SNAPSHOT ? BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED : BookSyncTransition::kRECOVERED) : BookSyncTransition::kNONE,
            .reason = MarketApplyReason::kNONE
        };
    }

    MarketApplyResult Event::apply_delta(KalshiMarket& market, const KalshiDeltaData& parsed_event) noexcept{
        if(market.book.sync_state == BookSyncState::kAWAITING_INITIAL_SNAPSHOT){
            return{
                .disposition = ApplyDisposition::kIGNORED,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = MarketApplyReason::kMISSING_INITIAL_SNAPSHOT
            };
        }

        if(market.book.sync_state == BookSyncState::kAWAITING_RECOVERY_SNAPSHOT){
            return{
                .disposition = ApplyDisposition::kIGNORED,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = MarketApplyReason::kMISSING_RECOVERY_SNAPSHOT
            };
        }

        auto index_opt = market.book.get_index(parsed_event.price_ticks);
        if(!index_opt.has_value()){
            return reject_delta(market, MarketApplyReason::kINVALID_PRICE);
        }

        const std::size_t index = *index_opt;

        if(parsed_event.side == Side::kBID){
            auto& level = market.book.bids[index];
            if(parsed_event.delta_qty_lots < 0){
                const auto remove_qty = static_cast<QtyLots>(-(parsed_event.delta_qty_lots +1))+1;
                if(level < remove_qty){
                    return reject_delta(market, MarketApplyReason::kNEGATIVE_LEVEL);
                }
                level -= remove_qty;
            }else{
                const auto add_qty = static_cast<QtyLots>(parsed_event.delta_qty_lots);
                if(level > std::numeric_limits<QtyLots>::max() - add_qty){
                    return reject_delta(market, MarketApplyReason::kOVERFLOW);
                }
                level += static_cast<QtyLots>(parsed_event.delta_qty_lots);
            }
        }else if(parsed_event.side == Side::kASK){
            auto& level = market.book.asks[index];
            if(parsed_event.delta_qty_lots < 0){
                const auto remove_qty = static_cast<QtyLots>(-(parsed_event.delta_qty_lots +1))+1;
                if(level < remove_qty){
                    return reject_delta(market, MarketApplyReason::kNEGATIVE_LEVEL);
                }
                level -= remove_qty;
            }else{
                const auto add_qty = static_cast<QtyLots>(parsed_event.delta_qty_lots);
                if(level > std::numeric_limits<QtyLots>::max() - add_qty){
                    return reject_delta(market, MarketApplyReason::kOVERFLOW);
                }
                level += add_qty;
            }
        }else{
            return reject_delta(market, MarketApplyReason::kINVALID_SIDE);
        }
        return MarketApplyResult{ApplyDisposition::kAPPLIED, BookSyncTransition::kNONE};

    }

    MarketApplyResult Event::apply_trade(KalshiMarket& market, const KalshiTradeData& parsed_event) noexcept{
        // Trades do not mutate book depth. Keep this stubbed until the event metrics bundle
        // owns trade-derived features such as OBI/VPIN/flow stats.
        return MarketApplyResult{
            .disposition = ApplyDisposition::kAPPLIED,
            .book_sync_transition = BookSyncTransition::kNONE,
            .reason = MarketApplyReason::kNONE
        };
    }

    MarketApplyResult Event::apply_lifecycle(KalshiMarket& market, const KalshiLifecycleData& parsed_event) noexcept{
        //stub: only lifecycle event I want to handle is the updated market close, etc
        return MarketApplyResult{
            .disposition = ApplyDisposition::kAPPLIED,
            .book_sync_transition = BookSyncTransition::kNONE,
            .reason = MarketApplyReason::kNONE
        };
    }

    void Event::update_derived_state_after_market_update(std::uint32_t event_market_index){
        //updating derived state (rolling metrics, state to hand over to model thread, etc)
    }

    MarketApplyResult Event::reject_snapshot(
        KalshiMarket& market,
        MarketApplyReason reason) noexcept
    {
        const auto previous_state = market.book.sync_state;

        // A new snapshot cannot repair invalid static market configuration.
        if (reason == MarketApplyReason::kINVALID_BOOK_SCALE) {
            return MarketApplyResult{
                .disposition = ApplyDisposition::kREJECTED,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = reason,
            };
        }

        market.book.invalidate();

        BookSyncTransition transition = BookSyncTransition::kNONE;

        switch (previous_state) {
            case BookSyncState::kSYNCHRONIZED:
                transition = BookSyncTransition::kBECAME_UNUSABLE;
                break;

            case BookSyncState::kAWAITING_INITIAL_SNAPSHOT:
                transition = BookSyncTransition::kRECOVERY_REQUIRED;
                break;

            case BookSyncState::kAWAITING_RECOVERY_SNAPSHOT:
                // Recovery incident is already active.
                transition = BookSyncTransition::kNONE;
                break;
        }

        return MarketApplyResult{
            .disposition = ApplyDisposition::kREJECTED,
            .book_sync_transition = transition,
            .reason = reason,
        };
    }

    MarketApplyResult Event::reject_delta(KalshiMarket& market, MarketApplyReason reason) noexcept{
        const bool was_usable = market.book.usable();
        market.book.invalidate();
        return MarketApplyResult{
            .disposition = ApplyDisposition::kREJECTED,
            .book_sync_transition = was_usable ? BookSyncTransition::kBECAME_UNUSABLE : BookSyncTransition::kNONE,
            .reason = reason
        };
    }

    bool Event::install_level(const KalshiBook& book, std::vector<QtyLots>& levels, const Level& level) const noexcept{
        auto index_opt = book.get_index(level.price_ticks);
        if(!index_opt.has_value() || *index_opt >= levels.size()){
            return false;
        }

        levels[*index_opt] = level.qty_lots;
        return true;
    }

    BookInvalidationResult Event::invalidate_market(std::uint32_t event_market_index, predex::ingest::kalshi::BookInvalidationReason reason) noexcept{
        if(event_market_index >= state_.markets.size()){
            return BookInvalidationResult{
                .target_found = false,
                .book_sync_transition = BookSyncTransition::kNONE,
                .reason = reason,
                .reject_reason = InvalidationRejectReason::kINVALID_MARKET_INDEX
            };
        }
        auto& book = state_.markets[event_market_index].book;

        const auto previous_state = book.sync_state;

        if(previous_state != BookSyncState::kAWAITING_RECOVERY_SNAPSHOT){
            book.invalidate();
        }

        BookSyncTransition transition = BookSyncTransition::kNONE;

        switch(previous_state){
            case BookSyncState::kSYNCHRONIZED:
                transition = BookSyncTransition::kBECAME_UNUSABLE;
                break;
            case BookSyncState::kAWAITING_INITIAL_SNAPSHOT:
                transition = BookSyncTransition::kRECOVERY_REQUIRED;
                break;
            case BookSyncState::kAWAITING_RECOVERY_SNAPSHOT:
                transition = BookSyncTransition::kNONE;
                break;
        }

        if(transition != BookSyncTransition::kNONE){
            //FUTURE: wipe/make all derived features stale for this market & communicate to strategy/model thread to avoid stale data.
        }

        return BookInvalidationResult{
            .target_found = true,
            .book_sync_transition = transition,
            .reason = reason,
            .reject_reason = InvalidationRejectReason::kNONE
        };
    }

    BookInvalidationSummary Event::invalidate_all_markets(predex::ingest::kalshi::BookInvalidationReason reason) noexcept{
        BookInvalidationSummary summary{};
        for(std::uint32_t idx = 0; idx < state_.markets.size(); ++idx){
            const auto result = invalidate_market(idx, reason);
            if(!result.target_found){
                continue;
            }
            ++summary.targets_found;
            switch(result.book_sync_transition){
                case BookSyncTransition::kBECAME_UNUSABLE:
                    ++summary.targets_became_unusable;
                    break;
                case BookSyncTransition::kRECOVERY_REQUIRED:
                    ++summary.targets_recovery_required;
                    break;
                case BookSyncTransition::kNONE:
                    ++summary.targets_already_awaiting_recovery;
                    break;
                case BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED:
                case BookSyncTransition::kRECOVERED:
                    // This should never happen when invalidating all markets.
                    break;
            }
        }
        return summary;
    }
}