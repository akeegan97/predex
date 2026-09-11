#include "predex/shard/event.hpp"
#include "predex/strategy/strategy_types.hpp"
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

    EventApplyResult Event::apply(
        std::uint32_t event_market_index,
        KalshiParsedEvent&& parsed_event) noexcept {

        if (event_market_index >= state_.markets.size()) {
            return EventApplyResult{
                .disposition = ApplyDisposition::kREJECTED,
                .book_sync_transition =
                    BookSyncTransition::kNONE,
                .reason =
                    MarketApplyReason::kINVALID_MARKET_INDEX,
                .book_changed = false,
                .event_became_usable = false,
                .event_revision = revision_,
            };
        }

        const bool event_was_usable = usable();

        const bool is_book_message =
            std::holds_alternative<KalshiSnapshotEvent>(
                parsed_event) ||
            std::holds_alternative<KalshiDeltaData>(
                parsed_event);

        auto& market = state_.markets[event_market_index];

        const MarketApplyResult market_result =
            apply_to_market(market, std::move(parsed_event));

        const bool book_changed =
            is_book_message &&
            market_result.disposition ==
                ApplyDisposition::kAPPLIED;

        const bool sync_state_changed =
            market_result.book_sync_transition !=
                BookSyncTransition::kNONE;

        if (book_changed || sync_state_changed) {
            ++revision_;
        }

        const bool event_is_usable = usable();

        const bool event_became_usable =
            !event_was_usable && event_is_usable;

        if (book_changed && event_is_usable) {
            update_derived_state_after_market_update(
                event_market_index);
        }

        return EventApplyResult{
            .disposition = market_result.disposition,
            .book_sync_transition =
                market_result.book_sync_transition,
            .reason = market_result.reason,
            .book_changed = book_changed,
            .event_became_usable =
                event_became_usable,
            .event_revision = revision_,
        };
    }

    std::uint64_t Event::revision() const noexcept{
        return revision_;
    }

    const KalshiMarket* Event::get_market(std::uint32_t event_market_index) const noexcept{
        if(event_market_index >= state_.markets.size()){
            return nullptr;
        }
        return &state_.markets[event_market_index];
    }

    MarketApplyResult Event::apply_to_market(//NOLINT - bugprone-exception-escape std::visit will not hit it's valueless_by_exception here
        KalshiMarket& market,
        KalshiParsedEvent&& parsed_event) noexcept{ 

        return std::visit(
            [&](auto&& event) noexcept -> MarketApplyResult {
                using T = std::decay_t<decltype(event)>;

                if constexpr (std::is_same_v<T, KalshiSnapshotEvent>) {
                    return apply_snapshot(market, std::forward<decltype(event)>(event));

                } else if constexpr (std::is_same_v<T, KalshiDeltaData>) {
                    return apply_delta(market, std::forward<decltype(event)>(event));

                } else if constexpr (std::is_same_v<T, KalshiTradeData>) {
                    return apply_trade(market, std::forward<decltype(event)>(event));

                } else if constexpr (std::is_same_v<T, KalshiLifecycleData>) {
                    return apply_lifecycle(market, std::forward<decltype(event)>(event));

                } else {
                    return MarketApplyResult{
                        .disposition = ApplyDisposition::kREJECTED,
                        .book_sync_transition = BookSyncTransition::kNONE,
                        .reason = MarketApplyReason::kUNKNOWN_EVENT_TYPE
                    };
                }
            },
            std::move(parsed_event));
    }

    MarketApplyResult Event::apply_snapshot(KalshiMarket& market, KalshiSnapshotEvent&& parsed_event) noexcept{
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
            //NOLINTNEXTLINE - readability-avoid-nested-conditional-operator think the below is actually concise to check the transition state
            (prev_sync_state == BookSyncState::kAWAITING_INITIAL_SNAPSHOT ? BookSyncTransition::kINITIAL_SNAPSHOT_INSTALLED : BookSyncTransition::kRECOVERED) : BookSyncTransition::kNONE,
            .reason = MarketApplyReason::kNONE
        };
    }

    MarketApplyResult Event::apply_delta(KalshiMarket& market, KalshiDeltaData&& parsed_event) noexcept{
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

    MarketApplyResult Event::apply_trade(KalshiMarket& market, KalshiTradeData&& parsed_event) noexcept{
        // Trades do not mutate book depth. Keep this stubbed until the event metrics bundle
        // owns trade-derived features such as OBI/VPIN/flow stats.
        return MarketApplyResult{
            .disposition = ApplyDisposition::kAPPLIED,
            .book_sync_transition = BookSyncTransition::kNONE,
            .reason = MarketApplyReason::kNONE
        };
    }

    MarketApplyResult Event::apply_lifecycle(KalshiMarket& market, KalshiLifecycleData&& parsed_event) noexcept{
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
                .reject_reason = InvalidationRejectReason::kINVALID_MARKET_INDEX,
                .event_revision = revision_,
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

        if (transition != BookSyncTransition::kNONE) {
            ++revision_;
        }

        return BookInvalidationResult{
            .target_found = true,
            .book_sync_transition = transition,
            .reason = reason,
            .reject_reason =
                InvalidationRejectReason::kNONE,
            .event_revision = revision_,
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

    std::size_t Event::market_count() const noexcept{
        return state_.markets.size();
    }
//NOLINTNEXTLINE
    std::optional<strategy::MonotonicPairObservation> Event::build_monotonic_pair_observation(std::uint32_t easier_market_index, const strategy::StrategyObservationContext& context) const noexcept{
        if(event_topology() != EventTopology::kMONOTONIC_CHAIN){
            return std::nullopt;
        }

        if(!usable()){
            return std::nullopt;
        }

        if (state_.markets.size() < 2 ||
            easier_market_index >= state_.markets.size() - 1) {
            return std::nullopt;
        }
        
        const KalshiMarket& easier_market = state_.markets[easier_market_index];
        const KalshiMarket& harder_market = state_.markets[easier_market_index + 1];
        
        if(easier_market.event_market_index != easier_market_index ||
           harder_market.event_market_index != easier_market_index + 1 ||
           !easier_market.strike_key.has_value() ||
           !harder_market.strike_key.has_value() ||
            (easier_market.strike_key.value() >= harder_market.strike_key.value())){
            return std::nullopt;
        }

        strategy::StrategyMarketView easier_market_view{};
        strategy::StrategyMarketView harder_market_view{};

        easier_market_view.market_id = easier_market.market_id;
        harder_market_view.market_id = harder_market.market_id;
        
        easier_market_view.event_market_index = easier_market.event_market_index;
        harder_market_view.event_market_index = harder_market.event_market_index;
        
        easier_market_view.strike_key = easier_market.strike_key.value();
        harder_market_view.strike_key = harder_market.strike_key.value();

        easier_market_view.tradeable = easier_market.tradeable;
        harder_market_view.tradeable = harder_market.tradeable;

        easier_market_view.market_time_s = easier_market.market_time_s;
        harder_market_view.market_time_s = harder_market.market_time_s;
        
        easier_market_view.market_close_time_s = easier_market.market_close_time_s;
        harder_market_view.market_close_time_s = harder_market.market_close_time_s;
        
        easier_market_view.market_expected_expiration_time_s = easier_market.market_expected_expiration_time_s;
        harder_market_view.market_expected_expiration_time_s = harder_market.market_expected_expiration_time_s;

        easier_market_view.market_expiration_time_s = easier_market.market_expiration_time_s;
        harder_market_view.market_expiration_time_s = harder_market.market_expiration_time_s;
    
        std::uint8_t bid_count = 0;

        for (std::size_t index = easier_market.book.bids.size();
            index > 0 && bid_count < strategy::kStrategyBookDepth;
            --index) {

            const std::size_t book_index = index - 1;
            const QtyLots quantity = easier_market.book.bids[book_index];

            if (quantity == 0) {
                continue;
            }

            const auto price =
                easier_market.book.price_ticks_at_index(book_index);

            if (!price.has_value()) {
                return std::nullopt;
            }

            easier_market_view.bids[bid_count] = {
                .price_ticks = *price,
                .quantity_lots = quantity,
            };

            ++bid_count;
        }

        easier_market_view.bid_count = bid_count;

        std::uint8_t ask_count = 0;

        for (std::size_t index = 0;
            index < easier_market.book.asks.size() &&
            ask_count < strategy::kStrategyBookDepth;
            ++index) {

            const QtyLots quantity = easier_market.book.asks[index];

            if (quantity == 0) {
                continue;
            }

            const auto price =
                easier_market.book.price_ticks_at_index(index);

            if (!price.has_value()) {
                return std::nullopt;
            }

            easier_market_view.asks[ask_count] = {
                .price_ticks = *price,
                .quantity_lots = quantity,
            };

            ++ask_count;
        }

        easier_market_view.ask_count = ask_count;

        bid_count = 0;
        ask_count = 0;

        for (std::size_t index = harder_market.book.bids.size();
            index > 0 && bid_count < strategy::kStrategyBookDepth;
            --index) {

            const std::size_t book_index = index - 1;
            const QtyLots quantity = harder_market.book.bids[book_index];

            if (quantity == 0) {
                continue;
            }

            const auto price =
                harder_market.book.price_ticks_at_index(book_index);

            if (!price.has_value()) {
                return std::nullopt;
            }

            harder_market_view.bids[bid_count] = {
                .price_ticks = *price,
                .quantity_lots = quantity,
            };

            ++bid_count;
        }

        harder_market_view.bid_count = bid_count;

        for (std::size_t index = 0;
            index < harder_market.book.asks.size() &&
            ask_count < strategy::kStrategyBookDepth;
            ++index) {

            const QtyLots quantity = harder_market.book.asks[index];

            if (quantity == 0) {
                continue;
            }

            const auto price =
                harder_market.book.price_ticks_at_index(index);

            if (!price.has_value()) {
                return std::nullopt;
            }

            harder_market_view.asks[ask_count] = {
                .price_ticks = *price,
                .quantity_lots = quantity,
            };

            ++ask_count;
        }

        harder_market_view.ask_count = ask_count;

        return strategy::MonotonicPairObservation{
            .universe_version = context.universe_version,
            .shard_index = context.shard_index,
            .event_id = event_id(),
            .event_revision = revision(),
            .ingress_timestamp_ns = context.ingress_timestamp_ns,
            .book_apply_timestamp_ns = context.book_apply_timestamp_ns,
            .publish_timestamp_ns = context.publish_timestamp_ns,
            .easier = (easier_market_view),
            .harder = (harder_market_view)
        };


    }
}