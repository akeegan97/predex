#include "predex/shard/event.hpp"

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

    bool Event::desynced() const noexcept{
        return state_.desynced;
    }

    EventApplyResult Event::apply(std::uint32_t event_market_index, const KalshiParsedEvent& parsed_event) noexcept{
        if(event_market_index >= state_.markets.size()){
            return EventApplyResult{EventApplyCode::kREJECTED, EventDesyncReason::kINVALID_MARKET_INDEX};
        }
        KalshiMarket& market = state_.markets[event_market_index];
        MarketApplyResult market_result = apply_to_market(market, parsed_event);
        switch(market_result.code){
            case MarketApplyCode::kAPPLIED:
                update_derived_state_after_market_update(event_market_index);
                return EventApplyResult{EventApplyCode::kAPPLIED, EventDesyncReason::kNONE};
            case MarketApplyCode::kREJECTED:
                return EventApplyResult{EventApplyCode::kREJECTED, EventDesyncReason::kNONE};
            case MarketApplyCode::kDESYNCED:
                mark_desynced(EventDesyncReason::kMARKET_APPLY_DESYNC);
                return EventApplyResult{EventApplyCode::kDESYNCED, last_desync_reason_};
        }

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
                return MarketApplyResult{MarketApplyCode::kREJECTED, MarketRejectReason::kUNKNOWN_EVENT_TYPE};
            }
        }, parsed_event);
    }

    MarketApplyResult Event::apply_snapshot(KalshiMarket& market, const KalshiSnapshotEvent& parsed_event) noexcept{
        market.book.set_index_grid();
        for(const auto& bid : parsed_event.bids){
            if(bid.price_ticks >= market.book.index_by_tick.size()){
                market.book.has_snapshot = false;
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            auto index_opt = market.book.get_index(bid.price_ticks);
            if(!index_opt.has_value()){
                market.book.has_snapshot = false;
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            std::size_t index = index_opt.value();
            if(index >= market.book.bids.size()){
                market.book.has_snapshot = false;
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            market.book.bids[index] = bid.qty_lots;
        }
        for(const auto& ask : parsed_event.asks){
            if(ask.price_ticks >= market.book.index_by_tick.size()){
                market.book.has_snapshot = false;
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            auto index_opt = market.book.get_index(ask.price_ticks);
            if(!index_opt.has_value()){
                market.book.has_snapshot = false;
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            std::size_t index = index_opt.value();
            if(index >= market.book.asks.size()){
                market.book.has_snapshot = false;
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            market.book.asks[index] = ask.qty_lots;
        }

        market.book.has_snapshot = true;
        market.book.desynced = false;

        return MarketApplyResult{MarketApplyCode::kAPPLIED, MarketRejectReason::kNONE};
    }

    MarketApplyResult Event::apply_delta(KalshiMarket& market, const KalshiDeltaData& parsed_event) noexcept{
        if(!market.book.has_snapshot){
            return MarketApplyResult{MarketApplyCode::kREJECTED, MarketRejectReason::kMISSING_SNAPSHOT};
        }
        auto index_opt = market.book.get_index(parsed_event.price_ticks);
        if(!index_opt.has_value()){
            market.book.desynced = true;
            return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
        }
        std::size_t index = index_opt.value();
        if(parsed_event.side == Side::kBID){
            if(index >= market.book.bids.size()){
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            auto& level = market.book.bids[index];
            if(parsed_event.delta_qty_lots < 0){
                const auto remove_qty = static_cast<QtyLots>(-(parsed_event.delta_qty_lots + 1)) + 1; // handle case where delta_qty_lots is INT64_MIN, which would overflow if negated 
                if(level < remove_qty){
                    market.book.desynced = true;
                    return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kNEGATIVE_LEVEL};
                }
                level -= remove_qty;
            }else{
                level += static_cast<predex::shard::QtyLots>(parsed_event.delta_qty_lots);
            }
        }else if(parsed_event.side == Side::kASK){
            if(index >= market.book.asks.size()){
                market.book.desynced = true;
                return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kINVALID_PRICE};
            }
            auto& level = market.book.asks[index];
            if(parsed_event.delta_qty_lots < 0){
                const auto remove_qty = static_cast<QtyLots>(-(parsed_event.delta_qty_lots + 1)) + 1; // handle case where delta_qty_lots is INT64_MIN, which would overflow if negated 
                if(level < remove_qty){
                    market.book.desynced = true;
                    return MarketApplyResult{MarketApplyCode::kDESYNCED, MarketRejectReason::kNEGATIVE_LEVEL};
                }
                level -= remove_qty;
            }else{
                level += static_cast<predex::shard::QtyLots>(parsed_event.delta_qty_lots);
            }
        }else{
            return MarketApplyResult{MarketApplyCode::kREJECTED, MarketRejectReason::kINVALID_SIDE};
        }
        return MarketApplyResult{MarketApplyCode::kAPPLIED, MarketRejectReason::kNONE};
    }

    MarketApplyResult Event::apply_trade(KalshiMarket& market, const KalshiTradeData& parsed_event) noexcept{
        // Trades do not mutate book depth. Keep this stubbed until the event metrics bundle
        // owns trade-derived features such as OBI/VPIN/flow stats.
        return MarketApplyResult{MarketApplyCode::kAPPLIED, MarketRejectReason::kNONE};
    }

    MarketApplyResult Event::apply_lifecycle(KalshiMarket& market, const KalshiLifecycleData& parsed_event) noexcept{
        //stub: only lifecycle event I want to handle is the updated market close, etc
        return MarketApplyResult{MarketApplyCode::kAPPLIED, MarketRejectReason::kNONE};
    }

    void Event::update_derived_state_after_market_update(std::uint32_t event_market_index){
        //updating derived state (rolling metrics, state to hand over to model thread, etc)
    }

    void Event::mark_desynced(EventDesyncReason reason){
        state_.desynced = true;
        last_desync_reason_ = reason;
    }


}