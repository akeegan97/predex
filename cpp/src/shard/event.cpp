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
        //stub: apply snapshot logic here, also check that snapshot is valid & update market state accordingly
        return MarketApplyResult{MarketApplyCode::kAPPLIED, MarketRejectReason::kNONE};
    }

    MarketApplyResult Event::apply_delta(KalshiMarket& market, const KalshiDeltaData& parsed_event) noexcept{
        //stub: apply delta logic here, also check that snapshot is present & that delta doesn't cause qty <0
        return MarketApplyResult{MarketApplyCode::kAPPLIED, MarketRejectReason::kNONE};
    }

    MarketApplyResult Event::apply_trade(KalshiMarket& market, const KalshiTradeData& parsed_event) noexcept{
        // apply trade logic here : used for tracking aggressor side, rolling trade metrics, etc
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