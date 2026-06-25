#pragma once 
#include "predex/shard/models.hpp"

namespace predex::shard{

    enum class MarketApplyCode : std::uint8_t {
        kAPPLIED = 0,
        kREJECTED = 1,
        kDESYNCED = 2,
    };

    enum class MarketRejectReason : std::uint8_t {
        kNONE = 0,
        kUNKNOWN_EVENT_TYPE,
        kINVALID_MARKET_INDEX,
        kINVALID_SIDE,
        kINVALID_PRICE,
        kMISSING_SNAPSHOT,
        kNEGATIVE_LEVEL,
        kMARKET_DESYNCED,
    };

    struct MarketApplyResult {
        MarketApplyCode code{MarketApplyCode::kAPPLIED};
        MarketRejectReason reason{MarketRejectReason::kNONE};
    };

    enum class EventApplyCode : std::uint8_t {
        kAPPLIED = 0,
        kREJECTED = 1,
        kDESYNCED = 2,
    };

    enum class EventDesyncReason : std::uint8_t {
        kNONE = 0,
        kMARKET_APPLY_DESYNC,
        kTOPOLOGY_RECOMPUTE_FAILED,
        kINVALID_MARKET_INDEX,
    };

    struct EventApplyResult {
        EventApplyCode code{EventApplyCode::kAPPLIED};
        EventDesyncReason reason{EventDesyncReason::kNONE};
    };

    class Event{
        public:
            explicit Event(KalshiEvent state);

            [[nodiscard]] EventId event_id() const noexcept;
            [[nodiscard]] EventTopology event_topology() const noexcept;
            [[nodiscard]] std::uint32_t shard_event_index() const noexcept;

            [[nodiscard]] bool desynced() const noexcept;

            [[nodiscard]] EventApplyResult apply(std::uint32_t event_market_index, const KalshiParsedEvent& parsed_event) noexcept;

            [[nodiscard]] const KalshiMarket* get_market(std::uint32_t event_market_index) const noexcept;
        private:
            KalshiEvent state_;
            EventDesyncReason last_desync_reason_{EventDesyncReason::kNONE};

            [[nodiscard]] MarketApplyResult apply_to_market(KalshiMarket& market, const KalshiParsedEvent& parsed_event) noexcept;

            [[nodiscard]] MarketApplyResult apply_snapshot(KalshiMarket& market, const KalshiSnapshotEvent& parsed_event) noexcept;
            [[nodiscard]] MarketApplyResult apply_delta(KalshiMarket& market, const KalshiDeltaData& parsed_event) noexcept;
            [[nodiscard]] MarketApplyResult apply_trade(KalshiMarket& market, const KalshiTradeData& parsed_event) noexcept;
            [[nodiscard]] MarketApplyResult apply_lifecycle(KalshiMarket& market, const KalshiLifecycleData& parsed_event) noexcept;

            void update_derived_state_after_market_update(std::uint32_t event_market_index);
            void mark_desynced(EventDesyncReason reason);
    };
}