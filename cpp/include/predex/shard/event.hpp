#pragma once 
#include "predex/shard/models.hpp"
#include "predex/ingest/kalshi/market_data/integrity_messages.hpp"
namespace predex::shard{

    enum class ApplyDisposition : std::uint8_t{
        kAPPLIED,
        kIGNORED,
        kREJECTED,
    };
    enum class BookSyncTransition : std::uint8_t{
        kNONE,
        kINITIAL_SNAPSHOT_INSTALLED,
        kBECAME_UNUSABLE,
        kRECOVERED,
        kRECOVERY_REQUIRED,
    };

    enum class MarketApplyReason : std::uint8_t{
        kNONE,
        kUNKNOWN_EVENT_TYPE,
        kINVALID_MARKET_INDEX,
        kINVALID_SIDE,
        kINVALID_PRICE,
        kMISSING_INITIAL_SNAPSHOT,
        kMISSING_RECOVERY_SNAPSHOT,
        kNEGATIVE_LEVEL,
        kINVALID_BOOK_SCALE,
        kOVERFLOW,
    };

    struct MarketApplyResult {
        ApplyDisposition disposition{ApplyDisposition::kAPPLIED};
        BookSyncTransition book_sync_transition{BookSyncTransition::kNONE};
        MarketApplyReason reason{MarketApplyReason::kNONE};
    };

    struct EventApplyResult {
        ApplyDisposition disposition{ApplyDisposition::kAPPLIED};
        BookSyncTransition book_sync_transition{
            BookSyncTransition::kNONE
        };
        MarketApplyReason reason{MarketApplyReason::kNONE};
    };

    enum class InvalidationRejectReason : std::uint8_t {
        kNONE,
        kINVALID_EVENT_INDEX,
        kINVALID_MARKET_INDEX,
        kMARKET_ID_MISMATCH,
    };

    struct BookInvalidationResult{
        bool target_found{false};
        BookSyncTransition book_sync_transition{BookSyncTransition::kNONE};
        predex::ingest::kalshi::BookInvalidationReason reason{};
        InvalidationRejectReason reject_reason{InvalidationRejectReason::kNONE};
    };

    struct BookInvalidationSummary{
        std::uint64_t targets_found{0};
        std::uint64_t targets_became_unusable{0};
        std::uint64_t targets_recovery_required{0};
        std::uint64_t targets_already_awaiting_recovery{0};
    };


    class Event{
        public:
            explicit Event(KalshiEvent state);

            [[nodiscard]] EventId event_id() const noexcept;
            [[nodiscard]] EventTopology event_topology() const noexcept;
            [[nodiscard]] std::uint32_t shard_event_index() const noexcept;

            [[nodiscard]] bool usable() const noexcept;

            [[nodiscard]] EventApplyResult apply(std::uint32_t event_market_index, const KalshiParsedEvent& parsed_event) noexcept;

            [[nodiscard]] const KalshiMarket* get_market(std::uint32_t event_market_index) const noexcept;

            [[nodiscard]] BookInvalidationResult invalidate_market(std::uint32_t event_market_index, predex::ingest::kalshi::BookInvalidationReason reason) noexcept; 
            [[nodiscard]] BookInvalidationSummary invalidate_all_markets(predex::ingest::kalshi::BookInvalidationReason reason) noexcept;

        private:
            KalshiEvent state_;

            [[nodiscard]] MarketApplyResult apply_to_market(KalshiMarket& market, const KalshiParsedEvent& parsed_event) noexcept;

            [[nodiscard]] MarketApplyResult apply_snapshot(KalshiMarket& market, const KalshiSnapshotEvent& parsed_event) noexcept;
            [[nodiscard]] MarketApplyResult apply_delta(KalshiMarket& market, const KalshiDeltaData& parsed_event) noexcept;
            [[nodiscard]] MarketApplyResult apply_trade(KalshiMarket& market, const KalshiTradeData& parsed_event) noexcept;
            [[nodiscard]] MarketApplyResult apply_lifecycle(KalshiMarket& market, const KalshiLifecycleData& parsed_event) noexcept;

            void update_derived_state_after_market_update(std::uint32_t event_market_index);

            [[nodiscard]] MarketApplyResult reject_snapshot(KalshiMarket& market, MarketApplyReason reason) noexcept;
            [[nodiscard]] MarketApplyResult reject_delta(KalshiMarket& market, MarketApplyReason reason) noexcept;
            bool install_level(const KalshiBook& book, std::vector<QtyLots>& levels, const Level& level) const noexcept;

    };
}