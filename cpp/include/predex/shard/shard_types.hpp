#pragma once 
#include "predex/shard/event.hpp"
#include "predex/shard/models.hpp"
#include "predex/shard/market_parser.hpp"
#include <vector>
#include <variant>
#include <cstdint>
#include <string>

namespace predex::shard{

    enum class ShardRunState : std::uint8_t{
        kUNINSTALLED = 0,
        kLIVE = 1,
        kPREPARING_STOP = 2,
        kSAFE_TO_STOP = 3,
        kDRAINING = 4,
        kDRAINED = 5,
        kFAULTED = 6,
    };

    struct InstallShardUniverse{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
        std::vector<KalshiEvent> events;
    };

    struct PrepareStopUniverse{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
    };

    struct DrainShardUniverse{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
    };

    struct ResumeShardUniverse{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
    };

    using ControlToShardCommand = std::variant<InstallShardUniverse, PrepareStopUniverse, DrainShardUniverse, ResumeShardUniverse>;

    struct ShardUniverseInstalled{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
        std::size_t event_count{};
    };

    struct ShardFaulted{
        std::uint32_t shard_index{};
        std::uint64_t universe_version{};
        std::string reason;
    };

    struct ShardStats{
        std::uint64_t frames_seen{0};
        std::uint64_t frames_applied{0};
        std::uint64_t parse_rejects{0};
        std::uint64_t event_rejects{0};
        std::uint64_t event_desyncs{0};
        std::uint64_t frames_to_logger{0};
        std::uint64_t frames_recycled{0};
        std::uint64_t leaked_handles{0};
        std::uint64_t missed_frames_to_logger{0};
        std::uint64_t event_ignored{0};

        std::uint64_t market_barriers_seen{0};
        std::uint64_t subscription_barriers_seen{0};
        std::uint64_t barrier_rejects{0};
        std::uint64_t markets_became_unusable{0};
        std::uint64_t markets_recovery_required{0};
        std::uint64_t markets_already_awaiting_recovery{0};
    };

    struct ShardSafeToStopUniverse{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
        std::string reason;
    };

    struct ShardDrainComplete{
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};
        ShardStats stats{};
    };

    struct ShardParseRejected{
        std::uint32_t shard_index{};
        std::uint64_t universe_version{};
        std::uint64_t sequence{};
        MarketId market_id{};
        EventId event_id{};
        KalshiParseFailureReason reason{};
    };

    struct ShardApplyRejected{
        std::uint32_t shard_index{};
        std::uint64_t universe_version{};
        std::uint64_t sequence{};
        MarketId market_id{};
        EventId event_id{};
        EventApplyResult result{};
    };

    struct ShardEventDesynced{
        std::uint32_t shard_index{};
        std::uint64_t universe_version{};
        std::uint64_t sequence{};
        MarketId market_id{};
        EventId event_id{};
        EventApplyResult result{};
    };

    struct ShardTelemetry{
        std::uint32_t shard_index{};
        std::uint64_t universe_version{};
        ShardStats stats{};
    };
    
    struct ShardMarketRecoveryRequired {
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};

        ingest::kalshi::IntegrityIncidentKey incident{};

        std::uint32_t sid{};
        std::uint64_t sequence{};

        MarketId market_id{};
        EventId event_id{};
        ingest::kalshi::BookInvalidationReason reason{
            ingest::kalshi::BookInvalidationReason::kNONE
        };
    };

    struct ShardSubscriptionRecoveryRequired {
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};

        ingest::kalshi::IntegrityIncidentKey incident{};

        std::uint32_t sid{};
        std::uint64_t expected_sequence{};
        std::uint64_t observed_sequence{};

        ingest::kalshi::BookInvalidationReason reason{
            ingest::kalshi::BookInvalidationReason::kNONE
        };
    };

    struct ShardRecoverySnapshotApplied {
        core::control::RecoveryId recovery_id{};
        std::uint64_t universe_version{};
        std::uint32_t shard_index{};

        std::uint32_t sid{};
        std::uint64_t sequence{};

        MarketId market_id{};
        BookSyncTransition transition{BookSyncTransition::kNONE};
    };
    using ShardToControlMessage = std::variant<
        ShardUniverseInstalled,
        ShardSafeToStopUniverse,
        ShardDrainComplete,
        ShardParseRejected,
        ShardApplyRejected,
        ShardEventDesynced,
        ShardTelemetry,
        ShardFaulted,
        ShardStats,
        ShardMarketRecoveryRequired,
        ShardRecoverySnapshotApplied
    >;
}
