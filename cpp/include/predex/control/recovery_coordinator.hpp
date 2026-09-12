#pragma once 
#include <cstddef>
#include <cstdint>
#include <chrono>
#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

#include "predex/control/control_types.hpp"
#include "predex/ingest/kalshi/market_data/integrity_messages.hpp"
#include "predex/router/router_types.hpp"
#include "predex/shard/shard_types.hpp"
namespace predex::core::control{
    enum class MarketRecoveryPhase : std::uint8_t{
        kPENDING_REQUEST = 0,
        kREQUEST_ENQUEUED = 1,
        kREQUEST_ACCEPTED = 2,
        kSNAPSHOT_APPLIED = 3,
        kFAILED = 4,
        kSUPERSEDED = 5,
    };

    enum class RecoveryObservationCode : std::uint8_t{
        kCREATED = 0,
        kDUPLICATE = 1,
        kSTALE_UNIVERSE = 2,
        kINVALID_INCIDENT = 3,
        kINVALID_REASON = 4,
        kUNKNOWN_MARKET = 5,
        kROUTE_MISMATCH = 6,
        kALREADY_RECOVERING = 7,
        kRECOVERY_ID_EXHAUSTED = 8,
    };

    struct RecoveryObservationResult{
        RecoveryObservationCode code{RecoveryObservationCode::kINVALID_INCIDENT};
        RecoveryId recovery_id{};
        std::size_t markets_affected{};
    };

    struct MarketRecoveryState{
        MarketId market_id{};
        EventId event_id{};
        std::uint32_t shard_index{};

        MarketRecoveryPhase phase{MarketRecoveryPhase::kPENDING_REQUEST};

        std::uint32_t attempts_sent{0};
        std::chrono::steady_clock::time_point last_attempt_time;
        std::chrono::steady_clock::time_point next_attempt_time;
    };

    struct RecoveryIncidentState{
        RecoveryId recovery_id{};
        ingest::kalshi::IntegrityIncidentKey source{};
        std::uint64_t universe_version{};
        std::uint32_t sid{};
        ingest::kalshi::BookInvalidationReason reason{
            ingest::kalshi::BookInvalidationReason::kNONE
        };

        std::vector<MarketRecoveryState> market_recoveries;

        std::chrono::steady_clock::time_point started_at;
        std::chrono::steady_clock::time_point last_progress_at;
    };

    enum class RecoveryFactDisposition : std::uint8_t{
        kAPPLIED,
        kIGNORED,
        kREJECTED,
    };

    enum class RecoveryFactEffect : std::uint8_t{
        kNONE,
        kREQUEST_ACCEPTED,
        kRETRY_SCHEDULED,
        kMARKET_RECOVERED,
        kMARKET_FAILED,
    };

    struct RecoveryFactResult{
        RecoveryFactDisposition disposition{RecoveryFactDisposition::kREJECTED};
        RecoveryFactEffect effect{RecoveryFactEffect::kNONE};
        RecoveryId recovery_id{};
        MarketId market_id{};
        bool incident_completed{false};
        std::chrono::nanoseconds recovery_duration{};
    };

    struct RecoverySupersessionSummary{
        std::size_t incidents_superseded{0};
        std::size_t markets_superseded{0};
        std::size_t active_latches_released{0};
    };

    struct RecoveryCoordinatorConfig {
        inline static constexpr std::uint32_t kDefaultMaxAttempts{5};
        inline static constexpr std::chrono::milliseconds kDefaultInitialBackoff{100};
        inline static constexpr std::chrono::milliseconds kDefaultMaxBackoff{5'000};
        inline static constexpr std::chrono::milliseconds kDefaultRequestAckTimeout{2'000};
        inline static constexpr std::chrono::milliseconds kDefaultSnapshotTimeout{5'000};
        std::uint32_t max_attempts{kDefaultMaxAttempts};
        std::chrono::milliseconds initial_backoff{kDefaultInitialBackoff};
        std::chrono::milliseconds max_backoff{kDefaultMaxBackoff};
        std::chrono::milliseconds request_ack_timeout{kDefaultRequestAckTimeout};
        std::chrono::milliseconds snapshot_timeout{kDefaultSnapshotTimeout};
    };

    struct RecoveryTimeoutSummary{
        std::size_t request_ack_timeouts{};
        std::size_t snapshot_timeouts{};
        std::size_t retries_scheduled{};
        std::size_t markets_failed{};
    };

    class RecoveryCoordinator{
        public:
            explicit RecoveryCoordinator(RecoveryCoordinatorConfig config = {}) : config_(config){}
            using TimePoint = std::chrono::steady_clock::time_point;

            [[nodiscard]] RecoveryObservationResult observe(const shard::ShardMarketRecoveryRequired& fact, const UniverseSnapshot& active_universe, TimePoint now);
            [[nodiscard]] RecoveryObservationResult observe(
                const router::OrderBookSubscriptionBarrierDelivered& fact,
                const UniverseSnapshot& active_universe,
                TimePoint now);
            [[nodiscard]] std::optional<RecoverMarketIo> next_pending_command(TimePoint now) const noexcept;
            [[nodiscard]] std::vector<RecoverMarketIo> next_pending_commands(
                TimePoint now,
                std::size_t maximum_commands) const;

            [[nodiscard]] bool mark_command_enqueued(const RecoverMarketIo& command, TimePoint now) noexcept;

        
            RecoveryFactResult handle(const IoRecoveryRequestAccepted& fact, TimePoint now) noexcept;
            RecoveryFactResult handle(const IoRecoveryRequestFailed& fact, TimePoint now) noexcept;
            RecoveryFactResult handle(const shard::ShardRecoverySnapshotApplied& fact, const UniverseSnapshot& active_universe, TimePoint now) noexcept;
            [[nodiscard]] RecoveryTimeoutSummary expire_timeouts(TimePoint now) noexcept;

            [[nodiscard]] std::size_t active_market_count() const noexcept{
                return active_recovery_by_market_.size();
            }

            [[nodiscard]] std::size_t active_incident_count() const noexcept;

            [[nodiscard]] RecoverySupersessionSummary supersede_before_universe(
                std::uint64_t new_universe_version,
                TimePoint now) noexcept;
        
        private:
            RecoveryId next_recovery_id_{1};

            struct IntegrityIncidentKeyHash{std::size_t operator()(const ingest::kalshi::IntegrityIncidentKey& key) const noexcept{
                return std::hash<std::uint64_t>()(key.incident_id) ^ std::hash<std::uint32_t>()(key.producer_index) ^ std::hash<std::uint8_t>()(static_cast<std::uint8_t>(key.origin));
            }};

            struct RecoverySourceKey{
                std::uint64_t universe_version{};
                ingest::kalshi::IntegrityIncidentKey incident{};

                friend bool operator==(
                    const RecoverySourceKey&,
                    const RecoverySourceKey&) = default;
            };

            struct RecoverySourceKeyHash{
                std::size_t operator()(const RecoverySourceKey& key) const noexcept{
                    const auto universe_hash =
                        std::hash<std::uint64_t>{}(key.universe_version);
                    const auto incident_hash =
                        IntegrityIncidentKeyHash{}(key.incident);
                    return universe_hash ^
                        (incident_hash + 0x9e3779b9U +
                         (universe_hash << 6U) +
                         (universe_hash >> 2U));
                }
            };

            std::unordered_map<RecoveryId, RecoveryIncidentState> incidents_;

            std::unordered_map<
                RecoverySourceKey,
                RecoveryId,
                RecoverySourceKeyHash
            > recovery_by_source_;

            std::unordered_map<MarketId, RecoveryId> active_recovery_by_market_;

            RecoveryCoordinatorConfig config_;

            std::chrono::milliseconds retry_backoff( std::uint32_t attempts_sent) const noexcept {
                auto delay = std::min(
                    config_.initial_backoff,
                    config_.max_backoff);

                for(std::uint32_t attempt = 1;
                    attempt < attempts_sent && delay < config_.max_backoff;
                    ++attempt){

                    if(delay.count() > config_.max_backoff.count() / 2){
                        return config_.max_backoff;
                    }

                    delay *= 2;
                }

                return std::min(delay, config_.max_backoff);
            }
    };



}
