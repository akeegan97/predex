#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <variant>

#include "predex/control/control_lifecycle.hpp"
namespace predex::core::control {

    enum class ComponentStatus : std::uint8_t{
        kUNKNOWN = 0,
        kSTOPPED = 1,
        kSTARTING = 2,
        kINSTALLING_UNIVERSE = 3,
        kREADY = 4,
        kLIVE = 5,
        kQUIESCING = 6,
        kFAULTED = 7,
    };

    struct UnknownMarketTickerStats{
        std::string market_ticker;
        std::uint8_t frame_kind{};
        std::uint64_t sid{};
        std::uint8_t channel{};
    };

    struct IoTelemetrySnapshot{
        std::uint64_t frames_received{0};
        std::uint64_t frames_published{0};
        std::uint64_t frames_dropped{0};

        std::uint64_t oversized_frames{0};
        std::uint64_t pool_exhausted{0};
        std::uint64_t missing_frame_slot{0};

        std::uint64_t envelope_parse_failed{0};
        std::uint64_t envelope_missing_market_ticker{0};
        std::uint64_t envelope_unsupported_type{0};

        std::uint64_t inactive_sid{0};
        std::uint64_t unknown_market_ticker{0};
        std::vector<UnknownMarketTickerStats> unknown_market_ticker_samples;
        std::uint64_t stamp_failed{0};

        std::uint64_t router_enqueue_failed{0};
        std::uint64_t logger_fallback_enqueued{0};
        std::uint64_t logger_fallback_failed{0};

        std::uint64_t recycle_failures{0};
    };

    struct RouterTelemetrySnapshot{
        std::uint64_t total_frames_seen{0};
        std::uint64_t frames_to_shards{0};
        std::uint64_t frames_to_logger{0};
        std::uint64_t frames_recycled{0};
        std::uint64_t leaked_handles{0};
    };

    struct ShardTelemetrySnapshot{
        std::uint64_t frames_seen{0};
        std::uint64_t frames_applied{0};
        std::uint64_t parse_rejects{0};
        std::uint64_t event_rejects{0};
        std::uint64_t event_desyncs{0};
        std::uint64_t frames_to_logger{0};
        std::uint64_t frames_recycled{0};
        std::uint64_t leaked_handles{0};
        std::uint64_t missed_frames_to_logger{0};
    };

    struct LoggerTelemetrySnapshot{
        std::uint64_t records_written{0};
        std::uint64_t bytes_written{0};
        std::uint64_t write_failures{0};
        std::uint64_t recycle_failures{0};
    };

    struct IoComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        bool connected{false};
        std::uint64_t installed_universe_version{0};
        std::uint64_t subscribed_universe_version{0};
        IoTelemetrySnapshot telemetry;
        std::string last_error;
    };

    struct RouterComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        std::string last_error;
        RouterTelemetrySnapshot telemetry;
    };

    struct ShardComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        std::uint64_t installed_universe_version{0};
        std::uint64_t safe_to_stop_universe_version{0};
        std::uint64_t drained_universe_version{0};
        std::string last_error;
        ShardTelemetrySnapshot telemetry;
    };

    struct LoggerComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        std::string output_file_path;
        std::string last_error;
        LoggerTelemetrySnapshot telemetry;
    };

    struct ProcessState {
        LifecyclePhase lifecycle{LifecyclePhase::kBOOTING};
        bool trading_enabled{false};
        bool shutdown_requested{false};
        
        std::uint64_t target_universe_version{0}; // the universe version that control plane has requested IO to install
        std::uint64_t active_universe_version{0}; // the universe version that is currently active in the process (i.e. has been installed and subscribed to by IO)

        IoComponentState io_component_state;
        RouterComponentState router_component_state;
        LoggerComponentState logger_component_state;
        std::vector<ShardComponentState> shard_component_states;
    };

    using MarketId = std::uint32_t;
    using EventId = std::uint32_t;

    using AffinityKey = std::uint64_t; // sharding events by this key ensures all events with the same key go to the same shard,
    

    enum class EventTopology : std::uint8_t{
        kUNKNOWN = 0,
        kMONOTONIC_CHAIN = 1,
        kMUTUALLY_EXCLUSIVE = 3,
        kUNORDERED_GROUP = 4,
        kSINGLE_MARKET = 5,
        //other topologies as needed
    };

    enum class PriceLevelStructure : std::uint8_t{
        kUNKNOWN = 0,
        kLINEAR_CENT = 1,
        kTAPERED_DECI_CENT = 2,
        kDECI_CENT = 3,
    };
    
    struct UniverseMarket {
        MarketId market_id{};
        std::string kalshi_ticker;
        bool tradeable{false};
        PriceLevelStructure price_level_structure{PriceLevelStructure::kLINEAR_CENT};
    };

    struct UniverseEvent {
        EventId event_id{};
        AffinityKey affinity_key{};
        EventTopology topology{EventTopology::kUNKNOWN};
        std::vector<UniverseMarket> markets;
    };

    struct UniverseMarketRoute{
        std::string kalshi_ticker;
        MarketId market_id{};
        EventId event_id{};
        AffinityKey affinity_key{};
        EventTopology topology{EventTopology::kUNKNOWN};
        std::uint32_t shard_index{};
        std::uint32_t shard_event_index{};
        std::uint32_t event_market_index{};
        bool tradeable{false};
        PriceLevelStructure price_level_structure{PriceLevelStructure::kLINEAR_CENT};
    };

    struct IoMarketSubscription{
        MarketId market_id{};
        std::string kalshi_ticker;
    };

    struct UniverseSnapshot{
        std::uint64_t version{};
        std::vector<UniverseEvent> events;
        std::vector<UniverseMarketRoute> market_routes;
    };

    // IO control commands and responses 

    struct ApplyUniverseSnapshotIo{
        std::shared_ptr<const UniverseSnapshot> snapshot;
    };

    struct ConnectIo{};
    struct DisconnectIo{};
    struct RecoverMarketIo{
        MarketId market_id{};
    };

    using ControlToIoCommand = std::variant<ApplyUniverseSnapshotIo, ConnectIo, DisconnectIo, RecoverMarketIo>;


    struct IoConnected{};
    struct IoDisconnected{
        std::string reason;
    };
    struct IoUniverseSnapshotApplied{
        std::uint64_t version{};
    };
    struct IoSubscriptionReady{
        std::uint64_t version{};
    };
    struct IoFaulted{
        std::string error_message;
    };
    struct IoTelemetry{
        IoTelemetrySnapshot telemetry;
    };

    using IoToControlStatus = std::variant<
        IoConnected,
        IoDisconnected,
        IoUniverseSnapshotApplied,
        IoSubscriptionReady,
        IoFaulted,
        IoTelemetry
    >;

    struct LoggerStarted{
        std::string output_file_path;
    };
    struct LoggerFaulted{
        std::string error_message;
    };
    struct LoggerTelemetry{
        LoggerTelemetrySnapshot telemetry;
    };

    using LoggerToControlStatus = std::variant<LoggerStarted, LoggerFaulted, LoggerTelemetry>;








}  // namespace predex::core::control
