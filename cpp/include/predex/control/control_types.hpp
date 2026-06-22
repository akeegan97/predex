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

    struct IoComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        bool connected{false};
        std::uint64_t installed_universe_version{0};
        std::uint64_t subscribed_universe_version{0};
        std::string last_error;
    };
    
    struct RouterTelemetrySnapshot{
        std::size_t total_frames_seen{0};
        std::size_t frames_to_shards{0};
        std::size_t frames_to_logger{0};
        std::size_t frames_recycled{0};
        std::size_t leaked_handles{0};
    };

    struct RouterComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        std::string last_error;
        RouterTelemetrySnapshot telemetry;
    };

    struct ProcessState {
        LifecyclePhase lifecycle{LifecyclePhase::kBOOTING};
        bool trading_enabled{false};
        bool shutdown_requested{false};
        
        std::uint64_t target_universe_version{0}; // the universe version that control plane has requested IO to install
        std::uint64_t active_universe_version{0}; // the universe version that is currently active in the process (i.e. has been installed and subscribed to by IO)

        IoComponentState io_component_state;
        RouterComponentState router_component_state;
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
    
    struct UniverseMarket {
        MarketId market_id{};
        std::string kalshi_ticker;
        bool tradeable{false};
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

    using IoToControlStatus = std::variant<IoConnected, IoDisconnected, IoUniverseSnapshotApplied, IoSubscriptionReady, IoFaulted>;








}  // namespace predex::core::control
