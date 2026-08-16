#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <variant>
#include <array>

#include "predex/control/control_lifecycle.hpp"
#include "predex/utils/latency_histogram.hpp"
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

    enum class TradingSessionPhase : std::uint8_t{
        kUNKNOWN = 0,
        kTRADING = 1,
        kREDUCE_ONLY = 2,
        kFLATTEN_TO_ZERO = 3,
        kSTOPPED = 4,
    };

    struct UnknownMarketTickerStats{
        std::string market_ticker;
        std::uint8_t frame_kind{};
        std::uint64_t sid{};
        std::uint8_t channel{};
    };

    struct MarketDataChannelTelemetrySnapshot{
        std::uint8_t channel{};
        std::uint64_t frames_observed{};
        std::uint64_t sequence_gaps{};
        std::uint64_t duplicate_sequences{};
        std::uint64_t stale_sequences{};
        std::uint64_t intentionally_filtered{};
        std::uint64_t logger_only_frames{};
        std::uint64_t downstream_delivery_losses{};
    };

    inline constexpr std::size_t kMarketDataChannelCount = 3;
    using MarketDataChannelTelemetry =
        std::array<MarketDataChannelTelemetrySnapshot, kMarketDataChannelCount>;
    using MarketDataChannelLatency =
        std::array<utils::LatencyHistogram, kMarketDataChannelCount>;

    [[nodiscard]] inline MarketDataChannelTelemetry make_market_data_channel_telemetry() noexcept{
        return MarketDataChannelTelemetry{{
            MarketDataChannelTelemetrySnapshot{.channel = 1},
            MarketDataChannelTelemetrySnapshot{.channel = 2},
            MarketDataChannelTelemetrySnapshot{.channel = 3},
        }};
    }

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
        std::uint64_t snapshot_requests_sent{0};
        std::uint64_t snapshot_requests_accepted{0};
        std::uint64_t snapshot_requests_failed{0};
        std::uint64_t frame_pool_in_use_high_water{0};
        std::uint64_t router_queue_depth_high_water{0};
        MarketDataChannelTelemetry channel_stats{make_market_data_channel_telemetry()};
        MarketDataChannelLatency wire_service_latency{};
    };

    struct RouterTelemetrySnapshot{
        std::uint64_t total_frames_seen{0};
        std::uint64_t frames_to_shards{0};
        std::uint64_t frames_to_logger{0};
        std::uint64_t frames_recycled{0};
        std::uint64_t leaked_handles{0};
        std::uint64_t market_barriers_received{0};
        std::uint64_t market_barriers_delivered{0};
        std::uint64_t subscription_barriers_received{0};
        std::uint64_t subscription_barriers_delivered{0};
        std::uint64_t barriers_deferred{0};
        std::uint64_t subscription_recovery_facts_deferred{0};
        std::uint64_t shard_queue_depth_high_water{0};
        MarketDataChannelTelemetry channel_stats{make_market_data_channel_telemetry()};
        MarketDataChannelLatency wire_to_router_latency{};
        MarketDataChannelLatency router_service_latency{};
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
        std::uint64_t event_ignored{0};
        std::uint64_t market_barriers_seen{0};
        std::uint64_t subscription_barriers_seen{0};
        std::uint64_t barrier_rejects{0};
        std::uint64_t markets_became_unusable{0};
        std::uint64_t markets_recovery_required{0};
        std::uint64_t markets_already_awaiting_recovery{0};
        MarketDataChannelLatency router_to_shard_latency{};
        MarketDataChannelLatency shard_service_latency{};
        MarketDataChannelLatency ingress_to_shard_latency{};
        MarketDataChannelLatency ingress_to_book_apply_latency{};
    };

    struct RecoveryTelemetrySnapshot{
        std::uint64_t incidents_created{};
        std::uint64_t incidents_deduplicated{};
        std::uint64_t markets_already_recovering{};
        std::uint64_t observations_rejected{};
        std::uint64_t markets_scheduled{};
        std::uint64_t requests_enqueued{};
        std::uint64_t request_enqueue_failures{};
        std::uint64_t requests_accepted{};
        std::uint64_t request_failures{};
        std::uint64_t retries_scheduled{};
        std::uint64_t request_ack_timeouts{};
        std::uint64_t snapshot_timeouts{};
        std::uint64_t markets_recovered{};
        std::uint64_t markets_failed{};
        std::uint64_t incidents_completed{};
        std::uint64_t incidents_superseded{};
        std::uint64_t markets_superseded{};
        std::uint64_t active_incidents{};
        std::uint64_t active_markets{};
        std::uint64_t recovery_duration_samples{};
        std::uint64_t recovery_duration_total_ns{};
        std::uint64_t recovery_duration_max_ns{};
        utils::LatencyHistogram recovery_duration_latency{};
    };

    struct LoggerTelemetrySnapshot{
        std::uint64_t records_written{0};
        std::uint64_t bytes_written{0};
        std::uint64_t write_failures{0};
        std::uint64_t recycle_failures{0};
        MarketDataChannelLatency shard_to_logger_latency{};
        MarketDataChannelLatency ingress_to_logger_write_latency{};
    };

    struct OmsTelemetrySnapshot{
        std::uint64_t installed_universe_version{0};
        std::uint64_t unknown_market_rejects{0};
        std::uint64_t non_tradeable_market_rejects{0};
        std::uint64_t strategy_intents_received{0};
        std::uint64_t strategy_intents_processed{0};
        std::uint64_t strategy_intents_rejected{0};

        std::uint64_t kalshi_commands_sent{0};
        std::uint64_t kalshi_commands_failed{0};

        std::uint64_t rest_responses_seen{0};
        std::uint64_t private_ws_events_seen{0};
        std::uint64_t reconciliation_events_seen{0};

        std::uint64_t order_state_updates_sent{0};
        std::uint64_t strategy_response_backpressure{0};

        std::uint64_t live_orders{0};
        std::uint64_t pending_submit_orders{0};
        std::uint64_t uncertain_orders{0};
    };

    struct PrivateOrderFeedTelemetrySnapshot{
        std::uint64_t messages_received{0};
        std::uint64_t messages_decoded{0};
        std::uint64_t messages_dropped{0};
        std::uint64_t parse_failures{0};
        std::uint64_t oms_enqueue_failures{0};
        std::uint64_t reconnects{0};
    };

    struct OrderRestTelemetrySnapshot{
        std::uint64_t commands_received{0};
        std::uint64_t requests_sent{0};
        std::uint64_t responses_received{0};
        std::uint64_t requests_failed{0};
        std::uint64_t retry_count{0};
        std::uint64_t oms_enqueue_failures{0};
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

    struct OmsComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        std::uint64_t installed_universe_version{0};
        bool trading_enabled{false};
        bool cancel_all_requested{false};
        OmsTelemetrySnapshot telemetry;
        std::string last_error;
    };

    struct PrivateOrderFeedComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        bool connected{false};
        std::uint64_t installed_universe_version{0};
        std::uint64_t subscribed_universe_version{0};
        std::string last_error;
        PrivateOrderFeedTelemetrySnapshot telemetry;
    };

    struct OrderRestComponentState{
        ComponentStatus status{ComponentStatus::kUNKNOWN};
        bool enabled{false};
        std::uint64_t installed_universe_version{0};
        std::string last_error;
        OrderRestTelemetrySnapshot telemetry;
    };

    struct ProcessState {
        LifecyclePhase lifecycle{LifecyclePhase::kBOOTING};
        TradingSessionPhase trading_session_phase{TradingSessionPhase::kTRADING};
        bool trading_enabled{false};
        bool shutdown_requested{false};
        
        std::uint64_t target_universe_version{0}; // the universe version that control plane has requested IO to install
        std::uint64_t active_universe_version{0}; // the universe version that is currently active in the process (i.e. has been installed and subscribed to by IO)

        IoComponentState io_component_state;
        RouterComponentState router_component_state;
        LoggerComponentState logger_component_state;
        OmsComponentState oms_component_state;
        PrivateOrderFeedComponentState private_order_feed_component_state;
        OrderRestComponentState order_rest_component_state;
        std::vector<ShardComponentState> shard_component_states;
        RecoveryTelemetrySnapshot recovery_telemetry;
    };

    using MarketId = std::uint32_t;
    using EventId = std::uint32_t;

    using RecoveryId = std::uint64_t;

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

    struct OrderMarketRoute{
        MarketId market_id{};
        EventId event_id{};
        std::string kalshi_ticker;
        bool tradeable{false};
        PriceLevelStructure price_level_structure{PriceLevelStructure::kLINEAR_CENT};
    };

    struct OrderRouteUniverse{
        std::uint64_t version{};
        std::vector<OrderMarketRoute> market_routes;
    };

    // IO control commands and responses 

    struct ApplyUniverseSnapshotIo{
        std::shared_ptr<const UniverseSnapshot> snapshot;
    };

    struct ConnectIo{};
    struct DisconnectIo{};
    struct RecoverMarketIo{
        RecoveryId recovery_id{};
        std::uint64_t universe_version{};
        MarketId market_id{};
        std::uint32_t request_attempt{};
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
    struct IoRecoveryRequestAccepted {
        RecoveryId recovery_id{};
        std::uint64_t universe_version{};
        MarketId market_id{};
        std::uint32_t request_attempt{};
    };

    struct IoRecoveryRequestFailed {
        RecoveryId recovery_id{};
        std::uint64_t universe_version{};
        MarketId market_id{};
        std::uint32_t request_attempt{};
        std::string reason;
    };

    using IoToControlStatus = std::variant<
        IoConnected,
        IoDisconnected,
        IoUniverseSnapshotApplied,
        IoSubscriptionReady,
        IoFaulted,
        IoTelemetry,
        IoRecoveryRequestAccepted,
        IoRecoveryRequestFailed
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

    // Private order feed control commands and responses

    struct ApplyOrderRouteUniverse{
        std::shared_ptr<const OrderRouteUniverse> snapshot;
    };

    struct ConnectPrivateOrderFeed{};
    struct DisconnectPrivateOrderFeed{};

    using ControlToPrivateOrderFeedCommand = std::variant<
        ApplyOrderRouteUniverse,
        ConnectPrivateOrderFeed,
        DisconnectPrivateOrderFeed
    >;

    struct PrivateOrderFeedConnected{};
    struct PrivateOrderFeedDisconnected{
        std::string reason;
    };
    struct PrivateOrderFeedUniverseApplied{
        std::uint64_t version{};
    };
    struct PrivateOrderFeedSubscriptionReady{
        std::uint64_t version{};
    };
    struct PrivateOrderFeedFaulted{
        std::string error_message;
    };
    struct PrivateOrderFeedTelemetry{
        PrivateOrderFeedTelemetrySnapshot telemetry;
    };

    using PrivateOrderFeedToControlStatus = std::variant<
        PrivateOrderFeedConnected,
        PrivateOrderFeedDisconnected,
        PrivateOrderFeedUniverseApplied,
        PrivateOrderFeedSubscriptionReady,
        PrivateOrderFeedFaulted,
        PrivateOrderFeedTelemetry
    >;

    // Order REST control commands and responses

    struct EnableOrderRest{};
    struct DisableOrderRest{};

    using ControlToOrderRestCommand = std::variant<
        ApplyOrderRouteUniverse,
        EnableOrderRest,
        DisableOrderRest
    >;

    struct OrderRestReady{};
    struct OrderRestDisabled{};
    struct OrderRestUniverseApplied{
        std::uint64_t version{};
    };
    struct OrderRestFaulted{
        std::string error_message;
    };
    struct OrderRestTelemetry{
        OrderRestTelemetrySnapshot telemetry;
    };

    using OrderRestToControlStatus = std::variant<
        OrderRestReady,
        OrderRestDisabled,
        OrderRestUniverseApplied,
        OrderRestFaulted,
        OrderRestTelemetry
    >;

    struct AllowTrading{};
    struct DisableTrading{};

    struct CancelAllOrders{}; // removes all resting orders if possible/still open at time of command req

    using ControlToOmsCommand = std::variant<ApplyOrderRouteUniverse, AllowTrading, DisableTrading, CancelAllOrders>;

    struct OmsReady{};
    struct OmsFaulted{
        std::string error_message;
    };
    struct OmsTelemetry{
        OmsTelemetrySnapshot telemetry;
    };
    struct OmsTradingEnabledChanged{
        bool trading_enabled{false};
    };

    struct OmsCancelAllStateChanged{
        bool cancel_all_requested{false};
        std::uint64_t live_orders{0};
    };

    struct OmsRestEgressDrained{
        std::uint64_t shutdown_epoch{};
        std::uint64_t completion_ts_ns{};
        std::uint64_t live_orders{0};
        std::uint64_t uncertain_orders{0};
    };

    using OmsToControlStatus = std::variant<
        OmsReady,
        OmsFaulted,
        OmsTelemetry,
        OmsTradingEnabledChanged,
        OmsCancelAllStateChanged,
        OmsRestEgressDrained
    >;

}  // namespace predex::core::control
