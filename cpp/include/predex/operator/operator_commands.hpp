#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>
#include "predex/control/control_types.hpp"

namespace predex::operator_admin {

enum class OperatorCommandType : std::uint8_t {
    kUNKNOWN = 0,
    kSTATUS = 1,
    kSHUTDOWN_GRACEFUL = 2,
    kSHUTDOWN_FORCEFUL = 3,
    kCOUNTERSTATS = 4,
    kALLOW_TRADING = 5,
    kDISABLE_TRADING = 6,
    kCANCEL_ALL_ORDERS = 7,
};

using OperatorCommandId = std::uint64_t;

struct OperatorCommand {
    OperatorCommandType type{OperatorCommandType::kUNKNOWN};
    OperatorCommandId request_id{0};
};

enum class OperatorResponseType : std::uint8_t {
    kACK = 0,
    kERROR = 1,
    kSTATUS = 2,
    kCOUNTERSTATS = 3,
};



struct OperatorStatusSnapshot {
    predex::core::control::LifecyclePhase lifecycle{predex::core::control::LifecyclePhase::kBOOTING};
    predex::core::control::TradingSessionPhase trading_session_phase{predex::core::control::TradingSessionPhase::kTRADING};
    bool trading_enabled{false};
    bool shutdown_requested{false};
};

struct UnknownMarketTickerStats{
    std::string market_ticker;
    std::uint8_t frame_kind{};
    std::uint64_t sid{};
    std::uint8_t channel{};
};

struct IoCounterStats{
    bool io_connected{false};
    std::uint64_t installed_universe_version{};
    std::uint64_t subscribed_universe_version{};
    std::uint64_t frames_received{};
    std::uint64_t frames_published{};
    std::uint64_t frames_dropped{};
    std::uint64_t oversized_frames{};
    std::uint64_t pool_exhausted{};
    std::uint64_t missing_frame_slot{};
    std::uint64_t envelope_parse_failed{};
    std::uint64_t envelope_missing_market_ticker{};
    std::uint64_t envelope_unsupported_type{};
    std::uint64_t inactive_sid{};
    std::uint64_t unknown_market_ticker{};
    std::vector<UnknownMarketTickerStats> unknown_market_ticker_samples;
    std::uint64_t stamp_failed{};
    std::uint64_t router_enqueue_failed{};
    std::uint64_t logger_fallback_enqueued{};
    std::uint64_t logger_fallback_failed{};
    std::uint64_t recycle_failures{};
    std::uint64_t snapshot_requests_sent{};
    std::uint64_t snapshot_requests_accepted{};
    std::uint64_t snapshot_requests_failed{};
    std::uint64_t frame_pool_in_use_high_water{};
    std::uint64_t router_queue_depth_high_water{};
    predex::core::control::MarketDataChannelTelemetry channel_stats{
        predex::core::control::make_market_data_channel_telemetry()};
    std::string last_error;
};
struct RouterCounterStats{
    std::uint64_t frames_seen{};
    std::uint64_t frames_to_shards{};
    std::uint64_t frames_to_logger{};
    std::uint64_t frames_recycled{};
    std::uint64_t leaked_handles{};
    std::uint64_t market_barriers_received{};
    std::uint64_t market_barriers_delivered{};
    std::uint64_t subscription_barriers_received{};
    std::uint64_t subscription_barriers_delivered{};
    std::uint64_t barriers_deferred{};
    std::uint64_t subscription_recovery_facts_deferred{};
    std::uint64_t shard_queue_depth_high_water{};
    predex::core::control::MarketDataChannelTelemetry channel_stats{
        predex::core::control::make_market_data_channel_telemetry()};
};
struct LoggerCounterStats{
    std::string output_file_path;
    std::uint64_t records_written{};
    std::uint64_t bytes_written{};
    std::uint64_t write_failures{};
    std::uint64_t recycle_failures{};
};
struct ShardCounterStats{
    std::uint64_t shard_index{};
    std::uint64_t frames_seen{};
    std::uint64_t frames_applied{};
    std::uint64_t parse_rejects{};
    std::uint64_t event_rejects{};
    std::uint64_t event_desyncs{};
    std::uint64_t frames_to_logger{};
    std::uint64_t frames_recycled{};
    std::uint64_t leaked_handles{};
    std::uint64_t missed_frames_to_logger{};
    std::uint64_t event_ignored{};
    std::uint64_t market_barriers_seen{};
    std::uint64_t subscription_barriers_seen{};
    std::uint64_t barrier_rejects{};
    std::uint64_t markets_became_unusable{};
    std::uint64_t markets_recovery_required{};
    std::uint64_t markets_already_awaiting_recovery{};
};

struct OmsCounterStats{
    bool trading_enabled{false};
    bool cancel_all_requested{false};
    std::uint64_t installed_universe_version{};
    std::uint64_t unknown_market_rejects{};
    std::uint64_t non_tradeable_market_rejects{};
    std::uint64_t strategy_intents_received{};
    std::uint64_t strategy_intents_processed{};
    std::uint64_t strategy_intents_rejected{};
    std::uint64_t kalshi_commands_sent{};
    std::uint64_t kalshi_commands_failed{};
    std::uint64_t rest_responses_seen{};
    std::uint64_t private_ws_events_seen{};
    std::uint64_t reconciliation_events_seen{};
    std::uint64_t order_state_updates_sent{};
    std::uint64_t strategy_response_backpressure{};
    std::uint64_t live_orders{};
    std::uint64_t pending_submit_orders{};
    std::uint64_t uncertain_orders{};
    std::string last_error;
};

struct PrivateOrderFeedCounterStats{
    bool connected{false};
    std::uint64_t installed_universe_version{};
    std::uint64_t subscribed_universe_version{};
    std::uint64_t messages_received{};
    std::uint64_t messages_decoded{};
    std::uint64_t messages_dropped{};
    std::uint64_t parse_failures{};
    std::uint64_t oms_enqueue_failures{};
    std::uint64_t reconnects{};
    std::string last_error;
};

struct OrderRestCounterStats{
    bool enabled{false};
    std::uint64_t installed_universe_version{};
    std::uint64_t commands_received{};
    std::uint64_t requests_sent{};
    std::uint64_t responses_received{};
    std::uint64_t requests_failed{};
    std::uint64_t retry_count{};
    std::uint64_t oms_enqueue_failures{};
    std::string last_error;
};


struct OperatorCounterStatsSnapshot{
    OperatorStatusSnapshot status_snapshot;
    IoCounterStats io_stats;
    RouterCounterStats router_stats;
    std::vector<ShardCounterStats> shard_stats;
    LoggerCounterStats logger_stats;
    OmsCounterStats oms_stats;
    PrivateOrderFeedCounterStats private_order_feed_stats;
    OrderRestCounterStats order_rest_stats;
    predex::core::control::RecoveryTelemetrySnapshot recovery_stats;
};
struct AckPayload {};

struct ErrorPayload {
    std::string message;
};

using OperatorResponsePayload =
    std::variant<AckPayload, ErrorPayload, OperatorStatusSnapshot, OperatorCounterStatsSnapshot>;

struct OperatorResponse {
    OperatorCommandId request_id{0};
    OperatorResponseType type{OperatorResponseType::kACK};
    OperatorResponsePayload payload{AckPayload{}};
};

}  // namespace predex::operator_admin
