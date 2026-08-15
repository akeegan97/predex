#include <nlohmann/json.hpp>

#include "predex/operator/operator_command_handler.hpp"
#include "predex/operator/operator_commands.hpp"
#include <thread>
#include <chrono>

namespace {

    predex::operator_admin::OperatorCommand parse_command(const std::string& command_line){
        predex::operator_admin::OperatorCommand cmd{};
        
        auto json = nlohmann::json::parse(command_line);
        const auto cmd_id_str = json.at("cmd_id").get<std::string>();
        const auto type_str = json.at("type").get<std::string>();

        cmd.request_id = std::stoull(cmd_id_str);
        if(type_str == "status"){
            cmd.type = predex::operator_admin::OperatorCommandType::kSTATUS;
        } else if(type_str == "shutdown-graceful"){
            cmd.type = predex::operator_admin::OperatorCommandType::kSHUTDOWN_GRACEFUL;
        } else if(type_str == "shutdown-forceful"){
            cmd.type = predex::operator_admin::OperatorCommandType::kSHUTDOWN_FORCEFUL;
        } else if(type_str == "counterstats"){
            cmd.type = predex::operator_admin::OperatorCommandType::kCOUNTERSTATS;
        }else if (type_str == "allow-trading"){
            cmd.type = predex::operator_admin::OperatorCommandType::kALLOW_TRADING;
        }else if(type_str == "disable-trading"){
            cmd.type = predex::operator_admin::OperatorCommandType::kDISABLE_TRADING;
        }else if(type_str == "cancel-all-orders"){
            cmd.type = predex::operator_admin::OperatorCommandType::kCANCEL_ALL_ORDERS;
        } else {
            cmd.type = predex::operator_admin::OperatorCommandType::kUNKNOWN;
        }

        return cmd;
    }

    std::string lifecycle_to_string(predex::core::control::LifecyclePhase lifecycle){
        switch(lifecycle){
            case predex::core::control::LifecyclePhase::kBOOTING:
                return "booting";
            case predex::core::control::LifecyclePhase::kWAITING_FOR_IO:
                return "waiting_for_io";
            case predex::core::control::LifecyclePhase::kIO_CONNECTED:
                return "io_connected";
            case predex::core::control::LifecyclePhase::kREADY:
                return "ready";
            case predex::core::control::LifecyclePhase::kLIVE_TRADING:
                return "live_trading";
            case predex::core::control::LifecyclePhase::kSHUTTING_DOWN:
                return "shutting_down";
            case predex::core::control::LifecyclePhase::kSTOPPED:
                return "stopped";
            case predex::core::control::LifecyclePhase::kREFRESHING:
                return "refreshing";
            case predex::core::control::LifecyclePhase::kFAULTED:
                return "faulted";
            default:
                return "unknown";
        }
    }

    std::string trading_session_phase_to_string(predex::core::control::TradingSessionPhase phase){
        switch(phase){
            case predex::core::control::TradingSessionPhase::kTRADING:
                return "trading";
            case predex::core::control::TradingSessionPhase::kREDUCE_ONLY:
                return "reduce_only";
            case predex::core::control::TradingSessionPhase::kFLATTEN_TO_ZERO:
                return "flatten_to_zero";
            case predex::core::control::TradingSessionPhase::kSTOPPED:
                return "stopped";
            case predex::core::control::TradingSessionPhase::kUNKNOWN://NOLINT
                return "unknown";
            default:
                return "unknown";
        }
    }

    std::string response_type_to_string(predex::operator_admin::OperatorResponseType type){
        switch(type){
            case predex::operator_admin::OperatorResponseType::kACK:
                return "ack";
            case predex::operator_admin::OperatorResponseType::kERROR:
                return "error";
            case predex::operator_admin::OperatorResponseType::kSTATUS:
                return "status";
            case predex::operator_admin::OperatorResponseType::kCOUNTERSTATS:
                return "counterstats";
            default:
                return "unknown";
        }
    }

    std::string frame_kind_to_string(std::uint8_t frame_kind){
        switch(frame_kind){
            case 1:
                return "orderbook_snapshot";
            case 2:
                return "orderbook_delta";
            case 3:
                return "trade";
            case 4:
                return "subscription_ack";
            case 5://NOLINT
                return "unsubscribed";
            case 6://NOLINT
                return "lifecycle";
            case 7://NOLINT
                return "heartbeat";
            case 0:
            default:
                return "unknown";
        }
    }

    std::string market_data_channel_to_string(std::uint8_t channel){
        switch(channel){
            case 1:
                return "orderbook_delta";
            case 2:
                return "trade";
            case 3:
                return "market_lifecycle_v2";
            case 0:
            default:
                return "unknown";
        }
    }
    
    nlohmann::json to_json(const predex::operator_admin::OperatorResponse& response){
        nlohmann::json json_response;
        json_response["request_id"] = std::to_string(response.request_id);
        json_response["type"] = response_type_to_string(response.type);
        json_response["ok"] = response.type != predex::operator_admin::OperatorResponseType::kERROR;


        std::visit([&json_response](const auto& payload){
            using T = std::decay_t<decltype(payload)>;
            if constexpr (std::is_same_v<T, predex::operator_admin::AckPayload>){
                json_response["payload"] = {{"message", "ack"}};
            }
            else if constexpr (std::is_same_v<T, predex::operator_admin::ErrorPayload>){
                json_response["payload"] = {{"message", payload.message}};
            }
            else if constexpr (std::is_same_v<T, predex::operator_admin::OperatorStatusSnapshot>){
                json_response["payload"] = {
                    {"lifecycle", lifecycle_to_string(payload.lifecycle)},
                    {"trading_session_phase", trading_session_phase_to_string(payload.trading_session_phase)},
                    {"trading_enabled", payload.trading_enabled},
                    {"shutdown_requested", payload.shutdown_requested},
                };
            }
            else if constexpr (std::is_same_v<T, predex::operator_admin::OperatorCounterStatsSnapshot>){
                nlohmann::json shard_stats_json = nlohmann::json::array();
                for(const auto& shard_stat : payload.shard_stats){
                    shard_stats_json.push_back({
                        {"shard_index", shard_stat.shard_index},
                        {"frames_seen", shard_stat.frames_seen},
                        {"frames_applied", shard_stat.frames_applied},
                        {"parse_rejects", shard_stat.parse_rejects},
                        {"event_rejects", shard_stat.event_rejects},
                        {"event_desyncs", shard_stat.event_desyncs},
                        {"frames_to_logger", shard_stat.frames_to_logger},
                        {"frames_recycled", shard_stat.frames_recycled},
                        {"leaked_handles", shard_stat.leaked_handles},
                        {"missed_frames_to_logger", shard_stat.missed_frames_to_logger},
                        {"event_ignored", shard_stat.event_ignored},
                        {"market_barriers_seen", shard_stat.market_barriers_seen},
                        {"subscription_barriers_seen", shard_stat.subscription_barriers_seen},
                        {"barrier_rejects", shard_stat.barrier_rejects},
                        {"markets_became_unusable", shard_stat.markets_became_unusable},
                        {"markets_recovery_required", shard_stat.markets_recovery_required},
                        {"markets_already_awaiting_recovery", shard_stat.markets_already_awaiting_recovery},
                    });
                }

                const auto channel_stats_json = [](const auto& channel_stats){
                    nlohmann::json result = nlohmann::json::array();
                    for(const auto& stats : channel_stats){
                        result.push_back({
                            {"channel", market_data_channel_to_string(stats.channel)},
                            {"channel_code", stats.channel},
                            {"frames_observed", stats.frames_observed},
                            {"sequence_gaps", stats.sequence_gaps},
                            {"duplicate_sequences", stats.duplicate_sequences},
                            {"stale_sequences", stats.stale_sequences},
                            {"intentionally_filtered", stats.intentionally_filtered},
                            {"logger_only_frames", stats.logger_only_frames},
                            {"downstream_delivery_losses", stats.downstream_delivery_losses},
                        });
                    }
                    return result;
                };

                nlohmann::json unknown_market_ticker_samples_json = nlohmann::json::array();
                for(const auto& sample : payload.io_stats.unknown_market_ticker_samples){
                    unknown_market_ticker_samples_json.push_back({
                        {"market_ticker", sample.market_ticker},
                        {"frame_kind", frame_kind_to_string(sample.frame_kind)},
                        {"frame_kind_code", static_cast<std::uint32_t>(sample.frame_kind)},
                        {"sid", sample.sid},
                        {"channel", market_data_channel_to_string(sample.channel)},
                        {"channel_code", static_cast<std::uint32_t>(sample.channel)},
                    });
                }

                json_response["payload"] = {
                    {"status_snapshot", {
                        {"lifecycle", lifecycle_to_string(payload.status_snapshot.lifecycle)},
                        {"trading_session_phase", trading_session_phase_to_string(payload.status_snapshot.trading_session_phase)},
                        {"trading_enabled", payload.status_snapshot.trading_enabled},
                        {"shutdown_requested", payload.status_snapshot.shutdown_requested},
                    }},
                    {"io_stats", {
                        {"io_connected", payload.io_stats.io_connected},
                        {"installed_universe_version", payload.io_stats.installed_universe_version},
                        {"subscribed_universe_version", payload.io_stats.subscribed_universe_version},
                        {"frames_received", payload.io_stats.frames_received},
                        {"frames_published", payload.io_stats.frames_published},
                        {"frames_dropped", payload.io_stats.frames_dropped},
                        {"oversized_frames", payload.io_stats.oversized_frames},
                        {"pool_exhausted", payload.io_stats.pool_exhausted},
                        {"missing_frame_slot", payload.io_stats.missing_frame_slot},
                        {"envelope_parse_failed", payload.io_stats.envelope_parse_failed},
                        {"envelope_missing_market_ticker", payload.io_stats.envelope_missing_market_ticker},
                        {"envelope_unsupported_type", payload.io_stats.envelope_unsupported_type},
                        {"inactive_sid", payload.io_stats.inactive_sid},
                        {"unknown_market_ticker", payload.io_stats.unknown_market_ticker},
                        {"unknown_market_ticker_samples", unknown_market_ticker_samples_json},
                        {"stamp_failed", payload.io_stats.stamp_failed},
                        {"router_enqueue_failed", payload.io_stats.router_enqueue_failed},
                        {"logger_fallback_enqueued", payload.io_stats.logger_fallback_enqueued},
                        {"logger_fallback_failed", payload.io_stats.logger_fallback_failed},
                        {"recycle_failures", payload.io_stats.recycle_failures},
                        {"snapshot_requests_sent", payload.io_stats.snapshot_requests_sent},
                        {"snapshot_requests_accepted", payload.io_stats.snapshot_requests_accepted},
                        {"snapshot_requests_failed", payload.io_stats.snapshot_requests_failed},
                        {"frame_pool_in_use_high_water", payload.io_stats.frame_pool_in_use_high_water},
                        {"router_queue_depth_high_water", payload.io_stats.router_queue_depth_high_water},
                        {"channel_stats", channel_stats_json(payload.io_stats.channel_stats)},
                        {"last_error", payload.io_stats.last_error},
                    }},
                    {"router_stats", {
                        {"frames_seen", payload.router_stats.frames_seen},
                        {"frames_to_shards", payload.router_stats.frames_to_shards},
                        {"frames_to_logger", payload.router_stats.frames_to_logger},
                        {"frames_recycled", payload.router_stats.frames_recycled},
                        {"leaked_handles", payload.router_stats.leaked_handles},
                        {"market_barriers_received", payload.router_stats.market_barriers_received},
                        {"market_barriers_delivered", payload.router_stats.market_barriers_delivered},
                        {"subscription_barriers_received", payload.router_stats.subscription_barriers_received},
                        {"subscription_barriers_delivered", payload.router_stats.subscription_barriers_delivered},
                        {"barriers_deferred", payload.router_stats.barriers_deferred},
                        {"subscription_recovery_facts_deferred", payload.router_stats.subscription_recovery_facts_deferred},
                        {"shard_queue_depth_high_water", payload.router_stats.shard_queue_depth_high_water},
                        {"channel_stats", channel_stats_json(payload.router_stats.channel_stats)},
                    }},
                    {"shard_stats", shard_stats_json},
                    {"logger_stats", {
                        {"output_file_path", payload.logger_stats.output_file_path},
                        {"records_written", payload.logger_stats.records_written},
                        {"bytes_written", payload.logger_stats.bytes_written},
                        {"write_failures", payload.logger_stats.write_failures},
                        {"recycle_failures", payload.logger_stats.recycle_failures},
                    }},
                    {"oms_stats", {
                        {"trading_enabled", payload.oms_stats.trading_enabled},
                        {"cancel_all_requested", payload.oms_stats.cancel_all_requested}, 
                        {"installed_universe_version", payload.oms_stats.installed_universe_version},
                        {"unknown_market_rejects", payload.oms_stats.unknown_market_rejects},
                        {"non_tradeable_market_rejects", payload.oms_stats.non_tradeable_market_rejects},
                        {"strategy_intents_received", payload.oms_stats.strategy_intents_received},
                        {"strategy_intents_processed", payload.oms_stats.strategy_intents_processed},
                        {"strategy_intents_rejected", payload.oms_stats.strategy_intents_rejected},
                        {"kalshi_commands_sent", payload.oms_stats.kalshi_commands_sent},
                        {"kalshi_commands_failed", payload.oms_stats.kalshi_commands_failed},
                        {"rest_responses_seen", payload.oms_stats.rest_responses_seen},
                        {"private_ws_events_seen", payload.oms_stats.private_ws_events_seen},
                        {"reconciliation_events_seen", payload.oms_stats.reconciliation_events_seen},
                        {"order_state_updates_sent", payload.oms_stats.order_state_updates_sent},
                        {"strategy_response_backpressure", payload.oms_stats.strategy_response_backpressure},
                        {"live_orders", payload.oms_stats.live_orders},
                        {"pending_submit_orders", payload.oms_stats.pending_submit_orders},
                        {"uncertain_orders", payload.oms_stats.uncertain_orders},
                        {"last_error", payload.oms_stats.last_error},
                    }},
                    {"private_order_feed_stats", {
                        {"connected", payload.private_order_feed_stats.connected},
                        {"installed_universe_version", payload.private_order_feed_stats.installed_universe_version},
                        {"subscribed_universe_version", payload.private_order_feed_stats.subscribed_universe_version},
                        {"messages_received", payload.private_order_feed_stats.messages_received},
                        {"messages_decoded", payload.private_order_feed_stats.messages_decoded},
                        {"messages_dropped", payload.private_order_feed_stats.messages_dropped},
                        {"parse_failures", payload.private_order_feed_stats.parse_failures},
                        {"oms_enqueue_failures", payload.private_order_feed_stats.oms_enqueue_failures},
                        {"reconnects", payload.private_order_feed_stats.reconnects},
                        {"last_error", payload.private_order_feed_stats.last_error},
                    }},
                    {"order_rest_stats", {
                        {"enabled", payload.order_rest_stats.enabled},
                        {"installed_universe_version", payload.order_rest_stats.installed_universe_version},
                        {"commands_received", payload.order_rest_stats.commands_received},
                        {"requests_sent", payload.order_rest_stats.requests_sent},
                        {"responses_received", payload.order_rest_stats.responses_received},
                        {"requests_failed", payload.order_rest_stats.requests_failed},
                        {"retry_count", payload.order_rest_stats.retry_count},
                        {"oms_enqueue_failures", payload.order_rest_stats.oms_enqueue_failures},
                        {"last_error", payload.order_rest_stats.last_error},
                    }},
                    {"recovery_stats", {
                        {"incidents_created", payload.recovery_stats.incidents_created},
                        {"incidents_deduplicated", payload.recovery_stats.incidents_deduplicated},
                        {"markets_already_recovering", payload.recovery_stats.markets_already_recovering},
                        {"observations_rejected", payload.recovery_stats.observations_rejected},
                        {"markets_scheduled", payload.recovery_stats.markets_scheduled},
                        {"requests_enqueued", payload.recovery_stats.requests_enqueued},
                        {"request_enqueue_failures", payload.recovery_stats.request_enqueue_failures},
                        {"requests_accepted", payload.recovery_stats.requests_accepted},
                        {"request_failures", payload.recovery_stats.request_failures},
                        {"retries_scheduled", payload.recovery_stats.retries_scheduled},
                        {"request_ack_timeouts", payload.recovery_stats.request_ack_timeouts},
                        {"snapshot_timeouts", payload.recovery_stats.snapshot_timeouts},
                        {"markets_recovered", payload.recovery_stats.markets_recovered},
                        {"markets_failed", payload.recovery_stats.markets_failed},
                        {"incidents_completed", payload.recovery_stats.incidents_completed},
                        {"incidents_superseded", payload.recovery_stats.incidents_superseded},
                        {"markets_superseded", payload.recovery_stats.markets_superseded},
                        {"active_incidents", payload.recovery_stats.active_incidents},
                        {"active_markets", payload.recovery_stats.active_markets},
                        {"recovery_duration_samples", payload.recovery_stats.recovery_duration_samples},
                        {"recovery_duration_total_ns", payload.recovery_stats.recovery_duration_total_ns},
                        {"recovery_duration_max_ns", payload.recovery_stats.recovery_duration_max_ns},
                    }},
                };
            }
            else{
                json_response["payload"] = {{"message","unknown"}};
            }
        }, response.payload);
        return json_response;
    }


    predex::socket::CommandResponse answer_operator(const predex::operator_admin::OperatorResponse& operator_response){
        const bool okay = operator_response.type != predex::operator_admin::OperatorResponseType::kERROR;
        return predex::socket::CommandResponse{
            .ok = okay,
            .body = to_json(operator_response).dump() + '\n',
        };
    }


}// annonymous

namespace predex::operator_admin{
    constexpr std::chrono::milliseconds kMAX_WAIT_TIME{500};
    constexpr std::chrono::milliseconds kPOLL_INTERVAL{50};
    socket::CommandResponse OperatorCommandHandler::handle_command(const std::string& command_line){
        try{
            auto cmd = parse_command(command_line);
            if(cmd.type == OperatorCommandType::kUNKNOWN){
                return socket::CommandResponse{
                    .ok = false,
                    .body = "command rejected: unknown command type",
                };
            }
            if(!queues_.server_to_control_queue.try_push(cmd)){
                return socket::CommandResponse{
                    .ok = false,
                    .body = "command rejected: control queue is full",
                };
            }
            OperatorResponse operator_response{};
            const auto deadline = std::chrono::steady_clock::now() + kMAX_WAIT_TIME;
            while(std::chrono::steady_clock::now() < deadline){
                if(queues_.control_to_server_queue.try_pop(operator_response)){
                    if(operator_response.request_id == cmd.request_id){
                        return answer_operator(operator_response);
                    }
                    /*
                        This should never happen as current intended usage is one command at a time, 
                        but if it does somehow we don't want to crash/stall the server so just return the error back.


                        for future use cases where multiple concurrent commands are expected, will necessitate a different 
                        architecture from SPSC queues. 
                    */
                    return socket::CommandResponse{
                        .ok = false,
                        .body = "command rejected: received response with mismatched request id",
                    };
                }
                std::this_thread::sleep_for(kPOLL_INTERVAL);
            }
        }catch(const nlohmann::json::exception& e){
            return socket::CommandResponse{
                .ok = false,
                .body = std::string("command rejected: failed to parse command - ") + e.what(),
            };
        }catch(const std::exception& e){
            return socket::CommandResponse{
                .ok = false,
                .body = std::string("command rejected: failed to parse command - ") + e.what(),
            };
        }
        return socket::CommandResponse{
            .ok = false,
            .body = "command rejected: timed out waiting for response",
        };
    }
}
