#include "predex/control/control_plane.hpp"
#include "predex/control/control_types.hpp"
#include "predex/shard/shard_types.hpp"

#include <algorithm>
#include <chrono>
#include <type_traits>
#include <utility>


namespace predex::core::control{

    namespace{

        [[nodiscard]] shard::MarketScale to_shard_market_scale(PriceLevelStructure structure) noexcept{
            switch(structure){
                case PriceLevelStructure::kLINEAR_CENT:
                    return shard::MarketScale::kLINEAR_CENTS;
                case PriceLevelStructure::kTAPERED_DECI_CENT:
                    return shard::MarketScale::kTAPERED_DECI_CENTS;
                case PriceLevelStructure::kDECI_CENT:
                    return shard::MarketScale::kDECI_CENTS;
                case PriceLevelStructure::kUNKNOWN:
                    return shard::MarketScale::kUNKNOWN;
            }
            return shard::MarketScale::kUNKNOWN;
        }

        [[nodiscard]] ShardTelemetrySnapshot to_control_shard_stats(const shard::ShardStats& stats) noexcept{
            return ShardTelemetrySnapshot{
                .frames_seen = stats.frames_seen,
                .frames_applied = stats.frames_applied,
                .parse_rejects = stats.parse_rejects,
                .event_rejects = stats.event_rejects,
                .event_desyncs = stats.event_desyncs,
                .frames_to_logger = stats.frames_to_logger,
                .frames_recycled = stats.frames_recycled,
                .leaked_handles = stats.leaked_handles,
                .missed_frames_to_logger = stats.missed_frames_to_logger,
                .event_ignored = stats.event_ignored,
                .market_barriers_seen = stats.market_barriers_seen,
                .subscription_barriers_seen = stats.subscription_barriers_seen,
                .barrier_rejects = stats.barrier_rejects,
                .markets_became_unusable = stats.markets_became_unusable,
                .markets_recovery_required = stats.markets_recovery_required,
                .markets_already_awaiting_recovery =
                    stats.markets_already_awaiting_recovery,
                .router_to_shard_latency = stats.router_to_shard_latency,
                .shard_service_latency = stats.shard_service_latency,
                .ingress_to_shard_latency = stats.ingress_to_shard_latency,
                .ingress_to_book_apply_latency =
                    stats.ingress_to_book_apply_latency,
            };
        }

        void account_recovery_observation(
            RecoveryTelemetrySnapshot& telemetry,
            const RecoveryObservationResult& result) noexcept{
            switch(result.code){
                case RecoveryObservationCode::kCREATED:
                    ++telemetry.incidents_created;
                    telemetry.markets_scheduled += result.markets_affected;
                    break;
                case RecoveryObservationCode::kDUPLICATE:
                    ++telemetry.incidents_deduplicated;
                    break;
                case RecoveryObservationCode::kALREADY_RECOVERING:
                    ++telemetry.markets_already_recovering;
                    break;
                case RecoveryObservationCode::kSTALE_UNIVERSE:
                case RecoveryObservationCode::kINVALID_INCIDENT:
                case RecoveryObservationCode::kINVALID_REASON:
                case RecoveryObservationCode::kUNKNOWN_MARKET:
                case RecoveryObservationCode::kROUTE_MISMATCH:
                case RecoveryObservationCode::kRECOVERY_ID_EXHAUSTED:
                    ++telemetry.observations_rejected;
                    break;
            }
        }

        void account_recovery_fact(
            RecoveryTelemetrySnapshot& telemetry,
            const RecoveryFactResult& result) noexcept{
            if(result.disposition != RecoveryFactDisposition::kAPPLIED){
                return;
            }
            switch(result.effect){
                case RecoveryFactEffect::kREQUEST_ACCEPTED:
                    ++telemetry.requests_accepted;
                    break;
                case RecoveryFactEffect::kRETRY_SCHEDULED:
                    ++telemetry.request_failures;
                    ++telemetry.retries_scheduled;
                    break;
                case RecoveryFactEffect::kMARKET_RECOVERED:
                    ++telemetry.markets_recovered;
                    break;
                case RecoveryFactEffect::kMARKET_FAILED:
                    ++telemetry.request_failures;
                    ++telemetry.markets_failed;
                    break;
                case RecoveryFactEffect::kNONE:
                    break;
            }
            if(result.incident_completed){
                ++telemetry.incidents_completed;
                const auto duration = static_cast<std::uint64_t>(
                    std::max<std::int64_t>(
                        0,
                        result.recovery_duration.count()));
                ++telemetry.recovery_duration_samples;
                telemetry.recovery_duration_total_ns += duration;
                telemetry.recovery_duration_max_ns = std::max(
                    telemetry.recovery_duration_max_ns,
                    duration);
                telemetry.recovery_duration_latency.record(duration);
            }
        }

        [[nodiscard]] std::vector<operator_admin::UnknownMarketTickerStats> to_operator_unknown_market_ticker_stats(
            const std::vector<UnknownMarketTickerStats>& samples){
            std::vector<operator_admin::UnknownMarketTickerStats> result;
            result.reserve(samples.size());
            for(const auto& sample : samples){
                result.push_back(operator_admin::UnknownMarketTickerStats{
                    .market_ticker = sample.market_ticker,
                    .frame_kind = sample.frame_kind,
                    .sid = sample.sid,
                    .channel = sample.channel,
                });
            }
            return result;
        }

        [[nodiscard]] std::vector<std::vector<shard::KalshiEvent>> build_shard_events(
            const UniverseSnapshot& snapshot,
            std::size_t shard_count
        ){
            std::vector<std::vector<shard::KalshiEvent>> events_by_shard(shard_count);

            for(const auto& route : snapshot.market_routes){
                if(route.shard_index >= shard_count){
                    continue;
                }

                auto& shard_events = events_by_shard[route.shard_index];
                if(route.shard_event_index >= shard_events.size()){
                    shard_events.resize(route.shard_event_index + 1);
                }

                auto& event = shard_events[route.shard_event_index];
                event.event_id = route.event_id;
                event.topology = route.topology;
                event.shard_event_index = route.shard_event_index;

                if(route.event_market_index >= event.markets.size()){
                    event.markets.resize(route.event_market_index + 1);
                }

                auto& market = event.markets[route.event_market_index];
                market.market_id = route.market_id;
                market.event_market_index = route.event_market_index;
                market.tradeable = route.tradeable;
                market.book.scale = to_shard_market_scale(route.price_level_structure);
            }

            return events_by_shard;
        }

        [[nodiscard]] std::shared_ptr<const OrderRouteUniverse> build_order_route_universe(
            const UniverseSnapshot& snapshot
        ){
            auto order_universe = std::make_shared<OrderRouteUniverse>();
            order_universe->version = snapshot.version;
            order_universe->market_routes.reserve(snapshot.market_routes.size());

            for(const auto& route : snapshot.market_routes){
                order_universe->market_routes.push_back(OrderMarketRoute{
                    .market_id = route.market_id,
                    .event_id = route.event_id,
                    .kalshi_ticker = route.kalshi_ticker,
                    .tradeable = route.tradeable,
                    .price_level_structure = route.price_level_structure,
                });
            }

            return order_universe;
        }

        [[nodiscard]] operator_admin::OperatorCounterStatsSnapshot build_counter_stats_snapshot(
            const ProcessState& process_state
        ){
            std::vector<operator_admin::ShardCounterStats> shard_stats;
            shard_stats.reserve(process_state.shard_component_states.size());

            for(std::uint64_t shard_index = 0; shard_index < process_state.shard_component_states.size(); ++shard_index){
                const auto& shard_state = process_state.shard_component_states[shard_index];
                const auto& telemetry = shard_state.telemetry;
                shard_stats.push_back(operator_admin::ShardCounterStats{
                    .shard_index = shard_index,
                    .frames_seen = telemetry.frames_seen,
                    .frames_applied = telemetry.frames_applied,
                    .parse_rejects = telemetry.parse_rejects,
                    .event_rejects = telemetry.event_rejects,
                    .event_desyncs = telemetry.event_desyncs,
                    .frames_to_logger = telemetry.frames_to_logger,
                    .frames_recycled = telemetry.frames_recycled,
                    .leaked_handles = telemetry.leaked_handles,
                    .missed_frames_to_logger = telemetry.missed_frames_to_logger,
                    .event_ignored = telemetry.event_ignored,
                    .market_barriers_seen = telemetry.market_barriers_seen,
                    .subscription_barriers_seen = telemetry.subscription_barriers_seen,
                    .barrier_rejects = telemetry.barrier_rejects,
                    .markets_became_unusable = telemetry.markets_became_unusable,
                    .markets_recovery_required = telemetry.markets_recovery_required,
                    .markets_already_awaiting_recovery =
                        telemetry.markets_already_awaiting_recovery,
                    .router_to_shard_latency = telemetry.router_to_shard_latency,
                    .shard_service_latency = telemetry.shard_service_latency,
                    .ingress_to_shard_latency = telemetry.ingress_to_shard_latency,
                    .ingress_to_book_apply_latency =
                        telemetry.ingress_to_book_apply_latency,
                });
            }

            const auto& io_state = process_state.io_component_state;
            const auto& router_state = process_state.router_component_state;
            const auto& logger_state = process_state.logger_component_state;
            const auto& oms_state = process_state.oms_component_state;
            const auto& private_order_feed_state = process_state.private_order_feed_component_state;
            const auto& order_rest_state = process_state.order_rest_component_state;

            return operator_admin::OperatorCounterStatsSnapshot{
                .status_snapshot = operator_admin::OperatorStatusSnapshot{
                    .lifecycle = process_state.lifecycle,
                    .trading_session_phase = process_state.trading_session_phase,
                    .trading_enabled = process_state.trading_enabled,
                    .shutdown_requested = process_state.shutdown_requested,
                },
                .io_stats = operator_admin::IoCounterStats{
                    .io_connected = io_state.connected,
                    .installed_universe_version = io_state.installed_universe_version,
                    .subscribed_universe_version = io_state.subscribed_universe_version,
                    .frames_received = io_state.telemetry.frames_received,
                    .frames_published = io_state.telemetry.frames_published,
                    .frames_dropped = io_state.telemetry.frames_dropped,
                    .oversized_frames = io_state.telemetry.oversized_frames,
                    .pool_exhausted = io_state.telemetry.pool_exhausted,
                    .missing_frame_slot = io_state.telemetry.missing_frame_slot,
                    .envelope_parse_failed = io_state.telemetry.envelope_parse_failed,
                    .envelope_missing_market_ticker = io_state.telemetry.envelope_missing_market_ticker,
                    .envelope_unsupported_type = io_state.telemetry.envelope_unsupported_type,
                    .inactive_sid = io_state.telemetry.inactive_sid,
                    .unknown_market_ticker = io_state.telemetry.unknown_market_ticker,
                    .unknown_market_ticker_samples = to_operator_unknown_market_ticker_stats(io_state.telemetry.unknown_market_ticker_samples),
                    .stamp_failed = io_state.telemetry.stamp_failed,
                    .router_enqueue_failed = io_state.telemetry.router_enqueue_failed,
                    .logger_fallback_enqueued = io_state.telemetry.logger_fallback_enqueued,
                    .logger_fallback_failed = io_state.telemetry.logger_fallback_failed,
                    .recycle_failures = io_state.telemetry.recycle_failures,
                    .snapshot_requests_sent = io_state.telemetry.snapshot_requests_sent,
                    .snapshot_requests_accepted = io_state.telemetry.snapshot_requests_accepted,
                    .snapshot_requests_failed = io_state.telemetry.snapshot_requests_failed,
                    .frame_pool_in_use_high_water = io_state.telemetry.frame_pool_in_use_high_water,
                    .router_queue_depth_high_water = io_state.telemetry.router_queue_depth_high_water,
                    .channel_stats = io_state.telemetry.channel_stats,
                    .wire_service_latency = io_state.telemetry.wire_service_latency,
                    .last_error = io_state.last_error,
                },
                .router_stats = operator_admin::RouterCounterStats{
                    .frames_seen = router_state.telemetry.total_frames_seen,
                    .frames_to_shards = router_state.telemetry.frames_to_shards,
                    .frames_to_logger = router_state.telemetry.frames_to_logger,
                    .frames_recycled = router_state.telemetry.frames_recycled,
                    .leaked_handles = router_state.telemetry.leaked_handles,
                    .market_barriers_received = router_state.telemetry.market_barriers_received,
                    .market_barriers_delivered = router_state.telemetry.market_barriers_delivered,
                    .subscription_barriers_received = router_state.telemetry.subscription_barriers_received,
                    .subscription_barriers_delivered = router_state.telemetry.subscription_barriers_delivered,
                    .barriers_deferred = router_state.telemetry.barriers_deferred,
                    .subscription_recovery_facts_deferred = router_state.telemetry.subscription_recovery_facts_deferred,
                    .shard_queue_depth_high_water = router_state.telemetry.shard_queue_depth_high_water,
                    .channel_stats = router_state.telemetry.channel_stats,
                    .wire_to_router_latency =
                        router_state.telemetry.wire_to_router_latency,
                    .router_service_latency =
                        router_state.telemetry.router_service_latency,
                },
                .shard_stats = std::move(shard_stats),
                .logger_stats = operator_admin::LoggerCounterStats{
                    .output_file_path = logger_state.output_file_path,
                    .records_written = logger_state.telemetry.records_written,
                    .bytes_written = logger_state.telemetry.bytes_written,
                    .write_failures = logger_state.telemetry.write_failures,
                    .recycle_failures = logger_state.telemetry.recycle_failures,
                    .shard_to_logger_latency =
                        logger_state.telemetry.shard_to_logger_latency,
                    .ingress_to_logger_write_latency =
                        logger_state.telemetry.ingress_to_logger_write_latency,
                },
                .oms_stats = operator_admin::OmsCounterStats{
                    .trading_enabled = oms_state.trading_enabled,
                    .cancel_all_requested = oms_state.cancel_all_requested,
                    .installed_universe_version = oms_state.installed_universe_version,
                    .unknown_market_rejects = oms_state.telemetry.unknown_market_rejects,
                    .non_tradeable_market_rejects = oms_state.telemetry.non_tradeable_market_rejects,
                    .strategy_intents_received = oms_state.telemetry.strategy_intents_received,
                    .strategy_intents_processed = oms_state.telemetry.strategy_intents_processed,
                    .strategy_intents_rejected = oms_state.telemetry.strategy_intents_rejected,
                    .kalshi_commands_sent = oms_state.telemetry.kalshi_commands_sent,
                    .kalshi_commands_failed = oms_state.telemetry.kalshi_commands_failed,
                    .rest_responses_seen = oms_state.telemetry.rest_responses_seen,
                    .private_ws_events_seen = oms_state.telemetry.private_ws_events_seen,
                    .reconciliation_events_seen = oms_state.telemetry.reconciliation_events_seen,
                    .order_state_updates_sent = oms_state.telemetry.order_state_updates_sent,
                    .strategy_response_backpressure = oms_state.telemetry.strategy_response_backpressure,
                    .live_orders = oms_state.telemetry.live_orders,
                    .pending_submit_orders = oms_state.telemetry.pending_submit_orders,
                    .uncertain_orders = oms_state.telemetry.uncertain_orders,
                    .last_error = oms_state.last_error,
                },
                .private_order_feed_stats = operator_admin::PrivateOrderFeedCounterStats{
                    .connected = private_order_feed_state.connected,
                    .installed_universe_version = private_order_feed_state.installed_universe_version,
                    .subscribed_universe_version = private_order_feed_state.subscribed_universe_version,
                    .messages_received = private_order_feed_state.telemetry.messages_received,
                    .messages_decoded = private_order_feed_state.telemetry.messages_decoded,
                    .messages_dropped = private_order_feed_state.telemetry.messages_dropped,
                    .parse_failures = private_order_feed_state.telemetry.parse_failures,
                    .oms_enqueue_failures = private_order_feed_state.telemetry.oms_enqueue_failures,
                    .reconnects = private_order_feed_state.telemetry.reconnects,
                    .last_error = private_order_feed_state.last_error,
                },
                .order_rest_stats = operator_admin::OrderRestCounterStats{
                    .enabled = order_rest_state.enabled,
                    .installed_universe_version = order_rest_state.installed_universe_version,
                    .commands_received = order_rest_state.telemetry.commands_received,
                    .requests_sent = order_rest_state.telemetry.requests_sent,
                    .responses_received = order_rest_state.telemetry.responses_received,
                    .requests_failed = order_rest_state.telemetry.requests_failed,
                    .retry_count = order_rest_state.telemetry.retry_count,
                    .oms_enqueue_failures = order_rest_state.telemetry.oms_enqueue_failures,
                    .last_error = order_rest_state.last_error,
                },
                .recovery_stats = process_state.recovery_telemetry,
            };
        }

    }

    ControlPlane::ControlPlane(
        OperatorQueues queues,
        ControlIoQueues io_queues,
        RouterQueue router_queue,
        ControlShardQueues shard_queues,
        ControlLoggerQueue logger_queue,
        ControlOmsQueues oms_queues,
        ControlPrivateOrderFeedQueues private_order_feed_queues,
        ControlOrderRestQueues order_rest_queues,
        RequiredComponents required_components,
        SyntheticTradingSessionConfig synthetic_session_config
    ) : queues_(queues),
        io_queues_(io_queues),
        router_queue_(router_queue),
        shard_queues_(std::move(shard_queues)),
        logger_queue_(logger_queue),
        oms_queues_(oms_queues),
        private_order_feed_queues_(private_order_feed_queues),
        order_rest_queues_(order_rest_queues),
        required_components_(required_components),
        synthetic_session_config_(synthetic_session_config){
        process_state_.shard_component_states.resize(shard_queues_.control_to_shard_queues.size());
        process_state_.trading_session_phase = compute_trading_session_phase();
    }
//NOLINTNEXTLINE
    OperatorPumpResult ControlPlane::process_operator_commands(){
        (void)update_trading_session_phase();
        OperatorPumpResult result{};
        operator_admin::OperatorCommand item{};
        const auto push_ack = [&](operator_admin::OperatorCommandId request_id){
            if(push_operator_response(operator_admin::OperatorResponse{
                .request_id = request_id,
                .type = operator_admin::OperatorResponseType::kACK,
                .payload = operator_admin::AckPayload{},
            })){
                result.responses_pushed_success++;
            }else{
                result.responses_pushed_failure++;
            }
        };

        const auto push_error = [&](operator_admin::OperatorCommandId request_id, std::string message){
            if(push_operator_response(operator_admin::OperatorResponse{
                .request_id = request_id,
                .type = operator_admin::OperatorResponseType::kERROR,
                .payload = operator_admin::ErrorPayload{.message = std::move(message)},
            })){
                result.responses_pushed_success++;
            }else{
                result.responses_pushed_failure++;
            }
        };

        while(queues_.operator_command_queue.try_pop(item)){
            result.commands_processed++;
            switch(item.type){
                case operator_admin::OperatorCommandType::kSTATUS:{                  
                    if(push_operator_response(operator_admin::OperatorResponse{
                        .request_id = item.request_id,
                        .type = operator_admin::OperatorResponseType::kSTATUS,
                        .payload = operator_admin::OperatorStatusSnapshot{
                            .lifecycle = process_state_.lifecycle,
                            .trading_session_phase = process_state_.trading_session_phase,
                            .trading_enabled = process_state_.trading_enabled,
                            .shutdown_requested = process_state_.shutdown_requested,
                        }
                    })){
                        result.responses_pushed_success++;
                    }else{
                        result.responses_pushed_failure++;
                    }
                    break;
                }
                case operator_admin::OperatorCommandType::kCOUNTERSTATS:{
                    if(push_operator_response(operator_admin::OperatorResponse{
                        .request_id = item.request_id,
                        .type = operator_admin::OperatorResponseType::kCOUNTERSTATS,
                        .payload = build_counter_stats_snapshot(process_state_),
                    })){
                        result.responses_pushed_success++;
                    }else{
                        result.responses_pushed_failure++;
                    }
                    break;
                }
                case operator_admin::OperatorCommandType::kUNKNOWN:{
                     push_error(item.request_id, "unknown command");
                     break;
                }
                case operator_admin::OperatorCommandType::kSHUTDOWN_GRACEFUL:{
                    process_state_.lifecycle = LifecyclePhase::kSHUTTING_DOWN;
                    process_state_.shutdown_requested = true;
                    process_state_.trading_enabled = false;

                    if(required_components_.oms){
                        (void)push_oms_command(ControlToOmsCommand{DisableTrading{}});
                    }
                    push_ack(item.request_id);
                    break;
                }
                case operator_admin::OperatorCommandType::kSHUTDOWN_FORCEFUL:{
                    process_state_.lifecycle = LifecyclePhase::kSHUTTING_DOWN;
                    process_state_.shutdown_requested = true;
                    process_state_.trading_enabled = false;
                    process_state_.oms_component_state.trading_enabled = false;
                    push_ack(item.request_id);
                    break;
                }
                case operator_admin::OperatorCommandType::kALLOW_TRADING:{
                    if(!required_components_.oms){
                        push_error(item.request_id, "cannot allow trading: OMS is not enabled");
                        break;
                    }
                    if(process_state_.trading_session_phase != TradingSessionPhase::kTRADING){
                        push_error(item.request_id, "cannot allow trading: synthetic trading session is no longer in trading phase");
                        break;
                    }
                    if(!required_components_ready_for_trading()){
                        push_error(item.request_id, "cannot allow trading: required trading components are not ready");
                        break;
                    }
                    if(!push_oms_command(ControlToOmsCommand{AllowTrading{}})){
                        push_error(item.request_id, "cannot allow trading: failed to enqueue OMS command");
                        break;
                    }
                    push_ack(item.request_id);
                    break;
                }
                case operator_admin::OperatorCommandType::kDISABLE_TRADING:{
                    process_state_.trading_enabled = false;
                    process_state_.oms_component_state.trading_enabled = false;
                    if(required_components_.oms && !push_oms_command(ControlToOmsCommand{DisableTrading{}})){
                        push_error(item.request_id, "failed to enqueue OMS disable-trading command");
                        break;
                    }
                    recompute_process_state();
                    push_ack(item.request_id);
                    break;
                }
                case operator_admin::OperatorCommandType::kCANCEL_ALL_ORDERS:{
                    if(!required_components_.oms){
                        push_error(item.request_id, "cannot cancel all orders: OMS is not enabled");
                        break;
                    }
                    if(!push_oms_command(ControlToOmsCommand{CancelAllOrders{}})){
                        push_error(item.request_id, "cannot cancel all orders: failed to enqueue OMS command");
                        break;
                    }
                    process_state_.oms_component_state.cancel_all_requested = true;
                    process_state_.trading_enabled = false;
                    process_state_.oms_component_state.trading_enabled = false;
                    recompute_process_state();
                    push_ack(item.request_id);
                    break;
                }

            }
        }
        return result;
    }       

    IoPumpResult ControlPlane::process_io_status() noexcept{
        IoPumpResult result{};
        IoToControlStatus status_out;
        
        while(io_queues_.io_to_control_status_queue.try_pop(status_out)){
            result.statuses_processed++;
            apply_io_status(status_out);
            recompute_process_state();
        }
        return result;
    }


    void ControlPlane::apply_io_status(const IoToControlStatus& status) noexcept{
        std::visit([&](auto&& stat){
            using T = std::decay_t<decltype(stat)>;
            if constexpr(std::is_same_v<T, IoConnected>){
                process_state_.io_component_state.status = ComponentStatus::kREADY;
                process_state_.io_component_state.connected = true;
                process_state_.io_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, IoDisconnected>){
                process_state_.io_component_state.status = ComponentStatus::kSTOPPED;
                process_state_.io_component_state.connected = false;
                process_state_.io_component_state.last_error = stat.reason;
            }else if constexpr(std::is_same_v<T, IoUniverseSnapshotApplied>){
                process_state_.io_component_state.installed_universe_version = stat.version;
                process_state_.io_component_state.status = ComponentStatus::kREADY;
                process_state_.io_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, IoSubscriptionReady>){
                process_state_.io_component_state.subscribed_universe_version = stat.version;
                process_state_.io_component_state.status = ComponentStatus::kLIVE;
                process_state_.io_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, IoFaulted>){
                process_state_.io_component_state.status = ComponentStatus::kFAULTED;
                process_state_.io_component_state.connected = false;
                process_state_.io_component_state.last_error = stat.error_message;
            }else if constexpr(std::is_same_v<T, IoTelemetry>){
                process_state_.io_component_state.telemetry = stat.telemetry;
            }else if constexpr(std::is_same_v<T, IoRecoveryRequestAccepted>){
                const auto result = recovery_coordinator_.handle(stat, std::chrono::steady_clock::now());
                account_recovery_fact(process_state_.recovery_telemetry, result);
            }else if constexpr(std::is_same_v<T, IoRecoveryRequestFailed>){
                const auto result = recovery_coordinator_.handle(stat, std::chrono::steady_clock::now());
                account_recovery_fact(process_state_.recovery_telemetry, result);
            }
        }, status);
    }

    TradingSessionPhase ControlPlane::compute_trading_session_phase() const noexcept{
        if(!synthetic_session_config_.enabled){
            return TradingSessionPhase::kTRADING;
        }

        const auto elapsed = std::chrono::steady_clock::now() - session_start_time_;
        const auto elapsed_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()
        );

        if(synthetic_session_config_.stopped_after_ns != 0 &&
           elapsed_ns >= synthetic_session_config_.stopped_after_ns){
            return TradingSessionPhase::kSTOPPED;
        }
        if(synthetic_session_config_.flatten_to_zero_after_ns != 0 &&
           elapsed_ns >= synthetic_session_config_.flatten_to_zero_after_ns){
            return TradingSessionPhase::kFLATTEN_TO_ZERO;
        }
        if(synthetic_session_config_.reduce_only_after_ns != 0 &&
           elapsed_ns >= synthetic_session_config_.reduce_only_after_ns){
            return TradingSessionPhase::kREDUCE_ONLY;
        }
        return TradingSessionPhase::kTRADING;
    }

    void ControlPlane::apply_trading_session_phase(TradingSessionPhase phase) noexcept{
        if(process_state_.trading_session_phase == phase){
            return;
        }

        process_state_.trading_session_phase = phase;
        if(phase != TradingSessionPhase::kTRADING){
            process_state_.trading_enabled = false;
            process_state_.oms_component_state.trading_enabled = false;
            if(required_components_.oms){
                (void)push_oms_command(ControlToOmsCommand{DisableTrading{}});
            }
        }
        recompute_process_state();
    }

    bool ControlPlane::update_trading_session_phase() noexcept{
        const TradingSessionPhase previous = process_state_.trading_session_phase;
        apply_trading_session_phase(compute_trading_session_phase());
        return process_state_.trading_session_phase != previous;
    }

    void ControlPlane::recompute_process_state() noexcept{
        if(process_state_.shutdown_requested){
            if((!required_components_.market_data || process_state_.io_component_state.status == ComponentStatus::kSTOPPED) &&
               (!required_components_.private_order_feed || process_state_.private_order_feed_component_state.status == ComponentStatus::kSTOPPED) &&
               (!required_components_.order_rest || process_state_.order_rest_component_state.status == ComponentStatus::kSTOPPED)){
                process_state_.lifecycle = LifecyclePhase::kSTOPPED;
            }else{
                process_state_.lifecycle = LifecyclePhase::kSHUTTING_DOWN;
            }
            return;
        }

        if(required_components_faulted()){
            process_state_.lifecycle = LifecyclePhase::kFAULTED;
            process_state_.trading_enabled = false;
            process_state_.oms_component_state.trading_enabled = false;
            return;
        }

        if(required_components_ready_for_trading() && process_state_.trading_enabled){
            process_state_.lifecycle = LifecyclePhase::kLIVE_TRADING;
            process_state_.active_universe_version = process_state_.target_universe_version;
            return;
        }

        if(required_components_ready_for_capture()){
            process_state_.lifecycle = LifecyclePhase::kREADY;
            process_state_.active_universe_version = process_state_.target_universe_version;
            return;
        }

        if(!required_components_.market_data ||
           process_state_.io_component_state.status == ComponentStatus::kREADY ||
           process_state_.io_component_state.status == ComponentStatus::kLIVE){
            process_state_.lifecycle = LifecyclePhase::kIO_CONNECTED;
            return;
        }

        process_state_.lifecycle = LifecyclePhase::kWAITING_FOR_IO;
    }

    bool ControlPlane::required_components_faulted() const{
        if(recovery_orchestration_faulted_){
            return true;
        }
        if(required_components_.market_data && process_state_.io_component_state.status == ComponentStatus::kFAULTED){
            return true;
        }
        if(process_state_.router_component_state.status == ComponentStatus::kFAULTED){
            return true;
        }
        if(required_components_.logger && process_state_.logger_component_state.status == ComponentStatus::kFAULTED){
            return true;
        }
        if(required_components_.oms && process_state_.oms_component_state.status == ComponentStatus::kFAULTED){
            return true;
        }
        if(required_components_.private_order_feed && process_state_.private_order_feed_component_state.status == ComponentStatus::kFAULTED){
            return true;
        }
        if(required_components_.order_rest && process_state_.order_rest_component_state.status == ComponentStatus::kFAULTED){
            return true;
        }
        if(required_components_.shards){
            return std::any_of(
                process_state_.shard_component_states.begin(),
                process_state_.shard_component_states.end(),
                [](const ShardComponentState& shard_state){
                    return shard_state.status == ComponentStatus::kFAULTED;
                }
            );
        }
        return false;
    }

    bool ControlPlane::required_components_ready_for_capture() const{
        const std::uint64_t target_version = process_state_.target_universe_version;
        if(target_version == 0){
            return false;
        }

        if(required_components_.market_data &&
           (process_state_.io_component_state.status != ComponentStatus::kLIVE ||
            process_state_.io_component_state.installed_universe_version != target_version ||
            process_state_.io_component_state.subscribed_universe_version != target_version)){
            return false;
        }

        if(required_components_.shards){
            if(process_state_.shard_component_states.empty()){
                return false;
            }
            const bool shards_ready = std::all_of(
                process_state_.shard_component_states.begin(),
                process_state_.shard_component_states.end(),
                [target_version](const ShardComponentState& shard_state){
                    return shard_state.status == ComponentStatus::kLIVE &&
                           shard_state.installed_universe_version == target_version;
                }
            );
            if(!shards_ready){
                return false;
            }
        }

        if(required_components_.logger &&
           process_state_.logger_component_state.status != ComponentStatus::kLIVE){
            return false;
        }

        return true;
    }

    bool ControlPlane::required_components_ready_for_trading() const{
        const std::uint64_t target_version = process_state_.target_universe_version;
        if(!required_components_ready_for_capture()){
            return false;
        }

        if(required_components_.oms &&
           (process_state_.oms_component_state.status != ComponentStatus::kREADY ||
            process_state_.oms_component_state.installed_universe_version != target_version)){
            return false;
        }

        if(required_components_.order_rest &&
           (process_state_.order_rest_component_state.status != ComponentStatus::kREADY ||
            process_state_.order_rest_component_state.installed_universe_version != target_version)){
            return false;
        }

        if(required_components_.private_order_feed &&
           (process_state_.private_order_feed_component_state.status != ComponentStatus::kLIVE ||
            process_state_.private_order_feed_component_state.installed_universe_version != target_version ||
            process_state_.private_order_feed_component_state.subscribed_universe_version != target_version)){
            return false;
        }

        return true;
    }

    std::uint64_t ControlPlane::install_universe(UniverseSnapshot snapshot){
        snapshot.version = next_universe_version_;
        auto next_universe =
            std::make_shared<const UniverseSnapshot>(std::move(snapshot));
        auto next_order_universe =
            build_order_route_universe(*next_universe);

        ++next_universe_version_;
        const auto supersession = recovery_coordinator_.supersede_before_universe(
            next_universe->version,
            std::chrono::steady_clock::now());
        process_state_.recovery_telemetry.incidents_superseded +=
            supersession.incidents_superseded;
        process_state_.recovery_telemetry.markets_superseded +=
            supersession.markets_superseded;

        active_universe_ = std::move(next_universe);
        active_order_universe_ = std::move(next_order_universe);
        process_state_.target_universe_version = active_universe_->version;
        recompute_process_state();
        return active_universe_->version;
    }

    bool ControlPlane::send_active_universe_to_io(){
        if(active_universe_ == nullptr){
            return false;
        }
        return push_io_command(ApplyUniverseSnapshotIo{.snapshot = active_universe_});
    }

    bool ControlPlane::send_active_order_universe_to_oms(){
        if(active_order_universe_ == nullptr || oms_queues_.control_to_oms_queue == nullptr){
            return false;
        }
        return oms_queues_.control_to_oms_queue->try_push(
            ControlToOmsCommand{ApplyOrderRouteUniverse{.snapshot = active_order_universe_}}
        );
    }

    bool ControlPlane::send_active_order_universe_to_private_order_feed(){
        if(active_order_universe_ == nullptr || private_order_feed_queues_.control_to_private_order_feed_queue == nullptr){
            return false;
        }
        return private_order_feed_queues_.control_to_private_order_feed_queue->try_push(
            ControlToPrivateOrderFeedCommand{ApplyOrderRouteUniverse{.snapshot = active_order_universe_}}
        );
    }

    bool ControlPlane::send_active_order_universe_to_order_rest(){
        if(active_order_universe_ == nullptr || order_rest_queues_.control_to_order_rest_queue == nullptr){
            return false;
        }
        return order_rest_queues_.control_to_order_rest_queue->try_push(
            ControlToOrderRestCommand{ApplyOrderRouteUniverse{.snapshot = active_order_universe_}}
        );
    }

    bool ControlPlane::push_private_order_feed_command(const ControlToPrivateOrderFeedCommand& cmd){//NOLINT
        if(private_order_feed_queues_.control_to_private_order_feed_queue == nullptr){
            return false;
        }
        return private_order_feed_queues_.control_to_private_order_feed_queue->try_push(cmd);
    }

    bool ControlPlane::push_order_rest_command(const ControlToOrderRestCommand& cmd){//NOLINT
        if(order_rest_queues_.control_to_order_rest_queue == nullptr){
            return false;
        }
        return order_rest_queues_.control_to_order_rest_queue->try_push(cmd);
    }

    bool ControlPlane::push_oms_command(const ControlToOmsCommand& cmd){//NOLINT 
        if(oms_queues_.control_to_oms_queue == nullptr){
            return false;
        }
        return oms_queues_.control_to_oms_queue->try_push(cmd);
    }

    bool ControlPlane::push_shard_command(std::uint32_t shard_index, shard::ControlToShardCommand command){
        if(shard_index >= shard_queues_.control_to_shard_queues.size()){
            return false;
        }
        auto* queue = shard_queues_.control_to_shard_queues[shard_index];
        if(queue == nullptr){
            return false;
        }
        return queue->try_push(std::move(command));
    }

    ShardPumpResult ControlPlane::send_active_universe_to_shards(){
        ShardPumpResult result{};
        if(active_universe_ == nullptr){
            return result;
        }

        const auto shard_count = shard_queues_.control_to_shard_queues.size();
        auto events_by_shard = build_shard_events(*active_universe_, shard_count);

        if(process_state_.shard_component_states.size() < shard_count){
            process_state_.shard_component_states.resize(shard_count);
        }

        for(std::uint32_t shard_index = 0; shard_index < shard_count; ++shard_index){
            shard::InstallShardUniverse command{
                .universe_version = active_universe_->version,
                .shard_index = shard_index,
                .events = std::move(events_by_shard[shard_index]),
            };
            if(push_shard_command(shard_index, shard::ControlToShardCommand{std::move(command)})){
                ++result.commands_pushed_success;
                auto& shard_state = process_state_.shard_component_states[shard_index];
                shard_state.status = ComponentStatus::kINSTALLING_UNIVERSE;
                shard_state.installed_universe_version = 0;
                shard_state.safe_to_stop_universe_version = 0;
                shard_state.drained_universe_version = 0;
                shard_state.last_error.clear();
            }else{
                ++result.commands_pushed_failure;
            }
        }
        recompute_process_state();
        return result;
    }

    ShardPumpResult ControlPlane::prepare_active_universe_stop_on_shards(){
        ShardPumpResult result{};
        if(active_universe_ == nullptr){
            return result;
        }

        const auto shard_count = shard_queues_.control_to_shard_queues.size();
        for(std::uint32_t shard_index = 0; shard_index < shard_count; ++shard_index){
            shard::PrepareStopUniverse command{
                .universe_version = active_universe_->version,
                .shard_index = shard_index,
            };
            if(push_shard_command(shard_index, shard::ControlToShardCommand{command})){
                ++result.commands_pushed_success;
                if(shard_index < process_state_.shard_component_states.size()){
                    process_state_.shard_component_states[shard_index].status = ComponentStatus::kQUIESCING;
                }
            }else{
                ++result.commands_pushed_failure;
            }
        }
        recompute_process_state();
        return result;
    }

    ShardPumpResult ControlPlane::drain_active_universe_on_shards(){
        ShardPumpResult result{};
        if(active_universe_ == nullptr){
            return result;
        }

        const auto shard_count = shard_queues_.control_to_shard_queues.size();
        for(std::uint32_t shard_index = 0; shard_index < shard_count; ++shard_index){
            shard::DrainShardUniverse command{
                .universe_version = active_universe_->version,
                .shard_index = shard_index,
            };
            if(push_shard_command(shard_index, shard::ControlToShardCommand{command})){
                ++result.commands_pushed_success;
                if(shard_index < process_state_.shard_component_states.size()){
                    process_state_.shard_component_states[shard_index].status = ComponentStatus::kQUIESCING;
                }
            }else{
                ++result.commands_pushed_failure;
            }
        }
        recompute_process_state();
        return result;
    }

    ShardPumpResult ControlPlane::resume_active_universe_on_shards(){
        ShardPumpResult result{};
        if(active_universe_ == nullptr){
            return result;
        }

        const auto shard_count = shard_queues_.control_to_shard_queues.size();
        for(std::uint32_t shard_index = 0; shard_index < shard_count; ++shard_index){
            shard::ResumeShardUniverse command{
                .universe_version = active_universe_->version,
                .shard_index = shard_index,
            };
            if(push_shard_command(shard_index, shard::ControlToShardCommand{command})){
                ++result.commands_pushed_success;
            }else{
                ++result.commands_pushed_failure;
            }
        }
        recompute_process_state();
        return result;
    }

    bool ControlPlane::process_one_router_message() noexcept{
        router::RouterToControl msg{};

        if (router_queue_.router_to_control_queue.try_pop(msg)){
            std::visit([&](auto&& m){//NOLINT
                using T = std::decay_t<decltype(m)>;
                if constexpr(std::is_same_v<T, router::RouterTelemetry>){
                    process_state_.router_component_state.telemetry.total_frames_seen = m.total_frames_seen;
                    process_state_.router_component_state.telemetry.frames_to_shards = m.frames_to_shards;
                    process_state_.router_component_state.telemetry.frames_to_logger = m.frames_to_logger;
                    process_state_.router_component_state.telemetry.frames_recycled = m.frames_recycled;
                    process_state_.router_component_state.telemetry.market_barriers_received = m.market_barriers_received;
                    process_state_.router_component_state.telemetry.market_barriers_delivered = m.market_barriers_delivered;
                    process_state_.router_component_state.telemetry.subscription_barriers_received = m.subscription_barriers_received;
                    process_state_.router_component_state.telemetry.subscription_barriers_delivered = m.subscription_barriers_delivered;
                    process_state_.router_component_state.telemetry.barriers_deferred = m.barriers_deferred;
                    process_state_.router_component_state.telemetry.subscription_recovery_facts_deferred = m.subscription_recovery_facts_deferred;
                    process_state_.router_component_state.telemetry.shard_queue_depth_high_water = m.shard_queue_depth_high_water;
                    process_state_.router_component_state.telemetry.channel_stats = m.channel_stats;
                    process_state_.router_component_state.telemetry.wire_to_router_latency =
                        m.wire_to_router_latency;
                    process_state_.router_component_state.telemetry.router_service_latency =
                        m.router_service_latency;
                }
                if constexpr(std::is_same_v<T, router::ShardBackpressure>){
                    //probably here want to update that shard's status 
                }
                if constexpr(std::is_same_v<T, router::OutOfSequenceFrame>){
                    process_state_.router_component_state.last_error = "Out of sequence frame detected: sid "+ std::to_string(m.sid) + " sequence " + std::to_string(m.sequence);
                }
                if constexpr(std::is_same_v<T, router::RouterHandleLeak>){
                    process_state_.router_component_state.telemetry.leaked_handles++;
                    process_state_.router_component_state.last_error = "Handle leak detected for universe version " + std::to_string(m.universe_version) + " shard index " + std::to_string(m.shard_index);
                    process_state_.router_component_state.status = ComponentStatus::kFAULTED;//major issue, needs attention
                }
                if constexpr(std::is_same_v<T, router::OrderBookSubscriptionBarrierDelivered>){
                    if(active_universe_ == nullptr){
                        ++process_state_.recovery_telemetry.observations_rejected;
                        return;
                    }
                    try{
                        const auto observation = recovery_coordinator_.observe(
                            m,
                            *active_universe_,
                            std::chrono::steady_clock::now());
                        account_recovery_observation(
                            process_state_.recovery_telemetry,
                            observation);
                    }catch(...){
                        ++process_state_.recovery_telemetry.observations_rejected;
                        recovery_orchestration_faulted_ = true;
                        process_state_.router_component_state.status =
                            ComponentStatus::kFAULTED;
                        process_state_.router_component_state.last_error =
                            "Failed to create subscription recovery incident";
                    }
                }
            }, msg);
            recompute_process_state();
            return true;
        }
        return false;
    }

    bool ControlPlane::process_router_messages() noexcept{
        bool processed_any = false;
        while(process_one_router_message()){
            processed_any = true;
        }
        return processed_any;
    }

    void ControlPlane::apply_shard_status(const shard::ShardToControlMessage& status) noexcept{
        std::visit([&](auto&& stat){
            using T = std::decay_t<decltype(stat)>;
            const auto ensure_shard_state = [&](std::uint32_t shard_index) -> ShardComponentState* {
                if(shard_index >= process_state_.shard_component_states.size()){
                    process_state_.shard_component_states.resize(shard_index + 1);
                }
                return &process_state_.shard_component_states[shard_index];
            };

            if constexpr(std::is_same_v<T, shard::ShardUniverseInstalled>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                shard_state->status = ComponentStatus::kLIVE;
                shard_state->installed_universe_version = stat.universe_version;
                shard_state->last_error.clear();
            }else if constexpr(std::is_same_v<T, shard::ShardSafeToStopUniverse>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                shard_state->status = ComponentStatus::kREADY;
                shard_state->safe_to_stop_universe_version = stat.universe_version;
                shard_state->last_error = stat.reason;
            }else if constexpr(std::is_same_v<T, shard::ShardDrainComplete>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                shard_state->status = ComponentStatus::kSTOPPED;
                shard_state->drained_universe_version = stat.universe_version;
                shard_state->telemetry = to_control_shard_stats(stat.stats);
                shard_state->last_error.clear();
            }else if constexpr(std::is_same_v<T, shard::ShardParseRejected>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                ++shard_state->telemetry.parse_rejects;
                shard_state->last_error = "Shard parse rejected sequence " + std::to_string(stat.sequence);
            }else if constexpr(std::is_same_v<T, shard::ShardApplyRejected>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                ++shard_state->telemetry.event_rejects;
                shard_state->last_error = "Shard apply rejected sequence " + std::to_string(stat.sequence);
            }else if constexpr(std::is_same_v<T, shard::ShardEventDesynced>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                ++shard_state->telemetry.event_desyncs;
                shard_state->last_error = "Shard event desynced sequence " + std::to_string(stat.sequence);
            }else if constexpr(std::is_same_v<T, shard::ShardTelemetry>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                shard_state->telemetry = to_control_shard_stats(stat.stats);
            }else if constexpr(std::is_same_v<T, shard::ShardFaulted>){
                auto* shard_state = ensure_shard_state(stat.shard_index);
                shard_state->status = ComponentStatus::kFAULTED;
                shard_state->last_error = stat.reason;
            }else if constexpr(std::is_same_v<T, shard::ShardMarketRecoveryRequired>){
                if(active_universe_ == nullptr){return;}
                try{
                    const auto result = recovery_coordinator_.observe(stat, *active_universe_, std::chrono::steady_clock::now());
                    account_recovery_observation(
                        process_state_.recovery_telemetry,
                        result);
                }catch(...){
                    ++process_state_.recovery_telemetry.observations_rejected;
                    recovery_orchestration_faulted_ = true;
                    auto* shard_state = ensure_shard_state(stat.shard_index);
                    shard_state->status = ComponentStatus::kFAULTED;
                    shard_state->last_error =
                        "Failed to create market recovery incident";
                }
            }else if constexpr(std::is_same_v<T, shard::ShardRecoverySnapshotApplied>){
                if(active_universe_ == nullptr){return;}
                const auto result = recovery_coordinator_.handle(stat, *active_universe_, std::chrono::steady_clock::now());
                account_recovery_fact(process_state_.recovery_telemetry, result);
            }
        }, status);
    }

    bool ControlPlane::process_one_shard_message() noexcept{
        for(auto* queue : shard_queues_.shard_to_control_queues){
            if(queue == nullptr){
                continue;
            }

            shard::ShardToControlMessage status{};
            if(queue->try_pop(status)){
                apply_shard_status(status);
                recompute_process_state();
                return true;
            }
        }
        return false;
    }

    bool ControlPlane::process_shard_messages() noexcept{
        bool processed_any = false;
        while(process_one_shard_message()){
            processed_any = true;
        }
        return processed_any;
    }

    void ControlPlane::apply_logger_status(const LoggerToControlStatus& status) noexcept{
        std::visit([&](auto&& stat){
            using T = std::decay_t<decltype(stat)>;
            if constexpr(std::is_same_v<T, LoggerStarted>){
                process_state_.logger_component_state.status = ComponentStatus::kLIVE;
                process_state_.logger_component_state.output_file_path = stat.output_file_path;
                process_state_.logger_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, LoggerFaulted>){
                process_state_.logger_component_state.status = ComponentStatus::kFAULTED;
                process_state_.logger_component_state.last_error = stat.error_message;
            }else if constexpr(std::is_same_v<T, LoggerTelemetry>){
                process_state_.logger_component_state.telemetry = stat.telemetry;
            }
        }, status);
    }

    bool ControlPlane::process_one_logger_message() noexcept{
        if(logger_queue_.logger_to_control_status_queue == nullptr){
            return false;
        }

        LoggerToControlStatus status{};
        if(logger_queue_.logger_to_control_status_queue->try_pop(status)){
            apply_logger_status(status);
            recompute_process_state();
            return true;
        }
        return false;
    }

    bool ControlPlane::process_logger_messages() noexcept{
        bool processed_any = false;
        while(process_one_logger_message()){
            processed_any = true;
        }
        return processed_any;
    }

    void ControlPlane::apply_oms_status(const OmsToControlStatus& status) noexcept{
        std::visit([&](auto&& stat){
            using T = std::decay_t<decltype(stat)>;
            if constexpr(std::is_same_v<T, OmsReady>){
                process_state_.oms_component_state.status = ComponentStatus::kREADY;
                process_state_.oms_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, OmsFaulted>){
                process_state_.oms_component_state.status = ComponentStatus::kFAULTED;
                process_state_.oms_component_state.last_error = stat.error_message;
                process_state_.oms_component_state.trading_enabled = false;
            }else if constexpr(std::is_same_v<T, OmsTelemetry>){
                process_state_.oms_component_state.telemetry = stat.telemetry;
                process_state_.oms_component_state.installed_universe_version = stat.telemetry.installed_universe_version;
            }else if constexpr(std::is_same_v<T, OmsTradingEnabledChanged>){
                process_state_.oms_component_state.trading_enabled = stat.trading_enabled;
                process_state_.trading_enabled = stat.trading_enabled;
            }else if constexpr(std::is_same_v<T, OmsCancelAllStateChanged>){
                process_state_.oms_component_state.cancel_all_requested = stat.cancel_all_requested;
            }else if constexpr(std::is_same_v<T, OmsRestEgressDrained>){
                process_state_.oms_component_state.telemetry.live_orders = stat.live_orders;
                process_state_.oms_component_state.telemetry.uncertain_orders = stat.uncertain_orders;
            }
        }, status);
    }

    bool ControlPlane::process_one_oms_status() noexcept{
        if(oms_queues_.oms_to_control_status_queue == nullptr){
            return false;
        }

        OmsToControlStatus status{};
        if(oms_queues_.oms_to_control_status_queue->try_pop(status)){
            apply_oms_status(status);
            recompute_process_state();
            return true;
        }
        return false;
    }

    bool ControlPlane::process_oms_status() noexcept{
        bool processed_any = false;
        while(process_one_oms_status()){
            processed_any = true;
        }
        return processed_any;
    }

    void ControlPlane::apply_private_order_feed_status(const PrivateOrderFeedToControlStatus& status) noexcept{
        std::visit([&](auto&& stat){
            using T = std::decay_t<decltype(stat)>;
            if constexpr(std::is_same_v<T, PrivateOrderFeedConnected>){
                process_state_.private_order_feed_component_state.status = ComponentStatus::kREADY;
                process_state_.private_order_feed_component_state.connected = true;
                process_state_.private_order_feed_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, PrivateOrderFeedDisconnected>){
                process_state_.private_order_feed_component_state.status = ComponentStatus::kSTOPPED;
                process_state_.private_order_feed_component_state.connected = false;
                process_state_.private_order_feed_component_state.last_error = stat.reason;
                process_state_.private_order_feed_component_state.subscribed_universe_version = 0;
            }else if constexpr(std::is_same_v<T, PrivateOrderFeedUniverseApplied>){
                process_state_.private_order_feed_component_state.status = ComponentStatus::kREADY;
                process_state_.private_order_feed_component_state.installed_universe_version = stat.version;
                process_state_.private_order_feed_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, PrivateOrderFeedSubscriptionReady>){
                process_state_.private_order_feed_component_state.status = ComponentStatus::kLIVE;
                process_state_.private_order_feed_component_state.subscribed_universe_version = stat.version;
                process_state_.private_order_feed_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, PrivateOrderFeedFaulted>){
                process_state_.private_order_feed_component_state.status = ComponentStatus::kFAULTED;
                process_state_.private_order_feed_component_state.connected = false;
                process_state_.private_order_feed_component_state.last_error = stat.error_message;
            }else if constexpr(std::is_same_v<T, PrivateOrderFeedTelemetry>){
                process_state_.private_order_feed_component_state.telemetry = stat.telemetry;
            }
        }, status);
    }

    bool ControlPlane::process_one_private_order_feed_status() noexcept{
        if(private_order_feed_queues_.private_order_feed_to_control_status_queue == nullptr){
            return false;
        }

        PrivateOrderFeedToControlStatus status{};
        if(private_order_feed_queues_.private_order_feed_to_control_status_queue->try_pop(status)){
            apply_private_order_feed_status(status);
            recompute_process_state();
            return true;
        }
        return false;
    }

    bool ControlPlane::process_private_order_feed_status() noexcept{
        bool processed_any = false;
        while(process_one_private_order_feed_status()){
            processed_any = true;
        }
        return processed_any;
    }

    void ControlPlane::apply_order_rest_status(const OrderRestToControlStatus& status) noexcept{
        std::visit([&](auto&& stat){
            using T = std::decay_t<decltype(stat)>;
            if constexpr(std::is_same_v<T, OrderRestReady>){
                process_state_.order_rest_component_state.status = ComponentStatus::kREADY;
                process_state_.order_rest_component_state.enabled = true;
                process_state_.order_rest_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, OrderRestDisabled>){
                process_state_.order_rest_component_state.status = ComponentStatus::kSTOPPED;
                process_state_.order_rest_component_state.enabled = false;
            }else if constexpr(std::is_same_v<T, OrderRestUniverseApplied>){
                process_state_.order_rest_component_state.status = ComponentStatus::kREADY;
                process_state_.order_rest_component_state.installed_universe_version = stat.version;
                process_state_.order_rest_component_state.last_error.clear();
            }else if constexpr(std::is_same_v<T, OrderRestFaulted>){
                process_state_.order_rest_component_state.status = ComponentStatus::kFAULTED;
                process_state_.order_rest_component_state.enabled = false;
                process_state_.order_rest_component_state.last_error = stat.error_message;
            }else if constexpr(std::is_same_v<T, OrderRestTelemetry>){
                process_state_.order_rest_component_state.telemetry = stat.telemetry;
            }
        }, status);
    }

    bool ControlPlane::process_one_order_rest_status() noexcept{
        if(order_rest_queues_.order_rest_to_control_status_queue == nullptr){
            return false;
        }

        OrderRestToControlStatus status{};
        if(order_rest_queues_.order_rest_to_control_status_queue->try_pop(status)){
            apply_order_rest_status(status);
            recompute_process_state();
            return true;
        }
        return false;
    }

    bool ControlPlane::process_order_rest_status() noexcept{
        bool processed_any = false;
        while(process_one_order_rest_status()){
            processed_any = true;
        }
        return processed_any;
    }

    RecoveryPumpResult ControlPlane::process_recovery(RecoveryCoordinator::TimePoint now) noexcept{
        RecoveryPumpResult result{};
        constexpr std::size_t kMaxCommandsPerPump = 64;

        if(recovery_orchestration_faulted_){
            result.code = RecoveryPumpCode::kCOORDINATOR_COMMIT_FAILED;
            return result;
        }
        if(active_universe_ == nullptr){
            return result;
        }

        const auto timeouts = recovery_coordinator_.expire_timeouts(now);
        process_state_.recovery_telemetry.request_ack_timeouts +=
            timeouts.request_ack_timeouts;
        process_state_.recovery_telemetry.snapshot_timeouts +=
            timeouts.snapshot_timeouts;
        process_state_.recovery_telemetry.retries_scheduled +=
            timeouts.retries_scheduled;
        process_state_.recovery_telemetry.markets_failed +=
            timeouts.markets_failed;
        process_state_.recovery_telemetry.active_incidents =
            recovery_coordinator_.active_incident_count();
        process_state_.recovery_telemetry.active_markets =
            recovery_coordinator_.active_market_count();
        if(!process_state_.io_component_state.connected){return result;}
        if(process_state_.io_component_state.status != ComponentStatus::kLIVE){return result;}
        if(process_state_.io_component_state.installed_universe_version != active_universe_->version){return result;}
        if(process_state_.io_component_state.subscribed_universe_version != active_universe_->version){return result;}

        const auto commands = recovery_coordinator_.next_pending_commands(
            now,
            kMaxCommandsPerPump);
        for(const auto& command : commands){
            if(!push_io_command(ControlToIoCommand{command})){
                ++result.commands_pushed_failure;
                ++process_state_.recovery_telemetry.request_enqueue_failures;
                result.code = RecoveryPumpCode::kIO_BACKPRESSURE;
                break;
            }
            if(!recovery_coordinator_.mark_command_enqueued(command, now)){
                result.code = RecoveryPumpCode::kCOORDINATOR_COMMIT_FAILED;
                recovery_orchestration_faulted_ = true;
                recompute_process_state();
                break;
            }
            ++result.commands_pushed_success;
            ++process_state_.recovery_telemetry.requests_enqueued;
        }
        return result;
    }


}
