#include "predex/control/control_plane.hpp"
#include "predex/control/control_types.hpp"

#include <algorithm>
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
            };
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

    }

    ControlPlane::ControlPlane(
        OperatorQueues queues,
        ControlIoQueues io_queues,
        RouterQueue router_queue,
        ControlShardQueues shard_queues
    ) : queues_(queues), io_queues_(io_queues), router_queue_(router_queue), shard_queues_(std::move(shard_queues)){
        process_state_.shard_component_states.resize(shard_queues_.control_to_shard_queues.size());
    }

    OperatorPumpResult ControlPlane::process_operator_commands(){
        OperatorPumpResult result{};
        operator_admin::OperatorCommand item{};
        while(queues_.operator_command_queue.try_pop(item)){
            result.commands_processed++;
            switch(item.type){
                case operator_admin::OperatorCommandType::kSTATUS:{                  
                    if(push_operator_response(operator_admin::OperatorResponse{
                        .request_id = item.request_id,
                        .type = operator_admin::OperatorResponseType::kSTATUS,
                        .payload = operator_admin::OperatorStatusSnapshot{
                            .lifecycle = process_state_.lifecycle,
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
                case operator_admin::OperatorCommandType::kUNKNOWN:{
                     if(push_operator_response(operator_admin::OperatorResponse{
                        .request_id = item.request_id,
                        .type = operator_admin::OperatorResponseType::kERROR,
                        .payload = operator_admin::ErrorPayload{
                            .message = "unknown command",
                        }
                    })){
                        result.responses_pushed_success++;
                    }else{
                        result.responses_pushed_failure++;
                    }
                     break;
                }
                case operator_admin::OperatorCommandType::kSHUTDOWN_GRACEFUL:{
                    process_state_.lifecycle = LifecyclePhase::kSHUTTING_DOWN;
                    process_state_.shutdown_requested = true;
                    process_state_.trading_enabled = false;

                    bool success = push_operator_response(operator_admin::OperatorResponse{
                        .request_id = item.request_id,
                        .type = operator_admin::OperatorResponseType::kACK,
                        .payload = operator_admin::AckPayload{},
                    });

                    if(success){
                        result.responses_pushed_success++;
                    }else{
                        //handle failed push
                        result.responses_pushed_failure++;
                    }
                    break;
                }
                case operator_admin::OperatorCommandType::kSHUTDOWN_FORCEFUL:{
                    //initiate forceful shutdown sequence
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
            }
        }, status);
    }

    void ControlPlane::recompute_process_state() noexcept{
        if(process_state_.shutdown_requested){
            if(process_state_.io_component_state.status == ComponentStatus::kSTOPPED){
                process_state_.lifecycle = LifecyclePhase::kSTOPPED;
            }else{
                process_state_.lifecycle = LifecyclePhase::kSHUTTING_DOWN;
            }
            return;
        }

        if(process_state_.io_component_state.status == ComponentStatus::kFAULTED ||
           process_state_.router_component_state.status == ComponentStatus::kFAULTED ||
           std::any_of(
                process_state_.shard_component_states.begin(),
                process_state_.shard_component_states.end(),
                [](const ShardComponentState& shard_state){
                    return shard_state.status == ComponentStatus::kFAULTED;
                }
            )){
            process_state_.lifecycle = LifecyclePhase::kFAULTED;
            process_state_.trading_enabled = false;
            return;
        }

        switch(process_state_.io_component_state.status){
            case ComponentStatus::kUNKNOWN:
            case ComponentStatus::kSTOPPED:
            case ComponentStatus::kSTARTING:
            case ComponentStatus::kQUIESCING:
                process_state_.lifecycle = LifecyclePhase::kWAITING_FOR_IO;
                break;
            case ComponentStatus::kINSTALLING_UNIVERSE:
            case ComponentStatus::kREADY:
                process_state_.lifecycle = LifecyclePhase::kIO_CONNECTED;
                break;
            case ComponentStatus::kLIVE:
                process_state_.lifecycle = LifecyclePhase::kREADY;
                break;
            case ComponentStatus::kFAULTED:
                process_state_.lifecycle = LifecyclePhase::kFAULTED;
                break;
        }
    }

    std::uint64_t ControlPlane::install_universe(UniverseSnapshot snapshot){
        snapshot.version = next_universe_version_++;
        active_universe_ = std::make_shared<const UniverseSnapshot>(std::move(snapshot));
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

}
