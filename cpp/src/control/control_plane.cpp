#include "predex/control/control_plane.hpp"
#include "predex/control/control_types.hpp"

#include <type_traits>


namespace predex::core::control{

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
           process_state_.router_component_state.status == ComponentStatus::kFAULTED){
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

}
