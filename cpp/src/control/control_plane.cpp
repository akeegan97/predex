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
                process_state_.lifecycle = LifecyclePhase::kWAITING_FOR_IO;
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
}
