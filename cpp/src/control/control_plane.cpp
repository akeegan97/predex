#include "predex/control/control_plane.hpp"


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

}