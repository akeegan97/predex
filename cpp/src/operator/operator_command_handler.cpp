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

    std::string response_type_to_string(predex::operator_admin::OperatorResponseType type){
        switch(type){
            case predex::operator_admin::OperatorResponseType::kACK:
                return "ack";
            case predex::operator_admin::OperatorResponseType::kERROR:
                return "error";
            case predex::operator_admin::OperatorResponseType::kSTATUS:
                return "status";
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
                    {"trading_enabled", payload.trading_enabled},
                    {"shutdown_requested", payload.shutdown_requested},
                };
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
