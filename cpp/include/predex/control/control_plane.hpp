#pragma once 

#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <array>
#include <string>
#include <cstdint>

#include "predex/utils/spsc_queue.hpp"
#include "predex/control/control_types.hpp"


namespace predex::core::control::kalshi{
/*
    ControlPlane is respsonsible for:
     - managing the overall lifecycle of predex 
        - initialization and shutdown of components
        - operator control (e.g. start/stop/refresh/reconfigure/etc)
        - orchastrating desync recovery, crash recovery, and other cross-thread coordination
*/
    struct IoQueues{
        utils::SPSCQueue<ControlIoCommand>& control_io_queue;
        utils::SPSCQueue<IoControlStatus>& io_control_status;
    };


    struct MarketTickerMap{
        std::uint64_t version{0};

        std::unordered_map<internal::MarketId, std::string> id_to_ticker;
        std::unordered_map<std::string, internal::MarketId> ticker_to_id;
    };



     

    class ControlPlane{
        public: 
            ControlPlane(IoQueues queues): control_io_queue_(queues.control_io_queue), io_control_status_queue_(queues.io_control_status){
                process_state_ = ProcessState{ProcessStatus::kBooting};
            }

            [[nodiscard]] ProcessState process_state()const{
                return process_state_;
            }

            void set_process_state(ProcessState new_state){
                process_state_ = new_state;
            }

            std::size_t pump_io_status(){
                // Interpret raw IO status in the context of the control-plane's desired IO state.
                IoControlStatus status{};
                std::size_t processed = 0;
                while(io_control_status_queue_.try_pop(status)){
                    switch(status.type){
                        case IoControlStatusType::kConnected:
                            io_state_.current_state = IoControlStateType::kConnected;
                            if(io_state_.target_state == IoControlStateType::kConnected){
                                io_state_.transition_in_flight = false;
                            }
                            if(process_state_.status == ProcessStatus::kBooting ||
                               process_state_.status == ProcessStatus::kWaitingForIo){
                                set_process_state(ProcessState{ProcessStatus::kIoConnected});
                            }
                            break;
                        case IoControlStatusType::kDisconnected:
                            io_state_.current_state = IoControlStateType::kDisconnected;
                            if(io_state_.target_state == IoControlStateType::kDisconnected){
                                io_state_.transition_in_flight = false;
                                if(process_state_.status == ProcessStatus::kShuttingDown){
                                    set_process_state(ProcessState{ProcessStatus::kStopped});
                                } else if(process_state_.status == ProcessStatus::kBooting ||
                                          process_state_.status == ProcessStatus::kWaitingForIo){
                                    set_process_state(ProcessState{ProcessStatus::kWaitingForIo});
                                }
                                break;
                            }

                            io_state_.transition_in_flight = false;
                            if(process_state_.status == ProcessStatus::kBooting ||
                               process_state_.status == ProcessStatus::kWaitingForIo ||
                               process_state_.status == ProcessStatus::kIoConnected ||
                               process_state_.status == ProcessStatus::kReady ||
                               process_state_.status == ProcessStatus::kLive){
                                set_process_state(ProcessState{ProcessStatus::kWaitingForIo});
                            }
                            break;
                    }
                    ++processed;
                }
                return processed;
            }

            bool send_io_command(ControlIoCommand command){
                if(control_io_queue_.try_push(command)){
                    io_state_.last_cmd_sent = command.type;
                    io_state_.transition_in_flight = true;
                    switch(command.type){
                        case ControlIoCommandType::kDisconnect:
                            io_state_.target_state = IoControlStateType::kDisconnected;
                            break;
                        case ControlIoCommandType::kReconnect:
                        case ControlIoCommandType::kRecoverMarket://no state difference for kRecoverMarket 
                            break;
                        case ControlIoCommandType::kRefresh:
                            io_state_.target_state = IoControlStateType::kConnected;
                            io_state_.current_state = IoControlStateType::kReconnecting;
                            break;
                    }
                    return true;
                }
                return false;
            }

            [[nodiscard]] std::uint64_t update_market_ticker_map(MarketTickerMap new_map){
                new_map.version = market_ticker_map_.version + 1;
                market_ticker_map_ = std::move(new_map);
                return market_ticker_map_.version;
            }  

            [[nodiscard]] UniverseSnapshot make_universe_snapshot() const{
                UniverseSnapshot snapshot;
                snapshot.version = market_ticker_map_.version;
                snapshot.markets.reserve(market_ticker_map_.id_to_ticker.size());
                for(const auto& [id, ticker]: market_ticker_map_.id_to_ticker){
                    snapshot.markets.push_back(UniverseMarket{
                        .id = id,
                        .kalshi_ticker = ticker
                    });
                }
                return snapshot;
            }


        private:
            ProcessState process_state_;
            IoControlState io_state_;

            utils::SPSCQueue<ControlIoCommand>& control_io_queue_;
            utils::SPSCQueue<IoControlStatus>& io_control_status_queue_;

            MarketTickerMap market_ticker_map_;

    };



}
