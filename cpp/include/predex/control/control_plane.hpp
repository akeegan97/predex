#pragma once 

#include <cstddef>

#include "predex/operator/operator_commands.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/control/control_types.hpp"


namespace predex::core::control{

    struct OperatorQueues{
        utils::SPSCQueue<::predex::operator_admin::OperatorCommand>& operator_command_queue;
        utils::SPSCQueue<::predex::operator_admin::OperatorResponse>& operator_response_queue;
    };

    struct OperatorPumpResult{
        std::size_t commands_processed{0};
        std::size_t responses_pushed_success{0};
        std::size_t responses_pushed_failure{0};
    };

    class ControlPlane{
        public: 
            ControlPlane(OperatorQueues queues)
                : queues_(queues) {}
        
            [[nodiscard]] OperatorPumpResult process_operator_commands();

            bool push_operator_response(const ::predex::operator_admin::OperatorResponse& response){
                return queues_.operator_response_queue.try_push(response);
            }

            [[nodiscard]] ProcessState process_state() const {
                return process_state_;
            }

        private:
            OperatorQueues queues_;
            ProcessState process_state_{LifecyclePhase::kBOOTING};
    };

}
