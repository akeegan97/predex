#pragma once 

#include "predex/socket/command_handler.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/operator/operator_commands.hpp"

namespace predex::operator_admin{

    struct ControlQueues{
        utils::SPSCQueue<OperatorCommand>& server_to_control_queue; //produced by handler
        utils::SPSCQueue<OperatorResponse>& control_to_server_queue; //consumed by handler
    };

    class OperatorCommandHandler : public socket::ICommandHandler{
        public:
            OperatorCommandHandler(ControlQueues queues) 
            : queues_(queues) {}
            socket::CommandResponse handle_command(const std::string& command_line) override;

            

        private:
            ControlQueues queues_;
    };
}