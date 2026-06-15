#pragma once 

#include "predex/socket/command_handler.hpp"
#include "predex/utils/spsc.hpp"

namespace predex::operator_admin{

    struct OperatorQueues{
        
    };

    class OperatorCommandHandler : public ICommandHandler{
        public:
            OperatorCommandHandler();
            CommandResponse handle_command(const std::string& command_line) override;
    };
}