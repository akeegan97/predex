#pragma once 

#include <string>

namespace predex::socket {
    struct CommandResponse {
        bool ok{false};
        std::string body;
    };

    class ICommandHandler{
        public:
            virtual ~ICommandHandler() = default;
            virtual CommandResponse handle_command(const std::string& command_line) = 0;
    };

}