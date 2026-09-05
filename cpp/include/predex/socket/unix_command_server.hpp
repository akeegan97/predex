#pragma once 
#include <functional>
#include <cstddef>
#include <cstdint>
#include <stop_token>
#include <string>

#include "predex/socket/command_handler.hpp"

namespace predex::socket{

    inline constexpr int kDEFAULT_LISTEN_BACKLOG = 8;
    inline constexpr std::uint32_t kDEFAULT_CLIENT_IO_TIMEOUT_MS = 100;
    inline constexpr std::size_t kDEFAULT_MAX_REQUEST_BYTES = 1024;

    struct OperatorSocketConfig{
        bool enabled{true};
        std::string socket_path{"/tmp/predex.sock"};
        int listen_backlog{kDEFAULT_LISTEN_BACKLOG};
        std::uint32_t client_io_timeout_ms{kDEFAULT_CLIENT_IO_TIMEOUT_MS};
        std::size_t max_request_bytes{kDEFAULT_MAX_REQUEST_BYTES};
        //maximum payload size excluding the newline character
    };

    using ServerReadyCallback = std::function<void()>;

    class UnixCommandServer{
        public:
            explicit UnixCommandServer(OperatorSocketConfig config,
                                      ICommandHandler* command_handler);
            ~UnixCommandServer() = default;

            UnixCommandServer(const UnixCommandServer&) = delete;
            UnixCommandServer& operator=(const UnixCommandServer&) = delete;
            UnixCommandServer(UnixCommandServer&&) = delete;
            UnixCommandServer& operator=(UnixCommandServer&&) = delete;

            void run(const std::stop_token& stop_token, std::string& error_out, ServerReadyCallback on_ready);

        private:
            ICommandHandler* command_handler_{nullptr};
            OperatorSocketConfig config_;
    };


} 