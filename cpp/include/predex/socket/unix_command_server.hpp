#pragma once 

#include <cstddef>
#include <cstdint>
#include <stop_token>
#include <string>

#include "predex/socket/command_handler.hpp"



namespace predex::socket{

    inline constexpr int kDEFAULT_LISTEN_BACKLOG = 8;
    inline constexpr std::uint32_t kDEFAULT_ACCEPT_TIMEOUT_MS = 100;
    inline constexpr std::size_t kDEFAULT_MAX_REQUEST_BYTES = 1024;

    struct OperatorSocketConfig{
        bool enabled{true};
        std::string socket_path{"/tmp/predex.sock"};
        int listen_backlog{kDEFAULT_LISTEN_BACKLOG};
        std::uint32_t accept_timeout_ms{kDEFAULT_ACCEPT_TIMEOUT_MS};
        std::size_t max_request_bytes{kDEFAULT_MAX_REQUEST_BYTES};
    };

    class UnixCommandServer{
        public:
            explicit UnixCommandServer(OperatorSocketConfig config,
                                      ICommandHandler* command_handler);
            ~UnixCommandServer();

            UnixCommandServer(const UnixCommandServer&) = delete;
            UnixCommandServer& operator=(const UnixCommandServer&) = delete;
            UnixCommandServer(UnixCommandServer&&) = delete;
            UnixCommandServer& operator=(UnixCommandServer&&) = delete;

            void run(const std::stop_token& stop_token, std::string& error_out);
            void stop() noexcept;

            [[nodiscard]] bool is_running() const noexcept;
            [[nodiscard]] const OperatorSocketConfig& config() const noexcept;

        private:
            ICommandHandler* command_handler_{nullptr};
            OperatorSocketConfig config_;
            int listen_fd_{-1};
    };


}