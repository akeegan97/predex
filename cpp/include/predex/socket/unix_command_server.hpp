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

    enum class WaitResult : std::uint8_t{
        kREADY = 0,
        kSTOPPED = 1,
        kERROR = 2,
    };

    enum class ReadRequestStatus : std::uint8_t{
        kOK = 0,
        kCLOSED = 1,
        kTOO_LARGE = 2,
        kREAD_ERROR = 3,
    };

    struct ReadRequestResult{
        ReadRequestStatus status{ReadRequestStatus::kOK};
        std::string request;
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

            bool open_listen_socket(std::string& error_out);
            void close_listen_socket() noexcept;
            WaitResult wait_for_client(const std::stop_token& stop_token, std::string& error_out) const;
            void handle_one_client(int client_fd);
            [[nodiscard]] ReadRequestResult read_request(int client_fd) const;
            bool write_response(int client_fd, std::string_view response);
    };


} 