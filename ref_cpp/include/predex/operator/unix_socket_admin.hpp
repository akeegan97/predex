#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace predex::operator_admin {

struct UnixSocketAdminConfig {
    bool enabled{true};
    std::string socket_path{"/tmp/predex.sock"};
    int listen_backlog{8};
    std::uint32_t accept_timeout_ms{100};
    std::size_t max_request_bytes{1024};
};

enum class AdminWireCommandType : std::uint8_t {
    kStatus,
    kShutdown,
    kUnknown,
};

struct AdminWireCommand {
    AdminWireCommandType type{AdminWireCommandType::kUnknown};
    std::string raw_line;
};

struct AdminWireResponse {
    bool ok{false};
    std::string body;
};

using AdminRequestHandler = std::function<AdminWireResponse(const AdminWireCommand&)>;

[[nodiscard]] std::optional<AdminWireCommand> parse_admin_wire_command(std::string_view line);
[[nodiscard]] std::string format_admin_wire_response(const AdminWireResponse& response);

class UnixSocketAdminServer {
  public:
    explicit UnixSocketAdminServer(UnixSocketAdminConfig config);
    ~UnixSocketAdminServer();

    UnixSocketAdminServer(const UnixSocketAdminServer&) = delete;
    UnixSocketAdminServer& operator=(const UnixSocketAdminServer&) = delete;
    UnixSocketAdminServer(UnixSocketAdminServer&&) = delete;
    UnixSocketAdminServer& operator=(UnixSocketAdminServer&&) = delete;

    [[nodiscard]] bool start(std::string& error_out);
    void stop() noexcept;
    [[nodiscard]] bool is_running() const noexcept;
    [[nodiscard]] const UnixSocketAdminConfig& config() const noexcept;

    bool serve_one(const std::stop_token& stop_token,
                   const AdminRequestHandler& handler,
                   std::string& error_out);

  private:
    UnixSocketAdminConfig config_;
    int listen_fd_{-1};
};

} // namespace predex::operator_admin
