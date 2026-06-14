#include "predex/operator/unix_socket_admin.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace predex::operator_admin {
namespace {

constexpr const char* kOkPrefix = "ok ";
constexpr const char* kErrorPrefix = "error ";

[[nodiscard]] std::string trim_ascii(std::string_view value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos) {
        return {};
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return std::string{value.substr(first, last - first + 1)};
}

[[nodiscard]] bool write_all(int fd, std::string_view data) {
    std::size_t written = 0;
    while (written < data.size()) {
        const auto result = ::write(fd, data.data() + written, data.size() - written);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        written += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool read_request_line(int fd,
                                     std::size_t max_request_bytes,
                                     std::string& line_out,
                                     std::string& error_out) {
    line_out.clear();
    constexpr std::size_t kChunkSize = 256;
    char buffer[kChunkSize];
    while (line_out.size() < max_request_bytes) {
        const auto bytes_read = ::read(fd, buffer, sizeof(buffer));
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }
            error_out = "read failed: " + std::string(std::strerror(errno));
            return false;
        }
        if (bytes_read == 0) {
            break;
        }

        line_out.append(buffer, static_cast<std::size_t>(bytes_read));
        const auto newline_pos = line_out.find('\n');
        if (newline_pos != std::string::npos) {
            line_out.resize(newline_pos);
            return true;
        }
    }

    if (line_out.size() >= max_request_bytes) {
        error_out = "request exceeds max_request_bytes";
        return false;
    }

    line_out = trim_ascii(line_out);
    return !line_out.empty();
}

} // namespace

std::optional<AdminWireCommand> parse_admin_wire_command(std::string_view line) {
    const std::string trimmed = trim_ascii(line);
    if (trimmed == "status") {
        return AdminWireCommand{
            .type = AdminWireCommandType::kStatus,
            .raw_line = trimmed,
        };
    }
    if (trimmed == "shutdown" || trimmed == "stop") {
        return AdminWireCommand{
            .type = AdminWireCommandType::kShutdown,
            .raw_line = trimmed,
        };
    }
    return std::nullopt;
}

std::string format_admin_wire_response(const AdminWireResponse& response) {
    std::string formatted = response.ok ? kOkPrefix : kErrorPrefix;
    formatted += response.body;
    formatted.push_back('\n');
    return formatted;
}

UnixSocketAdminServer::UnixSocketAdminServer(UnixSocketAdminConfig config)
    : config_(std::move(config)) {}

UnixSocketAdminServer::~UnixSocketAdminServer() {
    stop();
}

bool UnixSocketAdminServer::start(std::string& error_out) {
    if (listen_fd_ >= 0) {
        return true;
    }
    if (config_.socket_path.empty()) {
        error_out = "socket_path must not be empty";
        return false;
    }
    if (config_.socket_path.size() >= sizeof(sockaddr_un::sun_path)) {
        error_out = "socket_path exceeds sockaddr_un::sun_path capacity";
        return false;
    }

    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        error_out = "socket() failed: " + std::string(std::strerror(errno));
        return false;
    }

    ::unlink(config_.socket_path.c_str());

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, config_.socket_path.c_str(), sizeof(address.sun_path) - 1);

    if (::bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        error_out = "bind() failed: " + std::string(std::strerror(errno));
        ::close(fd);
        return false;
    }

    if (::listen(fd, config_.listen_backlog) != 0) {
        error_out = "listen() failed: " + std::string(std::strerror(errno));
        ::close(fd);
        ::unlink(config_.socket_path.c_str());
        return false;
    }

    listen_fd_ = fd;
    return true;
}

void UnixSocketAdminServer::stop() noexcept {
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (!config_.socket_path.empty()) {
        ::unlink(config_.socket_path.c_str());
    }
}

bool UnixSocketAdminServer::is_running() const noexcept {
    return listen_fd_ >= 0;
}

const UnixSocketAdminConfig& UnixSocketAdminServer::config() const noexcept {
    return config_;
}

bool UnixSocketAdminServer::serve_one(const std::stop_token& stop_token,
                                      const AdminRequestHandler& handler,
                                      std::string& error_out) {
    error_out.clear();
    if (listen_fd_ < 0) {
        error_out = "server is not running";
        return false;
    }

    pollfd poll_descriptor{
        .fd = listen_fd_,
        .events = POLLIN,
        .revents = 0,
    };
    const int poll_result =
        ::poll(&poll_descriptor, 1, static_cast<int>(config_.accept_timeout_ms));
    if (poll_result < 0) {
        if (errno == EINTR || stop_token.stop_requested()) {
            return true;
        }
        error_out = "poll() failed: " + std::string(std::strerror(errno));
        return false;
    }
    if (poll_result == 0 || (poll_descriptor.revents & POLLIN) == 0) {
        return true;
    }

    const int client_fd = ::accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) {
        if (errno == EINTR || stop_token.stop_requested()) {
            return true;
        }
        error_out = "accept() failed: " + std::string(std::strerror(errno));
        return false;
    }

    std::string line;
    std::string read_error;
    AdminWireResponse response{};
    if (!read_request_line(client_fd, config_.max_request_bytes, line, read_error)) {
        response.ok = false;
        response.body = read_error.empty() ? "empty request" : read_error;
    } else {
        const auto request = parse_admin_wire_command(line);
        if (!request.has_value()) {
            response.ok = false;
            response.body = "unknown command";
        } else {
            response = handler(*request);
        }
    }

    const std::string formatted_response = format_admin_wire_response(response);
    if (!write_all(client_fd, formatted_response)) {
        error_out = "write() failed: " + std::string(std::strerror(errno));
        ::close(client_fd);
        return false;
    }

    ::close(client_fd);
    return true;
}

} // namespace predex::operator_admin
