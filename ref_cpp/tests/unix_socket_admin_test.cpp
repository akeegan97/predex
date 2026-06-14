#include "predex/operator/unix_socket_admin.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stop_token>
#include <string>
#include <thread>

namespace {

int fail(const char* message) {
    std::cerr << "unix_socket_admin_test: " << message << '\n';
    return 1;
}

std::string make_socket_path() {
    return "/tmp/predex-admin-test-" + std::to_string(::getpid()) + ".sock";
}

std::string send_request(const std::string& socket_path, const std::string& request) {
    const int client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd < 0) {
        return {};
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, socket_path.c_str(), sizeof(address.sun_path) - 1);

    if (::connect(client_fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        ::close(client_fd);
        return {};
    }

    if (::write(client_fd, request.data(), request.size()) < 0) {
        ::close(client_fd);
        return {};
    }

    char buffer[512];
    const auto bytes_read = ::read(client_fd, buffer, sizeof(buffer));
    ::close(client_fd);
    if (bytes_read <= 0) {
        return {};
    }
    return std::string{buffer, static_cast<std::size_t>(bytes_read)};
}

} // namespace

int main() {
    predex::operator_admin::UnixSocketAdminServer server{
        predex::operator_admin::UnixSocketAdminConfig{
            .enabled = true,
            .socket_path = make_socket_path(),
            .listen_backlog = 4,
            .accept_timeout_ms = 25,
            .max_request_bytes = 256,
        }};

    std::string error;
    if (!server.start(error)) {
        return fail(error.c_str());
    }

    std::jthread server_thread([&server](const std::stop_token& stop_token) {
        std::string loop_error;
        while (!stop_token.stop_requested()) {
            const bool ok = server.serve_one(
                stop_token,
                [](const predex::operator_admin::AdminWireCommand& request) {
                    switch (request.type) {
                    case predex::operator_admin::AdminWireCommandType::kStatus:
                        return predex::operator_admin::AdminWireResponse{
                            .ok = true,
                            .body = "request_id=1 command=status result=completed",
                        };
                    case predex::operator_admin::AdminWireCommandType::kShutdown:
                        return predex::operator_admin::AdminWireResponse{
                            .ok = true,
                            .body = "request_id=2 command=shutdown result=accepted",
                        };
                    case predex::operator_admin::AdminWireCommandType::kUnknown:
                        return predex::operator_admin::AdminWireResponse{
                            .ok = false,
                            .body = "unknown command",
                        };
                    }
                    return predex::operator_admin::AdminWireResponse{
                        .ok = false,
                        .body = "unknown command",
                    };
                },
                loop_error);
            if (!ok) {
                std::fprintf(stderr, "unix_socket_admin_test: server loop error: %s\n",
                             loop_error.c_str());
                return;
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds{25});

    const std::string status_response = send_request(server.config().socket_path, "status\n");
    if (status_response.find("ok request_id=1 command=status result=completed") != 0) {
        server.stop();
        return fail("unexpected status response");
    }

    const std::string shutdown_response = send_request(server.config().socket_path, "shutdown\n");
    if (shutdown_response.find("ok request_id=2 command=shutdown result=accepted") != 0) {
        server.stop();
        return fail("unexpected shutdown response");
    }

    server_thread.request_stop();
    server.stop();
    server_thread.join();
    return 0;
}
