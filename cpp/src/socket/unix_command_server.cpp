#include <chrono>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <array>
#include <filesystem>
#include <limits>
#include <algorithm>
#include <iterator>
#include "predex/socket/unix_command_server.hpp"
#include "predex/socket/unique_fd.hpp"
#include "poll.h"
#include "predex/socket/socket_path_guard.hpp"


namespace predex::socket{
    UnixCommandServer::UnixCommandServer(OperatorSocketConfig config, ICommandHandler* command_handler):
        config_(std::move(config)),
        command_handler_(command_handler){}
    
//NOLINTNEXTLINE 
    void UnixCommandServer::run(const std::stop_token& stop_token, std::string& error_out, ServerReadyCallback on_ready){
        if(command_handler_ == nullptr){
            error_out = "command_handler is null";
            return;
        }

        if(config_.client_io_timeout_ms >
           static_cast<std::uint32_t>(std::numeric_limits<int>::max())){
            error_out = "client I/O timeout exceeds poll timeout range";
            return;
        }

        std::filesystem::path lock_path = config_.socket_path;
        lock_path += ".lock";

        UniqueFD lock_fd(::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600));//NOLINT

        SocketPathGuard socket_guard(config_.socket_path);

        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;

        if(config_.socket_path.size() >= sizeof(addr.sun_path)){
            error_out = "socket path is too long";
            return;
        }
        
        if(!lock_fd.is_valid()){
            error_out = "failed to open lock file: " + std::string(std::strerror(errno));
            return;
        }

        if(::flock(lock_fd.get(), LOCK_EX | LOCK_NB) < 0){
            if(errno == EWOULDBLOCK){
                error_out = "another instance is already running";
                return;
            }
            error_out = "failed to acquire lock on lock file: " + std::string(std::strerror(errno));
            return;
        }

        struct stat stat_t{};

        if(::lstat(config_.socket_path.c_str(), &stat_t) == 0){
            if(!S_ISSOCK(stat_t.st_mode)){
                error_out = "socket path exists and is not a socket: " + config_.socket_path;
                return;
            }
            if(stat_t.st_uid != ::geteuid()){
                error_out = "socket path exists and is not owned by the current user: " + config_.socket_path;
                return;
            }
            if(::unlink(config_.socket_path.c_str()) < 0){
                error_out = "failed to remove existing socket file: " + std::string(std::strerror(errno));
                return;
            }
        }else{
            if(errno != ENOENT){
                error_out = "failed to stat socket path: " + std::string(std::strerror(errno));
                return;
            }
        }

        UniqueFD listen_fd(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
        if(!listen_fd.is_valid()){
            error_out = "failed to create socket: " + std::string(std::strerror(errno));
            return;
        }

        ::memcpy(addr.sun_path, config_.socket_path.c_str(), config_.socket_path.size());

        if (::bind(
                listen_fd.get(),
                reinterpret_cast<sockaddr*>(&addr),
                sizeof(addr)
            ) < 0) {
            error_out = "failed to bind socket: " +
                        std::string{std::strerror(errno)};
            return;
        }

        socket_guard.mark_owned();

        if (::chmod(config_.socket_path.c_str(), 0600) < 0) { //NOLINT
            error_out = "failed to set socket permissions: " + std::string(std::strerror(errno));
            return;
        }

        if(::listen(listen_fd.get(), config_.listen_backlog) < 0){
            error_out = "failed to listen on socket: " + std::string(std::strerror(errno));
            return;
        }

        UniqueFD stop_fd(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
        if(!stop_fd.is_valid()){
            error_out = "failed to create eventfd: " + std::string(std::strerror(errno));
            return;
        }

        std::stop_callback stop_callback{
            stop_token, [&stop_fd]{
                const std::uint64_t value = 1;
                for(;;){
                    const ssize_t ret = ::write(stop_fd.get(), &value, sizeof(value));
                    if(ret == static_cast<ssize_t>(sizeof(value))){
                        return;
                    }
                    if(ret < 0 && errno == EINTR){
                        continue;
                    }
                    if(ret < 0 && errno == EAGAIN){
                        return;
                    }
                    return;
                }
            }
        };

        if(on_ready){
            on_ready();
        }

        for ( ; ; ) {

            std::array<pollfd, 2> fds{{
                {
                    .fd = listen_fd.get(),
                    .events = POLLIN,
                    .revents = 0
                },
                {
                    .fd = stop_fd.get(),
                    .events = POLLIN,
                    .revents = 0
                }
            }};


            int ret = ::poll(fds.data(), static_cast<nfds_t>(fds.size()), -1);


            if(ret < 0){
                if(errno == EINTR){
                    if(stop_token.stop_requested()){
                        break;
                    }
                    continue; 
                }
                error_out = "poll failed: " + std::string(std::strerror(errno));
                break;
            }

            if((fds[1].revents & POLLIN) != 0 || stop_token.stop_requested()){
                return;
            }

            if ((fds[1].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                error_out = "stop eventfd poll failure";
                return;
            }

            if ((fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                error_out = "listening socket poll failure";
                return;
            }
            

            if((fds[0].revents & POLLIN) !=0){
                //we have a new client connection:
                UniqueFD client_fd(::accept4(listen_fd.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC));


                bool request_complete = false;
                bool abandon_client = false;


                std::string request;
                
                if(!client_fd.is_valid()){
                    const int accept_error = errno;
                    if(accept_error == EINTR){
                        if(stop_token.stop_requested()){
                            return;
                        }
                        continue;
                    }
                    if(accept_error == EAGAIN ||
                       accept_error == EWOULDBLOCK ||
                       accept_error == ECONNABORTED){
                        continue;
                    }
                    error_out = "failed to accept client connection: " +
                                std::string(std::strerror(accept_error));
                    return;
                }

                const auto read_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_io_timeout_ms);

                while(!request_complete && !abandon_client){
                    const auto now = std::chrono::steady_clock::now();
                    if(now >= read_deadline){
                        abandon_client = true;
                        break;
                    }

                    const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(read_deadline - now);

                    const int timeout_ms = static_cast<int>(remaining.count());

                    std::array<pollfd, 2> inner_fds{{
                        {
                            .fd = client_fd.get(),
                            .events = POLLIN ,
                            .revents = 0
                        },
                        {
                            .fd = stop_fd.get(),
                            .events = POLLIN,
                            .revents = 0
                        }
                    }};

                    const int inner_ret = ::poll(inner_fds.data(), static_cast<nfds_t>(inner_fds.size()), timeout_ms);

                    if(inner_ret == 0){
                        abandon_client = true;
                        break;
                    }

                    if(inner_ret < 0){
                        if(errno == EINTR){
                            if(stop_token.stop_requested()){
                                abandon_client = true;
                                return;
                            }
                            continue; 
                        }
                        const int poll_error = errno;
                        error_out = "client read poll failed: " +
                                    std::string(std::strerror(poll_error));
                        return;
                    }

                    if((inner_fds[1].revents & POLLIN) != 0 || stop_token.stop_requested()){
                        return;
                    }

                    if((inner_fds[0].revents & POLLNVAL) != 0){
                        abandon_client = true;
                        break;
                    }

                    if((inner_fds[0].revents & (POLLIN | POLLHUP | POLLERR)) != 0){
                        
                        std::array<char, kDEFAULT_MAX_REQUEST_BYTES> buffer{};

                        for(;;){
                            const ssize_t bytes_read = ::read(client_fd.get(), buffer.data(), buffer.size());
                            if (bytes_read > 0) {
                                const auto count = static_cast<std::size_t>(bytes_read);

                                auto* const begin = buffer.begin();
                                auto* const end = begin + static_cast<std::ptrdiff_t>(count);
                                auto* const newline = std::find(begin, end, '\n');

                                const auto payload_count = static_cast<std::size_t>(
                                    std::distance(begin, newline));

                                if (request.size() + payload_count >
                                    config_.max_request_bytes) {
                                    abandon_client = true;
                                    break;
                                }

                                request.append(buffer.data(), payload_count);

                                if (newline != end) {
                                    request_complete = true;
                                    break;
                                }

                                continue;
                            }
                            if(bytes_read == 0){
                                abandon_client = true;
                                break;
                            }
                            if(errno == EINTR){
                                continue;
                            }
                            if(errno == EAGAIN || errno == EWOULDBLOCK){
                                break;
                            }
                            abandon_client = true;
                            break;
                        }

                    }
                }
                if(stop_token.stop_requested()){
                    return;
                }
                if(abandon_client || !request_complete){
                    continue;
                }
                CommandResponse response = command_handler_->handle_command(request);

                if (response.body.empty() || response.body.back() != '\n') {
                    response.body.push_back('\n');
                }

                std::array<struct pollfd, 2> write_fds{{
                    {
                        .fd = client_fd.get(),
                        .events = POLLOUT,
                        .revents = 0
                    },
                    {
                        .fd = stop_fd.get(),
                        .events = POLLIN,
                        .revents = 0
                    }
                }};
                
                const auto write_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_io_timeout_ms);

                std::size_t bytes_written_total = 0;

                while(bytes_written_total < response.body.size() && !abandon_client){
                    const auto now = std::chrono::steady_clock::now();

                    if(now >= write_deadline){
                        abandon_client = true;
                        break;
                    }
                    const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(write_deadline - now);
                    const int write_timeout_ms = static_cast<int>(remaining.count());

                    write_fds[0].revents = 0;
                    write_fds[1].revents = 0;

                    const int write_ret = ::poll(write_fds.data(), static_cast<nfds_t>(write_fds.size()), write_timeout_ms);

                    if(write_ret == 0){
                        abandon_client = true;
                        break;
                    }

                    if(write_ret < 0){
                        if(errno == EINTR){
                            if(stop_token.stop_requested()){
                                return;
                            }
                            continue;
                        }
                        const int poll_error = errno;
                        error_out = "client write poll failed: " +
                                    std::string(std::strerror(poll_error));
                        return;
                    }

                    if((write_fds[1].revents & POLLIN)!= 0 || stop_token.stop_requested()){
                        return;
                    }
                    if((write_fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) !=0){
                        abandon_client = true;
                        break;
                    }
                    if((write_fds[0].revents & POLLOUT) == 0){
                        continue;
                    }

                    const ssize_t bytes_written = ::send(client_fd.get(), response.body.data() + bytes_written_total, response.body.size() - bytes_written_total, MSG_NOSIGNAL);
                    if(bytes_written > 0){
                        bytes_written_total += static_cast<std::size_t>(bytes_written);
                        continue;
                    }
                    if(bytes_written < 0 && errno == EINTR){
                        continue;
                    }
                    if(bytes_written <0 && (errno == EAGAIN || errno == EWOULDBLOCK)){
                        continue;
                    }
                    abandon_client = true;
                    break;
                }
                
            }
        }
    }



   

}
