
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <array>
#include "predex/socket/unix_command_server.hpp"



namespace predex::socket{
    UnixCommandServer::UnixCommandServer(OperatorSocketConfig config, ICommandHandler* command_handler):
        config_(std::move(config)),
        command_handler_(command_handler){}
    
    UnixCommandServer::~UnixCommandServer(){
        stop();
    }

    void UnixCommandServer::run(const std::stop_token& stop_token, std::string& error_out){
        if(command_handler_ == nullptr){
            error_out = "command handler is null";
            return;
        }
        if(!open_listen_socket(error_out)){
            return;
        }

        while(true){
            const WaitResult wait_result = wait_for_client(stop_token, error_out);
            if(wait_result == WaitResult::kERROR){
                break;
            }
            if(wait_result == WaitResult::kSTOPPED){
                break;
            }
            if(wait_result == WaitResult::kREADY){
                int client_fd = ::accept(listen_fd_, nullptr, nullptr);
                if(client_fd < 0){
                    error_out = "failed to accept client connection: " + std::string(std::strerror(errno));
                    break;
                }
                handle_one_client(client_fd);
                ::close(client_fd);
            }
        }

        stop();
    }

    void UnixCommandServer::stop() noexcept{
        close_listen_socket();
        ::unlink(config_.socket_path.c_str());
    }

    OperatorSocketConfig const& UnixCommandServer::config() const noexcept{
        return config_;
    }

    bool UnixCommandServer::is_running() const noexcept{
        return listen_fd_ != -1;
    }

    bool UnixCommandServer::open_listen_socket(std::string& error_out){
        if(config_.socket_path.empty()){
            error_out = "socket path is empty";
            return false;
        }
        listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if(listen_fd_ < 0){
            error_out = "failed to create socket: " + std::string(std::strerror(errno));
            return false;
        }

        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;

        if(config_.socket_path.size() >= sizeof(addr.sun_path)){
            error_out = "socket path is too long";
            ::close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        std::strncpy(addr.sun_path, config_.socket_path.c_str(), sizeof(addr.sun_path) - 1);

        ::unlink(config_.socket_path.c_str());

        if(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0){
            error_out = "failed to bind socket: " + std::string(std::strerror(errno));
            ::close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        if(::listen(listen_fd_, config_.listen_backlog) < 0){
            error_out = "failed to listen on socket: " + std::string(std::strerror(errno));
            ::close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }
        return true;

    }

    void UnixCommandServer::close_listen_socket() noexcept{
        if(listen_fd_ != -1){
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    WaitResult UnixCommandServer::wait_for_client(const std::stop_token& stop_token, std::string& error_out) const{
        while(!stop_token.stop_requested()){
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(listen_fd_, &read_fds);

            timeval timeout{};
        
            timeout.tv_sec = config_.accept_timeout_ms / 1000; //NOLINT
            timeout.tv_usec = (config_.accept_timeout_ms % 1000) * 1000; //NOLINT

            int ret = ::select(listen_fd_ + 1, &read_fds, nullptr, nullptr, &timeout);
            
            if(ret < 0){
                if(errno == EINTR){
                    continue; 
                }
                /*
                errno is EBADF or stop_token is requested or listen_fd_ is -1, 
                means shutdown has been requested or its in the process of shutting down, return kSTOPPED 
                */
                if(errno == EBADF || stop_token.stop_requested() || listen_fd_ == -1){
                    return WaitResult::kSTOPPED; 
                }
                
                error_out = "select failed: " + std::string(std::strerror(errno));
                
                return WaitResult::kERROR;
            }
            
            if(ret == 0){
                continue; 
            }

            if(FD_ISSET(listen_fd_, &read_fds)){
                return WaitResult::kREADY;
            }
            
        }
        return WaitResult::kSTOPPED; 
    }

    ReadRequestResult UnixCommandServer::read_request(int client_fd) const{
        ReadRequestResult result{};
        std::array<char, kDEFAULT_MAX_REQUEST_BYTES> buffer{0};

        while(true){
            const ssize_t received = ::recv(client_fd, buffer.data(), buffer.size(),0);
            if(received <=0){
                break;
            }
            if(result.request.size() + static_cast<size_t>(received) > config_.max_request_bytes){
                result.status = ReadRequestStatus::kTOO_LARGE;
                break;
            }
            result.request.append(buffer.data(), static_cast<size_t>(received));
            if(result.request.find('\n') != std::string::npos){
                break;
            }

        }
        return result;
    }

    bool UnixCommandServer::write_response(int client_fd, std::string_view response){
        size_t total_sent = 0;
        while(total_sent < response.size()){
            const ssize_t sent = ::send(client_fd, response.data() + total_sent, response.size() - total_sent, MSG_NOSIGNAL);
            if(sent < 0){
                if(errno == EINTR){
                    continue; 
                }
                return false;
            }
            total_sent += static_cast<size_t>(sent);
        }
        return true;
    }

    void UnixCommandServer::handle_one_client(int client_fd){
        const ReadRequestResult read_result = read_request(client_fd);
        if(read_result.status != ReadRequestStatus::kOK){
            const std::string error_message = (read_result.status == ReadRequestStatus::kTOO_LARGE) ? 
                "request too large\n" : "failed to read request\n";
            write_response(client_fd, error_message);
            return;
        }
        if(read_result.request.empty()){
            return;
        }

        const socket::CommandResponse response = command_handler_->handle_command(read_result.request);
        write_response(client_fd, response.body);
    }

}