
#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string_view>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <nlohmann/json.hpp>

namespace predexctl{

namespace{

    constexpr std::uint32_t kBUFFERSIZE = 1024;
    constexpr std::uint32_t kCMD_ID_SHIFT = 16;
    constexpr std::uint64_t kCMD_ID_MASK = (1ULL << kCMD_ID_SHIFT) - 1;

    std::string make_cmd_id(){
        const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        std::uint64_t packed_id = (static_cast<std::uint64_t>(now) << kCMD_ID_SHIFT) | 
                                    (static_cast<std::uint64_t>(::getpid()) & kCMD_ID_MASK);
        
        return std::to_string(packed_id);
    }

    void usage(){
        /*
            How to use the predexctl command-line tool:
            predexctl [--socket PATH] <command> <args> 
        */
        std::cerr 
            <<"usage:\n"
            <<" predexctl [--socket PATH] status\n"
            <<" predexctl [--socket PATH] counterstats\n"
            <<" predexctl [--socket PATH] shutdown-graceful\n";
            
    }

    bool send_all_cmds(int sockfd, std::string_view payload){
        while(!payload.empty()){
            const ssize_t sent = ::send(sockfd, payload.data(), payload.size(), MSG_NOSIGNAL);
            if(sent <= 0){
                return false;
            }
            payload.remove_prefix(static_cast<size_t>(sent));
        }
        return true;
    }

    std::string read_response(int sockfd){
        std::string response;
        std::array<char, kBUFFERSIZE> buffer{};

        while(true){
            const ssize_t received = ::recv(sockfd, buffer.data(), buffer.size(), MSG_CMSG_CLOEXEC);
            if(received <= 0){
                break;
            }
            response.append(buffer.data(), static_cast<size_t>(received));
            if(response.find('\n')!= std::string::npos){
                break;
            }
        }

        return response;
    }

}// namespace

    constexpr std::string_view kDefaultSocketPath = "/tmp/predex_operator.sock";

    enum class SocketExitCode : std::uint8_t{
        kSuccess = 0,
        kInvalidArgs = 1,
        kSocketFailure = 2,
        kExitProtocolFailure = 3,
    };

}

int main(int argc, char** argv){
        std::string socket_path = std::string(predexctl::kDefaultSocketPath);
        int arg_index = 1;
        while(arg_index < argc){
            const std::string_view arg{argv[arg_index]};
            if(arg == "--socket"){
                if(arg_index + 1 >= argc){
                    predexctl::usage();
                    return static_cast<int>(predexctl::SocketExitCode::kInvalidArgs);
                }
                socket_path = argv[arg_index + 1];
                arg_index += 2;
                continue;
            }
            break;
        }

        if(arg_index >= argc){
            predexctl::usage();
            return static_cast<int>(predexctl::SocketExitCode::kInvalidArgs);
        }

        const std::string_view command{argv[arg_index]};

        std::string type;

        if(command == "status"){
            type = "status";
        }else if(command == "counterstats" || command == "stats"){
            type = "counterstats";
        }else if(command == "shutdown-graceful" || command == "shutdown_graceful"){
            type = "shutdown-graceful";
        }else if(command == "shutdown-forceful" || command == "shutdown_forceful"){
            type = "shutdown-forceful";
        }else{
            predexctl::usage();
            return static_cast<int>(predexctl::SocketExitCode::kInvalidArgs);
        }

        const int sockfd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        
        if(sockfd < 0){
            std::cerr << "Failed to create socket: " << std::strerror(errno) << std::endl;
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;

        if(socket_path.size() >= sizeof(addr.sun_path)){
            std::cerr << "Socket path is too long: " << socket_path << std::endl;
            return static_cast<int>(predexctl::SocketExitCode::kInvalidArgs);
        }

        std::strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

        if(::connect(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0){
            std::cerr << "Failed to connect to socket: " << std::strerror(errno) << std::endl;
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        const nlohmann::json cmd_json{
            {"cmd_id", predexctl::make_cmd_id()},
            {"type", type},
        };

        const std::string cmd_str = cmd_json.dump() + "\n";

        if(!predexctl::send_all_cmds(sockfd, cmd_str)){
            std::cerr << "Failed to send command: " << std::strerror(errno) << std::endl;
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        const auto response = predexctl::read_response(sockfd);
        if(response.empty()){
            std::cerr << "Failed to read response: " << std::strerror(errno) << std::endl;
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        ::close(sockfd);

        std::cout << response;

        return static_cast<int>(predexctl::SocketExitCode::kSuccess);

}
