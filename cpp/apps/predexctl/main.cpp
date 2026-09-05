
#include <array>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <optional>
#include <string_view>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <poll.h>
#include <algorithm>
#include <iterator>
#include <nlohmann/json.hpp>


#include <predex/socket/unique_fd.hpp>

namespace predexctl{

namespace{

    struct UnixCommandClientConfig{
        std::string socket_path;
        std::chrono::milliseconds timeout{2000}; // 2 seconds in milliseconds NOLINT
        std::size_t max_response_size{16 * 1024 * 1024}; // Maximum response size in bytes NOLINT 
        std::string command;
    };
    struct ParseArgsResult{
        bool ok{false};
        bool help_requested{false};
        UnixCommandClientConfig config;
        std::string error;
    };
    enum class ClientStatus : std::uint8_t{
        kSUCCESS = 0,
        kCLIENT_TIMEOUT = 1,
        kCONNECT_FAILURE = 2,
        kWRITE_FAILURE = 3,
        kEOF_BEFORE_RESPONSE = 4,
        kRESPONSE_TOO_LARGE = 5,
        kREAD_FAILURE = 6,
        kPROTOCOL_ERROR = 7,
    };
    struct UnixCommandClientResult{
        ClientStatus status;
        std::string error_message;
        std::string response;
    };
    enum class SocketExitCode : std::uint8_t{
        kSuccess = 0,
        kInvalidArgs = 1,
        kSocketFailure = 2,
        kExitProtocolFailure = 3,
        kSocketTimeout = 4,
        kWriteFailure = 5,
        kReadFailure = 6,
    };

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

    void usage(std::ostream& output){
        /*
            How to use the predexctl command-line tool:
            predexctl [--socket PATH] <command> <args> 
        */
        output
            <<"usage:\n"
            <<" predexctl [--socket PATH] [--timeout-ms N] status\n"
            <<" predexctl [--socket PATH] [--timeout-ms N] counterstats\n"
            <<" predexctl [--socket PATH] [--timeout-ms N] allow-trading\n"
            <<" predexctl [--socket PATH] [--timeout-ms N] disable-trading\n"
            <<" predexctl [--socket PATH] [--timeout-ms N] cancel-all-orders\n"
            <<" predexctl [--socket PATH] [--timeout-ms N] shutdown-graceful\n"
            <<" predexctl [--socket PATH] [--timeout-ms N] shutdown-forceful\n"
            <<"\n"
            <<"socket path precedence: --socket, then PREDEX_SOCKET_PATH\n";
            
    }

    std::optional<std::string> normalize_command(std::string_view command){
        if(command == "status"){
            return std::string{"status"};
        }
        if(command == "counterstats" || command == "stats"){
            return std::string{"counterstats"};
        }
        if(command == "shutdown-graceful" || command == "shutdown_graceful"){
            return std::string{"shutdown-graceful"};
        }
        if(command == "shutdown-forceful" || command == "shutdown_forceful"){
            return std::string{"shutdown-forceful"};
        }
        if(command == "allow-trading" || command == "allow_trading"){
            return std::string{"allow-trading"};
        }
        if(command == "disable-trading" || command == "disable_trading"){
            return std::string{"disable-trading"};
        }
        if(command == "cancel-all-orders" || command == "cancel_all_orders"){
            return std::string{"cancel-all-orders"};
        }
        return std::nullopt;
    }
//NOLINTNEXTLINE(readability-function-cognitive-complexity)
    ParseArgsResult parse_args(int argc, char** argv){
        ParseArgsResult result{};
        UnixCommandClientConfig config{};
        std::optional<std::string_view> command;
        bool explicit_socket = false;
        bool explicit_timeout = false;

        if(const char* socket_path = std::getenv("PREDEX_SOCKET_PATH");
           socket_path != nullptr && socket_path[0] != '\0'){
            config.socket_path = socket_path;
        }

        for(int i = 1; i < argc; ++i){
            const std::string_view arg{argv[i]};

            if(arg == "--help" || arg == "-h"){
                result.help_requested = true;
                return result;
            }

            if(arg == "--socket"){
                if(explicit_socket){
                    result.error = "--socket specified more than once";
                    return result;
                }
                if(i+1 >= argc){
                    result.error = "--socket requires a path";
                    return result;
                }
                const std::string_view socket_path{argv[i+1]};
                if(socket_path.empty()){
                    result.error = "--socket path must not be empty";
                    return result;
                }
                config.socket_path = socket_path;
                explicit_socket = true;
                ++i;
                continue;
            }

            if(arg == "--timeout-ms"){
                if(explicit_timeout){
                    result.error = "--timeout-ms specified more than once";
                    return result;
                }
                if(i+1 >= argc){
                    result.error = "--timeout-ms requires a value";
                    return result;
                }

                const std::string_view timeout_value{argv[i+1]};
                std::uint32_t timeout_ms = 0;
                const auto [end, parse_error] = std::from_chars(
                    timeout_value.data(),
                    timeout_value.data() + timeout_value.size(),
                    timeout_ms);

                if(parse_error != std::errc{} ||
                   end != timeout_value.data() + timeout_value.size() ||
                   timeout_ms == 0 ||
                   timeout_ms > static_cast<std::uint32_t>(std::numeric_limits<int>::max())){
                    result.error = "invalid --timeout-ms value";
                    return result;
                }

                config.timeout = std::chrono::milliseconds{timeout_ms};
                explicit_timeout = true;
                ++i;
                continue;
            }

            if(!arg.empty() && arg.front() == '-'){
                result.error = "unknown option: " + std::string{arg};
                return result;
            }

            if(command.has_value()){
                result.error = "only one command may be specified";
                return result;
            }

            if(arg.empty()){
                result.error = "command must not be empty";
                return result;
            }

            command = arg;
        }

        if(config.socket_path.empty()){
            result.error = "no socket path specified; use --socket or set PREDEX_SOCKET_PATH";
            return result;
        }

        if(!command.has_value()){
            result.error = "no command specified";
            return result;
        }

        auto normalized_command = normalize_command(*command);
        if(!normalized_command.has_value()){
            result.error = "unknown command: " + std::string{*command};
            return result;
        }

        config.command = std::move(*normalized_command);
        result.config = std::move(config);
        result.ok = true;
        return result;
    }




}// namespace predexctl
}
//NOLINTNEXTLINE
int main(int argc, char** argv){

    const auto parsed_args = predexctl::parse_args(argc, argv);

    if(parsed_args.help_requested){
        predexctl::usage(std::cout);
        return static_cast<int>(predexctl::SocketExitCode::kSuccess);
    }

    if(!parsed_args.ok){
        std::cerr << parsed_args.error << '\n';
        predexctl::usage(std::cerr);
        return static_cast<int>(predexctl::SocketExitCode::kInvalidArgs);
    }

    const auto& config = parsed_args.config;

    if(config.socket_path.empty() || config.socket_path.size() >= sizeof(sockaddr_un::sun_path)){
        std::cerr << "invalid socket path\n";
        return static_cast<int>(predexctl::SocketExitCode::kInvalidArgs);
    }

    predex::socket::UniqueFD sockfd(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));

    if(!sockfd.is_valid()){
        std::cerr << "failed to create socket: " << std::strerror(errno) << '\n';
        return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
    }

    const auto transaction_deadline = std::chrono::steady_clock::now() + config.timeout;
    bool connected = false;

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    ::memcpy(addr.sun_path, config.socket_path.c_str(), config.socket_path.size());
    addr.sun_path[config.socket_path.size()] = '\0';

    const int connect_ret = ::connect(sockfd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    if(connect_ret == 0){
        connected = true;
    }else{
        const int connect_errno = errno;
        if(
            connect_errno != EINPROGRESS && 
            connect_errno != EAGAIN && 
            connect_errno != EWOULDBLOCK &&
            connect_errno != EALREADY && 
            connect_errno != EINTR
        ){
            std::cerr << "failed to connect: " << std::strerror(connect_errno) << '\n';
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }
    }

    while(!connected){
        const auto now = std::chrono::steady_clock::now();
        
        if(now >= transaction_deadline){
            std::cerr << "connection timed out\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketTimeout);
        }

        const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(transaction_deadline - now);

        pollfd pfd{
            .fd = sockfd.get(),
            .events = POLLOUT,
            .revents = 0,
        };

        const int poll_ret = ::poll(&pfd, 1, static_cast<int>(remaining.count()));

        if(poll_ret == 0){
            std::cerr << "connection timeout\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketTimeout);
        }
        
        if(poll_ret < 0){
            if(errno == EINTR){
                continue;
            }
            std::cerr << "connection poll failed: " << std::strerror(errno) << '\n';
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        if((pfd.revents & POLLNVAL) != 0){
            std::cerr << "connection socket became invalid\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        if((pfd.revents & (POLLERR | POLLHUP | POLLOUT)) != 0){
            int socket_error = 0;
            socklen_t len = sizeof(socket_error);
            if(::getsockopt(sockfd.get(), SOL_SOCKET, SO_ERROR, &socket_error, &len) < 0){
                std::cerr << "failed to get socket error: " << std::strerror(errno) << '\n';
                return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
            }
            if(socket_error != 0){
                std::cerr << "connection socket error: " << std::strerror(socket_error) << '\n';
                return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
            }
            
            connected = true;
        }


    }

    const nlohmann::json request{
        {"cmd_id", predexctl::make_cmd_id()},
        {"type", config.command} 
    };

    const std::string request_str = request.dump() + '\n';

    std::size_t total_sent = 0;

    while(total_sent < request_str.size()){
        const auto now = std::chrono::steady_clock::now();
        if(now >= transaction_deadline){
            std::cerr << "send timed out\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketTimeout);
        }

        pollfd pfd{
            .fd = sockfd.get(),
            .events = POLLOUT,
            .revents = 0,
        };

        const int poll_ret = ::poll(&pfd, 1, static_cast<int>(std::chrono::ceil<std::chrono::milliseconds>(transaction_deadline - now).count()));

        if(poll_ret == 0){
            std::cerr << "send timed out\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketTimeout);
        }

        if(poll_ret < 0){
            if(errno == EINTR){
                continue;
            }
            std::cerr << "send poll failed: " << std::strerror(errno) << '\n';
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        if ((pfd.revents &
            (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            std::cerr << "socket closed while sending request\n";
            return static_cast<int>(
                predexctl::SocketExitCode::kWriteFailure);
        }

        if((pfd.revents & POLLOUT) != 0){

            const ssize_t sent = ::send(sockfd.get(), request_str.data() + total_sent, request_str.size() - total_sent, MSG_NOSIGNAL);

            if (sent > 0) {
                total_sent += static_cast<std::size_t>(sent);
                continue;
            }

            if (sent < 0 && errno == EINTR) {
                continue;
            }

            if (sent < 0 &&
                (errno == EAGAIN || errno == EWOULDBLOCK)) {
                continue;
            }

            if (sent == 0) {
                std::cerr << "server closed connection while sending request\n";
            } else {
                std::cerr << "send failed: " << std::strerror(errno) << '\n';
            }

            return static_cast<int>(
                predexctl::SocketExitCode::kWriteFailure);
        }

    }


    std::string response_str;
    std::array<char, predexctl::kBUFFERSIZE> buffer{};

    while(true){

        const auto now = std::chrono::steady_clock::now();
        if(now >= transaction_deadline){
            std::cerr << "receive timed out\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketTimeout);
        }
        
        pollfd pfd{
            .fd = sockfd.get(),
            .events = POLLIN,
            .revents = 0,
        };

        const int poll_ret = ::poll(&pfd, 1, static_cast<int>(std::chrono::ceil<std::chrono::milliseconds>(transaction_deadline - now).count()));

        if(poll_ret == 0){
            std::cerr << "receive timed out\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketTimeout);
        }

        if(poll_ret < 0){
            if(errno == EINTR){
                continue;
            }
            std::cerr << "receive poll failed: " << std::strerror(errno) << '\n';
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        if((pfd.revents & (POLLERR | POLLNVAL)) != 0){
            std::cerr << "receive poll error\n";
            return static_cast<int>(predexctl::SocketExitCode::kSocketFailure);
        }

        if((pfd.revents & (POLLIN | POLLHUP)) != 0){
            ssize_t received = ::recv(sockfd.get(), buffer.data(), buffer.size(), 0);
            if(received < 0){
                if(errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK){
                    continue;
                }
                std::cerr << "receive failed: " << std::strerror(errno) << '\n';
                return static_cast<int>(predexctl::SocketExitCode::kReadFailure);
            }
            if(received == 0){
                std::cerr << "connection closed by server\n";
                return static_cast<int>(predexctl::SocketExitCode::kReadFailure);
            }

            const auto count = static_cast<std::size_t>(received);

            auto* const begin = buffer.begin();
            auto* const end = begin + static_cast<std::ptrdiff_t>(count);
            auto* const newline = std::find(begin, end, '\n');

            const auto payload_count = static_cast<std::size_t>(
                std::distance(begin, newline));

            if (response_str.size() + payload_count >
                config.max_response_size) {
                std::cerr << "response too large\n";
                return static_cast<int>(
                    predexctl::SocketExitCode::kReadFailure);
            }

            response_str.append(buffer.data(), payload_count);

            if (newline != end) {
                break;
            }
        }
    }

    std::cout << response_str << '\n';

    return static_cast<int>(predexctl::SocketExitCode::kSuccess);

}
