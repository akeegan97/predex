#include "predex/exchange/kalshi/websocket_session.hpp"
#include <openssl/ssl.h>
#include <string>


namespace {

    std::string to_lower(std::string text) {
        std::transform(text.begin(), text.end(), text.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        return text;
    }

    bool is_default_port(std::string_view scheme, std::string_view port) {
        return (scheme == "wss" && port == "443") || (scheme == "ws" && port == "80");
    }

    std::optional<predex::exchange::kalshi::EndpointParts> parse_ws_endpoint(std::string_view endpoint, std::string* error_out){
        
        if(endpoint.empty()){
            *error_out = "Websocket endpoint is empty";
            return std::nullopt;
        }
        
        const auto scheme_pos = endpoint.find("://");
        if (scheme_pos == std::string_view::npos) {
            *error_out = "Websocket endpoint must include a scheme (ws:// or wss://)";
            return std::nullopt;
        }
        
        predex::exchange::kalshi::EndpointParts out;
        out.scheme = to_lower(std::string(endpoint.substr(0, scheme_pos)));
        if (out.scheme != "ws" && out.scheme != "wss") {
            *error_out = "Unsupported websocket scheme: " + out.scheme;
            return std::nullopt;
        }

        const auto host_pos = scheme_pos + 3;
        if (host_pos >= endpoint.size()) {
            *error_out = "Websocket endpoint missing host";
            return std::nullopt;
        }

        const auto path_pos = endpoint.find('/', host_pos);
        const std::string authority{
            path_pos == std::string_view::npos ? endpoint.substr(host_pos) : endpoint.substr(host_pos, path_pos - host_pos)
        };
        out.target = path_pos == std::string_view::npos ? "/" : std::string(endpoint.substr(path_pos));
        if (authority.empty()) {
            *error_out = "Websocket endpoint authority is empty";
            return std::nullopt;
        }
        if (authority.front() == '[') {
            const auto bracket_end = authority.find(']');
            if (bracket_end == std::string::npos) {
                *error_out = "Malformed IPv6 host in endpoint";
                return std::nullopt;
            }
            out.host = authority.substr(1, bracket_end - 1);

            if (bracket_end + 1 < authority.size()) {
                if (authority[bracket_end + 1] != ':') {
                    *error_out = "Malformed port in endpoint authority";
                    return std::nullopt;
                }
                out.port = authority.substr(bracket_end + 2);
            }
        } else {
            const auto colon = authority.rfind(':');
            if (colon != std::string::npos && authority.find(':') == colon) {
                out.host = authority.substr(0, colon);
                out.port = authority.substr(colon + 1);
            } else {
                out.host = authority;
            }
        }
        if (out.port.empty()) {
            out.port = out.scheme == "wss" ? "443" : "80";
        }
        return out;
    }

    std::string openssl_error_message(const std::string& context) {
        constexpr size_t kOpenSslErrorBufferSize = 256;
        const unsigned long code = ERR_get_error();
        if (code == 0) {
            return context;
        }

        std::array<char, kOpenSslErrorBufferSize> buffer{};
        ERR_error_string_n(code, buffer.data(), buffer.size());
        return context + ": " + std::string(buffer.data());
    }

    [[nodiscard]] std::uint64_t steady_now_ns() noexcept {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                            std::chrono::steady_clock::now().time_since_epoch())
                                            .count());
    }

}
namespace predex::exchange::kalshi {

    WebSocketSession::WebSocketSession(const IWsAdapter& adapter) :
        ssl_context_(boost::asio::ssl::context::tlsv12_client),
        ws_stream_(std::make_unique<WsStream>(io_context_, ssl_context_)),
        adapter_(adapter) {
            ssl_context_.set_verify_mode(boost::asio::ssl::verify_peer);
            boost::system::error_code error_code;
            const auto set_default_paths_result = ssl_context_.set_default_verify_paths(error_code);
            (void)set_default_paths_result;
            if (error_code) {
                last_error_ = "Failed to load default TLS CA paths: " + error_code.message();
            }
        }
    
    WebSocketSession::~WebSocketSession(){
        close();
    };

    bool WebSocketSession::connect(){
        close();
        reset_stream();
        last_error_.clear();
        connect_request_ = adapter_.build_connect_request();
        auto endpoint = parse_ws_endpoint(connect_request_.endpoint, &last_error_);
        if(!endpoint){return false;}

        if(endpoint->scheme != "wss"){
            last_error_ = "Only wss:// endpoints are supported right now";
            return false;
        }
        endpoint_ = *endpoint;

        boost::system::error_code error_code;
        const auto results = resolver_.resolve(endpoint_.host, endpoint_.port, error_code);
        if (error_code) {
            last_error_ = "Failed to resolve websocket host: " + error_code.message();
            return false;
        }

        boost::beast::get_lowest_layer(*ws_stream_).connect(results, error_code);
        if(error_code){
            last_error_ = "Failed to connect to websocket host: " + error_code.message();
            return false;
        }

        if(SSL_set_tlsext_host_name(ws_stream_->next_layer().native_handle(), endpoint_.host.c_str()) != 1){
            last_error_ =  openssl_error_message("Failed to set SNI hostname for TLS connection");
            return false;
        }

        if(SSL_set1_host(ws_stream_->next_layer().native_handle(), endpoint_.host.c_str()) != 1){
            last_error_ =  openssl_error_message("Failed to set hostname for TLS connection");
            return false;
        }

        const auto tls_handshake_result = ws_stream_->next_layer().handshake(boost::asio::ssl::stream_base::client, error_code);
        if(error_code){
            last_error_ = "TLS handshake failed: " + error_code.message();
            return false;
        }

        ws_stream_->set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::client));
        ws_stream_->set_option(boost::beast::websocket::stream_base::decorator(
            [this](boost::beast::websocket::request_type& req) {
                req.set(boost::beast::http::field::user_agent, "PredEx/1.0");
                for (const auto& [name, value] : connect_request_.headers) {
                    req.set(name, value);
                }
            }
        ));

        //Optional to include callback to report last_ping_recv_ns_
        ws_stream_->control_callback([this](boost::beast::websocket::frame_type kind, boost::beast::string_view payload) {
            if (kind == boost::beast::websocket::frame_type::ping) {
                last_ping_recv_ns_ = steady_now_ns();
            }
        });

        std::string handshake_host = endpoint_.host;
        
        if (!is_default_port(endpoint_.scheme, endpoint_.port)) {
            handshake_host += ":" + endpoint_.port;
        }

        ws_stream_->handshake(handshake_host, endpoint_.target, error_code);
        if(error_code){
            last_error_ = "Websocket handshake failed: " + error_code.message();
            return false;
        }

        ws_stream_->text(true);
        last_ping_recv_ns_ = steady_now_ns(); // initialize to now 
        connected_ = true;
        return true;

    }
    
    void WebSocketSession::close(){
        boost::system::error_code error_code;
        auto& socket = boost::beast::get_lowest_layer(*ws_stream_).socket();
        const auto cancelled_operations = socket.cancel(error_code);

        if(error_code && error_code != boost::beast::net::error::not_connected) {
            last_error_ = "Failed to cancel outstanding operations: " + error_code.message();
        }

        error_code.clear();

        const auto shutdown_result = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, error_code);
        if (error_code && error_code != boost::beast::net::error::not_connected) {
            last_error_ = "Failed to shutdown socket: " + error_code.message();
        }

        error_code.clear();

        const auto close_result = socket.close(error_code);
        if (error_code && error_code != boost::beast::net::error::not_connected) {
            last_error_ = "Failed to close socket: " + error_code.message();
        }

        connected_ = false;
    }

    bool WebSocketSession::send_text(std::string_view message){
        if(!connected_){
            last_error_ = "Websocket is not connected";
            return false;
        }

        boost::system::error_code error_code;
        ws_stream_->write(boost::asio::buffer(message), error_code);
        if(error_code){
            last_error_ = "Failed to send message: " + error_code.message();
            return false;
        }
        return true;
    }

    const WsConnectRequest& WebSocketSession::connect_request() const noexcept {
        return connect_request_;
    }

    std::string_view WebSocketSession::last_error() const noexcept {
        return last_error_;
    }

    ReadResult WebSocketSession::recv_text(std::chrono::milliseconds timeout){
        if(!connected_){
            last_error_ = "Websocket is not connected";
            return {.status = ReadStatus::kClosed};
        }
        read_buffer_.consume(read_buffer_.size());
        auto& stream = boost::beast::get_lowest_layer(*ws_stream_);
        stream.expires_after(timeout);
        boost::system::error_code error_code;
        ws_stream_->read(read_buffer_, error_code);
        stream.expires_never();
        if (error_code) {
            if (error_code == boost::beast::error::timeout || error_code == boost::beast::net::error::timed_out) {
                return {.status = ReadStatus::kTimeout};
            }
            if (error_code == boost::beast::websocket::error::closed || error_code == boost::beast::net::error::operation_aborted ||
                error_code == boost::beast::net::error::not_connected) {
                connected_ = false;
                return {.status = ReadStatus::kClosed};
            }
            last_error_ = "Failed to read message: " + error_code.message();
            connected_ = false;
            return {.status = ReadStatus::kError};
        }
        const auto buffer = read_buffer_.data();
        return {.status = ReadStatus::kMessage, 
                .payload = std::span<const std::byte>{reinterpret_cast<const std::byte*>(buffer.data()), buffer.size()}
                };
    }
}