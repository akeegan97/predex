#include "predex/websocket/client.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <string>
#include <string_view>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/system/error_code.hpp>
#include <openssl/err.h>
#include <openssl/ssl.h>

namespace predex::websocket {
namespace net = boost::asio;
namespace ssl = net::ssl;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

namespace {

struct EndpointParts {
    std::string scheme;
    std::string host;
    std::string port;
    std::string target;
};

std::string to_lower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char character) {
        return static_cast<char>(std::tolower(character));
    });
    return text;
}

bool is_default_port(std::string_view scheme, std::string_view port) {
    return (scheme == "wss" && port == "443") || (scheme == "ws" && port == "80");
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

std::optional<EndpointParts> parse_ws_endpoint(std::string_view endpoint, std::string* error_out) {
    if (endpoint.empty()) {
        *error_out = "Websocket endpoint is empty";
        return std::nullopt;
    }

    const auto scheme_pos = endpoint.find("://");
    if (scheme_pos == std::string_view::npos) {
        *error_out = "Websocket endpoint must include a scheme (ws:// or wss://)";
        return std::nullopt;
    }

    EndpointParts out;
    out.scheme = to_lower(std::string(endpoint.substr(0, scheme_pos)));
    if (out.scheme != "ws" && out.scheme != "wss") {
        *error_out = "Unsupported websocket scheme: " + out.scheme;
        return std::nullopt;
    }

    const auto authority_start = scheme_pos + 3;
    if (authority_start >= endpoint.size()) {
        *error_out = "Websocket endpoint missing authority";
        return std::nullopt;
    }

    const auto path_start = endpoint.find('/', authority_start);
    const std::string authority{
        path_start == std::string_view::npos
            ? endpoint.substr(authority_start)
            : endpoint.substr(authority_start, path_start - authority_start)};
    out.target =
        path_start == std::string_view::npos ? "/" : std::string(endpoint.substr(path_start));

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

    if (out.host.empty()) {
        *error_out = "Websocket endpoint host is empty";
        return std::nullopt;
    }

    if (out.port.empty()) {
        out.port = out.scheme == "wss" ? "443" : "80";
    }

    return out;
}

} // namespace

struct BoostBeastWsTransport::Impl {
    using WsStream = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

    net::io_context io_context;
    ssl::context ssl_context{ssl::context::tls_client};
    tcp::resolver resolver{io_context};
    std::unique_ptr<WsStream> ws_stream;
    beast::flat_buffer read_buffer;
    bool connected{false};
    std::string last_error;
    EndpointParts endpoint{};

    Impl() : ws_stream(std::make_unique<WsStream>(io_context, ssl_context)) {}

    void reset_stream() {
        ws_stream = std::make_unique<WsStream>(io_context, ssl_context);
        read_buffer.consume(read_buffer.size());
        connected = false;
    }
};

BoostBeastWsTransport::BoostBeastWsTransport() : impl_(std::make_unique<Impl>()) {
    impl_->ssl_context.set_verify_mode(ssl::verify_peer);
    boost::system::error_code error_code;
    const auto set_default_paths_result = impl_->ssl_context.set_default_verify_paths(error_code);
    (void)set_default_paths_result;
    if (error_code) {
        impl_->last_error = "Failed to load default TLS CA paths: " + error_code.message();
    }
}

BoostBeastWsTransport::~BoostBeastWsTransport() = default;

bool BoostBeastWsTransport::connect(const TransportConfig& config) {
    close();
    impl_->reset_stream();
    impl_->last_error.clear();

    auto endpoint = parse_ws_endpoint(config.endpoint, &impl_->last_error);
    if (!endpoint) {
        return false;
    }
    if (endpoint->scheme != "wss") {
        impl_->last_error =
            "Only wss:// endpoints are supported by BoostBeastWsTransport right now";
        return false;
    }
    impl_->endpoint = *endpoint;

    boost::system::error_code error_code;
    const auto results =
        impl_->resolver.resolve(impl_->endpoint.host, impl_->endpoint.port, error_code);
    if (error_code) {
        impl_->last_error = "DNS resolve failed: " + error_code.message();
        return false;
    }
    beast::get_lowest_layer(*impl_->ws_stream).connect(results, error_code);
    if (error_code) {
        impl_->last_error = "TCP connect failed: " + error_code.message();
        return false;
    }

    if (SSL_set_tlsext_host_name(impl_->ws_stream->next_layer().native_handle(),
                                 impl_->endpoint.host.c_str()) != 1) {
        impl_->last_error = openssl_error_message("SNI setup failed");
        return false;
    }

    const auto tls_handshake_result =
        impl_->ws_stream->next_layer().handshake(ssl::stream_base::client, error_code);
    (void)tls_handshake_result;
    if (error_code) {
        impl_->last_error = "TLS handshake failed: " + error_code.message();
        return false;
    }

    impl_->ws_stream->set_option(
        websocket::stream_base::timeout::suggested(beast::role_type::client));
    impl_->ws_stream->set_option(
        websocket::stream_base::decorator([headers = config.headers](websocket::request_type& req) {
            for (const auto& [key, value] : headers) {
                req.set(key, value);
            }
        }));

    std::string handshake_host = impl_->endpoint.host;
    if (!is_default_port(impl_->endpoint.scheme, impl_->endpoint.port)) {
        handshake_host += ":" + impl_->endpoint.port;
    }
    impl_->ws_stream->handshake(handshake_host, impl_->endpoint.target, error_code);
    if (error_code) {
        impl_->last_error = "Websocket handshake failed: " + error_code.message();
        return false;
    }

    impl_->ws_stream->text(true);
    impl_->connected = true;
    return true;
}

bool BoostBeastWsTransport::send_text(std::string_view payload) {
    if (!impl_->connected) {
        impl_->last_error = "Cannot send: websocket is not connected";
        return false;
    }

    boost::system::error_code error_code;
    impl_->ws_stream->write(net::buffer(payload), error_code);
    if (error_code) {
        impl_->last_error = "Websocket write failed: " + error_code.message();
        return false;
    }
    return true;
}

RecvResult BoostBeastWsTransport::recv_text(std::chrono::milliseconds timeout) {
    if (!impl_->connected) {
        impl_->last_error = "Cannot receive: websocket is not connected";
        return {.status = RecvStatus::kClosed};
    }

    impl_->read_buffer.consume(impl_->read_buffer.size());
    auto& stream = beast::get_lowest_layer(*impl_->ws_stream);
    stream.expires_after(timeout);
    boost::system::error_code error_code;
    impl_->ws_stream->read(impl_->read_buffer, error_code);
    stream.expires_never();

    if (error_code) {
        if (error_code == beast::error::timeout || error_code == net::error::timed_out) {
            return {.status = RecvStatus::kTimeout};
        }
        if (error_code == websocket::error::closed || error_code == net::error::operation_aborted ||
            error_code == net::error::not_connected) {
            impl_->connected = false;
            return {.status = RecvStatus::kClosed};
        }
        impl_->last_error = "Websocket read failed: " + error_code.message();
        impl_->connected = false;
        return {.status = RecvStatus::kError};
    }

    return {
        .status = RecvStatus::kMessage,
        .payload = beast::buffers_to_string(impl_->read_buffer.data()),
    };
}

void BoostBeastWsTransport::close() {
    boost::system::error_code error_code;
    auto& socket = beast::get_lowest_layer(*impl_->ws_stream).socket();

    socket.cancel(error_code);
    error_code.clear();
    socket.shutdown(tcp::socket::shutdown_both, error_code);
    error_code.clear();
    socket.close(error_code);

    impl_->connected = false;
    if (error_code && error_code != net::error::not_connected) {
        impl_->last_error = "Websocket close error: " + error_code.message();
    }
}

std::string_view BoostBeastWsTransport::last_error() const { return impl_->last_error; }

} // namespace predex::websocket
