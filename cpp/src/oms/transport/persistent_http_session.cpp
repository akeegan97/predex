#include "predex/oms/transport/persistent_http_session.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

namespace predex::core::oms::kalshi::transport {
namespace {

namespace net = boost::asio;
namespace ssl = net::ssl;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

constexpr std::size_t kHttpVersion = 11;

void join_path_into(std::string& out, std::string_view base, std::string_view target) {
    out.clear();
    out.reserve(base.size() + target.size() + 1);
    if (base.empty()) {
        out.append(target);
        return;
    }

    out.append(base);
    if (target.empty() || target.front() == '/') {
        out.append(target);
        return;
    }

    out.push_back('/');
    out.append(target);
}

[[nodiscard]] http::verb to_beast_verb(HttpMethod method) noexcept {
    switch (method) {
        case HttpMethod::kGet:
            return http::verb::get;
        case HttpMethod::kPost:
            return http::verb::post;
        case HttpMethod::kDelete:
            return http::verb::delete_;
    }
    return http::verb::get;
}

[[nodiscard]] std::string_view to_auth_method(HttpMethod method) noexcept {
    switch (method) {
        case HttpMethod::kGet:
            return "GET";
        case HttpMethod::kPost:
            return "POST";
        case HttpMethod::kDelete:
            return "DELETE";
    }
    return "GET";
}

} // namespace

struct PersistentHttpSession::ConnectionState {
    net::io_context io_context;
    ssl::context ssl_context{ssl::context::tls_client};
    std::optional<beast::ssl_stream<beast::tcp_stream>> stream;
    bool connected{false};
};

PersistentHttpSession::PersistentHttpSession(predex::websocket::kalshi::AuthSigner signer,
                                             std::string endpoint)
    : signer_(std::move(signer)),
      endpoint_(std::move(endpoint)),
      connection_(std::make_unique<ConnectionState>()),
      last_call_ts_(std::chrono::steady_clock::now()) {
    connection_->ssl_context.set_default_verify_paths();

    EndpointParts endpoint_parts;
    endpoint_valid_ = parse_endpoint(endpoint_, endpoint_parts, endpoint_parse_error_);
    if (endpoint_valid_) {
        endpoint_host_ = std::move(endpoint_parts.host);
        endpoint_port_ = std::move(endpoint_parts.port);
        endpoint_base_path_ = std::move(endpoint_parts.base_path);
    }
}

PersistentHttpSession::~PersistentHttpSession() {
    close_connection_();
}

PersistentHttpSession::PersistentHttpSession(PersistentHttpSession&&) noexcept = default;

PersistentHttpSession&
PersistentHttpSession::operator=(PersistentHttpSession&&) noexcept = default;

HttpResponse PersistentHttpSession::send_json_request(const HttpRequest& request) {
    if (!endpoint_valid_) {
        return {
            .ok = false,
            .status_code = 0,
            .body = {},
            .error_message = endpoint_parse_error_,
            .keep_alive = false,
        };
    }

    auto response = send_json_request_once_(request);
    if(response.ok || response.error_message != "transport_retry") {
        return response;
    }
    auto retry_response = send_json_request_once_(request);
    if(retry_response.ok || retry_response.error_message != "transport_retry") {
        return retry_response;
    }

    return {
        .ok = false,
        .status_code = 0,
        .body = {},
        .error_message = "transport_retry_failed",
        .keep_alive = false,
    };
}

void PersistentHttpSession::check_and_keep_warm(std::uint64_t threshold_seconds) {
    if (!endpoint_valid_ || connection_ == nullptr || !connection_->connected) {
        return;
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - last_call_ts_);
    if (elapsed.count() < static_cast<std::int64_t>(threshold_seconds)) {
        return;
    }

    (void)send_json_request(HttpRequest{
        .method = HttpMethod::kGet,
        .target = "/trade-api/v2/exchange/status",
        .authenticate = false,
    });
}

bool PersistentHttpSession::ensure_connected_() {
    if (!endpoint_valid_ || connection_ == nullptr) {
        return false;
    }
    if (connection_->connected) {
        return true;
    }

    try {
        connection_->stream.emplace(connection_->io_context, connection_->ssl_context);
        auto& stream = *connection_->stream;

        if (!SSL_set_tlsext_host_name(stream.native_handle(), endpoint_host_.c_str())) {
            connection_->stream.reset();
            return false;
        }

        tcp::resolver resolver{connection_->io_context};
        auto results = resolver.resolve(endpoint_host_, endpoint_port_);
        auto& lowest = beast::get_lowest_layer(stream);
        lowest.expires_after(kConnectTimeout);
        lowest.connect(results);

        lowest.socket().set_option(net::socket_base::keep_alive{true});

        lowest.expires_after(kConnectTimeout);
        stream.handshake(ssl::stream_base::client);
        lowest.expires_never();
        connection_->connected = true;
        return true;
    } catch (const std::exception&) {
        connection_->stream.reset();
        connection_->connected = false;
        return false;
    }
}

void PersistentHttpSession::close_connection_() noexcept {
    if (connection_ == nullptr) {
        return;
    }
    if (!connection_->connected) {
        connection_->stream.reset();
        return;
    }

    try {
        beast::error_code ignored;
        //NOLINTNEXTLINE
        (void)connection_->stream->shutdown(ignored);
    } catch (...) {
    }

    connection_->stream.reset();
    connection_->connected = false;
}

bool PersistentHttpSession::parse_endpoint(std::string_view endpoint,
                                           EndpointParts& out,
                                           std::string& error) {
    const auto scheme_pos = endpoint.find("://");
    if (scheme_pos == std::string_view::npos) {
        error = "REST endpoint must include scheme";
        return false;
    }

    std::string scheme{endpoint.substr(0, scheme_pos)};
    std::transform(scheme.begin(), scheme.end(), scheme.begin(), [](unsigned char car) {
        return static_cast<char>(std::tolower(car));
    });
    if (scheme != "https") {
        error = "Only https endpoints are supported";
        return false;
    }

    const auto authority_start = scheme_pos + 3;
    const auto path_start = endpoint.find('/', authority_start);
    const std::string authority{
        path_start == std::string_view::npos
            ? endpoint.substr(authority_start)
            : endpoint.substr(authority_start, path_start - authority_start)};
    if (authority.empty()) {
        error = "REST endpoint authority is empty";
        return false;
    }

    const auto colon_pos = authority.rfind(':');
    if (colon_pos == std::string::npos) {
        out.host = authority;
        out.port = "443";
    } else {
        out.host = authority.substr(0, colon_pos);
        out.port = authority.substr(colon_pos + 1);
    }

    out.base_path = path_start == std::string_view::npos
        ? ""
        : std::string(endpoint.substr(path_start));
    if (!out.base_path.empty() && out.base_path.back() == '/') {
        out.base_path.pop_back();
    }

    if (out.host.empty()) {
        error = "REST endpoint host is empty";
        return false;
    }

    return true;
}

HttpResponse PersistentHttpSession::send_json_request_once_(const HttpRequest& request) {
    if (!ensure_connected_()) {
        return {
            .ok = false,
            .status_code = 0,
            .body = {},
            .error_message = "transport_disconnected",
            .keep_alive = false,
        };
    }

    try {
        const auto request_target = build_request_target_(request.target);
        const auto signing_target = build_signing_target_(request_target);

        http::request<http::string_body> raw_request;
        raw_request.version(kHttpVersion);
        raw_request.method(to_beast_verb(request.method));
        raw_request.target(request_target);
        raw_request.set(http::field::host, endpoint_host_);
        raw_request.set(http::field::user_agent, "predex-oms-rest");

        if (!request.content_type.empty() && request.method != HttpMethod::kGet) {
            raw_request.set(http::field::content_type, request.content_type);
        }

        if (request.authenticate) {
            const auto auth_headers =
                signer_.make_auth_headers(std::string(to_auth_method(request.method)),
                                          signing_target);
            raw_request.set("KALSHI-ACCESS-KEY", auth_headers.key_id);
            raw_request.set("KALSHI-ACCESS-TIMESTAMP", auth_headers.timestamp_ms);
            raw_request.set("KALSHI-ACCESS-SIGNATURE", auth_headers.signature_base64);
        }

        if (!request.body.empty()) {
            raw_request.body() = request.body;
            raw_request.prepare_payload();
        }

        auto& stream = *connection_->stream;
        auto& lowest = beast::get_lowest_layer(stream);
        lowest.expires_after(kIoTimeout);
        http::write(stream, raw_request);

        beast::flat_buffer buffer;
        http::response<http::string_body> response;
        lowest.expires_after(kIoTimeout);
        http::read(stream, buffer, response);
        lowest.expires_never();

        last_call_ts_ = std::chrono::steady_clock::now();

        if (!response.keep_alive()) {
            close_connection_();
        }
        //NOLINTNEXTLINE
        const bool ok = response.result_int() >= 200 && response.result_int() < 300;
        return {
            .ok = ok,
            .status_code = static_cast<int>(response.result_int()),
            .body = response.body(),
            .error_message = ok
                ? std::string{}
                : "HTTP " + std::to_string(response.result_int()) + " body=" + response.body(),
            .keep_alive = response.keep_alive(),
        };
    } catch (const std::exception&) {
        close_connection_();
        return {
            .ok = false,
            .status_code = 0,
            .body = {},
            .error_message = "transport_retry",
            .keep_alive = false,
        };
    }
}

std::string PersistentHttpSession::build_request_target_(std::string_view target) const {
    std::string request_target;
    join_path_into(request_target, endpoint_base_path_, target);
    return request_target;
}
//NOLINTNEXTLINE 
std::string PersistentHttpSession::build_signing_target_(
    std::string_view request_target) const {
    const auto query_pos = request_target.find('?');
    if (query_pos == std::string_view::npos) {
        return std::string(request_target);
    }
    return std::string(request_target.substr(0, query_pos));
}

} // namespace predex::core::oms::kalshi::transport
