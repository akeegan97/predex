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
#include <string>
#include <thread>
#include <string_view>
#include <utility>

namespace predex::core::oms::kalshi::transport {
namespace {

namespace net = boost::asio;
namespace ssl = net::ssl;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

[[nodiscard]] internal::TimestampNs monotonic_now_ns() noexcept {
    return static_cast<internal::TimestampNs>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

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

[[nodiscard]] std::string transport_error_message(std::string_view kind,
                                                  std::string_view stage,
                                                  const beast::error_code& ec) {
    std::string message{kind};
    if (!stage.empty()) {
        message.push_back(':');
        message.append(stage);
    }
    if (ec) {
        message.push_back(':');
        message.append(ec.message());
        message.append(" (");
        message.append(ec.category().name());
        message.push_back(':');
        message.append(std::to_string(ec.value()));
        message.push_back(')');
    }
    return message;
}

[[nodiscard]] bool is_transport_retry_error(std::string_view error_message) noexcept {
    return error_message.rfind("transport_retry", 0) == 0;
}

} // namespace

struct PersistentHttpSession::ConnectionState {
    net::io_context io_context;
    ssl::context ssl_context{ssl::context::tls_client};
    tcp::resolver resolver{io_context};
    std::optional<beast::ssl_stream<beast::tcp_stream>> stream;
    bool connected{false};

    struct AsyncRequestState {
        HttpRequest request;
        std::string request_target;
        std::string signing_target;
        http::request<http::string_body> raw_request;
        beast::flat_buffer read_buffer;
        http::response<http::string_body> raw_response;
        HttpResponse response;
        bool completed{false};
    };

    std::unique_ptr<AsyncRequestState> async_request;
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
        return build_disconnected_response_(endpoint_parse_error_);
    }

    auto response = run_async_request_to_completion_(request);
    if (response.ok || !is_transport_retry_error(response.error_message)) {
        return response;
    }
    auto retry_response = run_async_request_to_completion_(request);
    ++retry_response.retry_count;
    if (retry_response.ok || !is_transport_retry_error(retry_response.error_message)) {
        return retry_response;
    }

    return {
        .ok = false,
        .status_code = 0,
        .retry_count = retry_response.retry_count,
        .reused_connection = retry_response.reused_connection,
        .resolve_start_ts_ns = retry_response.resolve_start_ts_ns,
        .resolve_end_ts_ns = retry_response.resolve_end_ts_ns,
        .connect_start_ts_ns = retry_response.connect_start_ts_ns,
        .connect_end_ts_ns = retry_response.connect_end_ts_ns,
        .handshake_start_ts_ns = retry_response.handshake_start_ts_ns,
        .handshake_end_ts_ns = retry_response.handshake_end_ts_ns,
        .write_start_ts_ns = retry_response.write_start_ts_ns,
        .request_sent_ts_ns = retry_response.request_sent_ts_ns,
        .response_recv_ts_ns = retry_response.response_recv_ts_ns,
        .body = {},
        .error_message = "transport_retry_failed",
        .keep_alive = false,
    };
}

bool PersistentHttpSession::start_json_request(HttpRequest request) {
    if (!endpoint_valid_ || connection_ == nullptr || connection_->async_request != nullptr) {
        return false;
    }
    begin_async_request_(std::move(request));
    return connection_->async_request != nullptr;
}

AsyncHttpPollResult PersistentHttpSession::poll_json_request() {
    if (connection_ == nullptr || connection_->async_request == nullptr) {
        return {.status = AsyncHttpRequestStatus::kIdle};
    }

    connection_->io_context.poll();
    if (connection_->async_request == nullptr) {
        return {.status = AsyncHttpRequestStatus::kIdle};
    }
    if (!connection_->async_request->completed) {
        return {.status = AsyncHttpRequestStatus::kInFlight};
    }

    HttpResponse response = std::move(connection_->async_request->response);
    reset_async_request_();
    return {
        .status = AsyncHttpRequestStatus::kCompleted,
        .response = std::move(response),
    };
}

bool PersistentHttpSession::has_inflight_request() const noexcept {
    return connection_ != nullptr && connection_->async_request != nullptr;
}

bool PersistentHttpSession::warm_up() {
    if (!endpoint_valid_ || connection_ == nullptr || connection_->async_request != nullptr) {
        return false;
    }
    const bool connected = ensure_connected_();
    if (connected) {
        last_call_ts_ = std::chrono::steady_clock::now();
    }
    return connected;
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

void PersistentHttpSession::close() noexcept {
    close_connection_();
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
    if (connection_->async_request != nullptr && !connection_->async_request->completed) {
        connection_->async_request->response = build_disconnected_response_("transport_closed");
        connection_->async_request->completed = true;
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

void PersistentHttpSession::reset_async_request_() noexcept {
    if (connection_ == nullptr) {
        return;
    }
    connection_->async_request.reset();
    connection_->io_context.restart();
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
    return run_async_request_to_completion_(request);
}

HttpResponse PersistentHttpSession::run_async_request_to_completion_(HttpRequest request) {
    if (!start_json_request(std::move(request))) {
        if (!endpoint_valid_) {
            return build_disconnected_response_(endpoint_parse_error_);
        }
        return build_disconnected_response_("transport_busy");
    }

    for (;;) {
        auto poll_result = poll_json_request();
        if (poll_result.status == AsyncHttpRequestStatus::kCompleted &&
            poll_result.response.has_value()) {
            return std::move(*poll_result.response);
        }
        if (poll_result.status == AsyncHttpRequestStatus::kIdle) {
            return build_disconnected_response_("transport_disconnected");
        }
        std::this_thread::yield();
    }
}

void PersistentHttpSession::begin_async_request_(HttpRequest request) {
    if (connection_ == nullptr) {
        return;
    }

    connection_->io_context.restart();
    connection_->async_request = std::make_unique<ConnectionState::AsyncRequestState>();
    auto& async = *connection_->async_request;
    async.request = std::move(request);
    async.request_target = build_request_target_(async.request.target);
    async.signing_target = build_signing_target_(async.request_target);
    async.raw_request.version(kHttpVersion);
    async.raw_request.method(to_beast_verb(async.request.method));
    async.raw_request.target(async.request_target);
    async.raw_request.set(http::field::host, endpoint_host_);
    async.raw_request.set(http::field::user_agent, "predex-oms-rest");

    if (!async.request.content_type.empty() && async.request.method != HttpMethod::kGet) {
        async.raw_request.set(http::field::content_type, async.request.content_type);
    }
    if (async.request.authenticate) {
        const auto auth_headers =
            signer_.make_auth_headers(std::string(to_auth_method(async.request.method)),
                                      async.signing_target);
        async.raw_request.set("KALSHI-ACCESS-KEY", auth_headers.key_id);
        async.raw_request.set("KALSHI-ACCESS-TIMESTAMP", auth_headers.timestamp_ms);
        async.raw_request.set("KALSHI-ACCESS-SIGNATURE", auth_headers.signature_base64);
    }
    if (!async.request.body.empty()) {
        async.raw_request.body() = async.request.body;
        async.raw_request.prepare_payload();
    }

    if (connection_->connected && connection_->stream.has_value()) {
        async.response.reused_connection = true;
        begin_async_write_();
        return;
    }

    connection_->stream.emplace(connection_->io_context, connection_->ssl_context);
    auto& stream = *connection_->stream;
    if (!SSL_set_tlsext_host_name(stream.native_handle(), endpoint_host_.c_str())) {
        complete_async_request_(build_disconnected_response_("transport_disconnected"));
        return;
    }

    async.response.reused_connection = false;
    async.response.resolve_start_ts_ns = monotonic_now_ns();
    connection_->resolver.async_resolve(
        endpoint_host_,
        endpoint_port_,
        [this](const beast::error_code& ec, tcp::resolver::results_type results) {
            if (connection_ != nullptr && connection_->async_request != nullptr) {
                connection_->async_request->response.resolve_end_ts_ns = monotonic_now_ns();
            }
            if (ec) {
                complete_async_request_(build_disconnected_response_(
                    transport_error_message("transport_retry", "resolve", ec)));
                return;
            }

            auto& lowest = beast::get_lowest_layer(*connection_->stream);
            lowest.expires_after(kConnectTimeout);
            if (connection_ != nullptr && connection_->async_request != nullptr) {
                connection_->async_request->response.connect_start_ts_ns = monotonic_now_ns();
            }
            lowest.async_connect(
                results,
                [this](const beast::error_code& connect_ec,
                       const tcp::resolver::results_type::endpoint_type&) {
                    if (connection_ != nullptr && connection_->async_request != nullptr) {
                        connection_->async_request->response.connect_end_ts_ns = monotonic_now_ns();
                    }
                    if (connect_ec) {
                        close_connection_();
                        complete_async_request_(build_disconnected_response_(
                            transport_error_message("transport_retry", "connect", connect_ec)));
                        return;
                    }

                    auto& lowest_layer = beast::get_lowest_layer(*connection_->stream);
                    lowest_layer.socket().set_option(net::socket_base::keep_alive{true});
                    lowest_layer.expires_after(kConnectTimeout);
                    if (connection_ != nullptr && connection_->async_request != nullptr) {
                        connection_->async_request->response.handshake_start_ts_ns =
                            monotonic_now_ns();
                    }
                    connection_->stream->async_handshake(
                        ssl::stream_base::client,
                        [this](const beast::error_code& handshake_ec) {
                            if (connection_ != nullptr && connection_->async_request != nullptr) {
                                connection_->async_request->response.handshake_end_ts_ns =
                                    monotonic_now_ns();
                            }
                            if (handshake_ec) {
                                close_connection_();
                                complete_async_request_(
                                    build_disconnected_response_(transport_error_message(
                                        "transport_retry",
                                        "handshake",
                                        handshake_ec)));
                                return;
                            }
                            beast::get_lowest_layer(*connection_->stream).expires_never();
                            connection_->connected = true;
                            begin_async_write_();
                        });
                });
        });
}

void PersistentHttpSession::begin_async_write_() {
    if (connection_ == nullptr || !connection_->stream.has_value() ||
        connection_->async_request == nullptr) {
        complete_async_request_(build_disconnected_response_("transport_disconnected"));
        return;
    }

    auto& async = *connection_->async_request;
    auto& stream = *connection_->stream;
    auto& lowest = beast::get_lowest_layer(stream);
    lowest.expires_after(kIoTimeout);
    async.response.write_start_ts_ns = monotonic_now_ns();
    http::async_write(
        stream,
        async.raw_request,
        [this](const beast::error_code& write_ec, std::size_t) {
            if (write_ec) {
                close_connection_();
                complete_async_request_(build_transport_error_response_(
                    transport_error_message("transport_retry", "write", write_ec),
                    0));
                return;
            }

            if (connection_ == nullptr || connection_->async_request == nullptr ||
                !connection_->stream.has_value()) {
                complete_async_request_(build_disconnected_response_("transport_disconnected"));
                return;
            }

            auto& current_async = *connection_->async_request;
            current_async.response.request_sent_ts_ns = monotonic_now_ns();
            auto& current_stream = *connection_->stream;
            auto& lowest_layer = beast::get_lowest_layer(current_stream);
            lowest_layer.expires_after(kIoTimeout);
            http::async_read(
                current_stream,
                current_async.read_buffer,
                current_async.raw_response,
                [this](const beast::error_code& read_ec, std::size_t) {
                    if (read_ec) {
                        const auto request_sent_ts_ns =
                            (connection_ != nullptr && connection_->async_request != nullptr)
                                ? connection_->async_request->response.request_sent_ts_ns
                                : 0;
                        close_connection_();
                        complete_async_request_(build_transport_error_response_(
                            transport_error_message("transport_retry", "read", read_ec),
                            request_sent_ts_ns));
                        return;
                    }

                    if (connection_ == nullptr || connection_->async_request == nullptr ||
                        !connection_->stream.has_value()) {
                        complete_async_request_(build_disconnected_response_("transport_disconnected"));
                        return;
                    }

                    auto& finished_async = *connection_->async_request;
                    auto& finished_stream = *connection_->stream;
                    auto& lowest_finished = beast::get_lowest_layer(finished_stream);
                    lowest_finished.expires_never();
                    last_call_ts_ = std::chrono::steady_clock::now();

                    const auto response_recv_ts_ns = monotonic_now_ns();
                    const int status_code = static_cast<int>(finished_async.raw_response.result_int());
                    const bool ok = status_code >= 200 && status_code < 300;
                    HttpResponse response{
                        .ok = ok,
                        .status_code = status_code,
                        .retry_count = 0,
                        .reused_connection = finished_async.response.reused_connection,
                        .resolve_start_ts_ns = finished_async.response.resolve_start_ts_ns,
                        .resolve_end_ts_ns = finished_async.response.resolve_end_ts_ns,
                        .connect_start_ts_ns = finished_async.response.connect_start_ts_ns,
                        .connect_end_ts_ns = finished_async.response.connect_end_ts_ns,
                        .handshake_start_ts_ns = finished_async.response.handshake_start_ts_ns,
                        .handshake_end_ts_ns = finished_async.response.handshake_end_ts_ns,
                        .write_start_ts_ns = finished_async.response.write_start_ts_ns,
                        .request_sent_ts_ns = finished_async.response.request_sent_ts_ns,
                        .response_recv_ts_ns = response_recv_ts_ns,
                        .body = finished_async.raw_response.body(),
                        .error_message = ok
                            ? std::string{}
                            : "HTTP " + std::to_string(status_code) +
                                  " body=" + finished_async.raw_response.body(),
                        .keep_alive = finished_async.raw_response.keep_alive(),
                    };
                    if (!response.keep_alive) {
                        close_connection_();
                    }
                    complete_async_request_(std::move(response));
                });
        });
}

void PersistentHttpSession::complete_async_request_(HttpResponse response) noexcept {
    if (connection_ == nullptr || connection_->async_request == nullptr) {
        return;
    }
    connection_->async_request->response = std::move(response);
    connection_->async_request->completed = true;
}

HttpResponse PersistentHttpSession::build_disconnected_response_(
    std::string error_message) const noexcept {
    return build_transport_error_response_(std::move(error_message), 0);
}

HttpResponse PersistentHttpSession::build_transport_error_response_(
    std::string error_message,
    internal::TimestampNs request_sent_ts_ns) const noexcept {
    return {
        .ok = false,
        .status_code = 0,
        .retry_count = 0,
        .reused_connection = false,
        .resolve_start_ts_ns = 0,
        .resolve_end_ts_ns = 0,
        .connect_start_ts_ns = 0,
        .connect_end_ts_ns = 0,
        .handshake_start_ts_ns = 0,
        .handshake_end_ts_ns = 0,
        .write_start_ts_ns = 0,
        .request_sent_ts_ns = request_sent_ts_ns,
        .response_recv_ts_ns = 0,
        .body = {},
        .error_message = std::move(error_message),
        .keep_alive = false,
    };
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
