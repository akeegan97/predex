#include "predex/websocket/kalshi/rest_client.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>

#include <string_view>
#include <utility>

namespace predex::websocket::kalshi {
    constexpr const std::size_t kRequestVersion = 11;
namespace {

namespace net = boost::asio;
namespace ssl = net::ssl;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

struct EndpointParts {
    std::string scheme;
    std::string host;
    std::string port;
    std::string base_path;
};

[[nodiscard]] bool parse_endpoint(std::string_view endpoint,
                                  EndpointParts& out,
                                  std::string& error) {
    const auto scheme_pos = endpoint.find("://");
    if (scheme_pos == std::string_view::npos) {
        error = "REST endpoint must include scheme";
        return false;
    }

    out.scheme = std::string(endpoint.substr(0, scheme_pos));
    std::transform(out.scheme.begin(), out.scheme.end(), out.scheme.begin(),
                   [](unsigned char car) { return static_cast<char>(std::tolower(car)); });
    if (out.scheme != "https") {
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

void join_path_into(std::string& out, const std::string& base, const std::string& path) {
    out.clear();
    out.reserve(base.size() + path.size() + 1);
    if (base.empty()) {
        out.append(path);
        return;
    }
    out.append(base);
    if (path.empty() || path[0] == '/') {
        out.append(path);
        return;
    }
    out.push_back('/');
    out.append(path);
}

// Kalshi order REST payloads use four orthogonal fields: `action` (buy/sell) and `side`
// (yes/no). `action` comes from the OrderIntent's OrderSide; `side` comes from the Outcome.
// Conflating the two is exactly the bug this client used to have: Side::kSell → "no" meant
// every sell intent was mistranslated from sell-YES into sell-NO.
[[nodiscard]] std::string_view action_from_side(predex::internal::Side side) noexcept {
    switch (side) {
        case predex::internal::Side::kBuy:
            return "buy";
        case predex::internal::Side::kSell:
            return "sell";
        default:
            return {};
    }
}

[[nodiscard]] std::string_view outcome_to_string(predex::core::oms::kalshi::Outcome outcome) noexcept {
    switch (outcome) {
        case predex::core::oms::kalshi::Outcome::kYes:
            return "yes";
        case predex::core::oms::kalshi::Outcome::kNo:
            return "no";
        default:
            return {};
    }
}

// Kalshi's accepted TIF strings. Anything else must fail client-side rather than silently
// default to GTC, which was the historical bug on IoC legs of the arb strategy.
[[nodiscard]] std::string_view tif_to_string(predex::core::oms::kalshi::OmsTimeInForce tif) noexcept {
    switch (tif) {
        case predex::core::oms::kalshi::OmsTimeInForce::kGtc:
            return "good_til_cancelled";
        case predex::core::oms::kalshi::OmsTimeInForce::kIoc:
            return "immediate_or_cancel";
        case predex::core::oms::kalshi::OmsTimeInForce::kFok:
            return "fill_or_kill";
        default:
            return {};
    }
}

void append_json_escaped(std::string& out, std::string_view value) {
    out.push_back('"');
    for (const char car : value) {
        switch (car) {
            case '\\':
                out.append("\\\\");
                break;
            case '"':
                out.append("\\\"");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                out.push_back(car);
                break;
        }
    }
    out.push_back('"');
}

void build_submit_order_body(const predex::core::oms::kalshi::SubmitOrderCmd& command,
    //NOLINTNEXTLINE
                             std::string_view market_ticker,
                             std::string_view action,
                             std::string_view side,
                             std::string_view time_in_force,
                             std::string_view price_field,
                             std::string& body) {
    body.clear();
    body.reserve(192 + market_ticker.size() + command.client_order_id.size());
    body.push_back('{');
    body.append("\"ticker\":");
    append_json_escaped(body, market_ticker);
    body.append(",\"action\":");
    append_json_escaped(body, action);
    body.append(",\"client_order_id\":");
    append_json_escaped(body, command.client_order_id);
    body.append(",\"side\":");
    append_json_escaped(body, side);
    body.append(",\"count\":");
    body.append(std::to_string(command.intent.qty_lots));
    body.append(",\"time_in_force\":");
    append_json_escaped(body, time_in_force);
    if (command.intent.limit_price_ticks.has_value()) {
        body.push_back(',');
        append_json_escaped(body, price_field);
        body.push_back(':');
        body.append(std::to_string(*command.intent.limit_price_ticks));
    }
    body.push_back('}');
}

void build_modify_order_body(const predex::core::oms::kalshi::ModifyOrderCmd& command,
                             std::string_view updated_client_order_id,
    //NOLINTNEXTLINE
                             std::string_view action,
                             std::string_view side,
                             std::string_view price_field,
                             std::string& body) {
    body.clear();
    body.reserve(192 + command.client_order_id.size() + updated_client_order_id.size());
    body.push_back('{');
    body.append("\"client_order_id\":");
    append_json_escaped(body, command.client_order_id);
    body.append(",\"updated_client_order_id\":");
    append_json_escaped(body, updated_client_order_id);
    body.append(",\"side\":");
    append_json_escaped(body, side);
    body.append(",\"action\":");
    append_json_escaped(body, action);
    body.append(",\"count\":");
    body.append(std::to_string(command.replacement_intent.qty_lots));
    if (command.replacement_intent.limit_price_ticks.has_value()) {
        body.push_back(',');
        append_json_escaped(body, price_field);
        body.push_back(':');
        body.append(std::to_string(*command.replacement_intent.limit_price_ticks));
    }
    body.push_back('}');
}

void build_order_target(std::string& target, std::string_view exchange_order_id, std::string_view suffix) {
    constexpr std::string_view kPrefix = "/trade-api/v2/portfolio/orders/";
    target.clear();
    target.reserve(kPrefix.size() + exchange_order_id.size() + suffix.size());
    target.append(kPrefix);
    target.append(exchange_order_id);
    target.append(suffix);
}

[[nodiscard]] bool is_unreserved(char car) noexcept {
    const unsigned char uch = static_cast<unsigned char>(car);
    return std::isalnum(uch) != 0 || car == '-' || car == '_' || car == '.' || car == '~';
}

void append_url_encoded(std::string& out, std::string_view value) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    for (const char car : value) {
        if (is_unreserved(car)) {
            out.push_back(car);
            continue;
        }
        out.push_back('%');
        const auto uch = static_cast<unsigned char>(car);
        out.push_back(kHex[(uch >> 4) & 0x0F]);
        out.push_back(kHex[uch & 0x0F]);
    }
}

void build_open_orders_target(std::string& target,
                              std::size_t limit,
                              const std::optional<std::string>& cursor) {
    constexpr std::string_view kPrefix = "/trade-api/v2/portfolio/orders?limit=";
    target.clear();
    target.reserve(kPrefix.size() + 128);
    target.append(kPrefix);
    target.append(std::to_string(limit));
    if (cursor.has_value() && !cursor->empty()) {
        target.append("&cursor=");
        append_url_encoded(target, *cursor);
    }
}

[[nodiscard]] std::string build_updated_client_order_id(std::string_view current_client_order_id) {
    thread_local std::uint64_t amend_seq = 0;
    std::string updated_client_order_id;
    updated_client_order_id.reserve(current_client_order_id.size() + 24);
    updated_client_order_id.append(current_client_order_id);
    updated_client_order_id.append("-am");
    updated_client_order_id.append(std::to_string(++amend_seq));
    return updated_client_order_id;
}

} // namespace

struct RestClient::ConnectionState {
    net::io_context io_context;
    ssl::context ssl_context{ssl::context::tls_client};
    std::optional<beast::ssl_stream<beast::tcp_stream>> stream;
    bool connected{false};
};

RestClient::RestClient(AuthSigner signer, std::string endpoint)
    : signer_(std::move(signer)),
      endpoint_(std::move(endpoint)),
      connection_(std::make_unique<ConnectionState>()) {
    connection_->ssl_context.set_default_verify_paths();

    EndpointParts endpoint_parts;
    endpoint_valid_ = parse_endpoint(endpoint_, endpoint_parts, endpoint_parse_error_);
    if (endpoint_valid_) {
        endpoint_host_ = std::move(endpoint_parts.host);
        endpoint_port_ = std::move(endpoint_parts.port);
        endpoint_base_path_ = std::move(endpoint_parts.base_path);
    }
}

RestClient::~RestClient() {
    close_connection_();
}

bool RestClient::ensure_connected_() {
    if (!endpoint_valid_) {
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
        lowest.connect(results);

        // SO_KEEPALIVE catches half-open sockets at the TCP layer without requiring an
        // application-level heartbeat. Windows' default 2h keepalive is too long, but
        // enabling it still helps on Linux defaults and behind NATs that drop silent
        // flows. Cheap insurance that pairs with the keep-warm idle ping.
        lowest.socket().set_option(net::socket_base::keep_alive{true});

        stream.handshake(ssl::stream_base::client);
        connection_->connected = true;
        return true;
    } catch (const std::exception&) {
        connection_->stream.reset();
        connection_->connected = false;
        return false;
    }
}

void RestClient::close_connection_() noexcept {
    if (connection_ == nullptr || !connection_->connected) {
        if (connection_ != nullptr) {
            connection_->stream.reset();
        }
        return;
    }
    try {
        beast::error_code ignored;
        //NOLINTNEXTLINE
        (void)connection_->stream->shutdown(ignored);
    } catch (...) {
        // swallow: shutdown failures on an already-dead socket are expected
    }
    connection_->stream.reset();
    connection_->connected = false;
}

RestCallResult RestClient::submit_order(
    const predex::core::oms::kalshi::SubmitOrderCmd& command,
    const std::string& market_ticker) {
    if (market_ticker.empty()) {
        return {.ok = false, .error = "market ticker is required for submit_order"};
    }

    const auto action = action_from_side(command.intent.side);
    if (action.empty()) {
        return {.ok = false, .error = "submit_order: order side must be Buy or Sell"};
    }
    const auto side = outcome_to_string(command.intent.outcome);
    if (side.empty()) {
        return {.ok = false, .error = "submit_order: outcome must be Yes or No"};
    }
    const auto tif = tif_to_string(command.intent.time_in_force);
    if (tif.empty()) {
        return {.ok = false, .error = "submit_order: time_in_force must be GTC, IOC, or FOK"};
    }
    const std::string_view price_field =
        command.intent.outcome == predex::core::oms::kalshi::Outcome::kYes
            ? "yes_price"
            : "no_price";
    thread_local std::string body_scratch;
    build_submit_order_body(command, market_ticker, action, side, tif, price_field, body_scratch);

    RestCallResult result =
        call_json_api("POST", "/trade-api/v2/portfolio/orders", body_scratch);
    if (!result.ok) {
        return result;
    }

    try {
        const auto parsed = nlohmann::json::parse(result.error);
        if (parsed.contains("order") && parsed["order"].is_object()) {
            const auto& order = parsed["order"];
            if (order.contains("order_id") && order["order_id"].is_string()) {
                result.exchange_order_id = order["order_id"].get<std::string>();
            }
        }
    } catch (...) {
        // keep success true even if response JSON shape is unexpected
    }
    return result;
}

RestCallResult RestClient::cancel_order(
    const predex::core::oms::kalshi::CancelOrderCmd& command) {
    if (!command.exchange_order_id.has_value() || command.exchange_order_id->empty()) {
        return {.ok = false, .error = "exchange order id required for cancel"};
    }
    thread_local std::string target_scratch;
    build_order_target(target_scratch, *command.exchange_order_id, "");
    return call_json_api("DELETE", target_scratch, "");
}

RestCallResult RestClient::modify_order(
    const predex::core::oms::kalshi::ModifyOrderCmd& command) {
    if (!command.exchange_order_id.has_value() || command.exchange_order_id->empty()) {
        return {.ok = false, .error = "exchange order id required for amend"};
    }
    const auto action = action_from_side(command.replacement_intent.side);
    if (action.empty()) {
        return {.ok = false, .error = "modify_order: order side must be Buy or Sell"};
    }
    const auto side = outcome_to_string(command.replacement_intent.outcome);
    if (side.empty()) {
        return {.ok = false, .error = "modify_order: outcome must be Yes or No"};
    }
    const std::string_view price_field =
        command.replacement_intent.outcome == predex::core::oms::kalshi::Outcome::kYes
            ? "yes_price"
            : "no_price";
    const std::string updated_client_order_id =
        build_updated_client_order_id(command.client_order_id);
    thread_local std::string body_scratch;
    build_modify_order_body(
        command, updated_client_order_id, action, side, price_field, body_scratch);
    thread_local std::string target_scratch;
    build_order_target(target_scratch, *command.exchange_order_id, "/amend");
    return call_json_api("POST", target_scratch, body_scratch);
}

OpenOrdersResult RestClient::fetch_open_orders(
    std::size_t limit,
    std::optional<std::string> cursor) {
    thread_local std::string target_scratch;
    build_open_orders_target(target_scratch, limit, cursor);
    RestCallResult result = call_json_api("GET", target_scratch, "");
    if (!result.ok) {
        return {.ok = false, .error = result.error};
    }

    OpenOrdersResult open_orders{.ok = true};
    try {
        const auto parsed = nlohmann::json::parse(result.error);
        if (!parsed.contains("orders") || !parsed["orders"].is_array()) {
            return open_orders;
        }
        for (const auto& order : parsed["orders"]) {
            if (!order.is_object()) {
                continue;
            }
            open_orders.orders.push_back(OpenOrderSnapshot{
                .order_id = order.value("order_id", ""),
                .client_order_id = order.value("client_order_id", ""),
                .ticker = order.value("ticker", ""),
                .status = order.value("status", ""),
                .side = order.value("side", ""),
                .action = order.value("action", ""),
                .fill_count_fp = order.value("fill_count_fp", "0.00"),
                .remaining_count_fp = order.value("remaining_count_fp", "0.00"),
                .initial_count_fp = order.value("initial_count_fp", "0.00"),
            });
        }
        if (parsed.contains("cursor") && parsed["cursor"].is_string()) {
            const auto parsed_cursor = parsed["cursor"].get<std::string>();
            if (!parsed_cursor.empty()) {
                open_orders.next_cursor = parsed_cursor;
            }
        }
    } catch (const std::exception& exception) {
        return {.ok = false, .error = exception.what()};
    }
    return open_orders;
}
//NOLINTNEXTLINE
RestCallResult RestClient::call_json_api(const std::string& method,
                                         const std::string& target,
                                         const std::string& body,
                                         bool authenticate) {
    if (!endpoint_valid_) {
        return {.ok = false, .error = endpoint_parse_error_};
    }

    thread_local std::string request_target_scratch;
    join_path_into(request_target_scratch, endpoint_base_path_, target);
    const auto& request_target = request_target_scratch;

    // Kalshi REST signing expects path-only canonicalization (without query string).
    std::string signing_path;
    if (authenticate) {
        const auto query_pos = request_target.find('?');
        signing_path = query_pos == std::string::npos
            ? request_target
            : request_target.substr(0, query_pos);
    }

    // Single-flight retry: on transport failure, tear down the stream, reconnect,
    // and replay the request exactly once. Kalshi deduplicates on client_order_id,
    // so a retry that reaches them twice is safe for submit_order; cancel/modify
    // are idempotent by construction.
    auto attempt = [&]() -> std::optional<RestCallResult> {
        if (!ensure_connected_()) {
            return RestCallResult{.ok = false, .error = "transport_disconnected"};
        }

        try {
            http::request<http::string_body> request;
            request.version(kRequestVersion);
            if (method == "POST") {
                request.method(http::verb::post);
            } else if (method == "GET") {
                request.method(http::verb::get);
            } else if (method == "DELETE") {
                request.method(http::verb::delete_);
            } else {
                return RestCallResult{.ok = false, .error = "unsupported HTTP method"};
            }

            request.target(request_target);
            request.set(http::field::host, endpoint_host_);
            request.set(http::field::user_agent, "predex-rest-client");
            if (method != "GET") {
                request.set(http::field::content_type, "application/json");
            }
            if (authenticate) {
                const auto auth_headers = signer_.make_auth_headers(method, signing_path);
                request.set("KALSHI-ACCESS-KEY", auth_headers.key_id);
                request.set("KALSHI-ACCESS-TIMESTAMP", auth_headers.timestamp_ms);
                request.set("KALSHI-ACCESS-SIGNATURE", auth_headers.signature_base64);
            }
            //NOLINTNEXTLINE
            if (method != "GET" && method != "DELETE") {
                request.body() = body;
                request.prepare_payload();
            } else if (!body.empty()) {
                request.body() = body;
                request.prepare_payload();
            }

            auto& stream = *connection_->stream;
            http::write(stream, request);

            beast::flat_buffer buffer;
            http::response<http::string_body> response;
            http::read(stream, buffer, response);

            last_call_ts_ = std::chrono::steady_clock::now();

            // If the server signals Connection: close, tear down now so the next
            // call reconnects instead of trying to write to a socket Kalshi is
            // about to close.
            if (!response.keep_alive()) {
                close_connection_();
            }

            const bool okay = response.result_int() >= 200 && response.result_int() < 300;
            if (!okay) {
                return RestCallResult{
                    .ok = false,
                    .error = "HTTP " + std::to_string(response.result_int()) +
                        " body=" + response.body(),
                };
            }
            return RestCallResult{.ok = true, .error = response.body()};
        } catch (const std::exception&) {
            close_connection_();
            return std::nullopt;  // signal retry
        }
    };

    if (auto result = attempt()) {
        return *result;
    }
    if (auto result = attempt()) {
        return *result;
    }
    return {.ok = false, .error = "transport_retry_failed"};
}

void RestClient::check_and_keep_warm(std::uint64_t threshold_seconds) {
    if (!endpoint_valid_ || connection_ == nullptr || !connection_->connected) {
        return;
    }
    const auto elapsed_s = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - last_call_ts_).count();
    if (elapsed_s < static_cast<std::int64_t>(threshold_seconds)) {
        return;
    }
    // GET /exchange/status: unauthenticated, 3-field response (exchange_active,
    // trading_active, exchange_estimated_resume_time), does not consume the write-rate
    // budget. Side effect we care about: last_call_ts_ is refreshed and the TLS session
    // stays warm so the next real order avoids the ~100ms handshake cliff.
    (void)call_json_api("GET", "/trade-api/v2/exchange/status", "", /*authenticate=*/false);
}

} // namespace predex::websocket::kalshi
