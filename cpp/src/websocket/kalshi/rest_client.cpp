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

[[nodiscard]] std::string side_to_string(predex::internal::Side side) {
    switch (side) {
        case predex::internal::Side::kBuy:
            return "yes";
        case predex::internal::Side::kSell:
            return "no";
        default:
            return "unknown";
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
    //NOLINTNEXTLINE
    body.append(",\"time_in_force\":\"good_til_cancelled\"");
    if (command.intent.limit_price_ticks.has_value()) {
        body.append(",\"yes_price\":");
        body.append(std::to_string(*command.intent.limit_price_ticks));
    }
    body.push_back('}');
}

void build_modify_order_body(const predex::core::oms::kalshi::ModifyOrderCmd& command,
                             std::string_view updated_client_order_id,
    //NOLINTNEXTLINE
                             std::string_view action,
                             std::string_view side,
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
        body.append(",\"yes_price\":");
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

RestClient::RestClient(AuthSigner signer, std::string endpoint)
    : signer_(std::move(signer)), endpoint_(std::move(endpoint)) {
    EndpointParts endpoint_parts;
    endpoint_valid_ = parse_endpoint(endpoint_, endpoint_parts, endpoint_parse_error_);
    if (endpoint_valid_) {
        endpoint_host_ = std::move(endpoint_parts.host);
        endpoint_port_ = std::move(endpoint_parts.port);
        endpoint_base_path_ = std::move(endpoint_parts.base_path);
    }
}

RestCallResult RestClient::submit_order(
    const predex::core::oms::kalshi::SubmitOrderCmd& command,
    const std::string& market_ticker) const {
    if (market_ticker.empty()) {
        return {.ok = false, .error = "market ticker is required for submit_order"};
    }

    const std::string action = command.intent.side == predex::internal::Side::kBuy
        ? "buy"
        : (command.intent.side == predex::internal::Side::kSell ? "sell" : "buy");
    const std::string side = side_to_string(command.intent.side);
    thread_local std::string body_scratch;
    build_submit_order_body(command, market_ticker, action, side, body_scratch);

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
    const predex::core::oms::kalshi::CancelOrderCmd& command) const {
    if (!command.exchange_order_id.has_value() || command.exchange_order_id->empty()) {
        return {.ok = false, .error = "exchange order id required for cancel"};
    }
    thread_local std::string target_scratch;
    build_order_target(target_scratch, *command.exchange_order_id, "");
    return call_json_api("DELETE", target_scratch, "");
}

RestCallResult RestClient::modify_order(
    const predex::core::oms::kalshi::ModifyOrderCmd& command) const {
    if (!command.exchange_order_id.has_value() || command.exchange_order_id->empty()) {
        return {.ok = false, .error = "exchange order id required for amend"};
    }
    const std::string action = command.replacement_intent.side == predex::internal::Side::kBuy
        ? "buy"
        : (command.replacement_intent.side == predex::internal::Side::kSell ? "sell" : "buy");
    const std::string side = side_to_string(command.replacement_intent.side);
    const std::string updated_client_order_id =
        build_updated_client_order_id(command.client_order_id);
    thread_local std::string body_scratch;
    build_modify_order_body(
        command, updated_client_order_id, action, side, body_scratch);
    thread_local std::string target_scratch;
    build_order_target(target_scratch, *command.exchange_order_id, "/amend");
    return call_json_api("POST", target_scratch, body_scratch);
}

OpenOrdersResult RestClient::fetch_open_orders(
    std::size_t limit,
    std::optional<std::string> cursor) const {
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
                                         const std::string& body) const {
    if (!endpoint_valid_) {
        return {.ok = false, .error = endpoint_parse_error_};
    }

    thread_local std::string request_target_scratch;
    join_path_into(request_target_scratch, endpoint_base_path_, target);
    const auto& request_target = request_target_scratch;
    const auto auth_headers = signer_.make_auth_headers(method, request_target);

    try {
        net::io_context io_context;
        ssl::context ssl_context{ssl::context::tls_client};
        ssl_context.set_default_verify_paths();

        tcp::resolver resolver{io_context};
        beast::ssl_stream<beast::tcp_stream> stream{io_context, ssl_context};

        if (!SSL_set_tlsext_host_name(stream.native_handle(), endpoint_host_.c_str())) {
            return {.ok = false, .error = "failed to set TLS SNI host"};
        }

        auto results = resolver.resolve(endpoint_host_, endpoint_port_);
        beast::get_lowest_layer(stream).connect(results);
        stream.handshake(ssl::stream_base::client);

        http::request<http::string_body> request;
        request.version(kRequestVersion);
        if (method == "POST") {
            request.method(http::verb::post);
        } else if (method == "GET") {
            request.method(http::verb::get);
        } else if (method == "DELETE") {
            request.method(http::verb::delete_);
        } else {
            return {.ok = false, .error = "unsupported HTTP method"};
        }

        request.target(request_target);
        request.set(http::field::host, endpoint_host_);
        request.set(http::field::user_agent, "predex-rest-client");
        if (method != "GET") {
            request.set(http::field::content_type, "application/json");
        }
        request.set("KALSHI-ACCESS-KEY", auth_headers.key_id);
        request.set("KALSHI-ACCESS-TIMESTAMP", auth_headers.timestamp_ms);
        request.set("KALSHI-ACCESS-SIGNATURE", auth_headers.signature_base64);
        //NOLINTNEXTLINE
        if (method != "GET" && method != "DELETE") {
            request.body() = body;
            request.prepare_payload();
        } else if (!body.empty()) {
            request.body() = body;
            request.prepare_payload();
        }

        http::write(stream, request);

        beast::flat_buffer buffer;
        http::response<http::string_body> response;
        http::read(stream, buffer, response);

        beast::error_code shutdown_error;
        //NOLINTNEXTLINE
        (void)stream.shutdown(shutdown_error);

        const bool okay = response.result_int() >= 200 && response.result_int() < 300;
        if (!okay) {
            return {
                .ok = false,
                .error = "HTTP " + std::to_string(response.result_int()) +
                    " body=" + response.body(),
            };
        }
        return {.ok = true, .error = response.body()};
    } catch (const std::exception& exception) {
        return {.ok = false, .error = exception.what()};
    }
}

} // namespace predex::websocket::kalshi
