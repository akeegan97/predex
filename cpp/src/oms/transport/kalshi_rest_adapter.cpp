#include "predex/oms/transport/kalshi_rest_adapter.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <limits>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

namespace predex::core::oms::kalshi::transport {
namespace {

[[nodiscard]] internal::TimestampNs monotonic_now_ns() {
    return static_cast<internal::TimestampNs>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] std::string_view action_from_side(internal::Side side) noexcept {
    switch (side) {
    case internal::Side::kBuy:
        return "buy";
    case internal::Side::kSell:
        return "sell";
    default:
        return {};
    }
}

[[nodiscard]] std::string_view outcome_to_string(Outcome outcome) noexcept {
    switch (outcome) {
    case Outcome::kUnknown:
        return {};
    case Outcome::kYes:
        return "yes";
    case Outcome::kNo:
        return "no";
    }
    return {};
}

[[nodiscard]] std::string_view tif_to_string(TimeInForce tif) noexcept {
    switch (tif) {
    case TimeInForce::kGtc:
        return "good_til_cancelled";
    case TimeInForce::kIoc:
        return "immediate_or_cancel";
    case TimeInForce::kFok:
        return "fill_or_kill";
    }
    return {};
}
constexpr std::size_t kRateLimitHttpCode = 429;
constexpr std::size_t kVenueRangeDownHttpCodeStart = 500;
constexpr std::size_t kVenueRangeDownHttpCodeEnd = 599;
[[nodiscard]] VenueRejectReason
venue_reject_reason_for_response(const HttpResponse& response) noexcept {
    if (response.status_code == kRateLimitHttpCode) {
        return VenueRejectReason::kRateLimit;
    }
    if (response.status_code >= kVenueRangeDownHttpCodeStart && response.status_code <= kVenueRangeDownHttpCodeEnd) {
        return VenueRejectReason::kVenueDown;
    }
    return VenueRejectReason::kUnknown;
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

[[nodiscard]] bool is_unreserved(char car) noexcept {
    const auto uch = static_cast<unsigned char>(car);
    return std::isalnum(uch) != 0 || car == '-' || car == '_' || car == '.' || car == '~';
}
constexpr std::size_t kMaxCents = 99;
[[nodiscard]] std::optional<std::int64_t>
internal_ticks_to_kalshi_price(internal::PriceTicks price_ticks) noexcept {
    if (price_ticks <= 0 || price_ticks >= internal::kMaxPriceTicks) {
        return std::nullopt;
    }
    if (price_ticks % internal::kTicksPerCent != 0) {
        return std::nullopt;
    }

    const auto cents = price_ticks / internal::kTicksPerCent;
    if (cents < 1 || cents > kMaxCents) {
        return std::nullopt;
    }
    return cents;
}

void append_url_encoded(std::string& out, std::string_view value) {
    // NOLINTNEXTLINE
    static constexpr char kHex[] = "0123456789ABCDEF";
    for (const char car : value) {
        if (is_unreserved(car)) {
            out.push_back(car);
            continue;
        }
        out.push_back('%');
        const auto uch = static_cast<unsigned char>(car);
        // NOLINTNEXTLINE
        out.push_back(kHex[(uch >> 4) & 0x0F]);
        // NOLINTNEXTLINE
        out.push_back(kHex[uch & 0x0F]);
    }
}
constexpr std::size_t kReserveBodySizeStart = 256;
void build_submit_body(const SubmitOrderCmd& command,
                       // NOLINTNEXTLINE
                       std::string_view market_ticker,
                       std::string_view action, 
                       std::string_view outcome_side,
                       std::string_view time_in_force, 
                       std::string_view price_field,
                       std::optional<std::int64_t> price_cents, 
                       std::string& body) {
    body.clear();
    body.reserve(kReserveBodySizeStart + market_ticker.size() + command.order.client_order_id.view().size());
    body.push_back('{');
    body.append("\"ticker\":");
    append_json_escaped(body, market_ticker);
    body.append(",\"action\":");
    append_json_escaped(body, action);
    body.append(",\"client_order_id\":");
    append_json_escaped(body, command.order.client_order_id.view());
    body.append(",\"side\":");
    append_json_escaped(body, outcome_side);
    body.append(",\"count_fp\":");
    append_json_escaped(body, internal::format_quantity_fp(command.intent.qty_lots));
    body.append(",\"time_in_force\":");
    append_json_escaped(body, time_in_force);
    if (price_cents.has_value()) {
        body.push_back(',');
        append_json_escaped(body, price_field);
        body.push_back(':');
        body.append(std::to_string(*price_cents));
    }
    body.push_back('}');
}

[[nodiscard]] bool append_submit_order_json_object(const SubmitOrderCmd& command,

                                                std::string_view market_ticker,
                                                    // NOLINTNEXTLINE
                                                std::string& out_body,
                                                std::string& error_message) {
    if (market_ticker.empty()) {
        error_message = "market_ticker is required for submit";
        return false;
    }
    const auto action = action_from_side(command.intent.side);
    if (action.empty()) {
        error_message = "submit_order: order side must be Buy or Sell";
        return false;
    }
    const auto outcome_side = outcome_to_string(command.intent.outcome);
    if (outcome_side.empty()) {
        error_message = "submit_order: outcome must be Yes or No";
        return false;
    }
    const auto tif = tif_to_string(command.intent.time_in_force);
    if (tif.empty()) {
        error_message = "submit_order: unsupported time_in_force";
        return false;
    }

    const std::string_view price_field =
        command.intent.outcome == Outcome::kYes ? "yes_price" : "no_price";
    std::optional<std::int64_t> price_cents;
    if (command.intent.limit_price_ticks.has_value()) {
        price_cents = internal_ticks_to_kalshi_price(*command.intent.limit_price_ticks);
        if (!price_cents.has_value()) {
            error_message =
                "submit_order: limit_price_ticks must convert to Kalshi integer cents in [1,99]";
            return false;
        }
    }
    build_submit_body(command,market_ticker, action, outcome_side, tif, price_field, price_cents, out_body);
    return true;
}

void build_modify_body(const ModifyOrderCmd& command,
                       // NOLINTNEXTLINE
                       std::string_view action, std::string_view outcome_side,
                       std::string_view price_field, std::optional<std::int64_t> price_cents,
                       std::string& body) {
    body.clear();
    body.reserve(kReserveBodySizeStart + command.corr.order.client_order_id.view().size() +
                 command.updated_client_order_id.view().size());
    body.push_back('{');
    body.append("\"client_order_id\":");
    append_json_escaped(body, command.corr.order.client_order_id.view());
    body.append(",\"updated_client_order_id\":");
    append_json_escaped(body, command.updated_client_order_id.view());
    body.append(",\"side\":");
    append_json_escaped(body, outcome_side);
    body.append(",\"action\":");
    append_json_escaped(body, action);
    body.append(",\"count_fp\":");
    append_json_escaped(body, internal::format_quantity_fp(command.replacement.qty_lots));
    if (price_cents.has_value()) {
        body.push_back(',');
        append_json_escaped(body, price_field);
        body.push_back(':');
        body.append(std::to_string(*price_cents));
    }
    body.push_back('}');
}

[[nodiscard]] internal::QtyLots parse_count_fp_to_lots(std::string_view value) {
    internal::QtyLots qty_lots = 0;
    if (!internal::parse_non_negative_quantity_fp(value, qty_lots)) {
        return 0;
    }
    return qty_lots;
}

[[nodiscard]] bool parse_non_negative_dollars_to_ticks(std::string_view value,
                                                       internal::PriceTicks& out_ticks);

[[nodiscard]] std::optional<internal::PriceTicks>
parse_snapshot_price_ticks(const nlohmann::json& order_json, Outcome outcome) {
    internal::PriceTicks parsed_ticks = 0;
    const nlohmann::json* price_text = nullptr;

    if (outcome == Outcome::kYes) {
        if (order_json.contains("yes_price_dollars") &&
            order_json["yes_price_dollars"].is_string()) {
            price_text = &order_json["yes_price_dollars"];
        }
    } else {
        if (order_json.contains("no_price_dollars") &&
            order_json["no_price_dollars"].is_string()) {
            price_text = &order_json["no_price_dollars"];
        }
    }
    if (price_text == nullptr) {
        return std::nullopt;
    }
    if (!parse_non_negative_dollars_to_ticks(price_text->get_ref<const std::string&>(),
                                             parsed_ticks)) {
        return std::nullopt;
    }
    return parsed_ticks;
}

[[nodiscard]] std::string normalized_order_status(const nlohmann::json& order_json) {
    if (!order_json.contains("status") || !order_json["status"].is_string()) {
        return {};
    }
    std::string status = order_json["status"].get<std::string>();
    std::transform(status.begin(), status.end(), status.begin(),
                   [](unsigned char car) { return static_cast<char>(std::tolower(car)); });
    return status;
}
constexpr std::uint64_t kMultiplier = 10;
[[nodiscard]] bool parse_non_negative_dollars_to_ticks(std::string_view value,
                                                       internal::PriceTicks& out_ticks) {
    constexpr auto kDollarToTicksScale =
        static_cast<std::uint64_t>(internal::kPriceTicksPerDollar);
    constexpr auto kI64MaxAsU64 =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());

    if (value.empty() || value.front() == '-' || value.front() == '+') {
        return false;
    }
    const std::size_t dot_pos = value.find('.');
    const std::string_view int_part =
        dot_pos == std::string_view::npos ? value : value.substr(0, dot_pos);
    const std::string_view frac_part =
        dot_pos == std::string_view::npos ? std::string_view{} : value.substr(dot_pos + 1);
    if (int_part.empty()) {
        return false;
    }
    for (char digit_char : int_part) {
        if (digit_char < '0' || digit_char > '9') {
            return false;
        }
    }
    for (char frac_char : frac_part) {
        if (frac_char < '0' || frac_char > '9') {
            return false;
        }
    }

    std::uint64_t dollars = 0;
    const auto [ptr, ec] =
        std::from_chars(int_part.data(), int_part.data() + int_part.size(), dollars);
    if (ec != std::errc{} || ptr != int_part.data() + int_part.size()) {
        return false;
    }

    std::uint64_t subcent_units = 0;
    const std::size_t digits_to_take =
        std::min<std::size_t>(frac_part.size(), internal::kPriceDecimalPlaces);
    for (std::size_t index = 0; index < digits_to_take; ++index) {
        subcent_units = subcent_units * kMultiplier + static_cast<std::uint64_t>(frac_part[index] - '0');
    }
    for (std::size_t index = digits_to_take; index < internal::kPriceDecimalPlaces; ++index) {
        subcent_units *= kMultiplier;
    }
    if (frac_part.size() > internal::kPriceDecimalPlaces &&
        frac_part[internal::kPriceDecimalPlaces] >= '5') {
        ++subcent_units;
        if (subcent_units == kDollarToTicksScale) {
            subcent_units = 0U;
            ++dollars;
        }
    }

    if (dollars > (kI64MaxAsU64 - subcent_units) / kDollarToTicksScale) {
        return false;
    }
    out_ticks = static_cast<internal::PriceTicks>(dollars * kDollarToTicksScale + subcent_units);
    return true;
}

} // namespace

KalshiRestAdapter::KalshiRestAdapter(PersistentHttpSession session, const MarketTickerMap* market_tickers_by_id)
    : session_(std::move(session)), market_tickers_by_id_(market_tickers_by_id) {}

std::string_view
KalshiRestAdapter::resolve_market_ticker_(internal::MarketId market_id) const noexcept {
    if (market_tickers_by_id_ == nullptr) {
        return {};
    }

    const auto itr = market_tickers_by_id_->find(market_id);
    if (itr == market_tickers_by_id_->end()) {
        return {};
    }

    return itr->second;
}
CommandResult KalshiRestAdapter::submit_order(const SubmitOrderCmd& command) {
    const auto prepared = prepare_submit_order(command);
    if (!prepared.ok) {
        return {.ok = false, .error_message = prepared.error_message};
    }
    const auto response = session_.send_json_request(prepared.request);
    RestTraceInfo trace = prepared.trace;
    trace.http_status_code = response.status_code;
    trace.retry_count = response.retry_count;
    trace.reused_connection = response.reused_connection;
    trace.resolve_start_ts_ns = response.resolve_start_ts_ns;
    trace.resolve_end_ts_ns = response.resolve_end_ts_ns;
    trace.connect_start_ts_ns = response.connect_start_ts_ns;
    trace.connect_end_ts_ns = response.connect_end_ts_ns;
    trace.handshake_start_ts_ns = response.handshake_start_ts_ns;
    trace.handshake_end_ts_ns = response.handshake_end_ts_ns;
    trace.write_start_ts_ns = response.write_start_ts_ns;
    trace.request_sent_ts_ns = response.request_sent_ts_ns;
    trace.response_recv_ts_ns = response.response_recv_ts_ns;
    trace.response_body = response.body;
    trace.error_message = response.error_message;
    return parse_submit_response_(response, command, std::move(trace));
}

CommandResult KalshiRestAdapter::cancel_order(const CancelOrderCmd& command) {
    const auto prepared = prepare_cancel_order(command);
    if (!prepared.ok) {
        return {.ok = false, .error_message = prepared.error_message};
    }
    const auto response = session_.send_json_request(prepared.request);
    RestTraceInfo trace = prepared.trace;
    trace.http_status_code = response.status_code;
    trace.retry_count = response.retry_count;
    trace.reused_connection = response.reused_connection;
    trace.resolve_start_ts_ns = response.resolve_start_ts_ns;
    trace.resolve_end_ts_ns = response.resolve_end_ts_ns;
    trace.connect_start_ts_ns = response.connect_start_ts_ns;
    trace.connect_end_ts_ns = response.connect_end_ts_ns;
    trace.handshake_start_ts_ns = response.handshake_start_ts_ns;
    trace.handshake_end_ts_ns = response.handshake_end_ts_ns;
    trace.write_start_ts_ns = response.write_start_ts_ns;
    trace.request_sent_ts_ns = response.request_sent_ts_ns;
    trace.response_recv_ts_ns = response.response_recv_ts_ns;
    trace.response_body = response.body;
    trace.error_message = response.error_message;
    return parse_cancel_response_(response, command, std::move(trace));
}

CommandResult KalshiRestAdapter::modify_order(const ModifyOrderCmd& command) {
    const auto prepared = prepare_modify_order(command);
    if (!prepared.ok) {
        return {.ok = false, .error_message = prepared.error_message};
    }
    const auto response = session_.send_json_request(prepared.request);
    RestTraceInfo trace = prepared.trace;
    trace.http_status_code = response.status_code;
    trace.retry_count = response.retry_count;
    trace.reused_connection = response.reused_connection;
    trace.resolve_start_ts_ns = response.resolve_start_ts_ns;
    trace.resolve_end_ts_ns = response.resolve_end_ts_ns;
    trace.connect_start_ts_ns = response.connect_start_ts_ns;
    trace.connect_end_ts_ns = response.connect_end_ts_ns;
    trace.handshake_start_ts_ns = response.handshake_start_ts_ns;
    trace.handshake_end_ts_ns = response.handshake_end_ts_ns;
    trace.write_start_ts_ns = response.write_start_ts_ns;
    trace.request_sent_ts_ns = response.request_sent_ts_ns;
    trace.response_recv_ts_ns = response.response_recv_ts_ns;
    trace.response_body = response.body;
    trace.error_message = response.error_message;
    return parse_modify_response_(response, command, std::move(trace));
}

OpenOrdersPage KalshiRestAdapter::fetch_open_orders(std::size_t limit,
    //NOLINTNEXTLINE -- accepting the copy of cursor for trace info capture
                                                    std::optional<std::string> cursor) {
    const auto response = session_.send_json_request(HttpRequest{
        .method = HttpMethod::kGet,
        .target = build_open_orders_target_(limit, cursor),
    });
    return parse_open_orders_response_(response);
}

PreparedCommandRequest
KalshiRestAdapter::prepare_submit_order(const SubmitOrderCmd& command) const {
    thread_local std::string body_scratch;
    std::string error_message;
    const std::string_view market_ticker = resolve_market_ticker_(command.market_id);
    if (!append_submit_order_json_object(command, market_ticker, body_scratch, error_message)) {
        return {.ok = false, .error_message = std::move(error_message)};
    }

    const auto target = build_submit_target_();
    return PreparedCommandRequest{
        .ok = true,
        .request =
            HttpRequest{
                .method = HttpMethod::kPost,
                .target = target,
                .body = body_scratch,
            },
        .trace =
            RestTraceInfo{
                .request_target = target,
                .request_body = body_scratch,
            },
    };
}
constexpr std::size_t kBodyReserveSizePerOrder = 512;
PreparedCommandRequest KalshiRestAdapter::prepare_batched_submit_orders(
    const std::vector<SubmitOrderCmd>& commands) const {
    if (commands.empty()) {
        return {.ok = false, .error_message = "batched submit requires at least one command"};
    }

    thread_local std::string body_scratch;
    thread_local std::string order_scratch;
    body_scratch.clear();
    body_scratch.reserve(kBodyReserveSizePerOrder * commands.size());
    body_scratch.append("{\"orders\":[");
    for (std::size_t index = 0; index < commands.size(); ++index) {
        std::string error_message;
        const std::string_view market_ticker = resolve_market_ticker_(commands[index].market_id);
        if (!append_submit_order_json_object(commands[index], market_ticker, order_scratch, error_message)) {
            return {.ok = false, .error_message = std::move(error_message)};
        }
        if (index > 0) {
            body_scratch.push_back(',');
        }
        body_scratch.append(order_scratch);
    }
    body_scratch.append("]}");

    const auto target = build_batched_submit_target_();
    return PreparedCommandRequest{
        .ok = true,
        .request =
            HttpRequest{
                .method = HttpMethod::kPost,
                .target = target,
                .body = body_scratch,
            },
        .trace =
            RestTraceInfo{
                .request_target = target,
                .request_body = body_scratch,
            },
    };
}

PreparedCommandRequest
KalshiRestAdapter::prepare_cancel_order(const CancelOrderCmd& command) const {
    if (!command.corr.order.exchange_order_id.has_value() ||
        command.corr.order.exchange_order_id->empty()) {
        return {.ok = false, .error_message = "exchange order id required for cancel"};
    }

    const auto target = build_cancel_target_(*command.corr.order.exchange_order_id);
    return PreparedCommandRequest{
        .ok = true,
        .request =
            HttpRequest{
                .method = HttpMethod::kDelete,
                .target = target,
            },
        .trace =
            RestTraceInfo{
                .request_target = target,
                .request_body = {},
            },
    };
}

PreparedCommandRequest
KalshiRestAdapter::prepare_modify_order(const ModifyOrderCmd& command) const {
    if (!command.corr.order.exchange_order_id.has_value() ||
        command.corr.order.exchange_order_id->empty()) {
        return {.ok = false, .error_message = "exchange order id required for amend"};
    }
    if (command.updated_client_order_id.empty()) {
        return {.ok = false, .error_message = "updated_client_order_id is required for amend"};
    }

    const auto action = action_from_side(command.replacement.side);
    if (action.empty()) {
        return {.ok = false, .error_message = "modify_order: order side must be Buy or Sell"};
    }
    const auto outcome_side = outcome_to_string(command.replacement.outcome);
    if (outcome_side.empty()) {
        return {.ok = false, .error_message = "modify_order: outcome must be Yes or No"};
    }

    const std::string_view price_field =
        command.replacement.outcome == Outcome::kYes ? "yes_price" : "no_price";
    thread_local std::string body_scratch;
    std::optional<std::int64_t> price_cents;
    if (command.replacement.limit_price_ticks.has_value()) {
        price_cents = internal_ticks_to_kalshi_price(*command.replacement.limit_price_ticks);
        if (!price_cents.has_value()) {
            return {.ok = false,
                    .error_message = "modify_order: limit_price_ticks must convert to Kalshi "
                                     "integer cents in [1,99]"};
        }
    }
    build_modify_body(command, action, outcome_side, price_field, price_cents, body_scratch);

    const auto target = build_modify_target_(*command.corr.order.exchange_order_id);
    return PreparedCommandRequest{
        .ok = true,
        .request =
            HttpRequest{
                .method = HttpMethod::kPost,
                .target = target,
                .body = body_scratch,
            },
        .trace =
            RestTraceInfo{
                .request_target = target,
                .request_body = body_scratch,
            },
    };
}

CommandResult KalshiRestAdapter::complete_submit_order(const SubmitOrderCmd& command,
                                                       const HttpResponse& response,
                                                       RestTraceInfo trace) const {
    return parse_submit_response_(response, command, std::move(trace));
}

CommandResult
KalshiRestAdapter::complete_batched_submit_orders(const std::vector<SubmitOrderCmd>& commands,
                                                  const HttpResponse& response,
                                                  RestTraceInfo trace) const {
    return parse_batched_submit_response_(response, commands, std::move(trace));
}

CommandResult KalshiRestAdapter::complete_cancel_order(const CancelOrderCmd& command,
                                                       const HttpResponse& response,
                                                       RestTraceInfo trace) const {
    return parse_cancel_response_(response, command, std::move(trace));
}

CommandResult KalshiRestAdapter::complete_modify_order(const ModifyOrderCmd& command,
                                                       const HttpResponse& response,
                                                       RestTraceInfo trace) const {
    return parse_modify_response_(response, command, std::move(trace));
}

bool KalshiRestAdapter::start_prepared_request(const PreparedCommandRequest& request) {
    if (!request.ok) {
        return false;
    }
    return session_.start_json_request(request.request);
}

AsyncHttpPollResult KalshiRestAdapter::poll_active_request() {
    return session_.poll_json_request();
}

bool KalshiRestAdapter::warm_up() { return session_.warm_up(); }

void KalshiRestAdapter::check_and_keep_warm(std::uint64_t threshold_seconds) {
    session_.check_and_keep_warm(threshold_seconds);
}

void KalshiRestAdapter::close() noexcept { session_.close(); }

std::string KalshiRestAdapter::build_submit_target_() { return "/trade-api/v2/portfolio/orders"; }

std::string KalshiRestAdapter::build_batched_submit_target_() {
    return "/trade-api/v2/portfolio/orders/batched";
}

std::string KalshiRestAdapter::build_cancel_target_(const ExchangeOrderId& exchange_order_id) {
    std::string target = "/trade-api/v2/portfolio/orders/";
    target.append(exchange_order_id.view());
    return target;
}

std::string KalshiRestAdapter::build_modify_target_(const ExchangeOrderId& exchange_order_id) {
    std::string target = "/trade-api/v2/portfolio/orders/";
    target.append(exchange_order_id.view());
    target.append("/amend");
    return target;
}

std::string KalshiRestAdapter::build_open_orders_target_(std::size_t limit,
                                                         const std::optional<std::string>& cursor) {
    std::string target = "/trade-api/v2/portfolio/orders?limit=" + std::to_string(limit);
    if (cursor.has_value() && !cursor->empty()) {
        target.append("&cursor=");
        append_url_encoded(target, *cursor);
    }
    return target;
}

CommandResult KalshiRestAdapter::parse_submit_response_(const HttpResponse& response,
                                                        const SubmitOrderCmd& command,
                                                        RestTraceInfo trace) {
    if (!response.ok) {
        CommandResult result{
            .ok = false,
            .error_message = response.error_message,
            .trace = std::move(trace),
        };
        if (response.status_code != 0) {
            result.event = VenueOrderReject{
                .order = command.order,
                .transport_submit_ts_ns = result.trace.request_sent_ts_ns,
                .recv_ts_ns = result.trace.response_recv_ts_ns,
                .http_status_code = static_cast<std::uint16_t>(response.status_code),
                .retry_count = static_cast<std::uint16_t>(response.retry_count),
                .reason = venue_reject_reason_for_response(response),
                .raw_reason_code = std::to_string(response.status_code),
                .raw_reason_message = response.body,
            };
            result.events.push_back(*result.event);
        }
        return result;
    }

    try {
        const auto parsed = nlohmann::json::parse(response.body);
        OmsOrderRef order = command.order;
        if (parsed.contains("order") && parsed["order"].is_object()) {
            const auto& order_json = parsed["order"];
            if (order_json.contains("order_id") && order_json["order_id"].is_string()) {
                ExchangeOrderId exchange_id{};
                const std::string_view order_id = order_json["order_id"].get_ref<const std::string&>();
                if (exchange_id.assign_from(order_id)) {
                    order.exchange_order_id = exchange_id;
                }
            }
        }

        if (!order.exchange_order_id.has_value() || order.exchange_order_id->empty()) {
            trace.error_message = "submit response missing order_id";
            return {.ok = false, .error_message = trace.error_message, .trace = std::move(trace)};
        }

        return {
            .ok = true,
            .event =
                VenueOrderAck{
                    .order = (order),
                    .transport_submit_ts_ns = trace.request_sent_ts_ns,
                    .recv_ts_ns = trace.response_recv_ts_ns,
                    .http_status_code = static_cast<std::uint16_t>(trace.http_status_code),
                    .retry_count = static_cast<std::uint16_t>(trace.retry_count),
                    .accepted_qty_lots = command.intent.qty_lots,
                },
            .events = {VenueOrderAck{
                .order = (order),
                .transport_submit_ts_ns = trace.request_sent_ts_ns,
                .recv_ts_ns = trace.response_recv_ts_ns,
                .http_status_code = static_cast<std::uint16_t>(trace.http_status_code),
                .retry_count = static_cast<std::uint16_t>(trace.retry_count),
                .accepted_qty_lots = command.intent.qty_lots,
            }},
            .trace = std::move(trace),
        };
    } catch (const std::exception& exception) {
        trace.error_message = exception.what();
        return {.ok = false, .error_message = trace.error_message, .trace = std::move(trace)};
    }
}

CommandResult
//NOLINTNEXTLINE
KalshiRestAdapter::parse_batched_submit_response_(const HttpResponse& response,
                                                  const std::vector<SubmitOrderCmd>& commands,
                                                  RestTraceInfo trace) {
    CommandResult result{
        .ok = false,
        .trace = trace,
    };

    if (!response.ok && response.status_code == 0) {
        result.error_message = response.error_message;
        return result;
    }

    auto make_reject_event = [&](const SubmitOrderCmd& command, VenueRejectReason reason,
                                 std::string raw_reason_code, std::string raw_reason_message) {
        return KalshiToOmsEvent{VenueOrderReject{
            .order = command.order,
            .transport_submit_ts_ns = trace.request_sent_ts_ns,
            .recv_ts_ns = trace.response_recv_ts_ns,
            .http_status_code = static_cast<std::uint16_t>(response.status_code),
            .retry_count = static_cast<std::uint16_t>(response.retry_count),
            .reason = reason,
            .raw_reason_code = std::move(raw_reason_code),
            .raw_reason_message = std::move(raw_reason_message),
        }};
    };

    try {
        const auto parsed = nlohmann::json::parse(response.body);
        if (!parsed.contains("orders") || !parsed["orders"].is_array()) {
            if (response.status_code != 0) {
                for (const auto& command : commands) {
                    result.events.push_back(
                        make_reject_event(command, venue_reject_reason_for_response(response),
                                          std::to_string(response.status_code), response.body));
                }
                result.ok = true;
                return result;
            }
            result.error_message = response.error_message.empty()
                                       ? "batched submit response missing orders array"
                                       : response.error_message;
            return result;
        }

        const auto& orders = parsed["orders"];
        if (orders.size() != commands.size()) {
            result.error_message = "batched submit response size mismatch";
            return result;
        }

        result.ok = true;
        result.events.reserve(commands.size());
        for (std::size_t index = 0; index < commands.size(); ++index) {
            const auto& entry = orders[index];
            const auto& command = commands[index];

            if (entry.contains("order") && entry["order"].is_object()) {
                OmsOrderRef order = command.order;
                const auto& order_json = entry["order"];
                if (order_json.contains("order_id") && order_json["order_id"].is_string()) {
                    ExchangeOrderId exchange_id{};
                    const std::string_view order_id = order_json["order_id"].get_ref<const std::string&>();
                    if (exchange_id.assign_from(order_id)) {
                        order.exchange_order_id = exchange_id;
                    }
                }
                const internal::QtyLots accepted_qty_lots =
                    std::max(parse_count_fp_to_lots(order_json.value("initial_count_fp", "")),
                             command.intent.qty_lots);
                const internal::QtyLots fill_qty_lots =
                    parse_count_fp_to_lots(order_json.value("fill_count_fp", ""));
                const internal::QtyLots remaining_qty_lots =
                    parse_count_fp_to_lots(order_json.value("remaining_count_fp", ""));
                const auto fill_price_ticks =
                    parse_snapshot_price_ticks(order_json, command.intent.outcome);
                const auto status = normalized_order_status(order_json);

                result.events.emplace_back(VenueOrderAck{
                    .order = order,
                    .transport_submit_ts_ns = trace.request_sent_ts_ns,
                    .recv_ts_ns = trace.response_recv_ts_ns,
                    .http_status_code = static_cast<std::uint16_t>(response.status_code),
                    .retry_count = static_cast<std::uint16_t>(response.retry_count),
                    .accepted_qty_lots = accepted_qty_lots,
                });
                const OmsOrderRef& emitted_order =
                    std::get<VenueOrderAck>(result.events.back()).order;
                if (fill_qty_lots > 0) {
                    const bool terminal_fill = remaining_qty_lots == 0 && status != "canceled";
                    if (terminal_fill) {
                        result.events.emplace_back(VenueOrderFill{
                            .order = emitted_order,
                            .recv_ts_ns = trace.response_recv_ts_ns,
                            .fill_qty_lots = fill_qty_lots,
                            .fill_price_ticks = fill_price_ticks.value_or(0),
                            .side = command.intent.side,
                        });
                    } else {
                        result.events.emplace_back(VenueOrderPartialFill{
                            .order = emitted_order,
                            .recv_ts_ns = trace.response_recv_ts_ns,
                            .fill_qty_lots = fill_qty_lots,
                            .fill_price_ticks = fill_price_ticks.value_or(0),
                            .side = command.intent.side,
                        });
                    }
                }
                if (status == "canceled") {
                    result.events.emplace_back(VenueOrderCanceled{
                        .order = emitted_order,
                        .recv_ts_ns = trace.response_recv_ts_ns,
                    });
                }
                continue;
            }

            if (entry.contains("error") && entry["error"].is_object()) {
                const auto& error_json = entry["error"];
                const auto raw_code =
                    error_json.value("code", std::to_string(response.status_code));
                const auto raw_message = error_json.value("message", response.body);
                result.events.push_back(make_reject_event(
                    command,
                    response.status_code != 0 ? venue_reject_reason_for_response(response)
                                              : VenueRejectReason::kUnknown,
                    raw_code, raw_message));
                continue;
            }

            result.ok = false;
            result.events.clear();
            result.error_message = "batched submit entry missing order/error";
            return result;
        }

        return result;
    } catch (const std::exception& exception) {
        if (response.status_code != 0) {
            result.ok = true;
            for (const auto& command : commands) {
                result.events.push_back(
                    make_reject_event(command, venue_reject_reason_for_response(response),
                                      std::to_string(response.status_code), response.body));
            }
            return result;
        }
        result.error_message = exception.what();
        return result;
    }
}

CommandResult KalshiRestAdapter::parse_cancel_response_(const HttpResponse& response,
                                                        const CancelOrderCmd& command,
                                                        RestTraceInfo trace) {
    if (!response.ok) {
        CommandResult result{
            .ok = false,
            .error_message = response.error_message,
            .trace = std::move(trace),
        };
        if (response.status_code != 0) {
            result.event = VenueCancelReject{
                .order = command.corr.order,
                .transport_submit_ts_ns = result.trace.request_sent_ts_ns,
                .recv_ts_ns = result.trace.response_recv_ts_ns,
                .http_status_code = static_cast<std::uint16_t>(response.status_code),
                .retry_count = static_cast<std::uint16_t>(response.retry_count),
                .reason = venue_reject_reason_for_response(response),
                .raw_reason_code = std::to_string(response.status_code),
                .raw_reason_message = response.body,
            };
        }
        return result;
    }

    return {
        .ok = true,
        .event =
            VenueCancelAck{
                .order = command.corr.order,
                .transport_submit_ts_ns = trace.request_sent_ts_ns,
                .recv_ts_ns = trace.response_recv_ts_ns,
                .http_status_code = static_cast<std::uint16_t>(trace.http_status_code),
                .retry_count = static_cast<std::uint16_t>(trace.retry_count),
            },
        .trace = std::move(trace),
    };
}

CommandResult KalshiRestAdapter::parse_modify_response_(const HttpResponse& response,
                                                        const ModifyOrderCmd& command,
                                                        RestTraceInfo trace) {
    if (!response.ok) {
        CommandResult result{
            .ok = false,
            .error_message = response.error_message,
            .trace = std::move(trace),
        };
        if (response.status_code != 0) {
            result.event = VenueModifyReject{
                .order = command.corr.order,
                .transport_submit_ts_ns = result.trace.request_sent_ts_ns,
                .recv_ts_ns = result.trace.response_recv_ts_ns,
                .http_status_code = static_cast<std::uint16_t>(response.status_code),
                .retry_count = static_cast<std::uint16_t>(response.retry_count),
                .reason = venue_reject_reason_for_response(response),
                .raw_reason_code = std::to_string(response.status_code),
                .raw_reason_message = response.body,
            };
        }
        return result;
    }

    OmsOrderRef order = command.corr.order;
    order.client_order_id = command.updated_client_order_id;

    return {
        .ok = true,
        .event =
            VenueModifyAck{
                .order = order,
                .transport_submit_ts_ns = trace.request_sent_ts_ns,
                .recv_ts_ns = trace.response_recv_ts_ns,
                .http_status_code = static_cast<std::uint16_t>(trace.http_status_code),
                .retry_count = static_cast<std::uint16_t>(trace.retry_count),
                .working_qty_lots = command.replacement.qty_lots,
                .working_price_ticks = command.replacement.limit_price_ticks,
            },
        .trace = std::move(trace),
    };
}

OpenOrdersPage KalshiRestAdapter::parse_open_orders_response_(const HttpResponse& response) {
    if (!response.ok) {
        return {.ok = false, .error_message = response.error_message};
    }

    OpenOrdersPage page{.ok = true};
    try {
        const auto parsed = nlohmann::json::parse(response.body);
        if (parsed.contains("orders") && parsed["orders"].is_array()) {
            for (const auto& order_json : parsed["orders"]) {
                if (!order_json.is_object()) {
                    continue;
                }

            OpenOrderSnapshot snapshot;

            if (order_json.contains("order_id") && order_json["order_id"].is_string()) {
                const auto& order_id_text = order_json["order_id"].get_ref<const std::string&>();
                ExchangeOrderId exchange_id{};
                if (exchange_id.assign_from(order_id_text)) {
                    snapshot.order.exchange_order_id = exchange_id;
                }
            }

                const auto& order_id_text = order_json["order_id"].get_ref<const std::string&>();
                ExchangeOrderId exchange_id{};
                if (exchange_id.assign_from(order_id_text)) {
                    snapshot.order.exchange_order_id = exchange_id;
                }

                snapshot.ticker = order_json.value("ticker", std::string{});
                snapshot.status = order_json.value("status", std::string{});
                snapshot.side = order_json.value("side", std::string{});
                snapshot.action = order_json.value("action", std::string{});
                snapshot.fill_count_fp = order_json.value("fill_count_fp", std::string{"0.00"});
                snapshot.remaining_count_fp =
                    order_json.value("remaining_count_fp", std::string{"0.00"});
                snapshot.initial_count_fp =
                    order_json.value("initial_count_fp", std::string{"0.00"});
                snapshot.yes_price_dollars = order_json.value("yes_price_dollars", std::string{});
                snapshot.no_price_dollars = order_json.value("no_price_dollars", std::string{});

                page.orders.push_back(std::move(snapshot));
            }
        }

        if (parsed.contains("cursor") && parsed["cursor"].is_string()) {
            const auto cursor = parsed["cursor"].get<std::string>();
            if (!cursor.empty()) {
                page.next_cursor = cursor;
            }
        }
    } catch (const std::exception& exception) {
        return {.ok = false, .error_message = exception.what()};
    }

    return page;
}

} // namespace predex::core::oms::kalshi::transport
