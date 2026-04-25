#include "predex/oms/transport/kalshi_rest_adapter.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cctype>
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

void append_url_encoded(std::string& out, std::string_view value) {
    //NOLINTNEXTLINE
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

void build_submit_body(const SubmitOrderCmd& command,
    //NOLINTNEXTLINE
                       std::string_view action,
                       std::string_view outcome_side,
                       std::string_view time_in_force,
                       std::string_view price_field,
                       std::string& body) {
    body.clear();
    body.reserve(256 + command.market_ticker.size() + command.order.client_order_id.value.size());
    body.push_back('{');
    body.append("\"ticker\":");
    append_json_escaped(body, command.market_ticker);
    body.append(",\"action\":");
    append_json_escaped(body, action);
    body.append(",\"client_order_id\":");
    append_json_escaped(body, command.order.client_order_id.value);
    body.append(",\"side\":");
    append_json_escaped(body, outcome_side);
    body.append(",\"count_fp\":");
    append_json_escaped(body, internal::format_quantity_fp(command.intent.qty_lots));
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

void build_modify_body(const ModifyOrderCmd& command,
    //NOLINTNEXTLINE
                       std::string_view action,
                       std::string_view outcome_side,
                       std::string_view price_field,
                       std::string& body) {
    body.clear();
    body.reserve(256 + command.corr.order.client_order_id.value.size() +
                 command.updated_client_order_id.value.size());
    body.push_back('{');
    body.append("\"client_order_id\":");
    append_json_escaped(body, command.corr.order.client_order_id.value);
    body.append(",\"updated_client_order_id\":");
    append_json_escaped(body, command.updated_client_order_id.value);
    body.append(",\"side\":");
    append_json_escaped(body, outcome_side);
    body.append(",\"action\":");
    append_json_escaped(body, action);
    body.append(",\"count_fp\":");
    append_json_escaped(body, internal::format_quantity_fp(command.replacement.qty_lots));
    if (command.replacement.limit_price_ticks.has_value()) {
        body.push_back(',');
        append_json_escaped(body, price_field);
        body.push_back(':');
        body.append(std::to_string(*command.replacement.limit_price_ticks));
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
                                                        internal::PriceTicks& out_ticks) {
    constexpr std::uint64_t kDollarToTicksScale =
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
        subcent_units = subcent_units * 10U + static_cast<std::uint64_t>(frac_part[index] - '0');
    }
    for (std::size_t index = digits_to_take; index < internal::kPriceDecimalPlaces; ++index) {
        subcent_units *= 10U;
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

KalshiRestAdapter::KalshiRestAdapter(PersistentHttpSession session)
    : session_(std::move(session)) {}

CommandResult KalshiRestAdapter::submit_order(const SubmitOrderCmd& command) {
    if (command.market_ticker.empty()) {
        return {.ok = false, .error_message = "market_ticker is required for submit"};
    }

    const auto action = action_from_side(command.intent.side);
    if (action.empty()) {
        return {.ok = false, .error_message = "submit_order: order side must be Buy or Sell"};
    }
    const auto outcome_side = outcome_to_string(command.intent.outcome);
    if (outcome_side.empty()) {
        return {.ok = false, .error_message = "submit_order: outcome must be Yes or No"};
    }
    const auto tif = tif_to_string(command.intent.time_in_force);
    if (tif.empty()) {
        return {.ok = false, .error_message = "submit_order: unsupported time_in_force"};
    }

    const std::string_view price_field =
        command.intent.outcome == Outcome::kYes ? "yes_price" : "no_price";
    thread_local std::string body_scratch;
    build_submit_body(command, action, outcome_side, tif, price_field, body_scratch);

    const auto response = session_.send_json_request(HttpRequest{
        .method = HttpMethod::kPost,
        .target = build_submit_target_(),
        .body = body_scratch,
    });
    return parse_submit_response_(response, command);
}

CommandResult KalshiRestAdapter::cancel_order(const CancelOrderCmd& command) {
    if (!command.corr.order.exchange_order_id.has_value() ||
        command.corr.order.exchange_order_id->value.empty()) {
        return {.ok = false, .error_message = "exchange order id required for cancel"};
    }

    const auto response = session_.send_json_request(HttpRequest{
        .method = HttpMethod::kDelete,
        .target = build_cancel_target_(*command.corr.order.exchange_order_id),
    });
    return parse_cancel_response_(response, command);
}

CommandResult KalshiRestAdapter::modify_order(const ModifyOrderCmd& command) {
    if (!command.corr.order.exchange_order_id.has_value() ||
        command.corr.order.exchange_order_id->value.empty()) {
        return {.ok = false, .error_message = "exchange order id required for amend"};
    }
    if (command.updated_client_order_id.value.empty()) {
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
    build_modify_body(command, action, outcome_side, price_field, body_scratch);

    const auto response = session_.send_json_request(HttpRequest{
        .method = HttpMethod::kPost,
        .target = build_modify_target_(*command.corr.order.exchange_order_id),
        .body = body_scratch,
    });
    return parse_modify_response_(response, command);
}

OpenOrdersPage KalshiRestAdapter::fetch_open_orders(std::size_t limit,
                                                    std::optional<std::string> cursor) {
    const auto response = session_.send_json_request(HttpRequest{
        .method = HttpMethod::kGet,
        .target = build_open_orders_target_(limit, cursor),
    });
    return parse_open_orders_response_(response);
}

void KalshiRestAdapter::check_and_keep_warm(std::uint64_t threshold_seconds) {
    session_.check_and_keep_warm(threshold_seconds);
}

std::string KalshiRestAdapter::build_submit_target_() {
    return "/trade-api/v2/portfolio/orders";
}

std::string KalshiRestAdapter::build_cancel_target_(const ExchangeOrderId& exchange_order_id) {
    return "/trade-api/v2/portfolio/orders/" + exchange_order_id.value;
}

std::string KalshiRestAdapter::build_modify_target_(const ExchangeOrderId& exchange_order_id) {
    return "/trade-api/v2/portfolio/orders/" + exchange_order_id.value + "/amend";
}

std::string KalshiRestAdapter::build_open_orders_target_(
    std::size_t limit,
    const std::optional<std::string>& cursor) {
    std::string target = "/trade-api/v2/portfolio/orders?limit=" + std::to_string(limit);
    if (cursor.has_value() && !cursor->empty()) {
        target.append("&cursor=");
        append_url_encoded(target, *cursor);
    }
    return target;
}

CommandResult KalshiRestAdapter::parse_submit_response_(const HttpResponse& response,
                                                        const SubmitOrderCmd& command) {
    if (!response.ok) {
        return {.ok = false, .error_message = response.error_message};
    }

    try {
        const auto parsed = nlohmann::json::parse(response.body);
        OmsOrderRef order = command.order;
        if (parsed.contains("order") && parsed["order"].is_object()) {
            const auto& order_json = parsed["order"];
            if (order_json.contains("order_id") && order_json["order_id"].is_string()) {
                order.exchange_order_id = ExchangeOrderId{
                    .value = order_json["order_id"].get<std::string>()};
            }
        }

        if (!order.exchange_order_id.has_value() || order.exchange_order_id->value.empty()) {
            return {.ok = false, .error_message = "submit response missing order_id"};
        }

        return {
            .ok = true,
            .event = VenueOrderAck{
                .order = std::move(order),
                .recv_ts_ns = monotonic_now_ns(),
                .accepted_qty_lots = command.intent.qty_lots,
            },
        };
    } catch (const std::exception& exception) {
        return {.ok = false, .error_message = exception.what()};
    }
}

CommandResult KalshiRestAdapter::parse_cancel_response_(const HttpResponse& response,
                                                        const CancelOrderCmd& command) {
    if (!response.ok) {
        return {.ok = false, .error_message = response.error_message};
    }

    return {
        .ok = true,
        .event = VenueCancelAck{
            .order = command.corr.order,
            .recv_ts_ns = monotonic_now_ns(),
        },
    };
}

CommandResult KalshiRestAdapter::parse_modify_response_(const HttpResponse& response,
                                                        const ModifyOrderCmd& command) {
    if (!response.ok) {
        return {.ok = false, .error_message = response.error_message};
    }

    OmsOrderRef order = command.corr.order;
    order.client_order_id = command.updated_client_order_id;

    return {
        .ok = true,
        .event = VenueModifyAck{
            .order = std::move(order),
            .recv_ts_ns = monotonic_now_ns(),
            .working_qty_lots = command.replacement.qty_lots,
            .working_price_ticks = command.replacement.limit_price_ticks,
        },
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
                snapshot.order.client_order_id = ClientOrderId{
                    .value = order_json.value("client_order_id", std::string{})};

                const std::string order_id = order_json.value("order_id", std::string{});
                if (!order_id.empty()) {
                    snapshot.order.exchange_order_id = ExchangeOrderId{.value = order_id};
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
                snapshot.yes_price_dollars =
                    order_json.value("yes_price_dollars", std::string{});
                snapshot.no_price_dollars =
                    order_json.value("no_price_dollars", std::string{});

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
