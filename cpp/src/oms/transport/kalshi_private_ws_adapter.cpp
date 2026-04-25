#include "predex/oms/transport/kalshi_private_ws_adapter.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <charconv>
#include <cctype>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace predex::core::oms::kalshi::transport {
namespace {

constexpr std::size_t kDefaultMaxPrivateWsEventsPerMessage = 32;

[[nodiscard]] internal::TimestampNs monotonic_now_ns() {
    return static_cast<internal::TimestampNs>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

[[nodiscard]] std::string to_lower(std::string value) {
    for (char& car : value) {
        car = static_cast<char>(std::tolower(static_cast<unsigned char>(car)));
    }
    return value;
}

[[nodiscard]] std::optional<std::uint64_t> read_u64(const nlohmann::json& object,
                                                    const char* key) {
    if (!object.contains(key)) {
        return std::nullopt;
    }
    const auto& value = object[key];
    if (value.is_number_unsigned()) {
        return value.get<std::uint64_t>();
    }
    if (value.is_number_integer() && value.get<std::int64_t>() >= 0) {
        return static_cast<std::uint64_t>(value.get<std::int64_t>());
    }
    if (value.is_string()) {
        try {
            return static_cast<std::uint64_t>(std::stoull(value.get<std::string>()));
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

[[nodiscard]] std::optional<std::int64_t> read_i64(const nlohmann::json& object,
                                                   const char* key) {
    if (!object.contains(key)) {
        return std::nullopt;
    }
    const auto& value = object[key];
    if (value.is_number_integer()) {
        return value.get<std::int64_t>();
    }
    if (value.is_number_unsigned()) {
        return static_cast<std::int64_t>(value.get<std::uint64_t>());
    }
    if (value.is_string()) {
        try {
            return static_cast<std::int64_t>(std::stoll(value.get<std::string>()));
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

[[nodiscard]] std::string read_string(const nlohmann::json& object,
                                      const char* key,
                                      std::string fallback = {}) {
    if (!object.contains(key) || !object[key].is_string()) {
        return fallback;
    }
    return object[key].get<std::string>();
}

[[nodiscard]] internal::QtyLots parse_count_fp_to_lots(const std::string& value) {
    if (value.empty()) {
        return 0;
    }
    const std::size_t dot_pos = value.find('.');
    const std::string_view integer_part =
        dot_pos == std::string::npos ? std::string_view(value)
                                     : std::string_view(value).substr(0, dot_pos);
    if (integer_part.empty()) {
        return 0;
    }
    std::int64_t parsed = 0;
    const auto [ptr, ec] =
        std::from_chars(integer_part.data(), integer_part.data() + integer_part.size(), parsed);
    if (ec != std::errc() || ptr != integer_part.data() + integer_part.size() || parsed < 0) {
        return 0;
    }
    return static_cast<internal::QtyLots>(parsed);
}

[[nodiscard]] bool parse_non_negative_dollars_to_ticks(std::string_view value,
                                                       internal::PriceTicks& out_ticks) {
    constexpr std::uint64_t kDollarToTicksScale = 10000U;
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
    const std::size_t digits_to_take = std::min<std::size_t>(frac_part.size(), 4U);
    for (std::size_t index = 0; index < digits_to_take; ++index) {
        subcent_units = subcent_units * 10U + static_cast<std::uint64_t>(frac_part[index] - '0');
    }
    for (std::size_t index = digits_to_take; index < 4U; ++index) {
        subcent_units *= 10U;
    }
    if (frac_part.size() >= 5U && frac_part[4] >= '5') {
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

[[nodiscard]] internal::Side side_from_action_text(const std::string& text) {
    const std::string lower = to_lower(text);
    if (lower == "buy") {
        return internal::Side::kBuy;
    }
    if (lower == "sell") {
        return internal::Side::kSell;
    }
    return internal::Side::kUnknown;
}

[[nodiscard]] OmsOrderRef parse_order_ref(const nlohmann::json& message) {
    OmsOrderRef order{};
    if (const auto oms_request_id = read_u64(message, "oms_request_id")) {
        order.oms_request_id = *oms_request_id;
    }
    const std::string client_order_id =
        read_string(message, "client_order_id", read_string(message, "client_id"));
    order.client_order_id = ClientOrderId{.value = client_order_id};
    const std::string exchange_order_id =
        read_string(message, "order_id", read_string(message, "exchange_order_id"));
    if (!exchange_order_id.empty()) {
        order.exchange_order_id = ExchangeOrderId{.value = exchange_order_id};
    }
    return order;
}

[[nodiscard]] std::vector<nlohmann::json> candidate_messages(const nlohmann::json& root) {
    std::vector<nlohmann::json> candidates;
    candidates.reserve(kDefaultMaxPrivateWsEventsPerMessage);

    if (root.is_object()) {
        if (root.contains("msg") && root["msg"].is_object()) {
            candidates.push_back(root["msg"]);
        }
        if (root.contains("order") && root["order"].is_object()) {
            candidates.push_back(root["order"]);
        }
        if (root.contains("orders") && root["orders"].is_array()) {
            for (const auto& item : root["orders"]) {
                candidates.push_back(item);
            }
        }
        candidates.push_back(root);
    } else if (root.is_array()) {
        for (const auto& item : root) {
            candidates.push_back(item);
        }
    }

    return candidates;
}

} // namespace

KalshiPrivateWsAdapter::KalshiPrivateWsAdapter(predex::websocket::kalshi::WsAdapter ws_adapter)
    : ws_adapter_(std::move(ws_adapter)) {}

const predex::websocket::kalshi::WsAdapter& KalshiPrivateWsAdapter::ws_adapter() const noexcept {
    return ws_adapter_;
}

void KalshiPrivateWsAdapter::reset_sequence_tracking() noexcept {
    last_seq_by_sid_.clear();
}

PrivateWsParseResult KalshiPrivateWsAdapter::parse_message(std::string_view payload) {
    PrivateWsParseResult result;

    nlohmann::json root;
    try {
        root = nlohmann::json::parse(payload);
    } catch (const std::exception& exception) {
        result.error_message = exception.what();
        return result;
    }

    if (root.is_object()) {
        const auto sid = read_u64(root, "sid");
        const auto seq = read_u64(root, "seq");
        if (sid.has_value() && seq.has_value()) {
            auto [it, inserted] = last_seq_by_sid_.emplace(*sid, *seq);
            if (!inserted) {
                if (*seq > it->second + 1) {
                    result.reconcile_request = PrivateWsReconcileRequest{
                        .reason = PrivateWsReconcileReason::kSeqGap};
                }
                if (*seq > it->second) {
                    it->second = *seq;
                }
            }
        }
    }

    const std::string root_type =
        root.is_object() ? read_string(root, "type") : std::string{};

    auto candidates = candidate_messages(root);
    if (candidates.size() > kDefaultMaxPrivateWsEventsPerMessage) {
        result.error_message = "too many events in private ws payload";
        return result;
    }

    const auto recv_ts_ns = monotonic_now_ns();
    for (const auto& candidate : candidates) {
        if (!candidate.is_object()) {
            continue;
        }
        auto event =
            parse_single_event_(root_type, parse_order_ref(candidate), recv_ts_ns, candidate);
        if (event.has_value()) {
            result.events.push_back(std::move(*event));
        }
    }

    return result;
}

std::optional<KalshiToOmsEvent> KalshiPrivateWsAdapter::parse_single_event_(
    const std::string& message_type,
    const OmsOrderRef& order,
    internal::TimestampNs recv_ts_ns,
    const nlohmann::json& message) {
    if (!message.is_object()) {
        return std::nullopt;
    }

    if (order.oms_request_id == 0 && order.client_order_id.value.empty() &&
        (!order.exchange_order_id.has_value() || order.exchange_order_id->value.empty())) {
        return std::nullopt;
    }

    const std::string event_type =
        read_string(message, "event_type", read_string(message, "type", message_type));
    const std::string lower_type = to_lower(event_type);
    const std::string lower_status = to_lower(read_string(message, "status", "unknown"));

    if (lower_type == "subscribed" || lower_type == "pong" || lower_type == "ping" ||
        lower_type == "heartbeat" || lower_type == "welcome") {
        return std::nullopt;
    }

    if (lower_type == "fill" || lower_type == "filled" || lower_type == "partial_fill" ||
        lower_type == "partial_filled") {
        internal::QtyLots fill_qty_lots = 0;
        if (const auto fill_qty = read_i64(message, "fill_qty")) {
            fill_qty_lots = static_cast<internal::QtyLots>(*fill_qty);
        } else if (const auto count = read_i64(message, "count")) {
            fill_qty_lots = static_cast<internal::QtyLots>(*count);
        } else if (message.contains("count_fp") && message["count_fp"].is_string()) {
            fill_qty_lots = parse_count_fp_to_lots(message["count_fp"].get<std::string>());
        } else if (message.contains("remaining_count_fp") && message["remaining_count_fp"].is_string() &&
                   message.contains("initial_count_fp") && message["initial_count_fp"].is_string()) {
            const auto remaining =
                parse_count_fp_to_lots(message["remaining_count_fp"].get<std::string>());
            const auto initial =
                parse_count_fp_to_lots(message["initial_count_fp"].get<std::string>());
            fill_qty_lots = initial > remaining ? (initial - remaining) : 0;
        }

        internal::PriceTicks fill_price_ticks = 0;
        if (const auto fill_price = read_i64(message, "fill_price")) {
            fill_price_ticks = static_cast<internal::PriceTicks>(*fill_price);
        } else if (const auto price = read_i64(message, "price")) {
            fill_price_ticks = static_cast<internal::PriceTicks>(*price);
        } else if (const std::string fill_price_dollars =
                       read_string(message, "fill_price_dollars");
                   !fill_price_dollars.empty()) {
            (void)parse_non_negative_dollars_to_ticks(fill_price_dollars, fill_price_ticks);
        } else {
            const std::string yes_price = read_string(message, "yes_price_dollars");
            const std::string no_price = read_string(message, "no_price_dollars");
            const std::string side = to_lower(read_string(message, "side"));
            if (side == "yes") {
                (void)parse_non_negative_dollars_to_ticks(yes_price, fill_price_ticks);
            } else if (side == "no") {
                (void)parse_non_negative_dollars_to_ticks(no_price, fill_price_ticks);
            } else if (!parse_non_negative_dollars_to_ticks(yes_price, fill_price_ticks)) {
                (void)parse_non_negative_dollars_to_ticks(no_price, fill_price_ticks);
            }
        }

        const auto side = side_from_action_text(read_string(message, "action"));
        if (lower_type == "partial_fill" || lower_type == "partial_filled") {
            return VenueOrderPartialFill{
                .order = order,
                .recv_ts_ns = recv_ts_ns,
                .fill_qty_lots = fill_qty_lots,
                .fill_price_ticks = fill_price_ticks,
                .side = side,
            };
        }
        return VenueOrderFill{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
            .fill_qty_lots = fill_qty_lots,
            .fill_price_ticks = fill_price_ticks,
            .side = side,
        };
    }

    if (lower_type == "reject" || lower_type == "rejected" || lower_type == "order_rejected" ||
        lower_status == "rejected") {
        return VenueOrderReject{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
            .reason = VenueRejectReason::kUnknown,
            .raw_reason_code = read_string(message, "reason_code", "reject"),
            .raw_reason_message = read_string(message, "reason", "order rejected"),
        };
    }

    if (lower_type == "cancel_reject" || lower_type == "cancel_rejected") {
        return VenueCancelReject{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
            .reason = VenueRejectReason::kUnknown,
            .raw_reason_code = read_string(message, "reason_code", "cancel_reject"),
            .raw_reason_message = read_string(message, "reason", "cancel rejected"),
        };
    }

    if (lower_type == "replace_reject" || lower_type == "replace_rejected") {
        return VenueModifyReject{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
            .reason = VenueRejectReason::kUnknown,
            .raw_reason_code = read_string(message, "reason_code", "replace_reject"),
            .raw_reason_message = read_string(message, "reason", "replace rejected"),
        };
    }

    if (lower_type == "replace_ack" || lower_type == "replace_accepted" ||
        lower_type == "modified" || lower_status == "pending_modify") {
        internal::QtyLots working_qty_lots = 0;
        if (const auto qty = read_i64(message, "new_count")) {
            working_qty_lots = static_cast<internal::QtyLots>(*qty);
        } else if (const auto count = read_i64(message, "count")) {
            working_qty_lots = static_cast<internal::QtyLots>(*count);
        } else if (message.contains("remaining_count_fp") && message["remaining_count_fp"].is_string()) {
            working_qty_lots = parse_count_fp_to_lots(message["remaining_count_fp"].get<std::string>());
        }

        std::optional<internal::PriceTicks> working_price_ticks;
        if (const auto price = read_i64(message, "new_price")) {
            working_price_ticks = static_cast<internal::PriceTicks>(*price);
        } else if (const auto price = read_i64(message, "price")) {
            working_price_ticks = static_cast<internal::PriceTicks>(*price);
        }

        return VenueModifyAck{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
            .working_qty_lots = working_qty_lots,
            .working_price_ticks = working_price_ticks,
        };
    }

    if (lower_type == "cancel_ack" || lower_type == "cancel_accepted" ||
        lower_type == "canceled" || lower_type == "cancelled" || lower_status == "canceled" ||
        lower_status == "cancelled") {
        return VenueOrderCanceled{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
        };
    }

    if (lower_type == "ack" || lower_type == "accepted" || lower_type == "order_accepted" ||
        lower_type == "user_order" || lower_status == "open" || lower_status == "resting" ||
        lower_status == "live") {
        internal::QtyLots accepted_qty_lots = 0;
        if (const auto count = read_i64(message, "count")) {
            accepted_qty_lots = static_cast<internal::QtyLots>(*count);
        } else if (message.contains("initial_count_fp") && message["initial_count_fp"].is_string()) {
            accepted_qty_lots = parse_count_fp_to_lots(message["initial_count_fp"].get<std::string>());
        }
        return VenueOrderAck{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
            .accepted_qty_lots = accepted_qty_lots,
        };
    }

    if (lower_status == "filled" || lower_status == "executed") {
        return VenueOrderFill{
            .order = order,
            .recv_ts_ns = recv_ts_ns,
        };
    }

    return std::nullopt;
}

} // namespace predex::core::oms::kalshi::transport
