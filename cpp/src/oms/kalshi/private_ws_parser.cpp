#include "predex/oms/kalshi/private_ws_parser.hpp"

#include <algorithm>
#include <chrono>
#include <charconv>
#include <cctype>
#include <limits>
#include <string>

#include <nlohmann/json.hpp>

namespace predex::core::oms::kalshi {
namespace {

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

[[nodiscard]] OrderLifecycleEventKind lifecycle_kind_from_text(const std::string& text) {
    const std::string lower = to_lower(text);
    if (lower == "ack" || lower == "accepted" || lower == "order_accepted") {
        return OrderLifecycleEventKind::kAck;
    }
    if (lower == "reject" || lower == "rejected" || lower == "order_rejected") {
        return OrderLifecycleEventKind::kReject;
    }
    if (lower == "partial_fill" || lower == "partial_filled") {
        return OrderLifecycleEventKind::kPartialFill;
    }
    if (lower == "fill" || lower == "filled") {
        return OrderLifecycleEventKind::kFill;
    }
    if (lower == "cancel_ack" || lower == "cancel_accepted") {
        return OrderLifecycleEventKind::kCancelAck;
    }
    if (lower == "cancel_reject" || lower == "cancel_rejected") {
        return OrderLifecycleEventKind::kCancelReject;
    }
    if (lower == "replace_ack" || lower == "replace_accepted" || lower == "modified") {
        return OrderLifecycleEventKind::kReplaceAck;
    }
    if (lower == "replace_reject" || lower == "replace_rejected") {
        return OrderLifecycleEventKind::kReplaceReject;
    }
    if (lower == "canceled" || lower == "cancelled") {
        return OrderLifecycleEventKind::kCanceled;
    }
    return OrderLifecycleEventKind::kReject;
}

[[nodiscard]] OmsOrderStatus order_status_from_text(const std::string& text) {
    const std::string lower = to_lower(text);
    if (lower == "live" || lower == "open" || lower == "resting") {
        return OmsOrderStatus::kLive;
    }
    if (lower == "rejected") {
        return OmsOrderStatus::kRejected;
    }
    if (lower == "partially_filled" || lower == "partial_fill") {
        return OmsOrderStatus::kPartiallyFilled;
    }
    if (lower == "filled" || lower == "executed") {
        return OmsOrderStatus::kFilled;
    }
    if (lower == "pending_cancel") {
        return OmsOrderStatus::kPendingCancel;
    }
    if (lower == "canceled" || lower == "cancelled") {
        return OmsOrderStatus::kCanceled;
    }
    if (lower == "pending_modify") {
        return OmsOrderStatus::kPendingModify;
    }
    if (lower == "replaced" || lower == "modified") {
        return OmsOrderStatus::kReplaced;
    }
    if (lower == "pending_submit") {
        return OmsOrderStatus::kPendingSubmit;
    }
    return OmsOrderStatus::kUnknown;
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

[[nodiscard]] std::optional<bool> read_bool(const nlohmann::json& object,
                                            const char* key) {
    if (!object.contains(key)) {
        return std::nullopt;
    }
    const auto& value = object[key];
    if (!value.is_boolean()) {
        return std::nullopt;
    }
    return value.get<bool>();
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

[[nodiscard]] internal::Side side_from_side_text(const std::string& text) {
    const std::string lower = to_lower(text);
    if (lower == "yes" || lower == "buy" || lower == "bid") {
        return internal::Side::kBuy;
    }
    if (lower == "no" || lower == "sell" || lower == "ask") {
        return internal::Side::kSell;
    }
    return internal::Side::kUnknown;
}
//NOLINTNEXTLINE
[[nodiscard]] bool parse_one_order_event(const nlohmann::json& message,
                                         const std::string& envelope_type,
                                         OrderLifecycleEvent& event) {
    if (!message.is_object()) {
        return false;
    }

    event.recv_ts_ns = monotonic_now_ns();

    const std::string event_type =
        read_string(message, "event_type", read_string(message, "type", envelope_type));
    event.kind = lifecycle_kind_from_text(event_type);

    const std::string status = read_string(message, "status", "unknown");
    event.status = order_status_from_text(status);

    event.client_order_id =
        read_string(message, "client_order_id", read_string(message, "client_id"));

    const std::string exchange_order_id =
        read_string(message, "order_id", read_string(message, "exchange_order_id"));
    if (!exchange_order_id.empty()) {
        event.exchange_order_id = exchange_order_id;
    }

    if (const auto oms_request_id = read_u64(message, "oms_request_id")) {
        event.oms_request_id = *oms_request_id;
    }
    if (const auto event_id = read_u64(message, "event_id")) {
        event.origin.event_id = static_cast<internal::EventId>(*event_id);
    }
    if (const auto market_id = read_u64(message, "market_id")) {
        event.origin.market_id = static_cast<internal::MarketId>(*market_id);
    }

    if (event_type == "user_order" || envelope_type == "user_order") {
        if (event.status == OmsOrderStatus::kLive) {
            event.kind = OrderLifecycleEventKind::kAck;
        } else if (event.status == OmsOrderStatus::kFilled) {
            event.kind = OrderLifecycleEventKind::kFill;
        } else if (event.status == OmsOrderStatus::kCanceled) {
            event.kind = OrderLifecycleEventKind::kCanceled;
        } else if (event.status == OmsOrderStatus::kRejected) {
            event.kind = OrderLifecycleEventKind::kReject;
        }
    } else if (event_type == "fill" || envelope_type == "fill") {
        event.kind = OrderLifecycleEventKind::kFill;
        if (event.status == OmsOrderStatus::kUnknown) {
            event.status = OmsOrderStatus::kFilled;
        }
    }

    if (event.kind == OrderLifecycleEventKind::kFill ||
        event.kind == OrderLifecycleEventKind::kPartialFill) {
        OrderFill fill{};
        fill.raw_action = read_string(message, "action");
        fill.raw_side = read_string(message, "side");
        const auto is_yes = read_bool(message, "is_yes");
        fill.raw_is_yes = is_yes.has_value() ? (*is_yes ? 1 : 0) : -1;

        if (const auto fill_qty = read_i64(message, "fill_qty")) {
            fill.fill_qty_lots = static_cast<internal::QtyLots>(*fill_qty);
        } else if (const auto fill_qty = read_i64(message, "count")) {
            fill.fill_qty_lots = static_cast<internal::QtyLots>(*fill_qty);
        } else if (message.contains("count_fp") && message["count_fp"].is_string()) {
            fill.fill_qty_lots = parse_count_fp_to_lots(message["count_fp"].get<std::string>());
        } else if (message.contains("remaining_count_fp") &&
                   message.contains("initial_count_fp") &&
                   message["remaining_count_fp"].is_string() &&
                   message["initial_count_fp"].is_string()) {
            const internal::QtyLots remaining =
                parse_count_fp_to_lots(message["remaining_count_fp"].get<std::string>());
            const internal::QtyLots initial =
                parse_count_fp_to_lots(message["initial_count_fp"].get<std::string>());
            fill.fill_qty_lots = initial > remaining ? (initial - remaining) : 0;
        }
        if (const auto fill_price = read_i64(message, "fill_price")) {
            fill.fill_price_ticks = static_cast<internal::PriceTicks>(*fill_price);
        } else if (const auto fill_price = read_i64(message, "price")) {
            fill.fill_price_ticks = static_cast<internal::PriceTicks>(*fill_price);
        } else if (message.contains("fill_price_dollars") &&
                   message["fill_price_dollars"].is_string()) {
            const auto fill_price_dollars = message["fill_price_dollars"].get<std::string>();
            static_cast<void>(
                parse_non_negative_dollars_to_ticks(fill_price_dollars, fill.fill_price_ticks));
        } else {
            const std::string yes_price_dollars = read_string(message, "yes_price_dollars");
            const std::string no_price_dollars = read_string(message, "no_price_dollars");
            const std::string side_value = to_lower(fill.raw_side);
            if (side_value == "yes") {
                static_cast<void>(
                    parse_non_negative_dollars_to_ticks(yes_price_dollars, fill.fill_price_ticks));
            } else if (side_value == "no") {
                static_cast<void>(
                    parse_non_negative_dollars_to_ticks(no_price_dollars, fill.fill_price_ticks));
            } else {
                if (!parse_non_negative_dollars_to_ticks(yes_price_dollars, fill.fill_price_ticks)) {
                    static_cast<void>(
                        parse_non_negative_dollars_to_ticks(no_price_dollars, fill.fill_price_ticks));
                }
            }
        }

        fill.side = side_from_action_text(fill.raw_action);
        if (fill.side == internal::Side::kUnknown) {
            fill.side = side_from_side_text(fill.raw_side);
        }
        if (fill.side == internal::Side::kUnknown && is_yes.has_value()) {
            fill.side = *is_yes ? internal::Side::kBuy : internal::Side::kSell;
        }
        event.data = fill;
        return true;
    }

    if (event.kind == OrderLifecycleEventKind::kReject) {
        event.data = OrderReject{
            .reason_code = read_string(message, "reason_code", "reject"),
            .reason_message = read_string(message, "reason", "order rejected"),
        };
        return true;
    }
    if (event.kind == OrderLifecycleEventKind::kCancelReject) {
        event.data = CancelReject{
            .reason_code = read_string(message, "reason_code", "cancel_reject"),
            .reason_message = read_string(message, "reason", "cancel rejected"),
        };
        return true;
    }
    if (event.kind == OrderLifecycleEventKind::kReplaceReject) {
        event.data = ReplaceReject{
            .reason_code = read_string(message, "reason_code", "replace_reject"),
            .reason_message = read_string(message, "reason", "replace rejected"),
        };
        return true;
    }
    if (event.kind == OrderLifecycleEventKind::kAck) {
        event.data = OrderAck{};
        return true;
    }
    if (event.kind == OrderLifecycleEventKind::kCancelAck) {
        event.data = CancelAck{};
        return true;
    }
    if (event.kind == OrderLifecycleEventKind::kReplaceAck) {
        ReplaceAck replace_ack{};
        if (const auto qty = read_i64(message, "new_count")) {
            replace_ack.replaced_qty_lots = static_cast<internal::QtyLots>(*qty);
        }
        if (const auto price = read_i64(message, "new_price")) {
            replace_ack.replaced_limit_price_ticks =
                static_cast<internal::PriceTicks>(*price);
        }
        event.data = replace_ack;
        return true;
    }

    event.data = std::monostate{};
    return true;
}

} // namespace
//NOLINTNEXTLINE
PrivateWsParseStatus PrivateWsParser::parse_message(
    std::string_view payload,
    std::vector<OrderLifecycleEvent>& out_events) const {
    out_events.clear();
    if (out_events.capacity() < kDefaultMaxPrivateWsEventsPerMessage) {
        out_events.reserve(kDefaultMaxPrivateWsEventsPerMessage);
    }

    nlohmann::json root;
    try {
        root = nlohmann::json::parse(payload);
    } catch (const std::exception&) {
        return PrivateWsParseStatus::kInvalidJson;
    }

    std::string root_type;
    if (root.is_object() && root.contains("type") && root["type"].is_string()) {
        root_type = root["type"].get<std::string>();
    }

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

    for (const auto& candidate : candidates) {
        OrderLifecycleEvent event{};
        if (!parse_one_order_event(candidate, root_type, event)) {
            continue;
        }
        if (event.client_order_id.empty() && !event.exchange_order_id.has_value() &&
            event.oms_request_id == 0) {
            continue;
        }
        if (out_events.size() >= kDefaultMaxPrivateWsEventsPerMessage) {
            return PrivateWsParseStatus::kTooManyEvents;
        }
        out_events.push_back(std::move(event));
    }

    return PrivateWsParseStatus::kOk;
}

} // namespace predex::core::oms::kalshi
