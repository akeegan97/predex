#include "trading/adapters/exchanges/kalshi/oms_adapter.hpp"

#include <charconv>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

namespace trading::adapters::exchanges::kalshi {
namespace {

std::string action_to_string(internal::OmsAction action) {
    switch (action) {
    case internal::OmsAction::kPlace:
        return "place_order";
    case internal::OmsAction::kCancel:
        return "cancel_order";
    case internal::OmsAction::kReplace:
        return "replace_order";
    case internal::OmsAction::kUnknown:
        return "unknown";
    }
    return "unknown";
}

std::string side_to_string(internal::Side side) {
    switch (side) {
    case internal::Side::kBuy:
        return "buy";
    case internal::Side::kSell:
        return "sell";
    case internal::Side::kBid:
        return "bid";
    case internal::Side::kAsk:
        return "ask";
    case internal::Side::kUnknown:
        return "unknown";
    }
    return "unknown";
}

std::optional<internal::Side> side_from_string(std::string_view side) {
    if (side == "buy") {
        return internal::Side::kBuy;
    }
    if (side == "sell") {
        return internal::Side::kSell;
    }
    if (side == "bid") {
        return internal::Side::kBid;
    }
    if (side == "ask") {
        return internal::Side::kAsk;
    }
    return std::nullopt;
}

std::string tif_to_string(internal::OmsTimeInForce tif) {
    switch (tif) {
    case internal::OmsTimeInForce::kGtc:
        return "gtc";
    case internal::OmsTimeInForce::kIoc:
        return "ioc";
    case internal::OmsTimeInForce::kFok:
        return "fok";
    case internal::OmsTimeInForce::kUnknown:
        return "unknown";
    }
    return "unknown";
}

std::optional<std::string> try_get_string(const nlohmann::json& object, std::string_view key) {
    const auto field_it = object.find(key);
    if (field_it == object.end() || !field_it->is_string()) {
        return std::nullopt;
    }
    return field_it->get<std::string>();
}

std::optional<internal::QtyLots> try_get_qty(const nlohmann::json& object, std::string_view key) {
    const auto field_it = object.find(key);
    if (field_it == object.end() || !field_it->is_number_integer()) {
        return std::nullopt;
    }
    return field_it->get<internal::QtyLots>();
}

std::optional<internal::PriceTicks> try_get_price(const nlohmann::json& object,
                                                  std::string_view key) {
    const auto field_it = object.find(key);
    if (field_it == object.end()) {
        return std::nullopt;
    }

    constexpr internal::PriceTicks kCentToSubcentScale = 100;
    if (field_it->is_number_integer()) {
        const auto cents = field_it->get<internal::PriceTicks>();
        if (cents < 0 || cents > std::numeric_limits<internal::PriceTicks>::max() / kCentToSubcentScale) {
            return std::nullopt;
        }
        return cents * kCentToSubcentScale;
    }
    if (field_it->is_number_float()) {
        const auto dollars = field_it->get<double>();
        if (dollars < 0.0) {
            return std::nullopt;
        }
        const double scaled = std::round(dollars * 10000.0);
        if (scaled < 0.0 ||
            scaled > static_cast<double>(std::numeric_limits<internal::PriceTicks>::max())) {
            return std::nullopt;
        }
        return static_cast<internal::PriceTicks>(scaled);
    }
    if (field_it->is_string()) {
        const auto value = field_it->get<std::string>();
        if (value.empty() || value.front() == '-' || value.front() == '+') {
            return std::nullopt;
        }

        const std::size_t dot = value.find('.');
        const std::string_view int_part =
            (dot == std::string::npos) ? std::string_view{value} : std::string_view{value}.substr(0, dot);
        const std::string_view frac_part =
            (dot == std::string::npos) ? std::string_view{} : std::string_view{value}.substr(dot + 1);
        if (int_part.empty()) {
            return std::nullopt;
        }
        for (const char digit_char : int_part) {
            if (digit_char < '0' || digit_char > '9') {
                return std::nullopt;
            }
        }
        for (const char digit_char : frac_part) {
            if (digit_char < '0' || digit_char > '9') {
                return std::nullopt;
            }
        }

        std::uint64_t dollars = 0;
        const auto [ptr, ec] =
            std::from_chars(int_part.data(), int_part.data() + int_part.size(), dollars);
        if (ec != std::errc{} || ptr != int_part.data() + int_part.size()) {
            return std::nullopt;
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
            if (subcent_units == 10000U) {
                subcent_units = 0U;
                ++dollars;
            }
        }

        constexpr std::uint64_t kScale = 10000U;
        constexpr auto kMax = static_cast<std::uint64_t>(std::numeric_limits<internal::PriceTicks>::max());
        if (dollars > (kMax - subcent_units) / kScale) {
            return std::nullopt;
        }
        return static_cast<internal::PriceTicks>(dollars * kScale + subcent_units);
    }

    return std::nullopt;
}

std::optional<internal::Side> try_get_side(const nlohmann::json& object, std::string_view key) {
    const auto side_value = try_get_string(object, key);
    if (!side_value.has_value()) {
        return std::nullopt;
    }
    return side_from_string(*side_value);
}

oms::ParseResult<internal::OrderStateUpdate>
parse_ack(const nlohmann::json& root, const nlohmann::json& message, std::string_view raw_payload) {
    const auto client_order_id = try_get_string(message, "client_order_id");
    const auto market_ticker = try_get_string(message, "market_ticker");
    const auto accepted_qty = try_get_qty(message, "accepted_qty");
    if (!client_order_id.has_value() || !market_ticker.has_value() || !accepted_qty.has_value()) {
        return oms::ParseResult<internal::OrderStateUpdate>::failure(
            oms::ParseError::kMissingField);
    }

    internal::OrderStateUpdate update{
        .exchange = internal::ExchangeId::kKalshi,
        .status = internal::OmsOrderStatus::kAccepted,
        .client_order_id = *client_order_id,
        .exchange_order_id = try_get_string(message, "exchange_order_id"),
        .market_ticker = *market_ticker,
        .recv_ts_ns = root.value("recv_ts_ns", 0U),
        .data =
            internal::OrderAck{
                .client_order_id = *client_order_id,
                .exchange_order_id = try_get_string(message, "exchange_order_id"),
                .market_ticker = *market_ticker,
                .accepted_qty_lots = *accepted_qty,
            },
        .raw_payload = std::string{raw_payload},
    };
    return oms::ParseResult<internal::OrderStateUpdate>::success(std::move(update));
}

oms::ParseResult<internal::OrderStateUpdate> parse_reject(const nlohmann::json& root,
                                                          const nlohmann::json& message,
                                                          std::string_view raw_payload) {
    const auto client_order_id = try_get_string(message, "client_order_id");
    const auto reason_code = try_get_string(message, "reason_code");
    if (!client_order_id.has_value() || !reason_code.has_value()) {
        return oms::ParseResult<internal::OrderStateUpdate>::failure(
            oms::ParseError::kMissingField);
    }

    const auto reason_message =
        try_get_string(message, "reason_message").value_or(std::string{"unspecified"});
    internal::OrderStateUpdate update{
        .exchange = internal::ExchangeId::kKalshi,
        .status = internal::OmsOrderStatus::kRejected,
        .client_order_id = *client_order_id,
        .exchange_order_id = try_get_string(message, "exchange_order_id"),
        .market_ticker = try_get_string(message, "market_ticker").value_or(std::string{}),
        .recv_ts_ns = root.value("recv_ts_ns", 0U),
        .data =
            internal::OrderReject{
                .client_order_id = *client_order_id,
                .exchange_order_id = try_get_string(message, "exchange_order_id"),
                .reason_code = *reason_code,
                .reason_message = reason_message,
            },
        .raw_payload = std::string{raw_payload},
    };
    return oms::ParseResult<internal::OrderStateUpdate>::success(std::move(update));
}

oms::ParseResult<internal::OrderStateUpdate> parse_fill(const nlohmann::json& root,
                                                        const nlohmann::json& message,
                                                        std::string_view raw_payload) {
    const auto client_order_id = try_get_string(message, "client_order_id");
    const auto market_ticker = try_get_string(message, "market_ticker");
    const auto fill_qty = try_get_qty(message, "fill_qty");
    const auto fill_price = try_get_price(message, "fill_price").has_value()
                                ? try_get_price(message, "fill_price")
                                : try_get_price(message, "fill_price_dollars");
    if (!client_order_id.has_value() || !market_ticker.has_value() || !fill_qty.has_value() ||
        !fill_price.has_value()) {
        return oms::ParseResult<internal::OrderStateUpdate>::failure(
            oms::ParseError::kMissingField);
    }

    internal::OrderStateUpdate update{
        .exchange = internal::ExchangeId::kKalshi,
        .status = internal::OmsOrderStatus::kFilled,
        .client_order_id = *client_order_id,
        .exchange_order_id = try_get_string(message, "exchange_order_id"),
        .market_ticker = *market_ticker,
        .recv_ts_ns = root.value("recv_ts_ns", 0U),
        .data =
            internal::OrderFill{
                .client_order_id = *client_order_id,
                .exchange_order_id = try_get_string(message, "exchange_order_id"),
                .market_ticker = *market_ticker,
                .fill_qty_lots = *fill_qty,
                .fill_price_ticks = *fill_price,
                .side = try_get_side(message, "side").value_or(internal::Side::kUnknown),
                .liquidity = internal::OmsLiquidity::kUnknown,
            },
        .raw_payload = std::string{raw_payload},
    };
    return oms::ParseResult<internal::OrderStateUpdate>::success(std::move(update));
}

} // namespace

internal::ExchangeId OmsAdapter::exchange_id() const { return internal::ExchangeId::kKalshi; }

std::string OmsAdapter::name() const { return "kalshi-oms"; }

bool OmsAdapter::supports(const internal::OrderIntent& intent) const {
    return intent.exchange == internal::ExchangeId::kKalshi &&
           intent.action != internal::OmsAction::kUnknown;
}

std::string OmsAdapter::build_request(const internal::OrderIntent& intent) const {
    if (!supports(intent)) {
        throw std::invalid_argument("Kalshi OmsAdapter does not support this order intent");
    }

    switch (intent.action) {
    case internal::OmsAction::kPlace:
        return build_place_request(intent);
    case internal::OmsAction::kCancel:
        return build_cancel_request(intent);
    case internal::OmsAction::kReplace:
        return build_replace_request(intent);
    case internal::OmsAction::kUnknown:
        break;
    }
    throw std::invalid_argument("Unknown OMS action");
}

oms::ParseResult<internal::OrderStateUpdate>
OmsAdapter::parse_update(std::string_view payload) const {
    try {
        const nlohmann::json root = nlohmann::json::parse(payload);
        if (!root.is_object()) {
            return oms::ParseResult<internal::OrderStateUpdate>::failure(
                oms::ParseError::kInvalidJson);
        }
        const auto type = try_get_string(root, "type");
        const auto message_it = root.find("msg");
        if (!type.has_value() || message_it == root.end() || !message_it->is_object()) {
            return oms::ParseResult<internal::OrderStateUpdate>::failure(
                oms::ParseError::kMissingField);
        }
        const auto& message = *message_it;

        if (*type == "order_ack") {
            return parse_ack(root, message, payload);
        }
        if (*type == "order_reject") {
            return parse_reject(root, message, payload);
        }
        if (*type == "fill") {
            return parse_fill(root, message, payload);
        }
        return oms::ParseResult<internal::OrderStateUpdate>::failure(
            oms::ParseError::kUnsupportedMessageType);
    } catch (...) {
        return oms::ParseResult<internal::OrderStateUpdate>::failure(oms::ParseError::kInvalidJson);
    }
}

std::string OmsAdapter::build_place_request(const internal::OrderIntent& intent) {
    if (intent.client_order_id.empty() || intent.market_ticker.empty() || intent.qty_lots <= 0 ||
        intent.side == internal::Side::kUnknown || !intent.limit_price_ticks.has_value()) {
        throw std::invalid_argument("Invalid place intent");
    }

    const nlohmann::json payload{
        {"action", action_to_string(intent.action)},
        {"client_order_id", intent.client_order_id},
        {"market_ticker", intent.market_ticker},
        {"side", side_to_string(intent.side)},
        {"qty", intent.qty_lots},
        {"limit_price", *intent.limit_price_ticks},
        {"time_in_force", tif_to_string(intent.time_in_force)},
    };
    return payload.dump();
}

std::string OmsAdapter::build_cancel_request(const internal::OrderIntent& intent) {
    if (intent.client_order_id.empty() && !intent.target_client_order_id.has_value() &&
        !intent.target_exchange_order_id.has_value()) {
        throw std::invalid_argument("Invalid cancel intent");
    }

    nlohmann::json payload{
        {"action", action_to_string(intent.action)},
        {"client_order_id", intent.client_order_id},
    };
    if (intent.target_client_order_id.has_value()) {
        payload["target_client_order_id"] = *intent.target_client_order_id;
    }
    if (intent.target_exchange_order_id.has_value()) {
        payload["target_exchange_order_id"] = *intent.target_exchange_order_id;
    }
    return payload.dump();
}

std::string OmsAdapter::build_replace_request(const internal::OrderIntent& intent) {
    if (!intent.target_client_order_id.has_value() &&
        !intent.target_exchange_order_id.has_value()) {
        throw std::invalid_argument("Invalid replace intent");
    }
    if (intent.client_order_id.empty() || intent.qty_lots <= 0 ||
        !intent.limit_price_ticks.has_value()) {
        throw std::invalid_argument("Invalid replace intent payload");
    }

    nlohmann::json payload{
        {"action", action_to_string(intent.action)},
        {"client_order_id", intent.client_order_id},
        {"qty", intent.qty_lots},
        {"limit_price", *intent.limit_price_ticks},
    };
    if (intent.target_client_order_id.has_value()) {
        payload["target_client_order_id"] = *intent.target_client_order_id;
    }
    if (intent.target_exchange_order_id.has_value()) {
        payload["target_exchange_order_id"] = *intent.target_exchange_order_id;
    }
    return payload.dump();
}

} // namespace trading::adapters::exchanges::kalshi
