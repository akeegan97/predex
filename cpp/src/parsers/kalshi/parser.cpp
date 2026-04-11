#include "predex/parsers/kalshi/parser.hpp"
#include "predex/internal/normalized_event.hpp"

#include <charconv>
#include <cmath>
#include <initializer_list>
#include <limits>

#include <simdjson.h>

namespace predex::core::parsers::kalshi {

using predex::parsers::ParseError;
template <typename T>
using ParseResult = predex::parsers::ParseResult<T>;

namespace {
constexpr std::int64_t kMaxPriceTicks = 10000LL;
std::int64_t reciprocal_price(std::int64_t price){
    return kMaxPriceTicks - price;
}
// Kalshi on-demand message type strings.
constexpr std::string_view kTypeOrderbook = "orderbook_snapshot";
constexpr std::string_view kTypeOrderbookDelta = "orderbook_delta";
constexpr std::string_view kTypeTrade = "trade";
constexpr auto kInt64MaxAsU64 = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
constexpr auto kScale = 10000U;
bool get_string(simdjson::ondemand::object& obj, std::string_view key, std::string_view& out) noexcept {
    auto result = obj.find_field_unordered(key).get_string();
    if (result.error() != simdjson::SUCCESS) {
        return false;
    }
    out = result.value_unsafe();
    return true;
}

bool get_int64(simdjson::ondemand::object& obj, std::string_view key, std::int64_t& out) noexcept {
    auto result = obj.find_field_unordered(key).get_int64();
    if (result.error() != simdjson::SUCCESS) {
        return false;
    }
    out = result.value_unsafe();
    return true;
}

bool get_value(simdjson::ondemand::object& obj,
               std::string_view key,
               simdjson::ondemand::value& out) noexcept {
    auto result = obj.find_field_unordered(key);
    if (result.error() != simdjson::SUCCESS) {
        return false;
    }
    out = result.value_unsafe();
    return true;
}

bool parse_signed_decimal_string_to_int64(std::string_view value, std::int64_t& out) noexcept {
    if (value.empty()) {
        return false;
    }

    bool negative = false;
    if (value.front() == '+' || value.front() == '-') {
        negative = value.front() == '-';
        value.remove_prefix(1);
    }
    if (value.empty()) {
        return false;
    }

    const std::size_t dot = value.find('.');
    const std::string_view int_part = (dot == std::string_view::npos) ? value : value.substr(0, dot);
    const std::string_view frac_part =
        (dot == std::string_view::npos) ? std::string_view{} : value.substr(dot + 1);
    if (int_part.empty()) {
        return false;
    }
    for (char digit_char : int_part) {
        if (digit_char < '0' || digit_char > '9') {
            return false;
        }
    }
    for (char frac_char : frac_part) {
        if (frac_char != '0') {
            return false;
        }
    }

    std::uint64_t parsed = 0;
    const auto [ptr, ec] = std::from_chars(int_part.data(), int_part.data() + int_part.size(), parsed);
    if (ec != std::errc{} || ptr != int_part.data() + int_part.size()) {
        return false;
    }

    if ((!negative && parsed > kInt64MaxAsU64) || (negative && parsed > (kInt64MaxAsU64 + 1U))) {
        return false;
    }

    if (!negative) {
        out = static_cast<std::int64_t>(parsed);
        return true;
    }
    if (parsed == kInt64MaxAsU64 + 1U) {
        out = std::numeric_limits<std::int64_t>::min();
        return true;
    }
    out = -static_cast<std::int64_t>(parsed);
    return true;
}

bool parse_non_negative_decimal_string_to_int64(std::string_view value, std::int64_t& out) noexcept {
    if (!parse_signed_decimal_string_to_int64(value, out)) {
        return false;
    }
    if (out < 0) {
        return false;
    }
    return true;
}
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
bool parse_non_negative_dollars_to_ticks(std::string_view value, std::int64_t& out) noexcept {
    if (value.empty() || value.front() == '-' || value.front() == '+') {
        return false;
    }

    const std::size_t dot = value.find('.');
    const std::string_view int_part = (dot == std::string_view::npos) ? value : value.substr(0, dot);
    const std::string_view frac_part =
        (dot == std::string_view::npos) ? std::string_view{} : value.substr(dot + 1);
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
    const auto [ptr, ec] = std::from_chars(int_part.data(), int_part.data() + int_part.size(), dollars);
    if (ec != std::errc{} || ptr != int_part.data() + int_part.size()) {
        return false;
    }

    std::uint64_t subcent_units = 0;
    if (!frac_part.empty()) {
        const std::size_t digits_to_take = std::min<std::size_t>(frac_part.size(), 4U);
        for (std::size_t index = 0; index < digits_to_take; ++index) {
            //NOLINTNEXTLINE(readability-magic-numbers)
            subcent_units = subcent_units * 10U + static_cast<std::uint64_t>(frac_part[index] - '0');
        }
        for (std::size_t index = digits_to_take; index < 4U; ++index) {
            //NOLINTNEXTLINE(readability-magic-numbers)
            subcent_units *= 10U;
        }
        //NOLINTNEXTLINE(readability-magic-numbers)
        if (frac_part.size() >= 5U && frac_part[4] >= '5') {
            ++subcent_units;
            if (subcent_units == kScale) {
                subcent_units = 0U;
                ++dollars;
            }
        }
    }


    if (dollars > (kInt64MaxAsU64 - subcent_units) / kScale) {
        return false;
    }
    out = static_cast<std::int64_t>(dollars * kScale + subcent_units);
    return true;
}

bool parse_lot_count_value(simdjson::ondemand::value& value, std::int64_t& out) noexcept {
    auto string_result = value.get_string();
    if (string_result.error() == simdjson::SUCCESS) {
        return parse_signed_decimal_string_to_int64(string_result.value_unsafe(), out);
    }

    auto int_result = value.get_int64();
    if (int_result.error() == simdjson::SUCCESS) {
        out = int_result.value_unsafe();
        return true;
    }

    auto double_result = value.get_double();
    if (double_result.error() != simdjson::SUCCESS) {
        return false;
    }
    const double as_double = double_result.value_unsafe();
    double integer_part = 0.0;
    if (std::modf(as_double, &integer_part) != 0.0 ||
        integer_part < static_cast<double>(std::numeric_limits<std::int64_t>::min()) ||
        integer_part > static_cast<double>(std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    out = static_cast<std::int64_t>(integer_part);
    return true;
}

bool parse_price_ticks_value(simdjson::ondemand::value& value, std::int64_t& out) noexcept {
    auto string_result = value.get_string();
    if (string_result.error() == simdjson::SUCCESS) {
        return parse_non_negative_dollars_to_ticks(string_result.value_unsafe(), out);
    }

    auto int_result = value.get_int64();
    if (int_result.error() == simdjson::SUCCESS) {
        constexpr std::int64_t kCentToSubcentScale = 100;
        const auto cents_value = int_result.value_unsafe();
        if (cents_value < 0 ||
            cents_value > std::numeric_limits<std::int64_t>::max() / kCentToSubcentScale) {
            return false;
        }
        out = cents_value * kCentToSubcentScale;
        return true;
    }

    auto double_result = value.get_double();
    if (double_result.error() != simdjson::SUCCESS) {
        return false;
    }
    const double as_double = double_result.value_unsafe();
    if (as_double < 0.0) {
        return false;
    }
    const double scaled = as_double * 10000.0;
    double integer_part = 0.0;
    if (std::modf(scaled, &integer_part) != 0.0 ||
        integer_part > static_cast<double>(std::numeric_limits<std::int64_t>::max())) {
        return false;
    }
    out = static_cast<std::int64_t>(integer_part);
    return true;
}

bool get_uint64(simdjson::ondemand::object& obj, std::string_view key, std::uint64_t& out) noexcept {
    auto result = obj.find_field_unordered(key).get_uint64();
    if (result.error() == simdjson::SUCCESS) {
        out = result.value_unsafe();
        return true;
    }

    std::int64_t signed_value = 0;
    if (!get_int64(obj, key, signed_value) || signed_value < 0) {
        return false;
    }
    out = static_cast<std::uint64_t>(signed_value);
    return true;
}

bool get_lot_count(simdjson::ondemand::object& msg, std::int64_t& out) noexcept {
    simdjson::ondemand::value count;
    if (get_value(msg, "count", count) && parse_lot_count_value(count, out)) {
        return out >= 0;
    }
    simdjson::ondemand::value count_fp;
    if (get_value(msg, "count_fp", count_fp) && parse_lot_count_value(count_fp, out)) {
        return out >= 0;
    }
    return false;
}

bool parse_first_price_ticks(simdjson::ondemand::object& msg,
                             std::initializer_list<std::string_view> keys,
                             std::int64_t& out,
                             std::string_view* matched_key = nullptr) noexcept {
    for (const auto key : keys) {
        simdjson::ondemand::value value;
        if (!get_value(msg, key, value)) {
            continue;
        }
        if (!parse_price_ticks_value(value, out)) {
            continue;
        }
        if (matched_key != nullptr) {
            *matched_key = key;
        }
        return true;
    }
    return false;
}

bool parse_first_lot_count(simdjson::ondemand::object& msg,
                           std::initializer_list<std::string_view> keys,
                           std::int64_t& out,
                           std::string_view* matched_key = nullptr) noexcept {
    for (const auto key : keys) {
        simdjson::ondemand::value value;
        if (!get_value(msg, key, value)) {
            continue;
        }
        if (!parse_lot_count_value(value, out)) {
            continue;
        }
        if (matched_key != nullptr) {
            *matched_key = key;
        }
        return true;
    }
    return false;
}

bool get_object(simdjson::ondemand::object& obj,
                std::string_view key,
                simdjson::ondemand::object& out) noexcept {
    auto result = obj.find_field_unordered(key).get_object();
    if (result.error() != simdjson::SUCCESS) {
        return false;
    }
    out = result.value_unsafe();
    return true;
}

bool get_array(simdjson::ondemand::object& obj,
               std::string_view key,
               simdjson::ondemand::array& out) noexcept {
    auto result = obj.find_field_unordered(key).get_array();
    if (result.error() != simdjson::SUCCESS) {
        return false;
    }
    out = result.value_unsafe();
    return true;
}

internal::Side parse_book_side(std::string_view side_token) {
    if (side_token == "yes" || side_token == "bid") {
        return internal::Side::kBid;
    }
    if (side_token == "no" || side_token == "ask") {
        return internal::Side::kAsk;
    }
    return internal::Side::kUnknown;
}

internal::Side parse_trade_side(std::string_view side_token) {
    if (side_token == "yes" || side_token == "buy") {
        return internal::Side::kBuy;
    }
    if (side_token == "no" || side_token == "sell") {
        return internal::Side::kSell;
    }
    return internal::Side::kUnknown;
}

void set_sequence_from_msg(simdjson::ondemand::object& msg, internal::NormalizedEvent& event) {
    std::uint64_t seq = 0;
    if (get_uint64(msg, "seq", seq) || get_uint64(msg, "seq_id", seq)) {
        event.raw_sequence_id = seq;
        event.meta.sequence_id = seq;
        return;
    }
    if (event.raw_sequence_id.has_value()) {
        event.meta.sequence_id = event.raw_sequence_id.value();
    }
}

bool parse_levels(simdjson::ondemand::array& arr, std::vector<internal::Level>& out, bool kalshi_reciprocal_price) noexcept {
    for (auto level_val : arr) {
        auto level_arr_result = level_val.get_array();
        if (level_arr_result.error() != simdjson::SUCCESS) {
            return false;
        }
        auto iter = level_arr_result.value_unsafe().begin();
        if (iter.error() != simdjson::SUCCESS) {
            return false;
        }

        internal::Level level{};

        simdjson::ondemand::value price_value = *iter;
        if (!parse_price_ticks_value(price_value, level.price_ticks)) {
            return false;
        }
        ++iter;
        if (iter.error() != simdjson::SUCCESS) {
            return false;
        }

        simdjson::ondemand::value qty_value = *iter;
        if (!parse_lot_count_value(qty_value, level.qty_lots)) {
            return false;
        }

        if (level.price_ticks < 0 || level.qty_lots < 0 || level.price_ticks > kMaxPriceTicks) {
            return false;
        }
        if(kalshi_reciprocal_price){
            level.price_ticks = kMaxPriceTicks - level.price_ticks;
        }
        out.push_back(level);
    }
    return true;
}

enum class OptionalLevelsParse : unsigned char {
    kNone = 0,
    kParsed,
    kInvalid,
};

OptionalLevelsParse parse_optional_levels(simdjson::ondemand::object& msg,
                                          std::initializer_list<std::string_view> keys,
                                          std::vector<internal::Level>& out,bool kalshi_reciprocal_price ) noexcept {
    for (const auto key : keys) {
        simdjson::ondemand::value value;
        if (!get_value(msg, key, value)) {
            continue;
        }

        auto array_result = value.get_array();
        if (array_result.error() != simdjson::SUCCESS) {
            return OptionalLevelsParse::kInvalid;
        }

        auto levels = array_result.value_unsafe();
        if (!parse_levels(levels, out,kalshi_reciprocal_price)) {
            return OptionalLevelsParse::kInvalid;
        }
        return OptionalLevelsParse::kParsed;
    }

    return OptionalLevelsParse::kNone;
}

ParseResult<internal::NormalizedEvent>
parse_snapshot(simdjson::ondemand::object& msg, internal::NormalizedEvent event) {
    event.type = internal::EventType::kSnapshot;

    internal::SnapshotData snapshot{};
    const auto yes_levels_parse =
        parse_optional_levels(msg, {"yes", "yes_dollars_fp"}, snapshot.bids,false);
    if (yes_levels_parse == OptionalLevelsParse::kInvalid) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kInvalidField);
    }

    const auto no_levels_parse =
        parse_optional_levels(msg, {"no", "no_dollars_fp"}, snapshot.asks,true);
    if (no_levels_parse == OptionalLevelsParse::kInvalid) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kInvalidField);
    }

    event.data = std::move(snapshot);
    set_sequence_from_msg(msg, event);
    return ParseResult<internal::NormalizedEvent>::success(std::move(event));
}

ParseResult<internal::NormalizedEvent>
parse_delta(simdjson::ondemand::object& msg,
            internal::NormalizedEvent event,
            bool strict_field_validation) {
    event.type = internal::EventType::kDelta;

    std::int64_t price = 0;
    std::int64_t delta_qty = 0;
    std::string_view side_token;
    internal::Side resolved_side = internal::Side::kUnknown;
    bool has_price = false;
    bool has_delta = false;
    set_sequence_from_msg(msg, event);

    if (get_string(msg, "side", side_token)) {
        resolved_side = parse_book_side(side_token);
        has_price = parse_first_price_ticks(msg, {"price", "price_dollars"}, price);
        has_delta = parse_first_lot_count(msg, {"delta", "delta_fp"}, delta_qty);
    } else {
        std::int64_t inferred_price = 0;
        std::int64_t inferred_delta = 0;

        const bool has_yes =
            parse_first_price_ticks(msg, {"yes_price", "yes_price_dollars"}, inferred_price) &&
            parse_first_lot_count(msg, {"yes_delta", "yes_delta_fp"}, inferred_delta);
        const bool has_no =
            parse_first_price_ticks(msg, {"no_price", "no_price_dollars"}, inferred_price) &&
            parse_first_lot_count(msg, {"no_delta", "no_delta_fp"}, inferred_delta);
        if (has_yes) {
            resolved_side = internal::Side::kBid;
            has_price = true;
            has_delta = true;
            price = inferred_price;
            delta_qty = inferred_delta;
        } else if (has_no) {
            resolved_side = internal::Side::kAsk;
            has_price = true;
            has_delta = true;
            price = reciprocal_price(inferred_price);
            delta_qty = inferred_delta;
        } else {
            has_price = parse_first_price_ticks(
                msg, {"price", "price_dollars", "yes_price", "yes_price_dollars", "no_price",
                      "no_price_dollars"},
                price);
            has_delta = parse_first_lot_count(
                msg, {"delta", "delta_fp", "yes_delta", "yes_delta_fp", "no_delta", "no_delta_fp"},
                delta_qty);
        }
    }
    if (!has_price || !has_delta) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kMissingField);
    }
    if (price < 0) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kInvalidField);
    }

    internal::DeltaData delta{};
    delta.side = resolved_side;
    if (delta.side == internal::Side::kUnknown && strict_field_validation) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kInvalidField);
    }
    delta.price_ticks = price;
    delta.delta_qty_lots = delta_qty;
    event.data = delta;
    return ParseResult<internal::NormalizedEvent>::success(std::move(event));
}

ParseResult<internal::NormalizedEvent>
parse_trade(simdjson::ondemand::object& msg,
            internal::NormalizedEvent event,
            bool strict_field_validation) {
    set_sequence_from_msg(msg, event);
    event.type = internal::EventType::kTrade;

    std::int64_t price = 0;
    std::int64_t qty = 0;
    simdjson::ondemand::value price_value;
    const bool has_price =
        (get_value(msg, "yes_price", price_value) && parse_price_ticks_value(price_value, price)) ||
        (get_value(msg, "price", price_value) && parse_price_ticks_value(price_value, price)) ||
        (get_value(msg, "yes_price_dollars", price_value) &&
         parse_price_ticks_value(price_value, price)) ||
        (get_value(msg, "price_dollars", price_value) &&
         parse_price_ticks_value(price_value, price));
    if (!has_price || !get_lot_count(msg, qty)) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kMissingField);
    }
    if (price < 0 || qty < 0) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kInvalidField);
    }

    internal::TradeData trade{};
    trade.price_ticks = price;
    trade.qty_lots = qty;

    std::string_view taker_side;
    if (get_string(msg, "taker_side", taker_side)) {
        trade.aggressor = parse_trade_side(taker_side);
        if (trade.aggressor == internal::Side::kUnknown && strict_field_validation) {
            trade.aggressor = internal::Side::kUnknown;
        }
    }

    std::string_view trade_id_view;
    if (get_string(msg, "trade_id", trade_id_view)) {
        trade.trade_id = std::string{trade_id_view};
    }

    event.data = trade;
    return ParseResult<internal::NormalizedEvent>::success(std::move(event));
}

} // namespace
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
predex::parsers::ParseResult<predex::internal::NormalizedEvent> predex::core::parsers::kalshi::Parser::parse(const predex::core::ingest::kalshi::FrameHandle& handle,
    const predex::core::ingest::kalshi::KalshiFrame& frame) const{

    predex::internal::NormalizedEvent event{};
    event.raw_sequence_id = handle.seq_;
    event.type = internal::EventType::kUnknown;
    event.meta.exchange = internal::ExchangeId::kKalshi;
    event.meta.topology_kind = handle.topology_kind_;
    event.meta.event_id = handle.event_id_;
    event.meta.affinity_key = handle.affinity_key_;
    event.meta.market_id = handle.market_id_;
    event.meta.recv_ns = frame.recv_ts_ns_;
    event.meta.sequence_id = handle.seq_; // Default sequence ID to the frame handle's sequence. This may be overwritten by the message's own sequence if present.
    // Kalshi does not provide exchange timestamps, so we leave event.meta.exchange_ts_ns as 0.
    //all other data needs to be parsed out of the payload, which is a JSON blob.

    const auto* payload = frame.payload.data();

    std::string payload_json{reinterpret_cast<const char*>(payload), frame.len_};
    simdjson::padded_string padded{payload_json};
    
    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(padded);
    if(doc.error() != simdjson::SUCCESS) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kInvalidJson);
    }
    auto root = doc.get_object();
    if(root.error() != simdjson::SUCCESS) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kInvalidJson);
    }
    auto obj = root.value_unsafe();

    std::string_view type_sv;
    if (!get_string(obj, "type", type_sv)) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kMissingField);
    }

    simdjson::ondemand::object msg;
    if ((type_sv == kTypeOrderbook ||
        type_sv == kTypeOrderbookDelta ||
        type_sv == kTypeTrade) &&
        !get_object(obj, "msg", msg)) {
        return ParseResult<internal::NormalizedEvent>::failure(ParseError::kMissingField);
    }

    if(type_sv == kTypeOrderbook) {
        return parse_snapshot(msg, std::move(event));
    }
    if (type_sv == kTypeOrderbookDelta) {
        return parse_delta(msg, std::move(event), /*strict_field_validation=*/true);
    }
    if (type_sv == kTypeTrade) {
        return parse_trade(msg, std::move(event), /*strict_field_validation=*/true);
    } 
    return ParseResult<internal::NormalizedEvent>::failure(ParseError::kUnsupportedMessageType);
    }
}// namespace predex::core::parsers::kalshi
