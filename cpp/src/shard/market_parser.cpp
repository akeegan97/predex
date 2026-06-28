#include "predex/shard/market_parser.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/shard/models.hpp"
#include <charconv>
#include <initializer_list>
#include <limits>
#include <string_view>




namespace {
    constexpr std::string_view kORDERBOOK = "orderbook_snapshot";
    constexpr std::string_view kORDERBOOK_DELTA = "orderbook_delta";
    constexpr std::string_view kTRADE = "trade";
    constexpr std::string_view kLIFECYCLE = "market_lifecycle";
    
    std::int64_t reciprocal_price(std::int64_t price){
        return static_cast<std::int64_t>(predex::shard::kTICKSCALE - price);
    }

    bool get_string(simdjson::ondemand::object& obj, std::string_view key, std::string_view& out) noexcept{
        auto result = obj.find_field_unordered(key).get_string();
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        out = result.value_unsafe();
        return true;
    }

    bool get_int64_t(simdjson::ondemand::object& obj, std::string_view key, std::int64_t& out) noexcept{
        auto result = obj.find_field_unordered(key).get_int64();
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        out = result.value_unsafe();
        return true;
    }

    bool get_uint64_t(simdjson::ondemand::object& obj, std::string_view key, std::uint64_t& out) noexcept{
        auto result = obj.find_field_unordered(key).get_uint64();
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        out = result.value_unsafe();
        return true;
    }

    bool get_value(simdjson::ondemand::object& obj, std::string_view key, simdjson::ondemand::value& out) noexcept{
        auto result = obj.find_field_unordered(key);
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        out = result.value_unsafe();
        return true;
    }
    bool read_object(simdjson::ondemand::object& value, std::string_view key,
                    simdjson::ondemand::object& out) noexcept {
        auto field = value.find_field_unordered(key);
        if (field.error() != simdjson::SUCCESS) {
            return false;
        }
        auto value_ = field.get_object();
        if (value_.error() != simdjson::SUCCESS) {
            return false;
        }
        out = value_.value();
        return true;
    }
//NOLINTNEXTLINE - accepting a high cognitive complexity for this function
    bool parse_scaled_fp(
        std::string_view value,
        std::uint64_t scale, // NOLINT
        std::size_t decimal_places,
        bool allow_negative,
        std::int64_t& out
    ) noexcept {
        if (value.empty() || scale == 0) {
            return false;
        }

        bool negative = false;
        if (value.front() == '-' || value.front() == '+') {
            negative = value.front() == '-';
            if (negative && !allow_negative) {
                return false;
            }
            value.remove_prefix(1);
        }
        if (value.empty()) {
            return false;
        }

        const std::size_t decimal_pos = value.find('.');
        const std::string_view integer_part = decimal_pos == std::string_view::npos ? value : value.substr(0, decimal_pos);
        const std::string_view fractional_part = decimal_pos == std::string_view::npos ? std::string_view{} : value.substr(decimal_pos + 1);

        if (integer_part.empty() || fractional_part.size() > decimal_places) {
            return false;
        }
//NOLINTNEXTLINE -
        for (const char c : integer_part) {
            if (c < '0' || c > '9') {
                return false;
            }
        }
//NOLINTNEXTLINE -
        for (const char c : fractional_part) {
            if (c < '0' || c > '9') {
                return false;
            }
        }

        std::uint64_t whole = 0;
        const auto [ptr, ec] = std::from_chars(integer_part.data(), integer_part.data() + integer_part.size(), whole);
        if (ec != std::errc{} || ptr != integer_part.data() + integer_part.size()) {
            return false;
        }

        constexpr std::uint64_t kMultiplier = 10;

        std::uint64_t fractional = 0;
        //NOLINTNEXTLINE -
        for (const char c : fractional_part) {
            fractional = fractional * kMultiplier + static_cast<std::uint64_t>(c - '0');
        }

        for (std::size_t i = fractional_part.size(); i < decimal_places; ++i) {
            fractional *= kMultiplier;
        }

        const auto int64_max = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
        if (fractional > int64_max || whole > (int64_max - fractional) / scale) {
            return false;
        }

        const std::uint64_t magnitude = whole * scale + fractional;
        if (negative) {
            out = -static_cast<std::int64_t>(magnitude);
        } else {
            out = static_cast<std::int64_t>(magnitude);
        }
        return true;
    }

    bool parse_fp_to_ticks(std::string_view value, std::int64_t& out) noexcept {
        constexpr std::size_t kPriceDecimalPlaces = 4;
        if (!parse_scaled_fp(value, predex::shard::kTICKSCALE, kPriceDecimalPlaces, false, out)) {
            return false;
        }
        return out >= 0 && static_cast<std::uint64_t>(out) <= predex::shard::kTICKSCALE;
    }

    bool parse_qty_fp_to_lots(std::string_view value, std::int64_t& out) noexcept {
        constexpr std::size_t kQtyDecimalPlaces = 2;
        return parse_scaled_fp(value, predex::shard::kQTY_SCALE, kQtyDecimalPlaces, false, out);
    }

    bool parse_delta_fp_to_lots(std::string_view value, std::int64_t& out) noexcept {
        constexpr std::size_t kQtyDecimalPlaces = 2;
        return parse_scaled_fp(value, predex::shard::kQTY_SCALE, kQtyDecimalPlaces, true, out);
    }

    bool parse_delta_side(std::string_view value, predex::shard::Side& out) noexcept {
        if (value == "yes" || value == "bid") {
            out = predex::shard::Side::kBID;
            return true;
        }
        if (value == "no" || value == "ask") {
            out = predex::shard::Side::kASK;
            return true;
        }
        return false;
    }

    bool parse_trade_aggressor(std::string_view value, predex::shard::AggressorSide& out) noexcept {
        if (value == "yes" || value == "buy") {
            out = predex::shard::AggressorSide::kBUY;
            return true;
        }
        if (value == "no" || value == "sell") {
            out = predex::shard::AggressorSide::kSELL;
            return true;
        }
        return false;
    }

    predex::shard::Side aggressor_to_book_side(predex::shard::AggressorSide aggressor) noexcept {
        switch (aggressor) {
            case predex::shard::AggressorSide::kBUY:
                return predex::shard::Side::kASK;
            case predex::shard::AggressorSide::kSELL:
                return predex::shard::Side::kBID;
            default:
                return predex::shard::Side::kUNKNOWN;
        }
    }

    bool get_array(simdjson::ondemand::object& obj, std::string_view key, simdjson::ondemand::array& out) noexcept{
        auto value_result = obj.find_field_unordered(key);
        if(value_result.error() != simdjson::SUCCESS){
            return false;
        }
        auto result = value_result.value_unsafe().get_array();
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        out = result.value_unsafe();
        return true;
    }

    bool get_first_value(
        simdjson::ondemand::object& obj,
        std::initializer_list<std::string_view> keys,
        simdjson::ondemand::value& out
    ) noexcept {
        for (const auto key : keys) {
            if (get_value(obj, key, out)) {
                return true;
            }
        }
        return false;
    }

    bool parse_price_value(simdjson::ondemand::value& value, std::int64_t& out) noexcept{
        auto result = value.get_string();
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        return parse_fp_to_ticks(result.value_unsafe(), out);
    }
    bool parse_price_value(simdjson::ondemand::value& value, std::uint64_t&out)noexcept{
        auto result = value.get_string();
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        std::int64_t temp{};
        if(!parse_fp_to_ticks(result.value_unsafe(), temp)){
            return false;
        }
        if(temp < 0){
            return false;
        }
        out = static_cast<std::uint64_t>(temp);
        return true;
    }

    bool parse_qty_value(simdjson::ondemand::value& value, std::uint64_t& out) noexcept{
        auto result = value.get_string();
        if(result.error() != simdjson::SUCCESS){
            return false;
        }
        std::int64_t temp{};
        if(!parse_qty_fp_to_lots(result.value_unsafe(), temp)){
            return false;
        }
        if(temp < 0){
            return false;
        }
        out = static_cast<std::uint64_t>(temp);
        return true;
    }

    bool parse_delta_qty_value(simdjson::ondemand::value& value, predex::shard::DeltaQtyLots& out) noexcept {
        auto string_result = value.get_string();
        if (string_result.error() != simdjson::SUCCESS) {
            return false;
        }

        std::int64_t parsed{};
        if (!parse_delta_fp_to_lots(string_result.value_unsafe(), parsed)) {
            return false;
        }

        out = parsed;
        return true;
    }

    bool parse_first_price_value(
        simdjson::ondemand::object& msg,
        std::initializer_list<std::string_view> keys,
        predex::shard::PriceTicks& out
    ) noexcept {
        simdjson::ondemand::value value{};
        return get_first_value(msg, keys, value) && parse_price_value(value, out);
    }

    bool parse_first_qty_value(
        simdjson::ondemand::object& msg,
        std::initializer_list<std::string_view> keys,
        predex::shard::QtyLots& out
    ) noexcept {
        simdjson::ondemand::value value{};
        return get_first_value(msg, keys, value) && parse_qty_value(value, out);
    }

    bool parse_first_delta_qty_value(
        simdjson::ondemand::object& msg,
        std::initializer_list<std::string_view> keys,
        predex::shard::DeltaQtyLots& out
    ) noexcept {
        simdjson::ondemand::value value{};
        return get_first_value(msg, keys, value) && parse_delta_qty_value(value, out);
    }

    bool parse_levels(simdjson::ondemand::array& arr, std::vector<predex::shard::Level>& out, bool reciprocal_price_enabled) noexcept{
        for(auto level_val : arr){
            auto level_arr_result = level_val->get_array();
            if(level_arr_result.error() != simdjson::SUCCESS){
                return false;
            }
            auto iter = level_arr_result.value_unsafe().begin();
            if(iter.error() != simdjson::SUCCESS){
                return false;
            }
            predex::shard::Level level{};
            simdjson::ondemand::value price_value = *iter;
            if(!parse_price_value(price_value, level.price_ticks)){
                return false;
            }
            ++iter;
            if(iter.error() != simdjson::SUCCESS){
                return false;
            }
            simdjson::ondemand::value qty_value = *iter;
            if(!parse_qty_value(qty_value, level.qty_lots)){
                return false;
            }
            if(level.price_ticks > predex::shard::kTICKSCALE){
                return false;
            }
            if(reciprocal_price_enabled){
                level.price_ticks = reciprocal_price(static_cast<std::int64_t>(level.price_ticks));
            }
            out.push_back(level);
        }
        return true;
    }

    enum class OptionalLevelsParse : std::uint8_t {
        kMISSING = 0,
        kPARSED = 1,
        kINVALID = 2,
    };

    OptionalLevelsParse parse_optional_levels(
        simdjson::ondemand::object& msg,
        std::string_view key,
        std::vector<predex::shard::Level>& out,
        bool reciprocal_price_enabled
    ) noexcept {
        auto value_result = msg.find_field_unordered(key);
        if (value_result.error() == simdjson::NO_SUCH_FIELD) {
            return OptionalLevelsParse::kMISSING;
        }
        if (value_result.error() != simdjson::SUCCESS) {
            return OptionalLevelsParse::kINVALID;
        }

        auto array_result = value_result.value_unsafe().get_array();
        if (array_result.error() != simdjson::SUCCESS) {
            return OptionalLevelsParse::kINVALID;
        }

        auto levels = array_result.value_unsafe();
        if (!parse_levels(levels, out, reciprocal_price_enabled)) {
            return OptionalLevelsParse::kINVALID;
        }
        return OptionalLevelsParse::kPARSED;
    }

    bool parse_snapshot(simdjson::ondemand::object& msg, predex::shard::KalshiSnapshotEvent& out) noexcept{
        const auto yes_levels = parse_optional_levels(msg, "yes_dollars_fp", out.bids, false);
        if(yes_levels == OptionalLevelsParse::kINVALID){
            return false;
        }

        const auto no_levels = parse_optional_levels(msg, "no_dollars_fp", out.asks, true);
        if(no_levels == OptionalLevelsParse::kINVALID){
            return false;
        }

        if (yes_levels == OptionalLevelsParse::kMISSING) {
            simdjson::ondemand::array legacy_yes_levels{};
            if(get_array(msg, "yes", legacy_yes_levels) && !parse_levels(legacy_yes_levels, out.bids, false)){
                return false;
            }
        }
        if (no_levels == OptionalLevelsParse::kMISSING) {
            simdjson::ondemand::array legacy_no_levels{};
            if(get_array(msg, "no", legacy_no_levels) && !parse_levels(legacy_no_levels, out.asks, true)){
                return false;
            }
        }
        return true;
    }

    bool parse_delta(simdjson::ondemand::object& msg, predex::shard::KalshiDeltaData& out) noexcept{
        std::string_view side_token{};
        if(!get_string(msg, "side", side_token)){
            return false;
        }
        if(!parse_delta_side(side_token, out.side)){
            return false;
        }
        if(!parse_first_price_value(msg, {"price_dollars", "price"}, out.price_ticks)){
            return false;
        }
        if(out.side == predex::shard::Side::kASK){
            out.price_ticks = static_cast<predex::shard::PriceTicks>(
                reciprocal_price(static_cast<std::int64_t>(out.price_ticks))
            );
        }
        if(!parse_first_delta_qty_value(msg, {"delta_fp", "delta"}, out.delta_qty_lots)){
            return false;
        }
        return true;
    }

    bool parse_trade(simdjson::ondemand::object& msg, predex::shard::KalshiTradeData& out) noexcept{
        if(!parse_first_price_value(msg, {"price_dollars", "yes_price_dollars", "price", "yes_price"}, out.price_ticks)){
            return false;
        }
        if(!parse_first_qty_value(msg, {"count_fp", "qty", "count"}, out.qty_lots)){
            return false;
        }
        std::string_view aggressor_token{};
        if(get_string(msg, "taker_side", aggressor_token)){
            if(!parse_trade_aggressor(aggressor_token, out.aggressor)){
                return false;
            }
            out.book_side = aggressor_to_book_side(out.aggressor);
        } else {
            out.aggressor = predex::shard::AggressorSide::kUNKNOWN;
            out.book_side = predex::shard::Side::kUNKNOWN;
        }
        return true;
    }

    bool parse_lifecycle(simdjson::ondemand::object& msg, predex::shard::KalshiLifecycleData& out) noexcept{
        // Implement lifecycle parsing logic here
        return true;
    }

}

namespace predex::shard{
    ParseResult MarketParser::parse(const ingest::kalshi::FrameHandle& handle, const ingest::kalshi::KalshiFrame& frame, KalshiParsedEvent& parsed_event) noexcept{
        ParseResult result{};
        const auto* payload_ptr = frame.payload.data();
        const char* buffer = reinterpret_cast<const char*>(payload_ptr);
        const auto buffer_length = static_cast<std::size_t>(frame.len);

        if(buffer_length == 0 || buffer == nullptr){
            result.reason = KalshiParseFailureReason::kINVALID_JSON;
            return result;
        }
        simdjson::padded_string_view json{
            reinterpret_cast<const char*>(frame.payload.data()),
            frame.len,
            frame.payload.size()
        };

        auto document_result = parser_.iterate(json);
        if(document_result.error() != simdjson::SUCCESS){
            result.reason = KalshiParseFailureReason::kINVALID_JSON;
            return result;
        }
        auto root_result = document_result.get_object();
        if(root_result.error() != simdjson::SUCCESS){
            result.reason = KalshiParseFailureReason::kINVALID_JSON;
            return result;
        }
        auto value = root_result.value_unsafe();

        simdjson::ondemand::object msg_object{};
        if(!read_object(value, "msg", msg_object)){
            result.reason = KalshiParseFailureReason::kMISSING_FIELD;
            return result;
        }

        switch (handle.kind) {
            case ingest::kalshi::FrameKind::kORDERBOOK_SNAPSHOT: {
                KalshiSnapshotEvent snapshot_event{};

                if (!parse_snapshot(msg_object, snapshot_event)) {
                    result.success = false;
                    result.reason = KalshiParseFailureReason::kMISSING_FIELD;
                    return result;
                }

                result.success = true;
                parsed_event = std::move(snapshot_event);
                break;
            }

            case ingest::kalshi::FrameKind::kORDERBOOK_DELTA: {
                KalshiDeltaData delta_event{};

                if (!parse_delta(msg_object, delta_event)) {
                    result.success = false;
                    result.reason = KalshiParseFailureReason::kMISSING_FIELD;
                    return result;
                }

                result.success = true;
                parsed_event = (delta_event);
                break;
            }

            case ingest::kalshi::FrameKind::kTRADE: {
                KalshiTradeData trade_event{};

                if (!parse_trade(msg_object, trade_event)) {
                    result.success = false;
                    result.reason = KalshiParseFailureReason::kMISSING_FIELD;
                    return result;
                }

                result.success = true;
                parsed_event = (trade_event);
                break;
            }

            case ingest::kalshi::FrameKind::kLIFECYCLE: {
                KalshiLifecycleData lifecycle_event{};

                if (!parse_lifecycle(msg_object, lifecycle_event)) {
                    result.success = false;
                    result.reason = KalshiParseFailureReason::kMISSING_FIELD;
                    return result;
                }

                result.success = true;
                parsed_event = (lifecycle_event);
                break;
            }

            default: {
                result.success = false;
                result.reason = KalshiParseFailureReason::kUNSUPPORTED_FRAME_KIND;
                return result;
            }
        }
        return result;
    }


}
