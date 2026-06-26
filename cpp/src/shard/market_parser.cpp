#include "predex/shard/market_parser.hpp"
#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/shard/models.hpp"
#include <string_view>
#include <charconv>




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

    bool parse_fp_to_ticks(std::string_view value, std::int64_t& out) noexcept {
        if (value.empty() || value.front() == '-' || value.front() == '+') {
            return false;
        }

        const std::size_t decimal_pos = value.find('.');
        const std::string_view integer_part =
            decimal_pos == std::string_view::npos ? value : value.substr(0, decimal_pos);
        const std::string_view fractional_part =
            decimal_pos == std::string_view::npos ? std::string_view{} : value.substr(decimal_pos + 1);

        if (integer_part.empty()) {
            return false;
        }

        for (char c : integer_part) {
            if (c < '0' || c > '9') {
                return false;
            }
        }

        for (char c : fractional_part) {
            if (c < '0' || c > '9') {
                return false;
            }
        }

        std::uint64_t whole = 0;
        const auto [ptr, ec] =
            std::from_chars(integer_part.data(), integer_part.data() + integer_part.size(), whole);
        if (ec != std::errc{} || ptr != integer_part.data() + integer_part.size()) {
            return false;
        }

        constexpr std::size_t kPriceDecimalPlaces = 4;
        constexpr std::uint64_t kMultiplier = 10;

        std::uint64_t fractional_ticks = 0;
        const std::size_t digits_to_take = std::min(fractional_part.size(), kPriceDecimalPlaces);

        for (std::size_t i = 0; i < digits_to_take; ++i) {
            fractional_ticks =
                fractional_ticks * kMultiplier + static_cast<std::uint64_t>(fractional_part[i] - '0');
        }

        for (std::size_t i = digits_to_take; i < kPriceDecimalPlaces; ++i) {
            fractional_ticks *= kMultiplier;
        }

        if (fractional_part.size() > kPriceDecimalPlaces) {
            return false;
        }

        if (whole > (std::numeric_limits<std::int64_t>::max() - fractional_ticks) / predex::shard::kTICKSCALE) {
            return false;
        }

        const auto ticks = whole * predex::shard::kTICKSCALE + fractional_ticks;
        if (ticks > predex::shard::kTICKSCALE) {
            return false;
        }

        out = static_cast<std::int64_t>(ticks);
        return true;
    }

    bool parse_qty_fp_to_lots(std::string_view value, std::int64_t& out) noexcept{
        if (value.empty() || value.front() == '-' || value.front() == '+') {
            return false;
        }

        const std::size_t decimal_pos = value.find('.');
        const std::string_view integer_part = decimal_pos == std::string_view::npos ? value : value.substr(0, decimal_pos);
        const std::string_view fractional_part = decimal_pos == std::string_view::npos ? std::string_view{} : value.substr(decimal_pos + 1);

        if (integer_part.empty()) {
            return false;
        }

        for (char c : integer_part) {
            if (c < '0' || c > '9') {
                return false;
            }
        }

        for (char c : fractional_part) {
            if (c < '0' || c > '9') {
                return false;
            }
        }

        std::uint64_t whole = 0;
        const auto [ptr, ec] = std::from_chars(integer_part.data(), integer_part.data() + integer_part.size(), whole);
        if (ec != std::errc{} || ptr != integer_part.data() + integer_part.size()) {
            return false;
        }

        constexpr std::size_t kQtyDecimalPlaces = 2;
        constexpr std::uint64_t kMultiplier = 10;

        std::uint64_t fractional_lots = 0;
        const std::size_t digits_to_take = std::min(fractional_part.size(), kQtyDecimalPlaces);

        for (std::size_t i = 0; i < digits_to_take; ++i) {
            fractional_lots =
                fractional_lots * kMultiplier + static_cast<std::uint64_t>(fractional_part[i] - '0');
        }

        for (std::size_t i = digits_to_take; i < kQtyDecimalPlaces; ++i) {
            fractional_lots *= kMultiplier;
        }

        if (fractional_part.size() > kQtyDecimalPlaces) {
            return false;
        }

        if (whole > (std::numeric_limits<std::int64_t>::max() - fractional_lots) / predex::shard::kQTY_SCALE) {
            return false;
        }

        const auto lots = whole * predex::shard::kQTY_SCALE + fractional_lots;
        out = static_cast<std::int64_t>(lots);
        return true;
    }

    bool parse_delta_side(std::string_view value, predex::shard::Side& out) noexcept {
        if (value == "bid") {
            out = predex::shard::Side::kBID;
            return true;
        }
        if (value == "ask") {
            out = predex::shard::Side::kASK;
            return true;
        }
        return false;
    }

    bool parse_trade_aggressor(std::string_view value, predex::shard::AggressorSide& out) noexcept {
        if (value == "buy") {
            out = predex::shard::AggressorSide::kBUY;
            return true;
        }
        if (value == "sell") {
            out = predex::shard::AggressorSide::kSELL;
            return true;
        }
        return false;
    }

    
}





namespace predex::shard{
    ParseResult MarketParser::parse(const ingest::kalshi::FrameHandle& handle, const ingest::kalshi::KalshiFrame& frame, KalshiParsedEvent& parsed_event) noexcept{
        ParseResult result{};
        switch(handle.kind){
            case ingest::kalshi::FrameKind::kORDERBOOK_SNAPSHOT:
                // parse snapshot event
                // ...
                result.success = true;
                break;
            case ingest::kalshi::FrameKind::kORDERBOOK_DELTA:
                // parse delta data
                // ...
                result.success = true;
                break;
            case ingest::kalshi::FrameKind::kTRADE:
                // parse trade data
                // ...
                result.success = true;
                break;
            case ingest::kalshi::FrameKind::kLIFECYCLE:
                // parse lifecycle data
                // ...
                result.success = true;
                break;
            default:
                result.reason = KalshiParseFailureReason::kUNSUPPORTED_FRAME_KIND;
                break;
        }
        return result;
    }


}

