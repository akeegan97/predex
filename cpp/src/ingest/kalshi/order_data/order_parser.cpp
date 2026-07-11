#include "predex/ingest/kalshi/order_data/order_parser.hpp"

#include "predex/shard/models.hpp"

#include <charconv>
#include <limits>

namespace predex::ingest::kalshi::order_data{
    namespace{
        
        bool read_uint64(simdjson::ondemand::object& obj, std::string_view key, std::uint64_t& out_value) noexcept{
            auto field_result = obj.find_field(key);
            if(field_result.error() != simdjson::SUCCESS){
                return false;
            }
            auto value_result = field_result.get_uint64();
            if(value_result.error() != simdjson::SUCCESS){
                return false;
            }
            out_value = value_result.value();
            return true;
        }

        bool read_bool(simdjson::ondemand::object& obj, std::string_view key, bool& out_value) noexcept{
            auto field_result = obj.find_field(key);
            if(field_result.error() != simdjson::SUCCESS){
                return false;
            }
            auto value_result = field_result.get_bool();
            if(value_result.error() != simdjson::SUCCESS){
                return false;
            }
            out_value = value_result.value();
            return true;
        }

        bool read_string(simdjson::ondemand::object& obj, std::string_view key, std::string_view& out_value) noexcept{
            auto field_result = obj.find_field(key);
            if(field_result.error() != simdjson::SUCCESS){
                return false;
            }
            auto value_result = field_result.get_string();
            if(value_result.error() != simdjson::SUCCESS){
                return false;
            }
            out_value = value_result.value();
            return true;
        }

        bool read_optional_string(simdjson::ondemand::object& obj, std::string_view key, std::string_view& out_value) noexcept{
            auto field_result = obj.find_field(key);
            if(field_result.error() != simdjson::SUCCESS){
                return false;
            }
            auto value_result = field_result.get_string();
            if(value_result.error() != simdjson::SUCCESS){
                return false;
            }
            out_value = value_result.value();
            return true;
        }

        bool read_object(simdjson::ondemand::object& obj, std::string_view key, simdjson::ondemand::object& out_value) noexcept{
            auto field_result = obj.find_field(key);
            if(field_result.error() != simdjson::SUCCESS){
                return false;
            }
            auto value_result = field_result.get_object();
            if(value_result.error() != simdjson::SUCCESS){
                return false;
            }
            out_value = value_result.value();
            return true;
        }

        bool parse_scaled_fp(//NOLINT
            std::string_view value,
            std::uint64_t scale,//NOLINT 
            std::size_t decimal_places,
            bool allow_negative,
            std::int64_t& out
        ) noexcept {
            if(value.empty() || scale == 0){
                return false;
            }

            bool negative = false;
            if(value.front() == '-' || value.front() == '+'){
                negative = value.front() == '-';
                if(negative && !allow_negative){
                    return false;
                }
                value.remove_prefix(1);
            }
            if(value.empty()){
                return false;
            }

            const std::size_t decimal_pos = value.find('.');
            const std::string_view integer_part = decimal_pos == std::string_view::npos ? value : value.substr(0, decimal_pos);
            const std::string_view fractional_part = decimal_pos == std::string_view::npos ? std::string_view{} : value.substr(decimal_pos + 1);

            if(integer_part.empty() || fractional_part.size() > decimal_places){
                return false;
            }
            for(const char ch : integer_part){//NOLINT
                if(ch < '0' || ch > '9'){
                    return false;
                }
            }
            for(const char ch : fractional_part){//NOLINT
                if(ch < '0' || ch > '9'){
                    return false;
                }
            }

            std::uint64_t whole = 0;
            const auto [ptr, ec] = std::from_chars(integer_part.data(), integer_part.data() + integer_part.size(), whole);
            if(ec != std::errc{} || ptr != integer_part.data() + integer_part.size()){
                return false;
            }

            constexpr std::uint64_t kDECIMAL_BASE = 10;
            std::uint64_t fractional = 0;
            for(const char ch : fractional_part){//NOLINT
                fractional = fractional * kDECIMAL_BASE + static_cast<std::uint64_t>(ch - '0');
            }
            for(std::size_t i = fractional_part.size(); i < decimal_places; ++i){
                fractional *= kDECIMAL_BASE;
            }

            const auto max_value = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
            if(fractional > max_value || whole > (max_value - fractional) / scale){
                return false;
            }

            const std::uint64_t magnitude = whole * scale + fractional;
            out = negative ? -static_cast<std::int64_t>(magnitude) : static_cast<std::int64_t>(magnitude);
            return true;
        }

        bool parse_price_ticks(std::string_view value, std::int64_t& out) noexcept{
            constexpr std::size_t kPRICE_DECIMAL_PLACES = 4;
            if(!parse_scaled_fp(value, shard::kTICKSCALE, kPRICE_DECIMAL_PLACES, false, out)){
                return false;
            }
            return out >= 0 && static_cast<std::uint64_t>(out) <= shard::kTICKSCALE;
        }

        bool parse_qty_lots(std::string_view value, std::int64_t& out) noexcept{
            constexpr std::size_t kQTY_DECIMAL_PLACES = 2;
            return parse_scaled_fp(value, shard::kQTY_SCALE, kQTY_DECIMAL_PLACES, false, out);
        }

        bool parse_ts_ms_to_ns(std::uint64_t ts_ms, std::uint64_t& out_ns) noexcept{
            constexpr std::uint64_t kNS_PER_MS = 1'000'000;
            if(ts_ms > std::numeric_limits<std::uint64_t>::max() / kNS_PER_MS){
                return false;
            }
            out_ns = ts_ms * kNS_PER_MS;
            return true;
        }

        bool parse_outcome(std::string_view token, oms::intent::Outcome& out) noexcept{
            if(token == "yes"){
                out = oms::intent::Outcome::kYES;
                return true;
            }
            if(token == "no"){
                out = oms::intent::Outcome::kNO;
                return true;
            }
            return false;
        }

        bool parse_order_state(std::string_view token, oms::OrderState& out) noexcept{
            if(token == "resting" || token == "open"){
                out = oms::OrderState::kWORKING;
                return true;
            }
            if(token == "partially_filled"){
                out = oms::OrderState::kPARTIALLY_FILLED;
                return true;
            }
            if(token == "filled" || token == "executed"){
                out = oms::OrderState::kFILLED;
                return true;
            }
            if(token == "canceled" || token == "cancelled"){
                out = oms::OrderState::kCANCELED;
                return true;
            }
            if(token == "rejected"){
                out = oms::OrderState::kREJECTED;
                return true;
            }
            if(token == "expired"){
                out = oms::OrderState::kEXPIRED;
                return true;
            }
            return false;
        }

        bool assign_exchange_order_id(std::string_view order_id, ParsedOrderMessage& out_message) noexcept{
            return out_message.order_event.exchange_order_id.assign_from(order_id);
        }

        bool assign_optional_client_order_id(std::string_view client_order_id, ParsedOrderMessage& out_message) noexcept{
            return client_order_id.empty() || out_message.order_event.client_order_id.assign_from(client_order_id);
        }

        void normalize_price_for_outcome(oms::intent::Outcome outcome, std::int64_t& price_ticks) noexcept{
            if(outcome == oms::intent::Outcome::kNO){
                price_ticks = static_cast<std::int64_t>(shard::kTICKSCALE) - price_ticks;
            }
        }
    }


    OrderIncomingMessageKind OrderParser::classify_message(std::span<const std::byte> message) noexcept {
        if (message.empty()) {
            return OrderIncomingMessageKind::kIGNORE;
        }

        const auto* data = reinterpret_cast<const char*>(message.data());
        simdjson::padded_string json{std::string_view{data, message.size()}};

        auto doc_result = parser_.iterate(json);
        if (doc_result.error() != simdjson::SUCCESS) {
            return OrderIncomingMessageKind::kMALFORMED;
        }

        auto root_result = doc_result.get_object();
        if (root_result.error() != simdjson::SUCCESS) {
            return OrderIncomingMessageKind::kMALFORMED;
        }

        auto root = root_result.value();

        std::string_view type{};
        if (!read_string(root, "type", type)) {
            return OrderIncomingMessageKind::kIGNORE;
        }

        if (type == "fill" || type == "user_order" || type == "market_position") {
            return OrderIncomingMessageKind::kORDER_DATA;
        }
        if (type == "subscribed" || type == "unsubscribed" || type == "error") {
            return OrderIncomingMessageKind::kCONTROL_RESPONSE;
        }

        return OrderIncomingMessageKind::kIGNORE;
    }
    
    OrderParseCode OrderParser::parse_control_response(std::span<const std::byte> message,OrderControlResponse& out_response) noexcept {
        out_response = OrderControlResponse{};

        if (message.empty()) {
            return OrderParseCode::kIGNORE;
        }

        const auto* data = reinterpret_cast<const char*>(message.data());
        simdjson::padded_string json{std::string_view{data, message.size()}};

        auto doc_result = parser_.iterate(json);
        if (doc_result.error() != simdjson::SUCCESS) {
            return OrderParseCode::kINVALID_JSON;
        }

        auto root_result = doc_result.get_object();
        if (root_result.error() != simdjson::SUCCESS) {
            return OrderParseCode::kINVALID_JSON;
        }

        auto root = root_result.value();

        if (!read_uint64(root, "id", out_response.request_id)) {
            return OrderParseCode::kMISSING_FIELD;
        }

        std::string_view type{};
        if (!read_string(root, "type", type)) {
            return OrderParseCode::kMISSING_FIELD;
        }

        if (type == "error") {
            out_response.success = false;
            out_response.error_message = "Kalshi websocket command failed";
            return OrderParseCode::kOK;
        }

        if (type == "subscribed") {
            simdjson::ondemand::object msg;
            std::uint64_t sid{};
            if (!read_object(root, "msg", msg) || !read_uint64(msg, "sid", sid)) {
                out_response.success = false;
                out_response.error_message = "subscribed response missing sid";
                return OrderParseCode::kMISSING_FIELD;
            }

            out_response.success = true;
            out_response.session_id = static_cast<std::int64_t>(sid);
            return OrderParseCode::kOK;
        }

        if (type == "unsubscribed") {
            out_response.success = true;
            return OrderParseCode::kOK;
        }

        out_response.success = false;
        out_response.error_message = "unsupported control response type";
        return OrderParseCode::kUNSUPPORTED_TYPE;
    }

    OrderParseCode OrderParser::parse_fill_event(simdjson::ondemand::object& msg, ParsedOrderMessage& out_message) noexcept{
        out_message.order_event.event_kind = oms::PrivateWsOrderEventKind::kFILL;

        std::string_view order_id{};
        if(!read_string(msg, "order_id", order_id) || !assign_exchange_order_id(order_id, out_message)){
            return OrderParseCode::kINVALID_ORDER_ID;
        }

        std::string_view ticker{};
        if(!read_string(msg, "market_ticker", ticker) || !out_message.assign_market_ticker(ticker)){
            return OrderParseCode::kMISSING_FIELD;
        }

        std::string_view side{};
        if(!read_string(msg, "side", side) || !parse_outcome(side, out_message.order_event.outcome)){
            return OrderParseCode::kMISSING_FIELD;
        }

        std::string_view price{};
        if(!read_string(msg, "yes_price_dollars", price) || !parse_price_ticks(price, out_message.order_event.last_fill_price_ticks)){
            return OrderParseCode::kMISSING_FIELD;
        }
        normalize_price_for_outcome(out_message.order_event.outcome, out_message.order_event.last_fill_price_ticks);

        std::string_view count{};
        if(!read_string(msg, "count_fp", count) || !parse_qty_lots(count, out_message.order_event.last_fill_qty_lots)){
            return OrderParseCode::kMISSING_FIELD;
        }

        std::uint64_t ts_ms{};
        if(read_uint64(msg, "ts_ms", ts_ms)){
            if(!parse_ts_ms_to_ns(ts_ms, out_message.order_event.venue_ts_ns)){
                return OrderParseCode::kMISSING_FIELD;
            }
        }

        return OrderParseCode::kOK;
    }

    OrderParseCode OrderParser::parse_user_order_event(simdjson::ondemand::object& msg, ParsedOrderMessage& out_message) noexcept{
        out_message.order_event.event_kind = oms::PrivateWsOrderEventKind::kUSER_ORDER;

        std::string_view order_id{};
        if(!read_string(msg, "order_id", order_id) || !assign_exchange_order_id(order_id, out_message)){
            return OrderParseCode::kINVALID_ORDER_ID;
        }

        std::string_view ticker{};
        if(!read_string(msg, "ticker", ticker) || !out_message.assign_market_ticker(ticker)){
            return OrderParseCode::kMISSING_FIELD;
        }

        std::string_view status{};
        if(!read_string(msg, "status", status) || !parse_order_state(status, out_message.order_event.order_state)){
            return OrderParseCode::kUNKNOWN_ORDER_STATE;
        }

        std::string_view side{};
        if(read_string(msg, "side", side)){
            if(!parse_outcome(side, out_message.order_event.outcome)){
                return OrderParseCode::kMISSING_FIELD;
            }
        }else{
            bool is_yes = false;
            if(!read_bool(msg, "is_yes", is_yes)){
                return OrderParseCode::kMISSING_FIELD;
            }
            out_message.order_event.outcome = is_yes ? oms::intent::Outcome::kYES : oms::intent::Outcome::kNO;
        }

        std::string_view fill_count{};
        if(!read_string(msg, "fill_count_fp", fill_count) || !parse_qty_lots(fill_count, out_message.order_event.cumulative_filled_qty_lots)){
            return OrderParseCode::kMISSING_FIELD;
        }

        std::string_view remaining_count{};
        if(!read_string(msg, "remaining_count_fp", remaining_count) || !parse_qty_lots(remaining_count, out_message.order_event.leaves_qty_lots)){
            return OrderParseCode::kMISSING_FIELD;
        }

        std::string_view initial_count{};
        if(!read_string(msg, "initial_count_fp", initial_count) || !parse_qty_lots(initial_count, out_message.order_event.ordered_qty_lots)){
            return OrderParseCode::kMISSING_FIELD;
        }

        std::string_view client_order_id{};
        if(read_optional_string(msg, "client_order_id", client_order_id) && !assign_optional_client_order_id(client_order_id, out_message)){
            return OrderParseCode::kINVALID_ORDER_ID;
        }

        std::uint64_t created_ts_ms{};
        if(read_uint64(msg, "created_ts_ms", created_ts_ms)){
            if(!parse_ts_ms_to_ns(created_ts_ms, out_message.order_event.venue_ts_ns)){
                return OrderParseCode::kMISSING_FIELD;
            }
        }

        return OrderParseCode::kOK;
    }

    OrderParseCode OrderParser::parse_market_position_event(simdjson::ondemand::object& msg, ParsedOrderMessage& out_message) noexcept{
        out_message.order_event.event_kind = oms::PrivateWsOrderEventKind::kMARKET_POSITION;

        std::string_view ticker{};
        if(!read_string(msg, "market_ticker", ticker) || !out_message.assign_market_ticker(ticker)){
            return OrderParseCode::kMISSING_FIELD;
        }

        return OrderParseCode::kIGNORE;
    }

    OrderParseCode OrderParser::parse_order_event(std::span<const std::byte> message, ParsedOrderMessage& out_message) noexcept {
        out_message = ParsedOrderMessage{};
        out_message.kind = OrderIncomingMessageKind::kORDER_DATA;

        if (message.empty()) {
            return OrderParseCode::kIGNORE;
        }

        const auto* data = reinterpret_cast<const char*>(message.data());
        simdjson::padded_string json{std::string_view{data, message.size()}};

        auto doc_result = parser_.iterate(json);
        if (doc_result.error() != simdjson::SUCCESS) {
            return OrderParseCode::kINVALID_JSON;
        }

        auto root_result = doc_result.get_object();
        if (root_result.error() != simdjson::SUCCESS) {
            return OrderParseCode::kINVALID_JSON;
        }

        auto root = root_result.value();

        std::string_view type{};
        if (!read_string(root, "type", type)) {
            return OrderParseCode::kMISSING_FIELD;
        }

        simdjson::ondemand::object msg;
        if(!read_object(root, "msg", msg)){
            return OrderParseCode::kMISSING_FIELD;
        }

        OrderParseCode code = OrderParseCode::kUNSUPPORTED_TYPE;
        if(type == "fill"){
            code = parse_fill_event(msg, out_message);
        }else if(type == "user_order"){
            code = parse_user_order_event(msg, out_message);
        }else if(type == "market_position"){
            code = parse_market_position_event(msg, out_message);
        }

        out_message.parse_code = code;
        return code;
    }

    OrderParseCode OrderParser::parse_message(std::span<const std::byte> message, ParsedOrderMessage& out_message) noexcept {
        out_message = ParsedOrderMessage{};

        auto kind = classify_message(message);
        out_message.kind = kind;

        switch (kind) {
            case OrderIncomingMessageKind::kCONTROL_RESPONSE:
                return parse_control_response(message, out_message.control_response);
            case OrderIncomingMessageKind::kORDER_DATA:
                return parse_order_event(message, out_message);
            case OrderIncomingMessageKind::kIGNORE:
                return OrderParseCode::kIGNORE;
            case OrderIncomingMessageKind::kMALFORMED:
                return OrderParseCode::kINVALID_JSON;
            default:
                return OrderParseCode::kUNSUPPORTED_TYPE;
        }
    }
}
