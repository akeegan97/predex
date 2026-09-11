#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"
#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/money_ticks.hpp"

#include <nlohmann/json.hpp>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <array>
#include <limits>
#include <type_traits>

namespace predex::exchange::kalshi{
namespace {
    constexpr std::int64_t kPRICE_TICKS_PER_DOLLAR = 10'000;
    constexpr std::int64_t kQUANTITY_LOTS_PER_CONTRACT = 100;

    struct V2OrderCoordinates {
        std::string_view side;
        std::int64_t yes_price_ticks{};
    };

    [[nodiscard]] std::optional<V2OrderCoordinates>
    to_v2_order_coordinates(
        oms::intent::Outcome outcome,
        oms::intent::OrderAction action,
        std::int64_t outcome_price_ticks) noexcept {

        if(outcome_price_ticks <= 0 ||
        outcome_price_ticks >= kPRICE_TICKS_PER_DOLLAR) {
            return std::nullopt;
        }

        const bool valid_outcome =
            outcome == oms::intent::Outcome::kYES ||
            outcome == oms::intent::Outcome::kNO;

        const bool valid_action =
            action == oms::intent::OrderAction::kBUY ||
            action == oms::intent::OrderAction::kSELL;

        if(!valid_outcome || !valid_action) {
            return std::nullopt;
        }

        const std::int64_t yes_price_ticks =
            outcome == oms::intent::Outcome::kYES
                ? outcome_price_ticks
                : kPRICE_TICKS_PER_DOLLAR - outcome_price_ticks;

        const bool is_yes_bid =
            (outcome == oms::intent::Outcome::kYES &&
            action == oms::intent::OrderAction::kBUY) ||
            (outcome == oms::intent::Outcome::kNO &&
            action == oms::intent::OrderAction::kSELL);

        return V2OrderCoordinates{
            .side = is_yes_bid ? "bid" : "ask",
            .yes_price_ticks = yes_price_ticks,
        };
    }

    [[nodiscard]] std::string_view tif_to_string(
        oms::intent::TimeInForce time_in_force) noexcept {

        switch(time_in_force) {
            case oms::intent::TimeInForce::kGTC:
                return "good_till_canceled";
            case oms::intent::TimeInForce::kIOC:
                return "immediate_or_cancel";
            case oms::intent::TimeInForce::kFOK:
                return "fill_or_kill";
        }

        return {};
    }

    [[nodiscard]] std::string scaled_integer_to_decimal(
        std::int64_t value,
        std::int64_t scale,//NOLINT
        std::size_t fractional_digits) {

        const std::int64_t whole = value / scale;
        const std::int64_t fractional = value % scale;

        std::string result = std::to_string(whole);
        result.push_back('.');

        const std::string fractional_text = std::to_string(fractional);
        result.append(
            fractional_digits - fractional_text.size(),
            '0');
        result.append(fractional_text);

        return result;
    }

    [[nodiscard]] std::optional<std::int64_t> parse_fixed_point(//NOLINT
        const nlohmann::json& value,
        std::int64_t scale, //NOLINT
        std::size_t fractional_digits,
        bool allow_negative = false) noexcept {

        if(!value.is_string()) {
            return std::nullopt;
        }

        const auto& text = value.get_ref<const std::string&>();
        if(text.empty()) {
            return std::nullopt;
        }

        bool negative = false;
        std::string_view magnitude_text{text};
        if(magnitude_text.front() == '-' || magnitude_text.front() == '+') {
            negative = magnitude_text.front() == '-';
            if(negative && !allow_negative) {
                return std::nullopt;
            }
            magnitude_text.remove_prefix(1);
        }
        if(magnitude_text.empty()) {
            return std::nullopt;
        }

        const std::size_t decimal_position = magnitude_text.find('.');
        if(decimal_position != std::string::npos &&
        magnitude_text.find('.', decimal_position + 1) != std::string::npos) {
            return std::nullopt;
        }

        const std::string_view whole_text{
            magnitude_text.data(),
            decimal_position == std::string::npos
                ? magnitude_text.size()
                : decimal_position
        };

        const std::string_view fractional_text =
            decimal_position == std::string::npos
                ? std::string_view{}
                : std::string_view{
                    magnitude_text.data() + decimal_position + 1,
                    magnitude_text.size() - decimal_position - 1
                };

        if(whole_text.empty() ||
        fractional_text.size() > fractional_digits) {
            return std::nullopt;
        }

        std::uint64_t whole{};
        for(const char ch : whole_text) { //NOLINT
            if(ch < '0' || ch > '9') {
                return std::nullopt;
            }

            const auto digit =
                static_cast<std::uint64_t>(ch - '0');

            if(whole >
            (std::numeric_limits<std::uint64_t>::max() - digit) / 10) { //NOLINT
                return std::nullopt;
            }

            whole = whole * 10 + digit;//NOLINT
        }

        std::uint64_t fractional{};
        for(const char ch : fractional_text) { //NOLINT
            if(ch < '0' || ch > '9') {
                return std::nullopt;
            }
            fractional =
                fractional * 10 + //NOLINT
                static_cast<std::uint64_t>(ch - '0');
        }

        for(std::size_t digits = fractional_text.size();
            digits < fractional_digits;
            ++digits) {
            fractional *= 10; //NOLINT
        }

        const auto unsigned_scale =
            static_cast<std::uint64_t>(scale);

        if(whole >
        (static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max()) -
            fractional) / unsigned_scale) {
            return std::nullopt;
        }

        const auto magnitude = static_cast<std::int64_t>(
            whole * unsigned_scale + fractional);
        return negative ? -magnitude : magnitude;
    }

    [[nodiscard]] std::optional<std::int64_t> cents_to_money_ticks(
        const nlohmann::json& value) noexcept {

        if(!value.is_number_integer() && !value.is_number_unsigned()) {
            return std::nullopt;
        }

        std::int64_t cents{};
        try {
            cents = value.get<std::int64_t>();
        } catch(...) {
            return std::nullopt;
        }

        constexpr std::int64_t kMONEY_TICKS_PER_CENT = 100;
        if(cents > std::numeric_limits<std::int64_t>::max() /
                kMONEY_TICKS_PER_CENT ||
        cents < std::numeric_limits<std::int64_t>::min() /
                kMONEY_TICKS_PER_CENT) {
            return std::nullopt;
        }
        return cents * kMONEY_TICKS_PER_CENT;
    }

    [[nodiscard]] std::string percent_encode_query_value(
        std::string_view value) {

        constexpr char kHEX[] = "0123456789ABCDEF"; //NOLINT
        std::string encoded;
        encoded.reserve(value.size());
        for(const unsigned char ch : value) { //NOLINT
            const bool unreserved =
                (ch >= 'A' && ch <= 'Z') ||
                (ch >= 'a' && ch <= 'z') ||
                (ch >= '0' && ch <= '9') ||
                ch == '-' || ch == '_' || ch == '.' || ch == '~';
            if(unreserved) {
                encoded.push_back(static_cast<char>(ch));
                continue;
            }
            encoded.push_back('%');
            encoded.push_back(kHEX[(ch >> 4U) & 0x0FU]);//NOLINT
            encoded.push_back(kHEX[ch & 0x0FU]); //NOLINT
        }
        return encoded;
    }

    [[nodiscard]] bool valid_http_success(
        const HttpResponse& response,
        std::string& error_out) {

        if(response.status_code == 0 || !response.error_message.empty()) {
            error_out = response.error_message.empty()
                ? "portfolio request failed without an HTTP status"
                : response.error_message;
            return false;
        }
        if(response.status_code < 200 || response.status_code >= 300) { //NOLINT
            error_out = response.body;
            return false;
        }
        return true;
    }

    void append_json_escaped(std::string& out, std::string_view value){
        out.push_back('"');
        for(const char ch : value){ //NOLINT
            switch(ch){
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
                    out.push_back(ch);
                    break;
            }
        }
        out.push_back('"');
    }

    [[nodiscard]] bool append_v2_order_object(
        std::string& body,
        const oms::SubmitOrderCmd& command,
        std::string_view market_ticker,
        std::string& error_out) {

        const auto& order = command.new_order_intent;

        if(command.oms_request_id == 0 ||
        command.client_order_id.empty()) {
            error_out =
                "submit_order: missing OMS or client order identifier";
            return false;
        }

        if(order.quantity_lots <= 0) {
            error_out =
                "submit_order: quantity_lots must be positive";
            return false;
        }

        const auto coordinates = to_v2_order_coordinates(
            order.outcome,
            order.action,
            order.price_ticks);

        if(!coordinates.has_value()) {
            error_out =
                "submit_order: invalid outcome, action, or price_ticks";
            return false;
        }

        const std::string_view tif =
            tif_to_string(order.time_in_force);

        if(tif.empty()) {
            error_out =
                "submit_order: unsupported time_in_force";
            return false;
        }

        if(order.liquidity_intent ==
            oms::intent::LiquidityIntent::kMAKER &&
        order.order_type ==
            oms::intent::OrderType::kMARKETABLE_LIMIT) {
            error_out =
                "submit_order: maker intent cannot be marketable";
            return false;
        }

        const std::string count =
            scaled_integer_to_decimal(
                order.quantity_lots,
                kQUANTITY_LOTS_PER_CONTRACT,
                2);

        const std::string price =
            scaled_integer_to_decimal(
                coordinates->yes_price_ticks,
                kPRICE_TICKS_PER_DOLLAR,
                4);

        body.push_back('{');

        body.append("\"ticker\":");
        append_json_escaped(body, market_ticker);

        body.append(",\"client_order_id\":");
        append_json_escaped(body, command.client_order_id.view());

        body.append(",\"side\":");
        append_json_escaped(body, coordinates->side);

        body.append(",\"count\":");
        append_json_escaped(body, count);

        body.append(",\"price\":");
        append_json_escaped(body, price);

        body.append(",\"time_in_force\":");
        append_json_escaped(body, tif);

        body.append(",\"self_trade_prevention_type\":");
        append_json_escaped(body, "taker_at_cross");

        body.append(",\"post_only\":");
        body.append(
            order.liquidity_intent ==
                oms::intent::LiquidityIntent::kMAKER
                ? "true"
                : "false");

        body.append(",\"cancel_order_on_pause\":true");
        body.append(",\"reduce_only\":");
        body.append(command.reduce_only ? "true" : "false");

        body.push_back('}');
        return true;
    }

    [[nodiscard]] oms::VenueRejectReason venue_reject_reason_for_status(std::uint16_t status_code) noexcept{
        if(status_code == 401 || status_code == 403){//NOLINT
            return oms::VenueRejectReason::kAuthFailed;
        }
        if(status_code == 404){//NOLINT
            return oms::VenueRejectReason::kOrderNotFound;
        }
        if(status_code == 429){//NOLINT
            return oms::VenueRejectReason::kRateLimited;
        }
        if(status_code >= 500){//NOLINT
            return oms::VenueRejectReason::kVenueDown;
        }
        return oms::VenueRejectReason::kUnknown;
    }

    [[nodiscard]] bool parse_execution_summary(
        const nlohmann::json& order_json,
        oms::RestOrderResponse& response,
        std::string& error_out){

        if(!order_json.contains("fill_count") ||
        !order_json.contains("remaining_count")) {
            error_out =
                "successful order response missing fill_count or remaining_count";
            return false;
        }

        const auto fill_quantity = parse_fixed_point(
            order_json["fill_count"],
            kQUANTITY_LOTS_PER_CONTRACT,
            2);

        const auto remaining_quantity = parse_fixed_point(
            order_json["remaining_count"],
            kQUANTITY_LOTS_PER_CONTRACT,
            2);

        if(!fill_quantity.has_value() ||
        !remaining_quantity.has_value()) {
            error_out =
                "order response contains invalid fixed-point quantities";
            return false;
        }

        response.execution_summary_present = true;
        response.acknowledged_fill_qty_lots = *fill_quantity;
        response.acknowledged_remaining_qty_lots =
            *remaining_quantity;

        if(order_json.contains("average_fill_price") &&
        !order_json["average_fill_price"].is_null()) {
            const auto average_price = parse_fixed_point(
                order_json["average_fill_price"],
                kPRICE_TICKS_PER_DOLLAR,
                4);

            if(!average_price.has_value()) {
                error_out =
                    "order response contains invalid average_fill_price";
                return false;
            }

            response.acknowledged_average_fill_price_ticks =
                *average_price;
        }

        if(order_json.contains("average_fee_paid") &&
        !order_json["average_fee_paid"].is_null()) {
            const auto fee = parse_fixed_point(
                order_json["average_fee_paid"],
                kPRICE_TICKS_PER_DOLLAR,
                4);

            if(!fee.has_value()) {
                error_out =
                    "order response contains invalid average_fee_paid";
                return false;
            }

            response.acknowledged_fee_paid_ticks = *fee;
        }

        if(order_json.contains("ts_ms") &&
        order_json["ts_ms"].is_number_unsigned()) {
            response.venue_ts_ms =
                order_json["ts_ms"].get<std::uint64_t>();
        }

        return true;
    }

    [[nodiscard]] bool parse_created_order(
        const nlohmann::json& order_json,
        oms::RestOrderResponse& response,
        std::string& error_out){

        if(!order_json.is_object()) {
            error_out = "order response entry is not an object";
            return false;
        }

        if(order_json.contains("error") &&
        order_json["error"].is_object() &&
        !order_json["error"].empty()) {
            response.result_code =
                oms::RestResultCode::kREJECTED;
            response.venue_reject_reason =
                oms::VenueRejectReason::kUnknown;
            response.raw_reason_message =
                order_json["error"].dump();
            return true;
        }

        if(!order_json.contains("order_id") ||
        !order_json["order_id"].is_string()) {
            error_out =
                "successful order response missing order_id";
            return false;
        }

        oms::ExchangeOrderId exchange_order_id{};
        if(!exchange_order_id.assign_from(
            order_json["order_id"].get_ref<const std::string&>())) {
            error_out =
                "order response contains invalid order_id";
            return false;
        }

        response.exchange_order_id = exchange_order_id;

        if(!parse_execution_summary(
            order_json,
            response,
            error_out)) {
            return false;
        }

        response.result_code = oms::RestResultCode::kACKED;
        response.venue_reject_reason =
            oms::VenueRejectReason::kNone;

        return true;
    }


}//namespace

    KalshiOrderRestAdapter::KalshiOrderRestAdapter(std::shared_ptr<const core::control::OrderRouteUniverse> order_routes):
        order_routes_(std::move(order_routes))
    {
        if(order_routes_){
            for(const auto& route : order_routes_->market_routes){
                ticker_by_market_id_.emplace(route.market_id, route.kalshi_ticker);
                route_by_ticker_.emplace(route.kalshi_ticker, route);
            }
        }
    }

    void KalshiOrderRestAdapter::apply_order_route_universe(std::shared_ptr<const core::control::OrderRouteUniverse> order_routes){
        order_routes_ = std::move(order_routes);
        ticker_by_market_id_.clear();
        route_by_ticker_.clear();
        if(order_routes_){
            for(const auto& route : order_routes_->market_routes){
                ticker_by_market_id_.emplace(route.market_id, route.kalshi_ticker);
                route_by_ticker_.emplace(route.kalshi_ticker, route);
            }
        }
    }

    PreparedOrderRestRequest
    KalshiOrderRestAdapter::prepare_command(
        const oms::OmsToKalshiCommand& command) const {

        return std::visit(
            [this](const auto& cmd) -> PreparedOrderRestRequest {
                using CmdType = std::decay_t<decltype(cmd)>;

                if constexpr(
                    std::is_same_v<CmdType, oms::SubmitOrderCmd>) {
                    return prepare_submit_order(cmd);
                } else if constexpr(
                    std::is_same_v<
                        CmdType,
                        oms::SubmitOrderBatchCmd>) {
                    return prepare_submit_order_batch(cmd);
                } else if constexpr(
                    std::is_same_v<CmdType, oms::CancelOrderCmd>) {
                    return prepare_cancel_order(cmd);
                } else if constexpr(
                    std::is_same_v<CmdType, oms::ModifyOrderCmd>) {
                    return prepare_modify_order(cmd);
                } else if constexpr(
                    std::is_same_v<
                        CmdType,
                        oms::RequestPortfolioReconciliation>) {
                    return PreparedOrderRestRequest{
                        .ok = false,
                        .error_message =
                            "RequestPortfolioReconciliation expands into multiple HTTP requests"
                    };
                } else if constexpr(
                    std::is_same_v<
                        CmdType,
                        oms::CloseOrderRestEgress>) {
                    return PreparedOrderRestRequest{
                        .ok = false,
                        .error_message =
                            "CloseOrderRestEgress does not generate an HTTP request"
                    };
                } else {
                    return PreparedOrderRestRequest{
                        .ok = false,
                        .error_message = "Unknown command type"
                    };
                }
            },
            command);
    }

    PreparedOrderRestRequest
    KalshiOrderRestAdapter::prepare_submit_order(
        const oms::SubmitOrderCmd& command) const {

        PreparedOrderRestRequest result =
            prepare_command_common(
                command,
                oms::RestCommandKind::kSUBMIT_ORDER);

        const auto market_ticker = market_ticker_for_id(
            command.new_order_intent.context.market_id);

        if(!market_ticker.has_value() ||
        market_ticker->empty()) {
            result.error_message =
                "submit_order: market_id missing from order route universe";
            return result;
        }

        std::string body;
        body.reserve(
            320 + //NOLINT
            market_ticker->size() +
            command.client_order_id.view().size());

        if(!append_v2_order_object(
            body,
            command,
            *market_ticker,
            result.error_message)) {
            return result;
        }

        result.request.method = HttpMethod::kPOST;
        result.request.target = build_submit_target();
        result.request.body = std::move(body);
        result.ok = true;

        return result;
    }

    PreparedOrderRestRequest
    KalshiOrderRestAdapter::prepare_submit_order_batch(
        const oms::SubmitOrderBatchCmd& command) const {

        PreparedOrderRestRequest result =
            prepare_command_common(
                command,
                oms::RestCommandKind::kSUBMIT_ORDER_BATCH);

        if(command.oms_request_id == 0 ||
        command.oms_group_id == 0 ||
        command.group_context.group_intent_id == 0) {
            result.error_message =
                "submit_order_batch: missing batch or group identifier";
            return result;
        }

        if(command.order_count == 0 ||
        command.order_count > command.orders.size()) {
            result.error_message =
                "submit_order_batch: invalid order_count";
            return result;
        }

        std::string body;
        body.reserve(
            32 + static_cast<std::size_t>(command.order_count) * 320); //NOLINT

        body.append("{\"orders\":[");

        for(std::uint8_t i = 0; i < command.order_count; ++i) {
            const auto& order_command = command.orders[i];
            const auto& context =
                order_command.new_order_intent.context;

            if(context.strategy_index !=
                command.group_context.strategy_index ||
            context.group_intent_id !=
                command.group_context.group_intent_id ||
            context.universe_version !=
                command.group_context.universe_version ||
            context.event_id !=
                command.group_context.event_id ||
            context.event_revision !=
                command.group_context.event_revision ||
            context.leg_index != i ||
            context.leg_count != command.order_count) {
                result.error_message =
                    "submit_order_batch: inconsistent leg context";
                return result;
            }

            const auto market_ticker =
                market_ticker_for_id(context.market_id);

            if(!market_ticker.has_value() ||
            market_ticker->empty()) {
                result.error_message =
                    "submit_order_batch: market_id missing from order route universe";
                return result;
            }

            if(i != 0) {
                body.push_back(',');
            }

            if(!append_v2_order_object(
                body,
                order_command,
                *market_ticker,
                result.error_message)) {
                return result;
            }
        }

        body.append("]}");

        result.request.method = HttpMethod::kPOST;
        result.request.target = build_submit_batch_target();
        result.request.body = std::move(body);
        result.ok = true;

        return result;
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_cancel_order(const oms::CancelOrderCmd& command) const{
        PreparedOrderRestRequest result = prepare_command_common(command, oms::RestCommandKind::kCANCEL_ORDER);
        if(!command.exchange_order_id.has_value() || command.exchange_order_id->empty()){
            result.error_message = "cancel_order: exchange_order_id is required";
            return result;
        }

        result.request.method = HttpMethod::kDELETE;
        result.request.target = build_cancel_target(*command.exchange_order_id);
        result.ok = true;
        return result;
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_modify_order(const oms::ModifyOrderCmd& command) const{
        PreparedOrderRestRequest result = prepare_command_common(command, oms::RestCommandKind::kMODIFY_ORDER);
        result.error_message = "modify_order: unsupported until ModifyOrderIntent carries replacement fields";
        return result;
    }

    PreparedPortfolioRestRequest
    KalshiOrderRestAdapter::prepare_portfolio_balance_request(
        const oms::RequestPortfolioReconciliation& command,
        HttpRequestId request_id) const {

        PreparedPortfolioRestRequest result{
            .kind = PortfolioRestRequestKind::kBALANCE,
            .reconciliation_id = command.reconciliation_id,
        };
        if(command.reconciliation_id == 0 || request_id == 0) {
            result.error_message =
                "portfolio balance request missing reconciliation or HTTP request id";
            return result;
        }
        result.request.request_id = request_id;
        result.request.method = HttpMethod::kGET;
        result.request.target = build_portfolio_balance_target();
        result.ok = true;
        return result;
    }

    PreparedPortfolioRestRequest
    KalshiOrderRestAdapter::prepare_portfolio_positions_request(
        const oms::RequestPortfolioReconciliation& command,
        HttpRequestId request_id,
        std::string_view cursor) const {

        PreparedPortfolioRestRequest result{
            .kind = PortfolioRestRequestKind::kPOSITIONS,
            .reconciliation_id = command.reconciliation_id,
        };
        if(command.reconciliation_id == 0 || request_id == 0) {
            result.error_message =
                "portfolio positions request missing reconciliation or HTTP request id";
            return result;
        }
        result.request.request_id = request_id;
        result.request.method = HttpMethod::kGET;
        result.request.target = build_portfolio_positions_target(cursor);
        result.ok = true;
        return result;
    }

    CompletedPortfolioBalanceRequest
    KalshiOrderRestAdapter::complete_portfolio_balance_request(
        const PreparedPortfolioRestRequest& prepared,
        const HttpResponse& response) const {

        CompletedPortfolioBalanceRequest result{
            .reconciliation_id = prepared.reconciliation_id,
        };
        if(prepared.kind != PortfolioRestRequestKind::kBALANCE ||
        response.request_id != prepared.request.request_id) {
            result.error_message = "mismatched portfolio balance response";
            return result;
        }
        if(!valid_http_success(response, result.error_message)) {
            return result;
        }
        try {
            const auto parsed = nlohmann::json::parse(response.body);
            if(!parsed.is_object() ||
            !parsed.contains("balance") ||
            !parsed.contains("portfolio_value")) {
                result.error_message =
                    "portfolio balance response missing balance or portfolio_value";
                return result;
            }
            const auto balance = cents_to_money_ticks(parsed["balance"]);
            const auto portfolio_value =
                cents_to_money_ticks(parsed["portfolio_value"]);
            if(!balance.has_value() || !portfolio_value.has_value()) {
                result.error_message =
                    "portfolio balance response contains invalid monetary values";
                return result;
            }
            result.available_balance_ticks = *balance;
            result.portfolio_value_ticks = *portfolio_value;
            result.ok = true;
        } catch(const std::exception& ex) {
            result.error_message = ex.what();
        }
        return result;
    }

    CompletedPortfolioPositionsRequest
    KalshiOrderRestAdapter::complete_portfolio_positions_request(//NOLINT
        const PreparedPortfolioRestRequest& prepared,
        const HttpResponse& response) const {

        CompletedPortfolioPositionsRequest result{
            .reconciliation_id = prepared.reconciliation_id,
        };
        if(prepared.kind != PortfolioRestRequestKind::kPOSITIONS ||
        response.request_id != prepared.request.request_id) {
            result.error_message = "mismatched portfolio positions response";
            return result;
        }
        if(!valid_http_success(response, result.error_message)) {
            return result;
        }

        try {
            const auto parsed = nlohmann::json::parse(response.body);
            if(!parsed.is_object() ||
            !parsed.contains("market_positions") ||
            !parsed["market_positions"].is_array()) {
                result.error_message =
                    "portfolio positions response missing market_positions";
                return result;
            }

            result.positions.reserve(parsed["market_positions"].size());
            for(const auto& item : parsed["market_positions"]) {
                if(!item.is_object() ||
                !item.contains("ticker") ||
                !item["ticker"].is_string() ||
                !item.contains("position_fp")) {
                    result.error_message =
                        "portfolio position entry missing ticker or position_fp";
                    return result;
                }

                const auto position = parse_fixed_point(
                    item["position_fp"],
                    kQUANTITY_LOTS_PER_CONTRACT,
                    2,
                    true);
                if(!position.has_value()) {
                    result.error_message =
                        "portfolio position entry has invalid position_fp";
                    return result;
                }

                oms::VenueMarketPositionSnapshot snapshot{
                    .market_ticker = item["ticker"].get<std::string>(),
                    .net_position_lots = *position,
                };

                const auto route_it =
                    route_by_ticker_.find(snapshot.market_ticker);
                if(route_it != route_by_ticker_.end()) {
                    snapshot.market_id = route_it->second.market_id;
                    snapshot.event_id = route_it->second.event_id;
                }

                auto parse_optional_money = [&](
                    std::string_view field,
                    std::int64_t& destination,//NOLINT
                    bool& present,
                    MoneyTickRounding rounding) -> bool {
                    if(!item.contains(field) || item[field].is_null()) {
                        return true;
                    }
                    if(!item[field].is_string()) {
                        return false;
                    }
                    const auto value = parse_money_ticks(
                        item[field].get_ref<const std::string&>(),
                        rounding);
                    if(!value.has_value()) {
                        return false;
                    }
                    destination = *value;
                    present = true;
                    return true;
                };

                if(!parse_optional_money(
                    "market_exposure_dollars",
                    snapshot.market_exposure_ticks,
                    snapshot.market_exposure_present,
                    MoneyTickRounding::kAWAY_FROM_ZERO)) {
                    result.error_message =
                        "portfolio position entry has invalid market_exposure_dollars";
                    return result;
                }

                if(!parse_optional_money(
                    "realized_pnl_dollars",
                    snapshot.realized_pnl_ticks,
                    snapshot.realized_pnl_present,
                    MoneyTickRounding::kFLOOR) ||
                !parse_optional_money(
                    "fees_paid_dollars",
                    snapshot.fees_paid_ticks,
                    snapshot.fees_paid_present,
                    MoneyTickRounding::kAWAY_FROM_ZERO)) {
                    result.error_message =
                        "portfolio position entry has invalid PnL or fee value";
                    return result;
                }

                result.positions.push_back(std::move(snapshot));
            }

            if(parsed.contains("cursor") && !parsed["cursor"].is_null()) {
                if(!parsed["cursor"].is_string()) {
                    result.error_message =
                        "portfolio positions cursor is not a string";
                    return result;
                }
                result.next_cursor = parsed["cursor"].get<std::string>();
            }
            result.ok = true;
        } catch(const std::exception& ex) {
            result.error_message = ex.what();
        }
        return result;
    }

    CompletedOrderRestRequest
    KalshiOrderRestAdapter::complete_request(
        const PreparedOrderRestRequest& prepared,
        const HttpResponse& response) const {

        CompletedOrderRestRequest result;
        result.response.command_kind = prepared.command_kind;
        result.response.transport_submit_ts_ns =
            prepared.request.trace.request_sent_ts_ns;
        result.response.transport_recv_ts_ns =
            response.trace.response_recv_ts_ns;
        result.response.http_status_code = response.status_code;
        result.response.retry_count = response.trace.retry_count;

        if(prepared.command_kind ==
        oms::RestCommandKind::kSUBMIT_ORDER_BATCH) {
            result.response.result_code =
                oms::RestResultCode::kNOT_SENT;
            result.error_message =
                "batch request passed to single-order completion";
            result.response.raw_reason_message =
                result.error_message;
            return result;
        }

        std::visit(
            [&result](const auto& cmd) {
                using T = std::decay_t<decltype(cmd)>;

                if constexpr(
                    std::is_same_v<T, oms::RequestPortfolioReconciliation>) {
                    result.response.context.oms_request_id =
                        cmd.reconciliation_id;
                } else {
                    result.response.context.oms_request_id =
                        cmd.oms_request_id;
                }

                if constexpr(
                    std::is_same_v<T, oms::SubmitOrderCmd>) {
                    result.response.context.context =
                        cmd.new_order_intent.context;
                    result.response.client_order_id =
                        cmd.client_order_id;
                } else if constexpr(
                    std::is_same_v<T, oms::CancelOrderCmd>) {
                    result.response.context.context =
                        cmd.cancel_order_intent.context;
                    result.response.client_order_id =
                        cmd.client_order_id;

                    if(cmd.exchange_order_id.has_value()) {
                        result.response.exchange_order_id =
                            *cmd.exchange_order_id;
                    }
                } else if constexpr(
                    std::is_same_v<T, oms::ModifyOrderCmd>) {
                    result.response.context.context =
                        cmd.modify_order_intent.context;
                    result.response.client_order_id =
                        cmd.client_order_id;

                    if(cmd.exchange_order_id.has_value()) {
                        result.response.exchange_order_id =
                            *cmd.exchange_order_id;
                    }
                }
            },
            prepared.source_command);

        if(response.status_code == 0 ||
        !response.error_message.empty()) {
            result.response.result_code =
                oms::RestResultCode::kTRANSPORT_ERROR;
            result.response.venue_reject_reason =
                oms::VenueRejectReason::kVenueDown;
            result.response.raw_reason_message =
                response.error_message;
            result.error_message = response.error_message;
            return result;
        }

        if(response.status_code < 200 || //NOLINT
        response.status_code >= 300) { //NOLINT
            result.response.result_code =
                oms::RestResultCode::kREJECTED;
            result.response.venue_reject_reason =
                venue_reject_reason_for_status(
                    response.status_code);
            result.response.raw_reason_message =
                response.body;
            result.error_message = response.body;
            return result;
        }

        if(prepared.command_kind ==
        oms::RestCommandKind::kSUBMIT_ORDER) {
            try {
                const auto parsed =
                    nlohmann::json::parse(response.body);

                if(!parse_created_order(
                    parsed,
                    result.response,
                    result.error_message)) {
                    result.response.result_code =
                        oms::RestResultCode::kTRANSPORT_ERROR;
                    result.response.venue_reject_reason =
                        oms::VenueRejectReason::kUnknown;
                    result.response.raw_reason_message =
                        response.body;
                    return result;
                }
            } catch(const std::exception& ex) {
                result.response.result_code =
                    oms::RestResultCode::kTRANSPORT_ERROR;
                result.response.venue_reject_reason =
                    oms::VenueRejectReason::kUnknown;
                result.response.raw_reason_message =
                    response.body;
                result.error_message = ex.what();
                return result;
            }
        } else {
            result.response.result_code =
                oms::RestResultCode::kACKED;
            result.response.venue_reject_reason =
                oms::VenueRejectReason::kNone;
        }

        result.ok = true;
        return result;
    }

    CompletedOrderRestBatchRequest
    KalshiOrderRestAdapter::complete_batch_request( //NOLINT
        const PreparedOrderRestRequest& prepared,
        const HttpResponse& response) const {

        CompletedOrderRestBatchRequest result;

        const auto* command =
            std::get_if<oms::SubmitOrderBatchCmd>(
                &prepared.source_command);

        if(command == nullptr ||
        prepared.command_kind !=
            oms::RestCommandKind::kSUBMIT_ORDER_BATCH) {
            result.response.result_code =
                oms::RestResultCode::kNOT_SENT;
            result.error_message =
                "non-batch request passed to batch completion";
            result.response.raw_reason_message =
                result.error_message;
            return result;
        }
        if(command->order_count == 0 ||
        command->order_count > command->orders.size() ||
        command->order_count >
            result.response.order_responses.size()) {
            result.response.result_code =
                oms::RestResultCode::kNOT_SENT;
            result.error_message =
                "batch completion contains invalid order_count";
            result.response.raw_reason_message =
                result.error_message;
            return result;
        }

        result.response.batch_oms_request_id =
            command->oms_request_id;
        result.response.oms_group_id =
            command->oms_group_id;
        result.response.group_context =
            command->group_context;
        result.response.requested_order_count =
            command->order_count;
        result.response.transport_submit_ts_ns =
            prepared.request.trace.request_sent_ts_ns;
        result.response.transport_recv_ts_ns =
            response.trace.response_recv_ts_ns;
        result.response.http_status_code =
            response.status_code;
        result.response.retry_count =
            response.trace.retry_count;

        for(std::uint8_t i = 0; i < command->order_count; ++i) {
            auto& leg_response =
                result.response.order_responses[i];
            const auto& leg_command = command->orders[i];

            leg_response.context = oms::OmsContext{
                .oms_request_id = leg_command.oms_request_id,
                .context =
                    leg_command.new_order_intent.context,
            };
            leg_response.command_kind =
                oms::RestCommandKind::kSUBMIT_ORDER;
            leg_response.result_code =
                oms::RestResultCode::kTRANSPORT_ERROR;
            leg_response.client_order_id =
                leg_command.client_order_id;
            leg_response.transport_submit_ts_ns =
                prepared.request.trace.request_sent_ts_ns;
            leg_response.transport_recv_ts_ns =
                response.trace.response_recv_ts_ns;
            leg_response.http_status_code =
                response.status_code;
            leg_response.retry_count =
                response.trace.retry_count;
        }

        if(response.status_code == 0 ||
        !response.error_message.empty()) {
            result.response.result_code =
                oms::RestResultCode::kTRANSPORT_ERROR;
            result.response.venue_reject_reason =
                oms::VenueRejectReason::kVenueDown;
            result.response.raw_reason_message =
                response.error_message;
            result.error_message = response.error_message;
            return result;
        }

        if(response.status_code < 200 || //NOLINT
        response.status_code >= 300) { //NOLINT
            result.response.result_code =
                oms::RestResultCode::kREJECTED;
            result.response.venue_reject_reason =
                venue_reject_reason_for_status(
                    response.status_code);
            result.response.raw_reason_message =
                response.body;
            result.error_message = response.body;

            for(std::uint8_t i = 0;
                i < command->order_count;
                ++i) {
                auto& leg_response =
                    result.response.order_responses[i];
                leg_response.result_code =
                    oms::RestResultCode::kREJECTED;
                leg_response.venue_reject_reason =
                    result.response.venue_reject_reason;
                leg_response.raw_reason_message =
                    response.body;
            }

            return result;
        }

        try {
            const auto parsed =
                nlohmann::json::parse(response.body);

            if(!parsed.is_object() ||
            !parsed.contains("orders") ||
            !parsed["orders"].is_array()) {
                result.error_message =
                    "batch response missing orders array";
                result.response.result_code =
                    oms::RestResultCode::kTRANSPORT_ERROR;
                result.response.raw_reason_message =
                    response.body;
                return result;
            }

            std::array<
                bool,
                oms::intent::kMAX_ORDERS_PER_GROUP
            > response_seen{};

            for(const auto& order_json : parsed["orders"]) {
                if(!order_json.is_object() ||
                !order_json.contains("client_order_id") ||
                !order_json["client_order_id"].is_string()) {
                    result.error_message =
                        "batch response entry missing client_order_id";
                    result.response.result_code =
                        oms::RestResultCode::kTRANSPORT_ERROR;
                    result.response.raw_reason_message =
                        response.body;
                    return result;
                }

                const auto& returned_client_id =
                    order_json["client_order_id"]
                        .get_ref<const std::string&>();

                std::optional<std::uint8_t> matched_index;

                for(std::uint8_t i = 0;
                    i < command->order_count;
                    ++i) {
                    if(!response_seen[i] &&
                    command->orders[i]
                            .client_order_id.view() ==
                        returned_client_id) {
                        matched_index = i;
                        break;
                    }
                }

                if(!matched_index.has_value()) {
                    result.error_message =
                        "batch response contains unknown or duplicate client_order_id";
                    result.response.result_code =
                        oms::RestResultCode::kTRANSPORT_ERROR;
                    result.response.raw_reason_message =
                        response.body;
                    return result;
                }

                auto& leg_response =
                    result.response.order_responses[
                        *matched_index];//NOLINT -- bugprone-unchecked-optional-access check is above

                if(!parse_created_order(
                    order_json,
                    leg_response,
                    result.error_message)) {
                    result.response.result_code =
                        oms::RestResultCode::kTRANSPORT_ERROR;
                    result.response.raw_reason_message =
                        response.body;
                    return result;
                }

                response_seen[*matched_index] = true; //NOLINT -- bugprone-unchecked-optional-access check is above
                ++result.response.response_order_count;
            }

            if(result.response.response_order_count !=
            command->order_count) {
                result.error_message =
                    "batch response omitted one or more submitted orders";
                result.response.result_code =
                    oms::RestResultCode::kTRANSPORT_ERROR;
                result.response.raw_reason_message =
                    response.body;
                return result;
            }

            result.response.result_code =
                oms::RestResultCode::kACKED;
            result.response.venue_reject_reason =
                oms::VenueRejectReason::kNone;
            result.ok = true;
            return result;

        } catch(const std::exception& ex) {
            result.response.result_code =
                oms::RestResultCode::kTRANSPORT_ERROR;
            result.response.venue_reject_reason =
                oms::VenueRejectReason::kUnknown;
            result.response.raw_reason_message =
                response.body;
            result.error_message = ex.what();
            return result;
        }
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_command_common(oms::OmsToKalshiCommand command, oms::RestCommandKind command_kind) const{
        PreparedOrderRestRequest result;
        result.source_command = std::move(command); //NOLINT
        result.command_kind = command_kind;
        std::visit([&result](const auto& cmd){
            using T = std::decay_t<decltype(cmd)>;
            if constexpr(std::is_same_v<T, oms::RequestPortfolioReconciliation>){
                result.request.request_id = cmd.reconciliation_id;
            }else{
                result.request.request_id = cmd.oms_request_id;
            }
        }, result.source_command);
        return result;
    }

    std::optional<std::string_view> KalshiOrderRestAdapter::market_ticker_for_id(oms::intent::MarketId market_id) const noexcept{
        auto iter = ticker_by_market_id_.find(market_id);
        if(iter != ticker_by_market_id_.end()){
            return iter->second;
        }
        return {};
    }

    std::string KalshiOrderRestAdapter::build_submit_target(){
        return "/trade-api/v2/portfolio/events/orders";
    }
    std::string KalshiOrderRestAdapter::build_submit_batch_target() {
        return "/trade-api/v2/portfolio/events/orders/batched";
    }
    std::string KalshiOrderRestAdapter::build_cancel_target(const oms::ExchangeOrderId& exchange_order_id){
        std::string target{"/trade-api/v2/portfolio/events/orders/"};
        target.append(exchange_order_id.view());
        return target;
    }
    std::string KalshiOrderRestAdapter::build_modify_target(const oms::ExchangeOrderId& exchange_order_id){
        std::string target{"/trade-api/v2/portfolio/events/orders/"};
        target.append(exchange_order_id.view());
        target.append("/amend");
        return target;
    }
    std::string KalshiOrderRestAdapter::build_portfolio_balance_target(){
        return "/trade-api/v2/portfolio/balance";
    }
    std::string KalshiOrderRestAdapter::build_portfolio_positions_target(
        std::string_view cursor){
        std::string target{
            "/trade-api/v2/portfolio/positions?limit=1000&count_filter=position"};
        if(!cursor.empty()){
            target.append("&cursor=");
            target.append(percent_encode_query_value(cursor));
        }
        return target;
    }
}
