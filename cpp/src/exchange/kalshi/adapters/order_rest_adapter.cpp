#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"
#include "predex/control/control_types.hpp"

#include <nlohmann/json.hpp>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>


namespace predex::exchange::kalshi{
namespace {
    constexpr std::int64_t kPRICE_TICKS_PER_DOLLAR = 10000;
    constexpr std::int64_t kTICKS_PER_CENT = kPRICE_TICKS_PER_DOLLAR / 100;
    constexpr std::int64_t kMIN_KALSHI_CENTS = 1;
    constexpr std::int64_t kMAX_KALSHI_CENTS = 99;

    [[nodiscard]] std::string_view action_to_string(oms::intent::OrderAction action) noexcept{
        switch(action){
            case oms::intent::OrderAction::kBUY:
                return "buy";
            case oms::intent::OrderAction::kSELL:
                return "sell";
            case oms::intent::OrderAction::kUNKNOWN:
                return {};
        }
        return {};
    }

    [[nodiscard]] std::string_view outcome_to_string(oms::intent::Outcome outcome) noexcept{
        switch(outcome){
            case oms::intent::Outcome::kYES:
                return "yes";
            case oms::intent::Outcome::kNO:
                return "no";
            case oms::intent::Outcome::kUNKNOWN:
                return {};
        }
        return {};
    }

    [[nodiscard]] std::string_view tif_to_string(oms::intent::TimeInForce time_in_force) noexcept{
        switch(time_in_force){
            case oms::intent::TimeInForce::kGTC:
                return "good_til_cancelled";
            case oms::intent::TimeInForce::kIOC:
                return "immediate_or_cancel";
            case oms::intent::TimeInForce::kFOK:
                return "fill_or_kill";
        }
        return {};
    }

    [[nodiscard]] std::optional<std::int64_t> price_ticks_to_cents(std::int64_t price_ticks) noexcept{
        if(price_ticks <= 0 || price_ticks >= kPRICE_TICKS_PER_DOLLAR){
            return std::nullopt;
        }
        if(price_ticks % kTICKS_PER_CENT != 0){
            return std::nullopt;
        }
        const std::int64_t cents = price_ticks / kTICKS_PER_CENT;
        if(cents < kMIN_KALSHI_CENTS || cents > kMAX_KALSHI_CENTS){
            return std::nullopt;
        }
        return cents;
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

    [[nodiscard]] std::string lots_to_count_fp(std::int64_t quantity_lots){
        return std::to_string(quantity_lots) + ".00";
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
}

    KalshiOrderRestAdapter::KalshiOrderRestAdapter(std::shared_ptr<const core::control::OrderRouteUniverse> order_routes):
        order_routes_(std::move(order_routes))
    {
        if(order_routes_){
            for(const auto& route : order_routes_->market_routes){
                ticker_by_market_id_.emplace(route.market_id, route.kalshi_ticker);
            }
        }
    }

    void KalshiOrderRestAdapter::apply_order_route_universe(std::shared_ptr<const core::control::OrderRouteUniverse> order_routes){
        order_routes_ = std::move(order_routes);
        ticker_by_market_id_.clear();
        if(order_routes_){
            for(const auto& route : order_routes_->market_routes){
                ticker_by_market_id_.emplace(route.market_id, route.kalshi_ticker);
            }
        }
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_command(const oms::OmsToKalshiCommand& command) const{
        return std::visit([this](const auto& cmd) -> PreparedOrderRestRequest{
            using CmdType = std::decay_t<decltype(cmd)>;
            if constexpr(std::is_same_v<CmdType, oms::SubmitOrderCmd>){
                return prepare_submit_order(cmd);
            } else if constexpr(std::is_same_v<CmdType, oms::CancelOrderCmd>){
                return prepare_cancel_order(cmd);
            } else if constexpr(std::is_same_v<CmdType, oms::ModifyOrderCmd>){
                return prepare_modify_order(cmd);
            } else if constexpr(std::is_same_v<CmdType, oms::CloseOrderRestEgress>){
                return PreparedOrderRestRequest{.ok = false, .error_message = "CloseOrderRestEgress does not generate an HTTP request"};
            } else {
                PreparedOrderRestRequest result;
                result.ok = false;
                result.error_message = "Unknown command type";
                return result;
            }
        }, command);
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_submit_order(const oms::SubmitOrderCmd& command) const{
        PreparedOrderRestRequest result = prepare_command_common(command, oms::RestCommandKind::kSUBMIT_ORDER);
        const auto& intent = command.new_order_intent;

        const auto market_ticker = market_ticker_for_id(intent.context.market_id);
        if(!market_ticker.has_value() || market_ticker->empty()){
            result.error_message = "submit_order: market_id missing from order route universe";
            return result;
        }

        const std::string_view action = action_to_string(intent.action);
        if(action.empty()){
            result.error_message = "submit_order: action must be buy or sell";
            return result;
        }

        const std::string_view outcome = outcome_to_string(intent.outcome);
        if(outcome.empty()){
            result.error_message = "submit_order: outcome must be yes or no";
            return result;
        }

        const std::string_view tif = tif_to_string(intent.time_in_force);
        if(tif.empty()){
            result.error_message = "submit_order: unsupported time_in_force";
            return result;
        }

        if(intent.quantity_lots <= 0){
            result.error_message = "submit_order: quantity_lots must be positive";
            return result;
        }

        const std::optional<std::int64_t> price_cents = price_ticks_to_cents(intent.price_ticks);
        if(!price_cents.has_value()){
            result.error_message = "submit_order: price_ticks must convert to Kalshi integer cents in [1,99]";
            return result;
        }

        const std::string_view price_field = intent.outcome == oms::intent::Outcome::kYES ? "yes_price" : "no_price";
        std::string body;
        body.reserve(256 + market_ticker->size() + command.client_order_id.view().size()); //NOLINT
        body.push_back('{');
        body.append("\"ticker\":");
        append_json_escaped(body, *market_ticker);
        body.append(",\"action\":");
        append_json_escaped(body, action);
        body.append(",\"client_order_id\":");
        append_json_escaped(body, command.client_order_id.view());
        body.append(",\"side\":");
        append_json_escaped(body, outcome);
        body.append(",\"count_fp\":");
        append_json_escaped(body, lots_to_count_fp(intent.quantity_lots));
        body.append(",\"time_in_force\":");
        append_json_escaped(body, tif);
        body.push_back(',');
        append_json_escaped(body, price_field);
        body.push_back(':');
        body.append(std::to_string(*price_cents));
        body.push_back('}');

        result.request.method = HttpMethod::kPOST;
        result.request.target = build_submit_target();
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

    CompletedOrderRestRequest KalshiOrderRestAdapter::complete_request(const PreparedOrderRestRequest& prepared, const HttpResponse& response) const{//NOLINT
        CompletedOrderRestRequest result;
        result.response.command_kind = prepared.command_kind;
        result.response.transport_submit_ts_ns = prepared.request.trace.request_sent_ts_ns;
        result.response.transport_recv_ts_ns = response.trace.response_recv_ts_ns;
        result.response.http_status_code = response.status_code;
        result.response.retry_count = response.trace.retry_count;
        result.response.raw_reason_message = response.ok ? std::string{} : response.body;

        std::visit([&](const auto& cmd){
            result.response.context = oms::OmsContext{
                .oms_request_id  = cmd.oms_request_id
            };
            using T = std::decay_t<decltype(cmd)>;

            if constexpr(std::is_same_v<T, oms::SubmitOrderCmd>){
                result.response.context.context = cmd.new_order_intent.context;
                result.response.client_order_id = cmd.client_order_id;
            }else if constexpr(std::is_same_v<T, oms::CancelOrderCmd>){
                result.response.context.context = cmd.cancel_order_intent.context;
                result.response.client_order_id = cmd.client_order_id;
            }else if constexpr(std::is_same_v<T, oms::ModifyOrderCmd>){
                result.response.context.context = cmd.modify_order_intent.context;
                result.response.client_order_id = cmd.client_order_id;
            }

           if constexpr(requires {cmd.exchange_order_id;}){if(cmd.exchange_order_id.has_value()){result.response.exchange_order_id = cmd.exchange_order_id.value();}}
        }, prepared.source_command);

        if(response.status_code == 0 || !response.error_message.empty()){
            result.response.result_code = oms::RestResultCode::kTRANSPORT_ERROR;
            result.response.venue_reject_reason = oms::VenueRejectReason::kVenueDown;
            result.error_message = response.error_message;
            return result;
        }
        if(response.status_code >= 200 && response.status_code < 300){ //NOLINT
            result.response.result_code = oms::RestResultCode::kACKED;
            if(prepared.command_kind == oms::RestCommandKind::kSUBMIT_ORDER){
                try{
                    const auto parsed = nlohmann::json::parse(response.body);
                    if(parsed.contains("order") && parsed["order"].is_object()){
                        const auto& order = parsed["order"];
                        if(order.contains("order_id") && order["order_id"].is_string()){
                            oms::ExchangeOrderId exchange_order_id{};
                            if(exchange_order_id.assign_from(order["order_id"].get<std::string>())){
                                result.response.exchange_order_id = exchange_order_id;
                            }
                        }
                    }
                    if(result.response.exchange_order_id.empty()){
                        result.ok = false;
                        result.response.result_code = oms::RestResultCode::kTRANSPORT_ERROR;
                        result.response.venue_reject_reason = oms::VenueRejectReason::kUnknown;
                        result.error_message = "submit_order: successful response missing order_id";
                        result.response.raw_reason_message = response.body;
                        return result;
                    }
                } catch(const std::exception& ex){
                    result.ok = false;
                    result.response.result_code = oms::RestResultCode::kTRANSPORT_ERROR;
                    result.response.venue_reject_reason = oms::VenueRejectReason::kUnknown;
                    result.error_message = ex.what();
                    result.response.raw_reason_message = response.body;
                    return result;
                }
            }
            result.ok = true;
            return result;
        } 

        result.response.result_code = oms::RestResultCode::kREJECTED;
        result.response.venue_reject_reason = venue_reject_reason_for_status(response.status_code);
        result.error_message = response.body;
        return result;

    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_command_common(oms::OmsToKalshiCommand command, oms::RestCommandKind command_kind) const{
        PreparedOrderRestRequest result;
        result.source_command = std::move(command); //NOLINT
        result.command_kind = command_kind;
        std::visit([&result](const auto& cmd){
            result.request.request_id = cmd.oms_request_id;
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
}

 