#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"
#include "predex/control/control_types.hpp"



namespace predex::exchange::kalshi{
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
            } else {
                PreparedOrderRestRequest result;
                result.ok = false;
                result.error_message = "Unknown command type";
                return result;
            }
        }, command);
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_submit_order(const oms::SubmitOrderCmd& command) const{
        return prepare_command_common(command, oms::RestCommandKind::kSUBMIT_ORDER);
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_cancel_order(const oms::CancelOrderCmd& command) const{
        return prepare_command_common(command, oms::RestCommandKind::kCANCEL_ORDER);
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_modify_order(const oms::ModifyOrderCmd& command) const{
        return prepare_command_common(command, oms::RestCommandKind::kMODIFY_ORDER);
    }

    CompletedOrderRestRequest KalshiOrderRestAdapter::complete_request(
        const PreparedOrderRestRequest& prepared,
        const HttpResponse& response
    ) const{
        CompletedOrderRestRequest result;
    }

    PreparedOrderRestRequest KalshiOrderRestAdapter::prepare_command_common(oms::OmsToKalshiCommand command, oms::RestCommandKind command_kind) const{
        PreparedOrderRestRequest result;
        result.ok = true;
        result.command = (command);
        result.command_kind = command_kind;
        return result;
    }
}



/*

    struct PreparedOrderRestRequest {
        bool ok{false};
        HttpRequest request;
        oms::OmsToKalshiCommand source_command;
        oms::RestCommandKind command_kind{oms::RestCommandKind::kUNKNOWN};
        std::string error_message;
    };

        struct HttpRequest {
        HttpRequestId request_id{};
        HttpMethod method{HttpMethod::kGET};
        std::string target;
        std::string body;
        std::string content_type{"application/json"};
        bool authenticate{true};
        std::vector<HttpHeader> headers;
        HttpTrace trace;
    };

    enum class HttpMethod : std::uint8_t {
        kGET = 1,
        kPOST = 2,
        kDELETE = 3,
        kPUT = 4,
    };


*/