#include "predex/exchange/kalshi/adapters/order_data_handler.hpp"

#include <nlohmann/json.hpp>
#include <string>


namespace{
    std::string channel_to_string(predex::exchange::kalshi::KalshiOrderDataChannel channel){
        switch(channel){
            case predex::exchange::kalshi::KalshiOrderDataChannel::kFILL:
                return "fill";
            case predex::exchange::kalshi::KalshiOrderDataChannel::kMARKET_POSITIONS:
                return "market_positions";
            case predex::exchange::kalshi::KalshiOrderDataChannel::kUSER_ORDERS:
                return "user_orders";
            default:
                throw std::invalid_argument("Unknown channel enum value");
        }
    }


}

namespace predex::exchange::kalshi{
    KalshiOrderDataHandler::KalshiOrderDataHandler(AuthSigner signer) : signer_(std::move(signer)) {}

    std::string KalshiOrderDataHandler::name() const {
        return "kalshi_order_data_handler";
    }

    WsConnectRequest KalshiOrderDataHandler::build_connect_request() const {
        const auto auth_headers = signer_.make_ws_headers("/trade-api/ws/v2");
        return WsConnectRequest{
            .endpoint = std::string(kORDER_DATA_WEBSOCKET_ENDPOINT),
            .headers =
                {
                    {"KALSHI-ACCESS-KEY", auth_headers.key_id},
                    {"KALSHI-ACCESS-TIMESTAMP", auth_headers.timestamp_ms},
                    {"KALSHI-ACCESS-SIGNATURE", auth_headers.signature_base64},
                },
        };
    }

    std::string KalshiOrderDataHandler::build_subscribe_message(std::uint64_t req_id, KalshiOrderDataChannel channel, std::span<const std::string> tickers) const {
        nlohmann::json params{{"channels", {channel_to_string(channel)}}};
        if (!tickers.empty()) {
            params["market_tickers"] = tickers;
        }

        const nlohmann::json payload{
            {"id", req_id},
            {"cmd", "subscribe"},
            {"params", std::move(params)},
        };
        return payload.dump();
    }

    std::string KalshiOrderDataHandler::build_unsubscribe_message(std::uint64_t req_id, KalshiOrderDataChannel channel, std::span<const std::int64_t> session_ids) const {
        nlohmann::json params{{"channels", {channel_to_string(channel)}}};
        if (!session_ids.empty()) {
            params["session_ids"] = session_ids;
        }

        const nlohmann::json payload{
            {"id", req_id},
            {"cmd", "unsubscribe"},
            {"params", std::move(params)},
        };
        return payload.dump();
    }
}
