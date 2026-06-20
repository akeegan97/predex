
#include "predex/exchange/kalshi/adapters/market_data_handler.hpp"

#include <nlohmann/json.hpp>
#include <string>


namespace {

    std::string channel_to_string(predex::exchange::kalshi::KalshiMarketDataChannel channel){
        switch(channel){
            case predex::exchange::kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA:
                return "orderbook_delta";
            case predex::exchange::kalshi::KalshiMarketDataChannel::kTRADE:
                return "trade";
            case predex::exchange::kalshi::KalshiMarketDataChannel::kMARKET_LIFECYCLE:
                return "market_lifecycle_v2";
            default:
                throw std::invalid_argument("Unknown channel enum value");
        }
    }
}

namespace predex::exchange::kalshi{

    KalshiMarketDataHandler::KalshiMarketDataHandler(AuthSigner signer) : signer_(std::move(signer)) {}

    std::string KalshiMarketDataHandler::name() const {
        return "kalshi_market_data_handler";
    }

    WsConnectRequest KalshiMarketDataHandler::build_connect_request() const {
        const auto auth_headers = signer_.make_ws_headers("/trade-api/ws/v2");
        return WsConnectRequest{
            .endpoint = std::string(kWEBSOCKET_ENDPOINT),
            .headers =
                {
                    {"KALSHI-ACCESS-KEY", auth_headers.key_id},
                    {"KALSHI-ACCESS-TIMESTAMP", auth_headers.timestamp_ms},
                    {"KALSHI-ACCESS-SIGNATURE", auth_headers.signature_base64},
                },
        };
    }

    std::string KalshiMarketDataHandler::build_subscribe_message(std::uint64_t req_id, KalshiMarketDataChannel channel, std::span<const std::string> tickers) const {
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
//NOLINTNEXTLINE -- allowing req_id & session_id to be left as is instead of wrapping into a strong typedef 
    std::string KalshiMarketDataHandler::build_update_message(std::uint64_t req_id, std::int64_t session_id, std::span<const std::string> tickers, std::string_view action) const {
        nlohmann::json params{
            {"sid", session_id},
            {"action", action},
        };
        if (!tickers.empty()) {
            params["market_tickers"] = tickers;
        }

        const nlohmann::json payload{
            {"id", req_id},
            {"cmd", "update_subscription"},
            {"params", std::move(params)},
        };
        return payload.dump();
    }

    std::string KalshiMarketDataHandler::build_unsubscribe_message(std::uint64_t req_id, std::span<const std::int64_t> session_ids) const {
        nlohmann::json params{
            {"sids", session_ids},
        };

        const nlohmann::json payload{
            {"id", req_id},
            {"cmd", "unsubscribe"},
            {"params", std::move(params)},
        };
        return payload.dump();
    }



}