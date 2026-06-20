#include "predex/exchange/kalshi/adapters/market_data_handler.hpp"
#include <string>


namespace {
    std::string 
} // anonymous

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

    std::string KalshiMarketDataHandler::build_subscribe_message(std::span<const std::string> tickers) const{}
    




}