#pragma once 

#include "predex/exchange/kalshi/handler.hpp"
#include "predex/exchange/kalshi/adapters/auth_signer.hpp"

namespace predex::exchange::kalshi{
    inline constexpr std::string_view kWEBSOCKET_ENDPOINT = "wss://api.elections.kalshi.com/trade-api/ws/v2";
    class KalshiMarketDataHandler final : public IWsAdapter {
    public:
        explicit KalshiMarketDataHandler(AuthSigner signer);

        [[nodiscard]] std::string name() const override;
        [[nodiscard]] WsConnectRequest build_connect_request() const override;
        [[nodiscard]] std::string build_subscribe_message(std::span<const std::string> tickers) const override;
        [[nodiscard]] std::string build_unsubscribe_message(std::span<const std::string> tickers) const override;

    private:
        AuthSigner signer_;
    };

}
