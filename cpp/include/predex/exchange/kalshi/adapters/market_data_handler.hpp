#pragma once 
#include <span>
#include "predex/exchange/kalshi/handler.hpp"
#include "predex/exchange/kalshi/adapters/auth_signer.hpp"
#include "predex/exchange/kalshi/market_data_protocol.hpp"

namespace predex::exchange::kalshi{
    inline constexpr std::string_view kWEBSOCKET_ENDPOINT = "wss://api.elections.kalshi.com/trade-api/ws/v2";
    class KalshiMarketDataHandler final : public IWsAdapter {
    public:
        explicit KalshiMarketDataHandler(AuthSigner signer);

        [[nodiscard]] std::string name() const override;
        [[nodiscard]] WsConnectRequest build_connect_request() const override;

        [[nodiscard]] std::string build_subscribe_message(std::uint64_t req_id,KalshiMarketDataChannel channel, std::span<const std::string> tickers)const;
        [[nodiscard]] std::string build_update_message(std::uint64_t req_id, std::int64_t session_id, std::span<const std::string> tickers, std::string_view action)const;
        [[nodiscard]] std::string build_unsubscribe_message(std::uint64_t req_id, std::span<const std::int64_t> session_ids)const;

    private:
        AuthSigner signer_;
    };

}
