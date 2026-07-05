#pragma once 
#include <span>
#include <string_view>
#include "predex/exchange/kalshi/handler.hpp"
#include "predex/exchange/kalshi/adapters/auth_signer.hpp"
#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"

namespace predex::exchange::kalshi{
    inline constexpr std::string_view kWEBSOCKET_ENDPOINT = "wss://api.elections.kalshi.com/trade-api/ws/v2";
    class KalshiOrderDataHandler final : public IWsAdapter {
    public:
        explicit KalshiOrderDataHandler(AuthSigner signer);

        [[nodiscard]] std::string name() const override;
        [[nodiscard]] WsConnectRequest build_connect_request() const override;

        [[nodiscard]] std::string build_subscribe_message(std::uint64_t req_id,KalshiOrderDataChannel channel, std::span<const std::string> tickers)const; // tickers not needed, but leaving the method signature in case I want to support narrower updates
        [[nodiscard]] std::string build_unsubscribe_message(std::uint64_t req_id, KalshiOrderDataChannel channel, std::span<const std::int64_t> session_ids)const;

    private:
        AuthSigner signer_;
    };

}