#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "predex/websocket/kalshi/auth_signer.hpp"
#include "predex/websocket/ws_adapter.hpp"

namespace predex::websocket::kalshi {

class WsAdapter final : public adapter::IExchangeWsAdapter {
  public:
    explicit WsAdapter(AuthSigner signer,
                       std::string endpoint = "wss://api.elections.kalshi.com/trade-api/ws/v2");

    [[nodiscard]] std::string name() const override;
    [[nodiscard]] adapter::ConnectRequest build_connect_request() const override;
    [[nodiscard]] std::string
    build_subscribe_message(std::string_view channel,
                            const std::vector<std::string>& market_tickers) const override;

  private:
    AuthSigner signer_;
    std::string endpoint_;
};

} // namespace predex::websocket::kalshi