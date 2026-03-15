#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "trading/adapters/exchanges/kalshi/auth_signer.hpp"
#include "trading/adapters/exchanges/ws_adapter.hpp"

namespace trading::adapters::exchanges::kalshi {

class WsAdapter final : public exchanges::IExchangeWsAdapter {
  public:
    explicit WsAdapter(AuthSigner signer,
                       std::string endpoint = "wss://api.elections.kalshi.com/trade-api/ws/v2");

    [[nodiscard]] std::string name() const override;
    [[nodiscard]] exchanges::ConnectRequest build_connect_request() const override;
    [[nodiscard]] std::string
    build_subscribe_message(std::string_view channel,
                            const std::vector<std::string>& market_tickers) const override;

  private:
    AuthSigner signer_;
    std::string endpoint_;
};

} // namespace trading::adapters::exchanges::kalshi
