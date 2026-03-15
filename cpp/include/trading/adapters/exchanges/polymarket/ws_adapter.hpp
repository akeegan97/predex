#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "trading/adapters/exchanges/ws_adapter.hpp"

namespace trading::adapters::exchanges::polymarket {

class WsAdapter final : public exchanges::IExchangeWsAdapter {
  public:
    explicit WsAdapter(std::string endpoint = "wss://ws-subscriptions-clob.polymarket.com/ws");

    [[nodiscard]] std::string name() const override;
    [[nodiscard]] exchanges::ConnectRequest build_connect_request() const override;
    [[nodiscard]] std::string
    build_subscribe_message(std::string_view channel,
                            const std::vector<std::string>& market_tickers) const override;

  private:
    std::string endpoint_;
};

} // namespace trading::adapters::exchanges::polymarket
