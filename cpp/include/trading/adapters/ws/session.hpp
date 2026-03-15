#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "trading/adapters/exchanges/ws_adapter.hpp"
#include "trading/adapters/ws/client.hpp"

namespace trading::adapters::ws {

class WsSession {
  public:
    WsSession(IWsTransport& transport, const exchanges::IExchangeWsAdapter& adapter);

    [[nodiscard]] bool connect();
    [[nodiscard]] bool subscribe(std::string_view channel,
                                 const std::vector<std::string>& market_tickers = {});
    [[nodiscard]] std::optional<std::string> recv_text();
    void close();

    [[nodiscard]] const exchanges::ConnectRequest& connect_request() const;
    [[nodiscard]] const std::string& last_subscribe_payload() const;
    [[nodiscard]] const std::string& last_error() const;

  private:
    IWsTransport& transport_;
    const exchanges::IExchangeWsAdapter& adapter_;
    exchanges::ConnectRequest connect_request_{};
    std::string last_subscribe_payload_;
    std::string last_error_;
};

} // namespace trading::adapters::ws
