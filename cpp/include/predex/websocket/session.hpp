#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "predex/websocket/ws_adapter.hpp"
#include "predex/websocket/client.hpp"

namespace predex::websocket {

class WsSession {
  public:
    WsSession(IWsTransport& transport, const adapter::IExchangeWsAdapter& adapter);

    [[nodiscard]] bool connect();
    [[nodiscard]] bool subscribe(std::string_view channel,
                                 const std::vector<std::string>& market_tickers = {});
    [[nodiscard]] std::optional<std::string> recv_text();
    void close();

    [[nodiscard]] const adapter::ConnectRequest& connect_request() const;
    [[nodiscard]] const std::string& last_subscribe_payload() const;
    [[nodiscard]] const std::string& last_error() const;

  private:
    IWsTransport& transport_;
    const adapter::IExchangeWsAdapter& adapter_;
    adapter::ConnectRequest connect_request_{};
    std::string last_subscribe_payload_;
    std::string last_error_;
};

} // namespace predex::websocket
