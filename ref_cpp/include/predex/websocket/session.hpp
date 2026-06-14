#pragma once

#include <chrono>
#include <string>
#include <string_view>
#include <vector>

#include "predex/websocket/client.hpp"
#include "predex/websocket/ws_adapter.hpp"

namespace predex::websocket {

class WsSession {
  public:
    WsSession(IWsTransport& transport, const adapter::IExchangeWsAdapter& adapter);

    [[nodiscard]] bool connect();
    [[nodiscard]] bool subscribe_universe(std::string_view channel,
                                 const std::vector<std::string>& market_tickers = {});
    //methods for single market recovery use cases
    [[nodiscard]] bool unsubscribe(std::string_view channel, const std::string& market_ticker);
    [[nodiscard]] bool subscribe(std::string_view channel, const std::string& market_ticker);

    [[nodiscard]] RecvResult recv_text(std::chrono::milliseconds timeout);
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
