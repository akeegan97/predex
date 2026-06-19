#pragma once

#include <cstddef>
#include <map>
#include <span>
#include <string>

namespace predex::exchange::kalshi {

class WebSocketSession;

struct WsConnectRequest {
    std::string endpoint;
    std::map<std::string, std::string> headers;
};

class IWsAdapter {
  public:
    virtual ~IWsAdapter() = default;

    [[nodiscard]] virtual std::string name() const = 0;
    [[nodiscard]] virtual WsConnectRequest build_connect_request() const = 0;

    virtual void on_connected(WebSocketSession& session) = 0;
    virtual void on_text(std::span<const std::byte> payload,
                         WebSocketSession& session) = 0;
    virtual void on_disconnected() = 0;
};

}  // namespace predex::exchange::kalshi
