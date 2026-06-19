#pragma once 

#include "predex/exchange/kalshi/handler.hpp"
#include "predex/exchange/kalshi/adapters/auth_signer.hpp"

namespace predex::exchange::kalshi{

class KalshiMarketDataHandler final : public IWsAdapter {
  public:
    explicit KalshiMarketDataHandler(AuthSigner signer);

    [[nodiscard]] std::string name() const override;
    [[nodiscard]] WsConnectRequest build_connect_request() const override;

    void on_connected(WebSocketSession& session) override;
    void on_text(std::span<const std::byte> payload,
                 WebSocketSession& session) override;
    void on_disconnected() override;

  private:
    AuthSigner signer_;
    };

}
