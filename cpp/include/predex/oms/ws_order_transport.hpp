#pragma once

#include "trading/adapters/ws/client.hpp"
#include "trading/oms/transport.hpp"

namespace trading::oms {

class WsOrderTransport final : public IOrderTransport {
  public:
    explicit WsOrderTransport(adapters::ws::IWsTransport& ws_transport);

    [[nodiscard]] bool connect(const OrderTransportConfig& config) override;
    [[nodiscard]] bool send_text(std::string_view payload) override;
    [[nodiscard]] std::optional<std::string> recv_text() override;
    void close() override;
    [[nodiscard]] std::string_view last_error() const override;

  private:
    adapters::ws::IWsTransport& ws_transport_;
};

} // namespace trading::oms
