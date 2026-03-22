#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace trading::oms {

struct OrderTransportConfig {
    std::string endpoint;
    std::map<std::string, std::string> headers;
};

class IOrderTransport {
  public:
    virtual ~IOrderTransport() = default;

    [[nodiscard]] virtual bool connect(const OrderTransportConfig& config) = 0;
    [[nodiscard]] virtual bool send_text(std::string_view payload) = 0;
    [[nodiscard]] virtual std::optional<std::string> recv_text() = 0;
    virtual void close() = 0;
    [[nodiscard]] virtual std::string_view last_error() const { return {}; }
};

} // namespace trading::oms
