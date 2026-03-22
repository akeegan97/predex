#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace trading::adapters::ws {

struct TransportConfig {
    std::string endpoint;
    std::map<std::string, std::string> headers;
};

class IWsTransport {
  public:
    virtual ~IWsTransport() = default;

    virtual bool connect(const TransportConfig& config) = 0;
    virtual bool send_text(std::string_view payload) = 0;
    virtual std::optional<std::string> recv_text() = 0;
    virtual void close() = 0;
    [[nodiscard]] virtual std::string_view last_error() const { return {}; }
};

class BoostBeastWsTransport final : public IWsTransport {
  public:
    BoostBeastWsTransport();
    ~BoostBeastWsTransport() override;

    bool connect(const TransportConfig& config) override;
    bool send_text(std::string_view payload) override;
    std::optional<std::string> recv_text() override;
    void close() override;

    [[nodiscard]] std::string_view last_error() const override;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace trading::adapters::ws
