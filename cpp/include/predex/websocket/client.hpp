#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace predex::websocket {

struct TransportConfig {
    std::string endpoint;
    std::map<std::string, std::string> headers;
};

enum class RecvStatus : std::uint8_t {
    kMessage = 1,
    kTimeout = 2,
    kClosed = 3,
    kError = 4,
};

struct RecvResult {
    RecvStatus status{RecvStatus::kError};
    std::string payload;
};

class IWsTransport {
  public:
    virtual ~IWsTransport() = default;

    virtual bool connect(const TransportConfig& config) = 0;
    virtual bool send_text(std::string_view payload) = 0;
    virtual RecvResult recv_text(std::chrono::milliseconds timeout) = 0;
    virtual void close() = 0;
    [[nodiscard]] virtual std::string_view last_error() const { return {}; }
};

class BoostBeastWsTransport final : public IWsTransport {
  public:
    BoostBeastWsTransport();
    ~BoostBeastWsTransport() override;

    bool connect(const TransportConfig& config) override;
    bool send_text(std::string_view payload) override;
    RecvResult recv_text(std::chrono::milliseconds timeout) override;
    void close() override;

    [[nodiscard]] std::string_view last_error() const override;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace predex::websocket
