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

    // steady_clock time_since_epoch in ns for the most recent ping frame the
    // transport has observed. Zero means no ping seen since connect. Used by
    // liveness watchdogs — Kalshi pings every ~10s, so a long-stale value
    // indicates a silently-dead connection (half-open TCP, peer crash, etc.)
    // that won't otherwise surface because we never send on the OMS channel.
    [[nodiscard]] virtual std::uint64_t last_ping_recv_ns() const { return 0; }
};

class BoostBeastWsTransport final : public IWsTransport {
  public:
    BoostBeastWsTransport();
    ~BoostBeastWsTransport() override;
    BoostBeastWsTransport(BoostBeastWsTransport&&) noexcept;
    BoostBeastWsTransport& operator=(BoostBeastWsTransport&&) noexcept;

    BoostBeastWsTransport(const BoostBeastWsTransport&) = delete;
    BoostBeastWsTransport& operator=(const BoostBeastWsTransport&) = delete;

    bool connect(const TransportConfig& config) override;
    bool send_text(std::string_view payload) override;
    RecvResult recv_text(std::chrono::milliseconds timeout) override;
    void close() override;

    [[nodiscard]] std::string_view last_error() const override;
    [[nodiscard]] std::uint64_t last_ping_recv_ns() const override;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace predex::websocket
