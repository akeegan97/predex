#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "trading/adapters/ws/session.hpp"

namespace trading::adapters::ws {

class IWsMessageSink {  
  public:
    virtual ~IWsMessageSink() = default;
    virtual bool push_message(std::string message) = 0;
};

struct WsFeedRunnerConfig {
    static constexpr auto kDefaultReconnectBackoff = std::chrono::milliseconds{1000};

    std::vector<std::string> channels;
    std::vector<std::string> market_tickers;
    std::chrono::milliseconds reconnect_backoff{kDefaultReconnectBackoff};
};

class WsFeedRunner final {
  public:
    WsFeedRunner(WsSession& session, IWsMessageSink& sink, WsFeedRunnerConfig config = {});
    ~WsFeedRunner();

    WsFeedRunner(const WsFeedRunner&) = delete;
    WsFeedRunner& operator=(const WsFeedRunner&) = delete;
    WsFeedRunner(WsFeedRunner&&) = delete;
    WsFeedRunner& operator=(WsFeedRunner&&) = delete;

    [[nodiscard]] bool start();
    void stop();

    [[nodiscard]] bool running() const;
    [[nodiscard]] uint64_t received_count() const;
    [[nodiscard]] uint64_t dropped_count() const;
    [[nodiscard]] std::string last_error() const;

  private:
    void run(const std::stop_token& stop_token);
    [[nodiscard]] bool connect_and_subscribe();
    void set_error(std::string_view error);

    WsSession& session_;
    IWsMessageSink& sink_;
    WsFeedRunnerConfig config_;

    std::jthread worker_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> received_count_{0};
    std::atomic<uint64_t> dropped_count_{0};

    mutable std::mutex error_mutex_;
    std::string last_error_;
};

} // namespace trading::adapters::ws
