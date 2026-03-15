#include "trading/adapters/ws/feed_runner.hpp"

#include <string>
#include <thread>
#include <utility>

#include "trading/adapters/logging/logger.hpp"

namespace trading::adapters::ws {

WsFeedRunner::WsFeedRunner(WsSession& session, IWsMessageSink& sink, WsFeedRunnerConfig config)
    : session_(session), sink_(sink), config_(std::move(config)) {}

WsFeedRunner::~WsFeedRunner() { stop(); }

bool WsFeedRunner::start() {
    if (worker_.joinable()) {
        return false;
    }

    received_count_.store(0, std::memory_order_relaxed);
    dropped_count_.store(0, std::memory_order_relaxed);
    set_error("");

    // Publish started state to readers of running().
    running_.store(true, std::memory_order_release);
    worker_ = std::jthread([this](const std::stop_token& stop_token) { run(stop_token); });
    return true;
}

void WsFeedRunner::stop() {
    if (!worker_.joinable()) {
        running_.store(false, std::memory_order_release);
        return;
    }

    worker_.request_stop();
    session_.close();
    worker_.join();
    running_.store(false, std::memory_order_release);
}

// Acquire pairs with start/stop release stores.
bool WsFeedRunner::running() const { return running_.load(std::memory_order_acquire); }

uint64_t WsFeedRunner::received_count() const {
    return received_count_.load(std::memory_order_relaxed);
}

uint64_t WsFeedRunner::dropped_count() const {
    return dropped_count_.load(std::memory_order_relaxed);
}

std::string WsFeedRunner::last_error() const {
    std::scoped_lock lock{error_mutex_};
    return last_error_;
}

void WsFeedRunner::run(const std::stop_token& stop_token) {
    while (!stop_token.stop_requested()) {
        if (!connect_and_subscribe()) {
            std::this_thread::sleep_for(config_.reconnect_backoff);
            continue;
        }

        while (!stop_token.stop_requested()) {
            auto message = session_.recv_text();
            if (!message) {
                set_error(session_.last_error());
                session_.close();
                break;
            }

            received_count_.fetch_add(1, std::memory_order_relaxed);
            if (!sink_.push_message(std::move(*message))) {
                dropped_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    session_.close();
    running_.store(false, std::memory_order_release);
}

bool WsFeedRunner::connect_and_subscribe() {
    if (!session_.connect()) {
        set_error(session_.last_error());
        return false;
    }

    for (const auto& channel : config_.channels) {
        if (!session_.subscribe(channel, config_.market_tickers)) {
            set_error(session_.last_error());
            session_.close();
            return false;
        }
        const std::string payload =
            "channel=" + channel + ", payload=" + session_.last_subscribe_payload();
        trading::adapters::logging::log_startup("trader.ws.subscribe", payload);
    }

    set_error("");
    return true;
}

void WsFeedRunner::set_error(std::string_view error) {
    std::scoped_lock lock{error_mutex_};
    last_error_.assign(error);
}

} // namespace trading::adapters::ws
