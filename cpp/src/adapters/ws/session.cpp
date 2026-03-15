#include "trading/adapters/ws/session.hpp"

#include <exception>

namespace trading::adapters::ws {

WsSession::WsSession(IWsTransport& transport, const exchanges::IExchangeWsAdapter& adapter)
    : transport_(transport), adapter_(adapter) {}

bool WsSession::connect() {
    try {
        connect_request_ = adapter_.build_connect_request();
    } catch (const std::exception& exception) {
        last_error_ = exception.what();
        return false;
    } catch (...) {
        last_error_ = "Unknown exception while building websocket connect request";
        return false;
    }

    const TransportConfig config{
        .endpoint = connect_request_.endpoint,
        .headers = connect_request_.headers,
    };
    if (!transport_.connect(config)) {
        if (const auto transport_error = transport_.last_error(); !transport_error.empty()) {
            last_error_.assign(transport_error);
        } else {
            last_error_ = "Websocket transport connect failed";
        }
        return false;
    }

    last_error_.clear();
    return true;
}

bool WsSession::subscribe(std::string_view channel,
                          const std::vector<std::string>& market_tickers) {
    try {
        last_subscribe_payload_ = adapter_.build_subscribe_message(channel, market_tickers);
    } catch (const std::exception& exception) {
        last_error_ = exception.what();
        return false;
    } catch (...) {
        last_error_ = "Unknown exception while building websocket subscribe payload";
        return false;
    }

    if (!transport_.send_text(last_subscribe_payload_)) {
        if (const auto transport_error = transport_.last_error(); !transport_error.empty()) {
            last_error_.assign(transport_error);
        } else {
            last_error_ = "Websocket transport send failed";
        }
        return false;
    }

    last_error_.clear();
    return true;
}

std::optional<std::string> WsSession::recv_text() {
    auto message = transport_.recv_text();
    if (!message) {
        if (const auto transport_error = transport_.last_error(); !transport_error.empty()) {
            last_error_.assign(transport_error);
        } else {
            last_error_ = "Websocket transport receive failed";
        }
        return std::nullopt;
    }

    last_error_.clear();
    return message;
}

void WsSession::close() { transport_.close(); }

const exchanges::ConnectRequest& WsSession::connect_request() const { return connect_request_; }

const std::string& WsSession::last_subscribe_payload() const { return last_subscribe_payload_; }

const std::string& WsSession::last_error() const { return last_error_; }

} // namespace trading::adapters::ws
