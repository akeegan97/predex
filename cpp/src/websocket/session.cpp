#include "predex/websocket/session.hpp"

#include <chrono>
#include <exception>

namespace predex::websocket {
WsSession::WsSession(IWsTransport& transport, const adapter::IExchangeWsAdapter& adapter)
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

bool WsSession::subscribe(std::string_view channel, const std::string& market_ticker){
    try{
        last_subscribe_payload_ = adapter_.build_subscribe_message(channel, {market_ticker});
    }catch (const std::exception& exception){
        last_error_ = exception.what();
        return false;
    }catch (...){
        last_error_ = "Unknown exception while building websocket subscribe payload";
        return false;
    }
    if(!transport_.send_text(last_subscribe_payload_)){
        if(const auto transport_error = transport_.last_error(); !transport_error.empty()){
            last_error_.assign(transport_error);
        }else{
            last_error_ = "Websocket transport send failed";
        }
        return false;
    }
    last_error_.clear();
    return true;
}

bool WsSession::unsubscribe(std::string_view channel, const std::string& market_ticker){
    
}

bool WsSession::subscribe_universe(std::string_view channel,
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

RecvResult WsSession::recv_text(std::chrono::milliseconds timeout) {
    auto recv_result = transport_.recv_text(timeout);
    if (recv_result.status == RecvStatus::kError) {
        if (const auto transport_error = transport_.last_error(); !transport_error.empty()) {
            last_error_.assign(transport_error);
        } else {
            last_error_ = "Websocket transport receive failed";
        }
        return recv_result;
    }

    if (recv_result.status == RecvStatus::kMessage) {
        last_error_.clear();
    }
    return recv_result;
}

void WsSession::close() { transport_.close(); }

const adapter::ConnectRequest& WsSession::connect_request() const { return connect_request_; }

const std::string& WsSession::last_subscribe_payload() const { return last_subscribe_payload_; }

const std::string& WsSession::last_error() const { return last_error_; }

} // namespace predex::websocket
