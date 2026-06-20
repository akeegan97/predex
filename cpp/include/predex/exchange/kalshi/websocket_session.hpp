#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/system/error_code.hpp>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "predex/exchange/kalshi/handler.hpp"

namespace predex::exchange::kalshi {

enum class ReadStatus : std::uint8_t {
    kMessage = 1,
    kTimeout = 2,
    kClosed = 3,
    kError = 4,
};

struct ReadResult {
    ReadStatus status{ReadStatus::kError};
    std::span<const std::byte> payload;
};

struct EndpointParts {
    std::string scheme;
    std::string host;
    std::string port;
    std::string target;
};

class WebSocketSession {
  public:
    explicit WebSocketSession(const IWsAdapter& adapter);
    ~WebSocketSession();

    WebSocketSession(WebSocketSession&&) noexcept;
    WebSocketSession& operator=(WebSocketSession&&) noexcept;

    WebSocketSession(const WebSocketSession&) = delete;
    WebSocketSession& operator=(const WebSocketSession&) = delete;

    [[nodiscard]] bool connect();
    [[nodiscard]] bool send_text(std::string_view message);
    [[nodiscard]] ReadResult recv_text(std::chrono::milliseconds timeout);
    
    void close();

    [[nodiscard]] const WsConnectRequest& connect_request() const noexcept;
    [[nodiscard]] std::string_view last_error() const noexcept;

  private:
    using WsStream =
        boost::beast::websocket::stream<
            boost::beast::ssl_stream<boost::beast::tcp_stream>>;

    void reset_stream() {
        ws_stream_ = std::make_unique<WsStream>(io_context_, ssl_context_);
        read_buffer_.consume(read_buffer_.size());
        connected_ = false;
        last_ping_recv_ns_ = 0;
    }

    const IWsAdapter& adapter_;
    WsConnectRequest connect_request_{};
    std::string last_error_;
    bool connected_{false};
    std::uint64_t last_ping_recv_ns_{0};

    EndpointParts endpoint_{}; 

    boost::asio::io_context io_context_;
    boost::asio::ssl::context ssl_context_{boost::asio::ssl::context::tls_client};
    boost::asio::ip::tcp::resolver resolver_{io_context_};
    std::unique_ptr<WsStream> ws_stream_;
    boost::beast::flat_buffer read_buffer_;



};

}  // namespace predex::exchange::kalshi
