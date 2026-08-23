#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <deque>

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
    kPENDING = 1,
    kMESSAGE = 2,
    kCLOSED = 3,
    kERROR = 4,
};

enum class ReadState : std::uint8_t{
    kIDLE = 0,
    kPENDING = 1,
    kMESSAGE_READY = 2
};

enum class TransportState : std::uint8_t{
    kDISCONNECTED = 0,
    kRUNNING = 1,
    kSTOPPED = 2,
    kFAULTED = 3
};

enum class SendStatus : std::uint8_t{
    kACCEPTED,
    kCLOSED,
    kQUEUE_FULL,
    kERROR
};

struct ReadResult {
    ReadStatus status{ReadStatus::kERROR};
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

    WebSocketSession(WebSocketSession&&) = delete;
    WebSocketSession& operator=(WebSocketSession&&) = delete;

    WebSocketSession(const WebSocketSession&) = delete;
    WebSocketSession& operator=(const WebSocketSession&) = delete;

    [[nodiscard]] bool connect();

    [[nodiscard]] SendStatus send_text(std::string message);

    [[nodiscard]] ReadResult recv_text();

    void consume_message();

    std::size_t poll();
    
    void close();

    [[nodiscard]] const WsConnectRequest& connect_request() const noexcept;
    [[nodiscard]] std::string_view last_error() const noexcept;

  private:
    using WsStream =
        boost::beast::websocket::stream<
            boost::beast::ssl_stream<boost::beast::tcp_stream>>;

    void reset_stream();
    void arm_read();
    void start_active_write();
    void transition_fault(std::string_view operation, const boost::system::error_code& error_code);

    void maybe_finish_stop();

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

    TransportState state_{TransportState::kDISCONNECTED};
    ReadState read_state_{ReadState::kIDLE};

    boost::beast::flat_buffer read_buffer_;

    std::optional<std::string> active_write_;
    std::deque<std::string> pending_writes_;

    std::size_t queued_write_bytes_{0};

};

}  // namespace predex::exchange::kalshi
