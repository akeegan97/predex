#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "predex/internal/market_types.hpp"
#include "predex/websocket/kalshi/auth_signer.hpp"

namespace predex::core::oms::kalshi::transport {

enum class HttpMethod : std::uint8_t {
    kGet = 1,
    kPost = 2,
    kDelete = 3,
};

struct HttpRequest {
    HttpMethod method{HttpMethod::kGet};
    std::string target;
    std::string body;
    bool authenticate{true};
    std::string content_type{"application/json"};
};

struct HttpResponse {
    bool ok{false};
    int status_code{0};
    std::uint32_t retry_count{0};
    bool reused_connection{false};
    internal::TimestampNs resolve_start_ts_ns{0};
    internal::TimestampNs resolve_end_ts_ns{0};
    internal::TimestampNs connect_start_ts_ns{0};
    internal::TimestampNs connect_end_ts_ns{0};
    internal::TimestampNs handshake_start_ts_ns{0};
    internal::TimestampNs handshake_end_ts_ns{0};
    internal::TimestampNs write_start_ts_ns{0};
    internal::TimestampNs request_sent_ts_ns{0};
    internal::TimestampNs response_recv_ts_ns{0};
    std::string body;
    std::string error_message;
    bool keep_alive{true};
};

enum class AsyncHttpRequestStatus : std::uint8_t {
    kIdle = 1,
    kInFlight = 2,
    kCompleted = 3,
};

struct AsyncHttpPollResult {
    AsyncHttpRequestStatus status{AsyncHttpRequestStatus::kIdle};
    std::optional<HttpResponse> response;
};

// Owns a persistent HTTPS session to Kalshi. This layer should know about
// connection reuse, TLS, auth headers, retries, and HTTP protocol mechanics,
// but it should not know anything about OMS order commands or venue events.
class PersistentHttpSession {
  public:
    explicit PersistentHttpSession(predex::websocket::kalshi::AuthSigner signer,
                                   std::string endpoint = "https://api.elections.kalshi.com");
    ~PersistentHttpSession();

    PersistentHttpSession(const PersistentHttpSession&) = delete;
    PersistentHttpSession& operator=(const PersistentHttpSession&) = delete;
    PersistentHttpSession(PersistentHttpSession&&) noexcept;
    PersistentHttpSession& operator=(PersistentHttpSession&&) noexcept;

    [[nodiscard]] HttpResponse send_json_request(const HttpRequest& request);
    [[nodiscard]] bool start_json_request(HttpRequest request);
    [[nodiscard]] AsyncHttpPollResult poll_json_request();
    [[nodiscard]] bool has_inflight_request() const noexcept;
    [[nodiscard]] bool warm_up();

    // Refreshes the TLS session opportunistically if the connection has been
    // idle for longer than threshold_seconds.
    void check_and_keep_warm(std::uint64_t threshold_seconds);
    void close() noexcept;

  private:
    static constexpr std::chrono::seconds kConnectTimeout{3};
    static constexpr std::chrono::seconds kIoTimeout{5};

    struct EndpointParts {
        std::string host;
        std::string port;
        std::string base_path;
    };

    struct ConnectionState;

    predex::websocket::kalshi::AuthSigner signer_;
    std::string endpoint_;
    std::string endpoint_host_;
    std::string endpoint_port_;
    std::string endpoint_base_path_;
    bool endpoint_valid_{false};
    std::string endpoint_parse_error_;
    std::unique_ptr<ConnectionState> connection_;
    std::chrono::steady_clock::time_point last_call_ts_;

    [[nodiscard]] bool ensure_connected_();
    void close_connection_() noexcept;
    void reset_async_request_() noexcept;

    [[nodiscard]] static bool parse_endpoint(std::string_view endpoint, EndpointParts& out,
                                             std::string& error);
    void begin_async_request_(HttpRequest request);
    void begin_async_write_();
    void complete_async_request_(HttpResponse response) noexcept;
    [[nodiscard]] HttpResponse
    build_disconnected_response_(std::string error_message) const noexcept;
    [[nodiscard]] HttpResponse
    build_transport_error_response_(std::string error_message,
                                    internal::TimestampNs request_sent_ts_ns) const noexcept;
    [[nodiscard]] HttpResponse send_json_request_once_(const HttpRequest& request);
    [[nodiscard]] HttpResponse run_async_request_to_completion_(HttpRequest request);
    [[nodiscard]] std::string build_request_target_(std::string_view target) const;
    [[nodiscard]] std::string build_signing_target_(std::string_view request_target) const;
};

} // namespace predex::core::oms::kalshi::transport
