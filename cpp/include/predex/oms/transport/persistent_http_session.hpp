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
    internal::TimestampNs request_sent_ts_ns{0};
    internal::TimestampNs response_recv_ts_ns{0};
    std::string body;
    std::string error_message;
    bool keep_alive{true};
};

// Owns a persistent HTTPS session to Kalshi. This layer should know about
// connection reuse, TLS, auth headers, retries, and HTTP protocol mechanics,
// but it should not know anything about OMS order commands or venue events.
class PersistentHttpSession {
  public:
    explicit PersistentHttpSession(
        predex::websocket::kalshi::AuthSigner signer,
        std::string endpoint = "https://api.elections.kalshi.com");
    ~PersistentHttpSession();

    PersistentHttpSession(const PersistentHttpSession&) = delete;
    PersistentHttpSession& operator=(const PersistentHttpSession&) = delete;
    PersistentHttpSession(PersistentHttpSession&&) noexcept;
    PersistentHttpSession& operator=(PersistentHttpSession&&) noexcept;

    [[nodiscard]] HttpResponse send_json_request(const HttpRequest& request);

    // Refreshes the TLS session opportunistically if the connection has been
    // idle for longer than threshold_seconds.
    void check_and_keep_warm(std::uint64_t threshold_seconds);

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

    [[nodiscard]] static bool parse_endpoint(std::string_view endpoint,
                                            EndpointParts& out,
                                            std::string& error);
    [[nodiscard]] HttpResponse send_json_request_once_(const HttpRequest& request);
    [[nodiscard]] std::string build_request_target_(std::string_view target) const;
    [[nodiscard]] std::string build_signing_target_(std::string_view request_target) const;
};

} // namespace predex::core::oms::kalshi::transport
