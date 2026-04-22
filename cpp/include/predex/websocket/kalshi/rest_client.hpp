#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "predex/oms/oms_types.hpp"
#include "predex/websocket/kalshi/auth_signer.hpp"

namespace predex::websocket::kalshi {
constexpr const std::size_t kOpenOrderFetchLimit = 200;
struct RestCallResult {
    bool ok{false};
    std::optional<std::string> exchange_order_id;
    std::string error;
};

struct OpenOrderSnapshot {
    std::string order_id;
    std::string client_order_id;
    std::string ticker;
    std::string status;
    // Kalshi `side` is the binary-contract outcome: "yes" or "no". Orthogonal to
    // `action` ("buy"/"sell"), which carries the buy/sell direction. Keeping both
    // fields prevents the prior mistranslation where sell-YES reconciled as sell-NO.
    std::string side;
    std::string action;
    std::string fill_count_fp;
    std::string remaining_count_fp;
    std::string initial_count_fp;
    std::string yes_price_dollars;
    std::string no_price_dollars;
};

struct OpenOrdersResult {
    bool ok{false};
    std::vector<OpenOrderSnapshot> orders;
    std::optional<std::string> next_cursor;
    std::string error;
};

// RestClient owns a persistent HTTPS keep-alive session to Kalshi. Holding the TLS
// connection open between calls replaces a ~100ms first-call handshake cliff with
// ~20-30ms steady-state latency. NOT thread-safe: single-owner. Today the owner is
// the OMS thread; PR B will hand ownership to a dedicated RestThread.
class RestClient {
  public:
    explicit RestClient(AuthSigner signer,
                        std::string endpoint = "https://api.elections.kalshi.com");
    ~RestClient();

    RestClient(const RestClient&) = delete;
    RestClient& operator=(const RestClient&) = delete;
    RestClient(RestClient&&) = delete;
    RestClient& operator=(RestClient&&) = delete;

    [[nodiscard]] RestCallResult
    submit_order(const predex::core::oms::kalshi::SubmitOrderCmd& command,
                 const std::string& market_ticker);

    [[nodiscard]] RestCallResult
    cancel_order(const predex::core::oms::kalshi::CancelOrderCmd& command);

    [[nodiscard]] RestCallResult
    modify_order(const predex::core::oms::kalshi::ModifyOrderCmd& command);

    [[nodiscard]] OpenOrdersResult
    fetch_open_orders(std::size_t limit = kOpenOrderFetchLimit,
                      std::optional<std::string> cursor = std::nullopt);

    // If the persistent connection has been idle longer than threshold_seconds,
    // fires an unauthenticated GET /exchange/status to keep the TLS session warm
    // (HTTP/1.1 keep-alive idle timeouts on AWS infra are typically ~60s, so
    // 30-45s is the safe cadence). Cheap: 3-field response, no auth, does not
    // count against write-rate-limit. No-op if not connected or within threshold.
    // Intended to be called from whatever thread owns the RestClient's idle loop.
    void check_and_keep_warm(std::uint64_t threshold_seconds);

  private:
    static constexpr std::chrono::seconds kConnectTimeout{3};
    static constexpr std::chrono::seconds kIoTimeout{5};

    AuthSigner signer_;
    std::string endpoint_;
    std::string endpoint_host_;
    std::string endpoint_port_;
    std::string endpoint_base_path_;
    bool endpoint_valid_{false};
    std::string endpoint_parse_error_;

    // Persistent connection state — Boost types kept out of this header via PIMPL.
    struct ConnectionState;
    std::unique_ptr<ConnectionState> connection_;
    std::chrono::steady_clock::time_point last_call_ts_{};

    [[nodiscard]] bool ensure_connected_();
    void close_connection_() noexcept;

    [[nodiscard]] RestCallResult call_json_api(const std::string& method,
                                               const std::string& target,
                                               const std::string& body,
                                               bool authenticate = true);
};

} // namespace predex::websocket::kalshi
