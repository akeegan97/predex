#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "predex/oms/oms_types.hpp"
#include "predex/oms/transport/persistent_http_session.hpp"

namespace predex::core::oms::kalshi::transport {

inline constexpr std::size_t kDefaultOpenOrderFetchLimit = 200;

struct OpenOrderSnapshot {
    OmsOrderRef order{};
    std::string ticker;
    std::string status;
    // Kalshi keeps binary-contract outcome (`side`) separate from trade
    // direction (`action`), so we keep both in the snapshot parser too.
    std::string side;
    std::string action;
    std::string fill_count_fp;
    std::string remaining_count_fp;
    std::string initial_count_fp;
    std::string yes_price_dollars;
    std::string no_price_dollars;
};

struct OpenOrdersPage {
    bool ok{false};
    std::vector<OpenOrderSnapshot> orders;
    std::optional<std::string> next_cursor;
    std::string error_message;
};

struct RestTraceInfo {
    std::string request_target;
    std::string request_body;
    int http_status_code{0};
    std::uint32_t retry_count{0};
    internal::TimestampNs request_sent_ts_ns{0};
    internal::TimestampNs response_recv_ts_ns{0};
    std::string response_body;
    std::string error_message;
};

struct CommandResult {
    bool ok{false};
    std::optional<KalshiToOmsEvent> event;
    std::vector<KalshiToOmsEvent> events;
    std::string error_message;
    RestTraceInfo trace{};
};

struct PreparedCommandRequest {
    bool ok{false};
    HttpRequest request{};
    RestTraceInfo trace{};
    std::string error_message;
};

// Kalshi-specific request/response translation layer. This owns endpoint paths,
// JSON payload shaping, and response parsing, but delegates raw HTTPS I/O to the
// persistent session below it.
class KalshiRestAdapter {
  public:
    explicit KalshiRestAdapter(PersistentHttpSession session);

    [[nodiscard]] CommandResult submit_order(const SubmitOrderCmd& command);
    [[nodiscard]] CommandResult cancel_order(const CancelOrderCmd& command);
    [[nodiscard]] CommandResult modify_order(const ModifyOrderCmd& command);

    [[nodiscard]] OpenOrdersPage fetch_open_orders(
        std::size_t limit = kDefaultOpenOrderFetchLimit,
        std::optional<std::string> cursor = std::nullopt);

    [[nodiscard]] PreparedCommandRequest prepare_submit_order(const SubmitOrderCmd& command) const;
    [[nodiscard]] PreparedCommandRequest prepare_batched_submit_orders(
        const std::vector<SubmitOrderCmd>& commands) const;
    [[nodiscard]] PreparedCommandRequest prepare_cancel_order(const CancelOrderCmd& command) const;
    [[nodiscard]] PreparedCommandRequest prepare_modify_order(const ModifyOrderCmd& command) const;
    [[nodiscard]] CommandResult complete_submit_order(const SubmitOrderCmd& command,
                                                      const HttpResponse& response,
                                                      RestTraceInfo trace) const;
    [[nodiscard]] CommandResult complete_batched_submit_orders(
        const std::vector<SubmitOrderCmd>& commands,
        const HttpResponse& response,
        RestTraceInfo trace) const;
    [[nodiscard]] CommandResult complete_cancel_order(const CancelOrderCmd& command,
                                                      const HttpResponse& response,
                                                      RestTraceInfo trace) const;
    [[nodiscard]] CommandResult complete_modify_order(const ModifyOrderCmd& command,
                                                      const HttpResponse& response,
                                                      RestTraceInfo trace) const;

    [[nodiscard]] bool start_prepared_request(const PreparedCommandRequest& request);
    [[nodiscard]] AsyncHttpPollResult poll_active_request();

    void check_and_keep_warm(std::uint64_t threshold_seconds);
    void close() noexcept;

  private:
    PersistentHttpSession session_;

    [[nodiscard]] static std::string build_submit_target_();
    [[nodiscard]] static std::string build_batched_submit_target_();
    [[nodiscard]] static std::string build_cancel_target_(
        const ExchangeOrderId& exchange_order_id);
    [[nodiscard]] static std::string build_modify_target_(
        const ExchangeOrderId& exchange_order_id);
    [[nodiscard]] static std::string build_open_orders_target_(
        std::size_t limit,
        const std::optional<std::string>& cursor);

    [[nodiscard]] static CommandResult parse_submit_response_(
        const HttpResponse& response,
        const SubmitOrderCmd& command,
        RestTraceInfo trace);
    [[nodiscard]] static CommandResult parse_batched_submit_response_(
        const HttpResponse& response,
        const std::vector<SubmitOrderCmd>& commands,
        RestTraceInfo trace);
    [[nodiscard]] static CommandResult parse_cancel_response_(
        const HttpResponse& response,
        const CancelOrderCmd& command,
        RestTraceInfo trace);
    [[nodiscard]] static CommandResult parse_modify_response_(
        const HttpResponse& response,
        const ModifyOrderCmd& command,
        RestTraceInfo trace);
    [[nodiscard]] static OpenOrdersPage parse_open_orders_response_(
        const HttpResponse& response);
};

} // namespace predex::core::oms::kalshi::transport
