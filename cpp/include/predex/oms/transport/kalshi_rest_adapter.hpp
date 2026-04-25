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

struct CommandResult {
    bool ok{false};
    std::optional<KalshiToOmsEvent> event;
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

    void check_and_keep_warm(std::uint64_t threshold_seconds);

  private:
    PersistentHttpSession session_;

    [[nodiscard]] static std::string build_submit_target_();
    [[nodiscard]] static std::string build_cancel_target_(
        const ExchangeOrderId& exchange_order_id);
    [[nodiscard]] static std::string build_modify_target_(
        const ExchangeOrderId& exchange_order_id);
    [[nodiscard]] static std::string build_open_orders_target_(
        std::size_t limit,
        const std::optional<std::string>& cursor);

    [[nodiscard]] static CommandResult parse_submit_response_(
        const HttpResponse& response,
        const SubmitOrderCmd& command);
    [[nodiscard]] static CommandResult parse_cancel_response_(
        const HttpResponse& response,
        const CancelOrderCmd& command);
    [[nodiscard]] static CommandResult parse_modify_response_(
        const HttpResponse& response,
        const ModifyOrderCmd& command);
    [[nodiscard]] static OpenOrdersPage parse_open_orders_response_(
        const HttpResponse& response);
};

} // namespace predex::core::oms::kalshi::transport
