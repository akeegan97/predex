#pragma once

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
    std::string side;
    std::string fill_count_fp;
    std::string remaining_count_fp;
    std::string initial_count_fp;
};

struct OpenOrdersResult {
    bool ok{false};
    std::vector<OpenOrderSnapshot> orders;
    std::optional<std::string> next_cursor;
    std::string error;
};

class RestClient {
  public:
    explicit RestClient(AuthSigner signer,
                        std::string endpoint = "https://api.elections.kalshi.com");

    [[nodiscard]] RestCallResult
    submit_order(const predex::core::oms::kalshi::SubmitOrderCmd& command,
                 const std::string& market_ticker) const;

    [[nodiscard]] RestCallResult
    cancel_order(const predex::core::oms::kalshi::CancelOrderCmd& command) const;

    [[nodiscard]] RestCallResult
    modify_order(const predex::core::oms::kalshi::ModifyOrderCmd& command) const;

    [[nodiscard]] OpenOrdersResult
    fetch_open_orders(std::size_t limit = kOpenOrderFetchLimit,
                      std::optional<std::string> cursor = std::nullopt) const;

  private:
    AuthSigner signer_;
    std::string endpoint_;
    std::string endpoint_host_;
    std::string endpoint_port_;
    std::string endpoint_base_path_;
    bool endpoint_valid_{false};
    std::string endpoint_parse_error_;

    [[nodiscard]] RestCallResult call_json_api(const std::string& method,
                                               const std::string& target,
                                               const std::string& body) const;
};

} // namespace predex::websocket::kalshi
