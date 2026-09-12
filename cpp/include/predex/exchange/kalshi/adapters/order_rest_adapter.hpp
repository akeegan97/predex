#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/http_types.hpp"
#include "predex/oms/oms_types.hpp"

namespace predex::exchange::kalshi {

    struct PreparedOrderRestRequest {
        bool ok{false};
        HttpRequest request;
        oms::OmsToKalshiCommand source_command;
        oms::RestCommandKind command_kind{oms::RestCommandKind::kUNKNOWN};
        std::string error_message;
    };

    struct CompletedOrderRestRequest {
        bool ok{false};
        oms::RestOrderResponse response;
        std::string error_message;
    };
    struct CompletedOrderRestBatchRequest {
        // True means the HTTP response was understood as a batch response.
        // Individual legs may still contain venue rejections.
        bool ok{false};

        oms::RestOrderBatchResponse response;
        std::string error_message;
    };

    enum class PortfolioRestRequestKind : std::uint8_t {
        kBALANCE = 1,
        kPOSITIONS = 2,
    };

    struct PreparedPortfolioRestRequest {
        bool ok{false};
        HttpRequest request;
        PortfolioRestRequestKind kind{PortfolioRestRequestKind::kBALANCE};
        std::uint64_t reconciliation_id{};
        std::string error_message;
    };

    struct CompletedPortfolioBalanceRequest {
        bool ok{false};
        std::uint64_t reconciliation_id{};
        std::int64_t available_balance_ticks{};
        std::int64_t portfolio_value_ticks{};
        std::string error_message;
    };

    struct CompletedPortfolioPositionsRequest {
        bool ok{false};
        std::uint64_t reconciliation_id{};
        std::vector<oms::VenueMarketPositionSnapshot> positions;
        std::string next_cursor;
        std::string error_message;
    };

    class KalshiOrderRestAdapter {
        public:
            KalshiOrderRestAdapter() = default;
            explicit KalshiOrderRestAdapter(std::shared_ptr<const core::control::OrderRouteUniverse> order_routes);

            void apply_order_route_universe(std::shared_ptr<const core::control::OrderRouteUniverse> order_routes);

            [[nodiscard]] PreparedOrderRestRequest prepare_command(const oms::OmsToKalshiCommand& command) const;
            [[nodiscard]] PreparedOrderRestRequest prepare_submit_order_batch(const oms::SubmitOrderBatchCmd& command) const;
            [[nodiscard]] PreparedOrderRestRequest prepare_submit_order(const oms::SubmitOrderCmd& command) const;
            [[nodiscard]] PreparedOrderRestRequest prepare_cancel_order(const oms::CancelOrderCmd& command) const;
            [[nodiscard]] PreparedOrderRestRequest prepare_modify_order(const oms::ModifyOrderCmd& command) const;

            [[nodiscard]] CompletedOrderRestRequest complete_request(
                const PreparedOrderRestRequest& prepared,
                const HttpResponse& response
            ) const;

            [[nodiscard]] CompletedOrderRestBatchRequest complete_batch_request(
                const PreparedOrderRestRequest& prepared,
                const HttpResponse& response
            ) const;

            [[nodiscard]] PreparedPortfolioRestRequest prepare_portfolio_balance_request(
                const oms::RequestPortfolioReconciliation& command,
                HttpRequestId request_id) const;

            [[nodiscard]] PreparedPortfolioRestRequest prepare_portfolio_positions_request(
                const oms::RequestPortfolioReconciliation& command,
                HttpRequestId request_id,
                std::string_view cursor = {}) const;

            [[nodiscard]] CompletedPortfolioBalanceRequest complete_portfolio_balance_request(
                const PreparedPortfolioRestRequest& prepared,
                const HttpResponse& response) const;

            [[nodiscard]] CompletedPortfolioPositionsRequest complete_portfolio_positions_request(
                const PreparedPortfolioRestRequest& prepared,
                const HttpResponse& response) const;

            [[nodiscard]] std::optional<std::string_view> market_ticker_for_id(oms::intent::MarketId market_id) const noexcept;

            [[nodiscard]] static std::string build_submit_target();
            [[nodiscard]] static std::string build_submit_batch_target();
            [[nodiscard]] static std::string build_cancel_target(const oms::ExchangeOrderId& exchange_order_id);
            [[nodiscard]] static std::string build_modify_target(const oms::ExchangeOrderId& exchange_order_id);
            [[nodiscard]] static std::string build_portfolio_balance_target();
            [[nodiscard]] static std::string build_portfolio_positions_target(std::string_view cursor = {});

        private:
            std::shared_ptr<const core::control::OrderRouteUniverse> order_routes_;
            std::unordered_map<oms::intent::MarketId, std::string> ticker_by_market_id_;
            std::unordered_map<std::string, core::control::OrderMarketRoute> route_by_ticker_;

            [[nodiscard]] PreparedOrderRestRequest prepare_command_common(
                oms::OmsToKalshiCommand command,
                oms::RestCommandKind command_kind
            ) const;
    };

} // namespace predex::exchange::kalshi
