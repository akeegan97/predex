#pragma once

#include <chrono>
#include <memory>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <limits>

#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"
#include "predex/exchange/kalshi/http2_session.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc.hpp"

namespace predex::exchange::kalshi {

    namespace control = ::predex::core::control;
    namespace oms = ::predex::oms;

    inline constexpr std::chrono::milliseconds kORDER_REST_TELEMETRY_INTERVAL{250};
    inline constexpr std::size_t kMAX_PENDING_OMS_EVENTS{64};
    inline constexpr std::size_t kMAX_HTTP_POLL_BATCH_SIZE{64};

    struct OrderRestControlQueues {
        utils::SPSCQueue<control::ControlToOrderRestCommand>& control_to_order_rest_queue;
        utils::SPSCQueue<control::OrderRestToControlStatus>& order_rest_to_control_queue;
    };

    struct OrderRestOmsQueues {
        utils::SPSCQueue<oms::OmsToKalshiCommand>& oms_to_order_rest_queue;
        utils::SPSCQueue<oms::KalshiToOmsEvent>& order_rest_to_oms_queue;
    };

    struct OrderRestSessionDeps {
        Http2Session http_session;
        KalshiOrderRestAdapter order_rest_adapter;
        OrderRestControlQueues control_queues;
        OrderRestOmsQueues oms_queues;
    };

    struct OrderRestSessionState { 
        bool enabled{false};
        bool faulted{false};
        std::string last_error;
    };

    class OrderRestSession {
        public:
            explicit OrderRestSession(OrderRestSessionDeps deps)
                : http_session_(std::move(deps.http_session)),
                  order_rest_adapter_(std::move(deps.order_rest_adapter)),
                  control_queues_(deps.control_queues),
                  oms_queues_(deps.oms_queues) {}

            void run(const std::stop_token& stop_token);

            [[nodiscard]] OrderRestSessionState state() const {
                return status_;
            }

        private:
            struct InflightRequest {
                std::variant<
                    PreparedOrderRestRequest,
                    PreparedPortfolioRestRequest
                > prepared;
            };

            struct ActivePortfolioReconciliation {
                oms::RequestPortfolioReconciliation command;
                bool balance_complete{false};
                bool positions_complete{false};
                std::int64_t available_balance_ticks{};
                std::int64_t portfolio_value_ticks{};
                std::vector<oms::VenueMarketPositionSnapshot> positions;
            };

            void drain_control_commands() noexcept;
            void drain_oms_commands() noexcept;
            void handle_control_command(const core::control::ControlToOrderRestCommand& command);
            void handle_oms_command(const oms::OmsToKalshiCommand& command);
            void handle_portfolio_reconciliation(
                const oms::RequestPortfolioReconciliation& command) noexcept;

            void apply_order_route_universe(const std::shared_ptr<const core::control::OrderRouteUniverse>& snapshot);
            void enable() noexcept;
            void disable(std::string reason = {});

            void receive_http(std::size_t max_batch_size) noexcept;
            [[nodiscard]] bool start_portfolio_request(
                PreparedPortfolioRestRequest prepared) noexcept;
            void complete_portfolio_request(
                const PreparedPortfolioRestRequest& prepared,
                const HttpResponse& response) noexcept;
            void maybe_finish_portfolio_reconciliation() noexcept;
            void fail_portfolio_reconciliation(std::string reason) noexcept;
            [[nodiscard]] HttpRequestId next_portfolio_http_request_id() noexcept;
            void maybe_send_telemetry() noexcept;

            [[nodiscard]] bool try_push_control_status(core::control::OrderRestToControlStatus status) noexcept;
            void try_send_pending_control_notifications() noexcept;
            [[nodiscard]] bool send_or_defer_oms_event(oms::KalshiToOmsEvent event) noexcept;
            [[nodiscard]] bool emit_local_reject(const PreparedOrderRestRequest& prepared, std::string reason) noexcept;

            [[nodiscard]] bool defer_oms_event(oms::KalshiToOmsEvent event) noexcept;

            void drain_pending_oms_events() noexcept; 
            void fault(std::string reason) noexcept;

            void handle_close_egress(const oms::CloseOrderRestEgress& command) noexcept;

            void finish_egress_drain() noexcept;


            Http2Session http_session_;
            KalshiOrderRestAdapter order_rest_adapter_;
            OrderRestControlQueues control_queues_;
            OrderRestOmsQueues oms_queues_;

            OrderRestSessionState status_;
            std::unordered_map<HttpRequestId, InflightRequest> inflight_requests_;
            std::optional<ActivePortfolioReconciliation>
                active_portfolio_reconciliation_;
            core::control::OrderRestTelemetrySnapshot telemetry_;
            std::chrono::steady_clock::time_point next_telemetry_send_{
                std::chrono::steady_clock::now() + kORDER_REST_TELEMETRY_INTERVAL
            };
            inline constexpr static std::size_t kMAX_INFLIGHT_REQUESTS{10};//unsure yet
            std::array<oms::KalshiToOmsEvent, kMAX_PENDING_OMS_EVENTS> pending_oms_events_;
            std::size_t pending_oms_head_{0};
            std::size_t pending_oms_tail_{0};
            std::size_t pending_oms_count_{0};

            std::uint64_t installed_universe_version_{0};
            bool pending_ready_notification_{false};
            bool pending_fault_notification_{false};

            bool egress_closed_{false};
            std::uint64_t shutdown_epoch_{0};
            bool drain_marker_sent_{false};
            HttpRequestId next_portfolio_http_request_id_{
                std::numeric_limits<HttpRequestId>::max()
            };
    };

} // namespace predex::exchange::kalshi
