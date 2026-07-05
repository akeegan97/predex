#pragma once

#include <chrono>
#include <optional>
#include <stop_token>
#include <utility>

#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"
#include "predex/exchange/kalshi/http2_session.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc.hpp"

namespace predex::exchange::kalshi {

    inline constexpr std::chrono::milliseconds kORDER_REST_TELEMETRY_INTERVAL{250};

    struct OrderRestControlQueues {
        utils::SPSCQueue<core::control::ControlToOrderRestCommand>& control_to_order_rest_queue;
        utils::SPSCQueue<core::control::OrderRestToControlStatus>& order_rest_to_control_queue;
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
                PreparedOrderRestRequest prepared;
            };

            void drain_control_commands() noexcept;
            void drain_oms_commands() noexcept;
            void handle_control_command(const core::control::ControlToOrderRestCommand& command);
            void handle_oms_command(const oms::OmsToKalshiCommand& command);

            void apply_order_route_universe(const std::shared_ptr<const core::control::OrderRouteUniverse>& snapshot);
            void enable();
            void disable(std::string reason = {});

            void pump_http_once() noexcept;
            void maybe_send_telemetry() noexcept;

            [[nodiscard]] bool push_control_status(core::control::OrderRestToControlStatus status) noexcept;
            [[nodiscard]] bool send_oms_event(oms::KalshiToOmsEvent event) noexcept;

            Http2Session http_session_;
            KalshiOrderRestAdapter order_rest_adapter_;
            OrderRestControlQueues control_queues_;
            OrderRestOmsQueues oms_queues_;

            OrderRestSessionState status_;
            std::optional<InflightRequest> inflight_request_;
            core::control::OrderRestTelemetrySnapshot telemetry_;
            std::chrono::steady_clock::time_point next_telemetry_send_{
                std::chrono::steady_clock::now() + kORDER_REST_TELEMETRY_INTERVAL
            };
    };

} // namespace predex::exchange::kalshi
