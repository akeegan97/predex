#pragma once
#include <cstddef>
#include <stop_token>
#include <vector>
#include <utility>
#include <memory>
#include <string>
#include <chrono>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <array>
#include <string_view>
#include <functional>
#include "predex/exchange/kalshi/websocket_session.hpp"
#include "predex/exchange/kalshi/adapters/order_data_handler.hpp"
#include "predex/ingest/kalshi/order_data/order_parser.hpp"
#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/utils/spsc.hpp"

namespace predex::ingest::kalshi::order_data{

    inline constexpr std::chrono::milliseconds kPRIVATE_ORDER_FEED_TELEMETRY_INTERVAL{250};
    inline constexpr std::size_t kPENDING_OMS_EVENTS_CAPACITY{1024};
    enum class OrderSubscriptionPhase : std::uint8_t{
        kIDLE = 0,
        kSUBSCRIBE_PENDING = 1,
        kSUBSCRIBED = 2,
        kUNSUBSCRIBE_PENDING = 3,
        kFAULTED = 4,
    };

    enum class OrderWsCommandKind : std::uint8_t{
        kSUBSCRIBE = 1,
        kUNSUBSCRIBE = 2,
    };

    struct ActiveOrderSubscription{
        exchange::kalshi::KalshiOrderDataChannel channel{};
        OrderSubscriptionPhase phase{OrderSubscriptionPhase::kIDLE};
        std::optional<std::int64_t> sid;
        std::string last_error;
    };

    struct PendingOrderWsCommand{
        std::uint64_t ws_command_id{};
        OrderWsCommandKind kind{};
        exchange::kalshi::KalshiOrderDataChannel channel{};
        std::uint64_t universe_version{};
    };

    struct OrderSessionControlQueues{
        utils::SPSCQueue<core::control::ControlToPrivateOrderFeedCommand>& control_to_order_session_queue;
        utils::SPSCQueue<core::control::PrivateOrderFeedToControlStatus>& order_session_to_control_queue;
    };

    struct OrderSessionOmsQueues{
        utils::SPSCQueue<oms::KalshiToOmsEvent>& private_ws_to_oms_queue;
    };

    struct OrderSessionDeps{
        exchange::kalshi::KalshiOrderDataHandler order_data_handler;
        std::vector<exchange::kalshi::KalshiOrderDataChannel> desired_channels;
        // to & from control plane queues
        OrderSessionControlQueues control_queues;
        // to OMS queues
        OrderSessionOmsQueues oms_queues;
    };

    struct OrderSessionState{
        bool connected{false};
        std::string last_error;
        std::uint64_t subscribed_universe_version{0};
        std::uint64_t installed_universe_version{0};
    };

    struct TickerHash{
        using is_transparent = void;
        std::size_t operator()(const std::string_view& str) const noexcept{
            return std::hash<std::string_view>{}(str);
        }
        std::size_t operator()(const std::string& str) const noexcept{
            return (*this)(std::string_view{str});
        }
        std::size_t operator()(const char* str) const noexcept{
            return (*this)(std::string_view{str});
        }
    };

    struct TickerEqual{
        using is_transparent = void;
        bool operator()(std::string_view lhs, std::string_view rhs) const noexcept{
            return lhs == rhs;
        }
        bool operator()(const std::string& lhs, const std::string& rhs) const noexcept{
            return (*this)(std::string_view{lhs}, std::string_view{rhs});
        }
        bool operator()(const char* lhs, const char* rhs) const noexcept{
            return (*this)(std::string_view{lhs}, std::string_view{rhs});
        }
    };

    class KalshiOrderSession{
        public:
            KalshiOrderSession(OrderSessionDeps deps)
                : order_data_handler_(std::move(deps.order_data_handler)), 
                  ws_session_(order_data_handler_),
                  control_queues_(deps.control_queues),
                  oms_queues_(deps.oms_queues),
                  desired_channels_(std::move(deps.desired_channels)) {}

            void run(const std::stop_token& stop_token);

            [[nodiscard]] OrderSessionState state() const{
                return status_;
            }
        private:

            void drain_control_commands() noexcept;
            void handle_control_command(const core::control::ControlToPrivateOrderFeedCommand& cmd);

            void apply_universe_snapshot(const std::shared_ptr<const core::control::OrderRouteUniverse>& snapshot);

            [[nodiscard]] bool connect();

            void disconnect(std::string reason = {});
            void clear_transport_subscription_state();

            [[nodiscard]] bool subscribe_active_universe();
            [[nodiscard]] bool subscribe_channel(exchange::kalshi::KalshiOrderDataChannel channel);
            [[nodiscard]] bool unsubscribe_channel(exchange::kalshi::KalshiOrderDataChannel channel);

            [[nodiscard]] bool push_control_status(core::control::PrivateOrderFeedToControlStatus status) noexcept;

            void maybe_send_telemetry() noexcept;

            bool service_transport_once() noexcept;

            [[nodiscard]] std::uint64_t next_ws_command_id() noexcept {
                return next_ws_command_id_++;
            }

            bool defer_oms_event(oms::KalshiToOmsEvent event) noexcept;
            bool emit_or_defer_oms_event(oms::KalshiToOmsEvent event) noexcept;
            void flush_deferred_oms_events() noexcept;

            [[nodiscard]] bool stamp_market_route(ParsedOrderMessage& parsed) noexcept;

            void handle_ws_control_response(const ParsedOrderMessage& parsed) noexcept;

            void handle_order_event(ParsedOrderMessage& parsed) noexcept;


            exchange::kalshi::KalshiOrderDataHandler order_data_handler_;
            exchange::kalshi::WebSocketSession ws_session_;
            OrderSessionControlQueues control_queues_;
            OrderSessionOmsQueues oms_queues_;
            OrderSessionState status_;

            std::shared_ptr<const core::control::OrderRouteUniverse> desired_universe_;
            std::unordered_map<std::string, core::control::OrderMarketRoute, TickerHash, TickerEqual> route_by_ticker_;
            std::vector<exchange::kalshi::KalshiOrderDataChannel> desired_channels_;
            std::unordered_map<exchange::kalshi::KalshiOrderDataChannel, ActiveOrderSubscription> active_subscriptions_;
            std::unordered_map<std::uint64_t, PendingOrderWsCommand> pending_ws_commands_;

            OrderParser order_parser_;

            core::control::PrivateOrderFeedTelemetrySnapshot telemetry_;
            std::chrono::steady_clock::time_point next_telemetry_send_{std::chrono::steady_clock::now() + kPRIVATE_ORDER_FEED_TELEMETRY_INTERVAL};
            std::uint64_t next_ws_command_id_{1};

            std::array<oms::KalshiToOmsEvent, kPENDING_OMS_EVENTS_CAPACITY> pending_oms_events_;
            std::size_t pending_oms_count_{0};
            std::size_t pending_oms_tail_{0};
            std::size_t pending_oms_head_{0};
    };

}
