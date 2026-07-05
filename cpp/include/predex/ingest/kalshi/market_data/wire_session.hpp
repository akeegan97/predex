#pragma once 

#include <string>
#include <vector>
#include <utility>
#include <stop_token>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <span>
#include <optional>
#include <cstdint>
#include <chrono>
#include <simdjson.h>

#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/exchange/kalshi/websocket_session.hpp"
#include "predex/exchange/kalshi/adapters/market_data_handler.hpp"
#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"

namespace predex::ingest::kalshi::market_data{

    inline constexpr std::size_t kMAX_RECYCLE_BATCH_SIZE = 64;
    inline constexpr std::size_t kMAX_UNKNOWN_MARKET_TICKER_SAMPLES = 8;
    inline constexpr std::chrono::milliseconds kIO_TELEMETRY_INTERVAL{250};

    using RouterQueue = predex::utils::SPSCQueue<FrameHandle>;

    struct WireSessionState{
        bool connected{false};
        std::string last_error;
    };

    struct ControlQueues{
        // to & from control plane queues
        predex::utils::SPSCQueue<predex::core::control::ControlToIoCommand>& control_to_io_queue;
        predex::utils::SPSCQueue<predex::core::control::IoToControlStatus>& io_to_control_status_queue;
    };

    using LoggerQueue = predex::utils::SPSCQueue<FrameHandle>;
    using RecycleQueues = std::vector<predex::utils::SPSCQueue<FrameHandle>*>;

    struct KalshiWireSessionDeps{
        FramePool& frame_pool;
        ControlQueues control_queues;
        RecycleQueues recycle_queues;
        RouterQueue& router_queue;
        LoggerQueue& logger_queue;
        exchange::kalshi::KalshiMarketDataHandler market_data_handler;
        std::vector<exchange::kalshi::KalshiMarketDataChannel> desired_channels;
    };

    enum class SubscriptionPhase : std::uint8_t {
        kIDLE = 0,
        kSUBSCRIBE_PENDING = 1,
        kSUBSCRIBED = 2,
        kUPDATE_PENDING = 3,
        kUNSUBSCRIBE_PENDING = 4,
        kFAULTED = 5,
    };

    enum class WsCommandKind : std::uint8_t {
        kSUBSCRIBE = 1,
        kADD_MARKETS = 2,
        kDELETE_MARKETS = 3,
        kUNSUBSCRIBE = 4,
    };

    enum class IncomingMessageKind : std::uint8_t {
        kCONTROL_RESPONSE = 1,
        kMARKET_DATA = 2,
        kIGNORE = 3,
        kMALFORMED = 4,
    };

    struct ActiveSubscription{
        exchange::kalshi::KalshiMarketDataChannel channel{};
        SubscriptionPhase phase{SubscriptionPhase::kIDLE};
        std::optional<std::int64_t> sid;
        std::unordered_set<core::control::MarketId> market_ids;
        std::string last_error;
    };

    struct PendingWsCommand{
        std::uint64_t ws_command_id{};
        WsCommandKind kind{};
        exchange::kalshi::KalshiMarketDataChannel channel{};
        std::vector<core::control::MarketId> market_ids;
    };

    struct MarketDataEnvelope {
        FrameKind kind{FrameKind::kUNKNOWN};
        std::uint32_t sid{};
        std::uint64_t sequence{};
        std::string_view market_ticker;
    };

    struct StringHash {
        using is_transparent = void;

        std::size_t operator()(std::string_view value) const noexcept {
            return std::hash<std::string_view>{}(value);
        }

        std::size_t operator()(const std::string& value) const noexcept {
            return std::hash<std::string_view>{}(std::string_view{value});
        }

        std::size_t operator()(const char* value) const noexcept {
            return std::hash<std::string_view>{}(std::string_view{value});
        }
    };

    enum class EnvelopeParseCode : std::uint8_t {
        kOK = 0,
        kINVALID_JSON,
        kMISSING_SID,
        kSID_OUT_OF_RANGE,
        kMISSING_TYPE,
        kUNSUPPORTED_TYPE,
        kMISSING_SEQUENCE,
        kMISSING_MSG,
        kMISSING_MARKET_TICKER,
    };
    enum class StampHandleCode : std::uint8_t {
        kOK = 0,
        kUNKNOWN_KIND,
        kEMPTY_MARKET_TICKER,
        kINACTIVE_SID,
        kUNKNOWN_MARKET_TICKER,
    };

    using RouteByTickerMap = std::unordered_map<std::string, core::control::UniverseMarketRoute, StringHash, std::equal_to<>>;

    class KalshiWireSession {
        public:
            KalshiWireSession(KalshiWireSessionDeps deps) : 
                frame_pool_(deps.frame_pool), 
                router_queue_(deps.router_queue), 
                control_queues_(deps.control_queues), 
                recycle_queues_(std::move(deps.recycle_queues)),
                logger_queue_(deps.logger_queue),
                market_data_handler_(std::move(deps.market_data_handler)),
                ws_session_(market_data_handler_),
                desired_channels_(std::move(deps.desired_channels)){}

            void run(const std::stop_token& stop_token);

            [[nodiscard]] WireSessionState state() const {
                return status_;
            }
            
            [[nodiscard]] bool pop_control_command(core::control::ControlToIoCommand& cmd_out) noexcept;        


        private:
            FramePool& frame_pool_;
            RouterQueue& router_queue_;
            ControlQueues control_queues_;
            RecycleQueues recycle_queues_;
            LoggerQueue& logger_queue_;

            exchange::kalshi::KalshiMarketDataHandler market_data_handler_;
            exchange::kalshi::WebSocketSession ws_session_;

            WireSessionState status_;

            std::shared_ptr<const core::control::UniverseSnapshot> desired_universe_{nullptr};
            RouteByTickerMap market_route_by_ticker_;
            std::unordered_map<core::control::MarketId, core::control::UniverseMarketRoute> market_route_by_id_;

            std::vector<exchange::kalshi::KalshiMarketDataChannel> desired_channels_;
            std::unordered_map<exchange::kalshi::KalshiMarketDataChannel, ActiveSubscription> active_subscriptions_;
            std::unordered_map<std::uint64_t, PendingWsCommand> pending_ws_commands_;
            simdjson::ondemand::parser message_classifier_parser_;
            simdjson::ondemand::parser market_data_envelope_parser_;
            simdjson::ondemand::parser control_response_parser_;

            std::size_t max_recycle_batch_size_{kMAX_RECYCLE_BATCH_SIZE};
            std::size_t next_recycle_queue_idx_{0};
            core::control::IoTelemetrySnapshot telemetry_;
            std::chrono::steady_clock::time_point next_telemetry_send_{std::chrono::steady_clock::now() + kIO_TELEMETRY_INTERVAL};

            std::uint64_t next_ws_command_id_{1};

            std::uint64_t next_ws_command_id() noexcept {
                return next_ws_command_id_++;
            }

            void drain_control_commands();
            void handle_control_command(const core::control::ControlToIoCommand& cmd);

            void apply_universe_snapshot(const std::shared_ptr<const core::control::UniverseSnapshot>& snapshot);
            [[nodiscard]] bool connect();
            void disconnect(std::string reason = {});
            void clear_transport_subscription_state();

            [[nodiscard]] bool subscribe_active_universe();
            [[nodiscard]] bool subscribe_channel(exchange::kalshi::KalshiMarketDataChannel channel,
                                                 std::span<const std::string> tickers);
            [[nodiscard]] bool add_markets(exchange::kalshi::KalshiMarketDataChannel channel,
                                           std::span<const std::string> tickers);
            [[nodiscard]] bool delete_markets(exchange::kalshi::KalshiMarketDataChannel channel,
                                              std::span<const std::string> tickers);
            [[nodiscard]] bool unsubscribe_channel(exchange::kalshi::KalshiMarketDataChannel channel);

            [[nodiscard]] bool push_control_status(core::control::IoToControlStatus status) noexcept;
            void maybe_send_telemetry() noexcept;
            void recycle_handle(FrameHandle handle) noexcept;
            void record_unknown_market_ticker(const core::control::UnknownMarketTickerStats& unknown_market_ticker);
            [[nodiscard]] std::uint8_t channel_for_sid(std::uint32_t sid) const noexcept;

            void drain_recycle_queues();
            void pump_socket_once();
            
            [[nodiscard]] IncomingMessageKind classify_incoming_message(std::span<const std::byte> payload);
            [[nodiscard]] EnvelopeParseCode parse_market_data_envelope(const KalshiFrame& frame,
                                                          MarketDataEnvelope& envelope_out);
            [[nodiscard]] StampHandleCode stamp_handle_from_envelope(FrameHandle& handle,
                                                          const MarketDataEnvelope& envelope);
            [[nodiscard]] bool is_active_sid(std::uint32_t sid) const noexcept;
            void publish_market_data_frame(std::span<const std::byte> payload);
            void handle_ws_control_response(std::span<const std::byte> payload);

            void report_fault(std::string error_message);


    };

}
