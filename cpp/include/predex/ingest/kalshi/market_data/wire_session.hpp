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
#include <deque>
#include <simdjson.h>
#include <cstddef>
#include <string_view>
#include <limits>


#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/exchange/kalshi/websocket_session.hpp"
#include "predex/exchange/kalshi/adapters/market_data_handler.hpp"
#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/kalshi_ws_protocol.hpp"


namespace predex::ingest::kalshi::market_data{

    class KalshiWireSessionTestPeer;

    inline constexpr std::size_t kMAX_RECYCLE_BATCH_SIZE = 64;
    inline constexpr std::size_t kMAX_UNKNOWN_MARKET_TICKER_SAMPLES = 8;
    inline constexpr std::chrono::milliseconds kIO_TELEMETRY_INTERVAL{250};

    using RouterQueue = predex::utils::SPSCQueue<MarketDataPathMessage>;

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
        kGET_SNAPSHOT = 5,
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
        std::optional<std::uint32_t> sid;
        std::unordered_set<core::control::MarketId> market_ids;
        std::string last_error;
    };

    struct PendingRecoveryTag{
        core::control::RecoveryId recovery_id{};
        std::uint64_t universe_version{};
        std::uint32_t request_attempt{};
    };

    struct RecoveryCommandContext{
        core::control::RecoveryId recovery_id{};
        std::uint64_t universe_version{};
        core::control::MarketId market_id{};
        std::uint32_t request_attempt{};
    };

    struct ActiveSidState{
        exchange::kalshi::KalshiMarketDataChannel channel{};
        std::optional<std::uint64_t> last_sequence;
    };

    enum class SequenceObservationCode: std::uint8_t{
        kFIRST,
        kCONTIGUOUS,
        kGAP,
        kDUPLICATE,
        kSTALE,
        kINACTIVE_SID,
    };

    struct SequenceObservation{
        SequenceObservationCode code{SequenceObservationCode::kINACTIVE_SID};
        exchange::kalshi::KalshiMarketDataChannel channel{};
        std::uint64_t expected_sequence{};
        std::uint64_t observed_sequence{};
    };

    class SidSequenceObserver{
        public:
            void activate(
                std::uint32_t sid,
                exchange::kalshi::KalshiMarketDataChannel channel){
                states_.insert_or_assign(
                    sid,
                    ActiveSidState{
                        .channel = channel,
                        .last_sequence = std::nullopt,
                    });
            }

            void deactivate(std::uint32_t sid) noexcept{
                states_.erase(sid);
            }

            void clear() noexcept{
                states_.clear();
            }

            [[nodiscard]] bool active(std::uint32_t sid) const noexcept{
                return states_.find(sid) != states_.end();
            }

            [[nodiscard]] std::uint8_t channel_for_sid(
                std::uint32_t sid) const noexcept{
                const auto iterator = states_.find(sid);
                if(iterator == states_.end()){
                    return 0;
                }
                return static_cast<std::uint8_t>(iterator->second.channel);
            }

            [[nodiscard]] SequenceObservation observe(
                std::uint32_t sid,
                std::uint64_t sequence) noexcept{
                SequenceObservation observation{
                    .code = SequenceObservationCode::kINACTIVE_SID,
                    .observed_sequence = sequence,
                };
                const auto iterator = states_.find(sid);
                if(iterator == states_.end()){
                    return observation;
                }
                auto& sid_state = iterator->second;
                observation.channel = sid_state.channel;

                if(!sid_state.last_sequence.has_value()){
                    sid_state.last_sequence = sequence;
                    observation.code = SequenceObservationCode::kFIRST;
                    observation.expected_sequence = sequence;
                    return observation;
                }

                const std::uint64_t previous = *sid_state.last_sequence;
                const std::uint64_t next_expected =
                    previous == std::numeric_limits<std::uint64_t>::max()
                        ? previous
                        : previous + 1U;
                observation.expected_sequence = next_expected;

                if(sequence == previous){
                    observation.code = SequenceObservationCode::kDUPLICATE;
                    return observation;
                }
                if(previous != std::numeric_limits<std::uint64_t>::max() &&
                   sequence == next_expected){
                    sid_state.last_sequence = sequence;
                    observation.code = SequenceObservationCode::kCONTIGUOUS;
                    return observation;
                }
                if(sequence > previous){
                    observation.code = SequenceObservationCode::kGAP;
                    sid_state.last_sequence = sequence;
                    return observation;
                }

                observation.code = SequenceObservationCode::kSTALE;
                return observation;
            }

        private:
            std::unordered_map<std::uint32_t, ActiveSidState> states_;
    };

    struct PendingWsCommand{
        std::uint64_t ws_command_id{};
        WsCommandKind kind{};
        exchange::kalshi::KalshiMarketDataChannel channel{};
        std::vector<core::control::MarketId> market_ids;
        std::optional<RecoveryCommandContext> recovery_context;
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
            friend class KalshiWireSessionTestPeer;
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

            std::unordered_map<core::control::MarketId,PendingRecoveryTag> pending_recovery_by_market_;

            SidSequenceObserver sequence_observer_;

            std::optional<MarketDataPathMessage> pending_integrity_barrier_;
            std::uint64_t next_wire_incident_id_{1};
            std::deque<core::control::IoToControlStatus>pending_recovery_statuses_;

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
            [[nodiscard]] EnvelopeParseCode parse_market_data_envelope( simdjson::padded_string_view payload,
                                                          MarketDataEnvelope& envelope_out);
            [[nodiscard]] StampHandleCode resolve_market_route(const MarketDataEnvelope& envelope,const core::control::UniverseMarketRoute*& route_out) const noexcept;
            void stamp_handle(FrameHandle& handle,const MarketDataEnvelope& envelope,const core::control::UniverseMarketRoute& route) noexcept;
            [[nodiscard]] bool is_active_sid(std::uint32_t sid) const noexcept;
            void publish_market_data_frame(std::span<const std::byte> payload);
            void handle_ws_control_response(std::span<const std::byte> payload);

            void recover_market(const core::control::RecoverMarketIo& recover_market_io);

            void report_fault(std::string error_message);

            [[nodiscard]] bool flush_pending_integrity_barrier()noexcept;
            [[nodiscard]] bool send_or_defer_integrity_barrier(MarketDataPathMessage barrier) noexcept;

            void send_or_defer_recovery_status(core::control::IoToControlStatus status);
            [[nodiscard]] bool flush_pending_recovery_statuses() noexcept;

            [[nodiscard]] core::control::MarketDataChannelTelemetrySnapshot*
            channel_telemetry(
                exchange::kalshi::KalshiMarketDataChannel channel) noexcept;
            void update_capacity_high_water() noexcept;

            [[nodiscard]] SequenceObservation observe_sequence(std::uint32_t sid, std::uint64_t sequence) noexcept;

    };

}
