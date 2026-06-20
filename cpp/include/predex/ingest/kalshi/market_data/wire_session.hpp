#pragma once 

#include <string>
#include <vector>
#include <utility>
#include <stop_token>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <span>
#include <string_view>


#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/exchange/kalshi/websocket_session.hpp"
#include "predex/exchange/kalshi/adapters/market_data_handler.hpp"
#include "predex/control/control_types.hpp"

namespace predex::ingest::kalshi::market_data{

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

    using RecycleQueues = std::vector<predex::utils::SPSCQueue<FrameHandle>*>;

    struct KalshiWireSessionDeps{
        FramePool& frame_pool;
        ControlQueues control_queues;
        RecycleQueues recycle_queues;
        RouterQueue& router_queue;
        exchange::kalshi::KalshiMarketDataHandler market_data_handler;
    };


    class KalshiWireSession {
        public:
            KalshiWireSession(KalshiWireSessionDeps deps) : 
                frame_pool_(deps.frame_pool), 
                router_queue_(deps.router_queue), 
                control_queues_(deps.control_queues), 
                recycle_queues_(std::move(deps.recycle_queues)),
                market_data_handler_(std::move(deps.market_data_handler)),
                ws_session_(market_data_handler_){}

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

            exchange::kalshi::KalshiMarketDataHandler market_data_handler_;
            exchange::kalshi::WebSocketSession ws_session_;

            WireSessionState status_;

            std::shared_ptr<const core::control::UniverseSnapshot> desired_universe_{nullptr};
            std::unordered_map<std::string, core::control::UniverseMarketRoute> market_routes_by_tickers_;
            std::unordered_set<core::control::MarketId> active_market_ids_;

            void drain_control_commands();
            void handle_control_command(const core::control::ControlToIoCommand& cmd);

            void apply_universe_snapshot(const std::shared_ptr<const core::control::UniverseSnapshot>& snapshot);
            [[nodiscard]] bool connect();
            void disconnect(std::string reason = {});

            [[nodiscard]] bool subscribe_active_universe();
            [[nodiscard]] bool subscribe_ticker(std::string_view ticker);
            [[nodiscard]] bool unsubscribe_ticker(std::string_view ticker);

            [[nodiscard]] bool push_control_status(core::control::IoToControlStatus status) noexcept;

            void drain_recycle_queues();
            void pump_socket_once();
            void publish_frame(std::span<const std::byte> payload);

            void report_fault(std::string error_message);


    };

}