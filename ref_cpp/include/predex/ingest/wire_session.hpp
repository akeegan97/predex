#pragma once

#include "predex/control/control_types.hpp"
#include "predex/ingest/frame_ingress_publisher.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/utils/spsc_queue.hpp"
#include "predex/websocket/client.hpp"
#include "predex/websocket/kalshi/auth_signer.hpp"
#include "predex/websocket/kalshi/ws_adapter.hpp"
#include "predex/websocket/session.hpp"


#include <string>
#include <utility>
#include <vector>
#include <unordered_set>

/*
Composer of the public venue feed session for market data ingestion,
separate from REST and private WebSocket sessions used for order management.
*/

namespace predex::core::ingest::kalshi {

enum class KalshiChannel : std::uint8_t{
    kOrderbookUpdates,
    kTrades,
    kLifeCycleEvents,
};
struct ActiveSubscription{
        KalshiChannel channel;
        std::optional<std::int64_t> sid;
        std::unordered_set<internal::MarketId> market_ids;
};
struct KalshiWireSessionConfig {
    std::string key_id;
    std::string private_key_pem;
    std::string endpoint = "wss://api.elections.kalshi.com/trade-api/ws/v2";
};

struct CommandQueues {
    utils::SPSCQueue<predex::core::control::ControlIoCommand>& control_to_io_queue;
    utils::SPSCQueue<predex::core::control::IoControlStatus>& io_to_control_status_queue;
};

struct IncomingQueues {
    // Fan-in of per-producer SPSC recycle queues.
    // Entries are borrowed and may be nullptr if that recycle lane is disabled.
    std::vector<utils::SPSCQueue<FrameHandle>*> recycle_queues;
};

struct OutgoingQueues {
    utils::SPSCQueue<FrameHandle>& router_queue;
};

struct KalshiWireSessionDeps {
    FramePool& frame_pool;
    CommandQueues command_queues;
    IncomingQueues incoming_queues;
    OutgoingQueues outgoing_queues;
};

class KalshiWireSession {
public:
    KalshiWireSession(
        KalshiWireSessionConfig config,
        KalshiWireSessionDeps deps
    )
        : config_(std::move(config))
        , control_to_io_queue_(deps.command_queues.control_to_io_queue)
        , io_to_control_status_queue_(deps.command_queues.io_to_control_status_queue)
        , adapter_(
              predex::websocket::kalshi::AuthSigner{
                  predex::websocket::kalshi::Credentials{
                      .key_id = config_.key_id,
                      .private_key_pem = config_.private_key_pem,
                  }
              },
              config_.endpoint
          )
        , session_(transport_, adapter_)
        , publisher_(
              deps.frame_pool,
              deps.outgoing_queues.router_queue,
              std::move(deps.incoming_queues.recycle_queues)
          )
    {}

    void run(const std::stop_token& stop_token);

    [[nodiscard]] const std::string& last_error() const { return session_.last_error(); }
          
    [[nodiscard]] bool pop_control_command(predex::core::control::ControlIoCommand& cmd_out) noexcept {
        return control_to_io_queue_.try_pop(cmd_out);
    }

    [[nodiscard]] bool send_control_status(const predex::core::control::IoControlStatus& status) noexcept {
        return io_to_control_status_queue_.try_push(status);
    }

    [[nodiscard]] IngestionTelemetry get_telemetry() const noexcept {
        return publisher_.get_telemetry();
    }



private:
    KalshiWireSessionConfig config_;

    utils::SPSCQueue<predex::core::control::ControlIoCommand>& control_to_io_queue_;
    utils::SPSCQueue<predex::core::control::IoControlStatus>& io_to_control_status_queue_;

    predex::websocket::BoostBeastWsTransport transport_;
    predex::websocket::kalshi::WsAdapter adapter_;
    predex::websocket::WsSession session_;
    FrameIngressPublisher publisher_;
};

} // namespace predex::core::ingest::kalshi