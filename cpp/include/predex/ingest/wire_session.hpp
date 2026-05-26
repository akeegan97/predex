#pragma once 
#include <string>
#include <utility>
#include <vector>

#include "predex/websocket/client.hpp"
#include "predex/websocket/kalshi/ws_adapter.hpp"
#include "predex/websocket/ws_adapter.hpp"
#include "predex/websocket/kalshi/auth_signer.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/websocket/session.hpp"
#include "predex/control/control_types.hpp"
#include "predex/utils/spsc_queue.hpp"    
#include "predex/ingest/frame_ingress_publisher.hpp"
/*
composer of the public venue feed session for market data ingestion, separate from REST & Private WebSocket sessions used for order management.
*/


namespace predex::core::ingest::kalshi{
    struct KalshiWireSessionConfig{
        std::string key_id;
        std::string private_key_pem;
        std::string endpoint = "wss://api.elections.kalshi.com/trade-api/ws/v2";
    };
    struct CommandQueues{
        utils::SPSCQueue<predex::core::control::ControlIoCommand>& control_to_io_queue;
        utils::SPSCQueue<predex::core::control::IoControlStatus>& io_to_control_status_queue;
    };
    struct IncomingQueues{
        //recycle_queues is a fan-in of per-producer SPSC queues (one per producer thread, e.g. logger, router, each shard).
        //passing via pointer since could be nullptr if no producers are configured to recycle frames back (i.e. if shards want to bypass recycling)
        std::vector<utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>*> recycle_queues;
    };
    struct OutgoingQueues{
        utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& router_queue;
    };
    struct KalshiWireSessionDeps{
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
            , transport_()
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
            , publisher_(deps.frame_pool, deps.outgoing_queues.router_queue, std::move(deps.incoming_queues.recycle_queues))
        {}
    private:
        KalshiWireSessionConfig config_;

        utils::SPSCQueue<predex::core::control::ControlIoCommand>& control_to_io_queue_;
        utils::SPSCQueue<predex::core::control::IoControlStatus>& io_to_control_status_queue_;

        predex::websocket::BoostBeastWsTransport transport_;
        predex::websocket::kalshi::WsAdapter adapter_;
        predex::websocket::WsSession session_;
        predex::core::ingest::kalshi::FrameIngressPublisher publisher_;
    };

}