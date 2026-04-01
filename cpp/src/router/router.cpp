#include "predex/router/router.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/router/market_registry.hpp"


namespace predex::core::routing{
    Router::Router(predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& ingress_queue,
        predex::core::ingest::kalshi::FramePool& frame_pool,
        const predex::core::routing::MarketRegistry &market_registry,
        predex::core::routing::ShardDispatch &shard_dispatch,
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue) noexcept
        : ingress_queue_(ingress_queue),
          frame_pool_(frame_pool),
          market_registry_(market_registry),
          shard_dispatch_(shard_dispatch),
          logger_queue_(logger_queue) {}
    RouteDecision Router::classify(predex::core::ingest::kalshi::FrameHandle& handle, const predex::core::ingest::kalshi::KalshiFrame& frame) noexcept{
        //Framehandle at this point is only stamped with iowriter timestamp,
        //need to extract market ticker, sid, seq, lookup/attach affinity key before we can make routing decision
        
        
    }
}