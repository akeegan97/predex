#include "predex/router/router.hpp"

namespace predex::router{

    void Router::route_frame(const predex::ingest::kalshi::FrameHandle& handle){
        total_frames_seen_++;
        current_frame_count_++;

        if(!check_sequence(handle)){
            OutOfSequenceFrame oos_frame{
                .sid = handle.sid,
                .sequence = handle.sequence,
                .market_id = handle.market_id,
                .event_id = handle.event_id,
                .shard_index = compute_shard_id(handle)
            };
            //best effort to notify control plane of out-of-sequence frame,
            //probably can start kick off into recover market, should be quicker
            //compared to waiting for impossible book state to be detected by shard and trigger recover
            (void)send_telemetry(oos_frame);
            if(try_route_to_logger(handle)){
                total_frames_to_logger_++;
            }
            else if(try_recycle(handle)){
                total_frames_recycled_++;
            }
            else{
                report_handle_leak(handle);
            }
            maybe_send_periodic_telemetry();
            return;
        }

        if (try_route_to_shard(handle)){
            total_frames_to_shards_++;
            maybe_send_periodic_telemetry();
            return;
        }

        ShardBackpressure backpressure{
            .shard_index = compute_shard_id(handle),
            .affinity_key = handle.affinity_key,
            .market_id = handle.market_id,
            .event_id = handle.event_id
        };
        (void)send_telemetry(backpressure);

        if(try_route_to_logger(handle)){
            total_frames_to_logger_++;
            maybe_send_periodic_telemetry();
            return;
        }

        if(try_recycle(handle)){
            total_frames_recycled_++;
        }
        else{
            report_handle_leak(handle);
        }
        maybe_send_periodic_telemetry();
    }

}
