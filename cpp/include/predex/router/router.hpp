#pragma once 

#include <vector>

#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/router/router_types.hpp"

namespace predex::router{

    inline constexpr std::size_t kFRAMETHRESHOLD = 1000;

    struct RouterQueues{
        std::vector<utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>*> router_to_shard_queues;
        utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>* router_to_logger_queue;
        utils::SPSCQueue<RouterToControl>* router_to_control_queue;
        utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>* last_resort_recycle_queue;
    };

    struct SidSequenceState{
        std::uint64_t universe_version{};
        std::uint64_t last_sequence{};
    };

    class Router{
        public:
            Router(RouterQueues queues): queues_(std::move(queues)){};

            void route_frame(const predex::ingest::kalshi::FrameHandle& handle);

        private:
            [[nodiscard]] bool send_telemetry(const RouterToControl& telemetry) const noexcept{
                if(queues_.router_to_control_queue == nullptr){
                    return false;
                }
                return queues_.router_to_control_queue->try_push(RouterToControl{telemetry});
            }
            
            [[nodiscard]] bool try_route_to_shard(const predex::ingest::kalshi::FrameHandle& handle) const noexcept{
                if(queues_.router_to_shard_queues.empty()){return false;}
                const auto shard_id = compute_shard_id(handle);
                if(shard_id >= queues_.router_to_shard_queues.size() || queues_.router_to_shard_queues[shard_id] == nullptr){
                    return false;
                }
                return queues_.router_to_shard_queues[shard_id]->try_push(handle);
            }
            
            [[nodiscard]] bool try_route_to_logger(const predex::ingest::kalshi::FrameHandle& handle) const noexcept{
                if(queues_.router_to_logger_queue == nullptr){
                    return false;
                }
                return queues_.router_to_logger_queue->try_push(handle);
            }

            [[nodiscard]] bool try_recycle(const predex::ingest::kalshi::FrameHandle& handle) const noexcept{
                if(queues_.last_resort_recycle_queue == nullptr){
                    return false;
                }
                return queues_.last_resort_recycle_queue->try_push(handle);
            }

            void maybe_send_periodic_telemetry() noexcept{
                if(telemetry_send_threshold_ == 0 ||
                   current_frame_count_ < telemetry_send_threshold_){
                    return;
                }

                RouterTelemetry telemetry{
                    .total_frames_seen = total_frames_seen_,
                    .frames_to_shards = total_frames_to_shards_,
                    .frames_to_logger = total_frames_to_logger_,
                    .frames_recycled = total_frames_recycled_
                };
                (void)send_telemetry(telemetry);
                current_frame_count_ = 0;
            }

            void report_handle_leak(const predex::ingest::kalshi::FrameHandle& handle) const noexcept{
                RouterHandleLeak leak{
                    .universe_version = handle.universe_version,
                    .sid = handle.sid,
                    .sequence = handle.sequence,
                    .pool_index = handle.pool_index,
                    .pool_generation = handle.pool_generation,
                    .shard_index = compute_shard_id(handle),
                    .market_id = handle.market_id,
                    .event_id = handle.event_id,
                };
                (void)send_telemetry(leak);
            }

            [[nodiscard]] bool check_sequence(const predex::ingest::kalshi::FrameHandle& handle) noexcept{
                if(handle.sid >= sequence_by_sid_.size()){
                    sequence_by_sid_.resize(handle.sid + 1);
                }
                auto& state = sequence_by_sid_[handle.sid];
                if(state.universe_version != handle.universe_version){
                    state.universe_version = handle.universe_version;
                    state.last_sequence = handle.sequence;
                    return true;
                }
                if(state.last_sequence != 0 && handle.sequence != state.last_sequence + 1){
                    return false;
                }
                state.last_sequence = handle.sequence;
                return true;
            }

            [[nodiscard]] std::size_t compute_shard_id(const predex::ingest::kalshi::FrameHandle& handle) const noexcept{
                if(queues_.router_to_shard_queues.empty()){
                    return 0;
                }
                return handle.affinity_key % queues_.router_to_shard_queues.size();
            }

            RouterQueues queues_;
            std::vector<SidSequenceState> sequence_by_sid_;
            
            // Telemetry counters
            std::uint64_t telemetry_send_threshold_ = kFRAMETHRESHOLD;
            std::uint64_t current_frame_count_ = 0;
            std::uint64_t total_frames_seen_ = 0;
            std::uint64_t total_frames_to_shards_ = 0;
            std::uint64_t total_frames_to_logger_ = 0;
            std::uint64_t total_frames_recycled_ = 0;
    };
}
