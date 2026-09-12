#pragma once 

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "predex/ingest/kalshi/market_data/frame_pool.hpp"
#include "predex/utils/spsc.hpp"
#include "predex/utils/latency_histogram.hpp"
#include "predex/utils/monotonic_clock.hpp"
#include "predex/router/router_types.hpp"

namespace predex::router{

    inline constexpr std::size_t kFRAMETHRESHOLD = 1000;

    struct RouterQueues{
        std::vector<utils::SPSCQueue<predex::ingest::kalshi::MarketDataPathMessage>*> router_to_shard_queues;
        utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>* router_to_logger_queue;
        utils::SPSCQueue<RouterToControl>* router_to_control_queue;
        utils::SPSCQueue<predex::ingest::kalshi::FrameHandle>* last_resort_recycle_queue;
    };

    enum class RouterRouteResult : std::uint8_t{
        kCOMPLETED,
        kBLOCKED,
        kFAULTED
    };

    enum class ShardEnqueueResult : std::uint8_t{
        kENQUEUED,
        kFULL,
        kINVALID_TARGET
    };

    enum class BarrierDeliveryResult : std::uint8_t{
        kDELIVERED,
        kBLOCKED,
        kINVALID_TARGET
    };


    struct PendingMarketBarrier{
        ingest::kalshi::MarketInvalidationBarrier barrier;
    };

    struct PendingSubscriptionBarrier{
        ingest::kalshi::OrderBookSubscriptionInvalidationBarrier barrier;
        std::size_t next_shard_idx{};
    };

    using PendingBarrier = std::variant<PendingMarketBarrier, PendingSubscriptionBarrier>;

    class Router{
        public:
            Router(RouterQueues queues): queues_(std::move(queues)){};

            [[nodiscard]] RouterRouteResult route_message(const predex::ingest::kalshi::MarketDataPathMessage& message);
            [[nodiscard]] RouterRouteResult flush_pending_barrier() noexcept;

        private:
            [[nodiscard]] bool send_telemetry(const RouterToControl& telemetry) const noexcept{
                if(queues_.router_to_control_queue == nullptr){
                    return false;
                }
                return queues_.router_to_control_queue->try_push(RouterToControl{telemetry});
            }

            [[nodiscard]] bool send_subscription_recovery_fact(
                const ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier) const noexcept;

            [[nodiscard]] RouterRouteResult finish_subscription_barrier(
                const ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier) noexcept;

            void update_shard_queue_high_water(
                const utils::SPSCQueue<ingest::kalshi::MarketDataPathMessage>& queue) noexcept;

            [[nodiscard]] core::control::MarketDataChannelTelemetrySnapshot*
            channel_stats(ingest::kalshi::FrameKind kind) noexcept;
            
            [[nodiscard]] ShardEnqueueResult try_route_to_shard(const predex::ingest::kalshi::FrameHandle& handle) noexcept{
                if(queues_.router_to_shard_queues.empty()){
                    return ShardEnqueueResult::kINVALID_TARGET;
                }
                const auto shard_id = static_cast<std::size_t>(handle.shard_index);
                if(shard_id >= queues_.router_to_shard_queues.size() || queues_.router_to_shard_queues[shard_id] == nullptr){
                    return ShardEnqueueResult::kINVALID_TARGET;
                }
                const ingest::kalshi::MarketDataPathMessage message{handle};
                auto& queue = *queues_.router_to_shard_queues[shard_id];
                if(!queue.try_push(message)){
                    return ShardEnqueueResult::kFULL;
                }
                update_shard_queue_high_water(queue);
                return ShardEnqueueResult::kENQUEUED;
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

            [[nodiscard]] bool terminal_handoff(const predex::ingest::kalshi::FrameHandle& handle) noexcept{
                if(try_route_to_logger(handle)){
                    ++total_frames_to_logger_;
                    if(auto* stats = channel_stats(handle.kind); stats != nullptr){
                        ++stats->logger_only_frames;
                    }
                    return true;
                }
                if(try_recycle(handle)){
                    ++total_frames_recycled_;
                    return true;
                }
                report_handle_leak(handle);
                return false;
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
                    .frames_recycled = total_frames_recycled_,
                    .market_barriers_received = market_barriers_received_,
                    .market_barriers_delivered = market_barriers_delivered_,
                    .subscription_barriers_received = subscription_barriers_received_,
                    .subscription_barriers_delivered = subscription_barriers_delivered_,
                    .barriers_deferred = barriers_deferred_,
                    .subscription_recovery_facts_deferred =
                        subscription_recovery_facts_deferred_,
                    .shard_queue_depth_high_water = shard_queue_depth_high_water_,
                    .channel_stats = channel_stats_,
                    .wire_to_router_latency = wire_to_router_latency_,
                    .router_service_latency = router_service_latency_,
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
                    .shard_index = handle.shard_index,
                    .market_id = handle.market_id,
                    .event_id = handle.event_id,
                };
                (void)send_telemetry(leak);
            }

            [[nodiscard]] RouterRouteResult route_frame(const predex::ingest::kalshi::FrameHandle& handle);

            [[nodiscard]] RouterRouteResult begin_barrier_delivery(const predex::ingest::kalshi::MarketInvalidationBarrier& barrier) noexcept;
            [[nodiscard]] RouterRouteResult begin_barrier_delivery(const predex::ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier) noexcept;

            [[nodiscard]] BarrierDeliveryResult route_barrier(const predex::ingest::kalshi::MarketInvalidationBarrier& barrier) noexcept;
            [[nodiscard]] BarrierDeliveryResult route_barrier(
                const predex::ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier,
                std::size_t& next_shard_idx) noexcept;

            RouterQueues queues_;

            std::optional<PendingBarrier> pending_barrier_;
            std::optional<OrderBookSubscriptionBarrierDelivered>
                pending_subscription_recovery_fact_;

            std::uint64_t next_router_incident_id_ = 1;
            
            // Telemetry counters
            std::uint64_t telemetry_send_threshold_ = kFRAMETHRESHOLD;
            std::uint64_t current_frame_count_ = 0;
            std::uint64_t total_frames_seen_ = 0;
            std::uint64_t total_frames_to_shards_ = 0;
            std::uint64_t total_frames_to_logger_ = 0;
            std::uint64_t total_frames_recycled_ = 0;
            std::uint64_t market_barriers_received_ = 0;
            std::uint64_t market_barriers_delivered_ = 0;
            std::uint64_t subscription_barriers_received_ = 0;
            std::uint64_t subscription_barriers_delivered_ = 0;
            std::uint64_t barriers_deferred_ = 0;
            std::uint64_t subscription_recovery_facts_deferred_ = 0;
            std::uint64_t shard_queue_depth_high_water_ = 0;
            core::control::MarketDataChannelTelemetry channel_stats_{
                core::control::make_market_data_channel_telemetry()};
            core::control::MarketDataChannelLatency wire_to_router_latency_{};
            core::control::MarketDataChannelLatency router_service_latency_{};
    };
}
