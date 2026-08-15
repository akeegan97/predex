#include "predex/router/router.hpp"

#include <type_traits>
#include <algorithm>

namespace predex::router{

    core::control::MarketDataChannelTelemetrySnapshot* Router::channel_stats(
        ingest::kalshi::FrameKind kind) noexcept{
        std::size_t index{};
        switch(kind){
            case ingest::kalshi::FrameKind::kORDERBOOK_SNAPSHOT:
            case ingest::kalshi::FrameKind::kORDERBOOK_DELTA:
                index = 0;
                break;
            case ingest::kalshi::FrameKind::kTRADE:
                index = 1;
                break;
            case ingest::kalshi::FrameKind::kLIFECYCLE:
                index = 2;
                break;
            case ingest::kalshi::FrameKind::kSUBSCRIPTION_ACK:
            case ingest::kalshi::FrameKind::kUNSUBSCRIBED:
            case ingest::kalshi::FrameKind::kHEARTBEAT:
            case ingest::kalshi::FrameKind::kUNKNOWN:
                return nullptr;
        }
        return &channel_stats_[index];
    }

    void Router::update_shard_queue_high_water(
        const utils::SPSCQueue<ingest::kalshi::MarketDataPathMessage>& queue) noexcept{
        shard_queue_depth_high_water_ = std::max<std::uint64_t>(
            shard_queue_depth_high_water_,
            queue.producer_size());
    }

    bool Router::send_subscription_recovery_fact(
        const ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier) const noexcept{
        return send_telemetry(OrderBookSubscriptionBarrierDelivered{
            .universe_version = barrier.universe_version,
            .incident = barrier.incident,
            .sid = barrier.sid,
            .expected_sequence = barrier.expected_sequence,
            .observed_sequence = barrier.observed_sequence,
            .reason = barrier.reason,
        });
    }

    RouterRouteResult Router::finish_subscription_barrier(
        const ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier) noexcept{
        ++subscription_barriers_delivered_;
        if(queues_.router_to_control_queue == nullptr){
            return RouterRouteResult::kFAULTED;
        }
        if(send_subscription_recovery_fact(barrier)){
            return RouterRouteResult::kCOMPLETED;
        }
        pending_subscription_recovery_fact_ =
            OrderBookSubscriptionBarrierDelivered{
                .universe_version = barrier.universe_version,
                .incident = barrier.incident,
                .sid = barrier.sid,
                .expected_sequence = barrier.expected_sequence,
                .observed_sequence = barrier.observed_sequence,
                .reason = barrier.reason,
            };
        ++subscription_recovery_facts_deferred_;
        return RouterRouteResult::kBLOCKED;
    }

    RouterRouteResult Router::route_message(const predex::ingest::kalshi::MarketDataPathMessage& message){
        if(pending_barrier_.has_value() ||
           pending_subscription_recovery_fact_.has_value()){
            return RouterRouteResult::kBLOCKED;
        }
        if(std::holds_alternative<predex::ingest::kalshi::FrameHandle>(message)){
            return route_frame(std::get<predex::ingest::kalshi::FrameHandle>(message));
        }
        if(std::holds_alternative<predex::ingest::kalshi::MarketInvalidationBarrier>(message)){
            ++market_barriers_received_;
            const auto& barrier = std::get<predex::ingest::kalshi::MarketInvalidationBarrier>(message);
            return begin_barrier_delivery(barrier);
        }
        if(std::holds_alternative<predex::ingest::kalshi::OrderBookSubscriptionInvalidationBarrier>(message)){
            ++subscription_barriers_received_;
            const auto& barrier = std::get<predex::ingest::kalshi::OrderBookSubscriptionInvalidationBarrier>(message);
            return begin_barrier_delivery(barrier);
        }
        return RouterRouteResult::kFAULTED;
    }

    RouterRouteResult Router::route_frame(const predex::ingest::kalshi::FrameHandle& handle){
        ++total_frames_seen_;
        ++current_frame_count_;
        if(auto* stats = channel_stats(handle.kind); stats != nullptr){
            ++stats->frames_observed;
        }

        switch(try_route_to_shard(handle)){
            case ShardEnqueueResult::kENQUEUED:
                ++total_frames_to_shards_;
                maybe_send_periodic_telemetry();
                return RouterRouteResult::kCOMPLETED;
            case ShardEnqueueResult::kFULL:{
                if(auto* stats = channel_stats(handle.kind); stats != nullptr){
                    ++stats->downstream_delivery_losses;
                }
                ShardBackpressure telemetry{
                    .shard_index = handle.shard_index,
                    .affinity_key = handle.affinity_key,
                    .market_id = handle.market_id,
                    .event_id = handle.event_id
                };
                (void)send_telemetry(telemetry);

                const auto handle_kind = handle.kind;
                if(!terminal_handoff(handle)){
                    maybe_send_periodic_telemetry();
                    return RouterRouteResult::kFAULTED;
                }

                switch(handle_kind){
                    case predex::ingest::kalshi::FrameKind::kTRADE:
                    case predex::ingest::kalshi::FrameKind::kHEARTBEAT:
                    case predex::ingest::kalshi::FrameKind::kLIFECYCLE:
                    case predex::ingest::kalshi::FrameKind::kSUBSCRIPTION_ACK:
                    case predex::ingest::kalshi::FrameKind::kUNSUBSCRIBED:
                    case predex::ingest::kalshi::FrameKind::kUNKNOWN:
                        maybe_send_periodic_telemetry();
                        return RouterRouteResult::kCOMPLETED;
                    case predex::ingest::kalshi::FrameKind::kORDERBOOK_DELTA:
                    case predex::ingest::kalshi::FrameKind::kORDERBOOK_SNAPSHOT:{
                        const predex::ingest::kalshi::MarketInvalidationBarrier barrier{
                            .universe_version = handle.universe_version,
                            .incident = {.origin = predex::ingest::kalshi::IntegrityIncidentOrigin::kROUTER,
                                         .producer_index = 0,
                                         .incident_id = next_router_incident_id_++
                                        },
                            .sid = handle.sid,
                            .sequence = handle.sequence,
                            .market_id = handle.market_id,
                            .event_id = handle.event_id,
                            .shard_index = handle.shard_index,
                            .shard_event_index = handle.shard_event_index,
                            .event_market_index = handle.event_market_index,
                            .reason = predex::ingest::kalshi::BookInvalidationReason::kROUTER_TO_SHARD_DELIVERY_LOSS
                        };
                        const auto result = begin_barrier_delivery(barrier);
                        maybe_send_periodic_telemetry();
                        return result;
                    }
                }
                maybe_send_periodic_telemetry();
                return RouterRouteResult::kFAULTED;
            }
            case ShardEnqueueResult::kINVALID_TARGET:{
                (void)terminal_handoff(handle);
                maybe_send_periodic_telemetry();
                return RouterRouteResult::kFAULTED;
            }
        }

        maybe_send_periodic_telemetry();
        return RouterRouteResult::kFAULTED;
    }

    RouterRouteResult Router::begin_barrier_delivery(
        const predex::ingest::kalshi::MarketInvalidationBarrier& barrier) noexcept{
        const auto result = route_barrier(barrier);
        switch(result){
            case BarrierDeliveryResult::kDELIVERED:
                ++market_barriers_delivered_;
                return RouterRouteResult::kCOMPLETED;
            case BarrierDeliveryResult::kBLOCKED:
                pending_barrier_ = PendingMarketBarrier{barrier};
                ++barriers_deferred_;
                return RouterRouteResult::kBLOCKED;
            case BarrierDeliveryResult::kINVALID_TARGET:
                return RouterRouteResult::kFAULTED;
        }
        return RouterRouteResult::kFAULTED;
    }

    RouterRouteResult Router::begin_barrier_delivery(
        const predex::ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier) noexcept{
        std::size_t next_shard_idx = 0;
        const auto result = route_barrier(barrier, next_shard_idx);
        switch(result){
            case BarrierDeliveryResult::kDELIVERED:
                return finish_subscription_barrier(barrier);
            case BarrierDeliveryResult::kBLOCKED:
                pending_barrier_ = PendingSubscriptionBarrier{barrier, next_shard_idx};
                ++barriers_deferred_;
                return RouterRouteResult::kBLOCKED;
            case BarrierDeliveryResult::kINVALID_TARGET:
                return RouterRouteResult::kFAULTED;
        }
        return RouterRouteResult::kFAULTED;
    }

    BarrierDeliveryResult Router::route_barrier(
        const predex::ingest::kalshi::MarketInvalidationBarrier& barrier) noexcept{
        if(barrier.reason == predex::ingest::kalshi::BookInvalidationReason::kNONE){
            return BarrierDeliveryResult::kINVALID_TARGET;
        }
        const auto shard_index = static_cast<std::size_t>(barrier.shard_index);
        if(shard_index >= queues_.router_to_shard_queues.size() ||
           queues_.router_to_shard_queues[shard_index] == nullptr){
            return BarrierDeliveryResult::kINVALID_TARGET;
        }
        const predex::ingest::kalshi::MarketDataPathMessage message{barrier};
        auto& queue = *queues_.router_to_shard_queues[shard_index];
        if(!queue.try_push(message)){
            return BarrierDeliveryResult::kBLOCKED;
        }
        update_shard_queue_high_water(queue);
        return BarrierDeliveryResult::kDELIVERED;
    }

    BarrierDeliveryResult Router::route_barrier(
        const predex::ingest::kalshi::OrderBookSubscriptionInvalidationBarrier& barrier,
        std::size_t& next_shard_idx) noexcept{
        if(barrier.reason == predex::ingest::kalshi::BookInvalidationReason::kNONE ||
           queues_.router_to_shard_queues.empty() ||
           next_shard_idx > queues_.router_to_shard_queues.size()){
            return BarrierDeliveryResult::kINVALID_TARGET;
        }
        while(next_shard_idx < queues_.router_to_shard_queues.size()){
            auto* const queue = queues_.router_to_shard_queues[next_shard_idx];
            if(queue == nullptr){
                return BarrierDeliveryResult::kINVALID_TARGET;
            }
            const predex::ingest::kalshi::MarketDataPathMessage message{barrier};
            if(!queue->try_push(message)){
                return BarrierDeliveryResult::kBLOCKED;
            }
            update_shard_queue_high_water(*queue);
            ++next_shard_idx;
        }
        return BarrierDeliveryResult::kDELIVERED;
    }

    RouterRouteResult Router::flush_pending_barrier() noexcept{
        if(pending_subscription_recovery_fact_.has_value()){
            if(!send_telemetry(*pending_subscription_recovery_fact_)){
                return RouterRouteResult::kBLOCKED;
            }
            pending_subscription_recovery_fact_.reset();
            return RouterRouteResult::kCOMPLETED;
        }
        if(!pending_barrier_.has_value()){
            return RouterRouteResult::kCOMPLETED;
        }

        const auto delivery_result = std::visit(
            [this](auto& pending) -> BarrierDeliveryResult {
                using Pending = std::decay_t<decltype(pending)>;
                if constexpr(std::is_same_v<Pending, PendingMarketBarrier>){
                    return route_barrier(pending.barrier);
                }else{
                    return route_barrier(pending.barrier, pending.next_shard_idx);
                }
            },
            *pending_barrier_);

        switch(delivery_result){
            case BarrierDeliveryResult::kDELIVERED:
                if(std::holds_alternative<PendingMarketBarrier>(
                       *pending_barrier_)){
                    ++market_barriers_delivered_;
                    pending_barrier_.reset();
                    return RouterRouteResult::kCOMPLETED;
                }
                {
                    const auto barrier =
                        std::get<PendingSubscriptionBarrier>(
                            *pending_barrier_).barrier;
                    pending_barrier_.reset();
                    return finish_subscription_barrier(barrier);
                }
            case BarrierDeliveryResult::kBLOCKED:
                return RouterRouteResult::kBLOCKED;
            case BarrierDeliveryResult::kINVALID_TARGET:
                return RouterRouteResult::kFAULTED;
        }
        return RouterRouteResult::kFAULTED;
    }

}
