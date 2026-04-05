#include "predex/router/router.hpp"
#include "predex/ingest/frame_pool.hpp"
#include "predex/router/market_registry.hpp"
#include <chrono>
#include <simdjson.h>



namespace predex::core::routing::kalshi{
    namespace{
        bool get_uint64(simdjson::ondemand::object& obj, std::string_view key, std::uint64_t& out) noexcept{
            auto result = obj.find_field_unordered(key).get_uint64();
            if (result.error() != simdjson::SUCCESS) {
                return false;
            }
            out = result.value_unsafe();
            return true;
        }
        bool get_string(simdjson::ondemand::object& obj, std::string_view key, std::string_view& out) noexcept{
            auto result = obj.find_field_unordered(key).get_string();
            if (result.error() != simdjson::SUCCESS) {
                return false;
            }
            out = result.value_unsafe();
            return true;
        }
        bool get_object(simdjson::ondemand::object& obj, std::string_view key, simdjson::ondemand::object& out) noexcept{
            auto result = obj.find_field_unordered(key).get_object();
            if (result.error() != simdjson::SUCCESS) {
                return false;
            }
            out = result.value_unsafe();
            return true;
        }
    }
    Router::Router(predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& ingress_queue,
        predex::core::ingest::kalshi::FramePool& frame_pool,
        const predex::core::routing::kalshi::MarketRegistry &market_registry,
        predex::core::routing::kalshi::ShardDispatch &shard_dispatch,
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue) noexcept
        : ingress_queue_(ingress_queue),
          frame_pool_(frame_pool),
          market_registry_(market_registry),
          shard_dispatch_(shard_dispatch),
          logger_queue_(logger_queue) {}
    RouteDecision Router::classify(predex::core::ingest::kalshi::FrameHandle& handle, const predex::core::ingest::kalshi::KalshiFrame& frame) noexcept{
        //Framehandle at this point is only stamped with iowriter timestamp,
        //need to extract market ticker, sid, seq, lookup/attach affinity key before we can make routing decision
        const auto* payload = reinterpret_cast<const char*>(frame.payload);
        simdjson::padded_string padded{payload, frame.len_};
        simdjson::ondemand::parser parser;
        auto doc = parser.iterate(padded);
        if(doc.error() != simdjson::SUCCESS) {
            return RouteDecision::kToLogger; 
        }
        auto root = doc.get_object();
        if(root.error() != simdjson::SUCCESS){
            return RouteDecision::kToLogger; 
        }
        auto obj = root.value_unsafe();
        std::string_view obj_type;
        if(!get_string(obj, "type", obj_type)){
            return RouteDecision::kToLogger; 
        }
        if(obj_type != "orderbook_delta" && obj_type != "trade" && obj_type != "orderbook_snapshot"){
            return RouteDecision::kToLogger; 
        }
        if(obj_type == "orderbook_delta"){
            handle.event_type_ = predex::core::ingest::kalshi::KalshiEventType::kDelta;
        }
        if(obj_type == "trade"){
            handle.event_type_ = predex::core::ingest::kalshi::KalshiEventType::kTrade;
        }
        if(obj_type == "orderbook_snapshot"){
            handle.event_type_ = predex::core::ingest::kalshi::KalshiEventType::kSnapshot;
        }

        std::uint64_t sid = 0;
        if(!get_uint64(obj, "sid", sid)){
            return RouteDecision::kToLogger; 
        }
        handle.sid_ = sid;
        std::uint64_t seq = 0;
        if(!get_uint64(obj, "seq", seq)){
            return RouteDecision::kToLogger; 
        }
        handle.seq_ = seq;
        simdjson::ondemand::object msg;
        if(!get_object(obj, "msg", msg)){
            return RouteDecision::kToLogger; 
        }
        std::string_view market_ticker;
        if(!get_string(msg, "market_ticker", market_ticker)){
            return RouteDecision::kToLogger; 
        }
        if(!lookup_route(handle, market_ticker)){
            return RouteDecision::kToLogger;
        }
        if(!check_sequence(handle)){
            return RouteDecision::kToLogger; 
        }
        return RouteDecision::kToShard;
        
        
    }
    bool Router::lookup_route(predex::core::ingest::kalshi::FrameHandle& handle, std::string_view market_ticker) const noexcept{
        MarketRoute route{};
        if(market_registry_.try_lookup(market_ticker, route)){
            handle.affinity_key_ = route.affinity_key_;
            handle.market_id_ = route.market_id_;
            return true;
        }
        return false;
    }
    bool Router::forward_to_logger(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        //best effort to forward to logger, if logger queue is full, we just drop the message
        return logger_queue_.try_push(handle);
    }
    bool Router::check_sequence(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        //check if the message is in order, duplicate, or out
        auto iterator = last_seq_by_sid_.find(handle.sid_);
        if(iterator == last_seq_by_sid_.end()){
            //first time seeing this sid, just insert
            last_seq_by_sid_.emplace(handle.sid_, handle.seq_);
            return true;
        }
        auto last_seq = iterator->second;
        if(handle.seq_ == last_seq + 1){
            //in order, update last seq
            iterator->second = handle.seq_;
            return true;
        }
        //out of order or duplicate, 
        return false;
    }
    std::size_t Router::compute_shard_id(std::uint16_t affinity_key, std::size_t shard_count) noexcept{
        //simple mod based sharding, can be replaced with consistent hashing if needed
        if(shard_count == 0){
            return 0; 
        }
        return affinity_key % shard_count;
    }
    bool Router::process_one() noexcept{
        predex::core::ingest::kalshi::FrameHandle handle{};
        if(!ingress_queue_.try_pop(handle)){
            return false; //no more messages to process
        }
        const auto *const frame = frame_pool_.frame(handle);
        if(frame == nullptr){
            ++telemetry_.dropped_frames_;
            return false; //invalid frame handle, drop it
        }
        auto decision = classify(handle, *frame);
        if(decision == RouteDecision::kToShard){
            auto shard_id = compute_shard_id(handle.affinity_key_, shard_dispatch_.shard_count());
            if(!shard_dispatch_.try_dispatch(shard_id, handle)){
                ++telemetry_.dropped_frames_;
                return forward_to_logger(handle); //failed to dispatch to shard, forward to logger for troubleshooting
            }
        }
        if(decision == RouteDecision::kToLogger){
            if(!forward_to_logger(handle)){
                ++telemetry_.dropped_frames_;
                return false; //failed to forward to logger, drop the message
            }
        }
        if(decision == RouteDecision::kDrop){
            ++telemetry_.dropped_frames_;
            return false; //explicitly classified as drop
        }
        ++telemetry_.processed_frames_;
        return true;
    }
    std::size_t Router::pump(size_t max_batch_size) noexcept{
        size_t processed = 0;
        while(processed < max_batch_size){
            if(!process_one()){
                break; 
            }
            ++processed;
        }
        return processed;
    }
    std::uint64_t Router::monotonic_now_ns() noexcept{
        //TODO check compared to using a lower latency clock source like __rdtsc with cpu frequency calibration
        auto now = std::chrono::steady_clock::now();
        auto epoch = now.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count();
    }
}