#include "predex/router/router.hpp"
#include "predex/ingest/frame_pool.hpp"
#include <chrono>
#include <cstdio>
#include <ctime>
#include <simdjson.h>



namespace predex::core::routing::kalshi{
    namespace{
        void format_utc_timestamp(char* buffer, std::size_t buffer_size) noexcept {
            const std::time_t now = std::time(nullptr);
            std::tm utc_tm{};
#if defined(_WIN32)
            gmtime_s(&utc_tm, &now);
#else
            gmtime_r(&now, &utc_tm);
#endif
            if (std::strftime(buffer, buffer_size, "%Y-%m-%d %H:%M:%S UTC", &utc_tm) == 0U) {
                std::snprintf(buffer, buffer_size, "unknown-time");
            }
        }

        const char* event_type_name(predex::core::ingest::kalshi::KalshiEventType type) noexcept {
            switch (type) {
                case predex::core::ingest::kalshi::KalshiEventType::kTrade:
                    return "trade";
                case predex::core::ingest::kalshi::KalshiEventType::kDelta:
                    return "delta";
                case predex::core::ingest::kalshi::KalshiEventType::kSnapshot:
                    return "snapshot";
                case predex::core::ingest::kalshi::KalshiEventType::kSubscribed:
                    return "subscribed";
                case predex::core::ingest::kalshi::KalshiEventType::kLifecycle:
                    return "lifecycle";
                case predex::core::ingest::kalshi::KalshiEventType::kUnknown:
                default:
                    return "unknown";
            }
        }

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
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& logger_queue,
        predex::utils::SPSCQueue<predex::core::audit::AuditEvent>* audit_queue,
        predex::utils::SPSCQueue<predex::core::ingest::kalshi::FrameHandle>& recycle_queue,
        bool enforce_sequence) noexcept
        : ingress_queue_(ingress_queue),
          frame_pool_(frame_pool),
          market_registry_(market_registry),
          shard_dispatch_(shard_dispatch),
          logger_queue_(logger_queue),
          audit_queue_(audit_queue),
          recycle_queue_(recycle_queue),
          enforce_sequence_(enforce_sequence) {}
    RouteDecision Router::classify(predex::core::ingest::kalshi::FrameHandle& handle, const predex::core::ingest::kalshi::KalshiFrame& frame) noexcept{
        //Framehandle at this point is only stamped with iowriter timestamp,
        //need to extract market ticker, sid, seq, lookup/attach affinity key before we can make routing decision

        if(frame.len_ == 0 || frame.len_ > predex::core::ingest::kalshi::kMaxFrameBytes){
            return RouteDecision::kToLogger; //invalid frame length, forward to logger for troubleshooting
        }

        const char* buf = reinterpret_cast<const char*>(frame.payload.data());
        const size_t len = frame.len_;
        auto doc = parser_.iterate(buf, len,frame.payload.size());
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
        if(obj_type != "orderbook_delta" && obj_type != "trade" &&
           obj_type != "orderbook_snapshot" && obj_type != "market_lifecycle"){
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
        if(obj_type == "market_lifecycle"){
            handle.event_type_ = predex::core::ingest::kalshi::KalshiEventType::kLifecycle;
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
            if (handle.event_type_ == predex::core::ingest::kalshi::KalshiEventType::kLifecycle) {
                return RouteDecision::kDrop;
            }
            return RouteDecision::kToLogger;
        }
        if(enforce_sequence_ &&
           handle.event_type_ != predex::core::ingest::kalshi::KalshiEventType::kLifecycle){
            if(!check_sequence(handle, market_ticker)){
                return RouteDecision::kToLogger;
            }
        }
        return RouteDecision::kToShard;
        
        
    }
    bool Router::lookup_route(predex::core::ingest::kalshi::FrameHandle& handle, std::string_view market_ticker) const noexcept{
        MarketRoute route{};
        if(market_registry_.try_lookup(market_ticker, route)){
            handle.affinity_key_ = route.affinity_key_;
            handle.market_id_ = route.market_id_;
            handle.event_id_ = route.event_id_;
            handle.topology_kind_ = route.topology_kind_;
            return true;
        }
        return false;
    }
    bool Router::forward_to_logger(const predex::core::ingest::kalshi::FrameHandle& handle) noexcept{
        //best effort to forward to logger, if logger queue is full, we just drop the message
        return logger_queue_.try_push(handle);
    }
    void Router::emit_shard_backpressure_audit(
        const predex::core::ingest::kalshi::FrameHandle& handle,
        std::size_t shard_id) noexcept {
        if (audit_queue_ == nullptr) {
            return;
        }
        static_cast<void>(audit_queue_->try_push(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kRouterShardBackpressure,
            .ts_ns = monotonic_now_ns(),
            .shard_id = static_cast<std::uint16_t>(shard_id),
            .frame_seq = handle.seq_,
            .frame_sid = handle.sid_,
            .event_id = handle.event_id_,
            .market_id = handle.market_id_,
            .reject_reason = static_cast<std::uint8_t>(handle.event_type_),
        }));
    }
    bool Router::check_sequence(const predex::core::ingest::kalshi::FrameHandle& handle,
                                std::string_view market_ticker) noexcept{
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
        ++telemetry_.sequence_rejects_;
        if (telemetry_.sequence_rejects_ <= 20U ||
            (telemetry_.sequence_rejects_ % 1000U) == 0U) {
            char time_buf[32];
            format_utc_timestamp(time_buf, sizeof(time_buf));
            std::fprintf(stdout,
                         "[%s] ROUTER | phase=sequence_reject | count=%zu | sid=%u | prev_seq=%llu"
                         " | curr_seq=%llu | event_type=%s | market_ticker=%.*s\n",
                         time_buf,
                         telemetry_.sequence_rejects_,
                         handle.sid_,
                         static_cast<unsigned long long>(last_seq),
                         static_cast<unsigned long long>(handle.seq_),
                         event_type_name(handle.event_type_),
                         static_cast<int>(market_ticker.size()),
                         market_ticker.data());
            std::fflush(stdout);
        }
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
            // Gen mismatch or out-of-range idx — handle does not reference any live frame,
            // so there is nothing to recycle.
            ++telemetry_.dropped_invalid_;
            return false;
        }
        const auto decision = classify(handle, *frame);
        if(decision == RouteDecision::kToShard){
            const auto shard_id = compute_shard_id(handle.affinity_key_, shard_dispatch_.shard_count());
            if(shard_dispatch_.try_dispatch(shard_id, handle)){
                ++telemetry_.processed_frames_;
                return true;
            }
            // Shard queue full; fall back to logger so the frame is still captured for tape.
            if(forward_to_logger(handle)){
                ++telemetry_.shard_backpressure_to_logger_;
                emit_shard_backpressure_audit(handle, shard_id);
                ++telemetry_.processed_frames_;
                return true;
            }
            // Both downstream paths full — recycle the handle so the frame pool does not bleed.
            ++telemetry_.dropped_backpressure_;
            (void)recycle_queue_.try_push(handle);
            return false;
        }
        if(decision == RouteDecision::kToLogger){
            if(forward_to_logger(handle)){
                ++telemetry_.processed_frames_;
                return true;
            }
            ++telemetry_.dropped_backpressure_;
            (void)recycle_queue_.try_push(handle);
            return false;
        }
        // kDrop: explicit benign drop (e.g. lifecycle for unregistered ticker). Recycle and
        // keep pumping so the startup shotgun blast drains quickly.
        ++telemetry_.dropped_unknown_ticker_lifecycle_;
        (void)recycle_queue_.try_push(handle);
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
    void Router::reset_sequence_state() noexcept {
        last_seq_by_sid_.clear();
    }

    std::uint64_t Router::monotonic_now_ns() noexcept{
        //TODO check compared to using a lower latency clock source like __rdtsc with cpu frequency calibration
        auto now = std::chrono::steady_clock::now();
        auto epoch = now.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count();
    }
}
