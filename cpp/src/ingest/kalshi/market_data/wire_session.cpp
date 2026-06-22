#include "predex/ingest/kalshi/market_data/wire_session.hpp"
#include "predex/control/control_types.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <chrono>
#include <cstring>
#include <limits>
#include <thread>

namespace {

    bool read_uint64(simdjson::ondemand::object& object, std::string_view key,
                    std::uint64_t& out) noexcept {
        auto field = object.find_field_unordered(key);
        if (field.error() != simdjson::SUCCESS) {
            return false;
        }
        auto value = field.get_uint64();
        if (value.error() != simdjson::SUCCESS) {
            return false;
        }
        out = value.value();
        return true;
    }

    bool read_int64(simdjson::ondemand::object& object, std::string_view key,
                    std::int64_t& out) noexcept {
        auto field = object.find_field_unordered(key);
        if (field.error() != simdjson::SUCCESS) {
            return false;
        }
        auto value = field.get_int64();
        if (value.error() != simdjson::SUCCESS) {
            return false;
        }
        out = value.value();
        return true;
    }

    bool read_string(simdjson::ondemand::object& object, std::string_view key,
                    std::string_view& out) noexcept {
        auto field = object.find_field_unordered(key);
        if (field.error() != simdjson::SUCCESS) {
            return false;
        }
        auto value = field.get_string();
        if (value.error() != simdjson::SUCCESS) {
            return false;
        }
        out = value.value();
        return true;
    }

    bool read_object(simdjson::ondemand::object& object, std::string_view key,
                    simdjson::ondemand::object& out) noexcept {
        auto field = object.find_field_unordered(key);
        if (field.error() != simdjson::SUCCESS) {
            return false;
        }
        auto value = field.get_object();
        if (value.error() != simdjson::SUCCESS) {
            return false;
        }
        out = value.value();
        return true;
    }


    std::uint64_t monotonic_now_ns() noexcept {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    }

    predex::ingest::kalshi::FrameKind frame_kind_from_type(std::string_view type) noexcept {
        using predex::ingest::kalshi::FrameKind;

        if(type == "orderbook_delta"){
            return FrameKind::kORDERBOOK_DELTA;
        }
        if(type == "orderbook_snapshot"){
            return FrameKind::kORDERBOOK_SNAPSHOT;
        }
        if(type == "trade"){
            return FrameKind::kTRADE;
        }
        if(type == "market_lifecycle" || type == "market_lifecycle_v2" ||
           type == "event_fee_update"){
            return FrameKind::kLIFECYCLE;
        }

        return FrameKind::kUNKNOWN;
    }
} // namespace

namespace predex::ingest::kalshi::market_data{

    void KalshiWireSession::run(const std::stop_token& stop_token){
        while(!stop_token.stop_requested()){
            drain_recycle_queues();
            drain_control_commands();

            if(status_.connected){
                pump_socket_once();
            }
            else{
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }
        }

        disconnect("wire session stopped");
        drain_recycle_queues();
    }

    bool KalshiWireSession::pop_control_command(core::control::ControlToIoCommand& cmd_out) noexcept{
        return control_queues_.control_to_io_queue.try_pop(cmd_out);
    }

    bool KalshiWireSession::push_control_status(core::control::IoToControlStatus status) noexcept{
        return control_queues_.io_to_control_status_queue.try_push(std::move(status));
    }

    void KalshiWireSession::drain_control_commands(){
        core::control::ControlToIoCommand cmd;
        while(pop_control_command(cmd)){
            handle_control_command(cmd);
        }
    }

    void KalshiWireSession::handle_control_command(const core::control::ControlToIoCommand& cmd){

        std::visit([&](auto&& cmd){
            using T = std::decay_t<decltype(cmd)>;
            if constexpr(std::is_same_v<T, core::control::ApplyUniverseSnapshotIo>){
                apply_universe_snapshot(cmd.snapshot);
            }
            else if constexpr(std::is_same_v<T, core::control::ConnectIo>){
                (void)connect();
            }
            else if constexpr(std::is_same_v<T, core::control::DisconnectIo>){
                disconnect();
            }
            else if constexpr(std::is_same_v<T, core::control::RecoverMarketIo>){
            }
        }, cmd);
    }

    void KalshiWireSession::apply_universe_snapshot(const std::shared_ptr<const core::control::UniverseSnapshot>& snapshot){
        
        if(snapshot == nullptr){
            report_fault("Received null universe snapshot");
            return;
        }
        
        desired_universe_ = snapshot;
        market_route_by_ticker_.clear();
        market_route_by_id_.clear();

        for(const auto& market_route : snapshot->market_routes){
            market_route_by_ticker_.emplace(market_route.kalshi_ticker, market_route);
            market_route_by_id_.emplace(market_route.market_id, market_route);
        }

        (void)push_control_status(core::control::IoUniverseSnapshotApplied{.version = desired_universe_->version});
        if(status_.connected && active_subscriptions_.empty()){
            (void)subscribe_active_universe();
        }
    }

    bool KalshiWireSession::connect(){
        
        if(ws_session_.connect()){
            status_.connected = true;
            status_.last_error.clear();
            clear_transport_subscription_state();
            (void)push_control_status(core::control::IoConnected{});
            if(desired_universe_ != nullptr){
                (void)subscribe_active_universe();
            }
            return true;
        }
        
        status_.connected = false;
        status_.last_error = ws_session_.last_error();

        report_fault("Failed to connect: " + status_.last_error);
        
        return false;
        
    }

    void KalshiWireSession::report_fault(std::string error_message){
        status_.connected = false;
        status_.last_error = std::move(error_message);
        clear_transport_subscription_state();
        (void)push_control_status(core::control::IoFaulted{.error_message = status_.last_error});
    }

    void KalshiWireSession::clear_transport_subscription_state(){
        pending_ws_commands_.clear();
        active_subscriptions_.clear();
    }

    bool KalshiWireSession::subscribe_active_universe(){
        if(desired_universe_ == nullptr){
            report_fault("Cannot subscribe to universe - no snapshot applied");
            return false;
        }
        bool all_subscriptions_sent = true;
        for(const auto& channel : desired_channels_){
            std::vector<std::string> tickers;
            for(const auto& market_route : desired_universe_->market_routes){
                tickers.push_back(market_route.kalshi_ticker);
            }
            if(!subscribe_channel(channel, tickers)){
                all_subscriptions_sent = false;
                report_fault("Failed to subscribe to channel " + std::to_string(static_cast<std::uint8_t>(channel)));
            }
        }
        return all_subscriptions_sent;
    }

    bool KalshiWireSession::subscribe_channel(exchange::kalshi::KalshiMarketDataChannel channel, std::span<const std::string> tickers){
        const auto ws_command_id = next_ws_command_id();
        pending_ws_commands_.emplace(ws_command_id, PendingWsCommand{
            .ws_command_id = ws_command_id,
            .kind = WsCommandKind::kSUBSCRIBE,
            .channel = channel,
            .market_ids = {},//entire universe so probably don't want to explode it by ticker
        });
        auto& subscription = active_subscriptions_[channel];
        subscription.channel = channel;
        subscription.phase = SubscriptionPhase::kSUBSCRIBE_PENDING;
        subscription.sid = std::nullopt;
        subscription.market_ids.clear();
        subscription.last_error.clear();

        if(!ws_session_.send_text(market_data_handler_.build_subscribe_message(ws_command_id, channel, tickers))){
            pending_ws_commands_.erase(ws_command_id);
            subscription.phase = SubscriptionPhase::kFAULTED;
            subscription.last_error = std::string{ws_session_.last_error()};
            return false;
        }

        return true;
    }

    bool KalshiWireSession::add_markets(exchange::kalshi::KalshiMarketDataChannel channel, std::span<const std::string> tickers){
        auto sub_it = active_subscriptions_.find(channel);
        if(sub_it == active_subscriptions_.end() || !sub_it->second.sid.has_value()){
            report_fault("Cannot add markets to channel - not currently subscribed: " + std::to_string(static_cast<std::uint8_t>(channel)));
            return false;
        }

        std::vector<core::control::MarketId> market_ids;
        for(const auto& ticker : tickers){
            const auto iterator = market_route_by_ticker_.find(ticker);
            if(iterator != market_route_by_ticker_.end()){
                market_ids.push_back(iterator->second.market_id);
            }
            else{
                report_fault("Cannot add market - ticker not found in universe: " + ticker);
                return false;
            }
        }
        const auto ws_command_id = next_ws_command_id();
        pending_ws_commands_.emplace(ws_command_id, PendingWsCommand{
            .ws_command_id = ws_command_id,
            .kind = WsCommandKind::kADD_MARKETS,
            .channel = channel,
            .market_ids = market_ids,
        });
        const auto sid = sub_it->second.sid.value();
        sub_it->second.phase = SubscriptionPhase::kUPDATE_PENDING;

        if(!ws_session_.send_text(market_data_handler_.build_update_message(ws_command_id, sid, tickers, "add_markets"))){
            pending_ws_commands_.erase(ws_command_id);
            sub_it->second.phase = SubscriptionPhase::kSUBSCRIBED;
            sub_it->second.last_error = std::string{ws_session_.last_error()};
            return false;
        }

        return true;
    }

    bool KalshiWireSession::delete_markets(exchange::kalshi::KalshiMarketDataChannel channel, std::span<const std::string> tickers){
        auto sub_it = active_subscriptions_.find(channel);
        if(sub_it == active_subscriptions_.end() || !sub_it->second.sid.has_value()){
            report_fault("Cannot delete markets from channel - not currently subscribed: " + std::to_string(static_cast<std::uint8_t>(channel)));
            return false;
        }

        std::vector<core::control::MarketId> market_ids;
        for(const auto& ticker : tickers){
            const auto iterator = market_route_by_ticker_.find(ticker);
            if(iterator != market_route_by_ticker_.end()){
                market_ids.push_back(iterator->second.market_id);
            }
            else{
                report_fault("Cannot delete market - ticker not found in universe: " + ticker);
                return false;
            }
        }
        const auto ws_command_id = next_ws_command_id();
        pending_ws_commands_.emplace(ws_command_id, PendingWsCommand{
            .ws_command_id = ws_command_id,
            .kind = WsCommandKind::kDELETE_MARKETS,
            .channel = channel,
            .market_ids = market_ids,
        });
        const auto sid = sub_it->second.sid.value();
        sub_it->second.phase = SubscriptionPhase::kUPDATE_PENDING;

        if(!ws_session_.send_text(market_data_handler_.build_update_message(ws_command_id, sid, tickers, "delete_markets"))){
            pending_ws_commands_.erase(ws_command_id);
            sub_it->second.phase = SubscriptionPhase::kSUBSCRIBED;
            sub_it->second.last_error = std::string{ws_session_.last_error()};
            return false;
        }

        return true;
    }
//NOLINTNEXTLINE
    void KalshiWireSession::handle_ws_control_response(std::span<const std::byte> payload){
        if(payload.empty()){
            return;
        }

        const auto* data = reinterpret_cast<const char*>(payload.data());
        simdjson::padded_string json{std::string_view{data, payload.size()}};

        auto doc_result = control_response_parser_.iterate(json);
        if(doc_result.error() != simdjson::SUCCESS){
            report_fault("Failed to parse websocket control response");
            return;
        }

        auto root_result = doc_result.get_object();
        if(root_result.error() != simdjson::SUCCESS){
            report_fault("Websocket control response was not a JSON object");
            return;
        }

        simdjson::ondemand::object root = root_result.value();
        std::uint64_t ws_command_id{};
        if(!read_uint64(root, "id", ws_command_id)){
            return;
        }

        auto pending_it = pending_ws_commands_.find(ws_command_id);
        if(pending_it == pending_ws_commands_.end()){
            return;
        }

        PendingWsCommand pending = std::move(pending_it->second);
        pending_ws_commands_.erase(pending_it);

        std::string_view response_type;
        if(!read_string(root, "type", response_type)){
            report_fault("Websocket control response missing type");
            return;
        }

        auto& subscription = active_subscriptions_[pending.channel];
        subscription.channel = pending.channel;

        if(response_type == "error"){
            subscription.phase = SubscriptionPhase::kFAULTED;
            subscription.last_error = "Kalshi websocket command failed";
            report_fault(subscription.last_error);
            return;
        }

        switch(pending.kind){
            case WsCommandKind::kSUBSCRIBE: {
                simdjson::ondemand::object msg;
                std::int64_t sid{};
                if(!read_object(root, "msg", msg) || !read_int64(msg, "sid", sid)){
                    subscription.phase = SubscriptionPhase::kFAULTED;
                    subscription.last_error = "Subscribed response missing sid";
                    report_fault(subscription.last_error);
                    return;
                }

                subscription.sid = sid;
                subscription.phase = SubscriptionPhase::kSUBSCRIBED;
                subscription.market_ids.clear();
                subscription.last_error.clear();
                if(desired_universe_ != nullptr){
                    for(const auto& route : desired_universe_->market_routes){
                        subscription.market_ids.insert(route.market_id);
                    }
                }

                bool all_channels_subscribed = desired_universe_ != nullptr;
                for(const auto channel : desired_channels_){
                    const auto iter = active_subscriptions_.find(channel);
                    if(iter == active_subscriptions_.end() ||
                       iter->second.phase != SubscriptionPhase::kSUBSCRIBED ||
                       !iter->second.sid.has_value()){
                        all_channels_subscribed = false;
                        break;
                    }
                }
                if(all_channels_subscribed){
                    (void)push_control_status(core::control::IoSubscriptionReady{
                        .version = desired_universe_->version,
                    });
                }
                break;
            }
            case WsCommandKind::kADD_MARKETS:
                for(const auto market_id : pending.market_ids){
                    subscription.market_ids.insert(market_id);
                }
                subscription.phase = SubscriptionPhase::kSUBSCRIBED;
                subscription.last_error.clear();
                break;
            case WsCommandKind::kDELETE_MARKETS:
                for(const auto market_id : pending.market_ids){
                    subscription.market_ids.erase(market_id);
                }
                subscription.phase = SubscriptionPhase::kSUBSCRIBED;
                subscription.last_error.clear();
                break;
            case WsCommandKind::kUNSUBSCRIBE:
                subscription.sid = std::nullopt;
                subscription.market_ids.clear();
                subscription.phase = SubscriptionPhase::kIDLE;
                subscription.last_error.clear();
                break;
        }

    }

    void KalshiWireSession::publish_market_data_frame(std::span<const std::byte> payload){
        if(payload.size() > predex::ingest::kalshi::kMaxFrameBytes){
            return;
        }

        predex::ingest::kalshi::FrameHandle handle{};
        if(!frame_pool_.try_acquire(handle)){
            drain_recycle_queues();
            if(!frame_pool_.try_acquire(handle)){
                return;
            }
        }

        auto* frame = frame_pool_.writable_frame(handle);
        if(frame == nullptr){
            (void)frame_pool_.recycle(handle);
            return;
        }

        frame->recv_ts_ns = monotonic_now_ns();
        frame->len = static_cast<std::uint32_t>(payload.size());
        frame->flags = 0;
        std::memcpy(frame->payload.data(), payload.data(), payload.size());

        MarketDataEnvelope envelope{};
        if(!parse_market_data_envelope(*frame, envelope)){
            (void)frame_pool_.recycle(handle);
            return;
        }

        if(!stamp_handle_from_envelope(handle, envelope)){
            (void)frame_pool_.recycle(handle);
            return;
        }

        if(!router_queue_.try_push(handle)){
            (void)frame_pool_.recycle(handle);
        }
    }
    
    void KalshiWireSession::drain_recycle_queues(){
        std::size_t total_recycled = 0;

        if(recycle_queues_.empty()){
            return;
        }
        std::size_t empty_queues = 0;
        while(total_recycled < max_recycle_batch_size_ && empty_queues < recycle_queues_.size()){
            auto* queue = recycle_queues_[next_recycle_queue_idx_];
            next_recycle_queue_idx_ = (next_recycle_queue_idx_ + 1) % recycle_queues_.size();
            
            if(queue == nullptr){
                ++empty_queues;
                continue;
            }

            predex::ingest::kalshi::FrameHandle handle{};
            if(!queue->try_pop(handle)){
                ++empty_queues;
                continue;
            }
            empty_queues = 0;
            if(frame_pool_.recycle(handle)){
                ++total_recycled;
            }
            else{
                //failure to recycle frame back into pool, will leak the frame/handle 
            }
        } 
    }

    void KalshiWireSession::pump_socket_once(){
        using predex::exchange::kalshi::ReadStatus;

        auto read_result = ws_session_.recv_text(std::chrono::milliseconds{1});
        switch(read_result.status){
            case ReadStatus::kTimeout:
                return;
            case ReadStatus::kClosed:
                status_.connected = false;
                status_.last_error = "websocket closed";
                clear_transport_subscription_state();
                (void)push_control_status(core::control::IoDisconnected{
                    .reason = status_.last_error,
                });
                return;
            case ReadStatus::kError:
                report_fault("websocket receive failed: " + std::string{ws_session_.last_error()});
                return;
            case ReadStatus::kMessage:
                break;
        }

        switch(classify_incoming_message(read_result.payload)){
            case IncomingMessageKind::kCONTROL_RESPONSE:
                handle_ws_control_response(read_result.payload);
                break;
            case IncomingMessageKind::kMARKET_DATA:
                publish_market_data_frame(read_result.payload);
                break;
            case IncomingMessageKind::kIGNORE:
            case IncomingMessageKind::kMALFORMED:
                break;
        }
        
    }

    IncomingMessageKind KalshiWireSession::classify_incoming_message(std::span<const std::byte> payload){
        if(payload.empty()){
            return IncomingMessageKind::kIGNORE;
        }

        const auto* data = reinterpret_cast<const char*>(payload.data());
        simdjson::padded_string json{std::string_view{data, payload.size()}};

        auto doc_result = message_classifier_parser_.iterate(json);
        if(doc_result.error() != simdjson::SUCCESS){
            return IncomingMessageKind::kMALFORMED;
        }

        auto root_result = doc_result.get_object();
        if(root_result.error() != simdjson::SUCCESS){
            return IncomingMessageKind::kMALFORMED;
        }

        simdjson::ondemand::object root = root_result.value();
        std::uint64_t ws_command_id{};
        if(read_uint64(root, "id", ws_command_id) &&
           pending_ws_commands_.find(ws_command_id) != pending_ws_commands_.end()){
            return IncomingMessageKind::kCONTROL_RESPONSE;
        }

        std::string_view type;
        if(!read_string(root, "type", type)){
            return IncomingMessageKind::kIGNORE;
        }

        if(type == "orderbook_delta" || type == "orderbook_snapshot" ||
           type == "trade" || type == "market_lifecycle" ||
           type == "market_lifecycle_v2" || type == "event_fee_update"){
            return IncomingMessageKind::kMARKET_DATA;
        }

        return IncomingMessageKind::kIGNORE;
    }

    bool KalshiWireSession::parse_market_data_envelope(const KalshiFrame& frame,
                                                       MarketDataEnvelope& envelope_out){
        envelope_out = MarketDataEnvelope{};

        const auto* data = reinterpret_cast<const char*>(frame.payload.data());
        simdjson::padded_string_view json{data, frame.len, frame.payload.size()};

        auto doc_result = market_data_envelope_parser_.iterate(json);
        if(doc_result.error() != simdjson::SUCCESS){return false;}
        auto root_result = doc_result.get_object();
        if(root_result.error() != simdjson::SUCCESS){return false;}
        simdjson::ondemand::object root = root_result.value();

        std::uint64_t session_id_out{};
        if(!read_uint64(root, "sid", session_id_out) ||
           session_id_out > std::numeric_limits<std::uint32_t>::max()){
            return false;
        }
        envelope_out.sid = static_cast<std::uint32_t>(session_id_out);

        std::string_view type_out;
        if(!read_string(root, "type", type_out)){
            return false;
        }
        envelope_out.kind = frame_kind_from_type(type_out);
        if(envelope_out.kind == FrameKind::kUNKNOWN){
            return false;
        }

        std::uint64_t sequence_out{};
        if(!read_uint64(root, "seq", sequence_out)){
            return false;
        }
        envelope_out.sequence = sequence_out;

        simdjson::ondemand::object msg;
        if(!read_object(root, "msg", msg)){
            return false;
        }

        std::string_view market_ticker_out;
        if(!read_string(msg, "market_ticker", market_ticker_out)){
            return false;
        }
        envelope_out.market_ticker = market_ticker_out;

        return true;
    }

    bool KalshiWireSession::stamp_handle_from_envelope(FrameHandle& handle,
                                                       const MarketDataEnvelope& envelope){
        if(envelope.kind == FrameKind::kUNKNOWN || envelope.market_ticker.empty()){
            return false;
        }

        if(!is_active_sid(envelope.sid)){
            return false;
        }

        const auto route_it = market_route_by_ticker_.find(std::string{envelope.market_ticker});
        if(route_it == market_route_by_ticker_.end()){
            return false;
        }

        const auto& route = route_it->second;
        handle.universe_version = desired_universe_ != nullptr ? desired_universe_->version : 0;
        handle.sequence = envelope.sequence;
        handle.sid = envelope.sid;
        handle.market_id = route.market_id;
        handle.event_id = route.event_id;
        handle.affinity_key = route.affinity_key;
        handle.topology = route.topology;
        handle.shard_index = route.shard_index;
        handle.shard_event_index = route.shard_event_index;
        handle.event_market_index = route.event_market_index;
        handle.kind = envelope.kind;

        return true;
    }

    bool KalshiWireSession::is_active_sid(std::uint32_t sid) const noexcept{
        //NOLINTNEXTLINE -- future work change to std::range
        for(const auto& [_, subscription] : active_subscriptions_){
            if(subscription.phase == SubscriptionPhase::kSUBSCRIBED &&
               subscription.sid.has_value() &&
               *subscription.sid >= 0 &&
               *subscription.sid <= std::numeric_limits<std::uint32_t>::max() &&
               static_cast<std::uint32_t>(*subscription.sid) == sid){
                return true;
            }
        }
        return false;
    }

    bool KalshiWireSession::unsubscribe_channel(exchange::kalshi::KalshiMarketDataChannel channel){
        auto sub_it = active_subscriptions_.find(channel);
        if(sub_it == active_subscriptions_.end() || !sub_it->second.sid.has_value()){
            report_fault("Cannot unsubscribe from channel - not currently subscribed: " + std::to_string(static_cast<std::uint8_t>(channel)));
            return false;
        }

        const std::int64_t sid = sub_it->second.sid.value();
        const auto ws_command_id = next_ws_command_id();
        pending_ws_commands_.emplace(ws_command_id, PendingWsCommand{
            .ws_command_id = ws_command_id,
            .kind = WsCommandKind::kUNSUBSCRIBE,
            .channel = channel,
            .market_ids = {},
        });
        sub_it->second.phase = SubscriptionPhase::kUNSUBSCRIBE_PENDING;
        //NOTE: might change build_unsubscribe_message to just take a single sid instead of a span since it's only ever called one channel at a time here.
        if(!ws_session_.send_text(market_data_handler_.build_unsubscribe_message(ws_command_id, {{sid}}))){
            pending_ws_commands_.erase(ws_command_id);
            sub_it->second.phase = SubscriptionPhase::kSUBSCRIBED;
            sub_it->second.last_error = std::string{ws_session_.last_error()};
            return false;
        }

        return true;
    }

    void KalshiWireSession::disconnect(std::string reason){
        if(status_.connected){
            ws_session_.close();
        }
        status_.connected = false;
        status_.last_error = std::move(reason);
        clear_transport_subscription_state();
        (void)push_control_status(core::control::IoDisconnected{
            .reason = status_.last_error,
        });
    }
}
