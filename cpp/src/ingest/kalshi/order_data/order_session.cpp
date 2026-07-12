#include "predex/ingest/kalshi/order_data/order_session.hpp"
#include "predex/control/control_types.hpp"
#include "predex/ingest/kalshi/order_data/order_parser.hpp"

#include <thread>

namespace {
std::uint64_t now_ns() noexcept {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}
}

namespace predex::ingest::kalshi::order_data{
    bool KalshiOrderSession::defer_oms_event(oms::KalshiToOmsEvent event) noexcept{
        if(pending_oms_count_ < pending_oms_events_.size()){
            pending_oms_events_[pending_oms_head_] = std::move(event);
            pending_oms_head_ = (pending_oms_head_ + 1) % pending_oms_events_.size();
            ++pending_oms_count_;
            return true;
        }
        return false;
    }

    bool KalshiOrderSession::emit_or_defer_oms_event(oms::KalshiToOmsEvent event) noexcept{
        if(pending_oms_count_ == 0 && oms_queues_.private_ws_to_oms_queue.try_push(std::move(event))){
            return true;
        }
        if(defer_oms_event(std::move(event))){
            return true;
        }
        ++telemetry_.oms_enqueue_failures;
        status_.last_error = "private order feed OMS egress buffer exhausted";
        (void)push_control_status(core::control::PrivateOrderFeedFaulted{
            .error_message = status_.last_error
        });
        // if we can't push to OMS, we need to close the websocket session to avoid further events and to signal to control that we are faulted
        ws_session_.close();
        status_.connected = false;
        return false;
    }

    void KalshiOrderSession::flush_deferred_oms_events() noexcept{
        while(pending_oms_count_ > 0 && oms_queues_.private_ws_to_oms_queue.try_push(std::move(pending_oms_events_[pending_oms_tail_]))){
            pending_oms_tail_ = (pending_oms_tail_ + 1) % pending_oms_events_.size();
            --pending_oms_count_;
        }
    }

    void KalshiOrderSession::apply_universe_snapshot(const std::shared_ptr<const core::control::OrderRouteUniverse>& snapshot){
        if(snapshot == nullptr){
            status_.last_error = "received null order route universe snapshot";
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{.error_message = status_.last_error});
            return;
        }

        desired_universe_ = snapshot;
        status_.installed_universe_version = snapshot->version;
        if(status_.subscribed_universe_version != snapshot->version){
            status_.subscribed_universe_version = 0;
        }
        route_by_ticker_.clear();

        for(const auto& market_route : snapshot->market_routes){
            route_by_ticker_.emplace(market_route.kalshi_ticker, market_route);
        }

        (void)push_control_status(core::control::PrivateOrderFeedUniverseApplied{.version = desired_universe_->version});
    }

    bool KalshiOrderSession::stamp_market_route(ParsedOrderMessage& parsed) noexcept{
        if(desired_universe_ == nullptr){
            return false;
        }
        const auto iter = route_by_ticker_.find(parsed.market_ticker());
        if(iter == route_by_ticker_.end()){
            parsed.order_event.market_id = 0;/* Current Market not in this universe but OMS might be able to match leave as zero*/
            return true;
        }
        parsed.order_event.market_id = iter->second.market_id;
        return true;
    }

    bool KalshiOrderSession::push_control_status(core::control::PrivateOrderFeedToControlStatus status) noexcept{
        return control_queues_.order_session_to_control_queue.try_push(std::move(status));
    }

    bool KalshiOrderSession::connect(){
        if(ws_session_.connect()){
            status_.connected = true;
            status_.last_error.clear();

            clear_transport_subscription_state();
            
            if(desired_universe_ != nullptr){
                (void)subscribe_active_universe();
            }
            
            (void)push_control_status(core::control::PrivateOrderFeedConnected{});
            
            return true;
        }

        status_.connected = false;
        status_.last_error = ws_session_.last_error();

        (void)push_control_status(core::control::PrivateOrderFeedDisconnected{
            .reason = status_.last_error
        });

        return false;
    }

    void KalshiOrderSession::clear_transport_subscription_state(){
        active_subscriptions_.clear();
        pending_ws_commands_.clear();
        status_.subscribed_universe_version = 0;
    }

    bool KalshiOrderSession::subscribe_active_universe(){
        if(desired_universe_ == nullptr){
            status_.last_error = "cannot subscribe to universe - no snapshot applied";
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{
                .error_message = status_.last_error
            });
            return false;
        }
        if(!status_.connected){
            status_.last_error = "cannot subscribe to universe - not connected";
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{
                .error_message = status_.last_error
            });
            return false;
        }
        for(const auto& channel : desired_channels_){ //NOLINT
            if(!subscribe_channel(channel)){
                return false;
            }
        }
        return true;
    }

    bool KalshiOrderSession::subscribe_channel(exchange::kalshi::KalshiOrderDataChannel channel){
        if(active_subscriptions_.find(channel) != active_subscriptions_.end()){
            status_.last_error = "cannot subscribe to channel - already subscribed: " + std::to_string(static_cast<std::uint8_t>(channel));
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{
                .error_message = status_.last_error
            });
            return false;
        }
        const auto ws_command_id = next_ws_command_id();
        const std::span<const std::string> ticker_filter{};//empty for no filter

        const auto msg = order_data_handler_.build_subscribe_message(ws_command_id, channel, ticker_filter);

        if (!ws_session_.send_text(msg)) {
            status_.last_error = std::string{"subscribe send failed: "} + std::string(ws_session_.last_error());
            active_subscriptions_[channel] = ActiveOrderSubscription{
                .channel = channel,
                .phase = OrderSubscriptionPhase::kFAULTED,
                .last_error = status_.last_error,
            };
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{.error_message = status_.last_error});
            return false;
        }
        pending_ws_commands_[ws_command_id] = PendingOrderWsCommand{
            .ws_command_id = ws_command_id,
            .kind = OrderWsCommandKind::kSUBSCRIBE,
            .channel = channel,
            .universe_version = status_.installed_universe_version
        };
        active_subscriptions_[channel] = ActiveOrderSubscription{
            .channel = channel,
            .phase = OrderSubscriptionPhase::kSUBSCRIBE_PENDING,
            .last_error = {},
        };

        return true;
    }

    bool KalshiOrderSession::unsubscribe_channel(exchange::kalshi::KalshiOrderDataChannel channel){
        auto sub_it = active_subscriptions_.find(channel);
        if(sub_it == active_subscriptions_.end() || !sub_it->second.sid.has_value()){
            status_.last_error = "cannot unsubscribe from channel - not currently subscribed: " + std::to_string(static_cast<std::uint8_t>(channel));
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{
                .error_message = status_.last_error
            });
            return false;
        }

        const auto ws_command_id = next_ws_command_id();
        const std::span<const std::int64_t> session_ids{&sub_it->second.sid.value(), 1};

        const auto msg = order_data_handler_.build_unsubscribe_message(ws_command_id, sub_it->second.channel, session_ids);
        if (!ws_session_.send_text(msg)) {
            status_.last_error = std::string{"unsubscribe send failed: "} + std::string(ws_session_.last_error());
            sub_it->second.phase = OrderSubscriptionPhase::kFAULTED;
            sub_it->second.last_error = status_.last_error;
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{.error_message = status_.last_error});
            return false;
        }
        pending_ws_commands_[ws_command_id] = PendingOrderWsCommand{
            .ws_command_id = ws_command_id,
            .kind = OrderWsCommandKind::kUNSUBSCRIBE,
            .channel = sub_it->second.channel,
            .universe_version = status_.installed_universe_version
        };
        sub_it->second.phase = OrderSubscriptionPhase::kUNSUBSCRIBE_PENDING;

        return true;
    }

    void KalshiOrderSession::pump_socket_once() noexcept{
        using predex::exchange::kalshi::ReadStatus;

        auto read_result = ws_session_.recv_text(std::chrono::milliseconds{1});
        switch(read_result.status){
            case ReadStatus::kTimeout:
                return;
            case ReadStatus::kClosed:
                disconnect("websocket closed");
                return;
            case ReadStatus::kError:
                status_.last_error = "private order feed receive failed: " + std::string{ws_session_.last_error()};
                clear_transport_subscription_state();
                status_.connected = false;
                (void)push_control_status(core::control::PrivateOrderFeedFaulted{.error_message = status_.last_error});
                return;
            case ReadStatus::kMessage:
                break;
        }
        ++telemetry_.messages_received;

        ParsedOrderMessage parsed{};
        const auto code = order_parser_.parse_message(read_result.payload, parsed);
        parsed.parse_code = code;

        if (code == OrderParseCode::kINVALID_JSON || 
            code == OrderParseCode::kMISSING_FIELD ||
            code == OrderParseCode::kUNKNOWN_ORDER_STATE ||
            code == OrderParseCode::kINVALID_ORDER_ID){
            ++telemetry_.parse_failures;
            ++telemetry_.messages_dropped;
            return;
        }

        if (code == OrderParseCode::kIGNORE){
            parsed.kind = OrderIncomingMessageKind::kIGNORE;
            ++telemetry_.messages_decoded;
            return;
        }
        switch(parsed.kind){
            case OrderIncomingMessageKind::kCONTROL_RESPONSE:
                handle_ws_control_response(parsed);
                break;
            case OrderIncomingMessageKind::kORDER_DATA:
                handle_order_event(parsed);
                break;
            case OrderIncomingMessageKind::kIGNORE:
            case OrderIncomingMessageKind::kMALFORMED:
                ++telemetry_.parse_failures;
                ++telemetry_.messages_dropped;
                break;
        }
    }

    void KalshiOrderSession::handle_ws_control_response(const ParsedOrderMessage& parsed) noexcept{
        using predex::ingest::kalshi::order_data::OrderWsCommandKind;
        if(parsed.kind != OrderIncomingMessageKind::kCONTROL_RESPONSE){
            return;
        }
        auto pending_iter = pending_ws_commands_.find(parsed.control_response.request_id);
        if(pending_iter == pending_ws_commands_.end()){
            // unknown request id - could be a stale ack from a previous universe version, ignore
            return;
        }
        auto pending = pending_iter->second;
        pending_ws_commands_.erase(pending_iter);

        if(pending.universe_version != status_.installed_universe_version){
            return; // stale ack 
        }
        auto& sub = active_subscriptions_[pending.channel];
        sub.channel = pending.channel;

        if(!parsed.control_response.success){
            sub.phase = OrderSubscriptionPhase::kFAULTED;
            sub.last_error = parsed.control_response.error_message;
            status_.last_error = "control response error: " + parsed.control_response.error_message;
            (void)push_control_status(core::control::PrivateOrderFeedFaulted{.error_message = status_.last_error});
            return;
        }


        switch(pending.kind){
            case OrderWsCommandKind::kSUBSCRIBE:{
                if (!parsed.control_response.session_id.has_value()){
                    sub.phase = OrderSubscriptionPhase::kFAULTED;
                    sub.last_error = "private order subscribe ack missing session id";
                    status_.last_error = sub.last_error;
                    (void)push_control_status(core::control::PrivateOrderFeedFaulted{
                        .error_message = status_.last_error
                    });
                    return;
                }
                sub.sid = parsed.control_response.session_id;
                sub.phase = OrderSubscriptionPhase::kSUBSCRIBED;
                sub.last_error.clear();

                bool all_channels_subscribed = (desired_universe_ != nullptr && !desired_channels_.empty());

                for (const auto channel : desired_channels_){
                    const auto iter = active_subscriptions_.find(channel);
                    if(iter == active_subscriptions_.end() || iter->second.phase != OrderSubscriptionPhase::kSUBSCRIBED || !iter->second.sid.has_value()){
                        all_channels_subscribed = false;
                        break;
                    }
                }
                if (all_channels_subscribed){
                    status_.subscribed_universe_version = desired_universe_->version;
                    (void)push_control_status(core::control::PrivateOrderFeedSubscriptionReady{
                        .version = desired_universe_->version
                    });
                }
                break;
            }
            case OrderWsCommandKind::kUNSUBSCRIBE:{
                sub.sid.reset();
                sub.phase = OrderSubscriptionPhase::kIDLE;
                sub.last_error.clear();
                bool all_channels_unsubscribed = true;
                for (const auto& [channel, sub] : active_subscriptions_) {
                    (void)channel;
                    if (sub.phase == OrderSubscriptionPhase::kSUBSCRIBED ||
                        sub.phase == OrderSubscriptionPhase::kSUBSCRIBE_PENDING ||
                        sub.phase == OrderSubscriptionPhase::kUNSUBSCRIBE_PENDING ||
                        sub.sid.has_value()) {
                        all_channels_unsubscribed = false;
                        break;
                    }
                }
                if (all_channels_unsubscribed) {
                    status_.subscribed_universe_version = 0;
                }
                break;
            }
        }
    }

    void KalshiOrderSession::handle_order_event(ParsedOrderMessage& parsed) noexcept {
        if (parsed.kind != OrderIncomingMessageKind::kORDER_DATA) {
            return;
        }

        if (parsed.order_event.event_kind == oms::PrivateWsOrderEventKind::kMARKET_POSITION) {
            ++telemetry_.messages_decoded;
            return;
        }

        if (!stamp_market_route(parsed)) {
            ++telemetry_.messages_dropped;
            return;
        }

        parsed.order_event.recv_ts_ns = now_ns();

        if (!emit_or_defer_oms_event(oms::KalshiToOmsEvent{parsed.order_event})) {
            ++telemetry_.messages_dropped;
            return;
        }

        ++telemetry_.messages_decoded;
    }

    void KalshiOrderSession::disconnect(std::string reason){
        if(status_.connected){
            ws_session_.close();
        }
        status_.connected = false;
        status_.last_error = std::move(reason);
        clear_transport_subscription_state();
        (void)push_control_status(core::control::PrivateOrderFeedDisconnected{
            .reason = status_.last_error,
        });
    }

    void KalshiOrderSession::maybe_send_telemetry() noexcept {
        const auto now = std::chrono::steady_clock::now();
        if (now < next_telemetry_send_) {
            return;
        }

        (void)push_control_status(core::control::PrivateOrderFeedTelemetry{
            .telemetry = telemetry_,
        });
        next_telemetry_send_ = now + kPRIVATE_ORDER_FEED_TELEMETRY_INTERVAL;
    }

    void KalshiOrderSession::drain_control_commands() noexcept{
        core::control::ControlToPrivateOrderFeedCommand cmd{};
        while(control_queues_.control_to_order_session_queue.try_pop(cmd)){
            handle_control_command(cmd);
        }
    }

    void KalshiOrderSession::handle_control_command(const core::control::ControlToPrivateOrderFeedCommand& cmd){
        std::visit([&](auto&& cmd){
            using T = std::decay_t<decltype(cmd)>;
            if constexpr(std::is_same_v<T, core::control::ApplyOrderRouteUniverse>){
                apply_universe_snapshot(cmd.snapshot);
                if(status_.connected){
                    clear_transport_subscription_state();
                    (void)subscribe_active_universe();
                }
            }
            else if constexpr(std::is_same_v<T, core::control::ConnectPrivateOrderFeed>){
                (void)connect();
            }
            else if constexpr(std::is_same_v<T, core::control::DisconnectPrivateOrderFeed>){
                disconnect();
            }
        }, cmd);
    }

    void KalshiOrderSession::run(const std::stop_token& stop_token){
        while(!stop_token.stop_requested()){
            flush_deferred_oms_events();
            drain_control_commands();

            if(status_.connected){
                pump_socket_once();
            }
            else{
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }
            maybe_send_telemetry();
        }
        disconnect("private order feed session stopped");
        flush_deferred_oms_events();
        maybe_send_telemetry();
    }


}