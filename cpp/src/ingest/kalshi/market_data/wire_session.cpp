#include "predex/ingest/kalshi/market_data/wire_session.hpp"
#include "predex/control/control_types.hpp"
#include "predex/utils/latency_histogram.hpp"
#include "predex/utils/monotonic_clock.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <chrono>
#include <cstring>
#include <limits>
#include <thread>
#include <algorithm>

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
        while (!stop_token.stop_requested()) {
            drain_recycle_queues();

            bool recovery_statuses_flushed = flush_pending_recovery_statuses();

            if (recovery_statuses_flushed) {
                drain_control_commands();

                recovery_statuses_flushed = flush_pending_recovery_statuses();
            }

            const bool integrity_barrier_flushed = flush_pending_integrity_barrier();

            const bool may_consume_message =
                recovery_statuses_flushed
                && integrity_barrier_flushed;

            bool transport_progressed = false;

            if (status_.connected) {
                transport_progressed = service_transport_once(may_consume_message);
            }
            if (!transport_progressed) {
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }

            maybe_send_telemetry();
        }

        disconnect("wire session stopped");

        drain_recycle_queues();
        (void)flush_pending_integrity_barrier();
        (void)flush_pending_recovery_statuses();

        maybe_send_telemetry();
    }

    std::uint8_t KalshiWireSession::channel_for_sid(std::uint32_t sid) const noexcept{
        return sequence_observer_.channel_for_sid(sid);
    }

    bool KalshiWireSession::is_active_sid(std::uint32_t sid) const noexcept{
        return sequence_observer_.active(sid);
    }

    SequenceObservation KalshiWireSession::observe_sequence(
        std::uint32_t sid,
        std::uint64_t sequence) noexcept{
        return sequence_observer_.observe(sid, sequence);
    }

    core::control::MarketDataChannelTelemetrySnapshot*
    KalshiWireSession::channel_telemetry(
        exchange::kalshi::KalshiMarketDataChannel channel) noexcept{
        const auto value = static_cast<std::uint8_t>(channel);
        if(value == 0 || value > telemetry_.channel_stats.size()){
            return nullptr;
        }
        return &telemetry_.channel_stats[value - 1U];
    }

    void KalshiWireSession::update_capacity_high_water() noexcept{
        telemetry_.frame_pool_in_use_high_water = std::max<std::uint64_t>(
            telemetry_.frame_pool_in_use_high_water,
            frame_pool_.capacity() - frame_pool_.available());
        telemetry_.router_queue_depth_high_water = std::max<std::uint64_t>(
            telemetry_.router_queue_depth_high_water,
            router_queue_.producer_size());
    }

    StampHandleCode KalshiWireSession::resolve_market_route(
        const MarketDataEnvelope& envelope,
        const core::control::UniverseMarketRoute*& route_out) const noexcept{
        route_out = nullptr;
        if(envelope.kind == FrameKind::kUNKNOWN){
            return StampHandleCode::kUNKNOWN_KIND;
        }
        if(envelope.market_ticker.empty()){
            return StampHandleCode::kEMPTY_MARKET_TICKER;
        }
        if(!is_active_sid(envelope.sid)){
            return StampHandleCode::kINACTIVE_SID;
        }
        if(desired_universe_ == nullptr){
            return StampHandleCode::kINACTIVE_SID;
        }
        const auto iterator = market_route_by_ticker_.find(envelope.market_ticker);
        if(iterator == market_route_by_ticker_.end()){
            return StampHandleCode::kUNKNOWN_MARKET_TICKER;
        }
        route_out = &iterator->second;
        return StampHandleCode::kOK;
    }

    void KalshiWireSession::stamp_handle(
        FrameHandle& handle,
        const MarketDataEnvelope& envelope,
        const core::control::UniverseMarketRoute& route) noexcept{
        handle.universe_version = desired_universe_ != nullptr
            ? desired_universe_->version
            : 0;
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
    }

    bool KalshiWireSession::flush_pending_integrity_barrier() noexcept{
        if(!pending_integrity_barrier_.has_value()){
            return true;
        }
        if(!router_queue_.try_push(*pending_integrity_barrier_)){
            return false;
        }
        update_capacity_high_water();
        pending_integrity_barrier_.reset();
        return true;
    }

    bool KalshiWireSession::send_or_defer_integrity_barrier(
        MarketDataPathMessage barrier) noexcept{
        if(pending_integrity_barrier_.has_value()){
            return false;
        }
        if(router_queue_.try_push(barrier)){
            update_capacity_high_water();
            return true;
        }
        pending_integrity_barrier_.emplace((barrier));
        return false;
    }

    void KalshiWireSession::send_or_defer_recovery_status(
        core::control::IoToControlStatus status){
        if(std::holds_alternative<core::control::IoRecoveryRequestAccepted>(
               status)){
            ++telemetry_.snapshot_requests_accepted;
        }else if(std::holds_alternative<core::control::IoRecoveryRequestFailed>(
                      status)){
            ++telemetry_.snapshot_requests_failed;
        }
        if(pending_recovery_statuses_.empty() &&
           control_queues_.io_to_control_status_queue.try_push(
               std::move(status))){
            return;
        }
        pending_recovery_statuses_.emplace_back(std::move(status));
    }

    bool KalshiWireSession::flush_pending_recovery_statuses() noexcept{
        while(!pending_recovery_statuses_.empty()){
            if(!control_queues_.io_to_control_status_queue.try_push(
                   std::move(pending_recovery_statuses_.front()))){
                return false;
            }
            pending_recovery_statuses_.pop_front();
        }
        return true;
    }

    bool KalshiWireSession::pop_control_command(core::control::ControlToIoCommand& cmd_out) noexcept{
        return control_queues_.control_to_io_queue.try_pop(cmd_out);
    }

    bool KalshiWireSession::push_control_status(core::control::IoToControlStatus status) noexcept{
        return control_queues_.io_to_control_status_queue.try_push(std::move(status));
    }

    void KalshiWireSession::maybe_send_telemetry() noexcept{
        const auto now = std::chrono::steady_clock::now();
        if(now < next_telemetry_send_ ||
           !pending_recovery_statuses_.empty()){
            return;
        }
        (void)push_control_status(core::control::IoTelemetry{
            .telemetry = telemetry_,
        });
        next_telemetry_send_ = now + kIO_TELEMETRY_INTERVAL;
    }

    void KalshiWireSession::recycle_handle(FrameHandle handle) noexcept{
        if(!frame_pool_.recycle(handle)){
            ++telemetry_.recycle_failures;
        }
    }

    void KalshiWireSession::record_unknown_market_ticker(const core::control::UnknownMarketTickerStats& unknown_market_ticker){
        auto& samples = telemetry_.unknown_market_ticker_samples;
        const auto already_sampled = std::find_if(samples.begin(), samples.end(), [&](const auto& sample){
            return sample.market_ticker == unknown_market_ticker.market_ticker &&
                   sample.frame_kind == unknown_market_ticker.frame_kind &&
                   sample.sid == unknown_market_ticker.sid &&
                   sample.channel == unknown_market_ticker.channel;
        }) != samples.end();
        if(already_sampled || samples.size() >= kMAX_UNKNOWN_MARKET_TICKER_SAMPLES){
            return;
        }
        samples.emplace_back(unknown_market_ticker);

    }

    void KalshiWireSession::drain_control_commands(){
        core::control::ControlToIoCommand cmd;
        while(pending_recovery_statuses_.empty() &&
              pop_control_command(cmd)){
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
                recover_market(cmd);
            }
        }, cmd);
    }

    void KalshiWireSession::recover_market(
        const core::control::RecoverMarketIo& command){
        const auto fail_request = [this, &command](std::string reason){
            send_or_defer_recovery_status(core::control::IoRecoveryRequestFailed{
                .recovery_id = command.recovery_id,
                .universe_version = command.universe_version,
                .market_id = command.market_id,
                .request_attempt = command.request_attempt,
                .reason = std::move(reason),
            });
        };

        if(command.recovery_id == 0 || command.request_attempt == 0){
            fail_request("Invalid recovery request identity");
            return;
        }

        if(!status_.connected){
            fail_request("Cannot request recovery snapshot while disconnected");
            return;
        }

        if(desired_universe_ == nullptr ||
           desired_universe_->version != command.universe_version){
            fail_request("Recovery request universe does not match installed universe");
            return;
        }

        const auto route_iterator = market_route_by_id_.find(command.market_id);
        
        if(route_iterator == market_route_by_id_.end()){
            fail_request("Recovery request market is not in the installed universe");
            return;
        }

        const auto channel = exchange::kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA;
        const auto subscription_iterator = active_subscriptions_.find(channel);

        if(
            subscription_iterator == active_subscriptions_.end() ||
            subscription_iterator->second.phase != SubscriptionPhase::kSUBSCRIBED ||
            !subscription_iterator->second.sid.has_value()
        ){
            fail_request("Order-book subscription is not active");
            return;
        }

        const auto existing_tag = pending_recovery_by_market_.find(command.market_id);
        if(existing_tag != pending_recovery_by_market_.end()){
            const auto& tag = existing_tag->second;
            const bool same_request =
                tag.recovery_id == command.recovery_id &&
                tag.universe_version == command.universe_version &&
                tag.request_attempt == command.request_attempt;
            if(!same_request){
                fail_request("Market already has a pending recovery snapshot");
                return;
            }

            const bool command_still_pending = std::any_of(
                pending_ws_commands_.begin(),
                pending_ws_commands_.end(),
                [&command](const auto& entry){
                    const auto& context = entry.second.recovery_context;
                    return context.has_value() &&
                           context->recovery_id == command.recovery_id &&
                           context->universe_version == command.universe_version &&
                           context->market_id == command.market_id &&
                           context->request_attempt == command.request_attempt;
                });
            if(!command_still_pending){
                send_or_defer_recovery_status(
                    core::control::IoRecoveryRequestAccepted{
                        .recovery_id = command.recovery_id,
                        .universe_version = command.universe_version,
                        .market_id = command.market_id,
                        .request_attempt = command.request_attempt,
                    });
            }
            return;
        }

        const auto ws_command_id = next_ws_command_id();
        
        const RecoveryCommandContext context{
            .recovery_id = command.recovery_id,
            .universe_version = command.universe_version,
            .market_id = command.market_id,
            .request_attempt = command.request_attempt,
        };
        
        pending_recovery_by_market_.emplace(
            command.market_id,
            PendingRecoveryTag{
                .recovery_id = command.recovery_id,
                .universe_version = command.universe_version,
                .request_attempt = command.request_attempt,
            });
        
        pending_ws_commands_.emplace(
            ws_command_id,
            PendingWsCommand{
                .ws_command_id = ws_command_id,
                .kind = WsCommandKind::kGET_SNAPSHOT,
                .channel = channel,
                .market_ids = {command.market_id},
                .recovery_context = context,
            });

        const auto& ticker = route_iterator->second.kalshi_ticker;
        const std::span<const std::string> tickers{&ticker, 1U};
        const auto sid = subscription_iterator->second.sid.value();

        const auto send_status = ws_session_.send_text(
            market_data_handler_.build_update_message(
                ws_command_id,
                static_cast<std::int64_t>(sid),
                tickers,
                "get_snapshot")
            );


        if(send_status == exchange::kalshi::SendStatus::kACCEPTED){
            ++telemetry_.snapshot_requests_sent;
            return;
        }

        pending_ws_commands_.erase(ws_command_id);
        const auto tag_iterator =
            pending_recovery_by_market_.find(command.market_id);
        if(tag_iterator != pending_recovery_by_market_.end() &&
           tag_iterator->second.recovery_id == command.recovery_id &&
           tag_iterator->second.request_attempt == command.request_attempt){
            pending_recovery_by_market_.erase(tag_iterator);
        }
        fail_request(
            "Failed to send recovery snapshot request: " +
            std::string{ws_session_.last_error()});
    }

    void KalshiWireSession::apply_universe_snapshot(const std::shared_ptr<const core::control::UniverseSnapshot>& snapshot){
        
        if(snapshot == nullptr){
            report_fault("Received null universe snapshot");
            return;
        }
        
        if(desired_universe_ != nullptr &&
           desired_universe_->version != snapshot->version){
            pending_recovery_by_market_.clear();
            for(auto iterator = pending_ws_commands_.begin();
                iterator != pending_ws_commands_.end();){
                if(iterator->second.kind == WsCommandKind::kGET_SNAPSHOT){
                    iterator = pending_ws_commands_.erase(iterator);
                }else{
                    ++iterator;
                }
            }
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
        for(const auto& [market_id, tag] : pending_recovery_by_market_){
            send_or_defer_recovery_status(core::control::IoRecoveryRequestFailed{
                .recovery_id = tag.recovery_id,
                .universe_version = tag.universe_version,
                .market_id = market_id,
                .request_attempt = tag.request_attempt,
                .reason = "Transport reset before recovery snapshot delivery",
            });
        }
        pending_recovery_by_market_.clear();
        pending_ws_commands_.clear();
        active_subscriptions_.clear();
        sequence_observer_.clear();
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
        if(subscription.sid.has_value()){
            sequence_observer_.deactivate(*subscription.sid);
        }
        subscription.channel = channel;
        subscription.phase = SubscriptionPhase::kSUBSCRIBE_PENDING;
        subscription.sid = std::nullopt;
        subscription.market_ids.clear();
        subscription.last_error.clear();

        const auto send_status = ws_session_.send_text(
            market_data_handler_.build_subscribe_message(ws_command_id, channel, tickers)
        );
        return send_status == exchange::kalshi::SendStatus::kACCEPTED;
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

        const auto send_status = ws_session_.send_text(
            market_data_handler_.build_update_message(ws_command_id, sid, tickers, "add_markets")
        );
        if(send_status != exchange::kalshi::SendStatus::kACCEPTED){
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

        const auto send_status = ws_session_.send_text(
            market_data_handler_.build_update_message(ws_command_id, sid, tickers, "delete_markets")
        );
        if(send_status != exchange::kalshi::SendStatus::kACCEPTED){
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
            if(pending.recovery_context.has_value()){
                const auto& context = *pending.recovery_context;
                const auto tag_iterator =
                    pending_recovery_by_market_.find(context.market_id);
                if(tag_iterator != pending_recovery_by_market_.end() &&
                   tag_iterator->second.recovery_id == context.recovery_id &&
                   tag_iterator->second.request_attempt == context.request_attempt){
                    pending_recovery_by_market_.erase(tag_iterator);
                }
                send_or_defer_recovery_status(
                    core::control::IoRecoveryRequestFailed{
                        .recovery_id = context.recovery_id,
                        .universe_version = context.universe_version,
                        .market_id = context.market_id,
                        .request_attempt = context.request_attempt,
                        .reason = "Recovery websocket response missing type",
                    });
                return;
            }
            report_fault("Websocket control response missing type");
            return;
        }

        if(pending.kind == WsCommandKind::kGET_SNAPSHOT){
            if(!pending.recovery_context.has_value()){
                report_fault("Recovery websocket command missing recovery context");
                return;
            }

            const auto& context = *pending.recovery_context;
            const auto erase_matching_tag = [this, &context](){
                const auto tag_iterator =
                    pending_recovery_by_market_.find(context.market_id);
                if(tag_iterator != pending_recovery_by_market_.end() &&
                   tag_iterator->second.recovery_id == context.recovery_id &&
                   tag_iterator->second.universe_version == context.universe_version &&
                   tag_iterator->second.request_attempt == context.request_attempt){
                    pending_recovery_by_market_.erase(tag_iterator);
                }
            };

            if(response_type == "error"){
                erase_matching_tag();
                send_or_defer_recovery_status(
                    core::control::IoRecoveryRequestFailed{
                        .recovery_id = context.recovery_id,
                        .universe_version = context.universe_version,
                        .market_id = context.market_id,
                        .request_attempt = context.request_attempt,
                        .reason = "Kalshi recovery snapshot request failed",
                    });
                return;
            }
            if(response_type != "ok"){
                erase_matching_tag();
                send_or_defer_recovery_status(
                    core::control::IoRecoveryRequestFailed{
                        .recovery_id = context.recovery_id,
                        .universe_version = context.universe_version,
                        .market_id = context.market_id,
                        .request_attempt = context.request_attempt,
                        .reason = "Unexpected Kalshi recovery snapshot response type",
                    });
                return;
            }

            send_or_defer_recovery_status(
                core::control::IoRecoveryRequestAccepted{
                    .recovery_id = context.recovery_id,
                    .universe_version = context.universe_version,
                    .market_id = context.market_id,
                    .request_attempt = context.request_attempt,
                });
            return;
        }

        const auto subscription_iterator =
            active_subscriptions_.find(pending.channel);
        if(subscription_iterator == active_subscriptions_.end()){
            report_fault("Websocket response targeted an unknown subscription");
            return;
        }
        auto& subscription = subscription_iterator->second;

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
                if(sid < 0 ||
                   static_cast<std::uint64_t>(sid) >
                       std::numeric_limits<std::uint32_t>::max()){
                    subscription.phase = SubscriptionPhase::kFAULTED;
                    subscription.last_error = "Subscribed response sid out of range";
                    report_fault(subscription.last_error);
                    return;
                }

                const auto normalized_sid = static_cast<std::uint32_t>(sid);
                if(sequence_observer_.active(normalized_sid) &&
                   sequence_observer_.channel_for_sid(normalized_sid) !=
                       static_cast<std::uint8_t>(pending.channel)){
                    subscription.phase = SubscriptionPhase::kFAULTED;
                    subscription.last_error =
                        "Subscribed response reused an active sid";
                    report_fault(subscription.last_error);
                    return;
                }

                if(subscription.sid.has_value() &&
                   *subscription.sid != normalized_sid){
                    sequence_observer_.deactivate(*subscription.sid);
                }

                subscription.sid = normalized_sid;
                subscription.phase = SubscriptionPhase::kSUBSCRIBED;
                subscription.market_ids.clear();
                subscription.last_error.clear();
                if(desired_universe_ != nullptr){
                    for(const auto& route : desired_universe_->market_routes){
                        subscription.market_ids.insert(route.market_id);
                    }
                }
                sequence_observer_.activate(normalized_sid, pending.channel);

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
                if(subscription.sid.has_value()){
                    sequence_observer_.deactivate(*subscription.sid);
                }
                subscription.sid = std::nullopt;
                subscription.market_ids.clear();
                subscription.phase = SubscriptionPhase::kIDLE;
                subscription.last_error.clear();
                break;
            case WsCommandKind::kGET_SNAPSHOT:
                break;
        }

    }
    //NOLINTNEXTLINE
    void KalshiWireSession::publish_market_data_frame(
        std::span<const std::byte> payload,
        std::uint64_t ingress_ts_ns){
        if(ingress_ts_ns == 0){
            ingress_ts_ns = predex::utils::monotonic_now_ns();
        }
        ++telemetry_.frames_received;
        if(payload.size() > predex::ingest::kalshi::kMaxFrameBytes){
            ++telemetry_.frames_dropped;
            ++telemetry_.oversized_frames;
            return;
        }

        const auto* payload_data = reinterpret_cast<const char*>(payload.data());
        simdjson::padded_string padded_payload{
            std::string_view{payload_data, payload.size()}};
        MarketDataEnvelope envelope{};
        const EnvelopeParseCode parse_result =
            parse_market_data_envelope(padded_payload, envelope);

        const bool sequence_available =
            parse_result == EnvelopeParseCode::kOK ||
            parse_result == EnvelopeParseCode::kMISSING_TYPE ||
            parse_result == EnvelopeParseCode::kUNSUPPORTED_TYPE ||
            parse_result == EnvelopeParseCode::kMISSING_MSG ||
            parse_result == EnvelopeParseCode::kMISSING_MARKET_TICKER;

        SequenceObservation sequence_observation{};
        if(sequence_available){
            sequence_observation =
                observe_sequence(envelope.sid, envelope.sequence);
            if(sequence_observation.code ==
               SequenceObservationCode::kINACTIVE_SID){
                ++telemetry_.frames_dropped;
                ++telemetry_.stamp_failed;
                ++telemetry_.inactive_sid;
                return;
            }

            auto* const channel_stats =
                channel_telemetry(sequence_observation.channel);
            if(channel_stats != nullptr){
                ++channel_stats->frames_observed;
                switch(sequence_observation.code){
                    case SequenceObservationCode::kGAP:
                        ++channel_stats->sequence_gaps;
                        break;
                    case SequenceObservationCode::kDUPLICATE:
                        ++channel_stats->duplicate_sequences;
                        break;
                    case SequenceObservationCode::kSTALE:
                        ++channel_stats->stale_sequences;
                        break;
                    case SequenceObservationCode::kFIRST:
                    case SequenceObservationCode::kCONTIGUOUS:
                    case SequenceObservationCode::kINACTIVE_SID:
                        break;
                }
            }

            if(sequence_observation.code == SequenceObservationCode::kGAP &&
               sequence_observation.channel ==
                   exchange::kalshi::KalshiMarketDataChannel::kORDERBOOK_DELTA){
                const OrderBookSubscriptionInvalidationBarrier barrier{
                    .universe_version = desired_universe_ != nullptr
                        ? desired_universe_->version
                        : 0,
                    .incident = {
                        .origin = IntegrityIncidentOrigin::kWIRE_SESSION,
                        .producer_index = 0,
                        .incident_id = next_wire_incident_id_++,
                    },
                    .sid = envelope.sid,
                    .expected_sequence =
                        sequence_observation.expected_sequence,
                    .observed_sequence =
                        sequence_observation.observed_sequence,
                    .reason = BookInvalidationReason::kEXCHANGE_SEQUENCE_GAP,
                };
                if(!send_or_defer_integrity_barrier(
                       MarketDataPathMessage{barrier})){
                    ++telemetry_.frames_dropped;
                    return;
                }
            }

            if(sequence_observation.code == SequenceObservationCode::kDUPLICATE ||
               sequence_observation.code == SequenceObservationCode::kSTALE){
                ++telemetry_.frames_dropped;
                return;
            }
        }

        switch(parse_result){
            case EnvelopeParseCode::kOK:
                break;

            case EnvelopeParseCode::kINVALID_JSON://NOLINT
                ++telemetry_.frames_dropped;
                ++telemetry_.envelope_parse_failed;
                return;

            case EnvelopeParseCode::kMISSING_SID:
            case EnvelopeParseCode::kSID_OUT_OF_RANGE:
            case EnvelopeParseCode::kMISSING_TYPE:
            case EnvelopeParseCode::kMISSING_SEQUENCE:
            case EnvelopeParseCode::kMISSING_MSG:
                ++telemetry_.frames_dropped;
                ++telemetry_.envelope_parse_failed;
                return;

            case EnvelopeParseCode::kUNSUPPORTED_TYPE:
                ++telemetry_.frames_dropped;
                ++telemetry_.envelope_unsupported_type;
                return;

            case EnvelopeParseCode::kMISSING_MARKET_TICKER:
                ++telemetry_.frames_dropped;
                ++telemetry_.envelope_missing_market_ticker;
                return;
        }

        const core::control::UniverseMarketRoute* route = nullptr;
        const StampHandleCode route_result =
            resolve_market_route(envelope, route);

        switch(route_result){
            case StampHandleCode::kOK:
                break;

            case StampHandleCode::kUNKNOWN_KIND:
                ++telemetry_.frames_dropped;
                ++telemetry_.stamp_failed;
                ++telemetry_.envelope_unsupported_type;
                return;

            case StampHandleCode::kEMPTY_MARKET_TICKER:
                ++telemetry_.frames_dropped;
                ++telemetry_.stamp_failed;
                ++telemetry_.envelope_missing_market_ticker;
                return;

            case StampHandleCode::kINACTIVE_SID:
                ++telemetry_.frames_dropped;
                ++telemetry_.stamp_failed;
                ++telemetry_.inactive_sid;
                return;

            case StampHandleCode::kUNKNOWN_MARKET_TICKER:
                ++telemetry_.frames_dropped;
                ++telemetry_.stamp_failed;
                ++telemetry_.unknown_market_ticker;
                if(sequence_available){
                    if(auto* stats = channel_telemetry(
                           sequence_observation.channel);
                       stats != nullptr){
                        ++stats->intentionally_filtered;
                    }
                }
                core::control::UnknownMarketTickerStats unknown_market_ticker{
                    .market_ticker = std::string{envelope.market_ticker},
                    .frame_kind = static_cast<std::uint8_t>(envelope.kind),
                    .sid = envelope.sid,
                    .channel = channel_for_sid(envelope.sid),
                };
                record_unknown_market_ticker(unknown_market_ticker);
                return;
        }

        if(route == nullptr){
            ++telemetry_.frames_dropped;
            ++telemetry_.stamp_failed;
            return;
        }

        const bool affects_order_book =
            envelope.kind == FrameKind::kORDERBOOK_SNAPSHOT ||
            envelope.kind == FrameKind::kORDERBOOK_DELTA;
        const auto emit_market_barrier =
            [this, &envelope, route](BookInvalidationReason reason){
                if(envelope.kind != FrameKind::kORDERBOOK_SNAPSHOT &&
                   envelope.kind != FrameKind::kORDERBOOK_DELTA){
                    return true;
                }
                const MarketInvalidationBarrier barrier{
                    .universe_version = desired_universe_ != nullptr
                        ? desired_universe_->version
                        : 0,
                    .incident = {
                        .origin = IntegrityIncidentOrigin::kWIRE_SESSION,
                        .producer_index = 0,
                        .incident_id = next_wire_incident_id_++,
                    },
                    .sid = envelope.sid,
                    .sequence = envelope.sequence,
                    .market_id = route->market_id,
                    .event_id = route->event_id,
                    .shard_index = route->shard_index,
                    .shard_event_index = route->shard_event_index,
                    .event_market_index = route->event_market_index,
                    .reason = reason,
                };
                return send_or_defer_integrity_barrier(
                    MarketDataPathMessage{barrier});
            };
        const auto fail_correlated_recovery =
            [this, &envelope, route](std::string reason){
                if(envelope.kind != FrameKind::kORDERBOOK_SNAPSHOT){
                    return;
                }
                const auto tag_iterator =
                    pending_recovery_by_market_.find(route->market_id);
                if(tag_iterator == pending_recovery_by_market_.end()){
                    return;
                }
                const PendingRecoveryTag tag = tag_iterator->second;
                pending_recovery_by_market_.erase(tag_iterator);
                for(auto iterator = pending_ws_commands_.begin();
                    iterator != pending_ws_commands_.end();){
                    const auto& context = iterator->second.recovery_context;
                    if(context.has_value() &&
                       context->recovery_id == tag.recovery_id &&
                       context->universe_version == tag.universe_version &&
                       context->market_id == route->market_id &&
                       context->request_attempt == tag.request_attempt){
                        iterator = pending_ws_commands_.erase(iterator);
                    }else{
                        ++iterator;
                    }
                }
                send_or_defer_recovery_status(
                    core::control::IoRecoveryRequestFailed{
                        .recovery_id = tag.recovery_id,
                        .universe_version = tag.universe_version,
                        .market_id = route->market_id,
                        .request_attempt = tag.request_attempt,
                        .reason = std::move(reason),
                    });
            };

        predex::ingest::kalshi::FrameHandle handle{};
        if(!frame_pool_.try_acquire(handle)){
            drain_recycle_queues();
            if(!frame_pool_.try_acquire(handle)){
                ++telemetry_.frames_dropped;
                ++telemetry_.pool_exhausted;
                if(affects_order_book){
                    (void)emit_market_barrier(
                        BookInvalidationReason::kWIRE_POOL_EXHAUSTION);
                    fail_correlated_recovery(
                        "Recovery snapshot dropped because the wire frame pool was exhausted");
                }
                return;
            }
        }
        update_capacity_high_water();

        auto* frame = frame_pool_.writable_frame(handle);
        if(frame == nullptr){
            ++telemetry_.frames_dropped;
            ++telemetry_.missing_frame_slot;
            recycle_handle(handle);
            if(affects_order_book){
                (void)emit_market_barrier(
                    BookInvalidationReason::kWIRE_POOL_EXHAUSTION);
                fail_correlated_recovery(
                    "Recovery snapshot dropped because its frame slot was unavailable");
            }
            return;
        }

        frame->recv_ts_ns = ingress_ts_ns;
        frame->len = static_cast<std::uint32_t>(payload.size());
        frame->flags = 0;
        std::memcpy(frame->payload.data(), payload.data(), payload.size());
        stamp_handle(handle, envelope, *route);
        handle.ingress_ts_ns = ingress_ts_ns;

        std::optional<PendingRecoveryTag> recovery_tag;
        if(envelope.kind == FrameKind::kORDERBOOK_SNAPSHOT){
            const auto tag_iterator =
                pending_recovery_by_market_.find(route->market_id);
            if(tag_iterator != pending_recovery_by_market_.end() &&
               tag_iterator->second.universe_version == handle.universe_version){
                recovery_tag = tag_iterator->second;
                handle.recovery_id = recovery_tag->recovery_id;
            }
        }

        handle.wire_publish_ts_ns = predex::utils::monotonic_now_ns();
        const auto latency_channel_index = market_data_channel_index(handle.kind);
        if(latency_channel_index < core::control::kMarketDataChannelCount){
            predex::utils::record_elapsed_ns(
                telemetry_.wire_service_latency[latency_channel_index],
                handle.ingress_ts_ns,
                handle.wire_publish_ts_ns);
        }

        if(!router_queue_.try_push(MarketDataPathMessage{handle})){
            ++telemetry_.router_enqueue_failed;
            if(sequence_available){
                if(auto* stats = channel_telemetry(
                       sequence_observation.channel);
                   stats != nullptr){
                    ++stats->downstream_delivery_losses;
                }
            }
            if(!logger_queue_.try_push(handle)){
                ++telemetry_.frames_dropped;
                ++telemetry_.logger_fallback_failed;
                recycle_handle(handle);
            }else{
                ++telemetry_.logger_fallback_enqueued;
                ++telemetry_.frames_published;
                if(sequence_available){
                    if(auto* stats = channel_telemetry(
                           sequence_observation.channel);
                       stats != nullptr){
                        ++stats->logger_only_frames;
                    }
                }
            }
            if(affects_order_book){
                (void)emit_market_barrier(
                    BookInvalidationReason::kWIRE_TO_ROUTER_DELIVERY_LOSS);
                fail_correlated_recovery(
                    "Recovery snapshot could not be delivered to the router");
            }
            return;
        }
        update_capacity_high_water();

        if(recovery_tag.has_value()){
            const auto tag_iterator =
                pending_recovery_by_market_.find(route->market_id);
            if(tag_iterator != pending_recovery_by_market_.end() &&
               tag_iterator->second.recovery_id == recovery_tag->recovery_id &&
               tag_iterator->second.universe_version ==
                   recovery_tag->universe_version &&
               tag_iterator->second.request_attempt ==
                   recovery_tag->request_attempt){
                pending_recovery_by_market_.erase(tag_iterator);
            }
        }
        ++telemetry_.frames_published;
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
                ++telemetry_.recycle_failures;
            }
        } 
    }

    bool KalshiWireSession::service_transport_once(bool may_consume){
        using predex::exchange::kalshi::ReadStatus;

        bool made_progress = ws_session_.poll() > 0;

        const auto read_result = ws_session_.recv_text();

        switch(read_result.status){
            case ReadStatus::kPENDING:
                return made_progress;
            case ReadStatus::kCLOSED:
                status_.connected = false;
                status_.last_error = "websocket closed";
                clear_transport_subscription_state();
                (void)push_control_status(core::control::IoDisconnected{
                    .reason = status_.last_error,
                });
                return true;
            case ReadStatus::kERROR:
                report_fault("websocket receive failed: " + std::string{ws_session_.last_error()});
                return true;
            case ReadStatus::kMESSAGE:
                break;
        }

        if(!may_consume){
            return made_progress;
        }
        const auto ingress_ts_ns = predex::utils::monotonic_now_ns();

        switch(classify_incoming_message(read_result.payload)){
            case IncomingMessageKind::kIGNORE:
                break;
            case IncomingMessageKind::kCONTROL_RESPONSE:
                handle_ws_control_response(read_result.payload);
                break;
            case IncomingMessageKind::kMARKET_DATA:
                publish_market_data_frame(read_result.payload, ingress_ts_ns);
                break;
            case IncomingMessageKind::kMALFORMED:
                report_fault("Malformed websocket message received");
                break;
        }

        if(status_.connected){
            ws_session_.consume_message();
        }
        
        return true;
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

        if(type == "ok" || type == "error" || type == "subscribed" ||
           type == "unsubscribed"){
            return IncomingMessageKind::kIGNORE;
        }

        std::uint64_t sid{};
        std::uint64_t sequence{};
        if(read_uint64(root, "sid", sid) &&
           read_uint64(root, "seq", sequence)){
            return IncomingMessageKind::kMARKET_DATA;
        }

        return IncomingMessageKind::kIGNORE;
    }

    EnvelopeParseCode KalshiWireSession::parse_market_data_envelope(
        simdjson::padded_string_view payload,
        MarketDataEnvelope& envelope_out){
        envelope_out = MarketDataEnvelope{};

        auto doc_result = market_data_envelope_parser_.iterate(payload);
        if(doc_result.error() != simdjson::SUCCESS){return EnvelopeParseCode::kINVALID_JSON;}
        auto root_result = doc_result.get_object();
        if(root_result.error() != simdjson::SUCCESS){return EnvelopeParseCode::kINVALID_JSON;}
        simdjson::ondemand::object root = root_result.value();

        std::uint64_t session_id_out{};
        if(!read_uint64(root, "sid", session_id_out)){
            return EnvelopeParseCode::kMISSING_SID;
        }
        if(session_id_out > std::numeric_limits<std::uint32_t>::max()){
            return EnvelopeParseCode::kSID_OUT_OF_RANGE;
        }
        envelope_out.sid = static_cast<std::uint32_t>(session_id_out);

        std::uint64_t sequence_out{};
        if(!read_uint64(root, "seq", sequence_out)){
            return EnvelopeParseCode::kMISSING_SEQUENCE;
        }
        envelope_out.sequence = sequence_out;

        std::string_view type_out;
        if(!read_string(root, "type", type_out)){
            return EnvelopeParseCode::kMISSING_TYPE;
        }
        envelope_out.kind = frame_kind_from_type(type_out);
        if(envelope_out.kind == FrameKind::kUNKNOWN){
            return EnvelopeParseCode::kUNSUPPORTED_TYPE;
        }

        simdjson::ondemand::object msg;
        if(!read_object(root, "msg", msg)){
            return EnvelopeParseCode::kMISSING_MSG;
        }

        std::string_view market_ticker_out;
        if(!read_string(msg, "market_ticker", market_ticker_out)){
            return EnvelopeParseCode::kMISSING_MARKET_TICKER;
        }
        envelope_out.market_ticker = market_ticker_out;

        return EnvelopeParseCode::kOK;
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
        const auto send_status = ws_session_.send_text(
            market_data_handler_.build_unsubscribe_message(ws_command_id, {{sid}})
        );
        if(send_status != exchange::kalshi::SendStatus::kACCEPTED){
            pending_ws_commands_.erase(ws_command_id);
            sub_it->second.phase = SubscriptionPhase::kSUBSCRIBED;
            sub_it->second.last_error = std::string{ws_session_.last_error()};
            return false;
        }

        return true;
    }

    void KalshiWireSession::disconnect(std::string reason){
        ws_session_.close();
        status_.connected = false;
        status_.last_error = std::move(reason);

        clear_transport_subscription_state();
        (void)push_control_status(core::control::IoDisconnected{
            .reason = status_.last_error,
        });
    }
}
