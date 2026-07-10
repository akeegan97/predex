#include "predex/exchange/kalshi/order_rest_session.hpp"
#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"
#include "predex/exchange/kalshi/http_types.hpp"
#include "predex/oms/oms_types.hpp"
#include <type_traits>
#include <variant>
#include <thread>

namespace predex::exchange::kalshi{
    namespace {
        [[nodiscard]] std::uint64_t now_ns() noexcept{
            return static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()
                ).count()
            );
        }
    }

    void OrderRestSession::apply_order_route_universe(const std::shared_ptr<const core::control::OrderRouteUniverse>& snapshot){
        if(!snapshot){
            fault("apply_order_route_universe: snapshot is null");
            return;
        }
        installed_universe_version_ = snapshot->version;
        order_rest_adapter_.apply_order_route_universe(snapshot);
    }

    void OrderRestSession::drain_control_commands() noexcept{
        control::ControlToOrderRestCommand command;
        while(control_queues_.control_to_order_rest_queue.try_pop(command)){
            handle_control_command(command);
        }
    }
    void OrderRestSession::drain_oms_commands() noexcept{
        oms::OmsToKalshiCommand command;
        while(!egress_closed_ && inflight_requests_.size() < kMAX_INFLIGHT_REQUESTS  && oms_queues_.oms_to_order_rest_queue.try_pop(command)){
            handle_oms_command(command);
        }
    }

    void OrderRestSession::enable() noexcept {
        if (status_.enabled && !status_.faulted) {
            return;
        }

        if (installed_universe_version_ == 0) {
            fault("cannot enable order REST without an installed universe");
            return;
        }

        if (status_.faulted &&
            (!inflight_requests_.empty() || pending_oms_count_ > 0)) {
            status_.last_error =
                "cannot recover order REST while requests or OMS events remain unresolved";
            return;
        }

        if (!http_session_.warm_up()) {
            fault(
                "failed to warm HTTP/2 session: " +
                std::string{http_session_.last_error()}
            );
            return;
        }

        status_.enabled = true;
        status_.faulted = false;
        status_.last_error.clear();
        pending_fault_notification_ = false;
        pending_ready_notification_ = true;
        try_send_pending_control_notifications();
    }
    void OrderRestSession::disable(std::string reason){
        status_.enabled = false;
        pending_ready_notification_ = false;
        status_.last_error = std::move(reason);
    }

    bool OrderRestSession::try_push_control_status(core::control::OrderRestToControlStatus status) noexcept{
        return control_queues_.order_rest_to_control_queue.try_push(std::move(status));
    }

    void OrderRestSession::try_send_pending_control_notifications() noexcept{
        if(pending_fault_notification_){
            if(try_push_control_status(control::OrderRestFaulted{
                    .error_message = status_.last_error
                })){
                pending_fault_notification_ = false;
            }
            return;
        }

        if(!pending_ready_notification_){
            return;
        }

        if(!status_.enabled || status_.faulted){
            pending_ready_notification_ = false;
            return;
        }

        if(try_push_control_status(control::OrderRestReady{})){
            pending_ready_notification_ = false;
        }
    }

    bool OrderRestSession::send_or_defer_oms_event(oms::KalshiToOmsEvent event) noexcept{
        if(pending_oms_count_ > 0){
            return defer_oms_event(std::move(event));
        }
        if(oms_queues_.order_rest_to_oms_queue.try_push(event)){
            return true;
        }
        ++telemetry_.oms_enqueue_failures;
        return defer_oms_event(std::move(event));
    }

    void OrderRestSession::handle_control_command(const core::control::ControlToOrderRestCommand& command){
        std::visit([this](const auto& cmd){
            using T = std::decay_t<decltype(cmd)>;
            if constexpr(std::is_same_v<T, core::control::EnableOrderRest>){
                enable();
            } else if constexpr(std::is_same_v<T, core::control::DisableOrderRest>){
                disable("control plane requested disable");
            } else if constexpr(std::is_same_v<T, core::control::ApplyOrderRouteUniverse>){
                apply_order_route_universe(cmd.snapshot);
            }
        }, command);
    }

    bool OrderRestSession::emit_local_reject(const PreparedOrderRestRequest& prepared, std::string reason) noexcept{
        oms::RestOrderResponse response{
            .command_kind = prepared.command_kind,
            .result_code = oms::RestResultCode::kNOT_SENT,
            .transport_recv_ts_ns = now_ns(),
            .venue_reject_reason = oms::VenueRejectReason::kNone,
            .raw_reason_message = std::move(reason),
        };

        std::visit([&response](const auto& cmd){
            using T = std::decay_t<decltype(cmd)>;
            response.context.oms_request_id = cmd.oms_request_id;

            if constexpr(std::is_same_v<T, oms::SubmitOrderCmd>){
                response.context.context = cmd.new_order_intent.context;
                response.client_order_id = cmd.client_order_id;
            }else if constexpr(std::is_same_v<T, oms::CancelOrderCmd>){
                response.context.context = cmd.cancel_order_intent.context;
                response.client_order_id = cmd.client_order_id;
                if(cmd.exchange_order_id.has_value()){
                    response.exchange_order_id = *cmd.exchange_order_id;
                }
            }else if constexpr(std::is_same_v<T, oms::ModifyOrderCmd>){
                response.context.context = cmd.modify_order_intent.context;
                    response.client_order_id = cmd.client_order_id;
                if(cmd.exchange_order_id.has_value()){
                    response.exchange_order_id = *cmd.exchange_order_id;
                }
            }
        }, prepared.source_command);

        ++telemetry_.requests_failed;
        if(!send_or_defer_oms_event(oms::KalshiToOmsEvent{std::move(response)})){

            ++telemetry_.oms_enqueue_failures;
            return false;
        }
        return true;
    }

    void OrderRestSession::handle_oms_command(const oms::OmsToKalshiCommand& command){
        ++telemetry_.commands_received;
        if(const auto* close_cmd = std::get_if<oms::CloseOrderRestEgress>(&command)){
            handle_close_egress(*close_cmd);
            return;
        }
        PreparedOrderRestRequest prepared = order_rest_adapter_.prepare_command(command);

        if(!prepared.ok){
            (void)emit_local_reject(prepared, prepared.error_message);
            return;
        }
        if(!status_.enabled){
            (void)emit_local_reject(prepared, "Order REST session is disabled");
            return;
        }

        const HttpRequestId request_id = prepared.request.request_id;

        if(inflight_requests_.contains(request_id)){
            (void)emit_local_reject(prepared, "Duplicate request_id in flight");
            return;
        }
        const auto http_start_result = http_session_.start_request(prepared.request);
        switch(http_start_result){
            case HttpStartResult::kACCEPTED:
                break;
            case HttpStartResult::kAT_CAPACITY:
                (void)emit_local_reject(prepared, "HTTP session at capacity");
                return;
            case HttpStartResult::kCLOSED:
                (void)emit_local_reject(prepared, "HTTP session closed");
                return;
            case HttpStartResult::kERROR:
                (void)emit_local_reject(prepared, "HTTP session error: " + std::string{http_session_.last_error()});
                return;
        }
        inflight_requests_.emplace(request_id, InflightRequest{std::move(prepared)});
        ++telemetry_.requests_sent;
    }

    bool OrderRestSession::defer_oms_event(oms::KalshiToOmsEvent event) noexcept{
        if(pending_oms_count_ >= pending_oms_events_.size()){
            //component faulted eventually surface to CP to ochestrate recovery/halt strategy from emitting more intents etc.
            return false;
        }
        pending_oms_events_[pending_oms_tail_ % pending_oms_events_.size()] = std::move(event);
        ++pending_oms_count_;
        ++pending_oms_tail_;
        return true;
    }

    void OrderRestSession::drain_pending_oms_events() noexcept{
        while(pending_oms_count_ > 0){
            const auto& event = pending_oms_events_[pending_oms_head_ % pending_oms_events_.size()];
            if(!oms_queues_.order_rest_to_oms_queue.try_push(event)){
                break;
            }

            pending_oms_events_[pending_oms_head_ % pending_oms_events_.size()] = oms::KalshiToOmsEvent{};//clear the slot

            --pending_oms_count_;
            ++pending_oms_head_;
        }
    }

    void OrderRestSession::receive_http(std::size_t max_batch_size) noexcept{

        for(std::size_t i = 0; i < max_batch_size; ++i){
            HttpPollResult poll_result = http_session_.poll();

            if(poll_result.status == HttpRequestStatus::kIDLE || poll_result.status == HttpRequestStatus::kIN_FLIGHT){break;}

            if(!poll_result.response.has_value()){
                fault("HttpPollResult has no response when status is COMPLETED");
                break;
            }
            HttpResponse response = std::move(*poll_result.response);

            auto inflight_iter = inflight_requests_.find(response.request_id);
            if(inflight_iter == inflight_requests_.end()){
                fault("Received HTTP response for unknown request_id");
                break;
            }

            PreparedOrderRestRequest prepared = std::move(inflight_iter->second.prepared); 
            inflight_requests_.erase(inflight_iter);

            CompletedOrderRestRequest completed = order_rest_adapter_.complete_request(prepared, response);
            ++telemetry_.responses_received;

            if(!completed.ok){
                ++telemetry_.requests_failed;
            }

            if(!send_or_defer_oms_event(oms::KalshiToOmsEvent{std::move(completed.response)})){
                fault("pending OMS event queue full, unable to send response to OMS");
                break;
            }

        }
    }

    void OrderRestSession::fault(std::string reason) noexcept{
        disable(std::move(reason));
        status_.faulted = true;
        pending_ready_notification_ = false;
        pending_fault_notification_ = true;
        try_send_pending_control_notifications();
    }

    void OrderRestSession::maybe_send_telemetry() noexcept{
        try_send_pending_control_notifications();

        const auto now = std::chrono::steady_clock::now();
        if(now < next_telemetry_send_){
            return;
        }
        next_telemetry_send_ = now + kORDER_REST_TELEMETRY_INTERVAL;

        core::control::OrderRestTelemetrySnapshot snapshot{
            .commands_received = telemetry_.commands_received,
            .requests_sent = telemetry_.requests_sent,
            .responses_received = telemetry_.responses_received,
            .requests_failed = telemetry_.requests_failed,
            .retry_count = telemetry_.retry_count,
            .oms_enqueue_failures = telemetry_.oms_enqueue_failures
        };

        (void)try_push_control_status(core::control::OrderRestTelemetry{.telemetry = snapshot});
    }

    void OrderRestSession::run(const std::stop_token& stop_token){
        while(!stop_token.stop_requested()){
            drain_control_commands();
            drain_pending_oms_events();

            
            receive_http(kMAX_HTTP_POLL_BATCH_SIZE);

            finish_egress_drain();

            if(status_.enabled && !status_.faulted && pending_oms_count_ == 0){
                drain_oms_commands();
            }

            maybe_send_telemetry();

            std::this_thread::yield();
        }
    }

    void OrderRestSession::handle_close_egress(const oms::CloseOrderRestEgress& command) noexcept{
        if(egress_closed_){
            if(shutdown_epoch_ != command.shutdown_epoch){
                fault("Received CloseOrderRestEgress with mismatched shutdown_epoch");
            }
            return;
        }
        if(command.shutdown_epoch == 0){
            fault("Received CloseOrderRestEgress with shutdown_epoch=0");
            return;
        }
        egress_closed_ = true;
        shutdown_epoch_ = command.shutdown_epoch;
        status_.enabled = false;
        pending_ready_notification_ = false;
        pending_fault_notification_ = false;
        try_send_pending_control_notifications();
    }

    void OrderRestSession::finish_egress_drain() noexcept{
        if(!egress_closed_ || drain_marker_sent_ || !inflight_requests_.empty() || pending_oms_count_ > 0){
            return;
        }
        if(!send_or_defer_oms_event(oms::KalshiToOmsEvent{oms::OrderRestEgressDrained{
            .shutdown_epoch = shutdown_epoch_,
            .completion_ts_ns = now_ns()
        }})){
            fault("Failed to send OrderRestEgressDrained event to OMS");
            return;
        }
        drain_marker_sent_ = true;
    }





}
