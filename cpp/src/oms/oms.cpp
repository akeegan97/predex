#include "predex/oms/oms.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <string_view>
#include <variant>

namespace {
    template <class... Ts>
    struct Overloaded : Ts... {
        using Ts::operator()...;
    };

    template <class... Ts>
    Overloaded(Ts...) -> Overloaded<Ts...>;

    [[nodiscard]] std::uint64_t now_ns() noexcept {
        using Clock = std::chrono::steady_clock;
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now().time_since_epoch()
            ).count()
        );
    }
}

namespace predex::oms{
constexpr std::size_t kMAX_DRAIN = 100;

    OmsPumpResult Oms::pump_once() noexcept{
        std::size_t work_done{0};
        work_done += try_send_pending_control_status() ? 1 : 0;
        work_done += drain_control_commands(kMAX_DRAIN);
        work_done += drain_venue_events(kMAX_DRAIN);
        work_done += drain_strategy_intents(kMAX_DRAIN);

        if(work_done > 0){
            return OmsPumpResult::kOK;
        }
        return OmsPumpResult::kNoWork;
        
    }

    std::size_t Oms::drain_strategy_intents(std::size_t max) noexcept{
        std::size_t processed{0};
        while(processed < max){
            bool did_work{false};
            for(auto* queue : queues_.strategy_intent_queues){
                if(queue == nullptr || processed >= max){
                    continue;
                }
                intent::StrategyIntent strategy_intent{};
                if(!queue->try_pop(strategy_intent)){
                    continue;
                }
                std::visit(Overloaded{
                    [this](const intent::NewOrderIntent& intent){ handle_strategy_intent(intent); },
                    [this](const intent::CancelOrderIntent& intent){ handle_strategy_intent(intent); },
                    [this](const intent::ModifyOrderIntent& intent){ handle_strategy_intent(intent); },
                    [this](const intent::GroupOrderIntent& intent){ handle_strategy_intent(intent); }
                }, strategy_intent);
                ++telemetry_.strategy_intents_received;
                ++processed;
                did_work = true;
            }
            if(!did_work){
                break;
            }
        }
        return processed;
    }

    std::size_t Oms::drain_venue_events(std::size_t max) noexcept{
        std::size_t processed{0};
        while(processed < max){
            KalshiToOmsEvent event{};
            if(!queues_.venue_event_queue.try_pop(event)){
                break;
            }
            std::visit(Overloaded{
                [this](const RestOrderResponse& response){ handle_venue_event(response); },
                [this](const PrivateWsOrderEvent& event){ handle_venue_event(event); },
                [this](const ReconciledOrderSnapshot& snapshot){ handle_venue_event(snapshot); },
                [this](const OrderRestEgressDrained& drained){ handle_venue_event(drained); }
            }, event);
            ++processed;
        }
        return processed;
    }

    std::size_t Oms::drain_control_commands(std::size_t max) noexcept{
        std::size_t processed{0};
        while(processed < max){
            core::control::ControlToOmsCommand command{};
            if(!queues_.control_command_queue.try_pop(command)){
                break;
            }
            std::visit(Overloaded{
                [this](const core::control::AllowTrading& command){ handle_control_command(command); },
                [this](const core::control::DisableTrading& command){ handle_control_command(command); },
                [this](const core::control::FlattenAllOrders& command){ handle_control_command(command); }
            }, command);
            ++processed;
        }
        return processed;
    }

    ClientOrderId Oms::make_client_order_id(intent::OmsRequestId oms_request_id) noexcept{
        ClientOrderId client_order_id{};
        std::array<char, kLENGTH_CLIENT_ORDER_ID> buffer{};
        constexpr std::string_view prefix{"px-"};
        std::copy(prefix.begin(), prefix.end(), buffer.begin());
        auto* begin = buffer.data() + prefix.size();
        auto* end = buffer.data() + buffer.size();
        const auto result = std::to_chars(begin, end, oms_request_id);
        if(result.ec != std::errc{}){
            client_order_id.clear();
            return client_order_id;
        }
        const std::string_view id{buffer.data(), static_cast<std::size_t>(result.ptr - buffer.data())};//NOLINT
        (void)client_order_id.assign_from(id);
        return client_order_id;
    }

    OmsContext Oms::make_context(intent::OmsRequestId oms_request_id, intent::IntentContext context) noexcept{
        return OmsContext{
            .oms_request_id = oms_request_id,
            .context = context,
        };
    }

    OrderStateUpdate Oms::make_order_state_update(const OrderRecord& record, VenueEventSource source, std::uint64_t update_ts_ns) const noexcept{
        OrderStateUpdate update{
            .client_order_id = record.client_order_id,
            .context = record.context,
            .order_state = record.order_state,
            .update_source = source,
            .working_qty_lots = record.leaves_qty_lots,
            .working_price_ticks = record.working_price_ticks,
            .outcome = record.outcome,
            .ordered_qty_lots = record.ordered_qty_lots,
            .cumulative_filled_qty_lots = record.cumulative_filled_qty_lots,
            .leaves_qty_lots = record.leaves_qty_lots,
            .last_update_ts_ns = update_ts_ns,
        };
        if(record.exchange_order_id.has_value()){
            update.exchange_order_id = *record.exchange_order_id;
        }
        return update;
    }

    OrderRecord* Oms::find_order(intent::OmsRequestId oms_request_id) noexcept{
        auto it = oms_request_id_to_order_record_map_.find(oms_request_id);//NOLINT
        if(it == oms_request_id_to_order_record_map_.end()){
            return nullptr;
        }
        return &it->second;
    }

    OrderRecord* Oms::find_order(const ClientOrderId& client_order_id) noexcept{
        if(client_order_id.empty()){
            return nullptr;
        }
        const auto id_it = client_order_id_to_oms_request_id_map_.find(client_order_id);
        if(id_it == client_order_id_to_oms_request_id_map_.end()){
            return nullptr;
        }
        return find_order(id_it->second);
    }

    OrderRecord* Oms::find_order(const ExchangeOrderId& exchange_order_id) noexcept{
        if(exchange_order_id.empty()){
            return nullptr;
        }
        const auto id_it = exchange_order_id_to_oms_request_id_map_.find(exchange_order_id);
        if(id_it == exchange_order_id_to_oms_request_id_map_.end()){
            return nullptr;
        }
        return find_order(id_it->second);
    }

    bool Oms::send_strategy_message(std::uint16_t strategy_index, OmsToStrategyMessage message) noexcept{
        if(strategy_index >= queues_.strategy_response_queues.size()){
            ++telemetry_.strategy_response_backpressure;
            return false;
        }
        auto* queue = queues_.strategy_response_queues[strategy_index];
        if(queue == nullptr || !queue->try_push(std::move(message))){//NOLINT
            ++telemetry_.strategy_response_backpressure;
            return false;
        }
        return true;
    }

    bool Oms::send_kalshi_command(OmsToKalshiCommand command) noexcept{
        if(!queues_.kalshi_command_queue.try_push(std::move(command))){//NOLINT
            ++telemetry_.kalshi_commands_failed;
            return false;
        }
        ++telemetry_.kalshi_commands_sent;
        return true;
    }

    bool Oms::send_control_status(core::control::OmsToControlStatus status) noexcept{
        return queues_.oms_status_queue.try_push(std::move(status));
    }

    void Oms::emit_order_state_update(const OrderRecord& record, VenueEventSource source, std::uint64_t update_ts_ns) noexcept{
        OrderStateUpdate update = make_order_state_update(record, source, update_ts_ns);
        if(send_strategy_message(record.context.context.strategy_index, OmsToStrategyMessage{update})){
            ++telemetry_.order_state_updates_sent;
        }
    }

    void Oms::emit_trading_enabled_changed() noexcept{
        (void)send_control_status(core::control::OmsToControlStatus{
            core::control::OmsTradingEnabledChanged{.trading_enabled = trading_enabled_}
        });
    }

    void Oms::emit_telemetry() noexcept{
        telemetry_.live_orders = live_order_count();
        telemetry_.pending_submit_orders = pending_submit_order_count();
        telemetry_.uncertain_orders = uncertain_order_count();
        (void)send_control_status(core::control::OmsToControlStatus{
            core::control::OmsTelemetry{.telemetry = telemetry_}
        });
    }

    void Oms::handle_strategy_intent(const intent::NewOrderIntent& intent) noexcept{
        const std::uint64_t recv_ts = now_ns();
        const intent::OmsRequestId oms_request_id = next_oms_request_id_++;
        const OmsContext context = make_context(oms_request_id, intent.context);

        auto reject = [&](RejectReason reason){
            ++telemetry_.strategy_intents_rejected;
            OmsResponse response{
                .context = context,
                .response_type = OmsResponseType::kREJECTED,
                .reject_reason = reason,
                .recv_ts_ns = recv_ts,
                .response_ts_ns = now_ns(),
            };
            (void)send_strategy_message(intent.context.strategy_index, OmsToStrategyMessage{response});
        };

        if(!trading_enabled_){
            reject(RejectReason::kTRADING_DISABLED);
            return;
        }
        if(intent.quantity_lots <= 0 ||
           intent.price_ticks <= 0 ||
           intent.outcome == intent::Outcome::kUNKNOWN ||
           intent.action == intent::OrderAction::kUNKNOWN){
            reject(RejectReason::kINVALID_INTENT);
            return;
        }

        ClientOrderId client_order_id = make_client_order_id(oms_request_id);
        if(client_order_id.empty()){
            reject(RejectReason::kOMS_NOT_READY);
            return;
        }

        SubmitOrderCmd command{
            .oms_request_id = oms_request_id,
            .client_order_id = client_order_id,
            .new_order_intent = intent,
            .submission_ts_ns = now_ns(),
        };
        if(!send_kalshi_command(OmsToKalshiCommand{command})){
            reject(RejectReason::kOMS_NOT_READY);
            return;
        }

        OrderRecord record{
            .context = context,
            .client_order_id = client_order_id,
            .order_state = OrderState::kPENDING_SUBMIT,
            .outcome = intent.outcome,
            .market_id = intent.context.market_id,
            .ordered_qty_lots = intent.quantity_lots,
            .working_price_ticks = intent.price_ticks,
            .leaves_qty_lots = intent.quantity_lots,
        };
        oms_request_id_to_order_record_map_.emplace(oms_request_id, record);
        client_order_id_to_oms_request_id_map_.emplace(client_order_id, oms_request_id);

        ++telemetry_.strategy_intents_processed;
        OmsResponse response{
            .context = context,
            .response_type = OmsResponseType::kACCEPTED,
            .reject_reason = RejectReason::kNONE,
            .recv_ts_ns = recv_ts,
            .response_ts_ns = now_ns(),
        };
        (void)send_strategy_message(intent.context.strategy_index, OmsToStrategyMessage{response});
    }

    void Oms::handle_strategy_intent(const intent::CancelOrderIntent& intent) noexcept{
        const std::uint64_t recv_ts = now_ns();
        const intent::OmsRequestId oms_request_id = next_oms_request_id_++;
        const OmsContext context = make_context(oms_request_id, intent.context);
        OrderRecord* target = find_order(intent.target_oms_request_id);

        auto respond = [&](OmsResponseType type, RejectReason reason){
            OmsResponse response{
                .context = context,
                .response_type = type,
                .reject_reason = reason,
                .recv_ts_ns = recv_ts,
                .response_ts_ns = now_ns(),
            };
            (void)send_strategy_message(intent.context.strategy_index, OmsToStrategyMessage{response});
        };

        if(target == nullptr){
            ++telemetry_.strategy_intents_rejected;
            respond(OmsResponseType::kREJECTED, RejectReason::kUNKNOWN_TARGET_ORDER);
            return;
        }

        if(!emit_cancel_command_for_record(*target, intent.context, oms_request_id, now_ns())){
            ++telemetry_.strategy_intents_rejected;
            respond(OmsResponseType::kREJECTED, RejectReason::kOMS_NOT_READY);
            return;
        }

        ++telemetry_.strategy_intents_processed;
        respond(OmsResponseType::kACCEPTED, RejectReason::kNONE);
    }

    void Oms::handle_strategy_intent(const intent::ModifyOrderIntent& intent) noexcept{
        const std::uint64_t recv_ts = now_ns();
        const intent::OmsRequestId oms_request_id = next_oms_request_id_++;
        const OmsContext context = make_context(oms_request_id, intent.context);
        OrderRecord* target = find_order(intent.target_oms_request_id);

        auto respond = [&](OmsResponseType type, RejectReason reason){
            OmsResponse response{
                .context = context,
                .response_type = type,
                .reject_reason = reason,
                .recv_ts_ns = recv_ts,
                .response_ts_ns = now_ns(),
            };
            (void)send_strategy_message(intent.context.strategy_index, OmsToStrategyMessage{response});
        };

        if(target == nullptr){
            ++telemetry_.strategy_intents_rejected;
            respond(OmsResponseType::kREJECTED, RejectReason::kUNKNOWN_TARGET_ORDER);
            return;
        }

        ModifyOrderCmd command{
            .oms_request_id = oms_request_id,
            .client_order_id = target->client_order_id,
            .exchange_order_id = target->exchange_order_id,
            .modify_order_intent = intent,
            .submission_ts_ns = now_ns(),
        };
        if(!send_kalshi_command(OmsToKalshiCommand{command})){
            ++telemetry_.strategy_intents_rejected;
            respond(OmsResponseType::kREJECTED, RejectReason::kOMS_NOT_READY);
            return;
        }

        target->previous_order_state = target->order_state;
        target->order_state = OrderState::kPENDING_MODIFY;
        target->pending_command_oms_request_id = oms_request_id;
        target->pending_command_kind = RestCommandKind::kMODIFY_ORDER;
        emit_order_state_update(*target, VenueEventSource::kOMS_INTERNAL, now_ns());
        ++telemetry_.strategy_intents_processed;
        respond(OmsResponseType::kACCEPTED, RejectReason::kNONE);
    }

    void Oms::handle_strategy_intent(const intent::GroupOrderIntent& intent) noexcept{
        const std::uint8_t leg_count = std::min(intent.leg_count, static_cast<std::uint8_t>(intent.new_orders.size()));
        for(std::uint8_t i = 0; i < leg_count; ++i){
            handle_strategy_intent(intent.new_orders[i]);
        }
    }

    void Oms::handle_venue_event(const RestOrderResponse& response) noexcept{
        ++telemetry_.rest_responses_seen;

        OrderRecord* record = find_order_for_rest_response(response);
        if(record == nullptr){
            return;
        }

        if(!response.exchange_order_id.empty()){
            record->exchange_order_id = response.exchange_order_id;
            exchange_order_id_to_oms_request_id_map_[response.exchange_order_id] = record->context.oms_request_id;
        }

        switch(response.result_code){
            case RestResultCode::kACKED:
                if(response.command_kind == RestCommandKind::kSUBMIT_ORDER){
                    record->order_state = OrderState::kWORKING;
                }else if(response.command_kind == RestCommandKind::kCANCEL_ORDER){
                    record->order_state = OrderState::kPENDING_CANCEL;
                }else if(response.command_kind == RestCommandKind::kMODIFY_ORDER){
                    record->order_state = OrderState::kPENDING_MODIFY;
                }
                clear_pending_command(*record);
                break;
            case RestResultCode::kREJECTED:
            case RestResultCode::kNOT_SENT:
                if(response.command_kind == RestCommandKind::kSUBMIT_ORDER){
                    record->order_state = OrderState::kREJECTED;
                    record->leaves_qty_lots = 0;
                    clear_pending_command(*record);
                }else if(response.command_kind == RestCommandKind::kCANCEL_ORDER ||
                         response.command_kind == RestCommandKind::kMODIFY_ORDER){
                    restore_previous_state(*record);
                }
                break;
            case RestResultCode::kTIMEOUT:
            case RestResultCode::kTRANSPORT_ERROR:
                record->previous_order_state = record->order_state;
                record->order_state = OrderState::kUNCERTAIN;
                break;
            case RestResultCode::kUNKNOWN:
                break;
        }

        emit_order_state_update(*record, VenueEventSource::kREST_RESPONSE, response.transport_recv_ts_ns);
    }

    void Oms::handle_venue_event(const PrivateWsOrderEvent& event) noexcept{
        ++telemetry_.private_ws_events_seen;

        OrderRecord* record = find_order(event.client_order_id);
        if(record == nullptr){
            record = find_order(event.exchange_order_id);
        }
        if(record == nullptr){
            return;
        }

        if(!event.exchange_order_id.empty()){
            record->exchange_order_id = event.exchange_order_id;
            exchange_order_id_to_oms_request_id_map_[event.exchange_order_id] = record->context.oms_request_id;
        }

        record->order_state = event.order_state;
        record->outcome = event.outcome;
        record->market_id = event.market_id;
        record->ordered_qty_lots = event.ordered_qty_lots;
        record->cumulative_filled_qty_lots = event.cumulative_filled_qty_lots;
        record->leaves_qty_lots = event.leaves_qty_lots;
        clear_pending_command(*record);
        emit_order_state_update(*record, VenueEventSource::kWEBSOCKET_FEED, event.recv_ts_ns);
    }

    void Oms::handle_venue_event(const ReconciledOrderSnapshot& /*snapshot*/) noexcept{
        ++telemetry_.reconciliation_events_seen;
    }

    void Oms::handle_venue_event(const OrderRestEgressDrained& drained) noexcept {
        pending_rest_egress_drained_ = core::control::OmsRestEgressDrained{
            .shutdown_epoch = drained.shutdown_epoch,
            .completion_ts_ns = drained.completion_ts_ns,
            .live_orders = live_order_count(),
            .uncertain_orders = uncertain_order_count(),
        };

        (void)try_send_pending_control_status();
    }

    bool Oms::try_send_pending_control_status() noexcept{
        if(!pending_rest_egress_drained_.has_value()){
            return false;
        }
        if(!send_control_status(core::control::OmsToControlStatus{*pending_rest_egress_drained_})){
            return false;
        }
        pending_rest_egress_drained_.reset();
        return true;
    }

    void Oms::handle_control_command(const core::control::AllowTrading& /*command*/) noexcept{
        trading_enabled_ = true;
        emit_trading_enabled_changed();
    }

    void Oms::handle_control_command(const core::control::DisableTrading& /*command*/) noexcept{
        trading_enabled_ = false;
        emit_trading_enabled_changed();
    }

    void Oms::handle_control_command(const core::control::FlattenAllOrders& /*command*/) noexcept{
        flatten_requested_ = true;
        trading_enabled_ = false;

        const std::uint64_t submission_ts = now_ns();
        std::uint64_t cancel_requests_sent{0};
        for(auto& [_, record] : oms_request_id_to_order_record_map_){
            if(emit_cancel_command_for_record(record, record.context.context, next_oms_request_id_++, submission_ts)){
                ++cancel_requests_sent;
            }
        }

        (void)send_control_status(core::control::OmsToControlStatus{
            core::control::OmsFlattenStateChanged{
                .flatten_requested = flatten_requested_,
                .live_orders = live_order_count()
            }
        });
        (void)cancel_requests_sent;
    }

    bool Oms::is_terminal(OrderState state) noexcept{
        return state == OrderState::kFILLED ||
               state == OrderState::kCANCELED ||
               state == OrderState::kREJECTED ||
               state == OrderState::kEXPIRED;
    }

    bool Oms::is_live(OrderState state) noexcept{
        return state != OrderState::kUNKNOWN && !is_terminal(state);
    }

    bool Oms::is_cancelable(OrderState state) noexcept{
        return state == OrderState::kPENDING_SUBMIT ||
               state == OrderState::kWORKING ||
               state == OrderState::kPARTIALLY_FILLED ||
               state == OrderState::kUNCERTAIN;
    }

    OrderRecord* Oms::find_order_for_rest_response(const RestOrderResponse& response) noexcept{
        if(!response.client_order_id.empty()){
            return find_order(response.client_order_id);
        }
        if(!response.exchange_order_id.empty()){
            return find_order(response.exchange_order_id);
        }
        if(OrderRecord* record = find_order(response.context.oms_request_id)){
            return record;
        }
        if(response.context.oms_request_id != 0){
            for(auto& [_, record] : oms_request_id_to_order_record_map_){
                if(record.pending_command_oms_request_id == response.context.oms_request_id){
                    return &record;
                }
            }
        }
        return nullptr;
    }

    bool Oms::emit_cancel_command_for_record(OrderRecord& record, intent::IntentContext context, intent::OmsRequestId command_oms_request_id, std::uint64_t submission_ts_ns) noexcept{
        if (!is_cancelable(record.order_state)) {
            return false;
        }

        CancelOrderCmd command{
            .oms_request_id = command_oms_request_id,
            .client_order_id = record.client_order_id,
            .exchange_order_id = record.exchange_order_id,
            .cancel_order_intent = intent::CancelOrderIntent{
                .context = context,
                .target_oms_request_id = record.context.oms_request_id,
            },
            .submission_ts_ns = submission_ts_ns,
        };

        if (!send_kalshi_command(OmsToKalshiCommand{command})) {
            return false;
        }

        record.previous_order_state = record.order_state;
        record.order_state = OrderState::kPENDING_CANCEL;
        record.pending_command_oms_request_id = command_oms_request_id;
        record.pending_command_kind = RestCommandKind::kCANCEL_ORDER;

        emit_order_state_update(record, VenueEventSource::kOMS_INTERNAL, submission_ts_ns);
        return true;
    }

    void Oms::clear_pending_command(OrderRecord& record) noexcept {
        record.pending_command_oms_request_id = 0;
        record.pending_command_kind = RestCommandKind::kUNKNOWN;
        record.previous_order_state = OrderState::kUNKNOWN;
    }

    void Oms::restore_previous_state(OrderRecord& record) noexcept {
        if(record.previous_order_state != OrderState::kUNKNOWN){
            record.order_state = record.previous_order_state;
        }

        clear_pending_command(record);
    }

    std::uint64_t Oms::live_order_count() const noexcept{
        std::uint64_t count{0};
        for(const auto& [_, record] : oms_request_id_to_order_record_map_){
            if(is_live(record.order_state)){
                ++count;
            }
        }
        return count;
    }

    std::uint64_t Oms::pending_submit_order_count()const noexcept{
        std::uint64_t count{0};
        for(const auto& [_, record] : oms_request_id_to_order_record_map_){
            if(record.order_state == OrderState::kPENDING_SUBMIT){
                ++count;
            }
        }
        return count;
    }

    std::uint64_t Oms::uncertain_order_count()const noexcept{
        std::uint64_t count{0};
        for(const auto& [_, record] : oms_request_id_to_order_record_map_){
            if(record.order_state == OrderState::kUNCERTAIN){
                ++count;
            }
        }
        return count;
    }
}
