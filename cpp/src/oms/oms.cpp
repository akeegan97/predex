#include "predex/oms/oms.hpp"
#include "predex/oms/oms_types.hpp"

#include <chrono>
#include <limits>

namespace predex::core::oms::kalshi{
    namespace {
        [[nodiscard]] std::int64_t latency_delta_ns(
            internal::TimestampNs end_ts_ns,
            internal::TimestampNs start_ts_ns) {
            if (end_ts_ns <= start_ts_ns) {
                return 0;
            }
            const auto raw_delta = end_ts_ns - start_ts_ns;
            constexpr auto max_i64 =
                static_cast<internal::TimestampNs>(std::numeric_limits<std::int64_t>::max());
            if (raw_delta > max_i64) {
                return std::numeric_limits<std::int64_t>::max();
            }
            return static_cast<std::int64_t>(raw_delta);
        }

        [[nodiscard]] internal::TimestampNs monotonic_now_ns() {
            return static_cast<internal::TimestampNs>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count());
        }

        // Interim in-process latency staging maps keyed by OMS request id.
        //NOLINTNEXTLINE
        static std::unordered_map<OmsRequestId, internal::TimestampNs> decision_ts_by_request_id{};
        //NOLINTNEXTLINE
        static std::unordered_map<OmsRequestId, internal::TimestampNs> transport_submit_ts_by_request_id{};
        //NOLINTNEXTLINE
        static std::unordered_map<OmsRequestId, internal::TimestampNs> first_fill_ts_by_request_id{};



        [[nodiscard]] OmsRequestId extract_oms_request_id(const IntentDecision& decision) {
            if (const auto* accepted = std::get_if<AcceptedIntent>(&decision.data)) {
                return accepted->oms_request_id;
            }
            if (const auto* modified = std::get_if<ModifiedIntent>(&decision.data)) {
                return modified->oms_request_id;
            }
            return 0;
        }

        [[nodiscard]] std::uint8_t extract_reject_reason(const IntentDecision& decision) {
            if (const auto* rejected = std::get_if<RejectedIntent>(&decision.data)) {
                return static_cast<std::uint8_t>(rejected->reason);
            }
            if (const auto* modified = std::get_if<ModifiedIntent>(&decision.data)) {
                return static_cast<std::uint8_t>(modified->reason);
            }
            return 0;
        }
    }
    
    Oms::Oms(std::vector<SubmissionQueue*> shard_intent_queues,
                 std::vector<DecisionQueue*> shard_decision_queues,
                 std::vector<LifecycleQueue*> shard_lifecycle_queues,
                 OmsTransportQueues transport_queues,
                 GlobalRiskManager global_risk,
                 AuditQueue* audit_queue)
                 //NOLINTNEXTLINE
        : global_risk_(std::move(global_risk)),
          shard_intent_queues_(std::move(shard_intent_queues)),
          shard_decision_queues_(std::move(shard_decision_queues)),
          shard_lifecycle_queues_(std::move(shard_lifecycle_queues)),
          //NOLINTNEXTLINE
          transport_queues_(std::move(transport_queues)),
          audit_queue_(audit_queue) {}
    
    //NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] OmsPumpResult Oms::pump(std::size_t max_transport_updates, std::size_t max_shard_intents) noexcept{
        OmsPumpResult result{};

        for(std::size_t i=0; i<max_transport_updates; i++){
            const OmsProcessCode code = process_one_transport_update();
            if(code == OmsProcessCode::kIdle){
                break;
            }
            result.code = code;
            if(code == OmsProcessCode::kProcessedTransportUpdate){
                ++result.processed_transport_updates;
                continue;
            }
            return result;
        }

        for(std::size_t i=0; i<max_shard_intents; i++){
            const OmsProcessCode code = process_one_shard_intent();
            if(code == OmsProcessCode::kIdle){
                if(result.processed_transport_updates > 0){
                    result.code = OmsProcessCode::kProcessedTransportUpdate;
                }
                break;
            }
            result.code = code;
            if(code == OmsProcessCode::kProcessedIntent){
                ++result.processed_intents;
                continue;
            }
            return result;
        }

        if(result.processed_intents > 0 && result.code == OmsProcessCode::kIdle){
            result.code = OmsProcessCode::kProcessedIntent;
        } else if(result.processed_transport_updates > 0 && result.code == OmsProcessCode::kIdle){
            result.code = OmsProcessCode::kProcessedTransportUpdate;
        }
        return result;
    }
    [[nodiscard]] const GlobalRiskState& Oms::global_risk_state() const noexcept{
        return global_risk_state_;
    }
    [[nodiscard]] std::size_t Oms::live_order_count() const noexcept{
        return orders_by_request_id_.size();
    }
    [[nodiscard]] std::uint64_t Oms::processed_intent_count() const noexcept{
        return processed_intent_count_;
    }
    [[nodiscard]] std::uint64_t Oms::processed_transport_update_count() const noexcept{
        return processed_transport_update_count_;
    }
    [[nodiscard]] std::uint64_t Oms::rejected_intent_count() const noexcept{
        return rejected_intent_count_;
    }


    [[nodiscard]] OmsProcessCode Oms::process_one_transport_update() noexcept{
        if(transport_queues_.inbound_update_queue == nullptr){
            return OmsProcessCode::kIdle;
        }
        OrderLifecycleEvent event{};
        if(!transport_queues_.inbound_update_queue->try_pop(event)){
            return OmsProcessCode::kIdle;
        }
        OrderState* const tracked_order = find_order_state(event);
        if (tracked_order == nullptr) {
            return OmsProcessCode::kError;
        }
        const OmsRequestId resolved_request_id = tracked_order->oms_request_id;
        const internal::TimestampNs decision_ts_ns =
            decision_ts_by_request_id.contains(resolved_request_id)
                ? decision_ts_by_request_id[resolved_request_id]
                : 0;
        const internal::TimestampNs transport_ts_ns =
            transport_submit_ts_by_request_id.contains(resolved_request_id)
                ? transport_submit_ts_by_request_id[resolved_request_id]
                : 0;

        if (std::holds_alternative<OrderFill>(event.data) &&
            !first_fill_ts_by_request_id.contains(resolved_request_id)) {
            first_fill_ts_by_request_id[resolved_request_id] = event.recv_ts_ns;
        }

        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kOmsLifecycle,
            .ts_ns = event.recv_ts_ns,
            .shard_id = event.origin.shard_id,
            .signal_id = event.origin.signal_id,
            .group_id = event.origin.group_id,
            .local_intent_id = event.origin.local_intent_id,
            .oms_request_id = resolved_request_id,
            .tick_recv_ns = tracked_order->origin.tick_recv_ns,
            .signal_ts_ns = tracked_order->origin.signal_ts_ns,
            .submission_enqueued_ns = tracked_order->origin.submission_enqueued_ns,
            .oms_decision_ts_ns = decision_ts_ns,
            .transport_submit_ts_ns = transport_ts_ns,
            .first_fill_recv_ns =
                first_fill_ts_by_request_id.contains(resolved_request_id)
                    ? first_fill_ts_by_request_id[resolved_request_id]
                    : 0,
            .terminal_recv_ns =
                (event.kind == OrderLifecycleEventKind::kReject ||
                 event.kind == OrderLifecycleEventKind::kCanceled ||
                 event.kind == OrderLifecycleEventKind::kFill)
                    ? event.recv_ts_ns
                    : 0,
            .decision_to_transport_ns = latency_delta_ns(transport_ts_ns, decision_ts_ns),
            .transport_to_first_fill_ns =
                first_fill_ts_by_request_id.contains(resolved_request_id)
                    ? latency_delta_ns(first_fill_ts_by_request_id[resolved_request_id], transport_ts_ns)
                    : 0,
            .tick_to_first_fill_ns =
                first_fill_ts_by_request_id.contains(resolved_request_id)
                    ? latency_delta_ns(first_fill_ts_by_request_id[resolved_request_id],
                                       tracked_order->origin.tick_recv_ns)
                    : 0,
            .tick_to_terminal_ns =
                (event.kind == OrderLifecycleEventKind::kReject ||
                 event.kind == OrderLifecycleEventKind::kCanceled ||
                 event.kind == OrderLifecycleEventKind::kFill)
                    ? latency_delta_ns(event.recv_ts_ns, tracked_order->origin.tick_recv_ns)
                    : 0,
            .exchange = internal::ExchangeId::kKalshi,
            .event_id = event.origin.event_id,
            .market_id = event.origin.market_id,
            .side = internal::Side::kUnknown,
            .leg_index = event.origin.leg_index,
            .leg_count = event.origin.leg_count,
            .qty_lots = 0,
            .price_ticks = 0,
            .decision_code = 0,
            .reject_reason = 0,
            .lifecycle_kind = static_cast<std::uint8_t>(event.kind),
            .order_status = static_cast<std::uint8_t>(event.status),
        });
        if(!apply_transport_update(event)){
            return OmsProcessCode::kError;
        }

        if (event.kind == OrderLifecycleEventKind::kReject ||
            event.kind == OrderLifecycleEventKind::kCanceled ||
            event.kind == OrderLifecycleEventKind::kFill) {
            decision_ts_by_request_id.erase(resolved_request_id);
            transport_submit_ts_by_request_id.erase(resolved_request_id);
            first_fill_ts_by_request_id.erase(resolved_request_id);
        }

        if(!emit_lifecycle_event(event)){
            return OmsProcessCode::kShardBackpressure;
        }
        ++processed_transport_update_count_;
        return OmsProcessCode::kProcessedTransportUpdate;
    }

    [[nodiscard]] OmsProcessCode Oms::process_one_shard_intent() noexcept {
        if (shard_intent_queues_.empty()) {
            return OmsProcessCode::kIdle;
        }

        for (std::size_t scanned = 0; scanned < shard_intent_queues_.size(); ++scanned) {
            const std::size_t shard_index =
                (next_shard_index_ + scanned) % shard_intent_queues_.size();
            SubmissionQueue* const queue = shard_intent_queues_[shard_index];
            if (queue == nullptr) {
                continue;
            }

            OmsSubmission submission{};
            if (!queue->try_pop(submission)) {
                continue;
            }

            next_shard_index_ = (shard_index + 1) % shard_intent_queues_.size();
            //NOLINTNEXTLINE
            const OmsProcessCode code = process_submission(std::move(submission));
            if (code == OmsProcessCode::kProcessedIntent) {
                ++processed_intent_count_;
            }
            return code;
        }

        return OmsProcessCode::kIdle;
    }

    [[nodiscard]] OmsProcessCode Oms::process_submission(OmsSubmission submission) noexcept {
        if (auto* intent = std::get_if<OrderIntent>(&submission)) {
            //NOLINTNEXTLINE
            return process_intent(std::move(*intent));
        }

        auto* group = std::get_if<GroupOrderIntent>(&submission);
        if (group == nullptr || group->leg_count == 0 ||
            group->leg_count > group->legs.size()) {
            return OmsProcessCode::kError;
        }

        for (std::size_t leg_index = 0; leg_index < group->leg_count; ++leg_index) {
            OrderIntent leg = group->legs[leg_index];
            leg.origin.group_id = group->group_id;
            leg.origin.leg_index = static_cast<std::uint16_t>(leg_index);
            leg.origin.leg_count = static_cast<std::uint16_t>(group->leg_count);

            const std::uint64_t rejected_before = rejected_intent_count_;
            //NOLINTNEXTLINE
            const OmsProcessCode code = process_intent(std::move(leg));
            if (code != OmsProcessCode::kProcessedIntent) {
                return code;
            }
            if (group->execution_policy ==
                    GroupExecutionPolicy::kAbortRemainingOnReject &&
                rejected_intent_count_ > rejected_before) {
                break;
            }
        }

        return OmsProcessCode::kProcessedIntent;
    }

    [[nodiscard]] OmsProcessCode Oms::process_intent(OrderIntent intent) noexcept{
        const OmsRequestId oms_request_id = next_oms_request_id_++;
        const ClientOrderId client_order_id = make_client_order_id(oms_request_id);
        const internal::TimestampNs signal_ts_ns =
            intent.origin.signal_ts_ns != 0 ? intent.origin.signal_ts_ns : intent.intent_ts_ns;
        const internal::TimestampNs submission_ts_ns =
            intent.origin.submission_enqueued_ns != 0
                ? intent.origin.submission_enqueued_ns
                : signal_ts_ns;
        const internal::TimestampNs tick_recv_ts_ns =
            intent.origin.tick_recv_ns != 0 ? intent.origin.tick_recv_ns : signal_ts_ns;
        const internal::TimestampNs decision_ts_ns = monotonic_now_ns();
        const std::int64_t submission_to_decision_ns =
            latency_delta_ns(decision_ts_ns, submission_ts_ns);

        IntentDecision decision =
            global_risk_.evaluate(intent, global_risk_state_, oms_request_id, client_order_id,
                                  decision_ts_ns);

        if (decision.code == IntentDecisionCode::kAccepted) {
            const auto* accepted_intent = std::get_if<AcceptedIntent>(&decision.data);
            if (accepted_intent == nullptr) {
                return OmsProcessCode::kError;
            }

            const SubmitOrderCmd submit_cmd{
                .oms_request_id = accepted_intent->oms_request_id,
                .intent = accepted_intent->intent,
                .client_order_id = accepted_intent->client_order_id,
            };

            if (transport_queues_.submit_queue == nullptr ||
                !transport_queues_.submit_queue->try_push(submit_cmd)) {
                decision = make_transport_reject(intent, decision_ts_ns);
            } else {
                const internal::TimestampNs transport_submit_ts_ns = monotonic_now_ns();
                decision_ts_by_request_id[accepted_intent->oms_request_id] = decision_ts_ns;
                transport_submit_ts_by_request_id[accepted_intent->oms_request_id] =
                    transport_submit_ts_ns;
                emit_audit(predex::core::audit::AuditEvent{
                    .kind = predex::core::audit::AuditKind::kOmsTransport,
                    .ts_ns = transport_submit_ts_ns,
                    .shard_id = accepted_intent->intent.origin.shard_id,
                    .signal_id = accepted_intent->intent.origin.signal_id,
                    .group_id = accepted_intent->intent.origin.group_id,
                    .local_intent_id = accepted_intent->intent.origin.local_intent_id,
                    .oms_request_id = accepted_intent->oms_request_id,
                    .tick_recv_ns = tick_recv_ts_ns,
                    .signal_ts_ns = signal_ts_ns,
                    .submission_enqueued_ns = submission_ts_ns,
                    .oms_decision_ts_ns = decision_ts_ns,
                    .transport_submit_ts_ns = transport_submit_ts_ns,
                    .submission_to_decision_ns =
                        latency_delta_ns(decision_ts_ns, submission_ts_ns),
                    .decision_to_transport_ns =
                        latency_delta_ns(transport_submit_ts_ns, decision_ts_ns),
                    .exchange = accepted_intent->intent.exchange,
                    .event_id = accepted_intent->intent.origin.event_id,
                    .market_id = accepted_intent->intent.origin.market_id,
                    .side = accepted_intent->intent.side,
                    .leg_index = accepted_intent->intent.origin.leg_index,
                    .leg_count = accepted_intent->intent.origin.leg_count,
                    .qty_lots = accepted_intent->intent.qty_lots,
                    .price_ticks = accepted_intent->intent.limit_price_ticks.value_or(0),
                });
                global_risk_.on_intent_accepted(*accepted_intent, global_risk_state_);
                insert_live_order(*accepted_intent, decision_ts_ns);
            }
        }

        const IntentOrigin audit_origin = extract_origin(decision);
        emit_audit(predex::core::audit::AuditEvent{
            .kind = predex::core::audit::AuditKind::kOmsDecision,
            .ts_ns = decision.decision_ts_ns,
            .shard_id = audit_origin.shard_id,
            .signal_id = audit_origin.signal_id,
            .group_id = audit_origin.group_id,
            .local_intent_id = audit_origin.local_intent_id,
            .oms_request_id = extract_oms_request_id(decision),
            .tick_recv_ns = tick_recv_ts_ns,
            .signal_ts_ns = signal_ts_ns,
            .submission_enqueued_ns = submission_ts_ns,
            .oms_decision_ts_ns = decision.decision_ts_ns,
            .submission_to_decision_ns = submission_to_decision_ns,
            .exchange = intent.exchange,
            .event_id = audit_origin.event_id,
            .market_id = audit_origin.market_id,
            .side = intent.side,
            .leg_index = audit_origin.leg_index,
            .leg_count = audit_origin.leg_count,
            .qty_lots = intent.qty_lots,
            .price_ticks = intent.limit_price_ticks.value_or(0),
            .decision_code = static_cast<std::uint8_t>(decision.code),
            .reject_reason = extract_reject_reason(decision),
        });

        if (!emit_intent_decision(decision)) {
            return OmsProcessCode::kShardBackpressure;
        }

        if (decision.code == IntentDecisionCode::kRejected) {
            ++rejected_intent_count_;
        }

        return OmsProcessCode::kProcessedIntent;
    }
   //NOLINTNEXTLINE
    [[nodiscard]] IntentDecision Oms::make_transport_reject(
        const OrderIntent& intent,
        internal::TimestampNs decision_ts_ns) const{
            return IntentDecision{
            .code = IntentDecisionCode::kRejected,
            .data = RejectedIntent{
                .intent = intent,
                .reason = IntentRejectReason::kVenueUnavailable,
            },
            .decision_ts_ns = decision_ts_ns,
        };
    }

    [[nodiscard]] bool Oms::emit_intent_decision(const IntentDecision& decision) noexcept{
        const auto shard_index = shard_index_for_origin(extract_origin(decision));
        if(!shard_index.has_value()){
            return false;
        }
        DecisionQueue* const queue = shard_decision_queues_[*shard_index];
        return queue != nullptr && queue->try_push(decision);
    }

    [[nodiscard]] bool Oms::emit_lifecycle_event(const OrderLifecycleEvent& event) noexcept{
        const auto shard_index = shard_index_for_origin(event.origin);
        if(!shard_index.has_value()){
            return false;
        }
        LifecycleQueue* const queue = shard_lifecycle_queues_[*shard_index];
        return queue != nullptr && queue->try_push(event);
    }

    void Oms::emit_audit(const predex::core::audit::AuditEvent& event) noexcept {
        if (audit_queue_ == nullptr) {
            return;
        }
        static_cast<void>(audit_queue_->try_push(event));
    }

    [[nodiscard]] IntentOrigin Oms::extract_origin(const IntentDecision& decision) {
        if (const auto* accepted = std::get_if<AcceptedIntent>(&decision.data)) {
            return accepted->intent.origin;
        }
        if (const auto* rejected = std::get_if<RejectedIntent>(&decision.data)) {
            return rejected->intent.origin;
        }
        if (const auto* modified = std::get_if<ModifiedIntent>(&decision.data)) {
            return modified->modified_intent.origin;
        }
        return {};
    }

    [[nodiscard]] std::optional<std::size_t> Oms::shard_index_for_origin(
        const IntentOrigin& origin) const noexcept {
        if (origin.shard_id >= shard_decision_queues_.size() ||
            origin.shard_id >= shard_lifecycle_queues_.size()) {
            return std::nullopt;
        }
        return static_cast<std::size_t>(origin.shard_id);
    }

    [[nodiscard]] ClientOrderId Oms::make_client_order_id(OmsRequestId oms_request_id){
        return "predex-" + std::to_string(oms_request_id) + "-" +
            std::to_string(next_client_order_seq_++);
    }

    void Oms::insert_live_order(const AcceptedIntent& accepted_intent,
                           internal::TimestampNs decision_ts_ns) {
        OrderState order_state{
            .origin = accepted_intent.intent.origin,
            .oms_request_id = accepted_intent.oms_request_id,
            .status = OmsOrderStatus::kPendingSubmit,
            .client_order_id = accepted_intent.client_order_id,
            .exchange_order_id = std::nullopt,
            .original_qty_lots = accepted_intent.intent.qty_lots,
            .live_qty_lots = accepted_intent.intent.qty_lots,
            .cum_fill_qty_lots = 0,
            .live_limit_price_ticks = accepted_intent.intent.limit_price_ticks,
            .last_update_ts_ns = decision_ts_ns,
            .oms_decision_ts_ns = decision_ts_ns,
            .transport_submit_ts_ns = decision_ts_ns,
            .first_fill_recv_ns = 0,
            .terminal_recv_ns = 0,
        };
        request_by_client_order_id_[order_state.client_order_id] = order_state.oms_request_id;
        orders_by_request_id_[order_state.oms_request_id] = std::move(order_state);
    }

    [[nodiscard]] bool Oms::apply_transport_update(const OrderLifecycleEvent& event){
        OrderState* const order_state = find_order_state(event);
        if (order_state == nullptr) {
            return false;
        }

        order_state->status = event.status;
        order_state->last_update_ts_ns = event.recv_ts_ns;
        if (event.exchange_order_id.has_value()) {
            order_state->exchange_order_id = event.exchange_order_id;
            request_by_exchange_order_id_[*event.exchange_order_id] = order_state->oms_request_id;
        }

        if (const auto* fill = std::get_if<OrderFill>(&event.data)) {
            order_state->cum_fill_qty_lots += fill->fill_qty_lots;
            order_state->live_qty_lots -= fill->fill_qty_lots;
            if (order_state->live_qty_lots < 0) {
                order_state->live_qty_lots = 0;
            }
            if (order_state->first_fill_recv_ns == 0) {
                order_state->first_fill_recv_ns = event.recv_ts_ns;
            }
        }
        if (const auto* replace_ack = std::get_if<ReplaceAck>(&event.data)) {
            order_state->live_qty_lots = replace_ack->replaced_qty_lots;
            order_state->live_limit_price_ticks = replace_ack->replaced_limit_price_ticks;
        }
        if (event.kind == OrderLifecycleEventKind::kReject ||
            event.kind == OrderLifecycleEventKind::kCanceled ||
            event.kind == OrderLifecycleEventKind::kFill) {
            order_state->live_qty_lots = 0;
            order_state->terminal_recv_ns = event.recv_ts_ns;
        }

        return true;
    }

    [[nodiscard]] OrderState* Oms::find_order_state(const OrderLifecycleEvent& event) {
        auto by_request = orders_by_request_id_.find(event.oms_request_id);
        if (by_request != orders_by_request_id_.end()) {
            return &by_request->second;
        }
        if (!event.client_order_id.empty()) {
            auto by_client = request_by_client_order_id_.find(event.client_order_id);
            if (by_client != request_by_client_order_id_.end()) {
                auto by_mapped_request = orders_by_request_id_.find(by_client->second);
                if (by_mapped_request != orders_by_request_id_.end()) {
                    return &by_mapped_request->second;
                }
            }
        }
        if (event.exchange_order_id.has_value()) {
            auto by_exchange = request_by_exchange_order_id_.find(*event.exchange_order_id);
            if (by_exchange != request_by_exchange_order_id_.end()) {
                auto by_mapped_request = orders_by_request_id_.find(by_exchange->second);
                if (by_mapped_request != orders_by_request_id_.end()) {
                    return &by_mapped_request->second;
                }
            }
        }
        return nullptr;
    }
}
