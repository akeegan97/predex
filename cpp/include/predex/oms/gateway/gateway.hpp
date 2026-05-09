#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "predex/oms/gateway/batch_planner.hpp"
#include "predex/oms/gateway/command_ingress.hpp"
#include "predex/oms/gateway/gateway_types.hpp"
#include "predex/oms/gateway/order_sequencer.hpp"
#include "predex/oms/gateway/rate_limiter.hpp"
#include "predex/oms/gateway/session_pool.hpp"
#include "predex/utils/spsc_queue.hpp"

namespace predex::core::oms::kalshi::gateway {

// Top-level transport/gateway orchestrator between OMS and venue I/O resources.
//
// Ownership:
// - owned by app/runtime wiring
// - owns transport-internal policy components by composition
//
// Responsibilities:
// - drain OMS command ingress
// - transform OMS commands into gateway dispatch objects
// - sequence by lineage
// - plan singleton/batched dispatch requests
// - apply rate-limit admission
// - hand admitted requests to SessionPool
// - merge REST/private-WS/recovery truth
// - emit normalized venue events back toward OMS
struct GatewayQueues {
    utils::SPSCQueue<OmsToKalshiCommand>* oms_command_queue{nullptr};
    utils::SPSCQueue<KalshiToOmsEvent>* venue_event_queue{nullptr};
};

struct GatewayConfig {
    constexpr static std::size_t kDefaultHotQueueCapacity = 1024;
    constexpr static std::size_t kDefaultRecoveryQueueCapacity = 256;
    constexpr static std::size_t kDefaultReconcileQueueCapacity = 256;
    constexpr static std::size_t kDefaultPostWriteRecoveryAttempts = 3;
    constexpr static std::uint64_t kDefaultPostWriteRecoveryBackoffMs = 25;
    std::size_t hot_queue_capacity{kDefaultHotQueueCapacity};
    std::size_t recovery_queue_capacity{kDefaultRecoveryQueueCapacity};
    std::size_t reconcile_queue_capacity{kDefaultReconcileQueueCapacity};
    std::size_t post_write_recovery_attempts{kDefaultPostWriteRecoveryAttempts};
    std::uint64_t post_write_recovery_backoff_ms{kDefaultPostWriteRecoveryBackoffMs};
    std::size_t post_write_recovery_fetch_limit{
        predex::core::oms::kalshi::transport::kDefaultOpenOrderFetchLimit};
};

struct GatewayTelemetry {
    std::uint64_t sequenced_items{0};
    std::uint64_t rate_limited_requests{0};
    std::uint64_t submitted_to_session_pool{0};
    std::uint64_t session_pool_backpressure{0};
    std::uint64_t emitted_venue_events{0};
    std::uint64_t venue_event_backpressure{0};
    std::uint64_t completed_requests{0};
    std::uint64_t uncertain_requests{0};
    std::uint64_t total_completion_latency_ns{0};
    std::uint64_t last_completion_latency_ns{0};
    std::uint64_t total_ingress_to_sequence_ns{0};
    std::uint64_t last_ingress_to_sequence_ns{0};
    std::uint64_t total_sequence_to_plan_ns{0};
    std::uint64_t last_sequence_to_plan_ns{0};
    std::uint64_t total_queue_to_plan_ns{0};
    std::uint64_t last_queue_to_plan_ns{0};
    std::uint64_t total_plan_to_admit_ns{0};
    std::uint64_t last_plan_to_admit_ns{0};
    std::uint64_t total_admit_to_start_ns{0};
    std::uint64_t last_admit_to_start_ns{0};
    std::uint64_t total_start_to_wire_ns{0};
    std::uint64_t last_start_to_wire_ns{0};
    std::uint64_t reused_connection_requests{0};
    std::uint64_t cold_connection_requests{0};
    std::uint64_t total_cold_setup_ns{0};
    std::uint64_t last_cold_setup_ns{0};
    std::uint64_t total_wire_to_response_ns{0};
    std::uint64_t last_wire_to_response_ns{0};
    std::uint64_t recovery_attempts{0};
    std::uint64_t recovery_resolved_requests{0};
    std::uint64_t recovery_resolved_items{0};
    std::uint64_t recovery_uncertain_items{0};
    std::uint64_t recovery_failures{0};
};

class Gateway {
  public:
    explicit Gateway(
        GatewayQueues queues, SessionPool session_pool,
        std::optional<transport::KalshiRestAdapter> recovery_rest_adapter = std::nullopt,
        GatewayConfig config = {})
        : queues_(queues),
          hot_envelope_queue_(std::make_unique<EnvelopeQueue>(config.hot_queue_capacity)),
          recovery_envelope_queue_(std::make_unique<EnvelopeQueue>(config.recovery_queue_capacity)),
          reconcile_envelope_queue_(
              std::make_unique<EnvelopeQueue>(config.reconcile_queue_capacity)),
          hot_item_queue_(std::make_unique<ItemQueue>(config.hot_queue_capacity)),
          recovery_item_queue_(std::make_unique<ItemQueue>(config.recovery_queue_capacity)),
          reconcile_item_queue_(std::make_unique<ItemQueue>(config.reconcile_queue_capacity)),
          hot_request_queue_(std::make_unique<RequestQueue>(config.hot_queue_capacity)),
          recovery_request_queue_(std::make_unique<RequestQueue>(config.recovery_queue_capacity)),
          reconcile_request_queue_(std::make_unique<RequestQueue>(config.reconcile_queue_capacity)),
          admitted_hot_request_queue_(std::make_unique<RequestQueue>(config.hot_queue_capacity)),
          admitted_recovery_request_queue_(
              std::make_unique<RequestQueue>(config.recovery_queue_capacity)),
          admitted_reconcile_request_queue_(
              std::make_unique<RequestQueue>(config.reconcile_queue_capacity)),
          command_ingress_(CommandIngressQueues{
              .oms_command_queue = queues_.oms_command_queue,
              .session_class_queues =
                  SessionClassQueues{
                      .hot_queue = hot_envelope_queue_.get(),
                      .recovery_queue = recovery_envelope_queue_.get(),
                      .reconcile_queue = reconcile_envelope_queue_.get(),
                  },
          }),
          order_sequencer_(OrderSequencerQueues{
              .hot_input_queue = hot_envelope_queue_.get(),
              .recovery_input_queue = recovery_envelope_queue_.get(),
              .reconcile_input_queue = reconcile_envelope_queue_.get(),
              .hot_output_queue = hot_item_queue_.get(),
              .recovery_output_queue = recovery_item_queue_.get(),
              .reconcile_output_queue = reconcile_item_queue_.get(),
          }),
          batch_planner_(BatchPlannerQueues{
              .hot_input_queue = hot_item_queue_.get(),
              .recovery_input_queue = recovery_item_queue_.get(),
              .reconcile_input_queue = reconcile_item_queue_.get(),
              .hot_output_queue = hot_request_queue_.get(),
              .recovery_output_queue = recovery_request_queue_.get(),
              .reconcile_output_queue = reconcile_request_queue_.get(),
          }),
          rate_limiter_(RateLimiterQueues{
              .hot_input_queue = hot_request_queue_.get(),
              .recovery_input_queue = recovery_request_queue_.get(),
              .reconcile_input_queue = reconcile_request_queue_.get(),
              .hot_output_queue = admitted_hot_request_queue_.get(),
              .recovery_output_queue = admitted_recovery_request_queue_.get(),
              .reconcile_output_queue = admitted_reconcile_request_queue_.get(),
          }),
          session_pool_(std::move(session_pool)),
          recovery_rest_adapter_(std::move(recovery_rest_adapter)), config_(config) {}

    [[nodiscard]] bool pump_once() {
        bool made_progress = false;
        made_progress = session_pool_.drain_completions() || made_progress;
        made_progress = flush_session_pool_completions() || made_progress;
        made_progress = command_ingress_.drain_one() || made_progress;
        made_progress = order_sequencer_.drain_one() || made_progress;
        made_progress = batch_planner_.drain_one() || made_progress;
        made_progress = rate_limiter_.drain_one() || made_progress;
        made_progress = dispatch_one() || made_progress;
        made_progress = flush_venue_events() || made_progress;
        return made_progress;
    }

    [[nodiscard]] CommandIngress& command_ingress() noexcept { return command_ingress_; }
    [[nodiscard]] OrderSequencer& order_sequencer() noexcept { return order_sequencer_; }
    [[nodiscard]] BatchPlanner& batch_planner() noexcept { return batch_planner_; }
    [[nodiscard]] RateLimiter& rate_limiter() noexcept { return rate_limiter_; }
    [[nodiscard]] SessionPool& session_pool() noexcept { return session_pool_; }
    [[nodiscard]] std::size_t warm_up_sessions() noexcept { return session_pool_.warm_up(); }
    void keep_warm_sessions() noexcept { session_pool_.keep_warm(); }
    [[nodiscard]] const GatewayTelemetry& telemetry() const noexcept { return telemetry_; }

  private:
    using EnvelopeQueue = utils::SPSCQueue<CommandEnvelope>;
    using ItemQueue = utils::SPSCQueue<DispatchItem>;
    using RequestQueue = utils::SPSCQueue<DispatchRequest>;

    GatewayQueues queues_{};
    std::unique_ptr<EnvelopeQueue> hot_envelope_queue_;
    std::unique_ptr<EnvelopeQueue> recovery_envelope_queue_;
    std::unique_ptr<EnvelopeQueue> reconcile_envelope_queue_;
    std::unique_ptr<ItemQueue> hot_item_queue_;
    std::unique_ptr<ItemQueue> recovery_item_queue_;
    std::unique_ptr<ItemQueue> reconcile_item_queue_;
    std::unique_ptr<RequestQueue> hot_request_queue_;
    std::unique_ptr<RequestQueue> recovery_request_queue_;
    std::unique_ptr<RequestQueue> reconcile_request_queue_;
    std::unique_ptr<RequestQueue> admitted_hot_request_queue_;
    std::unique_ptr<RequestQueue> admitted_recovery_request_queue_;
    std::unique_ptr<RequestQueue> admitted_reconcile_request_queue_;
    CommandIngress command_ingress_;
    OrderSequencer order_sequencer_;
    BatchPlanner batch_planner_;
    RateLimiter rate_limiter_;
    SessionPool session_pool_;
    std::optional<transport::KalshiRestAdapter> recovery_rest_adapter_;
    GatewayConfig config_{};
    GatewayTelemetry telemetry_{};

    std::deque<DispatchRequest> pending_hot_;
    std::deque<DispatchRequest> pending_recovery_;
    std::deque<DispatchRequest> pending_reconcile_;
    std::deque<SessionPoolCompletion> pending_completions_;
    std::deque<KalshiToOmsEvent> pending_venue_events_;

    [[nodiscard]] bool dispatch_one() {
        DispatchRequest request;
        if (try_take_admitted_request(DispatchClass::kHot, request) ||
            try_take_admitted_request(DispatchClass::kRecovery, request) ||
            try_take_admitted_request(DispatchClass::kReconcile, request)) {
            ++telemetry_.rate_limited_requests;
            const auto request_item_count = request.item_count(); // for telemetry only, since submit takes ownership and could move request 
            switch (session_pool_.submit(request)) {
            case SessionPoolSubmitResult::kAccepted:
                telemetry_.sequenced_items += request_item_count;
                ++telemetry_.submitted_to_session_pool;
                return true;
            case SessionPoolSubmitResult::kNoIdleConnection:
            case SessionPoolSubmitResult::kPoolUnavailable:
                requeue_request(std::move(request));
                ++telemetry_.session_pool_backpressure;
                return false;
            case SessionPoolSubmitResult::kInvalidRequest:
                release_request_lineages(request);
                return true;
            }
        }
        return false;
    }
//NOLINTNEXTLINE 
    [[nodiscard]] bool flush_session_pool_completions() {
        bool made_progress = false;
        while (auto completion = session_pool_.poll_completion()) {
            pending_completions_.push_back(std::move(*completion));
            made_progress = true;
        }

        while (!pending_completions_.empty()) {
            auto& completion = pending_completions_.front();
            maybe_recover_post_write_unknown(completion.completion);
            release_request_lineages(completion.completion.request);
            for (auto& event : completion.completion.emitted_events) {
                pending_venue_events_.push_back(std::move(event));
            }
            ++telemetry_.completed_requests;
            if (completion.completion.terminal_state == DispatchRequestState::kPostWriteUnknown) {
                ++telemetry_.uncertain_requests;
            }
            const auto completion_latency_ns =
                completion.completion.completed_ts_ns >= completion.completion.request.queued_ts_ns
                    ? completion.completion.completed_ts_ns -
                          completion.completion.request.queued_ts_ns
                    : 0;
            const auto first_item_sequenced_ts_ns =
                !completion.completion.request.items.empty()
                    ? completion.completion.request.items.front().sequenced_ts_ns
                    : 0;
            const auto ingress_to_sequence_ns =
                first_item_sequenced_ts_ns >= completion.completion.request.queued_ts_ns
                    ? first_item_sequenced_ts_ns - completion.completion.request.queued_ts_ns
                    : 0;
            const auto sequence_to_plan_ns =
                completion.completion.request.planned_ts_ns >= first_item_sequenced_ts_ns
                    ? completion.completion.request.planned_ts_ns - first_item_sequenced_ts_ns
                    : 0;
            const auto queue_to_plan_ns = completion.completion.request.planned_ts_ns >=
                                                  completion.completion.request.queued_ts_ns
                                              ? completion.completion.request.planned_ts_ns -
                                                    completion.completion.request.queued_ts_ns
                                              : 0;
            const auto plan_to_admit_ns = completion.completion.request.admitted_ts_ns >=
                                                  completion.completion.request.planned_ts_ns
                                              ? completion.completion.request.admitted_ts_ns -
                                                    completion.completion.request.planned_ts_ns
                                              : 0;
            const auto admit_to_start_ns =
                completion.completion.request.connection_start_ts_ns >=
                        completion.completion.request.admitted_ts_ns
                    ? completion.completion.request.connection_start_ts_ns -
                          completion.completion.request.admitted_ts_ns
                    : 0;
            const auto start_to_wire_ns =
                completion.completion.trace.has_value() &&
                        completion.completion.trace->request_sent_ts_ns >=
                            completion.completion.request.connection_start_ts_ns
                    ? completion.completion.trace->request_sent_ts_ns -
                          completion.completion.request.connection_start_ts_ns
                    : 0;
            const auto cold_setup_ns =
                completion.completion.trace.has_value() &&
                        !completion.completion.trace->reused_connection
                    ? ((completion.completion.trace->resolve_end_ts_ns >=
                                completion.completion.trace->resolve_start_ts_ns
                            ? completion.completion.trace->resolve_end_ts_ns -
                                  completion.completion.trace->resolve_start_ts_ns
                            : 0) +
                       (completion.completion.trace->connect_end_ts_ns >=
                                completion.completion.trace->connect_start_ts_ns
                            ? completion.completion.trace->connect_end_ts_ns -
                                  completion.completion.trace->connect_start_ts_ns
                            : 0) +
                       (completion.completion.trace->handshake_end_ts_ns >=
                                completion.completion.trace->handshake_start_ts_ns
                            ? completion.completion.trace->handshake_end_ts_ns -
                                  completion.completion.trace->handshake_start_ts_ns
                            : 0))
                    : 0;
            const auto wire_to_response_ns =
                completion.completion.trace.has_value() &&
                        completion.completion.trace->response_recv_ts_ns >=
                            completion.completion.trace->request_sent_ts_ns
                    ? completion.completion.trace->response_recv_ts_ns -
                          completion.completion.trace->request_sent_ts_ns
                    : 0;
            telemetry_.last_completion_latency_ns = completion_latency_ns;
            telemetry_.total_completion_latency_ns += completion_latency_ns;
            telemetry_.last_ingress_to_sequence_ns = ingress_to_sequence_ns;
            telemetry_.total_ingress_to_sequence_ns += ingress_to_sequence_ns;
            telemetry_.last_sequence_to_plan_ns = sequence_to_plan_ns;
            telemetry_.total_sequence_to_plan_ns += sequence_to_plan_ns;
            telemetry_.last_queue_to_plan_ns = queue_to_plan_ns;
            telemetry_.total_queue_to_plan_ns += queue_to_plan_ns;
            telemetry_.last_plan_to_admit_ns = plan_to_admit_ns;
            telemetry_.total_plan_to_admit_ns += plan_to_admit_ns;
            telemetry_.last_admit_to_start_ns = admit_to_start_ns;
            telemetry_.total_admit_to_start_ns += admit_to_start_ns;
            telemetry_.last_start_to_wire_ns = start_to_wire_ns;
            telemetry_.total_start_to_wire_ns += start_to_wire_ns;
            telemetry_.last_wire_to_response_ns = wire_to_response_ns;
            telemetry_.total_wire_to_response_ns += wire_to_response_ns;
            if (completion.completion.trace.has_value()) {
                if (completion.completion.trace->reused_connection) {
                    ++telemetry_.reused_connection_requests;
                } else {
                    ++telemetry_.cold_connection_requests;
                    telemetry_.last_cold_setup_ns = cold_setup_ns;
                    telemetry_.total_cold_setup_ns += cold_setup_ns;
                }
            }
            pending_completions_.pop_front();
            made_progress = true;
        }
        return made_progress;
    }

    [[nodiscard]] static internal::TimestampNs recovery_now_ns() noexcept {
        return static_cast<internal::TimestampNs>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }

    [[nodiscard]] static bool parse_non_negative_dollars_to_ticks(std::string_view value,
                                                                  internal::PriceTicks& out_ticks) {
        constexpr auto kScaleFactor = 10U;
        constexpr auto kDollarToTicksScale =
            static_cast<std::uint64_t>(internal::kPriceTicksPerDollar);
        if (value.empty() || value.front() == '-' || value.front() == '+') {
            return false;
        }
        const std::size_t dot_pos = value.find('.');
        const std::string_view int_part =
            dot_pos == std::string_view::npos ? value : value.substr(0, dot_pos);
        const std::string_view frac_part =
            dot_pos == std::string_view::npos ? std::string_view{} : value.substr(dot_pos + 1);
        if (int_part.empty()) {
            return false;
        }

        std::uint64_t dollars = 0;
        for (const char digit_char : int_part) {
            if (digit_char < '0' || digit_char > '9') {
                return false;
            }
            dollars = dollars * kScaleFactor + static_cast<std::uint64_t>(digit_char - '0');
        }

        std::uint64_t subcent_units = 0;
        const std::size_t digits_to_take =
            std::min<std::size_t>(frac_part.size(), internal::kPriceDecimalPlaces);
        for (std::size_t index = 0; index < digits_to_take; ++index) {
            const char frac_char = frac_part[index];
            if (frac_char < '0' || frac_char > '9') {
                return false;
            }
            subcent_units = subcent_units * kScaleFactor + static_cast<std::uint64_t>(frac_char - '0');
        }
        for (std::size_t index = digits_to_take; index < internal::kPriceDecimalPlaces; ++index) {
            subcent_units *= kScaleFactor;
        }

        out_ticks =
            static_cast<internal::PriceTicks>(dollars * kDollarToTicksScale + subcent_units);
        return true;
    }

    [[nodiscard]] static OmsOrderRef order_ref_for_command(const OmsToKalshiCommand& command) {
        return std::visit(
            [](const auto& typed_command) -> OmsOrderRef {
                using T = std::decay_t<decltype(typed_command)>;
                if constexpr (std::is_same_v<T, SubmitOrderCmd>) {
                    return typed_command.order;
                } else {
                    return typed_command.corr.order;
                }
            },
            command);
    }

    [[nodiscard]] static const OmsOrderRef*
    order_ref_for_event(const KalshiToOmsEvent& event) {
        return std::visit(
            [](const auto& typed_event) -> const OmsOrderRef* {
                return &typed_event.order;
            },
            event);
    }

    [[nodiscard]] static std::string stable_order_key(const OmsOrderRef& order) {
        if (!order.client_order_id.empty()) {
            return "cid:" + std::string{order.client_order_id.view()};
        }
        if (order.exchange_order_id.has_value() && !order.exchange_order_id->empty()) {
            return "xid:" + std::string{order.exchange_order_id->view()};
        }
        return "rid:" + std::to_string(order.oms_request_id);
    }

    [[nodiscard]] static std::optional<ReconcileOpenOrderSnapshot>
    build_reconcile_snapshot(const SubmitOrderCmd& command,
                             const transport::OpenOrderSnapshot& snapshot,
                             internal::TimestampNs recv_ts_ns) {
        internal::QtyLots initial_qty_lots = 0;
        internal::QtyLots working_qty_lots = 0;
        internal::QtyLots cumulative_filled_qty_lots = 0;
        if (!internal::parse_non_negative_quantity_fp(snapshot.initial_count_fp,
                                                      initial_qty_lots) ||
            !internal::parse_non_negative_quantity_fp(snapshot.remaining_count_fp,
                                                      working_qty_lots) ||
            !internal::parse_non_negative_quantity_fp(snapshot.fill_count_fp,
                                                      cumulative_filled_qty_lots)) {
            return std::nullopt;
        }

        std::optional<internal::PriceTicks> working_limit_price_ticks;
        internal::PriceTicks parsed_ticks = 0;
        const std::string& price_text = command.intent.outcome == Outcome::kYes
                                            ? snapshot.yes_price_dollars
                                            : snapshot.no_price_dollars;
        if (!price_text.empty() && parse_non_negative_dollars_to_ticks(price_text, parsed_ticks)) {
            working_limit_price_ticks = parsed_ticks;
        }

        OmsOrderRef order = command.order;
        if (!snapshot.order.client_order_id.empty()) {
            order.client_order_id = snapshot.order.client_order_id;
        }
        if (snapshot.order.exchange_order_id.has_value() &&
            !snapshot.order.exchange_order_id->empty()) {
            order.exchange_order_id = snapshot.order.exchange_order_id;
        }

        return ReconcileOpenOrderSnapshot{
            .order = order,
            .context = command.intent.context,
            .exchange = command.intent.exchange,
            .side = command.intent.side,
            .outcome = command.intent.outcome,
            .initial_qty_lots = initial_qty_lots,
            .working_qty_lots = working_qty_lots,
            .cumulative_filled_qty_lots = cumulative_filled_qty_lots,
            .working_limit_price_ticks = working_limit_price_ticks,
            .recv_ts_ns = recv_ts_ns,
        };
    }
//NOLINTNEXTLINE
    void maybe_recover_post_write_unknown(DispatchCompletion& completion) {
        if (completion.terminal_state != DispatchRequestState::kPostWriteUnknown) {
            return;
        }

        std::unordered_set<std::string> already_emitted_keys;
        for (const auto& event : completion.emitted_events) {
            if (const auto* order = order_ref_for_event(event); order != nullptr) {
                already_emitted_keys.insert(stable_order_key(*order));
            }
        }

        std::vector<const DispatchItem*> unresolved_submit_items;
        std::vector<const DispatchItem*> unresolved_other_items;
        unresolved_submit_items.reserve(completion.request.items.size());
        unresolved_other_items.reserve(completion.request.items.size());

        for (const auto& item : completion.request.items) {
            const OmsOrderRef order = order_ref_for_command(item.command);
            if (already_emitted_keys.contains(stable_order_key(order))) {
                continue;
            }
            if (std::holds_alternative<SubmitOrderCmd>(item.command)) {
                unresolved_submit_items.push_back(&item);
            } else {
                unresolved_other_items.push_back(&item);
            }
        }

        const internal::TimestampNs recovery_ts_ns = recovery_now_ns();
        std::size_t unresolved_count =
            unresolved_submit_items.size() + unresolved_other_items.size();

        if (!unresolved_submit_items.empty()) {
            ++telemetry_.recovery_attempts;
            const bool request_was_sent =
                completion.trace.has_value() && completion.trace->request_sent_ts_ns != 0;
            std::unordered_map<std::string, transport::OpenOrderSnapshot> recovered_by_client_id;
            if (request_was_sent && recovery_rest_adapter_.has_value()) {
                std::unordered_set<std::string> remaining_client_ids;
                for (const auto* item : unresolved_submit_items) {
                    const auto& submit = std::get<SubmitOrderCmd>(item->command);
                    if (!submit.order.client_order_id.empty()) {
                        remaining_client_ids.insert(std::string{submit.order.client_order_id.view()});
                    }
                }

                for (std::size_t attempt = 0; attempt < config_.post_write_recovery_attempts &&
                                              !remaining_client_ids.empty();
                     ++attempt) {
                    std::optional<std::string> cursor;
                    bool fetch_failed = false;
                    do {
                        auto page = recovery_rest_adapter_->fetch_open_orders(
                            config_.post_write_recovery_fetch_limit, cursor);
                        if (!page.ok) {
                            fetch_failed = true;
                            ++telemetry_.recovery_failures;
                            break;
                        }
                        for (auto& snapshot : page.orders) {
                            const std::string client_id{snapshot.order.client_order_id.view()};
                            if (client_id.empty() || !remaining_client_ids.contains(client_id)) {
                                continue;
                            }
                            recovered_by_client_id[client_id] = snapshot;
                            remaining_client_ids.erase(client_id);
                        }
                        cursor = page.next_cursor;
                    } while (cursor.has_value() && !remaining_client_ids.empty());

                    if (remaining_client_ids.empty() || fetch_failed ||
                        attempt + 1U >= config_.post_write_recovery_attempts) {
                        break;
                    }
                    if (config_.post_write_recovery_backoff_ms > 0) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds{config_.post_write_recovery_backoff_ms});
                    }
                }
            }

            for (const auto* item : unresolved_submit_items) {
                const auto& submit = std::get<SubmitOrderCmd>(item->command);
                const auto recovered_it =
                    recovered_by_client_id.find(std::string{submit.order.client_order_id.view()});
                if (recovered_it != recovered_by_client_id.end()) {
                    if (auto snapshot =
                            build_reconcile_snapshot(submit, recovered_it->second, recovery_ts_ns);
                        snapshot.has_value()) {
                            //NOLINTNEXTLINE 
                        completion.emitted_events.push_back(std::move(*snapshot));
                        ++telemetry_.recovery_resolved_items;
                        --unresolved_count;
                        continue;
                    }
                    ++telemetry_.recovery_failures;
                }
                completion.emitted_events.emplace_back(VenueOrderUncertain{
                    .order = submit.order,
                    .recv_ts_ns = recovery_ts_ns,
                });
                ++telemetry_.recovery_uncertain_items;
            }
        }

        for (const auto* item : unresolved_other_items) {
            completion.emitted_events.emplace_back(VenueOrderUncertain{
                .order = order_ref_for_command(item->command),
                .recv_ts_ns = recovery_ts_ns,
            });
            ++telemetry_.recovery_uncertain_items;
        }

        if (unresolved_count == 0) {
            completion.terminal_state = DispatchRequestState::kCompleted;
            completion.error_message = "recovered_via_open_orders";
            ++telemetry_.recovery_resolved_requests;
        }
    }

    [[nodiscard]] bool flush_venue_events() {
        if (queues_.venue_event_queue == nullptr) {
            pending_venue_events_.clear();
            return false;
        }

        bool made_progress = false;
        while (!pending_venue_events_.empty()) {
            if (!queues_.venue_event_queue->try_push(std::move(pending_venue_events_.front()))) {
                ++telemetry_.venue_event_backpressure;
                return made_progress;
            }
            pending_venue_events_.pop_front();
            ++telemetry_.emitted_venue_events;
            made_progress = true;
        }
        return made_progress;
    }

    [[nodiscard]] bool try_take_admitted_request(DispatchClass dispatch_class,
                                                 DispatchRequest& out_request) {
        auto& pending = pending_for_class(dispatch_class);
        if (!pending.empty()) {
            out_request = std::move(pending.front());
            pending.pop_front();
            return true;
        }

        auto* queue = admitted_queue_for_class(dispatch_class);
        if (queue == nullptr) {
            return false;
        }
        return queue->try_pop(out_request);
    }

    void requeue_request(DispatchRequest request) {
        pending_for_class(request.dispatch_class).push_front(std::move(request));
    }

    void release_request_lineages(const DispatchRequest& request) {
        for (const auto& item : request.items) {
            order_sequencer_.note_transport_complete(item.lineage_id);
        }
    }

    [[nodiscard]] std::deque<DispatchRequest>& pending_for_class(DispatchClass dispatch_class) {
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return pending_hot_;
        case DispatchClass::kRecovery:
            return pending_recovery_;
        case DispatchClass::kReconcile:
            return pending_reconcile_;
        }
        return pending_hot_;
    }

    [[nodiscard]] RequestQueue* admitted_queue_for_class(DispatchClass dispatch_class) noexcept {
        switch (dispatch_class) {
        case DispatchClass::kHot:
            return admitted_hot_request_queue_.get();
        case DispatchClass::kRecovery:
            return admitted_recovery_request_queue_.get();
        case DispatchClass::kReconcile:
            return admitted_reconcile_request_queue_.get();
        }
        return nullptr;
    }
};

} // namespace predex::core::oms::kalshi::gateway
