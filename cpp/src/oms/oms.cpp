#include "predex/oms/oms.hpp"
#include "predex/control/control_types.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <string_view>
#include <variant>
#include <limits>

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

    constexpr std::uint64_t kPRICE_TICKS_PER_DOLLAR = 10'000;
    constexpr std::uint64_t kQUANTITY_LOTS_PER_CONTRACT = 100;
    constexpr std::size_t kGROUP_ADMISSION_RESPONSE_SLOTS = 3;
    constexpr std::uint64_t kPORTFOLIO_RECONCILIATION_RETRY_NS =
        1'000'000'000;

    [[nodiscard]] bool valid_timestamp_chain(
        const predex::oms::intent::IntentContext& context) noexcept {

        return context.ingress_timestamp_ns != 0 &&
            context.ingress_timestamp_ns <=
                context.book_apply_timestamp_ns &&
            context.book_apply_timestamp_ns <=
                context.observation_publish_timestamp_ns &&
            context.observation_publish_timestamp_ns <=
                context.strategy_dequeue_timestamp_ns &&
            context.strategy_dequeue_timestamp_ns <=
                context.evaluation_complete_timestamp_ns &&
            context.evaluation_complete_timestamp_ns <=
                context.intent_publish_timestamp_ns;
    }

    [[nodiscard]] std::optional<std::uint64_t>
    full_notional_reservation(
        std::int64_t quantity_lots) noexcept {

        if(quantity_lots <= 0) {
            return std::nullopt;
        }

        const auto unsigned_quantity =
            static_cast<std::uint64_t>(quantity_lots);

        if(unsigned_quantity >
        std::numeric_limits<std::uint64_t>::max() /
            kPRICE_TICKS_PER_DOLLAR) {
            return std::nullopt;
        }

        const std::uint64_t product =
            unsigned_quantity * kPRICE_TICKS_PER_DOLLAR;

        return product / kQUANTITY_LOTS_PER_CONTRACT +
            static_cast<std::uint64_t>(
                product % kQUANTITY_LOTS_PER_CONTRACT != 0);
    }

    [[nodiscard]] std::optional<std::uint64_t> absolute_lots(
        std::int64_t quantity_lots) noexcept {
        if(quantity_lots == std::numeric_limits<std::int64_t>::min()) {
            return std::nullopt;
        }
        return static_cast<std::uint64_t>(
            quantity_lots < 0 ? -quantity_lots : quantity_lots);
    }

    [[nodiscard]] std::optional<std::uint64_t>
    inventory_full_notional_exposure(
        std::uint64_t absolute_quantity_lots) noexcept {

        if(absolute_quantity_lots == 0){
            return 0;
        }
        if(absolute_quantity_lots >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max())){
            return std::nullopt;
        }
        return full_notional_reservation(
            static_cast<std::int64_t>(absolute_quantity_lots));
    }

    [[nodiscard]] std::optional<std::int64_t> monetary_ticks(
        std::uint64_t quantity_lots,
        std::int64_t price_ticks) noexcept {
        if(price_ticks < 0 ||
        quantity_lots > static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max()) /
                static_cast<std::uint64_t>(price_ticks == 0 ? 1 : price_ticks)) {
            return std::nullopt;
        }
        const auto product =
            static_cast<std::int64_t>(quantity_lots) * price_ticks;
        return product /
            static_cast<std::int64_t>(kQUANTITY_LOTS_PER_CONTRACT);
    }

    [[nodiscard]] std::optional<std::int64_t> signed_yes_lots(
        predex::oms::intent::Outcome outcome,
        predex::oms::intent::OrderAction action,
        std::int64_t quantity_lots) noexcept {
        if(quantity_lots <= 0 ||
        outcome == predex::oms::intent::Outcome::kUNKNOWN ||
        action == predex::oms::intent::OrderAction::kUNKNOWN){
            return std::nullopt;
        }
        const bool positive =
            (outcome == predex::oms::intent::Outcome::kYES &&
                action == predex::oms::intent::OrderAction::kBUY) ||
            (outcome == predex::oms::intent::Outcome::kNO &&
                action == predex::oms::intent::OrderAction::kSELL);
        return positive ? quantity_lots : -quantity_lots;
    }
}

namespace predex::oms{
constexpr std::size_t kMAX_DRAIN = 100;

    OmsPumpResult Oms::pump_once() noexcept{
        std::size_t work_done{0};
        work_done += try_send_pending_control_status() ? 1 : 0;
        work_done += drain_control_commands(kMAX_DRAIN);
        work_done += drain_venue_events(kMAX_DRAIN);
        work_done += service_execution_incidents();
        work_done += maybe_request_portfolio_reconciliation() ? 1 : 0;
        maybe_report_ready();
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
            bool did_work{false};
            for(auto* queue : queues_.venue_event_queues){
                if(queue == nullptr || processed >= max){
                    continue;
                }
                KalshiToOmsEvent event{};
                if(!queue->try_pop(event)){
                    continue;
                }
            std::visit(Overloaded{
                [this](const RestOrderResponse& response) {
                    handle_venue_event(response);
                },
                [this](const RestOrderBatchResponse& response) {
                    handle_venue_event(response);
                },
                [this](const PrivateWsOrderEvent& event) {
                    handle_venue_event(event);
                },
                [this](const ReconciledOrderSnapshot& snapshot) {
                    handle_venue_event(snapshot);
                },
                [this](const VenuePortfolioSnapshot& snapshot) {
                    handle_venue_event(snapshot);
                },
                [this](const OrderRestEgressDrained& drained) {
                    handle_venue_event(drained);
                }
            }, event);
                ++processed;
                did_work = true;
            }
            if(!did_work){
                break;
            }
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
                [this](const core::control::ApplyOrderRouteUniverse& command){ handle_control_command(command); },
                [this](const core::control::AllowTrading& command){ handle_control_command(command); },
                [this](const core::control::DisableTrading& command){ handle_control_command(command); },
                [this](const core::control::CancelAllOrders& command){ handle_control_command(command); }
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

    GroupRecord* Oms::find_group(OmsGroupId oms_group_id) noexcept {

        const auto iter = group_by_id_.find(oms_group_id);
        if(iter == group_by_id_.end()) {
            return nullptr;
        }

        return &iter->second;
    }

    GroupRecord* Oms::find_group(
        std::uint16_t strategy_index,//NOLINT
        intent::GroupIntentId group_intent_id) noexcept {

        const auto strategy_it =
            group_id_by_strategy_intent_.find(strategy_index);

        if(strategy_it ==
        group_id_by_strategy_intent_.end()) {
            return nullptr;
        }

        const auto group_it =
            strategy_it->second.find(group_intent_id);

        if(group_it == strategy_it->second.end()) {
            return nullptr;
        }

        return find_group(group_it->second);
    }
    const GroupRecord* Oms::find_group(
        OmsGroupId oms_group_id) const noexcept {

        const auto iter = group_by_id_.find(oms_group_id);
        if(iter == group_by_id_.end()) {
            return nullptr;
        }

        return &iter->second;
    }

    const GroupRecord* Oms::find_group(
        std::uint16_t strategy_index,//NOLINT
        intent::GroupIntentId group_intent_id) const noexcept {

        const auto strategy_it =
            group_id_by_strategy_intent_.find(strategy_index);

        if(strategy_it ==
        group_id_by_strategy_intent_.end()) {
            return nullptr;
        }

        const auto group_it =
            strategy_it->second.find(group_intent_id);

        if(group_it == strategy_it->second.end()) {
            return nullptr;
        }

        return find_group(group_it->second);
    }

    StrategyPortfolioRecord&
    Oms::ensure_strategy_portfolio(
        std::uint16_t strategy_index) noexcept {

        auto [it, inserted] =
            portfolio_by_strategy_.try_emplace(strategy_index);

        if(inserted) {
            it->second = StrategyPortfolioRecord{
                .strategy_index = strategy_index,
                .next_sequence = 1,
                .allocation_limit_ticks =
                    risk_config_.strategy_allocation_limit_ticks,
                .available_capital_ticks =
                    risk_config_.strategy_allocation_limit_ticks,
            };
            recompute_strategy_available_capital(it->second);
        }

        return it->second;
    }

    StrategyMarketPositionRecord&
    Oms::ensure_strategy_market_position(
        std::uint16_t strategy_index,
        intent::MarketId market_id,
        intent::EventId event_id) noexcept {

        auto& positions =
            position_by_strategy_and_market_[strategy_index];

        auto [it, inserted] =
            positions.try_emplace(market_id);

        if(inserted) {
            it->second = StrategyMarketPositionRecord{
                .strategy_index = strategy_index,
                .market_id = market_id,
                .event_id = event_id,
            };
        }

        return it->second;
    }
//NOLINTNEXTLINE
    GroupAdmissionCheck Oms::check_group_admission(
        const intent::GroupOrderIntent& group) const noexcept {

        auto reject = [](RejectReason reason) {
            return GroupAdmissionCheck{
                .accepted = false,
                .reject_reason = reason,
            };
        };

        if(execution_incident_active_) {
            return reject(RejectReason::kEXECUTION_BLOCKED);
        }

        if(!trading_enabled_) {
            return reject(RejectReason::kTRADING_DISABLED);
        }

        if(active_order_universe_ == nullptr ||
        active_order_universe_->version == 0) {
            return reject(RejectReason::kOMS_NOT_READY);
        }

        if(!portfolio_ready_for_trading()) {
            return reject(RejectReason::kOMS_NOT_READY);
        }

        if(group.admission_policy !=
        intent::GroupAdmissionPolicy::kALL_OR_NONE) {
            return reject(RejectReason::kINVALID_INTENT);
        }

        if(group.context.strategy_index >=
            queues_.strategy_response_queues.size() ||
        queues_.strategy_response_queues[
            group.context.strategy_index] == nullptr) {
            return reject(RejectReason::kOMS_NOT_READY);
        }

        const auto* response_queue =
            queues_.strategy_response_queues[
                group.context.strategy_index];

        const std::size_t queued =
            response_queue->producer_size();

        if(queued > response_queue->capacity() ||
        response_queue->capacity() - queued <
            kGROUP_ADMISSION_RESPONSE_SLOTS) {
            return reject(RejectReason::kOMS_NOT_READY);
        }

        if(group.context.group_intent_id == 0 ||
        group.context.event_id == 0 ||
        group.context.universe_version == 0 ||
        group.context.event_revision == 0 ||
        group.context.leg_count != group.leg_count) {
            return reject(RejectReason::kINVALID_INTENT);
        }

        if(group.context.universe_version !=
        active_order_universe_->version) {
            return reject(RejectReason::kINVALID_INTENT);
        }

        if(group.leg_count < 2 ||
        group.leg_count > group.new_orders.size() ||
        group.leg_count >
            risk_config_.maximum_group_legs) {
            return reject(RejectReason::kINVALID_INTENT);
        }

        if(risk_config_.strategy_allocation_limit_ticks <= 0 ||
        risk_config_.maximum_group_reservation_ticks == 0 ||
        risk_config_.maximum_group_intent_age_ns == 0) {
            return reject(RejectReason::kRISK_REJECTED);
        }

        if(!valid_timestamp_chain(group.context)) {
            return reject(RejectReason::kINVALID_INTENT);
        }

        const std::uint64_t current_time_ns = now_ns();

        if(group.context.intent_publish_timestamp_ns >
            current_time_ns ||
        current_time_ns -
                group.context.intent_publish_timestamp_ns >
            risk_config_.maximum_group_intent_age_ns) {
            return reject(RejectReason::kINVALID_INTENT);
        }

        if(find_group(
            group.context.strategy_index,
            group.context.group_intent_id) != nullptr) {
            return reject(RejectReason::kDUPLICATE_INTENT);
        }

        if(group.expected_gross_edge_ticks <
            group.estimated_fee_ticks ||
        group.expected_net_edge_ticks !=
            group.expected_gross_edge_ticks -
                group.estimated_fee_ticks ||
        group.expected_net_edge_ticks == 0) {
            return reject(RejectReason::kINVALID_INTENT);
        }

        std::uint64_t required_capital_ticks{};

        for(std::uint8_t i = 0;
            i < group.leg_count;
            ++i) {
            const auto& leg = group.new_orders[i];
            const auto& context = leg.context;

            if(context.strategy_index !=
                group.context.strategy_index ||
            context.event_id !=
                group.context.event_id ||
            context.strategy_id !=
                group.context.strategy_id ||
            context.signal_id !=
                group.context.signal_id ||
            context.group_intent_id !=
                group.context.group_intent_id ||
            context.universe_version !=
                group.context.universe_version ||
            context.event_revision !=
                group.context.event_revision ||
            context.source_shard_index !=
                group.context.source_shard_index ||
            context.leg_index != i ||
            context.leg_count != group.leg_count ||
            context.market_id == 0 ||
            context.strategy_intent_id == 0) {
                return reject(RejectReason::kINVALID_INTENT);
            }

            if(leg.price_ticks <= 0 ||
            leg.price_ticks >=
                static_cast<std::int64_t>(
                    kPRICE_TICKS_PER_DOLLAR) ||
            leg.quantity_lots <= 0 ||
            leg.outcome == intent::Outcome::kUNKNOWN ||
            leg.action == intent::OrderAction::kUNKNOWN ||
            leg.liquidity_intent !=
                intent::LiquidityIntent::kTAKER ||
            leg.order_type !=
                intent::OrderType::kMARKETABLE_LIMIT ||
            leg.time_in_force != intent::TimeInForce::kFOK) {
                return reject(RejectReason::kINVALID_INTENT);
            }

            if(!valid_timestamp_chain(context)) {
                return reject(RejectReason::kINVALID_INTENT);
            }

            const auto* route =
                find_market_route(context.market_id);

            if(route == nullptr) {
                return reject(RejectReason::kUNKNOWN_MARKET);
            }

            if(!route->tradeable) {
                return reject(
                    RejectReason::kMARKET_NOT_TRADEABLE);
            }

            if(route->event_id != group.context.event_id) {
                return reject(RejectReason::kINVALID_INTENT);
            }

            for(std::uint8_t earlier = 0;
                earlier < i;
                ++earlier) {
                if(group.new_orders[earlier]
                        .context.market_id ==
                    context.market_id ||
                group.new_orders[earlier]
                        .context.strategy_intent_id ==
                    context.strategy_intent_id) {
                    return reject(RejectReason::kINVALID_INTENT);
                }
            }

            const auto leg_reservation =
                full_notional_reservation(
                    leg.quantity_lots);

            if(!leg_reservation.has_value() ||
            required_capital_ticks >
                std::numeric_limits<std::uint64_t>::max() -
                    *leg_reservation) {
                return reject(RejectReason::kRISK_REJECTED);
            }

            required_capital_ticks += *leg_reservation;
        }

        if(required_capital_ticks == 0 ||
        required_capital_ticks >
            risk_config_.maximum_group_reservation_ticks ||
        required_capital_ticks >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
            return reject(RejectReason::kRISK_REJECTED);
        }

        std::int64_t available_capital =
            risk_config_.strategy_allocation_limit_ticks;

        const auto portfolio_it =
            portfolio_by_strategy_.find(
                group.context.strategy_index);

        if(portfolio_it != portfolio_by_strategy_.end()) {
            available_capital =
                portfolio_it->second.available_capital_ticks;
        }

        if(available_capital < 0 ||
        required_capital_ticks >
            static_cast<std::uint64_t>(
                available_capital)) {
            return reject(RejectReason::kRISK_REJECTED);
        }

        return GroupAdmissionCheck{
            .accepted = true,
            .reject_reason = RejectReason::kNONE,
            .required_capital_ticks =
                required_capital_ticks,
        };
    }

    bool Oms::reserve_group_capital(
        GroupRecord& group) noexcept {

        if(group.reserved_capital_ticks == 0 ||
        group.reserved_capital_ticks >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
            return false;
        }

        auto& portfolio = ensure_strategy_portfolio(
            group.context.strategy_index);

        const auto reservation =
            static_cast<std::int64_t>(
                group.reserved_capital_ticks);

        if(portfolio.available_capital_ticks <
        reservation) {
            return false;
        }

        portfolio.reserved_order_capital_ticks += reservation;
        ++portfolio.active_group_count;
        recompute_strategy_available_capital(portfolio);

        group.capital_released = false;
        return true;
    }

    void Oms::release_group_capital(
        GroupRecord& group) noexcept {

        if(group.capital_released) {
            return;
        }

        auto& portfolio = ensure_strategy_portfolio(
            group.context.strategy_index);

        const auto reservation =
            static_cast<std::int64_t>(
                group.reserved_capital_ticks);

        const auto released = std::min(
            portfolio.reserved_order_capital_ticks,
            reservation);
        portfolio.reserved_order_capital_ticks -= released;
        group.reserved_capital_ticks -=
            static_cast<std::uint64_t>(released);

        if(portfolio.active_group_count > 0) {
            --portfolio.active_group_count;
        }

        group.capital_released = true;
        recompute_strategy_available_capital(portfolio);
        emit_strategy_portfolio_update(portfolio, now_ns());
    }

    void Oms::emit_group_admission_response(
        const GroupRecord& group,
        GroupAdmissionState admission_state,
        RejectReason reject_reason,
        std::uint64_t recv_ts_ns) noexcept {

        GroupAdmissionResponse response{
            .context = group.context,
            .oms_group_id = group.oms_group_id,
            .admission_state = admission_state,
            .reject_reason = reject_reason,
            .admitted_leg_count =
                static_cast<std::uint8_t>(admission_state ==
                    GroupAdmissionState::kACCEPTED
                    ? group.leg_count
                    : 0),
            .reserved_capital_ticks =
                admission_state ==
                    GroupAdmissionState::kACCEPTED
                    ? group.reserved_capital_ticks
                    : 0,
            .recv_ts_ns = recv_ts_ns,
            .response_ts_ns = now_ns(),
        };

        (void)send_strategy_message(
            group.context.strategy_index,
            OmsToStrategyMessage{response});
    }

    void Oms::emit_group_state_update(
        const GroupRecord& group,
        std::uint64_t update_ts_ns) noexcept {

        GroupStateUpdate update{
            .context = group.context,
            .oms_group_id = group.oms_group_id,
            .execution_state = group.execution_state,
            .failure_reason = group.failure_reason,
            .leg_count = group.leg_count,
            .repair_attempt_count =
                group.repair_attempt_count,
            .residual_exposure_present =
                group.residual_exposure_present,
            .last_update_ts_ns = update_ts_ns,
        };

        for(std::uint8_t i = 0;
            i < group.leg_count;
            ++i) {
            const OrderRecord* order =
                find_order(
                    group.original_leg_request_ids[i]);

            if(order == nullptr) {
                continue;
            }

            update.legs[i] = GroupLegState{
                .oms_request_id =
                    order->context.oms_request_id,
                .market_id = order->market_id,
                .leg_index = order->group_leg_index,
                .order_state = order->order_state,
                .ordered_qty_lots =
                    order->ordered_qty_lots,
                .cumulative_filled_qty_lots =
                    order->cumulative_filled_qty_lots,
                .leaves_qty_lots =
                    order->leaves_qty_lots,
            };
        }

        (void)send_strategy_message(
            group.context.strategy_index,
            OmsToStrategyMessage{update});
    }

    void Oms::emit_strategy_portfolio_update(
        StrategyPortfolioRecord& portfolio,
        std::uint64_t update_ts_ns) noexcept {

        StrategyPortfolioUpdate update{
            .strategy_index = portfolio.strategy_index,
            .sequence = portfolio.next_sequence++,
            .allocation_limit_ticks =
                portfolio.allocation_limit_ticks,
            .available_capital_ticks =
                portfolio.available_capital_ticks,
            .reserved_order_capital_ticks =
                portfolio.reserved_order_capital_ticks,
            .inventory_exposure_ticks =
                portfolio.inventory_exposure_ticks,
            .realized_pnl_ticks =
                portfolio.realized_pnl_ticks,
            .fees_paid_ticks =
                portfolio.fees_paid_ticks,
            .venue_available_balance_ticks =
                venue_portfolio_.available_balance_ticks,
            .venue_portfolio_reconciled =
                venue_portfolio_.reconciled,
            .open_order_count =
                portfolio.open_order_count,
            .active_group_count =
                portfolio.active_group_count,
            .update_ts_ns = update_ts_ns,
        };

        (void)send_strategy_message(
            portfolio.strategy_index,
            OmsToStrategyMessage{update});
    }

    void Oms::emit_strategy_market_position_update(
        StrategyMarketPositionRecord& position,
        std::uint64_t update_ts_ns) noexcept {

        auto& portfolio = ensure_strategy_portfolio(
            position.strategy_index);
        StrategyMarketPositionUpdate update{
            .strategy_index = position.strategy_index,
            .sequence = portfolio.next_sequence++,
            .market_id = position.market_id,
            .event_id = position.event_id,
            .net_position_lots = position.net_position_lots,
            .resting_buy_qty_lots = position.resting_buy_qty_lots,
            .resting_sell_qty_lots = position.resting_sell_qty_lots,
            .average_entry_price_ticks =
                position.average_entry_price_ticks,
            .position_cost_ticks = position.position_cost_ticks,
            .market_exposure_ticks = position.market_exposure_ticks,
            .realized_pnl_ticks = position.realized_pnl_ticks,
            .fees_paid_ticks = position.fees_paid_ticks,
            .update_ts_ns = update_ts_ns,
        };
        (void)send_strategy_message(
            position.strategy_index,
            OmsToStrategyMessage{update});
    }

    bool Oms::portfolio_ready_for_trading() const noexcept{
        return !risk_config_.require_portfolio_reconciliation ||
            venue_portfolio_.reconciled;
    }

    void Oms::recompute_strategy_available_capital(
        StrategyPortfolioRecord& portfolio) noexcept{
        const auto positive_reservations =
            std::max<std::int64_t>(
                0,
                portfolio.reserved_order_capital_ticks);
        const auto positive_inventory =
            std::max<std::int64_t>(
                0,
                portfolio.inventory_exposure_ticks);
        const auto strategy_committed =
            positive_inventory >
                std::numeric_limits<std::int64_t>::max() -
                    positive_reservations
                ? std::numeric_limits<std::int64_t>::max()
                : positive_reservations + positive_inventory;
        const auto strategy_available =
            std::max<std::int64_t>(
                0,
                portfolio.allocation_limit_ticks -
                    std::min(
                        portfolio.allocation_limit_ticks,
                        strategy_committed));

        if(!risk_config_.require_portfolio_reconciliation){
            portfolio.available_capital_ticks = strategy_available;
            return;
        }
        if(!venue_portfolio_.reconciled){
            portfolio.available_capital_ticks = 0;
            return;
        }

        std::int64_t global_reservations{};
        for(const auto& [_, other] : portfolio_by_strategy_){
            if(other.reserved_order_capital_ticks >
                std::numeric_limits<std::int64_t>::max() -
                    global_reservations){
                global_reservations =
                    std::numeric_limits<std::int64_t>::max();
                break;
            }
            global_reservations +=
                std::max<std::int64_t>(
                    0,
                    other.reserved_order_capital_ticks);
        }

        const auto venue_after_safety =
            std::max<std::int64_t>(
                0,
                venue_portfolio_.available_balance_ticks -
                    std::min(
                        venue_portfolio_.available_balance_ticks,
                        std::max<std::int64_t>(
                            0,
                            risk_config_.venue_safety_reserve_ticks)));
        const auto venue_available =
            std::max<std::int64_t>(
                0,
                venue_after_safety -
                    std::min(venue_after_safety, global_reservations));
        portfolio.available_capital_ticks =
            std::min(strategy_available, venue_available);
    }

    void Oms::recompute_all_strategy_available_capital() noexcept{
        for(auto& [_, portfolio] : portfolio_by_strategy_){
            recompute_strategy_available_capital(portfolio);
        }
    }

    bool Oms::maybe_request_portfolio_reconciliation() noexcept{
        if(!risk_config_.require_portfolio_reconciliation ||
        active_order_universe_ == nullptr ||
        active_order_universe_->version == 0 ||
        active_reconciliation_id_ != 0){
            return false;
        }

        const auto current_time = now_ns();
        if(portfolio_reconciliation_needed_ &&
        next_reconciliation_due_ts_ns_ != 0 &&
        current_time < next_reconciliation_due_ts_ns_){
            return false;
        }
        const bool periodic_due =
            risk_config_.portfolio_reconciliation_interval_ns != 0 &&
            venue_portfolio_.reconciled &&
            current_time >= next_reconciliation_due_ts_ns_;
        if(!portfolio_reconciliation_needed_ && !periodic_due){
            return false;
        }

        const auto reconciliation_id = next_reconciliation_id_++;
        if(!send_kalshi_command(OmsToKalshiCommand{
            RequestPortfolioReconciliation{
                .reconciliation_id = reconciliation_id,
                .universe_version = active_order_universe_->version,
                .submission_ts_ns = current_time,
            }
        })){
            return false;
        }
        active_reconciliation_id_ = reconciliation_id;
        portfolio_reconciliation_needed_ = false;
        ++telemetry_.portfolio_reconciliations_requested;
        emit_telemetry();
        return true;
    }

    void Oms::maybe_report_ready() noexcept{
        if(ready_reported_ ||
        active_order_universe_ == nullptr ||
        active_order_universe_->version == 0 ||
        !portfolio_ready_for_trading()){
            return;
        }
        if(send_control_status(
            core::control::OmsToControlStatus{
                core::control::OmsReady{}})){
            ready_reported_ = true;
        }
    }

    void Oms::apply_venue_market_position(
        const VenueMarketPositionSnapshot& position,
        std::uint64_t update_ts_ns) noexcept{
        if(position.market_id == 0){
            return;
        }
        const auto market_id = position.market_id;
        venue_position_by_market_[market_id] = position;

        if(queues_.strategy_response_queues.size() != 1){
            return;
        }

        auto& strategy_position = ensure_strategy_market_position(
            0,
            market_id,
            position.event_id);
        auto& portfolio = ensure_strategy_portfolio(0);

        const auto previous_exposure =
            strategy_position.market_exposure_ticks;
        const auto previous_realized =
            strategy_position.realized_pnl_ticks;
        const auto previous_fees =
            strategy_position.fees_paid_ticks;

        strategy_position.net_position_lots =
            position.net_position_lots;
        if(position.position_cost_present){
            strategy_position.position_cost_ticks =
                position.position_cost_ticks;
        }
        if(position.realized_pnl_present){
            strategy_position.realized_pnl_ticks =
                position.realized_pnl_ticks;
        }
        if(position.fees_paid_present){
            strategy_position.fees_paid_ticks =
                position.fees_paid_ticks;
        }

        const auto lots = absolute_lots(position.net_position_lots);
        const auto fallback_exposure = lots.has_value()
            ? inventory_full_notional_exposure(*lots)
            : std::nullopt;
        strategy_position.market_exposure_ticks =
            position.market_exposure_present
                ? position.market_exposure_ticks
                : fallback_exposure.has_value() &&
                    *fallback_exposure <=
                        static_cast<std::uint64_t>(
                            std::numeric_limits<std::int64_t>::max())
                    ? static_cast<std::int64_t>(*fallback_exposure)
                    : std::numeric_limits<std::int64_t>::max();

        if(lots.has_value() && *lots > 0 &&
        position.position_cost_ticks >= 0 &&
        static_cast<std::uint64_t>(position.position_cost_ticks) <=
            std::numeric_limits<std::uint64_t>::max() /
                kQUANTITY_LOTS_PER_CONTRACT){
            strategy_position.average_entry_price_ticks =
                static_cast<std::int64_t>(
                    static_cast<std::uint64_t>(
                        position.position_cost_ticks) *
                    kQUANTITY_LOTS_PER_CONTRACT /
                    *lots);
        }else{
            strategy_position.average_entry_price_ticks = 0;
        }

        portfolio.inventory_exposure_ticks +=
            strategy_position.market_exposure_ticks - previous_exposure;
        portfolio.realized_pnl_ticks +=
            strategy_position.realized_pnl_ticks - previous_realized;
        portfolio.fees_paid_ticks +=
            strategy_position.fees_paid_ticks - previous_fees;
        recompute_all_strategy_available_capital();
        emit_strategy_market_position_update(
            strategy_position,
            update_ts_ns);
        emit_strategy_portfolio_update(portfolio, update_ts_ns);
    }

    void Oms::account_fill(//NOLINT
        OrderRecord& order,
        const PrivateWsOrderEvent& event) noexcept{
        const auto outcome = event.outcome == intent::Outcome::kUNKNOWN
            ? order.outcome
            : event.outcome;
        const auto action = event.action == intent::OrderAction::kUNKNOWN
            ? order.action
            : event.action;
        if(event.last_fill_qty_lots <= 0 ||
        event.last_fill_price_ticks <= 0 ||
        event.last_fill_price_ticks >=
            static_cast<std::int64_t>(kPRICE_TICKS_PER_DOLLAR) ||
        outcome == intent::Outcome::kUNKNOWN ||
        action == intent::OrderAction::kUNKNOWN){
            return;
        }

        const auto resolved_signed_delta = signed_yes_lots(
            outcome,
            action,
            event.last_fill_qty_lots);
        if(!resolved_signed_delta.has_value()){
            return;
        }
        const std::int64_t signed_delta =
            *resolved_signed_delta;

        if(auto* group = find_group(order.oms_group_id);
        group != nullptr &&
        order.group_leg_index < group->leg_count){
            auto& residual =
                group->residual_yes_lots[order.group_leg_index];
            if((signed_delta > 0 &&
                residual >
                    std::numeric_limits<std::int64_t>::max() -
                        signed_delta) ||
            (signed_delta < 0 &&
                residual <
                    std::numeric_limits<std::int64_t>::min() -
                        signed_delta)){
                group->venue_state_uncertain = true;
            }else{
                residual += signed_delta;
            }
        }

        auto& position = ensure_strategy_market_position(
            order.context.context.strategy_index,
            order.market_id,
            order.context.context.event_id);
        auto& portfolio = ensure_strategy_portfolio(
            order.context.context.strategy_index);

        const auto old_abs = absolute_lots(position.net_position_lots);
        const auto delta_abs = absolute_lots(signed_delta);
        if(!old_abs.has_value() || !delta_abs.has_value()){
            return;
        }

        const std::int64_t yes_price =
            outcome == intent::Outcome::kYES
                ? event.last_fill_price_ticks
                : static_cast<std::int64_t>(kPRICE_TICKS_PER_DOLLAR) -
                    event.last_fill_price_ticks;
        const std::int64_t delta_side_price = signed_delta > 0
            ? yes_price
            : static_cast<std::int64_t>(kPRICE_TICKS_PER_DOLLAR) -
                yes_price;

        const bool same_direction =
            position.net_position_lots == 0 ||
            (position.net_position_lots > 0) == (signed_delta > 0);
        const std::int64_t new_position =
            position.net_position_lots + signed_delta;

        if(same_direction){
            const auto new_abs = *old_abs + *delta_abs;
            if(new_abs != 0){
                const long double weighted =
                    static_cast<long double>(*old_abs) *
                        position.average_entry_price_ticks +
                    static_cast<long double>(*delta_abs) *
                        delta_side_price;
                position.average_entry_price_ticks =
                    static_cast<std::int64_t>(
                        weighted / static_cast<long double>(new_abs));
            }
        }else{
            const auto closed_lots = std::min(*old_abs, *delta_abs);
            const std::int64_t exit_price =
                position.net_position_lots > 0
                    ? yes_price
                    : static_cast<std::int64_t>(kPRICE_TICKS_PER_DOLLAR) -
                        yes_price;
            const auto pnl = monetary_ticks(
                closed_lots,
                exit_price - position.average_entry_price_ticks < 0
                    ? position.average_entry_price_ticks - exit_price
                    : exit_price - position.average_entry_price_ticks);
            if(pnl.has_value()){
                const bool profitable =
                    exit_price >= position.average_entry_price_ticks;
                position.realized_pnl_ticks +=
                    profitable ? *pnl : -*pnl;
                portfolio.realized_pnl_ticks +=
                    profitable ? *pnl : -*pnl;
            }
            if(*delta_abs > *old_abs){
                position.average_entry_price_ticks =
                    delta_side_price;
            }else if(*delta_abs == *old_abs){
                position.average_entry_price_ticks = 0;
            }
        }

        const auto previous_exposure =
            position.market_exposure_ticks;
        position.net_position_lots = new_position;
        const auto new_abs = absolute_lots(new_position);
        const auto exposure = new_abs.has_value()
            ? inventory_full_notional_exposure(*new_abs)
            : std::nullopt;
        position.market_exposure_ticks = exposure.has_value()
            ? static_cast<std::int64_t>(*exposure)
            : std::numeric_limits<std::int64_t>::max();
        const auto cost = new_abs.has_value()
            ? monetary_ticks(
                *new_abs,
                position.average_entry_price_ticks)
            : std::nullopt;
        position.position_cost_ticks = cost.value_or(0);
        portfolio.inventory_exposure_ticks +=
            position.market_exposure_ticks - previous_exposure;

        const auto fill_reservation =
            full_notional_reservation(event.last_fill_qty_lots);
        if(fill_reservation.has_value()){
            const auto released = std::min<std::int64_t>(
                order.reserved_capital_ticks,
                static_cast<std::int64_t>(*fill_reservation));
            order.reserved_capital_ticks -= released;
            portfolio.reserved_order_capital_ticks -= std::min(
                portfolio.reserved_order_capital_ticks,
                released);
            if(auto* group = find_group(order.oms_group_id)){
                group->reserved_capital_ticks -= std::min<std::uint64_t>(
                    group->reserved_capital_ticks,
                    static_cast<std::uint64_t>(released));
            }
        }

        recompute_all_strategy_available_capital();
        emit_strategy_market_position_update(position, event.recv_ts_ns);
        emit_strategy_portfolio_update(portfolio, event.recv_ts_ns);
        portfolio_reconciliation_needed_ =
            risk_config_.require_portfolio_reconciliation;
    }

    bool Oms::group_has_residual_exposure(
        const GroupRecord& group) const noexcept{
        for(std::uint8_t i = 0; i < group.leg_count; ++i){
            if(group.residual_yes_lots[i] != 0){
                return true;
            }
        }
        return false;
    }

    bool Oms::group_has_confirmed_original_fill(
        const GroupRecord& group) noexcept{
        for(std::uint8_t i = 0; i < group.leg_count; ++i){
            const auto* order =
                find_order(group.original_leg_request_ids[i]);
            if(order != nullptr &&
            order->cumulative_filled_qty_lots > 0){
                return true;
            }
        }
        return false;
    }

    void Oms::latch_execution_incident(
        std::uint64_t /*update_ts_ns*/) noexcept{
        if(!execution_incident_active_){
            execution_incident_active_ = true;
            telemetry_.execution_incident_active = true;
            ++telemetry_.execution_incidents_latched;
        }
        if(trading_enabled_){
            trading_enabled_ = false;
            emit_trading_enabled_changed();
        }
    }

    void Oms::refresh_execution_incident_latch() noexcept{
        bool active{false};
        for(const auto& [_, group] : group_by_id_){
            const auto state = group.execution_state;
            if(state == GroupExecutionState::kREPAIR_REQUIRED ||
            state == GroupExecutionState::kCANCELING_REMAINDER ||
            state == GroupExecutionState::kUNWINDING ||
            state == GroupExecutionState::kUNCERTAIN ||
            state == GroupExecutionState::kREPAIR_FAILED){
                active = true;
                break;
            }
        }
        execution_incident_active_ = active;
        telemetry_.execution_incident_active = active;
        if(!active){
            execution_fault_reported_ = false;
        }
    }

    void Oms::maybe_report_execution_fault() noexcept{
        if(execution_fault_reported_){
            return;
        }
        for(const auto& [_, group] : group_by_id_){
            if(group.execution_state !=
                    GroupExecutionState::kUNCERTAIN &&
            group.execution_state !=
                    GroupExecutionState::kREPAIR_FAILED){
                continue;
            }
            const std::string message =
                group.execution_state ==
                    GroupExecutionState::kUNCERTAIN
                    ? "OMS group execution state is uncertain; operator reconciliation required"
                    : "OMS group repair failed; residual exposure may remain";
            if(send_control_status(core::control::OmsToControlStatus{
                core::control::OmsFaulted{
                    .error_message = message,
                }})){
                execution_fault_reported_ = true;
            }
            return;
        }
    }

    void Oms::fail_group_repair(
        GroupRecord& group,
        GroupFailureReason reason,
        std::uint64_t update_ts_ns) noexcept{
        const bool newly_failed =
            group.execution_state !=
                GroupExecutionState::kREPAIR_FAILED;
        group.execution_state =
            GroupExecutionState::kREPAIR_FAILED;
        group.failure_reason = reason;
        group.residual_exposure_present =
            group_has_residual_exposure(group);
        group.last_update_ts_ns = update_ts_ns;
        latch_execution_incident(update_ts_ns);
        if(newly_failed){
            ++telemetry_.group_repairs_failed;
            emit_group_state_update(group, update_ts_ns);
            emit_telemetry();
        }
        maybe_report_execution_fault();
    }

    void Oms::begin_group_repair(
        GroupRecord& group,
        GroupFailureReason reason,
        std::uint64_t update_ts_ns) noexcept{
        const bool already_repairing =
            group.execution_state ==
                GroupExecutionState::kREPAIR_REQUIRED ||
            group.execution_state ==
                GroupExecutionState::kCANCELING_REMAINDER ||
            group.execution_state ==
                GroupExecutionState::kUNWINDING ||
            group.execution_state ==
                GroupExecutionState::kUNCERTAIN ||
            group.execution_state ==
                GroupExecutionState::kREPAIR_FAILED;

        if(reason == GroupFailureReason::kVENUE_STATE_UNCERTAIN){
            group.venue_state_uncertain = true;
        }
        group.failure_reason = reason;
        group.residual_exposure_present =
            group_has_residual_exposure(group);
        group.execution_state = group.venue_state_uncertain
            ? GroupExecutionState::kUNCERTAIN
            : GroupExecutionState::kREPAIR_REQUIRED;
        group.last_update_ts_ns = update_ts_ns;
        latch_execution_incident(update_ts_ns);
        if(!already_repairing){
            emit_group_state_update(group, update_ts_ns);
            emit_telemetry();
        }
        continue_group_repair(group, update_ts_ns);
    }

    void Oms::continue_group_repair(
        GroupRecord& group,
        std::uint64_t update_ts_ns) noexcept{
        if(group.capital_released ||
        group.execution_state == GroupExecutionState::kREPAIR_FAILED){
            return;
        }

        std::array<OrderRecord*, intent::kMAX_ORDERS_PER_GROUP>
            cancelable_orders{};
        std::size_t cancelable_count{};
        bool cancellation_pending{false};

        for(std::uint8_t i = 0; i < group.leg_count; ++i){
            auto* original =
                find_order(group.original_leg_request_ids[i]);
            if(original == nullptr){
                group.venue_state_uncertain = true;
                continue;
            }
            if(original->order_state == OrderState::kUNCERTAIN){
                group.venue_state_uncertain = true;
            }
            if(original->order_state == OrderState::kPENDING_CANCEL){
                cancellation_pending = true;
            }else if(is_cancelable(original->order_state)){
                cancelable_orders[cancelable_count++] = original;
            }
        }

        if(cancelable_count > 0){
            if(group.cancel_attempt_count >=
                risk_config_.maximum_group_repair_attempts){
                if(group.venue_state_uncertain){
                    group.execution_state =
                        GroupExecutionState::kUNCERTAIN;
                    group.last_update_ts_ns = update_ts_ns;
                    emit_group_state_update(group, update_ts_ns);
                    maybe_report_execution_fault();
                }else{
                    fail_group_repair(
                        group,
                        GroupFailureReason::kCANCEL_FAILED,
                        update_ts_ns);
                }
                return;
            }

            const std::size_t queued =
                queues_.kalshi_command_queue.producer_size();
            if(queued > queues_.kalshi_command_queue.capacity() ||
            queues_.kalshi_command_queue.capacity() - queued <
                cancelable_count){
                group.execution_state = group.venue_state_uncertain
                    ? GroupExecutionState::kUNCERTAIN
                    : GroupExecutionState::kREPAIR_REQUIRED;
                return;
            }

            std::size_t commands_sent{};
            for(std::size_t i = 0; i < cancelable_count; ++i){
                if(emit_cancel_command_for_record(
                    *cancelable_orders[i],
                    group.context,
                    next_oms_request_id_++,
                    update_ts_ns)){
                    ++commands_sent;
                }
            }
            if(commands_sent != cancelable_count){
                group.venue_state_uncertain = true;
            }
            if(commands_sent > 0){
                ++group.cancel_attempt_count;
                telemetry_.group_repair_commands_sent +=
                    commands_sent;
            }
            group.execution_state = group.venue_state_uncertain
                ? GroupExecutionState::kUNCERTAIN
                : GroupExecutionState::kCANCELING_REMAINDER;
            group.last_update_ts_ns = update_ts_ns;
            emit_group_state_update(group, update_ts_ns);
            emit_telemetry();
            if(group.venue_state_uncertain){
                portfolio_reconciliation_needed_ =
                    risk_config_.require_portfolio_reconciliation;
                maybe_report_execution_fault();
            }
            return;
        }

        if(cancellation_pending){
            if(group.venue_state_uncertain){
                group.execution_state =
                    GroupExecutionState::kUNCERTAIN;
                maybe_report_execution_fault();
            }else{
                group.execution_state =
                    GroupExecutionState::kCANCELING_REMAINDER;
            }
            return;
        }

        bool repair_order_live{false};
        for(std::uint8_t i = 0; i < group.leg_count; ++i){
            const auto request_id =
                group.active_repair_request_ids[i];
            if(request_id == 0){
                continue;
            }
            const auto* repair = find_order(request_id);
            if(repair == nullptr ||
            repair->order_state == OrderState::kUNCERTAIN){
                group.venue_state_uncertain = true;
                continue;
            }
            if(is_live(repair->order_state)){
                repair_order_live = true;
            }else{
                group.active_repair_request_ids[i] = 0;
            }
        }

        group.residual_exposure_present =
            group_has_residual_exposure(group);

        if(group.venue_state_uncertain){
            group.execution_state = GroupExecutionState::kUNCERTAIN;
            group.failure_reason =
                GroupFailureReason::kVENUE_STATE_UNCERTAIN;
            group.last_update_ts_ns = update_ts_ns;
            portfolio_reconciliation_needed_ =
                risk_config_.require_portfolio_reconciliation;
            emit_group_state_update(group, update_ts_ns);
            emit_telemetry();
            maybe_report_execution_fault();
            return;
        }

        if(!group.residual_exposure_present){
            const bool repaired =
                group_has_confirmed_original_fill(group);
            group.execution_state = repaired
                ? GroupExecutionState::kFLATTENED
                : GroupExecutionState::kABORTED;
            group.failure_reason = repaired
                ? group.failure_reason
                : GroupFailureReason::kNONE;
            group.last_update_ts_ns = update_ts_ns;
            release_group_capital(group);
            if(repaired){
                ++telemetry_.group_repairs_completed;
            }
            emit_group_state_update(group, update_ts_ns);
            refresh_execution_incident_latch();
            emit_telemetry();
            return;
        }

        if(repair_order_live){
            group.execution_state = GroupExecutionState::kUNWINDING;
            return;
        }

        if(group.repair_attempt_count >=
            risk_config_.maximum_group_repair_attempts){
            fail_group_repair(
                group,
                GroupFailureReason::kUNWIND_FAILED,
                update_ts_ns);
            return;
        }

        std::size_t repair_count{};
        for(std::uint8_t i = 0; i < group.leg_count; ++i){
            repair_count +=
                static_cast<std::size_t>(
                    group.residual_yes_lots[i] != 0);
        }
        const std::size_t queued =
            queues_.kalshi_command_queue.producer_size();
        if(queued > queues_.kalshi_command_queue.capacity() ||
        queues_.kalshi_command_queue.capacity() - queued <
            repair_count){
            group.execution_state =
                GroupExecutionState::kREPAIR_REQUIRED;
            return;
        }

        std::size_t commands_sent{};
        for(std::uint8_t i = 0; i < group.leg_count; ++i){
            const auto residual = group.residual_yes_lots[i];
            if(residual == 0){
                continue;
            }
            const auto* original =
                find_order(group.original_leg_request_ids[i]);
            const auto quantity = absolute_lots(residual);
            if(original == nullptr || !quantity.has_value() ||
            *quantity == 0 ||
            *quantity > static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())){
                group.venue_state_uncertain = true;
                break;
            }

            const bool need_positive_yes = residual < 0;
            intent::OrderAction repair_action{};
            if(original->outcome == intent::Outcome::kYES){
                repair_action = need_positive_yes
                    ? intent::OrderAction::kBUY
                    : intent::OrderAction::kSELL;
            }else if(original->outcome == intent::Outcome::kNO){
                repair_action = need_positive_yes
                    ? intent::OrderAction::kSELL
                    : intent::OrderAction::kBUY;
            }else{
                group.venue_state_uncertain = true;
                break;
            }

            const auto repair_request_id =
                next_oms_request_id_++;
            const auto client_order_id =
                make_client_order_id(repair_request_id);
            if(client_order_id.empty()){
                group.venue_state_uncertain = true;
                break;
            }
            auto context = original->context.context;
            context.leg_index = i;
            const intent::NewOrderIntent repair_intent{
                .context = context,
                .outcome = original->outcome,
                .action = repair_action,
                .liquidity_intent =
                    intent::LiquidityIntent::kTAKER,
                .order_type =
                    intent::OrderType::kMARKETABLE_LIMIT,
                .time_in_force = intent::TimeInForce::kFOK,
                .price_ticks =
                    repair_action == intent::OrderAction::kBUY
                        ? static_cast<std::int64_t>(
                            kPRICE_TICKS_PER_DOLLAR - 1)
                        : 1,
                .quantity_lots =
                    static_cast<std::int64_t>(*quantity),
            };
            OrderRecord repair_record{
                .context = make_context(
                    repair_request_id,
                    context),
                .client_order_id = client_order_id,
                .order_state = OrderState::kPENDING_SUBMIT,
                .outcome = repair_intent.outcome,
                .action = repair_intent.action,
                .liquidity_intent =
                    repair_intent.liquidity_intent,
                .order_type = repair_intent.order_type,
                .time_in_force = repair_intent.time_in_force,
                .market_id = original->market_id,
                .ordered_qty_lots = repair_intent.quantity_lots,
                .working_price_ticks = repair_intent.price_ticks,
                .leaves_qty_lots = repair_intent.quantity_lots,
                .pending_command_oms_request_id =
                    repair_request_id,
                .pending_command_kind =
                    RestCommandKind::kSUBMIT_ORDER,
                .oms_group_id = group.oms_group_id,
                .group_leg_index = i,
                .repair_order = true,
            };
            oms_request_id_to_order_record_map_.emplace(
                repair_request_id,
                repair_record);
            client_order_id_to_oms_request_id_map_.emplace(
                client_order_id,
                repair_request_id);
            group.active_repair_request_ids[i] =
                repair_request_id;

            if(!send_kalshi_command(OmsToKalshiCommand{
                SubmitOrderCmd{
                    .oms_request_id = repair_request_id,
                    .client_order_id = client_order_id,
                    .new_order_intent = repair_intent,
                    .submission_ts_ns = update_ts_ns,
                    .reduce_only = true,
                }})){
                group.active_repair_request_ids[i] = 0;
                client_order_id_to_oms_request_id_map_.erase(
                    client_order_id);
                oms_request_id_to_order_record_map_.erase(
                    repair_request_id);
                group.venue_state_uncertain =
                    commands_sent > 0;
                break;
            }
            ++commands_sent;
        }

        if(group.venue_state_uncertain){
            group.execution_state = GroupExecutionState::kUNCERTAIN;
            group.failure_reason =
                GroupFailureReason::kVENUE_STATE_UNCERTAIN;
            group.last_update_ts_ns = update_ts_ns;
            latch_execution_incident(update_ts_ns);
            emit_group_state_update(group, update_ts_ns);
            emit_telemetry();
            maybe_report_execution_fault();
            return;
        }
        if(commands_sent == 0){
            return;
        }

        ++group.repair_attempt_count;
        ++telemetry_.group_repair_attempts;
        telemetry_.group_repair_commands_sent += commands_sent;
        auto& portfolio = ensure_strategy_portfolio(
            group.context.strategy_index);
        portfolio.open_order_count +=
            static_cast<std::uint32_t>(commands_sent);
        group.execution_state = GroupExecutionState::kUNWINDING;
        group.last_update_ts_ns = update_ts_ns;
        emit_group_state_update(group, update_ts_ns);
        emit_strategy_portfolio_update(portfolio, update_ts_ns);
        emit_telemetry();
    }

    std::size_t Oms::service_execution_incidents() noexcept{
        std::size_t progressed{};
        const auto update_ts_ns = now_ns();
        for(auto& [_, group] : group_by_id_){
            if(group.execution_state !=
                    GroupExecutionState::kREPAIR_REQUIRED &&
            group.execution_state !=
                    GroupExecutionState::kCANCELING_REMAINDER &&
            group.execution_state !=
                    GroupExecutionState::kUNWINDING){
                continue;
            }
            const auto previous_state = group.execution_state;
            const auto previous_cancel_attempts =
                group.cancel_attempt_count;
            const auto previous_repair_attempts =
                group.repair_attempt_count;
            continue_group_repair(group, update_ts_ns);
            if(group.execution_state != previous_state ||
            group.cancel_attempt_count != previous_cancel_attempts ||
            group.repair_attempt_count != previous_repair_attempts){
                ++progressed;
            }
        }
        maybe_report_execution_fault();
        return progressed;
    }

    void Oms::update_group_after_order_state_change(
        const OrderRecord& order,
        std::uint64_t update_ts_ns) noexcept{
        if(order.oms_group_id == 0){
            return;
        }
        auto* group = find_group(order.oms_group_id);
        if(group == nullptr){
            return;
        }

        if(order.repair_order ||
        group->execution_state ==
            GroupExecutionState::kREPAIR_REQUIRED ||
        group->execution_state ==
            GroupExecutionState::kCANCELING_REMAINDER ||
        group->execution_state ==
            GroupExecutionState::kUNWINDING ||
        group->execution_state ==
            GroupExecutionState::kUNCERTAIN ||
        group->execution_state ==
            GroupExecutionState::kREPAIR_FAILED){
            if(order.order_state == OrderState::kUNCERTAIN){
                group->venue_state_uncertain = true;
            }
            if(group->execution_state !=
                    GroupExecutionState::kREPAIR_FAILED){
                continue_group_repair(*group, update_ts_ns);
            }
            return;
        }

        if(group->capital_released){
            return;
        }

        bool all_filled = true;
        bool all_terminal = true;
        bool any_fill = false;
        bool any_uncertain = false;
        bool any_partial = false;
        bool any_terminal_failure = false;
        bool fills_fully_accounted = true;
        for(std::uint8_t i = 0; i < group->leg_count; ++i){
            const auto* leg =
                find_order(group->original_leg_request_ids[i]);
            if(leg == nullptr){
                any_uncertain = true;
                all_filled = false;
                all_terminal = false;
                fills_fully_accounted = false;
                continue;
            }
            all_filled = all_filled &&
                leg->order_state == OrderState::kFILLED;
            all_terminal = all_terminal &&
                is_terminal(leg->order_state);
            any_fill = any_fill ||
                leg->cumulative_filled_qty_lots > 0;
            any_uncertain = any_uncertain ||
                leg->order_state == OrderState::kUNCERTAIN;
            any_partial = any_partial ||
                leg->order_state == OrderState::kPARTIALLY_FILLED;
            any_terminal_failure = any_terminal_failure ||
                (is_terminal(leg->order_state) &&
                    leg->order_state != OrderState::kFILLED);
            fills_fully_accounted = fills_fully_accounted &&
                leg->reserved_capital_ticks == 0;
        }

        group->residual_exposure_present =
            group_has_residual_exposure(*group);
        if(any_uncertain || group->venue_state_uncertain ||
        (all_filled && !fills_fully_accounted)){
            begin_group_repair(
                *group,
                GroupFailureReason::kVENUE_STATE_UNCERTAIN,
                update_ts_ns);
        }else if(all_filled){
            group->execution_state =
                GroupExecutionState::kCOMPLETED;
            group->failure_reason = GroupFailureReason::kNONE;
            group->residual_exposure_present = false;
            group->last_update_ts_ns = update_ts_ns;
            release_group_capital(*group);
            emit_group_state_update(*group, update_ts_ns);
        }else if(all_terminal && !any_fill){
            group->execution_state =
                GroupExecutionState::kABORTED;
            group->failure_reason = GroupFailureReason::kNONE;
            group->residual_exposure_present = false;
            group->last_update_ts_ns = update_ts_ns;
            release_group_capital(*group);
            emit_group_state_update(*group, update_ts_ns);
        }else if(any_partial ||
        any_terminal_failure ||
        (all_terminal && any_fill)){
            begin_group_repair(
                *group,
                GroupFailureReason::kLEG_PARTIALLY_FILLED,
                update_ts_ns);
        }else{
            group->execution_state = any_fill
                ? GroupExecutionState::kPARTIALLY_FILLED
                : GroupExecutionState::kWORKING;
            group->last_update_ts_ns = update_ts_ns;
            emit_group_state_update(*group, update_ts_ns);
        }
    }

    void Oms::account_terminal_order(
        OrderRecord& order,
        std::uint64_t update_ts_ns) noexcept{
        if(!is_terminal(order.order_state) || order.terminal_accounted){
            return;
        }
        order.terminal_accounted = true;
        auto& portfolio = ensure_strategy_portfolio(
            order.context.context.strategy_index);
        if(portfolio.open_order_count > 0){
            --portfolio.open_order_count;
        }

        if(order.order_state != OrderState::kFILLED &&
        order.reserved_capital_ticks > 0){
            const auto released = std::min(
                portfolio.reserved_order_capital_ticks,
                order.reserved_capital_ticks);
            portfolio.reserved_order_capital_ticks -= released;
            if(auto* group = find_group(order.oms_group_id)){
                group->reserved_capital_ticks -=
                    std::min<std::uint64_t>(
                        group->reserved_capital_ticks,
                        static_cast<std::uint64_t>(released));
            }
            order.reserved_capital_ticks -= released;
        }

        recompute_all_strategy_available_capital();
        emit_strategy_portfolio_update(portfolio, update_ts_ns);
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

        if(execution_incident_active_){
            reject(RejectReason::kEXECUTION_BLOCKED);
            return;
        }
        if(!trading_enabled_){
            reject(RejectReason::kTRADING_DISABLED);
            return;
        }
        if(!portfolio_ready_for_trading()){
            reject(RejectReason::kOMS_NOT_READY);
            return;
        }
        if(intent.quantity_lots <= 0 ||
           intent.price_ticks <= 0 ||
           intent.outcome == intent::Outcome::kUNKNOWN ||
           intent.action == intent::OrderAction::kUNKNOWN){
            reject(RejectReason::kINVALID_INTENT);
            return;
        }
        const auto* route = find_market_route(intent.context.market_id);
        if(route == nullptr){
            ++telemetry_.unknown_market_rejects;
            reject(RejectReason::kUNKNOWN_MARKET);
            return;
        }
        if(!route->tradeable){
            ++telemetry_.non_tradeable_market_rejects;
            reject(RejectReason::kMARKET_NOT_TRADEABLE);
            return;
        }

        ClientOrderId client_order_id = make_client_order_id(oms_request_id);
        if(client_order_id.empty()){
            reject(RejectReason::kOMS_NOT_READY);
            return;
        }

        const auto required_capital =
            full_notional_reservation(intent.quantity_lots);
        if(!required_capital.has_value() ||
        *required_capital > static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max())){
            reject(RejectReason::kRISK_REJECTED);
            return;
        }
        auto& portfolio = ensure_strategy_portfolio(
            intent.context.strategy_index);
        const auto reservation =
            static_cast<std::int64_t>(*required_capital);
        if(portfolio.available_capital_ticks < reservation){
            reject(RejectReason::kRISK_REJECTED);
            return;
        }
        portfolio.reserved_order_capital_ticks += reservation;
        recompute_all_strategy_available_capital();

        SubmitOrderCmd command{
            .oms_request_id = oms_request_id,
            .client_order_id = client_order_id,
            .new_order_intent = intent,
            .submission_ts_ns = now_ns(),
        };
        if(!send_kalshi_command(OmsToKalshiCommand{command})){
            portfolio.reserved_order_capital_ticks -= reservation;
            recompute_all_strategy_available_capital();
            reject(RejectReason::kOMS_NOT_READY);
            return;
        }

        OrderRecord record{
            .context = context,
            .client_order_id = client_order_id,
            .order_state = OrderState::kPENDING_SUBMIT,
            .outcome = intent.outcome,
            .action = intent.action,
            .liquidity_intent = intent.liquidity_intent,
            .order_type = intent.order_type,
            .time_in_force = intent.time_in_force,
            .market_id = intent.context.market_id,
            .ordered_qty_lots = intent.quantity_lots,
            .working_price_ticks = intent.price_ticks,
            .leaves_qty_lots = intent.quantity_lots,
            .reserved_capital_ticks = reservation,
        };
        oms_request_id_to_order_record_map_.emplace(oms_request_id, record);
        client_order_id_to_oms_request_id_map_.emplace(client_order_id, oms_request_id);
        ++portfolio.open_order_count;

        ++telemetry_.strategy_intents_processed;
        OmsResponse response{
            .context = context,
            .response_type = OmsResponseType::kACCEPTED,
            .reject_reason = RejectReason::kNONE,
            .recv_ts_ns = recv_ts,
            .response_ts_ns = now_ns(),
        };
        (void)send_strategy_message(intent.context.strategy_index, OmsToStrategyMessage{response});
        emit_strategy_portfolio_update(portfolio, response.response_ts_ns);
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

        if(execution_incident_active_){
            ++telemetry_.strategy_intents_rejected;
            respond(
                OmsResponseType::kREJECTED,
                RejectReason::kEXECUTION_BLOCKED);
            return;
        }

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

    void Oms::handle_strategy_intent(
        const intent::GroupOrderIntent& intent) noexcept {

        const std::uint64_t recv_ts_ns = now_ns();

        const GroupAdmissionCheck admission =
            check_group_admission(intent);

        auto reject = [&](RejectReason reason) {
            ++telemetry_.strategy_intents_rejected;

            const GroupRecord rejected_group{
                .context = intent.context,
                .execution_state =
                    GroupExecutionState::kREJECTED,
                .leg_count = intent.leg_count,
                .last_update_ts_ns = now_ns(),
            };

            emit_group_admission_response(
                rejected_group,
                GroupAdmissionState::kREJECTED,
                reason,
                recv_ts_ns);
        };

        if(!admission.accepted) {
            reject(admission.reject_reason);
            return;
        }

        const OmsGroupId oms_group_id =
            next_oms_group_id_++;

        const intent::OmsRequestId batch_request_id =
            next_oms_request_id_++;

        const std::uint64_t submission_ts_ns = now_ns();

        GroupRecord group{
            .oms_group_id = oms_group_id,
            .context = intent.context,
            .batch_oms_request_id = batch_request_id,
            .execution_state =
                GroupExecutionState::kADMITTED,
            .leg_count = intent.leg_count,
            .reserved_capital_ticks =
                admission.required_capital_ticks,
            .admitted_ts_ns = submission_ts_ns,
            .last_update_ts_ns = submission_ts_ns,
        };

        SubmitOrderBatchCmd batch_command{
            .oms_request_id = batch_request_id,
            .oms_group_id = oms_group_id,
            .group_context = intent.context,
            .order_count = intent.leg_count,
            .submission_ts_ns = submission_ts_ns,
        };

        std::array<
            OrderRecord,
            intent::kMAX_ORDERS_PER_GROUP
        > order_records{};

        for(std::uint8_t i = 0;
            i < intent.leg_count;
            ++i) {
            const auto& leg = intent.new_orders[i];

            const intent::OmsRequestId leg_request_id =
                next_oms_request_id_++;

            ClientOrderId client_order_id =
                make_client_order_id(leg_request_id);

            if(client_order_id.empty()) {
                reject(RejectReason::kOMS_NOT_READY);
                return;
            }

            group.original_leg_request_ids[i] =
                leg_request_id;

            batch_command.orders[i] = SubmitOrderCmd{
                .oms_request_id = leg_request_id,
                .client_order_id = client_order_id,
                .new_order_intent = leg,
                .submission_ts_ns = submission_ts_ns,
            };

            order_records[i] = OrderRecord{
                .context = make_context(
                    leg_request_id,
                    leg.context),
                .client_order_id = client_order_id,
                .order_state =
                    OrderState::kPENDING_SUBMIT,
                .outcome = leg.outcome,
                .action = leg.action,
                .liquidity_intent =
                    leg.liquidity_intent,
                .order_type = leg.order_type,
                .time_in_force = leg.time_in_force,
                .market_id = leg.context.market_id,
                .ordered_qty_lots =
                    leg.quantity_lots,
                .working_price_ticks =
                    leg.price_ticks,
                .leaves_qty_lots =
                    leg.quantity_lots,
                .pending_command_oms_request_id =
                    leg_request_id,
                .pending_command_kind =
                    RestCommandKind::kSUBMIT_ORDER,
                .oms_group_id = oms_group_id,
                .group_leg_index = i,
                .repair_order = false,
                .reserved_capital_ticks =
                    static_cast<std::int64_t>(
                        *full_notional_reservation(
                            leg.quantity_lots)),
            };
        }

        if(!reserve_group_capital(group)) {
            reject(RejectReason::kRISK_REJECTED);
            return;
        }

        auto [group_it, group_inserted] =
            group_by_id_.emplace(
                oms_group_id,
                group);

        if(!group_inserted) {
            release_group_capital(group);
            reject(RejectReason::kDUPLICATE_INTENT);
            return;
        }

        group_id_by_strategy_intent_[
            intent.context.strategy_index
        ].emplace(
            intent.context.group_intent_id,
            oms_group_id);

        for(std::uint8_t i = 0;
            i < intent.leg_count;
            ++i) {
            const auto request_id =
                group.original_leg_request_ids[i];

            const auto& record = order_records[i];

            oms_request_id_to_order_record_map_.emplace(
                request_id,
                record);

            client_order_id_to_oms_request_id_map_.emplace(
                record.client_order_id,
                request_id);
        }

        if(!send_kalshi_command(
            OmsToKalshiCommand{
                batch_command
            })) {

            for(std::uint8_t i = 0;
                i < intent.leg_count;
                ++i) {
                const auto request_id =
                    group.original_leg_request_ids[i];

                client_order_id_to_oms_request_id_map_.erase(
                    order_records[i].client_order_id);

                oms_request_id_to_order_record_map_.erase(
                    request_id);
            }

            auto strategy_groups_it =
                group_id_by_strategy_intent_.find(
                    intent.context.strategy_index);

            if(strategy_groups_it !=
            group_id_by_strategy_intent_.end()) {
                strategy_groups_it->second.erase(
                    intent.context.group_intent_id);

                if(strategy_groups_it->second.empty()) {
                    group_id_by_strategy_intent_.erase(
                        strategy_groups_it);
                }
            }

            release_group_capital(group_it->second);
            group_by_id_.erase(group_it);

            reject(RejectReason::kOMS_NOT_READY);
            return;
        }

        group_it->second.execution_state =
            GroupExecutionState::kPENDING_VENUE;
        group_it->second.last_update_ts_ns =
            submission_ts_ns;

        ++telemetry_.strategy_intents_processed;

        auto& portfolio = ensure_strategy_portfolio(
            intent.context.strategy_index);

        portfolio.open_order_count += intent.leg_count;

        emit_group_admission_response(
            group_it->second,
            GroupAdmissionState::kACCEPTED,
            RejectReason::kNONE,
            recv_ts_ns);

        emit_group_state_update(
            group_it->second,
            submission_ts_ns);

        emit_strategy_portfolio_update(
            portfolio,
            submission_ts_ns);
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
        account_terminal_order(*record, response.transport_recv_ts_ns);
        if(!defer_group_state_updates_){
            update_group_after_order_state_change(
                *record,
                response.transport_recv_ts_ns);
        }
    }

    void Oms::handle_venue_event(const RestOrderBatchResponse& response) noexcept {

        const std::size_t safe_order_count =
            std::min<std::size_t>(
                response.requested_order_count,
                response.order_responses.size());

        const bool previous_defer = defer_group_state_updates_;
        defer_group_state_updates_ = true;
        bool batch_correlation_missing =
            safe_order_count != response.requested_order_count;

        for(std::size_t i = 0;
            i < safe_order_count;
            ++i) {

            RestOrderResponse leg_response =
                response.order_responses[i];

            // The adapter normally stamps every leg. Treat a missing result as
            // transport uncertainty rather than silently ignoring it.
            if(leg_response.result_code ==
            RestResultCode::kUNKNOWN) {
                leg_response.result_code =
                    response.result_code ==
                        RestResultCode::kUNKNOWN
                        ? RestResultCode::kTRANSPORT_ERROR
                        : response.result_code;

                leg_response.venue_reject_reason =
                    response.venue_reject_reason;

                if(leg_response.raw_reason_message.empty()) {
                    leg_response.raw_reason_message =
                        response.raw_reason_message;
                }
            }

            if(leg_response.context.oms_request_id == 0) {
                // There is no safe order record to mutate without its correlation
                // identifier. Full group handling will transition the group to
                // UNCERTAIN when this occurs.
                batch_correlation_missing = true;
                continue;
            }

            handle_venue_event(leg_response);
        }
        defer_group_state_updates_ = previous_defer;

        if(previous_defer){
            return;
        }
        auto* group = find_group(response.oms_group_id);
        if(group == nullptr || group->leg_count == 0){
            return;
        }
        const auto* first_order = find_order(
            group->original_leg_request_ids[0]);
        if(batch_correlation_missing || first_order == nullptr){
            group->venue_state_uncertain = true;
            begin_group_repair(
                *group,
                GroupFailureReason::kVENUE_STATE_UNCERTAIN,
                response.transport_recv_ts_ns);
            return;
        }
        update_group_after_order_state_change(
            *first_order,
            response.transport_recv_ts_ns);
    }

    void Oms::handle_venue_event(const PrivateWsOrderEvent& event) noexcept{
        ++telemetry_.private_ws_events_seen;

        if(event.event_kind == PrivateWsOrderEventKind::kMARKET_POSITION){
            const auto* route = find_market_route(event.market_id);
            apply_venue_market_position(
                VenueMarketPositionSnapshot{
                    .market_id = event.market_id,
                    .event_id = route == nullptr ? 0 : route->event_id,
                    .net_position_lots = event.net_position_lots,
                    .position_cost_ticks = event.position_cost_ticks,
                    .realized_pnl_ticks = event.realized_pnl_ticks,
                    .fees_paid_ticks = event.fees_paid_ticks,
                    .position_fee_cost_ticks =
                        event.position_fee_cost_ticks,
                    .volume_lots = event.volume_lots,
                    .position_cost_present = true,
                    .realized_pnl_present = true,
                    .fees_paid_present = true,
                },
                event.recv_ts_ns);
            ++telemetry_.venue_position_updates_seen;
            portfolio_reconciliation_needed_ =
                risk_config_.require_portfolio_reconciliation;
            emit_telemetry();
            return;
        }

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

        switch(event.event_kind){
            case PrivateWsOrderEventKind::kUSER_ORDER:
                record->order_state = event.order_state;
                record->outcome = event.outcome;
                record->market_id = event.market_id;
                record->ordered_qty_lots = event.ordered_qty_lots;
                record->cumulative_filled_qty_lots = event.cumulative_filled_qty_lots;
                record->leaves_qty_lots = event.leaves_qty_lots;
                clear_pending_command(*record);
                break;
            case PrivateWsOrderEventKind::kFILL:
                if(event.trade_id.empty()){
                    return;
                }
                if(!accounted_trade_ids_.insert(event.trade_id).second){
                    ++telemetry_.duplicate_fills_ignored;
                    return;
                }
                if(event.outcome != intent::Outcome::kUNKNOWN){
                    record->outcome = event.outcome;
                }
                if(event.market_id != 0){
                    record->market_id = event.market_id;
                }
                record->cumulative_filled_qty_lots += event.last_fill_qty_lots;
                if(record->leaves_qty_lots >= event.last_fill_qty_lots){
                    record->leaves_qty_lots -= event.last_fill_qty_lots;
                }
                if(record->leaves_qty_lots == 0 && record->ordered_qty_lots > 0){
                    record->order_state = OrderState::kFILLED;
                    clear_pending_command(*record);
                }else if(record->cumulative_filled_qty_lots > 0){
                    record->order_state = OrderState::kPARTIALLY_FILLED;
                }
                account_fill(*record, event);
                break;
            case PrivateWsOrderEventKind::kMARKET_POSITION:
            case PrivateWsOrderEventKind::kUNKNOWN:
                return;
        }
        emit_order_state_update(*record, VenueEventSource::kWEBSOCKET_FEED, event.recv_ts_ns);
        account_terminal_order(*record, event.recv_ts_ns);
        update_group_after_order_state_change(*record, event.recv_ts_ns);
    }

    void Oms::handle_venue_event(const ReconciledOrderSnapshot& /*snapshot*/) noexcept{
        ++telemetry_.reconciliation_events_seen;
    }

    void Oms::handle_venue_event(//NOLINT
        const VenuePortfolioSnapshot& snapshot) noexcept{
        ++telemetry_.reconciliation_events_seen;

        if(snapshot.reconciliation_id == 0 ||
        snapshot.reconciliation_id != active_reconciliation_id_){
            return;
        }
        if(active_order_universe_ == nullptr ||
        snapshot.universe_version != active_order_universe_->version){
            active_reconciliation_id_ = 0;
            portfolio_reconciliation_needed_ = true;
            next_reconciliation_due_ts_ns_ = 0;
            return;
        }
        active_reconciliation_id_ = 0;

        if(snapshot.result_code !=
            PortfolioReconciliationResultCode::kCOMPLETE){
            venue_portfolio_.reconciled = false;
            telemetry_.portfolio_reconciled = false;
            ++telemetry_.portfolio_reconciliations_failed;
            portfolio_reconciliation_needed_ = true;
            next_reconciliation_due_ts_ns_ =
                now_ns() + kPORTFOLIO_RECONCILIATION_RETRY_NS;
            if(trading_enabled_){
                trading_enabled_ = false;
                emit_trading_enabled_changed();
            }
            recompute_all_strategy_available_capital();
            emit_telemetry();
            return;
        }

        VenuePortfolioRecord next_portfolio{
            .available_balance_ticks =
                snapshot.available_balance_ticks,
            .portfolio_value_ticks =
                snapshot.portfolio_value_ticks,
            .reconciliation_id = snapshot.reconciliation_id,
            .received_ts_ns = snapshot.received_ts_ns,
            .reconciled = true,
        };
        std::unordered_map<
            intent::MarketId,
            VenueMarketPositionSnapshot
        > next_positions;
        next_positions.reserve(snapshot.market_positions.size());
        for(const auto& position : snapshot.market_positions){
            if(position.market_id != 0){
                next_positions[position.market_id] = position;
            }
        }

        venue_portfolio_ = next_portfolio;
        venue_position_by_market_ = std::move(next_positions);

        // Temporary, explicit single-strategy restart policy: when exactly one
        // strategy is wired, all reconciled venue inventory belongs to it.
        if(queues_.strategy_response_queues.size() == 1){
            auto& positions = position_by_strategy_and_market_[0];
            auto previous_positions = std::move(positions);
            positions.clear();
            auto& portfolio = ensure_strategy_portfolio(0);
            portfolio.inventory_exposure_ticks = 0;

            for(const auto& [market_id, venue_position] :
                venue_position_by_market_){
                const auto previous_it =
                    previous_positions.find(market_id);
                const auto previous_realized =
                    previous_it == previous_positions.end()
                        ? 0
                        : previous_it->second.realized_pnl_ticks;
                const auto previous_fees =
                    previous_it == previous_positions.end()
                        ? 0
                        : previous_it->second.fees_paid_ticks;
                const auto resolved_position_cost =
                    venue_position.position_cost_present
                        ? venue_position.position_cost_ticks
                        : previous_it == previous_positions.end()
                            ? 0
                            : previous_it->second.position_cost_ticks;
                const auto resolved_realized =
                    venue_position.realized_pnl_present
                        ? venue_position.realized_pnl_ticks
                        : previous_realized;
                const auto resolved_fees =
                    venue_position.fees_paid_present
                        ? venue_position.fees_paid_ticks
                        : previous_fees;
                const auto position_lots =
                    absolute_lots(venue_position.net_position_lots);
                auto resolved_average_entry =
                    previous_it == previous_positions.end()
                        ? 0
                        : previous_it->second.average_entry_price_ticks;
                if(venue_position.position_cost_present){
                    if(position_lots.has_value() &&
                    *position_lots > 0 &&
                    resolved_position_cost >= 0 &&
                    static_cast<std::uint64_t>(resolved_position_cost) <=
                        std::numeric_limits<std::uint64_t>::max() /
                            kQUANTITY_LOTS_PER_CONTRACT){
                        resolved_average_entry =
                            static_cast<std::int64_t>(
                                static_cast<std::uint64_t>(
                                    resolved_position_cost) *
                                kQUANTITY_LOTS_PER_CONTRACT /
                                *position_lots);
                    }else{
                        resolved_average_entry = 0;
                    }
                }
                const auto fallback_exposure = position_lots.has_value()
                    ? inventory_full_notional_exposure(*position_lots)
                    : std::nullopt;
                const auto safe_fallback_exposure =
                    fallback_exposure.has_value() &&
                    *fallback_exposure <=
                        static_cast<std::uint64_t>(
                            std::numeric_limits<std::int64_t>::max())
                        ? static_cast<std::int64_t>(*fallback_exposure)
                        : std::numeric_limits<std::int64_t>::max();
                auto& position = positions[market_id];
                position = StrategyMarketPositionRecord{
                    .strategy_index = 0,
                    .market_id = market_id,
                    .event_id = venue_position.event_id,
                    .net_position_lots =
                        venue_position.net_position_lots,
                    .average_entry_price_ticks =
                        resolved_average_entry,
                    .position_cost_ticks =
                        resolved_position_cost,
                    .market_exposure_ticks =
                        venue_position.market_exposure_present
                            ? venue_position.market_exposure_ticks
                            : safe_fallback_exposure,
                    .realized_pnl_ticks =
                        resolved_realized,
                    .fees_paid_ticks =
                        resolved_fees,
                };
                portfolio.inventory_exposure_ticks +=
                    position.market_exposure_ticks;
                portfolio.realized_pnl_ticks +=
                    position.realized_pnl_ticks - previous_realized;
                portfolio.fees_paid_ticks +=
                    position.fees_paid_ticks - previous_fees;
                emit_strategy_market_position_update(
                    position,
                    snapshot.received_ts_ns);
            }

            for(const auto& [market_id, previous] : previous_positions){
                if(positions.contains(market_id)){
                    continue;
                }
                auto [iter, _] = positions.emplace(
                    market_id,
                    StrategyMarketPositionRecord{
                        .strategy_index = 0,
                        .market_id = market_id,
                        .event_id = previous.event_id,
                        .realized_pnl_ticks =
                            previous.realized_pnl_ticks,
                        .fees_paid_ticks =
                            previous.fees_paid_ticks,
                    });
                emit_strategy_market_position_update(
                    iter->second,
                    snapshot.received_ts_ns);
            }
        }

        telemetry_.portfolio_reconciled = true;
        telemetry_.venue_available_balance_ticks =
            venue_portfolio_.available_balance_ticks;
        ++telemetry_.portfolio_reconciliations_completed;
        next_reconciliation_due_ts_ns_ =
            risk_config_.portfolio_reconciliation_interval_ns == 0
                ? 0
                : now_ns() +
                    risk_config_.portfolio_reconciliation_interval_ns;
        recompute_all_strategy_available_capital();
        for(auto& [_, portfolio] : portfolio_by_strategy_){
            emit_strategy_portfolio_update(
                portfolio,
                snapshot.received_ts_ns);
        }
        emit_telemetry();
        maybe_report_ready();
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
        trading_enabled_ =
            !execution_incident_active_ &&
            active_order_universe_ != nullptr &&
            active_order_universe_->version != 0 &&
            portfolio_ready_for_trading();
        emit_trading_enabled_changed();
    }

    void Oms::handle_control_command(const core::control::DisableTrading& /*command*/) noexcept{
        trading_enabled_ = false;
        emit_trading_enabled_changed();
    }

    void Oms::handle_control_command(const core::control::CancelAllOrders& /*command*/) noexcept{
        cancel_all_requested_ = true;
        trading_enabled_ = false;

        const std::uint64_t submission_ts = now_ns();
        std::uint64_t cancel_requests_sent{0};
        for(auto& [_, record] : oms_request_id_to_order_record_map_){
            if(emit_cancel_command_for_record(record, record.context.context, next_oms_request_id_++, submission_ts)){
                ++cancel_requests_sent;
            }
        }

        (void)send_control_status(core::control::OmsToControlStatus{
            core::control::OmsCancelAllStateChanged{
                .cancel_all_requested = cancel_all_requested_,
                .live_orders = live_order_count()
            }
        });
        (void)cancel_requests_sent;
    }

    void Oms::handle_control_command(const core::control::ApplyOrderRouteUniverse& command) noexcept{
        const auto previous_version = active_order_universe_ == nullptr
            ? 0
            : active_order_universe_->version;
        active_order_universe_ = command.snapshot;
        market_route_by_id_.clear();
        if(active_order_universe_ != nullptr){
            market_route_by_id_.reserve(active_order_universe_->market_routes.size());
            for(const auto& route : active_order_universe_->market_routes){
                market_route_by_id_[route.market_id] = route;
            }
            telemetry_.installed_universe_version = active_order_universe_->version;
        }else{
            telemetry_.installed_universe_version = 0;
        }
        if(active_order_universe_ == nullptr ||
        active_order_universe_->version != previous_version){
            ready_reported_ = false;
            if(trading_enabled_){
                trading_enabled_ = false;
                emit_trading_enabled_changed();
            }
            if(risk_config_.require_portfolio_reconciliation){
                venue_portfolio_.reconciled = false;
                telemetry_.portfolio_reconciled = false;
                portfolio_reconciliation_needed_ =
                    active_order_universe_ != nullptr;
                next_reconciliation_due_ts_ns_ = 0;
                recompute_all_strategy_available_capital();
            }
        }
        emit_telemetry();
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

    const core::control::OrderMarketRoute* Oms::find_market_route(intent::MarketId market_id) const noexcept{
        const auto iter = market_route_by_id_.find(market_id);
        if(iter == market_route_by_id_.end()){
            return nullptr;
        }
        return &iter->second;
    }

    bool Oms::is_tradeable_market(intent::MarketId market_id) const noexcept{
        const auto* route = find_market_route(market_id);
        return route != nullptr && route->tradeable;
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
