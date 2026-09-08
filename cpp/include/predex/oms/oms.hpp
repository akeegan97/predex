#pragma once
#include <cstdint>
#include <optional>
#include <vector>
#include <memory>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include "predex/oms/order_intents.hpp"
#include "predex/oms/oms_types.hpp"

#include "predex/control/control_types.hpp"
#include "predex/utils/spsc.hpp"
namespace predex::oms{

    enum class OmsPumpResult : std::uint8_t{
        kOK = 0,
        kNoWork = 1,
        kError = 2,
    };
    struct OrderRecord {
        OmsContext context{};
        ClientOrderId client_order_id{};
        std::optional<ExchangeOrderId> exchange_order_id;

        OrderState order_state{OrderState::kUNKNOWN};
        OrderState previous_order_state{OrderState::kUNKNOWN};

        intent::Outcome outcome{intent::Outcome::kUNKNOWN};
        intent::OrderAction action{intent::OrderAction::kUNKNOWN};
        intent::LiquidityIntent liquidity_intent{
            intent::LiquidityIntent::kUNKNOWN
        };
        intent::OrderType order_type{intent::OrderType::kLIMIT};
        intent::TimeInForce time_in_force{intent::TimeInForce::kGTC};
        intent::MarketId market_id{};

        std::int64_t ordered_qty_lots{};
        std::int64_t working_price_ticks{};
        std::int64_t cumulative_filled_qty_lots{};
        std::int64_t leaves_qty_lots{};

        intent::OmsRequestId pending_command_oms_request_id{};
        RestCommandKind pending_command_kind{RestCommandKind::kUNKNOWN};

        // Zero means this is an independent order.
        OmsGroupId oms_group_id{};
        std::uint8_t group_leg_index{};

        // True for an order generated internally to repair group exposure.
        bool repair_order{false};
        bool terminal_accounted{false};

        // Worst-case capital still attached to the unfilled portion.
        std::int64_t reserved_capital_ticks{};
    };
    struct OmsRiskConfig {
        // Virtual allocation applied independently to each strategy. The live
        // venue balance is acquired from reconciliation and remains a separate
        // account-wide constraint.
        std::int64_t strategy_allocation_limit_ticks{};
        std::int64_t venue_safety_reserve_ticks{};

        // Zero means no group may reserve capital.
        std::uint64_t maximum_group_reservation_ticks{};

        std::uint8_t maximum_group_legs{
            static_cast<std::uint8_t>(intent::kMAX_ORDERS_PER_GROUP)
        };

        std::uint8_t maximum_group_repair_attempts{2};
        std::uint64_t maximum_group_intent_age_ns{};
        std::uint64_t portfolio_reconciliation_interval_ns{};
        bool require_portfolio_reconciliation{false};
    };

    struct GroupAdmissionCheck {
        bool accepted{false};
        RejectReason reject_reason{RejectReason::kINVALID_INTENT};
        std::uint64_t required_capital_ticks{};
    };

    struct GroupRecord {
        OmsGroupId oms_group_id{};
        intent::IntentContext context{};
        intent::OmsRequestId batch_oms_request_id{};

        GroupExecutionState execution_state{
            GroupExecutionState::kUNKNOWN
        };
        GroupFailureReason failure_reason{
            GroupFailureReason::kNONE
        };

        std::array<
            intent::OmsRequestId,
            intent::kMAX_ORDERS_PER_GROUP
        > original_leg_request_ids{};

        std::array<
            intent::OmsRequestId,
            intent::kMAX_ORDERS_PER_GROUP
        > active_repair_request_ids{};

        // Confirmed net YES-equivalent exposure produced by each original leg
        // and all repair orders associated with it. Zero means that leg has no
        // residual exposure. This is updated only from deduplicated fill facts.
        std::array<
            std::int64_t,
            intent::kMAX_ORDERS_PER_GROUP
        > residual_yes_lots{};

        std::uint8_t leg_count{};
        std::uint8_t cancel_attempt_count{};
        std::uint8_t repair_attempt_count{};

        std::uint64_t reserved_capital_ticks{};

        bool residual_exposure_present{false};
        bool capital_released{false};
        bool venue_state_uncertain{false};

        std::uint64_t admitted_ts_ns{};
        std::uint64_t last_update_ts_ns{};
    };

    struct StrategyPortfolioRecord {
        std::uint16_t strategy_index{};
        PortfolioSequence next_sequence{1};

        std::int64_t allocation_limit_ticks{};
        std::int64_t available_capital_ticks{};
        std::int64_t reserved_order_capital_ticks{};
        std::int64_t inventory_exposure_ticks{};
        std::int64_t realized_pnl_ticks{};
        std::int64_t fees_paid_ticks{};

        std::uint32_t open_order_count{};
        std::uint32_t active_group_count{};
    };

    struct StrategyMarketPositionRecord {
        std::uint16_t strategy_index{};
        intent::MarketId market_id{};
        intent::EventId event_id{};

        // Positive is net YES; negative is net NO.
        std::int64_t net_position_lots{};

        std::int64_t resting_buy_qty_lots{};
        std::int64_t resting_sell_qty_lots{};

        std::int64_t average_entry_price_ticks{};
        std::int64_t position_cost_ticks{};
        std::int64_t market_exposure_ticks{};
        std::int64_t realized_pnl_ticks{};
        std::int64_t fees_paid_ticks{};
    };

    struct VenuePortfolioRecord {
        std::int64_t available_balance_ticks{};
        std::int64_t portfolio_value_ticks{};
        std::uint64_t reconciliation_id{};
        std::uint64_t received_ts_ns{};
        bool reconciled{false};
    };
    struct OmsQueues{
        std::vector<utils::SPSCQueue<intent::StrategyIntent>*> strategy_intent_queues;
        std::vector<utils::SPSCQueue<OmsToStrategyMessage>*> strategy_response_queues;
        utils::SPSCQueue<core::control::ControlToOmsCommand>& control_command_queue;
        utils::SPSCQueue<core::control::OmsToControlStatus>& oms_status_queue;
        utils::SPSCQueue<OmsToKalshiCommand>& kalshi_command_queue;
        std::vector<utils::SPSCQueue<KalshiToOmsEvent>*> venue_event_queues;

    };


    class Oms{
        public:
            explicit Oms(
            OmsQueues queues,
            OmsRiskConfig risk_config = {})
            : queues_(std::move(queues)),
            risk_config_(risk_config) {}

            [[nodiscard]] OmsPumpResult pump_once() noexcept;
            [[nodiscard]] std::size_t drain_strategy_intents(std::size_t max) noexcept;
            [[nodiscard]] std::size_t drain_venue_events(std::size_t max) noexcept;
            [[nodiscard]] std::size_t drain_control_commands(std::size_t max) noexcept;

        private:
            void handle_strategy_intent(const intent::NewOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::CancelOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::ModifyOrderIntent& intent) noexcept;
            void handle_strategy_intent(const intent::GroupOrderIntent& intent) noexcept;

            void handle_venue_event(
                const RestOrderResponse& response) noexcept;

            void handle_venue_event(
                const RestOrderBatchResponse& response) noexcept;

            void handle_venue_event(
                const PrivateWsOrderEvent& event) noexcept;

            void handle_venue_event(
                const ReconciledOrderSnapshot& snapshot) noexcept;

            void handle_venue_event(
                const VenuePortfolioSnapshot& snapshot) noexcept;

            void handle_venue_event(
                const OrderRestEgressDrained& drained) noexcept;
            
            void handle_control_command(const core::control::AllowTrading& command) noexcept;
            void handle_control_command(const core::control::DisableTrading& command) noexcept;
            void handle_control_command(const core::control::CancelAllOrders& command) noexcept;
            void handle_control_command(const core::control::ApplyOrderRouteUniverse& command) noexcept;

            [[nodiscard]] static bool is_terminal(OrderState state) noexcept;
            [[nodiscard]] static bool is_live(OrderState state) noexcept;
            [[nodiscard]] static bool is_cancelable(OrderState state) noexcept;

            [[nodiscard]] ClientOrderId make_client_order_id(intent::OmsRequestId oms_request_id) noexcept;
            [[nodiscard]] OmsContext make_context(intent::OmsRequestId oms_request_id, intent::IntentContext context) noexcept;
            [[nodiscard]] OrderStateUpdate make_order_state_update(const OrderRecord& record, VenueEventSource source, std::uint64_t update_ts_ns) const noexcept;
            [[nodiscard]] OrderRecord* find_order(intent::OmsRequestId oms_request_id) noexcept;
            [[nodiscard]] OrderRecord* find_order(const ClientOrderId& client_order_id) noexcept;
            [[nodiscard]] OrderRecord* find_order(const ExchangeOrderId& exchange_order_id) noexcept;
            [[nodiscard]] OrderRecord* find_order_for_rest_response(const RestOrderResponse& response) noexcept;

            [[nodiscard]] GroupRecord* find_group(
                OmsGroupId oms_group_id) noexcept;

            [[nodiscard]] GroupRecord* find_group(
                std::uint16_t strategy_index,
                intent::GroupIntentId group_intent_id) noexcept;

            [[nodiscard]] const GroupRecord* find_group(
                OmsGroupId oms_group_id) const noexcept;

            [[nodiscard]] const GroupRecord* find_group(
                std::uint16_t strategy_index,
                intent::GroupIntentId group_intent_id) const noexcept;

            [[nodiscard]] StrategyPortfolioRecord& ensure_strategy_portfolio(
                std::uint16_t strategy_index) noexcept;

            [[nodiscard]] StrategyMarketPositionRecord&
            ensure_strategy_market_position(
                std::uint16_t strategy_index,
                intent::MarketId market_id,
                intent::EventId event_id) noexcept;

            [[nodiscard]] GroupAdmissionCheck check_group_admission(
                const intent::GroupOrderIntent& group) const noexcept;

            [[nodiscard]] bool reserve_group_capital(
                GroupRecord& group) noexcept;

            void release_group_capital(GroupRecord& group) noexcept;

            void emit_group_admission_response(
                const GroupRecord& group,
                GroupAdmissionState admission_state,
                RejectReason reject_reason,
                std::uint64_t recv_ts_ns) noexcept;

            void emit_group_state_update(
                const GroupRecord& group,
                std::uint64_t update_ts_ns) noexcept;

            void emit_strategy_portfolio_update(
                StrategyPortfolioRecord& portfolio,
                std::uint64_t update_ts_ns) noexcept;

            void emit_strategy_market_position_update(
                StrategyMarketPositionRecord& position,
                std::uint64_t update_ts_ns) noexcept;

            [[nodiscard]] bool portfolio_ready_for_trading() const noexcept;
            [[nodiscard]] bool maybe_request_portfolio_reconciliation() noexcept;
            void maybe_report_ready() noexcept;
            void recompute_strategy_available_capital(
                StrategyPortfolioRecord& portfolio) noexcept;
            void recompute_all_strategy_available_capital() noexcept;
            void account_fill(
                OrderRecord& order,
                const PrivateWsOrderEvent& event) noexcept;
            void account_terminal_order(
                OrderRecord& order,
                std::uint64_t update_ts_ns) noexcept;
            void apply_venue_market_position(
                const VenueMarketPositionSnapshot& position,
                std::uint64_t update_ts_ns) noexcept;

            void update_group_after_order_state_change(
                const OrderRecord& order,
                std::uint64_t update_ts_ns) noexcept;

            void begin_group_repair(
                GroupRecord& group,
                GroupFailureReason reason,
                std::uint64_t update_ts_ns) noexcept;

            void continue_group_repair(
                GroupRecord& group,
                std::uint64_t update_ts_ns) noexcept;

            [[nodiscard]] std::size_t service_execution_incidents() noexcept;
            [[nodiscard]] bool group_has_residual_exposure(
                const GroupRecord& group) const noexcept;
            [[nodiscard]] bool group_has_confirmed_original_fill(
                const GroupRecord& group) noexcept;
            void latch_execution_incident(std::uint64_t update_ts_ns) noexcept;
            void refresh_execution_incident_latch() noexcept;
            void fail_group_repair(
                GroupRecord& group,
                GroupFailureReason reason,
                std::uint64_t update_ts_ns) noexcept;
            void maybe_report_execution_fault() noexcept;


            [[nodiscard]] const core::control::OrderMarketRoute* find_market_route(intent::MarketId market_id) const noexcept;
            [[nodiscard]] bool is_tradeable_market(intent::MarketId market_id) const noexcept;
            [[nodiscard]] bool send_strategy_message(std::uint16_t strategy_index, OmsToStrategyMessage message) noexcept;
            [[nodiscard]] bool send_kalshi_command(OmsToKalshiCommand command) noexcept;
            [[nodiscard]] bool send_control_status(core::control::OmsToControlStatus status) noexcept;
            [[nodiscard]] bool emit_cancel_command_for_record(OrderRecord& record, intent::IntentContext context, intent::OmsRequestId command_oms_request_id, std::uint64_t submission_ts_ns) noexcept;
            void emit_order_state_update(const OrderRecord& record, VenueEventSource source, std::uint64_t update_ts_ns) noexcept;
            void emit_trading_enabled_changed() noexcept;
            void emit_telemetry() noexcept;
            void clear_pending_command(OrderRecord& record) noexcept;
            void restore_previous_state(OrderRecord& record) noexcept;
            [[nodiscard]] std::uint64_t live_order_count() const noexcept;
            [[nodiscard]] std::uint64_t pending_submit_order_count() const noexcept;
            [[nodiscard]] std::uint64_t uncertain_order_count() const noexcept;
            [[nodiscard]] bool try_send_pending_control_status() noexcept;

            std::unordered_map<ClientOrderId, intent::OmsRequestId> client_order_id_to_oms_request_id_map_;
            std::unordered_map<ExchangeOrderId, intent::OmsRequestId> exchange_order_id_to_oms_request_id_map_;
            std::unordered_map<intent::OmsRequestId, OrderRecord> oms_request_id_to_order_record_map_;
            std::unordered_map<intent::MarketId, core::control::OrderMarketRoute> market_route_by_id_;
            std::shared_ptr<const core::control::OrderRouteUniverse> active_order_universe_;
            std::unordered_map<OmsGroupId, GroupRecord> group_by_id_;

            std::unordered_map<
                std::uint16_t,
                std::unordered_map<intent::GroupIntentId, OmsGroupId>
            > group_id_by_strategy_intent_;

            std::unordered_map<
                std::uint16_t,
                StrategyPortfolioRecord
            > portfolio_by_strategy_;

            std::unordered_map<
                std::uint16_t,
                std::unordered_map<
                    intent::MarketId,
                    StrategyMarketPositionRecord
                >
            > position_by_strategy_and_market_;
            std::unordered_map<
                intent::MarketId,
                VenueMarketPositionSnapshot
            > venue_position_by_market_;
            std::unordered_set<VenueTradeId> accounted_trade_ids_;
            OmsQueues queues_;
            OmsRiskConfig risk_config_;
            VenuePortfolioRecord venue_portfolio_;
            core::control::OmsTelemetrySnapshot telemetry_;
            bool trading_enabled_{false};
            bool cancel_all_requested_{false};
            bool ready_reported_{false};
            bool execution_incident_active_{false};
            bool execution_fault_reported_{false};
            bool defer_group_state_updates_{false};
            intent::OmsRequestId next_oms_request_id_{1};
            OmsGroupId next_oms_group_id_{1};
            std::uint64_t next_reconciliation_id_{1};
            std::uint64_t active_reconciliation_id_{0};
            std::uint64_t next_reconciliation_due_ts_ns_{0};
            bool portfolio_reconciliation_needed_{false};
            std::optional<core::control::OmsRestEgressDrained> pending_rest_egress_drained_;

    };


}
