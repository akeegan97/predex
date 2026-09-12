#include <gtest/gtest.h>

#include "predex/control/control_types.hpp"
#include "predex/oms/oms.hpp"
#include "predex/oms/oms_types.hpp"
#include "predex/oms/order_intents.hpp"
#include "predex/utils/monotonic_clock.hpp"
#include "predex/utils/spsc.hpp"

namespace {

namespace control = predex::core::control;
namespace oms = predex::oms;
namespace intent = predex::oms::intent;
namespace utils = predex::utils;

[[nodiscard]] oms::OmsRiskConfig default_risk_config() {
    return oms::OmsRiskConfig{
        .strategy_allocation_limit_ticks = 1'000'000,
        .maximum_group_reservation_ticks = 1'000'000,
        .maximum_group_legs = 2,
        .maximum_group_repair_attempts = 2,
        .maximum_group_intent_age_ns = 1'000'000'000,
    };
}

struct OmsHarness {
    utils::SPSCQueue<intent::StrategyIntent> strategy_to_oms{1024};
    utils::SPSCQueue<oms::OmsToStrategyMessage> oms_to_strategy{1024};
    utils::SPSCQueue<control::ControlToOmsCommand> control_to_oms{1024};
    utils::SPSCQueue<control::OmsToControlStatus> oms_to_control{1024};
    utils::SPSCQueue<oms::OmsToKalshiCommand> oms_to_kalshi{1024};
    utils::SPSCQueue<oms::KalshiToOmsEvent> kalshi_to_oms{1024};

    oms::OmsQueues queues{
        .strategy_intent_queues = {&strategy_to_oms},
        .strategy_response_queues = {&oms_to_strategy},
        .control_command_queue = control_to_oms,
        .oms_status_queue = oms_to_control,
        .kalshi_command_queue = oms_to_kalshi,
        .venue_event_queues = {&kalshi_to_oms},
    };

    oms::Oms uut;

    explicit OmsHarness(
        oms::OmsRiskConfig risk_config = default_risk_config())
        : uut(queues, risk_config) {}
};

[[nodiscard]] intent::NewOrderIntent make_valid_new_order() {
    return intent::NewOrderIntent{
        .context = intent::IntentContext{
            .strategy_index = 0,
            .market_id = 101,
            .event_id = 202,
            .strategy_id = 303,
            .strategy_intent_id = 404,
            .signal_id = 505,
        },
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kBUY,
        .liquidity_intent = intent::LiquidityIntent::kMAKER,
        .order_type = intent::OrderType::kLIMIT,
        .time_in_force = intent::TimeInForce::kGTC,
        .price_ticks = 4200,
        .quantity_lots = 7,
    };
}

[[nodiscard]] intent::GroupOrderIntent make_valid_group_order(
    intent::GroupIntentId group_intent_id = 1) {
    const auto now = utils::monotonic_now_ns();
    const intent::IntentContext group_context{
        .strategy_index = 0,
        .market_id = 101,
        .event_id = 202,
        .strategy_id = 303,
        .strategy_intent_id = 404,
        .signal_id = 505,
        .group_intent_id = group_intent_id,
        .leg_count = 2,
        .universe_version = 1,
        .event_revision = 1,
        .source_shard_index = 0,
        .ingress_timestamp_ns = now - 600,
        .book_apply_timestamp_ns = now - 500,
        .observation_publish_timestamp_ns = now - 400,
        .strategy_dequeue_timestamp_ns = now - 300,
        .evaluation_complete_timestamp_ns = now - 200,
        .intent_publish_timestamp_ns = now - 100,
    };
    auto easier_context = group_context;
    easier_context.market_id = 101;
    easier_context.strategy_intent_id = 405;
    easier_context.leg_index = 0;
    auto harder_context = group_context;
    harder_context.market_id = 102;
    harder_context.strategy_intent_id = 406;
    harder_context.leg_index = 1;

    intent::GroupOrderIntent group{
        .context = group_context,
        .leg_count = 2,
        .admission_policy =
            intent::GroupAdmissionPolicy::kALL_OR_NONE,
        .expected_gross_edge_ticks = 100,
        .estimated_fee_ticks = 10,
        .expected_net_edge_ticks = 90,
    };
    group.new_orders[0] = intent::NewOrderIntent{
        .context = easier_context,
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kBUY,
        .liquidity_intent = intent::LiquidityIntent::kTAKER,
        .order_type = intent::OrderType::kMARKETABLE_LIMIT,
        .time_in_force = intent::TimeInForce::kFOK,
        .price_ticks = 4'200,
        .quantity_lots = 100,
    };
    group.new_orders[1] = intent::NewOrderIntent{
        .context = harder_context,
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kSELL,
        .liquidity_intent = intent::LiquidityIntent::kTAKER,
        .order_type = intent::OrderType::kMARKETABLE_LIMIT,
        .time_in_force = intent::TimeInForce::kFOK,
        .price_ticks = 5'800,
        .quantity_lots = 100,
    };
    return group;
}

[[nodiscard]] intent::CancelOrderIntent make_valid_cancel_order(oms::ClientOrderId client_order_id) {
    return intent::CancelOrderIntent{
        .context = intent::IntentContext{
            .strategy_index = 0,
            .market_id = 101,
            .event_id = 202,
            .strategy_id = 303,
            .strategy_intent_id = 404,
            .signal_id = 505,
        },
        .target_oms_request_id = 1,
    };
}

[[nodiscard]] std::shared_ptr<const control::OrderRouteUniverse> make_order_universe(bool tradeable = true) {
    auto universe = std::make_shared<control::OrderRouteUniverse>();
    universe->version = 1;
    universe->market_routes.push_back(control::OrderMarketRoute{
        .market_id = 101,
        .event_id = 202,
        .kalshi_ticker = "TEST-MARKET",
        .tradeable = tradeable,
        .price_level_structure = control::PriceLevelStructure::kLINEAR_CENT,
    });
    universe->market_routes.push_back(control::OrderMarketRoute{
        .market_id = 102,
        .event_id = 202,
        .kalshi_ticker = "TEST-MARKET-2",
        .tradeable = tradeable,
        .price_level_structure = control::PriceLevelStructure::kLINEAR_CENT,
    });
    return universe;
}

void install_order_universe(OmsHarness& harness, bool tradeable = true) {
    ASSERT_TRUE(harness.control_to_oms.try_push(control::ControlToOmsCommand{
        control::ApplyOrderRouteUniverse{.snapshot = make_order_universe(tradeable)}
    }));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
}

void enable_trading(OmsHarness& harness) {
    install_order_universe(harness);
    ASSERT_TRUE(harness.control_to_oms.try_push(control::ControlToOmsCommand{control::AllowTrading{}}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    bool saw_trading_enabled{false};
    control::OmsToControlStatus status{};
    while(harness.oms_to_control.try_pop(status)){
        if(const auto* changed = std::get_if<control::OmsTradingEnabledChanged>(&status)){
            saw_trading_enabled = changed->trading_enabled;
            break;
        }
    }
    EXPECT_TRUE(saw_trading_enabled);
}

[[nodiscard]] oms::SubmitOrderCmd submit_order(OmsHarness& harness) {
    EXPECT_TRUE(harness.strategy_to_oms.try_push(intent::StrategyIntent{make_valid_new_order()}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage response_msg{};
    if(!harness.oms_to_strategy.try_pop(response_msg)){
        ADD_FAILURE() << "expected OMS response";
        return {};
    }
    const auto* response = std::get_if<oms::OmsResponse>(&response_msg);
    if(response == nullptr){
        ADD_FAILURE() << "expected OmsResponse";
        return {};
    }
    EXPECT_EQ(response->response_type, oms::OmsResponseType::kACCEPTED);
    EXPECT_EQ(response->reject_reason, oms::RejectReason::kNONE);

    oms::OmsToKalshiCommand command_msg{};
    if(!harness.oms_to_kalshi.try_pop(command_msg)){
        ADD_FAILURE() << "expected Kalshi command";
        return {};
    }
    const auto* submit = std::get_if<oms::SubmitOrderCmd>(&command_msg);
    if(submit == nullptr){
        ADD_FAILURE() << "expected SubmitOrderCmd";
        return {};
    }
    const auto result = *submit;
    while(harness.oms_to_strategy.try_pop(response_msg)){}
    return result;
}

[[nodiscard]] oms::SubmitOrderBatchCmd submit_group(
    OmsHarness& harness,
    intent::GroupIntentId group_intent_id = 1) {
    const auto group = make_valid_group_order(group_intent_id);
    EXPECT_TRUE(harness.strategy_to_oms.try_push(
        intent::StrategyIntent{group}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToKalshiCommand command{};
    if(!harness.oms_to_kalshi.try_pop(command)){
        ADD_FAILURE() << "expected SubmitOrderBatchCmd";
        return {};
    }
    const auto* batch =
        std::get_if<oms::SubmitOrderBatchCmd>(&command);
    if(batch == nullptr){
        ADD_FAILURE() << "expected SubmitOrderBatchCmd";
        return {};
    }
    oms::OmsToStrategyMessage strategy_message{};
    while(harness.oms_to_strategy.try_pop(strategy_message)){}
    return *batch;
}

[[nodiscard]] std::optional<oms::GroupStateUpdate>
latest_group_state(OmsHarness& harness) {
    std::optional<oms::GroupStateUpdate> result;
    oms::OmsToStrategyMessage message{};
    while(harness.oms_to_strategy.try_pop(message)){
        if(const auto* update =
            std::get_if<oms::GroupStateUpdate>(&message)){
            result = *update;
        }
    }
    return result;
}

TEST(OmsTest, RejectsNewOrdersWhenTradingDisabled) {
    OmsHarness harness{};

    ASSERT_TRUE(harness.strategy_to_oms.try_push(intent::StrategyIntent{make_valid_new_order()}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage message{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(message));
    const auto* response = std::get_if<oms::OmsResponse>(&message);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->response_type, oms::OmsResponseType::kREJECTED);
    EXPECT_EQ(response->reject_reason, oms::RejectReason::kTRADING_DISABLED);

    oms::OmsToKalshiCommand command{};
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(command));
}

TEST(OmsTest, AcceptsNewOrderWhenTradingEnabledAndEmitsSubmitCommand) {
    OmsHarness harness{};
    enable_trading(harness);

    const oms::SubmitOrderCmd submit = submit_order(harness);

    EXPECT_EQ(submit.oms_request_id, 1U);
    EXPECT_FALSE(submit.client_order_id.empty());
    EXPECT_EQ(submit.new_order_intent.context.market_id, 101U);
    EXPECT_EQ(submit.new_order_intent.price_ticks, 4200);
    EXPECT_EQ(submit.new_order_intent.quantity_lots, 7);
}

TEST(OmsTest, RejectsNewOrderForMarketOutsideInstalledUniverse) {
    OmsHarness harness{};
    enable_trading(harness);

    auto intent = make_valid_new_order();
    intent.context.market_id = 999;
    ASSERT_TRUE(harness.strategy_to_oms.try_push(intent::StrategyIntent{intent}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage message{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(message));
    const auto* response = std::get_if<oms::OmsResponse>(&message);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->response_type, oms::OmsResponseType::kREJECTED);
    EXPECT_EQ(response->reject_reason, oms::RejectReason::kUNKNOWN_MARKET);

    oms::OmsToKalshiCommand command{};
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(command));
}

TEST(OmsTest, RestSubmitAckMovesOrderToWorking) {
    OmsHarness harness{};
    enable_trading(harness);
    const oms::SubmitOrderCmd submit = submit_order(harness);

    oms::ExchangeOrderId exchange_order_id{};
    ASSERT_TRUE(exchange_order_id.assign_from("venue-order-1"));

    oms::RestOrderResponse ack{
        .context = oms::OmsContext{
            .oms_request_id = submit.oms_request_id,
            .context = submit.new_order_intent.context,
        },
        .command_kind = oms::RestCommandKind::kSUBMIT_ORDER,
        .result_code = oms::RestResultCode::kACKED,
        .client_order_id = submit.client_order_id,
        .exchange_order_id = exchange_order_id,
        .transport_submit_ts_ns = submit.submission_ts_ns,
        .transport_recv_ts_ns = submit.submission_ts_ns + 1000,
        .http_status_code = 200,
    };

    ASSERT_TRUE(harness.kalshi_to_oms.try_push(oms::KalshiToOmsEvent{ack}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage update_msg{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(update_msg));
    const auto* update = std::get_if<oms::OrderStateUpdate>(&update_msg);
    ASSERT_NE(update, nullptr);
    EXPECT_EQ(update->context.oms_request_id, submit.oms_request_id);
    EXPECT_EQ(update->order_state, oms::OrderState::kWORKING);
    EXPECT_EQ(update->update_source, oms::VenueEventSource::kREST_RESPONSE);
    EXPECT_EQ(update->exchange_order_id, exchange_order_id);
    EXPECT_EQ(update->ordered_qty_lots, 7);
    EXPECT_EQ(update->leaves_qty_lots, 7);
}

TEST(OmsTest, PrivateWsEventUpdatesCanonicalLifecycle) {
    OmsHarness harness{};
    enable_trading(harness);
    const oms::SubmitOrderCmd submit = submit_order(harness);

    oms::ExchangeOrderId exchange_order_id{};
    ASSERT_TRUE(exchange_order_id.assign_from("venue-order-1"));

    oms::PrivateWsOrderEvent event{
        .event_kind = oms::PrivateWsOrderEventKind::kUSER_ORDER,
        .client_order_id = submit.client_order_id,
        .exchange_order_id = exchange_order_id,
        .market_id = submit.new_order_intent.context.market_id,
        .outcome = intent::Outcome::kYES,
        .recv_ts_ns = 100,
        .venue_ts_ns = 90,
        .ws_sequence = 1,
        .order_state = oms::OrderState::kPARTIALLY_FILLED,
        .ordered_qty_lots = 7,
        .cumulative_filled_qty_lots = 3,
        .leaves_qty_lots = 4,
        .last_fill_qty_lots = 3,
        .last_fill_price_ticks = 4200,
    };

    ASSERT_TRUE(harness.kalshi_to_oms.try_push(oms::KalshiToOmsEvent{event}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage update_msg{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(update_msg));
    const auto* update = std::get_if<oms::OrderStateUpdate>(&update_msg);
    ASSERT_NE(update, nullptr);
    EXPECT_EQ(update->context.oms_request_id, submit.oms_request_id);
    EXPECT_EQ(update->order_state, oms::OrderState::kPARTIALLY_FILLED);
    EXPECT_EQ(update->update_source, oms::VenueEventSource::kWEBSOCKET_FEED);
    EXPECT_EQ(update->ordered_qty_lots, 7);
    EXPECT_EQ(update->cumulative_filled_qty_lots, 3);
    EXPECT_EQ(update->leaves_qty_lots, 4);
}

TEST(OmsTest, CancelOrderUnknownTarget){
    OmsHarness harness{};
    enable_trading(harness);

    intent::CancelOrderIntent cancel{
        .context = intent::IntentContext{
            .strategy_index = 0,
            .market_id = 101,
            .event_id = 202,
            .strategy_id = 303,
            .strategy_intent_id = 404,
            .signal_id = 505,
        },
        .target_oms_request_id = 9999, // unknown
    };

    ASSERT_TRUE(harness.strategy_to_oms.try_push(intent::StrategyIntent{cancel}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToStrategyMessage response_msg{};
    ASSERT_TRUE(harness.oms_to_strategy.try_pop(response_msg));
    const auto* response = std::get_if<oms::OmsResponse>(&response_msg);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->response_type, oms::OmsResponseType::kREJECTED);
    EXPECT_EQ(response->reject_reason, oms::RejectReason::kUNKNOWN_TARGET_ORDER);
}

TEST(OmsTest, PortfolioReconciliationGatesTradingAndInstallsVenueState) {
    auto risk = default_risk_config();
    risk.require_portfolio_reconciliation = true;
    risk.venue_safety_reserve_ticks = 10'000;
    risk.portfolio_reconciliation_interval_ns = 5'000'000'000;
    OmsHarness harness{risk};

    install_order_universe(harness);

    oms::OmsToKalshiCommand reconcile_message{};
    ASSERT_TRUE(harness.oms_to_kalshi.try_pop(reconcile_message));
    const auto* reconcile =
        std::get_if<oms::RequestPortfolioReconciliation>(
            &reconcile_message);
    ASSERT_NE(reconcile, nullptr);
    EXPECT_NE(reconcile->reconciliation_id, 0U);

    ASSERT_TRUE(harness.control_to_oms.try_push(
        control::ControlToOmsCommand{control::AllowTrading{}}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    bool enabled_before_snapshot{false};
    control::OmsToControlStatus status{};
    while(harness.oms_to_control.try_pop(status)){
        if(const auto* changed =
            std::get_if<control::OmsTradingEnabledChanged>(&status)){
            enabled_before_snapshot = changed->trading_enabled;
        }
    }
    EXPECT_FALSE(enabled_before_snapshot);

    oms::VenuePortfolioSnapshot snapshot{
        .reconciliation_id = reconcile->reconciliation_id,
        .universe_version = 1,
        .result_code =
            oms::PortfolioReconciliationResultCode::kCOMPLETE,
        .available_balance_ticks = 2'000'000,
        .portfolio_value_ticks = 750'000,
        .market_positions = {
            oms::VenueMarketPositionSnapshot{
                .market_ticker = "TEST-MARKET",
                .market_id = 101,
                .event_id = 202,
                .net_position_lots = 10,
                .market_exposure_ticks = 1'000,
                .market_exposure_present = true,
            }
        },
        .request_ts_ns = reconcile->submission_ts_ns,
        .received_ts_ns = reconcile->submission_ts_ns + 1'000,
    };
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{snapshot}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    std::optional<oms::StrategyPortfolioUpdate> portfolio_update;
    std::optional<oms::StrategyMarketPositionUpdate> position_update;
    oms::OmsToStrategyMessage strategy_message{};
    while(harness.oms_to_strategy.try_pop(strategy_message)){
        if(const auto* update =
            std::get_if<oms::StrategyPortfolioUpdate>(
                &strategy_message)){
            portfolio_update = *update;
        }
        if(const auto* update =
            std::get_if<oms::StrategyMarketPositionUpdate>(
                &strategy_message)){
            position_update = *update;
        }
    }
    ASSERT_TRUE(portfolio_update.has_value());
    EXPECT_TRUE(portfolio_update->venue_portfolio_reconciled);
    EXPECT_EQ(portfolio_update->venue_available_balance_ticks, 2'000'000);
    EXPECT_EQ(portfolio_update->inventory_exposure_ticks, 1'000);
    EXPECT_EQ(portfolio_update->available_capital_ticks, 999'000);
    ASSERT_TRUE(position_update.has_value());
    EXPECT_EQ(position_update->net_position_lots, 10);

    ASSERT_TRUE(harness.control_to_oms.try_push(
        control::ControlToOmsCommand{control::AllowTrading{}}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    bool enabled_after_snapshot{false};
    while(harness.oms_to_control.try_pop(status)){
        if(const auto* changed =
            std::get_if<control::OmsTradingEnabledChanged>(&status)){
            enabled_after_snapshot = changed->trading_enabled;
        }
    }
    EXPECT_TRUE(enabled_after_snapshot);

    oms::PrivateWsOrderEvent settled_position{
        .event_kind = oms::PrivateWsOrderEventKind::kMARKET_POSITION,
        .market_id = 101,
        .recv_ts_ns = snapshot.received_ts_ns + 1'000,
        .net_position_lots = 0,
        .position_cost_ticks = 0,
        .realized_pnl_ticks = 250,
        .fees_paid_ticks = 50,
    };
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{settled_position}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    portfolio_update.reset();
    position_update.reset();
    while(harness.oms_to_strategy.try_pop(strategy_message)){
        if(const auto* update =
            std::get_if<oms::StrategyPortfolioUpdate>(
                &strategy_message)){
            portfolio_update = *update;
        }
        if(const auto* update =
            std::get_if<oms::StrategyMarketPositionUpdate>(
                &strategy_message)){
            position_update = *update;
        }
    }
    ASSERT_TRUE(position_update.has_value());
    EXPECT_EQ(position_update->net_position_lots, 0);
    EXPECT_EQ(position_update->market_exposure_ticks, 0);
    EXPECT_EQ(position_update->realized_pnl_ticks, 250);
    EXPECT_EQ(position_update->fees_paid_ticks, 50);
    ASSERT_TRUE(portfolio_update.has_value());
    EXPECT_EQ(portfolio_update->inventory_exposure_ticks, 0);
    EXPECT_EQ(portfolio_update->available_capital_ticks, 1'000'000);
    EXPECT_EQ(portfolio_update->realized_pnl_ticks, 250);
    EXPECT_EQ(portfolio_update->fees_paid_ticks, 50);
}

TEST(OmsTest, FillTransfersReservationIntoInventoryAndDeduplicatesTrade) {
    OmsHarness harness{};
    enable_trading(harness);
    const oms::SubmitOrderCmd submit = submit_order(harness);

    oms::PrivateWsOrderEvent fill{
        .event_kind = oms::PrivateWsOrderEventKind::kFILL,
        .client_order_id = submit.client_order_id,
        .market_id = 101,
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kBUY,
        .recv_ts_ns = 1'000,
        .venue_ts_ns = 900,
        .ws_sequence = 1,
        .last_fill_qty_lots = 7,
        .last_fill_price_ticks = 4'200,
    };
    ASSERT_TRUE(fill.trade_id.assign_from("trade-1"));

    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{fill}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    std::optional<oms::StrategyPortfolioUpdate> portfolio_update;
    std::optional<oms::StrategyMarketPositionUpdate> position_update;
    oms::OmsToStrategyMessage message{};
    while(harness.oms_to_strategy.try_pop(message)){
        if(const auto* update =
            std::get_if<oms::StrategyPortfolioUpdate>(&message)){
            portfolio_update = *update;
        }
        if(const auto* update =
            std::get_if<oms::StrategyMarketPositionUpdate>(&message)){
            position_update = *update;
        }
    }

    ASSERT_TRUE(position_update.has_value());
    EXPECT_EQ(position_update->net_position_lots, 7);
    EXPECT_EQ(position_update->average_entry_price_ticks, 4'200);
    EXPECT_EQ(position_update->position_cost_ticks, 294);
    EXPECT_EQ(position_update->market_exposure_ticks, 700);

    ASSERT_TRUE(portfolio_update.has_value());
    EXPECT_EQ(portfolio_update->reserved_order_capital_ticks, 0);
    EXPECT_EQ(portfolio_update->inventory_exposure_ticks, 700);
    EXPECT_EQ(portfolio_update->available_capital_ticks, 999'300);
    EXPECT_EQ(portfolio_update->open_order_count, 0U);

    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{fill}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    EXPECT_FALSE(harness.oms_to_strategy.try_pop(message));
}

TEST(OmsTest, KnownLegMismatchLatchesAdmissionsAndCompletesReduceOnlyRepair) {
    OmsHarness harness{};
    enable_trading(harness);
    const auto batch = submit_group(harness);

    oms::PrivateWsOrderEvent first_leg_fill{
        .event_kind = oms::PrivateWsOrderEventKind::kFILL,
        .client_order_id = batch.orders[0].client_order_id,
        .market_id = 101,
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kBUY,
        .recv_ts_ns = 1'000,
        .venue_ts_ns = 900,
        .ws_sequence = 1,
        .last_fill_qty_lots = 100,
        .last_fill_price_ticks = 4'200,
    };
    ASSERT_TRUE(first_leg_fill.trade_id.assign_from("group-fill-1"));
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{first_leg_fill}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    // A fill notification for one FOK leg can precede the other leg's fill;
    // it is not by itself proof that the package failed.
    oms::OmsToKalshiCommand venue_command{};
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(venue_command));
    auto state = latest_group_state(harness);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(
        state->execution_state,
        oms::GroupExecutionState::kPARTIALLY_FILLED);

    const auto& failed_leg = batch.orders[1];
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{oms::RestOrderResponse{
            .context = oms::OmsContext{
                .oms_request_id = failed_leg.oms_request_id,
                .context = failed_leg.new_order_intent.context,
            },
            .command_kind = oms::RestCommandKind::kSUBMIT_ORDER,
            .result_code = oms::RestResultCode::kREJECTED,
            .client_order_id = failed_leg.client_order_id,
            .transport_recv_ts_ns = 2'000,
            .http_status_code = 400,
        }}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    ASSERT_TRUE(harness.oms_to_kalshi.try_pop(venue_command));
    const auto* repair =
        std::get_if<oms::SubmitOrderCmd>(&venue_command);
    ASSERT_NE(repair, nullptr);
    EXPECT_TRUE(repair->reduce_only);
    EXPECT_EQ(repair->new_order_intent.context.market_id, 101U);
    EXPECT_EQ(repair->new_order_intent.outcome, intent::Outcome::kYES);
    EXPECT_EQ(repair->new_order_intent.action, intent::OrderAction::kSELL);
    EXPECT_EQ(repair->new_order_intent.time_in_force, intent::TimeInForce::kFOK);
    EXPECT_EQ(repair->new_order_intent.price_ticks, 1);
    EXPECT_EQ(repair->new_order_intent.quantity_lots, 100);

    state = latest_group_state(harness);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(
        state->execution_state,
        oms::GroupExecutionState::kUNWINDING);
    EXPECT_TRUE(state->residual_exposure_present);
    EXPECT_EQ(state->repair_attempt_count, 1U);

    ASSERT_TRUE(harness.strategy_to_oms.try_push(
        intent::StrategyIntent{make_valid_group_order(2)}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    std::optional<oms::GroupAdmissionResponse> blocked_response;
    oms::OmsToStrategyMessage strategy_message{};
    while(harness.oms_to_strategy.try_pop(strategy_message)){
        if(const auto* response =
            std::get_if<oms::GroupAdmissionResponse>(
                &strategy_message)){
            blocked_response = *response;
        }
    }
    ASSERT_TRUE(blocked_response.has_value());
    EXPECT_EQ(
        blocked_response->admission_state,
        oms::GroupAdmissionState::kREJECTED);
    EXPECT_EQ(
        blocked_response->reject_reason,
        oms::RejectReason::kEXECUTION_BLOCKED);
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(venue_command));

    oms::PrivateWsOrderEvent repair_fill{
        .event_kind = oms::PrivateWsOrderEventKind::kFILL,
        .client_order_id = repair->client_order_id,
        .market_id = 101,
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kSELL,
        .recv_ts_ns = 3'000,
        .venue_ts_ns = 2'900,
        .ws_sequence = 2,
        .last_fill_qty_lots = 100,
        .last_fill_price_ticks = 4'100,
    };
    ASSERT_TRUE(repair_fill.trade_id.assign_from("group-repair-fill-1"));
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{repair_fill}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    state = latest_group_state(harness);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(
        state->execution_state,
        oms::GroupExecutionState::kFLATTENED);
    EXPECT_FALSE(state->residual_exposure_present);

    // Resolution clears the authoritative incident latch, but OMS remains
    // disabled until the operator explicitly enables it again.
    ASSERT_TRUE(harness.control_to_oms.try_push(
        control::ControlToOmsCommand{control::AllowTrading{}}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    bool reenabled{false};
    control::OmsToControlStatus control_status{};
    while(harness.oms_to_control.try_pop(control_status)){
        if(const auto* changed =
            std::get_if<control::OmsTradingEnabledChanged>(
                &control_status)){
            reenabled = changed->trading_enabled;
        }
    }
    EXPECT_TRUE(reenabled);
}

TEST(OmsTest, BatchWideNoFillRejectAbortsWithoutFalseRepairIncident) {
    OmsHarness harness{};
    enable_trading(harness);
    const auto batch = submit_group(harness);

    oms::RestOrderBatchResponse response{
        .batch_oms_request_id = batch.oms_request_id,
        .oms_group_id = batch.oms_group_id,
        .group_context = batch.group_context,
        .result_code = oms::RestResultCode::kACKED,
        .requested_order_count = batch.order_count,
        .response_order_count = batch.order_count,
        .transport_recv_ts_ns = 1'000,
        .http_status_code = 200,
    };
    for(std::uint8_t i = 0; i < batch.order_count; ++i){
        response.order_responses[i] = oms::RestOrderResponse{
            .context = oms::OmsContext{
                .oms_request_id = batch.orders[i].oms_request_id,
                .context = batch.orders[i].new_order_intent.context,
            },
            .command_kind = oms::RestCommandKind::kSUBMIT_ORDER,
            .result_code = oms::RestResultCode::kREJECTED,
            .client_order_id = batch.orders[i].client_order_id,
            .transport_recv_ts_ns = 1'000,
            .http_status_code = 400,
        };
    }
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{response}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToKalshiCommand repair{};
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(repair));
    const auto state = latest_group_state(harness);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(
        state->execution_state,
        oms::GroupExecutionState::kABORTED);
    EXPECT_FALSE(state->residual_exposure_present);

    // The all-rejected batch was atomically safe, so normal admission remains
    // enabled without requiring an operator reset.
    ASSERT_TRUE(harness.strategy_to_oms.try_push(
        intent::StrategyIntent{make_valid_group_order(2)}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    ASSERT_TRUE(harness.oms_to_kalshi.try_pop(repair));
    EXPECT_TRUE(std::holds_alternative<oms::SubmitOrderBatchCmd>(repair));
}

TEST(OmsTest, PartialOriginalFillCancelsRemainderBeforeUnwind) {
    OmsHarness harness{};
    enable_trading(harness);
    const auto batch = submit_group(harness);

    oms::PrivateWsOrderEvent partial_fill{
        .event_kind = oms::PrivateWsOrderEventKind::kFILL,
        .client_order_id = batch.orders[0].client_order_id,
        .market_id = 101,
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kBUY,
        .recv_ts_ns = 1'000,
        .ws_sequence = 1,
        .last_fill_qty_lots = 40,
        .last_fill_price_ticks = 4'200,
    };
    ASSERT_TRUE(partial_fill.trade_id.assign_from("partial-original"));
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{partial_fill}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    std::array<oms::CancelOrderCmd, 2> cancels{};
    for(auto& cancel : cancels){
        oms::OmsToKalshiCommand command{};
        ASSERT_TRUE(harness.oms_to_kalshi.try_pop(command));
        const auto* parsed = std::get_if<oms::CancelOrderCmd>(&command);
        ASSERT_NE(parsed, nullptr);
        cancel = *parsed;
    }
    EXPECT_NE(
        cancels[0].cancel_order_intent.target_oms_request_id,
        cancels[1].cancel_order_intent.target_oms_request_id);
    oms::OmsToKalshiCommand unexpected{};
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(unexpected));

    for(std::size_t i = 0; i < batch.order_count; ++i){
        ASSERT_TRUE(harness.kalshi_to_oms.try_push(
            oms::KalshiToOmsEvent{oms::PrivateWsOrderEvent{
                .event_kind =
                    oms::PrivateWsOrderEventKind::kUSER_ORDER,
                .client_order_id =
                    batch.orders[i].client_order_id,
                .market_id =
                    batch.orders[i].new_order_intent.context.market_id,
                .outcome = intent::Outcome::kYES,
                .recv_ts_ns = 2'000 + i,
                .ws_sequence = 2 + i,
                .order_state = oms::OrderState::kCANCELED,
                .ordered_qty_lots = 100,
                .cumulative_filled_qty_lots = i == 0 ? 40 : 0,
                .leaves_qty_lots = 0,
            }}));
        EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    }

    ASSERT_TRUE(harness.oms_to_kalshi.try_pop(unexpected));
    const auto* repair = std::get_if<oms::SubmitOrderCmd>(&unexpected);
    ASSERT_NE(repair, nullptr);
    EXPECT_TRUE(repair->reduce_only);
    EXPECT_EQ(repair->new_order_intent.quantity_lots, 40);
    EXPECT_EQ(repair->new_order_intent.action, intent::OrderAction::kSELL);
}

TEST(OmsTest, ExhaustedRepairAttemptsRemainFaultedAndCannotReenableTrading) {
    OmsHarness harness{};
    enable_trading(harness);
    const auto batch = submit_group(harness);

    oms::PrivateWsOrderEvent fill{
        .event_kind = oms::PrivateWsOrderEventKind::kFILL,
        .client_order_id = batch.orders[0].client_order_id,
        .market_id = 101,
        .outcome = intent::Outcome::kYES,
        .action = intent::OrderAction::kBUY,
        .recv_ts_ns = 1'000,
        .ws_sequence = 1,
        .last_fill_qty_lots = 100,
        .last_fill_price_ticks = 4'200,
    };
    ASSERT_TRUE(fill.trade_id.assign_from("failure-original-fill"));
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{fill}));
    ASSERT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    const auto& failed_leg = batch.orders[1];
    ASSERT_TRUE(harness.kalshi_to_oms.try_push(
        oms::KalshiToOmsEvent{oms::RestOrderResponse{
            .context = oms::OmsContext{
                .oms_request_id = failed_leg.oms_request_id,
                .context = failed_leg.new_order_intent.context,
            },
            .command_kind = oms::RestCommandKind::kSUBMIT_ORDER,
            .result_code = oms::RestResultCode::kREJECTED,
            .client_order_id = failed_leg.client_order_id,
            .transport_recv_ts_ns = 2'000,
        }}));
    ASSERT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);

    oms::SubmitOrderCmd last_repair{};
    for(std::uint8_t attempt = 0; attempt < 2; ++attempt){
        oms::OmsToKalshiCommand command{};
        ASSERT_TRUE(harness.oms_to_kalshi.try_pop(command));
        const auto* repair = std::get_if<oms::SubmitOrderCmd>(&command);
        ASSERT_NE(repair, nullptr);
        last_repair = *repair;
        ASSERT_TRUE(harness.kalshi_to_oms.try_push(
            oms::KalshiToOmsEvent{oms::RestOrderResponse{
                .context = oms::OmsContext{
                    .oms_request_id = repair->oms_request_id,
                    .context = repair->new_order_intent.context,
                },
                .command_kind = oms::RestCommandKind::kSUBMIT_ORDER,
                .result_code = oms::RestResultCode::kREJECTED,
                .client_order_id = repair->client_order_id,
                .transport_recv_ts_ns =
                    3'000 + static_cast<std::uint64_t>(attempt),
            }}));
        EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    }
    (void)last_repair;

    oms::OmsToKalshiCommand no_third_attempt{};
    EXPECT_FALSE(harness.oms_to_kalshi.try_pop(no_third_attempt));
    const auto state = latest_group_state(harness);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(
        state->execution_state,
        oms::GroupExecutionState::kREPAIR_FAILED);
    EXPECT_EQ(state->repair_attempt_count, 2U);
    EXPECT_TRUE(state->residual_exposure_present);

    ASSERT_TRUE(harness.control_to_oms.try_push(
        control::ControlToOmsCommand{control::AllowTrading{}}));
    EXPECT_EQ(harness.uut.pump_once(), oms::OmsPumpResult::kOK);
    bool saw_enabled{false};
    bool saw_fault{false};
    control::OmsToControlStatus status{};
    while(harness.oms_to_control.try_pop(status)){
        if(const auto* changed =
            std::get_if<control::OmsTradingEnabledChanged>(&status)){
            saw_enabled = saw_enabled || changed->trading_enabled;
        }
        saw_fault = saw_fault ||
            std::holds_alternative<control::OmsFaulted>(status);
    }
    EXPECT_FALSE(saw_enabled);
    EXPECT_TRUE(saw_fault);
}


} // namespace
