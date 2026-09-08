#include <gtest/gtest.h>

#include "predex/control/control_types.hpp"
#include "predex/oms/oms.hpp"
#include "predex/strategy/strategy.hpp"
#include "predex/utils/monotonic_clock.hpp"

namespace {

namespace oms = predex::oms;
namespace control = predex::core::control;
namespace strategy = predex::strategy;
namespace utils = predex::utils;

using ShardQueue = utils::SPSCQueue<strategy::ShardToStrategyMessage>;
using IntentQueue = utils::SPSCQueue<oms::intent::StrategyIntent>;
using ResponseQueue = utils::SPSCQueue<oms::OmsToStrategyMessage>;
using ControlCommandQueue = utils::SPSCQueue<control::ControlToOmsCommand>;
using ControlStatusQueue = utils::SPSCQueue<control::OmsToControlStatus>;
using VenueCommandQueue = utils::SPSCQueue<oms::OmsToKalshiCommand>;
using VenueEventQueue = utils::SPSCQueue<oms::KalshiToOmsEvent>;

[[nodiscard]] strategy::MonotonicArbStrategyConfig strategy_config() {
    return strategy::MonotonicArbStrategyConfig{
        .strategy_id = 77,
        .arb_config =
            strategy::MonotonicArbConfig{
                .order_quantity_lots = 100,
                .minimum_net_edge_ticks = 1,
                .require_top_gap_continuity = false,
                .require_near_top_multilevel_support = false,
                .bounded_easier_aggression_enabled = false,
                .bounded_harder_aggression_enabled = false,
                .maximum_easier_book_levels = 1,
                .maximum_harder_book_levels = 1,
                .taker_fee_rate_numerator = 0,
                .taker_fee_rate_denominator = 1,
            },
        .maximum_observation_age_ns = 1'000'000'000,
    };
}

[[nodiscard]] strategy::MonotonicPairObservation
observation(std::uint64_t revision, strategy::EventId event_id = 201,
            strategy::MarketId easier_market_id = 101, strategy::MarketId harder_market_id = 102) {
    const auto now = utils::monotonic_now_ns();
    return strategy::MonotonicPairObservation{
        .universe_version = 1,
        .shard_index = 0,
        .event_id = event_id,
        .event_revision = revision,
        .ingress_timestamp_ns = now - 300,
        .book_apply_timestamp_ns = now - 200,
        .publish_timestamp_ns = now - 100,
        .easier =
            strategy::StrategyMarketView{
                .market_id = easier_market_id,
                .event_market_index = 0,
                .strike_key = 10,
                .tradeable = true,
                .asks = {strategy::StrategyBookLevel{
                    .price_ticks = 4'000,
                    .quantity_lots = 100,
                }},
                .ask_count = 1,
            },
        .harder =
            strategy::StrategyMarketView{
                .market_id = harder_market_id,
                .event_market_index = 1,
                .strike_key = 20,
                .tradeable = true,
                .bids = {strategy::StrategyBookLevel{
                    .price_ticks = 6'000,
                    .quantity_lots = 100,
                }},
                .bid_count = 1,
            },
    };
}

struct StrategyHarness {
    ShardQueue shard_to_strategy{16};
    IntentQueue strategy_to_oms{16};
    ResponseQueue oms_to_strategy{16};
    strategy::MonotonicArbStrategy uut;

    StrategyHarness()
        : uut(0, 1, strategy_config(),
              strategy::StrategyQueues{
                  .shard_inputs = {strategy::StrategyShardInput{
                      .shard_index = 0,
                      .queue = &shard_to_strategy,
                  }},
                  .strategy_to_oms_queue = &strategy_to_oms,
                  .oms_to_strategy_queue = &oms_to_strategy,
              }) {}

    void publish_portfolio(std::int64_t available_capital_ticks = 100'000,
                           oms::PortfolioSequence sequence = 1) {
        ASSERT_TRUE(oms_to_strategy.try_push(oms::OmsToStrategyMessage{oms::StrategyPortfolioUpdate{
            .strategy_index = 0,
            .sequence = sequence,
            .allocation_limit_ticks = 100'000,
            .available_capital_ticks = available_capital_ticks,
            .venue_portfolio_reconciled = true,
        }}));
        EXPECT_EQ(uut.pump_once().code, strategy::StrategyPumpCode::kOMS_MESSAGE_HANDLED);
    }
};

TEST(StrategyTest, RequiresPortfolioThenPublishesFullyFormedGroupIntent) {
    StrategyHarness harness;

    ASSERT_TRUE(
        harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{observation(1)}));
    const auto before_portfolio = harness.uut.pump_once();
    EXPECT_EQ(before_portfolio.code, strategy::StrategyPumpCode::kCANDIDATE_SUPPRESSED);
    EXPECT_EQ(before_portfolio.intent_suppress_reason,
              strategy::StrategyIntentSuppressReason::kPORTFOLIO_UNKNOWN);
    EXPECT_FALSE(harness.strategy_to_oms.producer_size());

    harness.publish_portfolio();
    ASSERT_TRUE(
        harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{observation(2)}));
    const auto published = harness.uut.pump_once();
    ASSERT_EQ(published.code, strategy::StrategyPumpCode::kINTENT_PUBLISHED);
    ASSERT_TRUE(published.published_intent.has_value());

    oms::intent::StrategyIntent queued{};
    ASSERT_TRUE(harness.strategy_to_oms.try_pop(queued));
    const auto* group = std::get_if<oms::intent::GroupOrderIntent>(&queued);
    ASSERT_NE(group, nullptr);
    EXPECT_EQ(group->context.strategy_index, 0);
    EXPECT_EQ(group->context.strategy_id, 77U);
    EXPECT_EQ(group->context.event_id, 201U);
    EXPECT_EQ(group->context.event_revision, 2U);
    EXPECT_EQ(group->leg_count, 2U);
    EXPECT_EQ(group->admission_policy, oms::intent::GroupAdmissionPolicy::kALL_OR_NONE);
    EXPECT_EQ(group->new_orders[0].context.leg_index, 0U);
    EXPECT_EQ(group->new_orders[0].context.market_id, 101U);
    EXPECT_EQ(group->new_orders[0].outcome, oms::intent::Outcome::kYES);
    EXPECT_EQ(group->new_orders[0].action, oms::intent::OrderAction::kBUY);
    EXPECT_EQ(group->new_orders[1].context.leg_index, 1U);
    EXPECT_EQ(group->new_orders[1].context.market_id, 102U);
    EXPECT_EQ(group->new_orders[1].action, oms::intent::OrderAction::kSELL);
    EXPECT_EQ(group->new_orders[0].time_in_force, oms::intent::TimeInForce::kFOK);
    EXPECT_EQ(group->new_orders[1].order_type, oms::intent::OrderType::kMARKETABLE_LIMIT);
    EXPECT_LT(group->new_orders[0].context.strategy_intent_id,
              group->new_orders[1].context.strategy_intent_id);
    EXPECT_LE(group->context.evaluation_complete_timestamp_ns,
              group->context.intent_publish_timestamp_ns);
}

TEST(StrategyTest, ActiveGroupAndPendingCapitalPreventDuplicateExposure) {
    StrategyHarness harness;
    harness.publish_portfolio(30'000);

    ASSERT_TRUE(
        harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{observation(1)}));
    const auto first = harness.uut.pump_once();
    ASSERT_EQ(first.code, strategy::StrategyPumpCode::kINTENT_PUBLISHED);
    const auto group_id = first.published_intent->context.group_intent_id;

    ASSERT_TRUE(
        harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{observation(2)}));
    const auto same_event = harness.uut.pump_once();
    EXPECT_EQ(same_event.intent_suppress_reason,
              strategy::StrategyIntentSuppressReason::kEVENT_GROUP_ACTIVE);

    ASSERT_TRUE(harness.shard_to_strategy.try_push(
        strategy::ShardToStrategyMessage{observation(1, 202, 103, 104)}));
    const auto other_event = harness.uut.pump_once();
    EXPECT_EQ(other_event.intent_suppress_reason,
              strategy::StrategyIntentSuppressReason::kINSUFFICIENT_CAPITAL);

    const auto& original_group = *first.published_intent;
    ASSERT_TRUE(
        harness.oms_to_strategy.try_push(oms::OmsToStrategyMessage{oms::GroupAdmissionResponse{
            .context = original_group.context,
            .admission_state = oms::GroupAdmissionState::kREJECTED,
            .reject_reason = oms::RejectReason::kRISK_REJECTED,
        }}));
    EXPECT_EQ(harness.uut.pump_once().code, strategy::StrategyPumpCode::kOMS_MESSAGE_HANDLED);

    ASSERT_TRUE(
        harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{observation(3)}));
    const auto retried = harness.uut.pump_once();
    EXPECT_EQ(retried.code, strategy::StrategyPumpCode::kINTENT_PUBLISHED);
    EXPECT_NE(retried.published_intent->context.group_intent_id, group_id);
}

TEST(StrategyTest, UnsafeGroupLatchesStrategyFailClosed) {
    StrategyHarness harness;
    harness.publish_portfolio();
    ASSERT_TRUE(
        harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{observation(1)}));
    const auto published = harness.uut.pump_once();
    ASSERT_EQ(published.code, strategy::StrategyPumpCode::kINTENT_PUBLISHED);

    const auto context = published.published_intent->context;
    ASSERT_TRUE(
        harness.oms_to_strategy.try_push(oms::OmsToStrategyMessage{oms::GroupAdmissionResponse{
            .context = context,
            .admission_state = oms::GroupAdmissionState::kACCEPTED,
        }}));
    (void)harness.uut.pump_once();
    ASSERT_TRUE(harness.oms_to_strategy.try_push(oms::OmsToStrategyMessage{oms::GroupStateUpdate{
        .context = context,
        .execution_state = oms::GroupExecutionState::kREPAIR_REQUIRED,
    }}));
    (void)harness.uut.pump_once();

    ASSERT_TRUE(harness.shard_to_strategy.try_push(
        strategy::ShardToStrategyMessage{observation(1, 202, 103, 104)}));
    const auto blocked = harness.uut.pump_once();
    EXPECT_EQ(blocked.intent_suppress_reason,
              strategy::StrategyIntentSuppressReason::kEXECUTION_BLOCKED);
    EXPECT_EQ(harness.uut.stats().unsafe_groups_seen, 1U);
}

TEST(StrategyTest, EvaluatesDistinctAdjacentPairsAtSameEventRevision) {
    StrategyHarness harness;
    harness.publish_portfolio();

    auto first_pair = observation(1);
    first_pair.harder.bids[0].price_ticks = 3'000;
    ASSERT_TRUE(harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{first_pair}));
    EXPECT_EQ(harness.uut.pump_once().code, strategy::StrategyPumpCode::kOBSERVATION_REJECTED);

    auto second_pair = observation(1, 201, 102, 103);
    second_pair.easier.event_market_index = 1;
    second_pair.easier.strike_key = 20;
    second_pair.harder.event_market_index = 2;
    second_pair.harder.strike_key = 30;
    ASSERT_TRUE(harness.shard_to_strategy.try_push(strategy::ShardToStrategyMessage{second_pair}));
    EXPECT_EQ(harness.uut.pump_once().code, strategy::StrategyPumpCode::kINTENT_PUBLISHED);
}

TEST(StrategyTest, PublishedGroupPassesOmsAdmissionAndBecomesBatchCommand) {
    ShardQueue shard_to_strategy{16};
    IntentQueue strategy_to_oms{16};
    ResponseQueue oms_to_strategy{16};
    ControlCommandQueue control_to_oms{16};
    ControlStatusQueue oms_to_control{16};
    VenueCommandQueue oms_to_venue{16};
    VenueEventQueue venue_to_oms{16};

    strategy::MonotonicArbStrategy strategy_instance{
        0, 1, strategy_config(),
        strategy::StrategyQueues{
            .shard_inputs = {strategy::StrategyShardInput{
                .shard_index = 0,
                .queue = &shard_to_strategy,
            }},
            .strategy_to_oms_queue = &strategy_to_oms,
            .oms_to_strategy_queue = &oms_to_strategy,
        }};

    oms::Oms oms_instance{oms::OmsQueues{
                              .strategy_intent_queues = {&strategy_to_oms},
                              .strategy_response_queues = {&oms_to_strategy},
                              .control_command_queue = control_to_oms,
                              .oms_status_queue = oms_to_control,
                              .kalshi_command_queue = oms_to_venue,
                              .venue_event_queues = {&venue_to_oms},
                          },
                          oms::OmsRiskConfig{
                              .strategy_allocation_limit_ticks = 100'000,
                              .maximum_group_reservation_ticks = 50'000,
                              .maximum_group_legs = 2,
                              .maximum_group_intent_age_ns = 1'000'000'000,
                          }};

    auto universe = std::make_shared<control::OrderRouteUniverse>();
    universe->version = 1;
    universe->market_routes = {
        control::OrderMarketRoute{
            .market_id = 101,
            .event_id = 201,
            .kalshi_ticker = "TEST-EASIER",
            .tradeable = true,
            .price_level_structure = control::PriceLevelStructure::kLINEAR_CENT,
        },
        control::OrderMarketRoute{
            .market_id = 102,
            .event_id = 201,
            .kalshi_ticker = "TEST-HARDER",
            .tradeable = true,
            .price_level_structure = control::PriceLevelStructure::kLINEAR_CENT,
        },
    };
    ASSERT_TRUE(control_to_oms.try_push(
        control::ControlToOmsCommand{control::ApplyOrderRouteUniverse{.snapshot = universe}}));
    EXPECT_EQ(oms_instance.pump_once(), oms::OmsPumpResult::kOK);
    ASSERT_TRUE(control_to_oms.try_push(control::ControlToOmsCommand{control::AllowTrading{}}));
    EXPECT_EQ(oms_instance.pump_once(), oms::OmsPumpResult::kOK);

    ASSERT_TRUE(oms_to_strategy.try_push(oms::OmsToStrategyMessage{oms::StrategyPortfolioUpdate{
        .strategy_index = 0,
        .sequence = 1,
        .allocation_limit_ticks = 100'000,
        .available_capital_ticks = 100'000,
        .venue_portfolio_reconciled = true,
    }}));
    EXPECT_EQ(strategy_instance.pump_once().code, strategy::StrategyPumpCode::kOMS_MESSAGE_HANDLED);

    ASSERT_TRUE(shard_to_strategy.try_push(strategy::ShardToStrategyMessage{observation(1)}));
    EXPECT_EQ(strategy_instance.pump_once().code, strategy::StrategyPumpCode::kINTENT_PUBLISHED);
    EXPECT_EQ(oms_instance.pump_once(), oms::OmsPumpResult::kOK);

    oms::OmsToKalshiCommand venue_command{};
    ASSERT_TRUE(oms_to_venue.try_pop(venue_command));
    const auto* batch = std::get_if<oms::SubmitOrderBatchCmd>(&venue_command);
    ASSERT_NE(batch, nullptr);
    EXPECT_EQ(batch->order_count, 2U);
    EXPECT_EQ(batch->orders[0].new_order_intent.context.market_id, 101U);
    EXPECT_EQ(batch->orders[1].new_order_intent.context.market_id, 102U);
    EXPECT_EQ(batch->orders[0].new_order_intent.action, oms::intent::OrderAction::kBUY);
    EXPECT_EQ(batch->orders[1].new_order_intent.action, oms::intent::OrderAction::kSELL);
}

} // namespace
