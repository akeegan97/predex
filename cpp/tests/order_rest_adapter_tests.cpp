#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "predex/control/control_types.hpp"
#include "predex/exchange/kalshi/adapters/order_rest_adapter.hpp"

namespace {

namespace control = predex::core::control;
namespace kalshi = predex::exchange::kalshi;
namespace oms = predex::oms;
namespace intent = predex::oms::intent;

[[nodiscard]] std::shared_ptr<const control::OrderRouteUniverse>
make_routes() {
    auto routes = std::make_shared<control::OrderRouteUniverse>();
    routes->version = 1;
    routes->market_routes.push_back(control::OrderMarketRoute{
        .market_id = 101,
        .event_id = 202,
        .kalshi_ticker = "TEST-MARKET",
        .tradeable = true,
    });
    return routes;
}

TEST(OrderRestAdapterTest, ParsesBalanceCentsIntoMoneyTicks) {
    kalshi::KalshiOrderRestAdapter adapter{make_routes()};
    const oms::RequestPortfolioReconciliation command{
        .reconciliation_id = 9,
        .universe_version = 1,
        .submission_ts_ns = 10,
    };
    const auto prepared =
        adapter.prepare_portfolio_balance_request(command, 77);
    ASSERT_TRUE(prepared.ok);
    EXPECT_EQ(
        prepared.request.target,
        "/trade-api/v2/portfolio/balance");

    const kalshi::HttpResponse response{
        .request_id = 77,
        .ok = true,
        .status_code = 200,
        .body = R"json({"balance":12345,"portfolio_value":23456})json",
    };
    const auto completed =
        adapter.complete_portfolio_balance_request(prepared, response);
    ASSERT_TRUE(completed.ok) << completed.error_message;
    EXPECT_EQ(completed.available_balance_ticks, 1'234'500);
    EXPECT_EQ(completed.portfolio_value_ticks, 2'345'600);
}

TEST(OrderRestAdapterTest, ParsesPositionPageAndMapsKnownTicker) {
    kalshi::KalshiOrderRestAdapter adapter{make_routes()};
    const oms::RequestPortfolioReconciliation command{
        .reconciliation_id = 11,
        .universe_version = 1,
        .submission_ts_ns = 12,
    };
    const auto prepared =
        adapter.prepare_portfolio_positions_request(command, 88);
    ASSERT_TRUE(prepared.ok);
    EXPECT_EQ(
        prepared.request.target,
        "/trade-api/v2/portfolio/positions?limit=1000&count_filter=position");

    const kalshi::HttpResponse response{
        .request_id = 88,
        .ok = true,
        .status_code = 200,
        .body = R"json({
            "market_positions":[
                {
                    "ticker":"TEST-MARKET",
                    "position_fp":"-1.50",
                    "market_exposure_dollars":"0.7500",
                    "realized_pnl_dollars":"-0.1250",
                    "fees_paid_dollars":"0.0100"
                },
                {
                    "ticker":"OUTSIDE-UNIVERSE",
                    "position_fp":"2.00",
                    "market_exposure_dollars":"1.0000"
                }
            ],
            "cursor":"next/page"
        })json",
    };
    const auto completed =
        adapter.complete_portfolio_positions_request(prepared, response);
    ASSERT_TRUE(completed.ok) << completed.error_message;
    ASSERT_EQ(completed.positions.size(), 2U);
    EXPECT_EQ(completed.positions[0].market_id, 101U);
    EXPECT_EQ(completed.positions[0].event_id, 202U);
    EXPECT_EQ(completed.positions[0].net_position_lots, -150);
    EXPECT_EQ(completed.positions[0].market_exposure_ticks, 7'500);
    EXPECT_EQ(completed.positions[0].realized_pnl_ticks, -1'250);
    EXPECT_EQ(completed.positions[0].fees_paid_ticks, 100);
    EXPECT_EQ(completed.positions[1].market_id, 0U);
    EXPECT_EQ(completed.next_cursor, "next/page");

    const auto next_page =
        adapter.prepare_portfolio_positions_request(
            command,
            89,
            completed.next_cursor);
    EXPECT_NE(next_page.request.target.find("cursor=next%2Fpage"),
              std::string::npos);
}

TEST(OrderRestAdapterTest, ConservativelyRoundsSixPlacePortfolioMoney) {
    kalshi::KalshiOrderRestAdapter adapter{make_routes()};
    const oms::RequestPortfolioReconciliation command{
        .reconciliation_id = 12,
        .universe_version = 1,
        .submission_ts_ns = 13,
    };
    const auto prepared =
        adapter.prepare_portfolio_positions_request(command, 90);
    ASSERT_TRUE(prepared.ok);

    const kalshi::HttpResponse response{
        .request_id = 90,
        .ok = true,
        .status_code = 200,
        .body = R"json({
            "market_positions":[
                {
                    "ticker":"TEST-MARKET",
                    "position_fp":"1.00",
                    "market_exposure_dollars":"1.000001",
                    "realized_pnl_dollars":"0.123456",
                    "fees_paid_dollars":"0.000001"
                },
                {
                    "ticker":"TEST-MARKET",
                    "position_fp":"-1.00",
                    "market_exposure_dollars":"-1.000001",
                    "realized_pnl_dollars":"-0.123456",
                    "fees_paid_dollars":"0.010000"
                }
            ],
            "cursor":""
        })json",
    };

    const auto completed =
        adapter.complete_portfolio_positions_request(prepared, response);
    ASSERT_TRUE(completed.ok) << completed.error_message;
    ASSERT_EQ(completed.positions.size(), 2U);
    EXPECT_EQ(completed.positions[0].market_exposure_ticks, 10'001);
    EXPECT_EQ(completed.positions[0].realized_pnl_ticks, 1'234);
    EXPECT_EQ(completed.positions[0].fees_paid_ticks, 1);
    EXPECT_EQ(completed.positions[1].market_exposure_ticks, -10'001);
    EXPECT_EQ(completed.positions[1].realized_pnl_ticks, -1'235);
    EXPECT_EQ(completed.positions[1].fees_paid_ticks, 100);
}

TEST(OrderRestAdapterTest, RejectsPortfolioMoneyBeyondSixPlaces) {
    kalshi::KalshiOrderRestAdapter adapter{make_routes()};
    const oms::RequestPortfolioReconciliation command{
        .reconciliation_id = 13,
        .universe_version = 1,
        .submission_ts_ns = 14,
    };
    const auto prepared =
        adapter.prepare_portfolio_positions_request(command, 91);
    ASSERT_TRUE(prepared.ok);

    const kalshi::HttpResponse response{
        .request_id = 91,
        .ok = true,
        .status_code = 200,
        .body = R"json({
            "market_positions":[{
                "ticker":"TEST-MARKET",
                "position_fp":"1.00",
                "market_exposure_dollars":"0.1234567"
            }],
            "cursor":""
        })json",
    };

    const auto completed =
        adapter.complete_portfolio_positions_request(prepared, response);
    EXPECT_FALSE(completed.ok);
    EXPECT_EQ(
        completed.error_message,
        "portfolio position entry has invalid market_exposure_dollars");
}

TEST(OrderRestAdapterTest, SerializesOmsReduceOnlyRepairFlag) {
    kalshi::KalshiOrderRestAdapter adapter{make_routes()};
    oms::ClientOrderId client_order_id{};
    ASSERT_TRUE(client_order_id.assign_from("px-repair-1"));
    const oms::SubmitOrderCmd command{
        .oms_request_id = 1,
        .client_order_id = client_order_id,
        .new_order_intent = intent::NewOrderIntent{
            .context = intent::IntentContext{
                .strategy_index = 0,
                .market_id = 101,
                .event_id = 202,
            },
            .outcome = intent::Outcome::kYES,
            .action = intent::OrderAction::kSELL,
            .liquidity_intent = intent::LiquidityIntent::kTAKER,
            .order_type = intent::OrderType::kMARKETABLE_LIMIT,
            .time_in_force = intent::TimeInForce::kFOK,
            .price_ticks = 1,
            .quantity_lots = 100,
        },
        .submission_ts_ns = 2,
        .reduce_only = true,
    };

    const auto prepared = adapter.prepare_submit_order(command);
    ASSERT_TRUE(prepared.ok) << prepared.error_message;
    EXPECT_NE(
        prepared.request.body.find("\"reduce_only\":true"),
        std::string::npos);
}

} // namespace
