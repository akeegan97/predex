#pragma once 
#include <cstdint>  
#include <variant>
#include <array>
#include <cstddef>
namespace predex::oms::intent{
    //Communication types for Strategy -> OMS edge
    enum class TimeInForce : std::uint8_t {
        kGTC = 1,
        kIOC = 2,
        kFOK = 3,
    };

    enum class OrderType : std::uint8_t {
        kLIMIT = 1,
        kMARKETABLE_LIMIT = 2,
    };

    enum class LiquidityIntent : std::uint8_t{
        kUNKNOWN = 0,
        kMAKER = 1,
        kTAKER = 2,
    };

    enum class Outcome : std::uint8_t{
        kUNKNOWN = 0,
        kYES = 1,
        kNO = 2,
    };

    enum class OrderAction : std::uint8_t{
        kUNKNOWN = 0,
        kBUY = 1,
        kSELL = 2,
    };

    using StrategyId = std::uint32_t;
    using StrategyIntentId = std::uint32_t;
    using MarketId = std::uint32_t;
    using EventId = std::uint32_t;
    using SignalId = std::uint32_t;
    using GroupIntentId = std::uint64_t;
    using OmsRequestId = std::uint64_t;
    
    struct IntentContext{
        std::uint16_t strategy_index{}; // where to route this response back in the SPSC queue vector
        MarketId market_id{};
        EventId event_id{};
        StrategyId strategy_id{};
        StrategyIntentId strategy_intent_id{};
        SignalId signal_id{};
        GroupIntentId group_intent_id{};
        std::uint8_t leg_index{};
        std::uint8_t leg_count{};
    };

    struct NewOrderIntent{
        IntentContext context{};
        Outcome outcome{Outcome::kUNKNOWN};
        OrderAction action{OrderAction::kUNKNOWN};
        LiquidityIntent liquidity_intent{LiquidityIntent::kUNKNOWN};
        OrderType order_type{OrderType::kLIMIT};
        TimeInForce time_in_force{TimeInForce::kGTC};
        std::int64_t price_ticks{0};
        std::int64_t quantity_lots{0};
    };

    struct CancelOrderIntent{
        IntentContext context{};
        OmsRequestId target_oms_request_id{};
    };

    struct ModifyOrderIntent{
        IntentContext context{};
        OmsRequestId target_oms_request_id{};
        //addtional fields as needed price/qty etc
    };

    inline constexpr std::size_t kMAX_ORDERS_PER_GROUP = 10;

    struct GroupOrderIntent{
        IntentContext context{};
        std::array<NewOrderIntent, kMAX_ORDERS_PER_GROUP> new_orders;
        std::uint8_t leg_count{};
    };

    using StrategyIntent = std::variant<NewOrderIntent, CancelOrderIntent, ModifyOrderIntent, GroupOrderIntent>;

    

}
