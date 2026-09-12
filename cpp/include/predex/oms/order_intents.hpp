#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <variant>

namespace predex::oms::intent {

enum class TimeInForce : std::uint8_t {
    kGTC = 1,
    kIOC = 2,
    kFOK = 3,
};

enum class OrderType : std::uint8_t {
    kLIMIT = 1,
    kMARKETABLE_LIMIT = 2,
};

enum class LiquidityIntent : std::uint8_t {
    kUNKNOWN = 0,
    kMAKER = 1,
    kTAKER = 2,
};

enum class Outcome : std::uint8_t {
    kUNKNOWN = 0,
    kYES = 1,
    kNO = 2,
};

enum class OrderAction : std::uint8_t {
    kUNKNOWN = 0,
    kBUY = 1,
    kSELL = 2,
};

enum class GroupAdmissionPolicy : std::uint8_t {
    kUNKNOWN = 0,
    kALL_OR_NONE = 1,
};

using StrategyId = std::uint32_t;
using StrategyIntentId = std::uint32_t;
using MarketId = std::uint32_t;
using EventId = std::uint32_t;
using SignalId = std::uint32_t;
using GroupIntentId = std::uint64_t;
using OmsRequestId = std::uint64_t;
using UniverseVersion = std::uint64_t;
using EventRevision = std::uint64_t;
using TimestampNs = std::uint64_t;

struct IntentContext {
    std::uint16_t strategy_index{};

    MarketId market_id{};
    EventId event_id{};

    StrategyId strategy_id{};
    StrategyIntentId strategy_intent_id{};
    SignalId signal_id{};

    GroupIntentId group_intent_id{};
    std::uint8_t leg_index{};
    std::uint8_t leg_count{};

    UniverseVersion universe_version{};
    EventRevision event_revision{};
    std::uint32_t source_shard_index{};

    TimestampNs ingress_timestamp_ns{};
    TimestampNs book_apply_timestamp_ns{};
    TimestampNs observation_publish_timestamp_ns{};
    TimestampNs strategy_dequeue_timestamp_ns{};
    TimestampNs evaluation_complete_timestamp_ns{};
    TimestampNs intent_publish_timestamp_ns{};
};

struct NewOrderIntent {
    IntentContext context{};

    Outcome outcome{Outcome::kUNKNOWN};
    OrderAction action{OrderAction::kUNKNOWN};
    LiquidityIntent liquidity_intent{LiquidityIntent::kUNKNOWN};
    OrderType order_type{OrderType::kLIMIT};
    TimeInForce time_in_force{TimeInForce::kGTC};

    std::int64_t price_ticks{};
    std::int64_t quantity_lots{};
};

struct CancelOrderIntent {
    IntentContext context{};
    OmsRequestId target_oms_request_id{};
};

struct ModifyOrderIntent {
    IntentContext context{};
    OmsRequestId target_oms_request_id{};
};

inline constexpr std::size_t kMAX_ORDERS_PER_GROUP = 10;

struct GroupOrderIntent {
    IntentContext context{};

    std::array<NewOrderIntent, kMAX_ORDERS_PER_GROUP> new_orders{};
    std::uint8_t leg_count{};

    GroupAdmissionPolicy admission_policy{
        GroupAdmissionPolicy::kALL_OR_NONE
    };

    std::uint64_t expected_gross_edge_ticks{};
    std::uint64_t estimated_fee_ticks{};
    std::uint64_t expected_net_edge_ticks{};
};

using StrategyIntent = std::variant<
    NewOrderIntent,
    CancelOrderIntent,
    ModifyOrderIntent,
    GroupOrderIntent
>;

}
