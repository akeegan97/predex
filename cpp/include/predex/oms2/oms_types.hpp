#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>


#include "predex/internal/market_types.hpp"
namespace predex::core::oms2::kalshi{

    using OmsRequestId = std::uint64_t;
    using LocalIntentId = std::uint64_t;
    using GroupIntentId = std::uint64_t;
    /*
    somewhat unavoidable string usage since 
    Kalshi sends them around and are needed for cancel/modify
    + matching ws feed for bookkeeping
    */
    struct ClientOrderId{
        std::string value;
    };
    struct ExchangeOrderId{
        std::string value;
    };
    /*
    Enums/codes for OMS translation
    */
   enum class TimeInForce : std::uint8_t{
        kGtc = 1,
        kIoc = 2,
        kFoc = 3,
   };
   enum class OrderTypeIntent : std::uint8_t{
        kLimit = 1, // specified to limit potential slippage kLimit + kIoc stops walking book 
        kMarketableLimit = 2, // specified to aggressively walk the book
   };

   enum class LiquidityIntent : std::uint8_t{
        kDefault = 1, // taker intent
        kPostOnly = 2, // maker intent
   };


   enum class IntentRejectReason : std::uint16_t{
        kHardHalt = 0,
        kSoftHalt = 1,
        kGlobalRiskExceeded = 2,
        kInvalidParams = 3,
        kMarketClosed = 4,
        kStaleSignal = 5,
        kDuplicateClientOrderId = 6,
        kUnsupported = 7,
        kNone = 8,
   };

   enum class Outcome : std::uint8_t{
        kYes = 1,
        kNo = 2,
   };

   struct IntentOrigin{
        std::uint16_t shard_id{0};
        internal::AffinityKey affinity_key{0};
        std::uint16_t leg_index{0};
        std::uint16_t leg_count{0};
        std::uint64_t signal_id{0};
        LocalIntentId local_intent_id{0};

        /*
        latency fields
        */
        internal::TimestampNs signal_ts_ns{0};
        internal::TimestampNs tick_recv_ns{0};
        internal::TimestampNs submission_enqueued_ns{0};
        /*
        end latency fields
        */
        internal::EventId event_id{0};
        internal::MarketId market_id{0};
   };
   
   //Sent to REST client from OMS 

   struct OrderIntent{
        std::uint64_t group_order_id{0};
        internal::ExchangeId exchange{internal::ExchangeId::kUnknown};
        internal::Side side{internal::Side::kUnknown};
        Outcome outcome{};
        internal::QtyLots qty_lots{0};
        std::optional<internal::PriceTicks> limit_price_ticks;
        TimeInForce time_in_force{};
        LiquidityIntent liquidity_intent{};
        OrderTypeIntent order_type_intent{};
        IntentOrigin intent_origin{};

        /*
        latency fields
        */
       internal::TimestampNs intent_ts_ns{0};
   };

   enum class GroupExecutionPolicy : std::uint8_t{
        kUnwind = 1,
        kAbortRemaining = 2,
        kBestEffort = 3,
   };
inline constexpr std::uint8_t kMaxGroupOrderLegs = 4;
   struct GroupOrderIntent{
    /*
    although Kalshi doesn't support atomic batch orders, 
    I want to track orders generated in parallel by strategy together:
    ie for monotonic_arb/cdf_violation strategies
    */
        IntentOrigin intent_origin{};
        GroupIntentId group_intent_id{0};
        GroupExecutionPolicy execution_policy{};
        std::array<OrderIntent, kMaxGroupOrderLegs> legs{0};
        std::uint8_t leg_count{0};
        internal::TimestampNs intent_ts_ns{0};
   };

    struct CancelIntent {
        IntentOrigin origin{};
        std::optional<OmsRequestId> target_oms_request_id;
        ClientOrderId target_client_order_id;
        std::optional<ExchangeOrderId> target_exchange_order_id;
        internal::TimestampNs intent_ts_ns{0};
    };
/*
    ModifyIntent should either come from shard/strategy to OMS in a market making/resting order strategy 
    where it makes sense to reprice/reduce exposure without cancel + send a new intent:

    OMS should not be constructing ModifyIntents from shards OrderIntents directly

*/
    struct ModifyIntent{
        IntentOrigin origin{};
        std::optional<OmsRequestId> target_oms_request_id;
        ClientOrderId target_client_order_id;
        std::optional<ExchangeOrderId> target_exchange_order_id;
        OrderIntent replacement_intent{};
        internal::TimestampNs intent_ts_ns{0};
    };

    using OmsSubmission = 
        std::variant<OrderIntent, GroupOrderIntent, CancelIntent, ModifyIntent>;

    struct AcceptedIntent{
        OrderIntent intent{};
        OmsRequestId oms_request_id{0};
        ClientOrderId client_order_id;
    };

    struct RejectedIntent{
        OrderIntent intent{};
        IntentRejectReason reason{IntentRejectReason::kNone};
    };

    struct ModifiedIntent{
        OrderIntent original_intent{};
        OrderIntent modified_intent{};
        OmsRequestId oms_request_id{0};
        ClientOrderId client_order_id;
        IntentRejectReason reason{IntentRejectReason::kNone};
    };

    using IntentDecisionType = 
        std::variant<std::monostate, AcceptedIntent, RejectedIntent, ModifiedIntent>;
    
/*
Order Lifecycle Events
*/
/*
OMS mints ClientOrderId & OmsRequestId immediately following 
popping from shard<->OMS SPSC queue and before returning event lifecycle back to shard
*/
struct ShardOrderCorrelation {
    OmsRequestId oms_request_id{0};
    ClientOrderId client_order_id;
    LocalIntentId local_intent_id{0};
    GroupIntentId group_intent_id{0};
    std::uint16_t leg_index{0};
};
    struct IntentAccepted{
        ShardOrderCorrelation corr{};
    };
    struct IntentRejected{
        ShardOrderCorrelation corr{};
        IntentRejectReason rejection_reason{IntentRejectReason::kNone};
    };
    struct OrderWorking{
        ShardOrderCorrelation corr{};
        internal::QtyLots working_qty_lots{0};
        internal::PriceTicks working_price_ticks{0}; 
    };
    struct OrderPartiallyFilled{
        ShardOrderCorrelation corr{};
        internal::QtyLots filled_qty{0};
        internal::QtyLots unfilled_qty{0};
        internal::PriceTicks filled_price_ticks{0};
    };
    struct OrderFilled{
         ShardOrderCorrelation corr{};
        internal::QtyLots filled_qty{0};
        internal::PriceTicks filled_price_ticks{0};
    };
    struct OrderCanceled{
        ShardOrderCorrelation corr{};
    };
    struct OrderUncertain{
        ShardOrderCorrelation corr{};
    };
    enum class VenueRejectionReason : std::uint8_t{
        kNone = 0,
        kRateLimit = 1,
        kVenueDown = 2,
        kElse = 3,
    };
    struct OrderRejected{
        ShardOrderCorrelation corr{};
        VenueRejectionReason venue_rejected_reason{VenueRejectionReason::kNone};
    };

    using ShardOrderEvent  = std::variant<
        std::monostate, 
        IntentAccepted, 
        IntentRejected,
        OrderWorking,
        OrderPartiallyFilled,
        OrderFilled,
        OrderCanceled,
        OrderUncertain,
        OrderRejected
        >;


    enum class OrderLifeCycleEventKind : std::uint8_t {
        kAck,
        kReject,
        kPartialFill,
        kFill,
        kCancelAck,
        kCancelReject,
        kModifyAck,
        kModifyReject,
        kCanceled,
    };

    struct OrderAck{
        
    };

}