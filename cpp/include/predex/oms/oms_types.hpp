#pragma once
#include <cstdint>  
#include <variant>
#include <array>
#include <cstddef>
#include <string_view>
#include <algorithm>
#include <string>
#include <optional>
#include <functional>
#include <vector>

#include "predex/oms/order_intents.hpp"

namespace predex::oms{
    
    inline constexpr std::size_t kLENGTH_CLIENT_ORDER_ID = 64;
    inline constexpr std::size_t kLENGTH_EXCHANGE_ORDER_ID = 64;
    inline constexpr std::size_t kLENGTH_VENUE_TRADE_ID = 64;

    struct ClientOrderId{
        std::array<char, kLENGTH_CLIENT_ORDER_ID> storage{};
        std::size_t length{0};

        [[nodiscard]] std::string_view view() const noexcept{
            return {storage.data(), length};
        }

        [[nodiscard]] const char* c_str() const noexcept{
            return storage.data();
        }

        [[nodiscard]] bool operator==(const ClientOrderId& other) const noexcept{
            return view() == other.view();
        }

        [[nodiscard]] bool empty() const noexcept{
            return length == 0;
        }

        void clear() noexcept{
            storage[0] = '\0';
            length = 0;
        }

        [[nodiscard]] bool assign_from(std::string_view str) noexcept{
            if(str.size() >= kLENGTH_CLIENT_ORDER_ID){
                clear();
                return false;
            }
            std::copy(str.begin(), str.end(), storage.begin());
            length = str.size();
            if(length < kLENGTH_CLIENT_ORDER_ID){
                storage[length] = '\0';
            }
            return true;
        }

    };

    struct ExchangeOrderId{
        std::array<char, kLENGTH_EXCHANGE_ORDER_ID> storage{};
        std::size_t length{0};

        [[nodiscard]] std::string_view view() const noexcept{
            return {storage.data(), length};
        }

        [[nodiscard]] const char* c_str() const noexcept{
            return storage.data();
        }

        [[nodiscard]] bool operator==(const ExchangeOrderId& other) const noexcept{
            return view() == other.view();
        }
        
        [[nodiscard]] bool empty() const noexcept{
            return length == 0;
        }
        void clear() noexcept{
            storage[0] = '\0';
            length = 0;
        }

        [[nodiscard]] bool assign_from(std::string_view str) noexcept{
            if(str.size() >= kLENGTH_EXCHANGE_ORDER_ID){
                clear();
                return false;
            }
            std::copy(str.begin(), str.end(), storage.begin());
            length = str.size();
            if(length < kLENGTH_EXCHANGE_ORDER_ID){
                storage[length] = '\0';
            }
            return true;
        }
    };

    struct VenueTradeId{
        std::array<char, kLENGTH_VENUE_TRADE_ID> storage{};
        std::size_t length{0};

        [[nodiscard]] std::string_view view() const noexcept{
            return {storage.data(), length};
        }

        [[nodiscard]] bool operator==(const VenueTradeId& other) const noexcept{
            return view() == other.view();
        }

        [[nodiscard]] bool empty() const noexcept{
            return length == 0;
        }

        void clear() noexcept{
            storage[0] = '\0';
            length = 0;
        }

        [[nodiscard]] bool assign_from(std::string_view value) noexcept{
            if(value.size() >= storage.size()){
                clear();
                return false;
            }
            std::copy(value.begin(), value.end(), storage.begin());
            length = value.size();
            storage[length] = '\0';
            return true;
        }
    };

    struct OmsContext{
        intent::OmsRequestId oms_request_id{};
        intent::IntentContext context{};
    };

    using OmsGroupId = std::uint64_t;
    using PortfolioSequence = std::uint64_t;

    enum class OmsResponseType : std::uint8_t{
        kUNKNOWN = 0,
        kACCEPTED = 1,
        kREJECTED = 2,
    };

    enum class RejectReason : std::uint8_t{
        kNONE = 0,
        kTRADING_DISABLED,
        kRISK_REJECTED,
        kINVALID_INTENT,
        kUNKNOWN_TARGET_ORDER,
        kDUPLICATE_INTENT,
        kOMS_NOT_READY,
        kUNKNOWN_MARKET,
        kMARKET_NOT_TRADEABLE,
        kEXECUTION_BLOCKED,
    };

    struct OmsResponse{
        OmsContext context{};
        OmsResponseType response_type{OmsResponseType::kREJECTED};
        RejectReason reject_reason{RejectReason::kNONE};
        
        std::uint64_t recv_ts_ns{0};
        std::uint64_t response_ts_ns{0};
    };

    enum class OrderState : std::uint8_t{
        kUNKNOWN = 0,
        kPENDING_SUBMIT = 1,
        kWORKING = 2,
        kREJECTED = 3,
        kPARTIALLY_FILLED = 4,
        kFILLED = 5,
        kPENDING_CANCEL = 6,
        kCANCELED = 7,
        kPENDING_MODIFY = 8,
        kEXPIRED = 9,
        kUNCERTAIN = 10,
    };

    enum class VenueEventSource : std::uint8_t{
        kUNKNOWN = 0,
        kREST_RESPONSE = 1,
        kWEBSOCKET_FEED = 2,
        kRECONCILIATION = 3,
        kOMS_INTERNAL = 4,
    };

    struct OrderStateUpdate{
        ClientOrderId client_order_id{};
        ExchangeOrderId exchange_order_id{};

        OmsContext context{};
        OrderState order_state{OrderState::kUNKNOWN};
        VenueEventSource update_source{VenueEventSource::kUNKNOWN};

        std::int64_t working_qty_lots{0};
        std::int64_t working_price_ticks{0};

        intent::Outcome outcome{intent::Outcome::kUNKNOWN};

        std::int64_t ordered_qty_lots{0};
        std::int64_t cumulative_filled_qty_lots{0};
        std::int64_t leaves_qty_lots{0};

        std::int64_t last_fill_qty_lots{0};
        std::int64_t last_fill_price_ticks{0};

        std::uint64_t last_update_ts_ns{0};
    };

    enum class GroupAdmissionState : std::uint8_t {
        kUNKNOWN = 0,
        kACCEPTED,
        kREJECTED,
    };

    struct GroupAdmissionResponse {
        intent::IntentContext context{};

        OmsGroupId oms_group_id{};
        GroupAdmissionState admission_state{
            GroupAdmissionState::kUNKNOWN
        };
        RejectReason reject_reason{RejectReason::kNONE};

        std::uint8_t admitted_leg_count{};
        std::uint64_t reserved_capital_ticks{};

        std::uint64_t recv_ts_ns{};
        std::uint64_t response_ts_ns{};
    };

    enum class GroupExecutionState : std::uint8_t {
        kUNKNOWN = 0,

        // Passed OMS validation and risk admission.
        kADMITTED,

        // Batch has been handed to the REST egress.
        kPENDING_VENUE,

        // At least one leg is live, with no confirmed fills yet.
        kWORKING,

        // Some intended quantity has filled, but the package is incomplete.
        kPARTIALLY_FILLED,

        // Every intended leg completed successfully.
        kCOMPLETED,

        // OMS is canceling remaining original orders.
        kCANCELING_REMAINDER,

        // OMS is executing orders that remove residual exposure.
        kUNWINDING,

        // Repair completed and no residual group exposure remains.
        kFLATTENED,

        // Venue state must be reconciled before further action is safe.
        kUNCERTAIN,

        // Repair could not restore a safe state.
        kREPAIR_FAILED,

        // Group never passed admission.
        kREJECTED,
        // The package cannot complete as intended, but repair commands have not
        // yet been emitted.
        kREPAIR_REQUIRED,

        // The package ended without fills or residual exposure. Capital can be
        // returned immediately.
        kABORTED,
    };

    enum class GroupFailureReason : std::uint8_t {
        kNONE = 0,
        kLEG_REJECTED,
        kLEG_NOT_SENT,
        kLEG_EXPIRED_UNFILLED,
        kLEG_PARTIALLY_FILLED,
        kLEG_TIMEOUT,
        kLEG_TRANSPORT_ERROR,
        kCANCEL_FAILED,
        kUNWIND_FAILED,
        kVENUE_STATE_UNCERTAIN,
        kLEG_CANCELED,
        kBATCH_RESPONSE_MISMATCH,
    };

    struct GroupLegState {
        intent::OmsRequestId oms_request_id{};
        intent::MarketId market_id{};

        std::uint8_t leg_index{};
        OrderState order_state{OrderState::kUNKNOWN};

        std::int64_t ordered_qty_lots{};
        std::int64_t cumulative_filled_qty_lots{};
        std::int64_t leaves_qty_lots{};
    };

    struct GroupStateUpdate {
        intent::IntentContext context{};

        OmsGroupId oms_group_id{};
        GroupExecutionState execution_state{
            GroupExecutionState::kUNKNOWN
        };
        GroupFailureReason failure_reason{
            GroupFailureReason::kNONE
        };

        std::array<
            GroupLegState,
            intent::kMAX_ORDERS_PER_GROUP
        > legs{};

        std::uint8_t leg_count{};
        std::uint8_t repair_attempt_count{};

        bool residual_exposure_present{false};

        std::uint64_t last_update_ts_ns{};
    };

    struct StrategyPortfolioUpdate {
        std::uint16_t strategy_index{};
        PortfolioSequence sequence{};

        // Authoritative OMS values. Strategy estimates must never replace these.
        std::int64_t allocation_limit_ticks{};
        std::int64_t available_capital_ticks{};
        std::int64_t reserved_order_capital_ticks{};
        std::int64_t inventory_exposure_ticks{};
        std::int64_t realized_pnl_ticks{};
        std::int64_t fees_paid_ticks{};

        std::int64_t venue_available_balance_ticks{};
        bool venue_portfolio_reconciled{false};

        std::uint32_t open_order_count{};
        std::uint32_t active_group_count{};

        std::uint64_t update_ts_ns{};
    };

    struct StrategyMarketPositionUpdate {
        std::uint16_t strategy_index{};
        PortfolioSequence sequence{};

        intent::MarketId market_id{};
        intent::EventId event_id{};

        // Positive means net YES exposure; negative means net NO exposure.
        std::int64_t net_position_lots{};

        std::int64_t resting_buy_qty_lots{};
        std::int64_t resting_sell_qty_lots{};

        std::int64_t average_entry_price_ticks{};
        std::int64_t position_cost_ticks{};
        std::int64_t market_exposure_ticks{};
        std::int64_t realized_pnl_ticks{};
        std::int64_t fees_paid_ticks{};

        std::uint64_t update_ts_ns{};
    };


    using OmsToStrategyMessage = std::variant<
        OmsResponse,
        OrderStateUpdate,
        GroupAdmissionResponse,
        GroupStateUpdate,
        StrategyPortfolioUpdate,
        StrategyMarketPositionUpdate
    >;

    struct SubmitOrderCmd{
        intent::OmsRequestId oms_request_id{};
        ClientOrderId client_order_id{};
        intent::NewOrderIntent new_order_intent{};
        std::uint64_t submission_ts_ns{0};
        // Set only by OMS-generated exposure-reducing orders. Strategy
        // submissions must never be able to opt themselves into this path.
        bool reduce_only{false};
    };
    struct SubmitOrderBatchCmd {
        // Correlates the batch HTTP operation itself.
        intent::OmsRequestId oms_request_id{};

        OmsGroupId oms_group_id{};
        intent::IntentContext group_context{};

        std::array<
            SubmitOrderCmd,
            intent::kMAX_ORDERS_PER_GROUP
        > orders{};

        std::uint8_t order_count{};
        std::uint64_t submission_ts_ns{};
    };
    struct CancelOrderCmd{
        intent::OmsRequestId oms_request_id{};
        ClientOrderId client_order_id{};
        std::optional<ExchangeOrderId> exchange_order_id;
        intent::CancelOrderIntent cancel_order_intent{};
        std::uint64_t submission_ts_ns{0};
    };
    struct ModifyOrderCmd{
        intent::OmsRequestId oms_request_id{};
        ClientOrderId client_order_id{};
        std::optional<ExchangeOrderId> exchange_order_id;
        intent::ModifyOrderIntent modify_order_intent{};
        std::uint64_t submission_ts_ns{0};
    };
    struct CloseOrderRestEgress {
        intent::OmsRequestId oms_request_id{};
        std::uint64_t submission_ts_ns{0};
        std::uint64_t shutdown_epoch{};
    };

    struct RequestPortfolioReconciliation {
        std::uint64_t reconciliation_id{};
        intent::UniverseVersion universe_version{};
        std::uint64_t submission_ts_ns{};
    };


    using OmsToKalshiCommand = std::variant<
        SubmitOrderCmd,
        SubmitOrderBatchCmd,
        CancelOrderCmd,
        ModifyOrderCmd,
        RequestPortfolioReconciliation,
        CloseOrderRestEgress
    >;

    enum class RestCommandKind : std::uint8_t {
        kUNKNOWN = 0,
        kSUBMIT_ORDER = 1,
        kCANCEL_ORDER = 2,
        kMODIFY_ORDER = 3,
        kSUBMIT_ORDER_BATCH = 4,
        kPORTFOLIO_RECONCILIATION = 5,
    };

    enum class RestResultCode : std::uint8_t{
        kUNKNOWN=0,
        kACKED = 1,
        kREJECTED = 2,
        kTIMEOUT = 3,
        kTRANSPORT_ERROR = 4,
        kNOT_SENT = 5,
    };

    enum class VenueRejectReason : std::uint8_t{
        kNone = 0,
        kInvalidRequest = 1,
        kInvalidOrder = 2,
        kDuplicateOrder = 3,
        kOrderNotFound = 4,
        kMarketClosed = 5,
        kMarketHalted = 6,
        kRiskRejected = 7,
        kRateLimited = 8,
        kVenueDown = 9,
        kAuthFailed = 10,
        kOrderAlreadyTerminal = 11,
        kUnknown = 12,
    };

    struct RestOrderResponse{
        OmsContext context{};
        RestCommandKind command_kind{RestCommandKind::kUNKNOWN};
        RestResultCode result_code{RestResultCode::kUNKNOWN};

        ClientOrderId client_order_id{};
        ExchangeOrderId exchange_order_id{};

        std::uint64_t transport_submit_ts_ns{0};
        std::uint64_t transport_recv_ts_ns{0};
        std::uint16_t http_status_code{0};
        std::uint16_t retry_count{0};

        VenueRejectReason venue_reject_reason{VenueRejectReason::kNone};
        std::string raw_reason_message;
        bool execution_summary_present{false};

        std::int64_t acknowledged_fill_qty_lots{};
        std::int64_t acknowledged_remaining_qty_lots{};
        std::int64_t acknowledged_average_fill_price_ticks{};
        std::int64_t acknowledged_fee_paid_ticks{};

        std::uint64_t venue_ts_ms{};
    };

    struct RestOrderBatchResponse {
        intent::OmsRequestId batch_oms_request_id{};
        OmsGroupId oms_group_id{};
        intent::IntentContext group_context{};

        RestResultCode result_code{RestResultCode::kUNKNOWN};

        std::array<
            RestOrderResponse,
            intent::kMAX_ORDERS_PER_GROUP
        > order_responses{};

        std::uint8_t requested_order_count{};
        std::uint8_t response_order_count{};

        std::uint64_t transport_submit_ts_ns{};
        std::uint64_t transport_recv_ts_ns{};

        std::uint16_t http_status_code{};
        std::uint16_t retry_count{};

        VenueRejectReason venue_reject_reason{
            VenueRejectReason::kNone
        };

        std::string raw_reason_message;
    };

    enum class PrivateWsOrderEventKind : std::uint8_t{
        kUNKNOWN = 0,
        kUSER_ORDER = 1,
        kFILL = 2,
        kMARKET_POSITION = 3,
    };

    struct PrivateWsOrderEvent{
        PrivateWsOrderEventKind event_kind{PrivateWsOrderEventKind::kUNKNOWN};

        ClientOrderId client_order_id{};
        ExchangeOrderId exchange_order_id{};
        VenueTradeId trade_id{};

        intent::MarketId market_id{};
        intent::Outcome outcome{intent::Outcome::kUNKNOWN};
        intent::OrderAction action{intent::OrderAction::kUNKNOWN};

        std::uint64_t recv_ts_ns{0};
        std::uint64_t venue_ts_ns{0};
        std::uint64_t ws_sequence{0};

        OrderState order_state{OrderState::kUNKNOWN};

        std::int64_t ordered_qty_lots{0};
        std::int64_t cumulative_filled_qty_lots{0};
        std::int64_t leaves_qty_lots{0};

        std::int64_t last_fill_qty_lots{0};
        std::int64_t last_fill_price_ticks{0};

        // Populated for kMARKET_POSITION. Positive is net YES, negative net NO.
        std::int64_t net_position_lots{0};
        std::int64_t position_cost_ticks{0};
        std::int64_t realized_pnl_ticks{0};
        std::int64_t fees_paid_ticks{0};
        std::int64_t position_fee_cost_ticks{0};
        std::int64_t volume_lots{0};
    };

    struct ReconciledOrderSnapshot{};

    struct VenueMarketPositionSnapshot {
        std::string market_ticker;
        intent::MarketId market_id{};
        intent::EventId event_id{};

        std::int64_t net_position_lots{};
        std::int64_t position_cost_ticks{};
        std::int64_t market_exposure_ticks{};
        std::int64_t realized_pnl_ticks{};
        std::int64_t fees_paid_ticks{};
        std::int64_t position_fee_cost_ticks{};
        std::int64_t volume_lots{};

        bool position_cost_present{false};
        bool market_exposure_present{false};
        bool realized_pnl_present{false};
        bool fees_paid_present{false};
    };

    enum class PortfolioReconciliationResultCode : std::uint8_t {
        kUNKNOWN = 0,
        kCOMPLETE,
        kFAILED,
    };

    struct VenuePortfolioSnapshot {
        std::uint64_t reconciliation_id{};
        intent::UniverseVersion universe_version{};
        PortfolioReconciliationResultCode result_code{
            PortfolioReconciliationResultCode::kUNKNOWN
        };

        std::int64_t available_balance_ticks{};
        std::int64_t portfolio_value_ticks{};
        std::vector<VenueMarketPositionSnapshot> market_positions;

        std::uint64_t request_ts_ns{};
        std::uint64_t received_ts_ns{};
        std::string error_message;
    };

    struct OrderRestEgressDrained{
        std::uint64_t shutdown_epoch{};
        std::uint64_t completion_ts_ns{};
    };

    using KalshiToOmsEvent = std::variant<
        RestOrderResponse,
        RestOrderBatchResponse,
        PrivateWsOrderEvent,
        ReconciledOrderSnapshot,
        VenuePortfolioSnapshot,
        OrderRestEgressDrained
    >;

}

template<> struct std::hash<predex::oms::ClientOrderId>{
    std::size_t operator()(const predex::oms::ClientOrderId& client_id) const noexcept{
        return std::hash<std::string_view>{}(client_id.view());
    }
};
template<> struct std::hash<predex::oms::ExchangeOrderId>{
    std::size_t operator()(const predex::oms::ExchangeOrderId& exchange_id) const noexcept{
        return std::hash<std::string_view>{}(exchange_id.view());
    }
};
template<> struct std::hash<predex::oms::VenueTradeId>{
    std::size_t operator()(const predex::oms::VenueTradeId& trade_id) const noexcept{
        return std::hash<std::string_view>{}(trade_id.view());
    }
};
