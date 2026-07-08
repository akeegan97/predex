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

#include "predex/oms/order_intents.hpp"

namespace predex::oms{
    
    inline constexpr std::size_t kLENGTH_CLIENT_ORDER_ID = 64;
    inline constexpr std::size_t kLENGTH_EXCHANGE_ORDER_ID = 64;

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

    struct OmsContext{
        intent::OmsRequestId oms_request_id{};
        intent::IntentContext context{};
    };

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

    using OmsToStrategyMessage = std::variant<OmsResponse, OrderStateUpdate>;

    struct SubmitOrderCmd{
        intent::OmsRequestId oms_request_id{};
        ClientOrderId client_order_id{};
        intent::NewOrderIntent new_order_intent{};
        std::uint64_t submission_ts_ns{0};
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


    using OmsToKalshiCommand = std::variant<SubmitOrderCmd, CancelOrderCmd, ModifyOrderCmd, CloseOrderRestEgress>;

    enum class RestCommandKind : std::uint8_t{
        kUNKNOWN = 0,
        kSUBMIT_ORDER = 1,
        kCANCEL_ORDER = 2,
        kMODIFY_ORDER = 3,
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
    };

    struct PrivateWsOrderEvent{

        ClientOrderId client_order_id{};
        ExchangeOrderId exchange_order_id{};

        intent::MarketId market_id{};
        intent::Outcome outcome{intent::Outcome::kUNKNOWN};

        std::uint64_t recv_ts_ns{0};
        std::uint64_t venue_ts_ns{0};
        std::uint64_t ws_sequence{0};

        OrderState order_state{OrderState::kUNKNOWN};

        std::int64_t ordered_qty_lots{0};
        std::int64_t cumulative_filled_qty_lots{0};
        std::int64_t leaves_qty_lots{0};

        std::int64_t last_fill_qty_lots{0};
        std::int64_t last_fill_price_ticks{0};
    };

    struct ReconciledOrderSnapshot{};

    struct OrderRestEgressDrained{
        std::uint64_t shutdown_epoch{};
        std::uint64_t completion_ts_ns{};
    };

    using KalshiToOmsEvent = std::variant<RestOrderResponse, PrivateWsOrderEvent, ReconciledOrderSnapshot, OrderRestEgressDrained>;

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