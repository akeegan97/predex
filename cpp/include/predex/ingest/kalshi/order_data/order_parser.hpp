#pragma once 

#include "predex/oms/oms_types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

#include <simdjson.h>

namespace predex::ingest::kalshi::order_data{

    enum class OrderIncomingMessageKind : std::uint8_t{
        kCONTROL_RESPONSE = 1,
        kORDER_DATA = 2,
        kIGNORE = 3,
        kMALFORMED = 4,
    };

    struct OrderControlResponse{
        std::uint64_t request_id{};
        std::optional<std::int64_t> session_id;
        bool success{false};
        std::string error_message;
    };

    enum class OrderParseCode : std::uint8_t{
        kOK,
        kIGNORE,
        kINVALID_JSON,
        kUNSUPPORTED_TYPE,
        kMISSING_FIELD,
        kUNKNOWN_ORDER_STATE,
        kINVALID_ORDER_ID,
    };

    struct ParsedOrderMessage{
        OrderIncomingMessageKind kind{OrderIncomingMessageKind::kIGNORE};
        OrderParseCode parse_code{OrderParseCode::kOK};
        OrderControlResponse control_response;
        oms::PrivateWsOrderEvent order_event;
    };


    class OrderParser{
        public:
            OrderParser() = default;

            OrderIncomingMessageKind classify_message(std::span<const std::byte> message) noexcept;
            OrderParseCode parse_control_response(std::span<const std::byte> message, OrderControlResponse& out_response) noexcept;
            OrderParseCode parse_order_event(std::span<const std::byte> message, ParsedOrderMessage& out_message) noexcept;
            OrderParseCode parse_message(std::span<const std::byte> message, ParsedOrderMessage& out_message) noexcept;
        private:
            simdjson::ondemand::parser parser_;


    };


}
