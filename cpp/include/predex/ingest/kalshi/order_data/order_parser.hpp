#pragma once 

#include "predex/oms/oms_types.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include <simdjson.h>

namespace predex::ingest::kalshi::order_data{
inline constexpr std::size_t kMAX_TICKER_LEN = 128;
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

        std::array<char, kMAX_TICKER_LEN> market_ticker_storage{};
        std::size_t market_ticker_length{0};

        [[nodiscard]] std::string_view market_ticker() const noexcept{
            return {market_ticker_storage.data(), market_ticker_length};
        }
        [[nodiscard]] bool assign_market_ticker(std::string_view ticker) noexcept{
            if(ticker.size() >= kMAX_TICKER_LEN){
                market_ticker_length = 0;
                return false;
            }
            std::copy(ticker.begin(), ticker.end(), market_ticker_storage.begin());
            market_ticker_length = ticker.size();
            if(market_ticker_length < kMAX_TICKER_LEN){
                market_ticker_storage[market_ticker_length] = '\0';
            }
            return true;
        }
    };


    class OrderParser{
        public:
            OrderParser() = default;

            OrderIncomingMessageKind classify_message(std::span<const std::byte> message) noexcept;
            OrderParseCode parse_control_response(std::span<const std::byte> message, OrderControlResponse& out_response) noexcept;
            OrderParseCode parse_order_event(std::span<const std::byte> message, ParsedOrderMessage& out_message) noexcept;
            OrderParseCode parse_message(std::span<const std::byte> message, ParsedOrderMessage& out_message) noexcept;
        private:
            OrderParseCode parse_fill_event(simdjson::ondemand::object& msg, ParsedOrderMessage& out_message) noexcept;
            OrderParseCode parse_user_order_event(simdjson::ondemand::object& msg, ParsedOrderMessage& out_message) noexcept;
            OrderParseCode parse_market_position_event(simdjson::ondemand::object& msg, ParsedOrderMessage& out_message) noexcept;

            simdjson::ondemand::parser parser_;


    };


}
