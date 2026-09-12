#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace predex::exchange::kalshi {

    using HttpRequestId = std::uint64_t;

    enum class HttpMethod : std::uint8_t {
        kGET = 1,
        kPOST = 2,
        kDELETE = 3,
        kPUT = 4,
    };

    enum class HttpProtocol : std::uint8_t {
        kUNKNOWN = 0,
        kHTTP_1_1 = 1,
        kHTTP_2 = 2,
    };

    enum class HttpRequestStatus : std::uint8_t {
        kIDLE = 0,
        kIN_FLIGHT = 1,
        kCOMPLETED = 2,
    };

    struct HttpHeader {
        std::string name;
        std::string value;
    };

    struct HttpTrace {
        std::uint64_t enqueue_ts_ns{0};
        std::uint64_t resolve_start_ts_ns{0};
        std::uint64_t resolve_end_ts_ns{0};
        std::uint64_t connect_start_ts_ns{0};
        std::uint64_t connect_end_ts_ns{0};
        std::uint64_t tls_start_ts_ns{0};
        std::uint64_t tls_end_ts_ns{0};
        std::uint64_t write_start_ts_ns{0};
        std::uint64_t request_sent_ts_ns{0};
        std::uint64_t response_recv_ts_ns{0};
        std::uint16_t retry_count{0};
        bool reused_connection{false};
        HttpProtocol negotiated_protocol{HttpProtocol::kUNKNOWN};
    };

    struct HttpRequest {
        HttpRequestId request_id{};
        HttpMethod method{HttpMethod::kGET};
        std::string target;
        std::string body;
        std::string content_type{"application/json"};
        bool authenticate{true};
        std::vector<HttpHeader> headers;
        HttpTrace trace;
    };

    struct HttpResponse {
        HttpRequestId request_id{};
        bool ok{false};
        std::uint16_t status_code{0};
        std::string body;
        std::string error_message;
        HttpTrace trace;
    };

    struct HttpPollResult {
        HttpRequestStatus status{HttpRequestStatus::kIDLE};
        std::optional<HttpResponse> response;
    };

} // namespace predex::exchange::kalshi
