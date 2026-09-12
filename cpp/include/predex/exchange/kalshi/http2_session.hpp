#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <curl/curl.h>
#include <unordered_map>
#include <queue>


#include "predex/exchange/kalshi/adapters/auth_signer.hpp"
#include "predex/exchange/kalshi/http_types.hpp"

namespace predex::exchange::kalshi {

    inline constexpr std::string_view kKALSHI_REST_ENDPOINT = "https://api.elections.kalshi.com";

    struct Http2SessionConfig {
        std::string endpoint{std::string{kKALSHI_REST_ENDPOINT}};
        std::chrono::milliseconds connect_timeout{3000}; //NOLINT
        std::chrono::milliseconds request_timeout{5000};//NOLINT
        std::uint16_t max_retries{0};
        std::uint16_t max_concurrent_streams{10};//NOLINT
    };

    enum class HttpStartResult : std::uint8_t{
        kACCEPTED = 1,
        kAT_CAPACITY = 2,
        kCLOSED = 3,
        kERROR = 4,
    };

    struct ActiveRequest{
        HttpRequest request;
        HttpResponse response;

        CURL* curl_handle{nullptr};
        curl_slist* curl_headers{nullptr};

        std::string url;
        std::string method;
        std::string body;
        std::string response_body;
        std::string error_buffer;

        std::uint64_t deadline_ns{0};
        ActiveRequest() = default;
        ~ActiveRequest(){
            if(curl_handle != nullptr){
                curl_easy_cleanup(curl_handle);
                curl_handle = nullptr;
            }
            if(curl_headers != nullptr){
                curl_slist_free_all(curl_headers);
                curl_headers = nullptr;
            }
        }
        ActiveRequest(const ActiveRequest&) = delete;
        ActiveRequest& operator=(const ActiveRequest&) = delete;
        ActiveRequest(ActiveRequest&& other) noexcept :
            request(std::move(other.request)),
            response(std::move(other.response)),
            curl_handle(other.curl_handle),
            curl_headers(other.curl_headers),
            url(std::move(other.url)),
            method(std::move(other.method)),
            body(std::move(other.body)),
            response_body(std::move(other.response_body)),
            error_buffer(std::move(other.error_buffer)),
            deadline_ns(other.deadline_ns)
        {
            other.curl_handle = nullptr;
            other.curl_headers = nullptr;
        }
        ActiveRequest& operator=(ActiveRequest&& other) noexcept{
            if(this != &other){
                if(curl_handle != nullptr){
                    curl_easy_cleanup(curl_handle);
                    curl_handle = nullptr;
                }
                if(curl_headers != nullptr){
                    curl_slist_free_all(curl_headers);
                    curl_headers = nullptr;
                }
                request = std::move(other.request);
                response = std::move(other.response);
                curl_handle = other.curl_handle;
                curl_headers = other.curl_headers;
                url = std::move(other.url);
                method = std::move(other.method);
                body = std::move(other.body);
                response_body = std::move(other.response_body);
                error_buffer = std::move(other.error_buffer);
                deadline_ns = other.deadline_ns;
                other.curl_handle = nullptr;
                other.curl_headers = nullptr;
            }
            return *this;
        }
    };

    class Http2Session {
        public:
            Http2Session(AuthSigner signer, Http2SessionConfig config = {});
            ~Http2Session();

            Http2Session(const Http2Session&) = delete;
            Http2Session& operator=(const Http2Session&) = delete;

            Http2Session(Http2Session&&) noexcept;
            Http2Session& operator=(Http2Session&&) noexcept;

            [[nodiscard]] bool warm_up();
            [[nodiscard]] HttpStartResult start_request(HttpRequest request);
            [[nodiscard]] HttpPollResult poll();
            [[nodiscard]] bool has_inflight_requests() const noexcept;
            [[nodiscard]] std::string_view last_error() const noexcept;

            [[nodiscard]] std::size_t available_capacity() const noexcept;

            void close() noexcept;

        private:
            CURLM* curl_multi_handle_{nullptr};
            AuthSigner signer_;
            Http2SessionConfig config_;
            std::string last_error_;

            std::unordered_map<HttpRequestId, ActiveRequest> inflight_requests_;

            std::queue<HttpResponse> completed_responses_;

            bool is_warmed_up_{false};
            bool is_closed_{true};

    };

} // namespace predex::exchange::kalshi
