#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

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

    class Http2Session {
        public:
            Http2Session(AuthSigner signer, Http2SessionConfig config = {});
            ~Http2Session();

            Http2Session(const Http2Session&) = delete;
            Http2Session& operator=(const Http2Session&) = delete;

            Http2Session(Http2Session&&) noexcept;
            Http2Session& operator=(Http2Session&&) noexcept;

            [[nodiscard]] bool warm_up();
            [[nodiscard]] bool start_request(HttpRequest request);
            [[nodiscard]] HttpPollResult poll();
            [[nodiscard]] bool has_inflight_requests() const noexcept;
            [[nodiscard]] std::string_view last_error() const noexcept;

            void close() noexcept;

        private:
            struct Impl;
            std::unique_ptr<Impl> impl_;
    };

} // namespace predex::exchange::kalshi
