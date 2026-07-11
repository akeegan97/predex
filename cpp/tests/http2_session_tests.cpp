#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>

#include <curl/curl.h>
#include <gtest/gtest.h>

#include "predex/exchange/kalshi/adapters/auth_signer.hpp"
#include "predex/exchange/kalshi/http2_session.hpp"
#include "predex/exchange/kalshi/http_types.hpp"

namespace {

namespace kalshi = predex::exchange::kalshi;

struct CurlGlobalGuard {
    CurlGlobalGuard() {
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
            throw std::runtime_error("curl_global_init failed");
        }
    }

    ~CurlGlobalGuard() {
        curl_global_cleanup();
    }

    CurlGlobalGuard(const CurlGlobalGuard&) = delete;
    CurlGlobalGuard& operator=(const CurlGlobalGuard&) = delete;
};

std::string required_env_value(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error(std::string{"Missing required environment variable: "} + name);
    }
    return std::string{value};
}

} // namespace

TEST(Http2SessionLiveTest, DISABLED_UnauthenticatedMarketsGetNegotiatesHttp2) {
    CurlGlobalGuard curl_global;

    kalshi::Credentials credentials{
        .key_id = required_env_value("KALSHI_KEY_ID"),
        .private_key_pem = required_env_value("KALSHI_PRIVATE_KEY_PEM"),
    };

    kalshi::Http2SessionConfig config;
    config.endpoint = "https://external-api.kalshi.com";
    config.request_timeout = std::chrono::milliseconds{5000};
    config.connect_timeout = std::chrono::milliseconds{3000};
    config.max_concurrent_streams = 1;

    kalshi::Http2Session session{
        kalshi::AuthSigner{std::move(credentials)},
        std::move(config),
    };

    ASSERT_TRUE(session.warm_up()) << session.last_error();

    kalshi::HttpRequest request{
        .request_id = 1,
        .method = kalshi::HttpMethod::kGET,
        .target = "/trade-api/v2/markets?limit=1",
        .authenticate = false,
    };

    ASSERT_EQ(session.start_request(std::move(request)), kalshi::HttpStartResult::kACCEPTED)
        << session.last_error();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (std::chrono::steady_clock::now() < deadline) {
        kalshi::HttpPollResult result = session.poll();
        if (result.status == kalshi::HttpRequestStatus::kCOMPLETED) {
            ASSERT_TRUE(result.response.has_value());
            EXPECT_TRUE(result.response->ok) << result.response->error_message;
            EXPECT_EQ(result.response->status_code, 200U);
            EXPECT_EQ(result.response->trace.negotiated_protocol, kalshi::HttpProtocol::kHTTP_2);
            EXPECT_FALSE(result.response->body.empty());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }

    FAIL() << "timed out waiting for HTTP response";
}
