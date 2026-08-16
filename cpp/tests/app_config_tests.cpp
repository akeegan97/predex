#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "predex/config/app_config.hpp"

namespace predex::config{
namespace{

    class TemporaryConfigFile{
        public:
            TemporaryConfigFile()
                : path_(std::filesystem::temp_directory_path() /
                        "predex-thread-polling-config-test.json"){}

            ~TemporaryConfigFile(){
                std::error_code error;
                std::filesystem::remove(path_, error);
            }

            [[nodiscard]] const std::filesystem::path& path() const noexcept{
                return path_;
            }

        private:
            std::filesystem::path path_;
    };

    TEST(AppConfigTests, LegacyConfigDefaultsToLowLatencyPolling){
        const auto config = default_app_config();

        EXPECT_EQ(
            config.runtime.thread_polling.profile,
            utils::ThreadPollingProfile::kLOW_LATENCY);
    }

    TEST(AppConfigTests, LoadsHarvestThreadPollingConfiguration){
        TemporaryConfigFile file;
        {
            std::ofstream output{file.path()};
            ASSERT_TRUE(output.is_open());
            output << R"json({
                "runtime": {
                    "thread_polling": {
                        "profile": "harvest",
                        "spin_iterations": 11,
                        "yield_iterations": 7,
                        "min_sleep_us": 25,
                        "max_sleep_us": 400
                    }
                },
                "universe": {
                    "events": [{
                        "event_id": "1",
                        "affinity_key": "2",
                        "topology": "single_market",
                        "markets": [{
                            "market_id": "3",
                            "kalshi_ticker": "TEST-MARKET",
                            "price_level_structure": "linear_cent"
                        }]
                    }]
                }
            })json";
        }

        const auto config = load_app_config(file.path().string());

        EXPECT_EQ(
            config.runtime.thread_polling.profile,
            utils::ThreadPollingProfile::kHARVEST);
        EXPECT_EQ(config.runtime.thread_polling.spin_iterations, 11U);
        EXPECT_EQ(config.runtime.thread_polling.yield_iterations, 7U);
        EXPECT_EQ(
            config.runtime.thread_polling.min_sleep,
            std::chrono::microseconds{25});
        EXPECT_EQ(
            config.runtime.thread_polling.max_sleep,
            std::chrono::microseconds{400});
    }

}
}
