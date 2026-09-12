#include <gtest/gtest.h>

#include <chrono>

#include "predex/utils/idle_backoff.hpp"

namespace predex::utils{
namespace{

    TEST(IdleBackoffTests, HarvestProgressesThroughSpinYieldAndBoundedSleep){
        IdleBackoff backoff{IdleBackoffConfig{
            .profile = ThreadPollingProfile::kHARVEST,
            .spin_iterations = 2,
            .yield_iterations = 1,
            .min_sleep = std::chrono::microseconds{10},
            .max_sleep = std::chrono::microseconds{40},
        }};

        EXPECT_EQ(backoff.next_idle_decision().action, IdleBackoffAction::kSPIN);
        EXPECT_EQ(backoff.next_idle_decision().action, IdleBackoffAction::kSPIN);
        EXPECT_EQ(backoff.next_idle_decision().action, IdleBackoffAction::kYIELD);

        const auto first_sleep = backoff.next_idle_decision();
        EXPECT_EQ(first_sleep.action, IdleBackoffAction::kSLEEP);
        EXPECT_EQ(first_sleep.sleep_duration, std::chrono::microseconds{10});

        EXPECT_EQ(
            backoff.next_idle_decision().sleep_duration,
            std::chrono::microseconds{20});
        EXPECT_EQ(
            backoff.next_idle_decision().sleep_duration,
            std::chrono::microseconds{40});
        EXPECT_EQ(
            backoff.next_idle_decision().sleep_duration,
            std::chrono::microseconds{40});
    }

    TEST(IdleBackoffTests, WorkResetReturnsHarvestPolicyToSpinPhase){
        IdleBackoff backoff{IdleBackoffConfig{
            .profile = ThreadPollingProfile::kHARVEST,
            .spin_iterations = 1,
            .yield_iterations = 0,
            .min_sleep = std::chrono::microseconds{10},
            .max_sleep = std::chrono::microseconds{20},
        }};

        EXPECT_EQ(backoff.next_idle_decision().action, IdleBackoffAction::kSPIN);
        EXPECT_EQ(backoff.next_idle_decision().action, IdleBackoffAction::kSLEEP);

        backoff.reset();

        EXPECT_EQ(backoff.next_idle_decision().action, IdleBackoffAction::kSPIN);
        const auto sleep = backoff.next_idle_decision();
        EXPECT_EQ(sleep.action, IdleBackoffAction::kSLEEP);
        EXPECT_EQ(sleep.sleep_duration, std::chrono::microseconds{10});
    }

    TEST(IdleBackoffTests, LowLatencyPreservesContinuousYieldPolling){
        IdleBackoff backoff{IdleBackoffConfig{
            .profile = ThreadPollingProfile::kLOW_LATENCY,
            .spin_iterations = 0,
            .yield_iterations = 0,
            .min_sleep = std::chrono::microseconds{1},
            .max_sleep = std::chrono::microseconds{1},
        }};

        for(std::size_t iteration = 0; iteration < 100; ++iteration){
            EXPECT_EQ(
                backoff.next_idle_decision().action,
                IdleBackoffAction::kYIELD);
        }
    }

    TEST(IdleBackoffTests, RejectsInvalidHarvestSleepBounds){
        const auto construct_zero_sleep = []{
            return IdleBackoff{IdleBackoffConfig{
                .profile = ThreadPollingProfile::kHARVEST,
                .min_sleep = std::chrono::microseconds{0},
                .max_sleep = std::chrono::microseconds{1},
            }};
        };
        const auto construct_reversed_bounds = []{
            return IdleBackoff{IdleBackoffConfig{
                .profile = ThreadPollingProfile::kHARVEST,
                .min_sleep = std::chrono::microseconds{20},
                .max_sleep = std::chrono::microseconds{10},
            }};
        };

        EXPECT_THROW((void)construct_zero_sleep(), std::invalid_argument);
        EXPECT_THROW((void)construct_reversed_bounds(), std::invalid_argument);
    }

}
}
