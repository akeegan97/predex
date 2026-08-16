#pragma once

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>

namespace predex::utils{

    enum class ThreadPollingProfile : std::uint8_t{
        kLOW_LATENCY,
        kHARVEST,
    };

    struct IdleBackoffConfig{
        ThreadPollingProfile profile{ThreadPollingProfile::kLOW_LATENCY};
        std::uint32_t spin_iterations{64};
        std::uint32_t yield_iterations{64};
        std::chrono::microseconds min_sleep{50};
        std::chrono::microseconds max_sleep{1000};
    };

    enum class IdleBackoffAction : std::uint8_t{
        kSPIN,
        kYIELD,
        kSLEEP,
    };

    struct IdleBackoffDecision{
        IdleBackoffAction action{IdleBackoffAction::kSPIN};
        std::chrono::microseconds sleep_duration{0};
    };

    class IdleBackoff{
        public:
            explicit IdleBackoff(IdleBackoffConfig config)
                : config_(config),
                  next_sleep_(config.min_sleep){
                if(config_.profile == ThreadPollingProfile::kHARVEST &&
                   (config_.min_sleep.count() <= 0 ||
                    config_.max_sleep < config_.min_sleep)){
                    throw std::invalid_argument(
                        "Harvest idle backoff requires 0 < min_sleep <= max_sleep");
                }
            }

            void reset() noexcept{
                idle_iterations_ = 0;
                next_sleep_ = config_.min_sleep;
            }

            [[nodiscard]] IdleBackoffDecision next_idle_decision() noexcept{
                if(config_.profile == ThreadPollingProfile::kLOW_LATENCY){
                    return IdleBackoffDecision{.action = IdleBackoffAction::kYIELD};
                }

                if(idle_iterations_ < config_.spin_iterations){
                    ++idle_iterations_;
                    return IdleBackoffDecision{.action = IdleBackoffAction::kSPIN};
                }

                const std::uint64_t after_spin =
                    idle_iterations_ - config_.spin_iterations;
                if(after_spin < config_.yield_iterations){
                    ++idle_iterations_;
                    return IdleBackoffDecision{.action = IdleBackoffAction::kYIELD};
                }

                const auto sleep_duration = next_sleep_;
                if(next_sleep_ < config_.max_sleep){
                    const auto remaining = config_.max_sleep - next_sleep_;
                    next_sleep_ += remaining < next_sleep_
                        ? remaining
                        : next_sleep_;
                }
                return IdleBackoffDecision{
                    .action = IdleBackoffAction::kSLEEP,
                    .sleep_duration = sleep_duration,
                };
            }

            void idle(){
                const auto decision = next_idle_decision();
                switch(decision.action){
                    case IdleBackoffAction::kSPIN:
                        return;
                    case IdleBackoffAction::kYIELD:
                        std::this_thread::yield();
                        return;
                    case IdleBackoffAction::kSLEEP:
                        std::this_thread::sleep_for(decision.sleep_duration);
                        return;
                }
            }

        private:
            IdleBackoffConfig config_;
            std::uint64_t idle_iterations_{0};
            std::chrono::microseconds next_sleep_;
    };

}
