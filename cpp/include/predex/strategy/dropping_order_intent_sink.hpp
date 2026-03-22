#pragma once

#include <atomic>
#include <string_view>

#include "trading/strategy/order_intent_sink.hpp"

namespace trading::strategy {

class DroppingOrderIntentSink final : public IOrderIntentSink {
  public:
    [[nodiscard]] bool submit_intent(internal::OrderIntent intent) override {
        (void)intent;
        dropped_count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] std::string_view last_error() const override { return {}; }

    [[nodiscard]] std::uint64_t dropped_count() const {
        return dropped_count_.load(std::memory_order_relaxed);
    }

  private:
    std::atomic<std::uint64_t> dropped_count_{0};
};

} // namespace trading::strategy
