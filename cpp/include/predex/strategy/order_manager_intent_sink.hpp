#pragma once

#include <string>
#include <string_view>
#include <utility>

#include "trading/oms/order_manager.hpp"
#include "trading/strategy/order_intent_sink.hpp"

namespace trading::strategy {

class OrderManagerIntentSink final : public IOrderIntentSink {
  public:
    explicit OrderManagerIntentSink(oms::OrderManager& order_manager)
        : order_manager_(order_manager) {}

    [[nodiscard]] bool submit_intent(internal::OrderIntent intent) override {
        const auto request_id = order_manager_.submit(std::move(intent));
        if (request_id.has_value()) {
            last_error_.clear();
            return true;
        }
        last_error_ = order_manager_.last_error();
        return false;
    }

    [[nodiscard]] std::string_view last_error() const override { return last_error_; }

  private:
    oms::OrderManager& order_manager_;
    std::string last_error_;
};

} // namespace trading::strategy
