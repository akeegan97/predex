#pragma once

#include <memory>
#include <string>
#include <unordered_set>

#include "trading/shards/event_handler.hpp"

namespace trading::strategy {

class MarketFilterEventHandler final : public shards::IShardEventHandler {
  public:
    MarketFilterEventHandler(std::unordered_set<std::string> allowed_markets,
                             std::unique_ptr<shards::IShardEventHandler> delegate)
        : allowed_markets_(std::move(allowed_markets)), delegate_(std::move(delegate)) {}

    [[nodiscard]] bool on_event(const internal::NormalizedEvent& event) override {
        if (delegate_ == nullptr) {
            return false;
        }
        if (allowed_markets_.empty()) {
            return delegate_->on_event(event);
        }
        if (allowed_markets_.contains(event.market_ticker)) {
            return delegate_->on_event(event);
        }
        return true;
    }

  private:
    std::unordered_set<std::string> allowed_markets_;
    std::unique_ptr<shards::IShardEventHandler> delegate_;
};

} // namespace trading::strategy
