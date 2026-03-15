#include "trading/adapters/exchanges/polymarket/ws_adapter.hpp"

#include <nlohmann/json.hpp>
#include <utility>

namespace trading::adapters::exchanges::polymarket {

WsAdapter::WsAdapter(std::string endpoint) : endpoint_(std::move(endpoint)) {}

std::string WsAdapter::name() const { return "polymarket"; }

exchanges::ConnectRequest WsAdapter::build_connect_request() const {
    return exchanges::ConnectRequest{
        .endpoint = endpoint_,
        .headers = {},
    };
}

std::string WsAdapter::build_subscribe_message(
    std::string_view channel, const std::vector<std::string>& market_tickers) const {
    (void)market_tickers;
    const nlohmann::json payload{
        {"type", "subscribe"},
        {"channel", channel},
    };
    return payload.dump();
}

} // namespace trading::adapters::exchanges::polymarket
