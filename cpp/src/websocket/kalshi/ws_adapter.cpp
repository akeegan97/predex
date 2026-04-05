#include "predex/websocket/kalshi/ws_adapter.hpp"

#include <nlohmann/json.hpp>
#include <string_view>
#include <utility>

namespace predex::websocket::kalshi {
namespace {

std::string extract_path_from_endpoint(std::string_view endpoint) {
    const auto scheme_pos = endpoint.find("://");
    const auto host_start = scheme_pos == std::string_view::npos ? 0 : scheme_pos + 3;
    const auto path_pos = endpoint.find('/', host_start);
    if (path_pos == std::string_view::npos) {
        return "/";
    }
    return std::string(endpoint.substr(path_pos));
}

} // namespace

WsAdapter::WsAdapter(AuthSigner signer, std::string endpoint)
    : signer_(std::move(signer)), endpoint_(std::move(endpoint)) {}

std::string WsAdapter::name() const { return "kalshi"; }

predex::websocket::adapter::ConnectRequest WsAdapter::build_connect_request() const {
    const auto auth_headers = signer_.make_ws_headers(extract_path_from_endpoint(endpoint_));
    return predex::websocket::adapter::ConnectRequest{
        .endpoint = endpoint_,
        .headers =
            {
                {"KALSHI-ACCESS-KEY", auth_headers.key_id},
                {"KALSHI-ACCESS-TIMESTAMP", auth_headers.timestamp_ms},
                {"KALSHI-ACCESS-SIGNATURE", auth_headers.signature_base64},
            },
    };
}

std::string WsAdapter::build_subscribe_message(
    std::string_view channel, const std::vector<std::string>& market_tickers) const {
    nlohmann::json params{{"channels", {channel}}};
    if (!market_tickers.empty()) {
        params["market_tickers"] = market_tickers;
    }

    const nlohmann::json payload{
        {"id", 1},
        {"cmd", "subscribe"},
        {"params", std::move(params)},
    };
    return payload.dump();
}

} // namespace predex::websocket::kalshi
