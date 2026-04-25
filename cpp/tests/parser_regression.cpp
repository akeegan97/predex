#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <variant>

#include "predex/ingest/frame_pool.hpp"
#include "predex/internal/normalized_event.hpp"
#include "predex/parsers/kalshi/parser.hpp"

namespace {

using predex::core::ingest::kalshi::FrameHandle;
using predex::core::ingest::kalshi::KalshiFrame;
using predex::core::parsers::kalshi::Parser;
using predex::internal::DeltaData;
using predex::internal::EventType;
using predex::internal::Side;

bool load_payload(KalshiFrame& frame, const std::string& payload) {
    if (payload.size() > frame.payload.size()) {
        return false;
    }
    frame.recv_ts_ns_ = 123456789ULL;
    frame.len_ = static_cast<std::uint32_t>(payload.size());
    frame.flags_ = 0;
    std::fill(frame.payload.begin(), frame.payload.end(), std::byte{0});
    std::memcpy(frame.payload.data(), payload.data(), payload.size());
    return true;
}

int fail(std::string_view message) {
    std::cerr << "parser_regression_test: " << message << '\n';
    return 1;
}

}  // namespace

int main() {
    Parser parser{};
    FrameHandle handle{};
    handle.seq_ = 8073;
    handle.sid_ = 2;
    handle.market_id_ = 101U;
    handle.event_id_ = 202U;

    KalshiFrame frame{};
    const std::string payload =
        R"({"type":"orderbook_delta","sid":2,"seq":8073,"msg":{"market_ticker":"KXFED-26OCT-T2.75","price_dollars":"0.1300","delta_fp":"1.00","side":"no","ts":"2026-04-17T16:16:07.926378Z"}})";
    if (!load_payload(frame, payload)) {
        return fail("failed to load payload");
    }

    const auto result = parser.parse(handle, frame);
    if (!result.ok()) {
        return fail("parser returned failure");
    }

    const auto& event = *result.value();
    if (event.type != EventType::kDelta) {
        return fail("expected delta event type");
    }

    const auto* delta = std::get_if<DeltaData>(&event.data);
    if (delta == nullptr) {
        return fail("expected delta payload");
    }

    if (delta->side != Side::kAsk) {
        return fail("expected ask side for explicit no delta");
    }

    if (delta->price_ticks != 870) {
        std::cerr << "parser_regression_test: expected reciprocal ask price 870, got "
                  << delta->price_ticks << '\n';
        return 1;
    }

    if (delta->delta_qty_lots != 100) {
        std::cerr << "parser_regression_test: expected qty 100, got " << delta->delta_qty_lots
                  << '\n';
        return 1;
    }

    return 0;
}
