#include "trading/oms/paper_order_transport.hpp"

#include <algorithm>
#include <chrono>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace trading::oms {
namespace {

using nlohmann::json;

std::optional<std::string> try_get_string(const json& object, std::string_view key) {
    const auto field_it = object.find(key);
    if (field_it == object.end() || !field_it->is_string()) {
        return std::nullopt;
    }
    return field_it->get<std::string>();
}

std::optional<std::int64_t> try_get_int64(const json& object, std::string_view key) {
    const auto field_it = object.find(key);
    if (field_it == object.end() || !field_it->is_number_integer()) {
        return std::nullopt;
    }
    return field_it->get<std::int64_t>();
}

std::uint32_t fnv1a_32(std::string_view text) {
    constexpr std::uint32_t kOffset = 2166136261U;
    constexpr std::uint32_t kPrime = 16777619U;
    std::uint32_t hash = kOffset;
    for (const char ch : text) {
        hash ^= static_cast<std::uint8_t>(ch);
        hash *= kPrime;
    }
    return hash;
}

std::vector<std::int64_t> split_fill_qty(std::int64_t total_qty, std::size_t parts) {
    if (total_qty <= 0) {
        return {};
    }
    const std::size_t resolved_parts =
        std::max<std::size_t>(1U, std::min<std::size_t>(parts, static_cast<std::size_t>(total_qty)));
    std::vector<std::int64_t> chunks(resolved_parts, total_qty / static_cast<std::int64_t>(resolved_parts));
    const std::int64_t remainder = total_qty % static_cast<std::int64_t>(resolved_parts);
    for (std::int64_t index = 0; index < remainder; ++index) {
        chunks[static_cast<std::size_t>(index)] += 1;
    }
    return chunks;
}

} // namespace

PaperOrderTransport::PaperOrderTransport(PaperOrderTransportConfig config)
    : config_(config) {
    config_.fill_parts = std::max<std::size_t>(1U, config_.fill_parts);
    config_.place_reject_bps = std::min<std::size_t>(10'000U, config_.place_reject_bps);
}

bool PaperOrderTransport::connect(const OrderTransportConfig& config) {
    static_cast<void>(config);
    std::scoped_lock lock{mutex_};
    connected_ = true;
    next_recv_ts_ns_ = 1;
    next_exchange_order_id_ = 1;
    inbound_updates_.clear();
    last_error_code_.store(ErrorCode::kNone, std::memory_order_relaxed);
    return true;
}

bool PaperOrderTransport::send_text(std::string_view payload) {
    std::scoped_lock lock{mutex_};
    if (!connected_) {
        set_error(ErrorCode::kNotConnected);
        return false;
    }

    json request;
    try {
        request = json::parse(payload);
    } catch (...) {
        set_error(ErrorCode::kInvalidPayload);
        return false;
    }

    if (!request.is_object()) {
        set_error(ErrorCode::kInvalidPayload);
        return false;
    }

    const auto action = try_get_string(request, "action");
    const auto client_order_id = try_get_string(request, "client_order_id");
    if (!action.has_value() || !client_order_id.has_value() || client_order_id->empty()) {
        set_error(ErrorCode::kInvalidRequest);
        return false;
    }

    const auto now_ns = monotonic_now_ns();

    if (*action == "place_order") {
        const auto market_ticker = try_get_string(request, "market_ticker");
        const auto qty = try_get_int64(request, "qty");
        const auto limit_price = try_get_int64(request, "limit_price");
        const auto side = try_get_string(request, "side");
        if (!market_ticker.has_value() || !qty.has_value() || !limit_price.has_value() ||
            !side.has_value() || *qty <= 0) {
            set_error(ErrorCode::kInvalidRequest);
            return false;
        }

        if (should_reject_place(*client_order_id)) {
            const std::uint64_t reject_due_ns =
                now_ns + static_cast<std::uint64_t>(config_.reject_delay.count()) * 1'000'000ULL;
            enqueue_update(reject_due_ns, json{
                                              {"type", "order_reject"},
                                              {"recv_ts_ns", next_recv_ts_ns_++},
                                              {"msg",
                                               {
                                                   {"client_order_id", *client_order_id},
                                                   {"market_ticker", *market_ticker},
                                                   {"reason_code", "paper_place_reject"},
                                                   {"reason_message",
                                                    "paper transport deterministic place reject"},
                                               }},
                                          }
                                              .dump());
            last_error_code_.store(ErrorCode::kNone, std::memory_order_relaxed);
            return true;
        }

        const std::string exchange_order_id =
            "paper-ex-" + std::to_string(next_exchange_order_id_++);
        const std::uint64_t ack_due_ns =
            now_ns + static_cast<std::uint64_t>(config_.ack_delay.count()) * 1'000'000ULL;
        enqueue_update(ack_due_ns, json{
                                   {"type", "order_ack"},
                                   {"recv_ts_ns", next_recv_ts_ns_++},
                                   {"msg",
                                    {
                                        {"client_order_id", *client_order_id},
                                        {"exchange_order_id", exchange_order_id},
                                        {"market_ticker", *market_ticker},
                                        {"accepted_qty", *qty},
                                    }},
                               }
                                   .dump());

        if (config_.auto_fill_on_place) {
            std::uint64_t fill_due_ns =
                ack_due_ns + static_cast<std::uint64_t>(config_.fill_delay.count()) * 1'000'000ULL;
            const auto fill_chunks = split_fill_qty(*qty, config_.fill_parts);
            for (const auto fill_qty : fill_chunks) {
                enqueue_update(fill_due_ns, json{
                                            {"type", "fill"},
                                            {"recv_ts_ns", next_recv_ts_ns_++},
                                            {"msg",
                                             {
                                                 {"client_order_id", *client_order_id},
                                                 {"exchange_order_id", exchange_order_id},
                                                 {"market_ticker", *market_ticker},
                                                 {"fill_qty", fill_qty},
                                                 {"fill_price", *limit_price},
                                                 {"side", *side},
                                             }},
                                        }
                                            .dump());
                fill_due_ns +=
                    static_cast<std::uint64_t>(config_.fill_interval.count()) * 1'000'000ULL;
            }
        }

        last_error_code_.store(ErrorCode::kNone, std::memory_order_relaxed);
        return true;
    }

    if (*action == "cancel_order" || *action == "replace_order") {
        const auto market_ticker = try_get_string(request, "market_ticker");
        const std::uint64_t reject_due_ns =
            now_ns + static_cast<std::uint64_t>(config_.reject_delay.count()) * 1'000'000ULL;
        enqueue_update(reject_due_ns, json{
                                          {"type", "order_reject"},
                                          {"recv_ts_ns", next_recv_ts_ns_++},
                                          {"msg",
                                           {
                                               {"client_order_id", *client_order_id},
                                               {"market_ticker",
                                                market_ticker.value_or(std::string{"paper"})},
                                               {"reason_code", "paper_action_unsupported"},
                                               {"reason_message",
                                                "paper transport currently supports place_order only"},
                                           }},
                                      }
                                          .dump());
        last_error_code_.store(ErrorCode::kNone, std::memory_order_relaxed);
        return true;
    }

    set_error(ErrorCode::kUnknownAction);
    return false;
}

std::optional<std::string> PaperOrderTransport::recv_text() {
    std::scoped_lock lock{mutex_};
    if (!connected_ || inbound_updates_.empty()) {
        return std::nullopt;
    }
    const auto now_ns = monotonic_now_ns();
    if (inbound_updates_.front().due_ts_ns > now_ns) {
        return std::nullopt;
    }
    std::string payload = std::move(inbound_updates_.front().payload);
    inbound_updates_.pop_front();
    return payload;
}

void PaperOrderTransport::close() {
    std::scoped_lock lock{mutex_};
    connected_ = false;
    inbound_updates_.clear();
}

std::string_view PaperOrderTransport::last_error() const {
    switch (last_error_code_.load(std::memory_order_relaxed)) {
    case ErrorCode::kNone:
        return "";
    case ErrorCode::kNotConnected:
        return "paper transport not connected";
    case ErrorCode::kInvalidPayload:
        return "paper transport invalid request payload";
    case ErrorCode::kInvalidRequest:
        return "paper transport invalid request";
    case ErrorCode::kUnknownAction:
        return "paper transport unknown action";
    }
    return "paper transport unknown error";
}

void PaperOrderTransport::set_error(ErrorCode error_code) {
    last_error_code_.store(error_code, std::memory_order_relaxed);
}

bool PaperOrderTransport::should_reject_place(std::string_view client_order_id) const {
    if (config_.place_reject_bps == 0) {
        return false;
    }
    const std::uint32_t bucket = fnv1a_32(client_order_id) % 10'000U;
    return bucket < config_.place_reject_bps;
}

std::uint64_t PaperOrderTransport::monotonic_now_ns() {
    const auto now = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    return elapsed.count() > 0 ? static_cast<std::uint64_t>(elapsed.count()) : 0ULL;
}

void PaperOrderTransport::enqueue_update(std::uint64_t due_ts_ns, std::string payload) {
    const auto insert_it =
        std::upper_bound(inbound_updates_.begin(), inbound_updates_.end(), due_ts_ns,
                         [](std::uint64_t due_ns, const ScheduledUpdate& update) {
                             return due_ns < update.due_ts_ns;
                         });
    inbound_updates_.insert(insert_it, ScheduledUpdate{
                                           .due_ts_ns = due_ts_ns,
                                           .payload = std::move(payload),
                                       });
}

} // namespace trading::oms
