#include "predex/shards/event_store.hpp"
#include "predex/internal/normalized_event.hpp"

#include <algorithm>
#include <optional>

namespace predex::core::shards::kalshi{
    namespace {
        void clear_depth_view(DepthView<kMaxDepth>& depth) {
            for (auto& level : depth.bids) {
                level = SideDepthLevel{};
            }
            for (auto& level : depth.asks) {
                level = SideDepthLevel{};
            }
        }

        void fill_bid_levels(const BookState::BidLevels& bids, DepthView<kMaxDepth>& depth) {
            std::size_t depth_index = 0;
            for (std::int64_t price_ticks = kMaxPriceTicks;
                 price_ticks >= 0 && depth_index < kMaxDepth;
                 --price_ticks) {
                const auto qty_lots = bids[static_cast<std::size_t>(price_ticks)];
                if (qty_lots <= 0) {
                    continue;
                }
                depth.bids[depth_index] = SideDepthLevel{
                    .price_ticks = price_ticks,
                    .qty_lots = qty_lots,
                };
                ++depth_index;
            }
        }

        void fill_ask_levels(const BookState::AskLevels& asks, DepthView<kMaxDepth>& depth) {
            std::size_t depth_index = 0;
            for (std::size_t price_ticks = 0;
                 price_ticks < asks.size() && depth_index < kMaxDepth;
                 ++price_ticks) {
                const auto qty_lots = asks[price_ticks];
                if (qty_lots <= 0) {
                    continue;
                }
                depth.asks[depth_index] = SideDepthLevel{
                    .price_ticks = static_cast<internal::PriceTicks>(price_ticks),
                    .qty_lots = qty_lots,
                };
                ++depth_index;
            }
        }

        void refresh_market_view(EventMarketView& market_view, const BookState* book) {
            clear_depth_view(market_view.depth);
            market_view.has_book = false;
            market_view.desynced = false;
            market_view.last_trade.reset();

            if (book == nullptr || !book->has_snapshot) {
                return;
            }

            market_view.has_book = true;
            market_view.desynced = book->desynced;
            market_view.last_trade = book->last_trade;
            fill_bid_levels(book->bids, market_view.depth);
            fill_ask_levels(book->asks, market_view.depth);
        }

        bool recompute_complete(const MonotonicChainState& state, std::size_t expected_market_count) {
            if (expected_market_count == 0 || state.markets.size() != expected_market_count) {
                return false;
            }
            return std::all_of(
                state.markets.begin(),
                state.markets.end(),
                [](const ChainEntry& entry) { return entry.market.has_book; });
        }

        bool recompute_desynced(const MonotonicChainState& state, bool event_desynced) {
            if (event_desynced) {
                return true;
            }
            return std::any_of(
                state.markets.begin(),
                state.markets.end(),
                [](const ChainEntry& entry) { return entry.market.desynced; });
        }

        bool recompute_complete(const std::vector<EventMarketView>& markets,
                                std::size_t expected_market_count) {
            if (expected_market_count == 0 || markets.size() != expected_market_count) {
                return false;
            }
            return std::all_of(
                markets.begin(), markets.end(), [](const EventMarketView& market) { return market.has_book; });
        }

        bool recompute_desynced(const std::vector<EventMarketView>& markets, bool event_desynced) {
            if (event_desynced) {
                return true;
            }
            return std::any_of(
                markets.begin(),
                markets.end(),
                [](const EventMarketView& market) { return market.desynced; });
        }

        void update_monotonic_chain_state(MonotonicChainState& state,
                                          internal::MarketId market_id,
                                          const BookState* book,
                                          internal::TimestampNs recv_ns,
                                          bool event_desynced,
                                          std::size_t expected_market_count) {
            const auto iterator = state.market_index_by_id.find(market_id);
            if (iterator == state.market_index_by_id.end()) {
                state.desynced = true;
                state.last_update_ns = recv_ns;
                return;
            }

            auto& entry = state.markets[iterator->second];
            refresh_market_view(entry.market, book);
            state.last_update_ns = recv_ns;
            state.complete = recompute_complete(state, expected_market_count);
            state.desynced = recompute_desynced(state, event_desynced);
        }

        void update_group_market_state(std::vector<EventMarketView>& markets,
                                       const std::unordered_map<internal::MarketId, std::size_t>& market_index_by_id,
                                       //NOLINTNEXTLINE
                                       bool& complete,
                                       bool& desynced,
                                       internal::TimestampNs& last_update_ns,
                                       internal::MarketId market_id,
                                       const BookState* book,
                                       internal::TimestampNs recv_ns,
                                       bool event_desynced,
                                       std::size_t expected_market_count) {
            const auto iterator = market_index_by_id.find(market_id);
            if (iterator == market_index_by_id.end()) {
                desynced = true;
                last_update_ns = recv_ns;
                return;
            }

            auto& market = markets[iterator->second];
            refresh_market_view(market, book);
            last_update_ns = recv_ns;
            complete = recompute_complete(markets, expected_market_count);
            desynced = recompute_desynced(markets, event_desynced);
        }

        void update_mutually_exclusive_state(MutuallyExclusiveState& state,
                                             internal::MarketId market_id,
                                             const BookState* book,
                                             internal::TimestampNs recv_ns,
                                             bool event_desynced,
                                             std::size_t expected_market_count) {
            update_group_market_state(
                state.markets,
                state.market_index_by_id,
                state.complete,
                state.desynced,
                state.last_update_ns,
                market_id,
                book,
                recv_ns,
                event_desynced,
                expected_market_count);
        }

        void update_unordered_group_state(UnorderedGroupState& state,
                                          internal::MarketId market_id,
                                          const BookState* book,
                                          internal::TimestampNs recv_ns,
                                          bool event_desynced,
                                          std::size_t expected_market_count) {
            update_group_market_state(
                state.markets,
                state.market_index_by_id,
                state.complete,
                state.desynced,
                state.last_update_ns,
                market_id,
                book,
                recv_ns,
                event_desynced,
                expected_market_count);
        }

        void update_single_market_state(SingleMarketState& state,
                                        internal::MarketId market_id,
                                        const BookState* book,
                                        internal::TimestampNs recv_ns,
                                        bool event_desynced,
                                        std::size_t expected_market_count) {
            if (!state.market.has_value() || state.market->market_id != market_id) {
                state.desynced = true;
                state.last_update_ns = recv_ns;
                return;
            }

            refresh_market_view(*state.market, book);
            state.last_update_ns = recv_ns;
            state.desynced = event_desynced || state.market->desynced;
            if (expected_market_count != 0 && expected_market_count != 1) {
                state.desynced = true;
            }
        }

        bool initialize_market_views(const std::vector<EventMarketDefinition>& definitions,
                                     std::vector<EventMarketView>& markets,
                                     std::unordered_map<internal::MarketId, std::size_t>& market_index_by_id) {
            markets.clear();
            market_index_by_id.clear();
            markets.reserve(definitions.size());
            market_index_by_id.reserve(definitions.size());

            for (const auto& definition : definitions) {
                if (definition.market_id == 0) {
                    return false;
                }
                const std::size_t market_index = markets.size();
                auto [iterator, inserted] = market_index_by_id.emplace(definition.market_id, market_index);
                if (!inserted) {
                    return false;
                }
                static_cast<void>(iterator);
                markets.push_back(EventMarketView{
                    .market_id = definition.market_id,
                    .lifecycle = MarketLifecycleState{
                        .close_time_s = definition.close_time_s,
                        .tradeable = definition.tradeable,
                    },
                });
            }
            return true;
        }

        std::optional<EventDerivedState> build_derived_state(const EventDefinition& definition) {
            if (definition.markets.empty()) {
                return std::nullopt;
            }

            switch (definition.topology_kind) {
                case internal::EventTopologyKind::kMonotonicChain: {
                    std::vector<EventMarketDefinition> ordered_markets = definition.markets;
                    std::sort(ordered_markets.begin(),
                              ordered_markets.end(),
                              [](const EventMarketDefinition& lhs, const EventMarketDefinition& rhs) {
                                  if (lhs.strike_key != rhs.strike_key) {
                                      return lhs.strike_key < rhs.strike_key;
                                  }
                                  return lhs.market_id < rhs.market_id;
                              });

                    MonotonicChainState state{};
                    state.markets.reserve(ordered_markets.size());
                    state.market_index_by_id.reserve(ordered_markets.size());

                    for (const auto& market_definition : ordered_markets) {
                        const std::size_t market_index = state.markets.size();
                        auto [iterator, inserted] =
                            state.market_index_by_id.emplace(market_definition.market_id, market_index);
                        if (!inserted) {
                            return std::nullopt;
                        }
                        static_cast<void>(iterator);
                        state.markets.push_back(ChainEntry{
                            .market = EventMarketView{
                                .market_id = market_definition.market_id,
                                .lifecycle = MarketLifecycleState{
                                    .close_time_s = market_definition.close_time_s,
                                    .tradeable = market_definition.tradeable,
                                },
                            },
                            .strike_key = market_definition.strike_key,
                        });
                    }
                    return EventDerivedState{std::move(state)};
                }
                case internal::EventTopologyKind::kMutuallyExclusive: {
                    MutuallyExclusiveState state{};
                    if (!initialize_market_views(
                            definition.markets, state.markets, state.market_index_by_id)) {
                        return std::nullopt;
                    }
                    return EventDerivedState{std::move(state)};
                }
                case internal::EventTopologyKind::kUnorderedGroup: {
                    UnorderedGroupState state{};
                    if (!initialize_market_views(
                            definition.markets, state.markets, state.market_index_by_id)) {
                        return std::nullopt;
                    }
                    return EventDerivedState{std::move(state)};
                }
                case internal::EventTopologyKind::kSingleMarket: {
                    if (definition.markets.size() != 1) {
                        return std::nullopt;
                    }
                    SingleMarketState state{};
                    state.market = EventMarketView{
                        .market_id = definition.markets.front().market_id,
                        .lifecycle = MarketLifecycleState{
                            .close_time_s = definition.markets.front().close_time_s,
                            .tradeable = definition.markets.front().tradeable,
                        },
                    };
                    return EventDerivedState{std::move(state)};
                }
                case internal::EventTopologyKind::kUnknown:
                default:
                    return std::nullopt;
            }
        }

        bool is_tradeable(internal::MarketLifecycleStatus status) noexcept {
            switch (status) {
                case internal::MarketLifecycleStatus::kActivated:
                case internal::MarketLifecycleStatus::kCloseDateUpdated:
                case internal::MarketLifecycleStatus::kFractionalTradingUpdated:
                case internal::MarketLifecycleStatus::kPriceLevelStructureUpdated:
                    return true;
                default:
                    return false;
            }
        }

        EventMarketView* find_market_view(EventDerivedState& state,
                                          internal::MarketId market_id) noexcept {
            if (auto* chain = std::get_if<MonotonicChainState>(&state)) {
                auto it = chain->market_index_by_id.find(market_id);
                if (it != chain->market_index_by_id.end()) {
                    return &chain->markets[it->second].market;
                }
            } else if (auto* exclusive = std::get_if<MutuallyExclusiveState>(&state)) {
                auto it = exclusive->market_index_by_id.find(market_id);
                if (it != exclusive->market_index_by_id.end()) {
                    return &exclusive->markets[it->second];
                }
            } else if (auto* group = std::get_if<UnorderedGroupState>(&state)) {
                auto it = group->market_index_by_id.find(market_id);
                if (it != group->market_index_by_id.end()) {
                    return &group->markets[it->second];
                }
            } else if (auto* single = std::get_if<SingleMarketState>(&state)) {
                if (single->market.has_value() && single->market->market_id == market_id) {
                    return &(*single->market);
                }
            }
            return nullptr;
        }

        std::optional<Event> build_event(const EventDefinition& definition) {
            if (definition.event_id == 0 ||
                definition.topology_kind == internal::EventTopologyKind::kUnknown ||
                definition.markets.empty()) {
                return std::nullopt;
            }

            const std::size_t expected_market_count =
                definition.expected_market_count == 0 ? definition.markets.size()
                                                     : definition.expected_market_count;
            if (expected_market_count < definition.markets.size()) {
                return std::nullopt;
            }

            auto derived_state = build_derived_state(definition);
            if (!derived_state.has_value()) {
                return std::nullopt;
            }

            Event event{};
            event.event_id = definition.event_id;
            event.topology_kind = definition.topology_kind;
            event.expected_market_count = expected_market_count;
            event.derived_state = std::move(*derived_state);
            return event;
        }
    } // namespace

    bool EventStore::initialize(const std::vector<EventDefinition>& definitions) {
        events_.clear();
        events_.reserve(definitions.size());

        for (const auto& definition : definitions) {
            auto event = build_event(definition);
            if (!event.has_value()) {
                events_.clear();
                return false;
            }

            auto [iterator, inserted] = events_.emplace(definition.event_id, std::move(*event));
            if (!inserted) {
                events_.clear();
                return false;
            }
            static_cast<void>(iterator);
        }
        return true;
    }

    EventApplyCode Event::apply_market_update(const internal::NormalizedEvent& event){
        if (event.meta.event_id == 0 || event.meta.event_id != event_id) {
            return EventApplyCode::kRejected;
        }
        if (event.meta.topology_kind != internal::EventTopologyKind::kUnknown &&
            event.meta.topology_kind != topology_kind) {
            return EventApplyCode::kRejected;
        }

        if (event.type == internal::EventType::kLifecycle) {
            const auto* lifecycle = std::get_if<internal::MarketLifecycleData>(&event.data);
            if (lifecycle == nullptr) {
                return EventApplyCode::kParseFail;
            }
            auto* view = find_market_view(derived_state, event.meta.market_id);
            if (view == nullptr) {
                return EventApplyCode::kRejected;
            }
            view->lifecycle.open_ts_s = lifecycle->open_ts_s;
            view->lifecycle.close_ts_s = lifecycle->close_ts_s;
            view->lifecycle.tradeable = is_tradeable(lifecycle->status);
            last_update_ns = event.meta.recv_ns;
            return EventApplyCode::kApplied;
        }

        auto apply_result = book_store.apply_with_result(event);

        switch(apply_result.reason){
            case BookApplyRejectReason::kUnsupportedEventType:
                //ignore unsupported event types like heartbeat/status
                return EventApplyCode::kRejected;
            //NOLINTNEXTLINE
            case BookApplyRejectReason::kUnexpectedSnapshotAfterInit:
            case BookApplyRejectReason::kDeltaWhileDesynced:
            case BookApplyRejectReason::kTradeInvalidOrDesynced:
                desynced = true;
                return EventApplyCode::kRejected;
            case BookApplyRejectReason::kDeltaSequence:
            case BookApplyRejectReason::kInvalidSide:
            case BookApplyRejectReason::kNegativeQuantity:
            case BookApplyRejectReason::kInvalidSeq:
                //these are all reasons that indicate we are missing a delta or snapshot update, mark desynced and reject the event update
                desynced = true;
                return EventApplyCode::kRejected;
            default:
                break;
        }
        if(apply_result.applied){
            topology_kind = event.meta.topology_kind == internal::EventTopologyKind::kUnknown
                ? topology_kind
                : event.meta.topology_kind;
            last_update_ns = event.meta.recv_ns;

            const BookState* book = book_store.find(event.meta.market_id);
            if (auto* state = std::get_if<MonotonicChainState>(&derived_state)) {
                update_monotonic_chain_state(
                    *state,
                    event.meta.market_id,
                    book,
                    event.meta.recv_ns,
                    desynced,
                    expected_market_count);
            } else if (auto* state = std::get_if<MutuallyExclusiveState>(&derived_state)) {
                update_mutually_exclusive_state(
                    *state,
                    event.meta.market_id,
                    book,
                    event.meta.recv_ns,
                    desynced,
                    expected_market_count);
            } else if (auto* state = std::get_if<UnorderedGroupState>(&derived_state)) {
                update_unordered_group_state(
                    *state,
                    event.meta.market_id,
                    book,
                    event.meta.recv_ns,
                    desynced,
                    expected_market_count);
            } else if (auto* state = std::get_if<SingleMarketState>(&derived_state)) {
                update_single_market_state(
                    *state,
                    event.meta.market_id,
                    book,
                    event.meta.recv_ns,
                    desynced,
                    expected_market_count);
            }
            return EventApplyCode::kApplied;
        }
        return EventApplyCode::kRejected;
    }

    void EventStore::reset_all_books() noexcept {
        for (auto& [event_id, event] : events_) {
            event.book_store.reset_all();
        }
    }

    std::size_t EventStore::desynced_event_count() const noexcept {
        std::size_t count = 0;
        for (const auto& [event_id, event] : events_) {
            if (event.desynced) {
                ++count;
            }
        }
        return count;
    }

    const EventMarketView* Event::find_market_view(
        internal::MarketId market_id) const noexcept {
        if (const auto* chain = std::get_if<MonotonicChainState>(&derived_state)) {
            const auto it = chain->market_index_by_id.find(market_id);
            if (it != chain->market_index_by_id.end()) {
                return &chain->markets[it->second].market;
            }
        } else if (const auto* exclusive = std::get_if<MutuallyExclusiveState>(&derived_state)) {
            const auto it = exclusive->market_index_by_id.find(market_id);
            if (it != exclusive->market_index_by_id.end()) {
                return &exclusive->markets[it->second];
            }
        } else if (const auto* unordered = std::get_if<UnorderedGroupState>(&derived_state)) {
            const auto it = unordered->market_index_by_id.find(market_id);
            if (it != unordered->market_index_by_id.end()) {
                return &unordered->markets[it->second];
            }
        } else if (const auto* single = std::get_if<SingleMarketState>(&derived_state)) {
            if (single->market.has_value() && single->market->market_id == market_id) {
                return &*single->market;
            }
        }
        return nullptr;
    }

}
