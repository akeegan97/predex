from __future__ import annotations

from dataclasses import dataclass, field

from .features import ChainFeatureSnapshot, MarketFeatureSnapshot
from .models import BookDelta, BookLevel, LifecycleEvent, MarketSnapshot, PublicTrade, ResearchMarketRoute


@dataclass(frozen=True, slots=True)
class MarketQuote:
    price_ticks: int | None
    qty_lots: int = 0


@dataclass(slots=True)
class MarketBook:
    market_id: int
    market_ticker: str = ""
    event_market_index: int = 0
    bids: dict[int, int] = field(default_factory=dict)
    asks: dict[int, int] = field(default_factory=dict)
    initialized: bool = False
    last_sequence: int = 0
    best_bid_price_ticks: int | None = None
    best_bid_qty_lots: int = 0
    best_ask_price_ticks: int | None = None
    best_ask_qty_lots: int = 0

    def apply_snapshot(self, snapshot: MarketSnapshot) -> None:
        self.bids = _levels_to_book(snapshot.bid_levels)
        self.asks = _levels_to_book(snapshot.ask_levels)
        self._refresh_best_bid()
        self._refresh_best_ask()
        self.initialized = True
        self.last_sequence = snapshot.sequence

    def apply_delta(self, delta: BookDelta) -> None:
        book = self.bids if delta.side == "bid" else self.asks
        current_qty = book.get(delta.price_ticks, 0)
        next_qty = current_qty + delta.delta_qty_lots
        if next_qty < 0:
            raise ValueError(
                f"delta would make negative qty for market={self.market_id} "
                f"side={delta.side} price={delta.price_ticks}: {current_qty}+{delta.delta_qty_lots}"
            )
        if next_qty == 0:
            book.pop(delta.price_ticks, None)
        else:
            book[delta.price_ticks] = next_qty
        self._update_cached_quote(delta.side, delta.price_ticks, next_qty)
        self.initialized = True
        self.last_sequence = delta.sequence

    def best_bid(self) -> MarketQuote:
        return MarketQuote(price_ticks=self.best_bid_price_ticks, qty_lots=self.best_bid_qty_lots)

    def best_ask(self) -> MarketQuote:
        return MarketQuote(price_ticks=self.best_ask_price_ticks, qty_lots=self.best_ask_qty_lots)

    def mid_ticks(self) -> float | None:
        bid = self.best_bid().price_ticks
        ask = self.best_ask().price_ticks
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2.0

    def to_feature_snapshot(self) -> MarketFeatureSnapshot:
        bid = self.best_bid()
        ask = self.best_ask()
        return MarketFeatureSnapshot(
            market_id=self.market_id,
            market_ticker=self.market_ticker,
            event_market_index=self.event_market_index,
            best_bid_ticks=bid.price_ticks,
            best_bid_qty_lots=bid.qty_lots,
            best_ask_ticks=ask.price_ticks,
            best_ask_qty_lots=ask.qty_lots,
            mid_ticks=self.mid_ticks(),
            initialized=self.initialized,
        )

    def _update_cached_quote(self, side: str, price_ticks: int, next_qty: int) -> None:
        if side == "bid":
            if next_qty == 0 and price_ticks == self.best_bid_price_ticks:
                self._refresh_best_bid()
            elif next_qty > 0 and (self.best_bid_price_ticks is None or price_ticks > self.best_bid_price_ticks):
                self.best_bid_price_ticks = price_ticks
                self.best_bid_qty_lots = next_qty
            elif next_qty > 0 and price_ticks == self.best_bid_price_ticks:
                self.best_bid_qty_lots = next_qty
        else:
            if next_qty == 0 and price_ticks == self.best_ask_price_ticks:
                self._refresh_best_ask()
            elif next_qty > 0 and (self.best_ask_price_ticks is None or price_ticks < self.best_ask_price_ticks):
                self.best_ask_price_ticks = price_ticks
                self.best_ask_qty_lots = next_qty
            elif next_qty > 0 and price_ticks == self.best_ask_price_ticks:
                self.best_ask_qty_lots = next_qty

    def _refresh_best_bid(self) -> None:
        if not self.bids:
            self.best_bid_price_ticks = None
            self.best_bid_qty_lots = 0
            return
        price = max(self.bids)
        self.best_bid_price_ticks = price
        self.best_bid_qty_lots = self.bids[price]

    def _refresh_best_ask(self) -> None:
        if not self.asks:
            self.best_ask_price_ticks = None
            self.best_ask_qty_lots = 0
            return
        price = min(self.asks)
        self.best_ask_price_ticks = price
        self.best_ask_qty_lots = self.asks[price]


@dataclass(slots=True)
class EventChainState:
    event_id: int
    routes: tuple[ResearchMarketRoute, ...]
    books: dict[int, MarketBook] = field(init=False)
    sorted_routes: tuple[ResearchMarketRoute, ...] = field(init=False)
    last_recv_ts_ns: int = 0
    last_record_index: int = 0
    last_updated_market_id: int = 0
    trade_count: int = 0
    lifecycle_count: int = 0

    def __post_init__(self) -> None:
        self.sorted_routes = tuple(sorted(self.routes, key=lambda item: item.event_market_index))
        self.books = {
            route.market_id: MarketBook(
                market_id=route.market_id,
                market_ticker=route.market_ticker,
                event_market_index=route.event_market_index,
            )
            for route in self.sorted_routes
        }

    def apply(self, update: MarketSnapshot | BookDelta | PublicTrade | LifecycleEvent) -> ChainFeatureSnapshot | None:
        self.last_recv_ts_ns = update.recv_ts_ns
        self.last_record_index = update.record_index
        self.last_updated_market_id = update.market_id

        if isinstance(update, MarketSnapshot):
            self._book_for(update.market_id).apply_snapshot(update)
        elif isinstance(update, BookDelta):
            self._book_for(update.market_id).apply_delta(update)
        elif isinstance(update, PublicTrade):
            self.trade_count += 1
        elif isinstance(update, LifecycleEvent):
            self.lifecycle_count += 1
        else:
            raise TypeError(f"unsupported update type: {type(update)!r}")

        return self.snapshot()

    def snapshot(self) -> ChainFeatureSnapshot:
        return ChainFeatureSnapshot(
            event_id=self.event_id,
            recv_ts_ns=self.last_recv_ts_ns,
            record_index=self.last_record_index,
            updated_market_id=self.last_updated_market_id,
            markets=tuple(
                self.books[route.market_id].to_feature_snapshot()
                for route in self.sorted_routes
            ),
            context={
                "trade_count": self.trade_count,
                "lifecycle_count": self.lifecycle_count,
            },
        )

    def _book_for(self, market_id: int) -> MarketBook:
        try:
            return self.books[market_id]
        except KeyError as exc:
            raise KeyError(f"market_id {market_id} is not part of event {self.event_id}") from exc


def _levels_to_book(levels: tuple[BookLevel, ...]) -> dict[int, int]:
    book: dict[int, int] = {}
    for level in levels:
        if level.qty_lots < 0:
            raise ValueError(f"snapshot level has negative qty: {level}")
        if level.qty_lots > 0:
            book[level.price_ticks] = level.qty_lots
    return book
