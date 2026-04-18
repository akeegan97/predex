from __future__ import annotations

from dataclasses import dataclass, field

from .tape import MarketEvent


@dataclass(slots=True)
class TopOfBook:
    price_ticks: int
    qty_lots: int


@dataclass(slots=True)
class ReplayBookState:
    market_ticker: str
    has_snapshot: bool = False
    desynced: bool = False
    last_sequence_id: int | None = None
    bids: dict[int, int] = field(default_factory=dict)
    asks: dict[int, int] = field(default_factory=dict)
    pending_deltas: list[MarketEvent] = field(default_factory=list)
    last_trade_price_ticks: int | None = None
    last_trade_qty_lots: int | None = None

    def best_bid(self) -> TopOfBook | None:
        positive_levels = [(price, qty) for price, qty in self.bids.items() if qty > 0]
        if not positive_levels:
            return None
        price, qty = max(positive_levels, key=lambda level: level[0])
        return TopOfBook(price_ticks=price, qty_lots=qty)

    def best_ask(self) -> TopOfBook | None:
        positive_levels = [(price, qty) for price, qty in self.asks.items() if qty > 0]
        if not positive_levels:
            return None
        price, qty = min(positive_levels, key=lambda level: level[0])
        return TopOfBook(price_ticks=price, qty_lots=qty)

    def _apply_delta(self, event: MarketEvent) -> bool:
        if event.sequence_id is None or self.last_sequence_id is None:
            self.desynced = True
            return False
        if event.sequence_id <= self.last_sequence_id:
            return False
        levels = self.bids if event.side == "bid" else self.asks
        updated_qty = levels.get(event.price_ticks, 0) + event.delta_qty_lots
        if updated_qty < 0:
            self.desynced = True
            return False
        if updated_qty == 0:
            levels.pop(event.price_ticks, None)
        else:
            levels[event.price_ticks] = updated_qty
        self.last_sequence_id = event.sequence_id
        return True

    def apply(self, event: MarketEvent) -> bool:
        if event.raw_type == "orderbook_snapshot":
            if self.has_snapshot:
                self.desynced = True
                return False
            self.has_snapshot = True
            self.last_sequence_id = event.sequence_id
            self.bids = {price: qty for price, qty in event.bids if qty > 0}
            self.asks = {price: qty for price, qty in event.asks if qty > 0}
            pending = list(self.pending_deltas)
            self.pending_deltas.clear()
            for pending_event in pending:
                if not self._apply_delta(pending_event):
                    return False
            return True

        if event.raw_type == "orderbook_delta":
            if not self.has_snapshot:
                self.pending_deltas.append(event)
                return False
            if self.desynced:
                return False
            return self._apply_delta(event)

        if event.raw_type == "trade":
            if not self.has_snapshot or self.desynced:
                return False
            if event.price_ticks <= 0 or event.trade_qty_lots <= 0:
                return False
            self.last_trade_price_ticks = event.price_ticks
            self.last_trade_qty_lots = event.trade_qty_lots
            return True

        return False


@dataclass(slots=True)
class ReplayBookStore:
    books: dict[str, ReplayBookState] = field(default_factory=dict)

    def state_for(self, market_ticker: str) -> ReplayBookState:
        if market_ticker not in self.books:
            self.books[market_ticker] = ReplayBookState(market_ticker=market_ticker)
        return self.books[market_ticker]

    def apply(self, event: MarketEvent) -> ReplayBookState:
        state = self.state_for(event.market_ticker)
        state.apply(event)
        return state
