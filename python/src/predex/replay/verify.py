from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

from .audit import AuditEvent, SignalBundle
from .books import ReplayBookStore
from .config import ConfigIndex, EventRoute
from .tape import iter_market_events


_PRICE_SCALE = 1000.0
_CENT_SCALE = 100.0
_TICKS_PER_CENT = 10
_TAKER_FEE_RATE = 0.07
_SIDE_BUY = 3
_SIDE_SELL = 4
_RISK_REJECT_REASONS = {
    0: "none",
    1: "event_exposure_limit",
    2: "market_exposure_limit",
    3: "max_open_intents",
    4: "strategy_disabled",
    5: "invalid_signal",
    6: "invalid_intent",
}
_RISK_DECISION_CODES = {
    1: "accepted",
    2: "rejected",
    3: "clipped",
    4: "disabled",
    5: "error",
}


def _fee_ticks(price_ticks: int, qty_lots: int) -> int:
    if qty_lots <= 0 or price_ticks <= 0 or price_ticks >= int(_PRICE_SCALE):
        return 0
    price_dollars = float(price_ticks) / _PRICE_SCALE
    fee_dollars = _TAKER_FEE_RATE * float(qty_lots) * price_dollars * (1.0 - price_dollars)
    fee_cents = math.ceil(fee_dollars * _CENT_SCALE)
    return fee_cents * _TICKS_PER_CENT


@dataclass(frozen=True, slots=True)
class SignalVerificationResult:
    matched: bool
    reason: str
    event_id: int
    event_ticker: str
    record_index: int | None
    easier_market_ticker: str | None
    harder_market_ticker: str | None
    easier_ask_ticks: int | None
    harder_bid_ticks: int | None
    recomputed_edge_ticks: int | None
    risk_summary: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "matched": self.matched,
            "reason": self.reason,
            "event_id": self.event_id,
            "event_ticker": self.event_ticker,
            "record_index": self.record_index,
            "easier_market_ticker": self.easier_market_ticker,
            "harder_market_ticker": self.harder_market_ticker,
            "easier_ask_ticks": self.easier_ask_ticks,
            "harder_bid_ticks": self.harder_bid_ticks,
            "recomputed_edge_ticks": self.recomputed_edge_ticks,
            "risk_summary": list(self.risk_summary),
        }


def _risk_summary(bundle: SignalBundle) -> tuple[str, ...]:
    return tuple(
        f"{_RISK_DECISION_CODES.get(event.decision_code, str(event.decision_code))}:"
        f"{_RISK_REJECT_REASONS.get(event.reject_reason, str(event.reject_reason))}"
        for event in sorted(bundle.local_risk_events, key=lambda item: (item.leg_index, item.ts_ns))
    )


def _candidate_legs(bundle: SignalBundle) -> tuple[AuditEvent | None, AuditEvent | None]:
    legs = bundle.legs()
    buy_leg = next((event for event in legs if event.side == _SIDE_BUY), None)
    sell_leg = next((event for event in legs if event.side == _SIDE_SELL), None)
    return (buy_leg, sell_leg)


def _event_route(config_index: ConfigIndex, event_id: int) -> EventRoute | None:
    return config_index.events_by_id.get(event_id)


def verify_signal_bundle(
    bundle: SignalBundle,
    *,
    config_index: ConfigIndex,
    tape_path: str | Path,
) -> SignalVerificationResult:
    event_route = _event_route(config_index, bundle.event_id)
    if event_route is None:
        return SignalVerificationResult(
            matched=False,
            reason="event_id not found in config",
            event_id=bundle.event_id,
            event_ticker="",
            record_index=None,
            easier_market_ticker=None,
            harder_market_ticker=None,
            easier_ask_ticks=None,
            harder_bid_ticks=None,
            recomputed_edge_ticks=None,
            risk_summary=_risk_summary(bundle),
        )

    if event_route.topology_kind != "monotonic_chain":
        return SignalVerificationResult(
            matched=False,
            reason=f"unsupported topology for verification: {event_route.topology_kind}",
            event_id=bundle.event_id,
            event_ticker=event_route.markets[0].market_ticker.rsplit("-", 1)[0],
            record_index=None,
            easier_market_ticker=None,
            harder_market_ticker=None,
            easier_ask_ticks=None,
            harder_bid_ticks=None,
            recomputed_edge_ticks=None,
            risk_summary=_risk_summary(bundle),
        )

    buy_leg, sell_leg = _candidate_legs(bundle)
    easier_market = config_index.markets_by_id.get(buy_leg.market_id) if buy_leg is not None else None
    harder_market = config_index.markets_by_id.get(sell_leg.market_id) if sell_leg is not None else None

    if easier_market is None and harder_market is None:
        return SignalVerificationResult(
            matched=False,
            reason="could not infer any signal legs from audit",
            event_id=bundle.event_id,
            event_ticker=event_route.markets[0].market_ticker.rsplit("-", 1)[0],
            record_index=None,
            easier_market_ticker=None,
            harder_market_ticker=None,
            easier_ask_ticks=None,
            harder_bid_ticks=None,
            recomputed_edge_ticks=None,
            risk_summary=_risk_summary(bundle),
        )

    ordered_markets = list(event_route.markets)
    market_index_by_id = {market.market_id: index for index, market in enumerate(ordered_markets)}
    if easier_market is None and harder_market is not None:
        harder_index = market_index_by_id.get(harder_market.market_id)
        if harder_index is None or harder_index == 0:
            return SignalVerificationResult(
                matched=False,
                reason="sell leg is not part of an adjacent monotonic pair",
                event_id=bundle.event_id,
                event_ticker=harder_market.market_ticker.rsplit("-", 1)[0],
                record_index=None,
                easier_market_ticker=None,
                harder_market_ticker=harder_market.market_ticker,
                easier_ask_ticks=None,
                harder_bid_ticks=sell_leg.price_ticks if sell_leg else None,
                recomputed_edge_ticks=None,
                risk_summary=_risk_summary(bundle),
            )
        easier_market = ordered_markets[harder_index - 1]
    if harder_market is None and easier_market is not None:
        easier_index = market_index_by_id.get(easier_market.market_id)
        if easier_index is None or easier_index + 1 >= len(ordered_markets):
            return SignalVerificationResult(
                matched=False,
                reason="buy leg is not part of an adjacent monotonic pair",
                event_id=bundle.event_id,
                event_ticker=easier_market.market_ticker.rsplit("-", 1)[0],
                record_index=None,
                easier_market_ticker=easier_market.market_ticker,
                harder_market_ticker=None,
                easier_ask_ticks=buy_leg.price_ticks if buy_leg else None,
                harder_bid_ticks=None,
                recomputed_edge_ticks=None,
                risk_summary=_risk_summary(bundle),
            )
        harder_market = ordered_markets[easier_index + 1]

    if easier_market is None or harder_market is None:
        return SignalVerificationResult(
            matched=False,
            reason="leg market_id missing from config",
            event_id=bundle.event_id,
            event_ticker=event_route.markets[0].market_ticker.rsplit("-", 1)[0],
            record_index=None,
            easier_market_ticker=easier_market.market_ticker if easier_market else None,
            harder_market_ticker=harder_market.market_ticker if harder_market else None,
            easier_ask_ticks=buy_leg.price_ticks if buy_leg else None,
            harder_bid_ticks=sell_leg.price_ticks if sell_leg else None,
            recomputed_edge_ticks=None,
            risk_summary=_risk_summary(bundle),
        )

    if easier_market.strike_key >= harder_market.strike_key:
        return SignalVerificationResult(
            matched=False,
            reason="buy/sell legs do not follow monotonic strike ordering",
            event_id=bundle.event_id,
            event_ticker=easier_market.market_ticker.rsplit("-", 1)[0],
            record_index=None,
            easier_market_ticker=easier_market.market_ticker,
            harder_market_ticker=harder_market.market_ticker,
            easier_ask_ticks=None,
            harder_bid_ticks=None,
            recomputed_edge_ticks=None,
            risk_summary=_risk_summary(bundle),
        )

    watched_tickers = {market.market_ticker for market in event_route.markets}
    books = ReplayBookStore()
    for market_event in iter_market_events(tape_path):
        if market_event.market_ticker not in watched_tickers:
            continue
        books.apply(market_event)
        easier_state = books.books.get(easier_market.market_ticker)
        harder_state = books.books.get(harder_market.market_ticker)
        if easier_state is None or harder_state is None:
            continue
        easier_ask = easier_state.best_ask()
        harder_bid = harder_state.best_bid()
        if easier_ask is None or harder_bid is None:
            continue
        target_qty = max(
            buy_leg.qty_lots if buy_leg is not None else 0,
            sell_leg.qty_lots if sell_leg is not None else 0,
            1,
        )
        executable_qty = min(target_qty, easier_ask.qty_lots, harder_bid.qty_lots)
        if executable_qty <= 0:
            continue
        gross_edge_ticks = (harder_bid.price_ticks - easier_ask.price_ticks) * executable_qty
        net_edge_ticks = gross_edge_ticks - _fee_ticks(easier_ask.price_ticks, executable_qty) - _fee_ticks(
            harder_bid.price_ticks,
            executable_qty,
        )
        if (
            (buy_leg is None or easier_ask.price_ticks == buy_leg.price_ticks)
            and (sell_leg is None or harder_bid.price_ticks == sell_leg.price_ticks)
            and net_edge_ticks == bundle.group_signal.edge_ticks
        ):
            return SignalVerificationResult(
                matched=True,
                reason="matched replayed top-of-book and edge",
                event_id=bundle.event_id,
                event_ticker=easier_market.market_ticker.rsplit("-", 1)[0],
                record_index=market_event.record_index,
                easier_market_ticker=easier_market.market_ticker,
                harder_market_ticker=harder_market.market_ticker,
                easier_ask_ticks=easier_ask.price_ticks,
                harder_bid_ticks=harder_bid.price_ticks,
                recomputed_edge_ticks=net_edge_ticks,
                risk_summary=_risk_summary(bundle),
            )

    return SignalVerificationResult(
        matched=False,
        reason="no replayed book state matched the audited legs and edge",
        event_id=bundle.event_id,
        event_ticker=easier_market.market_ticker.rsplit("-", 1)[0],
        record_index=None,
        easier_market_ticker=easier_market.market_ticker,
        harder_market_ticker=harder_market.market_ticker,
        easier_ask_ticks=buy_leg.price_ticks if buy_leg is not None else None,
        harder_bid_ticks=sell_leg.price_ticks if sell_leg is not None else None,
        recomputed_edge_ticks=None,
        risk_summary=_risk_summary(bundle),
    )
