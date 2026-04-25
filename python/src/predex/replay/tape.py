from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterator


_TICKS_SCALE = Decimal("1000")
_MAX_PRICE_TICKS = 1000


def _parse_decimal_to_ticks(value: Any) -> int:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid price value: {value!r}") from exc
    return int((decimal_value * _TICKS_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


def _parse_decimal_to_lots(value: Any) -> int:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid quantity value: {value!r}") from exc
    return int(decimal_value.to_integral_value(rounding=ROUND_HALF_UP))


def _reciprocal_price(price_ticks: int) -> int:
    return _MAX_PRICE_TICKS - price_ticks


@dataclass(frozen=True, slots=True)
class TapePayload:
    record_index: int
    payload: bytes
    message: dict[str, Any]


@dataclass(frozen=True, slots=True)
class MarketEvent:
    record_index: int
    raw_type: str
    market_ticker: str
    sequence_id: int | None
    market_payload: dict[str, Any]
    bids: tuple[tuple[int, int], ...] = ()
    asks: tuple[tuple[int, int], ...] = ()
    side: str = ""
    price_ticks: int = 0
    delta_qty_lots: int = 0
    trade_qty_lots: int = 0


def iter_tape_payloads(path: str | Path) -> Iterator[TapePayload]:
    with Path(path).open("rb") as handle:
        record_index = 0
        while True:
            header = handle.read(4)
            if len(header) == 0:
                return
            if len(header) != 4:
                raise ValueError("tape ended mid-record header")
            (payload_len,) = struct.unpack("<I", header)
            payload = handle.read(payload_len)
            if len(payload) != payload_len:
                raise ValueError("tape ended mid-record payload")
            yield TapePayload(
                record_index=record_index,
                payload=payload,
                message=json.loads(payload),
            )
            record_index += 1


def _parse_snapshot(msg: dict[str, Any], payload: TapePayload) -> MarketEvent:
    bids = tuple(
        (_parse_decimal_to_ticks(price), _parse_decimal_to_lots(qty))
        for price, qty in msg.get("yes_dollars_fp") or []
    )
    asks = tuple(
        (_reciprocal_price(_parse_decimal_to_ticks(price)), _parse_decimal_to_lots(qty))
        for price, qty in msg.get("no_dollars_fp") or []
    )
    return MarketEvent(
        record_index=payload.record_index,
        raw_type="orderbook_snapshot",
        market_ticker=str(msg.get("market_ticker", "")),
        sequence_id=int(payload.message["seq"]),
        market_payload=msg,
        bids=bids,
        asks=asks,
    )


def _parse_delta(msg: dict[str, Any], payload: TapePayload) -> MarketEvent:
    side = str(msg.get("side", "")).lower()
    if side in {"yes", "bid"}:
        price_ticks = _parse_decimal_to_ticks(msg.get("price_dollars", msg.get("price")))
        normalized_side = "bid"
    elif side in {"no", "ask"}:
        raw_price = _parse_decimal_to_ticks(msg.get("price_dollars", msg.get("price")))
        price_ticks = _reciprocal_price(raw_price)
        normalized_side = "ask"
    elif "yes_price_dollars" in msg or "yes_price" in msg:
        price_ticks = _parse_decimal_to_ticks(msg.get("yes_price_dollars", msg.get("yes_price")))
        normalized_side = "bid"
    elif "no_price_dollars" in msg or "no_price" in msg:
        raw_price = _parse_decimal_to_ticks(msg.get("no_price_dollars", msg.get("no_price")))
        price_ticks = _reciprocal_price(raw_price)
        normalized_side = "ask"
    else:
        raise ValueError(f"delta payload missing side/price fields: {msg!r}")

    delta_qty = _parse_decimal_to_lots(
        msg.get("delta_fp", msg.get("delta", msg.get("yes_delta_fp", msg.get("no_delta_fp", 0))))
    )
    return MarketEvent(
        record_index=payload.record_index,
        raw_type="orderbook_delta",
        market_ticker=str(msg.get("market_ticker", "")),
        sequence_id=int(payload.message["seq"]),
        market_payload=msg,
        side=normalized_side,
        price_ticks=price_ticks,
        delta_qty_lots=delta_qty,
    )


def _parse_trade(msg: dict[str, Any], payload: TapePayload) -> MarketEvent:
    if "yes_price_dollars" in msg or "yes_price" in msg:
        price_ticks = _parse_decimal_to_ticks(msg.get("yes_price_dollars", msg.get("yes_price")))
    else:
        price_ticks = _parse_decimal_to_ticks(msg.get("price_dollars", msg.get("price", 0)))
    qty_lots = _parse_decimal_to_lots(msg.get("count_fp", msg.get("count", 0)))
    return MarketEvent(
        record_index=payload.record_index,
        raw_type="trade",
        market_ticker=str(msg.get("market_ticker", "")),
        sequence_id=int(payload.message["seq"]),
        market_payload=msg,
        price_ticks=price_ticks,
        trade_qty_lots=qty_lots,
    )


def decode_market_event(payload: TapePayload) -> MarketEvent | None:
    raw_type = str(payload.message.get("type", ""))
    if raw_type not in {"orderbook_snapshot", "orderbook_delta", "trade"}:
        return None
    msg = payload.message.get("msg")
    if not isinstance(msg, dict):
        return None
    if raw_type == "orderbook_snapshot":
        return _parse_snapshot(msg, payload)
    if raw_type == "orderbook_delta":
        return _parse_delta(msg, payload)
    return _parse_trade(msg, payload)


def iter_market_events(path: str | Path) -> Iterator[MarketEvent]:
    for payload in iter_tape_payloads(path):
        market_event = decode_market_event(payload)
        if market_event is not None and market_event.market_ticker:
            yield market_event
