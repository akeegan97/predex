from __future__ import annotations

import hashlib


def _stable_int(key: str, *, bytes_len: int, modulo: int, zero_allowed: bool) -> int:
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=bytes_len).digest()
    value = int.from_bytes(digest, byteorder="big", signed=False) % modulo
    if zero_allowed:
        return value
    return value if value != 0 else modulo - 1


def stable_event_id(event_ticker: str) -> int:
    return _stable_int(event_ticker, bytes_len=8, modulo=2**32 - 1, zero_allowed=False)


def stable_market_id(market_ticker: str) -> int:
    return _stable_int(market_ticker, bytes_len=8, modulo=2**32 - 1, zero_allowed=False)


def stable_affinity_key(event_ticker: str) -> int:
    return _stable_int(event_ticker, bytes_len=4, modulo=2**16, zero_allowed=True)
