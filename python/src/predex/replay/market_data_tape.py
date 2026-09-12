from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


TAPE_MAGIC = b"PDT2"
TAPE_VERSION = 2
FILE_HEADER = struct.Struct("<4sHH")
RECORD_HEADER = struct.Struct("<QQQQIIIIIIIBBH")

FRAME_KIND_NAMES = {
    0: "unknown",
    1: "orderbook_snapshot",
    2: "orderbook_delta",
    3: "trade",
    4: "subscription_ack",
    5: "unsubscribed",
    6: "lifecycle",
    7: "heartbeat",
}

TOPOLOGY_NAMES = {
    0: "unknown",
    1: "monotonic_chain",
    3: "mutually_exclusive",
    4: "unordered_group",
    5: "single_market",
}


@dataclass(frozen=True, slots=True)
class MarketDataTapeHeader:
    version: int
    flags: int


@dataclass(frozen=True, slots=True)
class MarketDataRecordHeader:
    universe_version: int
    recv_ts_ns: int
    sequence: int
    affinity_key: int
    sid: int
    market_id: int
    event_id: int
    shard_index: int
    shard_event_index: int
    event_market_index: int
    payload_len: int
    frame_kind: int
    topology: int
    flags: int

    @property
    def frame_kind_name(self) -> str:
        return FRAME_KIND_NAMES.get(self.frame_kind, f"unknown_{self.frame_kind}")

    @property
    def topology_name(self) -> str:
        return TOPOLOGY_NAMES.get(self.topology, f"unknown_{self.topology}")


@dataclass(frozen=True, slots=True)
class MarketDataTapeRecord:
    record_index: int
    header: MarketDataRecordHeader
    payload: bytes

    def decode_json(self) -> dict[str, Any]:
        decoded = json.loads(self.payload)
        if not isinstance(decoded, dict):
            raise ValueError(f"record {self.record_index} payload is not a JSON object")
        return decoded


def read_market_data_tape_header(path: str | Path) -> MarketDataTapeHeader:
    with Path(path).open("rb") as handle:
        raw_header = handle.read(FILE_HEADER.size)
    if len(raw_header) == 0:
        raise ValueError("tape is empty")
    if len(raw_header) != FILE_HEADER.size:
        raise ValueError("tape ended mid-file header")
    magic, version, flags = FILE_HEADER.unpack(raw_header)
    if magic != TAPE_MAGIC:
        raise ValueError(f"unsupported tape magic: {magic!r}")
    if version != TAPE_VERSION:
        raise ValueError(f"unsupported tape version: {version}")
    return MarketDataTapeHeader(version=version, flags=flags)


def iter_market_data_records(path: str | Path) -> Iterator[MarketDataTapeRecord]:
    with Path(path).open("rb") as handle:
        raw_header = handle.read(FILE_HEADER.size)
        if len(raw_header) == 0:
            return
        if len(raw_header) != FILE_HEADER.size:
            raise ValueError("tape ended mid-file header")
        magic, version, _flags = FILE_HEADER.unpack(raw_header)
        if magic != TAPE_MAGIC:
            raise ValueError(f"unsupported tape magic: {magic!r}")
        if version != TAPE_VERSION:
            raise ValueError(f"unsupported tape version: {version}")

        record_index = 0
        while True:
            raw_record_header = handle.read(RECORD_HEADER.size)
            if len(raw_record_header) == 0:
                return
            if len(raw_record_header) != RECORD_HEADER.size:
                raise ValueError(f"tape ended mid-record header at index {record_index}")

            header = MarketDataRecordHeader(*RECORD_HEADER.unpack(raw_record_header))
            payload = handle.read(header.payload_len)
            if len(payload) != header.payload_len:
                raise ValueError(f"tape ended mid-record payload at index {record_index}")

            yield MarketDataTapeRecord(
                record_index=record_index,
                header=header,
                payload=payload,
            )
            record_index += 1
