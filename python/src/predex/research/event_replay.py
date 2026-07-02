from __future__ import annotations

import heapq
from collections.abc import Iterable
from pathlib import Path
from typing import Callable, Iterator, Sequence

from .models import BookDelta, BookLevel, LifecycleEvent, MarketSnapshot, MarketUpdate, PublicTrade, ResearchMarketRoute


def _require_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError("research table replay requires pandas and a parquet engine") from exc
    return pd


def _table_path(run_dir: str | Path, table_name: str) -> Path:
    path = Path(run_dir) / "tables" / f"{table_name}.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def load_routes_from_tables(
    run_dir: str | Path,
    *,
    topology_filter: Iterable[str] | None = ("monotonic_chain",),
) -> dict[int, tuple[ResearchMarketRoute, ...]]:
    pd = _require_pandas()
    table = pd.read_parquet(
        _table_path(run_dir, "market_routes"),
        columns=[
            "event_id",
            "market_id",
            "market_ticker",
            "topology",
            "event_market_index",
            "tradeable",
            "price_level_structure",
        ],
    )
    topologies = None if topology_filter is None else {str(topology) for topology in topology_filter}
    if topologies is not None:
        table = table.loc[table["topology"].isin(topologies)]

    routes_by_event: dict[int, list[ResearchMarketRoute]] = {}
    for row in table.sort_values(["event_id", "event_market_index"]).itertuples(index=False):
        route = _route_from_row(row)
        routes_by_event.setdefault(route.event_id, []).append(route)
    return {
        event_id: tuple(routes)
        for event_id, routes in routes_by_event.items()
    }


def load_event_routes_from_tables(run_dir: str | Path, event_id: int) -> tuple[ResearchMarketRoute, ...]:
    pd = _require_pandas()
    table = pd.read_parquet(
        _table_path(run_dir, "market_routes"),
        columns=[
            "event_id",
            "market_id",
            "market_ticker",
            "topology",
            "event_market_index",
            "tradeable",
            "price_level_structure",
        ],
    )
    event_routes = table.loc[table["event_id"] == int(event_id)].sort_values("event_market_index")
    return tuple(_route_from_row(row) for row in event_routes.itertuples(index=False))


def iter_event_updates_from_tables(
    run_dir: str | Path,
    event_id: int,
    *,
    include_trades: bool = True,
    include_lifecycle: bool = False,
) -> Iterator[MarketUpdate]:
    pd = _require_pandas()
    event_id = int(event_id)

    frames = pd.read_parquet(
        _table_path(run_dir, "frames"),
        columns=["record_index", "frame_kind", "event_id"],
    )
    frame_index = frames.loc[frames["event_id"] == event_id, ["record_index", "frame_kind"]]
    if frame_index.empty:
        return

    snapshot_levels = _load_snapshot_levels(pd, run_dir, event_id)
    updates: list[MarketUpdate] = []
    frame_kinds = set(frame_index["frame_kind"].unique())

    if "orderbook_snapshot" in frame_kinds:
        snapshots = pd.read_parquet(
            _table_path(run_dir, "snapshots"),
            columns=["record_index", "recv_ts_ns", "sequence", "event_id", "market_id"],
        )
        for row in snapshots.loc[snapshots["event_id"] == event_id].itertuples(index=False):
            levels = snapshot_levels.get(int(row.record_index), {"bid": (), "ask": ()})
            updates.append(
                MarketSnapshot(
                    record_index=int(row.record_index),
                    recv_ts_ns=int(row.recv_ts_ns),
                    sequence=int(row.sequence),
                    event_id=int(row.event_id),
                    market_id=int(row.market_id),
                    bid_levels=levels["bid"],
                    ask_levels=levels["ask"],
                )
            )

    if "orderbook_delta" in frame_kinds:
        deltas = pd.read_parquet(
            _table_path(run_dir, "deltas"),
            columns=[
                "record_index",
                "recv_ts_ns",
                "sequence",
                "event_id",
                "market_id",
                "side",
                "price_ticks",
                "delta_qty_lots",
            ],
        )
        for row in deltas.loc[deltas["event_id"] == event_id].itertuples(index=False):
            updates.append(
                BookDelta(
                    record_index=int(row.record_index),
                    recv_ts_ns=int(row.recv_ts_ns),
                    sequence=int(row.sequence),
                    event_id=int(row.event_id),
                    market_id=int(row.market_id),
                    side="ask" if str(row.side) == "ask" else "bid",
                    price_ticks=int(row.price_ticks),
                    delta_qty_lots=int(row.delta_qty_lots),
                )
            )

    if include_trades and "trade" in frame_kinds:
        trades = pd.read_parquet(
            _table_path(run_dir, "trades"),
            columns=[
                "record_index",
                "recv_ts_ns",
                "sequence",
                "event_id",
                "market_id",
                "yes_price_ticks",
                "no_price_ticks",
                "qty_lots",
                "aggressor",
            ],
        )
        for row in trades.loc[trades["event_id"] == event_id].itertuples(index=False):
            updates.append(
                PublicTrade(
                    record_index=int(row.record_index),
                    recv_ts_ns=int(row.recv_ts_ns),
                    sequence=int(row.sequence),
                    event_id=int(row.event_id),
                    market_id=int(row.market_id),
                    yes_price_ticks=_optional_int(row.yes_price_ticks),
                    no_price_ticks=_optional_int(row.no_price_ticks),
                    qty_lots=_optional_int(row.qty_lots),
                    aggressor=str(row.aggressor),
                )
            )

    if include_lifecycle and "lifecycle" in frame_kinds:
        lifecycles = pd.read_parquet(
            _table_path(run_dir, "lifecycles"),
            columns=["record_index", "recv_ts_ns", "sequence", "event_id", "market_id", "msg_json"],
        )
        for row in lifecycles.loc[lifecycles["event_id"] == event_id].itertuples(index=False):
            updates.append(
                LifecycleEvent(
                    record_index=int(row.record_index),
                    recv_ts_ns=int(row.recv_ts_ns),
                    sequence=int(row.sequence),
                    event_id=int(row.event_id),
                    market_id=int(row.market_id),
                    msg_json=str(row.msg_json),
                )
            )

    yield from sorted(updates, key=lambda update: (update.record_index, update.recv_ts_ns))


def iter_updates_from_tables(
    run_dir: str | Path,
    *,
    topology_filter: Iterable[str] | None = ("monotonic_chain",),
    include_trades: bool = True,
    include_lifecycle: bool = False,
    batch_size: int = 65_536,
) -> Iterator[MarketUpdate]:
    routes_by_event = load_routes_from_tables(run_dir, topology_filter=topology_filter)
    event_ids = frozenset(routes_by_event)
    if not event_ids:
        return

    pd = _require_pandas()
    snapshot_levels = _load_snapshot_levels_for_events(pd, run_dir, event_ids)
    streams: list[Iterator[MarketUpdate]] = [
        _iter_snapshot_updates(run_dir, event_ids, snapshot_levels, batch_size=batch_size),
        _iter_delta_updates(run_dir, event_ids, batch_size=batch_size),
    ]
    if include_trades:
        streams.append(_iter_trade_updates(run_dir, event_ids, batch_size=batch_size))
    if include_lifecycle:
        streams.append(_iter_lifecycle_updates(run_dir, event_ids, batch_size=batch_size))

    yield from _merge_sorted_update_streams(streams)


def _load_snapshot_levels(pd, run_dir: str | Path, event_id: int) -> dict[int, dict[str, tuple[BookLevel, ...]]]:
    levels = pd.read_parquet(
        _table_path(run_dir, "snapshot_levels"),
        columns=["record_index", "event_id", "side", "price_ticks", "qty_lots"],
    )
    by_record: dict[int, dict[str, list[BookLevel]]] = {}
    for row in levels.loc[levels["event_id"] == event_id].itertuples(index=False):
        record_levels = by_record.setdefault(int(row.record_index), {"bid": [], "ask": []})
        side = "ask" if str(row.side) == "ask" else "bid"
        record_levels[side].append(
            BookLevel(
                price_ticks=int(row.price_ticks),
                qty_lots=int(row.qty_lots),
            )
        )
    return {
        record_index: {
            "bid": tuple(sorted(sides["bid"], key=lambda level: level.price_ticks, reverse=True)),
            "ask": tuple(sorted(sides["ask"], key=lambda level: level.price_ticks)),
        }
        for record_index, sides in by_record.items()
    }


def _optional_int(value) -> int | None:
    if value is None:
        return None
    try:
        if value != value:
            return None
    except TypeError:
        pass
    return int(value)


def _route_from_row(row) -> ResearchMarketRoute:
    return ResearchMarketRoute(
        event_id=int(row.event_id),
        market_id=int(row.market_id),
        market_ticker=str(row.market_ticker),
        event_market_index=int(row.event_market_index),
        topology=str(row.topology),
        price_level_structure=str(row.price_level_structure),
        tradeable=bool(row.tradeable),
    )


def _load_snapshot_levels_for_events(
    pd,
    run_dir: str | Path,
    event_ids: frozenset[int],
) -> dict[int, dict[str, tuple[BookLevel, ...]]]:
    levels = pd.read_parquet(
        _table_path(run_dir, "snapshot_levels"),
        columns=["record_index", "event_id", "side", "price_ticks", "qty_lots"],
    )
    levels = levels.loc[levels["event_id"].isin(event_ids)]
    by_record: dict[int, dict[str, list[BookLevel]]] = {}
    for row in levels.itertuples(index=False):
        record_levels = by_record.setdefault(int(row.record_index), {"bid": [], "ask": []})
        side = "ask" if str(row.side) == "ask" else "bid"
        record_levels[side].append(
            BookLevel(
                price_ticks=int(row.price_ticks),
                qty_lots=int(row.qty_lots),
            )
        )
    return {
        record_index: {
            "bid": tuple(sorted(sides["bid"], key=lambda level: level.price_ticks, reverse=True)),
            "ask": tuple(sorted(sides["ask"], key=lambda level: level.price_ticks)),
        }
        for record_index, sides in by_record.items()
    }


def _iter_snapshot_updates(
    run_dir: str | Path,
    event_ids: frozenset[int],
    snapshot_levels: dict[int, dict[str, tuple[BookLevel, ...]]],
    *,
    batch_size: int,
) -> Iterator[MarketUpdate]:
    columns = ["record_index", "recv_ts_ns", "sequence", "event_id", "market_id"]

    def build(row: dict[str, object]) -> MarketSnapshot:
        levels = snapshot_levels.get(int(row["record_index"]), {"bid": (), "ask": ()})
        return MarketSnapshot(
            record_index=int(row["record_index"]),
            recv_ts_ns=int(row["recv_ts_ns"]),
            sequence=int(row["sequence"]),
            event_id=int(row["event_id"]),
            market_id=int(row["market_id"]),
            bid_levels=levels["bid"],
            ask_levels=levels["ask"],
        )

    yield from _iter_table_updates(
        _table_path(run_dir, "snapshots"),
        columns,
        event_ids,
        build,
        batch_size=batch_size,
    )


def _iter_delta_updates(
    run_dir: str | Path,
    event_ids: frozenset[int],
    *,
    batch_size: int,
) -> Iterator[MarketUpdate]:
    columns = [
        "record_index",
        "recv_ts_ns",
        "sequence",
        "event_id",
        "market_id",
        "side",
        "price_ticks",
        "delta_qty_lots",
    ]

    def build(row: dict[str, object]) -> BookDelta:
        return BookDelta(
            record_index=int(row["record_index"]),
            recv_ts_ns=int(row["recv_ts_ns"]),
            sequence=int(row["sequence"]),
            event_id=int(row["event_id"]),
            market_id=int(row["market_id"]),
            side="ask" if str(row["side"]) == "ask" else "bid",
            price_ticks=int(row["price_ticks"]),
            delta_qty_lots=int(row["delta_qty_lots"]),
        )

    yield from _iter_table_updates(
        _table_path(run_dir, "deltas"),
        columns,
        event_ids,
        build,
        batch_size=batch_size,
    )


def _iter_trade_updates(
    run_dir: str | Path,
    event_ids: frozenset[int],
    *,
    batch_size: int,
) -> Iterator[MarketUpdate]:
    columns = [
        "record_index",
        "recv_ts_ns",
        "sequence",
        "event_id",
        "market_id",
        "yes_price_ticks",
        "no_price_ticks",
        "qty_lots",
        "aggressor",
    ]

    def build(row: dict[str, object]) -> PublicTrade:
        return PublicTrade(
            record_index=int(row["record_index"]),
            recv_ts_ns=int(row["recv_ts_ns"]),
            sequence=int(row["sequence"]),
            event_id=int(row["event_id"]),
            market_id=int(row["market_id"]),
            yes_price_ticks=_optional_int(row["yes_price_ticks"]),
            no_price_ticks=_optional_int(row["no_price_ticks"]),
            qty_lots=_optional_int(row["qty_lots"]),
            aggressor=str(row["aggressor"]),
        )

    yield from _iter_table_updates(
        _table_path(run_dir, "trades"),
        columns,
        event_ids,
        build,
        batch_size=batch_size,
    )


def _iter_lifecycle_updates(
    run_dir: str | Path,
    event_ids: frozenset[int],
    *,
    batch_size: int,
) -> Iterator[MarketUpdate]:
    columns = ["record_index", "recv_ts_ns", "sequence", "event_id", "market_id", "msg_json"]

    def build(row: dict[str, object]) -> LifecycleEvent:
        return LifecycleEvent(
            record_index=int(row["record_index"]),
            recv_ts_ns=int(row["recv_ts_ns"]),
            sequence=int(row["sequence"]),
            event_id=int(row["event_id"]),
            market_id=int(row["market_id"]),
            msg_json=str(row["msg_json"]),
        )

    yield from _iter_table_updates(
        _table_path(run_dir, "lifecycles"),
        columns,
        event_ids,
        build,
        batch_size=batch_size,
    )


def _iter_table_updates(
    path: Path,
    columns: Sequence[str],
    event_ids: frozenset[int],
    build: Callable[[dict[str, object]], MarketUpdate],
    *,
    batch_size: int,
) -> Iterator[MarketUpdate]:
    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError:
        pd = _require_pandas()
        frame = pd.read_parquet(path, columns=list(columns))
        for row in frame.loc[frame["event_id"].isin(event_ids)].itertuples(index=False):
            yield build(row._asdict())
        return

    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=list(columns)):
        for row in batch.to_pylist():
            if int(row["event_id"]) in event_ids:
                yield build(row)


def _merge_sorted_update_streams(streams: Iterable[Iterator[MarketUpdate]]) -> Iterator[MarketUpdate]:
    heap: list[tuple[int, int, int, MarketUpdate, Iterator[MarketUpdate]]] = []
    for stream_index, stream in enumerate(streams):
        try:
            update = next(stream)
        except StopIteration:
            continue
        heapq.heappush(heap, (update.record_index, update.recv_ts_ns, stream_index, update, stream))

    while heap:
        _record_index, _recv_ts_ns, stream_index, update, stream = heapq.heappop(heap)
        yield update
        try:
            next_update = next(stream)
        except StopIteration:
            continue
        heapq.heappush(
            heap,
            (next_update.record_index, next_update.recv_ts_ns, stream_index, next_update, stream),
        )
