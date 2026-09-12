from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Iterable

from predex.discovery.app_config import _event_domain, _event_time_fields, _parse_time_s
from predex.discovery.kalshi import DEFAULT_KALSHI_API_BASE_URL, KalshiPublicClient
from predex.discovery.models import EventRecord

from .app_config import load_app_config_index
from .materialize import _write_route_tables


@dataclass(frozen=True, slots=True)
class MetadataEnrichmentResult:
    run_dir: Path
    metadata_path: Path
    event_count: int
    fetched_event_count: int
    missing_event_count: int
    route_rows: dict[str, int] | None


def _load_json_if_exists(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _event_tickers_from_run(run_dir: Path) -> tuple[str, ...]:
    tickers: list[str] = []

    config = _load_json_if_exists(run_dir / "config.json")
    for event in (config.get("universe") or {}).get("events") or []:
        event_ticker = str(event.get("event_ticker", ""))
        if event_ticker:
            tickers.append(event_ticker)

    report = _load_json_if_exists(run_dir / "report.json")
    for event in report.get("included_events") or []:
        event_ticker = str(event.get("event_ticker", ""))
        if event_ticker:
            tickers.append(event_ticker)

    return tuple(dict.fromkeys(tickers))


def _event_metadata(event: EventRecord) -> dict:
    market_metadata = {}
    for market in event.markets:
        market_metadata[market.ticker] = {
            "market_time_s": _parse_time_s(market.primary_time_reference()),
            "market_close_time_s": _parse_time_s(market.close_time),
            "market_expected_expiration_time_s": _parse_time_s(market.expected_expiration_time),
            "market_expiration_time_s": _parse_time_s(market.expiration_time),
            "market_title": market.title,
            "market_subtitle": market.subtitle,
            "yes_sub_title": market.yes_sub_title,
            "no_sub_title": market.no_sub_title,
            "status": market.status,
            "price_level_structure": market.price_level_structure,
        }

    return {
        "event_ticker": event.event_ticker,
        "series_ticker": event.series_ticker,
        "event_title": event.title,
        "event_sub_title": event.sub_title,
        "event_category": event.category,
        "event_domain": _event_domain(event),
        **_event_time_fields(event.markets),
        "mutually_exclusive": event.mutually_exclusive,
        "markets": market_metadata,
    }


def _load_existing_overlay(path: Path) -> dict[str, dict]:
    payload = _load_json_if_exists(path)
    events = payload.get("events") or {}
    if isinstance(events, list):
        return {
            str(event.get("event_ticker", "")): dict(event)
            for event in events
            if event.get("event_ticker")
        }
    if isinstance(events, dict):
        return {
            str(event_ticker): dict(metadata)
            for event_ticker, metadata in events.items()
            if isinstance(metadata, dict)
        }
    return {}


def _write_overlay(path: Path, events: dict[str, dict], *, source: str) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "source": source,
                "created_at_utc": datetime.now(UTC).isoformat(),
                "event_count": len(events),
                "events": dict(sorted(events.items())),
            },
            indent=2,
            sort_keys=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _progress(message: str) -> None:
    sys.stderr.write(f"predex-replay: {message}\n")
    sys.stderr.flush()


def _run_dirs(run_dirs: Iterable[str | Path] | None, runs_root: str | Path | None) -> tuple[Path, ...]:
    discovered: list[Path] = []
    if run_dirs is not None:
        discovered.extend(Path(run_dir) for run_dir in run_dirs)
    if runs_root is not None:
        root = Path(runs_root)
        discovered.extend(
            run_dir
            for run_dir in sorted(root.iterdir())
            if run_dir.is_dir() and (run_dir / "config.json").exists()
        )
    return tuple(dict.fromkeys(discovered))


def enrich_run_metadata(
    *,
    run_dirs: Iterable[str | Path] | None = None,
    runs_root: str | Path | None = None,
    api_base_url: str = DEFAULT_KALSHI_API_BASE_URL,
    event_fetch_workers: int = 8,
    rewrite_route_tables: bool = True,
    progress_callback: Callable[[str], None] | None = _progress,
) -> tuple[MetadataEnrichmentResult, ...]:
    selected_runs = _run_dirs(run_dirs, runs_root)
    if not selected_runs:
        raise ValueError("provide at least one --run-dir or --runs-root")

    tickers_by_run = {run_dir: _event_tickers_from_run(run_dir) for run_dir in selected_runs}
    all_tickers = tuple(dict.fromkeys(ticker for tickers in tickers_by_run.values() for ticker in tickers))
    if not all_tickers:
        raise ValueError("no event tickers found in selected runs")

    client = KalshiPublicClient(
        base_url=api_base_url,
        event_fetch_workers=event_fetch_workers,
        progress_callback=progress_callback,
    )
    fetched_events = client.discover_events(event_tickers=list(all_tickers), status=None, limit=None)
    fetched_metadata = {event.event_ticker: _event_metadata(event) for event in fetched_events}

    results: list[MetadataEnrichmentResult] = []
    for run_dir, tickers in tickers_by_run.items():
        metadata_path = run_dir / "event_metadata.json"
        events = _load_existing_overlay(metadata_path)
        fetched_for_run = 0
        for ticker in tickers:
            metadata = fetched_metadata.get(ticker)
            if metadata is not None:
                events[ticker] = metadata
                fetched_for_run += 1
        _write_overlay(metadata_path, events, source="kalshi_public_api")

        route_rows = None
        if rewrite_route_tables:
            index = load_app_config_index(run_dir / "config.json", metadata_path=metadata_path)
            route_rows = _write_route_tables(index, run_dir / "tables", compression="zstd")

        results.append(
            MetadataEnrichmentResult(
                run_dir=run_dir,
                metadata_path=metadata_path,
                event_count=len(tickers),
                fetched_event_count=fetched_for_run,
                missing_event_count=len(tickers) - fetched_for_run,
                route_rows=route_rows,
            )
        )

    return tuple(results)
