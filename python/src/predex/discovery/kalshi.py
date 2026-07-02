from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from dataclasses import dataclass
from typing import Callable
from typing import Any
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import EventRecord


@dataclass(slots=True)
class KalshiPublicClient:
    base_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    user_agent: str = "predex-discovery/0.1"
    timeout_seconds: float = 10.0
    max_retries: int = 4
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 8.0
    event_fetch_workers: int = 8
    progress_callback: Callable[[str], None] | None = None

    def _urlopen(self, request: Request):
        return urlopen(request, timeout=self.timeout_seconds)

    def _sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def _report_progress(self, message: str) -> None:
        if self.progress_callback is not None:
            self.progress_callback(message)

    def _retry_delay(self, attempt: int, error: HTTPError | None = None) -> float:
        if error is not None:
            headers = error.headers or {}
            retry_after = str(headers.get("Retry-After", "")).strip()
            if retry_after:
                try:
                    return max(0.0, float(retry_after))
                except ValueError:
                    pass

        delay = self.initial_backoff_seconds * (2 ** attempt)
        return min(delay, self.max_backoff_seconds)

    def _open_json_request(self, request: Request) -> dict[str, Any]:
        attempt = 0
        while True:
            try:
                with self._urlopen(request) as response:
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as error:
                retriable = error.code in (408, 429, 500, 502, 503, 504)
                if not retriable or attempt >= self.max_retries:
                    raise
                delay = self._retry_delay(attempt, error)
                self._report_progress(
                    f"http {error.code} on {request.full_url}; retrying in {delay:.1f}s "
                    f"({attempt + 1}/{self.max_retries})"
                )
                self._sleep(delay)
                attempt += 1
            except URLError as error:
                if attempt >= self.max_retries:
                    raise
                delay = self._retry_delay(attempt)
                self._report_progress(
                    f"network error on {request.full_url}: {error.reason}; "
                    f"retrying in {delay:.1f}s ({attempt + 1}/{self.max_retries})"
                )
                self._sleep(delay)
                attempt += 1

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = urlencode(
            {
                key: value
                for key, value in (params or {}).items()
                if value is not None and value != ""
            },
            doseq=True,
        )
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        request = Request(url, headers={"User-Agent": self.user_agent})
        return self._open_json_request(request)

    def get_event(self, event_ticker: str) -> EventRecord:
        payload = self._get_json(f"/events/{event_ticker}", {"with_nested_markets": "true"})
        event_payload = payload.get("event") or {}
        if not event_payload:
            raise ValueError(f"Kalshi response for {event_ticker} did not contain an event payload")

        if not event_payload.get("markets") and payload.get("markets"):
            event_payload = dict(event_payload)
            event_payload["markets"] = payload["markets"]
        return EventRecord.from_api(event_payload)

    def list_event_tickers(
        self,
        *,
        series_ticker: str | None = None,
        status: str | None = "open",
        limit: int | None = 200,
    ) -> list[str]:
        tickers: list[str] = []
        cursor = ""
        remaining = limit

        if remaining is not None and remaining <= 0:
            raise ValueError("limit must be greater than zero when provided")

        while remaining is None or remaining > 0:
            page_size = 200 if remaining is None else min(remaining, 200)
            payload = self._get_json(
                "/events",
                {
                    "series_ticker": series_ticker,
                    "status": status,
                    "limit": page_size,
                    "cursor": cursor or None,
                },
            )
            page_events = payload.get("events") or []
            if not page_events:
                break

            self._report_progress(
                f"discovered {len(page_events)} event tickers"
                f"{' (paginated)' if cursor else ''}"
            )

            for event_payload in page_events:
                ticker = str(event_payload.get("event_ticker", ""))
                if ticker:
                    tickers.append(ticker)
                    if remaining is not None:
                        remaining -= 1
                    if remaining == 0:
                        break

            cursor = str(payload.get("cursor", ""))
            if not cursor:
                break

        return tickers

    def discover_events(
        self,
        *,
        event_tickers: list[str] | None = None,
        series_ticker: str | None = None,
        status: str | None = "open",
        limit: int | None = 200,
    ) -> list[EventRecord]:
        tickers = event_tickers or self.list_event_tickers(
            series_ticker=series_ticker,
            status=status,
            limit=limit,
        )
        ordered_unique_tickers = list(dict.fromkeys(tickers))
        events: list[EventRecord] = []
        total = len(ordered_unique_tickers)

        def fetch_one(index: int, event_ticker: str) -> tuple[int, EventRecord | None]:
            self._report_progress(f"fetching event {index}/{total}: {event_ticker}")
            try:
                return index, self.get_event(event_ticker)
            except (HTTPError, URLError, TimeoutError, ValueError) as error:
                self._report_progress(
                    f"skipping event {event_ticker} after repeated fetch failure: {error}"
                )
                return index, None

        max_workers = max(1, self.event_fetch_workers)
        if max_workers == 1 or total <= 1:
            for index, event_ticker in enumerate(ordered_unique_tickers, start=1):
                _, event = fetch_one(index, event_ticker)
                if event is not None:
                    events.append(event)
            return events

        events_by_index: list[EventRecord | None] = [None] * total
        with ThreadPoolExecutor(
            max_workers=min(max_workers, total),
            thread_name_prefix="kalshi-event-fetch",
        ) as executor:
            futures = [
                executor.submit(fetch_one, index, event_ticker)
                for index, event_ticker in enumerate(ordered_unique_tickers, start=1)
            ]
            for future in as_completed(futures):
                index, event = future.result()
                events_by_index[index - 1] = event

        return [event for event in events_by_index if event is not None]
