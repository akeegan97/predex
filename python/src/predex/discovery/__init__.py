"""Discovery and config synthesis helpers for Predex."""

from .affinity import stable_affinity_key, stable_event_id, stable_market_id
from .app_config import (
    AppConfigBuildResult,
    KalshiMarketDataSettings,
    KalshiSettings,
    RuntimeSettings,
    ThreadPollingSettings,
    build_app_config,
    build_app_config_result,
)
from .classifier import ClassifiedEvent, classify_event
from .config import TraderConfigBuildResult, build_trader_config, build_trader_config_result
from .kalshi import KalshiPublicClient
from .models import EventRecord, MarketRecord, TopologyKind

__all__ = [
    "AppConfigBuildResult",
    "ClassifiedEvent",
    "EventRecord",
    "KalshiPublicClient",
    "KalshiMarketDataSettings",
    "KalshiSettings",
    "MarketRecord",
    "RuntimeSettings",
    "ThreadPollingSettings",
    "TopologyKind",
    "build_app_config",
    "build_app_config_result",
    "TraderConfigBuildResult",
    "build_trader_config",
    "build_trader_config_result",
    "classify_event",
    "stable_affinity_key",
    "stable_event_id",
    "stable_market_id",
]
