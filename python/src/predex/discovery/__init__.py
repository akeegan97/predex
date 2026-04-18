"""Discovery and config synthesis helpers for Predex."""

from .affinity import stable_affinity_key, stable_event_id, stable_market_id
from .classifier import ClassifiedEvent, classify_event
from .config import TraderConfigBuildResult, build_trader_config, build_trader_config_result
from .kalshi import KalshiPublicClient
from .models import EventRecord, MarketRecord, TopologyKind

__all__ = [
    "ClassifiedEvent",
    "EventRecord",
    "KalshiPublicClient",
    "MarketRecord",
    "TopologyKind",
    "TraderConfigBuildResult",
    "build_trader_config",
    "build_trader_config_result",
    "classify_event",
    "stable_affinity_key",
    "stable_event_id",
    "stable_market_id",
]
