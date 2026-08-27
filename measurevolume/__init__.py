"""measurevolume: market-taker activity estimated from order-book snapshot diffs.

The headline number is *visible depth depletion*: liquidity that was resting
in the observable window of the book and is gone in the next snapshot. That
is an estimator, not confirmed executed volume; see `depletion` for the
assumptions and the confidence tiers.
"""

from .analyzer import (
    FULL_PROFILE,
    AnalysisConfig,
    Bucket,
    Interval,
    MarketAnalyzer,
    SpecSummary,
    VolumeTracker,
    bucket_series,
    summarize,
)
from .depletion import (
    DEFAULT_SPECS,
    NAIVE,
    WINDOW,
    BandSpec,
    DepletionBreakdown,
    estimate_depletion,
    estimate_depletion_multi,
)
from .models import OrderBookSide, OrderBookSnapshot
from .reader import OrderBookReader

__version__ = "1.0.0"

__all__ = [
    "DEFAULT_SPECS",
    "FULL_PROFILE",
    "NAIVE",
    "WINDOW",
    "AnalysisConfig",
    "BandSpec",
    "Bucket",
    "DepletionBreakdown",
    "Interval",
    "MarketAnalyzer",
    "OrderBookReader",
    "OrderBookSide",
    "OrderBookSnapshot",
    "SpecSummary",
    "VolumeTracker",
    "__version__",
    "bucket_series",
    "estimate_depletion",
    "estimate_depletion_multi",
    "summarize",
]
