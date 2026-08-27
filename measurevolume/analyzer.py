"""Per-exchange tracking, interval metrics, and time-bucket aggregation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from statistics import median

from .depletion import (
    DEFAULT_SPECS,
    BandSpec,
    DepletionBreakdown,
    estimate_depletion_multi,
)
from .models import OrderBookSnapshot
from .reader import OrderBookReader

_ZERO = Decimal(0)
_TEN_THOUSAND = Decimal(10_000)

FULL_PROFILE = "full"


@dataclass(frozen=True)
class AnalysisConfig:
    specs: tuple[BandSpec, ...] = DEFAULT_SPECS
    #: Interval gaps above this many seconds are flagged ``stale`` and kept
    #: out of clean aggregates (the bundled dataset's median cadence is ~4 s).
    gap_threshold: float = 15.0
    near_touch_bps: int = 5
    bucket_seconds: float = 60.0
    #: If set (e.g. 20), also track every exchange with its book truncated to
    #: the top N levels per side: the equal-window control experiment.
    top_n_control: int | None = None

    @property
    def bands(self) -> tuple[int | None, ...]:
        """Distinct band widths referenced by the specs (None = full window)."""
        seen: list[int | None] = []
        for spec in self.specs:
            if spec.band_bps not in seen:
                seen.append(spec.band_bps)
        return tuple(seen)

    def profile_names(self) -> tuple[str, ...]:
        names = [FULL_PROFILE]
        if self.top_n_control:
            names.append(f"top{self.top_n_control}")
        return tuple(names)


@dataclass
class Interval:
    """Metrics for one pair of consecutive snapshots of one exchange."""

    t: float  # timestamp of the newer snapshot
    dt: float
    stale: bool
    bids: dict[str, DepletionBreakdown]  # spec name -> breakdown
    asks: dict[str, DepletionBreakdown]
    depth: dict[int | None, Decimal]  # band -> resting value, both sides
    censored: dict[int | None, bool]  # band wider than the observable window?

    def total(self, spec_name: str) -> Decimal:
        return self.bids[spec_name].total + self.asks[spec_name].total


class VolumeTracker:
    """Accumulates `Interval` metrics for a single exchange (and profile)."""

    def __init__(self, exchange: str, config: AnalysisConfig):
        self.exchange = exchange
        self.config = config
        self.intervals: list[Interval] = []
        self._previous: OrderBookSnapshot | None = None
        self._max_depth = {"bids": 0, "asks": 0}

    def process_snapshot(self, snap: OrderBookSnapshot) -> None:
        cfg = self.config
        self._max_depth["bids"] = max(self._max_depth["bids"], len(snap.bids))
        self._max_depth["asks"] = max(self._max_depth["asks"], len(snap.asks))
        prev, self._previous = self._previous, snap
        if prev is None:
            return
        mid = prev.mid
        if mid is None:  # one-sided previous book: nothing to reference against
            return
        dt = snap.timestamp - prev.timestamp
        bids = estimate_depletion_multi(
            prev.bids,
            snap.bids,
            specs=cfg.specs,
            mid=mid,
            full_window=len(snap.bids) >= self._max_depth["bids"],
            near_touch_bps=cfg.near_touch_bps,
        )
        asks = estimate_depletion_multi(
            prev.asks,
            snap.asks,
            specs=cfg.specs,
            mid=mid,
            full_window=len(snap.asks) >= self._max_depth["asks"],
            near_touch_bps=cfg.near_touch_bps,
        )
        depth: dict[int | None, Decimal] = {}
        censored: dict[int | None, bool] = {}
        snap_mid = snap.mid
        for band in cfg.bands:
            if band is None:
                depth[band] = snap.bids.depth() + snap.asks.depth()
                censored[band] = False
            elif snap_mid is None:
                depth[band] = _ZERO
                censored[band] = True
            else:
                depth[band] = snap.bids.depth(mid=snap_mid, band_bps=band) + snap.asks.depth(
                    mid=snap_mid, band_bps=band
                )
                censored[band] = _is_censored(prev, snap, mid, band)
        self.intervals.append(
            Interval(
                t=snap.timestamp,
                dt=dt,
                stale=dt > cfg.gap_threshold,
                bids=bids,
                asks=asks,
                depth=depth,
                censored=censored,
            )
        )


def _is_censored(
    prev: OrderBookSnapshot, snap: OrderBookSnapshot, mid: Decimal, band_bps: int
) -> bool:
    """True when the band (centered on the previous mid) extends beyond what
    either snapshot's window could observe on either side."""
    delta = mid * Decimal(band_bps) / _TEN_THOUSAND
    lo, hi = mid - delta, mid + delta
    for side in (prev.bids, snap.bids):
        far = side.far
        if far is None or far > lo:
            return True
    for side in (prev.asks, snap.asks):
        far = side.far
        if far is None or far < hi:
            return True
    return False


@dataclass(frozen=True)
class SpecSummary:
    """Whole-run totals for one (exchange, profile, spec)."""

    exchange: str
    profile: str
    spec: BandSpec
    clean_bids: DepletionBreakdown
    clean_asks: DepletionBreakdown
    stale_total: Decimal  # both sides, stale intervals only
    covered_seconds: float
    n_intervals: int
    n_stale: int
    n_censored: int  # clean intervals whose band was censored

    @property
    def clean(self) -> DepletionBreakdown:
        return self.clean_bids + self.clean_asks

    @property
    def hourly_rate(self) -> Decimal | None:
        """Clean depletion per hour of covered time (BRL/h)."""
        if self.covered_seconds <= 0:
            return None
        return self.clean.total / Decimal(str(self.covered_seconds)) * 3600


def summarize(
    tracker: VolumeTracker, spec: BandSpec, *, profile: str = FULL_PROFILE
) -> SpecSummary:
    clean_bids = DepletionBreakdown()
    clean_asks = DepletionBreakdown()
    stale_total = _ZERO
    covered = 0.0
    n_stale = 0
    n_censored = 0
    for iv in tracker.intervals:
        if iv.stale:
            n_stale += 1
            stale_total += iv.total(spec.name)
            continue
        clean_bids = clean_bids + iv.bids[spec.name]
        clean_asks = clean_asks + iv.asks[spec.name]
        covered += iv.dt
        if iv.censored[spec.band_bps]:
            n_censored += 1
    return SpecSummary(
        exchange=tracker.exchange,
        profile=profile,
        spec=spec,
        clean_bids=clean_bids,
        clean_asks=clean_asks,
        stale_total=stale_total,
        covered_seconds=covered,
        n_intervals=len(tracker.intervals),
        n_stale=n_stale,
        n_censored=n_censored,
    )


@dataclass
class Bucket:
    """Fixed-width time bucket of clean-interval depletion."""

    start: float
    end: float
    covered: float  # seconds of clean intervals inside
    depletion: DepletionBreakdown  # both sides, clean intervals only
    has_gap: bool
    censored: bool
    depth_median: Decimal | None

    @property
    def rate(self) -> Decimal | None:
        """BRL of depletion per covered second, or None when nothing is covered."""
        if self.covered <= 0:
            return None
        return self.depletion.total / Decimal(str(self.covered))

    @property
    def turnover(self) -> Decimal | None:
        """Depletion rate as a fraction of the visible in-band depth, per second."""
        r = self.rate
        if r is None or not self.depth_median:
            return None
        return r / self.depth_median


def bucket_series(
    tracker: VolumeTracker, spec: BandSpec, *, bucket_seconds: float | None = None
) -> list[Bucket]:
    """Aggregate a tracker's intervals into fixed-width time buckets."""
    width = bucket_seconds or tracker.config.bucket_seconds
    buckets: dict[int, dict] = {}
    for iv in tracker.intervals:
        key = int(iv.t // width)
        b = buckets.setdefault(
            key,
            {
                "covered": 0.0,
                "depletion": DepletionBreakdown(),
                "has_gap": False,
                "censored": False,
                "depths": [],
            },
        )
        if iv.stale:
            b["has_gap"] = True
            continue
        b["covered"] += iv.dt
        b["depletion"] = b["depletion"] + iv.bids[spec.name] + iv.asks[spec.name]
        b["censored"] = b["censored"] or iv.censored[spec.band_bps]
        b["depths"].append(iv.depth[spec.band_bps])
    return [
        Bucket(
            start=key * width,
            end=(key + 1) * width,
            covered=b["covered"],
            depletion=b["depletion"],
            has_gap=b["has_gap"],
            censored=b["censored"],
            depth_median=median(b["depths"]) if b["depths"] else None,
        )
        for key, b in sorted(buckets.items())
    ]


class MarketAnalyzer:
    """Coordinates reading snapshots and tracking every (exchange, profile)."""

    def __init__(self, config: AnalysisConfig | None = None):
        self.config = config or AnalysisConfig()
        self.trackers: dict[tuple[str, str], VolumeTracker] = {}

    def tracker(self, exchange: str, profile: str = FULL_PROFILE) -> VolumeTracker | None:
        return self.trackers.get((exchange, profile))

    def analyze(
        self,
        source: str | OrderBookReader | Iterable[OrderBookSnapshot],
        exchanges: Sequence[str],
        *,
        limit: int | None = None,
        progress: Callable[[int, float], None] | None = None,
    ) -> None:
        """Consume ``source`` and update one tracker per (exchange, profile).

        ``source`` may be a path (``.csv``/``.csv.xz``/``.csv.tar.xz``), an
        entered `OrderBookReader`, or any iterable of snapshots. ``limit``
        stops after that many snapshots (all exchanges combined).
        ``progress(n_snapshots, fraction)`` is called every 200 snapshots.
        """
        cfg = self.config
        control = cfg.top_n_control
        for ex in exchanges:
            self.trackers[(ex, FULL_PROFILE)] = VolumeTracker(ex, cfg)
            if control:
                self.trackers[(ex, f"top{control}")] = VolumeTracker(ex, cfg)

        def run(snapshots: Iterable[OrderBookSnapshot], fraction: Callable[[], float]) -> None:
            for i, snap in enumerate(snapshots, 1):
                if limit is not None and i > limit:
                    break
                full = self.trackers.get((snap.exchange, FULL_PROFILE))
                if full is not None:
                    full.process_snapshot(snap)
                    if control:
                        self.trackers[(snap.exchange, f"top{control}")].process_snapshot(
                            snap.truncated(control)
                        )
                if progress is not None and i % 200 == 0:
                    progress(i, fraction())

        if isinstance(source, str):
            with OrderBookReader(source) as reader:
                run(reader, lambda: reader.progress)
        elif isinstance(source, OrderBookReader):
            run(source, lambda: source.progress)
        else:
            run(source, lambda: 0.0)

    def summaries(self) -> list[SpecSummary]:
        out: list[SpecSummary] = []
        for (exchange, profile), tracker in sorted(self.trackers.items()):
            for spec in self.config.specs:
                out.append(summarize(tracker, spec, profile=profile))
        return out
