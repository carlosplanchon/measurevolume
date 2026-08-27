"""Figures for the README and ad-hoc exploration (requires matplotlib)."""

from __future__ import annotations

import math
import os

from .analyzer import FULL_PROFILE, MarketAnalyzer, VolumeTracker, bucket_series
from .depletion import BandSpec

#: Heritage palette from the 2020 figure (bids, asks) per exchange.
_NAIVE_COLORS = {"EX1": ("#00ff00", "#ff0000"), "EX2": ("#00ffff", "#ffff00")}
_EXCHANGE_COLORS = {"EX1": "#00e676", "EX2": "#00b0ff"}
_FALLBACK = ("#ff6d00", "#d500f9", "#ffee58", "#ff1744")


def _plt():
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "plotting requires matplotlib; install with: pip install 'measurevolume[plot]'"
        ) from exc
    plt.style.use("dark_background")
    return plt


def _color(exchange: str, i: int) -> str:
    return _EXCHANGE_COLORS.get(exchange, _FALLBACK[i % len(_FALLBACK)])


def _legend(ax, **kwargs) -> None:
    """Legend whose sample lines stay visible even for hairline plots."""
    for line in ax.legend(**kwargs).get_lines():
        line.set_linewidth(1.8)


def _t0(analyzer: MarketAnalyzer) -> float:
    starts = [tr.intervals[0].t for tr in analyzer.trackers.values() if tr.intervals]
    if not starts:
        raise ValueError("no intervals to plot; run analyze() first")
    return min(starts)


def _spec(analyzer: MarketAnalyzer, name: str) -> BandSpec:
    for spec in analyzer.config.specs:
        if spec.name == name:
            return spec
    raise KeyError(f"spec {name!r} is not part of this analysis")


def _full_trackers(analyzer: MarketAnalyzer) -> list[tuple[str, VolumeTracker]]:
    return sorted(
        (ex, tr)
        for (ex, profile), tr in analyzer.trackers.items()
        if profile == FULL_PROFILE and tr.intervals
    )


def _bucket_xy(
    tracker: VolumeTracker, spec: BandSpec, t0: float, width: float
) -> tuple[list[float], list[float], list[tuple[float, float]]]:
    """Bucket centers (hours), rates (BRL/s, NaN where uncovered), gap spans."""
    xs: list[float] = []
    ys: list[float] = []
    gaps: list[tuple[float, float]] = []
    for b in bucket_series(tracker, spec, bucket_seconds=width):
        xs.append((b.start + width / 2 - t0) / 3600)
        ys.append(float(b.rate) if b.rate is not None else math.nan)
        if b.has_gap:
            gaps.append(((b.start - t0) / 3600, (b.end - t0) / 3600))
    return xs, ys, gaps


def _shade_gaps(ax, gaps: list[tuple[float, float]]) -> None:
    seen = set()
    for lo, hi in gaps:
        if (lo, hi) in seen:
            continue
        seen.add((lo, hi))
        ax.axvspan(lo, hi, color="#555555", alpha=0.3, lw=0)


def plot_naive(analyzer: MarketAnalyzer, path: str) -> str:
    """Reproduction of the historical measurement: naive full-window depletion
    per interval. Deliberately biased: each exchange contributes whatever its
    feed exposes (EX1: top-20, ~±45 bps; EX2: top-1000, -30 %/+15 %)."""
    plt = _plt()
    spec = _spec(analyzer, "naive")
    t0 = _t0(analyzer)
    fig, ax = plt.subplots(figsize=(14, 6), dpi=130)
    for i, (exchange, tracker) in enumerate(_full_trackers(analyzer)):
        bids_c, asks_c = _NAIVE_COLORS.get(exchange, (_color(exchange, i), "#ffffff"))
        xs = [(iv.t - t0) / 3600 for iv in tracker.intervals]
        ax.plot(
            xs,
            [float(iv.bids[spec.name].total) for iv in tracker.intervals],
            lw=0.5,
            c=bids_c,
            label=f"{exchange} bids",
        )
        ax.plot(
            xs,
            [float(iv.asks[spec.name].total) for iv in tracker.intervals],
            lw=0.5,
            c=asks_c,
            label=f"{exchange} asks",
        )
    ax.set_xlabel("hours from start")
    ax.set_ylabel("BRL per snapshot interval")
    ax.set_title(
        "Naive full-window depletion (historical semantics), biased:\n"
        "EX2's visible window is 40-70× wider in price than EX1's"
    )
    ax.grid(c="#222222")
    _legend(ax)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_banded(analyzer: MarketAnalyzer, path: str) -> str:
    """The corrected comparison: depletion rate inside fixed price bands
    around the mid, aggregated in time buckets; logger gaps shaded."""
    plt = _plt()
    banded = sorted(
        (s for s in analyzer.config.specs if s.band_bps is not None),
        key=lambda s: s.band_bps,
    )
    if not banded:
        raise ValueError("no banded specs configured")
    t0 = _t0(analyzer)
    width = analyzer.config.bucket_seconds
    fig, axes = plt.subplots(
        len(banded), 1, figsize=(14, 4.2 * len(banded)), dpi=130, sharex=True
    )
    if len(banded) == 1:
        axes = [axes]
    for ax, spec in zip(axes, banded):
        all_gaps: list[tuple[float, float]] = []
        for i, (exchange, tracker) in enumerate(_full_trackers(analyzer)):
            xs, ys, gaps = _bucket_xy(tracker, spec, t0, width)
            all_gaps.extend(gaps)
            ax.plot(xs, ys, lw=0.9, c=_color(exchange, i), label=exchange)
        _shade_gaps(ax, all_gaps)
        ax.set_ylabel("BRL/s")
        ax.set_title(f"±{spec.band_bps} bps around mid: visible depth depletion rate")
        ax.grid(c="#222222")
        _legend(ax, loc="upper right")
    axes[-1].set_xlabel("hours from start")
    fig.suptitle("Window-normalized comparison (shaded spans: logger gaps)", y=0.995)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_top_control(analyzer: MarketAnalyzer, path: str) -> str:
    """Control experiment: every book truncated to the same top-N levels,
    with the far-boundary (scroll-out) correction applied."""
    plt = _plt()
    n = analyzer.config.top_n_control
    if not n:
        raise ValueError("config.top_n_control is not set")
    spec = _spec(analyzer, "window")
    profile = f"top{n}"
    t0 = _t0(analyzer)
    width = analyzer.config.bucket_seconds
    fig, ax = plt.subplots(figsize=(14, 5), dpi=130)
    trackers = sorted(
        (ex, tr)
        for (ex, prof), tr in analyzer.trackers.items()
        if prof == profile and tr.intervals
    )
    all_gaps: list[tuple[float, float]] = []
    for i, (exchange, tracker) in enumerate(trackers):
        xs, ys, gaps = _bucket_xy(tracker, spec, t0, width)
        all_gaps.extend(gaps)
        ax.plot(xs, ys, lw=0.9, c=_color(exchange, i), label=f"{exchange} (top {n})")
    _shade_gaps(ax, all_gaps)
    ax.set_xlabel("hours from start")
    ax.set_ylabel("BRL/s")
    ax.set_title(
        f"Equal count windows: both books truncated to their top {n} levels "
        "(scroll-out corrected)"
    )
    ax.grid(c="#222222")
    _legend(ax, loc="upper right")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_turnover(analyzer: MarketAnalyzer, path: str) -> str:
    """Relative activity: how many times the resting in-band depth is turned
    over per hour; compares venues of very different sizes fairly."""
    plt = _plt()
    banded = [s for s in analyzer.config.specs if s.band_bps is not None]
    if not banded:
        raise ValueError("no banded specs configured")
    spec = max(banded, key=lambda s: s.band_bps)
    t0 = _t0(analyzer)
    width = analyzer.config.bucket_seconds
    fig, ax = plt.subplots(figsize=(14, 5), dpi=130)
    all_gaps: list[tuple[float, float]] = []
    for i, (exchange, tracker) in enumerate(_full_trackers(analyzer)):
        xs: list[float] = []
        ys: list[float] = []
        for b in bucket_series(tracker, spec, bucket_seconds=width):
            xs.append((b.start + width / 2 - t0) / 3600)
            t = b.turnover
            ys.append(float(t) * 3600 if t is not None else math.nan)
            if b.has_gap:
                all_gaps.append(((b.start - t0) / 3600, (b.end - t0) / 3600))
        ax.plot(xs, ys, lw=0.9, c=_color(exchange, i), label=exchange)
    _shade_gaps(ax, all_gaps)
    ax.set_xlabel("hours from start")
    ax.set_ylabel("in-band resting depth turned over per hour (\u00d7)")
    ax.set_title(f"Relative turnover inside ±{spec.band_bps} bps of mid")
    ax.grid(c="#222222")
    _legend(ax, loc="upper right")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def generate_all(analyzer: MarketAnalyzer, out_dir: str) -> dict[str, str]:
    """Produce the standard figure set into ``out_dir``; returns name -> path."""
    os.makedirs(out_dir, exist_ok=True)
    spec_names = {s.name for s in analyzer.config.specs}
    out: dict[str, str] = {}
    if "naive" in spec_names:
        out["naive"] = plot_naive(analyzer, os.path.join(out_dir, "naive_full_window.png"))
    if any(s.band_bps is not None for s in analyzer.config.specs):
        out["banded"] = plot_banded(analyzer, os.path.join(out_dir, "banded_comparison.png"))
        out["turnover"] = plot_turnover(analyzer, os.path.join(out_dir, "turnover.png"))
    n = analyzer.config.top_n_control
    if n and "window" in spec_names:
        out["top_control"] = plot_top_control(
            analyzer, os.path.join(out_dir, f"top{n}_control.png")
        )
    return out
