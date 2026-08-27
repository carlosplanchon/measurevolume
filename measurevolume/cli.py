"""Command-line interface: ``measurevolume analyze <data> [options]``."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal

from .analyzer import AnalysisConfig, MarketAnalyzer, SpecSummary
from .depletion import NAIVE, WINDOW, BandSpec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="measurevolume",
        description=(
            "Estimate market-taker activity from order-book snapshots by "
            "measuring visible depth depletion."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser(
        "analyze",
        help="analyze a snapshot log (.csv, .csv.xz or .csv.tar.xz)",
        description=(
            "Always computes the 'naive' view (historical semantics: every "
            "vanished level counts) and the 'window' view (scroll-out "
            "corrected), plus one banded view per --bands entry."
        ),
    )
    p.add_argument("data", help="path to the snapshot log")
    p.add_argument("--exchanges", nargs="+", default=["EX1", "EX2"], metavar="EX")
    p.add_argument(
        "--bands",
        nargs="*",
        type=int,
        default=[10, 25],
        metavar="BPS",
        help="price bands around mid, in bps (default: 10 25)",
    )
    p.add_argument("--bucket", type=float, default=60.0, help="bucket width in seconds")
    p.add_argument(
        "--gap-threshold",
        type=float,
        default=15.0,
        help="intervals longer than this many seconds are flagged stale",
    )
    p.add_argument(
        "--near-touch",
        type=int,
        default=5,
        metavar="BPS",
        help="near-touch tier width in bps from the previous best",
    )
    p.add_argument(
        "--top-n-control",
        type=int,
        default=None,
        metavar="N",
        help="also analyze every book truncated to its top N levels",
    )
    p.add_argument("--limit", type=int, default=None, help="stop after N snapshots")
    p.add_argument("--out", default=None, metavar="DIR", help="write figures to DIR")
    p.add_argument("--quiet", action="store_true", help="no progress output")
    return parser


def _fmt(value: Decimal | None) -> str:
    return "-" if value is None else f"{value:,.0f}"


def _print_group(summaries: list[SpecSummary]) -> None:
    first = summaries[0]
    print(f"=== {first.exchange} · {first.profile} ===")
    header = (
        f"  {'spec':<8} {'crossed':>16} {'near_touch':>16} {'deep':>16} "
        f"{'total':>16} {'BRL/h clean':>16} {'stale BRL':>14}  censored"
    )
    print(header)
    for s in summaries:
        clean = s.clean
        if s.spec.band_bps is None:
            censored = "-"
        else:
            n_clean = s.n_intervals - s.n_stale
            pct = s.n_censored / n_clean if n_clean else 0.0
            censored = f"{s.n_censored} iv ({pct:.1%})"
        print(
            f"  {s.spec.name:<8} {_fmt(clean.crossed):>16} {_fmt(clean.near_touch):>16} "
            f"{_fmt(clean.deep):>16} {_fmt(clean.total):>16} {_fmt(s.hourly_rate):>16} "
            f"{_fmt(s.stale_total):>14}  {censored}"
        )
    stale_pct = first.n_stale / first.n_intervals if first.n_intervals else 0.0
    print(
        f"  intervals: {first.n_intervals:,} · stale: {first.n_stale} ({stale_pct:.2%}) "
        f"· covered: {first.covered_seconds / 3600:.2f} h"
    )
    print()


def print_summary(analyzer: MarketAnalyzer) -> None:
    summaries = analyzer.summaries()
    groups: dict[tuple[str, str], list[SpecSummary]] = {}
    for s in summaries:
        groups.setdefault((s.exchange, s.profile), []).append(s)
    for key in sorted(groups):
        _print_group(groups[key])


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    bands = sorted(set(args.bands))
    specs = (NAIVE, WINDOW, *(BandSpec(f"band{b}", b) for b in bands))
    config = AnalysisConfig(
        specs=specs,
        gap_threshold=args.gap_threshold,
        near_touch_bps=args.near_touch,
        bucket_seconds=args.bucket,
        top_n_control=args.top_n_control,
    )
    analyzer = MarketAnalyzer(config)

    def progress(n: int, fraction: float) -> None:
        print(f"\rAnalyzing: {fraction:6.1%} ({n:,} snapshots)", end="", file=sys.stderr, flush=True)

    analyzer.analyze(
        args.data,
        args.exchanges,
        limit=args.limit,
        progress=None if args.quiet else progress,
    )
    if not args.quiet:
        print(file=sys.stderr)

    print_summary(analyzer)

    if args.out:
        from .plotting import generate_all

        for name, path in generate_all(analyzer, args.out).items():
            print(f"figure ({name}): {path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
