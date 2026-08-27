from decimal import Decimal

from measurevolume.analyzer import (
    AnalysisConfig,
    MarketAnalyzer,
    VolumeTracker,
    bucket_series,
    summarize,
)
from measurevolume.depletion import WINDOW, BandSpec
from measurevolume.models import OrderBookSide, OrderBookSnapshot


def snap(t: float, ex: str, bids, asks) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        timestamp=t,
        exchange=ex,
        bids=OrderBookSide.from_raw("bids", bids),
        asks=OrderBookSide.from_raw("asks", asks),
    )


ASKS = [["101", "1"], ["102", "1"]]


def test_gap_flag_separates_clean_and_stale_totals():
    cfg = AnalysisConfig(specs=(WINDOW,), gap_threshold=15.0)
    tr = VolumeTracker("EX", cfg)
    tr.process_snapshot(snap(0.0, "EX", [["100", "1"], ["99", "1"]], ASKS))
    tr.process_snapshot(snap(4.0, "EX", [["100", "1"]], ASKS))  # 99 vanished
    tr.process_snapshot(snap(104.0, "EX", [["100", "0.5"]], ASKS))  # 100 s gap
    assert [iv.stale for iv in tr.intervals] == [False, True]
    s = summarize(tr, WINDOW)
    assert s.clean.total == Decimal(99)
    assert s.stale_total == Decimal(50)
    assert s.covered_seconds == 4.0
    assert s.n_intervals == 2 and s.n_stale == 1


def test_censored_flag_when_band_exceeds_window():
    spec = BandSpec("band25", 25)
    cfg = AnalysisConfig(specs=(spec,))

    narrow = VolumeTracker("EX", cfg)
    books = ([["100", "1"], ["99.9", "1"]], [["100.2", "1"], ["100.3", "1"]])
    narrow.process_snapshot(snap(0.0, "EX", *books))
    narrow.process_snapshot(snap(4.0, "EX", *books))
    assert narrow.intervals[0].censored[25] is True

    wide = VolumeTracker("EX", cfg)
    books = ([["100", "1"], ["99.0", "1"]], [["100.2", "1"], ["101.0", "1"]])
    wide.process_snapshot(snap(0.0, "EX", *books))
    wide.process_snapshot(snap(4.0, "EX", *books))
    assert wide.intervals[0].censored[25] is False


def test_bucket_rate_and_turnover():
    cfg = AnalysisConfig(specs=(WINDOW,))
    tr = VolumeTracker("EX", cfg)
    tr.process_snapshot(snap(0.0, "EX", [["100", "1"], ["99", "1"]], ASKS))
    tr.process_snapshot(snap(4.0, "EX", [["100", "1"]], ASKS))  # depletion 99
    tr.process_snapshot(snap(8.0, "EX", [["100", "0.5"]], ASKS))  # depletion 50
    buckets = bucket_series(tr, WINDOW, bucket_seconds=60)
    assert len(buckets) == 1
    b = buckets[0]
    assert b.covered == 8.0
    assert b.depletion.total == Decimal(149)
    assert b.rate == Decimal(149) / Decimal(8)
    assert b.has_gap is False
    assert b.turnover is not None and b.turnover > 0


def test_gap_bucket_is_flagged_but_not_counted():
    cfg = AnalysisConfig(specs=(WINDOW,), gap_threshold=15.0)
    tr = VolumeTracker("EX", cfg)
    tr.process_snapshot(snap(0.0, "EX", [["100", "1"]], ASKS))
    tr.process_snapshot(snap(30.0, "EX", [["100", "0.5"]], ASKS))  # stale
    buckets = bucket_series(tr, WINDOW, bucket_seconds=60)
    assert len(buckets) == 1
    assert buckets[0].has_gap is True
    assert buckets[0].covered == 0.0
    assert buckets[0].rate is None


def test_top_n_control_profile():
    cfg = AnalysisConfig(specs=(WINDOW,), top_n_control=1)
    analyzer = MarketAnalyzer(cfg)
    snaps = [
        snap(0.0, "EX", [["100", "1"], ["99", "1"]], ASKS),
        snap(4.0, "EX", [["100", "1"]], ASKS),
    ]
    analyzer.analyze(snaps, ["EX"])
    full = analyzer.tracker("EX")
    top1 = analyzer.tracker("EX", "top1")
    assert full is not None and top1 is not None
    # Full profile sees the vanished 99 bid (book shrank: true end of book).
    assert summarize(full, WINDOW).clean.total == Decimal(99)
    # Truncated to top-1, the 99 level was never observable.
    assert summarize(top1, WINDOW, profile="top1").clean.total == 0


def test_analyze_respects_limit_and_unknown_exchanges():
    cfg = AnalysisConfig(specs=(WINDOW,))
    analyzer = MarketAnalyzer(cfg)
    snaps = [
        snap(0.0, "EX", [["100", "1"]], ASKS),
        snap(4.0, "OTHER", [["100", "1"]], ASKS),  # not tracked
        snap(8.0, "EX", [["100", "0.5"]], ASKS),
    ]
    analyzer.analyze(snaps, ["EX"], limit=1)
    assert analyzer.tracker("EX").intervals == []
    analyzer2 = MarketAnalyzer(cfg)
    analyzer2.analyze(snaps, ["EX"])
    assert len(analyzer2.tracker("EX").intervals) == 1
    assert analyzer2.summaries()[0].clean.total == Decimal(50)
