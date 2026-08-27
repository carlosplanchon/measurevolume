from decimal import Decimal

from measurevolume.models import OrderBookSide, OrderBookSnapshot

# Real formats from the bundled dataset: EX1 serializes numbers as JSON
# strings, EX2 as JSON numbers.
EX1_LINE = (
    '{"timestamp": 1592726830.0367844, "exchange": "EX1",'
    ' "bids": [["49779.15", "0.02"], ["49779.14", "0.00380661"]],'
    ' "asks": [["49860.31", "0.23061707"]]}'
)
EX2_LINE = (
    '{"timestamp": 1592726829.6742322, "exchange": "EX2",'
    ' "bids": [[49788.0, 0.06581847]],'
    ' "asks": [[49921.6, 0.25193795]]}'
)


def _all_decimal(side: OrderBookSide) -> bool:
    return all(
        isinstance(p, Decimal) and isinstance(q, Decimal)
        for p, q in side.levels.items()
    )


def test_parse_string_prices_ex1():
    snap = OrderBookSnapshot.from_json(EX1_LINE)
    assert snap.exchange == "EX1"
    assert isinstance(snap.timestamp, float)
    assert _all_decimal(snap.bids) and _all_decimal(snap.asks)
    assert snap.bids.levels[Decimal("49779.15")] == Decimal("0.02")


def test_parse_numeric_prices_ex2():
    snap = OrderBookSnapshot.from_json(EX2_LINE)
    assert _all_decimal(snap.bids) and _all_decimal(snap.asks)
    # parse_float=Decimal keeps the exact decimal text, no binary float round-trip
    assert snap.bids.levels[Decimal("49788.0")] == Decimal("0.06581847")
    assert snap.asks.levels[Decimal("49921.6")] == Decimal("0.25193795")


def test_duplicate_levels_aggregate():
    side = OrderBookSide.from_raw("bids", [["100", "1"], ["100", "2.5"]])
    assert side.levels == {Decimal(100): Decimal("3.5")}


def test_best_far_and_mid():
    snap = OrderBookSnapshot(
        timestamp=0.0,
        exchange="EX",
        bids=OrderBookSide.from_raw("bids", [["100", "1"], ["99", "1"]]),
        asks=OrderBookSide.from_raw("asks", [["101", "1"], ["102", "1"]]),
    )
    assert snap.bids.best == Decimal(100)
    assert snap.bids.far == Decimal(99)
    assert snap.asks.best == Decimal(101)
    assert snap.asks.far == Decimal(102)
    assert snap.mid == Decimal("100.5")


def test_crossed_book_mid_is_defined():
    snap = OrderBookSnapshot(
        timestamp=0.0,
        exchange="EX",
        bids=OrderBookSide.from_raw("bids", [["101", "1"]]),
        asks=OrderBookSide.from_raw("asks", [["100", "1"]]),
    )
    assert snap.mid == Decimal("100.5")


def test_empty_side_mid_is_none():
    snap = OrderBookSnapshot(
        timestamp=0.0,
        exchange="EX",
        bids=OrderBookSide("bids", {}),
        asks=OrderBookSide.from_raw("asks", [["100", "1"]]),
    )
    assert snap.mid is None
    assert snap.bids.best is None and snap.bids.far is None


def test_truncated_keeps_best_levels():
    bids = OrderBookSide.from_raw(
        "bids", [["100", "1"], ["99", "1"], ["98", "1"], ["97", "1"]]
    )
    asks = OrderBookSide.from_raw(
        "asks", [["101", "1"], ["102", "1"], ["103", "1"]]
    )
    assert set(bids.truncated(2).levels) == {Decimal(100), Decimal(99)}
    assert set(asks.truncated(2).levels) == {Decimal(101), Decimal(102)}
    assert bids.truncated(10) is bids  # nothing to cut


def test_depth_full_and_banded():
    bids = OrderBookSide.from_raw("bids", [["99.95", "2"], ["99.85", "1"]])
    assert bids.depth() == Decimal("299.75")
    mid = Decimal(100)
    assert bids.depth(mid=mid, band_bps=10) == Decimal("199.90")  # 99.85 is outside
    assert bids.depth(mid=mid, band_bps=25) == Decimal("299.75")
