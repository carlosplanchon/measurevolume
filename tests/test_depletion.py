from decimal import Decimal

from measurevolume.depletion import (
    DEFAULT_SPECS,
    estimate_depletion,
    estimate_depletion_multi,
)
from measurevolume.models import OrderBookSide


def mk(side: str, *pairs: tuple[str, str | int]) -> OrderBookSide:
    return OrderBookSide.from_raw(side, [[p, str(q)] for p, q in pairs])


def test_sweep_through_touch_is_crossed_tier():
    prev = mk("bids", ("100", 1), ("99", 1), ("98", 1), ("97", 5))
    cur = mk("bids", ("97", 5), ("96", 2))
    bd = estimate_depletion(prev, cur)
    assert bd.crossed == Decimal(297)
    assert bd.near_touch == 0 and bd.deep == 0


def test_asks_sweep_is_symmetric():
    prev = mk("asks", ("100", 1), ("101", 1), ("102", 5))
    cur = mk("asks", ("102", 5), ("103", 1))
    bd = estimate_depletion(prev, cur)
    assert bd.crossed == Decimal(201)
    assert bd.total == Decimal(201)


def test_scroll_out_excluded_by_far_boundary():
    # Price drifts up one tick: 101 appears on top, 81 falls out of the
    # 20-level window. Nothing was consumed.
    prev = mk("bids", *((str(p), 1) for p in range(100, 80, -1)))
    cur = mk("bids", *((str(p), 1) for p in range(101, 81, -1)))
    assert estimate_depletion(prev, cur).total == 0
    # Historical (naive) semantics counted that level as consumed:
    naive = estimate_depletion(prev, cur, far_boundary=False)
    assert naive.total == Decimal(81)
    assert naive.deep == Decimal(81)  # deep in the book: most likely not a fill


def test_partial_window_means_boundary_is_true_book_end():
    prev = mk("bids", ("100", 1), ("95", 3), ("90", 1))
    cur = mk("bids", ("100", 1))
    # Window truncated at N: disappearances beyond the far boundary are censored.
    assert estimate_depletion(prev, cur, full_window=True).total == 0
    # Window NOT full: the boundary is the real end of the book: they count.
    bd = estimate_depletion(prev, cur, full_window=False)
    assert bd.total == Decimal(375)
    assert bd.deep == Decimal(375)


def test_deep_cancel_is_deep_tier():
    prev = mk("bids", ("100", 1), ("95", 3), ("90", 1))
    cur = mk("bids", ("100", 1), ("90", 1), ("89", 1))
    bd = estimate_depletion(prev, cur)
    assert bd.deep == Decimal(285)
    assert bd.crossed == 0 and bd.near_touch == 0


def test_partial_reduction_at_touch_is_near_touch():
    prev = mk("bids", ("100", 2))
    cur = mk("bids", ("100", "0.5"))
    bd = estimate_depletion(prev, cur)
    assert bd.near_touch == Decimal(150)
    assert bd.crossed == 0 and bd.deep == 0


def test_consume_and_refill_nets_to_zero():
    # An execution refilled before the next snapshot is invisible; the
    # estimator underestimates by construction here.
    prev = mk("bids", ("100", 1))
    cur = mk("bids", ("100", 1))
    assert estimate_depletion(prev, cur).total == 0


def test_quantity_increase_is_ignored():
    prev = mk("bids", ("100", 1))
    cur = mk("bids", ("100", 2))
    assert estimate_depletion(prev, cur).total == 0


def test_band_filter_around_previous_mid():
    prev = mk("bids", ("99.95", 1), ("99.85", 1), ("99.5", 1))
    cur = mk("bids", ("99.5", 1))
    mid = Decimal(100)
    bd10 = estimate_depletion(prev, cur, mid=mid, band_bps=10)
    bd25 = estimate_depletion(prev, cur, mid=mid, band_bps=25)
    assert bd10.total == Decimal("99.95")
    assert bd25.total == Decimal("199.80")


def test_multi_matches_single_spec_results():
    prev = mk("bids", ("100", 1), ("99.98", 2), ("95", 3), ("90", 1))
    cur = mk("bids", ("100", 1), ("99.98", 1), ("90", 1), ("89", 1))
    mid = Decimal("100.05")
    multi = estimate_depletion_multi(prev, cur, specs=DEFAULT_SPECS, mid=mid)
    for spec in DEFAULT_SPECS:
        single = estimate_depletion(
            prev, cur, mid=mid, band_bps=spec.band_bps, far_boundary=spec.far_boundary
        )
        assert multi[spec.name] == single, spec.name


def test_opposite_sides_rejected():
    import pytest

    with pytest.raises(ValueError):
        estimate_depletion(mk("bids", ("100", 1)), mk("asks", ("100", 1)))


def test_banded_spec_requires_mid():
    import pytest

    with pytest.raises(ValueError):
        estimate_depletion(mk("bids", ("100", 1)), mk("bids", ("100", 1)), band_bps=10)
