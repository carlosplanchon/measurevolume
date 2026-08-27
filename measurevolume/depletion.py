"""Visible-depth-depletion estimator.

What snapshot differencing can and cannot see
---------------------------------------------
A reduction between two snapshots may be an execution, a cancellation, or a
level pushed out of the top-N window; an execution refilled before the next
snapshot is invisible. This module therefore measures *visible depth
depletion*, not confirmed executed volume, and splits it into confidence
tiers:

- ``crossed``    -- the level vanished entirely and the best price ended
                    beyond it, i.e. the touch moved through it
                    (high confidence: consistent with a fill).
- ``near_touch`` -- reduction within a few bps of the previous best
                    (medium confidence).
- ``deep``       -- reduction deep in the book (most likely a cancellation).

The far-boundary rule: in a top-N feed, the absence of a price *better*
than the current best is informative (a resting order there would
necessarily be visible), so only the tail beyond the *far* boundary is
censored. Previous levels beyond the current far boundary are excluded,
unless the current window is not full (``full_window=False``), in which
case the boundary is the true end of the book and disappearances there are
real.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from .models import OrderBookSide

_ZERO = Decimal(0)
_TEN_THOUSAND = Decimal(10_000)


@dataclass(frozen=True)
class BandSpec:
    """One way of looking at depletion: an optional price band around the
    previous mid, and whether to apply the far-boundary exclusion.
    ``band_bps=None`` means the whole visible window."""

    name: str
    band_bps: int | None
    far_boundary: bool = True


#: Historical semantics (2020/2026 code): every reduction counts, including
#: levels that merely scrolled out of the top-N window.
NAIVE = BandSpec("naive", None, far_boundary=False)
#: Whole visible window with the scroll-out correction.
WINDOW = BandSpec("window", None, far_boundary=True)

DEFAULT_SPECS: tuple[BandSpec, ...] = (
    NAIVE,
    WINDOW,
    BandSpec("band10", 10),
    BandSpec("band25", 25),
)


@dataclass(frozen=True)
class DepletionBreakdown:
    """Depletion in quote currency, split by confidence tier."""

    crossed: Decimal = _ZERO
    near_touch: Decimal = _ZERO
    deep: Decimal = _ZERO

    @property
    def total(self) -> Decimal:
        return self.crossed + self.near_touch + self.deep

    def __add__(self, other: DepletionBreakdown) -> DepletionBreakdown:
        return DepletionBreakdown(
            crossed=self.crossed + other.crossed,
            near_touch=self.near_touch + other.near_touch,
            deep=self.deep + other.deep,
        )


def estimate_depletion_multi(
    previous: OrderBookSide,
    current: OrderBookSide,
    *,
    specs: Sequence[BandSpec],
    mid: Decimal | None = None,
    full_window: bool = True,
    near_touch_bps: int = 5,
) -> dict[str, DepletionBreakdown]:
    """Depletion of ``previous`` vs ``current`` under several specs in one pass.

    ``mid`` is the reference mid-price the bands are centered on (use the
    *previous* snapshot's mid: the frame the disappeared liquidity was
    resting under). It is only required when a spec has a band.
    """
    if previous.side != current.side:
        raise ValueError("cannot diff opposite book sides")
    is_bid = previous.side == "bids"

    far = current.far
    new_best = current.best
    prev_best = previous.best
    near_delta = (
        prev_best * Decimal(near_touch_bps) / _TEN_THOUSAND
        if prev_best is not None
        else None
    )

    # Per-spec constraints, resolved once.
    plans: list[tuple[str, bool, Decimal | None, Decimal | None]] = []
    for spec in specs:
        boundary_active = spec.far_boundary and full_window and far is not None
        if spec.band_bps is None:
            lo = hi = None
        else:
            if mid is None:
                raise ValueError(f"spec {spec.name!r} has a band but mid is None")
            delta = mid * Decimal(spec.band_bps) / _TEN_THOUSAND
            lo, hi = mid - delta, mid + delta
        plans.append((spec.name, boundary_active, lo, hi))

    acc: dict[str, list[Decimal]] = {spec.name: [_ZERO, _ZERO, _ZERO] for spec in specs}
    current_levels = current.levels

    for price, old_qty in previous.levels.items():
        new_qty = current_levels.get(price)
        missing = new_qty is None
        reduction = old_qty if missing else old_qty - new_qty
        if reduction <= 0:
            continue
        value = reduction * price

        # Confidence tier: a property of the level, independent of the spec.
        if (
            missing
            and new_best is not None
            and (price > new_best if is_bid else price < new_best)
        ):
            tier = 0  # crossed
        elif near_delta is not None and abs(price - prev_best) <= near_delta:
            tier = 1  # near_touch
        else:
            tier = 2  # deep

        for name, boundary_active, lo, hi in plans:
            if boundary_active and (price < far if is_bid else price > far):
                continue
            if lo is not None and not (lo <= price <= hi):
                continue
            acc[name][tier] += value

    return {
        name: DepletionBreakdown(crossed=c, near_touch=n, deep=d)
        for name, (c, n, d) in acc.items()
    }


def estimate_depletion(
    previous: OrderBookSide,
    current: OrderBookSide,
    *,
    mid: Decimal | None = None,
    band_bps: int | None = None,
    far_boundary: bool = True,
    full_window: bool = True,
    near_touch_bps: int = 5,
) -> DepletionBreakdown:
    """Single-spec convenience wrapper around `estimate_depletion_multi`."""
    spec = BandSpec("only", band_bps, far_boundary)
    return estimate_depletion_multi(
        previous,
        current,
        specs=[spec],
        mid=mid,
        full_window=full_window,
        near_touch_bps=near_touch_bps,
    )["only"]
