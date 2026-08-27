"""Order book data model.

All prices and quantities are `Decimal`. The bundled dataset mixes two
encodings (one exchange serializes numbers as JSON strings, the other as
JSON numbers); parsing goes through ``json.loads(..., parse_float=Decimal)``
plus a defensive per-value conversion, so no binary float is ever
materialized for book data.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from json import loads
from typing import Literal

Side = Literal["bids", "asks"]

_ZERO = Decimal(0)
_TEN_THOUSAND = Decimal(10_000)


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):  # only reachable if a caller bypasses parse_float
        return Decimal(str(value))
    return Decimal(value)  # type: ignore[arg-type]  # str or int


@dataclass
class OrderBookSide:
    """One side of an order book: a mapping of price -> resting quantity."""

    side: Side
    levels: dict[Decimal, Decimal]

    @classmethod
    def from_raw(cls, side: Side, orders: Iterable[Iterable[object]]) -> OrderBookSide:
        """Build from raw ``[[price, qty], ...]`` pairs (strings or numbers).

        Duplicate price levels are aggregated by summing their quantities.
        """
        levels: dict[Decimal, Decimal] = {}
        for price_raw, qty_raw in orders:
            price = _to_decimal(price_raw)
            qty = _to_decimal(qty_raw)
            if price in levels:
                levels[price] += qty
            else:
                levels[price] = qty
        return cls(side=side, levels=levels)

    def __len__(self) -> int:
        return len(self.levels)

    @property
    def best(self) -> Decimal | None:
        if not self.levels:
            return None
        return max(self.levels) if self.side == "bids" else min(self.levels)

    @property
    def far(self) -> Decimal | None:
        """Deepest visible price: the far boundary of the top-N window."""
        if not self.levels:
            return None
        return min(self.levels) if self.side == "bids" else max(self.levels)

    def depth(self, *, mid: Decimal | None = None, band_bps: int | None = None) -> Decimal:
        """Resting value (price * qty), optionally only within ``band_bps`` of ``mid``."""
        if band_bps is None:
            return sum((p * q for p, q in self.levels.items()), _ZERO)
        if mid is None:
            raise ValueError("band_bps requires mid")
        delta = mid * Decimal(band_bps) / _TEN_THOUSAND
        lo, hi = mid - delta, mid + delta
        return sum((p * q for p, q in self.levels.items() if lo <= p <= hi), _ZERO)

    def truncated(self, n: int) -> OrderBookSide:
        """The ``n`` best levels of this side (simulates a narrower top-N feed)."""
        if len(self.levels) <= n:
            return self
        keys = sorted(self.levels, reverse=(self.side == "bids"))[:n]
        return OrderBookSide(side=self.side, levels={k: self.levels[k] for k in keys})


@dataclass
class OrderBookSnapshot:
    """A complete order book observation at one point in time."""

    timestamp: float
    exchange: str
    bids: OrderBookSide
    asks: OrderBookSide

    @classmethod
    def from_json(cls, line: str) -> OrderBookSnapshot:
        data = loads(line, parse_float=Decimal)
        return cls(
            timestamp=float(data["timestamp"]),
            exchange=data["exchange"],
            bids=OrderBookSide.from_raw("bids", data["bids"]),
            asks=OrderBookSide.from_raw("asks", data["asks"]),
        )

    @property
    def mid(self) -> Decimal | None:
        """(best bid + best ask) / 2, defined the same way even for a crossed book."""
        bb, ba = self.bids.best, self.asks.best
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2

    def truncated(self, n: int) -> OrderBookSnapshot:
        """Both sides truncated to their ``n`` best levels."""
        return OrderBookSnapshot(
            timestamp=self.timestamp,
            exchange=self.exchange,
            bids=self.bids.truncated(n),
            asks=self.asks.truncated(n),
        )
