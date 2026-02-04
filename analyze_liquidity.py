#!/usr/bin/env python3
"""
Market taker activity analyzer using OOP and iterator pattern.

This module analyzes order book snapshots to detect real volume consumed
by market takers (as opposed to wash trading or fake volume).
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterator, Dict, Optional
from json import loads


@dataclass
class OrderLevel:
    """Represents a single price level in the order book."""
    price: float
    quantity: float

    @property
    def volume(self) -> float:
        """Volume in quote currency (price * quantity)."""
        return self.price * self.quantity


@dataclass
class OrderBookSide:
    """Represents one side of the order book (bids or asks)."""
    levels: Dict[float, float]  # {price: quantity}

    @classmethod
    def from_list(cls, orders: list[list[str]]) -> OrderBookSide:
        """
        Create OrderBookSide from raw order format.

        Args:
            orders: List of [price, qty] pairs as strings

        Returns:
            OrderBookSide instance with parsed levels
        """
        levels = {float(price): float(qty) for price, qty in orders}
        return cls(levels=levels)

    def calculate_consumed_volume(self, previous: OrderBookSide) -> float:
        """
        Calculate volume consumed by comparing with previous snapshot.

        Only counts reductions (executed orders), not additions (new orders).
        This detects real market taker activity.

        Args:
            previous: Previous snapshot of this order book side

        Returns:
            Total consumed volume in quote currency
        """
        consumed = 0.0

        for price, old_qty in previous.levels.items():
            current_qty = self.levels.get(price, 0.0)
            qty_reduction = old_qty - current_qty

            if qty_reduction > 0:
                consumed += qty_reduction * price

        return consumed


@dataclass
class OrderBookSnapshot:
    """Represents a complete order book snapshot at a point in time."""
    timestamp: float
    exchange: str
    bids: OrderBookSide
    asks: OrderBookSide

    @classmethod
    def from_json(cls, json_str: str) -> OrderBookSnapshot:
        """
        Parse order book snapshot from JSON string.

        Args:
            json_str: JSON string with timestamp, exchange, bids, asks

        Returns:
            OrderBookSnapshot instance
        """
        data = loads(json_str)
        return cls(
            timestamp=data['timestamp'],
            exchange=data['exchange'],
            bids=OrderBookSide.from_list(data['bids']),
            asks=OrderBookSide.from_list(data['asks'])
        )


class OrderBookReader:
    """
    Iterator for reading order book snapshots from a file.

    Implements lazy loading - doesn't load entire file into memory.
    Use as context manager for proper resource cleanup.

    Example:
        with OrderBookReader("data.csv") as reader:
            for snapshot in reader:
                process(snapshot)
                print(f"Progress: {reader.progress * 100:.1f}%")
    """

    def __init__(self, filename: str):
        """
        Initialize reader.

        Args:
            filename: Path to CSV file with JSON snapshots (one per line)
        """
        self.filename = filename
        self._file = None
        self._total_lines = None
        self._current_line = 0

    def __enter__(self):
        """Open file and count lines for progress tracking."""
        self._file = open(self.filename, 'r')
        # Count total lines for progress calculation
        self._total_lines = sum(1 for _ in open(self.filename))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close file on exit."""
        if self._file:
            self._file.close()

    def __iter__(self) -> Iterator[OrderBookSnapshot]:
        """Return iterator (self)."""
        return self

    def __next__(self) -> OrderBookSnapshot:
        """
        Read and parse next snapshot.

        Returns:
            Next OrderBookSnapshot from file

        Raises:
            StopIteration: When end of file is reached
        """
        line = self._file.readline()

        if not line or line == '\n':
            raise StopIteration

        self._current_line += 1
        return OrderBookSnapshot.from_json(line.strip())

    @property
    def progress(self) -> float:
        """
        Current reading progress.

        Returns:
            Float between 0.0 and 1.0 representing progress
        """
        if self._total_lines == 0:
            return 0.0
        return self._current_line / self._total_lines


class VolumeTracker:
    """
    Tracks consumed volume for a single exchange.

    Maintains state between snapshots to calculate volume differences.
    """

    def __init__(self, exchange_name: str):
        """
        Initialize tracker for an exchange.

        Args:
            exchange_name: Identifier for the exchange
        """
        self.exchange_name = exchange_name
        self.previous_bids: Optional[OrderBookSide] = None
        self.previous_asks: Optional[OrderBookSide] = None

        self.bids_volumes: list[float] = []
        self.bids_timestamps: list[float] = []
        self.asks_volumes: list[float] = []
        self.asks_timestamps: list[float] = []

    def process_snapshot(self, snapshot: OrderBookSnapshot):
        """
        Process a new snapshot and update volume metrics.

        Compares with previous snapshot to detect consumed volume.

        Args:
            snapshot: New order book snapshot for this exchange
        """
        # Analyze bids (consumed by market sellers)
        if self.previous_bids is not None:
            volume = snapshot.bids.calculate_consumed_volume(self.previous_bids)
            self.bids_volumes.append(volume)
            self.bids_timestamps.append(snapshot.timestamp)

        # Analyze asks (consumed by market buyers)
        if self.previous_asks is not None:
            volume = snapshot.asks.calculate_consumed_volume(self.previous_asks)
            self.asks_volumes.append(volume)
            self.asks_timestamps.append(snapshot.timestamp)

        # Update state for next comparison
        self.previous_bids = snapshot.bids
        self.previous_asks = snapshot.asks

    @property
    def total_bids_volume(self) -> float:
        """Total volume consumed on bid side."""
        return sum(self.bids_volumes)

    @property
    def total_asks_volume(self) -> float:
        """Total volume consumed on ask side."""
        return sum(self.asks_volumes)

    @property
    def avg_hourly_volume(self) -> float:
        """
        Average volume per hour across entire tracking period.

        Returns:
            Average hourly volume, or 0.0 if no data
        """
        if not self.bids_timestamps or not self.asks_timestamps:
            return 0.0

        # Calculate time span in seconds
        time_lapse = max(
            self.bids_timestamps[-1],
            self.asks_timestamps[-1]
        ) - min(
            self.bids_timestamps[0],
            self.asks_timestamps[0]
        )

        if time_lapse == 0:
            return 0.0

        # Convert to hourly rate
        total_volume = self.total_bids_volume + self.total_asks_volume
        return (total_volume / time_lapse) * 3600


class MarketAnalyzer:
    """
    Main analyzer for market taker activity across multiple exchanges.

    Coordinates reading snapshots and tracking volume for each exchange.
    """

    def __init__(self):
        """Initialize analyzer with empty trackers."""
        self.trackers: Dict[str, VolumeTracker] = {}

    def analyze(self, filename: str, exchanges: list[str]):
        """
        Analyze order book log for specified exchanges.

        Reads snapshots using iterator pattern and updates trackers.

        Args:
            filename: Path to file with JSON snapshots (one per line)
            exchanges: List of exchange identifiers to track
        """
        # Initialize trackers for each exchange
        for exchange in exchanges:
            self.trackers[exchange] = VolumeTracker(exchange)

        # Process snapshots using iterator
        with OrderBookReader(filename) as reader:
            for snapshot in reader:
                # Display progress every 100 snapshots
                if reader._current_line % 100 == 0:
                    print(f"\rAnalyzing: {reader.progress * 100:.2f}%", end='')

                # Process if this exchange is being tracked
                if snapshot.exchange in self.trackers:
                    self.trackers[snapshot.exchange].process_snapshot(snapshot)

        print()  # Newline after progress indicator

    def get_tracker(self, exchange: str) -> VolumeTracker:
        """
        Get volume tracker for a specific exchange.

        Args:
            exchange: Exchange identifier

        Returns:
            VolumeTracker for the exchange, or None if not found
        """
        return self.trackers.get(exchange)

    def print_summary(self):
        """Print summary statistics for all tracked exchanges."""
        print("--- RESULTS ---")
        print("-" * 30)

        for name, tracker in self.trackers.items():
            print(f"{name} bids volume: {tracker.total_bids_volume:.2f}")
            print(f"{name} asks volume: {tracker.total_asks_volume:.2f}")
            print(f"{name} avg hourly volume: {tracker.avg_hourly_volume:.2f}")
            print("-" * 30)


if __name__ == "__main__":
    from vtclear import clear_screen
    import matplotlib.pyplot as plt

    plt.style.use("dark_background")

    # Analyze data
    analyzer = MarketAnalyzer()
    analyzer.analyze("ORDER_BOOK.csv", ["EX1", "EX2"])

    # Display results
    clear_screen()
    analyzer.print_summary()

    # Get trackers for plotting
    ex1 = analyzer.get_tracker("EX1")
    ex2 = analyzer.get_tracker("EX2")

    # Save results to file
    with open("analysis.txt", "w") as f:
        f.write(f"{ex1.bids_timestamps}\n{ex1.bids_volumes}\n")
        f.write(f"{ex2.bids_timestamps}\n{ex2.bids_volumes}\n")
        f.write(f"{ex1.asks_timestamps}\n{ex1.asks_volumes}\n")
        f.write(f"{ex2.asks_timestamps}\n{ex2.asks_volumes}")

    # Plot results
    plt.grid(c="#222222")
    plt.plot(ex1.bids_timestamps, ex1.bids_volumes,
             label="EX1 BIDS", lw=0.5, c="#00ff00")
    plt.plot(ex2.bids_timestamps, ex2.bids_volumes,
             label="EX2 BIDS", lw=0.5, c="#00ffff")
    plt.plot(ex1.asks_timestamps, ex1.asks_volumes,
             label="EX1 ASKS", lw=0.5, c="#ff0000")
    plt.plot(ex2.asks_timestamps, ex2.asks_volumes,
             label="EX2 ASKS", lw=0.5, c="#ffff00")
    plt.legend()
    plt.show()
