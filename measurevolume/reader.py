"""Streaming reader for order-book snapshot logs.

Accepts plain ``.csv`` (JSON Lines, one snapshot per line), ``.csv.xz`` and
``.csv.tar.xz``, decompressing on the fly with the standard library only;
the 545 MB bundled dataset never needs to be extracted to disk. Progress is
tracked by compressed bytes consumed, so it costs nothing.
"""

from __future__ import annotations

import io
import lzma
import os
import tarfile
from collections.abc import Iterator
from contextlib import suppress
from typing import IO

from .models import OrderBookSnapshot


class _StreamShim(io.RawIOBase):
    """Adapts tarfile's stream-mode extracted file for `io.TextIOWrapper`.

    In ``r|xz`` mode the extracted file object delegates ``seekable()`` to
    tarfile's internal ``_Stream``, which does not implement it; going
    through a RawIOBase (whose ``seekable()`` is False) sidesteps that.
    """

    def __init__(self, fileobj):
        self._fileobj = fileobj

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        data = self._fileobj.read(len(buffer))
        n = len(data)
        buffer[:n] = data
        return n


class OrderBookReader:
    """Iterate `OrderBookSnapshot`s from a snapshot log. Use as a context manager.

    Blank lines are skipped (they do not terminate the stream).

    Example:
        with OrderBookReader("ORDER_BOOK.csv.tar.xz") as reader:
            for snapshot in reader:
                process(snapshot)
                print(f"{reader.progress:.1%} ({reader.current_line} snapshots)")
    """

    def __init__(self, path: str):
        self.path = path
        self.current_line = 0
        self._raw: IO[bytes] | None = None
        self._text: IO[str] | None = None
        self._tar: tarfile.TarFile | None = None
        self._size = 0

    def __enter__(self) -> OrderBookReader:  # noqa: PYI034
        self._raw = open(self.path, "rb")
        self._size = os.fstat(self._raw.fileno()).st_size
        name = self.path.lower()
        if name.endswith((".tar.xz", ".txz")):
            self._tar = tarfile.open(fileobj=self._raw, mode="r|xz")
            member = self._tar.next()
            while member is not None and not member.isfile():
                member = self._tar.next()
            if member is None:
                raise ValueError(f"{self.path}: tar archive contains no regular file")
            extracted = self._tar.extractfile(member)
            if extracted is None:
                raise ValueError(f"{self.path}: cannot extract {member.name}")
            self._text = io.TextIOWrapper(
                io.BufferedReader(_StreamShim(extracted)), encoding="utf-8"
            )
        elif name.endswith(".xz"):
            self._text = io.TextIOWrapper(lzma.LZMAFile(self._raw), encoding="utf-8")
        else:
            self._text = io.TextIOWrapper(self._raw, encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._text is not None:
            with suppress(Exception):
                self._text.close()
        if self._tar is not None:
            with suppress(Exception):
                self._tar.close()
        if self._raw is not None:
            self._raw.close()
        self._text = self._tar = self._raw = None

    def __iter__(self) -> Iterator[OrderBookSnapshot]:
        if self._text is None:
            raise RuntimeError("OrderBookReader must be used as a context manager")
        for line in self._text:
            line = line.strip()
            if not line:
                continue
            self.current_line += 1
            yield OrderBookSnapshot.from_json(line)

    @property
    def progress(self) -> float:
        """Approximate progress in [0, 1], by compressed bytes consumed."""
        if self._raw is None or self._size == 0:
            return 0.0
        try:
            pos = self._raw.tell()
        except (OSError, ValueError):
            return 0.0
        return min(pos / self._size, 1.0)
