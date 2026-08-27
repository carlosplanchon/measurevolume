import lzma
import tarfile

import pytest

from measurevolume.reader import OrderBookReader

LINE1 = '{"timestamp": 1.0, "exchange": "EX1", "bids": [["100", "1"]], "asks": [["101", "2"]]}'
LINE2 = '{"timestamp": 2.0, "exchange": "EX2", "bids": [[99.5, 1.5]], "asks": [[100.5, 2.5]]}'
# Blank lines interleaved: they must be skipped, not treated as EOF.
CONTENT = LINE1 + "\n\n" + LINE2 + "\n\n"


def _check(path) -> None:
    with OrderBookReader(str(path)) as reader:
        snaps = list(reader)
        assert [s.exchange for s in snaps] == ["EX1", "EX2"]
        assert reader.current_line == 2
        assert 0.0 < reader.progress <= 1.0


def test_plain_csv(tmp_path):
    p = tmp_path / "book.csv"
    p.write_text(CONTENT)
    _check(p)


def test_xz_stream(tmp_path):
    p = tmp_path / "book.csv.xz"
    p.write_bytes(lzma.compress(CONTENT.encode()))
    _check(p)


def test_tar_xz_stream(tmp_path):
    inner = tmp_path / "book.csv"
    inner.write_text(CONTENT)
    p = tmp_path / "book.csv.tar.xz"
    with tarfile.open(p, "w:xz") as tf:
        tf.add(inner, arcname="book.csv")
    _check(p)


def test_iteration_requires_context_manager():
    reader = OrderBookReader("whatever.csv")
    with pytest.raises(RuntimeError):
        next(iter(reader))
