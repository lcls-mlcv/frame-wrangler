import pytest
from pathlib import Path

from frame_wrangler.stream.stream import Stream
from tests._utils import HEADER, make_chunk


# Module-level named functions (required for multiprocessing picklability)
def _is_event_40(chunk):
    return chunk.event == "//40"


def _always_true(chunk):
    return True


def _always_false(chunk):
    return False


def test_stream_loads(synthetic_stream_path):
    s = Stream(synthetic_stream_path)
    s.close()


def test_stream_len(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        assert len(s) == 4


def test_stream_header(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        assert s.header.startswith(b"CrystFEL")


def test_stream_iter_yields_chunks(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        chunks = list(s)
    assert len(chunks) == 4
    from frame_wrangler.stream.chunk import Chunk
    assert all(isinstance(c, Chunk) for c in chunks)


def test_stream_getitem(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        assert s[0].filename == "file1.h5"
        assert s[3].filename == "file4.h5"


def test_stream_write_roundtrip(synthetic_stream_path, tmp_path):
    out = tmp_path / "out.stream"
    with Stream(synthetic_stream_path) as s:
        original_header = s.header
        s.write(out)
    with Stream(out) as s2:
        assert len(s2) == 4
        assert s2.header == original_header


def test_stream_filter_event_40(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        filtered = s.filter(_is_event_40)
    assert len(filtered) == 2
    events = [c.event for c in filtered]
    assert all(e == "//40" for e in events)


def test_stream_filter_always_false(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        filtered = s.filter(_always_false)
    assert len(filtered) == 0


def test_stream_filter_always_true(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        filtered = s.filter(_always_true)
    assert len(filtered) == 4


def test_filtered_stream_write(synthetic_stream_path, tmp_path):
    out = tmp_path / "filtered.stream"
    with Stream(synthetic_stream_path) as s:
        filtered = s.filter(_is_event_40)
    filtered.write(out)
    with Stream(out) as s2:
        assert len(s2) == 2


def test_context_manager_closes(synthetic_stream_path):
    with Stream(synthetic_stream_path) as s:
        pass
    # After close, the mmap should be closed
    assert s._mmap.closed


def test_header_only_stream(tmp_path):
    """Stream with no chunks is valid."""
    path = tmp_path / "header_only.stream"
    path.write_bytes(HEADER)
    with Stream(path) as s:
        assert len(s) == 0
        assert s.header == HEADER


def test_large_synthetic_stream(tmp_path):
    """1000-chunk file to stress-test index building."""
    chunks = b"".join(make_chunk(f"f{i}.h5", f"//{i}") for i in range(1000))
    path = tmp_path / "large.stream"
    path.write_bytes(HEADER + chunks)
    with Stream(path) as s:
        assert len(s) == 1000
        assert s[0].event == "//0"
        assert s[999].event == "//999"


def test_malformed_stream_raises(tmp_path):
    """Extra Begin marker without matching End raises ValueError."""
    bad = HEADER + b"----- Begin chunk -----\n"
    path = tmp_path / "bad.stream"
    path.write_bytes(bad)
    with pytest.raises(ValueError, match="Malformed"):
        Stream(path)
