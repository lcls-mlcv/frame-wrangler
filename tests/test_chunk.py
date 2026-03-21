import pickle

from frame_wrangler.stream.chunk import Chunk
from tests._utils import make_chunk


RAW = make_chunk("mydata.h5", "//42", indexed=True)


def test_filename():
    chunk = Chunk(RAW)
    assert chunk.filename == "mydata.h5"


def test_event():
    chunk = Chunk(RAW)
    assert chunk.event == "//42"


def test_indexed_by():
    chunk = Chunk(RAW)
    assert chunk.indexed_by == "mosflm-nolatt-nocell"


def test_indexed_by_none():
    raw = make_chunk("f.h5", "//1", indexed=False)
    assert Chunk(raw).indexed_by == "none"


def test_missing_field_returns_none():
    chunk = Chunk(b"----- Begin chunk -----\n----- End chunk -----\n")
    assert chunk.filename is None
    assert chunk.event is None
    assert chunk.indexed_by is None


def test_lines_returns_list_of_strings():
    chunk = Chunk(RAW)
    lines = chunk.lines
    assert isinstance(lines, list)
    assert all(isinstance(l, str) for l in lines)
    assert any("Begin chunk" in l for l in lines)


def test_picklable():
    chunk = Chunk(RAW)
    data = pickle.dumps(chunk)
    restored = pickle.loads(data)
    assert restored.filename == chunk.filename
    assert restored.event == chunk.event


def test_repr():
    chunk = Chunk(RAW)
    r = repr(chunk)
    assert "mydata.h5" in r
    assert "//42" in r
