import pytest
from pathlib import Path

from tests._utils import HEADER, make_chunk


@pytest.fixture
def synthetic_stream_path(tmp_path: Path) -> Path:
    """Small synthetic stream with 4 chunks: events //40, //41, //40, //41."""
    path = tmp_path / "test.stream"
    chunks = [
        make_chunk("file1.h5", "//40"),
        make_chunk("file2.h5", "//41"),
        make_chunk("file3.h5", "//40", indexed=False),
        make_chunk("file4.h5", "//41"),
    ]
    path.write_bytes(HEADER + b"".join(chunks))
    return path
