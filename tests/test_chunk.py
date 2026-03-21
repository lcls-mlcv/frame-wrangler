import pickle
from pathlib import Path

import pytest

from frame_wrangler.stream.chunk import Chunk
from tests._utils import make_chunk


RAW = make_chunk("mydata.h5", "//42", indexed=True)

EXAMPLE_CHUNK = Path(__file__).parent.parent / "example_chunk.txt"


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


# --- Tests against the real example_chunk.txt ---

def test_example_filename():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.filename == "ZMQdata"


def test_example_event():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.event == "//1119"


def test_example_image_serial_number():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.image_serial_number == 1119


def test_example_hit():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.hit == 1


def test_example_indexed_by():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.indexed_by == "xgandalf-nolatt-cell"


def test_example_n_indexing_tries():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.n_indexing_tries == 1


def test_example_photon_energy():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.photon_energy_eV == pytest.approx(9499.119038)


def test_example_beam_divergence():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.beam_divergence == pytest.approx(0.0)


def test_example_beam_bandwidth():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.beam_bandwidth == pytest.approx(1.00e-08)


def test_example_beam_energy():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.beam_energy == pytest.approx(9499.119038)


def test_example_timestamp():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.timestamp == 4844957272179762928


def test_example_event_id():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.event_id == "4844957272179762928"


def test_example_source():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.source == "exp=mfx101211025,run=90,dir=/sdf/data/lcls/ds/mfx/mfx101211025/xtc"


def test_example_configuration_file():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert "monitor.yaml" in chunk.configuration_file


def test_example_average_camera_length():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.average_camera_length == pytest.approx(0.1247)


def test_example_num_peaks():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.num_peaks == 26


def test_example_peak_resolution():
    chunk = Chunk(EXAMPLE_CHUNK.read_bytes())
    assert chunk.peak_resolution == pytest.approx(2.776239)


def test_missing_new_fields_return_none():
    chunk = Chunk(b"----- Begin chunk -----\n----- End chunk -----\n")
    assert chunk.image_serial_number is None
    assert chunk.hit is None
    assert chunk.n_indexing_tries is None
    assert chunk.photon_energy_eV is None
    assert chunk.beam_divergence is None
    assert chunk.beam_bandwidth is None
    assert chunk.beam_energy is None
    assert chunk.timestamp is None
    assert chunk.event_id is None
    assert chunk.source is None
    assert chunk.configuration_file is None
    assert chunk.average_camera_length is None
    assert chunk.num_peaks is None
    assert chunk.peak_resolution is None
