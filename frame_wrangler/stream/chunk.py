from __future__ import annotations

import re


class Chunk:
    """
    A single chunk from a CrystFEL stream file.

    Constructed from the raw bytes of the chunk (inclusive of Begin/End delimiters).
    All metadata properties are parsed lazily and cached. The object is picklable.
    """

    # Matches both "Image filename:" (real data) and "Filename:" (older/synthetic)
    _RE_FILENAME        = re.compile(rb"^(?:Image )?[Ff]ilename:\s*(.+)$", re.MULTILINE)
    _RE_EVENT           = re.compile(rb"^Event:\s*(.+)$", re.MULTILINE)
    _RE_SERIAL          = re.compile(rb"^Image serial number:\s*(\S+)$", re.MULTILINE)
    _RE_HIT             = re.compile(rb"^hit\s*=\s*(\S+)$", re.MULTILINE)
    _RE_INDEXED         = re.compile(rb"^indexed_by\s*[=:]\s*(.+)$", re.MULTILINE)
    _RE_N_INDEXING      = re.compile(rb"^n_indexing_tries\s*=\s*(\S+)$", re.MULTILINE)
    _RE_PHOTON_ENERGY   = re.compile(rb"^photon_energy_eV\s*=\s*(\S+)", re.MULTILINE)
    _RE_BEAM_DIV        = re.compile(rb"^beam_divergence\s*=\s*(\S+)", re.MULTILINE)
    _RE_BEAM_BW         = re.compile(rb"^beam_bandwidth\s*=\s*(\S+)", re.MULTILINE)
    _RE_BEAM_ENERGY     = re.compile(rb"^header/float/beam_energy\s*=\s*(\S+)$", re.MULTILINE)
    _RE_TIMESTAMP       = re.compile(rb"^header/int/timestamp\s*=\s*(\S+)$", re.MULTILINE)
    _RE_EVENT_ID        = re.compile(rb"^header/str/event_id\s*=\s*(.+)$", re.MULTILINE)
    _RE_SOURCE          = re.compile(rb"^header/str/source\s*=\s*(.+)$", re.MULTILINE)
    _RE_CONFIG_FILE     = re.compile(rb"^header/str/configuration_file\s*=\s*(.+)$", re.MULTILINE)
    _RE_CAMERA_LENGTH   = re.compile(rb"^average_camera_length\s*=\s*(\S+)", re.MULTILINE)
    _RE_NUM_PEAKS       = re.compile(rb"^num_peaks\s*=\s*(\S+)$", re.MULTILINE)
    _RE_PEAK_RES        = re.compile(rb"^peak_resolution\s*=\s*(\S+)", re.MULTILINE)

    def __init__(self, raw: bytes):
        self._raw = raw
        self._parsed: dict = {}

    def _get(self, key: str, pattern: re.Pattern, convert=None) -> str | None:
        if key not in self._parsed:
            m = pattern.search(self._raw)
            val = m.group(1).decode("utf-8", errors="replace").strip() if m else None
            if val is not None and convert is not None:
                try:
                    val = convert(val)
                except (ValueError, TypeError):
                    val = None
            self._parsed[key] = val
        return self._parsed[key]

    @property
    def filename(self) -> str | None:
        return self._get("filename", self._RE_FILENAME)

    @property
    def event(self) -> str | None:
        return self._get("event", self._RE_EVENT)

    @property
    def image_serial_number(self) -> int | None:
        return self._get("image_serial_number", self._RE_SERIAL, int)

    @property
    def hit(self) -> int | None:
        return self._get("hit", self._RE_HIT, int)

    @property
    def indexed_by(self) -> str | None:
        return self._get("indexed_by", self._RE_INDEXED)

    @property
    def n_indexing_tries(self) -> int | None:
        return self._get("n_indexing_tries", self._RE_N_INDEXING, int)

    @property
    def photon_energy_eV(self) -> float | None:
        return self._get("photon_energy_eV", self._RE_PHOTON_ENERGY, float)

    @property
    def beam_divergence(self) -> float | None:
        return self._get("beam_divergence", self._RE_BEAM_DIV, float)

    @property
    def beam_bandwidth(self) -> float | None:
        return self._get("beam_bandwidth", self._RE_BEAM_BW, float)

    @property
    def beam_energy(self) -> float | None:
        return self._get("beam_energy", self._RE_BEAM_ENERGY, float)

    @property
    def timestamp(self) -> int | None:
        return self._get("timestamp", self._RE_TIMESTAMP, int)

    @property
    def event_id(self) -> str | None:
        return self._get("event_id", self._RE_EVENT_ID)

    @property
    def source(self) -> str | None:
        return self._get("source", self._RE_SOURCE)

    @property
    def configuration_file(self) -> str | None:
        return self._get("configuration_file", self._RE_CONFIG_FILE)

    @property
    def average_camera_length(self) -> float | None:
        return self._get("average_camera_length", self._RE_CAMERA_LENGTH, float)

    @property
    def num_peaks(self) -> int | None:
        return self._get("num_peaks", self._RE_NUM_PEAKS, int)

    @property
    def peak_resolution(self) -> float | None:
        return self._get("peak_resolution", self._RE_PEAK_RES, float)

    @property
    def lines(self) -> list[str]:
        return self._raw.decode("utf-8", errors="replace").splitlines(keepends=True)

    def __repr__(self) -> str:
        return f"<Chunk filename={self.filename!r} event={self.event!r}>"
