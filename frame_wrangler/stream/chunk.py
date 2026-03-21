import re


class Chunk:
    """
    A single chunk from a CrystFEL stream file.

    Constructed from the raw bytes of the chunk (inclusive of Begin/End delimiters).
    All metadata properties are parsed lazily and cached. The object is picklable.
    """

    _RE_FILENAME = re.compile(rb"^Filename:\s*(.+)$", re.MULTILINE)
    _RE_EVENT = re.compile(rb"^Event:\s*(.+)$", re.MULTILINE)
    _RE_INDEXED = re.compile(rb"^indexed_by:\s*(.+)$", re.MULTILINE)

    def __init__(self, raw: bytes):
        self._raw = raw
        self._parsed: dict = {}

    def _get(self, key: str, pattern: re.Pattern) -> str | None:
        if key not in self._parsed:
            m = pattern.search(self._raw)
            self._parsed[key] = m.group(1).decode("utf-8", errors="replace").strip() if m else None
        return self._parsed[key]

    @property
    def filename(self) -> str | None:
        return self._get("filename", self._RE_FILENAME)

    @property
    def event(self) -> str | None:
        return self._get("event", self._RE_EVENT)

    @property
    def indexed_by(self) -> str | None:
        return self._get("indexed_by", self._RE_INDEXED)

    @property
    def lines(self) -> list[str]:
        return self._raw.decode("utf-8", errors="replace").splitlines(keepends=True)

    def __repr__(self) -> str:
        return f"<Chunk filename={self.filename!r} event={self.event!r}>"
