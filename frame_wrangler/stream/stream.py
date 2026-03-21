import mmap
import multiprocessing
import pickle
import re
from pathlib import Path

from frame_wrangler.stream.chunk import Chunk
from frame_wrangler.stream._worker import _evaluate_chunk


def _write_stream(header: bytes, chunks_iter, dest: Path) -> None:
    with open(dest, "wb") as f:
        f.write(header)
        for chunk in chunks_iter:
            f.write(chunk._raw)


class _FilteredStream:
    """In-memory stream produced by Stream.filter()."""

    def __init__(self, header: bytes, chunk_raws: list[bytes]):
        self._header = header
        self._chunk_raws = chunk_raws

    @property
    def header(self) -> bytes:
        return self._header

    def __len__(self) -> int:
        return len(self._chunk_raws)

    def __iter__(self):
        for raw in self._chunk_raws:
            yield Chunk(raw)

    def filter(self, func) -> "_FilteredStream":
        try:
            pickle.dumps(func)
            use_mp = True
        except Exception:
            use_mp = False

        if use_mp:
            with multiprocessing.Pool() as pool:
                results = pool.starmap(
                    _evaluate_chunk,
                    [(r, func) for r in self._chunk_raws],
                    chunksize=256,
                )
        else:
            results = [_evaluate_chunk(r, func) for r in self._chunk_raws]

        kept = [r for r, ok in zip(self._chunk_raws, results) if ok]
        return _FilteredStream(header=self._header, chunk_raws=kept)

    def write(self, file_name) -> None:
        _write_stream(self._header, iter(self), Path(file_name))


class Stream:
    """
    Lazily-loaded, mmap-backed representation of a CrystFEL .stream file.

    The file is scanned once at construction to build an index of chunk byte
    offsets. Chunks are then accessed on demand via mmap slicing — no bulk load.

    Usage::

        with Stream("data.stream") as s:
            for chunk in s:
                print(chunk.event)

            filtered = s.filter(my_func)   # my_func(chunk) -> bool; must be picklable
            filtered.write("output.stream")
    """

    _RE_BEGIN = re.compile(rb"^----- Begin chunk -----$", re.MULTILINE)
    # Include the trailing newline so sliced chunk bytes are self-contained and
    # can be concatenated without losing line boundaries on write.
    _RE_END = re.compile(rb"^----- End chunk -----\n?", re.MULTILINE)

    def __init__(self, file_name):
        self._path = Path(file_name)
        self._file = open(self._path, "rb")
        self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self._header_end: int = 0
        self._chunk_offsets: list[tuple[int, int]] = []
        self._build_index()

    def _build_index(self) -> None:
        data = self._mmap
        begins = [m.start() for m in self._RE_BEGIN.finditer(data)]
        ends = [m.end() for m in self._RE_END.finditer(data)]

        if len(begins) != len(ends):
            raise ValueError(
                f"Malformed stream file: found {len(begins)} Begin markers "
                f"but {len(ends)} End markers in {self._path}"
            )

        self._header_end = begins[0] if begins else len(data)
        self._chunk_offsets = list(zip(begins, ends))

    @property
    def header(self) -> bytes:
        return bytes(self._mmap[: self._header_end])

    def __len__(self) -> int:
        return len(self._chunk_offsets)

    def __iter__(self):
        for start, end in self._chunk_offsets:
            yield Chunk(bytes(self._mmap[start:end]))

    def __getitem__(self, idx: int) -> Chunk:
        start, end = self._chunk_offsets[idx]
        return Chunk(bytes(self._mmap[start:end]))

    def filter(self, func) -> _FilteredStream:
        """
        Return a _FilteredStream containing only chunks where func(chunk) is True.

        func must be a picklable callable (named function or functools.partial)
        for multiprocessing to be used. Non-picklable callables (e.g. lambdas,
        local functions) fall back to single-threaded evaluation automatically.
        """
        chunk_raws = [bytes(self._mmap[s:e]) for s, e in self._chunk_offsets]
        try:
            pickle.dumps(func)
            use_mp = True
        except Exception:
            use_mp = False

        if use_mp:
            with multiprocessing.Pool() as pool:
                results = pool.starmap(
                    _evaluate_chunk,
                    [(r, func) for r in chunk_raws],
                    chunksize=256,
                )
        else:
            results = [_evaluate_chunk(r, func) for r in chunk_raws]

        kept = [r for r, ok in zip(chunk_raws, results) if ok]
        return _FilteredStream(header=self.header, chunk_raws=kept)

    def write(self, file_name) -> None:
        _write_stream(self.header, iter(self), Path(file_name))

    def close(self) -> None:
        self._mmap.close()
        self._file.close()

    def __enter__(self) -> "Stream":
        return self

    def __exit__(self, *_) -> None:
        self.close()
