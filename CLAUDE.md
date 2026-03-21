# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

frame-wrangler is a Python library for manipulating macromolecular crystallography diffraction image files. The initial focus is on CrystFEL `.stream` files, with plans to support other formats later.

## Build & Install

```bash
pip install -e .          # editable install
pip install -e ".[test]"  # with test dependencies
```

Uses setuptools for packaging (pyproject.toml). No hatch, tox, or other project management tools.

## Testing

```bash
pytest                    # run all tests
pytest tests/test_foo.py  # run a single test file
pytest -k "test_name"     # run a specific test
```

Uses pytest. CI runs via GitHub Actions.

## Architecture

### Core API (`Stream` and `Chunk`)

- **`Stream`** - Represents a CrystFEL stream file. Lazily loads chunks using mmap and seek for low memory footprint. Uses regex to find chunk boundaries.
  - `filter(func)` - Returns a new `_FilteredStream` containing only chunks where `func(chunk) -> bool` is True. Picklable callables use `multiprocessing.Pool`; non-picklable ones fall back to sequential evaluation.
  - `write(file_name)` - Writes the stream to disk
  - `__iter__` - Yields `Chunk` objects one at a time
- **`Chunk`** - Represents a single chunk within a stream file, with attributes for key metadata

### Key Design Principles

- **Lazy loading**: Chunks are loaded on demand via mmap/seek, not all at once
- **Regex-based parsing**: Chunk boundaries are found with regular expressions
- **Multiprocessing**: Python multiprocessing is used to parallelize data loading and filtering

### CLI

Entry point: `split_stream`

```
split_stream stream_file --event-codes=40,41 --labels=Dark,Light
```

Splits a stream file by event code into separate output files sharing the same header but containing different chunks. Uses argparse.
