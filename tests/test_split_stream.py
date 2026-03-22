import shutil
import subprocess
import sys
import pytest
from pathlib import Path

from frame_wrangler.stream.cli import main
from frame_wrangler.stream.stream import Stream


# Common psana args used across tests (no --binary-coding; tests add that individually)
PSANA_ARGS = ["--experiment=myexp", "--run=42"]


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------

def test_cli_missing_binary_coding(synthetic_stream_path):
    """--binary-coding is required."""
    with pytest.raises(SystemExit):
        main([str(synthetic_stream_path), "--event-codes=40,41", "--labels=Dark,Light"] + PSANA_ARGS)


def test_cli_mismatched_labels_binary_coding(synthetic_stream_path):
    """Number of labels must equal number of values."""
    with pytest.raises(SystemExit):
        main([str(synthetic_stream_path), "--event-codes=40,41",
              "--labels=Dark,Light", "--binary-coding=10"] + PSANA_ARGS)


def test_cli_invalid_value_wrong_length(synthetic_stream_path):
    """Value pattern length must equal number of event codes."""
    with pytest.raises(SystemExit):
        main([str(synthetic_stream_path), "--event-codes=40,41",
              "--labels=Dark", "--binary-coding=1"] + PSANA_ARGS)


def test_cli_invalid_value_non_binary(synthetic_stream_path):
    """Value pattern must contain only '0' and '1'."""
    with pytest.raises(SystemExit):
        main([str(synthetic_stream_path), "--event-codes=40,41",
              "--labels=Dark", "--binary-coding=1X"] + PSANA_ARGS)


def test_cli_missing_file():
    with pytest.raises((FileNotFoundError, OSError, SystemExit)):
        main(["nonexistent.stream", "--event-codes=40", "--labels=Dark", "--binary-coding=1"] + PSANA_ARGS)


def test_cli_psana_requires_experiment_and_run(synthetic_stream_path):
    with pytest.raises(SystemExit):
        main([str(synthetic_stream_path), "--event-codes=40,41",
              "--labels=Dark,Light", "--binary-coding=10,11", "--method=psana"])


def test_cli_stream_method_raises(synthetic_stream_path):
    with pytest.raises(NotImplementedError):
        main([str(synthetic_stream_path), "--event-codes=40,41",
              "--labels=Dark", "--binary-coding=10", "--method=stream"])


# ---------------------------------------------------------------------------
# NotImplementedError handling
# ---------------------------------------------------------------------------

def _raises_not_implemented(chunk):
    raise NotImplementedError("not yet")


def test_cli_notimplemented_warns_stderr(synthetic_stream_path, capsys, monkeypatch):
    """CLI catches NotImplementedError from filter function and warns on stderr."""
    import frame_wrangler.stream.psana_filter as pf

    monkeypatch.setattr(pf, "build_event_code_map", lambda *a, **kw: {})
    monkeypatch.setattr(pf, "make_pattern_filter", lambda *a, **kw: _raises_not_implemented)

    main([str(synthetic_stream_path), "--event-codes=40,41",
          "--labels=Dark,Light", "--binary-coding=10,11"] + PSANA_ARGS)
    captured = capsys.readouterr()
    assert "Warning" in captured.err or "not yet" in captured.err.lower()


def test_cli_notimplemented_no_output_files_written(synthetic_stream_path, monkeypatch):
    """CLI writes no output files when filter function raises NotImplementedError."""
    import frame_wrangler.stream.psana_filter as pf

    monkeypatch.setattr(pf, "build_event_code_map", lambda *a, **kw: {})
    monkeypatch.setattr(pf, "make_pattern_filter", lambda *a, **kw: _raises_not_implemented)

    parent = synthetic_stream_path.parent
    main([str(synthetic_stream_path), "--event-codes=40,41",
          "--labels=Dark", "--binary-coding=10"] + PSANA_ARGS)
    assert not (parent / "test_Dark.stream").exists()


# ---------------------------------------------------------------------------
# Integration tests (psana monkeypatched)
# ---------------------------------------------------------------------------

def _make_event_filter(codes, value):
    """Test helper: filter matching chunk.event against value pattern via event string."""
    expected_events = {f"//{c}" for c, v in zip(codes, value) if v == "1"}
    def _f(chunk):
        return chunk.event in expected_events
    return _f


def test_cli_with_patched_filter(synthetic_stream_path, tmp_path, monkeypatch):
    """Correctly splits stream into Dark/Light using monkeypatched psana."""
    import frame_wrangler.stream.psana_filter as pf

    monkeypatch.setattr(pf, "build_event_code_map", lambda *a, **kw: {})
    monkeypatch.setattr(pf, "make_pattern_filter",
                        lambda ts_map, codes, value: _make_event_filter(codes, value))

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    dest = work_dir / "test.stream"
    shutil.copy(synthetic_stream_path, dest)

    # "10" → only code 40 active → Dark; "01" → only code 41 active → Light
    main([str(dest), "--event-codes=40,41", "--labels=Dark,Light",
          "--binary-coding=10,01"] + PSANA_ARGS)

    with Stream(work_dir / "test_Dark.stream") as dark, \
         Stream(work_dir / "test_Light.stream") as light:
        assert len(dark) == 2
        assert len(light) == 2
        assert all(c.event == "//40" for c in dark)
        assert all(c.event == "//41" for c in light)


def test_cli_output_preserves_header(synthetic_stream_path, tmp_path, monkeypatch):
    import frame_wrangler.stream.psana_filter as pf
    from tests._utils import HEADER

    monkeypatch.setattr(pf, "build_event_code_map", lambda *a, **kw: {})
    monkeypatch.setattr(pf, "make_pattern_filter", lambda *a, **kw: (lambda chunk: True))

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    dest = work_dir / "test.stream"
    shutil.copy(synthetic_stream_path, dest)
    main([str(dest), "--event-codes=40", "--labels=All", "--binary-coding=1"] + PSANA_ARGS)

    with Stream(work_dir / "test_All.stream") as s:
        assert s.header == HEADER


def test_cli_entry_point_help():
    """Smoke test: installed entry point exits 0 for --help."""
    result = subprocess.run(
        [sys.executable, "-m", "frame_wrangler.stream.cli", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "split_stream" in result.stdout or "stream_file" in result.stdout
