import shutil
import subprocess
import sys
import pytest
from pathlib import Path

from frame_wrangler.stream.cli import main
from frame_wrangler.stream.stream import Stream


def test_cli_mismatched_codes_labels(synthetic_stream_path):
    with pytest.raises(SystemExit):
        main([str(synthetic_stream_path), "--event-codes=40,41", "--labels=Dark"])


def test_cli_missing_file():
    with pytest.raises((FileNotFoundError, OSError, SystemExit)):
        main(["nonexistent.stream", "--event-codes=40", "--labels=Dark"])


def test_cli_stub_warns_stderr(synthetic_stream_path, capsys):
    main([str(synthetic_stream_path), "--event-codes=40,41", "--labels=Dark,Light"])
    captured = capsys.readouterr()
    assert "not yet implemented" in captured.err.lower() or "Warning" in captured.err


def test_cli_stub_no_output_files_written(synthetic_stream_path):
    parent = synthetic_stream_path.parent
    main([str(synthetic_stream_path), "--event-codes=40", "--labels=Dark"])
    assert not (parent / "test_Dark.stream").exists()


def test_cli_with_patched_filter(synthetic_stream_path, tmp_path, monkeypatch):
    """Monkeypatch make_event_code_filter with a working implementation."""
    import frame_wrangler.stream.cli as cli_mod

    def _patched_factory(code):
        def _f(chunk):
            return chunk.event == f"//{code}"
        return _f

    monkeypatch.setattr(cli_mod, "make_event_code_filter", _patched_factory)

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    dest = work_dir / "test.stream"
    shutil.copy(synthetic_stream_path, dest)

    main([str(dest), "--event-codes=40,41", "--labels=Dark,Light"])

    dark_path = work_dir / "test_Dark.stream"
    light_path = work_dir / "test_Light.stream"
    assert dark_path.exists()
    assert light_path.exists()

    with Stream(dark_path) as dark, Stream(light_path) as light:
        assert len(dark) == 2
        assert len(light) == 2
        assert all(c.event == "//40" for c in dark)
        assert all(c.event == "//41" for c in light)


def test_cli_output_preserves_header(synthetic_stream_path, tmp_path, monkeypatch):
    import frame_wrangler.stream.cli as cli_mod
    from tests._utils import HEADER

    def _patched_factory(code):
        return lambda chunk: True

    monkeypatch.setattr(cli_mod, "make_event_code_filter", _patched_factory)

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    dest = work_dir / "test.stream"
    shutil.copy(synthetic_stream_path, dest)
    main([str(dest), "--event-codes=40", "--labels=All"])

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
