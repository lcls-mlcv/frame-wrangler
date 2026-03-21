import argparse
import sys
from pathlib import Path

from frame_wrangler.stream.stream import Stream


def make_event_code_filter(code: str):
    """
    Return a filter function for the given event code.

    TODO: implement. The event code may appear in the chunk metadata (Event: field)
    or may require querying an external database. For now this raises NotImplementedError.
    """
    def _filter(chunk):
        raise NotImplementedError(
            f"Event-code filtering is not yet implemented (requested code={code!r}). "
            "Implement make_event_code_filter() in cli.py."
        )
    return _filter


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="split_stream",
        description="Split a CrystFEL stream file into classes by event code.",
    )
    parser.add_argument("stream_file", help="Path to the input .stream file")
    parser.add_argument(
        "--event-codes",
        required=True,
        metavar="CODES",
        help="Comma-separated event codes, e.g. 40,41",
    )
    parser.add_argument(
        "--labels",
        required=True,
        metavar="LABELS",
        help="Comma-separated output labels (one per event code), e.g. Dark,Light",
    )
    args = parser.parse_args(argv)

    codes = [c.strip() for c in args.event_codes.split(",")]
    labels = [l.strip() for l in args.labels.split(",")]

    if len(codes) != len(labels):
        parser.error(
            f"--event-codes has {len(codes)} entries but --labels has {len(labels)}; "
            "they must match."
        )

    in_path = Path(args.stream_file)
    stem = in_path.stem

    with Stream(in_path) as stream:
        for code, label in zip(codes, labels):
            out_path = in_path.parent / f"{stem}_{label}.stream"
            filter_fn = make_event_code_filter(code)
            try:
                filtered = stream.filter(filter_fn)
                filtered.write(out_path)
                print(f"Wrote {len(filtered)} chunks to {out_path}")
            except NotImplementedError as exc:
                print(f"Warning: {exc}", file=sys.stderr)
                print(f"Skipping label={label!r} (code={code!r})", file=sys.stderr)


if __name__ == "__main__":
    main()
