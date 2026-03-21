from __future__ import annotations

import argparse
import sys
from pathlib import Path

from frame_wrangler.stream.stream import Stream


def make_event_code_filter(code: str, method: str = "psana", experiment: str | None = None, runs: list | None = None):
    """
    Return a filter function (chunk) -> bool for the given event code.

    Parameters
    ----------
    code:
        Event code to filter on.
    method:
        "psana" queries the psana DataSource for timestamps with the given code active.
        "stream" is not yet implemented.
    experiment:
        Required for method="psana".
    runs:
        Required for method="psana". List of run numbers as strings.
    """
    if method == "psana":
        from frame_wrangler.stream.psana_filter import make_psana_filter
        return make_psana_filter(experiment, runs, code)
    raise NotImplementedError(f"--method={method!r} is not yet implemented")


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
    parser.add_argument(
        "--method",
        choices=["psana", "stream"],
        default="psana",
        help="Method used to look up event codes (default: psana)",
    )
    parser.add_argument(
        "--experiment",
        metavar="EXPERIMENT",
        help="Experiment name (required when --method=psana)",
    )
    parser.add_argument(
        "--run",
        nargs="+",
        metavar="RUN",
        help="Run number(s) (required when --method=psana)",
    )
    args = parser.parse_args(argv)

    if args.method == "psana":
        missing = [f"--{f}" for f, v in [("experiment", args.experiment), ("run", args.run)] if v is None]
        if missing:
            parser.error(f"{', '.join(missing)} required when --method=psana")
    elif args.method == "stream":
        raise NotImplementedError("--method=stream is not yet implemented")

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
            try:
                filter_fn = make_event_code_filter(code, method=args.method, experiment=args.experiment, runs=args.run)
                filtered = stream.filter(filter_fn)
                filtered.write(out_path)
                print(f"Wrote {len(filtered)} chunks to {out_path}")
            except NotImplementedError as exc:
                print(f"Warning: {exc}", file=sys.stderr)
                print(f"Skipping label={label!r} (code={code!r})", file=sys.stderr)


if __name__ == "__main__":
    main()
