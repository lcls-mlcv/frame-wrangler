from __future__ import annotations

import argparse
import sys
from pathlib import Path

from frame_wrangler.stream.stream import Stream


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="split_stream",
        description="Split a CrystFEL stream file into classes by event code pattern.",
    )
    parser.add_argument("stream_file", help="Path to the input .stream file")
    parser.add_argument(
        "--event-codes",
        required=True,
        metavar="CODES",
        help="Comma-separated event codes to track, e.g. 40,41",
    )
    parser.add_argument(
        "--labels",
        required=True,
        metavar="LABELS",
        help="Comma-separated output class names, e.g. Dark,Light",
    )
    parser.add_argument(
        "--binary-coding",
        required=True,
        metavar="CODING",
        help=(
            "Comma-separated binary patterns (one per label) describing which "
            "event codes must be active. Each pattern has one digit per "
            "--event-codes entry: '1'=active, '0'=inactive. "
            "E.g. --event-codes=40,41 --binary-coding=10,11 means "
            "Dark requires code 40 on and 41 off; Light requires both on."
        ),
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
        metavar="RUN",
        help="Run number (required when --method=psana)",
    )
    parser.add_argument(
        "--outdir",
        metavar="DIR",
        help="Output directory for split stream files (default: same directory as input)",
    )
    args = parser.parse_args(argv)

    if args.method == "psana":
        missing = [f"--{f}" for f, v in [("experiment", args.experiment), ("run", args.run)] if v is None]
        if missing:
            parser.error(f"{', '.join(missing)} required when --method=psana")
    elif args.method == "stream":
        raise NotImplementedError("--method=stream is not yet implemented")

    codes = [int(c.strip()) for c in args.event_codes.split(",")]
    labels = [l.strip() for l in args.labels.split(",")]
    values = [v.strip() for v in args.binary_coding.split(",")]

    if len(labels) != len(values):
        parser.error(
            f"--labels has {len(labels)} entries but --binary-coding has {len(values)}; "
            "they must match."
        )
    bad_values = [v for v in values if len(v) != len(codes) or not all(c in "01" for c in v)]
    if bad_values:
        parser.error(
            f"each --binary-coding entry must be a binary string of length {len(codes)} "
            f"(one digit per --event-codes entry); invalid: {bad_values}"
        )

    in_path = Path(args.stream_file)
    if not in_path.exists():
        parser.error(f"stream file not found: {in_path}")

    stem = in_path.stem
    out_dir = Path(args.outdir) if args.outdir else in_path.parent

    if args.method == "psana":
        from frame_wrangler.stream.psana_filter import build_event_code_map, make_pattern_filter
        timestamp_map = build_event_code_map(args.experiment, args.run, codes)
    else:
        raise NotImplementedError(f"--method={args.method!r} is not yet implemented")

    with Stream(in_path) as stream:
        for label, value in zip(labels, values):
            out_path = out_dir / f"{stem}_{label}.stream"
            try:
                filter_fn = make_pattern_filter(timestamp_map, codes, value)
                filtered = stream.filter(filter_fn)
                filtered.write(out_path)
                print(f"Wrote {len(filtered)} chunks to {out_path}")
            except NotImplementedError as exc:
                print(f"Warning: {exc}", file=sys.stderr)
                print(f"Skipping label={label!r}", file=sys.stderr)


if __name__ == "__main__":
    main()
