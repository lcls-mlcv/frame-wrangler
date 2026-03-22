from __future__ import annotations


def build_event_code_map(experiment: str, run: str, codes: list[int]) -> dict[int, frozenset[int]]:
    """
    Query psana for all events in the given run and return a mapping of
    timestamp -> frozenset of active event codes (from the provided list).

    Parameters
    ----------
    experiment:
        Psana experiment name, e.g. "mfx101211025".
    run:
        Run number (as a string) to query.
    codes:
        List of integer event codes to track (e.g. [203, 204]).
    """
    from psana import DataSource

    result: dict[int, frozenset[int]] = {}
    ds = DataSource(exp=experiment, run=int(run), detectors=["timing"])
    myrun = next(ds.runs())
    timing = myrun.Detector("timing")
    for evt in myrun.events():
        evr = timing.raw.eventcodes(evt)
        result[evt.timestamp] = frozenset(c for c in codes if evr[c])
    return result


def make_pattern_filter(timestamp_map: dict[int, frozenset[int]], codes: list[int], value: str):
    """
    Return a filter function (chunk) -> bool.

    A chunk matches when its timestamp is in the map and the set of active
    codes matches the pattern described by ``value``.

    Parameters
    ----------
    timestamp_map:
        Mapping returned by build_event_code_map.
    codes:
        Ordered list of event codes corresponding to positions in ``value``.
    value:
        Binary string where '1' means the code at that position must be active
        and '0' means it must be inactive. E.g. "10" with codes [40, 41] means
        code 40 active and code 41 inactive.
    """
    expected = frozenset(c for c, v in zip(codes, value) if v == "1")

    def _filter(chunk):
        ts = chunk.timestamp
        if ts is None:
            return False
        active = timestamp_map.get(ts)
        if active is None:
            return False
        return active == expected

    return _filter
