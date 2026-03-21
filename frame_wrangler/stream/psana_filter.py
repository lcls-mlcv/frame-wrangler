from __future__ import annotations


def build_event_code_map(experiment: str, run: str, code: int) -> set[int]:
    """
    Query psana for all events in the given run and return the set of
    timestamps where the specified event code is active.

    Parameters
    ----------
    experiment:
        Psana experiment name, e.g. "mfx101211025".
    run:
        Run number (as a string) to query.
    code:
        Integer event code to filter on (e.g. 203 for laser-on).
    """
    from psana import DataSource

    active: set[int] = set()
    ds = DataSource(exp=experiment, run=int(run), detectors=["timing"])
    myrun = next(ds.runs())
    timing = myrun.Detector("timing")
    for evt in myrun.events():
        evr = timing.raw.eventcodes(evt)
        if evr[code]:
            active.add(evt.timestamp)
    return active


def make_psana_filter(experiment: str, run: str, code: str):
    """
    Return a filter function (chunk) -> bool that returns True when the chunk's
    timestamp is present in the psana event-code map for the given code.

    The chunk's Event: field is expected to be of the form "//TIMESTAMP" where
    TIMESTAMP is the integer psana event timestamp.
    """
    active = build_event_code_map(experiment, run, int(code))

    def _filter(chunk):
        event = chunk.event
        if event is None:
            return False
        try:
            ts = int(event.lstrip("/"))
        except ValueError:
            return False
        return ts in active

    return _filter
