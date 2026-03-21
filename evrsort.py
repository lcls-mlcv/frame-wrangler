from psana import DataSource
import argparse
from enum import StrEnum, auto
import subprocess
import re
from pathlib import Path


BASE_PATH = Path("/sdf/scratch/lcls/ds/mfx/mfx101211025/scratch/DiogoMelo/Cheetah/cheetah/hdf5")
EXPERIMENT: str = "mfx101211025"
LASER_ON_EVENT_CODE: int = 203
LASER_OFF_EVENT_CODE: int = 204


class LaserState(StrEnum):
    dark = auto()
    light = auto()


def retrieve_laser_on_off_map(run_number: int) -> dict[int, LaserState]:
    """
    the output dictionary maps timestamps to laser state ("on" or "off")
    """

    ds = DataSource(exp=EXPERIMENT, run=run_number, detectors=["timing"])
    laser_mapping: dict[int, LaserState] = {}

    myrun = next(ds.runs())
    timing = myrun.Detector("timing")

    for _, evt in enumerate(myrun.events()):
        evr_binary_array = timing.raw.eventcodes(evt)

        if evr_binary_array[LASER_OFF_EVENT_CODE] and not evr_binary_array[LASER_ON_EVENT_CODE]:
            laser_mapping[evt.timestamp] = LaserState.dark
        elif evr_binary_array[LASER_ON_EVENT_CODE] and not evr_binary_array[LASER_OFF_EVENT_CODE]:
            laser_mapping[evt.timestamp] = LaserState.light
        elif evr_binary_array[LASER_ON_EVENT_CODE] and evr_binary_array[LASER_OFF_EVENT_CODE]:
            raise ValueError(f"both ON and OFF EVR codes present for timestamp {evt.timestamp}")
        else:
            raise ValueError(f"! no laser EVRs for timestamp {evt.timestamp}")

    return laser_mapping


def make_custom_split_list(stream_files: list[Path], output_filepath: Path):

    for stream in stream_files:
        if not stream.exists():
            raise IOError(f"cannot find stream file: {stream}")

    with output_filepath.open("w") as f:
        for laser_state in [LaserState.dark, LaserState.light]:
            i = 0

            for stream in stream_files:
                pattern = r'Image filename:\s*(\S+)\s*Event:\s*(\S+)'
                with stream.open("r") as stream_f:
                    matches = re.findall(pattern, stream_f.read())

                for image_filename, event in matches:
                    f.write(f"{image_filename} {event} {laser_state}\n")
                    i += 1

            print(laser_state, i)


def main() -> None:

    parser = argparse.ArgumentParser()
    parser.add_argument("name", type=str)
    parser.add_argument("stream_file", type=str)
    parser.add_argument("start_run", type=int)
    parser.add_argument("end_run", type=int)
    args = parser.parse_args()

    laser_on_off_map = {}
    for run in range(args.start_run, args.end_run):
        try:
            print(f"building evr map for run {run}")
            laser_on_off_map |= retrieve_laser_on_off_map(run) 
        except Exception as e:
            print(e)
            print(f"skipping run {run}")

    output_filepath = Path(f"./custom-split-{args.name}.lst")

    make_custom_split_list([Path(args.stream_file)], output_filepath)
    

if __name__ == "__main__":
    main()
