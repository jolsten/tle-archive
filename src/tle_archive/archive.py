import datetime
import os
import pathlib
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, Union

import tqdm
from sgp4.api import Satrec
from sgp4.conveniences import sat_epoch_datetime

from tle_archive.config import Settings

PathLike = Union[str, bytes, os.PathLike, pathlib.Path]
TLETuple = tuple[str, str]


def tle_datetime(tle: TLETuple) -> datetime.datetime:
    sat = Satrec.twoline2rv(*tle)
    dt = sat_epoch_datetime(sat)
    return dt


def tle_epoch(tle: TLETuple) -> float:
    return float(tle[0][18:32].replace(" ", "0"))


def tle_satnum(tle: TLETuple) -> str:
    return tle[0][2:7].replace(" ", "0")


class AutoCollate(dict):
    """Autovivifify a new set if the key is missing"""

    def __missing__(self, key):
        value = self[key] = {}
        return value


def collate(
    tles: list[TLETuple], key: Callable[[TLETuple], str]
) -> dict[str, set[TLETuple]]:
    """Split a list of TLE tuples into a dictionary with the given key"""
    result = AutoCollate()
    for tle in tles:
        result[key(tle)][tle] = None
    return result


def read_tle(
    file: PathLike,
) -> list[TLETuple]:
    results = []
    with open(file, "r") as f:
        current = [None, None]
        for line in f:
            if line[0] == "1":
                current[0] = line
                current[1] = None
            elif line[0] == "2":
                current[1] = line
                results.append(tuple(current))
    return results


def read_tles(files: Iterable[PathLike]) -> list[TLETuple]:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(read_tle, file) for file in files]
        tles = []
        for future in futures:
            results = future.result()
            tles.extend(results)
    return tles


def sort_unique(tles: list[TLETuple]) -> list[TLETuple]:
    tles = sorted(tles, key=tle_satnum)
    tles = sorted(tles, key=tle_epoch)
    return list(dict.fromkeys(tles).keys())


def write_tle(filename: pathlib.Path, tle_list: Iterable[TLETuple]) -> None:
    with open(filename, "w") as f:
        for line1, line2 in sorted(tle_list, key=tle_datetime):
            print(line1, file=f)
            print(line2, file=f)


def ingest(config: Settings, progress_bar: bool = True):
    inbox_files = list(config.inbox.glob("*.txt"))

    ingest_tles = []
    for file in inbox_files:
        tles = read_tles(file)
        ingest_tles.extend(tles)

    collated = collate(ingest_tles, key=tle_satnum)
    iterable = sorted(collated.keys())
    if progress_bar:
        iterable = tqdm.tqdm(iterable)

    for satnum in iterable:
        objfile = config.object / f"{satnum}.txt"

        all_tles = set()
        if objfile.is_file():
            all_tles.update(read_tles(objfile))
        all_tles.update(collated[satnum])

        write_tle(objfile, all_tles)

    for file in inbox_files:
        shutil.move(file, config.daily)


if __name__ == "__main__":
    ingest()
