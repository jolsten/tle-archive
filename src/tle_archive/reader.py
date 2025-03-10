import datetime
import os
import pathlib
from typing import Callable, Iterable, Union

from sgp4.api import Satrec
from sgp4.conveniences import sat_epoch_datetime

PathLike = Union[str, bytes, os.PathLike, pathlib.Path]
TLETuple = tuple[str, str]


def tle_datetime(tle: TLETuple) -> datetime.datetime:
    sat = Satrec.twoline2rv(*tle)
    dt = sat_epoch_datetime(sat)
    return dt


def tle_satnum(tle: TLETuple) -> str:
    return tle[0][2:7].replace(" ", "0")


class AutoCollate(dict):
    """Autovivifify a new set if the key is missing"""

    def __missing__(self, key):
        value = self[key] = set()
        return value


def collate(
    tles: list[TLETuple], key: Callable[[TLETuple], str]
) -> dict[str, set[TLETuple]]:
    """Split a list of TLE tuples into a dictionary with the given key"""
    result = AutoCollate()
    for tle in tles:
        result[key(tle)].add(tle)
    return result


def read_tles(
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


def write_tle(filename: pathlib.Path, tle_list: Iterable[TLETuple]) -> None:
    with open(filename, "w") as f:
        for line1, line2 in sorted(tle_list, key=tle_datetime):
            print(line1, file=f)
            print(line2, file=f)
