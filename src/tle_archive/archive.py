import datetime
import os
import pathlib
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, TypeVar, Union

import tqdm

from tle_archive.config import Settings

PathLike = Union[str, bytes, os.PathLike, pathlib.Path]
TLETuple = tuple[str, str]


def tle_epoch(tle: TLETuple) -> float:
    """Get the epoch (float) from a TLE"""
    epoch = float(tle[0][18:32].replace(" ", "0"))
    if epoch // 1000 >= 57:
        epoch += 19000
    else:
        epoch += 20000
    return epoch


def tle_date(tle: TLETuple) -> str:
    """Get the date (as str) from a TLE."""
    epoch = tle_epoch(tle)
    year, doy = divmod(epoch, 1000)
    doy = doy // 1
    dt = datetime.datetime(int(year), 1, 1) + datetime.timedelta(days=int(doy))
    return dt.strftime("%Y%m%d")


def tle_satnum(tle: TLETuple) -> str:
    """Extract the (Alpha-5) Satnum from a TLE."""
    return tle[0][2:7].replace(" ", "0")


GroupByKey = TypeVar("GroupByKey")


def group_by(
    tles: list[TLETuple], key: Callable[[TLETuple], GroupByKey]
) -> dict[GroupByKey, list[TLETuple]]:
    """Groups input TLEs by values from a callable key."""
    results: dict[GroupByKey, list[TLETuple]] = {}
    for tle in tles:
        group = key(tle)
        if group not in results:
            results[group] = []
        results[group].append(tle)
    return results


def read_tle(
    file: PathLike,
) -> list[TLETuple]:
    """Read a single TLE file."""
    results = []
    with open(file, "r") as f:
        current = [None, None]
        for line in f:
            line = line.rstrip()
            if line[0] == "1":
                current[0] = line
                current[1] = None
            elif line[0] == "2":
                current[1] = line
                results.append(tuple(current))
    return results


def read_tles(files: Iterable[PathLike]) -> list[TLETuple]:
    """Read multiple TLE files."""
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(read_tle, file) for file in files]
        tles = []
        for future in futures:
            results = future.result()
            tles.extend(results)
    return tles


def unique(tles: list[TLETuple]) -> list[TLETuple]:
    """Returns input as a list list ensuring unique entries."""
    return list(dict.fromkeys(tles).keys())


def write_tle(filename: pathlib.Path, tle_list: Iterable[TLETuple]) -> None:
    with open(filename, "w") as f:
        for line1, line2 in sorted(tle_list, key=tle_epoch):
            print(line1, file=f)
            print(line2, file=f)


def ingest(config: Settings, progress_bar: bool = True):
    inbox_files = list(config.inbox.glob("*.txt"))

    ingest_tles = []
    for file in inbox_files:
        tles = read_tles(file)
        ingest_tles.extend(tles)

    collated = group_by(ingest_tles, key=tle_satnum)
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
