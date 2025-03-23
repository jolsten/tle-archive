import argparse
import pathlib
from concurrent.futures import Future, ThreadPoolExecutor

from tle_archive.archive import (
    group_by,
    read_tles,
    tle_date,
    tle_epoch,
    unique,
    write_tle,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", nargs="+", type=pathlib.Path)
    parser.add_argument("destination", type=pathlib.Path)
    args = parser.parse_args()

    files = []
    for path in args.source:
        more = path.glob("*.txt")
        files.extend(more)

    tles = read_tles(files)
    tles = unique(tles)

    by_date = group_by(tles, key=tle_date)

    with ThreadPoolExecutor() as executor:
        futures = []
        for date, tle_list in by_date.items():
            day_file = args.destination / f"{date}.tce"
            tle_list = sorted(tle_list, key=tle_epoch)
            # write_tle(day_file, tle_list)
            future: Future = executor.submit(write_tle, day_file, tle_list)
            futures.append(future)

        for future in futures:
            _ = future.result()
