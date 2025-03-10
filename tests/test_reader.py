import pathlib

import pytest

from tle_archive.reader import read_tles

TEST_DATA_PATH = pathlib.Path(__file__).parent / "data"
TEST_FILES = TEST_DATA_PATH.glob("**/*.txt")


def count_lines(filename) -> int:
    with open(filename, "r") as file:
        count = sum(1 for _ in file)
    return count


@pytest.mark.parametrize("file", TEST_FILES)
def test_read_tles(file):
    tles = read_tles(file)
    assert len(tles) == count_lines(file) // 2
