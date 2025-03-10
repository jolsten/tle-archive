import pathlib
import shutil
from tempfile import TemporaryDirectory

from tle_archive.archive import Settings, ingest


class TestArchive:
    def setup_class(self):
        self.tempdir = TemporaryDirectory()

        root = pathlib.Path(self.tempdir.name)
        inbox = root / "inbox/"
        obj = root / "obj/"
        day = root / "day/"

        for d in [inbox, obj, day]:
            d.mkdir()

        self.settings = Settings(archive=root)

        self.obj_files = list(pathlib.Path("tests/data/obj").glob("*.txt"))
        for file in self.obj_files:
            shutil.copy2(file, self.settings.object)

        self.inbox_files = list(pathlib.Path("tests/data/day").glob("*.txt"))
        for file in self.inbox_files:
            shutil.copy2(file, self.settings.inbox)

    def test_ingest(self):
        ingest(self.settings, progress_bar=False)

        for file in self.inbox_files:
            archived_file = self.settings.daily / file.name
            print(archived_file)
            assert archived_file.is_file()

    def teardown_class(self):
        self.tempdir.cleanup()
