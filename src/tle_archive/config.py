from pathlib import Path

from pydantic import DirectoryPath
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="TLE_",
        env_nested_delimiter="__",
    )

    archive: DirectoryPath
    daily: Path = "day"
    object: Path = "obj"
    inbox: Path = "inbox"

    def model_post_init(self, __context):
        super().model_post_init(__context)

        self.archive = self.archive.absolute()

        if not self.daily.is_absolute():
            self.daily = self.archive / self.daily

        if not self.object.is_absolute():
            self.object = self.archive / self.object

        if not self.inbox.is_absolute():
            self.inbox = self.archive / self.inbox


# settings = Settings()
