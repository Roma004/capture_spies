from __future__ import annotations

import os
import sys
from base64 import b64encode
from typing import Literal

from dotenv import load_dotenv
from pydantic import Extra, Field
from sqlalchemy.engine.url import URL

from pydantic_settings import BaseSettings


ASYNC_DRIVER_NAME = "postgresql+asyncpg"
SYNC_DRIVER_NAME = "postgresql"


if ".env" in os.listdir("./"):
    load_dotenv("./.env")


class CustomBaseSettings(BaseSettings):
    class Config:
        case_sensitive = False
        extra = Extra.ignore


class LoggingSettings(CustomBaseSettings):
    level: Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"] = Field(
        default="INFO",
    )
    format: str = (
        "{level} {time:YYYY-MM-DD HH:mm:ss} {name}:{function}-{message} | {extra}"
    )
    pod_name: str | None = Field(
        None, description="Name of POD. k8s should be set it automatically"
    )

    class Config(CustomBaseSettings.Config):
        env_prefix = "logging_"
        validate_by_name = True


class DatabaseSettings(CustomBaseSettings):
    host: str
    port: str
    user: str
    password: str
    db: str

    timeout: int = 60  # in seconds
    statement_timeout: int = 55

    @property
    def full_url_async(self) -> str:
        url = URL.create(
            drivername=ASYNC_DRIVER_NAME,
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.db,
        )
        return url.render_as_string(hide_password=False)

    class Config(CustomBaseSettings.Config):
        env_prefix = "postgres_"

    @property
    def full_url_sync(self) -> str:
        url = URL.create(
            drivername=SYNC_DRIVER_NAME,
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.db,
        )
        return url.render_as_string(hide_password=False)

class ServiceSettings(CustomBaseSettings):
    pass


settings = ServiceSettings()
db_settings = DatabaseSettings()
