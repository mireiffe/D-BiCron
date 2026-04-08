"""Environment-based configuration for DB, S3, and API connections."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str | None = None) -> str:
    val = os.getenv(key, default)
    if val is None:
        raise EnvironmentError(f"Required environment variable {key} is not set")
    return val


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @property
    def url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )


@dataclass(frozen=True)
class S3Config:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str


@dataclass(frozen=True)
class InfraConfig:
    susdb: DBConfig
    coredb: DBConfig
    s3: S3Config


def load_config() -> InfraConfig:
    return InfraConfig(
        susdb=DBConfig(
            host=_env("SUSDB_HOST"),
            port=int(_env("SUSDB_PORT", "5432")),
            dbname=_env("SUSDB_DBNAME"),
            user=_env("SUSDB_USER"),
            password=_env("SUSDB_PASSWORD"),
        ),
        coredb=DBConfig(
            host=_env("COREDB_HOST"),
            port=int(_env("COREDB_PORT", "5432")),
            dbname=_env("COREDB_DBNAME"),
            user=_env("COREDB_USER"),
            password=_env("COREDB_PASSWORD"),
        ),
        s3=S3Config(
            endpoint_url=_env("S3_ENDPOINT_URL"),
            access_key=_env("S3_ACCESS_KEY"),
            secret_key=_env("S3_SECRET_KEY"),
            bucket_name=_env("S3_BUCKET_NAME"),
        ),
    )
