"""Environment-based configuration for S3 and app-level settings.

DB 접속 정보는 data/databases.json 에서 관리합니다 (dbcron.db 모듈 참조).
이 모듈은 S3 등 DB 외 인프라 설정만 담당합니다.
"""

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
class S3Config:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str


@dataclass(frozen=True)
class AppConfig:
    s3: S3Config


def load_config() -> AppConfig:
    return AppConfig(
        s3=S3Config(
            endpoint_url=_env("S3_ENDPOINT_URL", ""),
            access_key=_env("S3_ACCESS_KEY", ""),
            secret_key=_env("S3_SECRET_KEY", ""),
            bucket_name=_env("S3_BUCKET_NAME", ""),
        ),
    )
