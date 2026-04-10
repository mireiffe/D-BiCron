"""Central DB registry: databases.json 기반 엔진 팩토리 및 유틸리티.

모든 DB 접속 정보는 data/databases.json 에서 관리합니다.
URL_BUILDERS / SYSTEM_SCHEMAS / ROW_COUNT_SQL 등 DB 타입별 설정도
이 모듈에서 통합 제공합니다.
"""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Callable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# ── 공용 경로 ─────────────────────────────────────────────────────

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ── DB 타입별 설정 ────────────────────────────────────────────────

URL_BUILDERS: dict[str, Callable[[dict], str]] = {
    "postgresql": lambda c: (
        f"postgresql+psycopg2://{c.get('user','')}:{c.get('password','')}"
        f"@{c['host']}:{c.get('port',5432)}/{c['dbname']}"
    ),
    "mssql": lambda c: (
        f"mssql+pymssql://{c.get('user','')}:{c.get('password','')}"
        f"@{c['host']}:{c.get('port',1433)}/{c['dbname']}"
    ),
    "sqlite": lambda c: f"sqlite:///{c['host']}",
    "clickhouse": lambda c: (
        f"clickhouse+http://{c.get('user','')}:{c.get('password','')}"
        f"@{c['host']}:{c.get('port',8123)}/{c['dbname']}"
    ),
}

SYSTEM_SCHEMAS: dict[str, set[str]] = {
    "postgresql": {"pg_catalog", "information_schema"},
    "mssql": {
        "sys", "INFORMATION_SCHEMA", "guest",
        "db_owner", "db_accessadmin", "db_securityadmin",
        "db_ddladmin", "db_backupoperator", "db_datareader",
        "db_datawriter", "db_denydatareader", "db_denydatawriter",
    },
    "sqlite": set(),
    "clickhouse": {"system", "INFORMATION_SCHEMA", "information_schema"},
}

ROW_COUNT_SQL: dict[str, str] = {
    "postgresql": (
        "SELECT COALESCE(n_live_tup,0) FROM pg_stat_user_tables "
        "WHERE schemaname=:s AND relname=:t"
    ),
    "mssql": (
        "SELECT SUM(p.rows) FROM sys.partitions p "
        "JOIN sys.tables t ON p.object_id=t.object_id "
        "JOIN sys.schemas s ON t.schema_id=s.schema_id "
        "WHERE s.name=:s AND t.name=:t AND p.index_id<2"
    ),
    "clickhouse": (
        "SELECT total_rows FROM system.tables "
        "WHERE database=:s AND name=:t"
    ),
}

SUPPORTED_TYPES = list(URL_BUILDERS.keys())


# ── databases.json 로드 ──────────────────────────────────────────

def load_databases() -> list[dict]:
    """data/databases.json 에서 등록된 DB 목록을 로드한다.

    b64 인코딩된 비밀번호가 있으면 자동으로 디코딩한다.
    """
    db_file = DATA_DIR / "databases.json"
    if not db_file.exists():
        return []
    with open(db_file, encoding="utf-8") as f:
        dbs = json.load(f)
    for d in dbs:
        if d.get("_enc") == "b64" and d.get("password"):
            d["password"] = base64.b64decode(d["password"]).decode()
    return dbs


def get_database(db_id: str) -> dict | None:
    """ID 로 단일 DB 설정을 찾는다."""
    for d in load_databases():
        if d["id"] == db_id:
            return d
    return None


# ── 엔진 팩토리 ──────────────────────────────────────────────────

def create_engine_for(db_cfg: dict) -> Engine:
    """DB 설정 dict 로부터 SQLAlchemy 엔진을 생성한다.

    db_cfg 는 databases.json 의 항목 또는 동일한 구조의 dict.
    """
    db_type = db_cfg.get("type", "postgresql")
    builder = URL_BUILDERS.get(db_type)
    if builder is None:
        raise ValueError(f"Unsupported DB type: {db_type}")
    return create_engine(builder(db_cfg), pool_pre_ping=True)


def create_engine_by_id(db_id: str) -> Engine:
    """databases.json 에서 ID 로 DB를 찾아 엔진을 생성한다."""
    db_cfg = get_database(db_id)
    if db_cfg is None:
        raise ValueError(
            f"DB '{db_id}' not found in databases.json. "
            f"Register it via WebUI or data/databases.json."
        )
    return create_engine_for(db_cfg)
