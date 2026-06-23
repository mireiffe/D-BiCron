"""접속 정보 레지스트리(JSON) + PG / ClickHouse / meta 커넥션 팩토리.

접속 정보는 ID 로 키잉된 JSON 파일에서 관리한다 (기본: config/connections.json,
PG2CH_CONNECTIONS 환경변수로 경로 변경 가능). 테이블 파이프라인 설정은
이 ID(source / target / meta)만 참조한다.

비밀 값은 파일에 직접 적는 대신 ``${ENV_VAR}`` / ``${ENV_VAR:-default}``
형태로 환경변수를 참조할 수 있다. ``"_enc": "b64"`` 인 항목의 password 는
base64 디코딩된다.

connections.json 예시:
{
  "my_postgres":   {"type": "postgresql", "host": "pg", "port": 5432,
                    "dbname": "shop", "user": "app", "password": "${PG_PASSWORD}"},
  "my_clickhouse": {"type": "clickhouse", "host": "ch", "port": 9000,
                    "dbname": "default", "user": "default", "password": ""},
  "meta":          {"type": "postgresql", "host": "pg", "port": 5432,
                    "dbname": "shop", "user": "app", "password": "${PG_PASSWORD}",
                    "schema": "pg2ch_meta"}
}
"""

from __future__ import annotations

import base64
import json
import os
import re
from pathlib import Path

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

DEFAULT_CONNECTIONS_PATH = "config/connections.json"


def connections_path(path: str | None = None) -> Path:
    """접속 정보 파일 경로 결정. 인자 > PG2CH_CONNECTIONS env > 기본값."""
    raw = path or os.environ.get("PG2CH_CONNECTIONS") or DEFAULT_CONNECTIONS_PATH
    return Path(raw)


def _interpolate_env(value):
    """문자열 안의 ${VAR} / ${VAR:-default} 를 환경변수로 치환."""
    if not isinstance(value, str):
        return value

    def repl(m: re.Match) -> str:
        var, default = m.group(1), m.group(2)
        env_val = os.environ.get(var)
        if env_val is not None:
            return env_val
        if default is not None:
            return default
        raise KeyError(
            f"connections.json references undefined env var ${{{var}}}"
        )

    return _ENV_RE.sub(repl, value)


def _resolve_cfg(cfg: dict) -> dict:
    """env 치환 + b64 password 디코딩을 적용한 사본 반환."""
    resolved = {k: _interpolate_env(v) for k, v in cfg.items()}
    if resolved.get("_enc") == "b64" and resolved.get("password"):
        resolved["password"] = base64.b64decode(resolved["password"]).decode()
    return resolved


def load_connections(path: str | None = None) -> dict[str, dict]:
    """접속 정보 전체를 {id: cfg} dict 로 로드 (env/secret 치환 적용)."""
    p = connections_path(path)
    if not p.exists():
        raise FileNotFoundError(f"connections file not found: {p}")
    with open(p, encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError(
            f"{p}: connections file must be a JSON object keyed by connection id"
        )
    # "_" 로 시작하는 키(_comment 등)와 dict 가 아닌 값은 주석으로 보고 건너뛴다.
    return {
        cid: _resolve_cfg(cfg)
        for cid, cfg in raw.items()
        if not cid.startswith("_") and isinstance(cfg, dict)
    }


def get_connection(conn_id: str, path: str | None = None) -> dict:
    """ID 로 단일 접속 정보를 찾는다. 없으면 KeyError."""
    conns = load_connections(path)
    if conn_id not in conns:
        available = ", ".join(sorted(conns)) or "(none)"
        raise KeyError(
            f"connection '{conn_id}' not found. available: {available}"
        )
    return conns[conn_id]


# ── 커넥션 팩토리 ───────────────────────────────────────────────


def pg_connect(cfg: dict):
    """PostgreSQL 커넥션 (psycopg2)."""
    import psycopg2

    if cfg.get("type") not in (None, "postgresql"):
        raise ValueError(f"expected postgresql connection, got '{cfg.get('type')}'")
    return psycopg2.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 5432)),
        dbname=cfg["dbname"],
        user=cfg.get("user", ""),
        password=cfg.get("password", ""),
        connect_timeout=int(cfg.get("connect_timeout", 30)),
    )


def ch_connect(cfg: dict):
    """ClickHouse 커넥션 (clickhouse-driver, native TCP)."""
    from clickhouse_driver import Client

    if cfg.get("type") not in (None, "clickhouse"):
        raise ValueError(f"expected clickhouse connection, got '{cfg.get('type')}'")
    port = cfg.get("native_port") or cfg.get("port") or 9000
    return Client(
        host=cfg["host"],
        port=int(port),
        database=cfg.get("dbname", "default"),
        user=cfg.get("user", "default"),
        password=cfg.get("password", ""),
        settings=cfg.get("settings") or {},
        connect_timeout=int(cfg.get("connect_timeout", 30)),
    )


def meta_connect(cfg: dict):
    """추적 메타 저장소(PostgreSQL) 커넥션. pg_connect 와 동일하되 의미 구분용."""
    return pg_connect(cfg)
