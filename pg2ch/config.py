"""테이블 파이프라인 설정(YAML) 로드 및 검증.

테이블당 YAML 파일 1개. (one DAG per table) 선택적으로 같은 디렉터리의
``_defaults.yaml`` 이 모든 테이블에 병합된다 (테이블별 값이 우선, settings 는 깊은 병합).

설정 예시는 config/tables/*.example.yaml 참조.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_TABLES_DIR = "config/tables"
_DEFAULTS_FILE = "_defaults.yaml"

_SYNC_MODES = {"append", "full_reload"}
_ROW_ERROR_POLICIES = {"dead_letter", "fail", "skip"}
import re as _re

_TABLE_ID_RE = _re.compile(r"^[A-Za-z0-9_.-]+$")


def tables_dir(path: str | None = None) -> Path:
    raw = path or os.environ.get("PG2CH_TABLES_DIR") or DEFAULT_TABLES_DIR
    return Path(raw)


def split_qualified(name: str, default_schema: str) -> tuple[str, str]:
    """'schema.table' → (schema, table). '.' 없으면 default_schema 사용."""
    if "." in name:
        schema, table = name.split(".", 1)
        return schema, table
    return default_schema, name


def _deep_merge(base: dict, override: dict) -> dict:
    """override 를 base 위에 병합. dict 값은 재귀 병합, 나머지는 override 우선."""
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


@dataclass
class TableConfig:
    """검증된 단일 테이블 복사 설정."""

    table_id: str
    source: str
    target: str
    source_table: str
    target_table: str
    sync_mode: str  # "append" | "full_reload"
    order_by: Any  # list[str] | str

    meta: str = "meta"

    # append 모드
    watermark_column: str | None = None
    timestamp_column: str | None = None
    watermark_overlap: Any = 0
    overlap_minutes: int = 0
    sync_since: str | None = None

    # DDL
    engine: str = "ReplacingMergeTree"
    primary_key: Any = None
    partition_by: str | None = None
    indexes: Any = None
    settings: Any = None

    # 컬럼
    drop_columns: list = field(default_factory=list)
    column_overrides: dict = field(default_factory=dict)
    use_nullable: bool = True

    # 배치 / 에러 처리
    batch_size: int = 100_000
    insert_types_check: bool = True
    on_row_error: str = "dead_letter"  # dead_letter | fail | skip
    max_failed_rows: int | None = None

    # post-sync
    optimize_after_sync: bool = False
    optimize_partitions: Any = None
    optimize_mutations_sync: int = 2

    # Airflow 스케줄링 메타 (DAG factory 가 사용)
    schedule: str | None = None
    start_date: str | None = None
    catchup: bool = False
    max_active_runs: int = 1
    retries: int = 1
    retry_delay_seconds: int = 300
    tags: list = field(default_factory=list)
    label: str | None = None

    raw: dict = field(default_factory=dict, repr=False)

    # ── 파생 속성 ────────────────────────────────────────────
    @property
    def effective_watermark_column(self) -> str | None:
        """증분 cutoff 에 쓰는 실제 컬럼 (watermark_column 우선, 없으면 timestamp_column)."""
        return self.watermark_column or self.timestamp_column

    @property
    def dag_id(self) -> str:
        return f"pg2ch_{self.table_id}"

    def source_parts(self) -> tuple[str, str]:
        return split_qualified(self.source_table, "public")

    def target_parts(self, default_db: str) -> tuple[str, str]:
        return split_qualified(self.target_table, default_db)

    # ── 검증 ─────────────────────────────────────────────────
    @classmethod
    def from_dict(cls, data: dict) -> "TableConfig":
        d = dict(data)
        known = {f for f in cls.__dataclass_fields__ if f != "raw"}
        unknown = set(d) - known - {"_comment", "_label"}
        # _ 로 시작하는 주석 키는 허용
        unknown = {k for k in unknown if not k.startswith("_")}
        if unknown:
            raise ValueError(
                f"unknown config key(s): {', '.join(sorted(unknown))}"
            )

        required = ["table_id", "source", "target", "source_table", "target_table", "sync_mode"]
        missing = [k for k in required if not d.get(k)]
        if missing:
            raise ValueError(f"missing required key(s): {', '.join(missing)}")

        cfg = cls(raw=dict(data), **{k: v for k, v in d.items() if k in known})
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not _TABLE_ID_RE.match(self.table_id):
            raise ValueError(
                f"table_id '{self.table_id}' invalid: use [A-Za-z0-9_.-] only"
            )
        if self.sync_mode not in _SYNC_MODES:
            raise ValueError(
                f"sync_mode must be one of {sorted(_SYNC_MODES)}, got '{self.sync_mode}'"
            )
        if self.on_row_error not in _ROW_ERROR_POLICIES:
            raise ValueError(
                f"on_row_error must be one of {sorted(_ROW_ERROR_POLICIES)}, "
                f"got '{self.on_row_error}'"
            )
        if not self.order_by:
            raise ValueError(f"{self.table_id}: order_by is required")
        if int(self.batch_size) <= 0:
            raise ValueError(f"{self.table_id}: batch_size must be a positive integer")
        if not isinstance(self.insert_types_check, bool):
            raise ValueError(f"{self.table_id}: insert_types_check must be boolean")
        if self.max_failed_rows is not None and int(self.max_failed_rows) < 0:
            raise ValueError(f"{self.table_id}: max_failed_rows must be >= 0")

        if self.sync_mode == "append" and not self.effective_watermark_column:
            raise ValueError(
                f"{self.table_id}: append mode requires watermark_column "
                f"(or timestamp_column as fallback)"
            )
        if self.sync_since and not self.timestamp_column:
            raise ValueError(
                f"{self.table_id}: sync_since requires timestamp_column to be set"
            )


def _read_yaml(path: Path) -> dict:
    import yaml

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: config must be a YAML mapping")
    return data


def load_table_config(path: str | Path, *, defaults: dict | None = None) -> TableConfig:
    """단일 YAML → TableConfig. defaults 가 주어지면 병합."""
    path = Path(path)
    data = _read_yaml(path)
    if defaults:
        data = _deep_merge(defaults, data)
    # table_id 기본값: 파일명(stem)
    data.setdefault("table_id", path.stem)
    return TableConfig.from_dict(data)


def load_defaults(directory: str | Path | None = None) -> dict:
    """_defaults.yaml 이 있으면 그 내용을, 없으면 {} 반환."""
    d = Path(directory) if directory else tables_dir()
    p = d / _DEFAULTS_FILE
    return _read_yaml(p) if p.exists() else {}


def load_all_table_configs(directory: str | Path | None = None) -> list[TableConfig]:
    """디렉터리의 모든 *.yaml(_defaults.yaml / *.example.yaml 제외)을 로드.

    table_id 중복은 에러.
    """
    d = Path(directory) if directory else tables_dir()
    if not d.exists():
        return []
    defaults = load_defaults(d)
    configs: list[TableConfig] = []
    seen: dict[str, str] = {}
    for p in sorted(d.glob("*.yaml")):
        if p.name == _DEFAULTS_FILE or p.name.endswith(".example.yaml"):
            continue
        cfg = load_table_config(p, defaults=defaults)
        if cfg.table_id in seen:
            raise ValueError(
                f"duplicate table_id '{cfg.table_id}' in {p.name} "
                f"(already defined in {seen[cfg.table_id]})"
            )
        seen[cfg.table_id] = p.name
        configs.append(cfg)
    return configs
