"""테이블 파이프라인 설정(YAML) 로드 및 검증.

테이블당 YAML 파일 1개. (one DAG per table) 선택적으로 같은 디렉터리의
``_defaults.yaml`` 이 모든 테이블에 병합된다 (테이블별 값이 우선, settings 는 깊은 병합).

PG source retention 은 테이블 설정과 분리된 단일 파일(``config/retention.yaml``)로
관리한다 — retention 은 copy 와 별개 스케줄의 전용 DAG(pg2ch_retention)에서 돌기
때문이다. ``load_retention_config`` 참조.

설정 예시는 config/tables/*.example.yaml, config/retention.example.yaml 참조.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .watermark import (
    WATERMARK_TYPES,
    parse_overlap,
    resolve_since,
    validate_retention_expr,
)

DEFAULT_TABLES_DIR = "config/tables"
DEFAULT_RETENTION_CONFIG = "config/retention.yaml"
_DEFAULTS_FILE = "_defaults.yaml"

_SYNC_MODES = {"append", "full_reload"}
_ROW_ERROR_POLICIES = {"dead_letter", "fail", "skip"}
_INTEGRITY_POLICIES = {"fail", "warn"}
_INTEGRITY_METHODS = {"count", "key_diff"}
import re as _re

_TABLE_ID_RE = _re.compile(r"^[A-Za-z0-9_.-]+$")


def tables_dir(path: str | None = None) -> Path:
    raw = path or os.environ.get("PG2CH_TABLES_DIR") or DEFAULT_TABLES_DIR
    return Path(raw)


def retention_config_path(path: str | None = None) -> Path:
    raw = path or os.environ.get("PG2CH_RETENTION_CONFIG") or DEFAULT_RETENTION_CONFIG
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

    # append 모드 — watermark 컬럼 하나로 증분/sync_since/overlap 을 전부 관리한다.
    watermark_column: str | None = None
    watermark_type: str | None = None  # serial | numeric | timestamp (컬럼 지정 시 필수)
    watermark_overlap: Any = 0  # timestamp → "30m" 상대 표현, serial/numeric → 숫자
    sync_since: Any = None  # watermark 하한: timestamp → "30d"|ISO, serial/numeric → 숫자

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

    # 무결성 검사 (retention 전 누락 row 탐지)
    integrity_enabled: bool = False
    integrity_method: str = "count"  # count | key_diff
    integrity_lookback_runs: int = 1
    integrity_on_mismatch: str = "fail"  # fail | warn
    integrity_tolerance: int = 0
    integrity_repair: bool = True
    integrity_repair_attempts: int = 1
    # 검사 질의를 이 ts 파티션 컬럼 >= (now - period) 로 제한해 CH 파티션을 프루닝한다
    # (watermark 가 파티션 키가 아닐 때 window 전체 스캔/uniqExact 메모리 초과 방지).
    # 둘 다 지정하거나 둘 다 비운다. period: "30d"/"12h" 상대 또는 ISO 절대.
    integrity_partition_column: str | None = None
    integrity_partition_period: str | None = None

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
    def dag_id(self) -> str:
        return f"pg2ch_{self.table_id}"

    def source_parts(self) -> tuple[str, str]:
        return split_qualified(self.source_table, "public")

    def target_parts(self, default_db: str) -> tuple[str, str]:
        return split_qualified(self.target_table, default_db)

    # ── 검증 ─────────────────────────────────────────────────
    _LEGACY_RETENTION_KEYS = (
        "retention",
        "retention_enabled",
        "source_retention",
        "source_retention_batch_size",
        "retention_lock_timeout_ms",
    )
    # 제거된 copy 설정 키 → 마이그레이션 힌트. watermark 컬럼 하나(+타입)로 통합됐다.
    _LEGACY_COPY_KEY_HINTS = {
        "timestamp_column": (
            "removed — incremental copy / sync_since are driven by "
            "watermark_column + watermark_type; the retention column is set in "
            f"{DEFAULT_RETENTION_CONFIG} (column/type)"
        ),
        "overlap_minutes": (
            "removed — use watermark_overlap (relative like '30m' when "
            "watermark_type is 'timestamp')"
        ),
    }

    @classmethod
    def from_dict(cls, data: dict) -> "TableConfig":
        d = dict(data)
        legacy_retention = [k for k in cls._LEGACY_RETENTION_KEYS if k in d]
        if legacy_retention:
            raise ValueError(
                f"retention is no longer configured per table "
                f"(found: {', '.join(legacy_retention)}) — move it to "
                f"{DEFAULT_RETENTION_CONFIG} (see config/retention.example.yaml)"
            )
        for key, hint in cls._LEGACY_COPY_KEY_HINTS.items():
            if key in d:
                raise ValueError(f"{key}: {hint}")

        integrity = d.pop("integrity", None)
        if integrity is not None:
            if not isinstance(integrity, dict):
                raise ValueError("integrity must be a mapping")
            integrity_keys = {
                "enabled": "integrity_enabled",
                "integrity_enabled": "integrity_enabled",
                "method": "integrity_method",
                "integrity_method": "integrity_method",
                "lookback_runs": "integrity_lookback_runs",
                "integrity_lookback_runs": "integrity_lookback_runs",
                "on_mismatch": "integrity_on_mismatch",
                "integrity_on_mismatch": "integrity_on_mismatch",
                "tolerance": "integrity_tolerance",
                "integrity_tolerance": "integrity_tolerance",
                "repair": "integrity_repair",
                "integrity_repair": "integrity_repair",
                "repair_attempts": "integrity_repair_attempts",
                "integrity_repair_attempts": "integrity_repair_attempts",
                "partition_column": "integrity_partition_column",
                "integrity_partition_column": "integrity_partition_column",
                "partition_period": "integrity_partition_period",
                "integrity_partition_period": "integrity_partition_period",
            }
            unknown_integrity = set(integrity) - set(integrity_keys)
            if unknown_integrity:
                raise ValueError(
                    "unknown integrity key(s): "
                    + ", ".join(sorted(unknown_integrity))
                )
            for src, dst in integrity_keys.items():
                if src in integrity and dst not in d:
                    d[dst] = integrity[src]

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

        if self.integrity_method not in _INTEGRITY_METHODS:
            raise ValueError(
                f"{self.table_id}: integrity_method must be one of "
                f"{sorted(_INTEGRITY_METHODS)}, got '{self.integrity_method}'"
            )
        if self.integrity_on_mismatch not in _INTEGRITY_POLICIES:
            raise ValueError(
                f"{self.table_id}: integrity_on_mismatch must be one of "
                f"{sorted(_INTEGRITY_POLICIES)}, got '{self.integrity_on_mismatch}'"
            )
        if int(self.integrity_lookback_runs) < 1:
            raise ValueError(
                f"{self.table_id}: integrity_lookback_runs must be >= 1"
            )
        if int(self.integrity_tolerance) < 0:
            raise ValueError(
                f"{self.table_id}: integrity_tolerance must be >= 0"
            )
        if not isinstance(self.integrity_repair, bool):
            raise ValueError(f"{self.table_id}: integrity_repair must be boolean")
        if int(self.integrity_repair_attempts) < 1:
            raise ValueError(
                f"{self.table_id}: integrity_repair_attempts must be >= 1"
            )
        if (self.integrity_partition_column is None) != (
            self.integrity_partition_period is None
        ):
            raise ValueError(
                f"{self.table_id}: integrity_partition_column and "
                f"integrity_partition_period must be set together (omit both to "
                f"scan the whole watermark window)"
            )
        if self.integrity_partition_period is not None:
            try:
                resolve_since("timestamp", self.integrity_partition_period)
            except ValueError as e:
                raise ValueError(
                    f"{self.table_id}: integrity_partition_period: {e}"
                ) from e
        if self.integrity_enabled and self.sync_mode != "append":
            raise ValueError(
                f"{self.table_id}: integrity_enabled requires append sync_mode"
            )

        if self.sync_mode == "append" and not self.watermark_column:
            raise ValueError(
                f"{self.table_id}: append mode requires watermark_column "
                f"(+ watermark_type)"
            )
        if self.watermark_column and not self.watermark_type:
            raise ValueError(
                f"{self.table_id}: watermark_column requires watermark_type "
                f"({' | '.join(WATERMARK_TYPES)})"
            )
        if self.watermark_type and not self.watermark_column:
            raise ValueError(
                f"{self.table_id}: watermark_type requires watermark_column"
            )
        if self.watermark_type is not None and self.watermark_type not in WATERMARK_TYPES:
            raise ValueError(
                f"{self.table_id}: watermark_type must be one of "
                f"{sorted(WATERMARK_TYPES)}, got '{self.watermark_type}'"
            )
        if self.sync_since is not None:
            if not self.watermark_type:
                raise ValueError(
                    f"{self.table_id}: sync_since requires watermark_column "
                    f"and watermark_type to be set"
                )
            try:
                resolve_since(self.watermark_type, self.sync_since)
            except ValueError as e:
                raise ValueError(f"{self.table_id}: sync_since: {e}") from e
        if self.watermark_overlap not in (None, 0, "0", ""):
            if not self.watermark_type:
                raise ValueError(
                    f"{self.table_id}: watermark_overlap requires watermark_column "
                    f"and watermark_type to be set"
                )
            try:
                parse_overlap(self.watermark_type, self.watermark_overlap)
            except ValueError as e:
                raise ValueError(f"{self.table_id}: {e}") from e


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


# ── PG source retention 설정 (config/retention.yaml) ─────────────────
# retention 은 copy DAG 와 분리된 전용 DAG(pg2ch_retention)에서 돈다. 삭제 기준
# 컬럼/타입은 기본적으로 테이블의 watermark_column/watermark_type 을 따르고,
# 테이블 항목의 column/type 으로 다른 컬럼을 지정할 수 있다. cutoff 상한은 여전히
# finalize 된 watermark 가 가리키는 마지막 synced 값으로 캡핑된다
# (copy 가 멈춘 동안 미복제 row 삭제 방지).

_RETENTION_TOP_KEYS = {
    "schedule", "start_date", "catchup", "max_active_runs", "max_active_tasks",
    "retries", "retry_delay_seconds", "tags", "defaults", "tables",
}
_RETENTION_DEFAULT_KEYS = {"batch_size", "lock_timeout_ms"}
_RETENTION_TABLE_KEYS = {
    "enabled", "retention", "column", "type", "batch_size", "lock_timeout_ms",
}


@dataclass
class RetentionPolicy:
    """retention.yaml 의 테이블 한 항목 (삭제 규칙).

    retention 값은 삭제 기준 컬럼의 타입에 따라 해석된다:
      - timestamp     : "180d"(now 기준 상대) 또는 ISO 절대 → col < cutoff
      - serial/numeric: 숫자 N → 마지막 synced 값 - N (keep-last-N)
    """

    table_id: str
    retention: Any  # 삭제 후보 상한 (타입별 해석 — 클래스 docstring 참조)
    column: str | None = None  # 삭제 기준 컬럼 (기본: 테이블의 watermark_column)
    type: str | None = None  # column 의 타입 (column 지정 시 필수)
    batch_size: int = 10_000
    lock_timeout_ms: int = 5_000

    def validate(self) -> None:
        if not _TABLE_ID_RE.match(self.table_id):
            raise ValueError(
                f"retention table_id '{self.table_id}' invalid: use [A-Za-z0-9_.-] only"
            )
        if self.retention is None or self.retention == "":
            raise ValueError(f"retention[{self.table_id}]: retention is required")
        if (self.column is None) != (self.type is None):
            raise ValueError(
                f"retention[{self.table_id}]: column and type must be set together "
                f"(omit both to use the table's watermark column)"
            )
        if self.type is not None and self.type not in WATERMARK_TYPES:
            raise ValueError(
                f"retention[{self.table_id}]: type must be one of "
                f"{sorted(WATERMARK_TYPES)}, got '{self.type}'"
            )
        # type 미지정이면 유효 타입(=테이블 watermark_type)이 로드 시점에 미확정
        # → 형식만 느슨히 검증하고, 엄밀한 검증은 PgRetention 실행 시점에 한다.
        try:
            validate_retention_expr(self.type, self.retention)
        except ValueError as e:
            raise ValueError(f"retention[{self.table_id}]: {e}") from e
        if int(self.batch_size) <= 0:
            raise ValueError(
                f"retention[{self.table_id}]: batch_size must be a positive integer"
            )
        if int(self.lock_timeout_ms) <= 0:
            raise ValueError(
                f"retention[{self.table_id}]: lock_timeout_ms must be a positive integer"
            )


@dataclass
class RetentionConfig:
    """retention DAG 전체 설정 (스케줄 + 테이블별 정책)."""

    schedule: str | None = None
    start_date: str | None = None
    catchup: bool = False
    max_active_runs: int = 1
    max_active_tasks: int | None = None  # 동시에 purge 할 테이블 수 제한 (None=Airflow 기본)
    retries: int = 1
    retry_delay_seconds: int = 300
    tags: list = field(default_factory=lambda: ["pg2ch", "retention"])
    policies: list = field(default_factory=list)

    def policy_for(self, table_id: str) -> RetentionPolicy | None:
        return next((p for p in self.policies if p.table_id == table_id), None)


def load_retention_config(path: str | Path | None = None) -> RetentionConfig | None:
    """retention.yaml 로드. 파일이 없으면 None (retention 전면 비활성).

    테이블 항목은 존재 자체가 활성이며, ``enabled: false`` 로 항목을 지우지 않고
    일시 비활성할 수 있다. table_id 가 실제 테이블 설정에 존재하는지는 여기서
    검증하지 않는다 (DAG factory / CLI 가 매칭 시점에 처리).
    """
    p = Path(path) if path else retention_config_path()
    if not p.exists():
        return None
    data = _read_yaml(p)

    unknown = {k for k in data if k not in _RETENTION_TOP_KEYS and not k.startswith("_")}
    if unknown:
        raise ValueError(f"{p}: unknown retention key(s): {', '.join(sorted(unknown))}")

    defaults = data.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ValueError(f"{p}: defaults must be a mapping")
    unknown = {
        k for k in defaults if k not in _RETENTION_DEFAULT_KEYS and not k.startswith("_")
    }
    if unknown:
        raise ValueError(
            f"{p}: unknown retention defaults key(s): {', '.join(sorted(unknown))}"
        )

    tables = data.get("tables") or {}
    if not isinstance(tables, dict):
        raise ValueError(f"{p}: tables must be a mapping of table_id to settings")

    policies: list[RetentionPolicy] = []
    for table_id, spec in sorted(tables.items()):
        spec = spec or {}
        if not isinstance(spec, dict):
            raise ValueError(f"{p}: tables.{table_id} must be a mapping")
        unknown = {
            k for k in spec if k not in _RETENTION_TABLE_KEYS and not k.startswith("_")
        }
        if unknown:
            raise ValueError(
                f"{p}: tables.{table_id}: unknown key(s): {', '.join(sorted(unknown))}"
            )
        merged = {**defaults, **{k: v for k, v in spec.items() if not k.startswith("_")}}
        if not merged.pop("enabled", True):
            continue
        if merged.get("retention") is None or merged.get("retention") == "":
            raise ValueError(f"{p}: tables.{table_id}: retention is required")
        policy = RetentionPolicy(table_id=str(table_id), **merged)
        policy.validate()
        policies.append(policy)

    cfg = RetentionConfig(
        schedule=data.get("schedule"),
        start_date=data.get("start_date"),
        catchup=bool(data.get("catchup", False)),
        max_active_runs=int(data.get("max_active_runs", 1)),
        max_active_tasks=(
            int(data["max_active_tasks"])
            if data.get("max_active_tasks") is not None
            else None
        ),
        retries=int(data.get("retries", 1)),
        retry_delay_seconds=int(data.get("retry_delay_seconds", 300)),
        tags=list(data.get("tags") or ["pg2ch", "retention"]),
        policies=policies,
    )
    if cfg.max_active_runs < 1:
        raise ValueError(f"{p}: max_active_runs must be >= 1")
    if cfg.max_active_tasks is not None and cfg.max_active_tasks < 1:
        raise ValueError(f"{p}: max_active_tasks must be >= 1")
    if cfg.retries < 0:
        raise ValueError(f"{p}: retries must be >= 0")
    return cfg
