"""PostgreSQL → ClickHouse 동기화 Job.

PG 소스에서 CH 타겟으로 테이블 데이터를 동기화합니다.

동작 모드:
  - Full copy: watermark 이력이 없으면 대상 테이블 TRUNCATE 후 전체 복사
  - Incremental: 기존 watermark 이후 변경분만 전송
    (ReplacingMergeTree 사용 시 overlap_minutes 또는 watermark_overlap 으로
     중복 허용, merge 시 자동 제거)

기능:
  - Column drop: 특정 소스 컬럼 제외
  - Column type override: LowCardinality, Decimal 등 CH 타입 직접 지정
  - ORDER BY / PRIMARY KEY / INDEX / PARTITION BY / ENGINE / SETTINGS 설정
  - 자동 테이블 생성 (CREATE TABLE IF NOT EXISTS)
  - Watermark 기반 증분 동기화 (timestamp / integer 모두 지원)
  - sync_since: timestamp_column 기반 하한 필터 (full copy / incremental 공통)
  - use_nullable: false 설정 시 PG nullable 컬럼을 CH Nullable 대신 기본값으로 대체
  - optimize_after_sync: 동기화 직후 OPTIMIZE TABLE ... FINAL 실행 (즉시 dedup)
    optimize_partitions 로 일부 파티션만 제한 가능

설정:
  PG2CH_CONFIG 환경변수로 JSON 설정 파일 경로 지정 (기본: pg2ch_config.json)
  source / target 은 databases.json 의 DB ID

PG source retention (오래된 row 삭제) 은 pg_retention job 으로 분리되었습니다.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from datetime import date, datetime, timedelta, timezone

from ..db import get_database
from .base import Job, JobResult

# ── PG → CH 기본 타입 매핑 ──────────────────────────────────────

_PG_TO_CH: dict[str, str] = {
    "smallint": "Int16",
    "integer": "Int32",
    "bigint": "Int64",
    "real": "Float32",
    "double precision": "Float64",
    "boolean": "UInt8",
    "character varying": "String",
    "character": "String",
    "text": "String",
    "bytea": "String",
    "date": "Date",
    "timestamp without time zone": "DateTime64(6, 'UTC')",
    "timestamp with time zone": "DateTime64(6, 'UTC')",
    "time without time zone": "String",
    "time with time zone": "String",
    "interval": "String",
    "json": "String",
    "jsonb": "String",
    "uuid": "UUID",
    "inet": "String",
    "cidr": "String",
    "macaddr": "String",
    "money": "Decimal(18,4)",
}


def _pg_type_to_ch(
    pg_type: str,
    *,
    nullable: bool = False,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    """PG data_type → CH 타입 문자열."""
    if pg_type == "numeric":
        p = precision if precision else 18
        s = scale if scale else 4
        base = f"Decimal({p},{s})"
    elif pg_type == "ARRAY" or pg_type == "USER-DEFINED":
        base = "String"
    else:
        base = _PG_TO_CH.get(pg_type, "String")
    return f"Nullable({base})" if nullable else base


def _unwrap_ch_type(ch_type: str) -> str:
    """Nullable / LowCardinality 래퍼를 제거하고 기본 타입만 반환."""
    s = ch_type
    changed = True
    while changed:
        changed = False
        for prefix in ("Nullable(", "LowCardinality("):
            if s.startswith(prefix) and s.endswith(")"):
                s = s[len(prefix) : -1]
                changed = True
    return s


def _extract_ch_datetime_tz(ch_type: str) -> str | None:
    """DateTime / DateTime64 타입 문자열에서 timezone 이름 추출. 없으면 None."""
    base = _unwrap_ch_type(ch_type)
    m = re.search(r"DateTime(?:64)?\([^)]*'([^']+)'\s*\)", base)
    return m.group(1) if m else None


def _ch_datetime_tzinfo(ch_type: str):
    """naive PG timestamp 에 부착할 tzinfo.

    CH 컬럼이 tz 를 가지면 그 tz 로, 그렇지 않으면 UTC 로 fallback.
    (tz 없는 DateTime/DateTime64 에 naive datetime 을 그냥 넘기면
     clickhouse-driver 가 system tz 로 해석해서 값이 흔들리므로 UTC 부착.)
    """
    tz_name = _extract_ch_datetime_tz(ch_type)
    if not tz_name or tz_name.upper() == "UTC":
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


_RELATIVE_RE = re.compile(r"^(\d+)\s*([dhm])$", re.IGNORECASE)


def _parse_relative_to_timedelta(raw: str) -> timedelta | None:
    """상대 시간 표현('30d'/'12h'/'90m')을 timedelta 로 변환. 절대값이면 None."""
    m = _RELATIVE_RE.match(raw.strip())
    if not m:
        return None
    amount = int(m.group(1))
    unit = m.group(2).lower()
    return {"d": timedelta(days=amount), "h": timedelta(hours=amount), "m": timedelta(minutes=amount)}[unit]


def _resolve_sync_since(raw: str) -> str:
    """sync_since 값을 ISO timestamp 문자열로 변환.

    지원 형식:
      - 상대: "30d" (일), "12h" (시간), "90m" (분)
      - 절대: ISO 8601 timestamp (그대로 반환)
    """
    delta = _parse_relative_to_timedelta(raw)
    if delta:
        return (datetime.now() - delta).isoformat()
    return raw


def _parse_watermark_overlap(raw) -> Decimal | None:
    """숫자형 watermark 의 lookback 크기를 Decimal 로 변환."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError("watermark_overlap must be a non-negative number")

    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError) as e:
        raise ValueError("watermark_overlap must be a non-negative number") from e

    if not value.is_finite() or value < 0:
        raise ValueError("watermark_overlap must be a non-negative number")
    if value == 0:
        return None
    return value


def _apply_watermark_overlap(src_table: str, watermark, raw_overlap):
    """숫자형 watermark 에 watermark_overlap 을 적용한 cutoff 를 반환."""
    overlap = _parse_watermark_overlap(raw_overlap)
    if overlap is None:
        return watermark

    try:
        value = Decimal(str(watermark).strip())
    except (InvalidOperation, ValueError) as e:
        raise ValueError(
            f"{src_table}: watermark_overlap requires a numeric watermark value"
        ) from e

    if not value.is_finite():
        raise ValueError(
            f"{src_table}: watermark_overlap requires a numeric watermark value"
        )

    cutoff = value - overlap
    if cutoff == cutoff.to_integral_value():
        return int(cutoff)
    return cutoff


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"


def _quote_ch_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _extract_ch_key_columns(expr: list[str] | tuple[str, ...] | str | None) -> set[str]:
    if isinstance(expr, (list, tuple)):
        return {str(col) for col in expr}
    return set()


def _format_ch_key_expr(expr: list[str] | tuple[str, ...] | str, *, name: str) -> str:
    if isinstance(expr, str):
        value = expr.strip()
        if not value:
            raise ValueError(f"{name} must not be empty")
        return value
    if isinstance(expr, (list, tuple)):
        if not expr:
            raise ValueError(f"{name} must not be empty")
        return "(" + ", ".join(_quote_ch_identifier(str(col)) for col in expr) + ")"
    raise ValueError(f"{name} must be a string or list of column names")


def _format_ch_index_expr(index: dict) -> str:
    if "expr" in index:
        return str(index["expr"]).strip()
    if "expression" in index:
        return str(index["expression"]).strip()
    if "column" in index:
        return _quote_ch_identifier(str(index["column"]))
    if "columns" in index:
        columns = index["columns"]
        if isinstance(columns, str):
            return _quote_ch_identifier(columns)
        if isinstance(columns, (list, tuple)) and columns:
            quoted = ", ".join(_quote_ch_identifier(str(col)) for col in columns)
            return f"({quoted})" if len(columns) > 1 else quoted
    raise ValueError("index must define expr, expression, column, or columns")


def _format_ch_index(index: dict | str) -> str:
    if isinstance(index, str):
        clause = index.strip()
        if not clause:
            raise ValueError("index clause must not be empty")
        return clause if clause.upper().startswith("INDEX ") else f"INDEX {clause}"

    if not isinstance(index, dict):
        raise ValueError("index must be a string or object")

    missing = [key for key in ("name", "type", "granularity") if key not in index]
    if missing:
        raise ValueError(f"index missing required field(s): {', '.join(missing)}")

    expr = _format_ch_index_expr(index)
    if not expr:
        raise ValueError("index expression must not be empty")

    return (
        f"INDEX {_quote_ch_identifier(str(index['name']))} {expr} "
        f"TYPE {index['type']} GRANULARITY {index['granularity']}"
    )


def _normalize_ch_indexes(indexes) -> list[dict | str]:
    if not indexes:
        return []
    if isinstance(indexes, (dict, str)):
        return [indexes]
    if isinstance(indexes, (list, tuple)):
        return list(indexes)
    raise ValueError("indexes must be a string, object, or list")


def _format_ch_setting_value(value) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        raw = value.strip()
        if (
            re.match(r"^-?\d+(\.\d+)?$", raw)
            or raw.startswith("'")
            or raw.startswith("[")
            or raw.startswith("(")
        ):
            return raw
        return "'" + raw.replace("\\", "\\\\").replace("'", "\\'") + "'"
    raise ValueError(f"unsupported ClickHouse setting value: {value!r}")


def _format_ch_settings(
    settings: dict | list[str] | tuple[str, ...] | str | None,
) -> str | None:
    if not settings:
        return None
    if isinstance(settings, str):
        clause = settings.strip()
        if not clause:
            return None
        if clause.upper().startswith("SETTINGS "):
            return clause[len("SETTINGS ") :].strip()
        return clause
    if isinstance(settings, (list, tuple)):
        clauses = [str(item).strip() for item in settings if str(item).strip()]
        return ", ".join(clauses) if clauses else None
    if isinstance(settings, dict):
        clauses = [
            f"{key} = {_format_ch_setting_value(value)}"
            for key, value in settings.items()
        ]
        return ", ".join(clauses) if clauses else None
    raise ValueError("settings must be a string, list, or object")


def _quote_ch_string(value) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _normalize_full_copy_strategy(value) -> str:
    raw = str(value or "python_stream").strip().lower()
    aliases = {
        "direct": "clickhouse_postgresql",
        "clickhouse_direct": "clickhouse_postgresql",
        "postgresql": "clickhouse_postgresql",
        "postgresql_table_function": "clickhouse_postgresql",
        "python": "python_stream",
        "stream": "python_stream",
    }
    strategy = aliases.get(raw, raw)
    if strategy not in {"clickhouse_postgresql", "python_stream"}:
        raise ValueError(
            "full_copy_strategy must be 'clickhouse_postgresql' or 'python_stream'"
        )
    return strategy


def _ch_default_expr(ch_type: str) -> str | None:
    base = _unwrap_ch_type(ch_type)
    if base == "String":
        return "''"
    if base == "UUID":
        return "toUUID('00000000-0000-0000-0000-000000000000')"
    if base == "Date":
        return "toDate('1970-01-01')"
    if base == "DateTime" or base.startswith("DateTime("):
        return "toDateTime('1970-01-01 00:00:00')"
    if base.startswith("DateTime64"):
        m = re.match(r"DateTime64\((\d+)", base)
        scale = m.group(1) if m else "6"
        return f"toDateTime64('1970-01-01 00:00:00', {scale})"
    if base.startswith(("Int", "UInt", "Float", "Decimal")):
        return "0"
    return None


# ── Job ─────────────────────────────────────────────────────────


class Pg2ChSyncJob(Job):
    name = "pg2ch_sync"
    label = "PG→CH 동기화"
    description = "PostgreSQL → ClickHouse 테이블 동기화 (full copy / incremental)"
    default_args: dict = {"config": "pg2ch_config.json"}
    scope = "pipeline"

    _WATERMARK_TABLE = "_pg2ch_watermarks"

    # ── entry point ──────────────────────────────────────────────

    def run(self, *, config: str = "pg2ch_config.json", **kwargs) -> JobResult:
        try:
            from clickhouse_driver import Client as _CHClient  # noqa: F401
        except ImportError:
            return JobResult(
                False,
                "clickhouse-driver 미설치: uv sync --extra clickhouse",
            )

        cfg = self._load_config(config)
        tables: list[dict] = cfg["tables"]
        defaults = cfg.get("defaults", {})

        src_db = get_database(cfg["source"])
        tgt_db = get_database(cfg["target"])
        if not src_db:
            return JobResult(False, f"Source DB '{cfg['source']}' not found in databases.json")
        if not tgt_db:
            return JobResult(False, f"Target DB '{cfg['target']}' not found in databases.json")
        if src_db.get("type") != "postgresql":
            return JobResult(False, f"Source must be postgresql, got '{src_db.get('type')}'")
        if tgt_db.get("type") != "clickhouse":
            return JobResult(False, f"Target must be clickhouse, got '{tgt_db.get('type')}'")

        total_rows = 0
        errors: list[str] = []

        for tc in tables:
            merged = {**defaults, **tc}
            if isinstance(defaults.get("settings"), dict) and isinstance(
                tc.get("settings"), dict
            ):
                merged["settings"] = {**defaults["settings"], **tc["settings"]}
            try:
                total_rows += self._sync_table(src_db, tgt_db, merged, cfg)
            except Exception as e:
                self.logger.exception("Failed: %s", tc.get("source_table", "?"))
                errors.append(f"{tc.get('source_table', '?')}: {e}")

        msg = f"Synced {total_rows} rows across {len(tables)} table(s)"

        if errors:
            return JobResult(
                success=False,
                message=msg + f", {len(errors)} error(s): " + "; ".join(errors),
                rows_affected=total_rows,
            )
        return JobResult(
            success=True,
            message=msg,
            rows_affected=total_rows,
        )

    # ── config ───────────────────────────────────────────────────

    @staticmethod
    def _load_config(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    # ── connections ──────────────────────────────────────────────

    @staticmethod
    def _pg_connect(db_cfg: dict):
        import psycopg2

        return psycopg2.connect(
            host=db_cfg["host"],
            port=int(db_cfg.get("port", 5432)),
            dbname=db_cfg["dbname"],
            user=db_cfg.get("user", ""),
            password=db_cfg.get("password", ""),
        )

    @staticmethod
    def _ch_connect(db_cfg: dict, sync_cfg: dict):
        from clickhouse_driver import Client

        port = sync_cfg.get("ch_native_port") or int(db_cfg.get("port", 9000))
        return Client(
            host=db_cfg["host"],
            port=port,
            database=db_cfg["dbname"],
            user=db_cfg.get("user", "default"),
            password=db_cfg.get("password", ""),
        )

    # ── watermark ────────────────────────────────────────────────

    def _ensure_watermark_table(self, ch, db_name: str) -> None:
        tbl = self._WATERMARK_TABLE
        ddl = (
            f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{tbl}` ("
            "  config_key String,"
            "  timestamp_column String,"
            "  value String,"
            "  updated_at DateTime64(3)"
            ") ENGINE = ReplacingMergeTree(updated_at)"
            " ORDER BY (config_key, timestamp_column)"
        )
        ch.execute(ddl)

    def _get_watermark(self, ch, db_name: str, key: str, ts_col: str) -> str | None:
        tbl = self._WATERMARK_TABLE
        rows = ch.execute(
            f"SELECT value FROM `{db_name}`.`{tbl}` FINAL"
            " WHERE config_key = %(key)s AND timestamp_column = %(ts_col)s"
            " LIMIT 1",
            {"key": key, "ts_col": ts_col},
        )
        if rows:
            return rows[0][0]
        return None

    def _save_watermark(self, ch, db_name: str, key: str, ts_col: str, value) -> None:
        tbl = self._WATERMARK_TABLE
        val_str = value.isoformat() if isinstance(value, datetime) else str(value)
        ch.execute(
            f"INSERT INTO `{db_name}`.`{tbl}`"
            " (config_key, timestamp_column, value, updated_at) VALUES",
            [(key, ts_col, val_str, datetime.now())],
        )

    # ── post-sync optimize ──────────────────────────────────────

    def _optimize_table(
        self,
        ch,
        db_name: str,
        table_name: str,
        *,
        partitions=None,
        mutations_sync: int = 2,
    ) -> None:
        """OPTIMIZE TABLE ... FINAL 실행하여 즉시 merge/dedup.

        partitions 가 주어지면 해당 파티션만, 아니면 전체 테이블을 대상으로 한다.
        mutations_sync 는 ClickHouse SETTINGS 로 전달되어 동기 대기 여부를 결정한다
        (0=async, 1=현재 서버 대기, 2=모든 replica 대기).
        """
        if partitions is None:
            targets: list[str | None] = [None]
        elif isinstance(partitions, (str, int)):
            targets = [str(partitions)]
        elif isinstance(partitions, (list, tuple)):
            targets = [str(p) for p in partitions if p is not None and str(p) != ""]
            if not targets:
                targets = [None]
        else:
            raise ValueError(
                "optimize_partitions must be a string, number, or list of those"
            )

        for partition in targets:
            sql = f"OPTIMIZE TABLE `{db_name}`.`{table_name}`"
            if partition is not None:
                sql += f" PARTITION {_quote_ch_string(partition)}"
            sql += " FINAL"
            sql += f" SETTINGS mutations_sync = {int(mutations_sync)}"
            self.logger.info(
                "%s.%s: OPTIMIZE FINAL%s",
                db_name,
                table_name,
                f" partition {partition}" if partition is not None else "",
            )
            ch.execute(sql)

    # ── PG schema introspection ──────────────────────────────────

    @staticmethod
    def _get_pg_columns(pg_conn, schema: str, table: str) -> list[dict]:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable,"
                "       numeric_precision, numeric_scale"
                " FROM information_schema.columns"
                " WHERE table_schema = %s AND table_name = %s"
                " ORDER BY ordinal_position",
                (schema, table),
            )
            return [
                {
                    "name": r[0],
                    "pg_type": r[1],
                    "nullable": r[2] == "YES",
                    "precision": r[3],
                    "scale": r[4],
                }
                for r in cur.fetchall()
            ]

    # ── column mapping ───────────────────────────────────────────

    @staticmethod
    def _build_ch_columns(
        pg_cols: list[dict],
        drop_columns: set[str],
        column_overrides: dict[str, str | dict],
        order_by: list[str],
        use_nullable: bool = True,
    ) -> list[dict]:
        """PG 컬럼 → CH 컬럼 정의 리스트.

        ORDER BY 컬럼은 Nullable 제거 (ClickHouse 제약).
        use_nullable=False 이면 모든 컬럼을 non-nullable 로 생성하고
        NULL 유입 시 타입별 기본값으로 대체한다.

        column_overrides 값은 두 형태를 허용한다:
          - 문자열: CH 타입 그대로 (예: "LowCardinality(String)")
          - 객체: {"type": "DateTime64(3, 'UTC')", "parse_format": "%Y%m%d %H%M%S%f", ...}
            추가 키:
              parse_format: strptime 포맷. PG text → CH DateTime* 변환에 사용.
              timezone: parse 결과에 부착할 tz (생략 시 CH 컬럼 tz → UTC fallback)
        """
        order_by_set = set(order_by)
        result = []
        for col in pg_cols:
            name = col["name"]
            if name in drop_columns:
                continue
            override_meta: dict | None = None
            if name in column_overrides:
                ov = column_overrides[name]
                if isinstance(ov, dict):
                    if "type" not in ov:
                        raise ValueError(
                            f"column_overrides[{name}] must include 'type'"
                        )
                    ch_type = ov["type"]
                    override_meta = {k: v for k, v in ov.items() if k != "type"}
                else:
                    ch_type = ov
            else:
                nullable = use_nullable and col["nullable"] and name not in order_by_set
                ch_type = _pg_type_to_ch(
                    col["pg_type"],
                    nullable=nullable,
                    precision=col["precision"],
                    scale=col["scale"],
                )
            entry = {"name": name, "ch_type": ch_type, "pg_type": col["pg_type"]}
            if override_meta:
                entry["override"] = override_meta
            result.append(entry)
        return result

    # ── CH DDL ───────────────────────────────────────────────────

    def _ensure_ch_table(
        self,
        ch,
        db_name: str,
        table: str,
        columns: list[dict],
        order_by: list[str] | str,
        partition_by: str | None,
        engine: str,
        primary_key: list[str] | str | None = None,
        indexes: list[dict | str] | tuple[dict | str, ...] | None = None,
        settings: dict | list[str] | tuple[str, ...] | str | None = None,
    ) -> None:
        definitions = [f"`{c['name']}` {c['ch_type']}" for c in columns]
        definitions.extend(
            _format_ch_index(index) for index in _normalize_ch_indexes(indexes)
        )
        col_defs = ", ".join(definitions)

        ddl = (
            f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table}` "
            f"({col_defs}) ENGINE = {engine}"
        )
        if partition_by:
            ddl += f" PARTITION BY {partition_by}"
        ddl += f" ORDER BY {_format_ch_key_expr(order_by, name='order_by')}"
        if primary_key:
            ddl += (
                f" PRIMARY KEY "
                f"{_format_ch_key_expr(primary_key, name='primary_key')}"
            )
        settings_clause = _format_ch_settings(settings)
        if settings_clause:
            ddl += f" SETTINGS {settings_clause}"

        self.logger.info("Ensuring table: %s.%s", db_name, table)
        self.logger.debug("DDL: %s", ddl)
        ch.execute(ddl)

    # ── direct full copy ──────────────────────────────────────────

    @staticmethod
    def _postgresql_table_function(src_cfg: dict, src_schema: str, src_name: str, tc: dict) -> str:
        host = (
            tc.get("clickhouse_postgresql_host")
            or tc.get("postgres_host_for_clickhouse")
            or src_cfg["host"]
        )
        port = (
            tc.get("clickhouse_postgresql_port")
            or tc.get("postgres_port_for_clickhouse")
            or src_cfg.get("port", 5432)
        )
        host_port = f"{host}:{int(port)}"
        args = [
            host_port,
            src_cfg["dbname"],
            src_name,
            src_cfg.get("user", ""),
            src_cfg.get("password", ""),
            src_schema,
        ]
        return "postgresql(" + ", ".join(_quote_ch_string(arg) for arg in args) + ")"

    @staticmethod
    def _direct_select_exprs(columns: list[dict]) -> str:
        exprs = []
        for col in columns:
            quoted = _quote_ch_identifier(col["name"])
            if col["ch_type"].startswith("Nullable("):
                exprs.append(quoted)
                continue

            default_expr = _ch_default_expr(col["ch_type"])
            if default_expr is None:
                exprs.append(quoted)
            else:
                exprs.append(f"ifNull({quoted}, {default_expr}) AS {quoted}")
        return ", ".join(exprs)

    def _full_copy_via_clickhouse_postgresql(
        self,
        ch,
        src_cfg: dict,
        src_schema: str,
        src_name: str,
        tgt_db: str,
        tgt_name: str,
        ch_columns: list[dict],
        col_names: list[str],
        ts_col: str | None,
        sync_since: str | None,
        wm_col: str | None,
        tc: dict,
        sync_cfg: dict,
    ) -> tuple[int, object | None]:
        col_insert = ", ".join(_quote_ch_identifier(c) for c in col_names)
        source = self._postgresql_table_function(src_cfg, src_schema, src_name, tc)
        query = (
            f"INSERT INTO `{tgt_db}`.`{tgt_name}` ({col_insert}) "
            f"SELECT {self._direct_select_exprs(ch_columns)} FROM {source}"
        )

        if sync_since and ts_col:
            query += f" WHERE {_quote_ch_identifier(ts_col)} >= {_quote_ch_string(sync_since)}"

        query_settings = tc.get("full_copy_query_settings")
        if query_settings is None:
            query_settings = sync_cfg.get("full_copy_query_settings")
        settings_clause = _format_ch_settings(query_settings)
        if settings_clause:
            query += f" SETTINGS {settings_clause}"

        self.logger.info("%s.%s: direct full copy via ClickHouse postgresql()", src_schema, src_name)
        ch.execute(query)

        count_expr = "count()"
        max_expr = (
            f", max({_quote_ch_identifier(wm_col)})"
            if wm_col and wm_col in col_names
            else ""
        )
        stats = ch.execute(
            f"SELECT {count_expr}{max_expr} FROM `{tgt_db}`.`{tgt_name}`"
        )
        if not stats:
            return 0, None

        row = stats[0]
        total_rows = int(row[0] or 0)
        max_wm = row[1] if max_expr and len(row) > 1 else None
        return total_rows, max_wm

    # ── row transform ────────────────────────────────────────────

    @staticmethod
    def _build_transformer(columns: list[dict]):
        """PG 결과 행 → CH INSERT 호환 변환 함수. 불필요하면 None 반환."""
        import json as _json

        transforms: dict[int, callable] = {}
        # non-nullable CH 컬럼에 NULL 유입 시 타입별 기본값으로 치환
        _CH_DEFAULTS: dict[str, object] = {
            "String": "",
            "UUID": "00000000-0000-0000-0000-000000000000",
            "Date": date(1970, 1, 1),
        }
        null_coerce: dict[int, object] = {}
        for i, col in enumerate(columns):
            pg_t = col["pg_type"]
            ch_type = col["ch_type"]
            base = _unwrap_ch_type(ch_type)

            if not ch_type.startswith("Nullable("):
                if base in _CH_DEFAULTS:
                    null_coerce[i] = _CH_DEFAULTS[base]
                elif base.startswith(("Int", "UInt", "Float", "Decimal")):
                    null_coerce[i] = 0
                elif base.startswith("DateTime"):
                    # clickhouse-driver expects datetime objects for DateTime/DateTime64
                    # under types_check=True. If the CH type carries a tz, attach the
                    # *same* tz so the wall-clock value is preserved.
                    tz_name = _extract_ch_datetime_tz(ch_type)
                    null_coerce[i] = (
                        datetime(1970, 1, 1, tzinfo=_ch_datetime_tzinfo(ch_type))
                        if tz_name
                        else datetime(1970, 1, 1)
                    )

            override = col.get("override") or {}
            parse_format = override.get("parse_format")

            if (
                parse_format
                and pg_t in ("character varying", "character", "text")
                and base.startswith("DateTime")
            ):
                # PG text → CH DateTime*: config 의 strptime 포맷으로 파싱.
                # 실패 시 즉시 raise (엄격 모드).
                _fmt = parse_format
                _tz_override = override.get("timezone")
                if _tz_override:
                    try:
                        from zoneinfo import ZoneInfo

                        _tz = ZoneInfo(_tz_override)
                    except Exception as _e:
                        raise ValueError(
                            f"column_overrides[{col['name']}].timezone "
                            f"invalid: {_tz_override}"
                        ) from _e
                else:
                    _tz = _ch_datetime_tzinfo(ch_type)
                _col_name = col["name"]

                def _dtparse(v, _fmt=_fmt, _tz=_tz, _name=_col_name):
                    if v is None:
                        return v
                    if not isinstance(v, str):
                        raise ValueError(
                            f"{_name}: expected string for parse_format, "
                            f"got {type(v).__name__}"
                        )
                    try:
                        dt = datetime.strptime(v, _fmt)
                    except ValueError as _e:
                        raise ValueError(
                            f"{_name}: failed to parse {v!r} with "
                            f"format {_fmt!r}: {_e}"
                        ) from _e
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=_tz)
                    return dt

                transforms[i] = _dtparse

            elif pg_t == "timestamp without time zone" and base.startswith("DateTime"):
                # psycopg2 returns naive datetime for PG `timestamp`. clickhouse-driver
                # interprets naive datetimes against the column's tz (or system tz),
                # which shifts the wall-clock value. Attach the column's tz directly so
                # the numbers users see in CH match what they see in PG.
                _tz = _ch_datetime_tzinfo(ch_type)

                def _tsconv(v, _tz=_tz):
                    if v is not None and getattr(v, "tzinfo", None) is None:
                        return v.replace(tzinfo=_tz)
                    return v

                transforms[i] = _tsconv

            elif pg_t in ("json", "jsonb"):

                def _jconv(v, _j=_json):
                    if v is not None and not isinstance(v, str):
                        return _j.dumps(v, ensure_ascii=False, default=str)
                    return v

                transforms[i] = _jconv

            elif pg_t == "boolean":

                def _bconv(v):
                    return int(v) if v is not None else v

                transforms[i] = _bconv

            elif pg_t in (
                "character varying",
                "character",
                "text",
            ) and base.startswith(("Int", "UInt", "Float", "Decimal")):
                # PG string → CH numeric (column_overrides 로 타입 변경 시)
                if base.startswith("Float"):
                    _conv = float
                elif base.startswith("Decimal"):
                    from decimal import Decimal as _Dec

                    _conv = _Dec
                else:
                    _conv = int

                def _nconv(v, _c=_conv):
                    if v is not None and isinstance(v, str):
                        return _c(v)
                    return v

                transforms[i] = _nconv

            elif pg_t == "numeric" and base.startswith(("Int", "UInt", "Float")):
                # PG numeric (Decimal) → CH Int/Float (column_overrides 로 타입 변경 시)
                _conv = float if base.startswith("Float") else int

                def _dconv(v, _c=_conv):
                    if v is not None:
                        return _c(v)
                    return v

                transforms[i] = _dconv

            elif base == "String" and pg_t not in (
                "character varying",
                "character",
                "text",
            ):
                # PG non-string (bytea, interval, inet, array 등) → CH String
                def _sconv(v):
                    if v is not None and not isinstance(v, str):
                        if isinstance(v, (bytes, memoryview)):
                            return bytes(v).hex()
                        return str(v)
                    return v

                transforms[i] = _sconv

        if not transforms and not null_coerce:
            return None

        def transform(row: tuple) -> tuple:
            lst = list(row)
            for idx, fn in transforms.items():
                lst[idx] = fn(lst[idx])
            for idx, default in null_coerce.items():
                if lst[idx] is None:
                    lst[idx] = default
            return tuple(lst)

        return transform

    # ── per-table sync ───────────────────────────────────────────

    def _sync_table(
        self,
        src_cfg: dict,
        tgt_cfg: dict,
        tc: dict,
        sync_cfg: dict,
    ) -> int:
        src_table: str = tc["source_table"]
        tgt_table: str = tc["target_table"]
        ts_col: str | None = tc.get("timestamp_column")
        wm_col: str | None = tc.get("watermark_column") or ts_col
        raw_since: str | None = tc.get("sync_since")
        sync_since: str | None = _resolve_sync_since(raw_since) if raw_since else None
        drop_cols = set(tc.get("drop_columns", []))
        col_overrides: dict = tc.get("column_overrides", {})
        order_by: list[str] | str = tc["order_by"]
        primary_key: list[str] | str | None = tc.get("primary_key")
        indexes = tc.get("indexes") or tc.get("indices") or tc.get("index") or []
        settings: dict | list[str] | str | None = tc.get("settings")
        partition_by: str | None = tc.get("partition_by")
        engine: str = tc.get("engine", "ReplacingMergeTree")
        batch_size: int = tc.get("batch_size", 100_000)
        overlap_min: int = tc.get("overlap_minutes", 0)
        watermark_overlap = tc.get("watermark_overlap", 0)
        use_nullable: bool = tc.get("use_nullable", True)
        optimize_after_sync: bool = tc.get("optimize_after_sync", False)
        optimize_partitions = tc.get("optimize_partitions")
        optimize_mutations_sync: int = int(tc.get("optimize_mutations_sync", 2))
        full_copy_strategy = _normalize_full_copy_strategy(
            tc.get("full_copy_strategy", sync_cfg.get("full_copy_strategy"))
        )
        full_copy_fallback = tc.get(
            "full_copy_fallback_to_python",
            sync_cfg.get("full_copy_fallback_to_python", True),
        )

        if sync_since and not ts_col:
            raise ValueError(
                f"{src_table}: sync_since requires timestamp_column to be set"
            )

        # schema.table 파싱
        src_schema, src_name = (
            src_table.split(".", 1) if "." in src_table else ("public", src_table)
        )
        tgt_db, tgt_name = (
            tgt_table.split(".", 1) if "." in tgt_table else (tgt_cfg["dbname"], tgt_table)
        )

        pg_conn = None
        ch = None
        try:
            pg_conn = self._pg_connect(src_cfg)
            ch = self._ch_connect(tgt_cfg, sync_cfg)

            # 1) PG 컬럼 조회
            pg_cols = self._get_pg_columns(pg_conn, src_schema, src_name)
            if not pg_cols:
                raise ValueError(f"Table {src_table} not found or has no columns")

            # 2) CH 컬럼 매핑
            key_columns = _extract_ch_key_columns(order_by) | _extract_ch_key_columns(
                primary_key
            )
            ch_columns = self._build_ch_columns(
                pg_cols, drop_cols, col_overrides, list(key_columns), use_nullable
            )
            col_names = [c["name"] for c in ch_columns]
            col_list_pg = ", ".join(f'"{c}"' for c in col_names)

            # 3) CH 테이블 생성
            self._ensure_ch_table(
                ch,
                tgt_db,
                tgt_name,
                ch_columns,
                order_by,
                partition_by,
                engine,
                primary_key=primary_key,
                indexes=indexes,
                settings=settings,
            )

            # 4) 동기화 모드 결정
            self._ensure_watermark_table(ch, tgt_db)
            wm_key = f"{sync_cfg['source']}.{src_table}"
            watermark = self._get_watermark(ch, tgt_db, wm_key, wm_col) if wm_col else None

            direct_full_copy_done = False
            total_rows = 0
            max_wm = None

            if watermark:
                cutoff = watermark
                applied_time_overlap = False
                if overlap_min:
                    try:
                        wm_dt = datetime.fromisoformat(watermark)
                        cutoff = (
                            wm_dt - timedelta(minutes=overlap_min)
                        ).isoformat()
                        applied_time_overlap = True
                    except (ValueError, TypeError):
                        pass  # 숫자형 watermark 는 watermark_overlap 로 처리

                if not applied_time_overlap:
                    cutoff = _apply_watermark_overlap(
                        src_table, watermark, watermark_overlap
                    )

                # WHERE 절 구성
                conditions = [f'"{wm_col}" > %s']
                params_list: list = [cutoff]

                if sync_since and ts_col:
                    if ts_col == wm_col:
                        # 동일 컬럼: sync_since 가 cutoff 보다 크면 대체
                        if sync_since > cutoff:
                            params_list[0] = sync_since
                    else:
                        # 별도 컬럼: timestamp 필터 추가
                        conditions.append(f'"{ts_col}" >= %s')
                        params_list.append(sync_since)

                where = " AND ".join(conditions)
                query = (
                    f'SELECT {col_list_pg} FROM "{src_schema}"."{src_name}" '
                    f'WHERE {where} ORDER BY "{wm_col}"'
                )
                params: tuple | None = tuple(params_list)
                self.logger.info(
                    "%s: incremental from %s (time overlap %dm, watermark overlap %s)",
                    src_table,
                    cutoff,
                    overlap_min,
                    watermark_overlap,
                )
            else:
                # Full copy
                self.logger.info("%s: full copy — truncating target", src_table)
                ch.execute(f"TRUNCATE TABLE IF EXISTS `{tgt_db}`.`{tgt_name}`")

                query = (
                    f'SELECT {col_list_pg} FROM "{src_schema}"."{src_name}"'
                )
                if sync_since and ts_col:
                    query += f' WHERE "{ts_col}" >= %s'
                    params = (sync_since,)
                    self.logger.info(
                        "%s: full copy with sync_since %s", src_table, sync_since
                    )
                else:
                    params = None
                order_col = wm_col or ts_col
                if order_col:
                    query += f' ORDER BY "{order_col}"'

                if full_copy_strategy == "clickhouse_postgresql":
                    try:
                        total_rows, max_wm = self._full_copy_via_clickhouse_postgresql(
                            ch,
                            src_cfg,
                            src_schema,
                            src_name,
                            tgt_db,
                            tgt_name,
                            ch_columns,
                            col_names,
                            ts_col,
                            sync_since,
                            wm_col,
                            tc,
                            sync_cfg,
                        )
                        direct_full_copy_done = True
                    except Exception as e:
                        if not full_copy_fallback:
                            raise
                        self.logger.warning(
                            "%s: direct full copy failed; falling back to "
                            "python_stream: %s",
                            src_table,
                            e,
                        )
                        ch.execute(f"TRUNCATE TABLE IF EXISTS `{tgt_db}`.`{tgt_name}`")

            # 5) 스트리밍 전송
            if not direct_full_copy_done:
                transformer = self._build_transformer(ch_columns)
                col_insert = ", ".join(f"`{c}`" for c in col_names)
                insert_sql = (
                    f"INSERT INTO `{tgt_db}`.`{tgt_name}` ({col_insert}) VALUES"
                )

                cursor = pg_conn.cursor(name="pg2ch_stream")
                cursor.itersize = batch_size

                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                wm_idx = (
                    col_names.index(wm_col)
                    if wm_col and wm_col in col_names
                    else None
                )

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    if wm_idx is not None:
                        for row in rows:
                            val = row[wm_idx]
                            if val is not None and (max_wm is None or val > max_wm):
                                max_wm = val

                    if transformer:
                        rows = [transformer(r) for r in rows]

                    ch.execute(insert_sql, rows, types_check=True)
                    total_rows += len(rows)
                    self.logger.info(
                        "%s: batch %d rows (total %d)", src_table, len(rows), total_rows
                    )

                cursor.close()
            pg_conn.rollback()  # read 트랜잭션 정리

            # 6) Watermark 저장
            if wm_col and max_wm is not None:
                self._save_watermark(ch, tgt_db, wm_key, wm_col, max_wm)
                self.logger.info("%s: watermark → %s", src_table, max_wm)

            # 7) Post-sync OPTIMIZE (즉시 dedup / merge)
            if optimize_after_sync:
                self._optimize_table(
                    ch,
                    tgt_db,
                    tgt_name,
                    partitions=optimize_partitions,
                    mutations_sync=optimize_mutations_sync,
                )

            mode = "incremental" if watermark else "full copy"
            self.logger.info("%s: %s complete — %d rows", src_table, mode, total_rows)
            return total_rows

        finally:
            if ch:
                ch.disconnect()
            if pg_conn:
                pg_conn.close()
