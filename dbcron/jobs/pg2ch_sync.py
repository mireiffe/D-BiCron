"""PostgreSQL → ClickHouse 동기화 Job.

PG 소스에서 CH 타겟으로 테이블 데이터를 동기화합니다.

동작 모드:
  - Full copy: watermark 이력이 없으면 대상 테이블 TRUNCATE 후 전체 복사
  - Incremental: 기존 watermark 이후 변경분만 전송
    (ReplacingMergeTree 사용 시 overlap_minutes 로 중복 허용, merge 시 자동 제거)

기능:
  - Column drop: 특정 소스 컬럼 제외
  - Column type override: LowCardinality, Decimal 등 CH 타입 직접 지정
  - ORDER BY / PARTITION BY / ENGINE 설정
  - 자동 테이블 생성 (CREATE TABLE IF NOT EXISTS)
  - Watermark 기반 증분 동기화 (timestamp / integer 모두 지원)
  - sync_since: timestamp_column 기반 하한 필터 (full copy / incremental 공통)
  - use_nullable: false 설정 시 PG nullable 컬럼을 CH Nullable 대신 기본값으로 대체

설정:
  PG2CH_CONFIG 환경변수로 JSON 설정 파일 경로 지정 (기본: pg2ch_config.json)
  source / target 은 databases.json 의 DB ID
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta

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
    "timestamp without time zone": "DateTime64(6)",
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


_RELATIVE_RE = re.compile(r"^(\d+)\s*([dhm])$", re.IGNORECASE)


def _resolve_sync_since(raw: str) -> str:
    """sync_since 값을 ISO timestamp 문자열로 변환.

    지원 형식:
      - 상대: "30d" (일), "12h" (시간), "90m" (분)
      - 절대: ISO 8601 timestamp (그대로 반환)
    """
    m = _RELATIVE_RE.match(raw.strip())
    if m:
        amount = int(m.group(1))
        unit = m.group(2).lower()
        delta = {"d": timedelta(days=amount), "h": timedelta(hours=amount), "m": timedelta(minutes=amount)}[unit]
        return (datetime.now() - delta).isoformat()
    return raw


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"


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
            try:
                n = self._sync_table(src_db, tgt_db, merged, cfg)
                total_rows += n
            except Exception as e:
                self.logger.exception("Failed: %s", tc.get("source_table", "?"))
                errors.append(f"{tc.get('source_table', '?')}: {e}")

        if errors:
            return JobResult(
                success=False,
                message=f"Synced {total_rows} rows, {len(errors)} error(s): "
                + "; ".join(errors),
                rows_affected=total_rows,
            )
        return JobResult(
            success=True,
            message=f"Synced {total_rows} rows across {len(tables)} table(s)",
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
        column_overrides: dict[str, str],
        order_by: list[str],
        use_nullable: bool = True,
    ) -> list[dict]:
        """PG 컬럼 → CH 컬럼 정의 리스트.

        ORDER BY 컬럼은 Nullable 제거 (ClickHouse 제약).
        use_nullable=False 이면 모든 컬럼을 non-nullable 로 생성하고
        NULL 유입 시 타입별 기본값으로 대체한다.
        """
        order_by_set = set(order_by)
        result = []
        for col in pg_cols:
            name = col["name"]
            if name in drop_columns:
                continue
            if name in column_overrides:
                ch_type = column_overrides[name]
            else:
                nullable = use_nullable and col["nullable"] and name not in order_by_set
                ch_type = _pg_type_to_ch(
                    col["pg_type"],
                    nullable=nullable,
                    precision=col["precision"],
                    scale=col["scale"],
                )
            result.append(
                {"name": name, "ch_type": ch_type, "pg_type": col["pg_type"]}
            )
        return result

    # ── CH DDL ───────────────────────────────────────────────────

    def _ensure_ch_table(
        self,
        ch,
        db_name: str,
        table: str,
        columns: list[dict],
        order_by: list[str],
        partition_by: str | None,
        engine: str,
    ) -> None:
        col_defs = ", ".join(f"`{c['name']}` {c['ch_type']}" for c in columns)
        order_clause = ", ".join(f"`{c}`" for c in order_by)

        ddl = (
            f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table}` "
            f"({col_defs}) ENGINE = {engine} "
            f"ORDER BY ({order_clause})"
        )
        if partition_by:
            ddl += f" PARTITION BY {partition_by}"

        self.logger.info("Ensuring table: %s.%s", db_name, table)
        self.logger.debug("DDL: %s", ddl)
        ch.execute(ddl)

    # ── CH schema introspection ─────────────────────────────────

    @staticmethod
    def _get_ch_column_types(ch, db_name: str, table_name: str) -> dict[str, str]:
        """기존 CH 테이블의 실제 컬럼 타입 조회."""
        rows = ch.execute(
            "SELECT name, type FROM system.columns "
            "WHERE database = %(db)s AND table = %(tbl)s",
            {"db": db_name, "tbl": table_name},
        )
        return {r[0]: r[1] for r in rows}

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
            "Date": "1970-01-01",
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
                    null_coerce[i] = "1970-01-01 00:00:00"

            if pg_t in ("json", "jsonb"):

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
        raw_since: str | None = tc.get("sync_since")
        sync_since: str | None = _resolve_sync_since(raw_since) if raw_since else None
        drop_cols = set(tc.get("drop_columns", []))
        col_overrides: dict = tc.get("column_overrides", {})
        order_by: list[str] = tc["order_by"]
        partition_by: str | None = tc.get("partition_by")
        engine: str = tc.get("engine", "ReplacingMergeTree")
        batch_size: int = tc.get("batch_size", 100_000)
        overlap_min: int = tc.get("overlap_minutes", 0)
        use_nullable: bool = tc.get("use_nullable", True)

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
            ch_columns = self._build_ch_columns(
                pg_cols, drop_cols, col_overrides, order_by, use_nullable
            )
            col_names = [c["name"] for c in ch_columns]
            col_list_pg = ", ".join(f'"{c}"' for c in col_names)

            # 3) CH 테이블 생성
            self._ensure_ch_table(
                ch, tgt_db, tgt_name, ch_columns, order_by, partition_by, engine
            )

            # 3-1) 기존 테이블의 실제 스키마로 ch_columns 보정
            actual_types = self._get_ch_column_types(ch, tgt_db, tgt_name)
            for col in ch_columns:
                actual = actual_types.get(col["name"])
                if actual and actual != col["ch_type"]:
                    self.logger.warning(
                        "%s: column '%s' type drift — expected %s, actual %s",
                        src_table,
                        col["name"],
                        col["ch_type"],
                        actual,
                    )
                    col["ch_type"] = actual

            # 4) 동기화 모드 결정
            self._ensure_watermark_table(ch, tgt_db)
            wm_key = f"{sync_cfg['source']}.{src_table}"
            watermark = self._get_watermark(ch, tgt_db, wm_key, ts_col) if ts_col else None

            if watermark:
                cutoff = watermark
                if overlap_min:
                    try:
                        wm_dt = datetime.fromisoformat(watermark)
                        cutoff = (
                            wm_dt - timedelta(minutes=overlap_min)
                        ).isoformat()
                    except (ValueError, TypeError):
                        pass  # 정수형 watermark — overlap 미적용

                # sync_since 가 watermark 보다 크면 sync_since 우선
                if sync_since and sync_since > cutoff:
                    cutoff = sync_since

                query = (
                    f'SELECT {col_list_pg} FROM "{src_schema}"."{src_name}" '
                    f'WHERE "{ts_col}" > %s ORDER BY "{ts_col}"'
                )
                params: tuple | None = (cutoff,)
                self.logger.info(
                    "%s: incremental from %s (overlap %dm)",
                    src_table,
                    cutoff,
                    overlap_min,
                )
            else:
                # Full copy
                self.logger.info("%s: full copy — truncating target", src_table)
                ch.execute(f"TRUNCATE TABLE IF EXISTS `{tgt_db}`.`{tgt_name}`")

                query = (
                    f'SELECT {col_list_pg} FROM "{src_schema}"."{src_name}"'
                )
                if sync_since:
                    query += f' WHERE "{ts_col}" >= %s'
                    params = (sync_since,)
                    self.logger.info(
                        "%s: full copy with sync_since %s", src_table, sync_since
                    )
                else:
                    params = None
                if ts_col:
                    query += f' ORDER BY "{ts_col}"'

            # 5) 스트리밍 전송
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

            total_rows = 0
            max_wm = None
            wm_idx = (
                col_names.index(ts_col)
                if ts_col and ts_col in col_names
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

            # 6) Watermark 저장
            if ts_col and max_wm is not None:
                self._save_watermark(ch, tgt_db, wm_key, ts_col, max_wm)
                self.logger.info("%s: watermark → %s", src_table, max_wm)

            mode = "incremental" if watermark else "full copy"
            self.logger.info("%s: %s complete — %d rows", src_table, mode, total_rows)
            return total_rows

        finally:
            if ch:
                ch.disconnect()
            if pg_conn:
                pg_conn.close()
