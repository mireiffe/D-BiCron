"""PostgreSQL → PostgreSQL 동기화 Job.

PG 소스에서 PG 타겟으로 테이블 데이터를 동기화합니다.

동작 모드:
  - Full copy + append: watermark 이력이 없으면 대상 테이블 TRUNCATE 후 전체 복사
  - Full copy + upsert: watermark 이력이 없으면 ON CONFLICT 로 전체 upsert (TRUNCATE 안 함)
  - Incremental: 기존 watermark 이후 변경분만 전송

기능:
  - Column drop: 특정 소스 컬럼 제외
  - Column type override: 타겟 PG 타입 직접 지정
  - 자동 테이블 생성 (CREATE TABLE IF NOT EXISTS)
  - 자동 인덱스 생성 (create_indexes)
  - Watermark 기반 증분 동기화 (timestamp / integer 모두 지원)
  - sync_since: timestamp_column 기반 하한 필터 (full copy / incremental 공통)
  - sync_mode: append (INSERT) / upsert (ON CONFLICT DO UPDATE)
  - source_retention: 동기화 완료 후 소스 오래된 행 자동 삭제

설정:
  PG2PG_CONFIG 환경변수로 JSON 설정 파일 경로 지정 (기본: pg2pg_config.json)
  source / target 은 databases.json 의 DB ID
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta

from psycopg2.extras import execute_values

from ..db import get_database
from .base import Job, JobResult

logger = logging.getLogger(__name__)

# ── PG 타입 재구성 헬퍼 ───────────────────────────────────────

# ARRAY udt_name → PG element type 매핑
_ARRAY_UDT_MAP: dict[str, str] = {
    "_bool": "boolean[]",
    "_int2": "smallint[]",
    "_int4": "integer[]",
    "_int8": "bigint[]",
    "_float4": "real[]",
    "_float8": "double precision[]",
    "_numeric": "numeric[]",
    "_text": "text[]",
    "_varchar": "character varying[]",
    "_char": "character[]",
    "_uuid": "uuid[]",
    "_date": "date[]",
    "_timestamp": "timestamp without time zone[]",
    "_timestamptz": "timestamp with time zone[]",
    "_json": "json[]",
    "_jsonb": "jsonb[]",
    "_inet": "inet[]",
}


def _reconstruct_pg_type(col: dict) -> str:
    """information_schema 컬럼 정보로 full PG 타입 문자열 재구성."""
    dt = col["pg_type"]

    if dt in ("character varying", "character"):
        max_len = col.get("max_length")
        return f"{dt}({max_len})" if max_len else dt

    if dt == "numeric":
        p = col.get("precision")
        s = col.get("scale")
        if p is not None:
            return f"numeric({p},{s})" if s is not None else f"numeric({p})"
        return "numeric"

    if dt == "ARRAY":
        udt = col.get("udt_name", "")
        return _ARRAY_UDT_MAP.get(udt, "text[]")

    if dt == "USER-DEFINED":
        return "text"

    return dt


# ── 공통 유틸리티 (pg2ch_sync 에서 복사) ─────────────────────

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


def _validate_source_retention(
    src_table: str,
    source_retention: str,
    sync_since: str | None,
    ts_col: str | None,
) -> None:
    """source_retention 설정 유효성 검증.

    - timestamp_column 필수
    - sync_since 가 있으면 source_retention > sync_since (동일 형식끼리만 비교)
    """
    if not ts_col:
        raise ValueError(
            f"{src_table}: source_retention requires timestamp_column to be set"
        )
    if not sync_since:
        return

    ret_delta = _parse_relative_to_timedelta(source_retention)
    since_delta = _parse_relative_to_timedelta(sync_since)

    if ret_delta is not None and since_delta is not None:
        if ret_delta <= since_delta:
            raise ValueError(
                f"{src_table}: source_retention ({source_retention}) must be "
                f"strictly greater than sync_since ({sync_since})"
            )
        return

    if ret_delta is None and since_delta is None:
        ret_ts = _resolve_sync_since(source_retention)
        since_ts = _resolve_sync_since(sync_since)
        if ret_ts >= since_ts:
            raise ValueError(
                f"{src_table}: source_retention cutoff ({ret_ts}) must be "
                f"older than sync_since cutoff ({since_ts})"
            )
        return

    raise ValueError(
        f"{src_table}: source_retention and sync_since must both be "
        f"relative (e.g. '180d') or both absolute ISO timestamps"
    )


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"


# ── Job ─────────────────────────────────────────────────────────


class Pg2PgSyncJob(Job):
    name = "pg2pg_sync"
    label = "PG→PG 동기화"
    description = "PostgreSQL → PostgreSQL 테이블 동기화 (full copy / incremental / upsert)"
    default_args: dict = {"config": "pg2pg_config.json"}
    scope = "pipeline"

    _WATERMARK_TABLE = "_pg2pg_watermarks"

    # ── entry point ──────────────────────────────────────────────

    def run(self, *, config: str = "pg2pg_config.json", **kwargs) -> JobResult:
        try:
            import psycopg2 as _pg2  # noqa: F401
        except ImportError:
            return JobResult(
                False,
                "psycopg2 미설치: uv add psycopg2-binary",
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
        if tgt_db.get("type") != "postgresql":
            return JobResult(False, f"Target must be postgresql, got '{tgt_db.get('type')}'")

        total_rows = 0
        total_purged = 0
        errors: list[str] = []

        for tc in tables:
            merged = {**defaults, **tc}
            try:
                synced, purged = self._sync_table(src_db, tgt_db, merged, cfg)
                total_rows += synced
                total_purged += purged
            except Exception as e:
                self.logger.exception("Failed: %s", tc.get("source_table", "?"))
                errors.append(f"{tc.get('source_table', '?')}: {e}")

        msg = f"Synced {total_rows} rows across {len(tables)} table(s)"
        if total_purged:
            msg += f", purged {total_purged} source rows"

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

    # ── watermark ────────────────────────────────────────────────

    def _ensure_watermark_table(self, pg_conn, schema: str) -> None:
        tbl = self._WATERMARK_TABLE
        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{tbl}" ('
            "  config_key TEXT NOT NULL,"
            "  timestamp_column TEXT NOT NULL,"
            "  value TEXT NOT NULL,"
            "  updated_at TIMESTAMP NOT NULL DEFAULT now(),"
            "  PRIMARY KEY (config_key, timestamp_column)"
            ")"
        )
        with pg_conn.cursor() as cur:
            cur.execute(ddl)
        pg_conn.commit()

    def _get_watermark(self, pg_conn, schema: str, key: str, ts_col: str) -> str | None:
        tbl = self._WATERMARK_TABLE
        with pg_conn.cursor() as cur:
            cur.execute(
                f'SELECT value FROM "{schema}"."{tbl}"'
                " WHERE config_key = %s AND timestamp_column = %s",
                (key, ts_col),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def _save_watermark(self, pg_conn, schema: str, key: str, ts_col: str, value) -> None:
        tbl = self._WATERMARK_TABLE
        val_str = value.isoformat() if isinstance(value, datetime) else str(value)
        with pg_conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{schema}"."{tbl}"'
                " (config_key, timestamp_column, value, updated_at)"
                " VALUES (%s, %s, %s, now())"
                " ON CONFLICT (config_key, timestamp_column) DO UPDATE SET"
                " value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
                (key, ts_col, val_str),
            )
        pg_conn.commit()

    # ── source retention purge ──────────────────────────────────

    def _purge_source(
        self,
        pg_conn,
        src_schema: str,
        src_name: str,
        ts_col: str,
        retention_cutoff: str,
        batch_size: int = 10_000,
        lock_timeout_ms: int = 5_000,
    ) -> int:
        """PG source 에서 retention_cutoff 이전 row 를 배치 삭제.

        SET LOCAL lock_timeout 으로 DDL lock 대기를 회피하며,
        LockNotAvailable 발생 시 graceful stop 후 부분 삭제 수를 반환한다.
        """
        src_fqn = f'"{src_schema}"."{src_name}"'
        total_deleted = 0

        while True:
            try:
                with pg_conn.cursor() as cur:
                    cur.execute(
                        f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms'"
                    )
                    cur.execute(
                        f"DELETE FROM {src_fqn} "
                        f"WHERE ctid = ANY(ARRAY("
                        f'  SELECT ctid FROM {src_fqn}'
                        f'  WHERE "{ts_col}" < %s'
                        f"  LIMIT %s"
                        f"))",
                        (retention_cutoff, batch_size),
                    )
                    deleted = cur.rowcount
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                err_name = type(e).__name__
                if "LockNotAvailable" in err_name or "lock timeout" in str(e).lower():
                    self.logger.warning(
                        "%s.%s: lock_timeout hit during purge, "
                        "stopping. Deleted %d so far.",
                        src_schema,
                        src_name,
                        total_deleted,
                    )
                    break
                raise

            total_deleted += deleted
            self.logger.info(
                "%s.%s: purged batch %d rows (total %d)",
                src_schema,
                src_name,
                deleted,
                total_deleted,
            )
            if deleted < batch_size:
                break

        return total_deleted

    # ── PG schema introspection ──────────────────────────────────

    @staticmethod
    def _get_pg_columns(pg_conn, schema: str, table: str) -> list[dict]:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable,"
                "       numeric_precision, numeric_scale,"
                "       character_maximum_length, udt_name"
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
                    "max_length": r[5],
                    "udt_name": r[6],
                }
                for r in cur.fetchall()
            ]

    # ── column mapping ───────────────────────────────────────────

    @staticmethod
    def _build_pg_columns(
        pg_cols: list[dict],
        drop_columns: set[str],
        column_overrides: dict[str, str],
    ) -> list[dict]:
        """소스 PG 컬럼 → 타겟 PG 컬럼 정의 리스트.

        PG→PG 이므로 타입은 기본적으로 passthrough.
        column_overrides 로 타겟 타입을 직접 지정할 수 있다.
        """
        result = []
        for col in pg_cols:
            name = col["name"]
            if name in drop_columns:
                continue
            if name in column_overrides:
                tgt_type = column_overrides[name]
            else:
                tgt_type = _reconstruct_pg_type(col)
            nullable = col["nullable"]
            result.append(
                {"name": name, "tgt_type": tgt_type, "pg_type": col["pg_type"], "nullable": nullable}
            )
        return result

    # ── PG DDL ───────────────────────────────────────────────────

    def _ensure_pg_table(
        self,
        pg_conn,
        schema: str,
        table: str,
        columns: list[dict],
        conflict_key: list[str] | None,
    ) -> None:
        col_defs = []
        for c in columns:
            null_clause = "" if c["nullable"] else " NOT NULL"
            col_defs.append(f'"{c["name"]}" {c["tgt_type"]}{null_clause}')

        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" '
            f'({", ".join(col_defs)}'
        )
        if conflict_key:
            ck = ", ".join(f'"{k}"' for k in conflict_key)
            ddl += f", UNIQUE ({ck})"
        ddl += ")"

        self.logger.info("Ensuring table: %s.%s", schema, table)
        self.logger.debug("DDL: %s", ddl)
        with pg_conn.cursor() as cur:
            cur.execute(ddl)
        pg_conn.commit()

    def _ensure_conflict_index(
        self,
        pg_conn,
        schema: str,
        table: str,
        conflict_key: list[str],
    ) -> None:
        """conflict_key 에 해당하는 UNIQUE 인덱스가 없으면 생성."""
        idx_name = f"_pg2pg_ck_{table}_{'_'.join(conflict_key)}"
        ck_cols = ", ".join(f'"{k}"' for k in conflict_key)
        ddl = (
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" '
            f'ON "{schema}"."{table}" ({ck_cols})'
        )
        with pg_conn.cursor() as cur:
            cur.execute(ddl)
        pg_conn.commit()

    def _ensure_indexes(
        self,
        pg_conn,
        schema: str,
        table: str,
        indexes: list[dict],
    ) -> None:
        """create_indexes 설정에 따라 인덱스 생성."""
        for idx_cfg in indexes:
            cols = idx_cfg["columns"]
            unique = idx_cfg.get("unique", False)
            col_suffix = "_".join(cols)
            idx_name = f"idx_{table}_{col_suffix}"
            col_list = ", ".join(f'"{c}"' for c in cols)
            unique_kw = "UNIQUE " if unique else ""
            ddl = (
                f'CREATE {unique_kw}INDEX IF NOT EXISTS "{idx_name}" '
                f'ON "{schema}"."{table}" ({col_list})'
            )
            self.logger.debug("Index DDL: %s", ddl)
            with pg_conn.cursor() as cur:
                cur.execute(ddl)
            pg_conn.commit()

    # ── row transform ────────────────────────────────────────────

    @staticmethod
    def _build_transformer(columns: list[dict]):
        """PG→PG 행 변환 함수. USER-DEFINED → text 변환만 필요. 불필요하면 None 반환."""
        transforms: dict[int, callable] = {}
        for i, col in enumerate(columns):
            pg_t = col["pg_type"]

            if pg_t == "USER-DEFINED" and col["tgt_type"] == "text":
                def _sconv(v):
                    if v is not None and not isinstance(v, str):
                        return str(v)
                    return v

                transforms[i] = _sconv

        if not transforms:
            return None

        def transform(row: tuple) -> tuple:
            lst = list(row)
            for idx, fn in transforms.items():
                lst[idx] = fn(lst[idx])
            return tuple(lst)

        return transform

    # ── SQL builders ─────────────────────────────────────────────

    @staticmethod
    def _build_upsert_sql(
        schema: str,
        table: str,
        col_names: list[str],
        conflict_key: list[str],
    ) -> str:
        """INSERT ... ON CONFLICT DO UPDATE SET ... SQL 템플릿 생성.

        psycopg2.extras.execute_values() 에서 사용할 %s 플레이스홀더 포함.
        """
        fqn = f'"{schema}"."{table}"'
        col_list = ", ".join(f'"{c}"' for c in col_names)
        conflict_cols = ", ".join(f'"{c}"' for c in conflict_key)

        update_cols = [c for c in col_names if c not in set(conflict_key)]
        if update_cols:
            set_clause = ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in update_cols
            )
            return (
                f"INSERT INTO {fqn} ({col_list}) VALUES %s "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
            )
        else:
            return (
                f"INSERT INTO {fqn} ({col_list}) VALUES %s "
                f"ON CONFLICT ({conflict_cols}) DO NOTHING"
            )

    @staticmethod
    def _build_append_sql(
        schema: str,
        table: str,
        col_names: list[str],
    ) -> str:
        """단순 INSERT SQL 템플릿 생성."""
        fqn = f'"{schema}"."{table}"'
        col_list = ", ".join(f'"{c}"' for c in col_names)
        return f"INSERT INTO {fqn} ({col_list}) VALUES %s"

    # ── per-table sync ───────────────────────────────────────────

    def _sync_table(
        self,
        src_cfg: dict,
        tgt_cfg: dict,
        tc: dict,
        sync_cfg: dict,
    ) -> tuple[int, int]:
        src_table: str = tc["source_table"]
        tgt_table: str = tc["target_table"]
        ts_col: str | None = tc.get("timestamp_column")
        raw_since: str | None = tc.get("sync_since")
        sync_since: str | None = _resolve_sync_since(raw_since) if raw_since else None
        drop_cols = set(tc.get("drop_columns", []))
        col_overrides: dict = tc.get("column_overrides", {})
        batch_size: int = tc.get("batch_size", 100_000)
        overlap_min: int = tc.get("overlap_minutes", 0)
        sync_mode: str = tc.get("sync_mode", "append")
        conflict_key: list[str] | None = tc.get("conflict_key")
        raw_retention: str | None = tc.get("source_retention")
        create_indexes: list[dict] = tc.get("create_indexes", [])

        if sync_mode == "upsert" and not conflict_key:
            raise ValueError(
                f"{src_table}: sync_mode 'upsert' requires conflict_key to be set"
            )

        if sync_since and not ts_col:
            raise ValueError(
                f"{src_table}: sync_since requires timestamp_column to be set"
            )

        if raw_retention:
            _validate_source_retention(src_table, raw_retention, raw_since, ts_col)
            retention_cutoff = _resolve_sync_since(raw_retention)
        else:
            retention_cutoff = None

        # schema.table 파싱
        src_schema, src_name = (
            src_table.split(".", 1) if "." in src_table else ("public", src_table)
        )
        tgt_schema, tgt_name = (
            tgt_table.split(".", 1) if "." in tgt_table else ("public", tgt_table)
        )

        pg_src = None
        pg_tgt = None
        try:
            pg_src = self._pg_connect(src_cfg)
            pg_tgt = self._pg_connect(tgt_cfg)

            # 1) PG 소스 컬럼 조회
            pg_cols = self._get_pg_columns(pg_src, src_schema, src_name)
            if not pg_cols:
                raise ValueError(f"Table {src_table} not found or has no columns")

            # 2) 타겟 PG 컬럼 매핑
            tgt_columns = self._build_pg_columns(pg_cols, drop_cols, col_overrides)
            col_names = [c["name"] for c in tgt_columns]
            col_list_pg = ", ".join(f'"{c}"' for c in col_names)

            # 3) 타겟 PG 테이블 생성
            self._ensure_pg_table(
                pg_tgt, tgt_schema, tgt_name, tgt_columns, conflict_key
            )

            # 3b) conflict_key 인덱스 보장 (기존 테이블)
            if conflict_key:
                self._ensure_conflict_index(pg_tgt, tgt_schema, tgt_name, conflict_key)

            # 3c) 추가 인덱스 생성
            if create_indexes:
                self._ensure_indexes(pg_tgt, tgt_schema, tgt_name, create_indexes)

            # 4) 동기화 모드 결정
            self._ensure_watermark_table(pg_tgt, tgt_schema)
            wm_key = f"{sync_cfg['source']}.{src_table}"
            watermark = self._get_watermark(pg_tgt, tgt_schema, wm_key, ts_col) if ts_col else None

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
                if sync_mode == "append":
                    self.logger.info("%s: full copy (append) — truncating target", src_table)
                    tgt_fqn = f'"{tgt_schema}"."{tgt_name}"'
                    with pg_tgt.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {tgt_fqn}")
                    pg_tgt.commit()
                else:
                    self.logger.info("%s: full copy (upsert) — no truncate", src_table)

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

            # 5) INSERT / UPSERT SQL 빌드
            if sync_mode == "upsert":
                insert_sql = self._build_upsert_sql(
                    tgt_schema, tgt_name, col_names, conflict_key
                )
            else:
                insert_sql = self._build_append_sql(
                    tgt_schema, tgt_name, col_names
                )

            # 6) 스트리밍 전송
            transformer = self._build_transformer(tgt_columns)

            cursor = pg_src.cursor(name="pg2pg_stream")
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

            page_size = min(batch_size, 10_000)

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

                with pg_tgt.cursor() as tgt_cur:
                    execute_values(tgt_cur, insert_sql, rows, page_size=page_size)
                pg_tgt.commit()

                total_rows += len(rows)
                self.logger.info(
                    "%s: batch %d rows (total %d)", src_table, len(rows), total_rows
                )

            cursor.close()
            pg_src.rollback()  # read 트랜잭션 정리

            # 7) Watermark 저장
            if ts_col and max_wm is not None:
                self._save_watermark(pg_tgt, tgt_schema, wm_key, ts_col, max_wm)
                self.logger.info("%s: watermark -> %s", src_table, max_wm)

            # 8) Source retention purge
            purged_rows = 0
            if retention_cutoff:
                self.logger.info(
                    "%s: purging source rows older than %s",
                    src_table,
                    retention_cutoff,
                )
                purged_rows = self._purge_source(
                    pg_src,
                    src_schema,
                    src_name,
                    ts_col,
                    retention_cutoff,
                    batch_size=min(batch_size, 10_000),
                )
                self.logger.info(
                    "%s: purged %d source rows", src_table, purged_rows
                )

            mode = "incremental" if watermark else "full copy"
            self.logger.info("%s: %s complete — %d rows", src_table, mode, total_rows)
            return total_rows, purged_rows

        finally:
            if pg_tgt:
                pg_tgt.close()
            if pg_src:
                pg_src.close()
