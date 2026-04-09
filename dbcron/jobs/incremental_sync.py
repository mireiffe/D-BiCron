"""Incremental sync: upsert rows from susdb -> coredb.

증분 동기화 Job.
Source DB 에서 timestamp 기준으로 delta 행을 읽어서 Target DB 에 upsert 합니다.

흐름:
  1) Target 에서 MAX(ts_column) 조회 (인덱스 있으므로 빠름)
  2) Cutoff = MAX(ts) - overlap_minutes (기본 30분, 변경분 누락 방지)
  3) Source 에서 COPY (SELECT ... WHERE ts > cutoff) TO STDOUT  (read-only, lock 없음)
  4) Target temp table 에 COPY FROM STDIN
  5) INSERT INTO target SELECT ... FROM temp ON CONFLICT DO UPDATE (upsert)
  6) Temp table 자동 삭제 (ON COMMIT DROP)

설정:
  SYNC_CONFIG 환경변수로 JSON 설정 파일 경로 지정 (기본: sync_config.json)
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

from ..db import create_coredb_engine, create_susdb_engine
from .base import Job, JobResult


class IncrementalSyncJob(Job):
    name = "incremental_sync"
    label = "증분 동기화"
    description = "Source DB delta 행을 Target DB 로 증분 동기화 (upsert)"
    default_args = {"days": 1}

    def run(self, *, days: int = 1, **kwargs) -> JobResult:
        sync_cfg = self._load_sync_config()
        tables = sync_cfg["tables"]

        src_engine = self._build_engine(sync_cfg, "source", create_susdb_engine)
        tgt_engine = self._build_engine(sync_cfg, "target", create_coredb_engine)

        total = 0
        errors: list[str] = []

        try:
            for tc in tables:
                try:
                    n = self._sync_table(src_engine, tgt_engine, tc, days)
                    total += n
                except Exception as e:
                    self.logger.exception("Failed: %s", tc["table"])
                    errors.append(f"{tc['table']}: {e}")
        finally:
            src_engine.dispose()
            tgt_engine.dispose()

        if errors:
            msg = f"Synced {total} rows, {len(errors)} error(s): {'; '.join(errors)}"
            return JobResult(success=False, message=msg, rows_affected=total)

        return JobResult(
            success=True,
            message=f"Synced {total} rows across {len(tables)} table(s)",
            rows_affected=total,
        )

    # ── config ────────────────────────────────────────────────────────

    def _load_sync_config(self) -> dict:
        path = os.getenv("SYNC_CONFIG", "sync_config.json")
        with open(path) as f:
            return json.load(f)

    def _build_engine(self, sync_cfg: dict, key: str, fallback_factory):
        """sync_config 에 DB 접속 정보가 있으면 사용, 없으면 기존 susdb/coredb fallback."""
        db = sync_cfg.get(key)
        if db:
            url = (
                f"postgresql://{db['user']}:{db['password']}"
                f"@{db['host']}:{db.get('port', 5432)}/{db['database']}"
            )
            self.logger.info("%s DB: %s@%s/%s", key, db["user"], db["host"], db["database"])
            return create_engine(url, pool_pre_ping=True)
        return fallback_factory(self.config)

    # ── per-table sync ────────────────────────────────────────────────

    def _sync_table(self, src_engine, tgt_engine, tc: dict, days: int) -> int:
        table = tc["table"]
        pk_cols: list[str] = tc["pk_columns"]
        ts_col: str = tc["ts_column"]
        src_schema = tc.get("source_schema", "public")
        tgt_schema = tc.get("target_schema", "public")

        q_src = f'"{src_schema}"."{table}"'
        q_tgt = f'"{tgt_schema}"."{table}"'

        # 1) Target 에 unique index 보장 (ON CONFLICT 에 필요, 최초 1회만 실질 생성)
        self._ensure_unique_index(tgt_engine, tgt_schema, table, pk_cols)

        # 2) Cutoff: target 의 MAX(ts_column), 없으면 days 만큼 lookback
        with tgt_engine.connect() as conn:
            max_ts = conn.execute(
                text(f'SELECT MAX("{ts_col}") FROM {q_tgt}')
            ).scalar()

        overlap_min = tc.get("overlap_minutes", 30)

        if max_ts is None:
            cutoff = datetime.now() - timedelta(days=days)
            self.logger.info("%s: empty target, lookback %d days", table, days)
        else:
            cutoff = max_ts - timedelta(minutes=overlap_min)
            self.logger.info("%s: resume from %s (overlap %dm)", table, cutoff, overlap_min)

        # 3) Target 컬럼 목록
        with tgt_engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t "
                "ORDER BY ordinal_position"
            ), {"s": tgt_schema, "t": table}).fetchall()
        columns = [r[0] for r in rows]
        col_list = ", ".join(f'"{c}"' for c in columns)

        # 4) Source 에서 delta COPY TO STDOUT → 메모리 버퍼
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S.%f")
        copy_out = (
            f"COPY (SELECT {col_list} FROM {q_src} "
            f'WHERE "{ts_col}" > \'{cutoff_str}\') TO STDOUT'
        )

        buf = io.BytesIO()
        src_raw = src_engine.raw_connection()
        try:
            with src_raw.cursor() as cur:
                cur.copy_expert(copy_out, buf)
        finally:
            src_raw.close()

        if buf.tell() == 0:
            self.logger.info("%s: no new rows", table)
            return 0

        buf.seek(0)
        data_bytes = buf.getbuffer().nbytes

        # 5) Target: temp table → INSERT ... ON CONFLICT DO UPDATE (upsert)
        temp = f"_sync_tmp_{table}"
        pk_list = ", ".join(f'"{c}"' for c in pk_cols)
        pk_set = set(pk_cols)
        update_cols = [c for c in columns if c not in pk_set]
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

        tgt_raw = tgt_engine.raw_connection()
        try:
            with tgt_raw.cursor() as cur:
                cur.execute(
                    f'CREATE TEMP TABLE "{temp}" '
                    f"(LIKE {q_tgt} INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                cur.copy_expert(f'COPY "{temp}" ({col_list}) FROM STDIN', buf)

                cur.execute(f'SELECT COUNT(*) FROM "{temp}"')
                candidates = cur.fetchone()[0]

                cur.execute(
                    f"INSERT INTO {q_tgt} ({col_list}) "
                    f'SELECT {col_list} FROM "{temp}" '
                    f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_set}"
                )
                upserted = cur.rowcount

            tgt_raw.commit()
            self.logger.info(
                "%s: %d upserted (%d candidates, %s)",
                table, upserted, candidates, _fmt_bytes(data_bytes),
            )
            return upserted
        except Exception:
            tgt_raw.rollback()
            raise
        finally:
            tgt_raw.close()

    # ── helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _ensure_unique_index(engine, schema: str, table: str, pk_cols: list[str]):
        """ON CONFLICT 에 필요한 unique index 를 target 에 생성합니다.

        해당 컬럼 조합에 unique index/constraint 가 이미 존재하면 스킵합니다.
        """
        # 해당 컬럼 조합의 unique index 존재 여부 확인
        check_sql = text("""
            SELECT 1
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema
              AND c.relname = :table
              AND i.indisunique
              AND (
                  SELECT array_agg(a.attname ORDER BY k.ord)
                  FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                  JOIN pg_catalog.pg_attribute a
                    ON a.attrelid = c.oid AND a.attnum = k.attnum
              ) = :cols
            LIMIT 1
        """)
        with engine.begin() as conn:
            exists = conn.execute(
                check_sql, {"schema": schema, "table": table, "cols": pk_cols}
            ).scalar()
            if exists:
                return

            idx_name = f"uq_sync_{table}_{'_'.join(pk_cols)}"
            col_list = ", ".join(f'"{c}"' for c in pk_cols)
            conn.execute(text(
                f'CREATE UNIQUE INDEX "{idx_name}" '
                f'ON "{schema}"."{table}" ({col_list})'
            ))


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"
