"""Incremental sync: source DB -> target DB upsert.

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
  source / target 은 databases.json 의 DB ID(문자열) 또는 인라인 접속 정보(dict)
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

from ..db import create_engine_by_id, create_engine_for
from .base import Job, JobResult


class IncrementalSyncJob(Job):
    name = "incremental_sync"
    label = "증분 동기화"
    description = "Source DB delta 행을 Target DB 로 증분 동기화 (upsert)"
    default_args = {"days": 1, "config": "sync_config.json"}
    scope = "pipeline"

    def run(self, *, days: int = 1, config: str = "sync_config.json", **kwargs) -> JobResult:
        sync_cfg = self._load_sync_config(config)
        tables = sync_cfg["tables"]

        src_engine = self._build_engine(sync_cfg, "source")
        tgt_engine = self._build_engine(sync_cfg, "target")

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

    @staticmethod
    def _load_sync_config(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    def _build_engine(self, sync_cfg: dict, key: str):
        """sync_config 의 source/target 설정으로 엔진을 생성한다.

        값이 문자열이면 databases.json 의 DB ID 로 조회,
        dict 이면 인라인 접속 정보로 직접 엔진 생성.
        """
        ref = sync_cfg.get(key)
        if ref is None:
            raise ValueError(
                f"sync_config.json 에 '{key}' 설정이 필요합니다. "
                f"databases.json 의 DB ID(문자열) 또는 인라인 접속 정보(dict)를 지정하세요."
            )
        if isinstance(ref, str):
            self.logger.info("%s DB: databases.json ID '%s'", key, ref)
            return create_engine_by_id(ref)
        # inline connection info (dict)
        url = (
            f"postgresql://{ref['user']}:{ref['password']}"
            f"@{ref['host']}:{ref.get('port', 5432)}/{ref['database']}"
        )
        self.logger.info("%s DB: %s@%s/%s", key, ref["user"], ref["host"], ref["database"])
        return create_engine(url, pool_pre_ping=True)

    # ── per-table sync ────────────────────────────────────────────────

    def _sync_table(self, src_engine, tgt_engine, tc: dict, days: int) -> int:
        table = tc["table"]
        pk_cols: list[str] = tc["pk_columns"]
        ts_col: str = tc["ts_column"]
        src_schema = tc.get("source_schema", "public")
        tgt_schema = tc.get("target_schema", "public")

        q_src = f'"{src_schema}"."{table}"'
        q_tgt = f'"{tgt_schema}"."{table}"'

        # (unique index 불필요 — DELETE+INSERT 방식 사용)

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

        # 5) Target: temp table → DELETE 기존 매칭 행 → INSERT (upsert)
        temp = f"_sync_tmp_{table}"
        pk_list = ", ".join(f'"{c}"' for c in pk_cols)
        pk_match = " AND ".join(
            f'{q_tgt}."{c}" = "{temp}"."{c}"' for c in pk_cols
        )

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

                # PK 매칭되는 기존 행 삭제 후 새로 삽입
                cur.execute(
                    f"DELETE FROM {q_tgt} USING \"{temp}\" WHERE {pk_match}"
                )
                cur.execute(
                    f"INSERT INTO {q_tgt} ({col_list}) "
                    f'SELECT {col_list} FROM "{temp}"'
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



def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"
