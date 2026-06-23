"""copy 추적 메타 저장소 (PostgreSQL).

"어디까지 복사되었나 / 어느 batch 의 어느 row 가 실패했나" 를 영속 기록한다.
스키마(기본 ``pg2ch_meta``)에 세 테이블:

  copy_run        — 테이블×실행(=Airflow task 1회) 단위. status / watermark_before
                    / watermark_after / rows_read·written·failed / batch_count.
                    다음 실행의 증분 cutoff(=어디까지 복사) 는 이 테이블의 마지막
                    성공 run.watermark_after 에서 읽는다.
  copy_batch      — run 안의 batch 단위. watermark_lo/hi, rows_in/written/failed,
                    status(success|partial|failed), attempts.
  copy_failed_row — dead-letter. 실패한 source row 의 watermark + 원본(JSONB) +
                    에러. resolved 플래그로 재처리 추적.

추적 write 는 즉시 커밋(autocommit)되어 복사 task 가 중간에 죽어도 남는다.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, date
from decimal import Decimal

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_SCHEMA = "pg2ch_meta"


def _safe_schema(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"invalid schema name: {name!r}")
    return name


def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    if isinstance(o, (bytes, memoryview)):
        return bytes(o).hex()
    return str(o)


def row_to_json(col_names: list[str], row) -> str:
    """col_names + row tuple → JSON 문자열 (datetime/Decimal/bytes 안전)."""
    return json.dumps(dict(zip(col_names, row)), ensure_ascii=False, default=_json_default)


def schema_ddl(schema: str = DEFAULT_SCHEMA) -> list[str]:
    """메타 스키마 + 테이블 + 인덱스 DDL 문장 리스트."""
    s = _safe_schema(schema)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.copy_run (
            run_id           BIGSERIAL PRIMARY KEY,
            table_id         TEXT NOT NULL,
            source_table     TEXT NOT NULL,
            target_table     TEXT NOT NULL,
            sync_mode        TEXT NOT NULL,
            status           TEXT NOT NULL,
            dag_id           TEXT,
            airflow_run_id   TEXT,
            task_id          TEXT,
            try_number       INT,
            watermark_column TEXT,
            watermark_before TEXT,
            watermark_after  TEXT,
            rows_read        BIGINT NOT NULL DEFAULT 0,
            rows_written     BIGINT NOT NULL DEFAULT 0,
            rows_failed      BIGINT NOT NULL DEFAULT 0,
            batch_count      INT NOT NULL DEFAULT 0,
            started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at      TIMESTAMPTZ,
            duration_ms      BIGINT,
            error            TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {s}.copy_batch (
            batch_id      BIGSERIAL PRIMARY KEY,
            run_id        BIGINT NOT NULL REFERENCES {s}.copy_run(run_id) ON DELETE CASCADE,
            table_id      TEXT NOT NULL,
            batch_seq     INT NOT NULL,
            status        TEXT NOT NULL,
            rows_in       BIGINT NOT NULL DEFAULT 0,
            rows_written  BIGINT NOT NULL DEFAULT 0,
            rows_failed   BIGINT NOT NULL DEFAULT 0,
            watermark_lo  TEXT,
            watermark_hi  TEXT,
            attempts      INT NOT NULL DEFAULT 1,
            started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at   TIMESTAMPTZ,
            error         TEXT,
            UNIQUE (run_id, batch_seq)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {s}.copy_failed_row (
            id              BIGSERIAL PRIMARY KEY,
            run_id          BIGINT NOT NULL REFERENCES {s}.copy_run(run_id) ON DELETE CASCADE,
            batch_id        BIGINT REFERENCES {s}.copy_batch(batch_id) ON DELETE CASCADE,
            table_id        TEXT NOT NULL,
            batch_seq       INT,
            watermark_value TEXT,
            row_data        JSONB,
            error           TEXT,
            failed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            resolved        BOOLEAN NOT NULL DEFAULT FALSE,
            resolved_at     TIMESTAMPTZ
        )
        """,
        f"CREATE INDEX IF NOT EXISTS copy_run_resume_idx ON {s}.copy_run "
        f"(table_id, watermark_column, run_id DESC)",
        f"CREATE INDEX IF NOT EXISTS copy_run_status_idx ON {s}.copy_run (status, started_at DESC)",
        f"CREATE INDEX IF NOT EXISTS copy_batch_run_idx ON {s}.copy_batch (run_id, batch_seq)",
        f"CREATE INDEX IF NOT EXISTS copy_failed_row_table_idx ON {s}.copy_failed_row "
        f"(table_id, resolved, failed_at DESC)",
        f"CREATE INDEX IF NOT EXISTS copy_failed_row_run_idx ON {s}.copy_failed_row (run_id)",
    ]


class MetaStore:
    """추적 메타 저장소 핸들. psycopg2 커넥션을 autocommit 으로 보유."""

    def __init__(self, conn, schema: str = DEFAULT_SCHEMA):
        self.conn = conn
        self.schema = _safe_schema(schema)
        try:
            self.conn.autocommit = True
        except Exception:
            pass

    @classmethod
    def connect(cls, cfg: dict):
        """meta 접속 정보 dict 로 MetaStore 생성. cfg['schema'] 로 스키마 지정."""
        from .connections import meta_connect

        conn = meta_connect(cfg)
        return cls(conn, schema=cfg.get("schema", DEFAULT_SCHEMA))

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ── 스키마 ───────────────────────────────────────────────
    def ensure_schema(self) -> None:
        with self.conn.cursor() as cur:
            for stmt in schema_ddl(self.schema):
                cur.execute(stmt)

    # ── run ──────────────────────────────────────────────────
    def start_run(
        self,
        *,
        table_id: str,
        source_table: str,
        target_table: str,
        sync_mode: str,
        watermark_column: str | None = None,
        watermark_before: str | None = None,
        dag_id: str | None = None,
        airflow_run_id: str | None = None,
        task_id: str | None = None,
        try_number: int | None = None,
    ) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.copy_run
                    (table_id, source_table, target_table, sync_mode, status,
                     watermark_column, watermark_before, dag_id, airflow_run_id,
                     task_id, try_number)
                VALUES (%s,%s,%s,%s,'running',%s,%s,%s,%s,%s,%s)
                RETURNING run_id
                """,
                (
                    table_id, source_table, target_table, sync_mode,
                    watermark_column, watermark_before, dag_id, airflow_run_id,
                    task_id, try_number,
                ),
            )
            return cur.fetchone()[0]

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        watermark_after: str | None = None,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_failed: int = 0,
        batch_count: int = 0,
        duration_ms: int | None = None,
        error: str | None = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.schema}.copy_run
                SET status=%s, watermark_after=%s, rows_read=%s, rows_written=%s,
                    rows_failed=%s, batch_count=%s, duration_ms=%s, error=%s,
                    finished_at=now()
                WHERE run_id=%s
                """,
                (
                    status, watermark_after, rows_read, rows_written, rows_failed,
                    batch_count, duration_ms, error[:4000] if error else None, run_id,
                ),
            )

    def get_resume_watermark(self, table_id: str, watermark_column: str) -> str | None:
        """마지막 성공/부분성공 run 의 watermark_after = 다음 증분 cutoff."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT watermark_after FROM {self.schema}.copy_run
                WHERE table_id=%s AND watermark_column=%s
                  AND status IN ('success','partial') AND watermark_after IS NOT NULL
                ORDER BY run_id DESC LIMIT 1
                """,
                (table_id, watermark_column),
            )
            row = cur.fetchone()
            return row[0] if row else None

    # ── batch ────────────────────────────────────────────────
    def record_batch(
        self,
        *,
        run_id: int,
        table_id: str,
        batch_seq: int,
        status: str,
        rows_in: int,
        rows_written: int,
        rows_failed: int,
        watermark_lo: str | None = None,
        watermark_hi: str | None = None,
        attempts: int = 1,
        error: str | None = None,
    ) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.copy_batch
                    (run_id, table_id, batch_seq, status, rows_in, rows_written,
                     rows_failed, watermark_lo, watermark_hi, attempts, error, finished_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                RETURNING batch_id
                """,
                (
                    run_id, table_id, batch_seq, status, rows_in, rows_written,
                    rows_failed, watermark_lo, watermark_hi, attempts,
                    error[:4000] if error else None,
                ),
            )
            return cur.fetchone()[0]

    # ── failed rows (dead-letter) ────────────────────────────
    def record_failed_rows(
        self,
        *,
        run_id: int,
        batch_id: int | None,
        table_id: str,
        batch_seq: int | None,
        failures: list[tuple[str | None, str, str]],
    ) -> None:
        """failures: [(watermark_value, row_json, error), ...]"""
        if not failures:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {self.schema}.copy_failed_row
                    (run_id, batch_id, table_id, batch_seq, watermark_value, row_data, error)
                VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s)
                """,
                [
                    (run_id, batch_id, table_id, batch_seq, wm, rj, (err or "")[:4000])
                    for (wm, rj, err) in failures
                ],
            )

    # ── 조회 (검사/대시보드용) ───────────────────────────────
    def run_summary(self, run_id: int) -> dict | None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT run_id, table_id, sync_mode, status, watermark_before,
                       watermark_after, rows_read, rows_written, rows_failed,
                       batch_count, started_at, finished_at, duration_ms, error
                FROM {self.schema}.copy_run WHERE run_id=%s
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if not r:
                return None
            cols = [
                "run_id", "table_id", "sync_mode", "status", "watermark_before",
                "watermark_after", "rows_read", "rows_written", "rows_failed",
                "batch_count", "started_at", "finished_at", "duration_ms", "error",
            ]
            return dict(zip(cols, r))

    def unresolved_failed_count(self, table_id: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {self.schema}.copy_failed_row "
                f"WHERE table_id=%s AND NOT resolved",
                (table_id,),
            )
            return int(cur.fetchone()[0])
