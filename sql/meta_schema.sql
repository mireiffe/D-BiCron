-- pg2ch 추적 메타 스키마 (PostgreSQL).
--
-- pg2ch.tracking.schema_ddl() 와 동일한 정의의 정본(canonical) 레퍼런스.
-- 코드는 ensure_schema() 로 자동 생성하므로 보통 직접 실행할 필요는 없으나,
-- 권한 분리 환경에서 DBA 가 미리 만들고 싶을 때 사용한다.
--
--   psql "$META_DSN" -f sql/meta_schema.sql
--
-- 스키마명을 바꾸려면 아래 pg2ch_meta 를 일괄 치환하고
-- connections.json 의 meta 항목 "schema" 도 동일하게 맞춘다.

CREATE SCHEMA IF NOT EXISTS pg2ch_meta;

-- ── copy_run : 테이블 × 실행(=Airflow task 1회) ─────────────────────────
-- 다음 실행의 증분 cutoff(="어디까지 복사되었나")는
--   마지막 status IN ('success','partial') run 의 watermark_after 에서 읽는다.
CREATE TABLE IF NOT EXISTS pg2ch_meta.copy_run (
    run_id           BIGSERIAL PRIMARY KEY,
    table_id         TEXT NOT NULL,
    source_table     TEXT NOT NULL,
    target_table     TEXT NOT NULL,
    sync_mode        TEXT NOT NULL,                  -- append | full_reload
    status           TEXT NOT NULL,                  -- running | success | partial | failed
    dag_id           TEXT,
    airflow_run_id   TEXT,
    task_id          TEXT,
    try_number       INT,
    watermark_column TEXT,
    watermark_before TEXT,                           -- 이 run 시작 시점 cutoff
    watermark_after  TEXT,                           -- 이 run 으로 전진한 high-watermark
    rows_read        BIGINT NOT NULL DEFAULT 0,
    rows_written     BIGINT NOT NULL DEFAULT 0,
    rows_failed      BIGINT NOT NULL DEFAULT 0,
    batch_count      INT NOT NULL DEFAULT 0,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    duration_ms      BIGINT,
    error            TEXT
);

-- ── copy_batch : run 안의 batch 단위 ───────────────────────────────────
CREATE TABLE IF NOT EXISTS pg2ch_meta.copy_batch (
    batch_id      BIGSERIAL PRIMARY KEY,
    run_id        BIGINT NOT NULL REFERENCES pg2ch_meta.copy_run(run_id) ON DELETE CASCADE,
    table_id      TEXT NOT NULL,
    batch_seq     INT NOT NULL,
    status        TEXT NOT NULL,                     -- success | partial | failed
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
);

-- ── copy_failed_row : dead-letter (어떤 batch 의 어떤 row 가 실패했나) ──
-- row_data 에 원본 source row 를 JSONB 로 보관 → 디버그/재처리(replay) 가능.
CREATE TABLE IF NOT EXISTS pg2ch_meta.copy_failed_row (
    id              BIGSERIAL PRIMARY KEY,
    run_id          BIGINT NOT NULL REFERENCES pg2ch_meta.copy_run(run_id) ON DELETE CASCADE,
    batch_id        BIGINT REFERENCES pg2ch_meta.copy_batch(batch_id) ON DELETE CASCADE,
    table_id        TEXT NOT NULL,
    batch_seq       INT,
    watermark_value TEXT,
    row_data        JSONB,
    error           TEXT,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS copy_run_resume_idx
    ON pg2ch_meta.copy_run (table_id, watermark_column, run_id DESC);
CREATE INDEX IF NOT EXISTS copy_run_status_idx
    ON pg2ch_meta.copy_run (status, started_at DESC);
CREATE INDEX IF NOT EXISTS copy_batch_run_idx
    ON pg2ch_meta.copy_batch (run_id, batch_seq);
CREATE INDEX IF NOT EXISTS copy_failed_row_table_idx
    ON pg2ch_meta.copy_failed_row (table_id, resolved, failed_at DESC);
CREATE INDEX IF NOT EXISTS copy_failed_row_run_idx
    ON pg2ch_meta.copy_failed_row (run_id);
