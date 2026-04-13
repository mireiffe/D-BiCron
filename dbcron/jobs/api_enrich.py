"""API Enrich Job.

소스 DB에서 행을 조회하고 외부 API를 호출하여
JSON 응답을 타겟 DB 테이블에 매핑/저장합니다.

동작 모드:
  - per_row: 소스 행 1건당 API 1회 호출
  - batch: 소스 행 N건을 묶어 API 1회 호출

기능:
  - Watermark 기반 증분 처리
  - JSON dot-path 응답 매핑 (e.g. "payment.amount", "items[0].name")
  - 자동 타겟 테이블 생성 (CREATE TABLE IF NOT EXISTS)
  - 타임스탬프 컬럼 자동 추가
  - 다중 DB 타입 타겟 지원 (ClickHouse, PostgreSQL, MSSQL, SQLite)
  - API 실패 시 skip + 로그 (다음 행 계속)

설정:
  config JSON 파일로 source/target DB, API 설정, 컬럼 매핑 지정
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from urllib.parse import quote

import httpx
from sqlalchemy import text

from ..db import create_engine_by_id, create_engine_for, get_database
from .base import Job, JobResult

# ── 유틸리티 ──────────────────────────────────────────────────────


def _extract_json_path(data: dict, path: str):
    """Dot-notation path 로 중첩 dict/list 에서 값을 추출한다.

    Examples:
        _extract_json_path({"a": {"b": 1}}, "a.b") → 1
        _extract_json_path({"items": [{"x": 1}]}, "items[0].x") → 1
    """
    current = data
    for part in re.split(r"\.", path):
        m = re.match(r"^(\w+)\[(\d+)\]$", part)
        if m:
            key, idx = m.group(1), int(m.group(2))
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
            if not isinstance(current, list) or idx >= len(current):
                return None
            current = current[idx]
        else:
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
    return current


def _expand_env(value: str) -> str:
    """``${VAR}`` 패턴을 환경변수 값으로 치환한다."""
    return re.sub(
        r"\$\{(\w+)\}",
        lambda m: os.environ.get(m.group(1), m.group(0)),
        value,
    )


def _resolve_url(template: str, row: dict) -> str:
    """``{column_name}`` 플레이스홀더를 row 값으로 치환한다."""

    def _replace(m: re.Match) -> str:
        key = m.group(1)
        val = row.get(key, "")
        return quote(str(val), safe="")

    return re.sub(r"\{(\w+)\}", _replace, template)


# ── 타임스탬프 타입 ───────────────────────────────────────────────

_TIMESTAMP_TYPES: dict[str, str] = {
    "clickhouse": "DateTime64(3)",
    "mssql": "DATETIME2",
    "postgresql": "TIMESTAMP",
    "sqlite": "TIMESTAMP",
}


# ── Job ───────────────────────────────────────────────────────────


class ApiEnrichJob(Job):
    name = "api_enrich"
    label = "API Enrich"
    description = "소스 DB 조회 → API 호출 → 응답을 타겟 DB에 저장"
    default_args: dict = {"config": "api_enrich_config.json"}
    scope = "pipeline"

    _WATERMARK_TABLE = "_api_enrich_watermarks"

    # ── entry point ──────────────────────────────────────────────

    def run(self, *, config: str = "api_enrich_config.json", **kwargs) -> JobResult:
        cfg = self._load_config(config)
        tables: list[dict] = cfg["tables"]
        defaults = cfg.get("defaults", {})
        api_cfg = cfg["api"]

        src_db = get_database(cfg["source"])
        tgt_db = get_database(cfg["target"])
        if not src_db:
            return JobResult(False, f"Source DB '{cfg['source']}' not found")
        if not tgt_db:
            return JobResult(False, f"Target DB '{cfg['target']}' not found")

        src_engine = self._build_engine(cfg, "source")
        tgt_engine = self._build_engine(cfg, "target")

        total_rows = 0
        errors: list[str] = []

        try:
            for tc in tables:
                merged = {**defaults, **tc}
                try:
                    n = self._enrich_table(
                        src_engine, tgt_engine, merged, api_cfg, cfg
                    )
                    total_rows += n
                except Exception as e:
                    self.logger.exception("Failed: %s", tc.get("source_table", "?"))
                    errors.append(f"{tc.get('source_table', '?')}: {e}")
        finally:
            src_engine.dispose()
            tgt_engine.dispose()

        if errors:
            return JobResult(
                success=False,
                message=(
                    f"Enriched {total_rows} rows, {len(errors)} error(s): "
                    + "; ".join(errors)
                ),
                rows_affected=total_rows,
            )
        return JobResult(
            success=True,
            message=f"Enriched {total_rows} rows across {len(tables)} table(s)",
            rows_affected=total_rows,
        )

    # ── config ───────────────────────────────────────────────────

    @staticmethod
    def _load_config(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    def _build_engine(self, cfg: dict, key: str):
        ref = cfg[key]
        if isinstance(ref, str):
            self.logger.info("%s DB: databases.json ID '%s'", key, ref)
            return create_engine_by_id(ref)
        return create_engine_for(ref)

    @staticmethod
    def _get_db_type(db_id) -> str:
        if isinstance(db_id, dict):
            return db_id.get("type", "postgresql")
        db = get_database(db_id)
        return db.get("type", "postgresql") if db else "postgresql"

    # ── watermark ────────────────────────────────────────────────

    def _ensure_watermark_table(self, engine, db_type: str) -> None:
        tbl = self._WATERMARK_TABLE
        if db_type == "clickhouse":
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {tbl} ("
                " config_key String,"
                " watermark_column String,"
                " value String,"
                " updated_at DateTime64(3)"
                ") ENGINE = ReplacingMergeTree(updated_at)"
                " ORDER BY (config_key, watermark_column)"
            )
        elif db_type == "mssql":
            ddl = (
                f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tbl}')"
                f" CREATE TABLE {tbl} ("
                " config_key NVARCHAR(512) NOT NULL,"
                " watermark_column NVARCHAR(256) NOT NULL,"
                " value NVARCHAR(512) NOT NULL,"
                " updated_at DATETIME2 NOT NULL)"
            )
        else:
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {tbl} ("
                " config_key VARCHAR(512) NOT NULL,"
                " watermark_column VARCHAR(256) NOT NULL,"
                " value VARCHAR(512) NOT NULL,"
                " updated_at TIMESTAMP NOT NULL)"
            )
        with engine.begin() as conn:
            conn.execute(text(ddl))

    def _get_watermark(
        self, engine, db_type: str, key: str, wm_col: str
    ) -> str | None:
        tbl = self._WATERMARK_TABLE
        final = " FINAL" if db_type == "clickhouse" else ""
        sql = (
            f"SELECT value FROM {tbl}{final}"
            " WHERE config_key = :key AND watermark_column = :wm_col"
        )
        with engine.connect() as conn:
            row = conn.execute(
                text(sql), {"key": key, "wm_col": wm_col}
            ).fetchone()
        return row[0] if row else None

    def _save_watermark(
        self, engine, db_type: str, key: str, wm_col: str, value
    ) -> None:
        tbl = self._WATERMARK_TABLE
        val_str = value.isoformat() if isinstance(value, datetime) else str(value)
        now = datetime.now()
        with engine.begin() as conn:
            if db_type != "clickhouse":
                conn.execute(
                    text(
                        f"DELETE FROM {tbl}"
                        " WHERE config_key = :key"
                        " AND watermark_column = :wm_col"
                    ),
                    {"key": key, "wm_col": wm_col},
                )
            conn.execute(
                text(
                    f"INSERT INTO {tbl}"
                    " (config_key, watermark_column, value, updated_at)"
                    " VALUES (:key, :wm_col, :value, :now)"
                ),
                {"key": key, "wm_col": wm_col, "value": val_str, "now": now},
            )

    # ── target DDL ───────────────────────────────────────────────

    def _ensure_target_table(
        self,
        engine,
        db_type: str,
        table: str,
        columns: dict[str, str],
        order_by: list[str] | None,
        engine_clause: str | None,
        partition_by: str | None,
    ) -> None:
        col_defs = ", ".join(f'"{n}" {t}' for n, t in columns.items())

        if db_type == "clickhouse":
            ddl = f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})"
            ddl += f" ENGINE = {engine_clause or 'MergeTree'}"
            if order_by:
                ddl += f" ORDER BY ({', '.join(order_by)})"
            else:
                ddl += " ORDER BY tuple()"
            if partition_by:
                ddl += f" PARTITION BY {partition_by}"
        elif db_type == "mssql":
            tbl_name = table.rsplit(".", 1)[-1]
            ddl = (
                f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tbl_name}')"
                f" CREATE TABLE {table} ({col_defs})"
            )
        else:
            ddl = f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})"

        self.logger.info("Ensuring table: %s", table)
        self.logger.debug("DDL: %s", ddl)
        with engine.begin() as conn:
            conn.execute(text(ddl))

    # ── API calls ────────────────────────────────────────────────

    def _call_api_per_row(
        self,
        client: httpx.Client,
        api_cfg: dict,
        row: dict,
    ) -> dict | None:
        url = _resolve_url(api_cfg["base_url"], row)
        method = api_cfg.get("method", "GET").upper()
        timeout = api_cfg.get("timeout_seconds", 10)
        try:
            resp = client.request(method, url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            self.logger.warning("API failed for %s: %s", url, exc)
            return None

    def _call_api_batch(
        self,
        client: httpx.Client,
        api_cfg: dict,
        rows: list[dict],
    ) -> list[dict | None]:
        url = api_cfg["base_url"]
        method = api_cfg.get("method", "POST").upper()
        timeout = api_cfg.get("timeout_seconds", 10)
        batch_key = api_cfg.get("batch_key", "ids")
        batch_id_col = api_cfg.get("batch_id_column")
        resp_array = api_cfg.get("response_array", "results")
        resp_id_field = api_cfg.get("response_id_field")

        ids = [r.get(batch_id_col) for r in rows] if batch_id_col else []
        body = {batch_key: ids}

        try:
            resp = client.request(method, url, json=body, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            self.logger.warning("Batch API failed: %s", exc)
            return [None] * len(rows)

        items = data.get(resp_array, []) if isinstance(data, dict) else data

        if resp_id_field and batch_id_col:
            by_id = {
                item.get(resp_id_field): item
                for item in items
                if isinstance(item, dict)
            }
            return [by_id.get(r.get(batch_id_col)) for r in rows]

        # positional fallback
        return [items[i] if i < len(items) else None for i in range(len(rows))]

    # ── core pipeline ────────────────────────────────────────────

    def _enrich_table(
        self,
        src_engine,
        tgt_engine,
        tc: dict,
        api_cfg: dict,
        full_cfg: dict,
    ) -> int:
        source_table: str = tc["source_table"]
        target_table: str = tc["target_table"]
        wm_col: str | None = tc.get("watermark_column")
        source_columns: dict[str, str] = tc["source_columns"]
        response_mapping: list[dict] = tc["response_mapping"]
        timestamp_col: str | None = tc.get("timestamp_column")
        batch_size: int = tc.get("batch_size", 500)
        overlap_min: int = tc.get("overlap_minutes", 0)
        mode: str = api_cfg.get("mode", "per_row")

        db_type = self._get_db_type(full_cfg["target"])

        # ── 타겟 컬럼 정의 ──────────────────────────────────────
        target_cols: dict[str, str] = {}
        for col_name, col_type in source_columns.items():
            target_cols[col_name] = col_type
        for mapping in response_mapping:
            target_cols[mapping["column"]] = mapping["type"]
        if timestamp_col:
            target_cols[timestamp_col] = _TIMESTAMP_TYPES.get(
                db_type, "TIMESTAMP"
            )

        # ── 소스 테이블 파싱 ────────────────────────────────────
        if "." in source_table:
            src_schema, src_name = source_table.split(".", 1)
        else:
            src_schema, src_name = "public", source_table

        # ── 타겟 DDL ───────────────────────────────────────────
        self._ensure_target_table(
            tgt_engine,
            db_type,
            target_table,
            target_cols,
            tc.get("order_by"),
            tc.get("engine"),
            tc.get("partition_by"),
        )

        # ── watermark ──────────────────────────────────────────
        self._ensure_watermark_table(tgt_engine, db_type)
        wm_key = f"{full_cfg['source']}.{source_table}"
        watermark = (
            self._get_watermark(tgt_engine, db_type, wm_key, wm_col)
            if wm_col
            else None
        )

        # ── 소스 쿼리 구성 ─────────────────────────────────────
        select_cols = list(source_columns.keys())
        if wm_col and wm_col not in select_cols:
            select_cols.append(wm_col)
        col_list_sql = ", ".join(f'"{c}"' for c in select_cols)

        if watermark:
            cutoff = watermark
            if overlap_min:
                try:
                    wm_dt = datetime.fromisoformat(watermark)
                    cutoff = (
                        wm_dt - timedelta(minutes=overlap_min)
                    ).isoformat()
                except (ValueError, TypeError):
                    pass
            query = (
                f'SELECT {col_list_sql} FROM "{src_schema}"."{src_name}"'
                f' WHERE "{wm_col}" > :cutoff ORDER BY "{wm_col}"'
            )
            params: dict = {"cutoff": cutoff}
            self.logger.info(
                "%s: incremental from %s (overlap %dm)",
                source_table,
                cutoff,
                overlap_min,
            )
        else:
            query = f'SELECT {col_list_sql} FROM "{src_schema}"."{src_name}"'
            if wm_col:
                query += f' ORDER BY "{wm_col}"'
            params = {}
            self.logger.info("%s: full scan", source_table)

        # ── API 헤더 ───────────────────────────────────────────
        headers = {k: _expand_env(v) for k, v in api_cfg.get("headers", {}).items()}

        # ── 스트리밍 enrichment ────────────────────────────────
        src_col_names = list(source_columns.keys())
        total_rows = 0
        max_wm = None

        with (
            httpx.Client(headers=headers) as http_client,
            src_engine.connect() as src_conn,
        ):
            result = src_conn.execute(text(query), params)
            col_keys = list(result.keys())

            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break

                row_dicts = [dict(zip(col_keys, r)) for r in rows]

                # watermark 추적
                if wm_col:
                    for rd in row_dicts:
                        val = rd.get(wm_col)
                        if val is not None and (max_wm is None or val > max_wm):
                            max_wm = val

                # API 호출
                if mode == "batch":
                    responses = self._call_api_batch(
                        http_client, api_cfg, row_dicts
                    )
                else:
                    responses = [
                        self._call_api_per_row(http_client, api_cfg, rd)
                        for rd in row_dicts
                    ]

                # enriched rows 구성
                enriched: list[dict] = []
                for rd, resp in zip(row_dicts, responses):
                    if resp is None:
                        continue
                    row_out: dict = {}
                    for col in src_col_names:
                        row_out[col] = rd.get(col)
                    for mapping in response_mapping:
                        row_out[mapping["column"]] = _extract_json_path(
                            resp, mapping["json_path"]
                        )
                    if timestamp_col:
                        row_out[timestamp_col] = datetime.now()
                    enriched.append(row_out)

                if not enriched:
                    continue

                # 타겟 INSERT
                ins_cols = list(enriched[0].keys())
                col_list = ", ".join(f'"{c}"' for c in ins_cols)
                placeholders = ", ".join(f":{c}" for c in ins_cols)
                insert_sql = text(
                    f"INSERT INTO {target_table} ({col_list})"
                    f" VALUES ({placeholders})"
                )
                with tgt_engine.begin() as tgt_conn:
                    tgt_conn.execute(insert_sql, enriched)

                total_rows += len(enriched)
                self.logger.info(
                    "%s: batch %d rows (total %d)",
                    source_table,
                    len(enriched),
                    total_rows,
                )

        # ── watermark 저장 ─────────────────────────────────────
        if wm_col and max_wm is not None:
            self._save_watermark(tgt_engine, db_type, wm_key, wm_col, max_wm)
            self.logger.info("%s: watermark → %s", source_table, max_wm)

        self.logger.info(
            "%s: complete — %d rows enriched", source_table, total_rows
        )
        return total_rows
