"""API Enrich Job.

소스 DB에서 행을 조회하고 외부 API를 호출하여
JSON 응답을 타겟 DB 테이블에 매핑/저장합니다.

동작 모드:
  - per_row: 소스 행 1건당 API 1회 호출 (확장 기능 지원)
  - batch: 소스 행 N건을 묶어 API 1회 호출 (기존 기능만)

확장 기능 (per_row 모드):
  - 멀티소스 쿼리 체인 (source_queries)
  - S3 객체 로더 (s3_objects)
  - 중첩 요청 바디 템플릿 (request_body_template)
  - 멀티배열 응답 병합 (response_merge)
  - 멀티타겟 쓰기 (child_targets)
  - Upsert / Replace 전략 (write_strategy)
  - 행 단위 병렬 처리 (max_workers)
  - 처리된 키 제외 (exclusion)

설정:
  config JSON 파일로 source/target DB, API 설정, 컬럼 매핑 지정
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import httpx
from sqlalchemy import text

from ..db import create_engine_by_id, create_engine_for, get_database
from .base import Job, JobResult

# ── enrich 서브패키지 ────────────────────────────────────────────
from .enrich.context import RowContext, run_query_chain
from .enrich.exclusion import filter_excluded_rows, get_existing_keys
from .enrich.parallel import run_parallel
from .enrich.request_builder import build_request_body, build_request_url
from .enrich.response_parser import extract_response_mapping, merge_response_arrays
from .enrich.s3_loader import RowSkipError, load_s3_objects
from .enrich.util import (
    _TIMESTAMP_TYPES,
    _expand_env,
    _extract_json_path,
    _resolve_url,
)
from .enrich.writer import (
    ensure_target_table,
    ensure_watermark_table,
    get_watermark,
    save_watermark,
    write_parent_and_children,
    write_rows,
)

# ── 하위 호환을 위한 re-export (기존 테스트에서 import) ────────────
__all__ = [
    "ApiEnrichJob",
    "_extract_json_path",
    "_expand_env",
    "_resolve_url",
]


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

    @staticmethod
    def _validate_config(tc: dict, api_cfg: dict) -> None:
        """비호환 기능 조합을 사전 검증한다."""
        mode = api_cfg.get("mode", "per_row")
        if mode == "batch":
            per_row_only = []
            if tc.get("source_queries"):
                per_row_only.append("source_queries")
            if tc.get("s3_objects"):
                per_row_only.append("s3_objects")
            if api_cfg.get("request_body_template"):
                per_row_only.append("request_body_template")
            if api_cfg.get("response_merge"):
                per_row_only.append("response_merge")
            if per_row_only:
                raise ValueError(
                    f"batch mode is incompatible with: {', '.join(per_row_only)}. "
                    f"Use per_row mode for these features."
                )

        strategy = tc.get("write_strategy", "insert")
        if strategy in ("upsert", "replace") and not tc.get("upsert_key_columns"):
            raise ValueError(
                f"write_strategy '{strategy}' requires upsert_key_columns"
            )

    # ── watermark (delegate to enrich.writer) ────────────────────

    def _ensure_watermark_table(self, engine, db_type: str) -> None:
        ensure_watermark_table(engine, db_type)

    def _get_watermark(
        self, engine, db_type: str, key: str, wm_col: str
    ) -> str | None:
        return get_watermark(engine, db_type, key, wm_col)

    def _save_watermark(
        self, engine, db_type: str, key: str, wm_col: str, value
    ) -> None:
        save_watermark(engine, db_type, key, wm_col, value)

    # ── target DDL (delegate to enrich.writer) ───────────────────

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
        ensure_target_table(
            engine, db_type, table, columns, order_by, engine_clause, partition_by
        )

    # ── API calls ────────────────────────────────────────────────

    def _call_api_per_row(
        self,
        client: httpx.Client,
        api_cfg: dict,
        row: dict,
        *,
        context: RowContext | None = None,
    ) -> dict | None:
        """per_row API 호출. context가 있으면 확장 기능 사용."""
        if context is not None:
            url = build_request_url(api_cfg["base_url"], context)
        else:
            url = _resolve_url(api_cfg["base_url"], row)

        method = api_cfg.get("method", "GET").upper()
        timeout = api_cfg.get("timeout_seconds", 10)

        kwargs: dict = {"timeout": timeout}
        body_template = api_cfg.get("request_body_template")
        if body_template and context is not None:
            kwargs["json"] = build_request_body(body_template, context)

        try:
            resp = client.request(method, url, **kwargs)
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

    # ── per-row pipeline (for parallel execution) ────────────────

    def _process_single_row(
        self,
        row: dict,
        tc: dict,
        api_cfg: dict,
        full_cfg: dict,
        query_engines: dict,
        http_client: httpx.Client,
        src_col_names: list[str],
        response_mapping: list[dict],
        merge_config: list[dict] | None,
        timestamp_col: str | None,
    ) -> tuple[list[dict], dict | None]:
        """단일 행 파이프라인. (enriched_rows, api_response) 반환.

        enriched_rows가 비어있으면 스킵된 행.
        예외 발생 시 실패 행.
        """
        # 1. 쿼리 체인 실행
        query_chain = tc.get("source_queries", [])
        ctx = run_query_chain(row, query_chain, query_engines)

        # 2. S3 객체 로드
        s3_config = tc.get("s3_objects")
        if s3_config:
            collections = load_s3_objects(ctx, s3_config)
            ctx.collections.update(collections)

        # 3. API 호출
        resp = self._call_api_per_row(http_client, api_cfg, row, context=ctx)
        if resp is None:
            return [], None

        # 4. 응답 파싱
        if merge_config:
            merged_elements = merge_response_arrays(resp, merge_config)
            if not merged_elements:
                return [], resp

            enriched: list[dict] = []
            for elem in merged_elements:
                row_out: dict = {}
                for col in src_col_names:
                    row_out[col] = row.get(col)
                row_out.update(extract_response_mapping(elem, response_mapping))
                if timestamp_col:
                    row_out[timestamp_col] = datetime.now()
                enriched.append(row_out)
            return enriched, resp
        else:
            row_out: dict = {}
            for col in src_col_names:
                row_out[col] = row.get(col)
            row_out.update(extract_response_mapping(resp, response_mapping))
            if timestamp_col:
                row_out[timestamp_col] = datetime.now()
            return [row_out], resp

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
        max_workers: int = api_cfg.get("max_workers", 1)

        # 확장 기능 설정
        exclusion_cfg = tc.get("exclusion")
        child_configs = tc.get("child_targets", [])
        write_strategy = tc.get("write_strategy", "insert")
        upsert_key_cols = tc.get("upsert_key_columns")
        merge_config = api_cfg.get("response_merge")

        # Config 검증
        self._validate_config(tc, api_cfg)

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

        # ── 타겟 DDL (부모 + 자식) ─────────────────────────────
        self._ensure_target_table(
            tgt_engine, db_type, target_table, target_cols,
            tc.get("order_by"), tc.get("engine"), tc.get("partition_by"),
        )
        for child_cfg in child_configs:
            child_cols: dict[str, str] = {
                child_cfg["foreign_key_column"]: target_cols.get(
                    child_cfg["parent_key_column"],
                    source_columns.get(child_cfg["parent_key_column"], "String"),
                )
            }
            for cm in child_cfg.get("response_mapping", []):
                child_cols[cm["column"]] = cm["type"]
            if timestamp_col:
                child_cols[timestamp_col] = _TIMESTAMP_TYPES.get(db_type, "TIMESTAMP")
            self._ensure_target_table(
                tgt_engine, db_type, child_cfg["target_table"], child_cols,
                child_cfg.get("order_by"), child_cfg.get("engine"),
                child_cfg.get("partition_by"),
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
                source_table, cutoff, overlap_min,
            )
        else:
            query = f'SELECT {col_list_sql} FROM "{src_schema}"."{src_name}"'
            if wm_col:
                query += f' ORDER BY "{wm_col}"'
            params = {}
            self.logger.info("%s: full scan", source_table)

        # ── API 헤더 ───────────────────────────────────────────
        headers = {k: _expand_env(v) for k, v in api_cfg.get("headers", {}).items()}

        # ── 쿼리 체인용 엔진 사전 생성 ────────────────────────
        query_engines: dict = {}
        for qcfg in tc.get("source_queries", []):
            db_id = qcfg["db"]
            if db_id not in query_engines:
                query_engines[db_id] = self._build_engine(
                    {db_id: full_cfg.get(db_id, db_id)}, db_id
                ) if isinstance(full_cfg.get(db_id), dict) else create_engine_by_id(db_id)

        # ── 스트리밍 enrichment ────────────────────────────────
        src_col_names = list(source_columns.keys())
        total_rows = 0
        max_wm = None

        try:
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

                    # ── exclusion 필터 ────────────────────────
                    if exclusion_cfg and exclusion_cfg.get("enabled"):
                        src_key = exclusion_cfg["key_column"]
                        tgt_key = exclusion_cfg.get("target_key_column", src_key)
                        candidate_keys = [r.get(src_key) for r in row_dicts]
                        existing = get_existing_keys(
                            tgt_engine, db_type, target_table, tgt_key,
                            candidate_keys=candidate_keys,
                        )
                        row_dicts = filter_excluded_rows(row_dicts, existing, src_key)
                        if not row_dicts:
                            continue

                    # ── batch 모드 (기존 동작) ────────────────
                    if mode == "batch":
                        responses = self._call_api_batch(
                            http_client, api_cfg, row_dicts
                        )
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

                        if enriched:
                            write_rows(
                                tgt_engine, db_type, target_table, enriched,
                                strategy=write_strategy,
                                key_columns=upsert_key_cols,
                            )
                            total_rows += len(enriched)
                        continue

                    # ── per_row 모드 (확장 파이프라인) ─────────
                    if max_workers > 1:
                        # 병렬 실행
                        def _pipeline(row):
                            enriched_rows, _ = self._process_single_row(
                                row, tc, api_cfg, full_cfg,
                                query_engines, http_client,
                                src_col_names, response_mapping,
                                merge_config, timestamp_col,
                            )
                            return enriched_rows

                        enriched_rows, stats = run_parallel(
                            row_dicts, _pipeline, max_workers,
                        )
                    else:
                        # 순차 실행
                        enriched_rows = []
                        all_responses: list[dict | None] = []
                        for rd in row_dicts:
                            try:
                                er, resp = self._process_single_row(
                                    rd, tc, api_cfg, full_cfg,
                                    query_engines, http_client,
                                    src_col_names, response_mapping,
                                    merge_config, timestamp_col,
                                )
                                enriched_rows.extend(er)
                                all_responses.append(resp)
                            except RowSkipError:
                                self.logger.info("Row skipped (S3 failure)")
                                all_responses.append(None)
                            except Exception:
                                self.logger.warning("Row pipeline failed", exc_info=True)
                                all_responses.append(None)

                    if not enriched_rows:
                        continue

                    # ── 쓰기 (메인 스레드) ────────────────────
                    if child_configs:
                        # 멀티타겟: 부모 + 자식
                        # 병렬 모드에서는 all_responses가 없으므로 부모만 직접 쓰기
                        if max_workers > 1:
                            n = write_rows(
                                tgt_engine, db_type, target_table,
                                enriched_rows, strategy=write_strategy,
                                key_columns=upsert_key_cols,
                            )
                        else:
                            n = write_parent_and_children(
                                tgt_engine, db_type, target_table,
                                enriched_rows, write_strategy, upsert_key_cols,
                                child_configs, all_responses, row_dicts,
                                timestamp_col,
                            )
                    else:
                        n = write_rows(
                            tgt_engine, db_type, target_table,
                            enriched_rows, strategy=write_strategy,
                            key_columns=upsert_key_cols,
                        )

                    total_rows += n
                    self.logger.info(
                        "%s: batch %d rows (total %d)",
                        source_table, n, total_rows,
                    )
        finally:
            for eng in query_engines.values():
                eng.dispose()

        # ── watermark 저장 ─────────────────────────────────────
        if wm_col and max_wm is not None:
            self._save_watermark(tgt_engine, db_type, wm_key, wm_col, max_wm)
            self.logger.info("%s: watermark → %s", source_table, max_wm)

        self.logger.info(
            "%s: complete — %d rows enriched", source_table, total_rows
        )
        return total_rows
