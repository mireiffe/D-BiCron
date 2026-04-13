"""Tests for dbcron/jobs/api_enrich.py."""

from __future__ import annotations

import json
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from dbcron.jobs.api_enrich import (
    ApiEnrichJob,
    _expand_env,
    _extract_json_path,
    _resolve_url,
)

# ── fixtures ────────────────────────────────────────────────────


@pytest.fixture()
def api_enrich_config(tmp_path):
    cfg = {
        "source": "pg_src",
        "target": "ch_tgt",
        "api": {
            "base_url": "https://api.example.com/v1/orders/{order_code}",
            "method": "GET",
            "headers": {"Authorization": "Bearer test-token"},
            "timeout_seconds": 5,
            "mode": "per_row",
        },
        "tables": [
            {
                "source_table": "public.orders",
                "source_columns": {"id": "Int64", "order_code": "String"},
                "watermark_column": "updated_at",
                "target_table": "default.order_enriched",
                "timestamp_column": "_enriched_at",
                "response_mapping": [
                    {"json_path": "status", "column": "order_status", "type": "String"},
                    {
                        "json_path": "payment.amount",
                        "column": "pay_amount",
                        "type": "Decimal(10,2)",
                    },
                ],
                "engine": "MergeTree",
                "order_by": ["id"],
                "batch_size": 100,
            },
        ],
    }
    path = tmp_path / "api_enrich_config.json"
    path.write_text(json.dumps(cfg, indent=2))
    return str(path)


@pytest.fixture()
def batch_api_config(tmp_path):
    cfg = {
        "source": "pg_src",
        "target": "ch_tgt",
        "api": {
            "base_url": "https://api.example.com/v1/orders/batch",
            "method": "POST",
            "headers": {},
            "timeout_seconds": 10,
            "mode": "batch",
            "batch_key": "codes",
            "batch_id_column": "order_code",
            "response_array": "results",
            "response_id_field": "code",
        },
        "tables": [
            {
                "source_table": "public.orders",
                "source_columns": {"id": "Int64", "order_code": "String"},
                "target_table": "default.order_enriched",
                "response_mapping": [
                    {"json_path": "status", "column": "order_status", "type": "String"},
                ],
                "engine": "MergeTree",
                "order_by": ["id"],
            },
        ],
    }
    path = tmp_path / "batch_config.json"
    path.write_text(json.dumps(cfg, indent=2))
    return str(path)


# ── 1. _extract_json_path ──────────────────────────────────────


class TestExtractJsonPath:
    def test_simple_key(self):
        assert _extract_json_path({"status": "ok"}, "status") == "ok"

    def test_nested_dot(self):
        data = {"payment": {"amount": 42.5}}
        assert _extract_json_path(data, "payment.amount") == 42.5

    def test_array_index(self):
        data = {"items": [{"name": "A"}, {"name": "B"}]}
        assert _extract_json_path(data, "items[0].name") == "A"
        assert _extract_json_path(data, "items[1].name") == "B"

    def test_missing_key_returns_none(self):
        assert _extract_json_path({"a": 1}, "b") is None

    def test_missing_nested_returns_none(self):
        assert _extract_json_path({"a": {"b": 1}}, "a.c") is None

    def test_array_out_of_bounds(self):
        data = {"items": [{"x": 1}]}
        assert _extract_json_path(data, "items[5].x") is None

    def test_deep_nesting(self):
        data = {"a": {"b": {"c": {"d": "deep"}}}}
        assert _extract_json_path(data, "a.b.c.d") == "deep"


# ── 2. _expand_env ─────────────────────────────────────────────


class TestExpandEnv:
    def test_replaces_env_var(self):
        with patch.dict(os.environ, {"MY_TOKEN": "secret123"}):
            assert _expand_env("Bearer ${MY_TOKEN}") == "Bearer secret123"

    def test_missing_var_kept(self):
        result = _expand_env("Bearer ${NONEXISTENT_VAR_12345}")
        assert "${NONEXISTENT_VAR_12345}" in result

    def test_no_placeholders(self):
        assert _expand_env("plain text") == "plain text"

    def test_multiple_vars(self):
        with patch.dict(os.environ, {"A": "1", "B": "2"}):
            assert _expand_env("${A}-${B}") == "1-2"


# ── 3. _resolve_url ────────────────────────────────────────────


class TestResolveUrl:
    def test_basic_substitution(self):
        url = _resolve_url(
            "https://api.com/orders/{code}", {"code": "ABC123"}
        )
        assert url == "https://api.com/orders/ABC123"

    def test_url_encoding(self):
        url = _resolve_url(
            "https://api.com/{q}", {"q": "hello world"}
        )
        assert url == "https://api.com/hello%20world"

    def test_multiple_placeholders(self):
        url = _resolve_url(
            "https://api.com/{a}/{b}", {"a": "x", "b": "y"}
        )
        assert url == "https://api.com/x/y"

    def test_missing_key_empty(self):
        url = _resolve_url("https://api.com/{missing}", {})
        assert url == "https://api.com/"


# ── 4. DDL ─────────────────────────────────────────────────────


class TestEnsureTargetTable:
    def _make_job(self):
        return ApiEnrichJob(config=None)

    def _get_ddl(self, db_type, **kwargs):
        job = self._make_job()
        engine = MagicMock()
        ctx = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        columns = kwargs.get(
            "columns", {"id": "Int64", "name": "String"}
        )
        job._ensure_target_table(
            engine,
            db_type,
            kwargs.get("table", "test_table"),
            columns,
            kwargs.get("order_by", ["id"]),
            kwargs.get("engine_clause", "MergeTree"),
            kwargs.get("partition_by"),
        )
        call_args = ctx.execute.call_args[0][0]
        return call_args.text if hasattr(call_args, "text") else str(call_args)

    def test_clickhouse_has_engine(self):
        ddl = self._get_ddl("clickhouse")
        assert "ENGINE = MergeTree" in ddl
        assert "ORDER BY" in ddl

    def test_clickhouse_partition(self):
        ddl = self._get_ddl("clickhouse", partition_by="toYYYYMM(ts)")
        assert "PARTITION BY toYYYYMM(ts)" in ddl

    def test_pg_no_engine(self):
        ddl = self._get_ddl("postgresql")
        assert "ENGINE" not in ddl
        assert "CREATE TABLE IF NOT EXISTS" in ddl

    def test_mssql_if_not_exists(self):
        ddl = self._get_ddl("mssql", table="dbo.test_table")
        assert "IF NOT EXISTS" in ddl
        assert "sys.tables" in ddl

    def test_sqlite_simple(self):
        ddl = self._get_ddl("sqlite")
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "ENGINE" not in ddl


# ── 5. Watermark DDL ──────────────────────────────────────────


class TestEnsureWatermarkTable:
    def _make_job(self):
        return ApiEnrichJob(config=None)

    def _get_ddl(self, db_type):
        job = self._make_job()
        engine = MagicMock()
        ctx = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        job._ensure_watermark_table(engine, db_type)
        call_args = ctx.execute.call_args[0][0]
        return call_args.text if hasattr(call_args, "text") else str(call_args)

    def test_clickhouse_replacing_merge_tree(self):
        ddl = self._get_ddl("clickhouse")
        assert "ReplacingMergeTree" in ddl
        assert "_api_enrich_watermarks" in ddl

    def test_pg_standard_table(self):
        ddl = self._get_ddl("postgresql")
        assert "VARCHAR" in ddl
        assert "ReplacingMergeTree" not in ddl

    def test_mssql_nvarchar(self):
        ddl = self._get_ddl("mssql")
        assert "NVARCHAR" in ddl
        assert "sys.tables" in ddl


# ── 6. Watermark get/save ──────────────────────────────────────


class TestWatermark:
    def _make_job(self):
        return ApiEnrichJob(config=None)

    def test_get_watermark_ch_uses_final(self):
        job = self._make_job()
        engine = MagicMock()
        ctx = MagicMock()
        ctx.execute.return_value.fetchone.return_value = ("2026-01-01",)
        engine.connect.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        val = job._get_watermark(engine, "clickhouse", "k", "wm")
        sql = ctx.execute.call_args[0][0]
        assert "FINAL" in (sql.text if hasattr(sql, "text") else str(sql))
        assert val == "2026-01-01"

    def test_get_watermark_pg_no_final(self):
        job = self._make_job()
        engine = MagicMock()
        ctx = MagicMock()
        ctx.execute.return_value.fetchone.return_value = None
        engine.connect.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        val = job._get_watermark(engine, "postgresql", "k", "wm")
        sql = ctx.execute.call_args[0][0]
        assert "FINAL" not in (sql.text if hasattr(sql, "text") else str(sql))
        assert val is None

    def test_save_watermark_ch_insert_only(self):
        job = self._make_job()
        engine = MagicMock()
        ctx = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        job._save_watermark(engine, "clickhouse", "k", "wm", "val")
        # CH: only INSERT, no DELETE
        assert ctx.execute.call_count == 1
        sql = ctx.execute.call_args[0][0]
        assert "INSERT" in (sql.text if hasattr(sql, "text") else str(sql))

    def test_save_watermark_pg_delete_then_insert(self):
        job = self._make_job()
        engine = MagicMock()
        ctx = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        job._save_watermark(engine, "postgresql", "k", "wm", "val")
        # PG: DELETE then INSERT
        assert ctx.execute.call_count == 2
        first_sql = ctx.execute.call_args_list[0][0][0]
        assert "DELETE" in (first_sql.text if hasattr(first_sql, "text") else str(first_sql))

    def test_save_watermark_datetime_value(self):
        job = self._make_job()
        engine = MagicMock()
        ctx = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        dt = datetime(2026, 1, 1, 12, 0, 0)
        job._save_watermark(engine, "clickhouse", "k", "wm", dt)
        params = ctx.execute.call_args[0][1]
        assert params["value"] == "2026-01-01T12:00:00"


# ── 7. API call per_row ───────────────────────────────────────


class TestCallApiPerRow:
    def _make_job(self):
        return ApiEnrichJob(config=None)

    def test_success(self):
        job = self._make_job()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "shipped"}
        mock_resp.raise_for_status = MagicMock()
        client = MagicMock(spec=httpx.Client)
        client.request.return_value = mock_resp

        api_cfg = {
            "base_url": "https://api.com/{id}",
            "method": "GET",
            "timeout_seconds": 5,
        }
        result = job._call_api_per_row(client, api_cfg, {"id": "123"})
        assert result == {"status": "shipped"}

    def test_http_error_returns_none(self):
        job = self._make_job()
        client = MagicMock(spec=httpx.Client)
        client.request.side_effect = httpx.ConnectError("timeout")

        api_cfg = {
            "base_url": "https://api.com/{id}",
            "method": "GET",
            "timeout_seconds": 1,
        }
        result = job._call_api_per_row(client, api_cfg, {"id": "123"})
        assert result is None

    def test_bad_json_returns_none(self):
        job = self._make_job()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.side_effect = ValueError("bad json")
        client = MagicMock(spec=httpx.Client)
        client.request.return_value = mock_resp

        api_cfg = {"base_url": "https://api.com/{id}", "method": "GET"}
        result = job._call_api_per_row(client, api_cfg, {"id": "1"})
        assert result is None


# ── 8. API call batch ──────────────────────────────────────────


class TestCallApiBatch:
    def _make_job(self):
        return ApiEnrichJob(config=None)

    def test_batch_with_id_matching(self):
        job = self._make_job()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "results": [
                {"code": "B", "status": "pending"},
                {"code": "A", "status": "shipped"},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        client = MagicMock(spec=httpx.Client)
        client.request.return_value = mock_resp

        api_cfg = {
            "base_url": "https://api.com/batch",
            "method": "POST",
            "batch_key": "codes",
            "batch_id_column": "order_code",
            "response_array": "results",
            "response_id_field": "code",
        }
        rows = [{"order_code": "A"}, {"order_code": "B"}]
        results = job._call_api_batch(client, api_cfg, rows)
        assert results[0]["status"] == "shipped"  # A
        assert results[1]["status"] == "pending"  # B

    def test_batch_failure_returns_nones(self):
        job = self._make_job()
        client = MagicMock(spec=httpx.Client)
        client.request.side_effect = httpx.ConnectError("fail")

        api_cfg = {
            "base_url": "https://api.com/batch",
            "method": "POST",
            "batch_key": "ids",
            "batch_id_column": "id",
            "response_array": "results",
            "response_id_field": "id",
        }
        rows = [{"id": 1}, {"id": 2}]
        results = job._call_api_batch(client, api_cfg, rows)
        assert results == [None, None]

    def test_batch_positional_fallback(self):
        job = self._make_job()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"results": [{"v": 1}, {"v": 2}]}
        mock_resp.raise_for_status = MagicMock()
        client = MagicMock(spec=httpx.Client)
        client.request.return_value = mock_resp

        api_cfg = {
            "base_url": "https://api.com/batch",
            "method": "POST",
            "batch_key": "ids",
            "response_array": "results",
            # no batch_id_column / response_id_field → positional
        }
        rows = [{"id": 1}, {"id": 2}]
        results = job._call_api_batch(client, api_cfg, rows)
        assert results[0] == {"v": 1}
        assert results[1] == {"v": 2}


# ── 9. run() validation ───────────────────────────────────────


class TestRunValidation:
    def _make_job(self):
        return ApiEnrichJob(config=None)

    def test_source_not_found(self, api_enrich_config, tmp_data_dir):
        job = self._make_job()
        with patch(
            "dbcron.jobs.api_enrich.get_database", side_effect=lambda x: None
        ):
            result = job.run(config=api_enrich_config)
        assert not result.success
        assert "Source DB" in result.message

    def test_target_not_found(self, api_enrich_config, tmp_data_dir):
        pg_db = {"id": "pg_src", "type": "postgresql", "host": "localhost"}

        def fake_get(db_id):
            return pg_db if db_id == "pg_src" else None

        job = self._make_job()
        with patch("dbcron.jobs.api_enrich.get_database", side_effect=fake_get):
            result = job.run(config=api_enrich_config)
        assert not result.success
        assert "Target DB" in result.message


# ── 10. Job meta ──────────────────────────────────────────────


class TestJobMeta:
    def test_name(self):
        assert ApiEnrichJob.name == "api_enrich"

    def test_scope_is_pipeline(self):
        assert ApiEnrichJob.scope == "pipeline"

    def test_default_args_contains_config(self):
        assert "config" in ApiEnrichJob.default_args

    def test_load_config(self, api_enrich_config):
        cfg = ApiEnrichJob._load_config(api_enrich_config)
        assert cfg["source"] == "pg_src"
        assert cfg["target"] == "ch_tgt"
        assert len(cfg["tables"]) == 1

    def test_load_config_missing(self):
        with pytest.raises(FileNotFoundError):
            ApiEnrichJob._load_config("/tmp/nonexistent_api_enrich_12345.json")


# ── 11. _get_db_type ──────────────────────────────────────────


class TestGetDbType:
    def test_string_id(self):
        with patch(
            "dbcron.jobs.api_enrich.get_database",
            return_value={"type": "clickhouse"},
        ):
            assert ApiEnrichJob._get_db_type("ch_tgt") == "clickhouse"

    def test_inline_dict(self):
        assert ApiEnrichJob._get_db_type({"type": "mssql"}) == "mssql"

    def test_unknown_defaults_to_pg(self):
        with patch("dbcron.jobs.api_enrich.get_database", return_value=None):
            assert ApiEnrichJob._get_db_type("unknown") == "postgresql"
