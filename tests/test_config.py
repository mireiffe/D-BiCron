"""Tests for pg2ch.config."""

from __future__ import annotations

import textwrap

import pytest

from pg2ch.config import (
    TableConfig,
    load_all_table_configs,
    load_table_config,
    split_qualified,
)


def _base(**over) -> dict:
    d = {
        "table_id": "orders",
        "source": "pg",
        "target": "ch",
        "source_table": "public.orders",
        "target_table": "default.orders",
        "sync_mode": "full_reload",
        "order_by": ["id"],
    }
    d.update(over)
    return d


class TestSplitQualified:
    def test_qualified(self):
        assert split_qualified("public.orders", "x") == ("public", "orders")

    def test_unqualified(self):
        assert split_qualified("orders", "public") == ("public", "orders")


class TestValidation:
    def test_minimal_full_reload_ok(self):
        cfg = TableConfig.from_dict(_base())
        assert cfg.dag_id == "pg2ch_orders"
        assert cfg.sync_mode == "full_reload"

    def test_append_requires_watermark(self):
        with pytest.raises(ValueError, match="append mode requires watermark_column"):
            TableConfig.from_dict(_base(sync_mode="append"))

    def test_append_with_watermark_ok(self):
        cfg = TableConfig.from_dict(_base(sync_mode="append", watermark_column="updated_at"))
        assert cfg.effective_watermark_column == "updated_at"

    def test_append_with_timestamp_fallback(self):
        cfg = TableConfig.from_dict(_base(sync_mode="append", timestamp_column="updated_at"))
        assert cfg.effective_watermark_column == "updated_at"

    def test_bad_sync_mode(self):
        with pytest.raises(ValueError, match="sync_mode must be"):
            TableConfig.from_dict(_base(sync_mode="upsert"))

    def test_bad_on_row_error(self):
        with pytest.raises(ValueError, match="on_row_error must be"):
            TableConfig.from_dict(_base(on_row_error="explode"))

    def test_missing_required(self):
        d = _base()
        del d["source_table"]
        with pytest.raises(ValueError, match="missing required"):
            TableConfig.from_dict(d)

    def test_unknown_key(self):
        with pytest.raises(ValueError, match="unknown config key"):
            TableConfig.from_dict(_base(typoed_key=1))

    def test_comment_keys_allowed(self):
        cfg = TableConfig.from_dict(_base(_comment="hi", _label="orders"))
        assert cfg.table_id == "orders"

    def test_bad_table_id(self):
        with pytest.raises(ValueError, match="table_id"):
            TableConfig.from_dict(_base(table_id="bad id!"))

    def test_sync_since_requires_timestamp(self):
        with pytest.raises(ValueError, match="sync_since requires timestamp_column"):
            TableConfig.from_dict(_base(sync_since="30d"))

    def test_empty_order_by(self):
        with pytest.raises(ValueError, match="order_by is required"):
            TableConfig.from_dict(_base(order_by=[]))

    def test_bad_batch_size(self):
        with pytest.raises(ValueError, match="batch_size"):
            TableConfig.from_dict(_base(batch_size=0))


class TestLoaders:
    def _write(self, p, body):
        p.write_text(textwrap.dedent(body), encoding="utf-8")

    def test_load_table_config(self, tmp_path):
        f = tmp_path / "orders.yaml"
        self._write(f, """
            table_id: orders
            source: pg
            target: ch
            source_table: public.orders
            target_table: default.orders
            sync_mode: append
            watermark_column: updated_at
            order_by: [id]
        """)
        cfg = load_table_config(f)
        assert cfg.sync_mode == "append"
        assert cfg.watermark_column == "updated_at"

    def test_table_id_defaults_to_stem(self, tmp_path):
        f = tmp_path / "events.yaml"
        self._write(f, """
            source: pg
            target: ch
            source_table: public.events
            target_table: default.events
            sync_mode: full_reload
            order_by: [id]
        """)
        cfg = load_table_config(f)
        assert cfg.table_id == "events"

    def test_defaults_merge(self, tmp_path):
        self._write(tmp_path / "_defaults.yaml", """
            source: pg
            target: ch
            meta: meta
            batch_size: 99999
            settings: {index_granularity: 8192}
        """)
        self._write(tmp_path / "orders.yaml", """
            source_table: public.orders
            target_table: default.orders
            sync_mode: full_reload
            order_by: [id]
            settings: {merge_with_ttl_timeout: 100}
        """)
        configs = load_all_table_configs(tmp_path)
        assert len(configs) == 1
        cfg = configs[0]
        assert cfg.source == "pg"
        assert cfg.batch_size == 99999
        # settings deep-merged
        assert cfg.settings == {"index_granularity": 8192, "merge_with_ttl_timeout": 100}

    def test_table_override_wins(self, tmp_path):
        self._write(tmp_path / "_defaults.yaml", "batch_size: 100\nsource: pg\ntarget: ch\n")
        self._write(tmp_path / "t.yaml", """
            source_table: public.t
            target_table: default.t
            sync_mode: full_reload
            order_by: [id]
            batch_size: 7
        """)
        cfg = load_all_table_configs(tmp_path)[0]
        assert cfg.batch_size == 7

    def test_example_files_skipped(self, tmp_path):
        self._write(tmp_path / "x.example.yaml", "garbage: true\n")
        assert load_all_table_configs(tmp_path) == []

    def test_duplicate_table_id_raises(self, tmp_path):
        body = """
            table_id: dup
            source: pg
            target: ch
            source_table: public.a
            target_table: default.a
            sync_mode: full_reload
            order_by: [id]
        """
        self._write(tmp_path / "a.yaml", body)
        self._write(tmp_path / "b.yaml", body)
        with pytest.raises(ValueError, match="duplicate table_id"):
            load_all_table_configs(tmp_path)

    def test_missing_dir_returns_empty(self, tmp_path):
        assert load_all_table_configs(tmp_path / "nope") == []
