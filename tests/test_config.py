"""Tests for pg2ch.config."""

from __future__ import annotations

import textwrap

import pytest

from pg2ch.config import (
    TableConfig,
    load_all_table_configs,
    load_retention_config,
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
        cfg = TableConfig.from_dict(
            _base(
                sync_mode="append",
                watermark_column="updated_at",
                watermark_type="timestamp",
            )
        )
        assert cfg.watermark_column == "updated_at"
        assert cfg.watermark_type == "timestamp"

    def test_watermark_column_requires_type(self):
        with pytest.raises(ValueError, match="watermark_column requires watermark_type"):
            TableConfig.from_dict(
                _base(sync_mode="append", watermark_column="updated_at")
            )

    def test_watermark_type_requires_column(self):
        with pytest.raises(ValueError, match="watermark_type requires watermark_column"):
            TableConfig.from_dict(_base(watermark_type="serial"))

    def test_unknown_watermark_type(self):
        with pytest.raises(ValueError, match="watermark_type must be one of"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="id",
                    watermark_type="uuid",
                )
            )

    def test_legacy_timestamp_column_rejected_with_hint(self):
        with pytest.raises(ValueError, match="watermark_column \\+ watermark_type"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="updated_at",
                    watermark_type="timestamp",
                    timestamp_column="updated_at",
                )
            )

    def test_legacy_overlap_minutes_rejected_with_hint(self):
        with pytest.raises(ValueError, match="use watermark_overlap"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="updated_at",
                    watermark_type="timestamp",
                    overlap_minutes=30,
                )
            )

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

    def test_sync_since_requires_watermark(self):
        with pytest.raises(ValueError, match="sync_since requires watermark_column"):
            TableConfig.from_dict(_base(sync_since="30d"))

    def test_sync_since_validated_against_watermark_type(self):
        with pytest.raises(ValueError, match="sync_since.*not a number"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="id",
                    watermark_type="serial",
                    sync_since="30d",
                )
            )

    def test_sync_since_serial_number_ok(self):
        cfg = TableConfig.from_dict(
            _base(
                sync_mode="append",
                watermark_column="id",
                watermark_type="serial",
                sync_since=100000,
            )
        )
        assert cfg.sync_since == 100000

    def test_overlap_validated_against_watermark_type(self):
        with pytest.raises(ValueError, match="relative like '30m'"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="updated_at",
                    watermark_type="timestamp",
                    watermark_overlap=30,
                )
            )

    def test_overlap_timestamp_relative_ok(self):
        cfg = TableConfig.from_dict(
            _base(
                sync_mode="append",
                watermark_column="updated_at",
                watermark_type="timestamp",
                watermark_overlap="30m",
            )
        )
        assert cfg.watermark_overlap == "30m"

    def test_empty_order_by(self):
        with pytest.raises(ValueError, match="order_by is required"):
            TableConfig.from_dict(_base(order_by=[]))

    def test_bad_batch_size(self):
        with pytest.raises(ValueError, match="batch_size"):
            TableConfig.from_dict(_base(batch_size=0))

    def test_bad_insert_types_check(self):
        with pytest.raises(ValueError, match="insert_types_check"):
            TableConfig.from_dict(_base(insert_types_check="false"))

    def test_legacy_retention_block_rejected_with_migration_hint(self):
        with pytest.raises(ValueError, match="config/retention.yaml"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="id",
                    retention={"enabled": True, "source_retention": "180d"},
                )
            )

    def test_legacy_flat_retention_keys_rejected(self):
        with pytest.raises(ValueError, match="no longer configured per table"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="id",
                    retention_enabled=True,
                    source_retention="180d",
                )
            )

    def test_integrity_defaults(self):
        cfg = TableConfig.from_dict(
            _base(sync_mode="append", watermark_column="id", watermark_type="serial")
        )
        assert cfg.integrity_enabled is False
        assert cfg.integrity_method == "count"
        assert cfg.integrity_lookback_runs == 1
        assert cfg.integrity_on_mismatch == "fail"
        assert cfg.integrity_tolerance == 0
        assert cfg.integrity_repair is True
        assert cfg.integrity_repair_attempts == 1
        assert cfg.integrity_batch_size == 1_000_000

    def test_integrity_nested_config_ok(self):
        cfg = TableConfig.from_dict(
            _base(
                sync_mode="append",
                watermark_column="id",
                watermark_type="serial",
                integrity={
                    "enabled": True,
                    "method": "key_diff",
                    "lookback_runs": 3,
                    "on_mismatch": "warn",
                    "tolerance": 5,
                    "repair": False,
                    "repair_attempts": 2,
                    "batch_size": 250_000,
                },
            )
        )
        assert cfg.integrity_enabled is True
        assert cfg.integrity_method == "key_diff"
        assert cfg.integrity_lookback_runs == 3
        assert cfg.integrity_on_mismatch == "warn"
        assert cfg.integrity_tolerance == 5
        assert cfg.integrity_repair is False
        assert cfg.integrity_repair_attempts == 2
        assert cfg.integrity_batch_size == 250_000

    def test_integrity_bad_method(self):
        with pytest.raises(ValueError, match="integrity_method must be"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append", watermark_column="id",
                    integrity={"method": "bogus"},
                )
            )

    def test_integrity_repair_attempts_must_be_positive(self):
        with pytest.raises(ValueError, match="integrity_repair_attempts must be >= 1"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append", watermark_column="id",
                    integrity={"repair_attempts": 0},
                )
            )

    def test_integrity_batch_size_must_be_positive(self):
        with pytest.raises(ValueError, match="integrity_batch_size must be a positive"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append", watermark_column="id",
                    integrity={"batch_size": 0},
                )
            )

    def test_integrity_flat_config_ok(self):
        cfg = TableConfig.from_dict(
            _base(
                sync_mode="append",
                watermark_column="id",
                watermark_type="serial",
                integrity_enabled=True,
                integrity_lookback_runs=2,
            )
        )
        assert cfg.integrity_enabled is True
        assert cfg.integrity_lookback_runs == 2

    def test_integrity_unknown_key_rejected(self):
        with pytest.raises(ValueError, match="unknown integrity key"):
            TableConfig.from_dict(
                _base(sync_mode="append", watermark_column="id", integrity={"nope": 1})
            )

    def test_integrity_bad_on_mismatch(self):
        with pytest.raises(ValueError, match="integrity_on_mismatch must be"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="id",
                    integrity={"on_mismatch": "explode"},
                )
            )

    def test_integrity_lookback_must_be_positive(self):
        with pytest.raises(ValueError, match="integrity_lookback_runs must be >= 1"):
            TableConfig.from_dict(
                _base(
                    sync_mode="append",
                    watermark_column="id",
                    integrity={"lookback_runs": 0},
                )
            )

    def test_integrity_enabled_requires_append(self):
        with pytest.raises(ValueError, match="integrity_enabled requires append"):
            TableConfig.from_dict(
                _base(sync_mode="full_reload", integrity_enabled=True)
            )


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
            watermark_type: timestamp
            order_by: [id]
        """)
        cfg = load_table_config(f)
        assert cfg.sync_mode == "append"
        assert cfg.watermark_column == "updated_at"
        assert cfg.watermark_type == "timestamp"

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


class TestRetentionConfig:
    def _load(self, tmp_path, body):
        f = tmp_path / "retention.yaml"
        f.write_text(textwrap.dedent(body), encoding="utf-8")
        return load_retention_config(f)

    def test_missing_file_returns_none(self, tmp_path):
        assert load_retention_config(tmp_path / "nope.yaml") is None

    def test_load_full_config(self, tmp_path):
        rcfg = self._load(tmp_path, """
            schedule: "0 4 * * *"
            start_date: "2026-01-01"
            max_active_runs: 2
            max_active_tasks: 3
            tags: [pg2ch, retention]
            defaults:
              batch_size: 20000
              lock_timeout_ms: 3000
            tables:
              orders:
                retention: 180d
              events:
                retention: "2026-01-01T00:00:00"
                batch_size: 500
        """)
        assert rcfg.schedule == "0 4 * * *"
        assert rcfg.max_active_runs == 2
        assert rcfg.max_active_tasks == 3
        assert len(rcfg.policies) == 2
        events = rcfg.policy_for("events")
        assert events.retention == "2026-01-01T00:00:00"
        assert events.batch_size == 500  # table override wins
        assert events.lock_timeout_ms == 3000  # from defaults
        orders = rcfg.policy_for("orders")
        assert orders.retention == "180d"
        assert orders.batch_size == 20000

    def test_policy_defaults_without_defaults_block(self, tmp_path):
        rcfg = self._load(tmp_path, """
            tables:
              orders:
                retention: 180d
        """)
        policy = rcfg.policy_for("orders")
        assert policy.batch_size == 10_000
        assert policy.lock_timeout_ms == 5_000
        assert rcfg.policy_for("unknown") is None

    def test_disabled_table_dropped(self, tmp_path):
        rcfg = self._load(tmp_path, """
            tables:
              orders:
                retention: 180d
                enabled: false
        """)
        assert rcfg.policies == []

    def test_empty_tables_ok(self, tmp_path):
        rcfg = self._load(tmp_path, "schedule: \"@daily\"\n")
        assert rcfg.policies == []

    def test_retention_value_required(self, tmp_path):
        with pytest.raises(ValueError, match="retention is required"):
            self._load(tmp_path, """
                tables:
                  orders:
                    batch_size: 100
            """)

    def test_bad_retention_expr(self, tmp_path):
        with pytest.raises(ValueError, match="relative like '180d'"):
            self._load(tmp_path, """
                tables:
                  orders:
                    retention: soon
            """)

    def test_retention_column_with_type_ok(self, tmp_path):
        rcfg = self._load(tmp_path, """
            tables:
              events:
                retention: 90d
                column: created_at
                type: timestamp
        """)
        policy = rcfg.policy_for("events")
        assert policy.column == "created_at"
        assert policy.type == "timestamp"

    def test_retention_serial_number_ok(self, tmp_path):
        rcfg = self._load(tmp_path, """
            tables:
              events:
                retention: 100000
                column: id
                type: serial
        """)
        assert rcfg.policy_for("events").retention == 100000

    def test_retention_column_requires_type(self, tmp_path):
        with pytest.raises(ValueError, match="column and type must be set together"):
            self._load(tmp_path, """
                tables:
                  events:
                    retention: 90d
                    column: created_at
            """)

    def test_retention_type_requires_column(self, tmp_path):
        with pytest.raises(ValueError, match="column and type must be set together"):
            self._load(tmp_path, """
                tables:
                  events:
                    retention: 90d
                    type: timestamp
            """)

    def test_retention_unknown_type(self, tmp_path):
        with pytest.raises(ValueError, match="type must be one of"):
            self._load(tmp_path, """
                tables:
                  events:
                    retention: 90d
                    column: created_at
                    type: uuid
            """)

    def test_retention_value_must_match_declared_type(self, tmp_path):
        with pytest.raises(ValueError, match="not a number"):
            self._load(tmp_path, """
                tables:
                  events:
                    retention: 90d
                    column: id
                    type: serial
            """)

    def test_bad_batch_size(self, tmp_path):
        with pytest.raises(ValueError, match="batch_size must be a positive"):
            self._load(tmp_path, """
                tables:
                  orders:
                    retention: 180d
                    batch_size: 0
            """)

    def test_unknown_top_level_key(self, tmp_path):
        with pytest.raises(ValueError, match="unknown retention key"):
            self._load(tmp_path, "sched: nope\ntables: {}\n")

    def test_unknown_table_key(self, tmp_path):
        with pytest.raises(ValueError, match="unknown key"):
            self._load(tmp_path, """
                tables:
                  orders:
                    retention: 180d
                    typo_key: 1
            """)

    def test_unknown_defaults_key(self, tmp_path):
        with pytest.raises(ValueError, match="unknown retention defaults key"):
            self._load(tmp_path, """
                defaults:
                  retention: 180d
                tables: {}
            """)

    def test_tables_must_be_mapping(self, tmp_path):
        with pytest.raises(ValueError, match="tables must be a mapping"):
            self._load(tmp_path, """
                tables:
                  - orders
            """)
