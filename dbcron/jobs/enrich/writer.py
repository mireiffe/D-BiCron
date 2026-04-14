"""타겟 테이블 DDL, watermark 관리, 행 쓰기."""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import text

from .util import _TIMESTAMP_TYPES

logger = logging.getLogger(__name__)

_WATERMARK_TABLE = "_api_enrich_watermarks"


# ── target DDL ───────────────────────────────────────────────────


def ensure_target_table(
    engine,
    db_type: str,
    table: str,
    columns: dict[str, str],
    order_by: list[str] | None,
    engine_clause: str | None,
    partition_by: str | None,
) -> None:
    """타겟 테이블이 없으면 생성한다."""
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

    logger.info("Ensuring table: %s", table)
    logger.debug("DDL: %s", ddl)
    with engine.begin() as conn:
        conn.execute(text(ddl))


# ── watermark ────────────────────────────────────────────────────


def ensure_watermark_table(engine, db_type: str) -> None:
    """워터마크 추적 테이블이 없으면 생성한다."""
    tbl = _WATERMARK_TABLE
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


def get_watermark(
    engine, db_type: str, key: str, wm_col: str
) -> str | None:
    """저장된 워터마크 값을 조회한다."""
    tbl = _WATERMARK_TABLE
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


def save_watermark(
    engine, db_type: str, key: str, wm_col: str, value
) -> None:
    """워터마크 값을 저장한다."""
    tbl = _WATERMARK_TABLE
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


# ── 행 쓰기 ─────────────────────────────────────────────────────


def write_rows(
    engine,
    db_type: str,
    table: str,
    rows: list[dict],
    strategy: str = "insert",
    key_columns: list[str] | None = None,
) -> int:
    """전략에 따라 행을 타겟 테이블에 쓴다.

    strategy:
      - "insert": 단순 INSERT (기본, 기존 동작)
      - "upsert": ON CONFLICT DO UPDATE (Phase 2에서 확장)
      - "replace": DELETE + INSERT (Phase 2에서 확장)
    """
    if not rows:
        return 0

    ins_cols = list(rows[0].keys())
    col_list = ", ".join(f'"{c}"' for c in ins_cols)
    placeholders = ", ".join(f":{c}" for c in ins_cols)

    if strategy == "insert":
        insert_sql = text(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        )
        with engine.begin() as conn:
            conn.execute(insert_sql, rows)
        return len(rows)

    if strategy == "upsert":
        return _write_upsert(engine, db_type, table, rows, ins_cols, key_columns)

    if strategy == "replace":
        return _write_replace(engine, db_type, table, rows, ins_cols, key_columns)

    raise ValueError(f"Unknown write_strategy: {strategy!r}")


def _write_upsert(
    engine, db_type: str, table: str, rows: list[dict],
    ins_cols: list[str], key_columns: list[str] | None,
) -> int:
    """Upsert 전략으로 행 쓰기.

    DB별 구현:
      - PostgreSQL: INSERT ... ON CONFLICT (keys) DO UPDATE SET ...
      - ClickHouse: 그대로 INSERT (ReplacingMergeTree 자체 dedup)
      - MSSQL: 행 단위 MERGE INTO ... USING ... WHEN MATCHED/NOT MATCHED
      - SQLite: INSERT OR REPLACE INTO ...
    """
    if not key_columns:
        raise ValueError("upsert strategy requires upsert_key_columns")

    col_list = ", ".join(f'"{c}"' for c in ins_cols)
    placeholders = ", ".join(f":{c}" for c in ins_cols)
    update_cols = [c for c in ins_cols if c not in key_columns]

    if db_type == "clickhouse":
        # ReplacingMergeTree handles dedup at merge time
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    elif db_type == "postgresql":
        conflict = ", ".join(f'"{c}"' for c in key_columns)
        set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
            f" ON CONFLICT ({conflict}) DO UPDATE SET {set_clause}"
        )

    elif db_type == "mssql":
        # MSSQL MERGE requires row-by-row execution
        return _write_upsert_mssql(engine, table, rows, ins_cols, key_columns)

    elif db_type == "sqlite":
        sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"

    else:
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    with engine.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)


def _write_upsert_mssql(
    engine, table: str, rows: list[dict],
    ins_cols: list[str], key_columns: list[str],
) -> int:
    """MSSQL MERGE 문으로 upsert."""
    update_cols = [c for c in ins_cols if c not in key_columns]
    on_clause = " AND ".join(
        f'target."{c}" = source."{c}"' for c in key_columns
    )
    update_set = ", ".join(
        f'target."{c}" = source."{c}"' for c in update_cols
    )
    insert_cols = ", ".join(f'"{c}"' for c in ins_cols)
    insert_vals = ", ".join(f'source."{c}"' for c in ins_cols)
    source_cols = ", ".join(f':{c} AS "{c}"' for c in ins_cols)

    merge_sql = (
        f"MERGE INTO {table} AS target"
        f" USING (SELECT {source_cols}) AS source"
        f" ON {on_clause}"
        f" WHEN MATCHED THEN UPDATE SET {update_set}"
        f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )

    with engine.begin() as conn:
        for row in rows:
            conn.execute(text(merge_sql), row)
    return len(rows)


def _write_replace(
    engine, db_type: str, table: str, rows: list[dict],
    ins_cols: list[str], key_columns: list[str] | None,
) -> int:
    """Replace 전략으로 행 쓰기: DELETE matching keys + INSERT."""
    if not key_columns:
        raise ValueError("replace strategy requires upsert_key_columns")

    col_list = ", ".join(f'"{c}"' for c in ins_cols)
    placeholders = ", ".join(f":{c}" for c in ins_cols)

    with engine.begin() as conn:
        # DELETE existing rows by key
        key_values = [
            {f"k{i}": row[kc] for i, kc in enumerate(key_columns)}
            for row in rows
        ]
        where_clause = " AND ".join(
            f'"{kc}" = :k{i}' for i, kc in enumerate(key_columns)
        )
        delete_sql = text(f"DELETE FROM {table} WHERE {where_clause}")
        for kv in key_values:
            conn.execute(delete_sql, kv)

        # INSERT new rows
        insert_sql = text(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        )
        conn.execute(insert_sql, rows)

    return len(rows)


# ── 멀티타겟 쓰기 (부모-자식) ────────────────────────────────────


def derive_child_rows(
    parent_rows: list[dict],
    api_responses: list[dict | None],
    child_config: dict,
) -> list[dict]:
    """부모 행과 API 응답에서 자식 행을 생성한다.

    Args:
        parent_rows: 부모 행 리스트 (source 컬럼 포함)
        api_responses: 각 부모에 대응하는 API 응답
        child_config: 자식 타겟 설정

    Returns:
        자식 행 리스트
    """
    from .response_parser import extract_response_mapping
    from .util import _extract_json_path

    parent_key_col = child_config["parent_key_column"]
    fk_col = child_config["foreign_key_column"]
    array_path = child_config.get("source_array_path")
    child_mapping = child_config.get("response_mapping", [])

    child_rows: list[dict] = []
    for parent, resp in zip(parent_rows, api_responses):
        if resp is None:
            continue

        pk_val = parent.get(parent_key_col)

        if array_path:
            items = _extract_json_path(resp, array_path)
            if not isinstance(items, list):
                continue
        else:
            items = [resp]

        for item in items:
            if not isinstance(item, dict):
                item = {child_mapping[0]["json_path"]: item} if child_mapping else {}
            row = extract_response_mapping(item, child_mapping)
            row[fk_col] = pk_val
            child_rows.append(row)

    return child_rows


def write_parent_and_children(
    tgt_engine,
    db_type: str,
    parent_table: str,
    parent_rows: list[dict],
    parent_strategy: str,
    parent_key_columns: list[str] | None,
    child_configs: list[dict],
    api_responses: list[dict | None],
    source_rows: list[dict],
    timestamp_col: str | None,
) -> int:
    """부모 행을 쓴 뒤 자식 행을 순차 쓴다.

    Args:
        tgt_engine: 타겟 DB 엔진
        db_type: DB 타입
        parent_table: 부모 테이블 이름
        parent_rows: enriched 부모 행
        parent_strategy: 부모 write_strategy
        parent_key_columns: 부모 upsert key columns
        child_configs: 자식 타겟 설정 리스트
        api_responses: 각 부모에 대응하는 API 응답 원본
        source_rows: 원본 소스 행 (부모 PK 값 참조용)
        timestamp_col: 타임스탬프 컬럼 (자식에도 적용)

    Returns:
        총 쓰기 행 수 (부모 + 자식)
    """
    total = 0

    # 부모 쓰기
    if parent_rows:
        total += write_rows(
            tgt_engine, db_type, parent_table, parent_rows,
            strategy=parent_strategy,
            key_columns=parent_key_columns,
        )

    # 자식 쓰기
    for child_cfg in child_configs:
        child_table = child_cfg["target_table"]
        child_strategy = child_cfg.get("write_strategy", "insert")
        child_key_cols = child_cfg.get("upsert_key_columns")

        child_rows = derive_child_rows(source_rows, api_responses, child_cfg)

        if timestamp_col:
            from datetime import datetime as _dt
            now = _dt.now()
            for row in child_rows:
                row[timestamp_col] = now

        if child_rows:
            total += write_rows(
                tgt_engine, db_type, child_table, child_rows,
                strategy=child_strategy,
                key_columns=child_key_cols,
            )

    return total
