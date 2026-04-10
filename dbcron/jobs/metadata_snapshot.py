"""Metadata snapshot: collect DB schema information and cache to JSON.

메타데이터 스냅샷 Job.
SQLAlchemy inspect() 를 사용하여 DB 종류에 관계없이 통일된 방식으로
테이블/컬럼/인덱스/FK/PK 메타데이터를 수집합니다.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, inspect, text

from .base import Job, JobResult

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# ── DB 타입별 설정 ──────────────────────────────────────────────

URL_BUILDERS: dict[str, callable] = {
    "postgresql": lambda c: f"postgresql+psycopg2://{c.get('user','')}:{c.get('password','')}@{c['host']}:{c.get('port',5432)}/{c['dbname']}",
    "mssql": lambda c: f"mssql+pymssql://{c.get('user','')}:{c.get('password','')}@{c['host']}:{c.get('port',1433)}/{c['dbname']}",
    "sqlite": lambda c: f"sqlite:///{c['host']}",
    "clickhouse": lambda c: f"clickhouse+http://{c.get('user','')}:{c.get('password','')}@{c['host']}:{c.get('port',8123)}/{c['dbname']}",
}

SYSTEM_SCHEMAS: dict[str, set[str]] = {
    "postgresql": {"pg_catalog", "information_schema"},
    "mssql": {"sys", "INFORMATION_SCHEMA", "guest", "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader", "db_datawriter", "db_denydatareader", "db_denydatawriter"},
    "sqlite": set(),
    "clickhouse": {"system", "INFORMATION_SCHEMA", "information_schema"},
}

# row count 추정 쿼리 (inspect 에 없는 유일한 항목)
ROW_COUNT_SQL: dict[str, str] = {
    "postgresql": "SELECT COALESCE(n_live_tup,0) FROM pg_stat_user_tables WHERE schemaname=:s AND relname=:t",
    "mssql": "SELECT SUM(p.rows) FROM sys.partitions p JOIN sys.tables t ON p.object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name=:s AND t.name=:t AND p.index_id<2",
    "clickhouse": "SELECT total_rows FROM system.tables WHERE database=:s AND name=:t",
}

SUPPORTED_TYPES = list(URL_BUILDERS.keys())


def _estimate_row_count(conn, db_type: str, schema: str, table: str) -> int:
    sql = ROW_COUNT_SQL.get(db_type)
    if sql:
        try:
            row = conn.execute(text(sql), {"s": schema, "t": table}).fetchone()
            return int(row[0]) if row and row[0] else 0
        except Exception:
            return 0
    # sqlite 및 기타: 정확한 count (보통 소규모)
    try:
        return int(conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0)
    except Exception:
        return 0


def collect_db_metadata(engine, db_type: str) -> dict[str, dict]:
    """SQLAlchemy inspect() 로 메타데이터 수집. DB 종류 무관."""
    insp = inspect(engine)
    skip = SYSTEM_SCHEMAS.get(db_type, set())
    tables: dict[str, dict] = {}

    schemas = [s for s in insp.get_schema_names() if s not in skip]

    with engine.connect() as conn:
        for schema in schemas:
            for tbl_name in insp.get_table_names(schema=schema):
                key = f"{schema}.{tbl_name}"

                # columns
                raw_cols = insp.get_columns(tbl_name, schema=schema)
                columns = [
                    {
                        "name": c["name"],
                        "type": str(c["type"]),
                        "nullable": c.get("nullable", True),
                        "ordinal": i + 1,
                    }
                    for i, c in enumerate(raw_cols)
                ]

                # primary key
                pk_info = insp.get_pk_constraint(tbl_name, schema=schema)
                primary_key = None
                if pk_info and pk_info.get("constrained_columns"):
                    primary_key = {
                        "name": pk_info.get("name") or f"{tbl_name}_pkey",
                        "columns": pk_info["constrained_columns"],
                    }

                # indexes
                indexes = [
                    {
                        "name": ix.get("name") or f"{tbl_name}_idx",
                        "columns": ix.get("column_names", []),
                        "unique": bool(ix.get("unique")),
                    }
                    for ix in insp.get_indexes(tbl_name, schema=schema)
                ]

                # foreign keys (outgoing)
                foreign_keys = []
                for fk in insp.get_foreign_keys(tbl_name, schema=schema):
                    ref_schema = fk.get("referred_schema") or schema
                    foreign_keys.append({
                        "name": fk.get("name") or f"{tbl_name}_fk",
                        "columns": fk["constrained_columns"],
                        "ref_table": f"{ref_schema}.{fk['referred_table']}",
                        "ref_columns": fk["referred_columns"],
                    })

                # row count
                row_count = _estimate_row_count(conn, db_type, schema, tbl_name)

                tables[key] = {
                    "schema": schema,
                    "table": tbl_name,
                    "estimated_row_count": row_count,
                    "columns": columns,
                    "primary_key": primary_key,
                    "indexes": indexes,
                    "foreign_keys": foreign_keys,
                    "referenced_by": [],  # 아래에서 역방향 계산
                }

    # referenced_by: 같은 DB 내 FK 데이터로 역방향 계산
    for key, info in tables.items():
        for fk in info["foreign_keys"]:
            ref_key = fk["ref_table"]
            if ref_key in tables:
                tables[ref_key]["referenced_by"].append({
                    "name": fk["name"],
                    "table": key,
                    "columns": fk["columns"],
                    "ref_columns": fk["ref_columns"],
                })

    return tables


class MetadataSnapshotJob(Job):
    name = "metadata_snapshot"
    label = "DB 메타데이터 스냅샷"
    description = "등록된 DB들의 테이블/컬럼/인덱스/FK/PK 메타데이터를 수집하여 캐시"
    default_args: dict = {}

    def run(self, **kwargs) -> JobResult:
        databases = self._load_databases()
        if not databases:
            return JobResult(
                success=False,
                message="등록된 DB가 없습니다. WebUI 또는 data/databases.json 으로 DB를 등록하세요.",
            )

        snapshot = {
            "snapshot_at": datetime.now().isoformat(timespec="seconds"),
            "databases": {},
        }
        total_tables = 0
        errors: list[str] = []

        for db_cfg in databases:
            db_id = db_cfg["id"]
            db_type = db_cfg.get("type", "postgresql")

            builder = URL_BUILDERS.get(db_type)
            if builder is None:
                errors.append(f"{db_id}: unsupported type '{db_type}'")
                continue

            engine = create_engine(builder(db_cfg), pool_pre_ping=True)
            try:
                tables = collect_db_metadata(engine, db_type)
                snapshot["databases"][db_id] = {
                    "host": db_cfg.get("host", ""),
                    "database": db_cfg.get("dbname", ""),
                    "tables": tables,
                }
                total_tables += len(tables)
                self.logger.info("%s (%s): %d tables", db_id, db_type, len(tables))
            except Exception as exc:
                self.logger.exception("Failed to collect %s", db_id)
                errors.append(f"{db_id}: {exc}")
            finally:
                engine.dispose()

        os.makedirs(DATA_DIR, exist_ok=True)
        out_path = DATA_DIR / "metadata_snapshot.json"
        prev_path = DATA_DIR / "metadata_snapshot_prev.json"

        # 이전 스냅샷 보존 (drift 비교용)
        if out_path.exists():
            try:
                out_path.rename(prev_path)
            except OSError:
                pass

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)

        # Append freshness log (row count tracking over time)
        freshness_path = DATA_DIR / "freshness_log.jsonl"
        ts = snapshot["snapshot_at"]
        with open(freshness_path, "a", encoding="utf-8") as f:
            for db_id, db_info in snapshot["databases"].items():
                for tbl_key, tbl in db_info.get("tables", {}).items():
                    entry = {"ts": ts, "db": db_id, "table": tbl_key, "rows": tbl["estimated_row_count"]}
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        if errors:
            msg = f"{total_tables}개 테이블 수집, {len(errors)}개 오류: {'; '.join(errors)}"
            return JobResult(success=False, message=msg, rows_affected=total_tables)

        return JobResult(
            success=True,
            message=f"{len(databases)}개 DB, {total_tables}개 테이블 메타데이터 수집 완료",
            rows_affected=total_tables,
        )

    @staticmethod
    def _load_databases() -> list[dict]:
        db_file = DATA_DIR / "databases.json"
        if not db_file.exists():
            return []
        with open(db_file, encoding="utf-8") as f:
            return json.load(f)
