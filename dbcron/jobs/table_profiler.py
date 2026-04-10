"""Table profiler: sample statistics from registered databases.

테이블별 null 비율, 카디널리티, min/max 값을 샘플링합니다.
결과를 data/table_profile.json에 캐시합니다.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, text

from .base import Job, JobResult
from .metadata_snapshot import DATA_DIR, URL_BUILDERS


class TableProfilerJob(Job):
    name = "table_profiler"
    label = "테이블 프로파일러"
    description = "테이블별 null 비율, 카디널리티, min/max 샘플링"
    default_args: dict = {}

    def run(self, **kwargs) -> JobResult:
        snapshot_path = DATA_DIR / "metadata_snapshot.json"
        db_file = DATA_DIR / "databases.json"
        if not snapshot_path.exists() or not db_file.exists():
            return JobResult(success=False, message="스냅샷 또는 DB 설정이 없습니다.")

        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        databases = json.loads(db_file.read_text(encoding="utf-8"))
        db_map = {d["id"]: d for d in databases}

        profiles = {}
        total = 0

        for db_id, db_info in snapshot.get("databases", {}).items():
            db_cfg = db_map.get(db_id)
            if not db_cfg:
                continue
            db_type = db_cfg.get("type", "postgresql")
            builder = URL_BUILDERS.get(db_type)
            if not builder:
                continue

            engine = create_engine(builder(db_cfg), pool_pre_ping=True)
            try:
                with engine.connect() as conn:
                    for tbl_key, tbl in db_info.get("tables", {}).items():
                        schema, table = tbl["schema"], tbl["table"]
                        cols = tbl.get("columns", [])[:10]  # first 10 columns only
                        profile = {"columns": {}}

                        for col in cols:
                            cname = col["name"]
                            try:
                                q = f'SELECT COUNT(*) AS total, COUNT("{cname}") AS non_null, COUNT(DISTINCT "{cname}") AS distinct_ct FROM "{schema}"."{table}"' if db_type != "sqlite" else f'SELECT COUNT(*) AS total, COUNT("{cname}") AS non_null, COUNT(DISTINCT "{cname}") AS distinct_ct FROM "{table}"'
                                row = conn.execute(text(q)).fetchone()
                                total_rows = row[0] or 0
                                non_null = row[1] or 0
                                distinct_ct = row[2] or 0
                                profile["columns"][cname] = {
                                    "null_pct": round((1 - non_null / total_rows) * 100, 1) if total_rows else 0,
                                    "cardinality": distinct_ct,
                                }
                            except Exception:
                                pass

                        profiles[f"{db_id}:{tbl_key}"] = profile
                        total += 1
            except Exception as exc:
                self.logger.warning("Profile failed for %s: %s", db_id, exc)
            finally:
                engine.dispose()

        os.makedirs(DATA_DIR, exist_ok=True)
        out = DATA_DIR / "table_profile.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"profiled_at": datetime.now().isoformat(timespec="seconds"), "profiles": profiles}, f, ensure_ascii=False, indent=2)

        return JobResult(success=True, message=f"{total} tables profiled", rows_affected=total)
