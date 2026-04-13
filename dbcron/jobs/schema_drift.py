"""Schema drift detector: compare current vs previous metadata snapshot.

이전 스냅샷과 현재 스냅샷을 비교하여 스키마 변경사항을 리포트합니다.
metadata_snapshot 이후에 실행하면 변경 사항을 즉시 확인할 수 있습니다.
"""

from __future__ import annotations

import json

from ..db import DATA_DIR
from .base import Job, JobResult


class SchemaDriftJob(Job):
    name = "schema_drift"
    label = "스키마 변경 감지"
    description = "이전 스냅샷과 비교하여 테이블/컬럼/PK 변경사항 리포트"
    default_args: dict = {}
    scope = "all_tables"
    bundled = True

    def run(self, **kwargs) -> JobResult:
        cur_path = DATA_DIR / "metadata_snapshot.json"
        prev_path = DATA_DIR / "metadata_snapshot_prev.json"

        if not cur_path.exists():
            return JobResult(success=False, message="현재 스냅샷이 없습니다. metadata_snapshot을 먼저 실행하세요.")
        if not prev_path.exists():
            return JobResult(success=True, message="이전 스냅샷이 없어 비교할 수 없습니다 (첫 실행).")

        cur = json.loads(cur_path.read_text(encoding="utf-8"))
        prev = json.loads(prev_path.read_text(encoding="utf-8"))

        # targets로 DB/table 범위 필터
        databases, table_filter = self.resolve_databases()
        target_db_ids = {d["id"] for d in databases} if databases else None

        changes = []
        breaking = 0

        for db_id, db_cur in cur.get("databases", {}).items():
            if target_db_ids and db_id not in target_db_ids:
                continue
            db_prev = prev.get("databases", {}).get(db_id)
            if not db_prev:
                changes.append(f"[NEW DB] {db_id}")
                continue

            cur_tables = {k: v for k, v in db_cur.get("tables", {}).items()
                          if table_filter(db_id, v.get("table", k), {})}
            prev_tables_raw = db_prev.get("tables", {})
            prev_tables = {k: v for k, v in prev_tables_raw.items()
                           if table_filter(db_id, v.get("table", k), {})}

            for tkey in cur_tables:
                if tkey not in prev_tables:
                    changes.append(f"[+TABLE] {db_id}/{tkey}")

            for tkey in prev_tables:
                if tkey not in cur_tables:
                    changes.append(f"[-TABLE] {db_id}/{tkey}")
                    breaking += 1

            for tkey, t_cur in cur_tables.items():
                t_prev = prev_tables.get(tkey)
                if not t_prev:
                    continue
                cur_cols = {c["name"]: c for c in t_cur.get("columns", [])}
                prev_cols = {c["name"]: c for c in t_prev.get("columns", [])}

                for cname in cur_cols:
                    if cname not in prev_cols:
                        changes.append(f"[+COL] {db_id}/{tkey}.{cname}")
                    elif cur_cols[cname]["type"] != prev_cols[cname]["type"]:
                        changes.append(f"[TYPE] {db_id}/{tkey}.{cname}: {prev_cols[cname]['type']} → {cur_cols[cname]['type']}")
                        breaking += 1

                for cname in prev_cols:
                    if cname not in cur_cols:
                        changes.append(f"[-COL] {db_id}/{tkey}.{cname}")
                        breaking += 1

                if json.dumps(t_cur.get("primary_key")) != json.dumps(t_prev.get("primary_key")):
                    changes.append(f"[PK] {db_id}/{tkey}: primary key changed")
                    breaking += 1

        for db_id in prev.get("databases", {}):
            if db_id not in cur.get("databases", {}):
                changes.append(f"[-DB] {db_id}")
                breaking += 1

        if not changes:
            return JobResult(success=True, message="스키마 변경 없음", rows_affected=0)

        report = "\n".join(changes)
        print(report)
        msg = f"{len(changes)}개 변경, {breaking}개 breaking"
        return JobResult(
            success=breaking == 0,
            message=msg,
            rows_affected=len(changes),
        )
