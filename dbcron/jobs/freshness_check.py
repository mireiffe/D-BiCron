"""Data freshness checker: detect stale tables by row count change.

최근 두 스냅샷의 row count를 비교하여 변화가 없는 테이블을 감지합니다.
데이터 적재가 중단되었을 때 빠르게 인지할 수 있습니다.
"""

from __future__ import annotations

import json

from ..db import DATA_DIR
from .base import Job, JobResult


class FreshnessCheckJob(Job):
    name = "freshness_check"
    label = "데이터 신선도 체크"
    description = "이전 스냅샷 대비 row count 변화가 없는 테이블 감지"
    default_args: dict = {}

    def run(self, **kwargs) -> JobResult:
        cur_path = DATA_DIR / "metadata_snapshot.json"
        prev_path = DATA_DIR / "metadata_snapshot_prev.json"

        if not cur_path.exists() or not prev_path.exists():
            return JobResult(success=True, message="비교할 스냅샷이 부족합니다.")

        cur = json.loads(cur_path.read_text(encoding="utf-8"))
        prev = json.loads(prev_path.read_text(encoding="utf-8"))

        stale = []
        total_checked = 0

        for db_id, db_cur in cur.get("databases", {}).items():
            db_prev = prev.get("databases", {}).get(db_id)
            if not db_prev:
                continue

            for tkey, t_cur in db_cur.get("tables", {}).items():
                t_prev = db_prev.get("tables", {}).get(tkey)
                if not t_prev:
                    continue
                total_checked += 1
                cur_rows = t_cur.get("estimated_row_count", 0)
                prev_rows = t_prev.get("estimated_row_count", 0)

                if cur_rows == prev_rows and cur_rows > 0:
                    stale.append(f"  {db_id}/{tkey}: {cur_rows} rows (unchanged)")

        if stale:
            report = f"Stale tables ({len(stale)}/{total_checked}):\n" + "\n".join(stale)
            print(report)
            return JobResult(
                success=True,
                message=f"{len(stale)}/{total_checked} tables unchanged",
                rows_affected=len(stale),
            )

        return JobResult(success=True, message=f"All {total_checked} tables show changes", rows_affected=0)
