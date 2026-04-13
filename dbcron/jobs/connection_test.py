"""Connection smoke test: verify connectivity to all registered databases.

등록된 모든 DB에 SELECT 1 수준의 연결 테스트를 수행합니다.
결과를 data/connection_test.json 에 캐시하여 캔버스에서 상태 표시 가능.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from sqlalchemy import text

from ..db import DATA_DIR, URL_BUILDERS, create_engine_for
from .base import Job, JobResult


class ConnectionTestJob(Job):
    name = "connection_test"
    label = "DB 연결 테스트"
    description = "등록된 모든 DB의 연결/인증 상태를 점검"
    default_args: dict = {}
    scope = "all_dbs"
    bundled = True

    def run(self, **kwargs) -> JobResult:
        databases, _ = self.resolve_databases()
        if not databases:
            return JobResult(success=False, message="등록된 DB가 없습니다.")

        results = []
        ok_count = 0

        for db_cfg in databases:
            db_id = db_cfg["id"]
            db_type = db_cfg.get("type", "postgresql")
            builder = URL_BUILDERS.get(db_type)

            entry = {
                "id": db_id,
                "type": db_type,
                "host": db_cfg.get("host", ""),
                "status": "fail",
                "latency_ms": None,
                "error": None,
                "tested_at": datetime.now().isoformat(timespec="seconds"),
            }

            if builder is None:
                entry["error"] = f"Unsupported DB type: {db_type}"
                results.append(entry)
                continue

            engine = create_engine_for(db_cfg)
            try:
                t0 = time.monotonic()
                with engine.connect() as conn:
                    if db_type == "clickhouse":
                        conn.execute(text("SELECT 1"))
                    elif db_type == "sqlite":
                        conn.execute(text("SELECT 1"))
                    else:
                        conn.execute(text("SELECT 1"))
                latency = round((time.monotonic() - t0) * 1000, 1)
                entry["status"] = "ok"
                entry["latency_ms"] = latency
                ok_count += 1
                self.logger.info("%s: OK (%sms)", db_id, latency)
            except Exception as exc:
                entry["error"] = str(exc)[:200]
                self.logger.warning("%s: FAIL — %s", db_id, exc)
            finally:
                engine.dispose()

            results.append(entry)

        os.makedirs(DATA_DIR, exist_ok=True)
        out_path = DATA_DIR / "connection_test.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({
                "tested_at": datetime.now().isoformat(timespec="seconds"),
                "results": results,
            }, f, ensure_ascii=False, indent=2)

        total = len(databases)
        fail = total - ok_count
        if fail:
            return JobResult(
                success=False,
                message=f"{ok_count}/{total} OK, {fail} FAIL",
                rows_affected=ok_count,
            )
        return JobResult(
            success=True,
            message=f"전체 {total}개 DB 연결 정상",
            rows_affected=ok_count,
        )

