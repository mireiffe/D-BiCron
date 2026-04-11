"""FK integrity scanner: detect orphan rows violating FK relationships.

메타데이터의 FK 관계를 기반으로 실제 데이터에서 고아 행을 탐지합니다.
DB에서 FK 제약이 없지만 논리적 참조가 있는 경우 유용합니다.
"""

from __future__ import annotations

import json

from sqlalchemy import text

from ..db import DATA_DIR, URL_BUILDERS, create_engine_for, load_databases, should_include_table
from .base import Job, JobResult


class FKIntegrityJob(Job):
    name = "fk_integrity"
    label = "FK 무결성 스캔"
    description = "FK 관계에서 참조 무결성 위반(고아 행) 탐지"
    default_args: dict = {}

    def run(self, **kwargs) -> JobResult:
        snapshot_path = DATA_DIR / "metadata_snapshot.json"
        if not snapshot_path.exists():
            return JobResult(success=False, message="스냅샷이 없습니다. metadata_snapshot을 먼저 실행하세요.")

        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        db_map = {d["id"]: d for d in load_databases()}

        violations = []
        checked = 0

        for db_id, db_info in snapshot.get("databases", {}).items():
            db_cfg = db_map.get(db_id)
            if not db_cfg:
                continue
            db_type = db_cfg.get("type", "postgresql")
            if db_type not in URL_BUILDERS:
                continue

            engine = create_engine_for(db_cfg)
            try:
                with engine.connect() as conn:
                    for tbl_key, tbl in db_info.get("tables", {}).items():
                        if not should_include_table(tbl["table"], db_cfg):
                            continue
                        for fk in tbl.get("foreign_keys", []):
                            ref_parts = fk["ref_table"].split(".")
                            if len(ref_parts) != 2:
                                continue
                            ref_schema, ref_table = ref_parts
                            src_col = fk["columns"][0] if fk["columns"] else None
                            ref_col = fk["ref_columns"][0] if fk["ref_columns"] else None
                            if not src_col or not ref_col:
                                continue

                            checked += 1
                            try:
                                if db_type == "sqlite":
                                    q = f'SELECT COUNT(*) FROM "{tbl["table"]}" c LEFT JOIN "{ref_table}" p ON c."{src_col}" = p."{ref_col}" WHERE c."{src_col}" IS NOT NULL AND p."{ref_col}" IS NULL'
                                else:
                                    q = f'SELECT COUNT(*) FROM "{tbl["schema"]}"."{tbl["table"]}" c LEFT JOIN "{ref_schema}"."{ref_table}" p ON c."{src_col}" = p."{ref_col}" WHERE c."{src_col}" IS NOT NULL AND p."{ref_col}" IS NULL'

                                orphans = conn.execute(text(q)).scalar() or 0
                                if orphans > 0:
                                    violations.append(f"  {db_id}/{tbl_key}.{src_col} → {fk['ref_table']}.{ref_col}: {orphans} orphans")
                            except Exception as exc:
                                self.logger.debug("FK check failed %s: %s", tbl_key, exc)
            except Exception as exc:
                self.logger.warning("FK scan failed for %s: %s", db_id, exc)
            finally:
                engine.dispose()

        if violations:
            report = f"FK violations ({len(violations)}/{checked} checks):\n" + "\n".join(violations)
            print(report)
            return JobResult(success=False, message=f"{len(violations)} FK violations in {checked} checks", rows_affected=len(violations))

        return JobResult(success=True, message=f"All {checked} FK checks passed", rows_affected=0)
