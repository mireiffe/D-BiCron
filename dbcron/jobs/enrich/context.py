"""RowContext 및 멀티소스 쿼리 체인.

소스 행 + 체인드 쿼리 결과 + S3 로드 객체를
단일 컨텍스트로 통합하여 API 요청 바디 구성에 사용합니다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class RowContext:
    """행 단위 파이프라인 컨텍스트.

    Attributes:
        source: 원본 소스 행 컬럼 (dict)
        queries: name → 단건 dict (many=false) 또는 다건 list[dict] (many=true)
        collections: name → 리스트 (S3 로더에서 채움)
    """

    source: dict[str, Any] = field(default_factory=dict)
    queries: dict[str, dict | list[dict]] = field(default_factory=dict)
    collections: dict[str, list] = field(default_factory=dict)

    def flat(self) -> dict[str, Any]:
        """모든 스칼라값을 단일 dict로 병합.

        source 먼저, 각 query 결과(many=false인 dict만) 순서대로 덮어쓰기.
        many=true인 쿼리 결과는 제외한다.
        """
        result = dict(self.source)
        for _name, qr in self.queries.items():
            if isinstance(qr, dict):
                result.update(qr)
        return result

    def get(self, key: str) -> Any:
        """키로 값을 조회한다.

        - "column" → flat()에서 검색
        - "query_name.column" → queries[query_name][column]
        - "$collections.name" → collections[name]
        """
        if key.startswith("$collections."):
            coll_name = key[len("$collections."):]
            return self.collections.get(coll_name)

        if "." in key:
            parts = key.split(".", 1)
            qr = self.queries.get(parts[0])
            if isinstance(qr, dict):
                return qr.get(parts[1])
            return None

        return self.flat().get(key)


def run_query_chain(
    source_row: dict,
    query_chain: list[dict],
    engines: dict[str, Any],
) -> RowContext:
    """쿼리 체인을 순차 실행하여 RowContext를 구축한다.

    Args:
        source_row: 원본 소스 행
        query_chain: source_queries 설정 리스트.
            각 항목: {name, db, sql, bind_from, many?}
        engines: db_id → SQLAlchemy Engine 매핑

    Returns:
        소스 행 + 쿼리 결과가 누적된 RowContext
    """
    ctx = RowContext(source=dict(source_row))

    for qcfg in query_chain:
        name = qcfg["name"]
        db_id = qcfg["db"]
        sql = qcfg["sql"]
        bind_from = qcfg.get("bind_from", "source")
        many = qcfg.get("many", False)

        engine = engines.get(db_id)
        if engine is None:
            logger.warning("Query chain: engine not found for db '%s'", db_id)
            ctx.queries[name] = [] if many else {}
            continue

        # 바인드 파라미터 결정
        if bind_from == "source":
            bind_params = dict(source_row)
        else:
            qr = ctx.queries.get(bind_from)
            if isinstance(qr, dict):
                bind_params = dict(qr)
            elif isinstance(qr, list) and qr and isinstance(qr[0], dict):
                # many=true 결과에서는 첫 행을 바인딩으로 사용
                bind_params = dict(qr[0])
            else:
                bind_params = {}

        # 쿼리 결과를 flat context에서도 보완 (상위 소스 + 이전 쿼리 병합)
        merged_params = ctx.flat()
        merged_params.update(bind_params)

        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql), merged_params)
                col_keys = list(result.keys())

                if many:
                    fetched = result.fetchall()
                    ctx.queries[name] = [
                        dict(zip(col_keys, r)) for r in fetched
                    ]
                else:
                    row = result.fetchone()
                    ctx.queries[name] = (
                        dict(zip(col_keys, row)) if row else {}
                    )
        except Exception:
            logger.exception("Query chain '%s' failed", name)
            ctx.queries[name] = [] if many else {}

    return ctx
