"""처리된 키 제외 (Processed-Key Exclusion).

타겟 테이블에 이미 존재하는 키를 조회하여
소스 쿼리 결과에서 제외하는 재개(resume) 방식.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)


def get_existing_keys(
    engine,
    db_type: str,
    table: str,
    key_column: str,
    candidate_keys: list | None = None,
) -> set:
    """타겟 테이블에서 기존 키 집합을 조회한다.

    Args:
        engine: SQLAlchemy 엔진
        db_type: DB 타입 ("clickhouse", "postgresql", ...)
        table: 타겟 테이블 이름
        key_column: 키 컬럼 이름
        candidate_keys: 주어지면 IN절로 범위 한정 (성능 최적화)

    Returns:
        타겟에 이미 존재하는 키 값의 set
    """
    final = " FINAL" if db_type == "clickhouse" else ""

    if candidate_keys:
        placeholders = ", ".join(f":k{i}" for i in range(len(candidate_keys)))
        sql = (
            f'SELECT DISTINCT "{key_column}" FROM {table}{final}'
            f' WHERE "{key_column}" IN ({placeholders})'
        )
        params = {f"k{i}": v for i, v in enumerate(candidate_keys)}
    else:
        sql = f'SELECT DISTINCT "{key_column}" FROM {table}{final}'
        params = {}

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    result = {r[0] for r in rows}
    logger.debug(
        "Exclusion: %d existing keys from %s.%s", len(result), table, key_column
    )
    return result


def filter_excluded_rows(
    rows: list[dict],
    existing_keys: set,
    source_key_column: str,
) -> list[dict]:
    """existing_keys에 있는 행을 제거한다.

    Args:
        rows: 소스 행 리스트
        existing_keys: 타겟에 이미 존재하는 키 set
        source_key_column: 소스 행에서 키로 사용할 컬럼 이름

    Returns:
        제외되지 않은 행 리스트
    """
    if not existing_keys:
        return rows

    filtered = [r for r in rows if r.get(source_key_column) not in existing_keys]
    excluded = len(rows) - len(filtered)
    if excluded:
        logger.info("Exclusion: filtered out %d already-processed rows", excluded)
    return filtered
