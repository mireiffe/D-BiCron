"""pg2ch — PostgreSQL → ClickHouse 복사 엔진.

Airflow 3.2.2 위에서 동작하는 테이블 단위 복사 파이프라인.

핵심 모듈:
  - chtypes   : PG → CH 타입 매핑 / CH 타입 문자열 유틸
  - ddl       : CH 컬럼 매핑 + CREATE TABLE DDL 생성
  - transform : PG row → CH INSERT 호환 row 변환
  - watermark : sync_since / watermark overlap 계산
  - connections: 접속 정보 레지스트리(JSON) + PG/CH/meta 커넥션 팩토리
  - config    : 테이블 파이프라인 설정(YAML) 로드 / 검증
  - tracking  : copy_run / copy_batch / copy_failed_row 메타 추적 저장소
  - copier    : append / full_reload 복사 오케스트레이션 (batch + row 단위 dead-letter)
  - retention : finalize 된 watermark 기준 PG source row 배치 삭제
  - cli       : CLI one-shot 실행기
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
