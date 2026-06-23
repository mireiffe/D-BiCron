# pg2ch

PostgreSQL → ClickHouse 복사 전용 파이프라인 (Apache Airflow 3.2.2 기반).
상세 내용은 `README.md` 참조.

- 접속 정보: `config/connections.json` (ID 로 키잉, `${ENV}` 치환 지원)
- 테이블 파이프라인: `config/tables/<table_id>.yaml` (테이블당 1개, `_defaults.yaml` 병합)
- 추적: PostgreSQL `pg2ch_meta` 스키마 (`copy_run` / `copy_batch` / `copy_failed_row`)

## 적재 모드 (테이블별 `sync_mode`)

- `append` — watermark 이후 증분. 첫 실행은 전체 복사 후 watermark 설정.
- `full_reload` — 매 실행 TRUNCATE 후 전체 재적재.

## Quick ref

- `uv sync --extra test` — 엔진 의존성 + 테스트 설치
- `uv run pytest -q` — 테스트 (DB 불필요, 전부 mock)
- `uv run pg2ch <init-meta|list|copy|status> ...` — CLI one-shot
- `docker compose build && docker compose up -d` — Airflow 스택 기동 (UI :8080)

## 새 테이블 추가

1. (필요 시) `config/connections.json` 에 source/target 접속 추가
2. `config/tables/<table_id>.yaml` 작성 — DAG `pg2ch_<table_id>` 자동 등록
3. 적재 로직/설정 키 변경 시 `tests/` 에 케이스 추가 (기능엔 항상 테스트 동반)

## 엔진 구조 (pg2ch/)

`chtypes`(타입) → `ddl`(DDL) / `transform`(row 변환) → `copier`(오케스트레이션).
`tracking`(메타 저장소), `config`(YAML 로드/검증), `connections`(접속), `cli`.
엔진은 Airflow 에 의존하지 않으며, `dags/pg2ch_factory.py` 만 Airflow API 를 쓴다.

## 복사/추적 설계 원칙

- 항상 Python 스트리밍으로 복사한다 (batch/row 단위 추적·격리를 위해).
- batch INSERT 실패 시 binary-split 로 나쁜 row 만 격리 → `copy_failed_row` dead-letter.
- 멀티 row batch 전체 실패 = 인프라/스키마 문제로 보고 fail-fast.
- 증분 재개 cutoff 는 `copy_run` 의 마지막 성공 `watermark_after` 에서 읽는다.
