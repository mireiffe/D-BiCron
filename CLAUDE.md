# pg2ch

PostgreSQL → ClickHouse 복사 전용 파이프라인 (Apache Airflow 3.2.2 기반).
상세 내용은 `README.md` 참조.

- 접속 정보: `config/connections.json` (ID 로 키잉, `${ENV}` 치환 지원)
- 테이블 파이프라인: `config/tables/<table_id>.yaml` (테이블당 1개, `_defaults.yaml` 병합)
- PG source retention: `config/retention.yaml` (단일 파일 — 전용 DAG `pg2ch_retention`,
  copy 와 별개 스케줄이라 삭제가 오래 걸려도 copy 가 안 밀린다)
- 추적: PostgreSQL `pg2ch_meta` 스키마 (`copy_run` / `copy_batch` / `copy_failed_row`)

## 적재 모드 (테이블별 `sync_mode`)

- `append` — watermark 이후 증분. 첫 실행은 전체 복사 후 watermark 설정.
  `watermark_column` + `watermark_type`(serial|numeric|timestamp) 필수 —
  sync_since/overlap/retention 값이 전부 이 타입으로 파싱된다 (추측 없음).
- `full_reload` — 매 실행 TRUNCATE 후 전체 재적재.

## Quick ref

- `uv sync --extra test` — 엔진 의존성 + 테스트 설치
- `uv run pytest -q` — 테스트 (DB 불필요, 전부 mock)
- `uv run pg2ch <init-meta|list|copy|verify|retention|status> ...` — CLI one-shot
- `docker compose build && docker compose up -d` — Airflow 스택 기동 (UI :8080)

## 새 테이블 추가

1. (필요 시) `config/connections.json` 에 source/target 접속 추가
2. `config/tables/<table_id>.yaml` 작성 — DAG `pg2ch_<table_id>` 자동 등록
3. 적재 로직/설정 키 변경 시 `tests/` 에 케이스 추가 (기능엔 항상 테스트 동반)

## 엔진 구조 (pg2ch/)

`chtypes`(타입) → `ddl`(DDL) / `transform`(row 변환) → `copier`(오케스트레이션).
`tracking`(메타 저장소), `config`(YAML 로드/검증), `connections`(접속),
`integrity`(무결성 검사 + 자가복구), `retention`(PG source 삭제), `cli`.
엔진은 Airflow 에 의존하지 않으며, `dags/pg2ch_factory.py` 만 Airflow API 를 쓴다.
테이블 DAG task 순서: `precheck → copy → finalize_watermark → verify`.
retention 전용 DAG(`pg2ch_retention`)는 테이블마다 `verify_<id> → retention_<id>` 체인.

## 복사/추적 설계 원칙

- 항상 Python 스트리밍으로 복사한다 (batch/row 단위 추적·격리를 위해).
- batch INSERT 실패 시 binary-split 로 나쁜 row 만 격리 → `copy_failed_row` dead-letter.
- 멀티 row batch 전체 실패 = 인프라/스키마 문제로 보고 fail-fast.
- 증분 재개 cutoff 는 finalize 된 `copy_run.watermark_after` 와, finalize 전에
  죽은 증분 run 이 남긴 마지막 `copy_batch.watermark_hi` 중 더 진행된 지점에서
  읽는다 (OOM/SIGKILL 로 죽어도 재복사 루프에 빠지지 않게).
- retention(=source 삭제)은 copy 와 분리된 전용 DAG 에서 `config/retention.yaml`
  정책대로 돈다. 삭제 기준 컬럼은 기본 watermark 컬럼이고 항목별 `column`/`type` 으로
  별도 지정 가능. 삭제는 그 컬럼 값을 오름차순 전진(keyset)하며 `(lo, hi]` 구간씩
  batch DELETE 한다 — lo 가 삭제 지점을 넘어 전진하므로 이미 지운 앞구간(dead tuple)을
  재스캔하지 않아 수억 행에서도 선형으로 끝난다. 삭제 기준 컬럼엔 source 인덱스가
  있어야 하며(없으면 batch 마다 정렬), 없으면 실행 시 경고를 남긴다. retention 값은 타입별 해석 — timestamp: "180d"|ISO(now 기준),
  serial/numeric: 숫자 N(마지막 synced 값 − N, keep-last-N). cutoff 는 finalize 된
  watermark 가 가리키는 마지막 synced 값(삭제 컬럼으로 환산)으로 캡핑한다 —
  copy 가 멈춘 동안 미복제 row 가 삭제되지 않게. 별도 컬럼은 watermark 와 함께
  증가하는 컬럼(삽입 시각 등)만 안전하다.
- retention 은 파괴적이므로 직전에 `verify` 로 최근 watermark 구간의
  source `count(*)` vs target `uniqExact(watermark)` 를 비교한다. target 은 distinct
  watermark 로 세야 overlap 재전송 중복(ReplacingMergeTree 머지 전)이 누락을 가리지
  않는다. 비교 식별자는 watermark 컬럼뿐이다 — order_by/primary_key 는 드라이버 타입
  표현 차이(Decimal scale, timestamp 정밀도 등)로 false mismatch 를 만들 수 있어 쓰지
  않는다. 검사 해상도는 watermark 값 단위 (serial/증가 id 처럼 unique 할 때 정밀).
  watermark 가 파티션 키가 아니면(예: serial id watermark + ts 파티션) CH 가 wm 범위
  질의에 파티션을 못 쳐내 모든 파티션을 열고, window 가 크면 `uniqExact` 가 수억
  distinct 를 RAM 에 올려 메모리 한계를 넘긴다. `integrity_partition_column`(=ts 파티션
  키) + `integrity_partition_period`("30d" 상대/ISO 절대)를 주면 양쪽 질의에
  `partition_column >= now-period` 를 **똑같이** 걸어(동일 조건이라 비교는 그대로 성립)
  CH 스캔을 최근 파티션으로 좁힌다. 대신 검사 범위가 그 기간으로 한정되므로 파티션
  컬럼은 watermark 와 함께 증가하는 append-only 성격일 때 안전하다(늦게 갱신된 오래된
  ts row 는 검사에서 빠질 수 있음). 둘 다 지정하거나 둘 다 비운다.
- 누락이 잡히면 그 watermark 값만 골라 재복사(self-heal)한다: source−target watermark
  값 diff → `copier.copy_missing_keys` (watermark 전진 안 함 → resume 무영향,
  ReplacingMergeTree 계열만, dead-letter 는 제외해 무한 재시도 방지). repair 후에도
  남으면 on_mismatch=fail 일 때 retention 을 막아 source 유실을 방지한다.
