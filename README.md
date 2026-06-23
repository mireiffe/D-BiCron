# pg2ch

**PostgreSQL → ClickHouse 복사 전용 파이프라인** — Apache Airflow 3.2.2 위에서 동작.

여러 PG 테이블을 대응되는 CH 테이블로 주기적으로 복사한다. 테이블마다 적재 방식과
주기가 다를 수 있고(설정으로 지정), 어디까지 복사되었는지 / 어느 batch 의 어느 row 가
실패했는지를 정밀하게 추적한다.

> 이 저장소는 과거 cron 기반 다중 DB 관리 플랫폼(dbcron)을 완전히 대체한 것이다.
> 옛 프로젝트는 `backup_cron` 브랜치에 보존되어 있다 → 아래 [기존 프로젝트](#기존-프로젝트-backup_cron) 참조.

---

## 핵심 개념

### 적재 모드 (테이블별 `sync_mode`)

PG 에 데이터가 들어오는 방식이 테이블마다 달라서, 두 모드를 지원한다.

| 모드 | 동작 | 용도 |
|------|------|------|
| `append` | 마지막 watermark 이후의 새 row 만 증분 전송. 첫 실행은 전체를 한 번 복사한 뒤 watermark 를 세운다. | 주기적으로 row 가 쌓이는 테이블 |
| `full_reload` | 매 실행마다 target 을 `TRUNCATE` 하고 전체를 다시 적재. | 주기적으로 truncate 후 통째로 재적재되는 테이블 |

### 자동 테이블 생성 + 타입 제어

대상 CH 테이블이 없으면 설정대로 생성한다 (`CREATE TABLE IF NOT EXISTS`).
`engine` / `order_by` / `primary_key` / `partition_by` / `indexes` / `settings` 와
컬럼 단위 `column_overrides`(LowCardinality, Decimal, text→DateTime 파싱 등) /
`drop_columns` / `use_nullable` 를 모두 설정으로 제어한다.

### 추적 & 실패 모델 (이 프로젝트의 핵심)

전용 PostgreSQL 메타 스키마(`pg2ch_meta`)에 세 테이블로 기록한다.

```
copy_run         테이블 × 실행(=Airflow task 1회)
  ├─ status              running | success | partial | failed
  ├─ watermark_before    이 실행 시작 시점 cutoff      ← "어디서부터"
  ├─ watermark_after     이 실행으로 전진한 high-watermark ← "어디까지 복사됨"
  └─ rows_read / written / failed / batch_count / duration_ms

copy_batch       run 안의 batch 단위
  ├─ batch_seq, status(success|partial|failed)
  ├─ rows_in / written / failed
  └─ watermark_lo / hi   ← "어느 batch 가 어느 구간을 다뤘나"

copy_failed_row  dead-letter (실패 row 원본 보관)
  ├─ batch_seq, watermark_value
  ├─ row_data (JSONB, 원본 source row)  ← "어느 row 가, 무슨 값으로 실패했나"
  └─ error, resolved
```

**증분 재개(resume)** 는 `copy_run` 의 마지막 성공 watermark 에서 읽는다 — 별도 상태
저장소가 없다. 다음 실행 cutoff = `MAX(watermark_after) WHERE status IN ('success','partial')`.

**batch INSERT 실패 처리**: ClickHouse 는 block 단위로 INSERT 가 실패하므로,
실패한 batch 는 **binary-split** 으로 나쁜 row 를 격리한다. 정상 row 는 적재하고
끝까지 실패하는 단일 row 만 `copy_failed_row` 에 보관(`on_row_error: dead_letter`)한다.
멀티 row batch 가 통째로 실패하면 row 단위가 아닌 인프라/스키마 문제로 보고 즉시
run 을 실패시킨다(fail-fast). 실패가 있어도 watermark 는 전진하므로(실패 row 는
dead-letter 에 보존됨) 파이프라인이 멈추지 않는다 — 이때 run 상태는 `partial`.

> 직접 `ClickHouse postgresql()` 테이블 함수로 복사하지 않고 항상 Python 스트리밍으로
> 복사하는 이유가 바로 이 row 단위 추적/격리 때문이다.

---

## 설정 & 실행

큰 흐름은 **① 접속 정보 → ② 테이블 규칙 → ③ 실행** 세 단계다.
`*.example` 파일이 템플릿이므로 복사해서 `.example` 만 떼고 값을 채운다.

```
config/connections.json    ← PG/CH/meta 접속 정보 (ID 로 관리)
config/tables/*.yaml         ← 테이블당 1개, 복사 규칙 + DDL + 스케줄
        └── dags/pg2ch_factory.py 가 읽어 pg2ch_<table_id> DAG 자동 생성
            또는  uv run pg2ch copy <table_id> 로 직접 실행
추적 결과 → Postgres pg2ch_meta 스키마
```

### 1. 접속 정보 — `config/connections.json`

```bash
cp config/connections.example.json config/connections.json
```

ID 로 키잉된 JSON. 테이블 설정은 이 ID(`source`/`target`/`meta`)만 참조한다.

```json
{
  "my_postgres":   {"type": "postgresql", "host": "10.0.0.5", "port": 5432,
                    "dbname": "shop", "user": "app", "password": "${PG_PASSWORD}"},
  "my_clickhouse": {"type": "clickhouse", "host": "10.0.0.9", "port": 9000,
                    "dbname": "default", "user": "default", "password": "${CH_PASSWORD:-}"},
  "meta":          {"type": "postgresql", "host": "10.0.0.5", "port": 5432,
                    "dbname": "shop", "user": "app", "password": "${PG_PASSWORD}",
                    "schema": "pg2ch_meta"}
}
```

- **`source`** = 실제 PG (복사 대상 테이블이 있는 곳), **`target`** = 실제 CH.
  ClickHouse `port` 는 **native 9000** (HTTP 8123 아님).
- **`meta`** = 추적 기록을 저장할 Postgres. 따로 둘 필요 없이 **source PG 를 재사용**해도
  된다 (`pg2ch_meta` 스키마가 자동 생성됨).
- 비밀 값은 파일에 직접 적지 말고 `${PG_PASSWORD}` 처럼 환경변수로 빼는 것을 권장(`.env` 에 작성).
  `${VAR:-기본값}` 문법과 `"_enc": "b64"` (base64 password) 도 지원.

### 2. 테이블 규칙 — `config/tables/<table_id>.yaml` (테이블당 1개)

같은 디렉터리의 `_defaults.yaml`(있으면)이 모든 테이블에 병합된다(테이블별 값 우선,
`settings` 는 깊은 병합). 전체 예시는 `config/tables/*.example.yaml` 참조.

**A) 새 row 가 쌓이는 테이블 → `append`**

```yaml
table_id: orders                 # 고유 ID → DAG id(pg2ch_orders) + 추적 키
source: my_postgres              # connections.json 의 ID
target: my_clickhouse
meta: meta

sync_mode: append
schedule: "*/15 * * * *"         # cron (DAG 스케줄)
start_date: "2026-01-01"

source_table: public.orders
target_table: default.orders

# append 증분 기준
watermark_column: updated_at     # 이 컬럼 > 마지막 watermark 인 row 만 전송
timestamp_column: updated_at     # sync_since 하한 / 정렬 기준
sync_since: 90d                  # 30d/12h/90m 상대값 또는 ISO 절대값
overlap_minutes: 30              # timestamp watermark 를 N분 앞당겨 재전송 (dedup 됨)
# watermark_overlap: 1000        # 숫자형 watermark 일 때 N 만큼 앞당겨 재전송

# DDL (대상 테이블이 없으면 이대로 생성)
engine: ReplacingMergeTree(updated_at)
order_by: [id]
primary_key: [id]
partition_by: toYYYYMM(created_at)
indexes:
  - {name: idx_status, column: status, type: set(1000), granularity: 1}
settings: {index_granularity: 8192}

# 컬럼 처리
drop_columns: [internal_note]
column_overrides:
  status: LowCardinality(String)
  amount: Decimal(18,4)
  occurred_at_str:               # text → DateTime 파싱
    type: "DateTime64(3, 'UTC')"
    parse_format: "%Y%m%d %H%M%S"
    timezone: "Asia/Seoul"
use_nullable: false

# 배치 / 에러 처리
batch_size: 50000
on_row_error: dead_letter        # dead_letter | skip | fail
max_failed_rows: 1000            # 누적 실패가 넘으면 run 실패 처리

# 적재 직후 즉시 dedup
optimize_after_sync: true
optimize_partitions: ["202606"]  # 생략 시 전체 테이블
```

**B) 통째로 갈아엎는 테이블 → `full_reload`** (매 실행 TRUNCATE 후 전체 재적재)

```yaml
table_id: price_book
source: my_postgres
target: my_clickhouse
sync_mode: full_reload
schedule: "0 4 * * *"            # 매일 04:00
source_table: public.price_book
target_table: default.price_book
engine: MergeTree
order_by: [sku]
column_overrides: {currency: LowCardinality(String), price: "Decimal(18,4)"}
use_nullable: false
```

**주요 키 요약**

| 항목 | 키 |
|------|-----|
| 필수 | `table_id`, `source`, `target`, `source_table`, `target_table`, `sync_mode`, `order_by` |
| 위치 | `source_table`, `target_table` (`schema.table`) |
| DDL | `engine`, `order_by`, `primary_key`, `partition_by`, `indexes`, `settings` |
| 타입/컬럼 | `column_overrides`, `drop_columns`, `use_nullable` |
| 증분(append) | `watermark_column`, `timestamp_column`, `sync_since`, `overlap_minutes`, `watermark_overlap` |
| 에러 | `on_row_error`(dead_letter\|skip\|fail), `max_failed_rows` |
| 스케줄 | `schedule`, `start_date`, `catchup`, `max_active_runs`, `retries`, `retry_delay_seconds`, `tags` |
| 후처리 | `optimize_after_sync`, `optimize_partitions`, `optimize_mutations_sync` |

### 3-a. 실행 — Airflow (Docker, 운영 방식)

```bash
cp .env.example .env             # PG_PASSWORD 등 채우기, AIRFLOW_UID=$(id -u)

docker compose build
docker compose up -d
# Airflow UI: http://localhost:8080
```

테이블당 DAG `pg2ch_<table_id>` 가 자동 생성된다. UI 에서 토글을 켜면 `schedule` 대로 돈다.
`docker-compose.yaml` 은 Airflow 공식 3.x 템플릿을 LocalExecutor 로 단순화한 것이며,
로컬 테스트용 ClickHouse 서비스를 포함한다.

> **로그인**: Airflow 3.x 기본 인증은 SimpleAuthManager 이고 `airflow users create` 는
> FAB 인증매니저 전용이다. 개발 편의를 위해 compose 는 `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`
> 로 **인증을 우회**한다(로그인 없이 admin). 운영에서는 이 값을 끄고, 생성된 비밀번호로
> 로그인하거나 FAB 인증매니저를 쓴다.
> ```bash
> # SimpleAuthManager 가 생성한 admin 비밀번호 확인 (우회를 끈 경우)
> docker compose exec airflow-apiserver \
>   cat /opt/airflow/simple_auth_manager_passwords.json.generated
> ```

### 3-b. 실행 — CLI (Airflow 없이 one-shot / 디버그)

```bash
uv sync                          # 엔진 의존성 설치
uv run pg2ch init-meta           # 추적 스키마(pg2ch_meta) 생성
uv run pg2ch list                # 설정된 테이블 목록
uv run pg2ch copy orders         # 특정 테이블 복사 1회
uv run pg2ch copy all            # 전체 복사
uv run pg2ch status orders       # 마지막 run / watermark / 미해결 실패 row 수
```

경로는 `--connections` / `--tables-dir` 또는 `PG2CH_CONNECTIONS` / `PG2CH_TABLES_DIR`
환경변수로 지정.

---

## ⚠️ 흔한 함정

1. **`connections.json` 은 당신의 실제 PG/CH 를 가리켜야 한다.** compose 안의 `postgres`
   서비스는 *Airflow 자체 메타DB*(dbname `airflow`, user `airflow`)다 — 예시의
   `dbname: shop / user: app` 을 그대로 두면 접속 실패한다. 로컬 테스트만 하려면 compose 에
   포함된 ClickHouse(`host: clickhouse, port: 9000`)를 `target` 으로 쓰고, `source`/`meta`
   는 접근 가능한 실제 PG 로 맞춘다.
2. **Docker 컨테이너 안에서는 서비스 이름으로 통신**한다(`postgres`, `clickhouse`). 외부 DB
   라면 컨테이너에서 닿는 실제 호스트/IP 를 적는다.
3. ClickHouse 포트는 **native(9000)** — clickhouse-driver 는 HTTP(8123)가 아니라 native
   TCP 를 쓴다.
4. **로그 권한**: dag-processor 가 `*.log` 를 못 만든다는 에러는 bind-mount 된 `./logs` 를
   컨테이너 사용자가 못 쓰는 권한 문제다. compose 의 `airflow-init` 가 root 로 `chown` 해
   자동 해결하지만, 호스트에서 로그를 직접 열어 보려면 `.env` 의 `AIRFLOW_UID` 를
   `$(id -u)` 로 맞춘다.
5. **로그인**: 위 [실행](#3-a-실행--airflow-docker-운영-방식) 박스의 로그인 노트 참조.

---

## 추적 데이터 조회

```sql
-- 테이블별 진행 현황 (어디까지 복사됐나)
SELECT table_id, status, watermark_after, rows_written, rows_failed, finished_at
FROM pg2ch_meta.copy_run
WHERE run_id IN (SELECT max(run_id) FROM pg2ch_meta.copy_run GROUP BY table_id);

-- 실패한 batch 와 그 watermark 구간
SELECT run_id, batch_seq, rows_failed, watermark_lo, watermark_hi, error
FROM pg2ch_meta.copy_batch WHERE status <> 'success' ORDER BY run_id DESC;

-- 어떤 row 가 무슨 값으로 실패했나 (재처리 대상)
SELECT table_id, batch_seq, watermark_value, row_data, error
FROM pg2ch_meta.copy_failed_row WHERE NOT resolved ORDER BY failed_at DESC;
```

`row_data` 에 원본 row 가 JSONB 로 보관되므로 수정 후 재적재(replay)가 가능하다.
스키마 정본은 `sql/meta_schema.sql`(코드의 `ensure_schema()` 가 자동 생성하기도 함).

---

## 새 테이블 추가하기

1. `config/connections.json` 에 source/target 접속이 없으면 추가.
2. `config/tables/<table_id>.yaml` 작성 (위 예시 또는 `*.example.yaml` 복붙).
3. 끝. DAG `pg2ch_<table_id>` 가 dag-processor 파싱 시 자동 등록된다.

---

## 프로젝트 구조

```
pg2ch/                     엔진 패키지 (Airflow 비의존, 단위 테스트 가능)
├── chtypes.py             PG→CH 타입 매핑 / CH 타입 문자열 유틸
├── ddl.py                 CH 컬럼 매핑 + CREATE TABLE DDL 생성
├── transform.py           PG row → CH INSERT 호환 변환
├── watermark.py           sync_since / overlap 계산
├── connections.py         접속 레지스트리(JSON) + PG/CH/meta 커넥션
├── config.py              테이블 설정(YAML) 로드/검증 (TableConfig)
├── tracking.py            메타 저장소 (copy_run/copy_batch/copy_failed_row)
├── copier.py              복사 오케스트레이션 (모드 + batch/row 추적)
└── cli.py                 CLI
dags/pg2ch_factory.py      config → DAG(테이블당 1개) 동적 생성
sql/meta_schema.sql        메타 스키마 정본
config/                    connections.json + tables/*.yaml (실제 설정은 gitignore)
docker-compose.yaml        Airflow 3.2.2 (LocalExecutor) + ClickHouse
Dockerfile                 apache/airflow:3.2.2 + clickhouse-driver + pg2ch
tests/                     pytest (mocked, DB 불필요)
```

## 테스트

```bash
uv run pytest -q
```

---

## 기존 프로젝트 (backup_cron)

이 저장소는 원래 **다중 DB 주기 관리용 cron job 플랫폼(dbcron)** — APScheduler 기반
스케줄러 + Express WebUI + 여러 job(enrich / pg2pg / retention / schema_drift 등) — 이었다.
PG→CH 복사 전용으로 전면 재작성하면서 그 코드 전체를 제거했다.

옛 코드는 **`backup_cron` 브랜치**에 그대로 보존되어 있다 (현재 `main` 의 직전 커밋이기도 함).

```bash
git checkout backup_cron        # 옛 dbcron 프로젝트 보기
git checkout main               # 현재 pg2ch 프로젝트로 복귀
```

> `backup_cron` 은 로컬 브랜치다. 옛 커밋들은 `main` 히스토리의 조상으로도 남아 원격에
> 보존되어 있으나, 라벨까지 원격에 두려면 `git push origin backup_cron` 로 푸시한다.
