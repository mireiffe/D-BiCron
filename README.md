# dbcron

다중 DB 주기적 관리용 cron job 플랫폼.

## 설치

```bash
uv sync
```

## 실행

```bash
# CLI one-shot
uv run dbcron <job_name> --days 1

# CLI with cron
uv run dbcron <job_name> --days 1 --cron "*/10 * * * *"

# Web UI
cd dbcron/webui && npm install && npm start
```

## DB 등록

DB 접속 정보는 `data/databases.json` 파일에서 관리합니다.
WebUI 에서 추가/삭제하거나 직접 편집할 수 있습니다.

```bash
# 샘플 데이터로 시작하기
uv run python scripts/seed.py
```

`databases.json` 형식:

```json
[
  {
    "id": "my_postgres",
    "type": "postgresql",
    "label": "My PostgreSQL",
    "color": "#00e5ff",
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypassword"
  }
]
```

지원 DB 타입: `postgresql`, `mssql`, `sqlite`, `clickhouse`

## 환경 변수

`.env.example`을 `.env`로 복사하고 값을 채워 넣으세요.

```bash
cp .env.example .env
```

DB 접속 정보는 환경 변수가 아닌 `data/databases.json`에서 관리합니다.
`.env`에는 S3, webhook, sync config 경로 등 인프라 설정만 포함됩니다.

## 새로운 Job 추가하기

3단계로 완료됩니다.

### 1단계: Job 클래스 작성

`dbcron/jobs/` 디렉토리에 새 파일을 만듭니다.

```python
# dbcron/jobs/my_sync.py
"""My sync job: some_source → target."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import text

from ..db import create_engine_by_id
from .base import Job, JobResult

logger = logging.getLogger(__name__)


class MySyncJob(Job):
    name = "my_sync"
    description = "source DB에서 데이터를 가져와 target DB에 적재합니다."

    def run(self, *, days: int = 1, **kwargs) -> JobResult:
        cutoff = datetime.now() - timedelta(days=days)

        engine = create_engine_by_id("my_target_db")
        with engine.begin() as conn:
            result = conn.execute(
                text("INSERT INTO ... SELECT ... WHERE created_at >= :cutoff"),
                {"cutoff": cutoff},
            )

        return JobResult(
            success=True,
            message=f"Synced {result.rowcount} rows",
            rows_affected=result.rowcount,
        )
```

**필수 규칙:**

- `Job`을 상속하고 `run(**kwargs) -> JobResult`를 구현합니다.
- `name` 클래스 변수는 CLI와 API에서 사용하는 고유 식별자입니다.
- DB 엔진은 `dbcron.db.create_engine_by_id(db_id)` 또는 `create_engine_for(db_cfg)`로 생성합니다.
- `self.logger`는 `job.<name>` 네임스페이스의 로거입니다.
- `execute()`를 직접 오버라이드하지 마세요 — 에러 핸들링과 로깅이 자동으로 처리됩니다.

### 2단계: JOB_REGISTRY에 등록

`dbcron/jobs/__init__.py`가 자동으로 `Job` 서브클래스를 탐색합니다.
별도 등록 없이 파일을 추가하면 바로 사용 가능합니다:

```bash
uv run dbcron my_sync --days 7
uv run dbcron my_sync --days 1 --cron "0 */6 * * *"
```

### 3단계: Web UI에 등록

`dbcron/webui/server.js`의 `AVAILABLE_JOBS` 배열에 항목을 추가합니다.

```javascript
const AVAILABLE_JOBS = [
  {
    name: "my_sync",           // JOB_REGISTRY 키와 동일
    label: "My Sync 적재",      // UI에 표시되는 이름
    description: "source → target 적재",  // UI 설명
    defaultArgs: { days: 1 },  // RUN 버튼 클릭 시 기본 인자
  },
];
```

`defaultArgs`의 키는 Job 클래스의 `run()` 메서드 파라미터와 일치해야 합니다.

## 프로젝트 구조

```
db_manager/
├── pyproject.toml          # uv 의존성 관리
├── .env.example            # 환경 변수 템플릿 (S3, webhook 등)
├── data/
│   └── databases.json      # DB 접속 정보 레지스트리
├── dbcron/
│   ├── main.py             # CLI 엔트리포인트
│   ├── config.py           # 환경 변수 → AppConfig (S3 등)
│   ├── db.py               # DB 레지스트리, 엔진 팩토리, URL 빌더
│   ├── scheduler.py        # APScheduler 래퍼
│   └── jobs/
│       ├── __init__.py     # JOB_REGISTRY (자동 탐색)
│       └── base.py         # Job ABC, JobResult dataclass
└── dbcron/webui/
    ├── server.js           # Express API + AVAILABLE_JOBS
    └── public/             # 정적 프론트엔드
```
