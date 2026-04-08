# dbcron

coredb 주기적 관리용 cron job 플랫폼.

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

## 환경 변수

`.env.example`을 `.env`로 복사하고 값을 채워 넣으세요.

```bash
cp .env.example .env
```

## 새로운 Job 추가하기

3단계로 완료됩니다.

### 1단계: Job 클래스 작성

`dbcron/jobs/` 디렉토리에 새 파일을 만듭니다.

```python
# dbcron/jobs/my_sync.py
"""My sync job: some_source → coredb."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import text

from ..db import create_coredb_engine
from .base import Job, JobResult

logger = logging.getLogger(__name__)


class MySyncJob(Job):
    name = "my_sync"
    description = "some_source에서 데이터를 가져와 coredb에 적재합니다."

    def run(self, *, days: int = 1, **kwargs) -> JobResult:
        cutoff = datetime.now() - timedelta(days=days)

        engine = create_coredb_engine(self.config)
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
- `self.config`는 `InfraConfig` 인스턴스로, `susdb`, `coredb`, `s3` 설정에 접근할 수 있습니다.
- `self.logger`는 `job.<name>` 네임스페이스의 로거입니다.
- `execute()`를 직접 오버라이드하지 마세요 — 에러 핸들링과 로깅이 자동으로 처리됩니다.

### 2단계: JOB_REGISTRY에 등록

`dbcron/jobs/__init__.py`에 import와 레지스트리 항목을 추가합니다.

```python
from .my_sync import MySyncJob

JOB_REGISTRY: dict[str, type] = {
    "my_sync": MySyncJob,
}
```

이것만으로 CLI와 cron 스케줄러에서 사용할 수 있습니다:

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
    description: "some_source → coredb 적재",  // UI 설명
    defaultArgs: { days: 1 },  // RUN 버튼 클릭 시 기본 인자
  },
];
```

`defaultArgs`의 키는 Job 클래스의 `run()` 메서드 파라미터와 일치해야 합니다.

## 프로젝트 구조

```
db_manager/
├── pyproject.toml          # uv 의존성 관리
├── .env.example            # 환경 변수 템플릿
├── dbcron/
│   ├── main.py             # CLI 엔트리포인트
│   ├── config.py           # 환경 변수 → InfraConfig
│   ├── db.py               # SQLAlchemy 엔진 팩토리
│   ├── scheduler.py        # APScheduler 래퍼
│   └── jobs/
│       ├── __init__.py     # JOB_REGISTRY
│       └── base.py         # Job ABC, JobResult dataclass
└── dbcron/webui/
    ├── server.js           # Express API + AVAILABLE_JOBS
    └── public/             # 정적 프론트엔드
```
