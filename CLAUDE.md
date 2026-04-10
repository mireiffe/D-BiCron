# db_manager

다중 DB 주기적 관리용 cron job 플랫폼. 상세 내용은 `README.md` 참조.

DB 접속 정보는 `data/databases.json` 에서 관리 (WebUI 또는 직접 편집).

## Quick ref

- `uv sync` — 의존성 설치
- `uv run dbcron <job> --days N` — CLI one-shot
- `cd dbcron/webui && npm start` — Web UI

## Adding a new job

1. `dbcron/jobs/`에 `Job` 서브클래스 작성 (`run(**kwargs) -> JobResult`)
2. `dbcron/jobs/__init__.py`의 `JOB_REGISTRY`에 등록
3. `dbcron/webui/server.js`의 `AVAILABLE_JOBS`에 UI 메타데이터 추가
