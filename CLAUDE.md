# db_manager

coredb 주기적 관리용 cron job 플랫폼.

## Structure

- `dbcron/` — Python 패키지 (config, db, scheduler, jobs)
- `dbcron/webui/` — Node.js Express Web UI (cron 등록/삭제/수동 실행)

## Running

```bash
# CLI one-shot
python -m dbcron.main recommendation --days 1

# CLI with cron
python -m dbcron.main recommendation --days 1 --cron "*/10 * * * *"

# Web UI
cd dbcron/webui && npm install && npm start
```

## Adding a new job

1. Create a `Job` subclass in `dbcron/jobs/` implementing `run(**kwargs) -> JobResult`
2. Register it in `dbcron/jobs/__init__.py` `JOB_REGISTRY`
3. Add UI metadata to `AVAILABLE_JOBS` in `dbcron/webui/server.js`
