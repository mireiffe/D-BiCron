"""CLI entrypoint for running jobs (one-shot or cron)."""

import argparse
import json
import logging
import sys

from .config import load_config
from .jobs import JOB_REGISTRY
from .scheduler import run_scheduled

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    parser = argparse.ArgumentParser(
        description="dbcron — DB management job runner",
    )
    parser.add_argument(
        "job",
        nargs="?",
        choices=list(JOB_REGISTRY),
        help="Job to execute",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Look-back window in days (default: 1)",
    )
    parser.add_argument(
        "--cron",
        type=str,
        default=None,
        help='Cron expression for repeated execution (e.g. "*/10 * * * *")',
    )
    parser.add_argument(
        "--targets",
        type=str,
        default=None,
        help='Target DB/tables as JSON (e.g. \'[{"db":"shop_db","table":"orders"}]\')',
    )
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="Print available jobs as JSON and exit",
    )

    args, _remaining = parser.parse_known_args()

    if args.list_jobs:
        jobs = []
        for name, cls in JOB_REGISTRY.items():
            jobs.append({
                "name": name,
                "label": getattr(cls, "label", "") or name,
                "description": getattr(cls, "description", ""),
                "defaultArgs": getattr(cls, "default_args", {"days": 1}),
                "scope": getattr(cls, "scope", "none"),
            })
        print(json.dumps(jobs))
        sys.exit(0)

    if not args.job:
        parser.error("job is required (or use --list-jobs)")

    # job 의 default_args 에 선언된 키를 동적으로 argparse 에 추가
    job_cls = JOB_REGISTRY[args.job]
    for key, default in getattr(job_cls, "default_args", {}).items():
        if key == "days":
            continue  # 전역 인자로 이미 정의됨
        parser.add_argument(f"--{key}", default=default)
    args = parser.parse_args()

    job_kwargs = {"days": args.days}
    for key in getattr(job_cls, "default_args", {}):
        if key == "days":
            continue
        val = getattr(args, key, None)
        if val is not None:
            job_kwargs[key] = val

    if args.targets:
        import json as _json

        try:
            job_kwargs["targets"] = _json.loads(args.targets)
        except _json.JSONDecodeError:
            parser.error("--targets must be valid JSON")

    try:
        config = load_config()
    except EnvironmentError:
        config = None  # jobs that don't need infra can run without config

    if args.cron:
        run_scheduled(args.job, config, args.cron, **job_kwargs)
    else:
        job_cls = JOB_REGISTRY[args.job]
        result = job_cls(config).execute(**job_kwargs)
        if not result.success:
            print(f"FAILED: {result.message}", file=sys.stderr)
            sys.exit(1)
        print(f"OK: {result.message} ({result.rows_affected} rows)")


if __name__ == "__main__":
    main()
