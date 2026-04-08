"""CLI entrypoint for running jobs (one-shot or cron)."""

import argparse
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
        description="coredb batch job runner",
    )
    parser.add_argument(
        "job",
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

    args = parser.parse_args()
    config = load_config()
    job_kwargs = {"days": args.days}

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
