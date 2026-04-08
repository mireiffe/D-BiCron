"""Python built-in cron scheduler using APScheduler."""

from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import AppConfig
from .jobs import JOB_REGISTRY

logger = logging.getLogger(__name__)


def run_scheduled(job_name: str, config: AppConfig, cron_expr: str, **job_kwargs):
    """Register a job with a cron expression and start the scheduler."""
    job_cls = JOB_REGISTRY.get(job_name)
    if job_cls is None:
        raise ValueError(f"Unknown job: {job_name}. Available: {list(JOB_REGISTRY)}")

    job_instance = job_cls(config)
    scheduler = BlockingScheduler()

    trigger = CronTrigger.from_crontab(cron_expr)
    scheduler.add_job(job_instance.execute, trigger, kwargs=job_kwargs, id=job_name)

    logger.info("Scheduled '%s' with cron '%s'", job_name, cron_expr)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
