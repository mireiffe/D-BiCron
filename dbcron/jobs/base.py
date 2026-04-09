"""Job base class and result type."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from ..config import InfraConfig

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    success: bool
    message: str
    rows_affected: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "message": self.message,
            "rows_affected": self.rows_affected,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


class Job(ABC):
    """Base class for all scheduled jobs."""

    name: str = "base"
    label: str = ""
    description: str = ""
    default_args: dict = {"days": 1}

    def __init__(self, config: InfraConfig):
        self.config = config
        self.logger = logging.getLogger(f"job.{self.name}")

    def execute(self, **kwargs) -> JobResult:
        self.logger.info("Starting job: %s", self.name)
        started = datetime.now()
        try:
            result = self.run(**kwargs)
            result.started_at = started
            result.finished_at = datetime.now()
            self.logger.info(
                "Job %s finished: success=%s, rows=%d",
                self.name,
                result.success,
                result.rows_affected,
            )
            return result
        except Exception as exc:
            self.logger.exception("Job %s failed with exception", self.name)
            return JobResult(
                success=False,
                message=str(exc),
                started_at=started,
                finished_at=datetime.now(),
            )

    @abstractmethod
    def run(self, **kwargs) -> JobResult:
        ...
