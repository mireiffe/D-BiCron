"""Job base class and result type."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from ..config import AppConfig
from ..db import resolve_targets

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
    # scope: "all_dbs" | "all_tables" | "pipeline" | "none"
    #   all_dbs    — DB 컨테이너 수준 (모든 DB 대상)
    #   all_tables — 모든 DB의 모든 테이블 대상
    #   pipeline   — pipeline_config 의 특정 source/target 테이블
    #   none       — DB 접근 없음
    scope: str = "none"

    def __init__(self, config: AppConfig | None = None):
        self.config = config
        self.logger = logging.getLogger(f"job.{self.name}")
        self._targets: list[dict] | None = None
        self._databases: list[dict] | None = None
        self._table_filter: Callable[[str, str, dict], bool] | None = None

    def resolve_databases(
        self, targets: list[dict] | None = None,
    ) -> tuple[list[dict], Callable[[str, str, dict], bool]]:
        """targets 기반으로 필터된 DB 목록과 테이블 필터 함수를 반환한다.

        execute() 에서 설정한 _targets 를 사용하거나, 직접 전달할 수 있다.
        결과는 캐시되어 같은 실행 내에서 재사용된다.
        """
        t = targets if targets is not None else self._targets
        if self._databases is None or targets is not None:
            self._databases, self._table_filter = resolve_targets(t)
        return self._databases, self._table_filter

    def execute(self, **kwargs) -> JobResult:
        self.logger.info("Starting job: %s", self.name)
        started = datetime.now()
        # targets를 kwarg에서 추출하여 인스턴스에 보관
        self._targets = kwargs.pop("targets", None)
        if self._targets:
            db_ids = list({t.get("db") for t in self._targets if t.get("db")})
            self.logger.info("Targets: %s", db_ids)
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
