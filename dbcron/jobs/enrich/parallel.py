"""행 단위 병렬 처리.

소스 행별 독립 파이프라인(쿼리 체인 → S3 로드 → API 호출 → 응답 파싱)을
ThreadPoolExecutor에서 병렬 실행합니다.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    """스레드 안전한 파이프라인 카운터."""

    success: int = 0
    skipped: int = 0
    failed: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_success(self, n: int = 1) -> None:
        with self._lock:
            self.success += n

    def record_skip(self, n: int = 1) -> None:
        with self._lock:
            self.skipped += n

    def record_failure(self, n: int = 1) -> None:
        with self._lock:
            self.failed += n

    @property
    def total(self) -> int:
        return self.success + self.skipped + self.failed


def run_parallel(
    rows: list[dict],
    pipeline_fn: Callable[[dict], list[dict]],
    max_workers: int,
) -> tuple[list[dict], PipelineStats]:
    """행별 파이프라인을 병렬(또는 순차) 실행한다.

    Args:
        rows: 소스 행 리스트
        pipeline_fn: 단일 행 → enriched 행 리스트.
            성공 시 list[dict] 반환.
            행 스킵 시 빈 리스트 반환.
            실패 시 예외 발생.
        max_workers: 최대 워커 수. 1이면 순차 실행.

    Returns:
        (모든 enriched 행 리스트, PipelineStats)
    """
    stats = PipelineStats()

    if not rows:
        return [], stats

    all_enriched: list[dict] = []

    if max_workers <= 1:
        # 순차 실행
        for row in rows:
            try:
                enriched = pipeline_fn(row)
                if enriched:
                    all_enriched.extend(enriched)
                    stats.record_success(len(enriched))
                else:
                    stats.record_skip()
            except Exception:
                logger.warning("Row pipeline failed", exc_info=True)
                stats.record_failure()
        return all_enriched, stats

    # 병렬 실행
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_row = {
            executor.submit(pipeline_fn, row): row for row in rows
        }
        for future in as_completed(future_to_row):
            try:
                enriched = future.result()
                if enriched:
                    all_enriched.extend(enriched)
                    stats.record_success(len(enriched))
                else:
                    stats.record_skip()
            except Exception:
                logger.warning("Row pipeline failed", exc_info=True)
                stats.record_failure()

    return all_enriched, stats
