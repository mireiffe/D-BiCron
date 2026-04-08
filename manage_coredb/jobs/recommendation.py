"""Recommendation job: susdb -> Recommend API -> coredb."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import httpx
from sqlalchemy import text

from ..db import create_coredb_engine, create_susdb_engine
from .base import Job, JobResult

logger = logging.getLogger(__name__)


class RecommendationJob(Job):
    name = "recommendation"
    description = "susdb에서 대상 데이터를 조회하여 추천 API를 거쳐 coredb에 적재합니다."

    def run(self, *, days: int = 1, **kwargs) -> JobResult:
        cutoff = datetime.now() - timedelta(days=days)

        # 1) susdb에서 대상 레코드 조회
        sus_engine = create_susdb_engine(self.config)
        with sus_engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, payload "
                    "FROM recommendations_queue "
                    "WHERE created_at >= :cutoff AND processed = false "
                    "ORDER BY created_at"
                ),
                {"cutoff": cutoff},
            ).fetchall()

        if not rows:
            return JobResult(success=True, message="No pending records", rows_affected=0)

        self.logger.info("Fetched %d records from susdb", len(rows))

        # 2) 추천 API 호출
        api_url = f"{self.config.recommend_api_host}/api/v1/recommend"
        results = []
        with httpx.Client(timeout=30) as client:
            for row in rows:
                resp = client.post(api_url, json={"id": row.id, "payload": row.payload})
                resp.raise_for_status()
                results.append({"source_id": row.id, **resp.json()})

        # 3) coredb에 결과 적재
        core_engine = create_coredb_engine(self.config)
        inserted = 0
        with core_engine.begin() as conn:
            for item in results:
                conn.execute(
                    text(
                        "INSERT INTO recommendations (source_id, score, recommended_items, created_at) "
                        "VALUES (:source_id, :score, :items, :now)"
                    ),
                    {
                        "source_id": item["source_id"],
                        "score": item.get("score", 0),
                        "items": str(item.get("recommended_items", [])),
                        "now": datetime.now(),
                    },
                )
                inserted += 1

        # 4) susdb 처리 완료 마킹
        processed_ids = [r["source_id"] for r in results]
        with sus_engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE recommendations_queue SET processed = true "
                    "WHERE id = ANY(:ids)"
                ),
                {"ids": processed_ids},
            )

        return JobResult(
            success=True,
            message=f"Processed {inserted} recommendations",
            rows_affected=inserted,
        )
