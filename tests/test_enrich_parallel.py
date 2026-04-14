"""Tests for dbcron/jobs/enrich/parallel.py."""

from __future__ import annotations

import threading
import time

from dbcron.jobs.enrich.parallel import PipelineStats, run_parallel


class TestPipelineStats:
    def test_basic_counters(self):
        stats = PipelineStats()
        stats.record_success(3)
        stats.record_skip(1)
        stats.record_failure(2)
        assert stats.success == 3
        assert stats.skipped == 1
        assert stats.failed == 2
        assert stats.total == 6

    def test_thread_safety(self):
        stats = PipelineStats()
        barrier = threading.Barrier(10)

        def worker():
            barrier.wait()
            for _ in range(100):
                stats.record_success()

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert stats.success == 1000


class TestRunParallel:
    def test_sequential_success(self):
        def pipeline(row):
            return [{"id": row["id"], "enriched": True}]

        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        enriched, stats = run_parallel(rows, pipeline, max_workers=1)
        assert len(enriched) == 3
        assert stats.success == 3
        assert stats.failed == 0

    def test_parallel_success(self):
        def pipeline(row):
            return [{"id": row["id"]}]

        rows = [{"id": i} for i in range(10)]
        enriched, stats = run_parallel(rows, pipeline, max_workers=4)
        assert len(enriched) == 10
        assert stats.success == 10

    def test_individual_failure_continues(self):
        def pipeline(row):
            if row["id"] == 2:
                raise ValueError("bad row")
            return [{"id": row["id"]}]

        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        enriched, stats = run_parallel(rows, pipeline, max_workers=2)
        assert len(enriched) == 2
        assert stats.success == 2
        assert stats.failed == 1

    def test_empty_return_is_skip(self):
        def pipeline(row):
            if row["id"] == 1:
                return []  # skip
            return [{"id": row["id"]}]

        rows = [{"id": 1}, {"id": 2}]
        enriched, stats = run_parallel(rows, pipeline, max_workers=1)
        assert len(enriched) == 1
        assert stats.skipped == 1
        assert stats.success == 1

    def test_empty_rows(self):
        enriched, stats = run_parallel([], lambda r: [r], max_workers=4)
        assert enriched == []
        assert stats.total == 0

    def test_default_sequential(self):
        """max_workers=1 should process sequentially (order preserved)."""
        order = []

        def pipeline(row):
            order.append(row["id"])
            return [row]

        rows = [{"id": i} for i in range(5)]
        run_parallel(rows, pipeline, max_workers=1)
        assert order == [0, 1, 2, 3, 4]

    def test_multi_row_output(self):
        """Pipeline can return multiple enriched rows per input."""
        def pipeline(row):
            return [{"id": row["id"], "i": 0}, {"id": row["id"], "i": 1}]

        rows = [{"id": 1}]
        enriched, stats = run_parallel(rows, pipeline, max_workers=1)
        assert len(enriched) == 2
        assert stats.success == 2
