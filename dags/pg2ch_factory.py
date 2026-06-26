"""pg2ch DAG factory — config/tables/*.yaml 당 DAG 1개를 동적 생성.

테이블마다 적재 방식(append / full_reload)·주기가 다르므로 테이블당 독립 DAG 로
만들어 스케줄·재시도·추적을 분리한다. dag_id 는 ``pg2ch_<table_id>``.

각 DAG 은 precheck → copy → finalize_watermark → verify → retention 순서의 task 를
가진다. verify 는 retention(=source 삭제) 직전에 최근 watermark 구간의 source/target
row 수를 비교해 누락을 잡고, 누락 시 retention 을 막는다(integrity_on_mismatch=fail).
batch/row 단위 진행·실패 추적은 pg2ch 메타 스키마(copy_run / copy_batch /
copy_failed_row)에 기록한다.

Airflow 3.x (apache/airflow:3.2.2) 기준 import 사용:
  from airflow.sdk import DAG, get_current_context
  from airflow.providers.standard.operators.python import PythonOperator
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from pg2ch.config import TableConfig, load_all_table_configs

if TYPE_CHECKING:
    from airflow.sdk import DAG

log = logging.getLogger("pg2ch.dag_factory")

_DEFAULT_START = datetime(2024, 1, 1)

# ── copy 동시성 제어 ────────────────────────────────────────────────
# 모든 pg2ch_* DAG 의 copy task 를 공유 pool 하나에 묶고, 슬롯 수(=동시 실행
# copy 수 상한)를 env PG2CH_COPY_CONCURRENCY 로 조절한다. LocalExecutor 에서
# 테이블 간 동시성을 막는 정석 레버이며, 동시 copy 의 배치 버퍼 합산으로 인한
# 워커 OOM 을 막는다. 0/unlimited/none/off = pool 미사용(무제한).
_COPY_POOL_NAME = os.environ.get("PG2CH_COPY_POOL", "pg2ch_copy")
_DEFAULT_COPY_CONCURRENCY = 8
_COPY_POOL_DESC = "pg2ch copy 동시성 (env PG2CH_COPY_CONCURRENCY)"
_synced_pool: tuple[str, int] | None = None


def _parse_start_date(value: str | None) -> datetime:
    if not value:
        return _DEFAULT_START
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        log.warning("invalid start_date %r — falling back to %s", value, _DEFAULT_START)
        return _DEFAULT_START


def _runtime_config(table_id: str) -> TableConfig:
    cfg = next((c for c in load_all_table_configs() if c.table_id == table_id), None)
    if cfg is None:
        raise RuntimeError(f"table config '{table_id}' not found at runtime")
    return cfg


def _airflow_context() -> dict:
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    ti = ctx.get("ti") or ctx.get("task_instance")
    return {
        "dag_id": getattr(dag_run, "dag_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "task_id": getattr(ti, "task_id", None),
        "try_number": getattr(ti, "try_number", None),
    }


def _current_ti():
    from airflow.sdk import get_current_context

    ctx = get_current_context()
    return ctx.get("ti") or ctx.get("task_instance")


def _make_precheck_callable(table_id: str):
    """table_id 를 바인딩한 precheck task 콜러블."""

    def _precheck(**_):
        from pg2ch.copier import TableCopier

        cfg = _runtime_config(table_id)
        plan = TableCopier(cfg).precheck()
        return plan.as_dict()

    return _precheck


def _make_copy_callable(table_id: str):
    """데이터를 적재하되 watermark 는 다음 task 에서 finalize 한다."""

    def _copy(**_):
        from pg2ch.copier import TableCopier

        cfg = _runtime_config(table_id)
        result = TableCopier(
            cfg,
            airflow_context=_airflow_context(),
        ).run(finalize_run=False)
        summary = result.as_dict()
        if result.status == "failed":
            raise RuntimeError(f"copy failed: {summary}")
        return summary  # XCom 으로 push

    return _copy


def _make_finalize_callable(table_id: str):
    """copy task 가 계산한 watermark_after 를 copy_run 에 최종 반영한다."""

    def _finalize(**_):
        from pg2ch.copier import TableCopier

        result = _current_ti().xcom_pull(task_ids="copy")
        if not result:
            raise RuntimeError("copy task result not found in XCom")
        cfg = _runtime_config(table_id)
        finalized = TableCopier(cfg).finalize(result)
        return finalized.as_dict()

    return _finalize


def _make_verify_callable(table_id: str):
    """retention 직전 무결성 검사. 설정이 켜져 있고 누락이 잡히면 raise 해 retention 을 막는다."""

    def _verify(**_):
        from pg2ch.integrity import IntegrityChecker

        cfg = _runtime_config(table_id)
        result = IntegrityChecker(cfg).run()
        summary = result.as_dict()
        if result.status == "mismatch":
            if cfg.integrity_on_mismatch == "fail":
                # raise → downstream retention 은 all_success 규칙으로 skip 된다.
                raise RuntimeError(f"integrity check failed (missing rows): {summary}")
            log.warning("%s: integrity mismatch (on_mismatch=warn): %s", table_id, summary)
        return summary  # XCom 으로 push

    return _verify


def _make_retention_callable(table_id: str):
    """설정이 켜져 있으면 PG source retention 을 실행한다."""

    def _retention(**_):
        from pg2ch.retention import PgRetention

        cfg = _runtime_config(table_id)
        result = PgRetention(cfg).run()
        return result.as_dict()

    return _retention


def _resolve_copy_concurrency() -> int | None:
    """env PG2CH_COPY_CONCURRENCY → 동시 copy 슬롯 수.

    - 미설정/빈값                 → 기본값(_DEFAULT_COPY_CONCURRENCY)
    - 0/unlimited/none/off        → 무제한(pool 미사용, None)
    - 양의 정수                   → 그 값
    - 음수/0 이하 정수            → 무제한(None)
    - 그 외(파싱 불가)            → 기본값
    """
    raw = os.environ.get("PG2CH_COPY_CONCURRENCY")
    if raw is None or raw.strip() == "":
        return _DEFAULT_COPY_CONCURRENCY
    token = raw.strip().lower()
    if token in ("0", "unlimited", "none", "off"):
        return None
    try:
        slots = int(token)
    except ValueError:
        log.warning(
            "invalid PG2CH_COPY_CONCURRENCY=%r — using default %d",
            raw, _DEFAULT_COPY_CONCURRENCY,
        )
        return _DEFAULT_COPY_CONCURRENCY
    return slots if slots > 0 else None


def _ensure_copy_pool() -> str | None:
    """env 기준으로 copy 전용 pool 을 생성/동기화하고 pool 이름을 반환한다.

    무제한이거나 airflow Pool API 를 쓸 수 없으면 None 을 반환해, copy task 가
    존재하지 않는 pool 을 참조해 스케줄이 멈추는 일이 없게 한다(=무풀로 동작).
    직전 동기화와 값이 같으면 DB write 를 생략한다(파싱마다 쓰지 않게).
    """
    global _synced_pool
    slots = _resolve_copy_concurrency()
    if slots is None:
        return None
    name = _COPY_POOL_NAME
    if _synced_pool == (name, slots):
        return name
    try:
        from airflow.models.pool import Pool
    except Exception:
        log.debug("airflow Pool API unavailable; copy task will run unpooled")
        return None
    try:
        try:
            Pool.create_or_update_pool(
                name=name, slots=slots, description=_COPY_POOL_DESC,
                include_deferred=False,
            )
        except TypeError:
            # 버전별 시그니처 차이(include_deferred 미지원) 호환
            Pool.create_or_update_pool(
                name=name, slots=slots, description=_COPY_POOL_DESC,
            )
        _synced_pool = (name, slots)
        log.info("pg2ch: copy pool %r synced to %d slot(s)", name, slots)
        return name
    except Exception:
        log.exception(
            "pg2ch: failed to sync copy pool %r; copy task will run unpooled", name
        )
        return None


def build_dag(cfg: TableConfig, copy_pool: str | None = None) -> DAG:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG

    default_args = {
        "retries": cfg.retries,
        "retry_delay": timedelta(seconds=cfg.retry_delay_seconds),
    }
    dag = DAG(
        dag_id=cfg.dag_id,
        description=(cfg.label or f"{cfg.source_table} → {cfg.target_table}")
        + f" ({cfg.sync_mode})",
        schedule=cfg.schedule,
        start_date=_parse_start_date(cfg.start_date),
        catchup=cfg.catchup,
        max_active_runs=cfg.max_active_runs,
        default_args=default_args,
        tags=list(cfg.tags) + [cfg.sync_mode],
        doc_md=(
            f"**pg2ch copy** `{cfg.source_table}` → `{cfg.target_table}`\n\n"
            f"- mode: `{cfg.sync_mode}`\n"
            f"- watermark: `{cfg.effective_watermark_column or '-'}`\n"
            f"- on_row_error: `{cfg.on_row_error}`\n"
            f"- integrity: `{'enabled' if cfg.integrity_enabled else 'disabled'}`\n"
            f"- retention: `{'enabled' if cfg.retention_enabled else 'disabled'}`\n"
        ),
    )
    with dag:
        precheck = PythonOperator(
            task_id="precheck",
            task_display_name="1. precheck",
            python_callable=_make_precheck_callable(cfg.table_id),
        )
        copy = PythonOperator(
            task_id="copy",
            task_display_name="2. copy",
            python_callable=_make_copy_callable(cfg.table_id),
            # copy_pool 이 지정되면 그 pool 슬롯 수만큼만 동시에 실행된다.
            **({"pool": copy_pool} if copy_pool else {}),
        )
        finalize = PythonOperator(
            task_id="finalize_watermark",
            task_display_name="3. finalize_watermark",
            python_callable=_make_finalize_callable(cfg.table_id),
        )
        verify = PythonOperator(
            task_id="verify",
            task_display_name="4. verify",
            python_callable=_make_verify_callable(cfg.table_id),
        )
        retention = PythonOperator(
            task_id="retention",
            task_display_name="5. retention",
            python_callable=_make_retention_callable(cfg.table_id),
        )
        precheck >> copy >> finalize >> verify >> retention
    return dag


def register_dags(global_namespace: dict) -> int:
    """설정을 읽어 모듈 전역에 DAG 객체들을 등록. 등록 수 반환."""
    count = 0
    try:
        configs = load_all_table_configs()
    except Exception:
        log.exception("failed to load table configs")
        return 0
    copy_pool = _ensure_copy_pool()
    for cfg in configs:
        try:
            dag = build_dag(cfg, copy_pool=copy_pool)
        except Exception:
            log.exception("failed to build DAG for table_id=%s", cfg.table_id)
            continue
        global_namespace[dag.dag_id] = dag
        count += 1
    log.info("pg2ch: registered %d DAG(s)", count)
    return count


# Airflow dag-processor 가 이 모듈을 파싱할 때 전역에 DAG 들이 노출되어야 한다.
register_dags(globals())
