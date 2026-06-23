"""pg2ch CLI — Airflow 밖에서 복사를 실행하거나 메타를 점검하는 one-shot 도구.

  pg2ch init-meta                 메타 스키마 생성 (copy_run / copy_batch / copy_failed_row)
  pg2ch list                      설정된 테이블 파이프라인 목록
  pg2ch copy <table_id|all>       복사 1회 실행
  pg2ch retention <table_id|all>  PG source retention 1회 실행
  pg2ch status <table_id>         마지막 run / watermark / 미해결 실패 row 수

경로는 환경변수로 조정:
  PG2CH_CONNECTIONS (기본 config/connections.json)
  PG2CH_TABLES_DIR  (기본 config/tables)
"""

from __future__ import annotations

import argparse
import logging
import sys

from .config import load_all_table_configs, load_table_config, tables_dir
from .connections import get_connection
from .copier import TableCopier
from .retention import PgRetention
from .tracking import MetaStore


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _find_config(table_id: str, dir_path):
    for cfg in load_all_table_configs(dir_path):
        if cfg.table_id == table_id:
            return cfg
    # 파일명으로도 시도
    p = tables_dir(dir_path) / f"{table_id}.yaml"
    if p.exists():
        return load_table_config(p)
    raise SystemExit(f"table config '{table_id}' not found")


def cmd_init_meta(args) -> int:
    conn_path = args.connections
    meta_conn = args.meta_conn
    cfg = get_connection(meta_conn, conn_path)
    with MetaStore.connect(cfg) as meta:
        meta.ensure_schema()
    print(f"meta schema '{cfg.get('schema', 'pg2ch_meta')}' ensured on '{meta_conn}'")
    return 0


def cmd_list(args) -> int:
    configs = load_all_table_configs(args.tables_dir)
    if not configs:
        print("(no table configs found)")
        return 0
    for c in configs:
        print(
            f"{c.table_id:24s} {c.sync_mode:11s} "
            f"{c.source_table} -> {c.target_table}  "
            f"[schedule={c.schedule or '-'} retention={'on' if c.retention_enabled else 'off'}]"
        )
    return 0


def cmd_copy(args) -> int:
    if args.table_id == "all":
        configs = load_all_table_configs(args.tables_dir)
    else:
        configs = [_find_config(args.table_id, args.tables_dir)]

    failures = 0
    for cfg in configs:
        copier = TableCopier(cfg, connections_path=args.connections)
        try:
            result = copier.run()
            print(
                f"[{result.status}] {cfg.table_id}: read={result.rows_read} "
                f"written={result.rows_written} failed={result.rows_failed} "
                f"batches={result.batch_count} run_id={result.run_id}"
            )
            if result.status != "success":
                failures += 1
        except Exception as e:
            failures += 1
            print(f"[failed] {cfg.table_id}: {e}", file=sys.stderr)
    return 1 if failures else 0


def cmd_retention(args) -> int:
    if args.table_id == "all":
        configs = load_all_table_configs(args.tables_dir)
    else:
        configs = [_find_config(args.table_id, args.tables_dir)]

    failures = 0
    for cfg in configs:
        try:
            result = PgRetention(cfg, connections_path=args.connections).run()
            print(
                f"[{result.status}] {cfg.table_id}: deleted={result.rows_deleted} "
                f"safe_cutoff={result.safe_cutoff or '-'} "
                f"reason={result.reason or '-'}"
            )
        except Exception as e:
            failures += 1
            print(f"[failed] {cfg.table_id}: {e}", file=sys.stderr)
    return 1 if failures else 0


def cmd_status(args) -> int:
    cfg = _find_config(args.table_id, args.tables_dir)
    meta_cfg = get_connection(cfg.meta, args.connections)
    with MetaStore.connect(meta_cfg) as meta:
        wm_col = cfg.effective_watermark_column
        wm = meta.get_resume_watermark(cfg.table_id, wm_col) if wm_col else None
        unresolved = meta.unresolved_failed_count(cfg.table_id)
    print(f"table_id        : {cfg.table_id}")
    print(f"sync_mode       : {cfg.sync_mode}")
    print(f"watermark_column: {wm_col or '-'}")
    print(f"resume watermark: {wm or '(none — next run is a full copy)'}")
    print(f"retention       : {'enabled' if cfg.retention_enabled else 'disabled'}")
    print(f"unresolved failed rows: {unresolved}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pg2ch", description="PostgreSQL → ClickHouse 복사")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--connections", default=None, help="connections.json 경로")
    p.add_argument("--tables-dir", default=None, help="테이블 설정 디렉터리")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("init-meta", help="메타 스키마 생성")
    s.add_argument("--meta-conn", default="meta", help="메타 접속 ID (기본 meta)")
    s.set_defaults(func=cmd_init_meta)

    s = sub.add_parser("list", help="테이블 파이프라인 목록")
    s.set_defaults(func=cmd_list)

    s = sub.add_parser("copy", help="복사 1회 실행")
    s.add_argument("table_id", help="table_id 또는 'all'")
    s.set_defaults(func=cmd_copy)

    s = sub.add_parser("retention", help="PG source retention 1회 실행")
    s.add_argument("table_id", help="table_id 또는 'all'")
    s.set_defaults(func=cmd_retention)

    s = sub.add_parser("status", help="마지막 run / watermark / 실패 row 수")
    s.add_argument("table_id")
    s.set_defaults(func=cmd_status)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
