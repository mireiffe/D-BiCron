"""Sample development environment seeder.

3개의 SQLite 샘플 DB를 생성하고, databases.json / pipeline_config.json 을
자동 구성한 뒤 메타데이터 스냅샷을 수집합니다.
외부 DB 서버 없이 클론 후 바로 실행 가능합니다.

Usage:
    uv run python scripts/seed.py
"""

import json
import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SAMPLE = DATA / "sample_dbs"


# ── 1. 샘플 SQLite DB 생성 ─────────────────────────────────────

def create_users_db():
    """사용자/인증 도메인"""
    db = SAMPLE / "users.db"
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS roles (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id INTEGER REFERENCES users(id),
            role_id INTEGER REFERENCES roles(id),
            PRIMARY KEY (user_id, role_id)
        );

        INSERT OR IGNORE INTO roles (id, name) VALUES (1,'admin'),(2,'editor'),(3,'viewer');
        INSERT OR IGNORE INTO users (id, name, email) VALUES
            (1,'Alice','alice@example.com'),
            (2,'Bob','bob@example.com'),
            (3,'Charlie','charlie@example.com'),
            (4,'Diana','diana@example.com'),
            (5,'Eve','eve@example.com');
        INSERT OR IGNORE INTO user_roles VALUES (1,1),(2,2),(3,3),(4,2),(5,3),(1,2);
    """)
    conn.commit()
    conn.close()
    print(f"  users.db — 3 tables, 5 users")


def create_shop_db():
    """이커머스 도메인"""
    db = SAMPLE / "shop.db"
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS orders (
            id         INTEGER PRIMARY KEY,
            user_email TEXT NOT NULL,
            status     TEXT DEFAULT 'pending',
            ordered_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id         INTEGER PRIMARY KEY,
            order_id   INTEGER REFERENCES orders(id),
            product_id INTEGER REFERENCES products(id),
            qty        INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id);
        CREATE INDEX IF NOT EXISTS idx_items_product ON order_items(product_id);

        INSERT OR IGNORE INTO products (id, name, price, stock) VALUES
            (1,'Keyboard',89000,50),(2,'Mouse',45000,120),(3,'Monitor',350000,30),
            (4,'Headset',72000,80),(5,'Webcam',55000,60);
        INSERT OR IGNORE INTO orders (id, user_email, status) VALUES
            (1,'alice@example.com','shipped'),(2,'bob@example.com','pending'),
            (3,'charlie@example.com','delivered'),(4,'alice@example.com','pending');
        INSERT OR IGNORE INTO order_items (id, order_id, product_id, qty) VALUES
            (1,1,1,1),(2,1,2,2),(3,2,3,1),(4,3,4,1),(5,3,5,1),(6,4,1,1),(7,4,4,2);
    """)
    conn.commit()
    conn.close()
    print(f"  shop.db  — 3 tables, 5 products, 4 orders")


def create_analytics_db():
    """분석 도메인 (싱크 대상)"""
    db = SAMPLE / "analytics.db"
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS page_views (
            id      INTEGER PRIMARY KEY,
            path    TEXT NOT NULL,
            user_email TEXT,
            viewed_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS events (
            id    INTEGER PRIMARY KEY,
            type  TEXT NOT NULL,
            payload TEXT,
            ts    TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS order_summary (
            id         INTEGER PRIMARY KEY,
            user_email TEXT,
            status     TEXT,
            synced_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_pv_path ON page_views(path);
        CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);

        INSERT OR IGNORE INTO page_views (id, path, user_email) VALUES
            (1,'/',NULL),(2,'/products','alice@example.com'),
            (3,'/products/1','bob@example.com'),(4,'/cart','alice@example.com');
        INSERT OR IGNORE INTO events (id, type, payload) VALUES
            (1,'click','{"button":"buy"}'),(2,'scroll','{"depth":80}'),
            (3,'click','{"button":"add_cart"}');
    """)
    conn.commit()
    conn.close()
    print(f"  analytics.db — 3 tables, 4 views, 3 events")


# ── 2. databases.json 등록 ─────────────────────────────────────

def write_databases_json():
    prefix = str(SAMPLE)
    dbs = [
        {
            "id": "users_db",
            "type": "sqlite",
            "label": "Users DB",
            "color": "#00e5ff",
            "host": f"{prefix}/users.db",
            "port": 0,
            "dbname": "users",
            "user": "",
            "password": "",
        },
        {
            "id": "shop_db",
            "type": "sqlite",
            "label": "Shop DB",
            "color": "#c8ff00",
            "host": f"{prefix}/shop.db",
            "port": 0,
            "dbname": "shop",
            "user": "",
            "password": "",
        },
        {
            "id": "analytics_db",
            "type": "sqlite",
            "label": "Analytics DB",
            "color": "#ff2d8a",
            "host": f"{prefix}/analytics.db",
            "port": 0,
            "dbname": "analytics",
            "user": "",
            "password": "",
        },
    ]
    out = DATA / "databases.json"
    with open(out, "w") as f:
        json.dump(dbs, f, indent=2)
    print(f"  databases.json — {len(dbs)} databases registered")


# ── 3. pipeline_config.json 생성 ───────────────────────────────

def write_pipeline_config():
    cfg = {
        "entry_points": [
            {
                "id": "ep_shop_api",
                "name": "Shop API",
                "description": "주문/상품 REST API",
                "type": "api",
                "targets": [
                    {"db": "shop_db", "schema": "main", "table": "orders"},
                    {"db": "shop_db", "schema": "main", "table": "products"},
                ],
            },
            {
                "id": "ep_tracker",
                "name": "Web Tracker",
                "description": "프론트엔드 이벤트 수집기",
                "type": "service",
                "targets": [
                    {"db": "analytics_db", "schema": "main", "table": "page_views"},
                    {"db": "analytics_db", "schema": "main", "table": "events"},
                ],
            },
        ],
        "pipelines": [
            {
                "id": "pipe_order_sync",
                "source_job": "incremental_sync",
                "description": "Shop → Analytics 주문 동기화",
                "connections": [
                    {
                        "from": {"db": "shop_db", "schema": "main", "table": "orders"},
                        "to": {"db": "analytics_db", "schema": "main", "table": "order_summary"},
                        "label": "order sync",
                    }
                ],
            },
        ],
    }
    out = ROOT / "pipeline_config.json"
    with open(out, "w") as f:
        json.dump(cfg, f, indent=2)
    print(f"  pipeline_config.json — {len(cfg['entry_points'])} entry points, {len(cfg['pipelines'])} pipelines")


# ── 4. 스케줄 등록 (team_vibe 매 30분) ─────────────────────────

def write_sample_schedules():
    schedules = {
        "nextId": 3,
        "schedules": [
            {
                "id": 1,
                "jobName": "metadata_snapshot",
                "cron": "0 */6 * * *",
                "args": {},
                "createdAt": "2026-04-10T00:00:00.000Z",
            },
            {
                "id": 2,
                "jobName": "team_vibe",
                "cron": "0 9 * * 1-5",
                "args": {"days": 1},
                "createdAt": "2026-04-10T00:00:00.000Z",
            },
        ],
    }
    out = DATA / "schedules.json"
    with open(out, "w") as f:
        json.dump(schedules, f, indent=2)
    print(f"  schedules.json — {len(schedules['schedules'])} cron schedules")


# ── 5. 메타데이터 스냅샷 수집 ──────────────────────────────────

def run_metadata_snapshot():
    from dbcron.jobs.metadata_snapshot import collect_db_metadata, DATA_DIR
    from sqlalchemy import create_engine

    dbs = json.loads((DATA / "databases.json").read_text())
    from dbcron.jobs.metadata_snapshot import URL_BUILDERS

    snapshot = {"snapshot_at": "", "databases": {}}
    total = 0
    for db_cfg in dbs:
        url = URL_BUILDERS[db_cfg["type"]](db_cfg)
        engine = create_engine(url)
        tables = collect_db_metadata(engine, db_cfg["type"])
        engine.dispose()
        snapshot["databases"][db_cfg["id"]] = {
            "host": db_cfg.get("host", ""),
            "database": db_cfg.get("dbname", ""),
            "tables": tables,
        }
        total += len(tables)

    from datetime import datetime
    snapshot["snapshot_at"] = datetime.now().isoformat(timespec="seconds")

    with open(DATA / "metadata_snapshot.json", "w") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    print(f"  metadata_snapshot.json — {total} tables across {len(dbs)} DBs")


# ── Main ────────────────────────────────────────────────────────

def main():
    os.makedirs(SAMPLE, exist_ok=True)
    os.makedirs(DATA, exist_ok=True)

    print("\n=== D-BiCron Dev Seed ===\n")

    print("[1/5] Creating sample SQLite databases...")
    create_users_db()
    create_shop_db()
    create_analytics_db()

    print("\n[2/5] Registering databases...")
    write_databases_json()

    print("\n[3/5] Writing pipeline config...")
    write_pipeline_config()

    print("\n[4/5] Registering sample cron schedules...")
    write_sample_schedules()

    print("\n[5/5] Collecting metadata snapshot...")
    run_metadata_snapshot()

    print("\n=== Done! Run 'npm start' to launch WebUI ===\n")


if __name__ == "__main__":
    main()
