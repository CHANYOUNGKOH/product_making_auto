"""SQLite 쿼리 레이어 — Product Hub 전용.

DB 경로: 환경변수 HUB_DB_PATH 우선, 없으면 oc_import.DEFAULT_DB_PATH 사용.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any


# ── DB 경로 ──────────────────────────────────────────────────────────────────

def _default_db_path() -> str:
    from godomall_register.oc_import import DEFAULT_DB_PATH
    return str(DEFAULT_DB_PATH)


def get_db_path() -> str:
    return os.environ.get("HUB_DB_PATH") or _default_db_path()


@contextmanager
def _conn(db_path: str | None = None):
    path = db_path or get_db_path()
    con = sqlite3.connect(path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


# ── 마이그레이션 ──────────────────────────────────────────────────────────────

_HUB_COLUMNS = [
    ("text_status",        "TEXT"),
    ("image_status",       "TEXT"),
    ("export_log",         "TEXT DEFAULT '[]'"),
    ("registered_stores",  "TEXT DEFAULT '[]'"),
]

_STORES_DDL = """
CREATE TABLE IF NOT EXISTS stores (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alias       TEXT UNIQUE NOT NULL,
    market      TEXT,
    group_id    TEXT,
    login_id    TEXT,
    active      INTEGER DEFAULT 1,
    strategy    TEXT,
    slot_count  INTEGER,
    created_at  TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at  TEXT DEFAULT (datetime('now', 'localtime'))
)
"""


def run_migrations(db_path: str | None = None) -> None:
    """products에 hub 컬럼 추가 + stores 테이블 생성 (멱등)."""
    with _conn(db_path) as con:
        cur = con.cursor()
        cur.execute("PRAGMA table_info(products)")
        existing = {row["name"] for row in cur.fetchall()}
        for col, col_type in _HUB_COLUMNS:
            if col not in existing:
                con.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
        con.execute(_STORES_DDL)


# ── 대시보드 ──────────────────────────────────────────────────────────────────

def get_dashboard_stats() -> dict[str, Any]:
    with _conn() as con:
        cur = con.cursor()

        cur.execute(
            "SELECT COUNT(*) FROM products WHERE product_status = 'ACTIVE'"
        )
        total_active = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM products "
            "WHERE product_status = 'ACTIVE' AND oc_price IS NOT NULL AND oc_price > 0"
        )
        shippable = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM products "
            "WHERE product_status = 'ACTIVE' "
            "AND text_status = 'done' AND image_status = 'done'"
        )
        processed = cur.fetchone()[0]

        cur.execute(
            "SELECT MAX(oc_synced_at) FROM products WHERE product_status = 'ACTIVE'"
        )
        last_sync_at = cur.fetchone()[0] or ""

    return {
        "total_active": total_active,
        "shippable": shippable,
        "processed": processed,
        "last_sync_at": last_sync_at,
    }


# ── 상품 DB ───────────────────────────────────────────────────────────────────

def get_products(
    q: str = "",
    category: str = "",
    quick_filter: str = "all",
    page: int = 1,
    per_page: int = 100,
) -> dict[str, Any]:
    """상품 목록 조회 (검색/필터/페이지네이션)."""
    clauses = ["product_status = 'ACTIVE'"]
    params: list[Any] = []

    if q:
        clauses.append("(상품코드 LIKE ? OR product_names_json LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]

    if category:
        clauses.append("카테고리명 = ?")
        params.append(category)

    # quick_filter 처리
    if quick_filter == "has_oc_price":
        clauses.append("oc_price IS NOT NULL AND oc_price > 0")
    elif quick_filter == "no_market":
        clauses.append("(export_log IS NULL OR export_log = '[]')")
    elif quick_filter not in ("all", "partial_market", ""):
        raise ValueError(f"Unknown quick_filter: {quick_filter!r}")
    # "all" and "partial_market" add no clause (partial_market not yet implemented)

    where = " AND ".join(clauses)
    offset = (page - 1) * per_page

    with _conn() as con:
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM products WHERE {where}", params)
        total = cur.fetchone()[0]

        cur.execute(
            f"""SELECT 상품코드, product_names_json, 카테고리명,
                       oc_price, text_status, image_status,
                       export_log, registered_stores, oc_synced_at
                FROM products WHERE {where}
                ORDER BY 상품코드
                LIMIT ? OFFSET ?""",
            params + [per_page, offset],
        )
        rows = cur.fetchall()

    items = []
    for row in rows:
        name = ""
        try:
            name_data = json.loads(row["product_names_json"] or "{}")
            name = name_data.get("name", "")
        except (json.JSONDecodeError, TypeError):
            name = row["product_names_json"] or ""

        items.append({
            "상품코드": row["상품코드"],
            "상품명": name,
            "카테고리명": row["카테고리명"] or "",
            "oc_price": row["oc_price"],
            "text_status": row["text_status"] or "todo",
            "image_status": row["image_status"] or "todo",
            "export_log": json.loads(row["export_log"] or "[]"),
            "registered_stores": json.loads(row["registered_stores"] or "[]"),
            "oc_synced_at": row["oc_synced_at"] or "",
        })

    return {"items": items, "total": total, "page": page, "per_page": per_page}


def get_categories() -> list[str]:
    with _conn() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT DISTINCT 카테고리명 FROM products "
            "WHERE product_status = 'ACTIVE' AND 카테고리명 IS NOT NULL AND 카테고리명 != '' "
            "ORDER BY 카테고리명"
        )
        return [row[0] for row in cur.fetchall()]


# ── 스토어 관리 ───────────────────────────────────────────────────────────────

def get_stores(active_only: bool = False) -> list[dict[str, Any]]:
    with _conn() as con:
        cur = con.cursor()
        if active_only:
            cur.execute(
                "SELECT id, alias, market, group_id, login_id, active, strategy, "
                "slot_count, created_at, updated_at FROM stores WHERE active = 1 ORDER BY alias"
            )
        else:
            cur.execute(
                "SELECT id, alias, market, group_id, login_id, active, strategy, "
                "slot_count, created_at, updated_at FROM stores ORDER BY alias"
            )
        return [dict(row) for row in cur.fetchall()]


def upsert_store(
    alias: str,
    market: str,
    group_id: str,
    login_id: str = "",
    strategy: str | None = None,
) -> None:
    with _conn() as con:
        con.execute(
            """INSERT INTO stores (alias, market, group_id, login_id, strategy)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(alias) DO UPDATE SET
                 market = excluded.market,
                 group_id = excluded.group_id,
                 login_id = excluded.login_id,
                 strategy = COALESCE(excluded.strategy, strategy),
                 updated_at = datetime('now', 'localtime')""",
            (alias, market, group_id, login_id, strategy),
        )
