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
    # worktree 구조: ...\상품가공프로그램\.worktrees\feat-pricing\
    # 실제 DB:       ...\상품가공프로그램\DB_save\products.db
    # worktree → parent(.worktrees) → parent(상품가공프로그램) → DB_save
    worktree_root = Path(__file__).resolve().parent.parent.parent  # hub/services/ → hub/ → feat-pricing/
    candidate = worktree_root.parent.parent / "DB_save" / "products.db"
    if candidate.exists():
        return str(candidate)
    # 폴백: oc_import 기본값
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
    ("vendor_code",        "TEXT"),  # import_new_from_vendor 수집 시 저장
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

_VENDORS_DDL = """
CREATE TABLE IF NOT EXISTS vendors (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_code       TEXT UNIQUE NOT NULL,
    vendor_name       TEXT,
    source            TEXT DEFAULT 'oc',
    product_count     INTEGER,
    category          TEXT,
    oc_link           TEXT,
    status            TEXT DEFAULT 'pending',
    processed_count   INTEGER DEFAULT 0,
    last_imported_at  TEXT,
    created_at        TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at        TEXT DEFAULT (datetime('now', 'localtime'))
)
"""

_STORE_CATEGORY_ASSIGNMENTS_DDL = """
CREATE TABLE IF NOT EXISTS store_category_assignments (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    store_alias      TEXT NOT NULL,
    oc_category_key  TEXT NOT NULL,
    oc_category_name TEXT,
    market_group     TEXT NOT NULL,
    assigned_at      TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(store_alias, oc_category_key)
);
"""

_MARKET_REGISTRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS market_registrations (
    상품코드          TEXT NOT NULL,
    store_alias       TEXT NOT NULL,
    market            TEXT NOT NULL,
    market_product_id TEXT,
    oc_category_key   TEXT,
    market_cat_code   TEXT,
    sell_price        INTEGER,
    strategy          TEXT,
    pipeline_run_id   TEXT,
    status            TEXT DEFAULT 'READY',
    created_at        TEXT DEFAULT (datetime('now', 'localtime')),
    confirmed_at      TEXT,
    PRIMARY KEY(상품코드, store_alias, created_at)
);
"""


def run_migrations(db_path: str | None = None) -> None:
    """products에 hub 컬럼 추가 + stores 테이블 생성 (멱등).

    products 테이블이 아직 없으면 컬럼 추가를 건너뜀.
    (products 테이블은 oc_import.py가 최초 실행 시 생성)
    """
    with _conn(db_path) as con:
        cur = con.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='products'"
        )
        if cur.fetchone():
            cur.execute("PRAGMA table_info(products)")
            existing = {row["name"] for row in cur.fetchall()}
            for col, col_type in _HUB_COLUMNS:
                if col not in existing:
                    con.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
        con.execute(_STORES_DDL)
        con.execute(_VENDORS_DDL)
        con.execute(_STORE_CATEGORY_ASSIGNMENTS_DDL)
        con.execute(_MARKET_REGISTRATIONS_DDL)
        # vendors.source 컬럼 마이그레이션 (기존 DB 대응)
        cur.execute("PRAGMA table_info(vendors)")
        vcols = {r["name"] for r in cur.fetchall()}
        if "source" not in vcols:
            con.execute("ALTER TABLE vendors ADD COLUMN source TEXT DEFAULT 'oc'")


# ── 대시보드 ──────────────────────────────────────────────────────────────────

def get_dashboard_stats() -> dict[str, Any]:
    with _conn() as con:
        cur = con.cursor()

        cur.execute("SELECT COUNT(*) FROM products")
        total_all = cur.fetchone()[0]

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

        cur.execute("SELECT MAX(oc_synced_at) FROM products")
        last_sync_at = cur.fetchone()[0] or ""

    return {
        "total_all":    total_all,
        "total_active": total_active,
        "shippable":    shippable,
        "processed":    processed,
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
            if isinstance(name_data, dict):
                name = name_data.get("name", "")
            elif isinstance(name_data, list) and name_data:
                name = name_data[0]
            else:
                name = str(name_data)
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


def parse_stores_from_excel(file_bytes: bytes) -> list[dict]:
    """
    Market_id_pw.xlsx 바이트 → stores 목록 파싱.

    '별칭', '마켓', '그룹' 헤더가 있는 시트를 찾아 파싱.
    헤더가 없는 시트는 무시.
    """
    import io
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    records = []
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        header = [str(h).strip() if h else "" for h in rows[0]]
        try:
            alias_col = header.index("별칭")
        except ValueError:
            continue
        market_col = header.index("마켓") if "마켓" in header else None
        group_col  = header.index("그룹") if "그룹" in header else None

        for row in rows[1:]:
            alias = str(row[alias_col]).strip() if row[alias_col] else ""
            if not alias or alias == "None":
                continue
            market = str(row[market_col]).strip() if market_col is not None and row[market_col] else ""
            group  = str(row[group_col]).strip()  if group_col  is not None and row[group_col]  else ""
            records.append({"alias": alias, "market": market, "group_id": group})
    return records


# ── 공급사 관리 ───────────────────────────────────────────────────────────────

def get_vendors(db_path: str | None = None) -> list[dict[str, Any]]:
    with _conn(db_path) as con:
        cur = con.cursor()
        cur.execute(
            """SELECT v.id, v.vendor_code, v.vendor_name, v.source, v.product_count, v.category,
                      v.oc_link, v.status, v.processed_count, v.last_imported_at,
                      v.created_at, v.updated_at,
                      COUNT(CASE WHEN p.product_status = 'ACTIVE'
                                  AND (p.text_status IS NULL OR p.text_status != 'done')
                             THEN 1 END) AS unprocessed_count,
                      COUNT(CASE WHEN p.product_status = 'ACTIVE' THEN 1 END) AS active_count
               FROM vendors v
               LEFT JOIN products p ON p.vendor_code = v.vendor_code
               GROUP BY v.id
               ORDER BY v.vendor_name"""
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_vendor(
    vendor_code: str,
    vendor_name: str = "",
    source: str = "oc",
    product_count: int | None = None,
    category: str = "",
    oc_link: str = "",
    status: str | None = None,
    processed_count: int | None = None,
    db_path: str | None = None,
) -> None:
    with _conn(db_path) as con:
        con.execute(
            """INSERT INTO vendors (vendor_code, vendor_name, source, product_count, category,
                                   oc_link, status, processed_count)
               VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, 'pending'), COALESCE(?, 0))
               ON CONFLICT(vendor_code) DO UPDATE SET
                 vendor_name     = COALESCE(NULLIF(excluded.vendor_name, ''), vendor_name),
                 product_count   = COALESCE(excluded.product_count, product_count),
                 category        = COALESCE(NULLIF(excluded.category, ''), category),
                 oc_link         = COALESCE(NULLIF(excluded.oc_link, ''), oc_link),
                 status          = COALESCE(excluded.status, status),
                 processed_count = CASE WHEN excluded.processed_count > 0
                                        THEN excluded.processed_count
                                        ELSE processed_count END,
                 updated_at      = datetime('now', 'localtime')""",
            (vendor_code, vendor_name, source, product_count, category, oc_link, status, processed_count),
        )


def sync_vendors_from_products(db_path: str | None = None) -> dict:
    """products 테이블의 distinct vendor_code를 vendors 테이블에 일괄 upsert.

    vendor_name이 없는 코드는 공란으로 등록 (나중에 엑셀/수동으로 보완).
    Returns: {"inserted": N, "already": M}
    """
    with _conn(db_path) as con:
        cur = con.cursor()
        cur.execute(
            "SELECT DISTINCT vendor_code FROM products "
            "WHERE vendor_code IS NOT NULL AND vendor_code != ''"
        )
        codes = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT vendor_code FROM vendors")
        existing = {r[0] for r in cur.fetchall()}

        inserted = 0
        for code in codes:
            if code not in existing:
                con.execute(
                    "INSERT INTO vendors (vendor_code, source) VALUES (?, 'oc')",
                    (str(code),),
                )
                inserted += 1

    return {"inserted": inserted, "already": len(existing)}


def mark_vendor_imported(vendor_code: str, db_path: str | None = None) -> None:
    """신규 상품 수집 완료 시 last_imported_at 갱신."""
    with _conn(db_path) as con:
        con.execute(
            "UPDATE vendors SET last_imported_at = datetime('now', 'localtime'), "
            "updated_at = datetime('now', 'localtime') WHERE vendor_code = ?",
            (vendor_code,),
        )


def parse_vendors_from_excel(file_bytes: bytes) -> list[dict]:
    """오너클랜 공급사 목록 Excel → vendors 목록 파싱.

    컬럼 구조: 공급사명, 공급사코드, 상품수(텍스트), 상품수(숫자), 카테고리명, OC링크
    - 빨간 텍스트 행 = 가공완료 (status='processed')
    - 공급사명에 "(N회)" = 가공 회차 (processed_count=N)
    - 공급사코드 없는 행 건너뜀
    """
    import io
    import re
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    records = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        all_rows = list(ws.iter_rows())  # Cell 객체 (폰트 색상 접근용)
        if not all_rows:
            continue

        header = [str(cell.value).strip() if cell.value else "" for cell in all_rows[0]]

        # 공급사코드 컬럼 필수
        code_col = None
        for keyword in ("공급사코드", "vendor", "코드"):
            for i, h in enumerate(header):
                if keyword in h.replace(" ", ""):
                    code_col = i
                    break
            if code_col is not None:
                break
        if code_col is None:
            continue

        name_col = next((i for i, h in enumerate(header) if "공급사명" in h), None)
        cat_col  = next((i for i, h in enumerate(header) if "카테고리" in h), None)
        link_col = next((i for i, h in enumerate(header) if "링크" in h or "link" in h.lower()), None)
        # 상품수(숫자) 우선, 없으면 상품수 아무거나
        cnt_col  = next((i for i, h in enumerate(header) if "상품수" in h and "숫자" in h), None)
        if cnt_col is None:
            cnt_col = next((i for i, h in enumerate(header) if "상품수" in h), None)

        for row in all_rows[1:]:
            code_cell = row[code_col]
            code = str(code_cell.value).strip() if code_cell.value else ""
            if not code or code in ("None", "공급사코드"):
                continue

            # 빨간 텍스트 = 가공완료
            is_processed = False
            name_cell = row[name_col] if name_col is not None else None
            if name_cell and name_cell.font and name_cell.font.color:
                fc = name_cell.font.color
                rgb = ""
                if fc.type == "rgb" and fc.rgb:
                    rgb = str(fc.rgb).upper()
                elif fc.type == "theme":
                    pass  # theme color — 판단 불가, 건너뜀
                # FFFF0000(순수빨강) 외 FF로 시작하는 빨간 계열 포함
                if rgb and rgb not in ("FF000000", "00000000", "") and rgb[2:4] == "FF" and rgb[4:6] <= "44" and rgb[6:] <= "44":
                    is_processed = True

            raw_name = str(name_cell.value).strip() if name_cell and name_cell.value else ""

            # "(N급)" = OC 등급 숫자 표기 — 공급사명에서 제거, oc_grade_num에 저장
            oc_grade_num: int | None = None
            m_grade = re.search(r"\((\d+)급\)", raw_name)
            if m_grade:
                oc_grade_num = int(m_grade.group(1))
            clean_name = re.sub(r"\s*\(\d+급\)", "", raw_name).strip()
            processed_count = 0

            cat  = str(row[cat_col].value).strip()  if cat_col  is not None and row[cat_col].value  else ""
            link = str(row[link_col].value).strip() if link_col is not None and row[link_col].value else ""
            cnt: int | None = None
            if cnt_col is not None and row[cnt_col].value is not None:
                try:
                    cnt = int(row[cnt_col].value)
                except (ValueError, TypeError):
                    cnt = None

            records.append({
                "vendor_code":     code,
                "vendor_name":     clean_name,
                "product_count":   cnt,
                "category":        cat,
                "oc_link":         link,
                "status":          "processed" if is_processed else "pending",
                "processed_count": processed_count,
                "oc_grade_num":    oc_grade_num,  # 참고용 (저장 제외)
            })

    return records
