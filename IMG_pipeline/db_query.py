"""대상 상품 조회 (ST4 완료 + 누끼 있음 + OC available)."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterator


HERE = Path(__file__).resolve().parent
WORKTREE_ROOT = HERE.parent
PROJECT_ROOT = WORKTREE_ROOT.parent.parent.parent
DB_PATH = PROJECT_ROOT / "DB_save" / "products.db"


_COLS_CACHE: dict[str, str] = {}


def _resolve_columns(db_path: Path = DB_PATH) -> dict[str, str]:
    """런타임에 실제 컬럼명 조회 (CP949 인코딩 대응)."""
    if _COLS_CACHE:
        return _COLS_CACHE
    con = sqlite3.connect(str(db_path))
    cols = [r[1] for r in con.execute("PRAGMA table_info(products)").fetchall()]
    con.close()
    _COLS_CACHE["code"] = next(c for c in cols if "상품코드" in c)
    _COLS_CACHE["nukki"] = [c for c in cols if c.endswith("url")][0]
    _COLS_CACHE["st4"] = next(c for c in cols if "ST4" in c)
    _COLS_CACHE["category"] = next((c for c in cols if c.endswith("명") and "상품" not in c), "")
    _COLS_CACHE["name"] = next((c for c in cols if "상품명" in c and "ST4" not in c), "")
    return _COLS_CACHE


def fetch_targets(
    db_path: Path = DB_PATH,
    limit: int | None = None,
    category_contains: str | None = None,
    exclude_codes: set[str] | None = None,
    exclude_done: bool = False,
) -> list[dict]:
    """대상 상품 목록 반환.

    조건: ST4 있음 + 누끼 있음 + oc_status='available' + ST2_JSON 있음
    exclude_done=True 시 image_slots 이미 있는 건 SQL에서 제외.
    """
    cols = _resolve_columns(db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    where = [
        f'"{cols["st4"]}" IS NOT NULL AND "{cols["st4"]}" != \'\'',
        f'"{cols["nukki"]}" IS NOT NULL AND "{cols["nukki"]}" != \'\'',
        f'oc_status = \'available\'',
        f'ST2_JSON IS NOT NULL AND ST2_JSON != \'\'',
    ]
    if exclude_done:
        where.append("(image_slots IS NULL OR image_slots = '' OR image_slots = '{}')")
    params: list = []
    if category_contains:
        where.append(f'"{cols["category"]}" LIKE ?')
        params.append(f"%{category_contains}%")

    sel = [
        f'"{cols["code"]}" AS code',
        f'"{cols["st4"]}" AS name',
        f'"{cols["nukki"]}" AS nukki_url',
        f'"{cols["category"]}" AS category' if cols["category"] else "'' AS category",
        "oc_status",
        "oc_price",
        "vendor_code",
        "ST2_JSON",
    ]
    q = f"SELECT {', '.join(sel)} FROM products WHERE {' AND '.join(where)}"
    if limit:
        q += f" LIMIT {limit}"

    rows = con.execute(q, params).fetchall()
    con.close()

    out = []
    for r in rows:
        code = r["code"]
        if exclude_codes and code in exclude_codes:
            continue
        try:
            st2 = json.loads(r["ST2_JSON"])
        except Exception:
            continue
        out.append({
            "code": code,
            "name": r["name"],
            "nukki_url": r["nukki_url"],
            "category": r["category"],
            "oc_status": r["oc_status"],
            "oc_price": r["oc_price"],
            "vendor_code": r["vendor_code"],
            "st2": st2,
        })
    return out


def count_targets(db_path: Path = DB_PATH, category_contains: str | None = None) -> int:
    cols = _resolve_columns(db_path)
    con = sqlite3.connect(str(db_path))
    where = [
        f'"{cols["st4"]}" IS NOT NULL AND "{cols["st4"]}" != \'\'',
        f'"{cols["nukki"]}" IS NOT NULL AND "{cols["nukki"]}" != \'\'',
        f'oc_status = \'available\'',
        f'ST2_JSON IS NOT NULL AND ST2_JSON != \'\'',
    ]
    params: list = []
    if category_contains:
        where.append(f'"{cols["category"]}" LIKE ?')
        params.append(f"%{category_contains}%")
    q = f"SELECT COUNT(*) FROM products WHERE {' AND '.join(where)}"
    n = con.execute(q, params).fetchone()[0]
    con.close()
    return n


if __name__ == "__main__":
    import sys
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    n = count_targets()
    print(f"Total targets: {n:,}")
    sample = fetch_targets(limit=5)
    for s in sample:
        print(f"  {s['code']} | {s['category']} | {s['name'][:50]}")
