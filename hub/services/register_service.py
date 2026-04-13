# hub/services/register_service.py
"""카테고리-스토어 배정 서비스."""
from __future__ import annotations

from hub.services.db_service import _conn

# 마켓 → 마켓그룹 매핑
MARKET_TO_GROUP: dict[str, str] = {
    "고도몰":      "naver",
    "스마트스토어": "naver",
    "옥션":        "esm",
    "지마켓":      "esm",
    "11번가":      "st11",
    "쿠팡":        "coupang",
    "인터파크":    "interpark",
    "티몬":        "tmon",
    "위메프":      "wmp",
}


def get_store_market_group(store_alias: str) -> str | None:
    """stores 테이블에서 store_alias의 market 조회 → market_group 반환."""
    with _conn() as con:
        row = con.execute(
            "SELECT market FROM stores WHERE alias = ?", [store_alias]
        ).fetchone()
    if not row:
        return None
    return MARKET_TO_GROUP.get(row["market"], row["market"])


def get_assignments(store_alias: str) -> list[dict]:
    """특정 스토어의 카테고리 배정 목록."""
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM store_category_assignments WHERE store_alias = ? ORDER BY oc_category_name",
            [store_alias],
        ).fetchall()
    return [dict(r) for r in rows]


def add_assignment(store_alias: str, oc_category_name: str) -> dict:
    """카테고리 배정 추가. oc_category_name(fullName 텍스트)을 key로 사용.

    같은 market_group 내 중복 시 conflict_stores 반환.
    """
    market_group = get_store_market_group(store_alias)
    if not market_group:
        raise ValueError(f"스토어 없음: {store_alias}")

    # 중복 체크: 같은 market_group 내 다른 스토어에 이미 배정?
    with _conn() as con:
        conflicts = con.execute(
            """SELECT store_alias FROM store_category_assignments
               WHERE oc_category_name = ? AND market_group = ? AND store_alias != ?""",
            [oc_category_name, market_group, store_alias],
        ).fetchall()
        conflict_stores = [r["store_alias"] for r in conflicts]

        con.execute(
            """INSERT OR REPLACE INTO store_category_assignments
               (store_alias, oc_category_key, oc_category_name, market_group)
               VALUES (?, ?, ?, ?)""",
            [store_alias, oc_category_name, oc_category_name, market_group],
            # oc_category_key = oc_category_name as fallback
        )
    return {"store_alias": store_alias, "oc_category_name": oc_category_name,
            "conflict_stores": conflict_stores}


def remove_assignment(store_alias: str, oc_category_name: str) -> dict:
    """카테고리 배정 제거. oc_category_name 기준."""
    with _conn() as con:
        con.execute(
            "DELETE FROM store_category_assignments WHERE store_alias=? AND oc_category_name=?",
            [store_alias, oc_category_name],
        )
    return {"deleted": True}


def get_oc_categories_with_counts() -> list[dict]:
    """OC 카테고리 목록 + 상품 수 (ACTIVE + oc_price > 0 기준)."""
    with _conn() as con:
        rows = con.execute(
            """SELECT 카테고리명, COUNT(*) as cnt
               FROM products
               WHERE product_status='ACTIVE' AND oc_price > 0
               GROUP BY 카테고리명
               ORDER BY 카테고리명"""
        ).fetchall()
    return [
        {"oc_category_name": r["카테고리명"], "product_count": r["cnt"]}
        for r in rows
    ]
