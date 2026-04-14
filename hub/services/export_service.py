"""Export 서비스 — dry-run 및 실제 출고."""
from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path
from typing import Any

from hub.services.db_service import _conn


def _export_path(file_id: str) -> Path:
    """파일 ID로 export 파일 경로를 계산한다."""
    tmp_dir = Path(tempfile.gettempdir()) / "product_hub_exports"
    tmp_dir.mkdir(exist_ok=True)
    return tmp_dir / f"{file_id}.xlsx"


def _get_strategy_policy(market: str, strategy_id: str) -> dict:
    """pricing_strategies.py에서 정책 로드."""
    try:
        from DB_save.pricing_strategies import get_pricing_strategy
        return get_pricing_strategy(market, strategy_id)
    except Exception:
        # 폴백: 기본 마진 정책
        return {
            "commission_rate": 0.07,
            "discount_rate": 0.0,
            "coupon_amount": 0,
            "reward_rate": 0.02,
        }


def _calc_sell_price(oc_price: int, policy: dict) -> int:
    """oc_price → 판매가 역산. price_engine.batch_solve 사용."""
    try:
        from DB_save.price_engine import batch_solve
        results = batch_solve(
            products=[{"cost": oc_price}],
            policy=policy,
            target_bands=[(0, 0)],
        )
        if results and results[0].get("sell_price"):
            return int(results[0]["sell_price"])
    except Exception:
        pass
    # 폴백: 원가 × 1.3
    return int(oc_price * 1.3)


def dry_run(
    categories: list[str],
    stores: list[str],
    strategy: str,
) -> dict[str, Any]:
    """출고 대상 상품 목록 + 계산된 판매가 반환. categories=[] → 전체."""
    with _conn() as con:
        cur = con.cursor()
        if categories:
            placeholders = ",".join("?" * len(categories))
            cur.execute(
                f"SELECT 상품코드, product_names_json, 카테고리명, oc_price "
                f"FROM products "
                f"WHERE product_status = 'ACTIVE' AND oc_price IS NOT NULL AND oc_price > 0 "
                f"AND 카테고리명 IN ({placeholders}) "
                f"ORDER BY 상품코드",
                categories,
            )
        else:
            cur.execute(
                "SELECT 상품코드, product_names_json, 카테고리명, oc_price "
                "FROM products "
                "WHERE product_status = 'ACTIVE' AND oc_price IS NOT NULL AND oc_price > 0 "
                "ORDER BY 상품코드"
            )
        rows = cur.fetchall()

    # 첫 번째 스토어의 마켓으로 정책 결정
    market = "고도몰"
    if stores:
        for mkt in ("고도몰", "스마트스토어", "옥션", "지마켓", "11번가", "쿠팡"):
            if any(s.startswith(mkt) for s in stores):
                market = mkt
                break

    policy = _get_strategy_policy(market, strategy)
    items = []
    for row in rows:
        oc_price = row["oc_price"]
        name = ""
        try:
            nd = json.loads(row["product_names_json"] or "{}")
            name = nd.get("name", "") if isinstance(nd, dict) else (nd[0] if isinstance(nd, list) and nd else str(nd))
        except Exception:
            name = row["product_names_json"] or ""

        sell_price = _calc_sell_price(oc_price, policy)
        items.append({
            "상품코드": row["상품코드"],
            "상품명": name,
            "카테고리명": row["카테고리명"] or "",
            "oc_price": oc_price,
            "sell_price": sell_price,
        })

    return {"items": items, "total": len(items), "stores": stores, "strategy": strategy}


def run_export(
    categories: list[str],
    stores: list[str],
    strategy: str,
    fmt: str = "godomall",
) -> dict[str, Any]:
    """실제 출고 파일 생성 + file_id 반환."""
    result = dry_run(categories, stores, strategy)
    if not result["items"]:
        return {"file_id": None, "file_url": None, "product_count": 0}

    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "출고목록"
        ws.append(["상품코드", "상품명", "카테고리", "원가", "판매가", "전략", "스토어"])
        for item in result["items"]:
            for store in stores:
                ws.append([
                    item["상품코드"], item["상품명"], item["카테고리명"],
                    item["oc_price"], item["sell_price"], strategy, store,
                ])

        file_id = uuid.uuid4().hex
        out_path = _export_path(file_id)
        wb.save(str(out_path))

        return {
            "file_id": file_id,
            "file_url": f"/api/export/download/{file_id}",
            "product_count": len(result["items"]),
        }
    except Exception as e:
        return {"error": str(e), "file_id": None, "file_url": None, "product_count": 0}


def get_export_file(file_id: str) -> Path | None:
    """파일 ID로 export 파일을 조회한다. 파일이 없으면 None 반환."""
    p = _export_path(file_id)
    return p if p.exists() else None
