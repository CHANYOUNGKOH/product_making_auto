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


import json
import uuid
import tempfile
from pathlib import Path


def get_pipeline_preview(store_alias: str, strategy: str) -> dict:
    """출고 예정 상품 미리보기. store_alias 배정 카테고리 기준 필터."""
    assignments = get_assignments(store_alias)
    cat_names = [a["oc_category_name"] for a in assignments]
    if not cat_names:
        return {"items": [], "total": 0, "store_alias": store_alias,
                "categories": [], "strategy": strategy}

    placeholders = ",".join("?" * len(cat_names))
    with _conn() as con:
        rows = con.execute(
            f"""SELECT 상품코드, product_names_json, 카테고리명, oc_price
                FROM products
                WHERE product_status='ACTIVE' AND oc_price > 0
                AND 카테고리명 IN ({placeholders})
                ORDER BY 카테고리명, 상품코드""",
            cat_names,
        ).fetchall()

    items = []
    for r in rows:
        name = ""
        try:
            nd = json.loads(r["product_names_json"] or "{}")
            name = nd.get("name", "") if isinstance(nd, dict) else (nd[0] if isinstance(nd, list) and nd else str(nd))
        except Exception:
            name = r["product_names_json"] or ""
        items.append({
            "상품코드": r["상품코드"],
            "상품명": name,
            "카테고리명": r["카테고리명"],
            "oc_price": r["oc_price"],
        })

    return {
        "items": items[:50],  # 미리보기 50개
        "total": len(items),
        "store_alias": store_alias,
        "categories": cat_names,
        "strategy": strategy,
    }


# 생성된 Excel 파일 경로 캐시 (프로세스 내 메모리)
_export_files: dict[str, str] = {}


def get_export_file(file_id: str) -> str | None:
    return _export_files.get(file_id)


def _get_store(store_alias: str) -> dict | None:
    with _conn() as con:
        row = con.execute("SELECT * FROM stores WHERE alias=?", [store_alias]).fetchone()
    return dict(row) if row else None


def get_db_path_fn() -> str:
    from hub.services.db_service import get_db_path
    return get_db_path()


def _get_export_counts(codes: list[str]) -> dict[str, int]:
    """상품별 기존 출고 횟수 조회 (상품명/이미지 순환 인덱스용)."""
    if not codes:
        return {}
    with _conn() as con:
        placeholders = ",".join("?" * len(codes))
        rows = con.execute(
            f"SELECT 상품코드, COUNT(*) as cnt FROM market_registrations "
            f"WHERE 상품코드 IN ({placeholders}) GROUP BY 상품코드",
            codes,
        ).fetchall()
    return {r["상품코드"]: r["cnt"] for r in rows}


def _record_ready(codes: list[str], store_alias: str, market: str,
                  strategy: str, pipeline_run_id: str, assignments: list[dict]) -> None:
    """상품 목록을 market_registrations에 READY로 기록."""
    cat_key_map = {a["oc_category_name"]: a["oc_category_key"] for a in assignments}
    with _conn() as con:
        for p_code in codes:
            row = con.execute(
                "SELECT 카테고리명 FROM products WHERE 상품코드=?", [p_code]
            ).fetchone()
            cat_name = row["카테고리명"] if row else ""
            oc_cat_key = cat_key_map.get(cat_name, "")
            con.execute(
                """INSERT OR IGNORE INTO market_registrations
                   (상품코드, store_alias, market, oc_category_key, strategy, pipeline_run_id, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'READY')""",
                [p_code, store_alias, market, oc_cat_key, strategy, pipeline_run_id],
            )


def run_godomall_pipeline(store_alias: str, strategy: str) -> dict:
    """고도몰 Excel 생성 → market_registrations READY 기록."""
    import sys
    _ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(_ROOT / "OC_ES_converter" / "scripts"))

    from godomall_register.pipeline_run import fetch_export_products, build_oc_dataframe
    from convert_godomall import convert_ownerclan_to_godomall, save_godomall, resolve_godomall_template_path

    assignments = get_assignments(store_alias)
    cat_names = [a["oc_category_name"] for a in assignments]
    if not cat_names:
        return {"error": "배정된 카테고리 없음", "product_count": 0}

    db_path = Path(get_db_path_fn())
    all_products = fetch_export_products(db_path)
    products = [p for p in all_products if p.get("카테고리명") in cat_names]
    if not products:
        return {"error": "출고 대상 없음", "product_count": 0}

    # 출고 카운터: 상품별 기존 등록 횟수 조회 (상품명/이미지 순환용)
    codes = [p["상품코드"] for p in products]
    export_counts = _get_export_counts(codes)

    oc_df = build_oc_dataframe(products, export_counts=export_counts)
    godomall_df, _ = convert_ownerclan_to_godomall(oc_df, strategy_id=strategy)

    run_id = uuid.uuid4().hex[:8]
    tmp_dir = Path(tempfile.gettempdir()) / "product_hub_exports"
    tmp_dir.mkdir(exist_ok=True)
    out_path = tmp_dir / f"고도몰_{store_alias}_{run_id}.xlsx"

    template = resolve_godomall_template_path()
    save_godomall(godomall_df, template, str(out_path))

    _record_ready(
        codes=[p["상품코드"] for p in products],
        store_alias=store_alias,
        market="고도몰",
        strategy=strategy,
        pipeline_run_id=run_id,
        assignments=assignments,
    )

    file_id = f"godomall_{run_id}"
    _export_files[file_id] = str(out_path)

    return {
        "file_url": f"/api/register/download/{file_id}",
        "product_count": len(products),
        "pipeline_run_id": run_id,
    }


def run_esellers_pipeline(store_alias: str, strategy: str) -> dict:
    """이셀러스 Excel 생성 → market_registrations READY 기록."""
    import sys
    _ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(_ROOT / "OC_ES_converter" / "scripts"))

    from godomall_register.pipeline_run import fetch_export_products, build_oc_dataframe
    from convert_base import convert_ownerclan_to_esellers

    assignments = get_assignments(store_alias)
    cat_names = [a["oc_category_name"] for a in assignments]
    if not cat_names:
        return {"error": "배정된 카테고리 없음", "product_count": 0}

    db_path = Path(get_db_path_fn())
    all_products = fetch_export_products(db_path)
    products = [p for p in all_products if p.get("카테고리명") in cat_names]
    if not products:
        return {"error": "출고 대상 없음", "product_count": 0}

    codes = [p["상품코드"] for p in products]
    export_counts = _get_export_counts(codes)

    oc_df = build_oc_dataframe(products, export_counts=export_counts)
    esellers_df, errors_df = convert_ownerclan_to_esellers(oc_df)

    run_id = uuid.uuid4().hex[:8]
    tmp_dir = Path(tempfile.gettempdir()) / "product_hub_exports"
    tmp_dir.mkdir(exist_ok=True)
    out_path = tmp_dir / f"이셀러스_{store_alias}_{run_id}.xlsx"
    esellers_df.to_excel(str(out_path), index=False)

    store_row = _get_store(store_alias)
    market = store_row["market"] if store_row else "이셀러스"
    _record_ready(
        codes=[p["상품코드"] for p in products],
        store_alias=store_alias,
        market=market,
        strategy=strategy,
        pipeline_run_id=run_id,
        assignments=assignments,
    )

    file_id = f"esellers_{run_id}"
    _export_files[file_id] = str(out_path)

    return {
        "file_url": f"/api/register/download/{file_id}",
        "product_count": len(products),
        "pipeline_run_id": run_id,
        "error_count": len(errors_df) if errors_df is not None else 0,
    }
