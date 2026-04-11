"""Ownerclan API -> products.db 동기화 (확장판).

역할:
  1. [갱신] sync_existing() - DB 상품코드 기준 oc_* 전체 갱신
           oc_price, oc_options_json, oc_content, oc_images_json 등 포함
  2. [신규] import_new_from_vendor() - 공급사(vendor) 신규 상품 INSERT
           공급사 관리는 향후 UI에서 처리

기존 ownerclan_sync.py 와 차이:
  - oc_options_json, oc_content, oc_images_json, oc_search_keywords 추가 저장
  - 공급사 기반 신규 상품 INSERT 지원

실행:
    cd .worktrees/feat-pricing
    python godomall_register/oc_import.py [--db PATH]
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# search_items용 경량 필드 (중첩 {} 없음 - _paginate 가 .format() 사용하므로)
LIGHT_FIELDS = "key status"

# get_items_by_keys용 전체 필드 (중첩 {} 허용 - _graphql() 직접 호출)
IMPORT_FIELDS = (
    "key name price(currency: KRW) status "
    "images(size: large) "
    "options { price quantity optionAttributes { name value } } "
    "category { fullName } "
    "content searchKeywords "
    "shippingFee shippingType metadata"
)

DEFAULT_DB_PATH = (
    Path(__file__).resolve().parent.parent / "DB_save" / "products.db"
)

OC_IMPORT_COLUMNS = [
    ("oc_price",           "INTEGER"),
    ("oc_options_json",    "TEXT"),
    ("oc_shipping_fee",    "INTEGER"),
    ("oc_shipping_type",   "TEXT"),
    ("oc_bundle_ship",     "TEXT"),
    ("oc_status",          "TEXT"),
    ("oc_content",         "TEXT"),
    ("oc_search_keywords", "TEXT"),
    ("oc_images_json",     "TEXT"),
    ("oc_prev_price",      "INTEGER"),
    ("oc_synced_at",       "TEXT"),
    ("oc_changed_at",      "TEXT"),
]


# ── 내부 헬퍼 ─────────────────────────────────────────────────────────────────

def _ensure_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(products)")
    existing = {row[1] for row in cursor.fetchall()}
    for col_name, col_type in OC_IMPORT_COLUMNS:
        if col_name not in existing:
            cursor.execute(f"ALTER TABLE products ADD COLUMN {col_name} {col_type}")
            logger.info("Added column: products.%s", col_name)
    conn.commit()


def _to_str(value) -> str:
    """API 응답값을 SQLite TEXT로 변환. list/dict는 JSON 직렬화."""
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def options_to_combo_text(options: list) -> str:
    """OC API options -> '옵션명,추가금,재고' 멀티라인 텍스트."""
    lines = []
    for opt in options:
        attrs = opt.get("optionAttributes") or []
        name = attrs[0].get("value", "기본") if attrs else "기본"
        price = opt.get("price") or 0
        qty = opt.get("quantity") or 999
        lines.append(f"{name},{price},{qty}")
    return "\n".join(lines)


def _extract_item_data(item: dict, now_iso: str) -> dict:
    """API 응답 item -> DB 저장용 dict."""
    metadata = item.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (ValueError, TypeError):
            metadata = {}

    options = item.get("options") or []
    images = item.get("images") or []
    cat = item.get("category") or {}

    return {
        "원본상품명":          item.get("name", ""),
        "카테고리명":          cat.get("fullName", ""),
        "oc_price":           item.get("price"),
        "oc_options_json":    json.dumps(options, ensure_ascii=False),
        "oc_shipping_fee":    item.get("shippingFee"),
        "oc_shipping_type":   item.get("shippingType"),
        "oc_bundle_ship":     metadata.get("bundleShipping"),
        "oc_status":          item.get("status"),
        "oc_content":         item.get("content", ""),
        "oc_search_keywords": _to_str(item.get("searchKeywords", "")),
        "oc_images_json":     json.dumps(images, ensure_ascii=False),
        "oc_synced_at":       now_iso,
    }


def _get_client(config_path):
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from godomall_register.ownerclan_client import OwnerclanClient
    return OwnerclanClient(config_path=config_path)


# ── 공개 API ──────────────────────────────────────────────────────────────────

def sync_existing(
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    progress_callback=None,
) -> dict:
    """DB 상품코드 기준으로 OC API에서 전체 필드 갱신.

    DB ACTIVE 상품코드 전부 -> get_items_by_keys() 배치 조회 -> oc_* UPDATE.
    신규 INSERT 없음.

    Returns:
        dict: updated, not_found, skipped, errors, synced_at
    """
    from godomall_register.ownerclan_client import OwnerclanApiError

    db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    now_iso = datetime.now(timezone.utc).isoformat()
    result = {"updated": 0, "not_found": 0, "skipped": 0, "errors": [], "synced_at": now_iso}

    if not db_path.exists():
        result["errors"].append(f"DB not found: {db_path}")
        return result

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_columns(conn)
        cursor = conn.cursor()

        # sync_existing: 이미 OC에서 한 번 이상 가격이 확인된 상품만 갱신.
        # (oc_price IS NOT NULL = OC API 에서 응답 받은 적 있는 상품)
        # 신규 상품 발굴은 import_new_from_vendor() 를 사용.
        cursor.execute("PRAGMA table_info(products)")
        col_names = {row[1] for row in cursor.fetchall()}
        if "oc_price" in col_names:
            cursor.execute(
                "SELECT DISTINCT 상품코드 FROM products "
                "WHERE product_status = 'ACTIVE' AND 상품코드 IS NOT NULL AND 상품코드 != '' "
                "AND oc_price IS NOT NULL"
            )
        else:
            # oc_price 컬럼 자체가 없으면 갱신할 대상이 없음
            logger.info("oc_price 컬럼 없음 - 먼저 import_new_from_vendor() 실행 필요")
            return result
        codes = [row[0] for row in cursor.fetchall()]
        total = len(codes)
        logger.info("DB ACTIVE 상품코드: %d개", total)

        if not codes:
            return result

        client = _get_client(config_path)
        if progress_callback:
            progress_callback(0, total, f"OC API 조회 중 ({total:,}개)...")
        try:
            items = client.get_items_by_keys(codes, fields=IMPORT_FIELDS, timeout=180)
        except OwnerclanApiError as exc:
            result["errors"].append(f"API error: {exc}")
            logger.error("API failed: %s", exc)
            return result

        api_map = {it["key"]: it for it in items if it.get("key")}
        logger.info("OC API 응답: %d개", len(api_map))

        for idx, code in enumerate(codes):
            item = api_map.get(code)
            if item is None:
                result["not_found"] += 1
                continue
            try:
                data = _extract_item_data(item, now_iso)
                oc_fields = {k: v for k, v in data.items() if k.startswith("oc_")}
                set_clause = ", ".join(f"{k} = ?" for k in oc_fields)
                cursor.execute(
                    f"UPDATE products SET {set_clause} WHERE 상품코드 = ?",
                    list(oc_fields.values()) + [code],
                )
                result["updated"] += 1
            except Exception as exc:
                result["errors"].append(f"{code}: {exc}")
                result["skipped"] += 1

            if progress_callback and (idx + 1) % 200 == 0:
                progress_callback(idx + 1, total, f"{idx+1:,}/{total:,} 갱신 중...")

        conn.commit()
        logger.info(
            "Sync done: updated=%d not_found=%d skipped=%d",
            result["updated"], result["not_found"], result["skipped"],
        )
    finally:
        conn.close()

    return result


def import_new_from_vendor(
    vendor: str,
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    progress_callback=None,
) -> dict:
    """특정 공급사(vendor)의 신규 상품을 DB에 INSERT.

    DB에 없는 상품코드만 추가. 이미 있으면 oc_* 갱신만.

    Args:
        vendor: OC API vendor 코드 (공급사 ID).

    Returns:
        dict: inserted, updated, skipped, errors, synced_at
    """
    from godomall_register.ownerclan_client import OwnerclanApiError

    db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    now_iso = datetime.now(timezone.utc).isoformat()
    result = {"inserted": 0, "updated": 0, "skipped": 0, "errors": [], "synced_at": now_iso}

    if not db_path.exists():
        result["errors"].append(f"DB not found: {db_path}")
        return result

    client = _get_client(config_path)

    # Step 1: 공급사 키 목록 수집 (LIGHT_FIELDS - 중첩 {} 없음)
    try:
        if progress_callback:
            progress_callback(0, 0, f"공급사 [{vendor}] 상품 목록 조회 중...")
        light = client.search_items(vendor=vendor, fields=LIGHT_FIELDS, first=100, timeout=120)
        keys = [it["key"] for it in light if it.get("key")]
        logger.info("Vendor [%s]: %d keys", vendor, len(keys))
    except OwnerclanApiError as exc:
        result["errors"].append(f"API error: {exc}")
        return result

    if not keys:
        return result

    # Step 2: 전체 데이터 조회
    if progress_callback:
        progress_callback(0, len(keys), f"상품 데이터 조회 중 ({len(keys)}개)...")
    try:
        items = client.get_items_by_keys(keys, fields=IMPORT_FIELDS, timeout=180)
    except OwnerclanApiError as exc:
        result["errors"].append(f"API error: {exc}")
        return result

    # Step 3: DB upsert
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_columns(conn)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''"
        )
        existing_codes = {row[0] for row in cursor.fetchall()}
        total = len(items)

        for idx, item in enumerate(items):
            code = item.get("key", "")
            if not code:
                result["skipped"] += 1
                continue
            try:
                data = _extract_item_data(item, now_iso)
                if code in existing_codes:
                    oc_fields = {k: v for k, v in data.items() if k.startswith("oc_")}
                    set_clause = ", ".join(f"{k} = ?" for k in oc_fields)
                    cursor.execute(
                        f"UPDATE products SET {set_clause} WHERE 상품코드 = ?",
                        list(oc_fields.values()) + [code],
                    )
                    result["updated"] += 1
                else:
                    row_data = {"상품코드": code, "product_status": "ACTIVE", **data}
                    cursor.execute("PRAGMA table_info(products)")
                    tbl_cols = {r[1] for r in cursor.fetchall()}
                    for col in row_data:
                        if col not in tbl_cols and col not in ("id", "created_at", "updated_at"):
                            try:
                                cursor.execute(f'ALTER TABLE products ADD COLUMN "{col}" TEXT')
                            except sqlite3.OperationalError:
                                pass
                    cols = list(row_data.keys())
                    ph = ", ".join("?" * len(cols))
                    col_str = ", ".join(f'"{c}"' for c in cols)
                    cursor.execute(
                        f"INSERT INTO products ({col_str}, created_at, updated_at) "
                        f"VALUES ({ph}, ?, ?)",
                        list(row_data.values()) + [now_iso, now_iso],
                    )
                    existing_codes.add(code)
                    result["inserted"] += 1
            except Exception as exc:
                result["errors"].append(f"{code}: {exc}")
                result["skipped"] += 1

            if progress_callback and (idx + 1) % 50 == 0:
                progress_callback(idx + 1, total, f"{idx+1}/{total} 처리 중...")

        conn.commit()
        logger.info(
            "Vendor import done: inserted=%d updated=%d skipped=%d",
            result["inserted"], result["updated"], result["skipped"],
        )
    finally:
        conn.close()

    return result


# pipeline_run.py 호환 alias
def import_all(db_path=None, config_path=None, progress_callback=None) -> dict:
    """sync_existing 의 alias."""
    return sync_existing(
        db_path=db_path, config_path=config_path,
        progress_callback=progress_callback,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    p = argparse.ArgumentParser(description="OC import")
    p.add_argument("--db", default=None)
    p.add_argument("--vendor", default=None, help="신규 공급사 코드")
    args = p.parse_args()

    db_path = Path(args.db) if args.db else DEFAULT_DB_PATH

    def _cb(cur, tot, msg):
        print(f"  [{cur}/{tot or '?'}] {msg}")

    if args.vendor:
        print(f"공급사 [{args.vendor}] 신규 상품 입고")
        r = import_new_from_vendor(args.vendor, db_path=db_path, progress_callback=_cb)
        print(f"  신규: {r['inserted']:,}  갱신: {r['updated']:,}  오류: {len(r['errors'])}")
    else:
        print(f"DB 기존 상품 갱신 ({db_path})")
        r = sync_existing(db_path=db_path, progress_callback=_cb)
        print(f"  갱신: {r['updated']:,}  미발견: {r['not_found']:,}  오류: {len(r['errors'])}")
