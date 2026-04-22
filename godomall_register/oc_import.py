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
    "key name price(currency: KRW) status openmarketSellable "
    "images(size: large) "
    "options { price quantity optionAttributes { name value } } "
    "category { fullName } "
    "content searchKeywords "
    "shippingFee shippingType metadata"
)

# oc_status → product_status 매핑
# available: 정상판매 → ACTIVE
# soldout: 품절(일시적) → SOLDOUT  (출고 차단, 재입고 가능)
# unavailable: 판매불가(공급사 숨김/정지) → INACTIVE
# discontinued: 단종 → INACTIVE
_OC_STATUS_TO_PRODUCT_STATUS = {
    "available":    "ACTIVE",
    "soldout":      "SOLDOUT",
    "unavailable":  "INACTIVE",
    "discontinued": "INACTIVE",
}

DEFAULT_DB_PATH = (
    Path(__file__).resolve().parent.parent / "DB_save" / "products.db"
)

OC_IMPORT_COLUMNS = [
    ("oc_price",                "INTEGER"),
    ("oc_options_json",         "TEXT"),
    ("oc_shipping_fee",         "INTEGER"),
    ("oc_shipping_type",        "TEXT"),
    ("oc_bundle_ship",          "TEXT"),
    ("oc_status",               "TEXT"),   # available / soldout / unavailable / discontinued
    ("oc_openmarket_sellable",  "INTEGER"), # 오픈마켓 판매 허용 여부 (1/0)
    ("oc_content",              "TEXT"),
    ("oc_search_keywords",      "TEXT"),
    ("oc_images_json",          "TEXT"),
    ("oc_prev_price",           "INTEGER"),
    ("oc_synced_at",            "TEXT"),
    ("oc_changed_at",           "TEXT"),
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
        "vendor_code":        str(metadata["vendorKey"]) if metadata.get("vendorKey") else None,
        "oc_bundle_ship":     metadata.get("bundleShipping"),
        "oc_status":               item.get("status"),
        "oc_openmarket_sellable":  1 if item.get("openmarketSellable") else 0,
        "oc_content":              item.get("content", ""),
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
    oc_price 유무 무관하게 전체 갱신. 신규 INSERT 없음.

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
        # ACTIVE 상품코드 전체 대상 (oc_price 유무 무관 — 모두 OC 상품코드)
        cursor.execute(
            "SELECT DISTINCT 상품코드 FROM products "
            "WHERE product_status = 'ACTIVE' AND 상품코드 IS NOT NULL AND 상품코드 != ''"
        )
        codes = [row[0] for row in cursor.fetchall()]
        total = len(codes)
        logger.info("DB ACTIVE 상품코드: %d개", total)

        if not codes:
            return result

        client = _get_client(config_path)
        BATCH = 5000  # OC API itemsByKeys 최대 단위
        n_batches = (total + BATCH - 1) // BATCH
        processed = 0

        for batch_idx, batch_start in enumerate(range(0, total, BATCH)):
            batch_codes = codes[batch_start:batch_start + BATCH]
            batch_num = batch_idx + 1
            if progress_callback:
                progress_callback(
                    processed, total,
                    f"API 조회 배치 {batch_num}/{n_batches} ({batch_start+1:,}~{batch_start+len(batch_codes):,})..."
                )
            try:
                items = client.get_items_by_keys(batch_codes, fields=IMPORT_FIELDS, timeout=180)
            except OwnerclanApiError as exc:
                result["errors"].append(f"Batch {batch_num} API error: {exc}")
                logger.error("API batch %d failed: %s", batch_num, exc)
                continue

            api_map = {it["key"]: it for it in items if it.get("key")}
            logger.info("Batch %d/%d API 응답: %d개", batch_num, n_batches, len(api_map))

            for code in batch_codes:
                item = api_map.get(code)
                if item is None:
                    result["not_found"] += 1
                    processed += 1
                    continue
                try:
                    data = _extract_item_data(item, now_iso)
                    oc_fields = {k: v for k, v in data.items() if k.startswith("oc_")}
                    # vendor_code도 함께 갱신 (기존 60k 상품 자동 채움)
                    if data.get("vendor_code"):
                        oc_fields["vendor_code"] = data["vendor_code"]
                    # oc_status → product_status 동기화
                    new_product_status = _OC_STATUS_TO_PRODUCT_STATUS.get(
                        data.get("oc_status"), "ACTIVE"
                    )
                    oc_fields["product_status"] = new_product_status
                    set_clause = ", ".join(f"{k} = ?" for k in oc_fields)
                    cursor.execute(
                        f"UPDATE products SET {set_clause} WHERE 상품코드 = ?",
                        list(oc_fields.values()) + [code],
                    )
                    result["updated"] += 1
                except Exception as exc:
                    result["errors"].append(f"{code}: {exc}")
                    result["skipped"] += 1
                processed += 1

            conn.commit()  # 배치 단위 commit
            if progress_callback:
                progress_callback(processed, total, f"배치 {batch_num}/{n_batches} 완료 ({processed:,}/{total:,})")
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
                    oc_fields["vendor_code"] = vendor  # 공급사 코드 갱신
                    set_clause = ", ".join(f"{k} = ?" for k in oc_fields)
                    cursor.execute(
                        f"UPDATE products SET {set_clause} WHERE 상품코드 = ?",
                        list(oc_fields.values()) + [code],
                    )
                    result["updated"] += 1
                else:
                    row_data = {"상품코드": code, "product_status": "ACTIVE", "vendor_code": vendor, **data}
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


def full_sync(
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    progress_callback=None,
) -> dict:
    """OC 전체 동기화 (단일 패스) — 기존 갱신 + 신규 INSERT + 공급사 발굴.

    Pass 1: allItems(available, key only) → 전체 OC 키 수집
    Pass 2: 기존 DB 상품 → itemsByKeys(IMPORT_FIELDS) → 전체 갱신
    Pass 3: 신규 상품  → itemsByKeys(SCAN_META_FIELDS)  → INSERT + 공급사 후보

    Returns:
        dict: total_oc, updated, not_found, inserted, set_inactive, new_vendors, vendors, errors
    """
    from godomall_register.ownerclan_client import OwnerclanApiError

    db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    now_iso = datetime.now(timezone.utc).isoformat()
    result = {
        "total_oc": 0,
        "updated": 0,
        "not_found": 0,
        "inserted": 0,
        "set_inactive": 0,
        "new_vendors": 0,
        "vendors": [],
        "errors": [],
        "synced_at": now_iso,
    }

    if not db_path.exists():
        result["errors"].append(f"DB not found: {db_path}")
        return result

    client = _get_client(config_path)

    # ── Pass 1: 전체 OC available 키 수집 ────────────────────────────────────
    if progress_callback:
        progress_callback(0, 0, "Pass 1: OC 전체 상품 키 수집 중 (수분 소요)...")
    try:
        light_items = client.search_items(
            status="available",
            fields=LIGHT_FIELDS,  # "key status" — 중첩 {} 없음
            first=100,
            timeout=300,
        )
        oc_available = {it["key"] for it in light_items if it.get("key")}
        result["total_oc"] = len(oc_available)
    except Exception as exc:
        result["errors"].append(f"Pass 1 실패: {exc}")
        return result

    if progress_callback:
        progress_callback(0, 0, f"Pass 1 완료: OC {len(oc_available):,}개 available")

    # DB 전체 상품코드 + 상태 로드
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_columns(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT 상품코드, product_status FROM products "
            "WHERE 상품코드 IS NOT NULL AND 상품코드 != ''"
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    db_keys_all = {r[0] for r in rows}
    db_active   = {r[0] for r in rows if r[1] == "ACTIVE"}

    update_keys = list(oc_available & db_keys_all)   # OC available ∩ DB (any status)
    new_keys    = list(oc_available - db_keys_all)    # OC available − DB
    gone_keys   = list(db_active - oc_available)      # DB ACTIVE − OC available → INACTIVE

    if progress_callback:
        progress_callback(0, 0,
            f"분류 완료: 갱신 {len(update_keys):,} · 신규 {len(new_keys):,} · 비활성 예정 {len(gone_keys):,}"
        )

    BATCH = 5000
    total_work = len(update_keys) + len(new_keys)
    processed  = 0

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_columns(conn)
        cur = conn.cursor()

        # ── Pass 2: 기존 상품 전체 갱신 ──────────────────────────────────────
        n2 = (len(update_keys) + BATCH - 1) // BATCH
        for bi, bs in enumerate(range(0, len(update_keys), BATCH)):
            batch = update_keys[bs:bs + BATCH]
            bn = bi + 1
            if progress_callback:
                progress_callback(processed, total_work,
                    f"Pass 2 갱신 배치 {bn}/{n2} ({bs+1:,}~{bs+len(batch):,})...")
            try:
                items = client.get_items_by_keys(batch, fields=IMPORT_FIELDS, timeout=180)
            except OwnerclanApiError as exc:
                result["errors"].append(f"갱신 배치 {bn}: {exc}")
                processed += len(batch)
                continue

            api_map = {it["key"]: it for it in items if it.get("key")}
            for code in batch:
                item = api_map.get(code)
                if item is None:
                    result["not_found"] += 1
                    processed += 1
                    continue
                try:
                    data = _extract_item_data(item, now_iso)
                    oc_fields = {k: v for k, v in data.items() if k.startswith("oc_")}
                    if data.get("vendor_code"):
                        oc_fields["vendor_code"] = data["vendor_code"]
                    oc_fields["product_status"] = _OC_STATUS_TO_PRODUCT_STATUS.get(
                        data.get("oc_status"), "ACTIVE"
                    )
                    set_clause = ", ".join(f"{k} = ?" for k in oc_fields)
                    cur.execute(
                        f"UPDATE products SET {set_clause} WHERE 상품코드 = ?",
                        list(oc_fields.values()) + [code],
                    )
                    result["updated"] += 1
                except Exception as exc:
                    result["errors"].append(f"{code}: {exc}")
                processed += 1

            conn.commit()
            if progress_callback:
                progress_callback(processed, total_work, f"갱신 배치 {bn}/{n2} 완료 ({processed:,}/{total_work:,})")

        # gone_keys → INACTIVE
        if gone_keys:
            if progress_callback:
                progress_callback(processed, total_work, f"비활성 처리 {len(gone_keys):,}개...")
            for i in range(0, len(gone_keys), BATCH):
                chunk = gone_keys[i:i + BATCH]
                ph = ",".join("?" * len(chunk))
                cur.execute(
                    f"UPDATE products SET product_status = 'INACTIVE', oc_synced_at = ? "
                    f"WHERE 상품코드 IN ({ph})",
                    [now_iso] + chunk,
                )
                result["set_inactive"] += cur.rowcount
            conn.commit()

        # ── Pass 3: 신규 상품 INSERT + 공급사 발굴 ───────────────────────────
        if new_keys:
            SCAN_META_FIELDS = "key status metadata"
            n3 = (len(new_keys) + BATCH - 1) // BATCH
            vendor_candidates: dict[str, dict] = {}

            try:
                cur.execute("SELECT vendor_code FROM vendors WHERE vendor_code IS NOT NULL")
                registered_vendors = {r[0] for r in cur.fetchall()}
            except sqlite3.OperationalError:
                registered_vendors = set()

            cur.execute("SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
            existing_codes = {r[0] for r in cur.fetchall()}

            for bi, bs in enumerate(range(0, len(new_keys), BATCH)):
                batch = new_keys[bs:bs + BATCH]
                bn = bi + 1
                if progress_callback:
                    progress_callback(processed, total_work,
                        f"Pass 3 신규 배치 {bn}/{n3} ({bs+1:,}~{bs+len(batch):,})...")
                try:
                    items = client.get_items_by_keys(batch, fields=SCAN_META_FIELDS, timeout=180)
                except Exception as exc:
                    result["errors"].append(f"신규 배치 {bn}: {exc}")
                    processed += len(batch)
                    continue

                for item in items:
                    code = item.get("key", "")
                    if not code or code in existing_codes:
                        continue
                    metadata = item.get("metadata") or {}
                    if isinstance(metadata, str):
                        try:
                            metadata = json.loads(metadata)
                        except Exception:
                            metadata = {}
                    vendor_code  = str(metadata["vendorKey"]) if metadata.get("vendorKey") else None
                    grade_detail = metadata.get("gradeDetail") or {}
                    release_rate = grade_detail.get("releaseRate", "")
                    average_ship = grade_detail.get("averageShip", "")
                    oc_status    = item.get("status", "available")
                    prod_status  = _OC_STATUS_TO_PRODUCT_STATUS.get(oc_status, "ACTIVE")
                    try:
                        cur.execute(
                            "INSERT INTO products "
                            "(상품코드, vendor_code, oc_status, product_status, "
                            " oc_synced_at, created_at, updated_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [code, vendor_code, oc_status, prod_status, now_iso, now_iso, now_iso],
                        )
                        existing_codes.add(code)
                        result["inserted"] += 1
                        if vendor_code and vendor_code not in registered_vendors:
                            if vendor_code not in vendor_candidates:
                                vendor_candidates[vendor_code] = {
                                    "vendor_code": vendor_code,
                                    "item_count": 0,
                                    "release_rate": release_rate,
                                    "average_ship": average_ship,
                                }
                            else:
                                if release_rate and not vendor_candidates[vendor_code]["release_rate"]:
                                    vendor_candidates[vendor_code]["release_rate"] = release_rate
                                if average_ship and not vendor_candidates[vendor_code]["average_ship"]:
                                    vendor_candidates[vendor_code]["average_ship"] = average_ship
                            vendor_candidates[vendor_code]["item_count"] += 1
                    except Exception as exc:
                        result["errors"].append(f"{code}: {exc}")

                processed += len(batch)
                conn.commit()
                if progress_callback:
                    progress_callback(processed, total_work,
                        f"신규 배치 {bn}/{n3} 완료 — 신규 {result['inserted']:,}개")

            _rs = {"GOOD": 0, "NORMAL": 1, "BAD": 2}
            result["new_vendors"] = len(vendor_candidates)
            result["vendors"] = sorted(
                vendor_candidates.values(),
                key=lambda v: (_rs.get(v.get("release_rate", ""), 3), -v["item_count"]),
            )

    finally:
        conn.close()

    if progress_callback:
        progress_callback(total_work, total_work,
            f"완료 — 갱신 {result['updated']:,} · 신규 {result['inserted']:,} · "
            f"비활성 {result['set_inactive']:,} · 신규공급사 {result['new_vendors']}개")

    logger.info(
        "full_sync done: updated=%d inserted=%d set_inactive=%d new_vendors=%d errors=%d",
        result["updated"], result["inserted"], result["set_inactive"],
        result["new_vendors"], len(result["errors"]),
    )
    return result


def scan_all_for_vendors(
    db_path: str | Path | None = None,
    config_path: str | Path | None = None,
    progress_callback=None,
) -> dict:
    """전체 OC 상품 2-pass 스캔 → 신규 공급사 발굴.

    Pass 1: allItems(available, key+status only) → 전체 키 수집 (메타 없음, 빠름)
    Pass 2: itemsByKeys(신규 키만, metadata) → vendorKey 추출 → DB INSERT

    Returns:
        dict: total_oc, new_items, new_vendors, vendors (list), errors
    """
    db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    now_iso = datetime.now(timezone.utc).isoformat()
    result = {
        "total_oc": 0,
        "new_items": 0,
        "new_vendors": 0,
        "vendors": [],
        "errors": [],
        "synced_at": now_iso,
    }

    client = _get_client(config_path)

    # Pass 1: 전체 available 키 수집 (메타데이터 없이 경량 조회)
    if progress_callback:
        progress_callback(0, 0, "Pass 1: OC 전체 상품 키 수집 중 (수분 소요)...")
    try:
        light_items = client.search_items(
            status="available",
            fields=LIGHT_FIELDS,  # "key status" — 중첩 {} 없음
            first=100,
            timeout=300,
        )
        oc_keys = {it["key"] for it in light_items if it.get("key")}
        result["total_oc"] = len(oc_keys)
    except Exception as exc:
        result["errors"].append(f"Pass 1 실패: {exc}")
        return result

    if progress_callback:
        progress_callback(0, 0, f"Pass 1 완료: OC {len(oc_keys):,}개 키 수집")

    if not oc_keys:
        return result

    # DB 기존 키 조회
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_columns(conn)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''"
        )
        db_keys = {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()

    new_keys = list(oc_keys - db_keys)
    if not new_keys:
        if progress_callback:
            progress_callback(len(oc_keys), len(oc_keys), "신규 상품 없음 — 스캔 완료")
        return result

    if progress_callback:
        progress_callback(
            0, len(new_keys),
            f"Pass 2: 신규 {len(new_keys):,}개 메타데이터 조회 시작...",
        )

    # Pass 2: 신규 키만 배치로 메타데이터 조회 → vendorKey 추출 → DB INSERT
    SCAN_META_FIELDS = "key status metadata"  # get_items_by_keys용 (중첩 {} 허용)
    BATCH = 5000
    total_new = len(new_keys)
    n_batches = (total_new + BATCH - 1) // BATCH
    processed = 0
    vendor_candidates: dict[str, dict] = {}

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_columns(conn)
        cursor = conn.cursor()

        # 등록된 공급사 목록 (vendors 테이블이 없으면 빈 set)
        try:
            cursor.execute("SELECT vendor_code FROM vendors WHERE vendor_code IS NOT NULL")
            registered_vendors = {row[0] for row in cursor.fetchall()}
        except sqlite3.OperationalError:
            registered_vendors = set()

        # DB 기존 코드 재확인
        cursor.execute(
            "SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''"
        )
        existing_codes = {row[0] for row in cursor.fetchall()}

        for batch_idx, batch_start in enumerate(range(0, total_new, BATCH)):
            batch_keys = new_keys[batch_start:batch_start + BATCH]
            batch_num = batch_idx + 1
            if progress_callback:
                progress_callback(
                    processed, total_new,
                    f"Pass 2 배치 {batch_num}/{n_batches} 조회 중 ({batch_start+1:,}~{batch_start+len(batch_keys):,})...",
                )
            try:
                items = client.get_items_by_keys(
                    batch_keys, fields=SCAN_META_FIELDS, timeout=180
                )
            except Exception as exc:
                result["errors"].append(f"Pass 2 배치 {batch_num}: {exc}")
                processed += len(batch_keys)
                continue

            for item in items:
                code = item.get("key", "")
                if not code or code in existing_codes:
                    continue

                metadata = item.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}

                vendor_code = str(metadata["vendorKey"]) if metadata.get("vendorKey") else None
                grade_detail = metadata.get("gradeDetail") or {}
                release_rate = grade_detail.get("releaseRate", "")
                average_ship = grade_detail.get("averageShip", "")
                oc_status = item.get("status", "available")
                product_status = _OC_STATUS_TO_PRODUCT_STATUS.get(oc_status, "ACTIVE")

                try:
                    cursor.execute(
                        "INSERT INTO products "
                        "(상품코드, vendor_code, oc_status, product_status, oc_synced_at, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [code, vendor_code, oc_status, product_status, now_iso, now_iso, now_iso],
                    )
                    existing_codes.add(code)
                    result["new_items"] += 1

                    if vendor_code and vendor_code not in registered_vendors:
                        if vendor_code not in vendor_candidates:
                            vendor_candidates[vendor_code] = {
                                "vendor_code": vendor_code,
                                "item_count": 0,
                                "release_rate": release_rate,
                                "average_ship": average_ship,
                            }
                        else:
                            # 첫 번째로 확인된 값으로 고정 (벤더별로 일정함)
                            if release_rate and not vendor_candidates[vendor_code]["release_rate"]:
                                vendor_candidates[vendor_code]["release_rate"] = release_rate
                            if average_ship and not vendor_candidates[vendor_code]["average_ship"]:
                                vendor_candidates[vendor_code]["average_ship"] = average_ship
                        vendor_candidates[vendor_code]["item_count"] += 1
                except Exception as exc:
                    result["errors"].append(f"{code}: {exc}")

            processed += len(batch_keys)
            conn.commit()
            if progress_callback:
                progress_callback(
                    processed, total_new,
                    f"Pass 2 배치 {batch_num}/{n_batches} 완료 — 신규 {result['new_items']:,}개",
                )

    finally:
        conn.close()

    # 우선순위: 출고율 GOOD → NORMAL → BAD → 미확인, 동순위면 상품수 많은 순
    _rate_score = {"GOOD": 0, "NORMAL": 1, "BAD": 2}

    def _priority(v):
        return (_rate_score.get(v.get("release_rate", ""), 3), -v["item_count"])

    result["new_vendors"] = len(vendor_candidates)
    result["vendors"] = sorted(vendor_candidates.values(), key=_priority)

    if progress_callback:
        progress_callback(
            total_new, total_new,
            f"스캔 완료 — 신규 상품 {result['new_items']:,}개, 신규 공급사 {result['new_vendors']}개 발굴",
        )
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
