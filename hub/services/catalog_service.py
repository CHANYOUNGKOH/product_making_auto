"""OC 카탈로그 서비스 — oc_catalog.db 관리 + 전체 스캔 + SSE 스트림."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oc_catalog")
_DONE_SENTINEL = object()

# ── 글로벌 스캔 상태 (폴링용) ─────────────────────────────────────────────────
_scan_state: dict = {
    "running": False,
    "message": "",
    "current": 0,
    "total": 0,
    "result": None,
    "error": None,
    "started_at": None,
    "finished_at": None,
}
_scan_lock = threading.Lock()

# oc_catalog.db 저장 필드 (중첩 {} 없음 — get_items_by_keys 호환)
CATALOG_FIELDS = (
    "key status price(currency: KRW) shippingFee shippingType "
    "openmarketSellable metadata"
)

_OC_STATUS_TO_PRODUCT_STATUS = {
    "available":    "ACTIVE",
    "soldout":      "SOLDOUT",
    "unavailable":  "INACTIVE",
    "discontinued": "INACTIVE",
}


# ── DB 경로 ───────────────────────────────────────────────────────────────────

def get_catalog_db_path() -> str:
    env = os.environ.get("HUB_CATALOG_DB_PATH")
    if env:
        return env
    from hub.services.db_service import get_db_path
    products_path = Path(get_db_path())
    return str(products_path.parent / "oc_catalog.db")


# ── 초기화 ────────────────────────────────────────────────────────────────────

_DDL_ITEMS = """
CREATE TABLE IF NOT EXISTS oc_items (
    key                 TEXT PRIMARY KEY,
    vendor_code         TEXT,
    status              TEXT,
    price_krw           INTEGER,
    shipping_fee        INTEGER,
    shipping_type       TEXT,
    bundle_shipping     TEXT,
    openmarket_sellable INTEGER,
    release_rate        TEXT,
    average_ship        TEXT,
    last_scanned_at     TEXT,
    last_updated_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_oc_items_vendor ON oc_items(vendor_code);
CREATE INDEX IF NOT EXISTS idx_oc_items_status ON oc_items(status);
"""

_DDL_META = """
CREATE TABLE IF NOT EXISTS oc_catalog_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

_DDL_CATEGORY_MARKETS = """
CREATE TABLE IF NOT EXISTS oc_category_markets (
    oc_category_key  TEXT NOT NULL,
    oc_category_name TEXT,
    market           TEXT NOT NULL,
    market_cat_code  TEXT,
    market_cat_name  TEXT,
    is_manual        INTEGER DEFAULT 0,
    updated_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (oc_category_key, market)
);
"""


def _init_catalog(conn: sqlite3.Connection) -> None:
    conn.executescript(_DDL_ITEMS)
    conn.executescript(_DDL_META)
    conn.executescript(_DDL_CATEGORY_MARKETS)
    conn.commit()


# ── 전체 스캔 ─────────────────────────────────────────────────────────────────

def catalog_full_scan(progress_callback=None) -> dict:
    """OC 전체 상품 스캔 → oc_catalog.db 구축 + products.db backfill.

    Pass 1: allItems(available, key+status) — 전체 키 수집 (경량)
    Pass 2: itemsByKeys(전체, CATALOG_FIELDS) — 가격/상태/배송비/공급사 저장
    Pass 3: products.db backfill — vendor_code, oc_price, oc_status 등 채움

    Returns:
        dict: total_oc, unique_vendors, backfilled, errors, scanned_at
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from godomall_register.ownerclan_client import OwnerclanClient, OwnerclanApiError

    now_iso = datetime.now(timezone.utc).isoformat()
    result = {
        "total_oc": 0,
        "unique_vendors": 0,
        "backfilled": 0,
        "errors": [],
        "scanned_at": now_iso,
    }

    catalog_path = get_catalog_db_path()
    client = OwnerclanClient()

    # ── Pass 1: 전체 key 수집 ─────────────────────────────────────────────────
    if progress_callback:
        progress_callback(0, 0, "Pass 1: OC 전체 상품 키 수집 중 (수분 소요)...")
    # Pass 1: 페이지당 1000개로 시도, 실패 시 100개로 재시도
    for page_size in (1000, 100):
        try:
            if progress_callback:
                progress_callback(0, 0, f"Pass 1: OC 전체 키 수집 중 ({page_size}개/페이지)...")
            light_items = client.search_items(
                status="available",
                fields="key status",
                first=page_size,
                timeout=300,
            )
            all_keys = [it["key"] for it in light_items if it.get("key")]
            result["total_oc"] = len(all_keys)
            break
        except Exception as exc:
            if page_size == 100:
                result["errors"].append(f"Pass 1 실패: {exc}")
                return result
            logger.warning("Pass 1 first=%d 실패, 100으로 재시도: %s", page_size, exc)

    if progress_callback:
        progress_callback(0, 0, f"Pass 1 완료: {len(all_keys):,}개 키 수집")

    if not all_keys:
        return result

    # ── Pass 2: 카탈로그 저장 ─────────────────────────────────────────────────
    BATCH = 5000
    n_batches = (len(all_keys) + BATCH - 1) // BATCH
    processed = 0

    cat_conn = sqlite3.connect(catalog_path)
    try:
        _init_catalog(cat_conn)
        cur = cat_conn.cursor()

        for bi, bs in enumerate(range(0, len(all_keys), BATCH)):
            batch = all_keys[bs:bs + BATCH]
            bn = bi + 1
            if progress_callback:
                progress_callback(processed, len(all_keys),
                    f"Pass 2 배치 {bn}/{n_batches} 조회 중 ({bs+1:,}~{bs+len(batch):,})...")
            try:
                items = client.get_items_by_keys(batch, fields=CATALOG_FIELDS, timeout=180)
            except OwnerclanApiError as exc:
                result["errors"].append(f"Pass 2 배치 {bn}: {exc}")
                processed += len(batch)
                continue

            for item in items:
                key = item.get("key", "")
                if not key:
                    continue
                metadata = item.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                grade = metadata.get("gradeDetail") or {}
                cur.execute(
                    """INSERT INTO oc_items
                       (key, vendor_code, status, price_krw, shipping_fee,
                        shipping_type, bundle_shipping, openmarket_sellable,
                        release_rate, average_ship, last_scanned_at, last_updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(key) DO UPDATE SET
                         vendor_code         = excluded.vendor_code,
                         status              = excluded.status,
                         price_krw           = excluded.price_krw,
                         shipping_fee        = excluded.shipping_fee,
                         shipping_type       = excluded.shipping_type,
                         bundle_shipping     = excluded.bundle_shipping,
                         openmarket_sellable = excluded.openmarket_sellable,
                         release_rate        = excluded.release_rate,
                         average_ship        = excluded.average_ship,
                         last_scanned_at     = excluded.last_scanned_at,
                         last_updated_at     = excluded.last_updated_at
                    """,
                    [
                        key,
                        str(metadata["vendorKey"]) if metadata.get("vendorKey") else None,
                        item.get("status"),
                        item.get("price"),
                        item.get("shippingFee"),
                        item.get("shippingType"),
                        metadata.get("bundleShipping"),
                        1 if item.get("openmarketSellable") else 0,
                        grade.get("releaseRate", ""),
                        grade.get("averageShip", ""),
                        now_iso,
                        now_iso,
                    ],
                )

            processed += len(batch)
            cat_conn.commit()
            if progress_callback:
                progress_callback(processed, len(all_keys),
                    f"Pass 2 배치 {bn}/{n_batches} 완료 ({processed:,}/{len(all_keys):,})")

        # 고유 공급사 수
        cur.execute("SELECT COUNT(DISTINCT vendor_code) FROM oc_items WHERE vendor_code IS NOT NULL")
        result["unique_vendors"] = cur.fetchone()[0]

        # 메타 갱신
        cur.execute("INSERT OR REPLACE INTO oc_catalog_meta VALUES ('last_full_scan_at', ?)", [now_iso])
        cur.execute("INSERT OR REPLACE INTO oc_catalog_meta VALUES ('total_scanned', ?)", [str(result["total_oc"])])
        cat_conn.commit()

    finally:
        cat_conn.close()

    if progress_callback:
        progress_callback(len(all_keys), len(all_keys),
            f"Pass 2 완료: {result['total_oc']:,}개 저장, 공급사 {result['unique_vendors']}개")

    # ── Pass 3: products.db backfill ─────────────────────────────────────────
    from hub.services.db_service import get_db_path
    products_path = get_db_path()

    if progress_callback:
        progress_callback(0, 0, "Pass 3: products.db 누락 컬럼 채우는 중...")

    try:
        prod_conn = sqlite3.connect(products_path)
        cat_conn2 = sqlite3.connect(catalog_path)
        try:
            prod_conn.execute("PRAGMA journal_mode=WAL")
            pc = prod_conn.cursor()
            cc = cat_conn2.cursor()

            # products.db에 있는 상품코드 목록
            pc.execute("SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
            prod_keys = {r[0] for r in pc.fetchall()}

            # oc_catalog에서 교차분 조회
            cc.execute(
                "SELECT key, vendor_code, status, price_krw, shipping_fee, "
                "shipping_type, bundle_shipping, openmarket_sellable "
                "FROM oc_items WHERE key IS NOT NULL"
            )
            updated = 0
            for row in cc.fetchall():
                key, vendor_code, status, price_krw, ship_fee, ship_type, bundle_ship, om_sell = row
                if key not in prod_keys:
                    continue
                product_status = _OC_STATUS_TO_PRODUCT_STATUS.get(status or "", "ACTIVE")
                pc.execute(
                    """UPDATE products SET
                         vendor_code         = COALESCE(vendor_code, ?),
                         oc_price            = COALESCE(oc_price, ?),
                         oc_shipping_fee     = COALESCE(oc_shipping_fee, ?),
                         oc_shipping_type    = COALESCE(oc_shipping_type, ?),
                         oc_bundle_ship      = COALESCE(oc_bundle_ship, ?),
                         oc_status           = COALESCE(oc_status, ?),
                         oc_openmarket_sellable = COALESCE(oc_openmarket_sellable, ?),
                         product_status      = COALESCE(product_status, ?),
                         oc_synced_at        = COALESCE(oc_synced_at, ?)
                       WHERE 상품코드 = ?""",
                    [vendor_code, price_krw, ship_fee, ship_type, bundle_ship,
                     status, om_sell, product_status, now_iso, key],
                )
                updated += prod_conn.execute("SELECT changes()").fetchone()[0]

            prod_conn.commit()
            result["backfilled"] = updated
        finally:
            prod_conn.close()
            cat_conn2.close()
    except Exception as exc:
        result["errors"].append(f"Pass 3 backfill 실패: {exc}")
        logger.exception("backfill error")

    if progress_callback:
        progress_callback(0, 0,
            f"완료 — OC {result['total_oc']:,}개 · 공급사 {result['unique_vendors']}개 · "
            f"backfill {result['backfilled']:,}개")

    logger.info("catalog_full_scan done: total_oc=%d vendors=%d backfilled=%d errors=%d",
                result["total_oc"], result["unique_vendors"],
                result["backfilled"], len(result["errors"]))
    return result


# ── 공급사 목록 조회 ──────────────────────────────────────────────────────────

def get_catalog_vendors(registered_codes: set[str] | None = None) -> list[dict]:
    """oc_catalog에서 vendor별 집계 → 미등록 공급사 우선순위 목록."""
    catalog_path = get_catalog_db_path()
    if not Path(catalog_path).exists():
        return []
    conn = sqlite3.connect(catalog_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                vendor_code,
                COUNT(*) AS item_count,
                MAX(CASE WHEN release_rate != '' THEN release_rate END) AS release_rate,
                MAX(CASE WHEN average_ship  != '' THEN average_ship  END) AS average_ship
            FROM oc_items
            WHERE vendor_code IS NOT NULL AND status = 'available'
            GROUP BY vendor_code
            ORDER BY
                CASE MAX(release_rate)
                    WHEN 'GOOD' THEN 0 WHEN 'NORMAL' THEN 1 WHEN 'BAD' THEN 2 ELSE 3 END,
                COUNT(*) DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    if registered_codes is not None:
        for r in rows:
            r["registered"] = r["vendor_code"] in registered_codes
    return rows


def get_catalog_meta() -> dict:
    """oc_catalog_meta 전체 조회."""
    catalog_path = get_catalog_db_path()
    if not Path(catalog_path).exists():
        return {}
    conn = sqlite3.connect(catalog_path)
    try:
        rows = conn.execute("SELECT key, value FROM oc_catalog_meta").fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        conn.close()


# ── 기존 상품 OC 동기화 ───────────────────────────────────────────────────────

def sync_from_existing_products(progress_callback=None) -> dict:
    """products.db 상품코드 기준으로 OC API 조회 → 가격/상태/vendor_code 갱신.

    allItems 스캔 없이 기존 65k 상품코드로 바로 itemsByKeys 호출.
    소요 시간: ~3~5분 (65k / 5000 = 13배치)
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from godomall_register.ownerclan_client import OwnerclanClient, OwnerclanApiError
    from hub.services.db_service import get_db_path

    now_iso = datetime.now(timezone.utc).isoformat()
    result = {
        "total_products": 0,
        "updated": 0,
        "not_found": 0,
        "errors": [],
        "synced_at": now_iso,
    }

    products_path = get_db_path()
    catalog_path = get_catalog_db_path()
    client = OwnerclanClient()

    # products.db 상품코드 전체 조회
    prod_conn = sqlite3.connect(products_path)
    try:
        pc = prod_conn.cursor()
        pc.execute("SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
        all_keys = [r[0] for r in pc.fetchall()]
        result["total_products"] = len(all_keys)
    finally:
        prod_conn.close()

    if not all_keys:
        return result

    if progress_callback:
        progress_callback(0, len(all_keys), f"총 {len(all_keys):,}개 상품코드 OC 조회 시작...")

    # oc_catalog.db UPSERT
    BATCH = 5000
    n_batches = (len(all_keys) + BATCH - 1) // BATCH
    processed = 0

    cat_conn = sqlite3.connect(catalog_path)
    try:
        _init_catalog(cat_conn)
        cat_cur = cat_conn.cursor()

        for bi, bs in enumerate(range(0, len(all_keys), BATCH)):
            batch = all_keys[bs:bs + BATCH]
            bn = bi + 1
            if progress_callback:
                progress_callback(processed, len(all_keys),
                    f"배치 {bn}/{n_batches} OC 조회 중 ({bs+1:,}~{bs+len(batch):,})...")
            try:
                items = client.get_items_by_keys(batch, fields=CATALOG_FIELDS, timeout=180)
            except OwnerclanApiError as exc:
                result["errors"].append(f"배치 {bn}: {exc}")
                processed += len(batch)
                continue

            found_keys = set()
            for item in items:
                key = item.get("key", "")
                if not key:
                    continue
                found_keys.add(key)
                metadata = item.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                grade = metadata.get("gradeDetail") or {}
                cat_cur.execute(
                    """INSERT INTO oc_items
                       (key, vendor_code, status, price_krw, shipping_fee,
                        shipping_type, bundle_shipping, openmarket_sellable,
                        release_rate, average_ship, last_scanned_at, last_updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(key) DO UPDATE SET
                         vendor_code         = excluded.vendor_code,
                         status              = excluded.status,
                         price_krw           = excluded.price_krw,
                         shipping_fee        = excluded.shipping_fee,
                         shipping_type       = excluded.shipping_type,
                         bundle_shipping     = excluded.bundle_shipping,
                         openmarket_sellable = excluded.openmarket_sellable,
                         release_rate        = excluded.release_rate,
                         average_ship        = excluded.average_ship,
                         last_updated_at     = excluded.last_updated_at""",
                    [key,
                     str(metadata["vendorKey"]) if metadata.get("vendorKey") else None,
                     item.get("status"), item.get("price"), item.get("shippingFee"),
                     item.get("shippingType"), metadata.get("bundleShipping"),
                     1 if item.get("openmarketSellable") else 0,
                     grade.get("releaseRate", ""), grade.get("averageShip", ""),
                     now_iso, now_iso],
                )
            cat_conn.commit()
            result["not_found"] += len(batch) - len(found_keys)
            processed += len(batch)
            if progress_callback:
                progress_callback(processed, len(all_keys),
                    f"배치 {bn}/{n_batches} 완료 ({processed:,}/{len(all_keys):,})")

        # 메타 갱신
        cat_cur.execute("INSERT OR REPLACE INTO oc_catalog_meta VALUES ('last_product_sync_at', ?)", [now_iso])
        cat_cur.execute("INSERT OR REPLACE INTO oc_catalog_meta VALUES ('total_products_synced', ?)", [str(result["total_products"])])
        cat_conn.commit()
    finally:
        cat_conn.close()

    # products.db 갱신 (COALESCE 없이 직접 UPDATE — 최신값으로 덮어씀)
    if progress_callback:
        progress_callback(processed, len(all_keys), "products.db 갱신 중...")
    try:
        prod_conn = sqlite3.connect(products_path)
        cat_conn2 = sqlite3.connect(catalog_path)
        try:
            prod_conn.execute("PRAGMA journal_mode=WAL")
            pc = prod_conn.cursor()
            cc = cat_conn2.cursor()
            cc.execute(
                "SELECT key, vendor_code, status, price_krw, shipping_fee, "
                "shipping_type, bundle_shipping, openmarket_sellable "
                "FROM oc_items WHERE key IS NOT NULL"
            )
            updated = 0
            for row in cc.fetchall():
                key, vendor_code, status, price_krw, ship_fee, ship_type, bundle_ship, om_sell = row
                product_status = _OC_STATUS_TO_PRODUCT_STATUS.get(status or "", "ACTIVE")
                pc.execute(
                    """UPDATE products SET
                         vendor_code            = ?,
                         oc_price               = ?,
                         oc_shipping_fee        = ?,
                         oc_shipping_type       = ?,
                         oc_bundle_ship         = ?,
                         oc_status              = ?,
                         oc_openmarket_sellable = ?,
                         product_status         = ?,
                         oc_synced_at           = ?
                       WHERE 상품코드 = ?""",
                    [vendor_code, price_krw, ship_fee, ship_type, bundle_ship,
                     status, om_sell, product_status, now_iso, key],
                )
                updated += prod_conn.execute("SELECT changes()").fetchone()[0]
            prod_conn.commit()
            result["updated"] = updated
        finally:
            prod_conn.close()
            cat_conn2.close()
    except Exception as exc:
        result["errors"].append(f"products.db 갱신 실패: {exc}")
        logger.exception("sync_from_existing products.db error")

    if progress_callback:
        progress_callback(len(all_keys), len(all_keys),
            f"완료 — {result['total_products']:,}개 조회 · {result['updated']:,}개 갱신 · "
            f"미발견 {result['not_found']:,}개")

    logger.info("sync_from_existing done: total=%d updated=%d not_found=%d errors=%d",
                result["total_products"], result["updated"],
                result["not_found"], len(result["errors"]))
    return result


# ── CSV 기반 전체 스캔 ────────────────────────────────────────────────────────

def scan_from_csv(csv_folder: str, progress_callback=None) -> dict:
    """selfcode CSV(상품코드+카테고리) → itemsByKeys → oc_catalog.db 전체 구축.

    allItems 페이지네이션 없이 CSV 키를 직접 사용 → Pass 1 생략.
    소요 예상: 9.4M / 5000배치 × ~20초 = 약 10시간.
    """
    import csv as _csv
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from godomall_register.ownerclan_client import OwnerclanClient, OwnerclanApiError
    from hub.services.db_service import get_db_path

    now_iso = datetime.now(timezone.utc).isoformat()
    result = {
        "total_keys": 0,
        "processed": 0,
        "unique_vendors": 0,
        "backfilled": 0,
        "errors": [],
        "scanned_at": now_iso,
    }

    catalog_path = get_catalog_db_path()
    client = OwnerclanClient()

    # ── CSV에서 전체 키 읽기 ──────────────────────────────────────────────────
    if progress_callback:
        progress_callback(0, 0, "CSV 파일에서 상품코드 읽는 중...")

    csv_folder_path = Path(csv_folder)
    all_keys: list[str] = []
    csv_files = sorted(csv_folder_path.glob("*.csv"))
    for csv_file in csv_files:
        try:
            with open(csv_file, encoding="cp949", errors="replace") as f:
                reader = _csv.reader(f)
                next(reader)  # 헤더 스킵
                for row in reader:
                    if row and row[0].strip():
                        all_keys.append(row[0].strip())
        except Exception as exc:
            result["errors"].append(f"CSV 읽기 실패 {csv_file.name}: {exc}")

    result["total_keys"] = len(all_keys)
    if progress_callback:
        progress_callback(0, len(all_keys), f"CSV 로드 완료: {len(all_keys):,}개 키")

    if not all_keys:
        result["errors"].append("CSV에서 키를 찾지 못했습니다.")
        return result

    # ── oc_catalog.db UPSERT (체크포인트 재개 지원) ───────────────────────────
    BATCH = 5000
    n_batches = (len(all_keys) + BATCH - 1) // BATCH

    cat_conn = sqlite3.connect(catalog_path)
    cat_conn.execute("PRAGMA journal_mode=WAL")
    try:
        _init_catalog(cat_conn)
        cat_cur = cat_conn.cursor()

        # 체크포인트: 이전 중단 지점 복원
        resume_row = cat_conn.execute(
            "SELECT value FROM oc_catalog_meta WHERE key='csv_scan_offset'"
        ).fetchone()
        start_offset = int(resume_row[0]) if resume_row else 0
        processed = start_offset
        if start_offset > 0 and progress_callback:
            progress_callback(start_offset, len(all_keys),
                f"이전 중단 지점 복원 — {start_offset:,}개부터 재개...")

        for bi, bs in enumerate(range(start_offset, len(all_keys), BATCH)):
            batch = all_keys[bs:bs + BATCH]
            bn = start_offset // BATCH + bi + 1

            if progress_callback:
                progress_callback(processed, len(all_keys),
                    f"배치 {bn:,}/{n_batches:,} OC 조회 중 ({bs+1:,}~{bs+len(batch):,})...")

            try:
                items = client.get_items_by_keys(batch, fields=CATALOG_FIELDS, timeout=180)
            except OwnerclanApiError as exc:
                result["errors"].append(f"배치 {bn}: {exc}")
                processed += len(batch)
                # 오류 배치도 오프셋 저장 (재시도보다 진행 우선)
                cat_conn.execute(
                    "INSERT OR REPLACE INTO oc_catalog_meta VALUES ('csv_scan_offset', ?)",
                    [str(processed)]
                )
                cat_conn.commit()
                continue

            for item in items:
                key = item.get("key", "")
                if not key:
                    continue
                metadata = item.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                grade = metadata.get("gradeDetail") or {}
                cat_cur.execute(
                    """INSERT INTO oc_items
                       (key, vendor_code, status, price_krw, shipping_fee,
                        shipping_type, bundle_shipping, openmarket_sellable,
                        release_rate, average_ship, last_scanned_at, last_updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(key) DO UPDATE SET
                         vendor_code         = excluded.vendor_code,
                         status              = excluded.status,
                         price_krw           = excluded.price_krw,
                         shipping_fee        = excluded.shipping_fee,
                         shipping_type       = excluded.shipping_type,
                         bundle_shipping     = excluded.bundle_shipping,
                         openmarket_sellable = excluded.openmarket_sellable,
                         release_rate        = excluded.release_rate,
                         average_ship        = excluded.average_ship,
                         last_updated_at     = excluded.last_updated_at""",
                    [key,
                     str(metadata["vendorKey"]) if metadata.get("vendorKey") else None,
                     item.get("status"), item.get("price"), item.get("shippingFee"),
                     item.get("shippingType"), metadata.get("bundleShipping"),
                     1 if item.get("openmarketSellable") else 0,
                     grade.get("releaseRate", ""), grade.get("averageShip", ""),
                     now_iso, now_iso],
                )

            processed += len(batch)
            result["processed"] = processed

            # 배치 완료마다 체크포인트 저장
            cat_cur.execute(
                "INSERT OR REPLACE INTO oc_catalog_meta VALUES ('csv_scan_offset', ?)",
                [str(processed)]
            )
            cat_conn.commit()

            if progress_callback:
                progress_callback(processed, len(all_keys),
                    f"배치 {bn:,}/{n_batches:,} 완료 ({processed:,}/{len(all_keys):,})")

        # 집계
        row = cat_conn.execute(
            "SELECT COUNT(DISTINCT vendor_code) FROM oc_items WHERE vendor_code IS NOT NULL"
        ).fetchone()
        result["unique_vendors"] = row[0] if row else 0

        # 메타 갱신 + 체크포인트 초기화 (완료)
        cat_cur.execute("INSERT OR REPLACE INTO oc_catalog_meta VALUES ('last_csv_scan_at', ?)", [now_iso])
        cat_cur.execute("INSERT OR REPLACE INTO oc_catalog_meta VALUES ('total_csv_keys', ?)", [str(result["total_keys"])])
        cat_cur.execute("DELETE FROM oc_catalog_meta WHERE key='csv_scan_offset'")
        cat_conn.commit()

    finally:
        cat_conn.close()

    # ── products.db backfill ──────────────────────────────────────────────────
    if progress_callback:
        progress_callback(processed, len(all_keys), "products.db backfill 중...")
    try:
        products_path = get_db_path()
        prod_conn = sqlite3.connect(products_path)
        cat_conn2 = sqlite3.connect(catalog_path)
        try:
            prod_conn.execute("PRAGMA journal_mode=WAL")
            pc = prod_conn.cursor()
            cc = cat_conn2.cursor()
            pc.execute("SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
            prod_keys = {r[0] for r in pc.fetchall()}
            cc.execute(
                "SELECT key, vendor_code, status, price_krw, shipping_fee, "
                "shipping_type, bundle_shipping, openmarket_sellable "
                "FROM oc_items WHERE key IS NOT NULL"
            )
            updated = 0
            for row in cc.fetchall():
                key, vendor_code, status, price_krw, ship_fee, ship_type, bundle_ship, om_sell = row
                if key not in prod_keys:
                    continue
                product_status = _OC_STATUS_TO_PRODUCT_STATUS.get(status or "", "ACTIVE")
                pc.execute(
                    """UPDATE products SET
                         vendor_code            = ?,
                         oc_price               = ?,
                         oc_shipping_fee        = ?,
                         oc_shipping_type       = ?,
                         oc_bundle_ship         = ?,
                         oc_status              = ?,
                         oc_openmarket_sellable = ?,
                         product_status         = ?,
                         oc_synced_at           = ?
                       WHERE 상품코드 = ?""",
                    [vendor_code, price_krw, ship_fee, ship_type, bundle_ship,
                     status, om_sell, product_status, now_iso, key],
                )
                updated += prod_conn.execute("SELECT changes()").fetchone()[0]
            prod_conn.commit()
            result["backfilled"] = updated
        finally:
            prod_conn.close()
            cat_conn2.close()
    except Exception as exc:
        result["errors"].append(f"backfill 실패: {exc}")
        logger.exception("scan_from_csv backfill error")

    if progress_callback:
        progress_callback(len(all_keys), len(all_keys),
            f"완료 — {result['total_keys']:,}개 · 공급사 {result['unique_vendors']:,}개 · "
            f"backfill {result['backfilled']:,}개")

    logger.info("scan_from_csv done: total=%d vendors=%d backfilled=%d errors=%d",
                result["total_keys"], result["unique_vendors"],
                result["backfilled"], len(result["errors"]))
    return result


# ── 백그라운드 스캔 (폴링용) ──────────────────────────────────────────────────

def _make_empty_state(job_type: str = "") -> dict:
    return {
        "running": False, "job_type": job_type,
        "message": "", "current": 0, "total": 0,
        "result": None, "error": None,
        "started_at": None, "finished_at": None,
    }


def _start_bg_job(job_type: str, target_fn, *args) -> dict:
    """공통 백그라운드 작업 시작 헬퍼."""
    global _scan_state
    with _scan_lock:
        if _scan_state["running"]:
            return {"started": False, "reason": "already_running", "state": dict(_scan_state)}
        _scan_state = {
            "running": True, "job_type": job_type,
            "message": "시작 중...", "current": 0, "total": 0,
            "result": None, "error": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
        }

    def _progress(current: int, total: int, message: str) -> None:
        _scan_state["current"] = current
        _scan_state["total"] = total
        _scan_state["message"] = message

    def _run() -> None:
        try:
            result = target_fn(*args, progress_callback=_progress)
            _scan_state["result"] = result
            _scan_state["error"] = None
        except Exception as exc:
            _scan_state["error"] = str(exc)
            _scan_state["message"] = f"오류: {exc}"
            logger.exception("background job '%s' failed", job_type)
        finally:
            _scan_state["running"] = False
            _scan_state["finished_at"] = datetime.now(timezone.utc).isoformat()

    threading.Thread(target=_run, daemon=True, name=f"oc_{job_type}").start()
    return {"started": True, "state": dict(_scan_state)}


def start_catalog_scan_bg() -> dict:
    """전체 OC 카탈로그 스캔 (allItems) 백그라운드 시작."""
    return _start_bg_job("full_scan", catalog_full_scan)


def start_sync_existing_bg() -> dict:
    """기존 products.db 상품코드 기준 OC 동기화 백그라운드 시작."""
    return _start_bg_job("sync_existing", sync_from_existing_products)


def start_scan_from_csv_bg(csv_folder: str) -> dict:
    """CSV 파일 기반 전체 OC 스캔 백그라운드 시작 (~10시간). 중단 시 자동 재개."""
    # 이전 체크포인트 확인
    catalog_path = get_catalog_db_path()
    resume_offset = 0
    if Path(catalog_path).exists():
        try:
            conn = sqlite3.connect(catalog_path)
            row = conn.execute(
                "SELECT value FROM oc_catalog_meta WHERE key='csv_scan_offset'"
            ).fetchone()
            resume_offset = int(row[0]) if row else 0
            conn.close()
        except Exception:
            pass
    result = _start_bg_job("csv_scan", scan_from_csv, csv_folder)
    if result.get("started") and resume_offset > 0:
        result["resumed_from"] = resume_offset
    return result


def get_scan_status() -> dict:
    """현재 작업 상태 반환 (폴링용)."""
    return dict(_scan_state)


# ── SSE 스트림 ────────────────────────────────────────────────────────────────

def _sse(event_dict: dict) -> str:
    return f"data: {json.dumps(event_dict, ensure_ascii=False)}\n\n"


async def catalog_scan_stream() -> AsyncGenerator[str, None]:
    """catalog_full_scan()을 쓰레드에서 실행하며 SSE 이벤트를 yield."""
    yield _sse({"type": "start", "message": "OC 전체 카탈로그 스캔 시작..."})

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def progress_cb(current: int, total: int, message: str) -> None:
        event = {"type": "progress", "current": current, "total": total, "message": message}
        asyncio.run_coroutine_threadsafe(queue.put(event), loop)

    def run_scan():
        try:
            result = catalog_full_scan(progress_callback=progress_cb)
            done = {
                "type": "done",
                "message": "스캔 완료",
                "total_oc":       result.get("total_oc", 0),
                "unique_vendors": result.get("unique_vendors", 0),
                "backfilled":     result.get("backfilled", 0),
                "errors":         result.get("errors", []),
            }
        except Exception as exc:
            done = {"type": "error", "message": str(exc)}
        asyncio.run_coroutine_threadsafe(queue.put(done), loop)
        asyncio.run_coroutine_threadsafe(queue.put(_DONE_SENTINEL), loop)

    future = loop.run_in_executor(_executor, run_scan)

    while True:
        try:
            event = await asyncio.wait_for(queue.get(), timeout=15.0)
        except asyncio.TimeoutError:
            yield _sse({"type": "ping"})
            continue
        if event is _DONE_SENTINEL:
            break
        yield _sse(event)

    await future
