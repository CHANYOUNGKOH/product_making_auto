"""Ownerclan → product.db sync: 도매가/배송비/상태 동기화."""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# Fields to request from ownerclan API via get_items_by_keys()
SYNC_FIELDS = "key name price(currency: KRW) status shippingFee shippingType metadata"

# DB column definitions: (column_name, sql_type, default_value)
OC_COLUMNS = [
    ("oc_price", "INTEGER", None),
    ("oc_shipping_fee", "INTEGER", None),
    ("oc_shipping_type", "TEXT", None),
    ("oc_bundle_ship", "TEXT", None),
    ("oc_status", "TEXT", None),
    ("oc_prev_price", "INTEGER", None),
    ("oc_prev_ship_fee", "INTEGER", None),
    ("oc_synced_at", "TEXT", None),
    ("oc_changed_at", "TEXT", None),
]

# Default DB path (same as DB_save/config.py FIXED_DB_PATH)
DEFAULT_DB_PATH = (
    Path(__file__).resolve().parent.parent / "DB_save" / "products.db"
)


# ── Schema Migration ────────────────────────────────────────────────────────


def _ensure_oc_columns(conn: sqlite3.Connection) -> None:
    """Add oc_* columns to products table if they don't exist."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(products)")
    existing = {row[1] for row in cursor.fetchall()}

    for col_name, col_type, _ in OC_COLUMNS:
        if col_name not in existing:
            cursor.execute(
                f"ALTER TABLE products ADD COLUMN {col_name} {col_type}"
            )
            logger.info("Added column products.%s (%s)", col_name, col_type)

    conn.commit()


# ── DB Helpers ───────────────────────────────────────────────────────────────


def _load_existing_oc_values(
    conn: sqlite3.Connection, product_codes: list[str]
) -> dict[str, dict]:
    """Load current oc_* values for given product codes.

    Returns: {product_code: {oc_price: ..., oc_shipping_fee: ..., ...}}
    """
    if not product_codes:
        return {}

    oc_col_names = [c[0] for c in OC_COLUMNS]
    cols = ", ".join(oc_col_names)

    result = {}
    # Query in chunks to avoid SQLite variable limit
    chunk_size = 500
    for i in range(0, len(product_codes), chunk_size):
        chunk = product_codes[i : i + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT 상품코드, {cols} FROM products "
            f"WHERE 상품코드 IN ({placeholders})",
            chunk,
        )
        for row in cursor.fetchall():
            code = row[0]
            result[code] = {
                oc_col_names[j]: row[j + 1] for j in range(len(oc_col_names))
            }
    return result


# ── Change Detection ─────────────────────────────────────────────────────────


def _extract_api_values(api_item: dict) -> dict:
    """Extract DB-relevant values from an API response item.

    Maps API field names to oc_* column values.
    metadata.bundleShipping → oc_bundle_ship (client auto-parses metadata).
    """
    metadata = api_item.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (ValueError, TypeError):
            metadata = {}

    return {
        "oc_price": api_item.get("price"),
        "oc_shipping_fee": api_item.get("shippingFee"),
        "oc_shipping_type": api_item.get("shippingType"),
        "oc_bundle_ship": metadata.get("bundleShipping"),
        "oc_status": api_item.get("status"),
    }


def _detect_changes(existing: dict, new_values: dict) -> dict:
    """Compare existing oc_* DB values with new API values.

    Returns dict of changed fields: {field_name: {"prev": old, "current": new}}
    Only reports changes when the existing value is not None (skip first sync).
    """
    changes = {}

    if (
        existing.get("oc_price") is not None
        and existing["oc_price"] != new_values["oc_price"]
    ):
        changes["price"] = {
            "prev": existing["oc_price"],
            "current": new_values["oc_price"],
        }

    if (
        existing.get("oc_shipping_fee") is not None
        and existing["oc_shipping_fee"] != new_values["oc_shipping_fee"]
    ):
        changes["shipping_fee"] = {
            "prev": existing["oc_shipping_fee"],
            "current": new_values["oc_shipping_fee"],
        }

    if (
        existing.get("oc_status") is not None
        and existing["oc_status"] != new_values["oc_status"]
    ):
        changes["status"] = {
            "prev": existing["oc_status"],
            "current": new_values["oc_status"],
        }

    return changes


# ── Public API ──────────────────────────────────────────────────────────────


def sync_products(
    db_path: str | Path | None = None,
    product_codes: list[str] | None = None,
    config_path: str | Path | None = None,
    progress_callback=None,
) -> dict:
    """Sync product.db oc_* columns with latest Ownerclan API data.

    Args:
        db_path: Path to products.db. None → DEFAULT_DB_PATH.
        product_codes: List of product codes to sync. None → all ACTIVE.
        config_path: Path to ownerclan_config.json. None → default.
        progress_callback: Optional callable(current, total, message) for progress.

    Returns:
        dict with keys: synced, price_changed, ship_changed,
        status_changed, not_found, errors, synced_at

    Note:
        If a product code appears in multiple rows (same code, different
        market_id), all rows are updated with the same oc_* values.
        This is intentional — wholesale price/status is per-product, not
        per-market.
    """
    from godomall_register.ownerclan_client import OwnerclanClient, OwnerclanApiError

    if db_path is None:
        db_path = DEFAULT_DB_PATH
    db_path = Path(db_path)

    # DB existence check
    if not db_path.exists():
        return {
            "synced": 0, "price_changed": [], "ship_changed": [],
            "status_changed": [], "not_found": [],
            "errors": [f"DB file not found: {db_path}"],
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }

    now_iso = datetime.now(timezone.utc).isoformat()

    result = {
        "synced": 0,
        "price_changed": [],
        "ship_changed": [],
        "status_changed": [],
        "not_found": [],
        "errors": [],
        "synced_at": now_iso,
    }

    # 1. DB connect + ensure columns
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_oc_columns(conn)

        # 2. Collect product codes
        if product_codes is None:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT 상품코드 FROM products "
                "WHERE product_status = 'ACTIVE' "
                "AND 상품코드 IS NOT NULL AND 상품코드 != ''"
            )
            product_codes = [row[0] for row in cursor.fetchall()]

        if not product_codes:
            logger.info("No product codes to sync")
            return result

        logger.info("Syncing %d product codes", len(product_codes))

        if progress_callback:
            progress_callback(0, len(product_codes), "API 호출 중...")

        # 3. Load existing oc_* values
        existing_map = _load_existing_oc_values(conn, product_codes)

        # 4. Call API
        client = OwnerclanClient(config_path=config_path)
        try:
            api_items = client.get_items_by_keys(
                product_codes, fields=SYNC_FIELDS, timeout=120
            )
        except OwnerclanApiError as exc:
            result["errors"].append(str(exc))
            logger.error("API call failed: %s", exc)
            return result

        # Build lookup by key
        api_map = {item["key"]: item for item in api_items if "key" in item}

        # 5. Detect changes + build updates
        update_cursor = conn.cursor()

        for code in product_codes:
            api_item = api_map.get(code)
            if api_item is None:
                result["not_found"].append(code)
                continue

            new_values = _extract_api_values(api_item)
            existing = existing_map.get(code, {})
            changes = _detect_changes(existing, new_values)

            # Build UPDATE SET clause
            update_fields = {
                "oc_price": new_values["oc_price"],
                "oc_shipping_fee": new_values["oc_shipping_fee"],
                "oc_shipping_type": new_values["oc_shipping_type"],
                "oc_bundle_ship": new_values["oc_bundle_ship"],
                "oc_status": new_values["oc_status"],
                "oc_synced_at": now_iso,
            }

            if changes:
                update_fields["oc_changed_at"] = now_iso

                if "price" in changes:
                    update_fields["oc_prev_price"] = changes["price"]["prev"]
                    result["price_changed"].append({
                        "code": code,
                        "prev": changes["price"]["prev"],
                        "current": changes["price"]["current"],
                    })

                if "shipping_fee" in changes:
                    update_fields["oc_prev_ship_fee"] = changes["shipping_fee"]["prev"]
                    result["ship_changed"].append({
                        "code": code,
                        "prev": changes["shipping_fee"]["prev"],
                        "current": changes["shipping_fee"]["current"],
                        "type": new_values["oc_shipping_type"],
                    })

                if "status" in changes:
                    result["status_changed"].append({
                        "code": code,
                        "prev": changes["status"]["prev"],
                        "current": changes["status"]["current"],
                    })

            # Execute UPDATE
            set_clause = ", ".join(f"{k} = ?" for k in update_fields)
            values = list(update_fields.values()) + [code]
            update_cursor.execute(
                f"UPDATE products SET {set_clause} WHERE 상품코드 = ?",
                values,
            )

            result["synced"] += 1

            if progress_callback and result["synced"] % 100 == 0:
                progress_callback(
                    result["synced"],
                    len(product_codes),
                    f"{result['synced']}/{len(product_codes)} 동기화 중...",
                )

        conn.commit()
        logger.info(
            "Sync complete: %d synced, %d price changes, %d status changes, %d not found",
            result["synced"],
            len(result["price_changed"]),
            len(result["status_changed"]),
            len(result["not_found"]),
        )

    finally:
        conn.close()

    return result


if __name__ == "__main__":
    """Smoke test — 실제 API + 실제 DB로 동기화 검증"""
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Resolve DB path
    db_path = DEFAULT_DB_PATH
    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1])

    if not db_path.exists():
        print(f"DB not found: {db_path}")
        sys.exit(1)

    print("=" * 60)
    print("오너클랜 DB 동기화 Smoke Test")
    print(f"DB: {db_path}")
    print("=" * 60)

    # 1. Check current ACTIVE count
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(DISTINCT 상품코드) FROM products "
        "WHERE product_status = 'ACTIVE' "
        "AND 상품코드 IS NOT NULL AND 상품코드 != ''"
    )
    total_active = cursor.fetchone()[0]
    conn.close()
    print(f"\n[1] ACTIVE 상품코드: {total_active}개")

    # 2. Sync a small sample first (max 10)
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT 상품코드 FROM products "
        "WHERE product_status = 'ACTIVE' "
        "AND 상품코드 IS NOT NULL AND 상품코드 != '' "
        "LIMIT 10"
    )
    sample_codes = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not sample_codes:
        print("  ACTIVE 상품 없음 — 테스트 불가")
        sys.exit(0)

    print(f"\n[2] 샘플 동기화 ({len(sample_codes)}건)")
    print(f"  Codes: {sample_codes[:5]}{'...' if len(sample_codes) > 5 else ''}")

    result = sync_products(db_path=db_path, product_codes=sample_codes)

    print(f"  Synced: {result['synced']}")
    print(f"  Price changed: {len(result['price_changed'])}")
    for pc in result["price_changed"]:
        print(f"    {pc['code']}: {pc['prev']}원 → {pc['current']}원")
    print(f"  Shipping changed: {len(result['ship_changed'])}")
    for sc in result["ship_changed"]:
        print(f"    {sc['code']}: {sc['prev']}원 → {sc['current']}원 ({sc['type']})")
    print(f"  Status changed: {len(result['status_changed'])}")
    for st in result["status_changed"]:
        print(f"    {st['code']}: {st['prev']} → {st['current']}")
    print(f"  Not found: {len(result['not_found'])}")
    if result["not_found"]:
        print(f"    {result['not_found'][:5]}")
    print(f"  Errors: {len(result['errors'])}")

    # 3. Verify DB was updated
    print(f"\n[3] DB 확인")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    for code in sample_codes[:3]:
        cursor.execute(
            "SELECT 상품코드, oc_price, oc_shipping_fee, oc_shipping_type, "
            "oc_bundle_ship, oc_status, oc_synced_at FROM products "
            "WHERE 상품코드 = ? LIMIT 1",
            (code,),
        )
        row = cursor.fetchone()
        if row:
            print(
                f"  {row['상품코드']}: "
                f"price={row['oc_price']}, "
                f"ship={row['oc_shipping_fee']} ({row['oc_shipping_type']}), "
                f"bundle={row['oc_bundle_ship']}, "
                f"status={row['oc_status']}, "
                f"synced={row['oc_synced_at']}"
            )
    conn.close()

    # 4. Test re-sync (should detect no changes since data hasn't changed)
    print(f"\n[4] 재동기화 (변동 없음 확인)")
    result2 = sync_products(db_path=db_path, product_codes=sample_codes[:3])
    print(f"  Synced: {result2['synced']}")
    print(f"  Price changed: {len(result2['price_changed'])} (expected: 0)")
    print(f"  Status changed: {len(result2['status_changed'])} (expected: 0)")

    # 5. Full sync option
    if total_active > 10:
        print(f"\n[5] 전체 ACTIVE 동기화 ({total_active}건)?")
        answer = input("    실행? (y/N): ").strip().lower()
        if answer == "y":
            import time

            start = time.time()
            result_full = sync_products(db_path=db_path)
            elapsed = time.time() - start
            print(f"  Synced: {result_full['synced']}/{total_active} ({elapsed:.1f}초)")
            print(f"  Price changed: {len(result_full['price_changed'])}")
            print(f"  Status changed: {len(result_full['status_changed'])}")
            print(f"  Not found: {len(result_full['not_found'])}")

    print("\n" + "=" * 60)
    print("Smoke test complete!")
    print("=" * 60)
