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
