"""공유 픽스처: 테스트 DB + FastAPI TestClient."""
import os
import sqlite3
import pytest
from fastapi.testclient import TestClient


PRODUCTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT,
    상품코드 TEXT NOT NULL,
    product_names_json TEXT,
    카테고리명 TEXT,
    product_status TEXT DEFAULT 'ACTIVE',
    oc_price INTEGER,
    oc_options_json TEXT,
    oc_content TEXT,
    oc_images_json TEXT,
    oc_synced_at TEXT,
    vendor_code TEXT,
    oc_shipping_fee INTEGER,
    oc_shipping_type TEXT,
    oc_bundle_ship TEXT,
    oc_status TEXT,
    oc_openmarket_sellable INTEGER,
    text_status TEXT,
    image_status TEXT,
    export_log TEXT DEFAULT '[]',
    registered_stores TEXT DEFAULT '[]',
    ST4_마켓상품명 TEXT,
    누끼url TEXT,
    연출url TEXT
);
"""

STORES_SCHEMA = """
CREATE TABLE IF NOT EXISTS stores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alias TEXT UNIQUE NOT NULL,
    market TEXT,
    group_id TEXT,
    login_id TEXT,
    active INTEGER DEFAULT 1,
    strategy TEXT,
    slot_count INTEGER,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
);
"""

STORE_CATEGORY_ASSIGNMENTS_SCHEMA = """
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

MARKET_REGISTRATIONS_SCHEMA = """
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

VENDORS_SCHEMA = """
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
);
"""


def _seed_products(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """INSERT INTO products
           (상품코드, product_names_json, 카테고리명, product_status, oc_price,
            text_status, image_status, oc_shipping_type,
            ST4_마켓상품명, 누끼url, 연출url)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            # W001: 텍스트완료 + 이미지완료(누끼+연출) + 가격 있음 → text_done, image_done, shippable / FREE
            ("W001", '{"name":"테스트상품A"}', "가전/디지털>TV", "ACTIVE", 50000, "done", "done",
             "FREE", "마켓상품명A", "https://cdn/W001_누끼.jpg", "https://cdn/W001_연출.jpg"),
            # W002: 텍스트완료 + 누끼만(연출없음) + 가격 있음 → text_done, image_partial / FREE_ABOVE
            ("W002", '{"name":"테스트상품B"}', "가전/디지털>냉장고", "ACTIVE", 30000, "done", None,
             "FREE_ABOVE", "마켓상품명B", "https://cdn/W002_누끼.jpg", None),
            # W003: 텍스트없음 + 이미지없음 + 가격없음 → 아무것도 해당 안 됨 / PAID
            ("W003", '{"name":"테스트상품C"}', "생활/주방>청소", "ACTIVE", None, None, None,
             "PAID", None, None, None),
            # W004: INACTIVE → 모든 ACTIVE 기준 집계에서 제외
            ("W004", '{"name":"비활성상품"}', "가전/디지털>TV", "INACTIVE", 10000, None, None,
             "FREE", None, None, None),
        ],
    )
    conn.commit()


def _seed_stores(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT INTO stores (alias, market, group_id, strategy) VALUES (?, ?, ?, ?)",
        [
            ("고도몰A1-1", "고도몰", "A1", "lowest_price"),
            ("스마트스토어A1-1", "스마트스토어", "A1", "normal_sale"),
            ("옥션A2-1", "옥션", "A2", "cpc_ad"),
        ],
    )
    conn.commit()


@pytest.fixture(scope="session")
def test_db_path(tmp_path_factory):
    """세션 범위 임시 DB 파일."""
    db_file = tmp_path_factory.mktemp("hub_test") / "test_products.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript(
        PRODUCTS_SCHEMA + STORES_SCHEMA +
        STORE_CATEGORY_ASSIGNMENTS_SCHEMA + MARKET_REGISTRATIONS_SCHEMA +
        VENDORS_SCHEMA
    )
    _seed_products(conn)
    _seed_stores(conn)
    conn.close()
    return str(db_file)


@pytest.fixture(scope="session")
def client(test_db_path):
    """FastAPI TestClient (DB 경로 환경변수로 주입)."""
    os.environ["HUB_DB_PATH"] = test_db_path
    from hub.app import app
    with TestClient(app) as c:
        yield c
    os.environ.pop("HUB_DB_PATH", None)
