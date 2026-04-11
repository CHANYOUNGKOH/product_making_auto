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
    text_status TEXT,
    image_status TEXT,
    export_log TEXT DEFAULT '[]',
    registered_stores TEXT DEFAULT '[]'
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


def _seed_products(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """INSERT INTO products
           (상품코드, product_names_json, 카테고리명, product_status, oc_price,
            text_status, image_status)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("W001", '{"name":"테스트상품A"}', "가전/디지털>TV", "ACTIVE", 50000, "done", "done"),
            ("W002", '{"name":"테스트상품B"}', "가전/디지털>냉장고", "ACTIVE", 30000, "done", None),
            ("W003", '{"name":"테스트상품C"}', "생활/주방>청소", "ACTIVE", None, None, None),
            ("W004", '{"name":"비활성상품"}', "가전/디지털>TV", "INACTIVE", 10000, None, None),
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
    conn.executescript(PRODUCTS_SCHEMA + STORES_SCHEMA)
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
