"""vendor_scan + discovery_scan 테스트."""
import os
import sqlite3
import json
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path
    # 테스트 간 공급사 격리: vendors 테이블 초기화
    conn = sqlite3.connect(test_db_path)
    conn.execute("DELETE FROM vendors")
    conn.commit()
    conn.close()
    yield
    # teardown: 다음 테스트를 위한 정리
    conn = sqlite3.connect(test_db_path)
    conn.execute("DELETE FROM vendors")
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def clean_catalog_artifacts(test_db_path):
    """vendor_scan이 backfill한 상품 제거 (세션 DB 오염 방지)."""
    yield
    conn = sqlite3.connect(test_db_path)
    # 원래 seed 상품 (W001-W004)만 유지
    conn.execute("DELETE FROM products WHERE 상품코드 NOT IN ('W001', 'W002', 'W003', 'W004')")
    conn.execute("DELETE FROM vendors")
    conn.commit()
    conn.close()


@pytest.fixture
def catalog_db(tmp_path):
    """임시 oc_catalog.db."""
    db_path = str(tmp_path / "oc_catalog_test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS oc_items (
            key TEXT PRIMARY KEY, vendor_code TEXT, status TEXT,
            price_krw INTEGER, shipping_fee INTEGER, shipping_type TEXT,
            bundle_shipping TEXT, openmarket_sellable INTEGER,
            release_rate TEXT, average_ship TEXT,
            last_scanned_at TEXT, last_updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS oc_catalog_meta (
            key TEXT PRIMARY KEY, value TEXT
        );
    """)
    conn.close()
    return db_path


def _make_oc_item(key: str, vendor_code: str, price: int = 10000) -> dict:
    """OC API 응답 모의 아이템."""
    return {
        "key": key,
        "status": "available",
        "price": price,
        "shippingFee": 3000,
        "shippingType": "FREE_ABOVE",
        "openmarketSellable": True,
        "metadata": {"vendorKey": vendor_code, "bundleShipping": "Y",
                     "gradeDetail": {"releaseRate": "98", "averageShip": "1.2"}},
    }


def _seed_vendor(test_db_path, vendor_code="V001", vendor_name="테스트공급사", status="active"):
    conn = sqlite3.connect(test_db_path)
    conn.execute(
        """INSERT OR IGNORE INTO vendors (vendor_code, vendor_name, status, source)
           VALUES (?, ?, ?, 'oc')""",
        [vendor_code, vendor_name, status],
    )
    conn.commit()
    conn.close()


# ── vendor_scan 테스트 ──

def test_vendor_scan_scans_active_vendors(test_db_path, catalog_db):
    """등록된 active 공급사의 상품이 oc_catalog.db에 저장되는지 확인."""
    _seed_vendor(test_db_path, "V001", "테스트공급사A")

    mock_items = [_make_oc_item("W100", "V001", 15000), _make_oc_item("W101", "V001", 20000)]

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = mock_items

        from hub.services.catalog_service import vendor_scan
        result = vendor_scan()

    assert result["errors"] == []
    assert result["scanned_vendors"] == 1
    assert result["total_items"] >= 2

    conn = sqlite3.connect(catalog_db)
    count = conn.execute("SELECT COUNT(*) FROM oc_items WHERE vendor_code='V001'").fetchone()[0]
    conn.close()
    assert count == 2


def test_vendor_scan_skips_inactive_vendors(test_db_path, catalog_db):
    """inactive 공급사는 스캔하지 않음."""
    _seed_vendor(test_db_path, "V002", "비활성공급사", status="inactive")

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = []

        from hub.services.catalog_service import vendor_scan
        result = vendor_scan()

    assert result["scanned_vendors"] == 0
    instance.search_items.assert_not_called()


def test_vendor_scan_detects_new_items(test_db_path, catalog_db):
    """oc_catalog.db에 없던 상품은 new_items로 카운트."""
    _seed_vendor(test_db_path, "V003")

    # 기존 아이템 1개 seed
    conn = sqlite3.connect(catalog_db)
    conn.execute(
        "INSERT INTO oc_items (key, vendor_code, status) VALUES ('W200', 'V003', 'available')")
    conn.commit()
    conn.close()

    mock_items = [_make_oc_item("W200", "V003"), _make_oc_item("W201", "V003")]

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = mock_items

        from hub.services.catalog_service import vendor_scan
        result = vendor_scan()

    assert result["errors"] == []
    assert result["new_items"] >= 1  # W201 is new


def test_vendor_scan_endpoint_returns_started(client, test_db_path):
    """POST /api/catalog/vendor-scan/start 가 started=True 반환."""
    with patch("hub.services.catalog_service._scan_state", {"running": False}), \
         patch("hub.services.catalog_service._scan_lock"), \
         patch("hub.services.catalog_service.vendor_scan", return_value={}):
        r = client.post("/api/catalog/vendor-scan/start")
    assert r.status_code == 200
    assert r.json().get("started") is True
