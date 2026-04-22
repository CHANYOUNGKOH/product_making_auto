"""상품 DB API 테스트."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


def test_get_products_default(client):
    r = client.get("/api/products")
    assert r.status_code == 200
    data = r.json()
    assert "items" in data and "total" in data
    assert data["total"] == 3  # 3 ACTIVE products


def test_get_products_search(client):
    r = client.get("/api/products?q=상품A")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["items"][0]["상품코드"] == "W001"


def test_get_products_category(client):
    r = client.get("/api/products?category=가전/디지털>TV")
    assert r.status_code == 200
    data = r.json()
    codes = [p["상품코드"] for p in data["items"]]
    assert "W001" in codes
    assert "W002" not in codes


def test_get_products_quick_filter(client):
    r = client.get("/api/products?quick_filter=has_oc_price")
    data = r.json()
    codes = [p["상품코드"] for p in data["items"]]
    assert "W003" not in codes


def test_get_products_pagination(client):
    r = client.get("/api/products?page=1&per_page=2")
    data = r.json()
    assert len(data["items"]) == 2
    assert data["total"] == 3


def test_product_item_has_required_fields(client):
    r = client.get("/api/products")
    item = r.json()["items"][0]
    for field in ("상품코드", "상품명", "카테고리명", "oc_price",
                  "text_status", "image_status", "export_log", "oc_synced_at"):
        assert field in item, f"Missing field: {field}"


def test_get_categories(client):
    r = client.get("/api/products/categories")
    assert r.status_code == 200
    cats = r.json()
    assert isinstance(cats, list)
    assert "가전/디지털>TV" in cats


def test_get_products_quick_filter_no_market(client):
    """export_log이 빈 상품만 반환."""
    r = client.get("/api/products?quick_filter=no_market")
    data = r.json()
    # W001/W002/W003 모두 export_log='[]' (seed에서 비어있음) → 3개
    assert data["total"] == 3


def test_get_products_unknown_quick_filter_returns_400(client):
    r = client.get("/api/products?quick_filter=invalid_filter")
    assert r.status_code == 400
