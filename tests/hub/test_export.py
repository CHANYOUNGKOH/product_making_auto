"""Export API 테스트."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


def test_dry_run_returns_items(client):
    payload = {
        "categories": ["가전/디지털>TV"],
        "stores": ["고도몰A1-1"],
        "strategy": "lowest_price",
    }
    r = client.post("/api/export/dry-run", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert "items" in data and "total" in data


def test_dry_run_only_includes_shippable(client):
    """oc_price 없는 상품(W003)은 dry-run 결과에서 제외."""
    payload = {
        "categories": [],
        "stores": ["고도몰A1-1"],
        "strategy": "lowest_price",
    }
    r = client.post("/api/export/dry-run", json=payload)
    data = r.json()
    codes = [item["상품코드"] for item in data["items"]]
    assert "W003" not in codes


def test_dry_run_item_has_price_fields(client):
    payload = {
        "categories": ["가전/디지털>TV"],
        "stores": ["고도몰A1-1"],
        "strategy": "lowest_price",
    }
    r = client.post("/api/export/dry-run", json=payload)
    data = r.json()
    if data["items"]:
        item = data["items"][0]
        assert "상품코드" in item
        assert "oc_price" in item
        assert "sell_price" in item


def test_dry_run_empty_categories_uses_all(client):
    """categories=[] 이면 전체 카테고리 상품을 대상으로 한다."""
    payload = {"categories": [], "stores": ["고도몰A1-1"], "strategy": "lowest_price"}
    r = client.post("/api/export/dry-run", json=payload)
    data = r.json()
    assert data["total"] >= 2  # W001, W002 at least (both have oc_price)
