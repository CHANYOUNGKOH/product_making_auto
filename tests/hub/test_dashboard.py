"""대시보드 API 테스트."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


def test_root_serves_html(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_dashboard_returns_stats(client):
    response = client.get("/api/dashboard")
    assert response.status_code == 200
    data = response.json()
    assert data["total_synced"] == 3     # W001, W002, W004 (oc_price IS NOT NULL)
    assert data["oc_available"] == 2     # W001, W002
    assert data["oc_unavailable"] == 1   # W004
    assert data["text_done"] == 2        # W001, W002 (available + ST4_마켓상품명)
    assert data["image_done"] == 1       # W001 (누끼+연출 모두)
    assert data["image_partial"] == 1    # W002 (누끼만)
    assert data["shippable"] == 2        # W001, W002 (available + 텍스트 + 가격)
    assert "last_sync_at" in data


def test_dashboard_stat_values_are_ints(client):
    data = client.get("/api/dashboard").json()
    for key in ("total_synced", "oc_available", "text_done", "image_done", "image_partial", "shippable"):
        assert isinstance(data[key], int), f"{key} should be int"


def test_dashboard_has_processing_stats(client):
    r = client.get("/api/dashboard")
    data = r.json()
    for key in ["total_synced", "oc_available", "oc_soldout", "oc_discontinued", "oc_unavailable",
                "text_done", "image_done", "image_partial", "shippable",
                "shipping_free", "shipping_conditional", "shipping_paid"]:
        assert key in data, f"Missing key: {key}"
        assert isinstance(data[key], int), f"{key} should be int"
