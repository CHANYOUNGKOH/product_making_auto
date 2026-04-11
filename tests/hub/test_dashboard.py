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
    assert data["total_active"] == 3
    assert data["shippable"] == 2
    assert data["processed"] == 1
    assert "last_sync_at" in data


def test_dashboard_stat_values_are_ints(client):
    data = client.get("/api/dashboard").json()
    for key in ("total_active", "shippable", "processed"):
        assert isinstance(data[key], int), f"{key} should be int"
