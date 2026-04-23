"""image-genai Hub API 테스트."""
import os

import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


def test_image_genai_summary_returns_status_counts(client):
    r = client.get("/api/image-genai/summary")
    assert r.status_code == 200
    data = r.json()
    assert data["total_targets"] == 3
    assert data["status_counts"]["done"] == 1
    assert data["status_counts"]["failed"] == 1
    assert data["status_counts"]["running"] == 1
    assert data["lane_counts"]["generated"] == 1
    assert data["lane_counts"]["codex"] == 1


def test_image_genai_targets_can_filter_failed(client):
    r = client.get("/api/image-genai/targets?status=failed")
    assert r.status_code == 200
    data = r.json()
    codes = [item["code"] for item in data["items"]]
    assert codes == ["W002"]


def test_image_genai_reset_stale_reverts_running_to_pending(client):
    r = client.post("/api/image-genai/reset-stale", json={"stale_minutes": 30})
    assert r.status_code == 200
    data = r.json()
    assert data["reset_count"] == 1

    r2 = client.get("/api/image-genai/targets?status=pending")
    assert r2.status_code == 200
    codes = [item["code"] for item in r2.json()["items"]]
    assert "W003" in codes


def test_image_genai_retry_clears_failed_error_and_sets_pending(client):
    r = client.post("/api/image-genai/retry", json={"code": "W002"})
    assert r.status_code == 200
    data = r.json()
    assert data["code"] == "W002"
    assert data["status"] == "pending"

    r2 = client.get("/api/image-genai/targets?status=pending")
    assert r2.status_code == 200
    pending = {item["code"]: item for item in r2.json()["items"]}
    assert pending["W002"]["genai_error"] in ("", None)


def test_image_genai_issue_roundtrip(client):
    save = client.post("/api/image-genai/issue", json={"code": "W002", "memo": "사람 손 위치 어색함"})
    assert save.status_code == 200
    assert save.json()["saved"] is True

    load = client.get("/api/image-genai/issue/W002")
    assert load.status_code == 200
    assert load.json()["memo"] == "사람 손 위치 어색함"
