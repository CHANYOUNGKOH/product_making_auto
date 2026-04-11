"""파이프라인 API 테스트."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


def test_pipeline_status_returns_dict(client):
    r = client.get("/api/pipeline/status")
    assert r.status_code == 200
    data = r.json()
    assert "oc_synced_count" in data


def test_sync_stream_responds_with_sse(client):
    """SSE 엔드포인트가 text/event-stream 반환 확인."""
    with client.stream("GET", "/api/pipeline/sync/stream?dry=true") as r:
        assert r.status_code == 200
        assert "text/event-stream" in r.headers.get("content-type", "")
