# tests/hub/test_register.py
"""카테고리-스토어 배정 API 테스트."""
import os
import sqlite3
import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


@pytest.fixture(autouse=True)
def clean_assignments(test_db_path):
    """テスト間でstore_category_assignmentsをクリア."""
    yield
    conn = sqlite3.connect(test_db_path)
    conn.execute("DELETE FROM store_category_assignments")
    conn.commit()
    conn.close()


def test_get_assignments_empty(client):
    r = client.get("/api/register/assignments/고도몰A1-1")
    assert r.status_code == 200
    assert r.json() == []


def test_add_assignment(client):
    r = client.post("/api/register/assignments", json={
        "store_alias": "고도몰A1-1",
        "oc_category_name": "생활/건강>문구/사무용품",
    })
    assert r.status_code == 200
    data = r.json()
    assert data["oc_category_name"] == "생활/건강>문구/사무용품"
    assert "conflict_stores" in data


def test_add_assignment_conflict_detected(client):
    """같은 market_group(naver)에 두 스토어가 같은 카테고리 배정 시 conflict 반환."""
    client.post("/api/register/assignments", json={
        "store_alias": "고도몰A1-1",
        "oc_category_name": "테스트카테고리",
    })
    r = client.post("/api/register/assignments", json={
        "store_alias": "스마트스토어A1-1",
        "oc_category_name": "테스트카테고리",
    })
    assert r.status_code == 200
    data = r.json()
    assert "고도몰A1-1" in data["conflict_stores"]


def test_remove_assignment(client):
    client.post("/api/register/assignments", json={
        "store_alias": "고도몰A1-1",
        "oc_category_name": "삭제테스트",
    })
    r = client.delete("/api/register/assignments/고도몰A1-1",
                      params={"oc_category_name": "삭제테스트"})
    assert r.status_code == 200
    assert r.json()["deleted"] is True


def test_get_oc_categories(client):
    r = client.get("/api/register/oc-categories")
    assert r.status_code == 200
    cats = r.json()
    assert isinstance(cats, list)
    # conftest seed: 가전/디지털>TV (W001 ACTIVE oc_price=50000), 가전/디지털>냉장고 (W002)
    names = [c["oc_category_name"] for c in cats]
    assert "가전/디지털>TV" in names
