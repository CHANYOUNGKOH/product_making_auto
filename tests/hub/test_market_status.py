# tests/hub/test_market_status.py
"""마켓 현황 API 테스트."""
import io
import os
import sqlite3
import pytest
import openpyxl


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


@pytest.fixture(autouse=True)
def clean_registrations(test_db_path):
    yield
    conn = sqlite3.connect(test_db_path)
    conn.execute("DELETE FROM market_registrations")
    conn.commit()
    conn.close()


def _seed_ready_registration(test_db_path, code="W001", store="고도몰A1-1", market="고도몰"):
    con = sqlite3.connect(test_db_path)
    con.execute(
        """INSERT OR IGNORE INTO market_registrations
           (상품코드, store_alias, market, status, created_at)
           VALUES (?, ?, ?, 'READY', datetime('now','localtime'))""",
        [code, store, market],
    )
    con.commit()
    con.close()


def _make_import_excel(codes: list[str]) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["상품코드", "상품명", "판매가"])
    for c in codes:
        ws.append([c, f"상품{c}", 10000])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


def test_get_market_status_empty(client):
    r = client.get("/api/market-status")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_import_updates_uploaded(client, test_db_path):
    _seed_ready_registration(test_db_path, "W001")
    excel = _make_import_excel(["W001"])
    r = client.post(
        "/api/market-status/import",
        data={"store_alias": "고도몰A1-1", "market": "고도몰"},
        files={"file": ("고도몰_상품목록.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["updated"] == 1
    assert data["failed"] == 0


def test_import_marks_failed_if_not_in_excel(client, test_db_path):
    _seed_ready_registration(test_db_path, "W002", store="고도몰A1-1")
    excel = _make_import_excel([])  # W002 없음
    r = client.post(
        "/api/market-status/import",
        data={"store_alias": "고도몰A1-1", "market": "고도몰"},
        files={"file": ("고도몰_상품목록.xlsx", excel,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["failed"] >= 1
