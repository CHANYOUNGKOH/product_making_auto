"""스토어 관리 API 테스트."""
import io
import os
import pytest
import openpyxl


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


def test_get_stores_returns_list(client):
    r = client.get("/api/stores")
    assert r.status_code == 200
    stores = r.json()
    assert isinstance(stores, list)
    assert len(stores) >= 3  # conftest에 3개 seed


def test_store_has_required_fields(client):
    stores = client.get("/api/stores").json()
    for field in ("alias", "market", "group_id", "strategy", "active"):
        assert field in stores[0], f"Missing field: {field}"


def _make_excel_bytes() -> bytes:
    """Market_id_pw.xlsx 최소 구조 모의."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "allways_info"
    ws.append(["별칭", "마켓", "그룹"])
    ws.append(["고도몰테스트-1", "고도몰", "A1"])
    ws.append(["스마트테스트-1", "스마트스토어", "A1"])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


def test_import_stores_from_excel(client):
    excel_data = _make_excel_bytes()
    r = client.post(
        "/api/stores/import",
        files={"file": ("Market_id_pw.xlsx", excel_data,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert r.status_code == 200
    data = r.json()
    assert "imported" in data


def test_import_stores_upserts(client):
    """동일 별칭으로 두 번 import해도 중복 없음."""
    excel_data = _make_excel_bytes()
    client.post("/api/stores/import",
                files={"file": ("Market_id_pw.xlsx", excel_data,
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
    r2 = client.post("/api/stores/import",
                     files={"file": ("Market_id_pw.xlsx", excel_data,
                                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
    assert r2.status_code == 200
