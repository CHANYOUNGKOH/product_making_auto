# tests/hub/test_catalog_categories.py
"""oc_category_markets 수집 테스트."""
import io
import os
import sqlite3
import tempfile
import pytest
import openpyxl


def _make_oc_excel_bytes() -> bytes:
    """OC 양식 최소 구조 모의."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "OWNERCLAN"
    # Row 0: 섹션 헤더
    ws.append(["상품기본정보", "", "", "카테고리정보", "", "", ""])
    # Row 1: 컬럼 헤더
    ws.append(["판매자상품코드1", "판매자상품코드2", "상품코드",
               "카테고리코드", "카테고리명", "마켓카테고리", "원본상품명"])
    # Row 2: 데이터
    ws.append(["W001", "ownerclan", "W001",
               "50003757", "생활/건강>문구/사무용품",
               "auction,71281100,생활>사무용품\ngmarket,300010585,생활>사무용품\nstorefarm,50003757,생활/건강>문구",
               "테스트상품"])
    # Row 3: 같은 카테고리 (중복 → 1개로 합쳐져야 함)
    ws.append(["W002", "ownerclan", "W002",
               "50003757", "생활/건강>문구/사무용품",
               "auction,71281100,생활>사무용품\ngmarket,300010585,생활>사무용품\nstorefarm,50003757,생활/건강>문구",
               "테스트상품B"])
    # Row 4: 다른 카테고리
    ws.append(["W003", "ownerclan", "W003",
               "12345678", "가전/디지털>TV",
               "st11,1010000,가전>TV\ncoupang,80000,가전>TV",
               "TV상품"])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


@pytest.fixture
def oc_excel_path(tmp_path):
    p = tmp_path / "oc_test.xlsx"
    p.write_bytes(_make_oc_excel_bytes())
    return str(p)


@pytest.fixture
def catalog_db_path(tmp_path):
    return str(tmp_path / "oc_catalog_test.db")


def test_fetch_categories_count(oc_excel_path, catalog_db_path):
    from godomall_register.market_category_fetch import fetch_from_oc_excel
    result = fetch_from_oc_excel(oc_excel_path, catalog_db_path)
    # 카테고리 2개 (50003757, 12345678)
    assert result["categories"] == 2


def test_fetch_deduplicates_same_category(oc_excel_path, catalog_db_path):
    from godomall_register.market_category_fetch import fetch_from_oc_excel
    fetch_from_oc_excel(oc_excel_path, catalog_db_path)
    con = sqlite3.connect(catalog_db_path)
    count = con.execute(
        "SELECT COUNT(DISTINCT oc_category_key) FROM oc_category_markets"
    ).fetchone()[0]
    con.close()
    assert count == 2  # W001과 W002는 같은 카테고리 50003757


def test_fetch_stores_market_codes(oc_excel_path, catalog_db_path):
    from godomall_register.market_category_fetch import fetch_from_oc_excel
    fetch_from_oc_excel(oc_excel_path, catalog_db_path)
    con = sqlite3.connect(catalog_db_path)
    row = con.execute(
        "SELECT market_cat_code FROM oc_category_markets "
        "WHERE oc_category_key=? AND market=?",
        ["50003757", "auction"]
    ).fetchone()
    con.close()
    assert row is not None
    assert row[0] == "71281100"


def test_fetch_missing_sheet_raises(tmp_path, catalog_db_path):
    import openpyxl
    wb = openpyxl.Workbook()
    wb.active.title = "WRONG"
    p = tmp_path / "wrong.xlsx"
    wb.save(str(p))

    from godomall_register.market_category_fetch import fetch_from_oc_excel
    with pytest.raises(ValueError, match="OWNERCLAN"):
        fetch_from_oc_excel(str(p), catalog_db_path)
