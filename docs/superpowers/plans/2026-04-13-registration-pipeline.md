# 등록 파이프라인 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** OC 카테고리별 마켓카테고리 코드 수집 → 스토어-카테고리 배정 → 고도몰/이셀러스 Excel 출고 파이프라인 Hub UI 구현

**Architecture:** OC양식 Excel에서 마켓카테고리 파싱 → oc_catalog.db의 oc_category_markets 테이블에 저장. products.db의 store_category_assignments로 스토어별 카테고리 배정 관리. register_service.py가 배정된 카테고리 기준 상품 필터 → pipeline_run.py(고도몰) / convert_ownerclan_to_esellers(이셀러스) 호출 → Excel 생성 후 market_registrations에 READY 기록.

**Tech Stack:** FastAPI, SQLite WAL, openpyxl, pandas, vanilla JS, pytest

---

## 파일 구조

| 파일 | 역할 |
|------|------|
| `godomall_register/market_category_fetch.py` | NEW: OC Excel 파싱 → oc_category_markets 저장 |
| `hub/routers/catalog.py` | MOD: POST /api/catalog/import-market-categories 추가 |
| `hub/routers/register.py` | NEW: 카테고리 배정 + 파이프라인 실행 API |
| `hub/services/register_service.py` | NEW: 배정 로직 + 파이프라인 실행 |
| `hub/routers/market_status.py` | NEW: 마켓 현황 import API |
| `hub/services/market_status_service.py` | NEW: Excel import + 현황 조회 |
| `hub/services/db_service.py` | MOD: store_category_assignments + market_registrations DDL |
| `hub/services/catalog_service.py` | MOD: oc_category_markets DDL |
| `hub/app.py` | MOD: 새 라우터 등록 |
| `hub/static/index.html` | MOD: 사이드바 + script 태그 |
| `hub/static/app.js` | MOD: PAGES 레지스트리 |
| `hub/static/pages/stores.js` | MOD: 카테고리 트리뷰 배정 UI |
| `hub/static/pages/dashboard.js` | MOD: 마켓별 현황 카드 |
| `hub/static/pages/pipeline_register.js` | NEW: 등록 파이프라인 UI |
| `tests/hub/conftest.py` | MOD: 새 테이블 seed 추가 |
| `tests/hub/test_register.py` | NEW |
| `tests/hub/test_market_status.py` | NEW |
| `tests/hub/test_catalog_categories.py` | NEW |

---

## Task 1: OC 마켓카테고리 수집 (Excel 파싱 → oc_category_markets)

**Files:**
- Create: `godomall_register/market_category_fetch.py`
- Modify: `hub/services/catalog_service.py` (oc_category_markets DDL)
- Modify: `hub/routers/catalog.py` (import 엔드포인트)
- Create: `tests/hub/test_catalog_categories.py`

- [ ] **Step 1: market_category_fetch.py 생성**

```python
# godomall_register/market_category_fetch.py
"""OC 양식 Excel → oc_category_markets 테이블 수집."""
from __future__ import annotations

import sqlite3
from pathlib import Path


def _parse_market_cat_string(s: str) -> dict[str, dict]:
    """'market,code,name\n...' → {market: {code, name}}."""
    result = {}
    for line in (s or "").strip().splitlines():
        parts = line.strip().split(",", 2)
        if len(parts) >= 2:
            market, code = parts[0].strip(), parts[1].strip()
            name = parts[2].strip() if len(parts) > 2 else ""
            if market and code:
                result[market] = {"code": code, "name": name}
    return result


def fetch_from_oc_excel(excel_path: str, catalog_db_path: str) -> dict:
    """OC 양식 Excel(OWNERCLAN 시트)에서 카테고리+마켓카테고리 추출 → oc_category_markets 저장.

    Returns: {"categories": int, "rows_inserted": int, "errors": list}
    """
    import openpyxl
    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    if "OWNERCLAN" not in wb.sheetnames:
        raise ValueError(f"'OWNERCLAN' 시트 없음. 시트목록: {wb.sheetnames}")
    ws = wb["OWNERCLAN"]

    rows = list(ws.iter_rows(values_only=True))
    # Row 0: 섹션 헤더 (상품기본정보, 카테고리정보, ...)
    # Row 1: 컬럼 헤더 (판매자상품코드1, ..., 카테고리코드, 카테고리명, 마켓카테고리, ...)
    if len(rows) < 3:
        return {"categories": 0, "rows_inserted": 0, "errors": ["데이터 행 없음"]}

    headers = list(rows[1])
    col = {str(h): i for i, h in enumerate(headers) if h}
    needed = {"카테고리코드", "카테고리명", "마켓카테고리"}
    missing = needed - col.keys()
    if missing:
        raise ValueError(f"필수 컬럼 없음: {missing}. 헤더: {headers[:10]}")

    ki = col["카테고리코드"]
    ni = col["카테고리명"]
    mi = col["마켓카테고리"]

    # 카테고리 단위 수집 (상품별 중복 제거)
    cats: dict[str, dict] = {}
    for row in rows[2:]:
        key = str(row[ki] or "").strip()
        name = str(row[ni] or "").strip()
        mcat = str(row[mi] or "").strip()
        if key and key not in cats:
            cats[key] = {"name": name, "mcat": mcat}

    # oc_category_markets 저장
    con = sqlite3.connect(catalog_db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS oc_category_markets (
            oc_category_key  TEXT NOT NULL,
            oc_category_name TEXT,
            market           TEXT NOT NULL,
            market_cat_code  TEXT,
            market_cat_name  TEXT,
            is_manual        INTEGER DEFAULT 0,
            updated_at       TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (oc_category_key, market)
        )
    """)

    rows_inserted = 0
    errors = []
    for cat_key, info in cats.items():
        markets = _parse_market_cat_string(info["mcat"])
        if not markets:
            errors.append(f"{cat_key}: 마켓카테고리 없음")
            continue
        for market, minfo in markets.items():
            con.execute("""
                INSERT OR REPLACE INTO oc_category_markets
                (oc_category_key, oc_category_name, market,
                 market_cat_code, market_cat_name, updated_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
            """, [cat_key, info["name"], market, minfo["code"], minfo["name"]])
            rows_inserted += 1

    con.commit()
    con.close()
    return {"categories": len(cats), "rows_inserted": rows_inserted, "errors": errors}
```

- [ ] **Step 2: 테스트 작성**

```python
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
```

- [ ] **Step 3: 테스트 실행 (실패 확인)**

```bash
cd .worktrees/feat-pricing
python -m pytest tests/hub/test_catalog_categories.py -v
```
Expected: FAIL with `ModuleNotFoundError` (파일 아직 없음)

- [ ] **Step 4: 테스트 통과 확인**

```bash
python -m pytest tests/hub/test_catalog_categories.py -v
```
Expected: 4 tests PASS

- [ ] **Step 5: catalog_service.py에 oc_category_markets DDL 추가**

`hub/services/catalog_service.py`의 `_init_catalog_db()` 또는 catalog DB 초기화 블록에 추가:

```python
# _ensure_catalog_schema() 또는 _init_catalog_db() 내부에 추가
_OC_CATEGORY_MARKETS_DDL = """
CREATE TABLE IF NOT EXISTS oc_category_markets (
    oc_category_key  TEXT NOT NULL,
    oc_category_name TEXT,
    market           TEXT NOT NULL,
    market_cat_code  TEXT,
    market_cat_name  TEXT,
    is_manual        INTEGER DEFAULT 0,
    updated_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (oc_category_key, market)
)
"""
```

catalog DB를 초기화하는 코드에서 `cat_conn.execute(_OC_CATEGORY_MARKETS_DDL)` 호출 추가.

- [ ] **Step 6: catalog.py에 import 엔드포인트 추가**

```python
# hub/routers/catalog.py 하단에 추가
@router.post("/api/catalog/import-market-categories")
async def import_market_categories(file: bytes = None):
    """OC 양식 Excel 업로드 → oc_category_markets 저장."""
    from fastapi import UploadFile, File
    # 이 시그니처로 변경 필요 — 아래 실제 구현 참고
```

실제로는 File upload를 받아야 하므로:

```python
# hub/routers/catalog.py에 추가 (상단 import에 UploadFile, File 추가)
from fastapi import UploadFile, File
import tempfile, os

@router.post("/api/catalog/import-market-categories")
async def import_market_categories(file: UploadFile = File(...)):
    """OC 양식 Excel 업로드 → oc_category_markets 저장."""
    from hub.services.catalog_service import get_catalog_db_path
    from godomall_register.market_category_fetch import fetch_from_oc_excel

    # 임시 파일에 저장
    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        result = fetch_from_oc_excel(tmp_path, get_catalog_db_path())
        return result
    finally:
        os.unlink(tmp_path)
```

- [ ] **Step 7: 커밋**

```bash
git add godomall_register/market_category_fetch.py \
        hub/routers/catalog.py \
        hub/services/catalog_service.py \
        tests/hub/test_catalog_categories.py
git commit -m "feat: OC Excel 마켓카테고리 파싱 + oc_category_markets 저장"
```

---

## Task 2: store_category_assignments 테이블 + 배정 API

**Files:**
- Modify: `hub/services/db_service.py` (DDL + 배정 함수)
- Create: `hub/routers/register.py`
- Create: `hub/services/register_service.py`
- Modify: `hub/app.py`
- Modify: `tests/hub/conftest.py`
- Create: `tests/hub/test_register.py`

- [ ] **Step 1: conftest.py에 store_category_assignments 시드 추가**

`tests/hub/conftest.py`의 스키마 상수에 추가:

```python
STORE_CATEGORY_ASSIGNMENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS store_category_assignments (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    store_alias      TEXT NOT NULL,
    oc_category_key  TEXT NOT NULL,
    oc_category_name TEXT,
    market_group     TEXT NOT NULL,
    assigned_at      TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(store_alias, oc_category_key)
)
"""

MARKET_REGISTRATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS market_registrations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    상품코드          TEXT NOT NULL,
    store_alias       TEXT NOT NULL,
    market            TEXT NOT NULL,
    market_product_id TEXT,
    oc_category_key   TEXT,
    market_cat_code   TEXT,
    sell_price        INTEGER,
    strategy          TEXT,
    pipeline_run_id   TEXT,
    status            TEXT DEFAULT 'READY',
    created_at        TEXT DEFAULT (datetime('now', 'localtime')),
    confirmed_at      TEXT,
    PRIMARY KEY(상품코드, store_alias, created_at)
)
"""
```

`test_db_path` fixture의 `conn.executescript(...)` 호출에 두 스키마 추가:

```python
conn.executescript(
    PRODUCTS_SCHEMA + STORES_SCHEMA +
    STORE_CATEGORY_ASSIGNMENTS_SCHEMA + MARKET_REGISTRATIONS_SCHEMA
)
```

- [ ] **Step 2: db_service.py에 DDL + 마이그레이션 추가**

`hub/services/db_service.py`에 상수 추가:

```python
_STORE_CATEGORY_ASSIGNMENTS_DDL = """
CREATE TABLE IF NOT EXISTS store_category_assignments (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    store_alias      TEXT NOT NULL,
    oc_category_key  TEXT NOT NULL,
    oc_category_name TEXT,
    market_group     TEXT NOT NULL,
    assigned_at      TEXT DEFAULT (datetime('now', 'localtime')),
    UNIQUE(store_alias, oc_category_key)
)
"""

_MARKET_REGISTRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS market_registrations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    상품코드          TEXT NOT NULL,
    store_alias       TEXT NOT NULL,
    market            TEXT NOT NULL,
    market_product_id TEXT,
    oc_category_key   TEXT,
    market_cat_code   TEXT,
    sell_price        INTEGER,
    strategy          TEXT,
    pipeline_run_id   TEXT,
    status            TEXT DEFAULT 'READY',
    created_at        TEXT DEFAULT (datetime('now', 'localtime')),
    confirmed_at      TEXT,
    PRIMARY KEY(상품코드, store_alias, created_at)
)
"""
```

`run_migrations()` 함수 내 기존 `con.execute(_VENDORS_DDL)` 다음에:

```python
con.execute(_STORE_CATEGORY_ASSIGNMENTS_DDL)
con.execute(_MARKET_REGISTRATIONS_DDL)
```

- [ ] **Step 3: register_service.py 생성**

```python
# hub/services/register_service.py
"""카테고리-스토어 배정 + 등록 파이프라인 서비스."""
from __future__ import annotations

from typing import Any
from hub.services.db_service import _conn

# 마켓 → 마켓그룹 매핑
MARKET_TO_GROUP: dict[str, str] = {
    "고도몰":      "naver",
    "스마트스토어": "naver",
    "옥션":        "esm",
    "지마켓":      "esm",
    "11번가":      "st11",
    "쿠팡":        "coupang",
    "인터파크":    "interpark",
    "티몬":        "tmon",
    "위메프":      "wmp",
}


def get_store_market_group(store_alias: str) -> str | None:
    """stores 테이블에서 store_alias의 market 조회 → market_group 반환."""
    with _conn() as con:
        row = con.execute(
            "SELECT market FROM stores WHERE alias = ?", [store_alias]
        ).fetchone()
    if not row:
        return None
    return MARKET_TO_GROUP.get(row["market"], row["market"])


def get_assignments(store_alias: str) -> list[dict]:
    """특정 스토어의 카테고리 배정 목록."""
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM store_category_assignments WHERE store_alias = ? ORDER BY oc_category_name",
            [store_alias],
        ).fetchall()
    return [dict(r) for r in rows]


def add_assignment(store_alias: str, oc_category_name: str) -> dict:
    """카테고리 배정 추가. oc_category_name(fullName 텍스트)을 key로 사용.

    같은 market_group 내 중복 시 conflict_stores 반환.
    """
    market_group = get_store_market_group(store_alias)
    if not market_group:
        raise ValueError(f"스토어 없음: {store_alias}")

    # 중복 체크: 같은 market_group 내 다른 스토어에 이미 배정?
    with _conn() as con:
        conflicts = con.execute(
            """SELECT store_alias FROM store_category_assignments
               WHERE oc_category_name = ? AND market_group = ? AND store_alias != ?""",
            [oc_category_name, market_group, store_alias],
        ).fetchall()
        conflict_stores = [r["store_alias"] for r in conflicts]

        con.execute(
            """INSERT OR REPLACE INTO store_category_assignments
               (store_alias, oc_category_key, oc_category_name, market_group)
               VALUES (?, ?, ?, ?)""",
            [store_alias, oc_category_name, oc_category_name, market_group],
            # oc_category_key = oc_category_name fallback (실제 key는 oc_catalog.db에 있음)
        )
    return {"store_alias": store_alias, "oc_category_name": oc_category_name,
            "conflict_stores": conflict_stores}


def remove_assignment(store_alias: str, oc_category_name: str) -> dict:
    """카테고리 배정 제거. oc_category_name 기준."""
    with _conn() as con:
        con.execute(
            "DELETE FROM store_category_assignments WHERE store_alias=? AND oc_category_name=?",
            [store_alias, oc_category_name],
        )
    return {"deleted": True}


def get_oc_categories_with_counts() -> list[dict]:
    """OC 카테고리 목록 + 상품 수 (ACTIVE + oc_price > 0 기준).

    oc_category_name을 key로 사용 (oc_category_key는 oc_catalog.db에서 조회).
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT 카테고리명, COUNT(*) as cnt
               FROM products
               WHERE product_status='ACTIVE' AND oc_price > 0
               GROUP BY 카테고리명
               ORDER BY 카테고리명"""
        ).fetchall()
    return [
        # oc_category_name을 id처럼 사용 (fullName 텍스트 = 매칭 기준)
        {"oc_category_name": r["카테고리명"], "product_count": r["cnt"]}
        for r in rows
    ]
```

- [ ] **Step 4: 테스트 작성**

```python
# tests/hub/test_register.py
"""카테고리-스토어 배정 API 테스트."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


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
```

- [ ] **Step 5: register.py 라우터 생성**

```python
# hub/routers/register.py
"""카테고리-스토어 배정 + 등록 파이프라인 API."""
from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/api/register/oc-categories")
async def oc_categories():
    """상품 DB의 OC 카테고리 목록 + 상품 수."""
    from hub.services.register_service import get_oc_categories_with_counts
    return get_oc_categories_with_counts()


@router.get("/api/register/assignments/{store_alias}")
async def get_assignments(store_alias: str):
    """특정 스토어의 카테고리 배정 목록."""
    from hub.services.register_service import get_assignments
    return get_assignments(store_alias)


@router.post("/api/register/assignments")
async def add_assignment(body: dict):
    """카테고리 배정 추가. body: {store_alias, oc_category_name}."""
    from hub.services.register_service import add_assignment
    return add_assignment(
        store_alias=body["store_alias"],
        oc_category_name=body["oc_category_name"],
    )


@router.delete("/api/register/assignments/{store_alias}")
async def remove_assignment(store_alias: str, oc_category_name: str):
    """카테고리 배정 제거. Query param: oc_category_name."""
    from hub.services.register_service import remove_assignment
    return remove_assignment(store_alias, oc_category_name)
```

- [ ] **Step 6: app.py에 라우터 등록**

`hub/app.py`의 import 라인 수정:

```python
from hub.routers import dashboard, products, pipeline, export, stores, vendors, catalog, register
```

`app.include_router(catalog.router)` 다음에:

```python
app.include_router(register.router)
```

- [ ] **Step 7: 테스트 실행**

```bash
python -m pytest tests/hub/test_register.py -v
```
Expected: 5 tests PASS

- [ ] **Step 8: 커밋**

```bash
git add hub/services/db_service.py hub/services/register_service.py \
        hub/routers/register.py hub/app.py \
        tests/hub/conftest.py tests/hub/test_register.py
git commit -m "feat: store_category_assignments + 카테고리 배정 API"
```

---

## Task 3: 스토어 관리 UI — 카테고리 트리뷰 배정

**Files:**
- Modify: `hub/static/pages/stores.js`

현재 stores.js 하단에 카테고리 배정 섹션 추가. 기존 스토어 목록 렌더 후 호출.

- [ ] **Step 1: stores.js에 카테고리 배정 패널 추가**

`hub/static/pages/stores.js` 파일의 renderStores 함수 내 스토어 클릭 또는 선택 이벤트에 카테고리 패널 연결.

파일 하단에 다음 함수 추가:

```javascript
// hub/static/pages/stores.js 하단에 추가

async function renderCategoryAssignment(container, storeAlias) {
  container.innerHTML = '<div style="color:var(--muted);font-size:13px">로딩 중...</div>';

  const [catsResp, assignedResp] = await Promise.all([
    fetch('/api/register/oc-categories').then(r => r.json()),
    fetch(`/api/register/assignments/${encodeURIComponent(storeAlias)}`).then(r => r.json()),
  ]);

  const assignedKeys = new Set(assignedResp.map(a => a.oc_category_key));

  // 트리뷰 빌드: 대>중 기준으로 그루핑
  const tree = {};
  for (const cat of catsResp) {
    const parts = (cat.oc_category_name || '').split('>');
    const top = parts[0] || '기타';
    if (!tree[top]) tree[top] = [];
    tree[top].push(cat);
  }

  let html = `<div style="margin-top:16px">
    <h4 style="font-size:14px;margin-bottom:12px">카테고리 배정 — <span style="color:var(--accent)">${storeAlias}</span></h4>
    <div style="max-height:400px;overflow-y:auto;border:1px solid var(--border);border-radius:8px;padding:12px">`;

  for (const [top, cats] of Object.entries(tree)) {
    html += `<div style="margin-bottom:8px">
      <div style="font-weight:600;font-size:13px;color:var(--muted);margin-bottom:4px">${top}</div>`;
    for (const cat of cats) {
      // oc_category_name을 식별자로 사용 (fullName 텍스트)
      const checked = assignedKeys.has(cat.oc_category_name) ? 'checked' : '';
      html += `<label style="display:flex;align-items:center;gap:8px;padding:4px 8px;
                              border-radius:4px;cursor:pointer;font-size:13px">
        <input type="checkbox" class="cat-assign-cb" ${checked}
               data-name="${cat.oc_category_name}">
        <span>${cat.oc_category_name.split('>').slice(1).join('>')}</span>
        <span style="margin-left:auto;color:var(--muted);font-size:12px">${cat.product_count}개</span>
      </label>`;
    }
    html += `</div>`;
  }

  html += `</div></div>`;
  container.innerHTML = html;

  // 체크박스 이벤트 (oc_category_name 기준)
  container.querySelectorAll('.cat-assign-cb').forEach(cb => {
    cb.addEventListener('change', async () => {
      const name = cb.dataset.name;
      if (cb.checked) {
        const r = await fetch('/api/register/assignments', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({store_alias: storeAlias, oc_category_name: name}),
        });
        const data = await r.json();
        if (data.conflict_stores?.length) {
          cb.parentElement.style.background = 'rgba(250,204,21,0.1)';
          cb.parentElement.title = `⚠ 같은 마켓그룹: ${data.conflict_stores.join(', ')}`;
        }
      } else {
        await fetch(
          `/api/register/assignments/${encodeURIComponent(storeAlias)}?oc_category_name=${encodeURIComponent(name)}`,
          {method: 'DELETE'},
        );
        cb.parentElement.style.background = '';
        cb.parentElement.title = '';
      }
    });
  });
}

window.renderCategoryAssignment = renderCategoryAssignment;
```

- [ ] **Step 2: stores.js tbody 렌더링에 data-alias 추가 + 클릭 이벤트**

`hub/static/pages/stores.js` 의 `tbody.innerHTML = stores.map(s => {` 블록에서
`return \`<tr>\`` → `return \`<tr data-alias="${s.alias}" style="cursor:pointer">\`` 로 변경.

그 다음 `loadAndRender()` 함수 하단 (tbody 렌더 후)에 추가:

```javascript
// tbody 렌더 직후 (loadAndRender 내부)
let catPanel = document.getElementById('cat-assign-panel');
if (!catPanel) {
  catPanel = document.createElement('div');
  catPanel.id = 'cat-assign-panel';
  catPanel.className = 'card';
  catPanel.style.display = 'none';
  // stores 테이블 카드 다음에 추가
  const tableCard = tbody.closest('.card');
  if (tableCard) tableCard.after(catPanel);
}

tbody.querySelectorAll('tr[data-alias]').forEach(tr => {
  tr.addEventListener('click', () => {
    catPanel.style.display = 'block';
    renderCategoryAssignment(catPanel, tr.dataset.alias);
  });
});
```

- [ ] **Step 3: 서버 재시작 후 수동 확인**

```bash
# 서버가 실행 중이면 재시작
# http://localhost:8080/#stores 접속
# 스토어 행 클릭 → 카테고리 트리뷰 표시 확인
# 체크박스 선택 → 배정 저장 확인
curl http://localhost:8080/api/register/assignments/고도몰A1-1
```

- [ ] **Step 4: 커밋**

```bash
git add hub/static/pages/stores.js
git commit -m "feat: 스토어 관리 UI 카테고리 트리뷰 배정"
```

---

## Task 4: market_registrations + 대시보드 마켓별 현황 + Excel import

**Files:**
- Create: `hub/routers/market_status.py`
- Create: `hub/services/market_status_service.py`
- Modify: `hub/static/pages/dashboard.js`
- Modify: `hub/app.py`
- Create: `tests/hub/test_market_status.py`

- [ ] **Step 1: market_status_service.py 생성**

```python
# hub/services/market_status_service.py
"""마켓별 현황 조회 + 셀러센터 Excel import."""
from __future__ import annotations

import json
from typing import Any
from hub.services.db_service import _conn


def get_market_status() -> list[dict]:
    """마켓별 등록 현황 집계 (store_alias, market, 상태별 수)."""
    with _conn() as con:
        rows = con.execute(
            """SELECT store_alias, market,
                      SUM(CASE WHEN status='READY'    THEN 1 ELSE 0 END) as ready_count,
                      SUM(CASE WHEN status='UPLOADED'  THEN 1 ELSE 0 END) as uploaded_count,
                      SUM(CASE WHEN status='UPLOAD_FAILED' THEN 1 ELSE 0 END) as failed_count,
                      MAX(confirmed_at) as last_import_at
               FROM market_registrations
               GROUP BY store_alias, market
               ORDER BY market, store_alias"""
        ).fetchall()
    return [dict(r) for r in rows]


def import_from_excel(store_alias: str, market: str, excel_path: str) -> dict:
    """셀러센터 Excel import → market_registrations 갱신.

    Excel 최소 요구사항: '상품코드' 열 존재.
    import에 있는 상품코드 → UPLOADED, 기존 READY인데 import에 없으면 → UPLOAD_FAILED.
    """
    import openpyxl
    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return {"updated": 0, "failed": 0, "error": "빈 파일"}

    headers = [str(h or "").strip() for h in rows[0]]
    try:
        code_idx = headers.index("상품코드")
    except ValueError:
        return {"updated": 0, "failed": 0, "error": f"'상품코드' 열 없음. 헤더: {headers[:10]}"}

    imported_codes = {
        str(row[code_idx]).strip()
        for row in rows[1:]
        if row[code_idx]
    }

    with _conn() as con:
        # READY 상태인 상품 중 import에 있으면 UPLOADED
        updated = 0
        failed = 0
        ready_rows = con.execute(
            """SELECT 상품코드, created_at FROM market_registrations
               WHERE store_alias=? AND market=? AND status='READY'""",
            [store_alias, market],
        ).fetchall()

        for row in ready_rows:
            code = row["상품코드"]
            if code in imported_codes:
                con.execute(
                    """UPDATE market_registrations SET status='UPLOADED', confirmed_at=datetime('now','localtime')
                       WHERE 상품코드=? AND store_alias=? AND created_at=?""",
                    [code, store_alias, row["created_at"]],
                )
                updated += 1
            else:
                con.execute(
                    """UPDATE market_registrations SET status='UPLOAD_FAILED'
                       WHERE 상품코드=? AND store_alias=? AND created_at=?""",
                    [code, store_alias, row["created_at"]],
                )
                failed += 1

    return {"updated": updated, "failed": failed}
```

- [ ] **Step 2: 테스트 작성**

```python
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
```

- [ ] **Step 3: market_status.py 라우터 생성**

```python
# hub/routers/market_status.py
"""마켓별 현황 + Excel import API."""
from __future__ import annotations

import os
import tempfile
from fastapi import APIRouter, UploadFile, File, Form

router = APIRouter()


@router.get("/api/market-status")
async def get_market_status():
    from hub.services.market_status_service import get_market_status
    return get_market_status()


@router.post("/api/market-status/import")
async def import_market_status(
    store_alias: str = Form(...),
    market: str = Form(...),
    file: UploadFile = File(...),
):
    """셀러센터 Excel 업로드 → READY 상품 상태 갱신."""
    from hub.services.market_status_service import import_from_excel
    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name
    try:
        return import_from_excel(store_alias, market, tmp_path)
    finally:
        os.unlink(tmp_path)
```

- [ ] **Step 4: app.py에 라우터 등록**

```python
from hub.routers import (dashboard, products, pipeline, export,
                          stores, vendors, catalog, register, market_status)
# ...
app.include_router(register.router)
app.include_router(market_status.router)
```

- [ ] **Step 5: 테스트 실행**

```bash
python -m pytest tests/hub/test_market_status.py -v
```
Expected: 4 tests PASS

- [ ] **Step 6: dashboard.js에 마켓별 현황 카드 추가**

`hub/static/pages/dashboard.js`의 `renderDashboard` 함수 내 기존 stat-grid 아래에:

```javascript
// 마켓별 현황 카드 (stat-grid 다음)
const marketCard = document.createElement('div');
marketCard.className = 'card';
marketCard.innerHTML = `
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
    <h3 style="font-size:16px">마켓별 등록 현황</h3>
    <label class="btn btn-primary" style="cursor:pointer;font-size:13px">
      📥 현황 import
      <input type="file" id="import-excel-input" accept=".xlsx" style="display:none">
    </label>
  </div>
  <div id="market-status-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px">
    <div style="color:var(--muted);font-size:13px">로딩 중...</div>
  </div>
`;
container.appendChild(marketCard);

// 마켓 현황 로드
fetch('/api/market-status').then(r => r.json()).then(data => {
  const grid = document.getElementById('market-status-grid');
  if (!grid) return;
  if (!data.length) {
    grid.innerHTML = '<div style="color:var(--muted);font-size:13px;grid-column:1/-1">등록 현황 없음 — 셀러센터 Excel import 필요</div>';
    return;
  }
  grid.innerHTML = data.map(d => `
    <div style="background:var(--surface);border-radius:8px;padding:14px">
      <div style="font-size:12px;color:var(--muted);margin-bottom:4px">${d.store_alias}</div>
      <div style="font-weight:700;font-size:22px">${(d.uploaded_count||0).toLocaleString()}</div>
      <div style="font-size:11px;color:var(--muted);margin-top:4px">
        대기 ${d.ready_count||0} · 실패 <span style="color:var(--red)">${d.failed_count||0}</span>
      </div>
      ${d.last_import_at ? `<div style="font-size:11px;color:var(--muted);margin-top:2px">${d.last_import_at.slice(0,10)}</div>` : ''}
    </div>
  `).join('');
}).catch(() => {});
```

- [ ] **Step 7: 커밋**

```bash
git add hub/routers/market_status.py hub/services/market_status_service.py \
        hub/static/pages/dashboard.js hub/app.py \
        tests/hub/test_market_status.py
git commit -m "feat: 대시보드 마켓별 현황 카드 + 셀러센터 Excel import"
```

---

## Task 5: 등록 파이프라인 Hub UI (고도몰 + 이셀러스 Excel 생성)

**Files:**
- Create: `hub/static/pages/pipeline_register.js`
- Modify: `hub/static/index.html`
- Modify: `hub/static/app.js`
- Modify: `hub/services/register_service.py` (파이프라인 실행 함수)
- Modify: `hub/routers/register.py` (파이프라인 엔드포인트)

- [ ] **Step 1: register_service.py에 파이프라인 함수 추가**

`hub/services/register_service.py` 하단에 추가:

```python
import json
import uuid
import tempfile
from pathlib import Path


def get_pipeline_preview(store_alias: str, strategy: str) -> dict:
    """출고 예정 상품 미리보기. store_alias 배정 카테고리 기준 필터."""
    assignments = get_assignments(store_alias)
    cat_names = [a["oc_category_name"] for a in assignments]
    if not cat_names:
        return {"items": [], "total": 0, "store_alias": store_alias,
                "categories": [], "strategy": strategy}

    placeholders = ",".join("?" * len(cat_names))
    with _conn() as con:
        rows = con.execute(
            f"""SELECT 상품코드, product_names_json, 카테고리명, oc_price
                FROM products
                WHERE product_status='ACTIVE' AND oc_price > 0
                AND 카테고리명 IN ({placeholders})
                ORDER BY 카테고리명, 상품코드""",
            cat_names,
        ).fetchall()

    items = []
    for r in rows:
        name = ""
        try:
            name = json.loads(r["product_names_json"] or "{}").get("name", "")
        except Exception:
            name = r["product_names_json"] or ""
        items.append({
            "상품코드": r["상품코드"],
            "상품명": name,
            "카테고리명": r["카테고리명"],
            "oc_price": r["oc_price"],
        })

    return {
        "items": items[:50],  # 미리보기 50개
        "total": len(items),
        "store_alias": store_alias,
        "categories": cat_names,
        "strategy": strategy,
    }


def run_godomall_pipeline(store_alias: str, strategy: str) -> dict:
    """고도몰 Excel 생성 → market_registrations READY 기록.

    Returns: {"file_url": str, "product_count": int, "pipeline_run_id": str}
    """
    import sys
    _ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(_ROOT / "OC_ES_converter" / "scripts"))

    from godomall_register.pipeline_run import fetch_export_products, build_oc_dataframe
    from convert_godomall import convert_ownerclan_to_godomall, save_godomall, resolve_godomall_template_path

    # 배정된 카테고리 기준 필터
    assignments = get_assignments(store_alias)
    cat_names = [a["oc_category_name"] for a in assignments]
    if not cat_names:
        return {"error": "배정된 카테고리 없음", "product_count": 0}

    db_path = Path(get_db_path_fn())
    all_products = fetch_export_products(db_path)
    products = [p for p in all_products if p.get("카테고리명") in cat_names]
    if not products:
        return {"error": "출고 대상 없음", "product_count": 0}

    oc_df = build_oc_dataframe(products)
    godomall_df, _ = convert_ownerclan_to_godomall(oc_df, strategy_id=strategy)

    run_id = uuid.uuid4().hex[:8]
    tmp_dir = Path(tempfile.gettempdir()) / "product_hub_exports"
    tmp_dir.mkdir(exist_ok=True)
    out_path = tmp_dir / f"고도몰_{store_alias}_{run_id}.xlsx"

    template = resolve_godomall_template_path()
    save_godomall(godomall_df, template, str(out_path))

    # market_registrations READY 기록
    _record_ready(
        codes=[p["상품코드"] for p in products],
        store_alias=store_alias,
        market="고도몰",
        strategy=strategy,
        pipeline_run_id=run_id,
        assignments=assignments,
    )

    file_id = f"godomall_{run_id}"
    _export_files[file_id] = str(out_path)

    return {
        "file_url": f"/api/register/download/{file_id}",
        "product_count": len(products),
        "pipeline_run_id": run_id,
    }


def run_esellers_pipeline(store_alias: str, strategy: str) -> dict:
    """이셀러스 Excel 생성 → market_registrations READY 기록."""
    import sys
    _ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(_ROOT / "OC_ES_converter" / "scripts"))

    from godomall_register.pipeline_run import fetch_export_products, build_oc_dataframe
    from convert_base import convert_ownerclan_to_esellers

    assignments = get_assignments(store_alias)
    cat_names = [a["oc_category_name"] for a in assignments]
    if not cat_names:
        return {"error": "배정된 카테고리 없음", "product_count": 0}

    db_path = Path(get_db_path_fn())
    all_products = fetch_export_products(db_path)
    products = [p for p in all_products if p.get("카테고리명") in cat_names]
    if not products:
        return {"error": "출고 대상 없음", "product_count": 0}

    oc_df = build_oc_dataframe(products)
    esellers_df, errors_df = convert_ownerclan_to_esellers(oc_df)

    run_id = uuid.uuid4().hex[:8]
    tmp_dir = Path(tempfile.gettempdir()) / "product_hub_exports"
    tmp_dir.mkdir(exist_ok=True)
    out_path = tmp_dir / f"이셀러스_{store_alias}_{run_id}.xlsx"
    esellers_df.to_excel(str(out_path), index=False)

    # market_registrations READY 기록 (이셀러스 = 옥션/지마켓 등)
    store_row = _get_store(store_alias)
    market = store_row["market"] if store_row else "이셀러스"
    _record_ready(
        codes=[p["상품코드"] for p in products],
        store_alias=store_alias,
        market=market,
        strategy=strategy,
        pipeline_run_id=run_id,
        assignments=assignments,
    )

    file_id = f"esellers_{run_id}"
    _export_files[file_id] = str(out_path)

    return {
        "file_url": f"/api/register/download/{file_id}",
        "product_count": len(products),
        "pipeline_run_id": run_id,
        "error_count": len(errors_df) if errors_df is not None else 0,
    }


# ── 내부 헬퍼 ──────────────────────────────────────────────────────────────────

_export_files: dict[str, str] = {}


def get_export_file(file_id: str) -> str | None:
    return _export_files.get(file_id)


def _get_store(store_alias: str) -> dict | None:
    with _conn() as con:
        row = con.execute("SELECT * FROM stores WHERE alias=?", [store_alias]).fetchone()
    return dict(row) if row else None


def get_db_path_fn() -> str:
    from hub.services.db_service import get_db_path
    return get_db_path()


def _record_ready(codes: list[str], store_alias: str, market: str,
                  strategy: str, pipeline_run_id: str, assignments: list[dict]) -> None:
    """상품 목록을 market_registrations에 READY로 기록."""
    cat_key_map = {a["oc_category_name"]: a["oc_category_key"] for a in assignments}
    with _conn() as con:
        for p_code in codes:
            # 카테고리 키 조회
            row = con.execute(
                "SELECT 카테고리명 FROM products WHERE 상품코드=?", [p_code]
            ).fetchone()
            cat_name = row["카테고리명"] if row else ""
            oc_cat_key = cat_key_map.get(cat_name, "")
            con.execute(
                """INSERT OR IGNORE INTO market_registrations
                   (상품코드, store_alias, market, oc_category_key, strategy, pipeline_run_id, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'READY')""",
                [p_code, store_alias, market, oc_cat_key, strategy, pipeline_run_id],
            )
```

- [ ] **Step 2: register.py에 파이프라인 엔드포인트 추가**

`hub/routers/register.py`에 추가:

```python
from fastapi.responses import FileResponse

@router.get("/api/register/preview/{store_alias}")
async def pipeline_preview(store_alias: str, strategy: str = "lowest_price"):
    from hub.services.register_service import get_pipeline_preview
    return get_pipeline_preview(store_alias, strategy)


@router.post("/api/register/run/godomall")
async def run_godomall(body: dict):
    from hub.services.register_service import run_godomall_pipeline
    return run_godomall_pipeline(body["store_alias"], body.get("strategy", "lowest_price"))


@router.post("/api/register/run/esellers")
async def run_esellers(body: dict):
    from hub.services.register_service import run_esellers_pipeline
    return run_esellers_pipeline(body["store_alias"], body.get("strategy", "lowest_price"))


@router.get("/api/register/download/{file_id}")
async def download_file(file_id: str):
    from hub.services.register_service import get_export_file
    path = get_export_file(file_id)
    if not path or not Path(path).exists():
        from fastapi import HTTPException
        raise HTTPException(404, "파일 없음")
    return FileResponse(path, filename=Path(path).name,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
```

- [ ] **Step 3: pipeline_register.js 생성**

```javascript
// hub/static/pages/pipeline_register.js
window.renderPipelineRegister = async function(container) {
  let stores = [];
  try {
    stores = await fetch('/api/stores').then(r => r.json());
  } catch(e) {
    container.innerHTML = '<div class="loading" style="color:var(--red)">스토어 로드 실패</div>';
    return;
  }

  const storeOpts = stores.filter(s => s.active).map(s =>
    `<option value="${s.alias}">${s.alias} (${s.market})</option>`
  ).join('');

  container.innerHTML = `
    <h1 class="page-title">🚀 등록 파이프라인</h1>
    <div style="display:grid;grid-template-columns:280px 1fr;gap:20px">
      <div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">스토어</h3>
          <select id="reg-store" style="width:100%;background:var(--bg);color:var(--text);
                  border:1px solid var(--border);border-radius:6px;padding:8px;font-size:14px">
            <option value="">선택...</option>${storeOpts}
          </select>
        </div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">가격 전략</h3>
          ${[['lowest_price','최저가'],['normal_sale','일반판매'],['cpc_ad','광고']]
            .map(([val,label],i) =>
              `<label style="display:flex;align-items:center;gap:6px;margin-bottom:6px;cursor:pointer">
                 <input type="radio" name="reg-strategy" value="${val}" ${i===0?'checked':''}> ${label}
               </label>`).join('')}
        </div>
        <div style="display:flex;flex-direction:column;gap:10px">
          <button class="btn btn-primary" id="btn-preview">📋 미리보기</button>
          <button class="btn btn-success" id="btn-godomall" disabled>▶ 고도몰 Excel</button>
          <button class="btn btn-success" id="btn-esellers" disabled>▶ 이셀러스 Excel</button>
        </div>
        <div id="failed-info" style="display:none;margin-top:16px">
          <div class="card" style="background:rgba(248,113,113,0.08);border-color:var(--red)">
            <div style="color:var(--red);font-size:13px" id="failed-text"></div>
          </div>
        </div>
      </div>
      <div>
        <div class="card" id="preview-card" style="display:none">
          <h3 style="margin-bottom:12px;font-size:15px">출고 미리보기</h3>
          <div id="preview-info" style="color:var(--muted);font-size:13px;margin-bottom:12px"></div>
          <table>
            <thead><tr><th>상품코드</th><th>상품명</th><th>카테고리</th><th>원가</th></tr></thead>
            <tbody id="preview-tbody"></tbody>
          </table>
        </div>
        <div id="download-area" style="margin-top:16px"></div>
      </div>
    </div>
  `;

  const storeEl    = document.getElementById('reg-store');
  const previewCard = document.getElementById('preview-card');
  const previewInfo = document.getElementById('preview-info');
  const previewBody = document.getElementById('preview-tbody');
  const btnGodomall = document.getElementById('btn-godomall');
  const btnEsellers = document.getElementById('btn-esellers');
  const dlArea      = document.getElementById('download-area');

  function getStrategy() {
    return document.querySelector('[name=reg-strategy]:checked')?.value || 'lowest_price';
  }

  document.getElementById('btn-preview').addEventListener('click', async () => {
    const store = storeEl.value;
    if (!store) { alert('스토어를 선택하세요'); return; }
    previewCard.style.display = 'none';

    const data = await fetch(`/api/register/preview/${encodeURIComponent(store)}?strategy=${getStrategy()}`)
      .then(r => r.json());

    previewCard.style.display = 'block';
    previewInfo.textContent = `총 ${data.total.toLocaleString()}개 | 카테고리 ${data.categories.length}개`;
    previewBody.innerHTML = data.items.map(p =>
      `<tr>
        <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
        <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.상품명}</td>
        <td style="color:var(--muted);font-size:12px">${p.카테고리명}</td>
        <td style="color:var(--muted)">${(p.oc_price||0).toLocaleString()}원</td>
      </tr>`
    ).join('');

    btnGodomall.disabled = data.total === 0;
    btnEsellers.disabled = data.total === 0;
  });

  async function runPipeline(endpoint, label) {
    const store = storeEl.value;
    if (!store) { alert('스토어를 선택하세요'); return; }
    btnGodomall.disabled = true;
    btnEsellers.disabled = true;
    dlArea.innerHTML = '<div style="color:var(--muted);font-size:13px">생성 중...</div>';

    const data = await fetch(endpoint, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({store_alias: store, strategy: getStrategy()}),
    }).then(r => r.json());

    if (data.file_url) {
      dlArea.innerHTML = `
        <div class="card" style="background:rgba(52,212,116,0.1);border-color:var(--green)">
          <p style="color:var(--green);margin-bottom:12px;font-weight:600">✅ ${label} 생성 완료 (${(data.product_count||0).toLocaleString()}개)</p>
          <a href="${data.file_url}" class="btn btn-success" download>⬇ 다운로드</a>
          ${data.error_count ? `<span style="margin-left:12px;color:var(--yellow);font-size:13px">⚠ 변환 오류 ${data.error_count}건</span>` : ''}
        </div>`;
    } else {
      dlArea.innerHTML = `<div class="card" style="background:rgba(248,113,113,0.1);border-color:var(--red)">
        <p style="color:var(--red)">${data.error || '생성 실패'}</p>
      </div>`;
    }
    btnGodomall.disabled = false;
    btnEsellers.disabled = false;
  }

  btnGodomall.addEventListener('click', () => runPipeline('/api/register/run/godomall', '고도몰 Excel'));
  btnEsellers.addEventListener('click', () => runPipeline('/api/register/run/esellers', '이셀러스 Excel'));
};
```

- [ ] **Step 4: index.html 사이드바 + script 태그 추가**

`hub/static/index.html`의 `<nav>` 내 Export 그룹:

```html
<div class="nav-group">
  <div class="nav-label">Export</div>
  <a class="nav-item" data-page="export" href="#">&#128717; 마켓 출고</a>
  <a class="nav-item" data-page="pipeline-register" href="#">&#128640; 등록 파이프라인</a>
</div>
```

script 태그 (`</body>` 전):

```html
<script src="/pages/pipeline_register.js"></script>
```

- [ ] **Step 5: app.js PAGES 레지스트리 추가**

`hub/static/app.js` 상단 PAGES 객체에:

```javascript
const PAGES = {
  dashboard:         window.renderDashboard,
  products:          window.renderProducts,
  vendors:           window.renderVendors,
  pipeline:          window.renderPipeline,
  export:            window.renderExport,
  stores:            window.renderStores,
  'pipeline-register': window.renderPipelineRegister,
};
```

- [ ] **Step 6: 수동 확인**

```bash
# 서버 재시작 후
# http://localhost:8080/#pipeline-register 접속
# 스토어 선택 → 미리보기 → 고도몰 Excel 생성 → 다운로드 확인
curl http://localhost:8080/api/register/preview/고도몰A1-1?strategy=lowest_price
```

- [ ] **Step 7: 커밋**

```bash
git add hub/static/pages/pipeline_register.js \
        hub/static/index.html hub/static/app.js \
        hub/services/register_service.py \
        hub/routers/register.py
git commit -m "feat: 등록 파이프라인 Hub UI (고도몰/이셀러스 Excel 생성)"
```

---

## Task 6: 전체 테스트 실행 + 마무리

- [ ] **Step 1: 전체 테스트 실행**

```bash
python -m pytest tests/hub/ -v
```
Expected: 모든 테스트 PASS (기존 + 신규)

- [ ] **Step 2: 서버 기동 확인**

```bash
python -m uvicorn hub.app:app --port 8080 --reload
```

브라우저 확인:
- `#dashboard` → 마켓별 현황 카드 표시
- `#stores` → 스토어 클릭 시 카테고리 트리뷰
- `#pipeline-register` → 스토어 선택 → 미리보기 → Excel 생성

- [ ] **Step 3: OC Excel import 테스트 (수동)**

OC양식 Excel 파일로 마켓카테고리 수집:

```bash
curl -X POST http://localhost:8080/api/catalog/import-market-categories \
  -F "file=@C:/Users/kohaz/Downloads/고도몰잇는것/ownerclan_shiningmall_고도몰잇는것_OWNERCLAN_2217212_1_1.xlsx"
# Expected: {"categories": N, "rows_inserted": M, "errors": [...]}
```

- [ ] **Step 4: 최종 커밋**

```bash
git add -A
git commit -m "feat: 등록 파이프라인 Phase 1 완성

- OC Excel 마켓카테고리 수집 (oc_category_markets)
- 카테고리-스토어 배정 API + 트리뷰 UI
- 마켓별 현황 대시보드 카드 + 셀러센터 Excel import
- 고도몰/이셀러스 Excel 출고 파이프라인 Hub UI
- market_registrations READY/UPLOADED/UPLOAD_FAILED 상태 추적"
```

---

## 구현 후 확인 체크리스트

- [ ] `GET /api/register/oc-categories` → 상품 DB의 카테고리 목록 반환
- [ ] `POST /api/register/assignments` → 배정 저장 + conflict 감지
- [ ] `GET /api/market-status` → 스토어별 READY/UPLOADED/FAILED 수
- [ ] `POST /api/market-status/import` → 셀러센터 Excel → 상태 갱신
- [ ] `POST /api/catalog/import-market-categories` → OC Excel → oc_category_markets
- [ ] `GET /api/register/preview/{store}` → 배정 카테고리 기준 상품 미리보기
- [ ] `POST /api/register/run/godomall` → 고도몰 Excel 생성 + READY 기록
- [ ] `POST /api/register/run/esellers` → 이셀러스 Excel 생성 + READY 기록
- [ ] 대시보드 마켓별 현황 카드 표시
- [ ] 스토어 관리 카테고리 트리뷰 배정 UI
- [ ] 등록 파이프라인 페이지 접근 가능
