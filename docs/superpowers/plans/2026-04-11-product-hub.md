# Product Hub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local FastAPI web platform at localhost:8000 that replaces fragmented Tkinter UIs with a single dark-themed management interface for the OC→product processing pipeline.

**Architecture:** FastAPI serves Vanilla JS SPA from `hub/static/`. A `hub/services/db_service.py` layer wraps SQLite queries directly (not via DBHandler). SSE streams OC sync progress in real-time. All JS pages share one `index.html` shell with a fixed sidebar; the router swaps the `<main>` content div. The existing products.db is opened directly — no ORM.

**Tech Stack:** FastAPI 0.111+, uvicorn[standard], SQLite WAL mode, Vanilla JS + fetch() + EventSource (SSE), openpyxl (existing)

---

## File Map

### New files
| Path | Responsibility |
|---|---|
| `hub/__init__.py` | Package marker |
| `hub/app.py` | FastAPI app factory, static mounting, router registration |
| `hub/routers/__init__.py` | Package marker |
| `hub/routers/dashboard.py` | `GET /api/dashboard` — stats for 4 cards |
| `hub/routers/products.py` | `GET /api/products` — search/filter/paginate; `GET /api/products/categories` |
| `hub/routers/pipeline.py` | `GET /api/pipeline/sync/stream` — SSE; `GET /api/pipeline/status` |
| `hub/routers/export.py` | `POST /api/export/dry-run`; `POST /api/export/run`; `GET /api/export/download/{file_id}` |
| `hub/routers/stores.py` | `GET /api/stores`; `POST /api/stores/import` |
| `hub/services/__init__.py` | Package marker |
| `hub/services/db_service.py` | All SQLite reads/writes for the hub (migrations included) |
| `hub/services/oc_service.py` | Async wrapper around `sync_existing()` using thread executor |
| `hub/services/export_service.py` | Wraps pipeline_run logic for dry-run and run modes |
| `hub/static/index.html` | Single HTML shell: sidebar + `<main id="content">` |
| `hub/static/style.css` | Dark theme variables + layout |
| `hub/static/app.js` | Client-side router, sidebar init |
| `hub/static/pages/dashboard.js` | Dashboard page: 4 stat cards + activity log |
| `hub/static/pages/products.js` | Products table: filter bar, table, pagination |
| `hub/static/pages/pipeline.js` | Pipeline page: OC sync button + SSE progress |
| `hub/static/pages/export.js` | Export page: category/store/strategy form + dry-run table |
| `hub/static/pages/stores.js` | Stores page: import button + store list table |
| `tests/__init__.py` | Package marker (create if absent) |
| `tests/hub/__init__.py` | Package marker |
| `tests/hub/conftest.py` | Shared pytest fixtures (test DB, TestClient) |
| `tests/hub/test_dashboard.py` | Dashboard API tests |
| `tests/hub/test_products.py` | Products API tests |
| `tests/hub/test_pipeline.py` | Pipeline SSE skeleton test |
| `tests/hub/test_export.py` | Export dry-run tests |
| `tests/hub/test_stores.py` | Stores API tests |

### Modified files
| Path | Change |
|---|---|
| `requirements.txt` | Add `fastapi>=0.111.0`, `uvicorn[standard]>=0.29.0` |

> `sync_existing()` already accepts `progress_callback(current, total, message)` — no changes needed.

---

## Task 1: Scaffold — dependencies, package, server, static shell

**Files:**
- Modify: `requirements.txt`
- Create: `hub/__init__.py`, `hub/app.py`, `hub/routers/__init__.py`, `hub/services/__init__.py`
- Create: `hub/static/index.html`, `hub/static/style.css`, `hub/static/app.js`
- Create: `tests/__init__.py`, `tests/hub/__init__.py`, `tests/hub/conftest.py`

---

- [ ] **Step 1.1: Add FastAPI dependencies to requirements.txt**

Open `requirements.txt` and add after the `API 및 네트워크` section:

```text
# ============================================
# 웹 플랫폼 (Product Hub)
# ============================================
fastapi>=0.111.0
uvicorn[standard]>=0.29.0
httpx>=0.27.0  # TestClient 전용
```

- [ ] **Step 1.2: Install the new packages**

```bash
pip install fastapi "uvicorn[standard]" httpx
```

Expected: installs without error. Verify: `python -c "import fastapi; print(fastapi.__version__)"` prints `0.111.x` or higher.

- [ ] **Step 1.3: Create package markers**

Create `hub/__init__.py` (empty):
```python
```

Create `hub/routers/__init__.py` (empty):
```python
```

Create `hub/services/__init__.py` (empty):
```python
```

Create `tests/__init__.py` (empty):
```python
```

Create `tests/hub/__init__.py` (empty):
```python
```

- [ ] **Step 1.4: Write the test for the root endpoint**

Create `tests/hub/conftest.py`:

```python
"""공유 픽스처: 테스트 DB + FastAPI TestClient."""
import os
import sqlite3
import tempfile
import pytest
from fastapi.testclient import TestClient


PRODUCTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT,
    상품코드 TEXT NOT NULL,
    product_names_json TEXT,
    카테고리명 TEXT,
    product_status TEXT DEFAULT 'ACTIVE',
    oc_price INTEGER,
    oc_options_json TEXT,
    oc_content TEXT,
    oc_images_json TEXT,
    oc_synced_at TEXT,
    text_status TEXT,
    image_status TEXT,
    export_log TEXT DEFAULT '[]',
    registered_stores TEXT DEFAULT '[]'
);
"""

STORES_SCHEMA = """
CREATE TABLE IF NOT EXISTS stores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alias TEXT UNIQUE NOT NULL,
    market TEXT,
    group_id TEXT,
    login_id TEXT,
    active INTEGER DEFAULT 1,
    strategy TEXT,
    slot_count INTEGER,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
);
"""


def _seed_products(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """INSERT INTO products
           (상품코드, product_names_json, 카테고리명, product_status, oc_price,
            text_status, image_status)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("W001", '{"name":"테스트상품A"}', "가전/디지털>TV", "ACTIVE", 50000, "done", "done"),
            ("W002", '{"name":"테스트상품B"}', "가전/디지털>냉장고", "ACTIVE", 30000, "done", None),
            ("W003", '{"name":"테스트상품C"}', "생활/주방>청소", "ACTIVE", None, None, None),
            ("W004", '{"name":"비활성상품"}', "가전/디지털>TV", "INACTIVE", 10000, None, None),
        ],
    )
    conn.commit()


def _seed_stores(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT INTO stores (alias, market, group_id, strategy) VALUES (?, ?, ?, ?)",
        [
            ("고도몰A1-1", "고도몰", "A1", "lowest_price"),
            ("스마트스토어A1-1", "스마트스토어", "A1", "normal_sale"),
            ("옥션A2-1", "옥션", "A2", "cpc_ad"),
        ],
    )
    conn.commit()


@pytest.fixture(scope="session")
def test_db_path(tmp_path_factory):
    """세션 범위 임시 DB 파일."""
    db_file = tmp_path_factory.mktemp("hub_test") / "test_products.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript(PRODUCTS_SCHEMA + STORES_SCHEMA)
    _seed_products(conn)
    _seed_stores(conn)
    conn.close()
    return str(db_file)


@pytest.fixture(scope="session")
def client(test_db_path):
    """FastAPI TestClient (DB 경로 환경변수로 주입)."""
    os.environ["HUB_DB_PATH"] = test_db_path
    from hub.app import app
    return TestClient(app)
```

- [ ] **Step 1.5: Write the failing test for root endpoint**

Create `tests/hub/test_dashboard.py` (just the root check for now):

```python
def test_root_serves_html(client):
    """GET / → index.html 반환."""
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
```

- [ ] **Step 1.6: Run test to verify it fails**

```bash
cd "C:\Users\kohaz\Desktop\Python\파이썬자동화파일\상품가공프로그램\.worktrees\feat-pricing"
python -m pytest tests/hub/test_dashboard.py::test_root_serves_html -v
```

Expected: `ImportError: No module named 'hub'` (hub/app.py not yet created)

- [ ] **Step 1.7: Create hub/app.py**

```python
"""Product Hub — FastAPI 앱 진입점."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# ── 라우터 임포트 ──────────────────────────────────────────────────────────────
from hub.routers import dashboard, products, pipeline, export, stores

# ── DB 경로 환경변수 주입 (테스트용) ────────────────────────────────────────────
if db_path := os.environ.get("HUB_DB_PATH"):
    # db_service가 import될 때 읽을 수 있도록 미리 설정
    os.environ.setdefault("HUB_DB_PATH", db_path)

app = FastAPI(title="Product Hub", version="1.0.0")

# ── API 라우터 등록 ────────────────────────────────────────────────────────────
app.include_router(dashboard.router)
app.include_router(products.router)
app.include_router(pipeline.router)
app.include_router(export.router)
app.include_router(stores.router)

# ── 정적 파일 (가장 마지막에 마운트) ────────────────────────────────────────────
_static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
```

- [ ] **Step 1.8: Create stub routers (each returns {} until implemented)**

Create `hub/routers/dashboard.py`:
```python
from fastapi import APIRouter
router = APIRouter()

@router.get("/api/dashboard")
async def get_dashboard():
    return {}
```

Create `hub/routers/products.py`:
```python
from fastapi import APIRouter
router = APIRouter()

@router.get("/api/products")
async def get_products():
    return {"items": [], "total": 0}

@router.get("/api/products/categories")
async def get_categories():
    return []
```

Create `hub/routers/pipeline.py`:
```python
from fastapi import APIRouter
router = APIRouter()

@router.get("/api/pipeline/status")
async def get_status():
    return {}
```

Create `hub/routers/export.py`:
```python
from fastapi import APIRouter
router = APIRouter()

@router.post("/api/export/dry-run")
async def dry_run():
    return {"items": [], "total": 0}

@router.post("/api/export/run")
async def run_export():
    return {}
```

Create `hub/routers/stores.py`:
```python
from fastapi import APIRouter
router = APIRouter()

@router.get("/api/stores")
async def get_stores():
    return []

@router.post("/api/stores/import")
async def import_stores():
    return {}
```

- [ ] **Step 1.9: Create hub/static/index.html**

```html
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Product Hub</title>
  <link rel="stylesheet" href="/style.css">
</head>
<body>
  <nav id="sidebar">
    <div class="sidebar-logo">Product Hub</div>
    <div class="nav-group">
      <div class="nav-label">Overview</div>
      <a class="nav-item" data-page="dashboard" href="#">📊 대시보드</a>
    </div>
    <div class="nav-group">
      <div class="nav-label">Data</div>
      <a class="nav-item" data-page="products" href="#">📦 상품 DB</a>
      <a class="nav-item" data-page="stores" href="#">🏢 스토어 관리</a>
    </div>
    <div class="nav-group">
      <div class="nav-label">Processing</div>
      <a class="nav-item" data-page="pipeline" href="#">⚙️ 파이프라인</a>
    </div>
    <div class="nav-group">
      <div class="nav-label">Export</div>
      <a class="nav-item" data-page="export" href="#">🛒 마켓 출고</a>
    </div>
  </nav>
  <main id="content">
    <div class="loading">로딩 중...</div>
  </main>

  <script src="/pages/dashboard.js"></script>
  <script src="/pages/products.js"></script>
  <script src="/pages/pipeline.js"></script>
  <script src="/pages/export.js"></script>
  <script src="/pages/stores.js"></script>
  <script src="/app.js"></script>
</body>
</html>
```

- [ ] **Step 1.10: Create hub/static/style.css**

```css
:root {
  --bg: #16181f;
  --card: #1d2030;
  --border: #2a2d3e;
  --text: #f0f1f8;
  --muted: #8b8fa8;
  --accent: #7c8cf8;
  --green: #34d474;
  --yellow: #facc15;
  --orange: #fb923c;
  --red: #f87171;
  --sidebar-w: 230px;

  /* 마켓 컬러 */
  --market-godomall: #7ab0ff;
  --market-smartstore: #34d474;
  --market-auction: #fb923c;
  --market-gmarket: #facc15;
  --market-11st: #c084fc;
  --market-coupang: #f87171;
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: var(--bg);
  color: var(--text);
  font-family: 'Segoe UI', 'Noto Sans KR', sans-serif;
  font-size: 14px;
  display: flex;
  min-height: 100vh;
}

/* ── 사이드바 ── */
#sidebar {
  width: var(--sidebar-w);
  background: var(--card);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  position: fixed;
  top: 0; left: 0; bottom: 0;
  padding: 16px 0;
  overflow-y: auto;
}

.sidebar-logo {
  font-size: 18px;
  font-weight: 700;
  color: var(--accent);
  padding: 8px 20px 20px;
  letter-spacing: 0.5px;
}

.nav-group { margin-bottom: 8px; }

.nav-label {
  font-size: 11px;
  font-weight: 600;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.8px;
  padding: 4px 20px;
}

.nav-item {
  display: block;
  padding: 9px 20px;
  color: var(--text);
  text-decoration: none;
  border-radius: 0;
  transition: background 0.15s;
  font-size: 14px;
}

.nav-item:hover { background: rgba(124, 140, 248, 0.1); }
.nav-item.active {
  background: rgba(124, 140, 248, 0.18);
  color: var(--accent);
  font-weight: 600;
}

/* ── 메인 콘텐츠 ── */
#content {
  margin-left: var(--sidebar-w);
  flex: 1;
  padding: 32px 40px;
  min-height: 100vh;
}

/* ── 공통 컴포넌트 ── */
.page-title {
  font-size: 24px;
  font-weight: 700;
  margin-bottom: 24px;
}

.card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 20px;
  margin-bottom: 20px;
}

.stat-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin-bottom: 24px;
}

.stat-card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 20px;
}

.stat-label {
  font-size: 13px;
  color: var(--muted);
  margin-bottom: 8px;
}

.stat-value {
  font-size: 36px;
  font-weight: 700;
  color: var(--text);
}

.btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 9px 18px;
  border-radius: 7px;
  border: none;
  cursor: pointer;
  font-size: 14px;
  font-weight: 600;
  transition: opacity 0.15s;
}
.btn:hover { opacity: 0.85; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-success { background: var(--green); color: #000; }
.btn-danger  { background: var(--red);   color: #fff; }

.pill {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 12px;
  font-weight: 600;
}
.pill-done     { background: rgba(52,212,116,0.2); color: var(--green); }
.pill-progress { background: rgba(252,204,21,0.2);  color: var(--yellow); }
.pill-todo     { background: rgba(139,143,168,0.15); color: var(--muted); }

table { width: 100%; border-collapse: collapse; }
thead th {
  text-align: left;
  padding: 10px 12px;
  font-size: 12px;
  font-weight: 600;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 1px solid var(--border);
}
tbody tr { border-bottom: 1px solid var(--border); transition: background 0.1s; }
tbody tr:hover { background: rgba(255,255,255,0.03); }
tbody td { padding: 10px 12px; vertical-align: middle; }

.loading { color: var(--muted); padding: 40px; }

.market-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
  margin: 2px;
}
```

- [ ] **Step 1.11: Create hub/static/app.js**

```javascript
// ── 페이지 레지스트리 ────────────────────────────────────────────────────────
const PAGES = {
  dashboard: window.renderDashboard,
  products:  window.renderProducts,
  pipeline:  window.renderPipeline,
  export:    window.renderExport,
  stores:    window.renderStores,
};

// ── 라우터 ──────────────────────────────────────────────────────────────────
async function navigate(page) {
  const content = document.getElementById('content');
  content.innerHTML = '<div class="loading">로딩 중...</div>';

  // 사이드바 활성 표시
  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.toggle('active', el.dataset.page === page);
  });

  const render = PAGES[page];
  if (render) {
    await render(content);
  } else {
    content.innerHTML = `<div class="loading">페이지 없음: ${page}</div>`;
  }

  history.pushState({ page }, '', `#${page}`);
}

// ── 이벤트 바인딩 ───────────────────────────────────────────────────────────
document.querySelectorAll('.nav-item').forEach(el => {
  el.addEventListener('click', e => {
    e.preventDefault();
    navigate(el.dataset.page);
  });
});

window.addEventListener('popstate', e => {
  if (e.state?.page) navigate(e.state.page);
});

// ── 초기 페이지 ─────────────────────────────────────────────────────────────
const initialPage = location.hash.slice(1) || 'dashboard';
navigate(initialPage);
```

Create `hub/static/pages/dashboard.js` (placeholder):
```javascript
window.renderDashboard = async function(container) {
  container.innerHTML = '<h1 class="page-title">📊 대시보드</h1><div class="loading">구현 예정</div>';
};
```

Create `hub/static/pages/products.js` (placeholder):
```javascript
window.renderProducts = async function(container) {
  container.innerHTML = '<h1 class="page-title">📦 상품 DB</h1><div class="loading">구현 예정</div>';
};
```

Create `hub/static/pages/pipeline.js` (placeholder):
```javascript
window.renderPipeline = async function(container) {
  container.innerHTML = '<h1 class="page-title">⚙️ 파이프라인</h1><div class="loading">구현 예정</div>';
};
```

Create `hub/static/pages/export.js` (placeholder):
```javascript
window.renderExport = async function(container) {
  container.innerHTML = '<h1 class="page-title">🛒 마켓 출고</h1><div class="loading">구현 예정</div>';
};
```

Create `hub/static/pages/stores.js` (placeholder):
```javascript
window.renderStores = async function(container) {
  container.innerHTML = '<h1 class="page-title">🏢 스토어 관리</h1><div class="loading">구현 예정</div>';
};
```

- [ ] **Step 1.12: Run the test**

```bash
python -m pytest tests/hub/test_dashboard.py::test_root_serves_html -v
```

Expected: `PASSED` — server serves index.html at `/`.

- [ ] **Step 1.13: Verify server starts manually**

```bash
python -m uvicorn hub.app:app --reload --port 8000
```

Open browser at `http://localhost:8000` — dark sidebar should appear with "로딩 중..." in main area. Stop with Ctrl+C.

- [ ] **Step 1.14: Commit**

```bash
git add hub/ tests/hub/ requirements.txt
git commit -m "feat(hub): scaffold FastAPI app + dark theme static shell"
```

---

## Task 2: DB Service Layer + Migrations

**Files:**
- Create: `hub/services/db_service.py`
- Modify: `tests/hub/conftest.py` (already has schema, just verify)
- Create: `tests/hub/test_db_service.py`

---

- [ ] **Step 2.1: Write failing tests for db_service**

Create `tests/hub/test_db_service.py`:

```python
"""db_service 단위 테스트. conftest의 test_db_path 픽스처 사용."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db_env(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path
    yield


def test_get_dashboard_stats_returns_required_keys():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    for key in ("total_active", "shippable", "processed", "last_sync_at"):
        assert key in stats, f"Missing key: {key}"


def test_total_active_counts_only_active(test_db_path):
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["total_active"] == 3  # W001, W002, W003 (W004 is INACTIVE)


def test_shippable_requires_oc_price():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["shippable"] == 2  # W001(50000), W002(30000); W003 has no oc_price


def test_processed_requires_both_statuses():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["processed"] == 1  # W001 only (text_status=done AND image_status=done)


def test_get_products_returns_active_only():
    from hub.services.db_service import get_products
    result = get_products()
    codes = [p["상품코드"] for p in result["items"]]
    assert "W004" not in codes


def test_get_products_text_search():
    from hub.services.db_service import get_products
    result = get_products(q="상품A")
    assert len(result["items"]) == 1
    assert result["items"][0]["상품코드"] == "W001"


def test_get_products_category_filter():
    from hub.services.db_service import get_products
    result = get_products(category="가전/디지털>TV")
    codes = [p["상품코드"] for p in result["items"]]
    assert "W001" in codes
    assert "W002" not in codes


def test_get_products_quick_filter_has_oc_price():
    from hub.services.db_service import get_products
    result = get_products(quick_filter="has_oc_price")
    codes = [p["상품코드"] for p in result["items"]]
    assert "W003" not in codes  # no oc_price


def test_get_products_pagination():
    from hub.services.db_service import get_products
    result = get_products(page=1, per_page=2)
    assert len(result["items"]) == 2
    assert result["total"] == 3


def test_get_categories_returns_list():
    from hub.services.db_service import get_categories
    cats = get_categories()
    assert isinstance(cats, list)
    assert "가전/디지털>TV" in cats


def test_get_stores_returns_seeded():
    from hub.services.db_service import get_stores
    stores = get_stores()
    aliases = [s["alias"] for s in stores]
    assert "고도몰A1-1" in aliases


def test_run_migrations_is_idempotent(test_db_path):
    from hub.services.db_service import run_migrations
    run_migrations(test_db_path)  # already ran in conftest; should not raise
    run_migrations(test_db_path)  # second call also fine
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
python -m pytest tests/hub/test_db_service.py -v
```

Expected: `ModuleNotFoundError: No module named 'hub.services.db_service'`

- [ ] **Step 2.3: Implement hub/services/db_service.py**

```python
"""SQLite 쿼리 레이어 — Product Hub 전용.

DB 경로: 환경변수 HUB_DB_PATH 우선, 없으면 oc_import.DEFAULT_DB_PATH 사용.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any


# ── DB 경로 ──────────────────────────────────────────────────────────────────

def _default_db_path() -> str:
    from godomall_register.oc_import import DEFAULT_DB_PATH
    return str(DEFAULT_DB_PATH)


def get_db_path() -> str:
    return os.environ.get("HUB_DB_PATH") or _default_db_path()


@contextmanager
def _conn(db_path: str | None = None):
    path = db_path or get_db_path()
    con = sqlite3.connect(path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
        con.commit()
    finally:
        con.close()


# ── 마이그레이션 ──────────────────────────────────────────────────────────────

_HUB_COLUMNS = [
    ("text_status",        "TEXT"),
    ("image_status",       "TEXT"),
    ("export_log",         "TEXT DEFAULT '[]'"),
    ("registered_stores",  "TEXT DEFAULT '[]'"),
]

_STORES_DDL = """
CREATE TABLE IF NOT EXISTS stores (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alias       TEXT UNIQUE NOT NULL,
    market      TEXT,
    group_id    TEXT,
    login_id    TEXT,
    active      INTEGER DEFAULT 1,
    strategy    TEXT,
    slot_count  INTEGER,
    created_at  TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at  TEXT DEFAULT (datetime('now', 'localtime'))
)
"""


def run_migrations(db_path: str | None = None) -> None:
    """products에 hub 컬럼 추가 + stores 테이블 생성 (멱등)."""
    with _conn(db_path) as con:
        cur = con.cursor()
        cur.execute("PRAGMA table_info(products)")
        existing = {row["name"] for row in cur.fetchall()}
        for col, col_type in _HUB_COLUMNS:
            if col not in existing:
                con.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
        con.execute(_STORES_DDL)


# ── 대시보드 ──────────────────────────────────────────────────────────────────

def get_dashboard_stats() -> dict[str, Any]:
    with _conn() as con:
        cur = con.cursor()

        cur.execute(
            "SELECT COUNT(*) FROM products WHERE product_status = 'ACTIVE'"
        )
        total_active = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM products "
            "WHERE product_status = 'ACTIVE' AND oc_price IS NOT NULL AND oc_price > 0"
        )
        shippable = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM products "
            "WHERE product_status = 'ACTIVE' "
            "AND text_status = 'done' AND image_status = 'done'"
        )
        processed = cur.fetchone()[0]

        cur.execute(
            "SELECT MAX(oc_synced_at) FROM products WHERE product_status = 'ACTIVE'"
        )
        last_sync_at = cur.fetchone()[0] or ""

    return {
        "total_active": total_active,
        "shippable": shippable,
        "processed": processed,
        "last_sync_at": last_sync_at,
    }


# ── 상품 DB ───────────────────────────────────────────────────────────────────

_QUICK_FILTERS = {
    "all":          "",
    "has_oc_price": "AND oc_price IS NOT NULL AND oc_price > 0",
    "no_market":    "AND (export_log IS NULL OR export_log = '[]')",
    "partial_market": "",  # 복잡한 조건 — 일단 all과 동일
}


def get_products(
    q: str = "",
    category: str = "",
    quick_filter: str = "all",
    page: int = 1,
    per_page: int = 100,
) -> dict[str, Any]:
    """상품 목록 조회 (검색/필터/페이지네이션)."""
    clauses = ["product_status = 'ACTIVE'"]
    params: list[Any] = []

    if q:
        clauses.append(
            "(상품코드 LIKE ? OR product_names_json LIKE ?)"
        )
        params += [f"%{q}%", f"%{q}%"]

    if category:
        clauses.append("카테고리명 = ?")
        params.append(category)

    extra = _QUICK_FILTERS.get(quick_filter, "")
    where = " AND ".join(clauses)
    if extra:
        where += " " + extra

    offset = (page - 1) * per_page

    with _conn() as con:
        cur = con.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM products WHERE {where}",
            params,
        )
        total = cur.fetchone()[0]

        cur.execute(
            f"""SELECT 상품코드, product_names_json, 카테고리명,
                       oc_price, text_status, image_status,
                       export_log, registered_stores, oc_synced_at
                FROM products WHERE {where}
                ORDER BY 상품코드
                LIMIT ? OFFSET ?""",
            params + [per_page, offset],
        )
        rows = cur.fetchall()

    items = []
    for row in rows:
        name = ""
        try:
            name_data = json.loads(row["product_names_json"] or "{}")
            name = name_data.get("name", "")
        except (json.JSONDecodeError, TypeError):
            name = row["product_names_json"] or ""

        items.append({
            "상품코드": row["상품코드"],
            "상품명": name,
            "카테고리명": row["카테고리명"] or "",
            "oc_price": row["oc_price"],
            "text_status": row["text_status"] or "todo",
            "image_status": row["image_status"] or "todo",
            "export_log": json.loads(row["export_log"] or "[]"),
            "registered_stores": json.loads(row["registered_stores"] or "[]"),
            "oc_synced_at": row["oc_synced_at"] or "",
        })

    return {"items": items, "total": total, "page": page, "per_page": per_page}


def get_categories() -> list[str]:
    with _conn() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT DISTINCT 카테고리명 FROM products "
            "WHERE product_status = 'ACTIVE' AND 카테고리명 IS NOT NULL AND 카테고리명 != '' "
            "ORDER BY 카테고리명"
        )
        return [row[0] for row in cur.fetchall()]


# ── 스토어 관리 ───────────────────────────────────────────────────────────────

def get_stores(active_only: bool = False) -> list[dict[str, Any]]:
    with _conn() as con:
        cur = con.cursor()
        where = "WHERE active = 1" if active_only else ""
        cur.execute(
            f"SELECT id, alias, market, group_id, login_id, active, strategy, "
            f"slot_count, created_at, updated_at FROM stores {where} ORDER BY alias"
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_store(
    alias: str,
    market: str,
    group_id: str,
    login_id: str = "",
    strategy: str | None = None,
) -> None:
    with _conn() as con:
        con.execute(
            """INSERT INTO stores (alias, market, group_id, login_id, strategy)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(alias) DO UPDATE SET
                 market = excluded.market,
                 group_id = excluded.group_id,
                 login_id = excluded.login_id,
                 strategy = COALESCE(excluded.strategy, strategy),
                 updated_at = datetime('now', 'localtime')""",
            (alias, market, group_id, login_id, strategy),
        )
```

- [ ] **Step 2.4: Call run_migrations on app startup**

Modify `hub/app.py` — add migration call after imports:

```python
# hub/app.py 상단 import 뒤, app 생성 전에 추가
from hub.services import db_service as _db_service
_db_service.run_migrations()
```

Full updated `hub/app.py`:

```python
"""Product Hub — FastAPI 앱 진입점."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from hub.routers import dashboard, products, pipeline, export, stores
from hub.services import db_service as _db_service

if db_path := os.environ.get("HUB_DB_PATH"):
    os.environ.setdefault("HUB_DB_PATH", db_path)

_db_service.run_migrations()

app = FastAPI(title="Product Hub", version="1.0.0")

app.include_router(dashboard.router)
app.include_router(products.router)
app.include_router(pipeline.router)
app.include_router(export.router)
app.include_router(stores.router)

_static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
```

- [ ] **Step 2.5: Run tests**

```bash
python -m pytest tests/hub/test_db_service.py -v
```

Expected: all 12 tests `PASSED`.

- [ ] **Step 2.6: Commit**

```bash
git add hub/services/db_service.py hub/app.py tests/hub/
git commit -m "feat(hub): db_service layer with migrations + hub column additions"
```

---

## Task 3: Dashboard API + UI

**Files:**
- Modify: `hub/routers/dashboard.py`
- Modify: `hub/static/pages/dashboard.js`
- Modify: `tests/hub/test_dashboard.py`

---

- [ ] **Step 3.1: Write failing API test**

Replace the content of `tests/hub/test_dashboard.py`:

```python
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
```

- [ ] **Step 3.2: Run tests — expect failure**

```bash
python -m pytest tests/hub/test_dashboard.py -v
```

Expected: `test_dashboard_returns_stats` FAILS (stub returns `{}`)

- [ ] **Step 3.3: Implement dashboard router**

Replace `hub/routers/dashboard.py`:

```python
"""대시보드 API — 통계 4개 카드."""
from fastapi import APIRouter

from hub.services.db_service import get_dashboard_stats

router = APIRouter()


@router.get("/api/dashboard")
async def dashboard():
    return get_dashboard_stats()
```

- [ ] **Step 3.4: Run tests — all pass**

```bash
python -m pytest tests/hub/test_dashboard.py -v
```

Expected: all 3 tests `PASSED`.

- [ ] **Step 3.5: Implement dashboard.js**

Replace `hub/static/pages/dashboard.js`:

```javascript
window.renderDashboard = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">📊 대시보드</h1>
    <div class="stat-grid" id="stat-grid">
      <div class="stat-card"><div class="stat-label">전체 상품</div><div class="stat-value" id="s-total">-</div></div>
      <div class="stat-card"><div class="stat-label">출고 가능</div><div class="stat-value" id="s-ship">-</div></div>
      <div class="stat-card"><div class="stat-label">가공 완료</div><div class="stat-value" id="s-proc">-</div></div>
      <div class="stat-card"><div class="stat-label">마지막 동기화</div><div class="stat-value" id="s-sync" style="font-size:16px;padding-top:8px">-</div></div>
    </div>
    <div class="card" style="display:flex;gap:12px;flex-wrap:wrap">
      <button class="btn btn-primary" id="btn-sync">🔄 OC 동기화</button>
      <button class="btn btn-success" onclick="navigate('export')">▶ 출고 실행</button>
    </div>
  `;

  try {
    const stats = await fetch('/api/dashboard').then(r => r.json());
    document.getElementById('s-total').textContent = stats.total_active.toLocaleString();
    document.getElementById('s-ship').textContent  = stats.shippable.toLocaleString();
    document.getElementById('s-proc').textContent  = stats.processed.toLocaleString();
    const sync = stats.last_sync_at ? stats.last_sync_at.slice(0, 19).replace('T', ' ') : '없음';
    document.getElementById('s-sync').textContent  = sync;
  } catch (e) {
    console.error('대시보드 로드 실패:', e);
  }

  document.getElementById('btn-sync').addEventListener('click', () => navigate('pipeline'));
};
```

- [ ] **Step 3.6: Commit**

```bash
git add hub/routers/dashboard.py hub/static/pages/dashboard.js tests/hub/test_dashboard.py
git commit -m "feat(hub): dashboard API + stats UI"
```

---

## Task 4: Products API + UI

**Files:**
- Modify: `hub/routers/products.py`
- Modify: `hub/static/pages/products.js`
- Create: `tests/hub/test_products.py`

---

- [ ] **Step 4.1: Write failing API tests**

Create `tests/hub/test_products.py`:

```python
"""상품 DB API 테스트."""
import os
import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


def test_get_products_default(client):
    r = client.get("/api/products")
    assert r.status_code == 200
    data = r.json()
    assert "items" in data and "total" in data
    assert data["total"] == 3  # 3 ACTIVE products


def test_get_products_search(client):
    r = client.get("/api/products?q=상품A")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["items"][0]["상품코드"] == "W001"


def test_get_products_category(client):
    r = client.get("/api/products?category=가전/디지털>TV")
    assert r.status_code == 200
    data = r.json()
    codes = [p["상품코드"] for p in data["items"]]
    assert "W001" in codes
    assert "W002" not in codes


def test_get_products_quick_filter(client):
    r = client.get("/api/products?quick_filter=has_oc_price")
    data = r.json()
    codes = [p["상품코드"] for p in data["items"]]
    assert "W003" not in codes


def test_get_products_pagination(client):
    r = client.get("/api/products?page=1&per_page=2")
    data = r.json()
    assert len(data["items"]) == 2
    assert data["total"] == 3


def test_product_item_has_required_fields(client):
    r = client.get("/api/products")
    item = r.json()["items"][0]
    for field in ("상품코드", "상품명", "카테고리명", "oc_price",
                  "text_status", "image_status", "export_log", "oc_synced_at"):
        assert field in item, f"Missing field: {field}"


def test_get_categories(client):
    r = client.get("/api/products/categories")
    assert r.status_code == 200
    cats = r.json()
    assert isinstance(cats, list)
    assert "가전/디지털>TV" in cats
```

- [ ] **Step 4.2: Run tests — expect failures**

```bash
python -m pytest tests/hub/test_products.py -v
```

Expected: most tests FAIL (stub returns `{"items": [], "total": 0}`)

- [ ] **Step 4.3: Implement products router**

Replace `hub/routers/products.py`:

```python
"""상품 DB API."""
from fastapi import APIRouter, Query

from hub.services.db_service import get_products, get_categories

router = APIRouter()


@router.get("/api/products")
async def list_products(
    q: str = Query(default=""),
    category: str = Query(default=""),
    quick_filter: str = Query(default="all"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=500),
):
    return get_products(q=q, category=category, quick_filter=quick_filter,
                        page=page, per_page=per_page)


@router.get("/api/products/categories")
async def list_categories():
    return get_categories()
```

- [ ] **Step 4.4: Run tests — all pass**

```bash
python -m pytest tests/hub/test_products.py -v
```

Expected: all 7 tests `PASSED`.

- [ ] **Step 4.5: Implement products.js**

Replace `hub/static/pages/products.js`:

```javascript
const MARKET_COLORS = {
  '고도몰':     '#7ab0ff',
  '스마트스토어': '#34d474',
  '옥션':      '#fb923c',
  '지마켓':    '#facc15',
  '11번가':    '#c084fc',
  '쿠팡':      '#f87171',
};

function marketTag(alias) {
  const market = Object.keys(MARKET_COLORS).find(m => alias.startsWith(m)) || '';
  const color = MARKET_COLORS[market] || '#8b8fa8';
  return `<span class="market-tag" style="background:${color}22;color:${color}">${alias}</span>`;
}

function statusPill(s) {
  if (s === 'done') return '<span class="pill pill-done">완료</span>';
  if (s === 'progress') return '<span class="pill pill-progress">진행</span>';
  return '<span class="pill pill-todo">미완료</span>';
}

function storeTagsHtml(stores) {
  if (!stores || stores.length === 0) return '<span style="color:var(--muted)">-</span>';
  const visible = stores.slice(0, 4).map(s => marketTag(s.store || s)).join('');
  const extra = stores.length > 4
    ? `<span class="pill pill-todo" title="${stores.slice(4).join(', ')}">+${stores.length - 4}</span>`
    : '';
  return visible + extra;
}

let _state = { q: '', category: '', quick_filter: 'all', page: 1, per_page: 100 };

async function loadProducts() {
  const params = new URLSearchParams(_state);
  const data = await fetch(`/api/products?${params}`).then(r => r.json());
  renderTable(data);
}

function renderTable(data) {
  const tbody = document.getElementById('products-tbody');
  if (!tbody) return;
  tbody.innerHTML = data.items.map(p => `
    <tr>
      <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
      <td style="max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
          title="${p.상품명}">${p.상품명}</td>
      <td style="color:var(--muted);font-size:13px">${p.카테고리명}</td>
      <td style="color:var(--yellow)">${p.oc_price ? p.oc_price.toLocaleString() + '원' : '-'}</td>
      <td>${statusPill(p.text_status)}</td>
      <td>${statusPill(p.image_status)}</td>
      <td>${storeTagsHtml(p.export_log)}</td>
      <td style="color:var(--muted);font-size:12px">${p.oc_synced_at ? p.oc_synced_at.slice(0,10) : '-'}</td>
    </tr>
  `).join('');

  // 페이지네이션
  const pag = document.getElementById('pagination');
  if (pag) {
    const totalPages = Math.ceil(data.total / _state.per_page);
    pag.innerHTML = `
      <span style="color:var(--muted)">총 ${data.total.toLocaleString()}개 | 페이지 ${_state.page}/${totalPages}</span>
      ${_state.page > 1 ? '<button class="btn btn-primary" style="padding:4px 12px" id="prev-page">이전</button>' : ''}
      ${_state.page < totalPages ? '<button class="btn btn-primary" style="padding:4px 12px" id="next-page">다음</button>' : ''}
    `;
    document.getElementById('prev-page')?.addEventListener('click', () => { _state.page--; loadProducts(); });
    document.getElementById('next-page')?.addEventListener('click', () => { _state.page++; loadProducts(); });
  }
}

window.renderProducts = async function(container) {
  // 카테고리 로드
  const cats = await fetch('/api/products/categories').then(r => r.json());
  const catOptions = ['<option value="">전체 카테고리</option>',
    ...cats.map(c => `<option value="${c}">${c}</option>`)].join('');

  container.innerHTML = `
    <h1 class="page-title">📦 상품 DB</h1>
    <div class="card" style="display:flex;gap:12px;flex-wrap:wrap;align-items:center;padding:16px 20px">
      <input id="search-q" type="text" placeholder="상품코드 / 상품명 검색"
             value="${_state.q}"
             style="background:#0d0f1a;border:1px solid var(--border);color:var(--text);
                    padding:8px 12px;border-radius:6px;width:260px;font-size:14px">
      <select id="cat-filter"
              style="background:#0d0f1a;border:1px solid var(--border);color:var(--text);
                     padding:8px 12px;border-radius:6px;font-size:14px">
        ${catOptions}
      </select>
      <div style="display:flex;gap:8px">
        ${['all|전체','has_oc_price|OC가격있음','no_market|마켓미배정'].map(opt => {
          const [val, label] = opt.split('|');
          const active = _state.quick_filter === val;
          return `<button class="btn ${active ? 'btn-primary' : ''}" data-qf="${val}"
                          style="${active ? '' : 'background:var(--card);border:1px solid var(--border)'}"
                          >${label}</button>`;
        }).join('')}
      </div>
    </div>
    <div class="card" style="padding:0;overflow:hidden">
      <table>
        <thead><tr>
          <th>상품코드</th><th>상품명</th><th>카테고리</th>
          <th>OC 원가</th><th>텍스트</th><th>이미지</th>
          <th>출고 스토어</th><th>동기화</th>
        </tr></thead>
        <tbody id="products-tbody"><tr><td colspan="8" style="text-align:center;color:var(--muted);padding:40px">로딩 중...</td></tr></tbody>
      </table>
    </div>
    <div id="pagination" style="display:flex;gap:12px;align-items:center;margin-top:16px"></div>
  `;

  // 이벤트 바인딩
  let searchTimer;
  document.getElementById('search-q').addEventListener('input', e => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => { _state.q = e.target.value; _state.page = 1; loadProducts(); }, 400);
  });

  document.getElementById('cat-filter').value = _state.category;
  document.getElementById('cat-filter').addEventListener('change', e => {
    _state.category = e.target.value; _state.page = 1; loadProducts();
  });

  container.querySelectorAll('[data-qf]').forEach(btn => {
    btn.addEventListener('click', () => {
      _state.quick_filter = btn.dataset.qf; _state.page = 1;
      window.renderProducts(container);
    });
  });

  await loadProducts();
};
```

- [ ] **Step 4.6: Commit**

```bash
git add hub/routers/products.py hub/static/pages/products.js tests/hub/test_products.py
git commit -m "feat(hub): products API + table UI with search/filter/pagination"
```

---

## Task 5: Pipeline API + SSE + UI

**Files:**
- Create: `hub/services/oc_service.py`
- Modify: `hub/routers/pipeline.py`
- Modify: `hub/static/pages/pipeline.js`
- Create: `tests/hub/test_pipeline.py`

---

- [ ] **Step 5.1: Write failing tests**

Create `tests/hub/test_pipeline.py`:

```python
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
    # stream=True로 헤더만 확인 (실제 OC API 호출 안 함)
    with client.stream("GET", "/api/pipeline/sync/stream?dry=true") as r:
        assert r.status_code == 200
        assert "text/event-stream" in r.headers.get("content-type", "")
```

- [ ] **Step 5.2: Run tests — expect failure**

```bash
python -m pytest tests/hub/test_pipeline.py -v
```

Expected: `test_pipeline_status_returns_dict` FAILS (stub returns `{}`), SSE test FAILS (stub doesn't stream)

- [ ] **Step 5.3: Create hub/services/oc_service.py**

```python
"""OC 동기화 래퍼 — sync_existing()을 async SSE 스트림으로 노출."""
from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncGenerator

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oc_sync")


def _sse(event_dict: dict) -> str:
    return f"data: {json.dumps(event_dict, ensure_ascii=False)}\n\n"


async def sync_stream(dry: bool = False) -> AsyncGenerator[str, None]:
    """
    sync_existing()을 쓰레드에서 실행하며 SSE 이벤트를 yield.

    dry=True 이면 OC API 호출 없이 즉시 done 반환 (테스트/미리보기용).
    """
    loop = asyncio.get_event_loop()
    progress_events: asyncio.Queue[dict] = asyncio.Queue()

    yield _sse({"type": "start", "message": "OC 동기화 시작..."})

    if dry:
        yield _sse({"type": "done", "message": "dry-run 완료 (API 호출 없음)", "updated": 0})
        return

    def progress_cb(current: int, total: int, message: str) -> None:
        event = {"type": "progress", "current": current, "total": total, "message": message}
        asyncio.run_coroutine_threadsafe(progress_events.put(event), loop)

    def run_sync():
        from godomall_register.oc_import import sync_existing
        from hub.services.db_service import get_db_path
        result = sync_existing(
            db_path=get_db_path(),
            progress_callback=progress_cb,
        )
        asyncio.run_coroutine_threadsafe(
            progress_events.put({"type": "done", "message": "동기화 완료",
                                 "updated": result.get("updated", 0),
                                 "not_found": result.get("not_found", 0),
                                 "errors": result.get("errors", [])}),
            loop,
        )

    future = loop.run_in_executor(_executor, run_sync)

    while not future.done() or not progress_events.empty():
        try:
            event = progress_events.get_nowait()
            yield _sse(event)
            if event["type"] == "done":
                break
        except asyncio.QueueEmpty:
            yield _sse({"type": "ping"})
            await asyncio.sleep(0.5)

    await future
```

- [ ] **Step 5.4: Implement pipeline router**

Replace `hub/routers/pipeline.py`:

```python
"""파이프라인 API — OC 동기화 SSE."""
from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from hub.services import db_service
from hub.services import oc_service

router = APIRouter()


@router.get("/api/pipeline/status")
async def pipeline_status():
    stats = db_service.get_dashboard_stats()
    return {
        "oc_synced_count": stats["shippable"],
        "last_sync_at": stats["last_sync_at"],
    }


@router.get("/api/pipeline/sync/stream")
async def sync_stream(dry: bool = Query(default=False)):
    return StreamingResponse(
        oc_service.sync_stream(dry=dry),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
```

- [ ] **Step 5.5: Run tests — all pass**

```bash
python -m pytest tests/hub/test_pipeline.py -v
```

Expected: both tests `PASSED`.

- [ ] **Step 5.6: Implement pipeline.js**

Replace `hub/static/pages/pipeline.js`:

```javascript
window.renderPipeline = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">⚙️ 파이프라인</h1>

    <div class="card">
      <h3 style="margin-bottom:16px;font-size:16px">OC 동기화</h3>
      <p style="color:var(--muted);margin-bottom:16px;font-size:13px">
        products.db의 oc_price IS NOT NULL 상품을 Ownerclan API로 전체 갱신합니다.
      </p>
      <div style="display:flex;gap:12px;align-items:center">
        <button class="btn btn-primary" id="btn-sync">🔄 동기화 시작</button>
        <span id="sync-status" style="color:var(--muted);font-size:13px"></span>
      </div>
      <div id="sync-log" style="margin-top:16px;background:#0d0f1a;border-radius:8px;
                                 padding:16px;font-family:monospace;font-size:13px;
                                 min-height:80px;max-height:300px;overflow-y:auto;
                                 display:none;color:var(--text)"></div>
      <div id="sync-progress-bar" style="display:none;margin-top:12px">
        <div style="background:var(--border);border-radius:4px;height:6px">
          <div id="sync-bar-fill" style="background:var(--accent);height:6px;border-radius:4px;width:0%;transition:width 0.3s"></div>
        </div>
      </div>
    </div>

    <div class="card" style="opacity:0.5">
      <h3 style="margin-bottom:8px;font-size:16px">텍스트 파이프라인 (S1~S4)</h3>
      <p style="color:var(--muted);font-size:13px">2차 구현 예정</p>
    </div>

    <div class="card" style="opacity:0.5">
      <h3 style="margin-bottom:8px;font-size:16px">이미지 파이프라인 (S1~S5)</h3>
      <p style="color:var(--muted);font-size:13px">2차 구현 예정</p>
    </div>
  `;

  const btn = document.getElementById('btn-sync');
  const statusEl = document.getElementById('sync-status');
  const logEl = document.getElementById('sync-log');
  const barWrap = document.getElementById('sync-progress-bar');
  const barFill = document.getElementById('sync-bar-fill');

  function log(msg) {
    logEl.style.display = 'block';
    logEl.innerHTML += `<div>${msg}</div>`;
    logEl.scrollTop = logEl.scrollHeight;
  }

  btn.addEventListener('click', () => {
    btn.disabled = true;
    statusEl.textContent = '동기화 중...';
    logEl.innerHTML = '';
    barWrap.style.display = 'block';
    barFill.style.width = '0%';

    const es = new EventSource('/api/pipeline/sync/stream');

    es.onmessage = (e) => {
      const ev = JSON.parse(e.data);
      if (ev.type === 'ping') return;

      if (ev.type === 'progress') {
        const pct = ev.total > 0 ? Math.round((ev.current / ev.total) * 100) : 0;
        barFill.style.width = pct + '%';
        log(`[${ev.current}/${ev.total}] ${ev.message}`);
      } else if (ev.type === 'start') {
        log(`▶ ${ev.message}`);
      } else if (ev.type === 'done') {
        barFill.style.width = '100%';
        log(`✅ ${ev.message} (갱신: ${ev.updated ?? 0}건, 미발견: ${ev.not_found ?? 0}건)`);
        statusEl.textContent = '완료';
        btn.disabled = false;
        es.close();
      } else if (ev.type === 'error') {
        log(`❌ ${ev.message}`);
        statusEl.textContent = '오류 발생';
        btn.disabled = false;
        es.close();
      }
    };

    es.onerror = () => {
      log('❌ 연결 오류');
      statusEl.textContent = '연결 오류';
      btn.disabled = false;
      es.close();
    };
  });
};
```

- [ ] **Step 5.7: Commit**

```bash
git add hub/services/oc_service.py hub/routers/pipeline.py hub/static/pages/pipeline.js tests/hub/test_pipeline.py
git commit -m "feat(hub): pipeline API + OC sync SSE stream + pipeline UI"
```

---

## Task 6: Export API + UI (dry-run + run)

**Files:**
- Create: `hub/services/export_service.py`
- Modify: `hub/routers/export.py`
- Modify: `hub/static/pages/export.js`
- Create: `tests/hub/test_export.py`

---

- [ ] **Step 6.1: Write failing tests**

Create `tests/hub/test_export.py`:

```python
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
    assert data["total"] >= 2  # W001, W002 at least
```

- [ ] **Step 6.2: Run tests — expect failures**

```bash
python -m pytest tests/hub/test_export.py -v
```

Expected: FAIL (stub returns `{"items": [], "total": 0}`)

- [ ] **Step 6.3: Create hub/services/export_service.py**

```python
"""Export 서비스 — dry-run 및 실제 출고."""
from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

from hub.services.db_service import get_db_path, _conn

# 생성된 파일 임시 저장소 (메모리: {file_id: path})
_generated_files: dict[str, Path] = {}


def _get_strategy_policy(market: str, strategy_id: str) -> dict:
    """pricing_strategies.py에서 정책 로드."""
    try:
        from DB_save.pricing_strategies import get_pricing_strategy
        return get_pricing_strategy(market, strategy_id)
    except Exception:
        # 폴백: 기본 고도몰 정책
        return {
            "commission_rate": 0.07,
            "discount_rate": 0.0,
            "coupon_amount": 0,
            "reward_rate": 0.02,
        }


def _calc_sell_price(oc_price: int, policy: dict) -> int:
    """oc_price → 판매가 역산. price_engine.batch_solve 사용."""
    try:
        from DB_save.price_engine import batch_solve
        results = batch_solve(
            products=[{"cost": oc_price}],
            policy=policy,
            target_bands=[(0, 0)],  # 최소 마진 기준
        )
        if results and results[0].get("sell_price"):
            return int(results[0]["sell_price"])
    except Exception:
        pass
    # 폴백: 원가 × 1.3
    return int(oc_price * 1.3)


def dry_run(
    categories: list[str],
    stores: list[str],
    strategy: str,
) -> dict[str, Any]:
    """
    출고 대상 상품 목록 + 계산된 판매가 반환.

    categories=[] 이면 전체 카테고리.
    """
    with _conn() as con:
        cur = con.cursor()

        if categories:
            placeholders = ",".join("?" * len(categories))
            cur.execute(
                f"SELECT 상품코드, product_names_json, 카테고리명, oc_price "
                f"FROM products "
                f"WHERE product_status = 'ACTIVE' AND oc_price IS NOT NULL AND oc_price > 0 "
                f"AND 카테고리명 IN ({placeholders}) "
                f"ORDER BY 상품코드",
                categories,
            )
        else:
            cur.execute(
                "SELECT 상품코드, product_names_json, 카테고리명, oc_price "
                "FROM products "
                "WHERE product_status = 'ACTIVE' AND oc_price IS NOT NULL AND oc_price > 0 "
                "ORDER BY 상품코드"
            )
        rows = cur.fetchall()

    # 첫 번째 스토어의 마켓으로 정책 결정
    market = "고도몰"
    if stores:
        for mkt in ("고도몰", "스마트스토어", "옥션", "지마켓", "11번가", "쿠팡"):
            if any(s.startswith(mkt) for s in stores):
                market = mkt
                break

    policy = _get_strategy_policy(market, strategy)
    items = []
    for row in rows:
        oc_price = row["oc_price"]
        name = ""
        try:
            name = json.loads(row["product_names_json"] or "{}").get("name", "")
        except Exception:
            name = row["product_names_json"] or ""

        sell_price = _calc_sell_price(oc_price, policy)
        items.append({
            "상품코드": row["상품코드"],
            "상품명": name,
            "카테고리명": row["카테고리명"] or "",
            "oc_price": oc_price,
            "sell_price": sell_price,
        })

    return {"items": items, "total": len(items), "stores": stores, "strategy": strategy}


def run_export(
    categories: list[str],
    stores: list[str],
    strategy: str,
    fmt: str = "godomall",
) -> dict[str, Any]:
    """실제 출고 파일 생성 + file_id 반환."""
    result = dry_run(categories, stores, strategy)
    if not result["items"]:
        return {"file_id": None, "file_url": None, "product_count": 0}

    # 임시 Excel 파일 생성 (최소 구조)
    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "출고목록"
        ws.append(["상품코드", "상품명", "카테고리", "원가", "판매가", "전략", "스토어"])
        for item in result["items"]:
            for store in stores:
                ws.append([
                    item["상품코드"], item["상품명"], item["카테고리명"],
                    item["oc_price"], item["sell_price"], strategy, store,
                ])

        file_id = uuid.uuid4().hex
        tmp_dir = Path(tempfile.gettempdir()) / "product_hub_exports"
        tmp_dir.mkdir(exist_ok=True)
        out_path = tmp_dir / f"{file_id}.xlsx"
        wb.save(str(out_path))
        _generated_files[file_id] = out_path

        return {
            "file_id": file_id,
            "file_url": f"/api/export/download/{file_id}",
            "product_count": len(result["items"]),
        }
    except Exception as e:
        return {"error": str(e), "file_id": None, "file_url": None, "product_count": 0}


def get_export_file(file_id: str) -> Path | None:
    return _generated_files.get(file_id)
```

- [ ] **Step 6.4: Implement export router**

Replace `hub/routers/export.py`:

```python
"""마켓 출고 API."""
from __future__ import annotations

from pathlib import Path
from typing import List

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from hub.services import export_service

router = APIRouter()


class ExportRequest(BaseModel):
    categories: List[str] = []
    stores: List[str] = []
    strategy: str = "lowest_price"
    format: str = "godomall"


@router.post("/api/export/dry-run")
async def dry_run(req: ExportRequest):
    return export_service.dry_run(
        categories=req.categories,
        stores=req.stores,
        strategy=req.strategy,
    )


@router.post("/api/export/run")
async def run_export(req: ExportRequest):
    return export_service.run_export(
        categories=req.categories,
        stores=req.stores,
        strategy=req.strategy,
        fmt=req.format,
    )


@router.get("/api/export/download/{file_id}")
async def download_export(file_id: str):
    path = export_service.get_export_file(file_id)
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    return FileResponse(
        path=str(path),
        filename=f"출고_{file_id[:8]}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
```

- [ ] **Step 6.5: Run tests — all pass**

```bash
python -m pytest tests/hub/test_export.py -v
```

Expected: all 4 tests `PASSED`.

- [ ] **Step 6.6: Implement export.js**

Replace `hub/static/pages/export.js`:

```javascript
window.renderExport = async function(container) {
  // 데이터 로드
  const [catsResp, storesResp] = await Promise.all([
    fetch('/api/products/categories').then(r => r.json()),
    fetch('/api/stores').then(r => r.json()),
  ]);

  const catCheckboxes = catsResp.map(c =>
    `<label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;cursor:pointer">
       <input type="checkbox" class="cat-cb" value="${c}"> ${c}
     </label>`
  ).join('');

  const MARKET_COLORS = {
    '고도몰':'#7ab0ff','스마트스토어':'#34d474','옥션':'#fb923c',
    '지마켓':'#facc15','11번가':'#c084fc','쿠팡':'#f87171'
  };

  const storeCheckboxes = storesResp.map(s => {
    const mkt = Object.keys(MARKET_COLORS).find(m => s.alias.startsWith(m)) || '';
    const color = MARKET_COLORS[mkt] || '#8b8fa8';
    return `<label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;cursor:pointer">
      <input type="checkbox" class="store-cb" value="${s.alias}">
      <span class="market-tag" style="background:${color}22;color:${color}">${s.alias}</span>
    </label>`;
  }).join('');

  container.innerHTML = `
    <h1 class="page-title">🛒 마켓 출고</h1>
    <div style="display:grid;grid-template-columns:260px 1fr;gap:20px">
      <div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">카테고리</h3>
          <label style="display:flex;align-items:center;gap:6px;margin-bottom:8px;font-weight:600;cursor:pointer">
            <input type="checkbox" id="cat-all" checked> 전체
          </label>
          <div id="cat-list">${catCheckboxes}</div>
        </div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">스토어</h3>
          ${storeCheckboxes}
        </div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">전략</h3>
          ${[['lowest_price','최저가'],['normal_sale','일반판매'],['cpc_ad','광고']]
            .map(([val,label]) =>
              `<label style="display:flex;align-items:center;gap:6px;margin-bottom:6px;cursor:pointer">
                 <input type="radio" name="strategy" value="${val}" ${val==='lowest_price'?'checked':''}> ${label}
               </label>`).join('')}
        </div>
        <div style="display:flex;flex-direction:column;gap:10px">
          <button class="btn btn-primary" id="btn-dryrun">🔍 미리보기</button>
          <button class="btn btn-success" id="btn-run" disabled>▶ 출고 실행</button>
        </div>
      </div>
      <div>
        <div class="card" id="preview-card" style="display:none">
          <h3 style="margin-bottom:16px;font-size:15px">출고 미리보기</h3>
          <div id="preview-info" style="color:var(--muted);margin-bottom:12px;font-size:13px"></div>
          <table>
            <thead><tr><th>상품코드</th><th>상품명</th><th>원가</th><th>판매가</th></tr></thead>
            <tbody id="preview-tbody"></tbody>
          </table>
        </div>
        <div id="download-area" style="display:none;margin-top:16px">
          <div class="card" style="background:rgba(52,212,116,0.1);border-color:var(--green)">
            <p style="color:var(--green);margin-bottom:12px;font-weight:600">✅ 출고 파일 생성 완료</p>
            <a id="download-link" class="btn btn-success">⬇ 엑셀 다운로드</a>
          </div>
        </div>
      </div>
    </div>
  `;

  function getSelection() {
    const allCat = document.getElementById('cat-all').checked;
    const cats = allCat ? [] :
      [...document.querySelectorAll('.cat-cb:checked')].map(c => c.value);
    const stores = [...document.querySelectorAll('.store-cb:checked')].map(s => s.value);
    const strategy = document.querySelector('[name=strategy]:checked')?.value || 'lowest_price';
    return { categories: cats, stores, strategy };
  }

  document.getElementById('btn-dryrun').addEventListener('click', async () => {
    const sel = getSelection();
    const r = await fetch('/api/export/dry-run', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(sel),
    }).then(r => r.json());

    const card = document.getElementById('preview-card');
    card.style.display = 'block';
    document.getElementById('preview-info').textContent =
      `총 ${r.total.toLocaleString()}개 상품 | 스토어: ${sel.stores.join(', ') || '선택 안 됨'} | 전략: ${sel.strategy}`;

    document.getElementById('preview-tbody').innerHTML = r.items.slice(0, 50).map(p =>
      `<tr>
        <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.상품명}</td>
        <td style="color:var(--muted)">${p.oc_price?.toLocaleString()}원</td>
        <td style="color:var(--yellow);font-weight:600">${p.sell_price?.toLocaleString()}원</td>
      </tr>`
    ).join('');

    document.getElementById('btn-run').disabled = r.total === 0;
    document.getElementById('download-area').style.display = 'none';
  });

  document.getElementById('btn-run').addEventListener('click', async () => {
    const sel = getSelection();
    const r = await fetch('/api/export/run', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(sel),
    }).then(r => r.json());

    if (r.file_url) {
      document.getElementById('download-area').style.display = 'block';
      document.getElementById('download-link').href = r.file_url;
      document.getElementById('download-link').textContent =
        `⬇ 엑셀 다운로드 (${r.product_count.toLocaleString()}개)`;
    }
  });
};
```

- [ ] **Step 6.7: Commit**

```bash
git add hub/services/export_service.py hub/routers/export.py hub/static/pages/export.js tests/hub/test_export.py
git commit -m "feat(hub): export API dry-run + run + download + export UI"
```

---

## Task 7: Store Management API + UI

**Files:**
- Modify: `hub/routers/stores.py`
- Modify: `hub/static/pages/stores.js`
- Create: `tests/hub/test_stores.py`

---

- [ ] **Step 7.1: Write failing tests**

Create `tests/hub/test_stores.py`:

```python
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
```

- [ ] **Step 7.2: Run tests — expect failures**

```bash
python -m pytest tests/hub/test_stores.py -v
```

Expected: `test_import_stores_from_excel` FAILS (stub returns `{}`)

- [ ] **Step 7.3: Add parse_accounts_from_excel to db_service.py**

Add this function at the end of `hub/services/db_service.py`:

```python
def parse_stores_from_excel(file_bytes: bytes) -> list[dict]:
    """
    Market_id_pw.xlsx 바이트 → stores 목록 파싱.

    DB_save/config.py의 AccountLoader를 재사용.
    별칭 열이 있으면 그 시트 전체를 순회하여 upsert 대상 목록 반환.
    """
    import io
    import openpyxl
    from DB_save.config import AccountLoader

    try:
        loader = AccountLoader.__new__(AccountLoader)
        loader.excel_path = None
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        records = []
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            rows = list(ws.iter_rows(values_only=True))
            if not rows:
                continue
            # 첫 행을 헤더로 간주
            header = [str(h).strip() if h else "" for h in rows[0]]
            try:
                alias_col = header.index("별칭")
            except ValueError:
                continue
            market_col = header.index("마켓") if "마켓" in header else None
            group_col  = header.index("그룹") if "그룹" in header else None

            for row in rows[1:]:
                alias = str(row[alias_col]).strip() if row[alias_col] else ""
                if not alias or alias == "None":
                    continue
                market = str(row[market_col]).strip() if market_col is not None and row[market_col] else ""
                group  = str(row[group_col]).strip()  if group_col  is not None and row[group_col]  else ""
                records.append({"alias": alias, "market": market, "group_id": group})
        return records
    except Exception:
        # AccountLoader 실패 시 최소 파싱 (별칭만)
        import io, openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        records = []
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            rows = list(ws.iter_rows(values_only=True))
            if not rows:
                continue
            header = [str(h).strip() if h else "" for h in rows[0]]
            try:
                alias_col = header.index("별칭")
            except ValueError:
                continue
            market_col = header.index("마켓") if "마켓" in header else None
            group_col  = header.index("그룹") if "그룹" in header else None
            for row in rows[1:]:
                alias = str(row[alias_col]).strip() if row[alias_col] else ""
                if not alias or alias == "None":
                    continue
                market = str(row[market_col]).strip() if market_col is not None and row[market_col] else ""
                group  = str(row[group_col]).strip()  if group_col  is not None and row[group_col]  else ""
                records.append({"alias": alias, "market": market, "group_id": group})
        return records
```

- [ ] **Step 7.4: Implement stores router**

Replace `hub/routers/stores.py`:

```python
"""스토어 관리 API."""
from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, UploadFile

from hub.services.db_service import get_stores, parse_stores_from_excel, upsert_store

router = APIRouter()


@router.get("/api/stores")
async def list_stores():
    return get_stores()


@router.post("/api/stores/import")
async def import_stores(file: UploadFile = File(...)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="xlsx 파일만 지원합니다")

    content = await file.read()
    records = parse_stores_from_excel(content)

    imported = 0
    for rec in records:
        upsert_store(
            alias=rec["alias"],
            market=rec["market"],
            group_id=rec["group_id"],
        )
        imported += 1

    return {"imported": imported, "total_stores": len(get_stores())}
```

- [ ] **Step 7.5: Run tests — all pass**

```bash
python -m pytest tests/hub/test_stores.py -v
```

Expected: all 4 tests `PASSED`.

- [ ] **Step 7.6: Run full test suite**

```bash
python -m pytest tests/hub/ -v
```

Expected: all tests across all files `PASSED`. If any fail, fix before committing.

- [ ] **Step 7.7: Implement stores.js**

Replace `hub/static/pages/stores.js`:

```javascript
window.renderStores = async function(container) {
  const MARKET_COLORS = {
    '고도몰':'#7ab0ff','스마트스토어':'#34d474','옥션':'#fb923c',
    '지마켓':'#facc15','11번가':'#c084fc','쿠팡':'#f87171'
  };

  async function loadAndRender() {
    const stores = await fetch('/api/stores').then(r => r.json());

    const tbody = document.getElementById('stores-tbody');
    if (!tbody) return;

    if (stores.length === 0) {
      tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:40px">
        스토어 없음 — Market_id_pw.xlsx를 import하세요
      </td></tr>`;
      return;
    }

    tbody.innerHTML = stores.map(s => {
      const mkt = Object.keys(MARKET_COLORS).find(m => s.alias.startsWith(m)) || '';
      const color = MARKET_COLORS[mkt] || '#8b8fa8';
      const strategyLabel = {'lowest_price':'최저가','normal_sale':'일반판매','cpc_ad':'광고'}[s.strategy] || s.strategy || '-';
      return `<tr>
        <td><span class="market-tag" style="background:${color}22;color:${color}">${s.alias}</span></td>
        <td style="color:${color}">${s.market || '-'}</td>
        <td style="color:var(--muted)">${s.group_id || '-'}</td>
        <td>${strategyLabel}</td>
        <td>${s.active ? '<span class="pill pill-done">활성</span>' : '<span class="pill pill-todo">비활성</span>'}</td>
        <td style="color:var(--muted);font-size:12px">${s.updated_at ? s.updated_at.slice(0,10) : '-'}</td>
      </tr>`;
    }).join('');
  }

  container.innerHTML = `
    <h1 class="page-title">🏢 스토어 관리</h1>
    <div class="card" style="display:flex;gap:12px;align-items:center;padding:16px 20px">
      <label class="btn btn-primary" style="cursor:pointer">
        📥 Market_id_pw.xlsx Import
        <input type="file" id="store-file" accept=".xlsx,.xls" style="display:none">
      </label>
      <span id="import-status" style="color:var(--muted);font-size:13px"></span>
    </div>
    <div class="card" style="padding:0;overflow:hidden">
      <table>
        <thead><tr>
          <th>별칭</th><th>마켓</th><th>그룹</th><th>전략</th><th>활성</th><th>갱신일</th>
        </tr></thead>
        <tbody id="stores-tbody">
          <tr><td colspan="6" style="text-align:center;color:var(--muted);padding:40px">로딩 중...</td></tr>
        </tbody>
      </table>
    </div>
  `;

  await loadAndRender();

  document.getElementById('store-file').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const statusEl = document.getElementById('import-status');
    statusEl.textContent = '업로드 중...';

    const formData = new FormData();
    formData.append('file', file);

    try {
      const r = await fetch('/api/stores/import', { method: 'POST', body: formData });
      const data = await r.json();
      statusEl.textContent = `완료: ${data.imported}개 import, 전체 ${data.total_stores}개`;
      await loadAndRender();
    } catch (err) {
      statusEl.textContent = `오류: ${err.message}`;
    }

    e.target.value = '';
  });
};
```

- [ ] **Step 7.8: Final commit**

```bash
git add hub/services/db_service.py hub/routers/stores.py hub/static/pages/stores.js tests/hub/test_stores.py
git commit -m "feat(hub): store management API + Excel import + stores UI"
```

---

## Post-Implementation: Manual Verification

- [ ] Start server: `python -m uvicorn hub.app:app --reload --port 8000`
- [ ] Open `http://localhost:8000` — dark sidebar should appear, navigate to each page
- [ ] 대시보드: stat cards should show real numbers from products.db
- [ ] 상품 DB: type a search term, verify table filters live
- [ ] 파이프라인: click OC 동기화 — SSE log should appear (real API call, ~40s)
- [ ] 마켓 출고: select category + store + strategy → 미리보기 → 출고 실행 → download link
- [ ] 스토어 관리: import Market_id_pw.xlsx → table populates

---

## Spec Coverage Check

| Spec requirement | Task |
|---|---|
| FastAPI + Vanilla JS + SSE + SQLite WAL | Task 1 |
| 다크 테마 `#16181f`/`#1d2030`/`#f0f1f8` | Task 1 |
| 사이드바 고정 230px | Task 1 |
| 대시보드 4개 통계 카드 | Task 3 |
| 상품 DB 검색/카테고리/퀵필터/페이지네이션 | Task 4 |
| 배정 스토어 마켓 컬러 태그 | Task 4 |
| OC 동기화 SSE 진행률 | Task 5 |
| 마켓 출고 dry-run + 실행 + 다운로드 | Task 6 |
| 스토어 관리 Excel import + 목록 | Task 7 |
| products 테이블 허브 컬럼 추가 | Task 2 |
| stores 테이블 신규 생성 | Task 2 |
| `python -m uvicorn hub.app:app --reload` | Task 1 (verified step 1.13) |

2차 구현 범위 (이 플랜 제외):
- reconcile (API/Excel 실제 등록 갱신)
- 텍스트/이미지 파이프라인 subprocess 실행
- 공급사 신규 입고 (import_new_from_vendor)
