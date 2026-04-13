# OC 카탈로그 증분 동기화 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 등록 공급사별 증분 스캔(vendor-scan) + 신규 공급사 발견 경량 스캔(discovery-scan) API 구현 + Hermes Agent 스케줄 등록

**Architecture:** `catalog_service.py`에 `vendor_scan()`, `discovery_scan()` 함수를 기존 `sync_from_existing_products()` 패턴과 동일하게 추가. `search_items(vendor=code)` API로 공급사별 전체 상품 페이지네이션 조회. `_start_bg_job()` 래퍼로 백그라운드 실행. Schedule (Remote Triggers)로 일간/주간/월간 Cron 등록.

**Tech Stack:** FastAPI, SQLite WAL, OwnerclanClient (GraphQL), threading, pytest, Claude Code Schedule

---

## 파일 구조

| 파일 | 역할 |
|------|------|
| `hub/services/catalog_service.py` | MOD: vendor_scan(), discovery_scan(), start_*_bg() 추가 |
| `hub/routers/catalog.py` | MOD: 2개 엔드포인트 추가 |
| `tests/hub/test_catalog_sync.py` | NEW: vendor_scan, discovery_scan 유닛 테스트 |

---

## Task 1: vendor_scan — 등록 공급사별 증분 스캔

**Files:**
- Modify: `hub/services/catalog_service.py`
- Modify: `hub/routers/catalog.py`
- Create: `tests/hub/test_catalog_sync.py`

- [ ] **Step 1: 테스트 파일 생성**

```python
# tests/hub/test_catalog_sync.py
"""vendor_scan + discovery_scan 테스트."""
import os
import sqlite3
import json
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


@pytest.fixture
def catalog_db(tmp_path):
    """임시 oc_catalog.db."""
    db_path = str(tmp_path / "oc_catalog_test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS oc_items (
            key TEXT PRIMARY KEY, vendor_code TEXT, status TEXT,
            price_krw INTEGER, shipping_fee INTEGER, shipping_type TEXT,
            bundle_shipping TEXT, openmarket_sellable INTEGER,
            release_rate TEXT, average_ship TEXT,
            last_scanned_at TEXT, last_updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS oc_catalog_meta (
            key TEXT PRIMARY KEY, value TEXT
        );
    """)
    conn.close()
    return db_path


def _make_oc_item(key: str, vendor_code: str, price: int = 10000) -> dict:
    """OC API 응답 모의 아이템."""
    return {
        "key": key,
        "status": "available",
        "price": price,
        "shippingFee": 3000,
        "shippingType": "FREE_ABOVE",
        "openmarketSellable": True,
        "metadata": {"vendorKey": vendor_code, "bundleShipping": "Y",
                      "gradeDetail": {"releaseRate": "98", "averageShip": "1.2"}},
    }


def _seed_vendor(test_db_path, vendor_code="V001", vendor_name="테스트공급사", status="active"):
    conn = sqlite3.connect(test_db_path)
    conn.execute(
        """INSERT OR IGNORE INTO vendors (vendor_code, vendor_name, status, source)
           VALUES (?, ?, ?, 'oc')""",
        [vendor_code, vendor_name, status],
    )
    conn.commit()
    conn.close()


# ── vendor_scan 테스트 ──

def test_vendor_scan_scans_active_vendors(test_db_path, catalog_db):
    """등록된 active 공급사의 상품이 oc_catalog.db에 저장되는지 확인."""
    _seed_vendor(test_db_path, "V001", "테스트공급사A")

    mock_items = [_make_oc_item("W100", "V001", 15000), _make_oc_item("W101", "V001", 20000)]

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = mock_items

        from hub.services.catalog_service import vendor_scan
        result = vendor_scan()

    assert result["scanned_vendors"] == 1
    assert result["total_items"] >= 2

    conn = sqlite3.connect(catalog_db)
    count = conn.execute("SELECT COUNT(*) FROM oc_items WHERE vendor_code='V001'").fetchone()[0]
    conn.close()
    assert count == 2


def test_vendor_scan_skips_inactive_vendors(test_db_path, catalog_db):
    """inactive 공급사는 스캔하지 않음."""
    _seed_vendor(test_db_path, "V002", "비활성공급사", status="inactive")

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = []

        from hub.services.catalog_service import vendor_scan
        result = vendor_scan()

    assert result["scanned_vendors"] == 0
    instance.search_items.assert_not_called()


def test_vendor_scan_detects_new_items(test_db_path, catalog_db):
    """oc_catalog.db에 없던 상품은 new_items로 카운트."""
    _seed_vendor(test_db_path, "V003")

    # 기존 아이템 1개 seed
    conn = sqlite3.connect(catalog_db)
    conn.execute(
        "INSERT INTO oc_items (key, vendor_code, status) VALUES ('W200', 'V003', 'available')")
    conn.commit()
    conn.close()

    mock_items = [_make_oc_item("W200", "V003"), _make_oc_item("W201", "V003")]

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = mock_items

        from hub.services.catalog_service import vendor_scan
        result = vendor_scan()

    assert result["new_items"] >= 1  # W201 is new


def test_vendor_scan_endpoint_returns_started(client, test_db_path):
    """POST /api/catalog/vendor-scan/start 가 started=True 반환."""
    with patch("hub.services.catalog_service._scan_state", {"running": False}), \
         patch("hub.services.catalog_service._scan_lock"), \
         patch("hub.services.catalog_service.vendor_scan", return_value={}):
        r = client.post("/api/catalog/vendor-scan/start")
    assert r.status_code == 200
```

- [ ] **Step 2: 테스트 실행 (실패 확인)**

```bash
cd "C:\Users\kohaz\Desktop\Python\파이썬자동화파일\상품가공프로그램\.worktrees\feat-pricing"
python -m pytest tests/hub/test_catalog_sync.py -v 2>&1 | head -20
```
Expected: ImportError — `vendor_scan` 아직 없음

- [ ] **Step 3: catalog_service.py에 vendor_scan() 함수 추가**

`hub/services/catalog_service.py` 파일 하단 (기존 `sync_from_existing_products` 함수 다음, `_start_bg_job` 전)에 추가:

```python
# ── 공급사별 증분 스캔 ─────────────────────────────────────────────────────────

def vendor_scan(progress_callback=None) -> dict:
    """등록 공급사별 allItems(vendor=code) 스캔 → oc_catalog.db upsert + products.db backfill.

    vendors 테이블에서 status != 'inactive' 공급사 조회 → 각 공급사 순차 스캔.
    소요 시간: ~10분 (공급사 50개 × 평균 1000상품 기준)
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from godomall_register.ownerclan_client import OwnerclanClient, OwnerclanApiError
    from hub.services.db_service import get_db_path

    now_iso = datetime.now(timezone.utc).isoformat()
    result = {
        "scanned_vendors": 0,
        "total_items": 0,
        "new_items": 0,
        "updated_items": 0,
        "backfilled": 0,
        "errors": [],
        "duration_sec": 0.0,
        "synced_at": now_iso,
    }

    import time
    t0 = time.monotonic()

    products_path = get_db_path()
    catalog_path = get_catalog_db_path()
    client = OwnerclanClient()

    # vendors 테이블에서 active 공급사 조회
    prod_conn = sqlite3.connect(products_path)
    prod_conn.row_factory = sqlite3.Row
    try:
        vendors = prod_conn.execute(
            "SELECT vendor_code FROM vendors WHERE status != 'inactive' AND vendor_code IS NOT NULL"
        ).fetchall()
    finally:
        prod_conn.close()

    vendor_codes = [r["vendor_code"] for r in vendors]
    if not vendor_codes:
        result["duration_sec"] = time.monotonic() - t0
        return result

    if progress_callback:
        progress_callback(0, len(vendor_codes), f"공급사 {len(vendor_codes)}개 스캔 시작...")

    # oc_catalog.db 준비
    cat_conn = sqlite3.connect(catalog_path)
    try:
        _init_catalog(cat_conn)
        cat_cur = cat_conn.cursor()

        # 기존 키 SET 수집 (신규 감지용)
        existing_keys = set(
            r[0] for r in cat_cur.execute("SELECT key FROM oc_items").fetchall()
        )

        all_new_keys = []

        for vi, vc in enumerate(vendor_codes):
            if progress_callback:
                progress_callback(vi, len(vendor_codes),
                    f"공급사 {vi+1}/{len(vendor_codes)}: {vc} 스캔 중...")

            try:
                items = client.search_items(
                    vendor=vc, status="available",
                    fields=CATALOG_FIELDS, first=100, timeout=180,
                )
            except OwnerclanApiError as exc:
                result["errors"].append(f"공급사 {vc}: {exc}")
                continue

            result["scanned_vendors"] += 1
            result["total_items"] += len(items)

            batch_new_keys = []
            for item in items:
                key = item.get("key", "")
                if not key:
                    continue

                is_new = key not in existing_keys
                if is_new:
                    result["new_items"] += 1
                    batch_new_keys.append(key)
                    existing_keys.add(key)
                else:
                    result["updated_items"] += 1

                metadata = item.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                grade = metadata.get("gradeDetail") or {}

                cat_cur.execute(
                    """INSERT INTO oc_items
                       (key, vendor_code, status, price_krw, shipping_fee,
                        shipping_type, bundle_shipping, openmarket_sellable,
                        release_rate, average_ship, last_scanned_at, last_updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(key) DO UPDATE SET
                         vendor_code         = excluded.vendor_code,
                         status              = excluded.status,
                         price_krw           = excluded.price_krw,
                         shipping_fee        = excluded.shipping_fee,
                         shipping_type       = excluded.shipping_type,
                         bundle_shipping     = excluded.bundle_shipping,
                         openmarket_sellable = excluded.openmarket_sellable,
                         release_rate        = excluded.release_rate,
                         average_ship        = excluded.average_ship,
                         last_updated_at     = excluded.last_updated_at""",
                    [key,
                     str(metadata["vendorKey"]) if metadata.get("vendorKey") else vc,
                     item.get("status"), item.get("price"), item.get("shippingFee"),
                     item.get("shippingType"), metadata.get("bundleShipping"),
                     1 if item.get("openmarketSellable") else 0,
                     grade.get("releaseRate", ""), grade.get("averageShip", ""),
                     now_iso, now_iso],
                )

            cat_conn.commit()
            all_new_keys.extend(batch_new_keys)

        # 메타 갱신
        cat_cur.execute(
            "INSERT OR REPLACE INTO oc_catalog_meta VALUES ('last_vendor_scan_at', ?)", [now_iso])
        cat_cur.execute(
            "INSERT OR REPLACE INTO oc_catalog_meta VALUES ('vendor_scan_count', ?)",
            [str(result["scanned_vendors"])])
        cat_conn.commit()
    finally:
        cat_conn.close()

    # products.db backfill (신규 키 → UPDATE 기존 + INSERT 미등록)
    # 주의: products.상품코드에 UNIQUE 제약 없음 — ON CONFLICT 사용 불가
    if progress_callback:
        progress_callback(len(vendor_codes), len(vendor_codes),
            f"products.db backfill 중...")
    try:
        prod_conn = sqlite3.connect(products_path)
        cat_conn2 = sqlite3.connect(catalog_path)
        try:
            prod_conn.execute("PRAGMA journal_mode=WAL")
            pc = prod_conn.cursor()
            cc = cat_conn2.cursor()

            # 기존 products.db 상품코드 SET
            pc.execute("SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
            existing_prod_keys = {r[0] for r in pc.fetchall()}

            # 이번 스캔에서 처리한 모든 키의 oc_items 조회
            all_scanned = list(existing_keys)  # vendor_scan에서 수집한 전체 키
            CHUNK = 5000
            bf_count = 0
            for ci in range(0, len(all_scanned), CHUNK):
                chunk = all_scanned[ci:ci+CHUNK]
                placeholders = ",".join("?" * len(chunk))
                cc.execute(
                    f"SELECT key, vendor_code, status, price_krw, shipping_fee, "
                    f"shipping_type, bundle_shipping, openmarket_sellable "
                    f"FROM oc_items WHERE key IN ({placeholders})",
                    chunk,
                )
                for row in cc.fetchall():
                    key, vc, status, price_krw, ship_fee, ship_type, bundle_ship, om_sell = row
                    product_status = _OC_STATUS_TO_PRODUCT_STATUS.get(status or "", "ACTIVE")
                    if key in existing_prod_keys:
                        # 기존 상품 → UPDATE (COALESCE: 기존값 보존)
                        pc.execute(
                            """UPDATE products SET
                                 vendor_code         = COALESCE(vendor_code, ?),
                                 oc_price            = ?,
                                 oc_shipping_fee     = ?,
                                 oc_shipping_type    = ?,
                                 oc_bundle_ship      = ?,
                                 oc_status           = ?,
                                 oc_openmarket_sellable = ?,
                                 product_status      = ?,
                                 oc_synced_at        = ?
                               WHERE 상품코드 = ?""",
                            [vc, price_krw, ship_fee, ship_type, bundle_ship,
                             status, om_sell, product_status, now_iso, key],
                        )
                        bf_count += prod_conn.execute("SELECT changes()").fetchone()[0]
                    elif key in set(all_new_keys):
                        # 신규 상품 → INSERT
                        pc.execute(
                            """INSERT INTO products
                               (상품코드, vendor_code, oc_price, oc_shipping_fee,
                                oc_shipping_type, oc_bundle_ship, oc_status,
                                oc_openmarket_sellable, product_status, oc_synced_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            [key, vc, price_krw, ship_fee, ship_type,
                             bundle_ship, status, om_sell, product_status, now_iso],
                        )
                        bf_count += 1

            prod_conn.commit()
            result["backfilled"] = bf_count
        finally:
            prod_conn.close()
            cat_conn2.close()
    except Exception as exc:
        result["errors"].append(f"products.db backfill 실패: {exc}")
        logger.exception("vendor_scan backfill error")

    # vendors 테이블 product_count 갱신
    try:
        prod_conn = sqlite3.connect(products_path)
        cat_conn3 = sqlite3.connect(catalog_path)
        try:
            for vc in vendor_codes:
                cnt = cat_conn3.execute(
                    "SELECT COUNT(*) FROM oc_items WHERE vendor_code=?", [vc]
                ).fetchone()[0]
                prod_conn.execute(
                    "UPDATE vendors SET product_count=?, updated_at=datetime('now','localtime') WHERE vendor_code=?",
                    [cnt, vc],
                )
            prod_conn.commit()
        finally:
            prod_conn.close()
            cat_conn3.close()
    except Exception as exc:
        result["errors"].append(f"vendors 갱신 실패: {exc}")

    result["duration_sec"] = round(time.monotonic() - t0, 1)

    if progress_callback:
        progress_callback(len(vendor_codes), len(vendor_codes),
            f"완료 — 공급사 {result['scanned_vendors']}개 · "
            f"상품 {result['total_items']:,}개 · "
            f"신규 {result['new_items']}개 · backfill {result['backfilled']}개")

    logger.info("vendor_scan done: vendors=%d items=%d new=%d backfilled=%d errors=%d",
                result["scanned_vendors"], result["total_items"],
                result["new_items"], result["backfilled"], len(result["errors"]))
    return result
```

- [ ] **Step 4: catalog_service.py에 start_vendor_scan_bg() 추가**

기존 `start_sync_existing_bg()` 바로 아래에 추가:

```python
def start_vendor_scan_bg() -> dict:
    """등록 공급사별 증분 스캔 백그라운드 시작."""
    return _start_bg_job("vendor_scan", vendor_scan)
```

- [ ] **Step 5: catalog.py에 엔드포인트 추가**

`hub/routers/catalog.py`에 추가:

```python
@router.post("/api/catalog/vendor-scan/start")
async def start_vendor_scan():
    from hub.services.catalog_service import start_vendor_scan_bg
    return start_vendor_scan_bg()
```

- [ ] **Step 6: 테스트 실행 (통과 확인)**

```bash
python -m pytest tests/hub/test_catalog_sync.py -v
```
Expected: 4 tests PASS

- [ ] **Step 7: 전체 테스트 실행 (리그레션 확인)**

```bash
python -m pytest tests/hub/ -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 8: 커밋**

```bash
git add hub/services/catalog_service.py hub/routers/catalog.py tests/hub/test_catalog_sync.py
git commit -m "feat: vendor-scan — 등록 공급사별 증분 스캔 API

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 2: discovery_scan — 신규 공급사 발견 경량 스캔

**Files:**
- Modify: `hub/services/catalog_service.py`
- Modify: `hub/routers/catalog.py`
- Modify: `tests/hub/test_catalog_sync.py`

- [ ] **Step 1: 테스트 추가**

`tests/hub/test_catalog_sync.py` 하단에 추가:

```python
# ── discovery_scan 테스트 ──

DISCOVERY_FIELDS = "key metadata"


def test_discovery_scan_finds_new_vendors(test_db_path, catalog_db):
    """기존 oc_catalog.db에 없는 vendor_code가 발견되면 vendors 테이블에 추가."""
    # 기존 vendor V001은 이미 oc_catalog.db에 있음
    conn = sqlite3.connect(catalog_db)
    conn.execute("INSERT INTO oc_items (key, vendor_code, status) VALUES ('W300', 'V001', 'available')")
    conn.commit()
    conn.close()

    # API 응답: V001(기존) + V999(신규)
    mock_items = [
        {"key": "W300", "metadata": {"vendorKey": "V001"}},
        {"key": "W400", "metadata": {"vendorKey": "V999"}},
        {"key": "W401", "metadata": {"vendorKey": "V999"}},
    ]

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = mock_items

        from hub.services.catalog_service import discovery_scan
        result = discovery_scan()

    assert result["new_vendors"] >= 1
    assert "V999" in result["new_vendor_codes"]

    # vendors 테이블에 V999가 status='discovered'로 삽입되었는지
    conn = sqlite3.connect(test_db_path)
    row = conn.execute("SELECT status FROM vendors WHERE vendor_code='V999'").fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "discovered"


def test_discovery_scan_counts_known_vendors(test_db_path, catalog_db):
    """이미 알려진 vendor는 new_vendors에 포함되지 않음."""
    conn = sqlite3.connect(catalog_db)
    conn.execute("INSERT INTO oc_items (key, vendor_code, status) VALUES ('W500', 'V010', 'available')")
    conn.commit()
    conn.close()

    mock_items = [
        {"key": "W500", "metadata": {"vendorKey": "V010"}},
    ]

    with patch("hub.services.catalog_service.get_catalog_db_path", return_value=catalog_db), \
         patch("hub.services.catalog_service.OwnerclanClient") as MockClient:
        instance = MockClient.return_value
        instance.search_items.return_value = mock_items

        from hub.services.catalog_service import discovery_scan
        result = discovery_scan()

    assert result["new_vendors"] == 0


def test_discovery_scan_endpoint(client):
    """POST /api/catalog/discovery-scan/start 가 응답."""
    with patch("hub.services.catalog_service._scan_state", {"running": False}), \
         patch("hub.services.catalog_service._scan_lock"), \
         patch("hub.services.catalog_service.discovery_scan", return_value={}):
        r = client.post("/api/catalog/discovery-scan/start")
    assert r.status_code == 200
```

- [ ] **Step 2: 테스트 실행 (실패 확인)**

```bash
python -m pytest tests/hub/test_catalog_sync.py::test_discovery_scan_finds_new_vendors -v
```
Expected: ImportError — `discovery_scan` 아직 없음

- [ ] **Step 3: catalog_service.py에 discovery_scan() 함수 추가**

`vendor_scan()` 함수 다음, `start_vendor_scan_bg()` 전에 추가:

```python
# ── 신규 공급사 발견 경량 스캔 ─────────────────────────────────────────────────

DISCOVERY_FIELDS = "key metadata"


def discovery_scan(progress_callback=None) -> dict:
    """allItems(status=available) 경량 스캔 — key+vendorKey만 수집 → 신규 공급사 발견.

    oc_catalog.db의 기존 vendor_code SET과 비교하여 신규 공급사를 vendors 테이블에 삽입.
    소요 시간: ~3-4시간 (9.4M 상품, key+metadata만)
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from godomall_register.ownerclan_client import OwnerclanClient, OwnerclanApiError
    from hub.services.db_service import get_db_path

    import time
    t0 = time.monotonic()
    now_iso = datetime.now(timezone.utc).isoformat()
    result = {
        "total_scanned": 0,
        "known_vendors": 0,
        "new_vendors": 0,
        "new_vendor_codes": [],
        "errors": [],
        "duration_sec": 0.0,
    }

    catalog_path = get_catalog_db_path()
    products_path = get_db_path()
    client = OwnerclanClient()

    # 기존 vendor_code SET 수집
    cat_conn = sqlite3.connect(catalog_path)
    try:
        _init_catalog(cat_conn)
        existing_vendors = set(
            r[0] for r in cat_conn.execute(
                "SELECT DISTINCT vendor_code FROM oc_items WHERE vendor_code IS NOT NULL"
            ).fetchall()
        )
    finally:
        cat_conn.close()

    if progress_callback:
        progress_callback(0, 0, f"기존 공급사 {len(existing_vendors):,}개. 전체 스캔 시작...")

    # allItems 경량 스캔
    scanned_vendors: dict[str, int] = {}  # vendor_code → 상품 수

    try:
        items = client.search_items(
            status="available",
            fields=DISCOVERY_FIELDS,
            first=1000,
            timeout=300,
        )
    except OwnerclanApiError as exc:
        result["errors"].append(f"allItems 스캔 실패: {exc}")
        result["duration_sec"] = round(time.monotonic() - t0, 1)
        return result

    result["total_scanned"] = len(items)

    for item in items:
        metadata = item.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}
        vc = str(metadata.get("vendorKey", "")).strip()
        if vc:
            scanned_vendors[vc] = scanned_vendors.get(vc, 0) + 1

    if progress_callback:
        progress_callback(result["total_scanned"], result["total_scanned"],
            f"스캔 완료. 공급사 {len(scanned_vendors):,}개 발견. 비교 중...")

    # 신규 공급사 = 스캔에서 발견 - 기존
    new_vendor_codes = set(scanned_vendors.keys()) - existing_vendors
    result["known_vendors"] = len(scanned_vendors) - len(new_vendor_codes)
    result["new_vendors"] = len(new_vendor_codes)
    result["new_vendor_codes"] = sorted(new_vendor_codes)[:20]  # 상위 20개만 반환

    # vendors 테이블에 신규 공급사 삽입
    if new_vendor_codes:
        try:
            prod_conn = sqlite3.connect(products_path)
            try:
                for vc in new_vendor_codes:
                    prod_conn.execute(
                        """INSERT OR IGNORE INTO vendors
                           (vendor_code, vendor_name, source, product_count, status)
                           VALUES (?, ?, 'oc_discovery', ?, 'discovered')""",
                        [vc, f"자동발견_{vc}", scanned_vendors.get(vc, 0)],
                    )
                prod_conn.commit()
            finally:
                prod_conn.close()
        except Exception as exc:
            result["errors"].append(f"vendors 삽입 실패: {exc}")

    # 메타 갱신
    try:
        cat_conn2 = sqlite3.connect(catalog_path)
        try:
            cat_conn2.execute(
                "INSERT OR REPLACE INTO oc_catalog_meta VALUES ('last_discovery_scan_at', ?)", [now_iso])
            cat_conn2.execute(
                "INSERT OR REPLACE INTO oc_catalog_meta VALUES ('discovery_new_vendors', ?)",
                [str(result["new_vendors"])])
            cat_conn2.commit()
        finally:
            cat_conn2.close()
    except Exception as exc:
        result["errors"].append(f"메타 갱신 실패: {exc}")

    result["duration_sec"] = round(time.monotonic() - t0, 1)

    if progress_callback:
        progress_callback(result["total_scanned"], result["total_scanned"],
            f"완료 — 스캔 {result['total_scanned']:,}개 · "
            f"신규 공급사 {result['new_vendors']}개 · "
            f"기존 공급사 {result['known_vendors']}개")

    logger.info("discovery_scan done: scanned=%d new_vendors=%d known=%d errors=%d",
                result["total_scanned"], result["new_vendors"],
                result["known_vendors"], len(result["errors"]))
    return result
```

- [ ] **Step 4: catalog_service.py에 start_discovery_scan_bg() 추가**

`start_vendor_scan_bg()` 바로 아래에 추가:

```python
def start_discovery_scan_bg() -> dict:
    """경량 전체 스캔 (신규 공급사 발견) 백그라운드 시작."""
    return _start_bg_job("discovery_scan", discovery_scan)
```

- [ ] **Step 5: catalog.py에 엔드포인트 추가**

```python
@router.post("/api/catalog/discovery-scan/start")
async def start_discovery_scan():
    from hub.services.catalog_service import start_discovery_scan_bg
    return start_discovery_scan_bg()
```

- [ ] **Step 6: 테스트 실행 (통과 확인)**

```bash
python -m pytest tests/hub/test_catalog_sync.py -v
```
Expected: 7 tests PASS (Task 1의 4개 + Task 2의 3개)

- [ ] **Step 7: 전체 테스트 실행**

```bash
python -m pytest tests/hub/ -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 8: 커밋**

```bash
git add hub/services/catalog_service.py hub/routers/catalog.py tests/hub/test_catalog_sync.py
git commit -m "feat: discovery-scan — 신규 공급사 발견 경량 스캔 API

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 3: Hermes Agent Schedule 등록

**Files:**
- 없음 (CLI로 Schedule trigger 등록)

- [ ] **Step 1: 일간 sync-existing schedule 등록**

```bash
# Claude Code Schedule 스킬로 등록
# /schedule create
```

Agent prompt:
```
Hub 서버(localhost:8080)에 POST /api/catalog/sync-existing/start 호출.
GET /api/catalog/scan/status를 30초 간격으로 폴링하여 running=false 될 때까지 대기.
완료 후 result에서 total_products, updated, not_found, errors 추출.
텔레그램으로 결과 리포트 전송:
"📊 일간 동기화 완료
- 조회: {total_products}개
- 갱신: {updated}개
- 미발견: {not_found}개
- 오류: {errors 수}건"
오류 발생 시 "⚠ sync-existing 실패: {error}" 전송.
```

Schedule: `0 3 * * *` (매일 03:00)

- [ ] **Step 2: 주간 vendor-scan schedule 등록**

Agent prompt:
```
Hub 서버(localhost:8080)에 POST /api/catalog/vendor-scan/start 호출.
GET /api/catalog/scan/status를 30초 간격으로 폴링하여 running=false 될 때까지 대기.
완료 후 result에서 scanned_vendors, total_items, new_items, backfilled, errors 추출.
텔레그램으로 결과 리포트 전송:
"📦 주간 공급사 스캔 완료
- 스캔 공급사: {scanned_vendors}개
- 총 상품: {total_items}개
- 신규 상품: {new_items}개
- backfill: {backfilled}개
- 오류: {errors 수}건"
오류 발생 시 "⚠ vendor-scan 실패: {error}" 전송.
```

Schedule: `0 4 * * 0` (매주 일요일 04:00)

- [ ] **Step 3: 월간 discovery-scan schedule 등록**

Agent prompt:
```
Hub 서버(localhost:8080)에 POST /api/catalog/discovery-scan/start 호출.
GET /api/catalog/scan/status를 30초 간격으로 폴링하여 running=false 될 때까지 대기.
완료 후 result에서 total_scanned, new_vendors, new_vendor_codes, known_vendors, errors 추출.
텔레그램으로 결과 리포트 전송:
"🔍 월간 공급사 발견 스캔 완료
- 스캔 상품: {total_scanned}개
- 기존 공급사: {known_vendors}개
- 신규 공급사: {new_vendors}개
- 신규 코드: {new_vendor_codes 상위 10개}
- 오류: {errors 수}건"
신규 공급사가 0개면 "신규 공급사 없음" 으로 축약.
오류 발생 시 "⚠ discovery-scan 실패: {error}" 전송.
```

Schedule: `0 2 1 * *` (매월 1일 02:00)

- [ ] **Step 4: Schedule 등록 확인**

```bash
# /schedule list 로 3개 trigger 확인
```

- [ ] **Step 5: 수동 트리거 테스트**

```bash
curl -X POST http://localhost:8080/api/catalog/vendor-scan/start
# Expected: {"started": true, "state": {...}}

curl http://localhost:8080/api/catalog/scan/status
# Expected: {"running": true, "job_type": "vendor_scan", ...}
```

---

## 구현 후 확인 체크리스트

- [ ] `POST /api/catalog/vendor-scan/start` → 등록 공급사 스캔 시작
- [ ] `POST /api/catalog/discovery-scan/start` → 경량 전체 스캔 시작
- [ ] vendor-scan: oc_catalog.db upsert + products.db backfill 동작
- [ ] vendor-scan: vendors 테이블 product_count 갱신
- [ ] discovery-scan: 신규 vendor_code → vendors 테이블 status='discovered'
- [ ] 기존 scan/status API로 진행 상황 조회 가능
- [ ] 동시 실행 방지 (running=true일 때 already_running 반환)
- [ ] 7개 신규 테스트 PASS + 기존 테스트 리그레션 없음
- [ ] Schedule trigger 3개 등록 완료 (daily/weekly/monthly)
