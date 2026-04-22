# Hub 고도몰 엑셀 파이프라인 완성 + main 통합

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hub 스토어 관리에서 고도몰 엑셀 내보내기가 정상 동작하도록 마무리하고, feat/pricing-strategies 브랜치를 main에 머지한다.

**Architecture:** worktree(`.worktrees/feat-pricing/`)에서 수정 → 테스트 통과 → 커밋 → main에 fast-forward merge → stash pop으로 main 미커밋 변경 복원.

**Tech Stack:** Python 3, FastAPI, SQLite, openpyxl, pytest, git worktree

---

## File Map

| 파일 | 역할 | 변경 |
|------|------|------|
| `hub/services/db_service.py` | 대시보드 통계 쿼리 | 이미 수정됨 (oc_status 기반) |
| `hub/static/pages/dashboard.js` | 대시보드 프론트 | 이미 수정됨 + post-sync 이미지 카드 보완 |
| `godomall_register/pipeline_run.py` | DB→OC DataFrame 변환 | 이미 수정됨 (oc_origin 추가) |
| `OC_ES_converter/scripts/convert_godomall.py` | OC→고도몰 필드 변환 | 이미 수정됨 (convert_origin) |
| `tests/hub/conftest.py` | 테스트 seed 데이터 | oc_status 값 추가 필요 |
| `tests/hub/test_dashboard.py` | 대시보드 API 테스트 | 신규 API 키에 맞춰 assertion 변경 |
| `tests/hub/test_db_service.py` | DB 서비스 단위 테스트 | 신규 API 키에 맞춰 assertion 변경 |

---

### Task 1: 자격증명 revert + post-sync 이미지 카드 보완

**Files:**
- Revert: `godomall_register/ownerclan_config.json`
- Modify: `hub/static/pages/dashboard.js:108-124`

- [ ] **Step 1: ownerclan_config.json revert**

```bash
cd .worktrees/feat-pricing
git checkout -- godomall_register/ownerclan_config.json
```

- [ ] **Step 2: dashboard.js post-sync 이미지 카드 추가**

`hub/static/pages/dashboard.js`의 post-sync 갱신 블록(line ~114)에서 `s-text` 다음, `s-ship` 앞에 3줄 추가:

```javascript
          document.getElementById('s-text').textContent = (d.text_done||0).toLocaleString();
          document.getElementById('s-img-both').textContent = (d.image_done||0).toLocaleString();
          document.getElementById('s-img-nk').textContent = (d.image_partial||0).toLocaleString();
          document.getElementById('s-img-none').textContent = (d.image_none||0).toLocaleString();
          document.getElementById('s-ship').textContent = (d.shippable||0).toLocaleString();
```

- [ ] **Step 3: 브라우저에서 확인**

http://localhost:8000/#dashboard 열어서 5개 통계 카드 + 가공 상세 카드가 정상 표시되는지 확인.

---

### Task 2: 테스트 fixture에 oc_status 추가

**Files:**
- Modify: `tests/hub/conftest.py:100-122`

- [ ] **Step 1: seed 데이터에 oc_status 컬럼 추가**

`_seed_products()` INSERT문에 `oc_status` 컬럼을 추가한다. 기존 `oc_origin`은 이미 스키마에 있으나 seed에 없으므로 함께 추가.

```python
def _seed_products(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """INSERT INTO products
           (상품코드, product_names_json, 카테고리명, product_status, oc_price,
            text_status, image_status, oc_shipping_type, oc_status,
            ST4_마켓상품명, 누끼url, 연출url)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            # W001: available + 가격있음 + 텍스트완료 + 누끼+연출 → text_done, image_done, shippable
            ("W001", '{"name":"테스트상품A"}', "가전/디지털>TV", "ACTIVE", 50000, "done", "done",
             "free", "available", "마켓상품명A", "https://cdn/W001_누끼.jpg", "https://cdn/W001_연출.jpg"),
            # W002: available + 가격있음 + 텍스트완료 + 누끼만 → text_done, image_partial
            ("W002", '{"name":"테스트상품B"}', "가전/디지털>냉장고", "ACTIVE", 30000, "done", None,
             "freeAbove", "available", "마켓상품명B", "https://cdn/W002_누끼.jpg", None),
            # W003: available + 가격없음 → oc_status=available이지만 oc_price NULL이므로 total_synced 미포함
            ("W003", '{"name":"테스트상품C"}', "생활/주방>청소", "ACTIVE", None, None, None,
             "inAdvance", "available", None, None, None),
            # W004: unavailable → 판매불가
            ("W004", '{"name":"비활성상품"}', "가전/디지털>TV", "INACTIVE", 10000, None, None,
             "free", "unavailable", None, None, None),
        ],
    )
    conn.commit()
```

주요 변경:
- INSERT 컬럼에 `oc_status` 추가
- W001: `oc_status='available'`, `oc_shipping_type='free'` (소문자, 실제 OC API 값)
- W002: `oc_status='available'`, `oc_shipping_type='freeAbove'`
- W003: `oc_status='available'`, `oc_price=None` (synced 미포함)
- W004: `oc_status='unavailable'`, `oc_price=10000` (unavailable이므로 oc_available 미포함)

- [ ] **Step 2: 기대값 정리**

신규 API 기준 기대값:
- `total_synced` = 3 (W001+W002+W004, oc_price IS NOT NULL)
- `oc_available` = 2 (W001+W002, oc_price IS NOT NULL AND oc_status='available')
- `oc_soldout` = 0
- `oc_discontinued` = 0
- `oc_unavailable` = 1 (W004)
- `text_done` = 2 (W001+W002, oc_price+oc_status='available'+ST4)
- `image_done` = 1 (W001)
- `image_partial` = 1 (W002)
- `image_none` = 0 (W003는 oc_price NULL이라 제외, W004는 unavailable)
- `shippable` = 2 (W001+W002)
- `shipping_free` = 2 (W001 free + W004 free, oc_price IS NOT NULL 기준)
- `shipping_conditional` = 1 (W002 freeAbove → LOWER IN 조건 매칭)
- `shipping_paid` = 0

---

### Task 3: test_dashboard.py 수정

**Files:**
- Modify: `tests/hub/test_dashboard.py`

- [ ] **Step 1: test_dashboard_returns_stats 수정**

```python
def test_dashboard_returns_stats(client):
    response = client.get("/api/dashboard")
    assert response.status_code == 200
    data = response.json()
    assert data["total_synced"] == 3     # W001, W002, W004 (oc_price IS NOT NULL)
    assert data["oc_available"] == 2     # W001, W002
    assert data["oc_unavailable"] == 1   # W004
    assert data["text_done"] == 2        # W001, W002 (available + ST4_마켓상품명)
    assert data["image_done"] == 1       # W001 (누끼+연출 모두)
    assert data["image_partial"] == 1    # W002 (누끼만)
    assert data["shippable"] == 2        # W001, W002 (available + 텍스트 + 가격)
    assert "last_sync_at" in data
```

- [ ] **Step 2: test_dashboard_stat_values_are_ints 수정**

```python
def test_dashboard_stat_values_are_ints(client):
    data = client.get("/api/dashboard").json()
    for key in ("total_synced", "oc_available", "text_done", "image_done", "image_partial", "shippable"):
        assert isinstance(data[key], int), f"{key} should be int"
```

- [ ] **Step 3: test_dashboard_has_processing_stats 수정**

이 테스트는 이미 `shipping_free` 등 키를 체크하므로 `total_synced`, `oc_available` 등 신규 키도 추가:

```python
def test_dashboard_has_processing_stats(client):
    r = client.get("/api/dashboard")
    data = r.json()
    for key in ["total_synced", "oc_available", "oc_soldout", "oc_discontinued", "oc_unavailable",
                "text_done", "image_done", "image_partial", "shippable",
                "shipping_free", "shipping_conditional", "shipping_paid"]:
        assert key in data, f"Missing key: {key}"
        assert isinstance(data[key], int), f"{key} should be int"
```

---

### Task 4: test_db_service.py 수정

**Files:**
- Modify: `tests/hub/test_db_service.py:12-38`

- [ ] **Step 1: test_get_dashboard_stats_returns_required_keys 수정**

```python
def test_get_dashboard_stats_returns_required_keys():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    for key in ("total_synced", "oc_available", "oc_soldout", "oc_discontinued",
                "oc_unavailable", "shippable", "text_done", "image_done",
                "image_partial", "shipping_free", "shipping_conditional",
                "shipping_paid", "last_sync_at"):
        assert key in stats, f"Missing key: {key}"
```

- [ ] **Step 2: test_total_active_counts_only_active → 이름+로직 변경**

```python
def test_total_synced_counts_oc_price_not_null():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["total_synced"] == 3  # W001, W002, W004 (oc_price IS NOT NULL)
    assert stats["oc_available"] == 2  # W001, W002 (available + oc_price)
    assert stats["oc_unavailable"] == 1  # W004
```

- [ ] **Step 3: test_shippable/text_done 기대값 확인**

`test_shippable_requires_oc_price`와 `test_text_done_counts_st4_marketname`은 기대값이 동일(shippable=2, text_done=2, image_done=1, image_partial=1)하므로 assertion 값은 유지. 다만 image_none 검증 추가:

```python
def test_text_done_counts_st4_marketname():
    from hub.services.db_service import get_dashboard_stats
    stats = get_dashboard_stats()
    assert stats["text_done"] == 2    # W001, W002
    assert stats["image_done"] == 1   # W001 (누끼+연출)
    assert stats["image_partial"] == 1  # W002 (누끼만)
    assert stats["image_none"] == 0   # available 중 이미지 없는 건 없음
```

---

### Task 5: pytest 실행 + 수정

**Files:** (없음, 검증만)

- [ ] **Step 1: 전체 테스트 실행**

```bash
cd .worktrees/feat-pricing
python -m pytest tests/hub/ -v
```

Expected: ALL PASS

- [ ] **Step 2: 실패 시 수정**

shipping 관련 기대값이 틀릴 수 있음. `LOWER(oc_shipping_type) IN ('freeabove', 'free_above', 'uponarrival')` 조건에서 seed의 `freeAbove`가 매칭되는지 확인. `LOWER('freeAbove') = 'freeabove'` → 매칭 OK.

---

### Task 6: 커밋

- [ ] **Step 1: 변경 파일 스테이징**

```bash
cd .worktrees/feat-pricing
git add OC_ES_converter/scripts/convert_godomall.py \
        godomall_register/pipeline_run.py \
        hub/services/db_service.py \
        hub/static/pages/dashboard.js \
        tests/hub/conftest.py \
        tests/hub/test_dashboard.py \
        tests/hub/test_db_service.py
```

제외: `godomall_register/ownerclan_config.json` (자격증명)

- [ ] **Step 2: 커밋**

```bash
git commit -m "fix: 대시보드 oc_status 기반 리팩터 + 원산지 파이프라인 연결

- db_service: dashboard stats를 oc_status 기준으로 변경 (판매가능/품절/단종/판매불가)
- db_service: shipping_type LOWER() 정규화
- pipeline_run: oc_origin fetch + build_oc_dataframe 원산지 매핑
- convert_godomall: convert_origin() import 및 적용
- dashboard.js: 신규 API 키 대응 + post-sync 이미지 카드 갱신 보완
- tests: conftest seed에 oc_status 추가, 테스트 assertion 신규 키 대응"
```

---

### Task 7: E2E 파이프라인 검증

**Files:** (없음, 검증만)

- [ ] **Step 1: CLI dry-run**

```bash
cd .worktrees/feat-pricing
PYTHONUTF8=1 python godomall_register/pipeline_run.py --skip-import --dry-run --strategy lowest_price
```

확인사항:
- 5행 미리보기 출력됨
- `goods_price` ≠ `cost_price` (전략 적용됨)
- `goods_name` 비어있지 않음

- [ ] **Step 2: 실제 엑셀 생성**

```bash
PYTHONUTF8=1 python godomall_register/pipeline_run.py --skip-import --strategy lowest_price --output DB_save/test_godomall_output.xlsx
```

확인사항:
- 파일 생성됨
- origin_name 컬럼에 값 존재 (국산, 수입/아시아/중국 등)
- option 블록 정상 (옵션 있는 상품)
- image_name에 magnify/detail/list/main 패턴

- [ ] **Step 3: Hub UI 테스트**

```bash
cd .worktrees/feat-pricing
PYTHONUTF8=1 python -m uvicorn hub.app:app --port 8000
```

1. http://localhost:8000/#stores 접속
2. 고도몰 스토어 클릭
3. 카테고리 배정
4. 전략 선택 → 미리보기 클릭
5. ▶ 고도몰 버튼 → 다운로드
6. 엑셀 열어서 검수

---

### Task 8: main 머지

- [ ] **Step 1: main worktree에서 stash**

```bash
cd /c/Users/kohaz/Desktop/Python/파이썬자동화파일/상품가공프로그램
git stash
```

- [ ] **Step 2: fast-forward merge**

```bash
git merge feat/pricing-strategies
```

- [ ] **Step 3: stash pop + conflict 해소**

```bash
git stash pop
```

예상 충돌: `OC_ES_converter/scripts/convert_godomall.py` — main의 shipping_strategy 추가 + feat의 convert_origin import 둘 다 유지하여 해소.

- [ ] **Step 4: post-merge 검증**

```bash
python -m pytest tests/hub/ -v
```

- [ ] **Step 5: worktree 정리**

```bash
git worktree remove .worktrees/feat-pricing
git branch -d feat/pricing-strategies
```
