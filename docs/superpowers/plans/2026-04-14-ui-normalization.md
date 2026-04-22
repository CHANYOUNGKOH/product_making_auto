# Hub UI 정규화 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hub UI를 실제 가공 데이터에 맞게 정규화 — 대시보드 가공 현황, 상품 가공상태 계산, 스토어+등록 파이프라인 통합, 공급사 퍼널 표시

**Architecture:** `get_dashboard_stats()`와 `get_products()`의 SQL을 CASE WHEN으로 변경하여 text_status/image_status를 실제 컬럼(ST4_마켓상품명, 누끼url, 연출url) 기반으로 계산. stores.js를 마켓별 세로 트리뷰로 리디자인하고 pipeline_register 기능을 흡수. vendors.js 버튼 축소 + 퍼널 카운트 표시.

**Tech Stack:** FastAPI, SQLite, vanilla JS, pytest

---

## 파일 구조

| 파일 | 변경 | 역할 |
|------|------|------|
| `hub/services/db_service.py` | MOD | get_dashboard_stats() 가공 현황 추가, get_products() 가공상태 계산 |
| `hub/routers/dashboard.py` | 변경 없음 | 기존 그대로 |
| `hub/static/pages/dashboard.js` | MOD | 가공 현황 5카드 + 배송비 렌더 |
| `hub/static/pages/products.js` | MOD | text_status/image_status pill 업데이트 + 배송비 컬럼 |
| `hub/static/pages/stores.js` | MOD | 마켓별 세로 트리뷰 + 등록 파이프라인 통합 |
| `hub/static/pages/vendors.js` | MOD | 버튼 축소 + 퍼널 카운트 |
| `hub/static/index.html` | MOD | pipeline-register nav 삭제 + script 태그 삭제 |
| `hub/static/app.js` | MOD | PAGES에서 pipeline-register 삭제 |
| `hub/routers/vendors.py` | MOD | GET /api/vendors 퍼널 카운트 추가 |
| `tests/hub/test_dashboard.py` | MOD | 가공 현황 카드 테스트 |

---

## Task 1: 대시보드 가공 현황 카드 + 배송비 통계

**Files:**
- Modify: `hub/services/db_service.py` (get_dashboard_stats)
- Modify: `hub/static/pages/dashboard.js`
- Modify: `tests/hub/test_dashboard.py`

- [ ] **Step 1: 테스트 수정**

`tests/hub/test_dashboard.py`에서 기존 `test_dashboard_returns_stats` 테스트를 확인하고, 새 필드를 검증하는 테스트 추가:

```python
# tests/hub/test_dashboard.py 하단에 추가
def test_dashboard_has_processing_stats(client):
    r = client.get("/api/dashboard")
    data = r.json()
    assert "text_done" in data
    assert "image_done" in data
    assert "image_partial" in data
    assert "shippable" in data
    assert "shipping_free" in data
    assert "shipping_conditional" in data
    assert "shipping_paid" in data
```

- [ ] **Step 2: get_dashboard_stats() 수정**

`hub/services/db_service.py`의 `get_dashboard_stats()` (lines 153-187)을 수정. 기존 `processed` 계산을 실제 컬럼 기반으로 변경:

```python
def get_dashboard_stats() -> dict[str, Any]:
    """대시보드 통계."""
    with _conn() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total_all,
                COUNT(CASE WHEN product_status = 'ACTIVE' THEN 1 END) as total_active,
                -- 텍스트 완료 (ST4_마켓상품명 존재)
                COUNT(CASE WHEN product_status = 'ACTIVE'
                           AND ST4_마켓상품명 IS NOT NULL AND ST4_마켓상품명 != '' THEN 1 END) as text_done,
                -- 이미지 완료 (누끼+연출 모두)
                COUNT(CASE WHEN product_status = 'ACTIVE'
                           AND 누끼url IS NOT NULL AND 누끼url != ''
                           AND 연출url IS NOT NULL AND 연출url != '' THEN 1 END) as image_done,
                -- 이미지 부분 (누끼만)
                COUNT(CASE WHEN product_status = 'ACTIVE'
                           AND 누끼url IS NOT NULL AND 누끼url != ''
                           AND (연출url IS NULL OR 연출url = '') THEN 1 END) as image_partial,
                -- 출고 가능 (ST4 + 누끼 + 가격)
                COUNT(CASE WHEN product_status = 'ACTIVE'
                           AND ST4_마켓상품명 IS NOT NULL AND ST4_마켓상품명 != ''
                           AND 누끼url IS NOT NULL AND 누끼url != ''
                           AND oc_price IS NOT NULL AND oc_price > 0 THEN 1 END) as shippable,
                -- 배송비 통계
                COUNT(CASE WHEN product_status = 'ACTIVE' AND oc_shipping_type = 'FREE' THEN 1 END) as shipping_free,
                COUNT(CASE WHEN product_status = 'ACTIVE' AND oc_shipping_type = 'FREE_ABOVE' THEN 1 END) as shipping_conditional,
                COUNT(CASE WHEN product_status = 'ACTIVE'
                           AND oc_shipping_type IS NOT NULL AND oc_shipping_type != ''
                           AND oc_shipping_type NOT IN ('FREE', 'FREE_ABOVE') THEN 1 END) as shipping_paid
            FROM products
        """)
        row = cur.fetchone()

        last_sync = con.execute("SELECT MAX(oc_synced_at) FROM products").fetchone()[0]

    return {
        "total_all": row["total_all"],
        "total_active": row["total_active"],
        "text_done": row["text_done"],
        "image_done": row["image_done"],
        "image_partial": row["image_partial"],
        "shippable": row["shippable"],
        "shipping_free": row["shipping_free"],
        "shipping_conditional": row["shipping_conditional"],
        "shipping_paid": row["shipping_paid"],
        "last_sync_at": last_sync or "",
    }
```

- [ ] **Step 3: 테스트 실행**

```bash
python -m pytest tests/hub/test_dashboard.py -v
```

- [ ] **Step 4: dashboard.js 수정**

기존 4카드 → 가공 현황 카드 + 배송비 바 + 마켓 현황으로 변경.
`hub/static/pages/dashboard.js`의 `renderDashboard` 함수 내 stat-grid HTML을 교체:

```javascript
// stat-grid를 다음으로 교체
container.innerHTML = `
  <h1 class="page-title">대시보드</h1>
  <div class="card">
    <h3 style="font-size:16px;margin-bottom:16px">가공 현황</h3>
    <div class="stat-grid">
      <div class="stat-card"><div class="stat-value" id="s-total">-</div><div class="stat-label">전체 ACTIVE</div></div>
      <div class="stat-card"><div class="stat-value" id="s-text">-</div><div class="stat-label">텍스트 완료</div></div>
      <div class="stat-card"><div class="stat-value" id="s-img">-</div><div class="stat-label">이미지 완료</div></div>
      <div class="stat-card"><div class="stat-value" id="s-img-p">-</div><div class="stat-label">누끼만</div></div>
      <div class="stat-card"><div class="stat-value" id="s-ship">-</div><div class="stat-label">출고 가능</div></div>
    </div>
  </div>
  <div class="card">
    <h3 style="font-size:16px;margin-bottom:12px">배송비 분포</h3>
    <div id="shipping-bar" style="display:flex;gap:16px;font-size:14px">로딩 중...</div>
  </div>
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h3 style="font-size:16px">마켓별 등록 현황</h3>
    </div>
    <div id="market-status-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px">
      로딩 중...
    </div>
  </div>
  <div class="card">
    <div style="display:flex;gap:12px;align-items:center">
      <button class="btn btn-primary" id="btn-sync">🔄 OC 동기화</button>
      <span id="sync-status" style="color:var(--muted);font-size:13px"></span>
    </div>
    <div id="sync-progress" style="display:none;margin-top:12px"></div>
  </div>
`;
```

그 다음 fetch 콜백에서 새 필드 매핑:

```javascript
fetch('/api/dashboard').then(r => r.json()).then(d => {
  document.getElementById('s-total').textContent = (d.total_active||0).toLocaleString();
  document.getElementById('s-text').textContent = (d.text_done||0).toLocaleString();
  document.getElementById('s-img').textContent = (d.image_done||0).toLocaleString();
  document.getElementById('s-img-p').textContent = (d.image_partial||0).toLocaleString();
  document.getElementById('s-ship').textContent = (d.shippable||0).toLocaleString();

  // 배송비 바
  const shipBar = document.getElementById('shipping-bar');
  if (shipBar) {
    shipBar.innerHTML = `
      <span>무료 <b>${(d.shipping_free||0).toLocaleString()}</b></span>
      <span>조건부 <b>${(d.shipping_conditional||0).toLocaleString()}</b></span>
      <span>유료 <b>${(d.shipping_paid||0).toLocaleString()}</b></span>
    `;
  }
}).catch(() => {});
```

마켓 현황 fetch + OC 동기화 버튼은 기존 코드 유지.

- [ ] **Step 5: 전체 테스트 실행**

```bash
python -m pytest tests/hub/ -v
```

- [ ] **Step 6: 커밋**

```bash
git add hub/services/db_service.py hub/static/pages/dashboard.js tests/hub/test_dashboard.py
git commit -m "feat: 대시보드 가공 현황 5카드 + 배송비 통계"
```

---

## Task 2: 상품 DB 가공상태 계산 + 배송비 컬럼

**Files:**
- Modify: `hub/services/db_service.py` (get_products)
- Modify: `hub/static/pages/products.js`
- Modify: `tests/hub/test_products.py`

- [ ] **Step 1: get_products() SQL 수정**

`hub/services/db_service.py`의 `get_products()` (lines 228-236) SELECT 쿼리를 수정:

```sql
SELECT 상품코드, product_names_json, 카테고리명,
       oc_price, oc_shipping_fee, oc_shipping_type,
       CASE WHEN ST4_마켓상품명 IS NOT NULL AND ST4_마켓상품명 != '' THEN 'done' ELSE 'todo' END as text_status,
       CASE WHEN 누끼url IS NOT NULL AND 누끼url != '' AND 연출url IS NOT NULL AND 연출url != '' THEN 'done'
            WHEN 누끼url IS NOT NULL AND 누끼url != '' THEN 'partial'
            ELSE 'todo' END as image_status,
       export_log, registered_stores, oc_synced_at
FROM products WHERE {where}
ORDER BY 상품코드
LIMIT ? OFFSET ?
```

items dict에 배송비 추가:

```python
items.append({
    "상품코드": row["상품코드"],
    "상품명": name,
    "카테고리명": row["카테고리명"] or "",
    "oc_price": row["oc_price"],
    "oc_shipping_fee": row["oc_shipping_fee"],
    "oc_shipping_type": row["oc_shipping_type"] or "",
    "text_status": row["text_status"],
    "image_status": row["image_status"],
    "export_log": json.loads(row["export_log"] or "[]"),
    "registered_stores": json.loads(row["registered_stores"] or "[]"),
    "oc_synced_at": row["oc_synced_at"] or "",
})
```

- [ ] **Step 2: quick_filter에 shippable 추가**

get_products()의 quick_filter 분기에 추가:

```python
elif quick_filter == "shippable":
    clauses.append("""ST4_마켓상품명 IS NOT NULL AND ST4_마켓상품명 != ''
                      AND 누끼url IS NOT NULL AND 누끼url != ''
                      AND oc_price IS NOT NULL AND oc_price > 0""")
```

- [ ] **Step 3: products.js 테이블에 배송비 컬럼 추가 + pill 업데이트**

`hub/static/pages/products.js`에서:
- thead에 `<th>배송</th>` 추가
- tbody에 배송비 셀 추가: `<td style="font-size:12px">${p.oc_shipping_type || '-'}</td>`
- image_status pill에 'partial' 상태 추가: `pill-progress` 클래스

```javascript
// image_status pill 렌더링 (기존 done/todo → done/partial/todo)
const imgPill = p.image_status === 'done' ? 'pill-done'
              : p.image_status === 'partial' ? 'pill-progress'
              : 'pill-todo';
```

- [ ] **Step 4: 테스트 실행**

```bash
python -m pytest tests/hub/ -v
```

- [ ] **Step 5: 커밋**

```bash
git add hub/services/db_service.py hub/static/pages/products.js
git commit -m "feat: 상품 가공상태 CASE WHEN 계산 + 배송비 컬럼 + shippable 필터"
```

---

## Task 3: 스토어 관리 리디자인 — 마켓별 트리뷰 + 등록 파이프라인 통합

**Files:**
- Modify: `hub/static/pages/stores.js` (전체 리디자인)
- Modify: `hub/static/index.html` (pipeline-register nav+script 삭제)
- Modify: `hub/static/app.js` (PAGES에서 pipeline-register 삭제)

- [ ] **Step 1: stores.js 전체 리디자인**

`hub/static/pages/stores.js` 전체를 교체. 마켓별 세로 컬럼 + 스토어 클릭 시 카테고리 배정 + Excel 생성 통합:

```javascript
// hub/static/pages/stores.js
window.renderStores = async function(container) {
  let stores = [];
  try {
    stores = await fetch('/api/stores').then(r => r.json());
  } catch(e) {
    container.innerHTML = '<div style="color:var(--red)">스토어 로드 실패</div>';
    return;
  }

  // 마켓별 그루핑
  const MARKET_ORDER = ['고도몰', '스마트스토어', '옥션', '지마켓', '11번가'];
  const byMarket = {};
  for (const m of MARKET_ORDER) byMarket[m] = [];
  for (const s of stores) {
    const m = s.market || '기타';
    if (!byMarket[m]) byMarket[m] = [];
    byMarket[m].push(s);
  }
  // 그룹 내 정렬
  for (const m in byMarket) byMarket[m].sort((a,b) => a.alias.localeCompare(b.alias));

  const markets = Object.entries(byMarket).filter(([,v]) => v.length > 0);

  container.innerHTML = `
    <h1 class="page-title">스토어 관리</h1>
    <div class="card">
      <div id="store-tree" style="display:grid;grid-template-columns:repeat(${markets.length},1fr);gap:12px;max-height:400px;overflow-y:auto">
        ${markets.map(([market, ss]) => `
          <div>
            <div style="font-weight:700;font-size:14px;margin-bottom:8px;padding:4px 8px;
                        background:var(--surface);border-radius:6px;text-align:center">
              ${market} <span style="color:var(--muted);font-weight:400">(${ss.length})</span>
            </div>
            ${ss.map(s => `
              <div class="store-item" data-alias="${s.alias}" data-market="${s.market}"
                   style="padding:6px 10px;border-radius:4px;cursor:pointer;font-size:13px;
                          margin-bottom:2px;display:flex;justify-content:space-between;align-items:center">
                <span>${s.alias.replace(market, '').replace(/^[-_]/, '')}</span>
                <span style="font-size:11px;color:var(--muted)">${s.group_id||''}</span>
              </div>
            `).join('')}
          </div>
        `).join('')}
      </div>
    </div>
    <div id="store-detail" class="card" style="display:none">
      <div id="store-detail-content"></div>
    </div>
  `;

  // 스토어 클릭 → 상세 패널
  let activeAlias = null;
  container.querySelectorAll('.store-item').forEach(el => {
    el.addEventListener('click', () => {
      container.querySelectorAll('.store-item').forEach(e => e.style.background = '');
      el.style.background = 'var(--accent-bg, rgba(99,102,241,0.1))';
      activeAlias = el.dataset.alias;
      const detailPanel = document.getElementById('store-detail');
      detailPanel.style.display = 'block';
      renderStoreDetail(document.getElementById('store-detail-content'), el.dataset.alias, el.dataset.market);
    });
  });
};

async function renderStoreDetail(container, storeAlias, market) {
  container.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h3 style="font-size:16px">${storeAlias}
        <span style="color:var(--muted);font-size:13px;font-weight:400"> | ${market}</span>
      </h3>
      <div style="display:flex;gap:8px">
        <select id="detail-strategy" style="background:var(--bg);color:var(--text);border:1px solid var(--border);
                border-radius:6px;padding:4px 8px;font-size:13px">
          <option value="lowest_price">최저가</option>
          <option value="normal_sale">일반판매</option>
          <option value="cpc_ad">광고</option>
        </select>
        <button class="btn btn-primary" id="btn-detail-preview" style="font-size:13px">📋 미리보기</button>
        <button class="btn btn-success" id="btn-detail-godomall" style="font-size:13px" disabled>▶ 고도몰</button>
        <button class="btn btn-success" id="btn-detail-esellers" style="font-size:13px" disabled>▶ 이셀러스</button>
      </div>
    </div>
    <div id="cat-assign-area"><div style="color:var(--muted);font-size:13px">카테고리 로딩 중...</div></div>
    <div id="detail-preview" style="display:none;margin-top:16px"></div>
    <div id="detail-download" style="margin-top:12px"></div>
  `;

  // 카테고리 배정 UI (기존 renderCategoryAssignment 로직 인라인)
  const [catsResp, assignedResp] = await Promise.all([
    fetch('/api/register/oc-categories').then(r => r.json()),
    fetch(`/api/register/assignments/${encodeURIComponent(storeAlias)}`).then(r => r.json()),
  ]);
  const assignedNames = new Set(assignedResp.map(a => a.oc_category_name));
  const tree = {};
  for (const cat of catsResp) {
    const top = (cat.oc_category_name || '').split('>')[0] || '기타';
    if (!tree[top]) tree[top] = [];
    tree[top].push(cat);
  }

  const catArea = document.getElementById('cat-assign-area');
  let catHtml = `<div style="max-height:300px;overflow-y:auto;border:1px solid var(--border);border-radius:8px;padding:12px">`;
  for (const [top, cats] of Object.entries(tree)) {
    catHtml += `<div style="margin-bottom:6px"><div style="font-weight:600;font-size:12px;color:var(--muted);margin-bottom:3px">${top}</div>`;
    for (const cat of cats) {
      const checked = assignedNames.has(cat.oc_category_name) ? 'checked' : '';
      catHtml += `<label style="display:flex;align-items:center;gap:6px;padding:3px 6px;font-size:12px;cursor:pointer">
        <input type="checkbox" class="cat-cb" ${checked} data-name="${cat.oc_category_name}">
        <span>${cat.oc_category_name.split('>').slice(1).join('>') || cat.oc_category_name}</span>
        <span style="margin-left:auto;color:var(--muted);font-size:11px">${cat.product_count}개</span>
      </label>`;
    }
    catHtml += `</div>`;
  }
  catHtml += `</div><div style="font-size:12px;color:var(--muted);margin-top:6px">배정: ${assignedNames.size}개 카테고리</div>`;
  catArea.innerHTML = catHtml;

  // 체크박스 이벤트
  catArea.querySelectorAll('.cat-cb').forEach(cb => {
    cb.addEventListener('change', async () => {
      const name = cb.dataset.name;
      if (cb.checked) {
        const r = await fetch('/api/register/assignments', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({store_alias: storeAlias, oc_category_name: name}),
        }).then(r => r.json());
        if (r.conflict_stores?.length) {
          cb.parentElement.style.background = 'rgba(250,204,21,0.1)';
          cb.parentElement.title = `⚠ 같은 마켓그룹: ${r.conflict_stores.join(', ')}`;
        }
      } else {
        await fetch(`/api/register/assignments/${encodeURIComponent(storeAlias)}?oc_category_name=${encodeURIComponent(name)}`,
          {method: 'DELETE'});
        cb.parentElement.style.background = '';
        cb.parentElement.title = '';
      }
    });
  });

  // 미리보기 + Excel 생성 (기존 pipeline_register 로직)
  function getStrategy() { return document.getElementById('detail-strategy')?.value || 'lowest_price'; }

  document.getElementById('btn-detail-preview')?.addEventListener('click', async () => {
    const data = await fetch(`/api/register/preview/${encodeURIComponent(storeAlias)}?strategy=${getStrategy()}`)
      .then(r => r.json());
    const previewEl = document.getElementById('detail-preview');
    previewEl.style.display = 'block';
    previewEl.innerHTML = `
      <div style="font-size:13px;color:var(--muted);margin-bottom:8px">총 ${data.total.toLocaleString()}개 | 카테고리 ${data.categories.length}개</div>
      <table><thead><tr><th>코드</th><th>상품명</th><th>카테고리</th><th>원가</th></tr></thead>
      <tbody>${data.items.map(p => `<tr>
        <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
        <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.상품명}</td>
        <td style="color:var(--muted);font-size:12px">${p.카테고리명}</td>
        <td>${(p.oc_price||0).toLocaleString()}원</td>
      </tr>`).join('')}</tbody></table>`;
    document.getElementById('btn-detail-godomall').disabled = data.total === 0;
    document.getElementById('btn-detail-esellers').disabled = data.total === 0;
  });

  async function runPipeline(endpoint, label) {
    const dlArea = document.getElementById('detail-download');
    dlArea.innerHTML = '<span style="color:var(--muted);font-size:13px">생성 중...</span>';
    const data = await fetch(endpoint, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({store_alias: storeAlias, strategy: getStrategy()}),
    }).then(r => r.json());
    if (data.file_url) {
      dlArea.innerHTML = `<div style="color:var(--green);font-size:13px">✅ ${label} ${(data.product_count||0).toLocaleString()}개
        <a href="${data.file_url}" class="btn btn-success" style="margin-left:8px;font-size:12px" download>⬇ 다운로드</a></div>`;
    } else {
      dlArea.innerHTML = `<span style="color:var(--red);font-size:13px">${data.error || '실패'}</span>`;
    }
  }

  document.getElementById('btn-detail-godomall')?.addEventListener('click', () => runPipeline('/api/register/run/godomall', '고도몰'));
  document.getElementById('btn-detail-esellers')?.addEventListener('click', () => runPipeline('/api/register/run/esellers', '이셀러스'));
}
```

- [ ] **Step 2: index.html에서 pipeline-register 제거**

`hub/static/index.html`에서:
- `<a class="nav-item" data-page="pipeline-register"...>` 줄 삭제
- `<script src="/pages/pipeline_register.js">` 줄 삭제

- [ ] **Step 3: app.js에서 pipeline-register 제거**

`hub/static/app.js`의 PAGES에서 `'pipeline-register': window.renderPipelineRegister,` 줄 삭제

- [ ] **Step 4: 커밋**

```bash
git add hub/static/pages/stores.js hub/static/index.html hub/static/app.js
git commit -m "feat: 스토어 관리 리디자인 — 마켓별 트리뷰 + 등록 파이프라인 통합"
```

---

## Task 4: 공급사 관리 — 버튼 축소 + 퍼널 카운트

**Files:**
- Modify: `hub/routers/vendors.py` (GET /api/vendors 응답에 가공완료 수 + 주요 카테고리 추가)
- Modify: `hub/services/db_service.py` (get_vendors 쿼리 수정)
- Modify: `hub/static/pages/vendors.js` (버튼 축소 + 테이블 퍼널)

- [ ] **Step 1: get_vendors() 쿼리에 가공완료 수 + 카테고리 추가**

`hub/services/db_service.py`의 `get_vendors()` 함수를 찾아 수정. 기존 JOIN에 ST4 가공완료 카운트와 주요 카테고리를 추가:

기존 쿼리의 SELECT에 추가:
```sql
COUNT(CASE WHEN p.ST4_마켓상품명 IS NOT NULL AND p.ST4_마켓상품명 != '' THEN 1 END) as processed_count,
(SELECT p2.카테고리명 FROM products p2
 WHERE p2.vendor_code = v.vendor_code AND p2.product_status = 'ACTIVE'
 GROUP BY p2.카테고리명 ORDER BY COUNT(*) DESC LIMIT 1) as top_category
```

- [ ] **Step 2: vendors.js 리디자인**

`hub/static/pages/vendors.js`에서 OC 버튼 4개를 2개로 축소하고 테이블을 퍼널 형식으로:

상단 버튼:
```html
<div style="display:flex;gap:10px;margin-bottom:16px">
  <button class="btn btn-primary" id="btn-add-vendor">+ 공급사 추가</button>
  <button class="btn" id="btn-sync-vendors" style="background:var(--surface);border:1px solid var(--border)">🔄 동기화</button>
</div>
```

테이블 컬럼:
```
코드 | 공급사명 | OC 상품수 → 활성화 → 가공완료 | 주요 카테고리 | 상태
```

각 셀 렌더:
```javascript
`<td style="font-size:13px">
  <span style="color:var(--muted)">${(v.product_count||0).toLocaleString()}</span>
  <span style="color:var(--muted)"> → </span>
  <span>${(v.active_count||0).toLocaleString()}</span>
  <span style="color:var(--muted)"> → </span>
  <span style="color:var(--green)">${(v.processed_count||0).toLocaleString()}</span>
</td>`
```

- [ ] **Step 3: 커밋**

```bash
git add hub/services/db_service.py hub/routers/vendors.py hub/static/pages/vendors.js
git commit -m "feat: 공급사 관리 — 버튼 축소 + 퍼널 카운트(OC→활성화→가공완료)"
```

---

## Task 5: 전체 테스트 + 마무리

- [ ] **Step 1: 전체 테스트**

```bash
python -m pytest tests/hub/ -v
```

- [ ] **Step 2: 서버 재시작 + 브라우저 확인**

```bash
# 서버 재시작
pkill -f "uvicorn hub.app"; sleep 1
python -m uvicorn hub.app:app --port 8080 &
```

확인:
- `#dashboard` → 가공 현황 5카드 + 배송비
- `#products` → 가공상태 pill (done/partial/todo) + 배송비 컬럼
- `#stores` → 마켓별 세로 트리뷰 + 클릭 시 카테고리 배정 + Excel
- `#vendors` → 버튼 2개 + 퍼널 카운트

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "chore: UI 정규화 마무리 — 전체 테스트 통과"
```
