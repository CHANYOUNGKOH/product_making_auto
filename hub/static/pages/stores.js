window.renderStores = async function(container) {
  const MARKET_COLORS = {
    '고도몰':'#7ab0ff','스마트스토어':'#34d474','옥션':'#fb923c',
    '지마켓':'#facc15','11번가':'#c084fc','쿠팡':'#f87171'
  };

  async function loadAndRender() {
    let stores = [];
    try {
      const r = await fetch('/api/stores');
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      stores = await r.json();
    } catch (e) {
      document.getElementById('stores-tbody').innerHTML =
        `<tr><td colspan="6" style="color:var(--red);padding:20px">로드 실패: ${e.message}</td></tr>`;
      return;
    }

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
      return `<tr data-alias="${s.alias}" style="cursor:pointer">
        <td><span class="market-tag" style="background:${color}22;color:${color}">${s.alias}</span></td>
        <td style="color:${color}">${s.market || '-'}</td>
        <td style="color:var(--muted)">${s.group_id || '-'}</td>
        <td>${strategyLabel}</td>
        <td>${s.active ? '<span class="pill pill-done">활성</span>' : '<span class="pill pill-todo">비활성</span>'}</td>
        <td style="color:var(--muted);font-size:12px">${s.updated_at ? s.updated_at.slice(0,10) : '-'}</td>
      </tr>`;
    }).join('');

    // 카테고리 배정 패널
    let catPanel = document.getElementById('cat-assign-panel');
    if (!catPanel) {
      catPanel = document.createElement('div');
      catPanel.id = 'cat-assign-panel';
      catPanel.className = 'card';
      catPanel.style.display = 'none';
      const tableCard = tbody.closest('.card');
      if (tableCard) tableCard.after(catPanel);
    }

    tbody.querySelectorAll('tr[data-alias]').forEach(tr => {
      tr.addEventListener('click', () => {
        catPanel.style.display = 'block';
        renderCategoryAssignment(catPanel, tr.dataset.alias);
      });
    });
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
    statusEl.style.color = 'var(--muted)';

    const formData = new FormData();
    formData.append('file', file);

    try {
      const r = await fetch('/api/stores/import', { method: 'POST', body: formData });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      statusEl.textContent = `완료: ${data.imported}개 import, 전체 ${data.total_stores}개`;
      statusEl.style.color = 'var(--green)';
      await loadAndRender();
    } catch (err) {
      statusEl.textContent = `오류: ${err.message}`;
      statusEl.style.color = 'var(--red)';
    }

    e.target.value = '';
  });
};

async function renderCategoryAssignment(container, storeAlias) {
  container.innerHTML = '<div style="color:var(--muted);font-size:13px">로딩 중...</div>';

  const [catsResp, assignedResp] = await Promise.all([
    fetch('/api/register/oc-categories').then(r => r.json()),
    fetch(`/api/register/assignments/${encodeURIComponent(storeAlias)}`).then(r => r.json()),
  ]);

  const assignedKeys = new Set(assignedResp.map(a => a.oc_category_name));

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
      const checked = assignedKeys.has(cat.oc_category_name) ? 'checked' : '';
      html += `<label style="display:flex;align-items:center;gap:8px;padding:4px 8px;
                              border-radius:4px;cursor:pointer;font-size:13px">
        <input type="checkbox" class="cat-assign-cb" ${checked}
               data-name="${cat.oc_category_name}">
        <span>${cat.oc_category_name.split('>').slice(1).join('>') || cat.oc_category_name}</span>
        <span style="margin-left:auto;color:var(--muted);font-size:12px">${cat.product_count}개</span>
      </label>`;
    }
    html += `</div>`;
  }

  html += `</div></div>`;
  container.innerHTML = html;

  // 체크박스 이벤트
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
