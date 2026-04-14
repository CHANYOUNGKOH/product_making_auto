// hub/static/pages/stores.js
window.renderStores = async function(container) {
  let stores = [];
  try {
    stores = await fetch('/api/stores').then(r => r.json());
  } catch(e) {
    container.innerHTML = '<div style="color:var(--red)">스토어 로드 실패</div>';
    return;
  }

  const MARKET_ORDER = ['고도몰', '스마트스토어', '옥션', '지마켓', '11번가'];
  const byMarket = {};
  for (const m of MARKET_ORDER) byMarket[m] = [];
  for (const s of stores) {
    const m = s.market || '기타';
    if (!byMarket[m]) byMarket[m] = [];
    byMarket[m].push(s);
  }
  for (const m in byMarket) byMarket[m].sort((a,b) => a.alias.localeCompare(b.alias));
  const markets = Object.entries(byMarket).filter(([,v]) => v.length > 0);

  container.innerHTML = `
    <h1 class="page-title">스토어 관리</h1>
    <div class="card">
      <div id="store-tree" style="display:grid;grid-template-columns:repeat(${markets.length},1fr);gap:16px;max-height:420px;overflow-y:auto">
        ${markets.map(([market, ss]) => `
          <div>
            <div style="font-weight:700;font-size:14px;margin-bottom:10px;padding:6px 10px;
                        background:var(--surface);border-radius:6px;text-align:center">
              ${market} <span style="color:var(--muted);font-weight:400">(${ss.length})</span>
            </div>
            ${ss.map(s => `
              <div class="store-item" data-alias="${s.alias}" data-market="${s.market}"
                   style="padding:6px 10px;border-radius:4px;cursor:pointer;font-size:13px;
                          margin-bottom:2px;display:flex;justify-content:space-between;align-items:center;
                          transition:background 0.15s">
                <span>${s.alias}</span>
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

  container.querySelectorAll('.store-item').forEach(el => {
    el.addEventListener('click', () => {
      container.querySelectorAll('.store-item').forEach(e => e.style.background = '');
      el.style.background = 'var(--accent-bg, rgba(99,102,241,0.1))';
      document.getElementById('store-detail').style.display = 'block';
      renderStoreDetail(document.getElementById('store-detail-content'), el.dataset.alias, el.dataset.market);
    });
  });
};

async function renderStoreDetail(container, storeAlias, market) {
  container.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:8px">
      <h3 style="font-size:16px;margin:0">${storeAlias}
        <span style="color:var(--muted);font-size:13px;font-weight:400"> | ${market}</span>
      </h3>
      <div style="display:flex;gap:8px;align-items:center">
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

  // 카테고리 배정 UI
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

  // 미리보기 + Excel
  function getStrategy() { return document.getElementById('detail-strategy')?.value || 'lowest_price'; }

  document.getElementById('btn-detail-preview')?.addEventListener('click', async () => {
    const data = await fetch(`/api/register/preview/${encodeURIComponent(storeAlias)}?strategy=${getStrategy()}`).then(r => r.json());
    const el = document.getElementById('detail-preview');
    el.style.display = 'block';
    el.innerHTML = `
      <div style="font-size:13px;color:var(--muted);margin-bottom:8px">총 ${data.total.toLocaleString()}개 | 카테고리 ${data.categories.length}개</div>
      <div style="max-height:250px;overflow-y:auto">
      <table><thead><tr><th>코드</th><th>상품명</th><th>카테고리</th><th>원가</th></tr></thead>
      <tbody>${data.items.map(p => `<tr>
        <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
        <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.상품명||''}</td>
        <td style="color:var(--muted);font-size:12px">${p.카테고리명||''}</td>
        <td>${(p.oc_price||0).toLocaleString()}원</td>
      </tr>`).join('')}</tbody></table></div>`;
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
