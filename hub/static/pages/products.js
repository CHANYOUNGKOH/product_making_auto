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
  const r = await fetch(`/api/products?${params}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

function renderTable(data) {
  const tbody = document.getElementById('products-tbody');
  if (!tbody) return;
  if (data.items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--muted);padding:40px">상품 없음</td></tr>';
    return;
  }
  tbody.innerHTML = data.items.map(p => `
    <tr>
      <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
      <td style="max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
          title="${p.상품명}">${p.상품명}</td>
      <td style="color:var(--muted);font-size:13px">${p.카테고리명}</td>
      <td style="color:var(--yellow)">${p.oc_price != null ? p.oc_price.toLocaleString() + '원' : '-'}</td>
      <td>${statusPill(p.text_status)}</td>
      <td>${statusPill(p.image_status)}</td>
      <td>${storeTagsHtml(p.export_log)}</td>
      <td style="color:var(--muted);font-size:12px">${p.oc_synced_at ? p.oc_synced_at.slice(0,10) : '-'}</td>
    </tr>
  `).join('');

  const pag = document.getElementById('pagination');
  if (pag) {
    const totalPages = Math.ceil(data.total / _state.per_page);
    pag.innerHTML = `
      <span style="color:var(--muted)">총 ${data.total.toLocaleString()}개 | 페이지 ${_state.page}/${totalPages}</span>
      ${_state.page > 1 ? '<button class="btn btn-primary" style="padding:4px 12px" id="prev-page">이전</button>' : ''}
      ${_state.page < totalPages ? '<button class="btn btn-primary" style="padding:4px 12px" id="next-page">다음</button>' : ''}
    `;
    document.getElementById('prev-page')?.addEventListener('click', () => { _state.page--; refresh(); });
    document.getElementById('next-page')?.addEventListener('click', () => { _state.page++; refresh(); });
  }
}

async function refresh() {
  try {
    const data = await loadProducts();
    renderTable(data);
  } catch (e) {
    const tbody = document.getElementById('products-tbody');
    if (tbody) tbody.innerHTML = '<tr><td colspan="8" style="color:var(--red);padding:20px">로드 실패 — 새로고침 해주세요</td></tr>';
  }
}

window.renderProducts = async function(container) {
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

  let searchTimer;
  document.getElementById('search-q').addEventListener('input', e => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => { _state.q = e.target.value; _state.page = 1; refresh(); }, 400);
  });

  document.getElementById('cat-filter').value = _state.category;
  document.getElementById('cat-filter').addEventListener('change', e => {
    _state.category = e.target.value; _state.page = 1; refresh();
  });

  container.querySelectorAll('[data-qf]').forEach(btn => {
    btn.addEventListener('click', () => {
      _state.quick_filter = btn.dataset.qf; _state.page = 1;
      window.renderProducts(container);
    });
  });

  await refresh();
};
