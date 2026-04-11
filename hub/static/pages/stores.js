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
