const _rateColor = { GOOD: '#00c864', NORMAL: '#ffb400', BAD: '#ff4d4d' };

function renderCatalogVendors(listEl, vendors) {
  if (!vendors?.length) {
    listEl.innerHTML = '<p style="color:var(--muted);font-size:13px">카탈로그 공급사 없음</p>';
    return;
  }
  listEl.innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead>
        <tr style="border-bottom:1px solid var(--border);color:var(--muted)">
          <th style="text-align:left;padding:6px 4px">공급사코드</th>
          <th style="text-align:center;padding:6px 4px">출고율</th>
          <th style="text-align:center;padding:6px 4px">출고속도</th>
          <th style="text-align:right;padding:6px 4px">상품수</th>
          <th style="text-align:center;padding:6px 4px">상태</th>
          <th style="padding:6px 4px"></th>
        </tr>
      </thead>
      <tbody>
        ${vendors.map(v => `
          <tr style="border-bottom:1px solid var(--border)">
            <td style="padding:6px 4px;font-family:monospace;font-size:12px">${v.vendor_code}</td>
            <td style="text-align:center;padding:6px 4px;color:${_rateColor[v.release_rate]||'var(--muted)'}">
              ${v.release_rate||'-'}
            </td>
            <td style="text-align:center;padding:6px 4px;color:${_rateColor[v.average_ship]||'var(--muted)'}">
              ${v.average_ship||'-'}
            </td>
            <td style="text-align:right;padding:6px 4px">${(v.item_count||0).toLocaleString()}</td>
            <td style="text-align:center;padding:6px 4px">
              ${v.registered
                ? '<span style="color:#00c864;font-size:11px">등록됨</span>'
                : '<span style="color:var(--muted);font-size:11px">미등록</span>'}
            </td>
            <td style="padding:6px 4px;text-align:right">
              ${!v.registered ? `
                <button class="btn catalog-vendor-add" data-code="${v.vendor_code}"
                  style="font-size:11px;padding:3px 8px;background:var(--surface);border:1px solid var(--border)">
                  + OC 등록
                </button>` : ''}
            </td>
          </tr>
        `).join('')}
      </tbody>
    </table>
    <p style="color:var(--muted);font-size:11px;margin-top:8px">
      ※ 출고율 GOOD→NORMAL→BAD 순, 동순위는 상품수 많은 순
    </p>
  `;
  listEl.querySelectorAll('.catalog-vendor-add').forEach(btn => {
    btn.addEventListener('click', async () => {
      btn.disabled = true; btn.textContent = '등록 중...';
      try {
        await fetch('/api/vendors', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ vendor_code: btn.dataset.code, source: 'oc' }),
        });
        btn.textContent = '✓';
      } catch { btn.textContent = '오류'; btn.disabled = false; }
    });
  });
}

window.renderVendors = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">🏭 공급사 관리</h1>

    <div style="display:flex;gap:10px;margin-bottom:16px;align-items:center">
      <button class="btn btn-primary" id="btn-add-vendor">+ 공급사 추가</button>
      <button class="btn" id="btn-sync-vendors"
        style="background:var(--surface);border:1px solid var(--border);color:var(--text)">🔄 동기화</button>
      <div id="scan-badge" style="margin-left:auto;font-size:12px;color:var(--muted)"></div>
    </div>

    <div id="add-vendor-form" style="display:none;margin-bottom:16px">
      <div class="card" style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;padding:12px">
        <input id="v-code" type="text" placeholder="공급사코드"
          style="background:var(--surface);border:1px solid var(--border);color:var(--text);
                 padding:6px 10px;border-radius:6px;font-size:13px;width:140px">
        <input id="v-name" type="text" placeholder="공급사명"
          style="background:var(--surface);border:1px solid var(--border);color:var(--text);
                 padding:6px 10px;border-radius:6px;font-size:13px;width:180px">
        <button class="btn btn-primary" id="btn-save-vendor" style="font-size:13px">저장</button>
        <button class="btn" id="btn-cancel-vendor"
          style="font-size:13px;background:var(--surface);border:1px solid var(--border);color:var(--muted)">취소</button>
        <span id="add-vendor-status" style="font-size:12px;color:var(--muted)"></span>
      </div>
    </div>

    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <h3 style="font-size:15px">공급사 목록</h3>
        <div style="font-size:12px;color:var(--muted)" id="vendor-count"></div>
      </div>
      <div id="vendor-list">로딩 중...</div>
    </div>
  `;

  async function loadVendors() {
    const listEl = document.getElementById('vendor-list');
    const countEl = document.getElementById('vendor-count');
    try {
      const r = await fetch('/api/vendors');
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const vendors = await r.json();
      countEl.textContent = `총 ${vendors.length}개`;

      if (!vendors.length) {
        listEl.innerHTML = '<p style="color:var(--muted);font-size:13px">등록된 공급사가 없습니다.</p>';
        return;
      }

      listEl.innerHTML = `
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead>
            <tr style="border-bottom:1px solid var(--border);color:var(--muted)">
              <th style="text-align:left;padding:6px 4px">코드</th>
              <th style="text-align:left;padding:6px 4px">공급사명</th>
              <th style="text-align:left;padding:6px 4px">OC → 활성화 → 가공완료</th>
              <th style="text-align:left;padding:6px 4px">주요 카테고리</th>
              <th style="text-align:center;padding:6px 4px">상태</th>
            </tr>
          </thead>
          <tbody>
            ${vendors.map(v => `
              <tr style="border-bottom:1px solid var(--border)">
                <td style="padding:6px 4px"><code style="color:#7ab0ff;font-size:12px">${v.vendor_code}</code></td>
                <td style="padding:6px 4px">${v.vendor_name||''}</td>
                <td style="padding:6px 4px;font-size:13px">
                  <span style="color:var(--muted)">${(v.product_count||0).toLocaleString()}</span>
                  <span style="color:var(--muted)"> → </span>
                  <span>${(v.active_count||0).toLocaleString()}</span>
                  <span style="color:var(--muted)"> → </span>
                  <span style="color:var(--green)">${(v.processed_count||0).toLocaleString()}</span>
                </td>
                <td style="padding:6px 4px;font-size:12px;color:var(--muted)">${v.top_category||'-'}</td>
                <td style="text-align:center;padding:6px 4px">
                  <span class="${v.status==='active'?'pill-done':v.status==='pending'?'pill-progress':v.status==='discovered'?'pill-progress':'pill-todo'}">
                    ${{active:'활성',pending:'대기',discovered:'신규발견',inactive:'비활성'}[v.status]||v.status||'대기'}
                  </span>
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      `;
    } catch (e) {
      listEl.innerHTML = `<p style="color:var(--red)">로드 실패: ${e.message}</p>`;
    }
  }

  // 공급사 추가 폼 토글
  document.getElementById('btn-add-vendor').addEventListener('click', () => {
    const form = document.getElementById('add-vendor-form');
    form.style.display = form.style.display === 'none' ? 'block' : 'none';
  });

  document.getElementById('btn-cancel-vendor').addEventListener('click', () => {
    document.getElementById('add-vendor-form').style.display = 'none';
  });

  document.getElementById('btn-save-vendor').addEventListener('click', async () => {
    const code   = document.getElementById('v-code').value.trim();
    const name   = document.getElementById('v-name').value.trim();
    const statusEl = document.getElementById('add-vendor-status');
    if (!code) { statusEl.textContent = '공급사코드를 입력하세요.'; return; }
    statusEl.textContent = '저장 중...';
    try {
      const r = await fetch('/api/vendors', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ vendor_code: code, vendor_name: name, source: 'oc' }),
      });
      if (!r.ok) { const d = await r.json(); throw new Error(d.detail || r.status); }
      document.getElementById('v-code').value = '';
      document.getElementById('v-name').value = '';
      document.getElementById('add-vendor-form').style.display = 'none';
      statusEl.textContent = '';
      loadVendors();
    } catch (e) {
      statusEl.textContent = `오류: ${e.message}`;
    }
  });

  // 동기화 (products → vendors)
  document.getElementById('btn-sync-vendors').addEventListener('click', async () => {
    const badge = document.getElementById('scan-badge');
    const btn = document.getElementById('btn-sync-vendors');
    btn.disabled = true;
    badge.textContent = '동기화 중...';
    try {
      const r = await fetch('/api/vendors/sync-from-products', { method: 'POST' });
      const d = await r.json();
      badge.textContent = `완료 — 신규 ${d.inserted}개 추가 (기존 ${d.already}개)`;
      loadVendors();
    } catch (e) {
      badge.textContent = `오류: ${e.message}`;
    }
    btn.disabled = false;
  });

  loadVendors();
};
