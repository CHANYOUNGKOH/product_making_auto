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

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">

      <div class="card">
        <h3 style="margin-bottom:12px;font-size:15px">OC 공급사</h3>
        <p style="color:var(--muted);font-size:12px;margin-bottom:12px">
          oC API vendorName 미제공 — 코드 기준으로 관리.<br>
          이름은 엑셀 import로 보완.
        </p>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn btn-primary" id="btn-import-all">
            📥 전체 공급사 신규수집
          </button>
          <button class="btn" id="btn-sync-products"
            style="background:rgba(255,255,255,0.08);border:1px solid var(--border);color:var(--text)">
            🔄 DB 공급사 추출
          </button>
          <button class="btn btn-primary" id="btn-sync-existing">
            ⚡ OC 동기화 (기존상품)
          </button>
          <button class="btn" id="btn-scan-all"
            style="background:rgba(255,255,255,0.08);border:1px solid var(--border);color:var(--text)">
            📡 전체 OC 스캔
          </button>
        </div>
        <p style="color:var(--muted);font-size:11px;margin-top:8px">
          전체 수집: 50,000개 초과 공급사(원청추정) 자동 제외<br>
          전체 스캔: OC 전체 상품 수집 → 미등록 공급사 우선순위 (출고율+상품수 기준)
        </p>
        <div id="sync-status" style="margin-top:4px;font-size:12px;color:var(--muted)"></div>
      </div>

      <div class="card">
        <h3 style="margin-bottom:12px;font-size:15px">외부 공급사</h3>
        <p style="color:var(--muted);font-size:12px;margin-bottom:12px">
          오너클랜 외 공급처 등록.
        </p>
        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end">
          <label class="btn" style="cursor:pointer;background:var(--surface);border:1px solid var(--border)">
            📥 엑셀 import
            <input type="file" id="v-excel" accept=".xlsx,.xls" style="display:none">
          </label>
          <button class="btn" id="btn-add-manual"
            style="background:var(--surface);border:1px solid var(--border)">
            + 수동 등록
          </button>
        </div>
        <div id="excel-status" style="margin-top:8px;font-size:12px;color:var(--muted)"></div>
        <div id="manual-form" style="display:none;margin-top:12px">
          <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end">
            <input id="v-code" type="text" placeholder="공급사코드"
              style="background:var(--surface);border:1px solid var(--border);color:var(--text);
                     padding:6px 10px;border-radius:6px;font-size:13px;width:140px">
            <input id="v-name" type="text" placeholder="공급사명"
              style="background:var(--surface);border:1px solid var(--border);color:var(--text);
                     padding:6px 10px;border-radius:6px;font-size:13px;width:160px">
            <select id="v-source"
              style="background:var(--surface);border:1px solid var(--border);color:var(--text);
                     padding:6px 10px;border-radius:6px;font-size:13px">
              <option value="external">외부</option>
              <option value="oc">OC</option>
            </select>
            <button class="btn btn-primary" id="btn-save-manual" style="font-size:13px">저장</button>
          </div>
        </div>
      </div>
    </div>

    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <h3 style="font-size:15px">공급사 목록</h3>
        <div style="font-size:12px;color:var(--muted)" id="vendor-count"></div>
      </div>
      <div id="vendor-list">로딩 중...</div>
    </div>

    <div class="card" id="scan-card" style="display:none">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <h3 style="font-size:15px">전체 상품 스캔</h3>
        <button id="btn-scan-close" style="background:none;border:none;cursor:pointer;color:var(--muted);font-size:18px">×</button>
      </div>
      <div id="scan-log" style="font-size:12px;color:var(--muted);margin-bottom:12px;min-height:20px"></div>
      <div id="scan-progress" style="display:none;margin-bottom:12px">
        <div style="background:var(--border);border-radius:4px;height:6px;overflow:hidden">
          <div id="scan-bar" style="background:var(--accent);height:100%;width:0%;transition:width 0.3s"></div>
        </div>
        <div id="scan-progress-text" style="font-size:11px;color:var(--muted);margin-top:4px"></div>
      </div>
      <div id="scan-result" style="display:none">
        <p style="font-size:12px;color:var(--muted);margin-bottom:8px" id="scan-summary"></p>
        <div id="scan-vendor-list"></div>
      </div>
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
              <th style="text-align:center;padding:6px 4px">소스</th>
              <th style="text-align:center;padding:6px 4px">상태</th>
              <th style="text-align:right;padding:6px 4px">DB 상품</th>
              <th style="text-align:right;padding:6px 4px;color:#ffb400">미가공</th>
              <th style="text-align:right;padding:6px 4px">OC 상품수</th>
            </tr>
          </thead>
          <tbody>
            ${vendors.map(v => `
              <tr style="border-bottom:1px solid var(--border)">
                <td style="padding:6px 4px;font-family:monospace;font-size:12px">${v.vendor_code}</td>
                <td style="padding:6px 4px">${v.vendor_name || '<span style="color:var(--muted)">-</span>'}</td>
                <td style="text-align:center;padding:6px 4px">
                  <span style="padding:2px 6px;border-radius:10px;font-size:11px;
                    background:${v.source === 'oc' ? 'rgba(100,160,255,0.15)' : 'rgba(200,100,255,0.15)'};
                    color:${v.source === 'oc' ? '#64a0ff' : '#c864ff'}">
                    ${v.source === 'oc' ? 'OC' : '외부'}
                  </span>
                </td>
                <td style="text-align:center;padding:6px 4px">
                  <span style="padding:2px 6px;border-radius:10px;font-size:11px;
                    background:${v.status === 'processed' ? 'rgba(0,200,100,0.15)' : 'rgba(255,180,0,0.15)'};
                    color:${v.status === 'processed' ? '#00c864' : '#ffb400'}">
                    ${v.status === 'processed' ? '가공완료' : '대기'}
                  </span>
                </td>
                <td style="text-align:right;padding:6px 4px">${(v.active_count||0).toLocaleString()}</td>
                <td style="text-align:right;padding:6px 4px;color:${v.unprocessed_count>0?'#ffb400':'var(--muted)'}">${(v.unprocessed_count||0).toLocaleString()}</td>
                <td style="text-align:right;padding:6px 4px;color:var(--muted)">${v.product_count!=null?v.product_count.toLocaleString():'-'}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      `;
    } catch (e) {
      listEl.innerHTML = `<p style="color:var(--red)">로드 실패: ${e.message}</p>`;
    }
  }

  // 전체 공급사 신규수집 (1~2시간 소요 가능)
  document.getElementById('btn-import-all').addEventListener('click', async () => {
    const statusEl = document.getElementById('sync-status');
    const btn = document.getElementById('btn-import-all');
    if (!confirm('전체 공급사 신규 상품 수집을 시작합니다.\n완료까지 1~2시간이 걸릴 수 있습니다.')) return;
    btn.disabled = true;
    statusEl.textContent = '수집 중... (완료까지 시간이 걸립니다)';
    try {
      const r = await fetch('/api/vendors/import-all', { method: 'POST' });
      const d = await r.json();
      statusEl.textContent = `완료 — 신규 ${d.inserted}건 추가, 갱신 ${d.updated}건, 제외 ${d.skipped_vendors?.length||0}개 공급사`;
      if (d.errors?.length) statusEl.textContent += ` (오류 ${d.errors.length}건)`;
      loadVendors();
    } catch (e) {
      statusEl.textContent = `오류: ${e.message}`;
    }
    btn.disabled = false;
  });

  // DB 공급사 일괄 추출
  document.getElementById('btn-sync-products').addEventListener('click', async () => {
    const statusEl = document.getElementById('sync-status');
    statusEl.textContent = '추출 중...';
    try {
      const r = await fetch('/api/vendors/sync-from-products', { method: 'POST' });
      const d = await r.json();
      statusEl.textContent = `완료 — 신규 ${d.inserted}개 추가 (기존 ${d.already}개)`;
      loadVendors();
    } catch (e) {
      statusEl.textContent = `오류: ${e.message}`;
    }
  });

  // 엑셀 import (OC 공급사 목록)
  document.getElementById('v-excel').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const statusEl = document.getElementById('excel-status');
    statusEl.textContent = '파싱 중...';
    const fd = new FormData();
    fd.append('file', file);
    try {
      const r = await fetch('/api/vendors/import-excel', { method: 'POST', body: fd });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.status);
      statusEl.textContent = `완료: ${d.imported}개`;
      loadVendors();
    } catch (err) {
      statusEl.textContent = `오류: ${err.message}`;
    }
    e.target.value = '';
  });

  // 수동 등록 폼 토글
  document.getElementById('btn-add-manual').addEventListener('click', () => {
    const form = document.getElementById('manual-form');
    form.style.display = form.style.display === 'none' ? 'block' : 'none';
  });

  document.getElementById('btn-save-manual').addEventListener('click', async () => {
    const code   = document.getElementById('v-code').value.trim();
    const name   = document.getElementById('v-name').value.trim();
    const source = document.getElementById('v-source').value;
    if (!code) return;
    try {
      await fetch('/api/vendors', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ vendor_code: code, vendor_name: name, source }),
      });
      document.getElementById('v-code').value = '';
      document.getElementById('v-name').value = '';
      document.getElementById('manual-form').style.display = 'none';
      loadVendors();
    } catch (e) {
      alert('오류: ' + e.message);
    }
  });

  // OC 동기화 (기존 65k 상품코드 기준 — ~3~5분)
  document.getElementById('btn-sync-existing').addEventListener('click', async () => {
    const card  = document.getElementById('scan-card');
    const logEl = document.getElementById('scan-log');
    const btn   = document.getElementById('btn-sync-existing');

    if (window._scanState?.running) {
      card.style.display = 'block';
      card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      return;
    }

    card.style.display = 'block';
    logEl.textContent = 'OC 동기화 요청 중...';
    btn.disabled = true;
    window._scanResultShown = false;
    card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

    try {
      const r = await fetch('/api/catalog/sync-existing/start', { method: 'POST' });
      const d = await r.json();
      if (d.reason === 'already_running') {
        logEl.textContent = '이미 작업 중입니다.';
      } else {
        logEl.textContent = '총 상품코드 집계 중... (곧 진행률 표시됩니다)';
      }
    } catch (e) {
      logEl.style.color = 'var(--red)';
      logEl.textContent = `요청 실패: ${e.message}`;
      btn.disabled = false;
    }
  });

  // 전체 상품 스캔 (폴링 방식 — 페이지 이동해도 서버에서 계속 실행)
  document.getElementById('btn-scan-all').addEventListener('click', async () => {
    const card   = document.getElementById('scan-card');
    const logEl  = document.getElementById('scan-log');
    const btn    = document.getElementById('btn-scan-all');

    // 이미 실행 중이면 카드만 표시
    if (window._scanState?.running) {
      card.style.display = 'block';
      card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      return;
    }

    card.style.display = 'block';
    logEl.textContent = '스캔 요청 중...';
    btn.disabled = true;
    window._scanResultShown = false;
    card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

    try {
      const r = await fetch('/api/catalog/scan/start', { method: 'POST' });
      const d = await r.json();
      if (d.reason === 'already_running') {
        logEl.textContent = '이미 스캔 중입니다. 진행 상황은 아래 표시됩니다.';
      } else {
        logEl.textContent = 'Pass 1: OC 전체 키 수집 중... (수분 소요)';
      }
      // 이후 app.js 글로벌 폴러가 3초마다 상태 갱신
    } catch (e) {
      logEl.style.color = 'var(--red)';
      logEl.textContent = `요청 실패: ${e.message}`;
      btn.disabled = false;
    }
  });

  document.getElementById('btn-scan-close').addEventListener('click', () => {
    document.getElementById('scan-card').style.display = 'none';
  });

  loadVendors();
};
