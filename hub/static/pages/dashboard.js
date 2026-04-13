window.renderDashboard = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">📊 대시보드</h1>
    <div class="stat-grid" id="stat-grid">
      <div class="stat-card"><div class="stat-label">전체 상품</div><div class="stat-value" id="s-total">-</div></div>
      <div class="stat-card"><div class="stat-label">출고 가능</div><div class="stat-value" id="s-ship">-</div></div>
      <div class="stat-card"><div class="stat-label">가공 완료</div><div class="stat-value" id="s-proc">-</div></div>
      <div class="stat-card"><div class="stat-label">마지막 동기화</div><div class="stat-value" id="s-sync" style="font-size:16px;padding-top:8px">-</div></div>
    </div>

    <div class="card">
      <h3 style="margin-bottom:16px;font-size:16px">OC 동기화</h3>
      <p style="color:var(--muted);margin-bottom:16px;font-size:13px">
        OC 전체 상품 동기화: 기존 갱신 (가격·배송비·상태·옵션) + 신규 입고 + 미등록 공급사 발굴
      </p>
      <div style="display:flex;gap:12px;align-items:center">
        <button class="btn btn-primary" id="btn-sync">🔄 동기화 시작</button>
        <button class="btn btn-success" id="btn-export">▶ 출고 실행</button>
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
  `;

  // 마켓별 현황 카드 (stat-grid 다음)
  const marketCard = document.createElement('div');
  marketCard.className = 'card';
  marketCard.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h3 style="font-size:16px">마켓별 등록 현황</h3>
    </div>
    <div id="market-status-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px">
      <div style="color:var(--muted);font-size:13px">로딩 중...</div>
    </div>
  `;
  container.appendChild(marketCard);

  // 마켓 현황 로드
  fetch('/api/market-status').then(r => r.json()).then(data => {
    const grid = document.getElementById('market-status-grid');
    if (!grid) return;
    if (!data.length) {
      grid.innerHTML = '<div style="color:var(--muted);font-size:13px;grid-column:1/-1">등록 현황 없음 — 셀러센터 Excel import 필요</div>';
      return;
    }
    grid.innerHTML = data.map(d => `
      <div style="background:var(--surface);border-radius:8px;padding:14px">
        <div style="font-size:12px;color:var(--muted);margin-bottom:4px">${d.store_alias}</div>
        <div style="font-weight:700;font-size:22px">${(d.uploaded_count||0).toLocaleString()}</div>
        <div style="font-size:11px;color:var(--muted);margin-top:4px">
          대기 ${d.ready_count||0} · 실패 <span style="color:var(--red)">${d.failed_count||0}</span>
        </div>
        ${d.last_import_at ? `<div style="font-size:11px;color:var(--muted);margin-top:2px">${d.last_import_at.slice(0,10)}</div>` : ''}
      </div>
    `).join('');
  }).catch(() => {});

  // 통계 로드
  try {
    const r = await fetch('/api/dashboard');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const stats = await r.json();
    document.getElementById('s-total').textContent = stats.total_all.toLocaleString();
    document.getElementById('s-ship').textContent  = stats.shippable.toLocaleString();
    document.getElementById('s-proc').textContent  = stats.processed.toLocaleString();
    const sync = stats.last_sync_at ? stats.last_sync_at.slice(0, 19).replace('T', ' ') : '없음';
    document.getElementById('s-sync').textContent  = sync;
  } catch (e) {
    console.error('대시보드 로드 실패:', e);
    const grid = document.getElementById('stat-grid');
    if (grid) grid.innerHTML = '<div class="stat-card" style="color:var(--red);grid-column:1/-1">데이터 로드 실패 — 새로고침 해주세요</div>';
  }

  // 출고 실행 버튼
  document.getElementById('btn-export').addEventListener('click', () => navigate('export'));

  // OC 동기화 SSE
  const btn       = document.getElementById('btn-sync');
  const statusEl  = document.getElementById('sync-status');
  const logEl     = document.getElementById('sync-log');
  const barWrap   = document.getElementById('sync-progress-bar');
  const barFill   = document.getElementById('sync-bar-fill');

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
      let ev;
      try { ev = JSON.parse(e.data); } catch { return; }
      if (ev.type === 'ping') return;

      if (ev.type === 'progress') {
        const pct = ev.total > 0 ? Math.round((ev.current / ev.total) * 100) : 0;
        barFill.style.width = pct + '%';
        log(`[${ev.current}/${ev.total}] ${ev.message}`);
      } else if (ev.type === 'start') {
        log(`▶ ${ev.message}`);
      } else if (ev.type === 'done') {
        barFill.style.width = '100%';
        log(`✅ ${ev.message}`);
        log(`   갱신 ${(ev.updated??0).toLocaleString()} · 신규 ${(ev.inserted??0).toLocaleString()} · 비활성 ${(ev.set_inactive??0).toLocaleString()} · 미발견 ${(ev.not_found??0).toLocaleString()}`);
        if (ev.new_vendors > 0) {
          log(`   📦 미등록 공급사 ${ev.new_vendors}개 발굴 — 공급사 관리 페이지에서 확인하세요`);
        }
        if (ev.errors?.length) log(`   ⚠ 오류 ${ev.errors.length}건`);
        statusEl.textContent = '완료';
        btn.disabled = false;
        es.close();
        // 통계 카드 갱신
        fetch('/api/dashboard').then(r => r.json()).then(stats => {
          document.getElementById('s-total').textContent = stats.total_all.toLocaleString();
          document.getElementById('s-ship').textContent  = stats.shippable.toLocaleString();
          const sync = stats.last_sync_at ? stats.last_sync_at.slice(0, 19).replace('T', ' ') : '없음';
          document.getElementById('s-sync').textContent  = sync;
        }).catch(() => {});
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
