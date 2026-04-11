window.renderDashboard = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">📊 대시보드</h1>
    <div class="stat-grid" id="stat-grid">
      <div class="stat-card"><div class="stat-label">전체 상품</div><div class="stat-value" id="s-total">-</div></div>
      <div class="stat-card"><div class="stat-label">출고 가능</div><div class="stat-value" id="s-ship">-</div></div>
      <div class="stat-card"><div class="stat-label">가공 완료</div><div class="stat-value" id="s-proc">-</div></div>
      <div class="stat-card"><div class="stat-label">마지막 동기화</div><div class="stat-value" id="s-sync" style="font-size:16px;padding-top:8px">-</div></div>
    </div>
    <div class="card" style="display:flex;gap:12px;flex-wrap:wrap">
      <button class="btn btn-primary" id="btn-sync">🔄 OC 동기화</button>
      <button class="btn btn-success" id="btn-export">▶ 출고 실행</button>
    </div>
  `;

  try {
    const r = await fetch('/api/dashboard');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const stats = await r.json();
    document.getElementById('s-total').textContent = stats.total_active.toLocaleString();
    document.getElementById('s-ship').textContent  = stats.shippable.toLocaleString();
    document.getElementById('s-proc').textContent  = stats.processed.toLocaleString();
    const sync = stats.last_sync_at ? stats.last_sync_at.slice(0, 19).replace('T', ' ') : '없음';
    document.getElementById('s-sync').textContent  = sync;
  } catch (e) {
    console.error('대시보드 로드 실패:', e);
    const grid = document.getElementById('stat-grid');
    if (grid) grid.innerHTML = '<div class="stat-card" style="color:var(--red);grid-column:1/-1">데이터 로드 실패 — 새로고침 해주세요</div>';
  }

  document.getElementById('btn-sync').addEventListener('click', () => navigate('pipeline'));
  document.getElementById('btn-export').addEventListener('click', () => navigate('export'));
};
