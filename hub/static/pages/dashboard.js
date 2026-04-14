window.renderDashboard = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">대시보드</h1>
    <div class="card">
      <h3 style="font-size:16px;margin-bottom:16px">가공 현황</h3>
      <div class="stat-grid">
        <div class="stat-card"><div class="stat-value" id="s-all">-</div><div class="stat-label">전체 DB</div></div>
        <div class="stat-card"><div class="stat-value" id="s-total">-</div><div class="stat-label">판매가능</div></div>
        <div class="stat-card" style="opacity:0.6"><div class="stat-value" id="s-soldout">-</div><div class="stat-label">품절</div></div>
        <div class="stat-card" style="opacity:0.6"><div class="stat-value" id="s-inactive">-</div><div class="stat-label">판매불가</div></div>
        <div class="stat-card"><div class="stat-value" id="s-text">-</div><div class="stat-label">상품명 완료</div></div>
        <div class="stat-card"><div class="stat-value" id="s-img-both">-</div><div class="stat-label">누끼+연출</div></div>
        <div class="stat-card"><div class="stat-value" id="s-img-nk">-</div><div class="stat-label">누끼만</div></div>
        <div class="stat-card" style="opacity:0.6"><div class="stat-value" id="s-img-none">-</div><div class="stat-label">원본만</div></div>
        <div class="stat-card"><div class="stat-value" id="s-ship">-</div><div class="stat-label">출고 가능</div></div>
      </div>
    </div>
    <div class="card">
      <h3 style="font-size:16px;margin-bottom:12px">배송비 분포</h3>
      <div id="shipping-bar" style="display:flex;gap:24px;font-size:14px">로딩 중...</div>
    </div>
    <div class="card">
      <h3 style="font-size:16px;margin-bottom:16px">마켓별 등록 현황</h3>
      <div id="market-status-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px">로딩 중...</div>
    </div>
    <div class="card">
      <div style="display:flex;gap:12px;align-items:center">
        <button class="btn btn-primary" id="btn-sync">🔄 OC 동기화</button>
        <span id="sync-status" style="color:var(--muted);font-size:13px"></span>
      </div>
      <div id="sync-progress" style="display:none;margin-top:12px"></div>
    </div>
  `;

  // 가공 현황 + 배송비 통계 로드
  fetch('/api/dashboard').then(r => r.json()).then(d => {
    document.getElementById('s-all').textContent = (d.total_all||0).toLocaleString();
    document.getElementById('s-total').textContent = (d.total_active||0).toLocaleString();
    document.getElementById('s-soldout').textContent = (d.soldout||0).toLocaleString();
    document.getElementById('s-inactive').textContent = (d.inactive||0).toLocaleString();
    document.getElementById('s-text').textContent = (d.text_done||0).toLocaleString();
    document.getElementById('s-img-both').textContent = (d.image_done||0).toLocaleString();
    document.getElementById('s-img-nk').textContent = (d.image_partial||0).toLocaleString();
    document.getElementById('s-img-none').textContent = (d.image_none||0).toLocaleString();
    document.getElementById('s-ship').textContent = (d.shippable||0).toLocaleString();
    const shipBar = document.getElementById('shipping-bar');
    if (shipBar) {
      shipBar.innerHTML = `
        <span>무료 <b>${(d.shipping_free||0).toLocaleString()}</b></span>
        <span>조건부 <b>${(d.shipping_conditional||0).toLocaleString()}</b></span>
        <span>유료 <b>${(d.shipping_paid||0).toLocaleString()}</b></span>
      `;
    }
  }).catch(() => {});

  // 마켓별 등록 현황 로드
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

  // OC 동기화 SSE
  const btn      = document.getElementById('btn-sync');
  const statusEl = document.getElementById('sync-status');
  const progress = document.getElementById('sync-progress');

  btn.addEventListener('click', () => {
    btn.disabled = true;
    statusEl.textContent = '동기화 중...';
    progress.style.display = 'block';
    progress.innerHTML = '';

    const es = new EventSource('/api/pipeline/sync/stream');

    es.onmessage = (e) => {
      let ev;
      try { ev = JSON.parse(e.data); } catch { return; }
      if (ev.type === 'ping') return;

      if (ev.type === 'progress') {
        progress.innerHTML = `<div style="font-size:13px;color:var(--muted)">[${ev.current}/${ev.total}] ${ev.message}</div>`;
      } else if (ev.type === 'start') {
        progress.innerHTML = `<div style="font-size:13px;color:var(--muted)">▶ ${ev.message}</div>`;
      } else if (ev.type === 'done') {
        statusEl.textContent = '완료';
        btn.disabled = false;
        es.close();
        // 통계 카드 갱신
        fetch('/api/dashboard').then(r => r.json()).then(d => {
          document.getElementById('s-total').textContent = (d.total_active||0).toLocaleString();
          document.getElementById('s-text').textContent = (d.text_done||0).toLocaleString();
          document.getElementById('s-img').textContent = (d.image_done||0).toLocaleString();
          document.getElementById('s-img-p').textContent = (d.image_partial||0).toLocaleString();
          document.getElementById('s-ship').textContent = (d.shippable||0).toLocaleString();
          const shipBar = document.getElementById('shipping-bar');
          if (shipBar) {
            shipBar.innerHTML = `
              <span>무료 <b>${(d.shipping_free||0).toLocaleString()}</b></span>
              <span>조건부 <b>${(d.shipping_conditional||0).toLocaleString()}</b></span>
              <span>유료 <b>${(d.shipping_paid||0).toLocaleString()}</b></span>
            `;
          }
        }).catch(() => {});
      } else if (ev.type === 'error') {
        statusEl.textContent = '오류 발생';
        btn.disabled = false;
        es.close();
      }
    };

    es.onerror = () => {
      statusEl.textContent = '연결 오류';
      btn.disabled = false;
      es.close();
    };
  });
};
