// -- 페이지 레지스트리 --
const PAGES = {
  dashboard:         window.renderDashboard,
  products:          window.renderProducts,
  vendors:           window.renderVendors,
  imageGenai:        window.renderImageGenai,
  pipeline:          window.renderPipeline,
  export:            window.renderExport,
  stores:            window.renderStores,
};

// -- 라우터 --
async function navigate(page) {
  const content = document.getElementById('content');
  content.innerHTML = '<div class="loading">로딩 중...</div>';

  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.toggle('active', el.dataset.page === page);
  });

  const render = PAGES[page];
  if (render) {
    await render(content);
  } else {
    content.innerHTML = `<div class="loading">페이지 없음: ${page}</div>`;
  }

  history.pushState({ page }, '', `#${page}`);
}

// -- 이벤트 바인딩 --
document.querySelectorAll('.nav-item').forEach(el => {
  el.addEventListener('click', e => {
    e.preventDefault();
    navigate(el.dataset.page);
  });
});

window.addEventListener('popstate', e => {
  if (e.state?.page) navigate(e.state.page);
});

// -- 글로벌 스캔 상태 폴러 --
window._scanState = null;

async function _pollScanStatus() {
  try {
    const r = await fetch('/api/catalog/scan/status');
    if (!r.ok) return;
    const s = await r.json();
    window._scanState = s;

    const badge = document.getElementById('scan-badge');
    if (badge) badge.style.display = s.running ? 'inline' : 'none';

    // vendors 페이지가 열려있으면 스캔 UI 갱신
    const hash = location.hash.slice(1);
    if (hash === 'vendors') {
      const logEl   = document.getElementById('scan-log');
      const barEl   = document.getElementById('scan-bar');
      const progEl  = document.getElementById('scan-progress');
      const progTxt = document.getElementById('scan-progress-text');
      const card    = document.getElementById('scan-card');
      const btn     = document.getElementById('btn-scan-all');

      if (logEl && (s.running || s.finished_at)) {
        if (card) card.style.display = 'block';

        // 경과 시간 표시
        let elapsed = '';
        if (s.started_at) {
          const sec = Math.floor((Date.now() - new Date(s.started_at).getTime()) / 1000);
          const m = Math.floor(sec / 60), ss = sec % 60;
          elapsed = ` (경과 ${m}분 ${ss}초)`;
        }
        if (logEl) logEl.textContent = s.message + (s.running ? elapsed : '');

        const pct = s.total > 0 ? Math.round((s.current / s.total) * 100) : 0;
        if (progEl) progEl.style.display = s.running && s.total > 0 ? 'block' : 'none';
        if (barEl)  barEl.style.width = (s.running ? pct : 100) + '%';
        if (progTxt && s.total > 0) {
          progTxt.textContent = `${s.current.toLocaleString()} / ${s.total.toLocaleString()} (${pct}%)`;
        }
        if (btn) btn.disabled = s.running;

        // 완료 직후 한 번만 공급사 목록 로드
        if (!s.running && s.result && !window._scanResultShown) {
          window._scanResultShown = true;
          const resultEl  = document.getElementById('scan-result');
          const summaryEl = document.getElementById('scan-summary');
          const listEl    = document.getElementById('scan-vendor-list');
          if (resultEl) resultEl.style.display = 'block';
          if (summaryEl) {
            const res = s.result;
            if (s.job_type === 'sync_existing') {
              summaryEl.textContent =
                `조회 ${(res.total_products||0).toLocaleString()}개 · ` +
                `갱신 ${(res.updated||0).toLocaleString()}개 · ` +
                `미발견 ${(res.not_found||0).toLocaleString()}개` +
                (res.errors?.length ? ` · 오류 ${res.errors.length}건` : '');
            } else {
              summaryEl.textContent =
                `OC 전체 ${(res.total_oc||0).toLocaleString()}개 · ` +
                `공급사 ${res.unique_vendors||0}개 · ` +
                `backfill ${(res.backfilled||0).toLocaleString()}개` +
                (res.errors?.length ? ` · 오류 ${res.errors.length}건` : '');
            }
          }
          if (listEl) {
            listEl.innerHTML = '<p style="color:var(--muted);font-size:13px">로딩 중...</p>';
            fetch('/api/catalog/vendors')
              .then(r => r.json())
              .then(vendors => window.renderCatalogVendors && renderCatalogVendors(listEl, vendors))
              .catch(() => { if (listEl) listEl.innerHTML = ''; });
          }
        }
      }
    }
  } catch (_) { /* 서버 재시작 등 무시 */ }
}

// 3초마다 폴링
setInterval(_pollScanStatus, 3000);
// 초기 1회 즉시 실행
_pollScanStatus();

// -- 초기 페이지 --
const initialPage = location.hash.slice(1) || 'dashboard';
navigate(initialPage);
