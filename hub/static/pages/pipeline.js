window.renderPipeline = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">⚙️ 파이프라인</h1>

    <div class="card">
      <h3 style="margin-bottom:16px;font-size:16px">OC 동기화</h3>
      <p style="color:var(--muted);margin-bottom:16px;font-size:13px">
        products.db의 oc_price IS NOT NULL 상품을 Ownerclan API로 전체 갱신합니다.
      </p>
      <div style="display:flex;gap:12px;align-items:center">
        <button class="btn btn-primary" id="btn-sync">🔄 동기화 시작</button>
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

    <div class="card" style="opacity:0.5">
      <h3 style="margin-bottom:8px;font-size:16px">텍스트 파이프라인 (S1~S4)</h3>
      <p style="color:var(--muted);font-size:13px">2차 구현 예정</p>
    </div>

    <div class="card" style="opacity:0.5">
      <h3 style="margin-bottom:8px;font-size:16px">이미지 파이프라인 (S1~S5)</h3>
      <p style="color:var(--muted);font-size:13px">2차 구현 예정</p>
    </div>
  `;

  const btn = document.getElementById('btn-sync');
  const statusEl = document.getElementById('sync-status');
  const logEl = document.getElementById('sync-log');
  const barWrap = document.getElementById('sync-progress-bar');
  const barFill = document.getElementById('sync-bar-fill');

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
        log(`✅ ${ev.message} (갱신: ${ev.updated ?? 0}건, 미발견: ${ev.not_found ?? 0}건)`);
        statusEl.textContent = '완료';
        btn.disabled = false;
        es.close();
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
