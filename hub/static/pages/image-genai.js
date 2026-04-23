const IMAGE_GENAI_STATE = {
  status: 'all',
};

function imageGenaiPill(status) {
  const map = {
    done: 'pill-done',
    running: 'pill-progress',
    failed: 'pill-danger',
    pending: 'pill-todo',
    skipped: 'pill-todo',
  };
  return `<span class="pill ${map[status] || 'pill-todo'}">${status}</span>`;
}

async function loadImageGenaiPageData() {
  const statusParam = IMAGE_GENAI_STATE.status === 'all' ? '' : `?status=${encodeURIComponent(IMAGE_GENAI_STATE.status)}`;
  const [summaryRes, targetsRes] = await Promise.all([
    fetch('/api/image-genai/summary'),
    fetch(`/api/image-genai/targets${statusParam}`),
  ]);
  if (!summaryRes.ok || !targetsRes.ok) {
    throw new Error(`HTTP ${summaryRes.status}/${targetsRes.status}`);
  }
  return {
    summary: await summaryRes.json(),
    targets: await targetsRes.json(),
  };
}

function renderImageGenaiCards(summary) {
  const status = summary.status_counts || {};
  return `
    <div class="stat-grid">
      <div class="stat-card"><div class="stat-label">대상 상품</div><div class="stat-value">${(summary.total_targets || 0).toLocaleString()}</div></div>
      <div class="stat-card"><div class="stat-label">완료</div><div class="stat-value" style="color:var(--green)">${(status.done || 0).toLocaleString()}</div></div>
      <div class="stat-card"><div class="stat-label">실패</div><div class="stat-value" style="color:var(--red)">${(status.failed || 0).toLocaleString()}</div></div>
      <div class="stat-card"><div class="stat-label">실행중</div><div class="stat-value" style="color:var(--yellow)">${(status.running || 0).toLocaleString()}</div></div>
    </div>
  `;
}

function renderImageGenaiTable(items) {
  if (!items.length) {
    return '<div class="card"><div class="loading">조건에 맞는 항목이 없습니다.</div></div>';
  }
  const rows = items.map(item => `
    <tr>
      <td><code style="color:#7ab0ff">${item.code}</code></td>
      <td>${item.name || '-'}</td>
      <td style="color:var(--muted)">${item.category || '-'}</td>
      <td>${imageGenaiPill(item.genai_status)}</td>
      <td style="color:var(--muted)">${(item.lane_keys || []).join(', ') || '-'}</td>
      <td style="max-width:240px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:${item.genai_error ? 'var(--red)' : 'var(--muted)'}">${item.genai_error || '-'}</td>
      <td style="max-width:220px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--muted)">${item.genai_issue_memo || '-'}</td>
      <td style="display:flex;gap:8px">
        <button class="btn btn-primary" data-retry="${item.code}" style="padding:6px 10px">Retry</button>
        <button class="btn" data-issue="${item.code}" style="padding:6px 10px;background:var(--card);border:1px solid var(--border);color:var(--text)">Memo</button>
      </td>
    </tr>
  `).join('');
  return `
    <div class="card" style="padding:0;overflow:hidden">
      <table>
        <thead>
          <tr>
            <th>코드</th>
            <th>상품명</th>
            <th>카테고리</th>
            <th>상태</th>
            <th>Lane</th>
            <th>마지막 오류</th>
            <th>Issue Memo</th>
            <th>제어</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

async function refreshImageGenai(container) {
  container.querySelector('#image-genai-body').innerHTML = '<div class="loading">로딩 중...</div>';
  try {
    const { summary, targets } = await loadImageGenaiPageData();
    container.querySelector('#image-genai-cards').innerHTML = renderImageGenaiCards(summary);
    container.querySelector('#image-genai-body').innerHTML = renderImageGenaiTable(targets.items || []);

    container.querySelectorAll('[data-retry]').forEach(btn => {
      btn.addEventListener('click', async () => {
        await fetch('/api/image-genai/retry', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code: btn.dataset.retry }),
        });
        await refreshImageGenai(container);
      });
    });

    container.querySelectorAll('[data-issue]').forEach(btn => {
      btn.addEventListener('click', async () => {
        const current = (targets.items || []).find(item => item.code === btn.dataset.issue)?.genai_issue_memo || '';
        const memo = window.prompt(`Issue memo for ${btn.dataset.issue}`, current);
        if (memo == null) return;
        await fetch('/api/image-genai/issue', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code: btn.dataset.issue, memo }),
        });
        await refreshImageGenai(container);
      });
    });
  } catch (error) {
    container.querySelector('#image-genai-body').innerHTML =
      '<div class="card"><div class="loading" style="color:var(--red)">image-genai 상태를 불러오지 못했습니다.</div></div>';
  }
}

window.renderImageGenai = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">Image GenAI</h1>
    <div class="card" style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
      <select id="image-genai-status"
              style="background:#0d0f1a;border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:6px;font-size:14px">
        <option value="all">전체 상태</option>
        <option value="pending">pending</option>
        <option value="running">running</option>
        <option value="done">done</option>
        <option value="failed">failed</option>
        <option value="skipped">skipped</option>
      </select>
      <button id="image-genai-refresh" class="btn btn-primary">새로고침</button>
      <button id="image-genai-reset" class="btn" style="background:var(--card);border:1px solid var(--border);color:var(--text)">Stale Reset</button>
      <span style="color:var(--muted);font-size:13px">운영은 Hub에서 보고, 실제 실행 엔진은 기존 IMG_pipeline을 그대로 사용합니다.</span>
    </div>
    <div id="image-genai-cards"></div>
    <div id="image-genai-body"></div>
  `;

  const statusEl = container.querySelector('#image-genai-status');
  statusEl.value = IMAGE_GENAI_STATE.status;
  statusEl.addEventListener('change', async e => {
    IMAGE_GENAI_STATE.status = e.target.value;
    await refreshImageGenai(container);
  });

  container.querySelector('#image-genai-refresh').addEventListener('click', async () => {
    await refreshImageGenai(container);
  });

  container.querySelector('#image-genai-reset').addEventListener('click', async () => {
    await fetch('/api/image-genai/reset-stale', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stale_minutes: 30 }),
    });
    await refreshImageGenai(container);
  });

  await refreshImageGenai(container);
};
