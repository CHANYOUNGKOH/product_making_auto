window.renderExport = async function(container) {
  let catsResp = [], storesResp = [];
  try {
    const [cr, sr] = await Promise.all([
      fetch('/api/products/categories'),
      fetch('/api/stores'),
    ]);
    if (!cr.ok || !sr.ok) throw new Error('API 오류');
    catsResp = await cr.json();
    storesResp = await sr.json();
  } catch (e) {
    container.innerHTML = '<h1 class="page-title">🛒 마켓 출고</h1><div class="loading" style="color:var(--red)">데이터 로드 실패 — 새로고침 해주세요</div>';
    return;
  }

  const MARKET_COLORS = {
    '고도몰':'#7ab0ff','스마트스토어':'#34d474','옥션':'#fb923c',
    '지마켓':'#facc15','11번가':'#c084fc','쿠팡':'#f87171'
  };

  const catCheckboxes = catsResp.map(c =>
    `<label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;cursor:pointer">
       <input type="checkbox" class="cat-cb" value="${c}"> ${c}
     </label>`
  ).join('');

  const storeCheckboxes = storesResp.map(s => {
    const mkt = Object.keys(MARKET_COLORS).find(m => s.alias.startsWith(m)) || '';
    const color = MARKET_COLORS[mkt] || '#8b8fa8';
    return `<label style="display:flex;align-items:center;gap:6px;margin-bottom:4px;cursor:pointer">
      <input type="checkbox" class="store-cb" value="${s.alias}">
      <span class="market-tag" style="background:${color}22;color:${color}">${s.alias}</span>
    </label>`;
  }).join('');

  container.innerHTML = `
    <h1 class="page-title">🛒 마켓 출고</h1>
    <div style="display:grid;grid-template-columns:260px 1fr;gap:20px">
      <div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">카테고리</h3>
          <label style="display:flex;align-items:center;gap:6px;margin-bottom:8px;font-weight:600;cursor:pointer">
            <input type="checkbox" id="cat-all" checked> 전체
          </label>
          <div id="cat-list">${catCheckboxes}</div>
        </div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">스토어</h3>
          ${storeCheckboxes || '<p style="color:var(--muted);font-size:13px">스토어 없음 — 스토어 관리에서 import 해주세요</p>'}
        </div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">전략</h3>
          ${[['lowest_price','최저가'],['normal_sale','일반판매'],['cpc_ad','광고']]
            .map(([val,label]) =>
              `<label style="display:flex;align-items:center;gap:6px;margin-bottom:6px;cursor:pointer">
                 <input type="radio" name="strategy" value="${val}" ${val==='lowest_price'?'checked':''}> ${label}
               </label>`).join('')}
        </div>
        <div style="display:flex;flex-direction:column;gap:10px">
          <button class="btn btn-primary" id="btn-dryrun">🔍 미리보기</button>
          <button class="btn btn-success" id="btn-run" disabled>▶ 출고 실행</button>
        </div>
      </div>
      <div>
        <div class="card" id="preview-card" style="display:none">
          <h3 style="margin-bottom:16px;font-size:15px">출고 미리보기</h3>
          <div id="preview-info" style="color:var(--muted);margin-bottom:12px;font-size:13px"></div>
          <table>
            <thead><tr><th>상품코드</th><th>상품명</th><th>원가</th><th>판매가</th></tr></thead>
            <tbody id="preview-tbody"></tbody>
          </table>
        </div>
        <div id="download-area" style="display:none;margin-top:16px">
          <div class="card" style="background:rgba(52,212,116,0.1);border-color:var(--green)">
            <p style="color:var(--green);margin-bottom:12px;font-weight:600">✅ 출고 파일 생성 완료</p>
            <a id="download-link" class="btn btn-success">⬇ 엑셀 다운로드</a>
          </div>
        </div>
        <div id="export-error" style="display:none;margin-top:16px">
          <div class="card" style="background:rgba(248,113,113,0.1);border-color:var(--red)">
            <p id="export-error-msg" style="color:var(--red)"></p>
          </div>
        </div>
      </div>
    </div>
  `;

  function getSelection() {
    const allCat = document.getElementById('cat-all').checked;
    const cats = allCat ? [] :
      [...document.querySelectorAll('.cat-cb:checked')].map(c => c.value);
    const stores = [...document.querySelectorAll('.store-cb:checked')].map(s => s.value);
    const strategy = document.querySelector('[name=strategy]:checked')?.value || 'lowest_price';
    return { categories: cats, stores, strategy };
  }

  document.getElementById('btn-dryrun').addEventListener('click', async () => {
    const sel = getSelection();
    document.getElementById('preview-card').style.display = 'none';
    document.getElementById('download-area').style.display = 'none';
    document.getElementById('export-error').style.display = 'none';

    try {
      const r = await fetch('/api/export/dry-run', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(sel),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();

      document.getElementById('preview-card').style.display = 'block';
      document.getElementById('preview-info').textContent =
        `총 ${data.total.toLocaleString()}개 상품 | 스토어: ${sel.stores.join(', ') || '선택 안 됨'} | 전략: ${sel.strategy}`;

      document.getElementById('preview-tbody').innerHTML = data.items.slice(0, 50).map(p =>
        `<tr>
          <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
          <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.상품명}</td>
          <td style="color:var(--muted)">${p.oc_price?.toLocaleString()}원</td>
          <td style="color:var(--yellow);font-weight:600">${p.sell_price?.toLocaleString()}원</td>
        </tr>`
      ).join('');

      document.getElementById('btn-run').disabled = data.total === 0;
    } catch (e) {
      document.getElementById('export-error').style.display = 'block';
      document.getElementById('export-error-msg').textContent = `미리보기 실패: ${e.message}`;
    }
  });

  document.getElementById('btn-run').addEventListener('click', async () => {
    const sel = getSelection();
    document.getElementById('download-area').style.display = 'none';
    document.getElementById('export-error').style.display = 'none';

    try {
      const r = await fetch('/api/export/run', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(sel),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();

      if (data.file_url) {
        document.getElementById('download-area').style.display = 'block';
        const link = document.getElementById('download-link');
        link.href = data.file_url;
        link.textContent = `⬇ 엑셀 다운로드 (${data.product_count.toLocaleString()}개)`;
      } else if (data.error) {
        throw new Error(data.error);
      }
    } catch (e) {
      document.getElementById('export-error').style.display = 'block';
      document.getElementById('export-error-msg').textContent = `출고 실패: ${e.message}`;
    }
  });
};
