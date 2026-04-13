// hub/static/pages/pipeline_register.js
window.renderPipelineRegister = async function(container) {
  let stores = [];
  try {
    stores = await fetch('/api/stores').then(r => r.json());
  } catch(e) {
    container.innerHTML = '<div class="loading" style="color:var(--red)">스토어 로드 실패</div>';
    return;
  }

  const storeOpts = stores.filter(s => s.active).map(s =>
    `<option value="${s.alias}">${s.alias} (${s.market})</option>`
  ).join('');

  container.innerHTML = `
    <h1 class="page-title">🚀 등록 파이프라인</h1>
    <div style="display:grid;grid-template-columns:280px 1fr;gap:20px">
      <div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">스토어</h3>
          <select id="reg-store" style="width:100%;background:var(--bg);color:var(--text);
                  border:1px solid var(--border);border-radius:6px;padding:8px;font-size:14px">
            <option value="">선택...</option>${storeOpts}
          </select>
        </div>
        <div class="card">
          <h3 style="margin-bottom:12px;font-size:15px">가격 전략</h3>
          ${[['lowest_price','최저가'],['normal_sale','일반판매'],['cpc_ad','광고']]
            .map(([val,label],i) =>
              `<label style="display:flex;align-items:center;gap:6px;margin-bottom:6px;cursor:pointer">
                 <input type="radio" name="reg-strategy" value="${val}" ${i===0?'checked':''}> ${label}
               </label>`).join('')}
        </div>
        <div style="display:flex;flex-direction:column;gap:10px">
          <button class="btn btn-primary" id="btn-preview">📋 미리보기</button>
          <button class="btn btn-success" id="btn-godomall" disabled>▶ 고도몰 Excel</button>
          <button class="btn btn-success" id="btn-esellers" disabled>▶ 이셀러스 Excel</button>
        </div>
      </div>
      <div>
        <div class="card" id="preview-card" style="display:none">
          <h3 style="margin-bottom:12px;font-size:15px">출고 미리보기</h3>
          <div id="preview-info" style="color:var(--muted);font-size:13px;margin-bottom:12px"></div>
          <table>
            <thead><tr><th>상품코드</th><th>상품명</th><th>카테고리</th><th>원가</th></tr></thead>
            <tbody id="preview-tbody"></tbody>
          </table>
        </div>
        <div id="download-area" style="margin-top:16px"></div>
      </div>
    </div>
  `;

  const storeEl     = document.getElementById('reg-store');
  const previewCard = document.getElementById('preview-card');
  const previewInfo = document.getElementById('preview-info');
  const previewBody = document.getElementById('preview-tbody');
  const btnGodomall = document.getElementById('btn-godomall');
  const btnEsellers = document.getElementById('btn-esellers');
  const dlArea      = document.getElementById('download-area');

  function getStrategy() {
    return document.querySelector('[name=reg-strategy]:checked')?.value || 'lowest_price';
  }

  document.getElementById('btn-preview').addEventListener('click', async () => {
    const store = storeEl.value;
    if (!store) { alert('스토어를 선택하세요'); return; }
    previewCard.style.display = 'none';

    const data = await fetch(`/api/register/preview/${encodeURIComponent(store)}?strategy=${getStrategy()}`)
      .then(r => r.json());

    previewCard.style.display = 'block';
    previewInfo.textContent = `총 ${data.total.toLocaleString()}개 | 카테고리 ${data.categories.length}개`;
    previewBody.innerHTML = data.items.map(p =>
      `<tr>
        <td><code style="color:#7ab0ff">${p.상품코드}</code></td>
        <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${p.상품명}</td>
        <td style="color:var(--muted);font-size:12px">${p.카테고리명}</td>
        <td style="color:var(--muted)">${(p.oc_price||0).toLocaleString()}원</td>
      </tr>`
    ).join('');

    btnGodomall.disabled = data.total === 0;
    btnEsellers.disabled = data.total === 0;
  });

  async function runPipeline(endpoint, label) {
    const store = storeEl.value;
    if (!store) { alert('스토어를 선택하세요'); return; }
    btnGodomall.disabled = true;
    btnEsellers.disabled = true;
    dlArea.innerHTML = '<div style="color:var(--muted);font-size:13px">생성 중...</div>';

    const data = await fetch(endpoint, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({store_alias: store, strategy: getStrategy()}),
    }).then(r => r.json());

    if (data.file_url) {
      dlArea.innerHTML = `
        <div class="card" style="background:rgba(52,212,116,0.1);border-color:var(--green)">
          <p style="color:var(--green);margin-bottom:12px;font-weight:600">✅ ${label} 생성 완료 (${(data.product_count||0).toLocaleString()}개)</p>
          <a href="${data.file_url}" class="btn btn-success" download>⬇ 다운로드</a>
          ${data.error_count ? `<span style="margin-left:12px;color:var(--yellow);font-size:13px">⚠ 변환 오류 ${data.error_count}건</span>` : ''}
        </div>`;
    } else {
      dlArea.innerHTML = `<div class="card" style="background:rgba(248,113,113,0.1);border-color:var(--red)">
        <p style="color:var(--red)">${data.error || '생성 실패'}</p>
      </div>`;
    }
    btnGodomall.disabled = false;
    btnEsellers.disabled = false;
  }

  btnGodomall.addEventListener('click', () => runPipeline('/api/register/run/godomall', '고도몰 Excel'));
  btnEsellers.addEventListener('click', () => runPipeline('/api/register/run/esellers', '이셀러스 Excel'));
};
