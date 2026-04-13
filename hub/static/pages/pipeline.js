window.renderPipeline = async function(container) {
  container.innerHTML = `
    <h1 class="page-title">⚙️ 파이프라인</h1>

    <div class="card" style="opacity:0.5">
      <h3 style="margin-bottom:8px;font-size:16px">텍스트 파이프라인 (ST1 → ST2 → ST2.5 → ST3 → ST4)</h3>
      <p style="color:var(--muted);font-size:13px">ST1 정제 → ST2 분석 → ST2.5 키워드보강 → ST3 상품명생성 → ST4 품질검증</p>
      <p style="color:var(--muted);font-size:13px;margin-top:8px">2차 구현 예정</p>
    </div>

    <div class="card" style="opacity:0.5">
      <h3 style="margin-bottom:8px;font-size:16px">이미지 파이프라인 (IMG-S1 → S5)</h3>
      <p style="color:var(--muted);font-size:13px">누끼 → 라벨링 → 분석 → 연출생성 → 품질검수</p>
      <p style="color:var(--muted);font-size:13px;margin-top:8px">2차 구현 예정</p>
    </div>
  `;
};
