// -- 페이지 레지스트리 --
const PAGES = {
  dashboard: window.renderDashboard,
  products:  window.renderProducts,
  pipeline:  window.renderPipeline,
  export:    window.renderExport,
  stores:    window.renderStores,
};

// -- 라우터 --
async function navigate(page) {
  const content = document.getElementById('content');
  content.innerHTML = '<div class="loading">로딩 중...</div>';

  // 사이드바 활성 표시
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

// -- 초기 페이지 --
const initialPage = location.hash.slice(1) || 'dashboard';
navigate(initialPage);
