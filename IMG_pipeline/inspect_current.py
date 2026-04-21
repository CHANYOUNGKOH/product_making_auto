"""현재 ChatGPT 페이지 DOM 실태 조사."""
from __future__ import annotations

import sys
from pathlib import Path

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

from playwright.sync_api import sync_playwright
from IMG_pipeline.lanes.web.session import cdp_url


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(cdp_url(9222))
        ctx = browser.contexts[0]
        page = next((p for p in ctx.pages if "chatgpt.com" in (p.url or "")),
                    ctx.pages[0])
        print(f"url = {page.url}")

        imgs = page.locator("img").all()
        print(f"\n=== <img> 총 {len(imgs)}개 ===")
        for i, im in enumerate(imgs):
            try:
                alt = im.get_attribute("alt") or ""
                src = (im.get_attribute("src") or "")[:120]
                cls = (im.get_attribute("class") or "")[:40]
                print(f"[{i}] alt={alt[:50]!r} class={cls!r} src={src!r}")
            except Exception as e:
                print(f"[{i}] err: {e}")

        # 다운로드 버튼 후보
        print("\n=== 다운로드 후보 ===")
        for sel in [
            'button[aria-label*="Download" i]',
            'button[aria-label*="다운로드" i]',
            'a[download]',
            'button[aria-label*="Save" i]',
            'button[aria-label*="More" i]',
        ]:
            n = page.locator(sel).count()
            if n:
                print(f"  {sel!r}: count={n}")

        # 마지막 assistant message 안의 구조
        print("\n=== [data-message-author-role='assistant'] 마지막 ===")
        asst = page.locator('[data-message-author-role="assistant"]').all()
        if asst:
            last = asst[-1]
            html = last.inner_html()
            print(f"HTML 길이: {len(html)}")
            # 주요 태그 수집
            print(html[:3000])


if __name__ == "__main__":
    main()
