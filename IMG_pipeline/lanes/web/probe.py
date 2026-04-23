"""ChatGPT DOM 진단 스크립트.

Chrome persistent profile로 chatgpt.com 열고, 사용자가 로그인하면
주요 요소들(입력창/첨부버튼/전송버튼/이미지다운로드)의 실제 DOM 속성을 dump.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


HERE = Path(__file__).resolve().parent
IMG_PIPELINE = HERE.parent.parent
SESSIONS = IMG_PIPELINE / "web_sessions" / "chatgpt_acc1"


def main():
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    SESSIONS.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSIONS),
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 900},
            accept_downloads=True,
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto("https://chatgpt.com/", wait_until="domcontentloaded")

        print("=== 1단계: Google 로그인 ===")
        print("브라우저에서 Google 로그인 후 Enter 누르세요...")
        input()

        print("\n=== 2단계: 홈 페이지 DOM 진단 ===")

        # 입력창
        print("[textarea/placeholder 탐색]")
        for sel in [
            'textarea',
            '[contenteditable="true"]',
            '#prompt-textarea',
        ]:
            n = page.locator(sel).count()
            if n:
                first = page.locator(sel).first
                try:
                    ph = first.get_attribute("placeholder") or first.get_attribute("data-placeholder") or ""
                    aria = first.get_attribute("aria-label") or ""
                    print(f"  {sel!r}: count={n}, placeholder={ph!r}, aria-label={aria!r}")
                except Exception as e:
                    print(f"  {sel!r}: count={n}, attr err={e}")

        # 버튼 맵
        print("\n[모든 버튼 aria-label/name 수집 — 상위 30개]")
        btns = page.locator("button").all()
        for i, b in enumerate(btns[:30]):
            try:
                aria = b.get_attribute("aria-label") or ""
                txt = (b.inner_text(timeout=500) or "").strip()[:40]
                tid = b.get_attribute("data-testid") or ""
                if aria or txt or tid:
                    print(f"  [{i:02d}] aria={aria!r} txt={txt!r} testid={tid!r}")
            except Exception:
                pass

        # file input
        print("\n[file input]")
        fins = page.locator('input[type="file"]')
        print(f"  count={fins.count()}")
        for i in range(fins.count()):
            try:
                el = fins.nth(i)
                print(f"  [{i}] accept={el.get_attribute('accept')!r} name={el.get_attribute('name')!r}")
            except Exception:
                pass

        print("\n=== 3단계: 테스트 프롬프트 전송 ===")
        print("이제 1건 테스트 — Enter 누르면 자동 첨부+전송 시도합니다")
        sample_nukki = IMG_PIPELINE / "poc_input" / "W3FE6F3" / "nukki.jpg"
        print(f"  누끼: {sample_nukki} ({'있음' if sample_nukki.exists() else '없음'})")
        input("Enter to proceed...")

        # 텍스트 입력창 찾기
        ta = page.locator('#prompt-textarea, textarea, [contenteditable="true"]').first
        ta.click()

        # 첨부: input[type=file] 직접 set
        fins = page.locator('input[type="file"]')
        if fins.count() == 0:
            # + 버튼 클릭
            for aria in ["Add photos & files", "Attach files", "Attach", "Add"]:
                try:
                    page.get_by_role("button", name=aria).first.click(timeout=3000)
                    break
                except Exception:
                    continue
            page.wait_for_timeout(500)
            fins = page.locator('input[type="file"]')

        print(f"  file input 준비 후 count={fins.count()}")
        if fins.count() > 0:
            fins.first.set_input_files(str(sample_nukki))
            print("  누끼 첨부 set_input_files OK")
            page.wait_for_timeout(3000)

        # 프롬프트 입력
        prompt = "Generate a 1:1 photorealistic lifestyle thumbnail of the attached product being actively used. Do not add any text to the image."
        ta.fill(prompt)
        print("  프롬프트 입력 OK")

        # 전송
        sent = False
        for sel in ['[data-testid="send-button"]',
                    'button[aria-label*="Send" i]',
                    'button:has-text("Send")']:
            try:
                page.locator(sel).first.click(timeout=3000)
                sent = True
                print(f"  전송 OK ({sel})")
                break
            except Exception:
                continue
        if not sent:
            ta.press("Enter")
            print("  전송 Enter fallback")

        print("\n=== 4단계: 이미지 생성 대기 (최대 4분) ===")
        t0 = time.time()
        last_count = 0
        while (time.time() - t0) < 240:
            c = page.locator("img").count()
            if c != last_count:
                last_count = c
                print(f"  img count: {c} @ {int(time.time()-t0)}s")
            # 생성 이미지 감지
            gen = page.locator('img[alt*="Generated"]').count()
            if gen > 0:
                print(f"  ✓ Generated image detected: {gen}")
                break
            page.wait_for_timeout(3000)

        print("\n=== 5단계: 다운로드 버튼 찾기 ===")
        # 이미지 hover 후 다운로드 버튼 수집
        imgs = page.locator("img").all()
        for i, im in enumerate(imgs[-5:]):
            try:
                alt = im.get_attribute("alt") or ""
                src = im.get_attribute("src") or ""
                print(f"  [IMG -{5-i}] alt={alt[:40]!r} src={src[:80]!r}")
            except Exception:
                pass
        # 마지막 이미지 hover
        try:
            imgs[-1].scroll_into_view_if_needed()
            imgs[-1].hover()
            page.wait_for_timeout(1000)
            # 다운로드 후보
            for sel in ['button[aria-label="Download"]',
                        'button[aria-label*="Download" i]',
                        'a[download]',
                        'button:has-text("Download")']:
                n = page.locator(sel).count()
                if n:
                    print(f"  다운로드 후보 {sel!r}: count={n}")
        except Exception as e:
            print(f"  hover err: {e}")

        print("\n=== 진단 완료. Enter로 종료 ===")
        input()
        ctx.close()


if __name__ == "__main__":
    main()
