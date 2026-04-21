"""E2E 테스트: ChatGPT 웹 자동화 1건 전체 흐름 검증.

전제: 이미 로그인된 chrome이 포트 9222에서 실행 중이거나, 스크립트가 실행.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

from IMG_pipeline.lanes.web.session import launch_debug_chrome, cdp_url
from IMG_pipeline.lanes.web.chatgpt import (
    ChatGPTAutomation, RateLimited, TimeoutGeneration)
from playwright.sync_api import sync_playwright


HERE = Path(__file__).resolve().parent
POC_INPUT = HERE / "poc_input"
OUT_DIR = HERE / "codex_output"

TEST_CODE = "W3FE6F3"  # 이미 prompt_gpt.txt + nukki.jpg 준비된 샘플


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    port = 9222
    nukki = POC_INPUT / TEST_CODE / "nukki.jpg"
    prompt_file = POC_INPUT / TEST_CODE / "prompt_gpt.txt"
    out = OUT_DIR / TEST_CODE / "web_e2e.png"
    out.parent.mkdir(parents=True, exist_ok=True)

    assert nukki.exists(), f"nukki 없음: {nukki}"
    assert prompt_file.exists(), f"prompt 없음: {prompt_file}"
    log(f"nukki={nukki.stat().st_size} bytes, prompt={prompt_file.stat().st_size} bytes")

    log("1. Chrome 확인/실행")
    proc = launch_debug_chrome(account_name="chatgpt_acc1", port=port,
                                url="https://chatgpt.com/")
    if proc:
        log(f"  Chrome 신규 실행 pid={proc.pid}")
        time.sleep(3)
    else:
        log("  기존 Chrome 재사용")

    log("2. Playwright CDP 연결")
    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(cdp_url(port))
        ctx = browser.contexts[0]
        # chatgpt 탭 선택
        page = None
        for p in ctx.pages:
            try:
                if "chatgpt.com" in (p.url or ""):
                    page = p
                    break
            except Exception:
                continue
        if page is None:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto("https://chatgpt.com/")

        log(f"  page.url = {page.url}")

        bot = ChatGPTAutomation(page, logger=log)

        log("3. 로그인 상태 확인")
        if not bot.is_logged_in():
            log("  ❌ 로그인 안 됨 — 브라우저에서 로그인 후 재시도 필요")
            return
        log("  ✓ 로그인 OK")

        log("4. new_chat() (이미지 모드 강제 포함)")
        try:
            bot.new_chat()
        except Exception as e:
            log(f"  ❌ new_chat 실패: {e}")
            return

        log("5. 누끼 첨부")
        try:
            bot.attach_image(str(nukki))
            log("  ✓ 첨부 완료")
        except Exception as e:
            log(f"  ❌ attach 실패: {e}")
            return

        log("6. 프롬프트 전송")
        prompt = prompt_file.read_text(encoding="utf-8")
        try:
            bot.send_prompt(prompt)
            log("  ✓ 전송")
        except Exception as e:
            log(f"  ❌ send 실패: {e}")
            return

        log("7. 이미지 생성 대기 (최대 4분)")
        try:
            src = bot.wait_for_generated_image(timeout_sec=240)
            log(f"  ✓ 생성 감지: src={src[:100]}")
        except TimeoutGeneration:
            log("  ❌ 생성 타임아웃")
            return
        except RateLimited as e:
            log(f"  ❌ rate limit: {e}")
            return

        log("8. 다운로드")
        try:
            saved = bot.download_last_image(str(out))
            log(f"  ✓ 저장: {saved}")
            log(f"  파일크기: {out.stat().st_size} bytes")
        except Exception as e:
            log(f"  ❌ 다운로드 실패: {e}")
            return

        log("=== E2E 성공 ===")


if __name__ == "__main__":
    main()
