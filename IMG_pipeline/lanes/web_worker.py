"""Web 레인 워커 프로세스 (CDP 붙기 방식).

Cloudflare 우회:
1. launch_debug_chrome() — Chrome을 디버그 포트로 독립 실행
2. Playwright가 connect_over_cdp로 붙어서 자동화
3. Cloudflare 검증은 사용자가 수동 통과
"""
from __future__ import annotations

import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from IMG_pipeline.post_processor import mark_status


def _build_prompt(prompt_path: Path) -> str:
    return prompt_path.read_text(encoding="utf-8")


def worker_loop(task_q: "mp.Queue", event_q: "mp.Queue",
                account_name: str = "chatgpt_acc1",
                headless: bool = False,
                cdp_port: int = 9222):
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass

    from IMG_pipeline.lanes.web.session import (
        launch_debug_chrome, cdp_url, profile_dir)
    from IMG_pipeline.lanes.web.chatgpt import (
        ChatGPTAutomation, RateLimited, TimeoutGeneration)

    event_q.put({"kind": "log", "lane": "web",
                 "message": f"Web worker starting (pid={os.getpid()}, profile={account_name})"})

    # Chrome 독립 실행 (Cloudflare 사용자 통과 위해)
    proc = launch_debug_chrome(account_name=account_name, port=cdp_port,
                                url="https://chatgpt.com/")
    if proc is None:
        event_q.put({"kind": "log", "lane": "web",
                     "message": f"[info] 이미 포트 {cdp_port} 에 Chrome 이 열려 있음 — 재사용"})
    else:
        event_q.put({"kind": "log", "lane": "web",
                     "message": f"Chrome launched (pid={proc.pid}, port={cdp_port}). "
                                f"Cloudflare 보안 뜨면 직접 통과 + Google 로그인 해주세요."})

    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(cdp_url(cdp_port))
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        # chatgpt.com 탭만 대상으로 잡기 — Google 부가 탭 무시
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

        # 새 탭 생기면 ChatGPT 탭을 계속 유지
        def _keep_chatgpt(new_page):
            try:
                url = new_page.url or ""
                if "chatgpt.com" not in url:
                    event_q.put({"kind": "log", "lane": "web",
                                 "message": f"[info] 외부 탭 무시: {url[:80]}"})
            except Exception:
                pass
        ctx.on("page", _keep_chatgpt)

        bot = ChatGPTAutomation(page, logger=lambda m: event_q.put(
            {"kind": "log", "lane": "web", "message": m}))

        # 로그인/Cloudflare 대기 (최대 10분)
        if not bot.is_logged_in():
            event_q.put({"kind": "log", "lane": "web",
                         "message": "⚠ 로그인/Cloudflare 대기 중 — 브라우저에서 작업 완료 후 자동 진행됩니다 (최대 10분)."})
            if not bot.wait_for_login(timeout_sec=600):
                event_q.put({"kind": "log", "lane": "web",
                             "message": "로그인 타임아웃. 종료."})
                event_q.put({"kind": "done", "lane": "web",
                             "message": "login timeout"})
                return
            event_q.put({"kind": "log", "lane": "web",
                         "message": "✓ 로그인/Cloudflare 통과 확인"})

        from IMG_pipeline.lanes.web.chatgpt import delete_current_chat
        consecutive_fails = 0
        MAX_CONSECUTIVE_FAIL = 5
        while True:
            task = task_q.get()
            if task is None:
                break
            code = task["code"]
            mark_status(code, "running", error=None, lock=True)
            event_q.put({"kind": "status", "lane": "web", "code": code,
                         "status": "running"})
            t0 = time.time()
            try:
                # 생성 루프 (재시도 포함)
                retries = task.get("max_retries", 1)
                last_exc = None
                for attempt in range(retries + 1):
                    try:
                        bot.new_chat()
                        bot.attach_image(task["nukki_path"])
                        bot.send_prompt(_build_prompt(Path(task["prompt_path"])))
                        timeout_sec = task.get("timeout", 240)
                        if timeout_sec > 1000:
                            timeout_sec //= 1000
                        bot.wait_for_generated_image(timeout_sec=timeout_sec)
                        bot.download_last_image(task["out_path"])
                        last_exc = None
                        break
                    except Exception as e:
                        last_exc = e
                        if attempt < retries:
                            event_q.put({"kind": "log", "lane": "web",
                                         "message": f"[{code}] attempt {attempt+1} 실패 재시도: {type(e).__name__}"})
                            time.sleep(3)
                if last_exc is not None:
                    raise last_exc

                # 후처리
                url = None
                try:
                    from IMG_pipeline.post_processor import process_one
                    result = process_one(Path(task["out_path"]), code, "web",
                                          logger=lambda m: event_q.put(
                                              {"kind": "log", "lane": "web",
                                               "message": m}))
                    url = result["url"]
                except Exception as pe:
                    mark_status(code, "failed", error=f"post-process failed: {pe}", lock=False)
                    event_q.put({"kind": "log", "lane": "web",
                                 "message": f"[{code}] post-process failed: {pe}"})
                    event_q.put({"kind": "status", "lane": "web", "code": code,
                                 "status": "failed",
                                 "message": f"POST-PROCESS FAIL: {pe}"})
                    consecutive_fails += 1
                    continue

                elapsed = time.time() - t0
                consecutive_fails = 0

                # 채팅 자동 삭제
                if task.get("auto_delete_chat", True):
                    try:
                        if delete_current_chat(page):
                            event_q.put({"kind": "log", "lane": "web",
                                         "message": f"[{code}] 채팅 삭제 완료"})
                    except Exception:
                        pass

                event_q.put({
                    "kind": "status", "lane": "web", "code": code,
                    "status": "done",
                    "message": f"OK {elapsed:.1f}s{' → ' + url if url else ''}",
                    "meta": {"elapsed": elapsed, "url": url},
                })
            except RateLimited as e:
                mark_status(code, "failed", error=f"RATE LIMITED: {e}", lock=False)
                event_q.put({"kind": "status", "lane": "web", "code": code,
                             "status": "failed",
                             "message": f"RATE LIMITED: {e}"})
            except TimeoutGeneration as e:
                mark_status(code, "failed", error=f"TIMEOUT: {e}", lock=False)
                event_q.put({"kind": "status", "lane": "web", "code": code,
                             "status": "failed",
                             "message": f"TIMEOUT: {e}"})
            except Exception as e:
                consecutive_fails += 1
                mark_status(code, "failed", error=f"ERROR: {type(e).__name__}: {e}", lock=False)
                event_q.put({"kind": "status", "lane": "web", "code": code,
                             "status": "failed",
                             "message": f"ERROR: {type(e).__name__}: {e}"})
                # CDP/세션 크래시 감지 → 재연결 시도
                sess_dead = False
                try:
                    sess_dead = not bot.is_session_alive()
                except Exception:
                    sess_dead = True
                if sess_dead or "TargetClosed" in type(e).__name__ or "Connection" in type(e).__name__:
                    event_q.put({"kind": "log", "lane": "web",
                                 "message": "[!] 세션/CDP 이상 감지 — 재연결 시도"})
                    try:
                        ctx.close()
                    except Exception: pass
                    time.sleep(5)
                    try:
                        from IMG_pipeline.lanes.web.session import launch_debug_chrome
                        launch_debug_chrome(account_name=account_name, port=cdp_port,
                                             url="https://chatgpt.com/")
                        time.sleep(5)
                        browser = pw.chromium.connect_over_cdp(cdp_url(cdp_port))
                        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
                        page = next((p for p in ctx.pages if "chatgpt.com" in (p.url or "")),
                                     ctx.pages[0] if ctx.pages else ctx.new_page())
                        bot = ChatGPTAutomation(page, logger=lambda m: event_q.put(
                            {"kind": "log", "lane": "web", "message": m}))
                        event_q.put({"kind": "log", "lane": "web",
                                     "message": "✓ 재연결 성공"})
                        consecutive_fails = 0
                    except Exception as re:
                        event_q.put({"kind": "log", "lane": "web",
                                     "message": f"재연결 실패: {re}"})
                if consecutive_fails >= MAX_CONSECUTIVE_FAIL:
                    event_q.put({"kind": "log", "lane": "web",
                                 "message": f"[!] 연속 {MAX_CONSECUTIVE_FAIL}회 실패 — 10분 대기 후 재개"})
                    time.sleep(600)
                    consecutive_fails = 0
                    event_q.put({"kind": "log", "lane": "web", "message": "워커 재개"})

        # CDP로 붙은 경우 browser.close() 하면 실제 Chrome이 유지되므로 disconnect 만
        try:
            browser.close()
        except Exception:
            pass

    event_q.put({"kind": "done", "lane": "web", "message": "worker exit"})
