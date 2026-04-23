"""ChatGPT 웹 단위 동작 (Playwright 기반).

ChatGPT 최신 UI 구조 반영:
- 입력창: #prompt-textarea (contenteditable div) 또는 textarea
- 첨부: "+" 버튼 → "Add photos & files" 메뉴 또는 직접 input[type=file]
- 전송: [data-testid="send-button"] / [data-testid="composer-send-button"]
- 생성 이미지: img with alt starting "Generated image"
- 다운로드: 이미지 hover 후 Download 버튼 (aria-label="Download")
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, Locator, TimeoutError as PWTimeout
from playwright.sync_api import Download


class RateLimited(Exception):
    pass


class TimeoutGeneration(Exception):
    pass


# 채팅 삭제 — 생성 후 사이드바 누적 방지
def delete_current_chat(page) -> bool:
    """현재 채팅 삭제. URL이 /c/<id> 일 때만 유효."""
    try:
        import time as _t
        # 현재 채팅 ID 확인
        url = page.url or ""
        if "/c/" not in url:
            return False
        # 사이드바에서 현재 active 항목의 3-dot 메뉴 열기
        # 방법 1: 현재 URL 기반 href 찾고 그 안의 버튼
        cur_id = url.split("/c/")[-1].split("?")[0]
        for sel in [
            f'a[href*="/c/{cur_id}"]',
            f'a[href$="{cur_id}"]',
        ]:
            try:
                link = page.locator(sel).first
                link.hover(timeout=2000)
                _t.sleep(0.3)
                # 3-dot 버튼 (옵션 메뉴)
                opened = False
                for btn_sel in [
                    f'{sel} ~ button[aria-label*="Options" i]',
                    f'{sel} button[aria-label*="Options" i]',
                    'button[aria-haspopup="menu"][aria-label*="Options" i]',
                ]:
                    try:
                        page.locator(btn_sel).first.click(timeout=1500)
                        opened = True
                        break
                    except Exception:
                        continue
                if not opened:
                    continue
                _t.sleep(0.3)
                # 메뉴에서 "Delete" / "삭제"
                for label in ["Delete", "삭제", "Delete chat"]:
                    try:
                        page.get_by_role("menuitem", name=label).first.click(timeout=1500)
                        _t.sleep(0.3)
                        # 확인 다이얼로그
                        for conf in ["Delete", "삭제", "Confirm"]:
                            try:
                                page.get_by_role("button", name=conf).last.click(timeout=1500)
                                return True
                            except Exception:
                                continue
                        return True
                    except Exception:
                        continue
            except Exception:
                continue
        return False
    except Exception:
        return False


class ChatGPTAutomation:
    BASE_URL = "https://chatgpt.com/"
    RATE_LIMIT_PATTERNS = [
        "reached your limit", "try again later", "usage cap",
        "You've reached", "message limit",
        "사용량 초과", "한도에 도달", "메시지 제한", "이미지 생성 한도",
        "You've hit the", "try again in",
    ]
    LOGIN_PAGE_PATTERNS = ["/auth/", "/login", "sign in", "Continue with"]

    def __init__(self, page: Page, logger=print):
        self.page = page
        self.log = logger

    # ── 저수준 locator 헬퍼 ───────────────────────────────────────────────

    def _input_editor(self) -> Locator:
        """입력 editor — contenteditable div 또는 textarea, 여러 후보 중 가시 요소."""
        candidates = [
            "#prompt-textarea",
            '[contenteditable="true"][translate="no"]',
            'textarea[placeholder*="sk" i]',  # "Ask anything"
            'textarea',
            '[contenteditable="true"]',
        ]
        for sel in candidates:
            loc = self.page.locator(sel).first
            try:
                if loc.is_visible(timeout=1000):
                    return loc
            except Exception:
                continue
        raise RuntimeError("input editor not found")

    def _send_button(self) -> Locator:
        for sel in [
            '[data-testid="send-button"]',
            '[data-testid="composer-send-button"]',
            'button[aria-label*="Send" i]',
            'button:has-text("Send")',
        ]:
            loc = self.page.locator(sel).first
            try:
                if loc.is_visible(timeout=1000):
                    return loc
            except Exception:
                continue
        raise RuntimeError("send button not found")

    # ── 세션 / 로그인 ─────────────────────────────────────────────────────

    def goto_home(self, timeout: int = 30_000):
        self.page.goto(self.BASE_URL, wait_until="domcontentloaded", timeout=timeout)
        self.page.wait_for_timeout(1500)

    def is_logged_in(self) -> bool:
        try:
            # URL이 login/auth 페이지면 즉시 false
            url = (self.page.url or "").lower()
            for pat in self.LOGIN_PAGE_PATTERNS:
                if pat.lower() in url:
                    return False
            self._input_editor()
            return True
        except Exception:
            return False

    def is_session_alive(self) -> bool:
        """페이지가 닫혔거나 브라우저 컨텍스트가 끊겼는지 검사."""
        try:
            if self.page.is_closed(): return False
            # URL 조회 시도 → connection 확인
            _ = self.page.url
            return True
        except Exception:
            return False

    def wait_for_login(self, timeout_sec: int = 600) -> bool:
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            if self.is_logged_in():
                return True
            self.page.wait_for_timeout(2000)
        return False

    # ── 새 채팅 ───────────────────────────────────────────────────────────

    def new_chat(self):
        self.page.goto(self.BASE_URL, wait_until="domcontentloaded")
        for _ in range(15):
            try:
                self._input_editor()
                break
            except Exception:
                self.page.wait_for_timeout(1000)
        else:
            raise RuntimeError("new_chat: editor never appeared")
        # 이미지 생성 모드 강제
        self.enable_image_mode()

    def enable_image_mode(self):
        """composer의 + 메뉴에서 '이미지 만들기' 선택. 이미 활성 시 no-op.

        실패해도 프롬프트 기반 image-gen 라우팅이 되는 경우가 많아 예외는 경고만.
        """
        # 이미 chip 이 떠 있으면 스킵
        for indicator in ["이미지 만들기", "Create image", "Image generation", "DALL·E"]:
            try:
                if self.page.locator(f'text="{indicator}"').first.is_visible(timeout=500):
                    # 이미 선택된 표시면 스킵
                    pass
            except Exception:
                pass

        # + 버튼 열기
        opened = False
        for sel in [
            'button[aria-label*="Add" i]',
            'button[aria-label*="Attach" i]',
            'button[aria-label*="Tools" i]',
            'button:has(svg)[data-testid*="plus" i]',
        ]:
            try:
                self.page.locator(sel).first.click(timeout=2000)
                opened = True
                break
            except Exception:
                continue
        if not opened:
            self.log("enable_image_mode: + 메뉴 버튼 미발견 (건너뜀)")
            return

        self.page.wait_for_timeout(500)

        # 메뉴에서 이미지 만들기 선택
        for label in ["이미지 만들기", "Create image", "Create an image",
                      "Generate image", "Image"]:
            try:
                self.page.get_by_role("menuitem", name=label).first.click(timeout=1500)
                self.log(f"이미지 모드 활성화: {label}")
                self.page.wait_for_timeout(400)
                return
            except Exception:
                try:
                    self.page.get_by_text(label, exact=False).first.click(timeout=1000)
                    self.log(f"이미지 모드 활성화(text): {label}")
                    return
                except Exception:
                    continue

        self.log("enable_image_mode: 메뉴 항목 미발견 — ESC 로 닫고 계속")
        try:
            self.page.keyboard.press("Escape")
        except Exception:
            pass

    # ── 첨부 ──────────────────────────────────────────────────────────────

    def attach_image(self, file_path: str):
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(file_path)

        # 1차: DOM에 이미 input[type=file] 있는지
        fins = self.page.locator('input[type="file"]')
        if fins.count() == 0:
            # 2차: + 버튼 클릭해서 노출
            opened = False
            for name in ["Add photos & files", "Attach files", "Attach",
                         "Upload files", "Add"]:
                try:
                    self.page.get_by_role("button", name=name).first.click(timeout=2500)
                    opened = True
                    break
                except Exception:
                    continue
            if not opened:
                # aria-label 기반
                try:
                    self.page.locator('button[aria-label*="Add" i]').first.click(timeout=2500)
                    opened = True
                except Exception:
                    pass

            self.page.wait_for_timeout(600)

            # 메뉴에서 "Upload from computer" 선택 가능
            for name in ["Upload from computer", "From computer", "Upload file"]:
                try:
                    self.page.get_by_role("menuitem", name=name).first.click(timeout=1500)
                    break
                except Exception:
                    try:
                        self.page.get_by_text(name, exact=False).first.click(timeout=1500)
                        break
                    except Exception:
                        continue

            self.page.wait_for_timeout(800)
            fins = self.page.locator('input[type="file"]')

        if fins.count() == 0:
            raise RuntimeError("file input not available after attach attempts")

        fins.first.set_input_files(str(path))
        # 업로드 완료 대기 — thumbnail 프리뷰 나타날 때까지
        self.page.wait_for_timeout(2500)

    # ── 프롬프트 입력 + 전송 ──────────────────────────────────────────────

    def send_prompt(self, text: str):
        editor = self._input_editor()
        editor.click()
        try:
            editor.fill(text)
        except Exception:
            editor.press_sequentially(text, delay=3)

        self.page.wait_for_timeout(300)
        try:
            self._send_button().click()
        except Exception:
            editor.press("Enter")

        # 전송 후 대화 하단으로 스크롤 — 새 메시지/이미지가 뷰포트 밖에 생성되는 문제 방지
        self.page.wait_for_timeout(500)
        try:
            self.page.keyboard.press("End")
        except Exception:
            pass
        try:
            self.page.evaluate(
                "document.querySelector('main')?.scrollTo(0, 1e9);"
                "window.scrollTo(0, document.body.scrollHeight);"
            )
        except Exception:
            pass

    # ── 생성 대기 ─────────────────────────────────────────────────────────

    def _count_generated_images(self) -> int:
        return self.page.locator('img[alt*="Generated" i], img[alt*="생성된 이미지"]').count()

    def _check_rate_limit(self):
        for pat in self.RATE_LIMIT_PATTERNS:
            try:
                if self.page.get_by_text(pat, exact=False).count() > 0:
                    raise RateLimited(pat)
            except RateLimited:
                raise
            except Exception:
                continue

    def wait_for_generated_image(self, timeout_sec: int = 240) -> str:
        t0 = time.time()
        baseline = self._count_generated_images()
        self.log(f"wait_for_generated_image baseline={baseline}")
        while time.time() - t0 < timeout_sec:
            self._check_rate_limit()
            # 주기적으로 스크롤
            try:
                self.page.evaluate(
                    "document.querySelector('main')?.scrollTo(0, 1e9);"
                    "window.scrollTo(0, document.body.scrollHeight);"
                )
            except Exception:
                pass
            cur = self._count_generated_images()
            if cur > baseline:
                self.log(f"새 이미지 감지: baseline={baseline} → cur={cur}")
                self.page.wait_for_timeout(2500)
                img = self.page.locator(
                    'img[alt*="Generated" i], img[alt*="생성된 이미지"]').last
                src = img.get_attribute("src") or ""
                # src 가 data-url / blob 이면 한 번 더 대기
                if not src or src.startswith("data:") or src.startswith("blob:"):
                    self.page.wait_for_timeout(1500)
                    src = img.get_attribute("src") or ""
                return src
            self.page.wait_for_timeout(2000)
        raise TimeoutGeneration(f"{timeout_sec}s")

    # ── 다운로드 ──────────────────────────────────────────────────────────

    def download_last_image(self, save_to: str) -> str:
        save_path = Path(save_to)
        save_path.parent.mkdir(parents=True, exist_ok=True)

        img = self.page.locator('img[alt*="Generated" i], img[alt*="생성된 이미지"]').last
        img.scroll_into_view_if_needed()
        self.page.wait_for_timeout(800)

        src = img.get_attribute("src") or ""
        self.log(f"image src={src[:120]}")

        # 방법 1 (최우선): Playwright 페이지 컨텍스트로 src 직접 요청 — 세션쿠키 자동 포함
        if src.startswith("http"):
            try:
                resp = self.page.request.get(src, timeout=30_000)
                if resp.ok:
                    save_path.write_bytes(resp.body())
                    self.log(f"downloaded via page.request.get ({len(resp.body())} bytes)")
                    return str(save_path)
                else:
                    self.log(f"page.request status={resp.status}")
            except Exception as e:
                self.log(f"page.request failed: {e}")

        # 방법 2: blob: URL 이면 Playwright evaluate로 fetch + base64 인코딩
        if src.startswith("blob:"):
            try:
                b64 = self.page.evaluate(
                    """async (url) => {
                        const r = await fetch(url);
                        const b = await r.blob();
                        const buf = await b.arrayBuffer();
                        return btoa(String.fromCharCode(...new Uint8Array(buf)));
                    }""", src)
                import base64
                save_path.write_bytes(base64.b64decode(b64))
                self.log("downloaded via blob fetch+base64")
                return str(save_path)
            except Exception as e:
                self.log(f"blob fetch failed: {e}")

        # 방법 3: 이미지 클릭 → 뷰어 → 다운로드 버튼
        try:
            img.click(timeout=4000)
            self.page.wait_for_timeout(1500)
            for sel in [
                'button[aria-label="Download"]',
                'button[aria-label*="Download" i]',
                'button[aria-label*="다운로드" i]',
                'a[download]',
            ]:
                try:
                    with self.page.expect_download(timeout=10_000) as dl_info:
                        self.page.locator(sel).last.click(timeout=2500)
                    dl: Download = dl_info.value
                    dl.save_as(str(save_path))
                    self.log(f"downloaded via viewer [{sel}]")
                    try: self.page.keyboard.press("Escape")
                    except Exception: pass
                    return str(save_path)
                except Exception:
                    continue
            try: self.page.keyboard.press("Escape")
            except Exception: pass
        except Exception as e:
            self.log(f"viewer click failed: {e}")

        # 방법 4: 이미지 element screenshot (원본 해상도 손실 가능)
        try:
            img.screenshot(path=str(save_path))
            self.log("downloaded via element screenshot")
            return str(save_path)
        except Exception as e:
            raise RuntimeError(f"모든 다운로드 방법 실패: {e}")
