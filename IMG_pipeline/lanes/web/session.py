"""Chromium persistent profile 관리 + CDP 연결.

Cloudflare Turnstile 등 봇 감지 우회용:
1. 사용자가 실제 Chrome을 디버그 포트로 수동 실행 → Cloudflare 사람 검증 통과
2. Playwright는 CDP로 붙어서 자동화만 수행

헬퍼:
- launch_debug_chrome(profile_name, port) : 독립 실행 Chrome 프로세스 시작
- attach_cdp(port)                         : Playwright가 붙을 connect URL 반환
"""
from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent
IMG_PIPELINE = HERE.parent.parent
SESSIONS_DIR = IMG_PIPELINE / "web_sessions"


def profile_dir(account_name: str) -> Path:
    p = SESSIONS_DIR / account_name
    p.mkdir(parents=True, exist_ok=True)
    return p


def _port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        try:
            s.connect(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _find_chrome_binary() -> str:
    if platform.system() == "Windows":
        candidates = [
            os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        ]
        for c in candidates:
            if os.path.exists(c):
                return c
    else:
        c = shutil.which("google-chrome") or shutil.which("chrome")
        if c:
            return c
    raise FileNotFoundError("Chrome not found — install Google Chrome")


def launch_debug_chrome(account_name: str = "chatgpt_acc1",
                        port: int = 9222,
                        url: str = "https://chatgpt.com/") -> subprocess.Popen | None:
    """디버그 포트 열린 Chrome 인스턴스 시작. 이미 열려 있으면 None."""
    if _port_open(port):
        return None
    chrome = _find_chrome_binary()
    udd = profile_dir(account_name)
    cmd = [
        chrome,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={udd}",
        "--no-first-run",
        "--no-default-browser-check",
        url,
    ]
    proc = subprocess.Popen(cmd)
    # 포트 열릴 때까지 대기 (최대 20초)
    for _ in range(40):
        if _port_open(port):
            return proc
        time.sleep(0.5)
    return proc


def cdp_url(port: int = 9222) -> str:
    return f"http://127.0.0.1:{port}"
