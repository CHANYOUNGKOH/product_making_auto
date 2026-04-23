"""ChatGPT 웹 DOM 접근성 locator 정의.

유지보수: DOM 바뀌면 여기만 수정.
우선순위: role/placeholder/aria > CSS.
"""
from __future__ import annotations


class ChatGPT:
    # URL
    BASE_URL = "https://chatgpt.com/"
    LOGIN_URL_PATTERN = "auth"  # login 페이지 URL 감지용

    # 로그인 판별 (메인 채팅 UI 요소)
    LOGIN_CHECK_TEXTAREA_PLACEHOLDER = "Ask anything"  # 로그인되면 입력창 나타남

    # 새 채팅
    NEW_CHAT_NAV = {"role": "link", "name": "New chat"}

    # 첨부
    ATTACH_BUTTON = {"role": "button", "name": "Attach"}  # 또는 "Add files"
    ATTACH_FROM_COMPUTER = {"role": "menuitem", "name": "computer"}  # submenu if any

    # 프롬프트 입력
    PROMPT_TEXTAREA = {"placeholder": "Ask anything"}  # 또는 id=prompt-textarea

    # 전송
    SEND_BUTTON = {"testid": "send-button"}  # fallback role:button name:Send

    # 이미지 생성 감지
    GENERATED_IMAGE_ALT_CONTAINS = "Generated"  # <img alt="Generated image ..">
    IMAGE_IN_CONVERSATION = "img[alt*='Generated']"

    # 다운로드
    IMAGE_ACTIONS_HOVER_CONTAINER = "div[data-testid*='conversation']"  # hover target
    DOWNLOAD_BUTTON = {"role": "button", "name": "Download"}
    DOWNLOAD_ICON_ARIA = {"aria-label": "Download"}

    # 생성 중 / 에러 패턴
    GENERATING_INDICATOR_TEXT = "Generating"
    RATE_LIMIT_TEXT_PATTERNS = [
        "reached your limit",
        "try again later",
        "usage cap",
        "You've reached",
    ]
