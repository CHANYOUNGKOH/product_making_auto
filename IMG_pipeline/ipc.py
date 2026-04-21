"""GUI ↔ Worker 프로세스 간 IPC 이벤트 정의."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


Lane = Literal["codex", "web"]
Status = Literal["queued", "running", "done", "failed", "skipped"]


@dataclass
class Event:
    """워커 → GUI로 보내는 이벤트."""
    kind: Literal["status", "log", "done"]  # status=상품 상태변경, log=문자열, done=워커 종료
    lane: Lane
    code: str = ""
    status: Status | None = None
    message: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class Task:
    """GUI → 워커로 보내는 작업."""
    code: str
    nukki_path: str  # absolute
    prompt_path: str  # absolute
    out_path: str  # absolute
