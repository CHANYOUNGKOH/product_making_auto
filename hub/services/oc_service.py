"""OC 동기화 래퍼 — sync_existing()을 async SSE 스트림으로 노출."""
from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncGenerator

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oc_sync")

# sentinel: 큐 종료 신호
_DONE_SENTINEL = object()


def _sse(event_dict: dict) -> str:
    return f"data: {json.dumps(event_dict, ensure_ascii=False)}\n\n"


async def sync_stream(dry: bool = False) -> AsyncGenerator[str, None]:
    """
    sync_existing()을 쓰레드에서 실행하며 SSE 이벤트를 yield.

    dry=True 이면 OC API 호출 없이 즉시 done 반환 (테스트/미리보기용).
    """
    yield _sse({"type": "start", "message": "OC 동기화 시작..."})

    if dry:
        yield _sse({"type": "done", "message": "dry-run 완료 (API 호출 없음)", "updated": 0})
        return

    loop = asyncio.get_running_loop()
    progress_events: asyncio.Queue = asyncio.Queue()

    def progress_cb(current: int, total: int, message: str) -> None:
        event = {"type": "progress", "current": current, "total": total, "message": message}
        asyncio.run_coroutine_threadsafe(progress_events.put(event), loop)

    def run_sync():
        try:
            from godomall_register.oc_import import sync_existing
            from hub.services.db_service import get_db_path
            result = sync_existing(
                db_path=get_db_path(),
                progress_callback=progress_cb,
            )
            done_event = {
                "type": "done",
                "message": "동기화 완료",
                "updated": result.get("updated", 0),
                "not_found": result.get("not_found", 0),
                "errors": result.get("errors", []),
            }
        except Exception as exc:
            done_event = {"type": "error", "message": str(exc)}
        # 마지막 이벤트 put 후 sentinel로 큐 종료 신호
        asyncio.run_coroutine_threadsafe(progress_events.put(done_event), loop)
        asyncio.run_coroutine_threadsafe(progress_events.put(_DONE_SENTINEL), loop)

    future = loop.run_in_executor(_executor, run_sync)

    # 큐에서 sentinel을 받을 때까지 이벤트를 소비
    # await progress_events.get()이 보장하는 전달 순서 덕분에 race condition 없음
    while True:
        event = await progress_events.get()
        if event is _DONE_SENTINEL:
            break
        yield _sse(event)

    # thread exception 재전파 (있는 경우)
    await future
