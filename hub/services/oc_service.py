"""OC 동기화 래퍼 — sync_existing()을 async SSE 스트림으로 노출."""
from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncGenerator

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oc_sync")


def _sse(event_dict: dict) -> str:
    return f"data: {json.dumps(event_dict, ensure_ascii=False)}\n\n"


async def sync_stream(dry: bool = False) -> AsyncGenerator[str, None]:
    """
    sync_existing()을 쓰레드에서 실행하며 SSE 이벤트를 yield.

    dry=True 이면 OC API 호출 없이 즉시 done 반환 (테스트/미리보기용).
    """
    loop = asyncio.get_running_loop()
    progress_events: asyncio.Queue[dict] = asyncio.Queue()

    yield _sse({"type": "start", "message": "OC 동기화 시작..."})

    if dry:
        yield _sse({"type": "done", "message": "dry-run 완료 (API 호출 없음)", "updated": 0})
        return

    def progress_cb(current: int, total: int, message: str) -> None:
        event = {"type": "progress", "current": current, "total": total, "message": message}
        asyncio.run_coroutine_threadsafe(progress_events.put(event), loop)

    def run_sync():
        from godomall_register.oc_import import sync_existing
        from hub.services.db_service import get_db_path
        result = sync_existing(
            db_path=get_db_path(),
            progress_callback=progress_cb,
        )
        asyncio.run_coroutine_threadsafe(
            progress_events.put({"type": "done", "message": "동기화 완료",
                                 "updated": result.get("updated", 0),
                                 "not_found": result.get("not_found", 0),
                                 "errors": result.get("errors", [])}),
            loop,
        )

    future = loop.run_in_executor(_executor, run_sync)

    while not future.done() or not progress_events.empty():
        try:
            event = progress_events.get_nowait()
            yield _sse(event)
            if event["type"] == "done":
                break
        except asyncio.QueueEmpty:
            yield _sse({"type": "ping"})
            await asyncio.sleep(0.5)

    await future
