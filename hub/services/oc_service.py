"""OC 동기화 래퍼 — full_sync()를 async SSE 스트림으로 노출."""
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
    """full_sync()를 쓰레드에서 실행하며 SSE 이벤트를 yield.

    Pass 1(전체 키) → Pass 2(기존 갱신) → Pass 3(신규 INSERT + 공급사 발굴).
    dry=True 이면 API 호출 없이 즉시 done 반환 (테스트용).
    """
    yield _sse({"type": "start", "message": "OC 전체 동기화 시작 (Pass 1: 키 수집 중)..."})

    if dry:
        yield _sse({"type": "done", "message": "dry-run 완료", "updated": 0,
                    "inserted": 0, "new_vendors": 0, "vendors": []})
        return

    loop = asyncio.get_running_loop()
    progress_events: asyncio.Queue = asyncio.Queue()

    def progress_cb(current: int, total: int, message: str) -> None:
        event = {"type": "progress", "current": current, "total": total, "message": message}
        asyncio.run_coroutine_threadsafe(progress_events.put(event), loop)

    def run_sync():
        try:
            from godomall_register.oc_import import full_sync
            from hub.services.db_service import get_db_path
            result = full_sync(
                db_path=get_db_path(),
                progress_callback=progress_cb,
            )
            done_event = {
                "type": "done",
                "message": "동기화 완료",
                "total_oc":    result.get("total_oc", 0),
                "updated":     result.get("updated", 0),
                "not_found":   result.get("not_found", 0),
                "inserted":    result.get("inserted", 0),
                "set_inactive": result.get("set_inactive", 0),
                "new_vendors": result.get("new_vendors", 0),
                "vendors":     result.get("vendors", []),
                "errors":      result.get("errors", []),
            }
        except Exception as exc:
            done_event = {"type": "error", "message": str(exc)}
        asyncio.run_coroutine_threadsafe(progress_events.put(done_event), loop)
        asyncio.run_coroutine_threadsafe(progress_events.put(_DONE_SENTINEL), loop)

    future = loop.run_in_executor(_executor, run_sync)

    # 15초마다 ping — 긴 Pass 1 중 브라우저 SSE 연결 유지
    while True:
        try:
            event = await asyncio.wait_for(progress_events.get(), timeout=15.0)
        except asyncio.TimeoutError:
            yield _sse({"type": "ping"})
            continue
        if event is _DONE_SENTINEL:
            break
        yield _sse(event)

    await future
