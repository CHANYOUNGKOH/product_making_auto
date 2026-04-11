"""파이프라인 API — OC 동기화 SSE."""
from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from hub.services import db_service
from hub.services import oc_service

router = APIRouter()


@router.get("/api/pipeline/status")
async def pipeline_status():
    stats = db_service.get_dashboard_stats()
    return {
        "oc_synced_count": stats["shippable"],
        "last_sync_at": stats["last_sync_at"],
    }


@router.get("/api/pipeline/sync/stream")
async def sync_stream(dry: bool = Query(default=False)):
    return StreamingResponse(
        oc_service.sync_stream(dry=dry),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
