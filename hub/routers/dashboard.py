"""대시보드 API — 통계 4개 카드."""
from fastapi import APIRouter

from hub.services.db_service import get_dashboard_stats

router = APIRouter()


@router.get("/api/dashboard")
async def dashboard():
    return get_dashboard_stats()
