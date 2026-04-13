# hub/routers/market_status.py
"""마켓별 현황 + Excel import API."""
from __future__ import annotations

import os
import tempfile
from fastapi import APIRouter, UploadFile, File, Form

router = APIRouter()


@router.get("/api/market-status")
async def get_market_status():
    from hub.services.market_status_service import get_market_status
    return get_market_status()


@router.post("/api/market-status/import")
async def import_market_status(
    store_alias: str = Form(...),
    market: str = Form(...),
    file: UploadFile = File(...),
):
    """셀러센터 Excel 업로드 → READY 상품 상태 갱신."""
    from hub.services.market_status_service import import_from_excel
    suffix = os.path.splitext(file.filename or "")[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name
    try:
        return import_from_excel(store_alias, market, tmp_path)
    finally:
        os.unlink(tmp_path)
