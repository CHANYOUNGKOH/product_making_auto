"""OC 카탈로그 API."""
from __future__ import annotations

import os
import tempfile

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import StreamingResponse

router = APIRouter()


@router.post("/api/catalog/scan/start")
async def start_scan():
    """전체 OC 카탈로그 스캔 백그라운드 시작 (allItems — 수시간 소요)."""
    from hub.services.catalog_service import start_catalog_scan_bg
    return start_catalog_scan_bg()


@router.post("/api/catalog/sync-existing/start")
async def start_sync_existing():
    """기존 products.db 상품코드 기준 OC 동기화 백그라운드 시작 (~3~5분)."""
    from hub.services.catalog_service import start_sync_existing_bg
    return start_sync_existing_bg()


@router.post("/api/catalog/scan-csv/start")
async def start_scan_csv(body: dict = None):
    """selfcode CSV 기반 전체 OC 스캔 (~10시간)."""
    from hub.services.catalog_service import start_scan_from_csv_bg
    folder = (body or {}).get("folder", r"C:\Users\kohaz\Downloads\selfcode_2026-04-12")
    return start_scan_from_csv_bg(folder)


@router.get("/api/catalog/scan/status")
async def scan_status():
    """현재 스캔 상태 조회 (폴링용)."""
    from hub.services.catalog_service import get_scan_status
    return get_scan_status()


@router.get("/api/catalog/scan")
async def scan_catalog():
    """OC 전체 상품 스캔 → oc_catalog.db 구축 + products.db backfill (SSE, 레거시)."""
    from hub.services.catalog_service import catalog_scan_stream
    return StreamingResponse(
        catalog_scan_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/catalog/vendors")
async def catalog_vendors():
    """oc_catalog 기반 공급사 목록 (출고율+상품수 정렬, 등록 여부 포함)."""
    from hub.services.catalog_service import get_catalog_vendors
    from hub.services.db_service import get_vendors
    registered = {v["vendor_code"] for v in get_vendors()}
    return get_catalog_vendors(registered_codes=registered)


@router.get("/api/catalog/meta")
async def catalog_meta():
    """oc_catalog 스캔 메타 정보 (마지막 스캔 시각, 총 상품수 등)."""
    from hub.services.catalog_service import get_catalog_meta
    return get_catalog_meta()


@router.post("/api/catalog/import-market-categories")
async def import_market_categories(file: UploadFile = File(...)):
    """OC 양식 Excel 업로드 → oc_category_markets 저장."""
    from hub.services.catalog_service import get_catalog_db_path
    from godomall_register.market_category_fetch import fetch_from_oc_excel

    suffix = os.path.splitext(file.filename or "")[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        result = fetch_from_oc_excel(tmp_path, get_catalog_db_path())
        return result
    finally:
        os.unlink(tmp_path)
