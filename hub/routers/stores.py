"""스토어 관리 API."""
from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, UploadFile

from hub.services.db_service import get_stores, parse_stores_from_excel, upsert_store

router = APIRouter()


@router.get("/api/stores")
async def list_stores():
    return get_stores()


@router.post("/api/stores/import")
async def import_stores(file: UploadFile = File(...)):
    if not (file.filename or "").endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="xlsx 파일만 지원합니다")

    content = await file.read()
    records = parse_stores_from_excel(content)

    imported = 0
    for rec in records:
        upsert_store(
            alias=rec["alias"],
            market=rec["market"],
            group_id=rec["group_id"],
        )
        imported += 1

    return {"imported": imported, "total_stores": len(get_stores())}
