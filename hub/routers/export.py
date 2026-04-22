"""마켓 출고 API."""
from __future__ import annotations

from pathlib import Path
from typing import List

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from hub.services import export_service

router = APIRouter()


class ExportRequest(BaseModel):
    categories: List[str] = []
    stores: List[str] = []
    strategy: str = "lowest_price"
    format: str = "godomall"


@router.post("/api/export/dry-run")
async def dry_run(req: ExportRequest):
    return export_service.dry_run(
        categories=req.categories,
        stores=req.stores,
        strategy=req.strategy,
    )


@router.post("/api/export/run")
async def run_export(req: ExportRequest):
    return export_service.run_export(
        categories=req.categories,
        stores=req.stores,
        strategy=req.strategy,
        fmt=req.format,
    )


@router.get("/api/export/download/{file_id}")
async def download_export(file_id: str):
    path = export_service.get_export_file(file_id)
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    return FileResponse(
        path=str(path),
        filename=f"출고_{file_id[:8]}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
