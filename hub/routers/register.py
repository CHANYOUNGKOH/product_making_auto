# hub/routers/register.py
"""카테고리-스토어 배정 API."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
from pydantic import BaseModel


class AssignmentIn(BaseModel):
    store_alias: str
    oc_category_name: str


router = APIRouter()


@router.get("/api/register/oc-categories")
async def oc_categories():
    """상품 DB의 OC 카테고리 목록 + 상품 수."""
    from hub.services.register_service import get_oc_categories_with_counts
    return get_oc_categories_with_counts()


@router.get("/api/register/assignments/{store_alias}")
async def get_assignments(store_alias: str):
    """특정 스토어의 카테고리 배정 목록."""
    from hub.services.register_service import get_assignments
    return get_assignments(store_alias)


@router.post("/api/register/assignments")
async def add_assignment(body: AssignmentIn):
    """카테고리 배정 추가. body: {store_alias, oc_category_name}."""
    from hub.services.register_service import add_assignment
    try:
        return add_assignment(
            store_alias=body.store_alias,
            oc_category_name=body.oc_category_name,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.delete("/api/register/assignments/{store_alias}")
async def remove_assignment(store_alias: str, oc_category_name: str):
    """카테고리 배정 제거. Query param: oc_category_name."""
    from hub.services.register_service import remove_assignment
    return remove_assignment(store_alias, oc_category_name)


@router.get("/api/register/preview/{store_alias}")
async def pipeline_preview(store_alias: str, strategy: str = "lowest_price"):
    from hub.services.register_service import get_pipeline_preview
    return get_pipeline_preview(store_alias, strategy)


@router.post("/api/register/run/godomall")
async def run_godomall(body: dict):
    from hub.services.register_service import run_godomall_pipeline
    store_alias = body.get("store_alias")
    if not store_alias:
        raise HTTPException(status_code=422, detail="store_alias 필수")
    return run_godomall_pipeline(store_alias, body.get("strategy", "lowest_price"))


@router.post("/api/register/run/esellers")
async def run_esellers(body: dict):
    from hub.services.register_service import run_esellers_pipeline
    store_alias = body.get("store_alias")
    if not store_alias:
        raise HTTPException(status_code=422, detail="store_alias 필수")
    return run_esellers_pipeline(store_alias, body.get("strategy", "lowest_price"))


@router.get("/api/register/download/{file_id}")
async def download_file(file_id: str):
    from hub.services.register_service import get_export_file
    path = get_export_file(file_id)
    if not path or not Path(path).exists():
        raise HTTPException(404, "파일 없음")
    return FileResponse(path, filename=Path(path).name,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
