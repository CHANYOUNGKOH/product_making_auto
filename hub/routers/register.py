# hub/routers/register.py
"""카테고리-스토어 배정 API."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
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
