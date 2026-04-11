"""상품 DB API."""
from fastapi import APIRouter, HTTPException, Query

from hub.services.db_service import get_categories, get_products

router = APIRouter()


@router.get("/api/products")
async def list_products(
    q: str = Query(default=""),
    category: str = Query(default=""),
    quick_filter: str = Query(default="all"),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=500),
):
    try:
        return get_products(q=q, category=category, quick_filter=quick_filter,
                            page=page, per_page=per_page)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/products/categories")
async def list_categories():
    return get_categories()
