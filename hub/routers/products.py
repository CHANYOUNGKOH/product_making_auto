from fastapi import APIRouter
router = APIRouter()

@router.get("/api/products")
async def get_products():
    return {"items": [], "total": 0}

@router.get("/api/products/categories")
async def get_categories():
    return []
