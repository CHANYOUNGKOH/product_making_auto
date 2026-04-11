from fastapi import APIRouter
router = APIRouter()

@router.get("/api/stores")
async def get_stores():
    return []

@router.post("/api/stores/import")
async def import_stores():
    return {}
