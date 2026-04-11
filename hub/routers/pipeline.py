from fastapi import APIRouter
router = APIRouter()

@router.get("/api/pipeline/status")
async def get_status():
    return {}
