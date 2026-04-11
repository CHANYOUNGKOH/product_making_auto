from fastapi import APIRouter
router = APIRouter()

@router.get("/api/dashboard")
async def get_dashboard():
    return {}
