"""API endpoints for Hub image-genai monitoring and control."""
from __future__ import annotations

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from hub.services import image_genai_service

router = APIRouter()


class ResetStaleRequest(BaseModel):
    stale_minutes: int = Field(default=30, ge=1, le=1440)


class RetryRequest(BaseModel):
    code: str


class IssueMemoRequest(BaseModel):
    code: str
    memo: str = ""


@router.get("/api/image-genai/summary")
async def summary():
    return image_genai_service.get_summary()


@router.get("/api/image-genai/targets")
async def targets(status: str | None = Query(default=None)):
    return image_genai_service.list_targets(status=status)


@router.post("/api/image-genai/reset-stale")
async def reset_stale(req: ResetStaleRequest):
    return image_genai_service.reset_stale(stale_minutes=req.stale_minutes)


@router.post("/api/image-genai/retry")
async def retry(req: RetryRequest):
    return image_genai_service.retry(code=req.code)


@router.post("/api/image-genai/issue")
async def save_issue(req: IssueMemoRequest):
    return image_genai_service.save_issue(code=req.code, memo=req.memo)


@router.get("/api/image-genai/issue/{code}")
async def load_issue(code: str):
    return image_genai_service.load_issue(code=code)
