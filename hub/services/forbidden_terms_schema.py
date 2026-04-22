from __future__ import annotations

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field


class ForbiddenSeverity(str, Enum):
    HIGH = "HIGH"
    MED = "MED"
    LOW = "LOW"


class ForbiddenScope(str, Enum):
    ALL = "all"
    NAVER = "naver"
    GODOMALL = "godomall"
    SMARTSTORE = "smartstore"
    ELEVENST = "11st"
    ESM = "esm"


class ForbiddenTermRecord(BaseModel):
    term: str = Field(min_length=1)
    severity: ForbiddenSeverity
    scope: ForbiddenScope
    reason: str = Field(min_length=1)
    source: str = Field(min_length=1)
    added_at: date


__all__ = [
    "ForbiddenScope",
    "ForbiddenSeverity",
    "ForbiddenTermRecord",
]
