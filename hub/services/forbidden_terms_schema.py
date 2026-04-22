from __future__ import annotations

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field

from hub.services.register_service import MARKET_TO_GROUP


class ForbiddenSeverity(str, Enum):
    HIGH = "HIGH"
    MED = "MED"
    LOW = "LOW"


CANONICAL_FORBIDDEN_SCOPE_VALUES = tuple(
    dict.fromkeys(["all", *MARKET_TO_GROUP.values()])
)


class ForbiddenScope(str, Enum):
    ALL = "all"
    NAVER = "naver"
    ESM = "esm"
    ST11 = "st11"
    COUPANG = "coupang"
    INTERPARK = "interpark"
    TMON = "tmon"
    WMP = "wmp"


class ForbiddenTermRecord(BaseModel):
    term: str = Field(min_length=1)
    severity: ForbiddenSeverity
    scope: ForbiddenScope
    reason: str = Field(min_length=1)
    source: str = Field(min_length=1)
    added_at: date


__all__ = [
    "CANONICAL_FORBIDDEN_SCOPE_VALUES",
    "ForbiddenScope",
    "ForbiddenSeverity",
    "ForbiddenTermRecord",
]
