"""Hub service layer for the shared image-genai pipeline state."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from hub.services.db_service import _conn, get_db_path
from IMG_pipeline import db_query
from IMG_pipeline.post_processor import (
    _ensure_columns,
    get_issue_memo,
    mark_status,
    reset_stale_locks,
    save_issue_memo,
)


def _db_path() -> Path:
    return Path(get_db_path())


def _ensure_genai_schema() -> None:
    db_path = _db_path()
    con = sqlite3.connect(str(db_path), timeout=30)
    try:
        _ensure_columns(con)
    finally:
        con.close()


def _target_rows() -> list[dict[str, Any]]:
    _ensure_genai_schema()
    db_path = _db_path()
    cols = db_query._resolve_columns(db_path)
    query = f"""
        SELECT
            "{cols["code"]}" AS code,
            "{cols["st4"]}" AS name,
            "{cols["nukki"]}" AS nukki_url,
            {f'"{cols["category"]}"' if cols.get("category") else "''"} AS category,
            ST2_JSON,
            image_slots,
            genai_status,
            genai_error,
            genai_locked_at,
            genai_issue_memo,
            genai_updated_at
        FROM products
        WHERE "{cols["st4"]}" IS NOT NULL AND "{cols["st4"]}" != ''
          AND "{cols["nukki"]}" IS NOT NULL AND "{cols["nukki"]}" != ''
          AND oc_status = 'available'
          AND ST2_JSON IS NOT NULL AND ST2_JSON != ''
        ORDER BY "{cols["code"]}"
    """
    with _conn(str(db_path)) as con:
        return [dict(row) for row in con.execute(query).fetchall()]


def _parse_slots(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text or "pending"


def _has_slot_url(value: Any) -> bool:
    if isinstance(value, dict):
        return bool(value.get("url"))
    if isinstance(value, list):
        return any(_has_slot_url(item) for item in value)
    return False


def _lane_key(key: str) -> str:
    if key.startswith("generated"):
        return "generated"
    return key


def get_summary() -> dict[str, Any]:
    items = _target_rows()
    status_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    stale_running = 0

    for item in items:
        status = _normalize_status(item.get("genai_status"))
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "running" and not item.get("genai_locked_at"):
            stale_running += 1

        for key, value in _parse_slots(item.get("image_slots")).items():
            lane = _lane_key(key)
            if _has_slot_url(value):
                lane_counts[lane] = lane_counts.get(lane, 0) + 1

    return {
        "total_targets": len(items),
        "status_counts": status_counts,
        "lane_counts": lane_counts,
        "stale_running_without_lock": stale_running,
    }


def list_targets(status: str | None = None) -> dict[str, Any]:
    requested = _normalize_status(status) if status else None
    items: list[dict[str, Any]] = []

    for row in _target_rows():
        normalized_status = _normalize_status(row.get("genai_status"))
        if requested and normalized_status != requested:
            continue
        slots = _parse_slots(row.get("image_slots"))
        items.append(
            {
                "code": row["code"],
                "name": row.get("name") or "",
                "category": row.get("category") or "",
                "nukki_url": row.get("nukki_url") or "",
                "genai_status": normalized_status,
                "genai_error": row.get("genai_error"),
                "genai_locked_at": row.get("genai_locked_at"),
                "genai_issue_memo": row.get("genai_issue_memo"),
                "genai_updated_at": row.get("genai_updated_at"),
                "image_slots": slots,
                "lane_keys": sorted(_lane_key(key) for key, value in slots.items() if _has_slot_url(value)),
            }
        )

    return {"items": items, "total": len(items)}


def reset_stale(stale_minutes: int) -> dict[str, Any]:
    count = reset_stale_locks(stale_minutes=stale_minutes, db_path=_db_path())
    return {"reset_count": count, "stale_minutes": stale_minutes}


def retry(code: str) -> dict[str, Any]:
    mark_status(code, "pending", error=None, lock=False, db_path=_db_path())
    return {"code": code, "status": "pending"}


def save_issue(code: str, memo: str) -> dict[str, Any]:
    save_issue_memo(code, memo, db_path=_db_path())
    return {"saved": True, "code": code, "memo": memo}


def load_issue(code: str) -> dict[str, Any]:
    return {"code": code, "memo": get_issue_memo(code, db_path=_db_path()) or ""}
