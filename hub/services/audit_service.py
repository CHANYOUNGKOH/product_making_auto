"""SQLite helpers for pipeline_working.db audit_inventory."""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from hub.services.db_service import get_db_path as get_products_db_path


WORKING_DB_NAME = "pipeline_working.db"

AUDIT_INVENTORY_DDL = """
CREATE TABLE IF NOT EXISTS audit_inventory (
    phase TEXT NOT NULL,
    component TEXT NOT NULL,
    file_path TEXT,
    symbol TEXT,
    line_range TEXT,
    status TEXT NOT NULL,
    evidence TEXT NOT NULL CHECK (trim(evidence) != ''),
    gap_to TEXT,
    owner_hint TEXT,
    notes TEXT,
    verified_by TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (phase, component)
)
"""


def get_working_db_path() -> str:
    configured = os.environ.get("PIPELINE_WORKING_DB_PATH")
    if configured:
        return configured
    source_path = os.environ.get("HUB_DB_PATH") or get_products_db_path()
    return str(Path(source_path).with_name(WORKING_DB_NAME))


@contextmanager
def _conn(db_path: str | None = None):
    path = db_path or get_working_db_path()
    con = sqlite3.connect(path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _require_text(name: str, value: str | None) -> str:
    if value is None:
        raise ValueError(f"{name} must be non-empty")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} must be non-empty")
    return text


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def run_migrations(db_path: str | None = None) -> None:
    with _conn(db_path) as con:
        con.execute(AUDIT_INVENTORY_DDL)


def upsert_audit(
    phase: str,
    component: str,
    *,
    file_path: str | None = None,
    symbol: str | None = None,
    line_range: str | None = None,
    status: str,
    evidence: str | None,
    gap_to: str | None = None,
    owner_hint: str | None = None,
    notes: str | None = None,
    verified_by: str,
) -> dict[str, Any]:
    run_migrations()

    record = {
        "phase": _require_text("phase", phase),
        "component": _require_text("component", component),
        "file_path": _optional_text(file_path),
        "symbol": _optional_text(symbol),
        "line_range": _optional_text(line_range),
        "status": _require_text("status", status),
        "evidence": _require_text("evidence", evidence),
        "gap_to": _optional_text(gap_to),
        "owner_hint": _optional_text(owner_hint),
        "notes": _optional_text(notes),
        "verified_by": _require_text("verified_by", verified_by),
    }

    with _conn() as con:
        con.execute(
            """
            INSERT INTO audit_inventory (
                phase, component, file_path, symbol, line_range, status,
                evidence, gap_to, owner_hint, notes, verified_by
            )
            VALUES (
                :phase, :component, :file_path, :symbol, :line_range, :status,
                :evidence, :gap_to, :owner_hint, :notes, :verified_by
            )
            ON CONFLICT(phase, component) DO UPDATE SET
                file_path = excluded.file_path,
                symbol = excluded.symbol,
                line_range = excluded.line_range,
                status = excluded.status,
                evidence = excluded.evidence,
                gap_to = excluded.gap_to,
                owner_hint = excluded.owner_hint,
                notes = excluded.notes,
                verified_by = excluded.verified_by,
                updated_at = datetime('now', 'localtime')
            """,
            record,
        )

    return query_audit(phase=record["phase"], component=record["component"])[0]


def query_audit(
    phase: str | None = None,
    status: str | None = None,
    component: str | None = None,
) -> list[dict[str, Any]]:
    run_migrations()

    clauses: list[str] = []
    params: list[Any] = []
    if phase:
        clauses.append("phase = ?")
        params.append(phase)
    if status:
        clauses.append("status = ?")
        params.append(status)
    if component:
        clauses.append("component = ?")
        params.append(component)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _conn() as con:
        rows = con.execute(
            f"""
            SELECT
                phase, component, file_path, symbol, line_range, status,
                evidence, gap_to, owner_hint, notes, verified_by,
                created_at, updated_at
            FROM audit_inventory
            {where}
            ORDER BY phase, component
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def count_by_phase() -> dict[str, int]:
    run_migrations()

    with _conn() as con:
        rows = con.execute(
            """
            SELECT phase, COUNT(*) AS row_count
            FROM audit_inventory
            GROUP BY phase
            ORDER BY phase
            """
        ).fetchall()
    return {row["phase"]: row["row_count"] for row in rows}
