"""audit_service unit tests."""
import os
import sqlite3
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def set_db(test_db_path):
    os.environ["HUB_DB_PATH"] = test_db_path


@pytest.fixture()
def audit_db_path(test_db_path):
    path = Path(test_db_path).with_name("pipeline_working.db")
    if path.exists():
        path.unlink()
    yield path
    if path.exists():
        path.unlink()


def test_run_migrations_creates_audit_inventory_in_pipeline_working_db(audit_db_path, test_db_path):
    from hub.services.audit_service import run_migrations

    run_migrations()
    run_migrations()

    con = sqlite3.connect(str(audit_db_path))
    row = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='audit_inventory'"
    ).fetchone()
    con.close()

    assert row is not None
    ddl = row[0]
    assert ddl.startswith("CREATE TABLE audit_inventory")
    assert "PRIMARY KEY (phase, component)" in ddl
    assert "CHECK (trim(evidence) != '')" in ddl

    source_con = sqlite3.connect(test_db_path)
    source_row = source_con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_inventory'"
    ).fetchone()
    source_con.close()
    assert source_row is None


def test_upsert_audit_updates_existing_phase_component(audit_db_path):
    from hub.services.audit_service import query_audit, upsert_audit

    upsert_audit(
        "PH-T0-000",
        "audit_service",
        status="todo",
        evidence="red test written",
        verified_by="hubcoder",
    )
    upsert_audit(
        "PH-T0-000",
        "audit_service",
        file_path="hub/services/audit_service.py",
        symbol="upsert_audit",
        line_range="1-80",
        status="done",
        evidence="green test passed",
        owner_hint="hubcoder",
        notes="implemented pipeline_working db support",
        verified_by="hubcoder",
    )

    rows = query_audit()

    assert len(rows) == 1
    assert rows[0]["phase"] == "PH-T0-000"
    assert rows[0]["component"] == "audit_service"
    assert rows[0]["status"] == "done"
    assert rows[0]["file_path"] == "hub/services/audit_service.py"
    assert rows[0]["symbol"] == "upsert_audit"
    assert rows[0]["line_range"] == "1-80"
    assert rows[0]["evidence"] == "green test passed"
    assert rows[0]["owner_hint"] == "hubcoder"
    assert rows[0]["notes"] == "implemented pipeline_working db support"
    assert rows[0]["verified_by"] == "hubcoder"


@pytest.mark.parametrize("bad_evidence", [None, "", "   "])
def test_upsert_audit_rejects_blank_evidence(bad_evidence):
    from hub.services.audit_service import upsert_audit

    with pytest.raises(ValueError):
        upsert_audit(
            "PH-T0-000",
            "audit_service",
            status="done",
            evidence=bad_evidence,
            verified_by="hubcoder",
        )


def test_query_audit_filters_and_count_by_phase(audit_db_path):
    from hub.services.audit_service import count_by_phase, query_audit, upsert_audit

    upsert_audit(
        "PH-T0-001",
        "ddl",
        status="done",
        evidence="ddl synced",
        verified_by="hubcoder",
    )
    upsert_audit(
        "PH-T0-001",
        "tests",
        status="done",
        evidence="pytest green",
        verified_by="hubcoder",
    )
    upsert_audit(
        "PH-T0-002",
        "docs",
        status="todo",
        evidence="waiting for readme sync",
        verified_by="hubcoder",
    )

    done_rows = query_audit(phase="PH-T0-001", status="done")

    assert [row["component"] for row in done_rows] == ["ddl", "tests"]
    assert count_by_phase() == {"PH-T0-001": 2, "PH-T0-002": 1}
