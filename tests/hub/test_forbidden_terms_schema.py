from datetime import date

import pytest
from pydantic import ValidationError

from hub.services.forbidden_terms_schema import (
    ForbiddenScope,
    ForbiddenSeverity,
    ForbiddenTermRecord,
)


VALID_PAYLOAD = {
    "term": "<placeholder-term>",
    "severity": "HIGH",
    "scope": "all",
    "reason": "Placeholder reason for operator-managed forbidden term.",
    "source": "operator_manual_seed",
    "added_at": "2026-04-22",
}


def test_forbidden_term_record_parses_supported_enum_values():
    record = ForbiddenTermRecord(**VALID_PAYLOAD)

    assert record.severity is ForbiddenSeverity.HIGH
    assert record.scope is ForbiddenScope.ALL
    assert record.added_at == date(2026, 4, 22)


def test_forbidden_term_record_rejects_unknown_severity():
    with pytest.raises(ValidationError) as excinfo:
        ForbiddenTermRecord(**{**VALID_PAYLOAD, "severity": "CRITICAL"})

    assert "severity" in str(excinfo.value)


def test_forbidden_term_record_rejects_unknown_scope():
    with pytest.raises(ValidationError) as excinfo:
        ForbiddenTermRecord(**{**VALID_PAYLOAD, "scope": "coupang"})

    assert "scope" in str(excinfo.value)
