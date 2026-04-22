from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from hub.services.forbidden_terms_schema import (
    ForbiddenScope,
    ForbiddenSeverity,
    ForbiddenTermRecord,
)
from hub.services.register_service import MARKET_TO_GROUP


VALID_PAYLOAD = {
    "term": "<placeholder-term>",
    "severity": "HIGH",
    "scope": "all",
    "reason": "Placeholder reason for operator-managed forbidden term.",
    "source": "operator_manual_seed",
    "added_at": "2026-04-22",
}


EXPECTED_SCOPE_VALUES = {"all", *MARKET_TO_GROUP.values()}
DOCS_DIR = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "tickets"
    / "phase5-forbidden-word"
)


def test_forbidden_term_record_parses_supported_enum_values():
    record = ForbiddenTermRecord(**VALID_PAYLOAD)

    assert record.severity is ForbiddenSeverity.HIGH
    assert record.scope is ForbiddenScope.ALL
    assert record.added_at == date(2026, 4, 22)


def test_forbidden_scope_values_follow_register_service_canonical_keys():
    scope_values = {scope.value for scope in ForbiddenScope}

    assert scope_values == EXPECTED_SCOPE_VALUES


def test_forbidden_term_record_rejects_unknown_severity():
    with pytest.raises(ValidationError) as excinfo:
        ForbiddenTermRecord(**{**VALID_PAYLOAD, "severity": "CRITICAL"})

    assert "severity" in str(excinfo.value)


def test_forbidden_term_record_rejects_unknown_scope():
    with pytest.raises(ValidationError) as excinfo:
        ForbiddenTermRecord(**{**VALID_PAYLOAD, "scope": "smartstore"})

    assert "scope" in str(excinfo.value)


@pytest.mark.parametrize(
    ("filename", "expected_terms"),
    [
        ("seed-dict.md", ["금지어", "scope", "naver", "st11", "coupang"]),
        (
            "user-explain-forbidden-word.md",
            ["운영자", "scope", "naver", "st11", "coupang"],
        ),
    ],
)
def test_phase5_forbidden_word_docs_use_korean_first_canonical_scope_keys(
    filename: str,
    expected_terms: list[str],
):
    content = (DOCS_DIR / filename).read_text(encoding="utf-8")

    for term in expected_terms:
        assert term in content
