"""Compact human-feedback memory for image prompt iteration.

This module keeps a small distilled memory from review outcomes so prompts
improve over time without growing endlessly.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
MEMORY_PATH = HERE / "prompt_feedback_memory.json"


ISSUE_RULES = {
    "[4분할]": "Enforce a single-scene single-angle photograph with no collage, grid, or inset layout.",
    "[사이즈오인]": "Make size and weight unmistakably real so the product never looks miniaturized or oversized.",
    "[연출없음]": "Show a consequential real-life use moment that explains why this product exists.",
    "[효과만]": "Depict believable physical interaction instead of abstract effect-only imagery.",
    "[라벨깨짐]": "Preserve package graphics and labels from the reference without inventing extra text.",
    "[인종]": "When a person appears, anchor the human subject to the Korean-market locale cues.",
    "[얼굴]": "Prefer hands and partial body unless a face is essential to the product story.",
    "[과묘사]": "Keep the scene documentary and plausible, not over-dramatized or fantasy-like.",
    "[품질낮음]": "Favor cleaner lighting, clearer subject separation, and more convincing photographic texture.",
}

SUCCESS_RULES = {
    "[연출좋음]": "Keep scenes that immediately communicate why the product is useful in daily life.",
    "[실사용감]": "Prefer lived-in in-use moments over catalog posing.",
    "[손자연]": "Keep hand poses and body interaction grounded and natural.",
    "[크기정확]": "Preserve realistic body-relative scale and weight cues.",
    "[브랜드보존]": "Keep product color, material, and package identity tightly aligned to the reference.",
    "[구도좋음]": "Use a clean square composition with one dominant focal action.",
}

DEFAULT_MEMORY = {
    "issue_counts": {},
    "success_counts": {},
    "recent_events": [],
    "recent_notes": [],
    "updated_at": "",
}

ISSUE_NOTE_HINTS = {
    ISSUE_RULES["[연출없음]"]: ["왜", "필요", "실사용", "연출", "순간", "in-use", "daily life"],
    ISSUE_RULES["[효과만]"]: ["효과만", "추상", "interaction", "상호작용"],
    ISSUE_RULES["[사이즈오인]"]: ["크기", "무게", "scale", "mini", "작아", "커보"],
    ISSUE_RULES["[라벨깨짐]"]: ["라벨", "텍스트", "브랜드", "package"],
}

SUCCESS_NOTE_HINTS = {
    SUCCESS_RULES["[실사용감]"]: ["실사용", "생활", "lived-in", "daily life"],
    SUCCESS_RULES["[손자연]"]: ["손", "자연", "hand", "pose", "interaction"],
    SUCCESS_RULES["[크기정확]"]: ["크기", "무게", "scale", "body-relative"],
    SUCCESS_RULES["[구도좋음]"]: ["구도", "focal", "composition", "framing"],
}


def _load() -> dict[str, Any]:
    if not MEMORY_PATH.exists():
        return json.loads(json.dumps(DEFAULT_MEMORY))
    try:
        data = json.loads(MEMORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return json.loads(json.dumps(DEFAULT_MEMORY))
    if not isinstance(data, dict):
        return json.loads(json.dumps(DEFAULT_MEMORY))
    merged = json.loads(json.dumps(DEFAULT_MEMORY))
    merged.update(data)
    return merged


def _save(data: dict[str, Any]) -> None:
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    MEMORY_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _bucket_key(product_class: str | None) -> str:
    return (product_class or "generic").strip() or "generic"


def _inc(store: dict[str, dict[str, int]], bucket: str, tag: str) -> None:
    bucket_data = store.setdefault(bucket, {})
    bucket_data[tag] = int(bucket_data.get(tag, 0)) + 1


def _append_event(data: dict[str, Any], event: dict[str, Any]) -> None:
    events = data.setdefault("recent_events", [])
    events.append(event)
    del events[:-30]


def _append_note(
    data: dict[str, Any],
    *,
    kind: str,
    tag: str,
    note: str,
    bucket: str,
    lane: str,
    code: str,
) -> None:
    cleaned = (note or "").strip()
    if not cleaned:
        return
    notes = data.setdefault("recent_notes", [])
    notes.append(
        {
            "kind": kind,
            "tag": tag,
            "note": cleaned[:240],
            "product_class": bucket,
            "lane": lane,
            "code": code,
            "ts": datetime.now().isoformat(timespec="seconds"),
        }
    )
    del notes[:-20]


def record_issue(
    tag: str,
    *,
    product_class: str | None = None,
    lane: str = "",
    code: str = "",
    note: str = "",
) -> None:
    if not tag:
        return
    data = _load()
    bucket = _bucket_key(product_class)
    _inc(data.setdefault("issue_counts", {}), "global", tag)
    _inc(data.setdefault("issue_counts", {}), bucket, tag)
    _append_event(
        data,
        {
            "kind": "issue",
            "tag": tag,
            "product_class": bucket,
            "lane": lane,
            "code": code,
            "ts": datetime.now().isoformat(timespec="seconds"),
        },
    )
    _append_note(data, kind="issue", tag=tag, note=note, bucket=bucket, lane=lane, code=code)
    _save(data)


def record_success(
    tag: str,
    *,
    product_class: str | None = None,
    lane: str = "",
    code: str = "",
    note: str = "",
) -> None:
    if not tag:
        return
    data = _load()
    bucket = _bucket_key(product_class)
    _inc(data.setdefault("success_counts", {}), "global", tag)
    _inc(data.setdefault("success_counts", {}), bucket, tag)
    _append_event(
        data,
        {
            "kind": "success",
            "tag": tag,
            "product_class": bucket,
            "lane": lane,
            "code": code,
            "ts": datetime.now().isoformat(timespec="seconds"),
        },
    )
    _append_note(data, kind="success", tag=tag, note=note, bucket=bucket, lane=lane, code=code)
    _save(data)


def _weighted_counts(data: dict[str, Any], key: str, product_class: str) -> dict[str, int]:
    source = data.get(key, {})
    counts: dict[str, int] = {}
    for tag, count in source.get("global", {}).items():
        counts[tag] = counts.get(tag, 0) + int(count)
    for tag, count in source.get(product_class, {}).items():
        counts[tag] = counts.get(tag, 0) + int(count) * 2
    return counts


def _top_rules(counts: dict[str, int], mapping: dict[str, str], *, min_count: int, limit: int) -> list[str]:
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    rules: list[str] = []
    for tag, count in ranked:
        if count < min_count:
            continue
        rule = mapping.get(tag)
        if not rule or rule in rules:
            continue
        rules.append(rule)
        if len(rules) >= limit:
            break
    return rules


def _note_rule_scores(
    notes: list[dict[str, Any]],
    product_class: str,
    *,
    kind: str,
) -> dict[str, int]:
    hint_map = ISSUE_NOTE_HINTS if kind == "issue" else SUCCESS_NOTE_HINTS
    scores: dict[str, int] = {}
    for item in notes:
        if item.get("kind") != kind:
            continue
        note_class = item.get("product_class") or "generic"
        if note_class not in ("generic", product_class):
            continue
        text = str(item.get("note", "")).strip().lower()
        if not text:
            continue
        weight = 2 if note_class == product_class else 1
        for rule, hints in hint_map.items():
            if any(hint.lower() in text for hint in hints):
                scores[rule] = scores.get(rule, 0) + weight
    return scores


def _rank_rules(
    counts: dict[str, int],
    mapping: dict[str, str],
    note_scores: dict[str, int],
    *,
    promote_at: int,
    candidate_at: int,
    limit: int,
) -> tuple[list[str], list[str]]:
    scored_rules: dict[str, int] = {}
    for tag, count in counts.items():
        rule = mapping.get(tag)
        if not rule:
            continue
        scored_rules[rule] = max(scored_rules.get(rule, 0), int(count))
    for rule, score in note_scores.items():
        scored_rules[rule] = scored_rules.get(rule, 0) + int(score)

    ranked = sorted(scored_rules.items(), key=lambda item: (-item[1], item[0]))
    promoted: list[str] = []
    candidate: list[str] = []
    for rule, score in ranked:
        if score >= promote_at and rule not in promoted:
            promoted.append(rule)
        elif score >= candidate_at and rule not in candidate:
            candidate.append(rule)
    return promoted[:limit], candidate[:limit]


def get_memory_snapshot(product_class: str | None = None) -> dict[str, Any]:
    data = _load()
    bucket = _bucket_key(product_class)
    issue_counts = _weighted_counts(data, "issue_counts", bucket)
    success_counts = _weighted_counts(data, "success_counts", bucket)
    recent_notes = list(data.get("recent_notes", []))[-8:]
    issue_note_scores = _note_rule_scores(recent_notes, bucket, kind="issue")
    success_note_scores = _note_rule_scores(recent_notes, bucket, kind="success")
    promoted_avoid_rules, candidate_avoid_rules = _rank_rules(
        issue_counts,
        ISSUE_RULES,
        issue_note_scores,
        promote_at=2,
        candidate_at=1,
        limit=3,
    )
    promoted_reinforce_rules, candidate_reinforce_rules = _rank_rules(
        success_counts,
        SUCCESS_RULES,
        success_note_scores,
        promote_at=2,
        candidate_at=1,
        limit=2,
    )
    return {
        "product_class": bucket,
        "updated_at": data.get("updated_at", ""),
        "avoid_rules": promoted_avoid_rules,
        "reinforce_rules": promoted_reinforce_rules,
        "promoted_avoid_rules": promoted_avoid_rules,
        "candidate_avoid_rules": candidate_avoid_rules,
        "promoted_reinforce_rules": promoted_reinforce_rules,
        "candidate_reinforce_rules": candidate_reinforce_rules,
        "issue_counts": issue_counts,
        "success_counts": success_counts,
        "recent_events": list(data.get("recent_events", []))[-8:],
        "recent_notes": recent_notes,
    }


def format_memory_lines(snapshot: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for rule in snapshot.get("promoted_reinforce_rules", []):
        lines.append(f"Reinforce: {rule}")
    for rule in snapshot.get("promoted_avoid_rules", []):
        lines.append(f"Avoid: {rule}")
    if len(lines) < 3:
        for rule in snapshot.get("candidate_reinforce_rules", []):
            candidate_line = f"Watch: {rule}"
            if candidate_line not in lines:
                lines.append(candidate_line)
            if len(lines) >= 3:
                break
    if len(lines) < 4:
        for rule in snapshot.get("candidate_avoid_rules", []):
            candidate_line = f"Watch: {rule}"
            if candidate_line not in lines:
                lines.append(candidate_line)
            if len(lines) >= 4:
                break
    return lines[:5]
