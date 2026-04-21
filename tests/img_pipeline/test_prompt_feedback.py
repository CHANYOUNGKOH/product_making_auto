from pathlib import Path

from IMG_pipeline import prompt_feedback
from IMG_pipeline.prompt_builder import build_image_prompts


def _sample_st2():
    return {
        "meta": {
            "기본상품명": "테스트 상품",
            "카테고리_경로": "생활/건강 > 청소",
        },
        "core_attributes": {
            "상품타입": "generic",
            "주요용도": ["청소"],
            "사용대상": ["Korean adult"],
            "사이즈": {"width": "20cm"},
        },
        "naming_seeds": {
            "상황/장소/계절": ["a lived-in Korean home"],
            "스타일형용사": ["practical", "authentic"],
            "차별화포인트": ["easy everyday cleaning"],
            "기능/효과표현": ["clear utility"],
        },
        "usage_scenarios": ["an adult using the product during routine cleaning"],
    }


def test_prompt_feedback_distills_repeated_issue_tags(tmp_path, monkeypatch):
    memory_path = tmp_path / "prompt_feedback_memory.json"
    monkeypatch.setattr(prompt_feedback, "MEMORY_PATH", memory_path)

    prompt_feedback.record_issue("[연출없음]", product_class="generic", lane="codex", code="W001")
    prompt_feedback.record_issue("[연출없음]", product_class="generic", lane="web", code="W002")

    snapshot = prompt_feedback.get_memory_snapshot("generic")

    assert snapshot["avoid_rules"]
    assert any("why this product exists" in line.lower() for line in snapshot["avoid_rules"])


def test_prompt_feedback_promotes_structured_review_notes(tmp_path, monkeypatch):
    memory_path = tmp_path / "prompt_feedback_memory.json"
    monkeypatch.setattr(prompt_feedback, "MEMORY_PATH", memory_path)

    prompt_feedback.record_issue(
        "[연출없음]",
        product_class="generic",
        lane="codex",
        code="W001",
        note="왜 이 제품이 필요한지 보이는 실제 사용 장면이 약함",
    )
    prompt_feedback.record_issue(
        "[연출없음]",
        product_class="generic",
        lane="web",
        code="W002",
        note="실사용 순간보다 연출 의도가 약해서 왜 필요한지 한눈에 안 보임",
    )
    prompt_feedback.record_success(
        "[손자연]",
        product_class="generic",
        lane="web",
        code="W003",
        note="손 포즈가 자연스럽고 실제 사용처럼 느껴짐",
    )
    prompt_feedback.record_success(
        "[손자연]",
        product_class="generic",
        lane="codex",
        code="W004",
        note="손과 제품 상호작용이 자연스러워 신뢰감이 있음",
    )

    snapshot = prompt_feedback.get_memory_snapshot("generic")

    assert snapshot["promoted_avoid_rules"]
    assert snapshot["promoted_reinforce_rules"]
    assert snapshot["recent_notes"]
    assert any("why this product exists" in line.lower() for line in snapshot["promoted_avoid_rules"])
    assert any("natural" in line.lower() for line in snapshot["promoted_reinforce_rules"])


def test_build_image_prompts_includes_distilled_memory(tmp_path, monkeypatch):
    memory_path = tmp_path / "prompt_feedback_memory.json"
    monkeypatch.setattr(prompt_feedback, "MEMORY_PATH", memory_path)

    prompt_feedback.record_issue("[연출없음]", product_class="generic", lane="codex", code="W001")
    prompt_feedback.record_issue("[연출없음]", product_class="generic", lane="web", code="W002")
    prompt_feedback.record_success("[실사용감]", product_class="generic", lane="web", code="W003")
    prompt_feedback.record_success("[실사용감]", product_class="generic", lane="codex", code="W004")

    prompts = build_image_prompts(_sample_st2())

    assert prompts["_memory_lines"]
    assert "LEARNED PRIORITIES FROM HUMAN REVIEW" in prompts["gpt_prompt"]
    assert "Human-review priorities:" in prompts["gemini_prompt"]
    assert "Why this image" not in prompts["gpt_prompt"]


def test_format_memory_lines_prefers_promoted_rules_over_raw_notes(tmp_path, monkeypatch):
    memory_path = tmp_path / "prompt_feedback_memory.json"
    monkeypatch.setattr(prompt_feedback, "MEMORY_PATH", memory_path)

    prompt_feedback.record_issue(
        "[연출없음]",
        product_class="generic",
        lane="codex",
        code="W001",
        note="이 제품을 왜 쓰는지 한 번에 보였으면 좋겠음",
    )
    prompt_feedback.record_issue(
        "[연출없음]",
        product_class="generic",
        lane="web",
        code="W002",
        note="왜 필요한지 설명되는 실사용 컷이 필요함",
    )

    lines = prompt_feedback.format_memory_lines(prompt_feedback.get_memory_snapshot("generic"))

    assert lines
    assert any("Avoid:" in line for line in lines)
    assert not any("한 번에 보였으면" in line for line in lines)
