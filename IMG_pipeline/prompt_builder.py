"""ST2_JSON → 연출형 이미지 생성 프롬프트 빌더 (v6).

v6 개선 (PoC 2026-04-20 3차 테스트 교훈):
  1. Locale 기본값 "South Korea" 자동 주입 (Gemini 서양인 기본값 교정)
  2. 콘센트/전기 상품 → 한국 220V 매립형 소켓 locale cue 강제
  3. 연출 기본 = 손/부분만 (show_face=False 기본)
  4. 출산/육아 카테고리 → 성인 보호자로 치환 + No minors 제약 강제
  5. "오픈마켓 한국 상품" 맥락 자동 반영

v5 유지:
  - 테이프/롤/스틱 스케일 강제, apparel body-scale, active use, texture 타입별
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from IMG_pipeline.prompt_feedback import format_memory_lines, get_memory_snapshot


# ── 규칙 yaml 로드 ─────────────────────────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
_WORKTREE = _HERE.parent
_SKILL_DIR = _WORKTREE / ".claude" / "skills" / "image-genai-prompt-writer"
# worktree/.claude/skills/... → 실제 위치: 프로젝트/.claude/skills/... (worktree가 projectroot/.claude/worktrees/<wt>/)
# walk up ancestors until we find .claude/skills/image-genai-prompt-writer
def _find_skill_dir() -> Path:
    cur = _HERE
    for _ in range(6):
        cand = cur / ".claude" / "skills" / "image-genai-prompt-writer"
        if cand.is_dir():
            return cand
        cur = cur.parent
    return _HERE.parent.parent.parent.parent / ".claude" / "skills" / "image-genai-prompt-writer"

SKILL_DIR = _find_skill_dir()
RULES_FILE = SKILL_DIR / "rules" / "v7.active.yaml"

def _load_rules() -> dict:
    if RULES_FILE.exists():
        try:
            with RULES_FILE.open(encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"[prompt_builder] rules 로드 실패, fallback: {e}")
    return {}

_RULES = _load_rules()


# ── 유틸 ───────────────────────────────────────────────────────────────────────

def _first(items: list | None, default: str = "") -> str:
    if not items:
        return default
    return str(items[0]) if items[0] else default


def _join(items: list | None, max_n: int = 3, sep: str = ", ") -> str:
    if not items:
        return ""
    return sep.join(str(x) for x in items[:max_n] if x)


# ── 상품 타입 분류 (yaml 기반) ──────────────────────────────────────────────────

# 우선순위: outlet > roll > stick > glove > apparel > sack > small_hardware > tool > cosmetic
_TYPE_ORDER = ["outlet", "roll", "stick", "glove", "apparel",
               "sack", "small_hardware", "tool", "cosmetic"]

def _type_keywords() -> dict[str, list[str]]:
    return _RULES.get("product_type_keywords", {})

def _classify_product(core: dict, meta: dict) -> str:
    product_type = (core.get("상품타입") or "").lower()
    parent_cat = (core.get("상위카테고리") or "").lower()
    cat_path = (meta.get("카테고리_경로") or "").lower()
    blob = f"{product_type} {parent_cat} {cat_path}"
    kw_map = _type_keywords()
    for t in _TYPE_ORDER:
        keys = kw_map.get(t) or []
        if any(k.lower() in blob for k in keys):
            return t
    return "generic"


# ── 타입별 매핑 (yaml 기반) ────────────────────────────────────────────────────

_SCALE_BY_TYPE = _RULES.get("scale_by_type", {})

_TEXTURE_BY_TYPE = _RULES.get("texture_by_type", {})

# 타입이 얼굴을 보여주는 게 자연스러운지
_FACE_NATURAL = set(_RULES.get("face_policy", {}).get("show_face_for_classes",
                                                       ["apparel", "cosmetic"]))

# 타입별 locale prop cue
_LOCALE_PROP_CUE = _RULES.get("locale_prop_cues", {})


# ── 카테고리 기반 미성년자 감지 ────────────────────────────────────────────────

_fp = _RULES.get("face_policy", {})
_MINOR_CAT_KEYWORDS = tuple(_fp.get("child_category_keywords",
    ["출산/육아", "유아", "아동", "영유아", "키즈", "baby", "kids"]))
_MINOR_USER_KEYWORDS = tuple(_fp.get("child_user_keywords",
    ["영유아", "아동", "어린이", "유아", "아기", "kids", "child", "infant"]))
_MINOR_REPLACEMENT = _fp.get("child_replacement_user", "a Korean adult parent or caregiver")


def _involves_minors(meta: dict, user: str) -> bool:
    cat_path = meta.get("카테고리_경로", "") or ""
    if any(k in cat_path for k in _MINOR_CAT_KEYWORDS):
        return True
    if user and any(k in user for k in _MINOR_USER_KEYWORDS):
        return True
    return False


# ── 휴먼스케일 fallback ────────────────────────────────────────────────────────

_SIZE_RX_CM = re.compile(r"(\d+(?:\.\d+)?)\s*(?:cm|센티)", re.I)
_SIZE_RX_MM = re.compile(r"(\d+(?:\.\d+)?)\s*(?:mm|밀리)", re.I)
_SIZE_RX_KG = re.compile(r"(\d+(?:\.\d+)?)\s*kg", re.I)


def _size_based_scale(size: dict | None) -> str:
    if not size:
        return ""
    txt = " ".join(str(v) for v in size.values() if v)
    if not txt:
        return ""

    widths = []
    for v, unit in re.findall(r"폭\s*(\d+(?:\.\d+)?)\s*(mm|cm)", txt):
        widths.append(float(v) / 10 if unit == "mm" else float(v))
    if widths:
        w = max(widths)
        if w < 10:
            return "compact, palm-sized, held between thumb and forefinger"
        if w < 30:
            return "hand-sized, held in one hand"
        if w < 80:
            return "held in two hands, forearm-length"

    lengths_cm = []
    for m in _SIZE_RX_CM.finditer(txt):
        lengths_cm.append(float(m.group(1)))
    for m in _SIZE_RX_MM.finditer(txt):
        lengths_cm.append(float(m.group(1)) / 10)
    if lengths_cm:
        mx = max(lengths_cm)
        if mx < 10:
            return "compact, palm-sized, held between thumb and forefinger"
        if mx < 30:
            return "hand-sized, held in one hand"
        if mx < 80:
            return "held in two hands, forearm-length"
        if mx < 150:
            return "torso-sized, standing roughly at chest height"
        return ""

    # 무게 기반 (액체/가루/세트 상품)
    kg = _SIZE_RX_KG.search(txt)
    if kg:
        val = float(kg.group(1))
        if val >= 20:
            return ("a heavy bulk item (approximately {v}kg) that must be handled with BOTH hands "
                    "or set down on the floor/ground; never shown held casually in one hand".format(v=int(val)))
        if val >= 5:
            return ("a medium-heavy container (approximately {v}kg); if held, must be held firmly "
                    "with both hands or supported against the body, never dangling one-handed".format(v=int(val)))
        if val >= 1:
            return "a hand-held container, around {v}kg, held securely".format(v=int(val))
        return "a hand-held small item"

    return ""


def _resolve_scale(ptype: str, core: dict) -> str:
    # 무게 우선: 5kg 이상이면 타입 분류 무시하고 무게 기반 스케일 강제
    size = core.get("사이즈") or {}
    size_txt = " ".join(str(v) for v in size.values() if v)
    kg_match = _SIZE_RX_KG.search(size_txt)
    if kg_match:
        val = float(kg_match.group(1))
        if val >= 5:
            return _size_based_scale(size)  # 무게 기반 문구 반환
    # 그 외엔 타입 매핑 우선, 없으면 치수 기반
    if ptype in _SCALE_BY_TYPE:
        return _SCALE_BY_TYPE[ptype]
    return _size_based_scale(size)


# ── 옵션/세트 표현 ────────────────────────────────────────────────────────────

def _variants_cue(core: dict) -> str:
    colors = core.get("색상/옵션_리스트") or []
    set_comp = (core.get("세트구성") or "").strip()
    cue = ""
    if len(colors) >= 2:
        cue = f"multiple color/option variants visible together ({len(colors)} types)"
    if set_comp and set_comp not in ("단품", "단품형"):
        cue = f"{cue}; set composition: {set_comp}" if cue else f"set composition: {set_comp}"
    return cue


# ── SCENE 슬롯 추출 ────────────────────────────────────────────────────────────

def _extract_slots(st2: dict, locale: str = "South Korea") -> dict:
    meta = st2.get("meta", {}) or {}
    core = st2.get("core_attributes", {}) or {}
    naming = st2.get("naming_seeds", {}) or {}
    scenarios = st2.get("usage_scenarios") or []

    ptype = _classify_product(core, meta)
    raw_user = _join(core.get("사용대상"), 2)
    minors = _involves_minors(meta, raw_user)

    # 미성년자 관련 → 성인 보호자로 치환
    if minors:
        user = "a Korean adult parent or caregiver"
    else:
        user = raw_user

    # 얼굴 기본값: apparel/cosmetic 만 True, 나머지 False
    show_face = ptype in _FACE_NATURAL

    return {
        "product_type_class": ptype,
        "primary_action": _first(scenarios, "상품이 실사용되는 자연스러운 장면"),
        "environment": _join(naming.get("상황/장소/계절"), 2),
        "user": user,
        "raw_user_had_minors": minors,
        "mood": _join(naming.get("스타일형용사"), 3) or "실용적이고 사실적인",
        "usp": _first(naming.get("차별화포인트")),
        "effect": _first(naming.get("기능/효과표현")),
        "scale": _resolve_scale(ptype, core),
        "texture": _TEXTURE_BY_TYPE.get(ptype, _TEXTURE_BY_TYPE["generic"]),
        "variants": _variants_cue(core),
        "locale": locale,
        "show_face": show_face,
        "locale_prop_cue": _LOCALE_PROP_CUE.get(ptype, ""),
        "product_name": meta.get("기본상품명", ""),
        "main_use": _first(core.get("주요용도")),
        "category_path": meta.get("카테고리_경로", ""),
    }


# ── 템플릿 빌더 ────────────────────────────────────────────────────────────────

_LOCALE_ADJECTIVE = {
    "South Korea": "Korean",
    "Korea": "Korean",
    "한국": "Korean",
    "Japan": "Japanese",
    "United States": "American",
    "USA": "American",
    "China": "Chinese",
    "Taiwan": "Taiwanese",
}


def _framing_phrase(show_face: bool, locale: str) -> str:
    adj = _LOCALE_ADJECTIVE.get(locale, locale)
    if show_face:
        return (
            f"The scene features a {adj} adult as the user; "
            f"the face is allowed but must look authentically {adj}."
        )
    return (
        "The frame focuses on hands and partial body interacting with the product; "
        "no faces are shown."
    )


def _memory_lines_for_class(product_class: str) -> list[str]:
    snapshot = get_memory_snapshot(product_class)
    return format_memory_lines(snapshot)


def build_gemini_prompt(st2: dict, locale: str = "South Korea") -> str:
    s = _extract_slots(st2, locale)
    memory_lines = _memory_lines_for_class(s["product_type_class"])

    pieces = [
        f"ONE photorealistic documentary photograph (a single scene, single camera angle — "
        f"NOT a collage, grid, or multi-panel composition) showing {s['primary_action']}"
    ]
    if s["environment"]:
        pieces.append(f" The setting is {s['environment']}, shot in {s['locale']}.")
    else:
        pieces.append(f" Shot in {s['locale']}.")

    pieces.append(" " + _framing_phrase(s["show_face"], s["locale"]))

    if s["user"]:
        pieces.append(
            f" {s['user']} is ACTIVELY USING the product right now — this is a real moment of use, "
            f"not a catalog pose or a staged product-on-white shot."
        )
    else:
        pieces.append(
            " A real person is ACTIVELY USING the product right now — a real moment of use, "
            "not a catalog pose."
        )

    pieces.append(
        " The product's color, material, and packaging labels match the attached reference image exactly. "
        "Do not invent captions, brand marks, slogans, watermarks, or signage anywhere in the frame; "
        "any dense label text on the product should be partially obscured or at an angle rather than "
        "sharply legible."
    )

    if s["locale_prop_cue"]:
        pieces.append(f" {s['locale_prop_cue']}.")

    if s["usp"]:
        pieces.append(f" The strength — {s['usp']} — is visible through the user's interaction.")

    if s["scale"]:
        pieces.append(
            f" TRUE-TO-LIFE SCALE: {s['scale']}. A heavy or bulky product must look heavy; "
            f"do not shrink it to a convenient hand-held size."
        )

    if s["variants"]:
        pieces.append(f" Composition note: {s['variants']}.")

    pieces.append(
        f" Captured on 35mm film with a 50mm lens at f/2.8, soft natural daylight, subtle grain. "
        f"Texture: {s['texture']}."
    )

    pieces.append(f" Overall mood: {s['mood']}.")
    pieces.append(
        " 1:1 square, single frame, the product clearly the focal point at 35-55% of the frame. "
        "Do not apply effects, overlays, or stylization to the reference — generate a fresh scene."
    )

    if memory_lines:
        pieces.append(" Human-review priorities: " + " ".join(memory_lines[:4]))

    return "".join(pieces).strip()


def build_gpt_prompt(st2: dict, locale: str = "South Korea") -> str:
    s = _extract_slots(st2, locale)
    memory_lines = _memory_lines_for_class(s["product_type_class"])

    bg = [
        s["environment"] or "자연광이 있는 실제 사용 환경",
        f"Shot in {s['locale']} — local atmosphere; any signage in Hangul only.",
    ]
    if s["locale_prop_cue"]:
        bg.append(s["locale_prop_cue"] + ".")

    subj = [_framing_phrase(s["show_face"], s["locale"])]
    if s["user"]:
        subj.append(f"{s['user']} is ACTIVELY USING the product RIGHT NOW — {s['primary_action']}")
    else:
        subj.append(f"A real person is ACTIVELY USING the product RIGHT NOW — {s['primary_action']}")

    details = [
        "- Product appearance, color, material, and ALL label graphics match the attached reference image.",
        "- Show the product IN USE (held, applied, worn, or operated) — never floating, centered, or staged.",
    ]
    if s["usp"]:
        details.append(f"- Emphasize via the user's interaction: {s['usp']}")
    if s["effect"] and s["effect"] != s["usp"]:
        details.append(f"- Functional effect visible: {s['effect']}")
    if s["scale"]:
        details.append(f"- True-to-life scale: {s['scale']}")
    if s["variants"]:
        details.append(f"- Variants: {s['variants']} (all visible together as separate units)")
    details += [
        "- Camera: 50mm lens, f/2.8 shallow depth of field, subtle film grain",
        "- Lighting: soft natural daylight",
        f"- Texture: {s['texture']}",
        f"- Mood: {s['mood']}",
    ]

    # v7: 핵심 HARD RULES 4가지로 압축
    constraints = [
        "=== HARD RULES (non-negotiable) ===",
        "1. SINGLE SCENE, SINGLE ANGLE. One photograph. No collage, no grid, no multi-panel, "
        "no before/after, no step-by-step sequence, no insets.",
        "2. LIFESTYLE / IN-USE ONLY. Must depict a real moment of actual use. "
        "NOT a white-background catalog shot, NOT an effect-only edit of the reference, "
        "NOT an isolated product photo.",
        "3. SIZE & WEIGHT MUST MATCH REALITY. Respect the scale/weight cue above. "
        "A heavy or bulky product MUST look heavy (gripped with both hands, supported, "
        "or resting on a surface). Do not shrink a large product to hand-carry size.",
        "4. NO AI-INVENTED TEXT. Keep the product's packaging labels as they appear in the reference "
        "(angled or partially obscured so dense Korean text is not rendered sharply). "
        "Do NOT add captions, brand badges, slogans, watermarks, or extra signs anywhere.",
        "",
        "- 1:1 square, photo-realistic (real DSLR documentary photograph).",
        "- No cartoon / illustration / CGI / rendered look.",
        "- Do not recolor or redesign the product.",
    ]
    if s["raw_user_had_minors"]:
        constraints.append("- No children, minors, or minors' faces in the frame — only adult hands/body visible")
    if not s["show_face"]:
        constraints.append("- No full human faces in the frame — show hands / partial body only")
    if memory_lines:
        constraints.append("")
        constraints.append("LEARNED PRIORITIES FROM HUMAN REVIEW:")
        constraints.extend(f"- {line}" for line in memory_lines[:4])

    return (
        "BACKGROUND:\n" + "\n".join(bg)
        + "\n\nSUBJECT:\n" + "\n".join(subj)
        + "\n\nDETAILS:\n" + "\n".join(details)
        + "\n\nCONSTRAINTS:\n" + "\n".join(constraints)
    ).strip()


# ── 통합 API ───────────────────────────────────────────────────────────────────

def build_image_prompts(st2: dict[str, Any], locale: str = "South Korea") -> dict[str, str]:
    s = _extract_slots(st2, locale)
    memory_snapshot = get_memory_snapshot(s["product_type_class"])
    memory_lines = format_memory_lines(memory_snapshot)
    alt_parts = [p for p in [s["product_name"], s["main_use"] or s["environment"]] if p]
    alt_caption = " - ".join(alt_parts) if alt_parts else s["product_name"]
    why_parts = [part for part in [s["primary_action"], s["usp"], s["effect"], s["main_use"]] if part]
    why_this_image = " | ".join(dict.fromkeys(why_parts))

    return {
        "gemini_prompt": build_gemini_prompt(st2, locale),
        "gpt_prompt": build_gpt_prompt(st2, locale),
        "alt_caption": alt_caption,
        "_why_this_image": why_this_image,
        "_memory_lines": memory_lines,
        "_memory_updated_at": memory_snapshot.get("updated_at", ""),
        "_product_class": s["product_type_class"],
        "_show_face": s["show_face"],
        "_involves_minors": s["raw_user_had_minors"],
    }


def build_image_prompt(st2: dict[str, Any]) -> dict[str, str]:
    out = build_image_prompts(st2)
    return {"scene_prompt": out["gemini_prompt"], "alt_caption": out["alt_caption"]}
