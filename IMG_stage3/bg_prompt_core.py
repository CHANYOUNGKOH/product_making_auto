"""
bg_prompt_core.py

Stage BG_PROMPT: ST2_JSON(한국어) → 배경/영상 프롬프트 JSON(영문) 생성 코어 모듈.

- OpenAI API(gpt-5-mini 등)를 호출해서
  bg_positive_en / bg_negative_en / video_motion_prompt_en / video_full_prompt_en
  4개 필드를 가진 JSON을 반환한다.
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, Optional, Union

from openai import OpenAI

# === 기본 설정 ===

# 필요하면 원하는 파일명으로 변경해서 쓰면 됨
API_KEY_FILE = ".openai_api_key_bg_prompt"

# 실제 사용 시, OpenAI 대시보드에서 확인한 모델명으로 교체
DEFAULT_MODEL = "gpt-5-mini"  # 예: "gpt-4.1-mini" 등으로 수정 가능

# 모델별 가격 (USD per Million Tokens)
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5": {"input": 1.25, "output": 10.0},
    "gpt-5-mini": {"input": 0.25, "output": 2.00},
    "gpt-5-nano": {"input": 0.05, "output": 0.40},
}


# === API Key 유틸 ===

def load_api_key_from_file(path: str = API_KEY_FILE) -> Optional[str]:
    """텍스트 파일에서 API 키를 읽는다. 없으면 None."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            key = f.read().strip()
        return key or None
    except Exception:
        return None


def save_api_key_to_file(api_key: str, path: str = API_KEY_FILE) -> None:
    """API 키를 텍스트 파일에 저장한다."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(api_key.strip())


def get_openai_client(api_key: Optional[str] = None) -> OpenAI:
    """
    OpenAI 클라이언트를 생성한다.
    - api_key가 None이면 환경변수 OPENAI_API_KEY를 사용.
    """
    if api_key is None:
        # 파일에 저장된 키가 있으면 우선 사용
        file_key = load_api_key_from_file()
        if file_key:
            api_key = file_key
    if api_key is None:
        # 마지막으로 환경변수
        api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        raise RuntimeError("OpenAI API 키가 설정되지 않았습니다. "
                           "API_KEY_FILE 또는 환경변수 OPENAI_API_KEY를 확인하세요.")

    return OpenAI(api_key=api_key)


# === System Prompt (최종 버전) ===
# 기존프롬포트 - 안정성 위주
# BG_SYSTEM_PROMPT = r"""
# You are the AI Art Director for an e-commerce automation pipeline.

# TASK:

# Combine ST2_JSON (context) and IMG_ANALYSIS_JSON (visual structure) and generate 4 English prompts.

# OUTPUT FORMAT:

# Return exactly one JSON object (no markdown):

# {
#   "bg_positive_en": "...",
#   "bg_negative_en": "...",
#   "video_motion_prompt_en": "...",
#   "video_full_prompt_en": "..."
# }

# ----------------------------------------------------
# CORE PRINCIPLES (STRICT)
# ----------------------------------------------------

# 1) Structure Invariance (Bone)
#    - The base structure MUST come from IMG_ANALYSIS_JSON.bg_layout_hint_en.
#    - Do NOT alter the physical objects or perspective described in the hint.

# 2) Context Overlay (Skin)
#    - Add only material/place adjectives from ST2_JSON.
#    - Do NOT introduce new objects or change surface type.

# 3) Negative Space
#    - NEVER describe the product. Only empty environment, surface, lighting, and mood.

# ----------------------------------------------------
# FIELD RULES
# ----------------------------------------------------

# 1) bg_positive_en
#    - MUST start with: "Empty"
#    - MUST follow this simplified structure for maximum stability:
#      "Empty [bg_layout_hint_en adjusted with minimal context], [perspective keywords], [empty area phrase], [lighting_condition], product photography background, depth of field, 8k, high resolution, masterpiece"

#    - Perspective keywords (add all for the matching type):
#      * top_down:
#          "flat lay view", "top down perspective", "single surface only, no walls"
#      * front_view:
#          "eye level shot", "front view", "surface with wall background"
#      * angled:
#          "angled perspective", "isometric depth"
#      * side_view:
#          "side profile view", "elongated surface"

#    - Empty area phrase must be simple and fixed:
#      "clear empty center area for the product"

#    - Context adjectives must NOT create new objects.
#      Examples: "wooden", "marble", "studio-like", "clean indoor".

# 2) bg_negative_en
#    - Output ONLY comma-separated **single-word nouns**.
#    - Base list:
#      text, watermark, logo, brand, human, person, face, hands, mannequin, pets, clutter, dirt, distortion, noise, blur, lowquality
#    - Dynamic Injection:
#      - Extract product nouns from ST2_JSON.
#      - Translate them into ONE English noun each (e.g., "basket", "fan", "shoe").
#      - Add them to the list.
#      - NO compound nouns (e.g., NOT "storage basket").

# 3) video_motion_prompt_en
#    - If is_flat_lay == true:
#        "Static top-down camera with almost no movement or very subtle slow rotation, smooth 5 second loop, no camera shake"
#    - Else:
#        "Static camera with very subtle slow zoom in matching the perspective, smooth 5 second loop, no camera shake"

# 4) video_full_prompt_en
#    - Construct a NEW sentence:
#      - Include: one perspective keyword + lighting_condition + the empty area phrase.
#      - Then append the motion prompt.
#    - End with:
#      "high quality 5 second loop video, no people, no text, no logo, no morphing, no distortion"

# ----------------------------------------------------
# FINAL CHECK
# ----------------------------------------------------

# - Output valid JSON only.
# - All values must be English.
# - bg_positive_en MUST start with "Empty".
# """

# 새프롬포트 - 중앙물건제거 및 토큰절약 - 마룻바닥위주로 생성, 다채로움이 사라짐
# BG_SYSTEM_PROMPT = r"""
# You are the AI Art Director for an e-commerce automation pipeline.

# TASK:
# Combine ST2_JSON and IMG_ANALYSIS_JSON to generate 4 English prompts.
# Output must be a single JSON object.

# FORMAT:
# {
#   "bg_positive_en": "...",
#   "bg_negative_en": "...",
#   "video_motion_prompt_en": "...",
#   "video_full_prompt_en": "..."
# }

# ----------------------------------------------------
# CORE RULES
# ----------------------------------------------------

# 1) Structural Consistency
# - Follow the surface orientation and perspective from IMG_ANALYSIS_JSON.bg_layout_hint_en.

# 2) Surface / Background Separation
# - Surface: realistic material texture (wood, marble, fabric) but completely empty.
# - Background: only soft blurred environment mood, no identifiable shapes.

# 3) Protected Empty Zone
# - The center area must remain fully unobstructed for product compositing.
# - No objects or object-like silhouettes may appear on the surface.

# 4) Realistic Minimalism
# - Allow natural material texture and soft ambient shading.
# - Forbid props or physical items.

# ----------------------------------------------------
# FIELD RULES
# ----------------------------------------------------

# 1) bg_positive_en
# Must start with "Empty".

# Structure:
# "Empty [photography style],
# clean realistic [material texture] surface [bg_layout_hint_en],
# [perspective keywords],
# large empty center area for product placement,
# soft blurred background without recognizable objects,
# [lighting_condition], [color_tone], depth of field, high resolution"

# Photography styles (select ONE based on ST2_JSON):
# - nature lifestyle photography
# - cozy lifestyle photography
# - bright commercial photography
# - sleek commercial photography
# - elegant editorial photography
# - professional commercial photography

# Perspective keywords:
# - flat lay view / top-down perspective / single surface
# - eye-level shot / front view
# - angled perspective
# - side profile view

# 2) bg_negative_en
# Single-word nouns only:
# text, watermark, logo, brand,
# human, person, face, hands, mannequin,
# object, item, prop, decoration, furniture, tool,
# clutter, distortion, noise, lowquality,
# studio, backdrop
# + product-related nouns from ST2_JSON

# 3) video_motion_prompt_en
# - flat lay: "Static top-down camera, no movement, smooth 5s loop"
# - others: "Static camera, subtle slow zoom, smooth 5s loop"

# 4) video_full_prompt_en
# - Mention perspective, lighting, empty center area, and motion.
# - End with: "high quality 5s loop, no people, no text, no logo"
# """

# 다채로움 상승 토큰절약적용
BG_SYSTEM_PROMPT = r"""
You are the AI Art Director for an e-commerce background generation pipeline.

TASK:
Combine ST2_JSON (product context) and IMG_ANALYSIS_JSON (layout analysis)
to generate 4 English prompts.
The output must be a single valid JSON object.

FORMAT:
{
  "bg_positive_en": "...",
  "bg_negative_en": "...",
  "video_motion_prompt_en": "...",
  "video_full_prompt_en": "..."
}

----------------------------------------------------
GLOBAL RULES
----------------------------------------------------

1) Structural Obedience
- Follow IMG_ANALYSIS_JSON.bg_layout_hint_en exactly.
- Preserve camera angle, surface orientation, and depth.

2) Foreground / Background Logic (CRITICAL)
- Foreground (Surface):
  * realistic material texture (wood, marble, fabric, concrete)
  * physically empty: NO objects, NO silhouettes, NO object shadows
- Background (Distance):
  * lifestyle context derived from ST2_JSON
  * visually rich but heavily blurred (bokeh)
  * allowed to add atmosphere, light, seasonal mood

Rule summary:
"Rich blurred background, fully empty surface."

3) Protected Product Placement Zone (MOST IMPORTANT)
- The empty area for product compositing is NOT always centered.
- Define the empty zone based on:
  * IMG_ANALYSIS_JSON.subject_position
  * IMG_ANALYSIS_JSON.subject_size
- That zone must remain completely empty:
  * no objects
  * no texture breaks
  * no stains
  * no shadows

4) Natural Realism (No Studio Look)
- Avoid plain studio backdrops or uniform walls.
- Allow natural surface texture, ambient light gradients, soft imperfections.
- Forbid props, furniture on the surface, or decorative objects in foreground.

----------------------------------------------------
bg_positive_en
----------------------------------------------------
Must start with "Empty".

Sentence structure:

"Empty [photography style],
clean realistic [surface material with subtle texture] surface [bg_layout_hint_en],
[perspective keywords],
a clearly defined empty placement zone positioned according to subject_position
and sized according to subject_size, kept completely free of objects and shadows,
[soft but rich blurred background context based on ST2_JSON],
[lighting_condition], [color_tone], depth of field, high resolution, natural realistic look"

Photography style (select ONE using ST2_JSON):
- seasonal household / living goods → "cozy lifestyle photography"
- kitchen / daily utility goods → "bright kitchen commercial photography"
- storage / utility / industrial → "sleek commercial photography"
- outdoor / seasonal → "nature lifestyle photography"
- default → "professional lifestyle photography"

Perspective keywords (from IMG_ANALYSIS_JSON.view_point):
- top_down → "flat lay view, top-down perspective, single continuous surface"
- front_view → "eye-level shot, front view"
- angled → "angled perspective with natural depth"
- side_view → "side profile perspective with elongated surface"

Background context generation:
- Use ST2_JSON usage_scenarios, season, and category mood
- Examples (DO NOT mention objects explicitly):
  * "soft winter daylight filtering through a blurred home interior"
  * "subtle natural light and blurred residential ambience"
  * "diffused indoor light with seasonal warm atmosphere"
  * "muted outdoor daylight with soft background bokeh"

----------------------------------------------------
bg_negative_en
----------------------------------------------------
Comma-separated single-word nouns only.

Base blockers:
text, watermark, logo, brand,
human, person, face, hands, mannequin,
object, item, prop, decoration, furniture, tool,
foreground, obstacle, clutter,
shadow, silhouette, stain,
distortion, noise, lowquality,
studio, backdrop

Dynamic product blocking:
- Extract core product nouns from ST2_JSON
- Convert to ONE English noun each and add
  (e.g. bubblewrap, film, sheet, roll)

----------------------------------------------------
video_motion_prompt_en
----------------------------------------------------
- If is_flat_lay is true:
  "Static top-down camera, almost no movement, smooth 5 second loop, no camera shake"
- Else:
  "Static camera with very subtle slow zoom matching the perspective,
   smooth 5 second loop, no camera shake"

----------------------------------------------------
video_full_prompt_en
----------------------------------------------------
- Create a new sentence (do NOT copy bg_positive_en).
- Mention:
  * perspective
  * lighting_condition
  * empty placement zone matching subject_position
  * camera motion
- End with:
  "high quality 5 second loop video, no people, no text, no logo"

----------------------------------------------------
FINAL VALIDATION
----------------------------------------------------
- Output valid JSON only.
- All text must be English.
- bg_positive_en MUST start with "Empty".
- The placement zone must follow subject_position and subject_size.
"""


# === 메시지 생성 ===

def build_bg_prompt_messages(
    st2_json_raw: Union[str, Dict[str, Any]],
    img_analysis_data: Optional[Dict[str, Any]] = None
) -> list[Dict[str, str]]:
    """
    ST2_JSON과 IMG_ANALYSIS_JSON을 받아서
    OpenAI chat/completions용 messages 리스트를 만든다.
    
    Args:
        st2_json_raw: ST2_JSON (raw string 또는 dict)
        img_analysis_data: IMG_ANALYSIS_JSON (dict) - view_point, lighting_condition, is_flat_lay, bg_layout_hint_en 등 포함
    """
    if isinstance(st2_json_raw, dict):
        st2_str = json.dumps(st2_json_raw, ensure_ascii=False, indent=2)
    else:
        st2_str = str(st2_json_raw)

    # IMG_ANALYSIS_JSON 구성
    if img_analysis_data:
        img_analysis_json = {
            "view_point": img_analysis_data.get("view_point", ""),
            "subject_position": img_analysis_data.get("subject_position", ""),
            "subject_size": img_analysis_data.get("subject_size", ""),
            "lighting_condition": img_analysis_data.get("lighting_condition", ""),
            "color_tone": img_analysis_data.get("color_tone", ""),
            "shadow_presence": img_analysis_data.get("shadow_presence", ""),
            "background_simplicity": img_analysis_data.get("background_simplicity", ""),
            "is_flat_lay": img_analysis_data.get("is_flat_lay", False),
            "bg_layout_hint_en": img_analysis_data.get("bg_layout_hint_en", ""),
        }
        img_analysis_str = json.dumps(img_analysis_json, ensure_ascii=False, indent=2)
    else:
        img_analysis_str = None

    # User content 구성
    if img_analysis_str:
        user_content = (
            "Below are ST2_JSON (context) and IMG_ANALYSIS_JSON (visual structure) for one product.\n"
            "Follow the rules above to generate a single JSON object with 4 fields:\n"
            "bg_positive_en, bg_negative_en, video_motion_prompt_en, video_full_prompt_en.\n\n"
            "[ST2_JSON]\n\n"
            f"{st2_str}\n\n"
            "[IMG_ANALYSIS_JSON]\n\n"
            f"{img_analysis_str}"
        )
    else:
        # IMG_ANALYSIS_JSON이 없는 경우 (하위 호환성)
        user_content = (
            "Below is ST2_JSON for one product.\n"
            "Follow the rules above to generate a single JSON object with 4 fields:\n"
            "bg_positive_en, bg_negative_en, video_motion_prompt_en, video_full_prompt_en.\n\n"
            "[ST2_JSON]\n\n"
            f"{st2_str}"
        )

    messages = [
        {"role": "system", "content": BG_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
    return messages


# === OpenAI 호출 래퍼 ===

def call_bg_prompt_api(
    st2_json_raw: Union[str, Dict[str, Any]],
    model: str = DEFAULT_MODEL,
    api_key: Optional[str] = None,
    temperature: float = 0.2,  # 호환성을 위해 유지하지만 gpt-5 계열에서는 사용 안 함
    img_analysis_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    ST2_JSON과 IMG_ANALYSIS_JSON을 입력으로 받아
    OpenAI API를 호출하고, 파싱된 JSON(dict)을 반환한다.

    실패 시 예외를 발생시킨다.
    
    Args:
        st2_json_raw: ST2_JSON (raw string 또는 dict)
        model: 모델명
        api_key: API 키
        temperature: 온도 (gpt-5 계열에서는 사용 안 함)
        img_analysis_data: IMG_ANALYSIS_JSON (dict) - view_point, lighting_condition, is_flat_lay, bg_layout_hint_en 등 포함
    
    Note: gpt-5 계열 모델은 temperature 파라미터를 지원하지 않으므로 사용하지 않습니다.
    """
    client = get_openai_client(api_key=api_key)
    messages = build_bg_prompt_messages(st2_json_raw, img_analysis_data)

    # gpt-5 계열은 temperature를 지원하지 않으므로 파라미터에 포함하지 않음
    response = client.chat.completions.create(
        model=model,
        messages=messages,
    )

    content = response.choices[0].message.content
    # content는 순수 JSON 문자열이어야 한다.
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"모델 응답을 JSON으로 파싱하지 못했습니다: {e}\n\ncontent=\n{content}")

    # 최소 필드 검증(뼈대)
    for key in ("bg_positive_en", "bg_negative_en", "video_motion_prompt_en", "video_full_prompt_en"):
        if key not in data:
            raise ValueError(f"응답 JSON에 필드 '{key}' 가 없습니다. content=\n{content}")

    return data


# === 테스트용 메인 ===

if __name__ == "__main__":
    # 간단한 로컬 테스트용 (실제 사용 시 GUI/배치에서 import)
    example_st2 = {
        "meta": {
            "기본상품명": "산업용 대형 선풍기 방진 커버 공장 공업용 방진 덮개",
            "판매형태": "단품형",
            "옵션_원본": "그레이 85cm 특대형",
            "카테고리_경로": "생활/건강>수납/정리용품>선풍기커버",
        }
        # ... 나머지는 필요시 추가
    }

    try:
        result = call_bg_prompt_api(example_st2)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as e:
        print("[ERROR]", e)
