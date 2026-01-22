"""
IMG_analysis_core_Casche.py

썸네일 이미지 분석: 이미지 → 카메라 각도, 레이아웃, 조명 분석 JSON 생성 코어 모듈 (캐싱 최적화 버전)

- OpenAI Vision API를 호출해서
  view_point, subject_position, subject_size, lighting_condition, color_tone,
  shadow_presence, background_simplicity, is_flat_lay, bg_layout_hint_en
  9개 필드를 가진 JSON을 반환한다.
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
  * 동적 콘텐츠(이미지)를 user 프롬프트에 배치
  * 프롬프트 프리픽스가 모든 요청에서 동일하도록 구성
"""

from __future__ import annotations

import os
import json
import base64
from io import BytesIO
from typing import Any, Dict, Optional, Union
from PIL import Image

from openai import OpenAI

# === 기본 설정 ===

# 필요하면 원하는 파일명으로 변경해서 쓰면 됨
API_KEY_FILE = ".openai_api_key_img_analysis"

# 실제 사용 시, OpenAI 대시보드에서 확인한 모델명으로 교체
DEFAULT_MODEL = "gpt-5-mini"  # Vision API 지원 모델 사용

# 모델별 가격 (USD per Million Tokens)
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5": {"input": 1.25, "output": 10.0},
    "gpt-5-mini": {"input": 0.25, "output": 2.00},
    "gpt-5-nano": {"input": 0.05, "output": 0.40},
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
}

# 이미지 리사이즈 설정 (호환성 유지용, 실제로는 resize_mode 파라미터 사용)
INPUT_SIZE = 1000  # 원본 이미지 크기
OUTPUT_SIZE = 512  # API 전송용 리사이즈 크기 (기본값)


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


# === 이미지 처리 유틸 ===

def image_to_base64_data_url(image_path: str, max_width: int = None, log_func=None) -> str:
    """
    이미지 파일을 가로 기준 비율 유지 리사이즈 후 base64 data URL로 변환합니다.
    
    Args:
        image_path: 이미지 파일 경로
        max_width: 최대 가로 (px). None이면 리사이징 안 함. 가로 기준 비율 유지 리사이즈 (크롭/패딩 금지).
        log_func: 로그 출력 함수 (선택사항)
    
    Returns:
        "data:image/png;base64,..." 또는 "data:image/jpeg;base64,..." 형식의 문자열
    """
    import mimetypes
    
    mime, _ = mimetypes.guess_type(image_path)
    if mime is None:
        mime = "image/jpeg"
    
    # 리사이징이 필요하면 PIL 사용
    if max_width is not None:
        try:
            with Image.open(image_path) as img:
                # 원본 크기 확인
                original_width, original_height = img.size
                need_resize = False
                new_width = original_width
                new_height = original_height
                
                # 가로 기준 리사이즈 (비율 유지, 크롭/패딩 금지)
                if original_width > max_width:
                    # 비율 유지하면서 가로 기준으로 리사이징
                    ratio = max_width / original_width
                    new_width = max_width
                    new_height = round(original_height * ratio)
                    need_resize = True
                    
                    # 디버깅: 리사이즈 정보 로그
                    if log_func:
                        reduction_pct = (1 - (new_width * new_height) / (original_width * original_height)) * 100
                        log_func(f"[리사이즈 적용] 이미지 리사이즈: original=({original_width}, {original_height}) -> new=({new_width}, {new_height}) (가로 {max_width}px)")
                else:
                    # 이미지가 이미 max_width 이하이면 리사이즈 불필요
                    if log_func:
                        log_func(f"[리사이즈] 이미지 리사이즈 불필요: original=({original_width}, {original_height}) (이미 가로 {max_width}px 이하)")
                
                if need_resize:
                    # RGB 모드로 변환 (RGBA 등도 지원)
                    if img.mode not in ('RGB', 'L'):
                        img = img.convert('RGB')
                    
                    # 고품질 리샘플링 (Lanczos 권장)
                    img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    
                    # JPEG로 저장 (메모리, 투명도가 있으면 PNG 사용)
                    output = BytesIO()
                    if img.mode == 'RGBA' or img.mode == 'LA':
                        img_resized.save(output, format='PNG', optimize=True)
                        mime = "image/png"
                    else:
                        img_resized.save(output, format='JPEG', quality=85, optimize=True)
                        mime = "image/jpeg"
                    output.seek(0)
                    
                    # base64 인코딩
                    b64 = base64.b64encode(output.read()).decode("ascii")
                    
                    return f"data:{mime};base64,{b64}"
        except Exception as e:
            # 리사이징 실패 시 원본 사용
            error_msg = f"[WARN] 이미지 리사이징 실패 ({os.path.basename(image_path)}): {e}, 원본 사용"
            if log_func:
                log_func(error_msg)
            else:
                print(error_msg)
    
    # 리사이징 없이 원본 사용
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"


# === System Prompt ===
# ⚠️ 중요: 프롬프트 캐싱 활성화를 위해 1024 토큰 이상이어야 함
ANALYSIS_SYSTEM_PROMPT = """You are a commercial photography analyst specializing in e-commerce product image analysis. Your expertise includes camera angles, lighting conditions, composition, and technical photography aspects.

Analyze the given product thumbnail image and output one JSON object describing its camera angle, layout, and lighting. The result will be used to generate a background that must match the image's perspective and lighting for product compositing. Accuracy in perspective and lighting matching is critical for seamless product integration.

[Core Rules and Principles]

- Output ONLY one JSON object. No markdown, no comments, no explanations, no additional text.
- All values must be in English. Use standard English terminology for photography and lighting.
- Describe the scene objectively based solely on what you observe. Do NOT guess product purpose, brand, or functionality.
- Focus exclusively on technical photography aspects: camera angle, lighting direction and quality, composition, shadows, and background characteristics.
- Do NOT analyze product features, materials, functionality, or any product-specific attributes.
- Maintain consistency: similar images should produce similar analysis results.
- Be precise: small differences in camera angle or lighting should be reflected accurately in the output.

[Field Definitions and Allowed Values]

1) view_point (choose exactly one):
   - "front_view": Camera positioned at eye level (approximately 0-15 degrees from horizontal), facing the product directly. The product appears as if viewed straight-on. Common for product catalog shots.
   - "top_down": Camera positioned directly above (approximately 85-90 degrees from horizontal), looking straight down. The product is viewed from above. This is the classic flat lay perspective.
   - "angled": Camera positioned at an angle between 30-60 degrees from horizontal. The product is viewed from above but at an angle, showing both top and side surfaces. This is the most common e-commerce product photography angle.
   - "side_view": Camera viewing from the side, showing the product's profile. The camera is positioned perpendicular to the product's main axis, typically at eye level or slightly above.
   - "close_up": Very close camera position where the product fills most or all of the frame. The camera angle can vary, but the key characteristic is the extreme proximity to the subject.

2) subject_position (choose one):
   - "center": Product is centered in the frame
   - "slightly_left": Product is slightly to the left of center
   - "slightly_right": Product is slightly to the right of center
   - "top_center": Product is at the top center of the frame
   - "bottom_center": Product is at the bottom center of the frame

3) subject_size (choose one):
   - "very_small": Product occupies less than 20% of frame
   - "small": Product occupies 20-40% of frame
   - "medium": Product occupies 40-60% of frame
   - "large": Product occupies 60-80% of frame
   - "very_large": Product occupies more than 80% of frame

4) lighting_condition:
   - Short descriptive phrase (typically 3-8 words) describing the lighting quality, direction, and characteristics.
   - Include: light quality (soft/hard/diffuse), direction (from top/left/right/front), source type (studio/natural/artificial), and any notable characteristics.
   - Examples:
     * "soft diffuse studio lighting"
     * "soft diffuse studio lighting with subtle top-left emphasis"
     * "hard directional light from left"
     * "natural daylight from window"
     * "warm artificial lighting"
     * "cool fluorescent lighting"
     * "soft ambient lighting with subtle shadows"
     * "soft diffuse studio lighting with slight specular highlights"
     * "even soft studio lighting with minimal shadows"

5) color_tone:
   - "warm": Yellow, orange, red tones dominant
   - "cool": Blue, cyan, green tones dominant
   - "neutral": Balanced, no strong color cast

6) shadow_presence (choose one):
   - "none": No visible shadows
   - "soft": Soft, diffused shadows with gradual edges
   - "grounded": Shadows that connect the product to the surface
   - "floating": Shadows that make the product appear to float
   - "strong": Hard, dark shadows with sharp edges

7) background_simplicity:
   - "simple": Minimal background, few elements, clean
   - "complex": Busy background, multiple elements, detailed

8) is_flat_lay:
   - true: Camera is directly top-down (90 degrees, looking straight down)
   - false: Any other camera angle

9) bg_layout_hint_en:
   - One concise sentence (typically 15-25 words) describing ONLY an empty background that matches the view_point, lighting, and surface characteristics observed in the image.
   - CRITICAL: Do NOT mention the specific product, product type, or any product-related elements.
   - CRITICAL: If view_point is "front_view", you MUST include "floor", "tabletop", or "surface" to provide spatial context and anchor the object.
   - Required elements: surface type/material, perspective (matching view_point), lighting condition, and emphasis on empty space.
   - Surface types: white studio background, wooden tabletop, marble surface, concrete floor, fabric surface, paper surface, etc.
   - Examples:
     * "Empty clean white studio background seen from eye level with soft diffuse lighting"
     * "Top-down view of an empty clean white tabletop with soft diffuse studio lighting and subtle soft shadows"
     * "Empty marble surface seen from an angled perspective with warm artificial lighting"
     * "Empty concrete floor seen from front view with cool fluorescent lighting"
     * "Empty clean white seamless studio surface seen from an angled perspective with soft diffuse lighting and subtle soft shadows on the surface"
     * "Empty clean white tabletop surface seen from eye level with soft diffuse studio lighting and even illumination"

[JSON Output Format]

{
  "view_point": "",
  "subject_position": "",
  "subject_size": "",
  "lighting_condition": "",
  "color_tone": "",
  "shadow_presence": "",
  "background_simplicity": "",
  "is_flat_lay": true,
  "bg_layout_hint_en": ""
}

[Important Notes]

- The analysis must be purely technical and objective.
- Focus on what you can see, not what you infer about the product.
- bg_layout_hint_en is critical for background generation - be precise and descriptive.
- All fields are required - do not omit any field.

[Analysis Examples]

Example 1 - Top-down flat lay:
Input: Product image showing a top-down view of items on a white table.
Output:
{
  "view_point": "top_down",
  "subject_position": "center",
  "subject_size": "large",
  "lighting_condition": "soft diffuse studio lighting",
  "color_tone": "neutral",
  "shadow_presence": "soft",
  "background_simplicity": "simple",
  "is_flat_lay": true,
  "bg_layout_hint_en": "Top-down view of an empty clean white tabletop with soft diffuse studio lighting and subtle soft shadows"
}

Example 2 - Angled product shot:
Input: Product image showing an item at a 45-degree angle on a white surface.
Output:
{
  "view_point": "angled",
  "subject_position": "slightly_right",
  "subject_size": "large",
  "lighting_condition": "soft diffuse studio lighting with subtle top-left emphasis",
  "color_tone": "neutral",
  "shadow_presence": "grounded",
  "background_simplicity": "simple",
  "is_flat_lay": false,
  "bg_layout_hint_en": "Empty clean white studio surface seen from an angled perspective with soft diffuse lighting and subtle grounded shadows"
}

Example 3 - Front view product:
Input: Product image showing a front-facing view of an item on a surface.
Output:
{
  "view_point": "front_view",
  "subject_position": "center",
  "subject_size": "medium",
  "lighting_condition": "soft diffuse studio lighting",
  "color_tone": "neutral",
  "shadow_presence": "soft",
  "background_simplicity": "simple",
  "is_flat_lay": false,
  "bg_layout_hint_en": "Empty clean white tabletop surface seen from eye level with soft diffuse studio lighting and even illumination"
}

Example 4 - Side profile view:
Input: Product image showing a side profile of an item.
Output:
{
  "view_point": "side_view",
  "subject_position": "center",
  "subject_size": "large",
  "lighting_condition": "soft diffuse studio lighting",
  "color_tone": "neutral",
  "shadow_presence": "grounded",
  "background_simplicity": "simple",
  "is_flat_lay": false,
  "bg_layout_hint_en": "Empty clean white studio background seen from a side/profile perspective with a flat tabletop surface, soft diffuse lighting and subtle soft shadows"
}

[Edge Cases and Special Considerations]

1. Ambiguous camera angles:
   - If the angle is between 30-60 degrees, use "angled"
   - If it's closer to 90 degrees (looking down), use "top_down" and set is_flat_lay to true
   - If it's closer to 0 degrees (eye level), use "front_view"

2. Multiple products in frame:
   - Analyze the primary/most prominent product
   - Use its position and size for subject_position and subject_size

3. Mixed lighting:
   - Describe the dominant lighting condition
   - If multiple light sources are visible, describe the overall effect

4. Unusual backgrounds:
   - Even if the background is complex, describe it accurately
   - The bg_layout_hint_en should still describe an empty background matching the perspective

5. Shadow interpretation:
   - "grounded" shadows clearly connect the product to the surface
   - "floating" shadows create separation between product and surface
   - "soft" shadows have gradual edges and moderate darkness
   - "strong" shadows are dark with sharp, defined edges

[Quality Assurance Checklist]

Before outputting, verify:
- All 9 fields are filled with appropriate values
- view_point accurately matches the actual camera angle observed in the image
- is_flat_lay is true only for true top-down (85-90 degree) views, false for all other angles
- bg_layout_hint_en does NOT mention the product itself, product type, or any product-related elements
- bg_layout_hint_en includes perspective (matching view_point), surface type, and lighting information
- All text values are in English using standard photography terminology
- JSON format is valid (no trailing commas, proper quotes, correct boolean values)
- subject_position and subject_size accurately reflect the product's placement and size in the frame
- lighting_condition describes the actual lighting observed, not assumptions

[Common Mistakes to Avoid]

1. Do NOT set is_flat_lay to true for angled views (even if the angle is steep)
2. Do NOT include product names or product types in bg_layout_hint_en
3. Do NOT guess lighting conditions - base them on what you actually see
4. Do NOT use "close_up" for view_point unless the product truly fills most of the frame
5. Do NOT mix camera angle descriptions - choose the single most accurate view_point
6. Do NOT omit required fields - all 9 fields must be present in the output
7. Do NOT use markdown formatting - output pure JSON only
8. Do NOT add explanatory text outside the JSON object

[Technical Photography Guidelines]

Camera Angle Determination:
- Observe the horizon line and surface orientation to determine the exact angle
- Top-down (90 degrees): Surface appears as a flat plane with no visible depth
- Angled (30-60 degrees): Surface shows depth and perspective, both top and side visible
- Front view (0-15 degrees): Surface appears as a vertical or near-vertical plane
- Side view: Product's profile is the primary visible aspect

Lighting Analysis:
- Observe shadow direction to determine light source position
- Observe shadow softness to determine light quality (hard vs soft)
- Observe color temperature to determine warm vs cool vs neutral
- Observe shadow presence to determine if shadows exist and their characteristics

Subject Analysis:
- Measure subject position relative to frame center (use rule of thirds as reference)
- Estimate subject size as percentage of total frame area
- Consider both width and height when determining size category

Background Analysis:
- Assess complexity: simple backgrounds have minimal elements, complex backgrounds have multiple visible elements
- Note surface material and texture if visible
- Consider depth of field effects on background blur

Output the JSON object now."""


# === 메시지 생성 ===

def build_analysis_messages(
    image_path: str,
    use_cache_optimization: bool = False,
    max_width: int = None
) -> list[Dict[str, Any]]:
    """
    이미지 경로를 받아서
    OpenAI vision API용 messages 리스트를 만든다.
    
    Args:
        image_path: 이미지 파일 경로
        use_cache_optimization: 캐싱 최적화 모드 사용 여부 (기본값: False, 호환성 유지)
        max_width: 최대 가로 (px). None이면 리사이징 안 함. 가로 기준 비율 유지 리사이즈 (크롭/패딩 금지).
    """
    # 이미지를 base64 data URL로 변환
    data_url = image_to_base64_data_url(image_path, max_width=max_width)
    
    user_text = "Analyze this product thumbnail image and output the JSON object describing camera angle, layout, and lighting according to the rules."
    
    if use_cache_optimization:
        # 캐싱 최적화 모드: Responses API 형식
        user_content = [
            {
                "type": "input_text",
                "text": user_text
            },
            {
                "type": "input_image",
                "image_url": data_url  # /v1/responses API에서는 image_url이 직접 문자열이어야 함
            }
        ]
    else:
        # 일반 모드: Chat Completions API 형식
        user_content = [
            {
                "type": "text",
                "text": user_text
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": data_url
                }
            }
        ]

    messages = [
        {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
    return messages

def build_analysis_batch_payload(
    row_index: int,
    image_path: str,
    model_name: str,
    reasoning_effort: str,
    use_cache_optimization: bool = True,
    max_width: int = None,
    log_func=None
) -> Dict[str, Any]:
    """
    Batch API용 요청 payload를 생성한다.
    
    Args:
        row_index: 행 인덱스 (custom_id 생성용)
        image_path: 이미지 파일 경로
        model_name: 모델명
        reasoning_effort: reasoning effort (gpt-5 계열용)
        use_cache_optimization: 캐싱 최적화 모드 사용 여부
        max_width: 최대 가로 (px). None이면 리사이징 안 함. 가로 기준 비율 유지 리사이즈 (크롭/패딩 금지).
        log_func: 로그 출력 함수 (선택사항)
    
    Returns:
        Batch API용 request 객체
    """
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"이미지 파일을 찾을 수 없습니다: {image_path}")
    
    # 이미지를 base64 data URL로 변환
    data_url = image_to_base64_data_url(image_path, max_width=max_width, log_func=log_func)
    
    user_text = "Analyze this product thumbnail image and output the JSON object describing camera angle, layout, and lighting according to the rules."
    
    if use_cache_optimization:
        # System 메시지 (텍스트만, 정적)
        system_content = [{"type": "input_text", "text": ANALYSIS_SYSTEM_PROMPT}]
        
        # User 메시지 (텍스트 + 이미지, 동적)
        user_content = [
            {
                "type": "input_text",
                "text": user_text
            },
            {
                "type": "input_image",
                "image_url": data_url  # /v1/responses API에서는 image_url이 직접 문자열이어야 함
            }
        ]
        
        # Body 구성 (Responses API)
        body = {
            "model": model_name,
            "input": [
                {
                    "role": "system",
                    "content": system_content,
                },
                {
                    "role": "user",
                    "content": user_content,
                }
            ],
        }
        
        # reasoning.effort (Responses API)
        is_reasoning = any(x in model_name for x in ["gpt-5", "o1", "o3"])
        if is_reasoning and reasoning_effort != "none":
            body["reasoning"] = {"effort": reasoning_effort}
        
        # text.format: JSON 출력 강제 (Structured Outputs)
        body["text"] = {"format": {"type": "json_object"}}
        
        url = "/v1/responses"
    else:
        # 일반 모드: 기존 방식 유지
        user_content = [
            {
                "type": "text",
                "text": user_text
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": data_url
                }
            }
        ]
        
        body = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
        }
        
        is_reasoning = any(x in model_name for x in ["gpt-5", "o1", "o3"])
        if is_reasoning and reasoning_effort != "none":
            body["reasoning_effort"] = reasoning_effort
        
        url = "/v1/chat/completions"

    request_obj = {
        "custom_id": f"row_{row_index}",
        "method": "POST",
        "url": url,
        "body": body
    }
    return request_obj


# === OpenAI 호출 래퍼 ===

def call_image_analysis_api(
    image_path: str,
    model: str = DEFAULT_MODEL,
    api_key: Optional[str] = None,
    reasoning_effort: Optional[str] = None,
    use_cache_optimization: bool = False,
) -> tuple[Dict[str, Any], Any]:
    """
    이미지 경로를 입력으로 받아
    OpenAI Vision API를 호출하고, 파싱된 JSON(dict)과 response 객체를 반환한다.

    Returns:
        (result_dict, response): 분석 결과 딕셔너리와 API 응답 객체
    
    실패 시 예외를 발생시킨다.
    
    Note: gpt-5 계열 모델은 reasoning_effort 파라미터를 사용합니다.
    Note: use_cache_optimization이 True이면 Responses API를 사용합니다 (캐싱 최적화).
    """
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"이미지 파일을 찾을 수 없습니다: {image_path}")
    
    client = get_openai_client(api_key=api_key)
    
    if use_cache_optimization:
        # 캐싱 최적화 모드: Chat Completions API 사용 (Vision API는 이미지 포함)
        # system/user 분리된 메시지 구조 사용
        messages = build_analysis_messages(image_path, use_cache_optimization=False)
        
        # API 파라미터 구성
        params = {
            "model": model,
            "messages": messages,
        }
        
        # gpt-5 계열은 reasoning_effort 사용
        if reasoning_effort and reasoning_effort != "none":
            params["reasoning_effort"] = reasoning_effort
        
        # prompt_cache_key 및 prompt_cache_retention (이미지가 다르므로 효과는 제한적)
        # 하지만 system 프롬프트는 동일하므로 일부 캐싱 효과는 있음
        params["prompt_cache_key"] = "img_analysis_v1"
        # prompt_cache_retention: 모델이 지원하는 경우에만 추가
        # Extended retention 지원 모델: gpt-5.1, gpt-5.1-codex, gpt-5.1-codex-mini, gpt-5.1-chat-latest, gpt-5, gpt-5-codex, gpt-4.1
        # gpt-5-mini, gpt-5-nano는 prompt_cache_retention 파라미터를 지원하지 않음
        if model in ["gpt-5.1", "gpt-5.1-codex", "gpt-5.1-codex-mini", "gpt-5.1-chat-latest", "gpt-5", "gpt-5-codex", "gpt-4.1"]:
            params["prompt_cache_retention"] = "extended"  # 24시간 retention
        elif model not in ["gpt-5-mini", "gpt-5-nano"]:
            # 기타 모델은 in-memory 사용 (5~10분 inactivity, 최대 1시간)
            params["prompt_cache_retention"] = "in_memory"
        
        # text.format: JSON 출력 강제 (Structured Outputs)
        params["text"] = {"format": {"type": "json_object"}}
        
        response = client.chat.completions.create(**params)
    else:
        # 일반 모드: Chat Completions API 사용
        messages = build_analysis_messages(image_path, use_cache_optimization=False)

        # API 파라미터 구성
        params = {
            "model": model,
            "messages": messages,
        }
        
        # gpt-5 계열은 reasoning_effort 사용
        if reasoning_effort and reasoning_effort != "none":
            params["reasoning_effort"] = reasoning_effort

        response = client.chat.completions.create(**params)

    content = response.choices[0].message.content
    # content는 순수 JSON 문자열이어야 한다.
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"모델 응답을 JSON으로 파싱하지 못했습니다: {e}\n\ncontent=\n{content}")

    # 필수 필드 검증
    required_fields = [
        "view_point", "subject_position", "subject_size", "lighting_condition",
        "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
        "bg_layout_hint_en"
    ]
    for key in required_fields:
        if key not in data:
            raise ValueError(f"응답 JSON에 필드 '{key}' 가 없습니다. content=\n{content}")

    return data, response


# === 테스트용 메인 ===

if __name__ == "__main__":
    # 간단한 로컬 테스트용
    test_image_path = "test_thumbnail.png"  # 테스트용 이미지 경로
    
    if os.path.exists(test_image_path):
        try:
            result, response = call_image_analysis_api(test_image_path)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            print(f"\n[Usage] {response.usage}")
        except Exception as e:
            print("[ERROR]", e)
    else:
        print(f"[INFO] 테스트 이미지가 없습니다: {test_image_path}")

