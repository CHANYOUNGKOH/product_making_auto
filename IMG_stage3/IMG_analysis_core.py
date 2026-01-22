"""
IMG_analysis_core.py

썸네일 이미지 분석: 이미지 → 카메라 각도, 레이아웃, 조명 분석 JSON 생성 코어 모듈.

- OpenAI Vision API를 호출해서
  view_point, subject_position, subject_size, lighting_condition, color_tone,
  shadow_presence, background_simplicity, is_flat_lay, bg_layout_hint_en
  9개 필드를 가진 JSON을 반환한다.
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

# 이미지 리사이즈 설정
INPUT_SIZE = 1000  # 원본 이미지 크기
OUTPUT_SIZE = 512  # API 전송용 리사이즈 크기


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

def resize_image_to_512(image_path: str) -> Image.Image:
    """
    이미지를 512x512로 리사이즈합니다.
    - 원본 비율을 유지하면서 중앙 크롭
    - 투명도가 있으면 RGBA 모드 유지
    """
    img = Image.open(image_path)
    
    # RGBA 모드로 변환 (투명도 지원)
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    
    # 비율 유지하면서 512x512로 리사이즈
    img.thumbnail((OUTPUT_SIZE, OUTPUT_SIZE), Image.Resampling.LANCZOS)
    
    # 정확히 512x512로 만들기 (중앙 크롭)
    width, height = img.size
    if width != OUTPUT_SIZE or height != OUTPUT_SIZE:
        # 중앙 크롭
        left = (width - OUTPUT_SIZE) // 2
        top = (height - OUTPUT_SIZE) // 2
        right = left + OUTPUT_SIZE
        bottom = top + OUTPUT_SIZE
        img = img.crop((left, top, right, bottom))
    
    return img


def image_to_base64_data_url(image_path: str) -> str:
    """
    이미지 파일을 512x512로 리사이즈한 후 base64 data URL로 변환합니다.
    
    Returns:
        "data:image/png;base64,..." 형식의 문자열
    """
    img = resize_image_to_512(image_path)
    
    # BytesIO에 저장
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    
    # base64 인코딩
    img_bytes = buffer.getvalue()
    img_base64 = base64.b64encode(img_bytes).decode("utf-8")
    
    return f"data:image/png;base64,{img_base64}"


# === System Prompt ===

ANALYSIS_SYSTEM_PROMPT = """You are a commercial photography analyst.  

Analyze the given product thumbnail image and output one JSON object describing its camera angle, layout, and lighting.  

The result will be used to generate a background that must match the image's perspective and lighting.

Rules:  

- Output ONLY one JSON object. No markdown, no comments.  

- All values must be in English.  

- Describe the scene objectively. Do NOT guess product purpose or brand.

Choose values from the allowed sets below:

1) view_point (choose one):

   - "front_view", "top_down", "angled", "side_view", "close_up"

2) subject_position (choose one):

   - "center", "slightly_left", "slightly_right", "top_center", "bottom_center"

3) subject_size (choose one):

   - "very_small", "small", "medium", "large", "very_large"

4) lighting_condition:

   - Short phrase (e.g., "soft diffuse studio lighting", "hard directional light from left")

5) color_tone:

   - "warm", "cool", "neutral"

6) shadow_presence (choose one):

   - "none", "soft", "grounded", "floating", "strong"

7) background_simplicity:

   - "simple" or "complex"

8) is_flat_lay:

   - true only if the camera is directly top-down

9) bg_layout_hint_en:

   - One short sentence describing ONLY an empty background matching the view_point and lighting.

   - Do NOT mention the specific product.

   - If view_point is "front_view", you MUST mention "floor", "tabletop", or "surface" to anchor the object

   - Examples:

     - "Empty clean white studio background seen from eye level with soft diffuse lighting"

     - "Top-down view of an empty wooden tabletop with soft daylight"

JSON format to output:

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
"""


# === 메시지 생성 ===

def build_analysis_messages(image_path: str) -> list[Dict[str, Any]]:
    """
    이미지 경로를 받아서
    OpenAI vision API용 messages 리스트를 만든다.
    """
    # 이미지를 base64 data URL로 변환
    data_url = image_to_base64_data_url(image_path)
    
    user_content = [
        {
            "type": "text",
            "text": "Analyze this product thumbnail image and output the JSON object describing camera angle, layout, and lighting according to the rules."
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


# === OpenAI 호출 래퍼 ===

def call_image_analysis_api(
    image_path: str,
    model: str = DEFAULT_MODEL,
    api_key: Optional[str] = None,
    reasoning_effort: Optional[str] = None,
) -> tuple[Dict[str, Any], Any]:
    """
    이미지 경로를 입력으로 받아
    OpenAI Vision API를 호출하고, 파싱된 JSON(dict)과 response 객체를 반환한다.

    Returns:
        (result_dict, response): 분석 결과 딕셔너리와 API 응답 객체
    
    실패 시 예외를 발생시킨다.
    
    Note: gpt-5 계열 모델은 reasoning_effort 파라미터를 사용합니다.
    """
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"이미지 파일을 찾을 수 없습니다: {image_path}")
    
    client = get_openai_client(api_key=api_key)
    messages = build_analysis_messages(image_path)

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

