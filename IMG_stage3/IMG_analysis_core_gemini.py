"""
IMG_analysis_core_gemini.py

썸네일 이미지 분석: 이미지 → 카메라 각도, 레이아웃, 조명 분석 JSON 생성 코어 모듈 (Gemini 버전)

- Gemini Vision API를 호출해서
  view_point, subject_position, subject_size, lighting_condition, color_tone,
  shadow_presence, background_simplicity, is_flat_lay, bg_layout_hint_en
  9개 필드를 가진 JSON을 반환한다.
- Implicit Caching 자동 적용 (System Instruction 동일)
"""

from __future__ import annotations

import os
import json
import base64
from io import BytesIO
from typing import Any, Dict, Optional, Union

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# Gemini API
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# === 기본 설정 ===
API_KEY_FILE = ".gemini_api_key_img_analysis"
DEFAULT_MODEL = "gemini-2.5-flash-lite"

# 모델별 가격 (USD per Million Tokens)
MODEL_PRICING_USD_PER_MTOK = {
    "gemini-2.5-flash-lite": {"input": 0.05, "output": 0.20},
    "gemini-2.5-flash-preview-05-20": {"input": 0.15, "output": 0.60},
    "gemini-2.0-flash": {"input": 0.10, "output": 0.40},
}

# 이미지 리사이즈 설정
INPUT_SIZE = 1000
OUTPUT_SIZE = 768  # Gemini 1타일 = 258 토큰/이미지 (384~768 동일 비용)


# === API Key 유틸 ===
def load_api_key_from_file(path: str = API_KEY_FILE) -> Optional[str]:
    """텍스트 파일에서 API 키를 읽는다."""
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
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(api_key.strip())


def get_gemini_client(api_key: Optional[str] = None):
    """Gemini 클라이언트를 생성한다."""
    if api_key is None:
        file_key = load_api_key_from_file()
        if file_key:
            api_key = file_key
    if api_key is None:
        api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise RuntimeError("Gemini API 키가 설정되지 않았습니다.")

    return genai.Client(api_key=api_key)


# === 이미지 처리 유틸 ===
def image_to_base64_data(image_path: str, max_size: int = None, log_func=None) -> tuple:
    """
    이미지 파일을 base64로 변환합니다.

    Args:
        max_size: 가로세로 중 긴 쪽의 최대 크기 (비율 유지)
            384 - 토큰 절약 (258 토큰/이미지, Gemini 최소)
            768 - 고품질 (1타일)

    Returns:
        (base64_data, mime_type) 튜플
    """
    if not PIL_AVAILABLE:
        raise ImportError("Pillow 패키지가 설치되지 않았습니다.")

    try:
        with Image.open(image_path) as img:
            original_width, original_height = img.size

            # 리사이즈 필요 여부 확인 (가로세로 중 큰 쪽 기준)
            if max_size and (original_width > max_size or original_height > max_size):
                ratio = min(max_size / original_width, max_size / original_height)
                new_width = round(original_width * ratio)
                new_height = round(original_height * ratio)

                if log_func:
                    log_func(f"[리사이즈] ({original_width}, {original_height}) -> ({new_width}, {new_height})")

                # RGB 모드로 변환
                if img.mode not in ('RGB', 'L'):
                    img = img.convert('RGB')

                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

            # JPEG로 저장
            output = BytesIO()
            if img.mode == 'RGBA' or img.mode == 'LA':
                img = img.convert('RGB')
            img.save(output, format='JPEG', quality=85, optimize=True)
            output.seek(0)

            b64 = base64.b64encode(output.read()).decode("ascii")
            return b64, "image/jpeg"

    except Exception as e:
        if log_func:
            log_func(f"[WARN] 이미지 처리 실패: {e}")
        raise


# === System Instruction (Gemini 최적화) ===
ANALYSIS_SYSTEM_INSTRUCTION = """You are a commercial photography analyst specializing in e-commerce product image analysis. Your expertise includes camera angles, lighting conditions, composition, and technical photography aspects.

Analyze the given product thumbnail image and output one JSON object describing its camera angle, layout, and lighting. The result will be used to generate a background that must match the image's perspective and lighting for product compositing.

[Core Rules]
- Output ONLY one JSON object. No markdown, no comments, no explanations.
- All values must be in English.
- Describe the scene objectively based solely on what you observe.
- Focus exclusively on technical photography aspects.

[Field Definitions]

1) view_point (choose exactly one):
   - "front_view": Camera at eye level (0-15 degrees), facing product directly
   - "top_down": Camera directly above (85-90 degrees)
   - "angled": Camera at 30-60 degrees from horizontal
   - "side_view": Camera viewing from the side
   - "close_up": Very close camera position

2) subject_position (choose one):
   - "center": Product is centered in the frame
   - "left": Product is positioned to the left
   - "right": Product is positioned to the right
   - "offset": Product is positioned off-center

3) subject_size (choose one):
   - "small": Product occupies less than 30% of frame
   - "medium": Product occupies 30-60% of frame
   - "large": Product occupies 60-80% of frame
   - "full": Product fills entire frame (80%+)

4) lighting_condition (choose one):
   - "soft_diffused": Even, shadowless lighting
   - "directional": Light from specific direction
   - "studio": Professional studio lighting
   - "natural": Natural daylight
   - "dramatic": High contrast lighting

5) color_tone (choose one):
   - "warm": Yellow/orange tones
   - "cool": Blue/green tones
   - "neutral": Balanced colors
   - "vibrant": Saturated colors
   - "muted": Desaturated colors

6) shadow_presence (choose one):
   - "none": No visible shadows
   - "subtle": Light, soft shadows
   - "moderate": Visible but not harsh
   - "strong": Dark, defined shadows

7) background_simplicity (choose one):
   - "plain": Solid color or white
   - "simple": Minimal texture
   - "textured": Visible texture/pattern
   - "complex": Multiple elements

8) is_flat_lay (boolean):
   - true: If view_point is "top_down" AND product lies flat
   - false: Otherwise

9) bg_layout_hint_en (string):
   - A 10-20 word English description of the background layout
   - Example: "white studio surface with soft gradient, product centered on clean background"

[Output Format]
{
  "view_point": "...",
  "subject_position": "...",
  "subject_size": "...",
  "lighting_condition": "...",
  "color_tone": "...",
  "shadow_presence": "...",
  "background_simplicity": "...",
  "is_flat_lay": true/false,
  "bg_layout_hint_en": "..."
}"""

# User 프롬프트 템플릿
ANALYSIS_USER_PROMPT = "Analyze this product image and output the JSON analysis result."


def analyze_image(
    image_path: str,
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    max_width: int = OUTPUT_SIZE,
    log_func=None,
) -> Dict[str, Any]:
    """
    이미지를 분석하여 카메라 각도, 레이아웃, 조명 정보를 JSON으로 반환합니다.

    Args:
        image_path: 이미지 파일 경로
        api_key: Gemini API 키 (None이면 파일/환경변수에서 로드)
        model: 사용할 모델명
        max_width: 이미지 최대 가로 크기
        log_func: 로그 출력 함수

    Returns:
        분석 결과 딕셔너리
    """
    if not GEMINI_AVAILABLE:
        raise ImportError("google-genai 패키지가 설치되지 않았습니다.")

    # 이미지 로드 및 base64 변환
    img_base64, mime_type = image_to_base64_data(image_path, max_size=max_width, log_func=log_func)

    # Gemini 클라이언트 생성
    client = get_gemini_client(api_key)

    # API 호출
    response = client.models.generate_content(
        model=model,
        contents=[
            types.Part.from_bytes(data=base64.b64decode(img_base64), mime_type=mime_type),
            ANALYSIS_USER_PROMPT  # Gemini SDK는 문자열 직접 전달
        ],
        config=types.GenerateContentConfig(
            system_instruction=ANALYSIS_SYSTEM_INSTRUCTION,
            temperature=0.3,
            max_output_tokens=1024,
        )
    )

    # 결과 파싱
    text = response.text.strip() if response.text else ""

    # JSON 추출 (마크다운 코드블록 제거)
    import re
    text = re.sub(r"^```(?:json)?\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    text = text.strip()

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        result = {"error": "JSON 파싱 실패", "raw_response": text}

    # 토큰 사용량 추출
    usage_metadata = getattr(response, 'usage_metadata', None)
    if usage_metadata:
        result["_usage"] = {
            "input_tokens": getattr(usage_metadata, 'prompt_token_count', 0),
            "output_tokens": getattr(usage_metadata, 'candidates_token_count', 0),
        }

    return result


def compute_cost_usd(model_name: str, input_tokens: int, output_tokens: int) -> float:
    """비용 계산 (USD)"""
    pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
    cost = (input_tokens / 1_000_000) * pricing["input"] + (output_tokens / 1_000_000) * pricing["output"]
    return cost


# =====================================
# Gemini Batch API 관련 함수 (IMG Analysis용)
# - JSONL 업로드 → 배치 생성 → 상태 폴링 → 결과 다운로드 → 병합
# =====================================

import pandas as pd

# Batch API 50% 할인 가격
MODEL_PRICING_BATCH: Dict[str, Dict[str, float]] = {
    "gemini-2.5-flash-lite": {
        "input_per_million": 0.05,    # Batch API 50% 할인
        "output_per_million": 0.20,   # Batch API 50% 할인
    },
    "gemini-2.5-flash-preview-05-20": {
        "input_per_million": 0.075,
        "output_per_million": 0.30,
    },
    "gemini-2.0-flash": {
        "input_per_million": 0.05,
        "output_per_million": 0.20,
    },
}


def compute_batch_cost_usd(
    model_name: str,
    total_input_tokens: int,
    total_output_tokens: int,
) -> Optional[Dict[str, float]]:
    """Batch API 비용 계산 (50% 할인 적용)"""
    pricing = MODEL_PRICING_BATCH.get(model_name)
    if not pricing:
        return None

    in_million = total_input_tokens / 1_000_000.0
    out_million = total_output_tokens / 1_000_000.0

    input_cost = in_million * pricing["input_per_million"]
    output_cost = out_million * pricing["output_per_million"]
    total_cost = input_cost + output_cost

    return {
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": total_cost,
    }


def safe_str(val: Any) -> str:
    """NaN, None 등을 빈 문자열로 변환"""
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    return str(val).strip()


def create_batch_input_jsonl(
    excel_path: str,
    jsonl_path: str,
    max_width: int = OUTPUT_SIZE,
    skip_existing: bool = True,
    skip_bad_label: bool = True,
    log_func=None,
):
    """
    엑셀 파일 → Gemini Batch API용 JSONL 생성 (IMG Analysis).
    이미지를 base64로 인코딩하여 JSONL에 포함.
    """
    df = pd.read_excel(excel_path)

    # 필수 컬럼 확인
    if "IMG_S1_누끼" not in df.columns:
        raise ValueError("필수 컬럼(IMG_S1_누끼)이 없습니다.")

    # 결과 컬럼들
    result_cols = [
        "view_point", "subject_position", "subject_size", "lighting_condition",
        "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
        "bg_layout_hint_en"
    ]

    total_rows = len(df)
    written_count = 0
    skipped_rows = []
    skipped_existing = 0
    skipped_bad = 0
    skipped_no_image = 0

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            # 기존 결과가 있으면 스킵
            if skip_existing:
                val = safe_str(row.get("view_point", ""))
                if val and val != "nan":
                    skipped_existing += 1
                    continue

            # bad 라벨 스킵
            if skip_bad_label:
                human_label = safe_str(row.get("IMG_S1_휴먼라벨", "")).lower()
                ai_label = safe_str(row.get("IMG_S1_AI라벨", "")).lower()
                if human_label == "bad" or ai_label == "bad":
                    skipped_bad += 1
                    continue

            # 이미지 경로 확인
            thumbnail_path = safe_str(row.get("IMG_S1_누끼", ""))
            if not thumbnail_path or thumbnail_path == "nan":
                skipped_no_image += 1
                continue

            if not os.path.exists(thumbnail_path):
                skipped_rows.append({"엑셀_인덱스": idx, "누락항목": "이미지 파일 없음", "경로": thumbnail_path})
                continue

            # 이미지를 base64로 변환
            try:
                img_base64, mime_type = image_to_base64_data(thumbnail_path, max_size=max_width, log_func=None)
            except Exception as e:
                skipped_rows.append({"엑셀_인덱스": idx, "누락항목": f"이미지 변환 실패: {e}", "경로": thumbnail_path})
                continue

            # Gemini Batch JSONL 형식 (이미지 포함)
            request_obj = {
                "key": f"row-{idx}",
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {"inlineData": {"mimeType": mime_type, "data": img_base64}},
                                {"text": ANALYSIS_USER_PROMPT}
                            ]
                        }
                    ],
                    "systemInstruction": {
                        "parts": [{"text": ANALYSIS_SYSTEM_INSTRUCTION}]
                    },
                    "generationConfig": {
                        "temperature": 0.3,
                        "maxOutputTokens": 1024,
                    }
                }
            }

            f.write(json.dumps(request_obj, ensure_ascii=False) + "\n")
            written_count += 1

            if log_func and written_count % 10 == 0:
                log_func(f"JSONL 생성 진행: {written_count}건...")

    # 스킵된 행 저장
    skipped_path = None
    if skipped_rows:
        base, _ = os.path.splitext(excel_path)
        skipped_path = f"{base}_img_analysis_skipped_rows.xlsx"
        df_skipped = pd.DataFrame(skipped_rows)
        df_skipped.to_excel(skipped_path, index=False)

    return {
        "total_rows": total_rows,
        "written_count": written_count,
        "skipped_count": len(skipped_rows),
        "skipped_existing": skipped_existing,
        "skipped_bad": skipped_bad,
        "skipped_no_image": skipped_no_image,
        "skipped_path": skipped_path,
    }


def upload_jsonl_file(
    client,
    jsonl_path: str,
    display_name: str = None,
) -> str:
    """JSONL 파일을 Gemini File API에 업로드."""
    import hashlib
    import tempfile
    import shutil
    from datetime import datetime

    # 한글 경로 인코딩 오류 방지: 임시 디렉토리에 ASCII 파일명으로 복사
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_hash = hashlib.md5(jsonl_path.encode('utf-8')).hexdigest()[:8]
    temp_filename = f"img_analysis_batch_{timestamp}_{file_hash}.jsonl"

    if display_name is None:
        display_name = temp_filename

    # 임시 디렉토리에 복사
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, temp_filename)
    shutil.copy2(jsonl_path, temp_path)

    try:
        uploaded_file = client.files.upload(
            file=temp_path,
            config=types.UploadFileConfig(
                display_name=display_name,
                mime_type="application/jsonl"
            )
        )
        return uploaded_file.name
    finally:
        # 임시 파일 정리
        if os.path.exists(temp_path):
            os.remove(temp_path)


def create_batch_job(
    client,
    model_name: str,
    src_file_name: str,
    display_name: str = None,
) -> Dict[str, Any]:
    """Gemini Batch Job 생성."""
    config = {}
    if display_name:
        config["display_name"] = display_name

    batch_job = client.batches.create(
        model=model_name,
        src=src_file_name,
        config=config if config else None
    )

    return {
        "name": batch_job.name,
        "state": batch_job.state.name if hasattr(batch_job.state, 'name') else str(batch_job.state),
        "create_time": str(getattr(batch_job, 'create_time', '')),
    }


def get_batch_status(
    client,
    batch_name: str,
) -> Dict[str, Any]:
    """Gemini Batch Job 상태 조회."""
    batch_job = client.batches.get(name=batch_name)

    result = {
        "name": batch_job.name,
        "state": batch_job.state.name if hasattr(batch_job.state, 'name') else str(batch_job.state),
    }

    if hasattr(batch_job, 'batch_stats') and batch_job.batch_stats:
        stats = batch_job.batch_stats
        result["total_count"] = getattr(stats, 'total_count', 0)
        result["succeeded_count"] = getattr(stats, 'succeeded_count', 0)
        result["failed_count"] = getattr(stats, 'failed_count', 0)

    if hasattr(batch_job, 'dest') and batch_job.dest:
        if hasattr(batch_job.dest, 'file_name') and batch_job.dest.file_name:
            result["output_file_name"] = batch_job.dest.file_name

    return result


def download_batch_results(
    client,
    output_file_name: str,
    local_path: str,
) -> str:
    """Gemini Batch 결과 파일 다운로드."""
    file_content = client.files.download(file=output_file_name)

    if hasattr(file_content, 'read'):
        content = file_content.read()
    elif hasattr(file_content, 'content'):
        content = file_content.content
    else:
        content = file_content

    if isinstance(content, bytes):
        content = content.decode('utf-8')

    with open(local_path, "w", encoding="utf-8") as f:
        f.write(content)

    return local_path


def parse_batch_results(
    results_jsonl_path: str,
) -> list:
    """Gemini Batch 결과 JSONL 파싱."""
    results = []

    with open(results_jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    result = json.loads(line)
                    results.append(result)
                except json.JSONDecodeError:
                    continue

    return results


def extract_img_analysis_from_response_dict(resp: Dict[str, Any]) -> Dict[str, Any]:
    """Gemini Batch 결과에서 이미지 분석 결과 추출."""
    import re
    try:
        response = resp.get("response", {})

        candidates = response.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if parts:
                text = parts[0].get("text", "")
                text = text.strip()

                # JSON 추출 (마크다운 코드블록 제거)
                text = re.sub(r"^```(?:json)?\n?", "", text)
                text = re.sub(r"\n?```$", "", text)
                text = text.strip()

                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"error": "JSON 파싱 실패", "raw": text}

        if response.get("text"):
            text = response["text"].strip()
            text = re.sub(r"^```(?:json)?\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
            text = text.strip()

            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"error": "JSON 파싱 실패", "raw": text}

    except Exception:
        pass
    return {}


def extract_usage_from_response_dict(resp: Dict[str, Any]) -> tuple:
    """Gemini Batch 결과에서 토큰 사용량 추출."""
    try:
        response = resp.get("response", {})
        usage = response.get("usageMetadata", {})
        in_tok = int(usage.get("promptTokenCount", 0))
        out_tok = int(usage.get("candidatesTokenCount", 0))
        cached_tok = int(usage.get("cachedContentTokenCount", 0))
        return in_tok, out_tok, cached_tok
    except Exception:
        return 0, 0, 0


def merge_results_to_excel(
    excel_path: str,
    results: list,
    output_path: str,
) -> tuple:
    """
    Gemini Batch 결과를 엑셀에 병합.
    Returns: (병합된 행 수, 총 입력 토큰, 총 출력 토큰)
    """
    df = pd.read_excel(excel_path)

    result_cols = [
        "view_point", "subject_position", "subject_size", "lighting_condition",
        "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
        "bg_layout_hint_en"
    ]

    for col in result_cols:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)

    cnt = 0
    total_input_tokens = 0
    total_output_tokens = 0

    for result in results:
        key = result.get("key", "")

        analysis_data = extract_img_analysis_from_response_dict(result)
        in_tok, out_tok, _ = extract_usage_from_response_dict(result)
        total_input_tokens += in_tok
        total_output_tokens += out_tok

        if analysis_data and not analysis_data.get("error"):
            try:
                if key.startswith("row-"):
                    idx = int(key.split("-")[1])
                else:
                    idx = int(key.split("_")[1])

                if 0 <= idx < len(df):
                    for col in result_cols:
                        if col in analysis_data:
                            val = analysis_data[col]
                            if isinstance(val, bool):
                                df.at[idx, col] = str(val).lower()
                            else:
                                df.at[idx, col] = str(val) if val else ""
                    cnt += 1
            except Exception:
                pass

    df.to_excel(output_path, index=False)
    return cnt, total_input_tokens, total_output_tokens


# 배치 상태 상수
BATCH_STATE_PENDING = "JOB_STATE_PENDING"
BATCH_STATE_RUNNING = "JOB_STATE_RUNNING"
BATCH_STATE_SUCCEEDED = "JOB_STATE_SUCCEEDED"
BATCH_STATE_FAILED = "JOB_STATE_FAILED"
BATCH_STATE_CANCELLED = "JOB_STATE_CANCELLED"

COMPLETED_STATES = [BATCH_STATE_SUCCEEDED, BATCH_STATE_FAILED, BATCH_STATE_CANCELLED]


def is_batch_completed(state: str) -> bool:
    """배치가 완료되었는지 확인."""
    return state in COMPLETED_STATES


def is_batch_succeeded(state: str) -> bool:
    """배치가 성공했는지 확인."""
    return state == BATCH_STATE_SUCCEEDED
