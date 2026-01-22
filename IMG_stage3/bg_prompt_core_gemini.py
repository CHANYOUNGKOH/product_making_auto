"""
bg_prompt_core_gemini.py

Stage BG_PROMPT: ST2_JSON(한국어) → 배경/영상 프롬프트 JSON(영문) 생성 코어 모듈 (Gemini 버전)

- Gemini API를 호출해서
  bg_positive_en / bg_negative_en / video_motion_prompt_en / video_full_prompt_en
  4개 필드를 가진 JSON을 반환한다.
- Implicit Caching 자동 적용 (System Instruction 동일)
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, Optional

# Gemini API
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# === 기본 설정 ===
API_KEY_FILE = ".gemini_api_key_bg_prompt"
DEFAULT_MODEL = "gemini-2.5-flash-lite"

# 모델별 가격 (USD per Million Tokens)
MODEL_PRICING_USD_PER_MTOK = {
    "gemini-2.5-flash-lite": {"input": 0.05, "output": 0.20},
    "gemini-2.5-flash-preview-05-20": {"input": 0.15, "output": 0.60},
    "gemini-2.0-flash": {"input": 0.10, "output": 0.40},
}


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


# === System Instruction (Gemini 최적화) ===
BG_SYSTEM_INSTRUCTION = r"""You are the AI Art Director for an e-commerce automation pipeline.

TASK:
Combine ST2_JSON (product context) and IMG_ANALYSIS_JSON (visual structure) to generate 4 English prompts for background generation.

OUTPUT FORMAT:
Return exactly one JSON object (no markdown):
{
  "bg_positive_en": "...",
  "bg_negative_en": "...",
  "video_motion_prompt_en": "...",
  "video_full_prompt_en": "..."
}

[CORE PRINCIPLES]

1) Structure Invariance
   - The base structure MUST come from IMG_ANALYSIS_JSON.bg_layout_hint_en.
   - Do NOT alter the physical objects or perspective described.

2) Context Overlay
   - Add only material/place adjectives from ST2_JSON.
   - Do NOT introduce new objects.

3) Negative Space
   - NEVER describe the product. Only describe empty environment, surface, lighting, and mood.

[FIELD RULES]

1) bg_positive_en
   - MUST start with: "Empty"
   - Structure: "Empty [bg_layout_hint_en adjusted], [perspective keywords], [empty area phrase], [lighting], product photography background, depth of field, 8k, high resolution, masterpiece"
   - Perspective keywords:
     * top_down: "flat lay view", "top down perspective", "single surface only"
     * front_view: "eye level shot", "front view", "surface with wall background"
     * angled: "angled perspective", "isometric depth"
     * side_view: "side profile view"
   - Empty area phrase: "clear empty center area for the product"

2) bg_negative_en
   - Output ONLY comma-separated single-word nouns.
   - Base list: text, watermark, logo, brand, human, person, face, hands, mannequin, pets, clutter, dirt, distortion, noise, blur, lowquality
   - Add product nouns from ST2_JSON (translated to single English nouns).

3) video_motion_prompt_en
   - If is_flat_lay == true:
       "Static top-down camera with almost no movement or very subtle slow rotation, smooth 5 second loop, no camera shake"
   - Else:
       "Static camera with very subtle slow zoom in matching the perspective, smooth 5 second loop, no camera shake"

4) video_full_prompt_en
   - Construct a NEW sentence with perspective + lighting + empty area phrase + motion prompt.
   - End with: "high quality 5 second loop video, no people, no text, no logo, no morphing, no distortion"

[FINAL CHECK]
- Output valid JSON only.
- All values must be English.
- bg_positive_en MUST start with "Empty"."""

# User 프롬프트 템플릿
BG_USER_PROMPT_TEMPLATE = """[INPUT DATA]

1) ST2_JSON (Product Context):
{st2_json}

2) IMG_ANALYSIS_JSON (Visual Structure):
{img_analysis_json}

Generate the 4-field JSON output as specified."""


def generate_bg_prompt(
    st2_json: str,
    img_analysis_json: str,
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    log_func=None,
) -> Dict[str, Any]:
    """
    ST2_JSON과 IMG_ANALYSIS_JSON을 입력받아 배경 프롬프트 JSON을 생성합니다.

    Args:
        st2_json: Stage 2에서 생성된 JSON 문자열
        img_analysis_json: 이미지 분석 JSON 문자열
        api_key: Gemini API 키 (None이면 파일/환경변수에서 로드)
        model: 사용할 모델명
        log_func: 로그 출력 함수

    Returns:
        배경 프롬프트 딕셔너리
    """
    if not GEMINI_AVAILABLE:
        raise ImportError("google-genai 패키지가 설치되지 않았습니다.")

    # 사용자 프롬프트 생성
    user_prompt = BG_USER_PROMPT_TEMPLATE.format(
        st2_json=st2_json,
        img_analysis_json=img_analysis_json
    )

    # Gemini 클라이언트 생성
    client = get_gemini_client(api_key)

    # API 호출
    response = client.models.generate_content(
        model=model,
        contents=[{"role": "user", "parts": [{"text": user_prompt}]}],
        config=types.GenerateContentConfig(
            system_instruction=BG_SYSTEM_INSTRUCTION,
            temperature=0.3,
            max_output_tokens=2048,
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
# Gemini Batch API 관련 함수 (BG Prompt용)
# - GPT Batch API와 동일한 워크플로우
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
    skip_existing: bool = True,
):
    """
    엑셀 파일 → Gemini Batch API용 JSONL 생성 (BG Prompt).
    """
    df = pd.read_excel(excel_path)

    # 필수 컬럼 확인
    if "ST2_JSON" not in df.columns:
        raise ValueError("필수 컬럼(ST2_JSON)이 없습니다.")

    # 이미지 분석 컬럼들
    img_analysis_cols = [
        "view_point", "subject_position", "subject_size", "lighting_condition",
        "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
        "bg_layout_hint_en"
    ]

    result_cols = ["bg_positive_en", "bg_negative_en", "video_motion_prompt_en", "video_full_prompt_en"]

    total_rows = len(df)
    written_count = 0
    skipped_rows = []
    skipped_existing = 0

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            # 기존 결과가 있으면 스킵
            if skip_existing:
                has_result = False
                for col in result_cols:
                    if col in df.columns:
                        val = safe_str(row.get(col, ""))
                        if val and val != "nan":
                            has_result = True
                            break
                if has_result:
                    skipped_existing += 1
                    continue

            # ST2_JSON 확인
            st2_json = safe_str(row.get("ST2_JSON", ""))
            if not st2_json:
                skipped_rows.append({"엑셀_인덱스": idx, "누락항목": "ST2_JSON"})
                continue

            # IMG_ANALYSIS_JSON 구성
            img_analysis = {}
            for col in img_analysis_cols:
                if col in df.columns:
                    val = safe_str(row.get(col, ""))
                    if val:
                        if col == "is_flat_lay":
                            img_analysis[col] = val.lower() == "true"
                        else:
                            img_analysis[col] = val

            if not img_analysis.get("view_point"):
                skipped_rows.append({"엑셀_인덱스": idx, "누락항목": "view_point"})
                continue

            img_analysis_json = json.dumps(img_analysis, ensure_ascii=False)

            # User 프롬프트 생성
            user_prompt = BG_USER_PROMPT_TEMPLATE.format(
                st2_json=st2_json,
                img_analysis_json=img_analysis_json
            )

            # Gemini Batch JSONL 형식
            request_obj = {
                "key": f"row-{idx}",
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [{"text": user_prompt}]
                        }
                    ],
                    "systemInstruction": {
                        "parts": [{"text": BG_SYSTEM_INSTRUCTION}]
                    },
                    "generationConfig": {
                        "temperature": 0.3,
                        "maxOutputTokens": 2048,
                    }
                }
            }

            f.write(json.dumps(request_obj, ensure_ascii=False) + "\n")
            written_count += 1

    # 스킵된 행 저장
    skipped_path = None
    if skipped_rows:
        base, _ = os.path.splitext(excel_path)
        skipped_path = f"{base}_bg_prompt_skipped_rows.xlsx"
        df_skipped = pd.DataFrame(skipped_rows)
        df_skipped.to_excel(skipped_path, index=False)

    return {
        "total_rows": total_rows,
        "written_count": written_count,
        "skipped_count": len(skipped_rows),
        "skipped_existing": skipped_existing,
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
    temp_filename = f"bg_prompt_batch_{timestamp}_{file_hash}.jsonl"

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


def extract_bg_prompt_from_response_dict(resp: Dict[str, Any]) -> Dict[str, str]:
    """Gemini Batch 결과에서 BG 프롬프트 추출."""
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

    result_cols = ["bg_positive_en", "bg_negative_en", "video_motion_prompt_en", "video_full_prompt_en"]

    for col in result_cols:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)

    cnt = 0
    total_input_tokens = 0
    total_output_tokens = 0

    for result in results:
        key = result.get("key", "")

        bg_data = extract_bg_prompt_from_response_dict(result)
        in_tok, out_tok, _ = extract_usage_from_response_dict(result)
        total_input_tokens += in_tok
        total_output_tokens += out_tok

        if bg_data and not bg_data.get("error"):
            try:
                if key.startswith("row-"):
                    idx = int(key.split("-")[1])
                else:
                    idx = int(key.split("_")[1])

                if 0 <= idx < len(df):
                    for col in result_cols:
                        if col in bg_data:
                            df.at[idx, col] = bg_data[col]
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
