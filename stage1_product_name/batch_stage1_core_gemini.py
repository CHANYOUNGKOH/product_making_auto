# batch_stage1_core_gemini.py
"""
STAGE1 Batch API 핵심 로직 (Gemini 2.5 Flash-Lite 버전)
- Gemini Batch API 사용 (비용 50% 절감)
- JSONL 업로드 → 배치 생성 → 상태 폴링 → 결과 다운로드 → 병합
- GPT Batch API와 동일한 워크플로우
"""

import os
import json
import time
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

# Gemini API
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# =====================================
# Gemini 최적화: System Instruction + Few-Shot 예시
# =====================================
STAGE1_SYSTEM_INSTRUCTION = """당신은 온라인 쇼핑몰 위탁판매용 상품명 정제 전문가입니다.
입력된 정보를 바탕으로 광고와 브랜드 거품을 제거하고, 검색에 강한 정보 중심 상품명 한 줄을 작성하십시오.

## 핵심 원칙
- 원본에 명시된 정보만 사용 (추측/유추/추론 금지)
- 브랜드/쇼핑몰명/광고 문구 제거
- 결과는 "명사형 키워드 나열" 형태의 상품명 한 줄
- 띄어쓰기 포함 최대 60자

## 정제 규칙
1. 구조: [핵심 제품군] > [기능/사용상황] > [규격/수량] > [대상/스타일]
2. 노이즈 제거: 무료배송, 특가, 인기, 추천, 최저가, 핫딜, ○○몰, ○○샵 등
3. 브랜드 제거: 나이키, 아디다스 등 대중 브랜드명
4. 기호 제거: ♥, ★, !!, ??, [], / 등 불필요 기호
5. 유지 대상: 재질/특성/기능 단어 (피치기모, 방수, DIY 등)

## 출력 형식
- 정제된 상품명 텍스트 **한 줄만** 출력
- 라벨, 설명, 문장형 표현 없이 순수한 상품명만
- 모든 출력이 동일한 스타일/톤 유지

## Few-Shot 예시

### 예시 1
입력: 카테고리=여성의류, 판매형태=옵션형, 원본상품명=★무료배송★ [공식몰] 프리미엄 겨울 기모 레깅스 여성용 1+1 특가!!
출력: 겨울 기모 레깅스 여성용 1+1

### 예시 2
입력: 카테고리=캠핑용품, 판매형태=단일, 원본상품명=(당일출고) ○○스토어 캠핑 대용량 아이스박스 25L 쿨러 가성비 추천
출력: 캠핑 아이스박스 25L 쿨러

### 예시 3
입력: 카테고리=남성의류, 판매형태=옵션형, 원본상품명=남성 반팔 티셔츠 여름 인기 베스트 할인
출력: 남성 반팔 티셔츠 여름

### 예시 4
입력: 카테고리=전자제품, 판매형태=단일, 원본상품명=무선 블루투스 이어폰 노이즈캔슬링 특가
출력: 무선 블루투스 이어폰 노이즈캔슬링

### 예시 5
입력: 카테고리=생활용품, 판매형태=단일, 원본상품명=주방 일회용 위생장갑 100매 대용량 특가
출력: 일회용 위생장갑 100매

### 예시 6
입력: 카테고리=침구류, 판매형태=옵션형, 원본상품명=여름 냉감 이불 싱글 150×200cm 시원한 추천
출력: 여름 냉감 이불 싱글 150×200cm

### 예시 7
입력: 카테고리=반려동물, 판매형태=단일, 원본상품명=고양이 스크래처 골판지 리필 3개입 인기
출력: 고양이 스크래처 골판지 리필 3개

위 규칙과 예시를 따라 정제된 상품명 한 줄만 출력하십시오."""

# User 프롬프트 템플릿 (동적 데이터만 포함)
STAGE1_USER_PROMPT_TEMPLATE = """[입력 정보]
- 카테고리명: {category}
- 판매형태: {sale_type}
- 원본 상품명: {raw_name}

위 정보를 바탕으로 정제된 상품명 한 줄을 출력하세요."""


def safe_str(v: Any) -> str:
    """NaN/None 안전하게 문자열로 변환 + strip."""
    if v is None:
        return ""
    try:
        import pandas as pd
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def fmt_safe(v: Any) -> str:
    """str(v)를 .format()에 안전하게 넣기 위한 이스케이프."""
    s = safe_str(v)
    return s.replace("{", "{{").replace("}", "}}")


# API 키 파일 경로
API_KEY_FILE = ".gemini_api_key_stage1_batch"

# 기본 모델
DEFAULT_MODEL = "gemini-2.5-flash-lite"

# Gemini 모델별 가격 (USD / 1M tokens) - Batch API 50% 할인 적용
MODEL_PRICING: Dict[str, Dict[str, float]] = {
    "gemini-2.5-flash-lite": {
        "input_per_million": 0.05,    # Batch API 50% 할인
        "output_per_million": 0.20,   # Batch API 50% 할인
    },
    "gemini-2.5-flash-preview-05-20": {
        "input_per_million": 0.075,   # 50% 할인
        "output_per_million": 0.30,
    },
    "gemini-2.0-flash": {
        "input_per_million": 0.05,    # 50% 할인
        "output_per_million": 0.20,
    },
}


def compute_cost_usd(
    model_name: str,
    total_input_tokens: int,
    total_output_tokens: int,
) -> Optional[Dict[str, float]]:
    """모델별 토큰 단가를 이용해 대략적인 비용(USD) 계산."""
    pricing = MODEL_PRICING.get(model_name)
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


# =====================================
# API 키 로드/저장
# =====================================
def load_api_key_from_file() -> str:
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""


def save_api_key_to_file(key: str) -> None:
    try:
        with open(API_KEY_FILE, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")


# =====================================
# 응답 텍스트 / 사용량 추출 (Gemini Batch 형식)
# =====================================
def extract_text_from_response_dict(resp: Dict[str, Any]) -> str:
    """Gemini Batch 결과에서 텍스트 추출."""
    try:
        # Batch API 결과 형식
        response = resp.get("response", {})

        # candidates에서 텍스트 추출
        candidates = response.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if parts:
                text = parts[0].get("text", "")
                return text.strip().splitlines()[0].strip()

        # 직접 text 필드가 있는 경우
        if response.get("text"):
            return response["text"].strip().splitlines()[0].strip()

    except Exception:
        pass
    return ""


def extract_usage_from_response_dict(resp: Dict[str, Any]) -> Tuple[int, int, int]:
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


# =====================================
# Gemini Batch API 클라이언트 생성
# =====================================
def get_gemini_client(api_key: Optional[str] = None):
    """Gemini 클라이언트 생성."""
    if not GEMINI_AVAILABLE:
        raise ImportError("google-genai 패키지가 설치되지 않았습니다.")

    if not api_key:
        api_key = load_api_key_from_file()

    if not api_key:
        raise ValueError("Gemini API 키가 설정되지 않았습니다.")

    return genai.Client(api_key=api_key)


# =====================================
# Batch API 핵심 로직 (Gemini)
# =====================================
def create_batch_input_jsonl(
    excel_path: str,
    jsonl_path: str,
    model_name: str = DEFAULT_MODEL,
    skip_existing: bool = True,
):
    """
    엑셀 파일(원본상품명, 카테고리명, 판매형태) → Gemini Batch API용 JSONL 생성.
    """
    df = pd.read_excel(excel_path)

    required_cols = ["원본상품명", "카테고리명", "판매형태"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"엑셀에 필수 컬럼이 없습니다: {col}")

    total_rows = len(df)
    written_count = 0
    skipped_rows: List[Dict[str, Any]] = []
    skipped_existing = 0

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            # 기존 결과가 있으면 스킵
            if skip_existing and "ST1_결과상품명" in df.columns:
                existing = safe_str(row.get("ST1_결과상품명", ""))
                if existing and existing != "nan":
                    skipped_existing += 1
                    continue

            raw_name = safe_str(row["원본상품명"])
            category = safe_str(row["카테고리명"])
            sale_type = safe_str(row["판매형태"])

            missing_fields = []
            if not category:
                missing_fields.append("카테고리명")
            if not sale_type:
                missing_fields.append("판매형태")
            if not raw_name:
                missing_fields.append("원본상품명")

            if missing_fields:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": ", ".join(missing_fields),
                    "카테고리명": category,
                    "판매형태": sale_type,
                    "원본상품명": raw_name,
                })
                continue

            # Gemini Batch JSONL 형식
            user_prompt = STAGE1_USER_PROMPT_TEMPLATE.format(
                category=fmt_safe(category),
                sale_type=fmt_safe(sale_type),
                raw_name=fmt_safe(raw_name)
            )

            # Gemini Batch API 요청 형식
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
                        "parts": [{"text": STAGE1_SYSTEM_INSTRUCTION}]
                    },
                    "generationConfig": {
                        "temperature": 0.3,
                        "maxOutputTokens": 256,
                    }
                }
            }

            f.write(json.dumps(request_obj, ensure_ascii=False) + "\n")
            written_count += 1

    # 스킵된 행 저장
    skipped_path = None
    if skipped_rows:
        base, _ = os.path.splitext(excel_path)
        skipped_path = f"{base}_stage1_skipped_rows.xlsx"
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
    """
    JSONL 파일을 Gemini File API에 업로드.
    Returns: 업로드된 파일의 name (URI)
    """
    import hashlib
    import tempfile
    import shutil
    from datetime import datetime

    # 한글 경로 인코딩 오류 방지: 임시 디렉토리에 ASCII 파일명으로 복사
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_hash = hashlib.md5(jsonl_path.encode('utf-8')).hexdigest()[:8]
    temp_filename = f"stage1_batch_{timestamp}_{file_hash}.jsonl"

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
    """
    Gemini Batch Job 생성.
    Returns: 배치 작업 정보
    """
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
    """
    Gemini Batch Job 상태 조회.
    """
    batch_job = client.batches.get(name=batch_name)

    result = {
        "name": batch_job.name,
        "state": batch_job.state.name if hasattr(batch_job.state, 'name') else str(batch_job.state),
    }

    # 완료 통계
    if hasattr(batch_job, 'batch_stats') and batch_job.batch_stats:
        stats = batch_job.batch_stats
        result["total_count"] = getattr(stats, 'total_count', 0)
        result["succeeded_count"] = getattr(stats, 'succeeded_count', 0)
        result["failed_count"] = getattr(stats, 'failed_count', 0)

    # 결과 파일 정보
    if hasattr(batch_job, 'dest') and batch_job.dest:
        if hasattr(batch_job.dest, 'file_name') and batch_job.dest.file_name:
            result["output_file_name"] = batch_job.dest.file_name

    return result


def download_batch_results(
    client,
    output_file_name: str,
    local_path: str,
) -> str:
    """
    Gemini Batch 결과 파일 다운로드.
    Returns: 저장된 로컬 파일 경로
    """
    file_content = client.files.download(file=output_file_name)

    # 파일 내용 저장
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
) -> List[Dict[str, Any]]:
    """
    Gemini Batch 결과 JSONL 파싱.
    """
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


def merge_results_to_excel(
    excel_path: str,
    results: List[Dict],
    output_path: str,
) -> Tuple[int, int, int]:
    """
    Gemini Batch 결과를 엑셀에 병합.
    Returns: (병합된 행 수, 총 입력 토큰, 총 출력 토큰)
    """
    df = pd.read_excel(excel_path)

    if "ST1_결과상품명" not in df.columns:
        df["ST1_결과상품명"] = ""
    df["ST1_결과상품명"] = df["ST1_결과상품명"].astype(str)

    cnt = 0
    total_input_tokens = 0
    total_output_tokens = 0

    for result in results:
        key = result.get("key", "")

        # 텍스트 추출
        text = extract_text_from_response_dict(result)

        # 토큰 사용량 추출
        in_tok, out_tok, _ = extract_usage_from_response_dict(result)
        total_input_tokens += in_tok
        total_output_tokens += out_tok

        if text:
            try:
                if key.startswith("row-"):
                    idx = int(key.split("-")[1])
                else:
                    idx = int(key.split("_")[1])

                if 0 <= idx < len(df):
                    df.at[idx, "ST1_결과상품명"] = text
                    cnt += 1
            except Exception:
                pass

    df.to_excel(output_path, index=False)
    return cnt, total_input_tokens, total_output_tokens


# =====================================
# 배치 상태 상수
# =====================================
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
