"""
stage3_core_gemini.py

- Stage 3(최종 상품명 생성)를 위한 프롬프트 템플릿과
  엑셀 한 행(row) → Stage3Request 변환 유틸을 모아둔 모듈.
- Gemini 2.5 Flash-Lite 최적화 버전
  * Few-Shot 예시 추가 (Gemini 필수)
  * 규칙 압축: 장황한 설명 → 핵심만 간결하게
  * 톤 일관성 명시
  * System Instruction을 모든 요청에 동일하게 배치 (Implicit Caching)

※ 사용 시나리오 (예상)
  1) Stage 2까지 완료된 엑셀 파일에는 최소한 다음 컬럼이 존재한다고 가정:
     - ST2_JSON : Stage 2에서 추출된 JSON 문자열

  2) (선택) Stage 3 전용 설정 컬럼을 추가로 둘 수 있다.
     - ST3_마켓        : 네이버 / 쿠팡 / 11번가 / 지마켓 / 옥션 / 기타 등
     - ST3_최대글자수  : 정수 (없으면 기본 50)
     - ST3_출력개수    : 정수 (없으면 자동, 대략 10개)
     - ST3_명명전략    : "통합형" / "옵션포함형" 등 (없으면 기본 "통합형")

  3) Stage3 GUI(or 배치 코드)에서
     - pandas로 엑셀을 로드한 뒤, 각 행에 대해
         req = build_stage3_request_from_row(row, default_settings)
       를 호출하여 system_instruction과 user_prompt를 얻고,
     - 이들을 Gemini Batch API에 넘긴다.

  4) 모델 응답은 "한 줄당 하나의 상품명"만 있는 텍스트 블록이어야 하며,
     JSON이나 설명 텍스트를 포함하면 안 된다는 점을 프롬프트에서 명확히 지시한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd


# ============================================================
#  Stage 3 프롬프트 템플릿 (Gemini 2.5 Flash-Lite 최적화 버전)
#  - Few-Shot 예시 포함 (Gemini 필수)
#  - 규칙 압축 및 핵심만 간결하게
#  - 톤 일관성 명시
#  - System Instruction + User Contents 분리 (Implicit Caching)
# ============================================================

# System Instruction (정적, 모든 요청에서 동일 - Implicit Caching 대상)
STAGE3_SYSTEM_INSTRUCTION = """당신은 위탁판매 상품의 최종 상품명을 짓는 전문 카피라이터입니다.
Stage 2 JSON 데이터를 바탕으로 오픈마켓(네이버/쿠팡)용 상품명 후보를 생성합니다.

## 핵심 규칙 (Strict)
1. 브랜드명/쇼핑몰명/제조사명 절대 금지
2. 광고어 금지: 무료배송, 특가, 세일, 1+1, 최저가, 인기템 등
3. 금지 기호: / , - [ ] ( ) ~ ♥ ★ !! ?? (단, 사이즈 'x'는 허용: 50x250cm)
4. JSON에 없는 정보 추가 금지 (재질, 인증, 수량 등)
5. 옵션 나열 금지 → 통합형 대표명으로 작성

## 네이밍 패턴
[핵심카테고리] + [주요기능 1-2개] + [사용상황/차별점 1개]

## 마켓별 톤
- 네이버: 검색 키워드 자연스럽게 연결
- 쿠팡: 상품정체 + 핵심기능 직관적 구조

## 출력 형식
- 번호, 불릿, 따옴표 없이 상품명만 한 줄에 하나씩
- 모든 출력이 동일한 스타일/톤 유지

## Few-Shot 예시

### 예시 1: 귀마개 (JSON 입력 → 출력)
입력 JSON:
{"core_attributes": {"category": "귀마개", "material": "니트", "features": ["방한", "보온"]}, "usage_scenarios": ["겨울 외출", "스키장"], "search_keywords": ["겨울귀마개", "방한귀마개", "니트귀마개"]}

출력:
겨울 방한 니트 귀마개 보온 스키장용
니트 방한 귀마개 겨울 외출 보온
스키장 방한 니트 귀마개 겨울용

### 예시 2: 벽지 (JSON 입력 → 출력)
입력 JSON:
{"core_attributes": {"category": "폼벽지", "size": "50x250cm", "features": ["단열", "쿠션", "셀프시공"]}, "usage_scenarios": ["실내 인테리어", "리모델링"], "search_keywords": ["폼단열벽지", "셀프시공벽지", "쿠션벽지"]}

출력:
폼 단열 쿠션 벽지 50x250cm 셀프시공
셀프시공 폼 단열 벽지 인테리어용
쿠션 단열 폼벽지 50x250cm 리모델링

### 예시 3: 손목시계 (JSON 입력 → 출력)
입력 JSON:
{"core_attributes": {"category": "손목시계", "material": "우레탄밴드", "features": ["무소음", "아날로그"]}, "usage_scenarios": ["수능시험", "학교"], "search_keywords": ["수능시계", "무소음시계", "학생시계"]}

출력:
무소음 수능 손목시계 우레탄밴드 학생용
수능시험 무소음 아날로그 손목시계
학생 무소음 손목시계 우레탄밴드 수능용"""

# User 프롬프트 템플릿 (동적 데이터만 포함)
STAGE3_USER_PROMPT_TEMPLATE = """[설정]
- 마켓: {market}
- 최대 글자수: {max_len}자
- 출력 개수: {num_candidates}개

[입력 데이터 (JSON)]
{json_body}

위 JSON의 core_attributes, usage_scenarios, search_keywords, naming_seeds를 활용하여 상품명 후보를 생성하세요.
규칙과 Few-Shot 예시의 스타일을 따라 출력하세요."""


# 기존 프롬포트 (단일 프롬프트 버전, 참고용)
STAGE3_PROMPT_TEMPLATE = """
당신은 위탁판매 상품의 '최종 상품명'을 짓는 전문 카피라이터(Stage 3)입니다.
Stage 2에서 생성된 JSON 데이터를 바탕으로, 오픈마켓(네이버/쿠팡 등)에서 클릭과 구매를 유도하는 상품명 후보를 생성하십시오.

[입력 설정]
- 마켓: "{market}"
- 최대 글자수: {max_len}  (미설정·0이면 50자로 간주)
- 출력 개수: {num_candidates}개
- 명명 전략: "{naming_strategy}" (실제 생성은 항상 통합형 대표명으로 작성)

[입력 데이터 (JSON)]
- 아래 JSON의 core_attributes, usage_scenarios, search_keywords, naming_seeds를 모두 상품명 재료로 활용하십시오.
{json_body}

[핵심 규칙]
1) 브랜드명/쇼핑몰명/제조사명 절대 금지
2) 광고어 금지: 무료배송, 특가, 세일, 1+1, 최저가, 인기템, MD추천 등
3) 금지 기호: / , - [ ] ( ) ~ ♥ ★ !! ?? (단, 사이즈 'x'는 허용)
4) JSON에 없는 정보 추가 금지
5) 옵션 나열 금지 → 통합형 대표명으로 작성

[네이밍 패턴]
- 구조: [핵심카테고리] + [주요기능 1-2개] + [사용상황/차별점 1개]
- search_keywords의 중요 키워드를 앞부분에 자연스럽게 배치

[마켓별 톤]
- 네이버: 검색 키워드 자연스럽게 연결
- 쿠팡: 상품정체 + 핵심기능 직관적 구조

[출력 형식]
- 번호, 불릿, 따옴표 없이 상품명만 한 줄에 하나씩
- 출력 개수 이내에서 고품질 후보만 생성
"""


# ============================================================
#  유틸 함수: safe_str, fmt_safe
#  - Stage 2와 동일 패턴 유지 (NaN → "", str 변환, 중괄호 이스케이프)
# ============================================================
def safe_str(v: Any) -> str:
    """
    NaN/None 안전하게 문자열로 변환 + strip.
    """
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def fmt_safe(v: Any) -> str:
    """
    str(v)를 한 번 감싼 뒤, .format()에 안전하게 넣기 위한 이스케이프.
    - { → {{, } → }}
    """
    s = safe_str(v)
    return s.replace("{", "{{").replace("}", "}}")


# ============================================================
#  dataclass 정의
# ============================================================
@dataclass
class Stage3Settings:
    """
    Stage 3 전역/기본 설정.
    - market         : 마켓명 (네이버 / 쿠팡 / 11번가 / 지마켓 / 옥션 / 기타)
    - max_len        : 최대 글자 수 (None 또는 0/이하는 기본 50으로 처리)
    - num_candidates : 출력 후보 개수 (None이면 "자동", 대략 10개)
    - naming_strategy: "통합형" / "옵션포함형" / 기타(향후 확장용)
    """
    market: str = "네이버"
    max_len: int = 50
    num_candidates: Optional[int] = None
    naming_strategy: str = "통합형"


@dataclass
class Stage3Request:
    """
    Stage 3 한 행(row)에 대한 최종 요청 정보 (Gemini 최적화 버전).
    - system_instruction : 시스템 지시문 (정적, 모든 요청에서 동일 - Implicit Caching)
    - user_prompt        : 사용자 프롬프트 (동적 데이터 포함)
    - market             : 해석용 마켓명
    - max_len            : 실제로 지키도록 요구할 최대 글자 수
    - num_candidates     : 요구 후보 수 (None이면 자동)
    - naming_strategy    : 통합형 / 옵션포함형 등
    - raw_json           : ST2_JSON 원본 문자열 (디버깅용)
    """
    system_instruction: str
    user_prompt: str
    market: str
    max_len: int
    num_candidates: Optional[int]
    naming_strategy: str
    raw_json: str


# ============================================================
#  내부 유틸: 정수 파싱
# ============================================================
def _coerce_positive_int(v: Any) -> Optional[int]:
    """
    엑셀 셀 값 등에서 양의 정수로 해석 가능한 경우 int로 변환, 아니면 None.
    - "50", "50.0", 50, 50.0 등은 50으로 변환
    - 0, 음수, 빈 문자열, NaN, None 등은 None
    """
    if v is None:
        return None
    try:
        # pandas NaN 처리
        if isinstance(v, float) and pd.isna(v):
            return None
        if isinstance(v, (int,)):
            if v <= 0:
                return None
            return int(v)
        if isinstance(v, float):
            if v <= 0:
                return None
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        val = float(s)
        if val <= 0:
            return None
        return int(val)
    except Exception:
        return None


# ============================================================
#  Stage3 설정/프롬프트 빌더
# ============================================================
def build_stage3_settings_from_row(
    row: "pd.Series",
    default: Optional[Stage3Settings] = None,
) -> Stage3Settings:
    """
    한 행(row)에서 ST3_* 관련 컬럼을 읽어 Stage3Settings를 구성.
    - default가 주어지면, 행 단위 설정이 비어 있을 때 그 값을 사용.
    - default가 없으면, Stage3Settings() 기본값 사용.
    """
    if default is None:
        default = Stage3Settings()

    # 1) 마켓
    market_cell = safe_str(row.get("ST3_마켓", ""))
    market = market_cell or default.market or "네이버"

    # 2) 최대글자수
    max_len_cell = row.get("ST3_최대글자수", None)
    max_len_parsed = _coerce_positive_int(max_len_cell)
    if max_len_parsed is None:
        max_len_parsed = _coerce_positive_int(default.max_len) or 50

    # 3) 출력개수
    num_candidates_cell = row.get("ST3_출력개수", None)
    num_candidates_parsed = _coerce_positive_int(num_candidates_cell)
    if num_candidates_parsed is None:
        # default.num_candidates가 None이면 그대로 None 유지 → "자동" 모드
        num_candidates_parsed = default.num_candidates

    # 4) 명명전략
    strategy_cell = safe_str(row.get("ST3_명명전략", ""))
    naming_strategy = strategy_cell or default.naming_strategy or "통합형"

    return Stage3Settings(
        market=market,
        max_len=max_len_parsed,
        num_candidates=num_candidates_parsed,
        naming_strategy=naming_strategy,
    )


def build_stage3_prompt(
    json_body: str,
    settings: Stage3Settings,
) -> tuple[str, str]:
    """
    Stage 2 JSON 문자열(json_body)과 Stage3Settings를 받아
    system_instruction과 user_prompt를 생성 (Gemini 최적화 버전).

    Returns:
        (system_instruction, user_prompt) 튜플
    """
    if not json_body:
        raise ValueError("Stage 3 프롬프트를 만들기 위해서는 ST2_JSON(내용)이 필요합니다.")

    # num_candidates는 프롬프트 상에서 비워두면 "자동" 모드로 동작
    if settings.num_candidates is None:
        num_candidates_display = "10"  # 기본값
    else:
        num_candidates_display = str(settings.num_candidates)

    # System Instruction은 항상 동일 (정적 - Implicit Caching)
    system_instruction = STAGE3_SYSTEM_INSTRUCTION

    # User 프롬프트는 동적 데이터만 포함
    user_prompt = STAGE3_USER_PROMPT_TEMPLATE.format(
        market=fmt_safe(settings.market),
        max_len=settings.max_len if settings.max_len > 0 else 50,
        num_candidates=num_candidates_display,
        json_body=fmt_safe(json_body),
    )

    return (system_instruction, user_prompt)


def build_stage3_request_from_row(
    row: "pd.Series",
    default_settings: Optional[Stage3Settings] = None,
    st2_col: str = "ST2_JSON",
) -> Stage3Request:
    """
    엑셀 한 행(row)과 전역 기본 설정(default_settings)을 받아
    Stage3Request를 생성한다.

    사용 예)
        settings_global = Stage3Settings(market="네이버", max_len=50, num_candidates=None)
        for idx, row in df.iterrows():
            req = build_stage3_request_from_row(row, settings_global)
            # req.system_instruction, req.user_prompt를 Gemini Batch API에 전달
    """
    # 1) ST2_JSON 확보
    raw_json = safe_str(row.get(st2_col, ""))
    if not raw_json:
        raise ValueError(
            f"행에 '{st2_col}' 값이 비어 있습니다. "
            "먼저 Stage 2(JSON 추출)를 완료해야 Stage 3를 실행할 수 있습니다."
        )

    # 2) 행 단위 + 전역 설정을 합쳐 최종 Stage3Settings 구성
    settings = build_stage3_settings_from_row(row, default_settings)

    # 3) 프롬프트 생성 (Gemini 최적화: system_instruction/user_prompt 분리)
    system_instruction, user_prompt = build_stage3_prompt(raw_json, settings)

    return Stage3Request(
        system_instruction=system_instruction,
        user_prompt=user_prompt,
        market=settings.market,
        max_len=settings.max_len,
        num_candidates=settings.num_candidates,
        naming_strategy=settings.naming_strategy,
        raw_json=raw_json,
    )


# ============================================================
#  Gemini Batch API 관련 함수 (Stage 3용)
#  - GPT Batch API와 동일한 워크플로우
#  - JSONL 업로드 → 배치 생성 → 상태 폴링 → 결과 다운로드 → 병합
# ============================================================

# Gemini API Import
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# API 키 파일 경로
API_KEY_FILE = ".gemini_api_key_stage3_batch"

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
    import os
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""


def save_api_key_to_file(key: str) -> None:
    import os
    try:
        with open(API_KEY_FILE, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")


# =====================================
# Gemini 클라이언트 생성
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
    settings: Stage3Settings,
    skip_existing: bool = True,
    st2_col: str = "ST2_JSON",
):
    """
    엑셀 파일(ST2_JSON 컬럼) → Gemini Batch API용 JSONL 생성.
    """
    import os
    import json

    df = pd.read_excel(excel_path)

    if st2_col not in df.columns:
        raise ValueError(f"엑셀에 필수 컬럼이 없습니다: {st2_col}")

    total_rows = len(df)
    written_count = 0
    skipped_rows = []
    skipped_existing = 0

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            # 기존 결과가 있으면 스킵
            if skip_existing and "ST3_결과상품명" in df.columns:
                existing = safe_str(row.get("ST3_결과상품명", ""))
                if existing and existing != "nan":
                    skipped_existing += 1
                    continue

            # ST2_JSON 확인
            raw_json = safe_str(row.get(st2_col, ""))
            if not raw_json:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": st2_col,
                })
                continue

            try:
                req = build_stage3_request_from_row(row, settings, st2_col)
            except Exception as e:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": str(e),
                })
                continue

            # Gemini Batch JSONL 형식
            request_obj = {
                "key": f"row-{idx}",
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [{"text": req.user_prompt}]
                        }
                    ],
                    "systemInstruction": {
                        "parts": [{"text": req.system_instruction}]
                    },
                    "generationConfig": {
                        "temperature": 0.7,
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
        skipped_path = f"{base}_stage3_skipped_rows.xlsx"
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
    import os
    import hashlib
    import tempfile
    import shutil
    from datetime import datetime

    # 한글 경로 인코딩 오류 방지: 임시 디렉토리에 ASCII 파일명으로 복사
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_hash = hashlib.md5(jsonl_path.encode('utf-8')).hexdigest()[:8]
    temp_filename = f"stage3_batch_{timestamp}_{file_hash}.jsonl"

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
) -> list:
    """
    Gemini Batch 결과 JSONL 파싱.
    """
    import json
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
                return text.strip()

        # 직접 text 필드가 있는 경우
        if response.get("text"):
            return response["text"].strip()

    except Exception:
        pass
    return ""


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

    if "ST3_결과상품명" not in df.columns:
        df["ST3_결과상품명"] = ""
    df["ST3_결과상품명"] = df["ST3_결과상품명"].astype(str)

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
                    df.at[idx, "ST3_결과상품명"] = text
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
