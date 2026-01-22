"""
stage4_2_core_gemini.py

Stage 4-2: LLM 기반 상품명 재정렬/필터링 코어 모듈 (Gemini 2.5 Flash-Lite 버전)
- 프롬프트 관리, API 호출, 비용 계산 담당
- Implicit Caching 자동 적용 (System Instruction 동일)
- Few-Shot 예시 포함
"""

import os
import re
from dataclasses import dataclass
from typing import Optional, Any, Dict
import pandas as pd

# Gemini API
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# =====================================
# 공통 경로 및 설정
# =====================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEY_FILE = os.path.join(BASE_DIR, ".gemini_api_key_stage4_2")

# Gemini 모델별 가격표 (USD / 1M Token)
MODEL_PRICING_USD_PER_MTOK = {
    "gemini-2.5-flash-lite": {"input": 0.05, "output": 0.20},
    "gemini-2.5-flash-preview-05-20": {"input": 0.15, "output": 0.60},
    "gemini-2.0-flash": {"input": 0.10, "output": 0.40},
}

# =====================================
# [프롬프트] Gemini 최적화 버전 (Safety Net 포함)
# =====================================

# System Instruction (정적, 모든 요청에서 동일 - Implicit Caching 대상)
STAGE4_2_SYSTEM_INSTRUCTION = """당신은 한국 이커머스 시장의 **'상품명 최적화 전문가(SEO & Conversion Specialist)'**입니다.
입력된 후보 리스트를 검증 데이터(JSON)와 대조하여 **거짓·부적절한 상품명을 제거**하고,
살아남은 후보를 **구매 전환율(CTR)이 높을 것 같은 순서대로 재정렬**하십시오.

[1단계: 제거 규칙 (Filtering)]
*다음 기준 중 하나라도 위반하면 그 후보는 최종 결과에서 **완전히 삭제**하십시오.*

1. **팩트 오류 / 과장**
   - ST2 JSON에 없는 수량·용량·재질·구성·대상·기능을 포함한 경우 삭제합니다.
   - 예: JSON에 "면 100%"가 없는데 "면 100% 티셔츠"라고 한 경우 (X)
   - 예: JSON에 "12L"이 없는데 "12L 아이스박스"라고 한 경우 (X)

2. **허위 마케팅 문구**
   - '무료배송, 파격 세일, 공식몰, 최저가, 특가, 행사, 1+1, 사은품' 등 근거 없는 단언이 포함된 경우 삭제합니다.

3. **정체 불명 / 키워드 나열**
   - 직관적이지 않거나 단순 키워드 나열로만 구성된 경우 삭제합니다.

4. **품질 미달 / 중복**
   - 어색한 어순, 띄어쓰기 오류, 의미 중복이 있는 경우 삭제합니다.

[2단계: 정렬 기준 (Ranking)]
*제거되지 않고 남은 후보들은 이미 사실성이 검증되었습니다. 매출 관점으로 정렬하십시오.*

1. **[1순위] 매력도·클릭률** - 고객의 니즈를 자극하여 클릭하고 싶게 만드는가?
2. **[2순위] 직관성** - 카테고리와 핵심 특징이 한눈에 이해되는가?
3. **[3순위] 자연스러움** - 한국어 어순이 자연스러운가?

[3단계: 안전장치 (Safety Net)]
- 위 규칙 적용 후 **모든 후보가 삭제되었다면**, 절대 빈 결과를 출력하지 마십시오.
- 대신, ST1(기준 상품명)과 ST2(JSON)의 사실 정보만을 조합하여
  **가장 안전하고 판매력이 좋은 상품명 1개를 새로 작성하여 출력**하십시오.

[중요 규칙]
1. **작문 금지**: 후보가 1개 이상 남으면, 절대 새로 짓지 말고 순서만 바꾸십시오.
2. **출력 제한**: 오직 상품명 텍스트만 줄바꿈으로 나열하십시오. 번호, 설명, 기호 금지.

## Few-Shot 예시

### 예시 1
입력:
- ST1: 겨울 방한 니트 귀마개
- ST2 JSON: {"core_attributes": {"재질": ["니트"], "기타기능": ["방한", "보온"]}}
- 후보 목록:
  겨울 방한 니트 귀마개 보온
  니트 귀마개 무료배송 특가
  방한 겨울 니트 귀마개 스키용

출력:
겨울 방한 니트 귀마개 보온
방한 겨울 니트 귀마개 스키용

(설명: "무료배송 특가"는 허위 마케팅으로 삭제됨)

### 예시 2
입력:
- ST1: 캠핑 아이스박스 12L
- ST2 JSON: {"core_attributes": {"사이즈": {"기타": "12L"}, "기타기능": ["보냉"]}}
- 후보 목록:
  캠핑 아이스박스 25L 보냉
  휴대용 캠핑 쿨러 12L
  캠핑 아이스박스 12L 보냉용

출력:
캠핑 아이스박스 12L 보냉용
휴대용 캠핑 쿨러 12L

(설명: "25L"은 JSON의 12L과 다르므로 삭제됨)

### 예시 3 (모든 후보가 삭제된 경우)
입력:
- ST1: 여성 반팔 티셔츠
- ST2 JSON: {"core_attributes": {"사용대상": ["여성"], "재질": ["면 95%"]}}
- 후보 목록:
  여성 반팔 티셔츠 면 100%
  여름 티셔츠 무료배송
  여성복 티셔츠 특가

출력:
여성 반팔 티셔츠 면 95%

(설명: 모든 후보가 삭제되어 ST1+ST2 기반으로 안전한 상품명 생성)

위 규칙을 엄수하여, 검증된 상품명만 정렬하여 출력하십시오."""

# User 프롬프트 템플릿 (동적 데이터만 포함)
STAGE4_2_USER_PROMPT_TEMPLATE = """[입력 정보]
- 기준 상품명(ST1): {st1_refined_name}
- 상세 속성(ST2 JSON, 사실 정보): {st2_json}
- 후보 상품명 목록(ST3 Result, 줄바꿈 구분):
---
{candidate_list}
---"""

# =====================================
# 유틸리티 함수
# =====================================
def safe_str(val: Any) -> str:
    """NaN, None, float 등을 빈 문자열이나 문자열로 안전하게 변환"""
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip()


def fmt_safe(v: Any) -> str:
    """str(v)를 .format()에 안전하게 넣기 위한 이스케이프."""
    s = safe_str(v)
    return s.replace("{", "{{").replace("}", "}}")


def load_api_key_from_file(path: str = API_KEY_FILE) -> Optional[str]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip() or None
        except:
            return None
    return None


def save_api_key_to_file(key: str, path: str = API_KEY_FILE) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(key)
    except:
        pass


# =====================================
# 데이터 클래스
# =====================================
@dataclass
class Stage4_2Settings:
    """설정 컨테이너"""
    model_name: str = "gemini-2.5-flash-lite"
    temperature: float = 0.3


@dataclass
class Stage4_2Request:
    """LLM 요청 데이터 (Gemini 최적화 버전)"""
    row_index: int
    product_code: str
    system_instruction: str  # 정적 프롬프트 (모든 요청에서 동일)
    user_prompt: str         # 동적 프롬프트 (입력 데이터 포함)
    st1_name: str
    st3_candidates: str


@dataclass
class Stage4_2CallUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    input_cost: float = 0.0
    output_cost: float = 0.0
    total_cost: float = 0.0


@dataclass
class Stage4_2Result:
    row_index: int
    product_code: str
    output_text: str
    model: str
    usage: Stage4_2CallUsage
    error: Optional[str] = None


# =====================================
# 요청 빌더
# =====================================
def build_stage4_2_request_from_row(
    row: pd.Series,
    idx: int,
    cand_col: str = "ST3_결과상품명"
) -> Stage4_2Request:
    """엑셀 행 데이터를 분석하여 요청 객체 생성"""
    p_code = safe_str(row.get('상품코드', ''))
    st1_val = safe_str(row.get('ST1_결과상품명', ''))
    st2_val = safe_str(row.get('ST2_JSON', '{}'))
    st3_val = safe_str(row.get(cand_col, ''))

    # 프롬프트 조립 (Gemini 최적화: system/user 분리)
    system_instruction = STAGE4_2_SYSTEM_INSTRUCTION
    user_prompt = STAGE4_2_USER_PROMPT_TEMPLATE.format(
        st1_refined_name=fmt_safe(st1_val),
        st2_json=fmt_safe(st2_val),
        candidate_list=fmt_safe(st3_val)
    )

    return Stage4_2Request(
        row_index=idx,
        product_code=p_code,
        system_instruction=system_instruction,
        user_prompt=user_prompt,
        st1_name=st1_val,
        st3_candidates=st3_val
    )


# =====================================
# [Batch API용] Payload 빌더
# =====================================
def build_stage4_2_batch_payload(
    row_index: int,
    row: pd.Series,
    model_name: str,
    cand_col: str = "ST3_결과상품명"
) -> Optional[Dict[str, Any]]:
    """엑셀 행을 받아 Gemini Batch API용 JSON 객체 생성"""
    st1_val = safe_str(row.get('ST1_결과상품명', ''))
    st2_val = safe_str(row.get('ST2_JSON', '{}'))
    st3_val = safe_str(row.get(cand_col, ''))

    # 후보가 없으면 None 반환
    if not st3_val or st3_val.lower() == 'nan':
        return None

    # 프롬프트 조립
    user_prompt = STAGE4_2_USER_PROMPT_TEMPLATE.format(
        st1_refined_name=fmt_safe(st1_val),
        st2_json=fmt_safe(st2_val),
        candidate_list=fmt_safe(st3_val)
    )

    # Gemini Batch JSONL 형식 (camelCase 사용)
    request_obj = {
        "key": f"row-{row_index}",
        "request": {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": user_prompt}]
                }
            ],
            "systemInstruction": {
                "parts": [{"text": STAGE4_2_SYSTEM_INSTRUCTION}]
            },
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 2048,
            }
        }
    }
    return request_obj


# =====================================
# Core Logic Class (Gemini)
# =====================================
class Stage4_2Core:
    def __init__(self, api_key: str):
        if not GEMINI_AVAILABLE:
            raise ImportError("google-genai 패키지가 설치되지 않았습니다.")
        self.client = genai.Client(api_key=api_key)

    def execute_request(self, req: Stage4_2Request, settings: Stage4_2Settings) -> Stage4_2Result:
        """준비된 요청 객체로 실제 API 호출 수행"""

        # 후보가 아예 없으면 API 호출 스킵 -> ST1 안전망 반환
        if not req.st3_candidates:
            return Stage4_2Result(
                row_index=req.row_index,
                product_code=req.product_code,
                output_text=req.st1_name,
                model=settings.model_name,
                usage=Stage4_2CallUsage(),
                error="No Candidates (Safety Net: ST1 used)"
            )

        try:
            # Gemini API 호출
            response = self.client.models.generate_content(
                model=settings.model_name,
                contents=[{"role": "user", "parts": [{"text": req.user_prompt}]}],
                config=types.GenerateContentConfig(
                    system_instruction=req.system_instruction,
                    temperature=settings.temperature,
                    max_output_tokens=2048,
                )
            )

            # 결과 정제 (마크다운 제거)
            content = response.text.strip() if response.text else ""
            content = re.sub(r"^```(?:json|text)?\n", "", content)
            content = re.sub(r"\n```$", "", content)
            content = content.strip()

            # 만약 결과가 비었다면 ST1 사용 (비상 대책)
            if not content:
                content = req.st1_name

            # 사용량 계산
            usage_data = self._extract_usage(response, settings.model_name)

            return Stage4_2Result(
                row_index=req.row_index,
                product_code=req.product_code,
                output_text=content,
                model=settings.model_name,
                usage=usage_data,
                error=None
            )

        except Exception as e:
            return Stage4_2Result(
                row_index=req.row_index,
                product_code=req.product_code,
                output_text="",
                model=settings.model_name,
                usage=Stage4_2CallUsage(),
                error=str(e)
            )

    def _extract_usage(self, response: Any, model_name: str) -> Stage4_2CallUsage:
        usage_metadata = getattr(response, 'usage_metadata', None)
        if not usage_metadata:
            return Stage4_2CallUsage()

        i_tok = getattr(usage_metadata, 'prompt_token_count', 0) or 0
        o_tok = getattr(usage_metadata, 'candidates_token_count', 0) or 0

        pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})

        input_cost = (i_tok / 1_000_000) * pricing["input"]
        output_cost = (o_tok / 1_000_000) * pricing["output"]
        total_cost = input_cost + output_cost

        return Stage4_2CallUsage(
            input_tokens=i_tok,
            output_tokens=o_tok,
            input_cost=input_cost,
            output_cost=output_cost,
            total_cost=total_cost
        )


# =====================================
# Gemini Batch API 관련 함수 (Stage 4-2용)
# - GPT Batch API와 동일한 워크플로우
# - JSONL 업로드 → 배치 생성 → 상태 폴링 → 결과 다운로드 → 병합
# =====================================

# 기본 모델
DEFAULT_MODEL = "gemini-2.5-flash-lite"

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


def get_gemini_client(api_key: Optional[str] = None):
    """Gemini 클라이언트 생성."""
    if not GEMINI_AVAILABLE:
        raise ImportError("google-genai 패키지가 설치되지 않았습니다.")

    if not api_key:
        api_key = load_api_key_from_file()

    if not api_key:
        raise ValueError("Gemini API 키가 설정되지 않았습니다.")

    return genai.Client(api_key=api_key)


def create_batch_input_jsonl(
    excel_path: str,
    jsonl_path: str,
    skip_existing: bool = True,
    cand_col: str = "ST3_결과상품명",
):
    """
    엑셀 파일 → Gemini Batch API용 JSONL 생성.
    """
    import json

    df = pd.read_excel(excel_path)

    total_rows = len(df)
    written_count = 0
    skipped_rows = []
    skipped_existing = 0

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            # 기존 결과가 있으면 스킵
            if skip_existing and "ST4_최종상품명" in df.columns:
                existing = safe_str(row.get("ST4_최종상품명", ""))
                if existing and existing != "nan":
                    skipped_existing += 1
                    continue

            # ST3 후보 확인
            st3_val = safe_str(row.get(cand_col, ""))
            if not st3_val:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": cand_col,
                })
                continue

            # Batch Payload 생성
            payload = build_stage4_2_batch_payload(idx, row, DEFAULT_MODEL, cand_col)
            if payload:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                written_count += 1

    # 스킵된 행 저장
    skipped_path = None
    if skipped_rows:
        base, _ = os.path.splitext(excel_path)
        skipped_path = f"{base}_stage4_skipped_rows.xlsx"
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
    temp_filename = f"stage4_batch_{timestamp}_{file_hash}.jsonl"

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
        response = resp.get("response", {})

        candidates = response.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if parts:
                text = parts[0].get("text", "")
                # 첫 줄만 반환 (최종 상품명)
                first_line = text.strip().split("\n")[0].strip()
                return first_line

        if response.get("text"):
            first_line = response["text"].strip().split("\n")[0].strip()
            return first_line

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

    if "ST4_최종상품명" not in df.columns:
        df["ST4_최종상품명"] = ""
    df["ST4_최종상품명"] = df["ST4_최종상품명"].astype(str)

    cnt = 0
    total_input_tokens = 0
    total_output_tokens = 0

    for result in results:
        key = result.get("key", "")

        text = extract_text_from_response_dict(result)
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
                    df.at[idx, "ST4_최종상품명"] = text
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
