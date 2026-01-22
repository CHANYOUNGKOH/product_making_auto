"""
stage4_2_core_Casche.py

Stage 4-2: LLM 기반 상품명 재정렬/필터링 코어 모듈 (캐싱 최적화 버전)
- 프롬프트 관리, API 호출, 비용 계산 담당
- GUI용 실시간 처리 함수 및 Batch API용 Payload 생성 함수 모두 포함
- 순환 참조 방지를 위해 모든 정의를 이 파일 내부에 포함
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
  * 동적 콘텐츠(입력 데이터)를 user 프롬프트에 배치
  * 프롬프트 프리픽스가 모든 요청에서 동일하도록 구성
"""

import os
import re
from dataclasses import dataclass
from typing import Optional, Any, Dict
import pandas as pd
from openai import OpenAI

# =====================================
# 공통 경로 및 설정
# =====================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEY_FILE = os.path.join(BASE_DIR, ".openai_api_key_stage4_2")

# 모델별 가격표 (USD / 1M Token)
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5":       {"input": 1.25, "output": 10.0},
    "gpt-5-mini":  {"input": 0.25, "output": 2.00},
    "gpt-5-nano":  {"input": 0.05, "output": 0.40},
    "gpt-4o":      {"input": 2.50, "output": 10.00}, 
}

# =====================================
# [프롬프트] 캐싱 최적화 버전 (Safety Net 포함)
# =====================================
# OpenAI Prompt Caching 가이드에 맞게 재구성:
# - 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
# - 동적 콘텐츠(입력 데이터)를 user 프롬프트에 배치
# - 프롬프트 프리픽스가 모든 요청에서 동일하도록 구성

# System 프롬프트 (완전히 정적, 모든 요청에서 동일)
# ⚠️ 중요: 프롬프트 캐싱 활성화를 위해 1024 토큰 이상이어야 함
STAGE4_2_SYSTEM_PROMPT = """당신은 한국 이커머스 시장의 **'상품명 최적화 전문가(SEO & Conversion Specialist)'**입니다.
입력된 후보 리스트를 검증 데이터(JSON)와 대조하여 **거짓·부적절한 상품명을 제거**하고,
살아남은 후보를 **구매 전환율(CTR)이 높을 것 같은 순서대로 재정렬**하십시오.

[1단계: 제거 규칙 (Filtering)]
*다음 기준 중 하나라도 위반하면 그 후보는 최종 결과에서 **완전히 삭제**하십시오.*

1. **팩트 오류 / 과장**
   - ST2 JSON에 없는 수량·용량·재질·구성·대상·기능을 포함한 경우 삭제합니다.
   - 예: JSON에 "면 100%"가 없는데 "면 100% 티셔츠"라고 한 경우 (X)
   - 예: JSON에 "12L"이 없는데 "12L 아이스박스"라고 한 경우 (X)
   - 예: JSON에 "방수" 기능이 없는데 "방수 가방"이라고 한 경우 (X)

2. **허위 마케팅 문구**
   - '무료배송, 파격 세일, 공식몰, 최저가, 특가, 행사, 1+1, 사은품' 등 근거 없는 단언이 포함된 경우 삭제합니다.
   - 예: "무료배송 겨울 방한 니트 귀마개" (X - 무료배송 언급)
   - 예: "특가 할인 방한 귀마개" (X - 특가/할인 언급)

3. **정체 불명 / 키워드 나열**
   - 직관적이지 않거나 단순 키워드 나열로만 구성된 경우 삭제합니다.
   - 예: "방한 겨울 니트 귀마개 모자 넥워머 세트 외출" (X - 키워드 나열)
   - 예: "니트 방한 겨울" (X - 정체 불명)

4. **품질 미달 / 중복**
   - 어색한 어순, 띄어쓰기 오류, 의미 중복이 있는 경우 삭제합니다.
   - 예: "겨울방한니트귀마개" (X - 띄어쓰기 오류)
   - 예: "겨울 겨울 방한 니트 귀마개" (X - 중복)
   - 예: "방한용 방한 귀마개" (X - 의미 중복)

[2단계: 정렬 기준 (Ranking)]
*제거되지 않고 남은 후보들은 이미 사실성이 검증되었습니다. 매출 관점으로 정렬하십시오.*

1. **[1순위] 매력도·클릭률**
   - 고객의 니즈를 자극하여 클릭하고 싶게 만드는가?
   - 구체적인 사용 상황이나 해결하는 문제가 명확한가?
   - 예: "겨울 방한 니트 귀마개 모자 넥워머 세트" (O - 구체적)
   - 예: "니트 귀마개" (X - 너무 단순, 매력도 낮음)

2. **[2순위] 직관성**
   - 카테고리와 핵심 특징이 한눈에 이해되는가?
   - 상품의 정체가 명확한가?
   - 예: "방한 귀마개 세트 겨울용 니트" (O - 직관적)
   - 예: "니트 세트 겨울" (X - 정체 불명)

3. **[3순위] 자연스러움**
   - 한국어 어순이 자연스러운가?
   - 읽기 편하고 이해하기 쉬운가?
   - 예: "겨울 방한 니트 귀마개 모자 넥워머 세트" (O - 자연스러움)
   - 예: "니트 겨울 방한 귀마개 모자" (X - 어순 어색)

[3단계: 안전장치 (Safety Net)]
- 위 규칙 적용 후 **모든 후보가 삭제되었다면**, 절대 빈 결과를 출력하지 마십시오.
- 대신, ST1(기준 상품명)과 ST2(JSON)의 사실 정보만을 조합하여
  **가장 안전하고 판매력이 좋은 상품명 1개를 새로 작성하여 출력**하십시오.
- 새로 작성할 때도 위의 제거 규칙을 반드시 준수하십시오.
- 예: ST1이 "나이키 겨울 방한 니트 귀마개"이고 모든 후보가 삭제된 경우
  → "겨울 방한 니트 귀마개 모자 넥워머 세트" (브랜드 제거, 사실 정보만 사용)

[중요 규칙]
1. **작문 금지**: 후보가 1개 이상 남으면, 절대 새로 짓지 말고 순서만 바꾸십시오.
   - 예: 후보가 3개 남았으면 그 3개를 정렬만 하고, 4번째를 새로 만들지 않습니다.
2. **출력 제한**: 오직 상품명 텍스트만 줄바꿈으로 나열하십시오. 번호, 설명, 기호 금지.
   - 예: "겨울 방한 니트 귀마개 모자 넥워머 세트\n방한 귀마개 세트 겨울용 니트" (O)
   - 예: "1. 겨울 방한 니트 귀마개\n2. 방한 귀마개 세트" (X - 번호 포함)

[4단계: 추가 가이드라인 및 주의사항]
- **검증 우선**: ST2 JSON 데이터와의 일치 여부를 최우선으로 확인하십시오. 추측이나 일반적 지식에 의존하지 마십시오.
- **정렬 원칙**: 매출 전환율이 높을 것으로 예상되는 순서로 정렬하되, 사실성 검증을 먼저 수행하십시오.
- **안전성**: 모든 후보가 제거되는 경우에만 Safety Net을 사용하여 새 상품명을 작성하십시오.
- **일관성**: 동일한 입력에 대해서는 항상 동일한 결과를 출력하도록 규칙을 일관되게 적용하십시오.
- **효율성**: 불필요한 정보나 중복 표현은 제거하되, 핵심 정보는 반드시 유지하십시오.

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
    """
    str(v)를 한 번 감싼 뒤, .format()에 안전하게 넣기 위한 이스케이프.
    - { → {{, } → }}
    """
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
# 데이터 클래스 (GUI용)
# =====================================
@dataclass
class Stage4_2Settings:
    """GUI 설정을 담는 컨테이너"""
    model_name: str = "gpt-5"
    reasoning_effort: str = "medium"

@dataclass
class Stage4_2Request:
    """LLM 요청 데이터 (GUI 전용) - 캐싱 최적화 버전"""
    row_index: int
    product_code: str
    system_prompt: str  # 정적 프롬프트 (모든 요청에서 동일)
    user_prompt: str    # 동적 프롬프트 (입력 데이터 포함)
    prompt: str         # 호환성 유지용 (GUI에서 사용)
    st1_name: str 
    st3_candidates: str 

@dataclass
class Stage4_2CallUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    input_cost: float = 0.0
    output_cost: float = 0.0
    total_cost: float = 0.0

@dataclass
class Stage4_2Result:
    row_index: int
    product_code: str
    output_text: str
    model: str
    effort: str
    usage: Stage4_2CallUsage
    error: Optional[str] = None

# =====================================
# [GUI용] 실시간 요청 빌더
# =====================================
def build_stage4_2_request_from_row(
    row: pd.Series, 
    idx: int,
    cand_col: str = "ST3_결과상품명"
) -> Stage4_2Request:
    """
    엑셀 행 데이터를 분석하여 GUI용 요청 객체(Request)를 생성
    """
    p_code = safe_str(row.get('상품코드', ''))
    st1_val = safe_str(row.get('ST1_결과상품명', ''))
    st2_val = safe_str(row.get('ST2_JSON', '{}'))
    st3_val = safe_str(row.get(cand_col, ''))

    # 프롬프트 조립 (캐싱 최적화: system/user 분리)
    system_prompt = STAGE4_2_SYSTEM_PROMPT
    user_prompt = STAGE4_2_USER_PROMPT_TEMPLATE.format(
        st1_refined_name=fmt_safe(st1_val),
        st2_json=fmt_safe(st2_val),
        candidate_list=fmt_safe(st3_val)
    )
    
    # 호환성 유지: GUI용 전체 프롬프트 (기존 방식)
    prompt = f"{system_prompt}\n\n{user_prompt}"

    return Stage4_2Request(
        row_index=idx,
        product_code=p_code,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prompt=prompt,  # 호환성 유지
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
    effort: str,
    cand_col: str = "ST3_결과상품명",
    use_cache_optimization: bool = True
) -> Optional[Dict[str, Any]]:
    """
    엑셀 행을 받아 OpenAI Batch 업로드용 JSON 객체 1개를 생성.
    (stage4_2_batch_api.py 에서 사용)
    캐싱 최적화 버전: system/user 프롬프트 분리
    """
    # 1. 데이터 추출
    st1_val = safe_str(row.get('ST1_결과상품명', ''))
    st2_val = safe_str(row.get('ST2_JSON', '{}'))
    st3_val = safe_str(row.get(cand_col, ''))

    # 후보가 없으면 None 반환 (배치 요청 생성 스킵)
    if not st3_val or st3_val.lower() == 'nan':
        return None

    # 2. 프롬프트 조립 (캐싱 최적화: system/user 분리)
    if use_cache_optimization:
        # System 메시지 (텍스트만, 정적)
        system_content = [{"type": "input_text", "text": STAGE4_2_SYSTEM_PROMPT}]
        
        # User 메시지 (텍스트만, 동적)
        user_prompt = STAGE4_2_USER_PROMPT_TEMPLATE.format(
            st1_refined_name=fmt_safe(st1_val),
            st2_json=fmt_safe(st2_val),
            candidate_list=fmt_safe(st3_val)
        )
        user_content = [{"type": "input_text", "text": user_prompt}]
        
        # 3. Body 구성 (Responses API)
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
        if is_reasoning and effort in ["low", "medium", "high"]:
            body["reasoning"] = {"effort": effort}
        elif not is_reasoning:
            body["temperature"] = 0.3
        
        url = "/v1/responses"
    else:
        # 일반 모드: 기존 방식 유지
        user_prompt = STAGE4_2_USER_PROMPT_TEMPLATE.format(
            st1_refined_name=fmt_safe(st1_val),
            st2_json=fmt_safe(st2_val),
            candidate_list=fmt_safe(st3_val)
        )
        prompt = f"{STAGE4_2_SYSTEM_PROMPT}\n\n{user_prompt}"
        
        body = {
            "model": model_name,
            "messages": [{"role": "user", "content": prompt}],
        }
        
        is_reasoning = any(x in model_name for x in ["gpt-5", "o1", "o3"])
        if is_reasoning and effort in ["low", "medium", "high"]:
            body["reasoning_effort"] = effort
        elif not is_reasoning:
            body["temperature"] = 0.3
        
        url = "/v1/chat/completions"

    # 4. Batch Request 구조 반환
    # custom_id에 row 인덱스를 넣어 나중에 병합할 때 사용
    request_obj = {
        "custom_id": f"row_{row_index}",  
        "method": "POST",
        "url": url,
        "body": body
    }
    return request_obj

# =====================================
# [GUI용] Core Logic Class
# =====================================
class Stage4_2Core:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

    def execute_request(self, req: Stage4_2Request, settings: Stage4_2Settings) -> Stage4_2Result:
        """준비된 요청 객체로 실제 API 호출 수행"""
        
        # 후보가 아예 없으면 API 호출 스킵 -> ST1 안전망 반환
        if not req.st3_candidates:
             return Stage4_2Result(
                row_index=req.row_index,
                product_code=req.product_code,
                output_text=req.st1_name,
                model=settings.model_name,
                effort="skipped",
                usage=Stage4_2CallUsage(),
                error="No Candidates (Safety Net: ST1 used)"
            )

        try:
            messages = [{"role": "user", "content": req.prompt}]
            params = {
                "model": settings.model_name,
                "messages": messages,
            }

            is_reasoning = any(x in settings.model_name for x in ["gpt-5", "o1", "o3"])
            if is_reasoning:
                params["reasoning_effort"] = settings.reasoning_effort
            else:
                params["temperature"] = 0.3

            response = self.client.chat.completions.create(**params)

            # 결과 정제 (마크다운 제거)
            content = response.choices[0].message.content.strip()
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
                effort=settings.reasoning_effort if is_reasoning else "n/a",
                usage=usage_data,
                error=None
            )

        except Exception as e:
            return Stage4_2Result(
                row_index=req.row_index,
                product_code=req.product_code,
                output_text="",
                model=settings.model_name,
                effort=settings.reasoning_effort,
                usage=Stage4_2CallUsage(),
                error=str(e)
            )

    def _extract_usage(self, response: Any, model_name: str) -> Stage4_2CallUsage:
        usage = getattr(response, "usage", None)
        if not usage:
            return Stage4_2CallUsage()

        i_tok = getattr(usage, "prompt_tokens", 0) or 0
        o_tok = getattr(usage, "completion_tokens", 0) or 0
        
        r_tok = 0
        details = getattr(usage, "completion_tokens_details", None)
        if details:
            r_tok = getattr(details, "reasoning_tokens", 0) or 0

        pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
        
        input_cost = (i_tok / 1_000_000) * pricing["input"]
        output_cost = (o_tok / 1_000_000) * pricing["output"]
        total_cost = input_cost + output_cost

        return Stage4_2CallUsage(
            input_tokens=i_tok, 
            output_tokens=o_tok, 
            reasoning_tokens=r_tok, 
            input_cost=input_cost,
            output_cost=output_cost,
            total_cost=total_cost
        )