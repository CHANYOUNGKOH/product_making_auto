"""
stage4_2_core.py

- Stage 4-2(최종 심사 및 정렬)를 위한 프롬프트 템플릿
- 엑셀 행(row) -> 요청 객체(Request) 변환 로직 분리 (Stage 3 구조 도입)
- 비용 계산 및 API 호출 담당
"""

import os
import re
from dataclasses import dataclass
from typing import Optional, Any
import pandas as pd
from openai import OpenAI

# =====================================
# 공통 경로 및 설정
# =====================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEY_FILE = os.path.join(BASE_DIR, ".openai_api_key_stage4_2")

# 모델별 가격표 (Stage 3와 동일하게 관리)
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5":       {"input": 1.25, "output": 10.0},
    "gpt-5-mini":  {"input": 0.25, "output": 2.00},
    "gpt-5-nano":  {"input": 0.05, "output": 0.40},
}

# =====================================
# [프롬프트] 최종 확정 (Safety Net 포함)
# =====================================
PROMPT_TEMPLATE = """
당신은 한국 이커머스 시장의 **'상품명 최적화 전문가(SEO & Conversion Specialist)'**입니다.
입력된 후보 리스트를 검증 데이터(JSON)와 대조하여 **거짓·부적절한 상품명을 제거**하고,
살아남은 후보를 **구매 전환율(CTR)이 높을 것 같은 순서대로 재정렬**하십시오.

[입력 정보]
- 기준 상품명(ST1): {st1_refined_name}
- 상세 속성(ST2 JSON, 사실 정보): {st2_json}
- 후보 상품명 목록(ST3 Result):
---
{candidate_list}
---

[1단계: 제거 규칙 (Filtering)]
*다음 기준 중 하나라도 위반하면 그 후보는 최종 결과에서 **완전히 삭제**하십시오.*
1. **팩트 오류 / 과장**: ST2 JSON에 없는 수량·용량·재질·구성·대상·기능 포함.
2. **허위 마케팅 문구**: '무료배송, 파격 세일, 공식몰' 등 근거 없는 단언.
3. **정체 불명 / 키워드 나열**: 직관적이지 않거나 단순 키워드 나열.
4. **품질 미달 / 중복**: 어색한 어순, 띄어쓰기 오류, 의미 중복.

[2단계: 정렬 기준 (Ranking)]
*제거되지 않고 남은 후보들은 이미 사실성이 검증되었습니다. 매출 관점으로 정렬하십시오.*
1. **[1순위] 매력도·클릭률**: 고객의 니즈를 자극하여 클릭하고 싶게 만드는가?
2. **[2순위] 직관성**: 카테고리와 핵심 특징이 한눈에 이해되는가?
3. **[3순위] 자연스러움**: 한국어 어순이 자연스러운가?

[3단계: 안전장치 (Safety Net)]
- 위 규칙 적용 후 **모든 후보가 삭제되었다면**, 절대 빈 결과를 출력하지 마십시오.
- 대신, ST1(기준 상품명)과 ST2(JSON)의 사실 정보만을 조합하여
  **가장 안전하고 판매력이 좋은 상품명 1개를 새로 작성하여 출력**하십시오.

[중요 규칙]
1. **작문 금지**: 후보가 1개 이상 남으면, 절대 새로 짓지 말고 순서만 바꾸십시오.
2. **출력 제한**: 오직 상품명 텍스트만 줄바꿈으로 나열하십시오. 번호, 설명, 기호 금지.
"""

# =====================================
# 유틸리티 & 데이터 클래스
# =====================================
def safe_str(val: Any) -> str:
    """NaN, None, float 등을 빈 문자열이나 문자열로 안전하게 변환"""
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip()

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

@dataclass
class Stage4_2Settings:
    """GUI에서 설정한 옵션들을 담는 컨테이너"""
    model_name: str = "gpt-5"
    reasoning_effort: str = "medium"

@dataclass
class Stage4_2Request:
    """LLM에 보낼 프롬프트와 메타데이터"""
    row_index: int
    product_code: str
    prompt: str
    st1_name: str  # 백업용(안전망)
    st3_candidates: str # 원본 후보군(로깅용)

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
# 데이터 변환 (Builder)
# =====================================
def build_stage4_2_request_from_row(
    row: pd.Series, 
    idx: int,
    cand_col: str = "ST3_결과상품명"
) -> Stage4_2Request:
    """
    엑셀 행 데이터를 분석하여 LLM 요청 객체(Request)를 생성
    """
    p_code = safe_str(row.get('상품코드', ''))
    st1_val = safe_str(row.get('ST1_정제상품명', ''))
    st2_val = safe_str(row.get('ST2_JSON', '{}'))
    st3_val = safe_str(row.get(cand_col, ''))

    # 프롬프트 조립
    prompt = PROMPT_TEMPLATE.format(
        st1_refined_name=st1_val,
        st2_json=st2_val,
        candidate_list=st3_val
    )

    return Stage4_2Request(
        row_index=idx,
        product_code=p_code,
        prompt=prompt,
        st1_name=st1_val,
        st3_candidates=st3_val
    )


# =====================================
# Core Logic
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

            # 추론 모델 여부 확인
            is_reasoning = any(x in settings.model_name for x in ["gpt-5", "o1", "o3"])
            
            if is_reasoning:
                params["reasoning_effort"] = settings.reasoning_effort
            else:
                params["temperature"] = 0.3

            response = self.client.chat.completions.create(**params)

            # 결과 정제
            content = response.choices[0].message.content.strip()
            content = re.sub(r"^```(?:json|text)?\n", "", content)
            content = re.sub(r"\n```$", "", content)
            content = content.strip()

            if not content: # 빈값이면 ST1 사용
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