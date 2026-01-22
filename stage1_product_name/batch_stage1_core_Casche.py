# batch_stage1_core_Casche.py
"""
STAGE1 Batch API + 엑셀 병합 핵심 로직 (캐싱 최적화 버전)
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
  * 동적 콘텐츠(입력 데이터)를 user 프롬프트에 배치
  * 프롬프트 프리픽스가 모든 요청에서 동일하도록 구성
"""

import os
import json
import time
import threading
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from openai import OpenAI

from prompts_stage1 import build_stage1_prompt, safe_str
from stage1_run_history import append_run_history

# =====================================
# 프롬프트 캐싱 최적화: System/User 분리
# =====================================
# System 프롬프트 (완전히 정적, 모든 요청에서 동일)
# ⚠️ 중요: 프롬프트 캐싱 활성화를 위해 1024 토큰 이상이어야 함
STAGE1_SYSTEM_PROMPT = """당신은 온라인 쇼핑몰 위탁판매용 상품명 정제 전문가입니다.
입력된 정보를 바탕으로 광고와 브랜드 거품을 제거하고, 검색에 강한 정보 중심 상품명 한 줄을 작성하십시오.
최종 출력 전에 아래 규칙을 모두 만족하는지 스스로 점검하되, 점검 과정이나 이유는 출력하지 마십시오.

[핵심 원칙]
- 원본에 명시된 정보만 사용합니다. (추측/유추/추론 금지)
- 브랜드/쇼핑몰명/광고 문구 제거 후, 의미 있는 정보만 남깁니다.
- 결과는 "명사형 키워드 나열" 형태의 상품명 한 줄입니다.
- 라벨/설명/문장형 표현/이모티콘/불필요 기호를 절대 출력하지 않습니다.
- 띄어쓰기 포함 최대 60자. 초과 시 덜 중요한 수식어부터 제거합니다.

[정제 규칙]

1. 구조 재배열 (우선순위)
- 권장 순서: [핵심 제품군] > [기능/사용상황] > [규격/수량] > [대상/스타일] > [시즌/테마] > [옵션/색상]
- 문장이 아닌 명사형 키워드 나열로 작성합니다.
- 60자 초과 시 중요도가 낮은 수식어부터 제거하여 60자 이내로 줄입니다.
- 예시: "특가! 인기 여성 코트 가을용" → "여성 가을 코트" (광고 제거, 순서 정리)

2. 노이즈 및 브랜드 제거
- 삭제 대상(예시):
  · 광고·홍보 문구: 무료배송, 당일배송, 오늘출발, 특가, 행사, 인기, 추천, 최저가, 빅세일,
    주문폭주, 인싸템, 핫딜, MD추천, 한정수량, 국민템, 신상품, 베스트, 할인, 세일, 이벤트,
    쿠폰, 사은품, 재입고, 강추, 대박, 완판, 프리미엄, 고퀄, 갓성비
  · 상점/몰/마켓명: ○○몰, ○○샵, ○○스토어, ○○마켓, 공식몰, 전문몰, 직영몰,
    플래그십스토어, 오프라인매장, 총판, 도매
  · 위탁판매자가 경쟁우위를 갖지 못하는 대중 브랜드명: 나이키, 아디다스, 휠라, 뉴발란스 등
  · 이모티콘·과한 기호: ♥, ★, !!, ??, [], {}, /, ·, |, ~ 등 의미 없는 기호
- 괄호는 꼭 필요한 규격/옵션 표기에만 최소한으로 사용하고,
  불필요한 괄호/빈 괄호/중복 괄호는 제거합니다.
  예시: "의자(의자)" → "의자"
- 유지 대상(속성어): 피치기모, 쿠션폼, 수제, 핸드메이드, DIY, 호환용, 교체용, 리필용,
  방수, 방풍, 발수, 미끄럼방지 등 "재질·특성·기능"을 나타내는 단어

3. 정보 정확성(추측 금지)
- 원본에 없는 정보(재질, 기능, 용량, 인증, 구성, 브랜드)를 새로 만들지 않습니다.
- 숫자/모델명/사이즈/용량/수량은 원본에 나온 값만 사용하며 임의로 바꾸지 않습니다.
- 불필요한 반복/어색한 혼용은 정돈하되 의미는 바꾸지 않습니다.
  예시: "티셔츠 티셔츠" → "티셔츠"
  예시: "T-shirt 티셔츠" → "티셔츠" (중복 제거)

4. 숫자·단위·기호 정돈(값은 유지)
- 단위/표기는 의미를 유지하며 간결하게 정돈합니다. (cm, mm, m, L, ℓ, kg, g, 개, 매, 장, 입, 세트 등)
- 곱셈 표기는 가능한 한 "×"로 통일합니다. (예: 10x20 → 10×20)
- "1개입/1P/1PCS/1EA" 등은 의미가 같으면 "1개"처럼 간단히 정돈합니다.
- 기호/구분자(, / | ·)는 가능하면 공백으로 대체합니다.

5. 옵션/색상 처리(원본에 있을 때만)
- 원본 상품명 또는 입력 옵션에 실제로 존재하는 옵션/색상만 반영합니다.
- 옵션이 길면 "구분에 가장 중요한 1개 옵션"만 남기고 나머지는 제거합니다.
- 원본에 옵션/색상이 없으면 새로 추가하지 않습니다.

6. 카테고리별 특수 규칙('원본에 명시된 항목만' 적용)
- 패션/의류: (원본에 있을 때만) 색상, 사이즈, 소재 순서로 자연스럽게 배치
- 전자제품: (원본에 있을 때만) 모델명, 용량, 색상 순서로 배치
- 생활용품: (원본에 있을 때만) 용량/수량, 재질, 용도 순서로 배치
※ 카테고리로 정보를 "유추"하여 추가/삭제하지 않습니다. 원본에 있는 항목만 정렬합니다.

7. 길이 제한 처리(60자 초과 시)
- 아래 순서대로 덜 중요한 것부터 제거합니다.
  1) 광고 문구(이미 제거 대상)
  2) 중복 표현
  3) 중요도 낮은 수식어(예: 고급, 프리미엄 등)
  4) 부차적 스타일/대상 정보(원본에 있어도 길이 초과 시 후순위로 삭제)
  5) 시즌/테마(필수 정보가 아니면 삭제)
※ 삭제 판단에서도 "추측/유추"는 하지 않습니다.

[출력 형식]
- 정제된 상품명 텍스트 **한 줄만** 출력하십시오.
- 라벨, 설명, 문장형 표현, 이모티콘 없이 순수한 상품명만 출력합니다.
- 줄바꿈 금지.

[자체 점검(출력 금지)]
- 광고/상점/브랜드가 남아 있지 않은가?
- 원본에 없는 정보를 추가하지 않았는가?
- 60자 이내인가?
- 한 줄로, 라벨 없이 상품명만 출력했는가?

[정제 예시(형식 학습용 / 출력은 예시처럼 "한 줄 상품명만")]
- 원본: ★무료배송★ [공식몰] 프리미엄 겨울 기모 레깅스 여성용 1+1 특가!!
  결과: 겨울 기모 레깅스 여성용 1+1
- 원본: (당일출고) ○○스토어 캠핑 대용량 아이스박스 25L 쿨러 가성비 추천
  결과: 캠핑 아이스박스 25L 쿨러
- 원본: 남성 반팔 티셔츠 여름 인기 베스트 할인
  결과: 남성 반팔 티셔츠 여름
- 원본: 블랙 M 면 티셔츠 (브랜드명) 정품
  결과: 면 티셔츠 블랙 M
- 원본: 아이폰 15 256GB 블랙 케이스 세트 특가
  결과: 아이폰 15 256GB 블랙 케이스 세트
- 원본: 1L 유리 보온병 텀블러 추천
  결과: 1L 유리 보온병 텀블러
- 원본: 반려동물 강아지 산책 하네스 리드줄 세트 소형견
  결과: 강아지 산책 하네스 리드줄 세트 소형견
- 원본: 차량용 범용 에어컨 필터 2개입 교체용
  결과: 차량용 범용 에어컨 필터 교체용 2개
- 원본: 주방 일회용 위생장갑 100매 대용량 특가
  결과: 일회용 위생장갑 100매
- 원본: 욕실 미끄럼방지 발매트 논슬립
  결과: 욕실 미끄럼방지 발매트
- 원본: 다용도 수납함 플라스틱 정리박스 대형
  결과: 플라스틱 수납함 정리박스 대형
- 원본: 무선 블루투스 이어폰 노이즈캔슬링
  결과: 무선 블루투스 이어폰 노이즈캔슬링
- 원본: USB C타입 고속충전 케이블 2m 2개입
  결과: USB C타입 고속충전 케이블 2m 2개
- 원본: A4 클리어파일 20P 서류정리
  결과: A4 클리어파일 20P 서류정리
- 원본: 캔들워머 전구포함 세트 북유럽 감성
  결과: 캔들워머 전구포함 세트
- 원본: 수제 DIY 비즈 팔찌 만들기 키트
  결과: DIY 비즈 팔찌 만들기 키트
- 원본: 여름 냉감 이불 싱글 150×200cm
  결과: 여름 냉감 이불 싱글 150×200cm
- 원본: 겨울 방풍 기모 장갑 남성용
  결과: 겨울 방풍 기모 장갑 남성용
- 원본: 어린이 미술 물감 12색 세트
  결과: 어린이 미술 물감 12색 세트
- 원본: 고양이 스크래처 골판지 리필 3개입
  결과: 고양이 스크래처 골판지 리필 3개
- 원본: 캠핑 접이식 의자 경량 휴대용
  결과: 캠핑 접이식 의자 경량 휴대용

위 규칙을 엄수하여, 정제된 상품명 텍스트 **한 줄만** 출력하십시오."""

# User 프롬프트 템플릿 (동적 데이터만 포함)
STAGE1_USER_PROMPT_TEMPLATE = """[입력 정보]
- 카테고리명: {category}
- 판매형태: {sale_type}  (참고용 메타 정보이며, 결과 상품명에 그대로 쓰지 말 것)
- 원본 상품명: {raw_name}"""

def fmt_safe(v: Any) -> str:
    """
    str(v)를 한 번 감싼 뒤, .format()에 안전하게 넣기 위한 이스케이프.
    - { → {{, } → }}
    """
    s = safe_str(v)
    return s.replace("{", "{{").replace("}", "}}")

# API 키 파일 경로 (GUI와 공유)
API_KEY_FILE = ".openai_api_key_batch"

# =======================
# 시간/타임존 유틸
# =======================
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def get_seoul_now():
    """
    Asia/Seoul 기준 현재 시각을 datetime으로 반환.
    zoneinfo 가 없으면 naive datetime 으로 fallback.
    """
    from datetime import datetime, timezone, timedelta

    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("Asia/Seoul"))
    # fallback: UTC+9 고정
    return datetime.now(timezone(timedelta(hours=9)))


# =======================
# API 키 로드/저장
# =======================

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


# =======================
# 토큰 단가 & 비용 계산 (ST1 전용)
# =======================

# 모델별 100만 토큰당 단가 (USD) - ST1 러너와 동일하게 맞춤
MODEL_PRICING: Dict[str, Dict[str, float]] = {
    # 참고: 실제 가격은 OpenAI 공식 문서 기준으로 필요 시 수정
    "gpt-5": {
        "input_per_million": 1.250,
        "cached_input_per_million": 0.125,
        "output_per_million": 10.000,
    },
    "gpt-5-mini": {
        "input_per_million": 0.250,
        "cached_input_per_million": 0.025,
        "output_per_million": 1.250,
    },
    "gpt-5-nano": {
        "input_per_million": 0.050,
        "cached_input_per_million": 0.005,
        "output_per_million": 0.300,
    },
}


def compute_cost_usd(
    model_name: str,
    total_input_tokens: int,
    total_output_tokens: int,
) -> Optional[Dict[str, float]]:
    """
    모델별 토큰 단가를 이용해 대략적인 비용(USD) 계산.
    - 캐시 입력 토큰은 아직 구분하지 않으므로 일반 입력 단가만 사용.
    - 모델 정보가 없으면 None 반환.
    """
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
# 응답 텍스트 / 사용량 추출
# =====================================

def extract_text_from_response_dict(resp: Dict[str, Any]) -> str:
    """
    Batch 결과 JSONL 안의 'response' 딕셔너리에서
    사람이 읽을 텍스트만 뽑아내는 함수.

    ⚠️ 주의:
    Batch API에서는 한 줄이 이런 형태다.
      {
        "custom_id": "row-0",
        "response": {
          "status_code": 200,
          "request_id": "res_xxx",
          "body": { ...responses.create 결과... }
        },
        "error": null
      }

    그래서 먼저 resp["body"] 를 꺼내서 그 안에서 output 을 찾아야 한다.
    """
    try:
        # 1) Batch 응답 envelope 풀기 (status_code / body 구조)
        body = resp.get("body") if isinstance(resp, dict) and "body" in resp else resp

        chunks: List[str] = []

        # 2) Responses API 표준 구조: body["output"][..]["content"][..]["text"]
        output_list = body.get("output") or []
        for out in output_list:
            o_type = out.get("type")
            # type 이 따로 안 붙거나 "message" 인 경우만 사용
            if o_type not in (None, "message"):
                continue

            content_list = out.get("content") or []
            for c in content_list:
                t_obj = c.get("text")
                if isinstance(t_obj, str):
                    # text 가 그냥 문자열일 때
                    chunks.append(t_obj)
                elif isinstance(t_obj, dict):
                    # {"value": "..."} 형태일 때
                    val = t_obj.get("value")
                    if isinstance(val, str):
                        chunks.append(val)

        if chunks:
            full_text = "\n".join(chunks).strip()
            # 우리는 "정제된 상품명 한 줄"만 필요하니까 첫 줄만 사용
            first_line = full_text.splitlines()[0].strip()
            return first_line

    except Exception:
        # 여기서 에러 나더라도 아래 fallback 으로 넘어가도록 조용히 무시
        pass

    # 3) 혹시 body 에 output_text 필드만 있는 경우 (미래 호환용)
    maybe = resp.get("output_text") if isinstance(resp, dict) else None
    if isinstance(maybe, str) and maybe.strip():
        return maybe.strip()

    return ""


def extract_usage_from_response_dict(resp: Dict[str, Any]) -> Tuple[int, int, int]:
    """
    Batch 결과 JSONL 안의 'response' 딕셔너리에서
    토큰 사용량 (input, output, reasoning)을 추출.
    """
    try:
        body = resp.get("body") if isinstance(resp, dict) and "body" in resp else resp
        usage = body.get("usage") or {}
        in_tok = int(usage.get("input_tokens") or 0)
        out_tok = int(usage.get("output_tokens") or 0)

        reasoning_tok = 0
        details = usage.get("output_tokens_details") or {}
        if isinstance(details, dict):
            reasoning_tok = int(details.get("reasoning_tokens") or 0)

        return in_tok, out_tok, reasoning_tok
    except Exception:
        return 0, 0, 0


# =====================================
# Batch API 핵심 로직
# =====================================

def create_batch_input_jsonl(
    excel_path: str,
    jsonl_path: str,
    model_name: str = "gpt-5-mini",
    reasoning_effort: str = "low",
):
    """
    엑셀 파일(원본상품명, 카테고리명, 판매형태) → Batch API용 JSONL 생성.
    - 카테고리명 / 판매형태 / 원본상품명 중 하나라도 비어 있으면 그 행은 JSONL에서 제외.
    - 제외된 행은 별도 엑셀 파일(<원본명>_stage1_skipped_rows.xlsx)에 저장.
    - 반환값(info_dict)으로 전체/변환/제외 개수와 제외파일 경로를 돌려줌.
    """
    df = pd.read_excel(excel_path)

    required_cols = ["원본상품명", "카테고리명", "판매형태"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"엑셀에 필수 컬럼이 없습니다: {col}")

    total_rows = len(df)
    written_count = 0
    skipped_rows: List[Dict[str, Any]] = []

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
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

            # 하나라도 비어 있으면 JSONL에는 안 쓰고, 스킵 목록에만 저장
            if missing_fields:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": ", ".join(missing_fields),
                    "카테고리명": category,
                    "판매형태": sale_type,
                    "원본상품명": raw_name,
                })
                continue

            # 프롬프트 캐싱 최적화: system/user 분리
            system_content = [{"type": "input_text", "text": STAGE1_SYSTEM_PROMPT}]
            user_prompt = STAGE1_USER_PROMPT_TEMPLATE.format(
                category=fmt_safe(category),
                sale_type=fmt_safe(sale_type),
                raw_name=fmt_safe(raw_name)
            )
            user_content = [{"type": "input_text", "text": user_prompt}]

            body: Dict[str, Any] = {
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
                "reasoning": {"effort": reasoning_effort},
                # 프롬프트 캐싱 최적화
                "prompt_cache_key": "stage1_v1",  # 버킷 분산은 GUI에서 처리
                # prompt_cache_retention은 모델이 지원하지 않을 수 있으므로 제거
                # (prompt_cache_key만으로도 프롬프트 캐싱이 작동할 수 있음)
            }

            item = {
                "custom_id": f"row-{idx}",
                "method": "POST",
                "url": "/v1/responses",
                "body": body,
            }

            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            written_count += 1

    # 스킵된 행 요약 엑셀 저장
    skipped_path = ""
    if skipped_rows:
        base_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        skipped_path = os.path.join(base_dir, f"{base_name}_stage1_skipped_rows.xlsx")
        skipped_df = pd.DataFrame(skipped_rows)
        skipped_df.to_excel(skipped_path, index=False)

    # 간단 요약 반환
    info = {
        "total_rows": total_rows,
        "written_count": written_count,
        "skipped_count": len(skipped_rows),
        "skipped_path": skipped_path,
    }
    return info


def submit_batch(jsonl_path: str, client: OpenAI, completion_window: str = "24h") -> str:
    """
    JSONL 파일 업로드 후 Batch 생성, batch_id 반환
    """
    with open(jsonl_path, "rb") as f:
        file_obj = client.files.create(
            file=f,
            purpose="batch",
        )

    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/responses",
        completion_window=completion_window,
    )
    return batch.id


def wait_and_collect_batch(
    batch_id: str,
    excel_path: str,
    output_excel_path: str,
    client: OpenAI,
    poll_interval_sec: int = 30,
    log_fn=None,
    stop_event: Optional[threading.Event] = None,
    model_name: Optional[str] = None,
    reasoning_effort: Optional[str] = None,
) -> None:
    """
    - batch_id 상태를 폴링해서 completed 되면
    - output JSONL을 다운로드하고
    - custom_id(row-0, row-1, ...) 기준으로 ST1_결과상품명 컬럼에 병합
    - stop_event 가 set 되면 수집 중단
    - 수집이 정상 완료되면 stage1_run_history 에 토큰/비용 로그 남김
    """
    def log(msg: str):
        if log_fn:
            log_fn(msg)
        else:
            print(msg)

    def check_stop():
        if stop_event is not None and stop_event.is_set():
            log("[COLLECT] 사용자 요청으로 수집 중단.")
            raise RuntimeError("사용자가 결과 수집을 중단했습니다.")

    start_dt = get_seoul_now()
    start_time = time.time()

    log(f"[COLLECT] batch_id={batch_id} 상태 조회 시작...")

    # 1) Batch 상태 폴링
    while True:
        check_stop()

        batch = client.batches.retrieve(batch_id)
        log(f"  - status={batch.status}, request_counts={getattr(batch, 'request_counts', None)}")
        if batch.status in ("completed", "failed", "cancelled", "expired"):
            break

        # poll_interval_sec 동안 1초 단위로 끊어서 중단 여부 체크
        for _ in range(poll_interval_sec):
            check_stop()
            time.sleep(1)

    check_stop()

    if batch.status != "completed":
        raise RuntimeError(f"배치가 완료 상태가 아닙니다: status={batch.status}")

    output_file_id = getattr(batch, "output_file_id", None)
    if not output_file_id:
        # 신버전에서 output_file_ids 배열일 수도 있으므로 보조 처리
        output_ids = getattr(batch, "output_file_ids", None)
        if output_ids and isinstance(output_ids, (list, tuple)) and len(output_ids) > 0:
            output_file_id = output_ids[0]

    if not output_file_id:
        raise RuntimeError("batch.output_file_id 를 찾을 수 없습니다.")

    log(f"[COLLECT] output_file_id={output_file_id} 다운로드 중...")
    file_content = client.files.content(output_file_id)

    if hasattr(file_content, "read"):
        data_bytes = file_content.read()
    elif hasattr(file_content, "iter_bytes"):
        # 일부 클라이언트 구현에서는 iter_bytes() 로 chunk 가 올 수 있음
        chunks = []
        for ch in file_content.iter_bytes():
            chunks.append(ch)
        data_bytes = b"".join(chunks)
    else:
        data_bytes = file_content  # type: ignore

    text = data_bytes.decode("utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]

    # 2) JSONL 한 줄씩 파싱 → 결과/토큰 집계
    result_map: Dict[str, str] = {}
    total_in_tok = 0
    total_out_tok = 0
    total_reasoning_tok = 0
    api_rows = 0

    for ln in lines:
        obj = json.loads(ln)
        custom_id = obj.get("custom_id")
        resp = obj.get("response")
        error = obj.get("error")

        if error is not None:
            log(f"[ERROR] custom_id={custom_id} 에러 발생: {error}")
            continue
        if not resp:
            continue

        refined = extract_text_from_response_dict(resp)
        result_map[custom_id] = refined

        in_tok, out_tok, reasoning_tok = extract_usage_from_response_dict(resp)
        total_in_tok += in_tok
        total_out_tok += out_tok
        total_reasoning_tok += reasoning_tok
        api_rows += 1

    log(f"[COLLECT] 결과 매핑 개수: {len(result_map)}")
    log(
        f"[USAGE] API 호출 수(api_rows)={api_rows}, "
        f"input_tokens={total_in_tok}, output_tokens={total_out_tok}, "
        f"reasoning_tokens={total_reasoning_tok}"
    )

    # 3) 엑셀 병합
    df = pd.read_excel(excel_path)
    total_rows = len(df)

    if "ST1_결과상품명" not in df.columns:
        df["ST1_결과상품명"] = ""
    if "ST1_판매형태" not in df.columns:
        df["ST1_판매형태"] = ""

    for idx in range(len(df)):
        cid = f"row-{idx}"
        if cid in result_map:
            df.at[idx, "ST1_결과상품명"] = result_map[cid]
            df.at[idx, "ST1_판매형태"] = safe_str(df.at[idx, "판매형태"])

    df.to_excel(output_excel_path, index=False)
    log(f"[COLLECT] 엑셀 병합 완료: {output_excel_path}")

    # 4) 비용 계산 + 러닝 타임/히스토리 기록
    elapsed_seconds = time.time() - start_time
    finish_dt = get_seoul_now()

    input_cost_usd = None
    output_cost_usd = None
    total_cost_usd = None

    if model_name:
        cost_info = compute_cost_usd(model_name, total_in_tok, total_out_tok)
        if cost_info:
            input_cost_usd = cost_info["input_cost"]
            output_cost_usd = cost_info["output_cost"]
            total_cost_usd = cost_info["total_cost"]
            log(
                f"[COST] model={model_name}, "
                f"input=${input_cost_usd:.6f}, output=${output_cost_usd:.6f}, "
                f"total=${total_cost_usd:.6f}"
            )

    # stage1_run_history.xlsx 에 한 줄 추가 (ST1-BATCH)
    try:
        append_run_history(
            stage="ST1-BATCH",
            model_name=model_name or "(unknown)",
            reasoning_effort=reasoning_effort or "(unknown)",
            src_file=excel_path,
            total_rows=total_rows,
            api_rows=api_rows,
            elapsed_seconds=elapsed_seconds,
            total_in_tok=total_in_tok,
            total_out_tok=total_out_tok,
            total_reasoning_tok=total_reasoning_tok,
            input_cost_usd=input_cost_usd,
            output_cost_usd=output_cost_usd,
            total_cost_usd=total_cost_usd,
            start_dt=start_dt,
            finish_dt=finish_dt,
        )
        log("[INFO] stage1_run_history.xlsx 에 ST1-BATCH 실행 기록 추가 완료.")
    except Exception as e:
        log(f"[WARN] 실행 이력 기록(stage1_run_history) 중 예외 발생: {e}")
