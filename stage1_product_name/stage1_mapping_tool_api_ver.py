import os
import re
import threading
import json
from typing import Optional, Tuple, Any

import pandas as pd
from openai import OpenAI

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# =========================
# 전역 설정
# =========================

client: Optional[OpenAI] = None
STOP_REQUESTED = False

# API 키 저장 파일 (스크립트와 같은 폴더)
CONFIG_API_KEY_PATH = os.path.join(os.path.dirname(__file__), ".openai_api_key")

# 최종 출력 토큰 상한 (정제 상품명 한 줄이면 128이면 충분)
DEFAULT_MAX_OUTPUT_TOKENS = 128


def safe_str(v) -> str:
    """NaN/None 안전하게 문자열로 변환 + strip."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


# =========================
# Stage1 공통 SYSTEM 프롬프트
# =========================

STAGE1_SYSTEM_PROMPT = """너는 전문 MD이자 온라인 쇼핑몰 *위탁판매*용 상품명 정제 전문가다.  
네 임무는 공급처에서 온 “원본 상품명”을 깔끔하고 정보 중심의 “정제된 상품명”으로 바꾸는 것이다.

[메타 규칙 – 내부 점검]
1. 답변을 만들기 전에 스스로 다음 기준으로 결과물을 점검한다.
   - 정보 정확성: 원본에 있는 정보만 사용하고, 추측 정보(브랜드/용량/재질/기능 등)를 새로 만들지 않는다.
   - 구조: 가능한 한 다음 순서를 우선 고려한다.  
     → 핵심 제품군/타입 > 핵심 기능·효과/사용상황 > 규격·용량/세트·수량 > 사용대상·스타일/핏 > 시즌·테마 > 색상/옵션(필요 시)
   - 간결성: 최대 60자 이내, 의미 없는 수식어·중복 단어·광고 문구·불필요한 기호 제거.
   - 일관성: 동일 유형의 정보는 유사한 표현과 순서를 유지한다(예: “크리스마스 파티 가랜드 레터 산타 장식 소품”처럼).
2. 위 기준에 스스로 “만점”이라 판단될 때까지 내부적으로만 여러 번 다듬은 뒤,
   사용자에게는 최종 완성된 한 줄만 출력한다.
   내부 검토 과정·초안·자기평가는 어떤 형식으로도 출력하지 않는다.

[역할]
- 너는 “상품명 정제”만 담당한다.
- 원본 상품명과 카테고리명을 함께 참고하여, 온라인 위탁판매용으로 적합한 정보 중심 상품명을 만들어야 한다.
- 이 단계의 상품명은 “최종 노출용”이 아니라, 이후 단계(Stage 2, 3)에서 참고할 **정제 버전**이라는 점을 기억한다.

[위탁판매 특이 규칙]
- 브랜드 경쟁력이 없으므로, 일반적인 브랜드명/샵명은 상품명에서 제거한다.
  - 예: 나이키, 아디다스, ○○샵, ○○몰, 엘리스, ○○마켓 등은 모두 제거.
  - 단, “피치기모, 쿠션폼, 수제, 핸드메이드, DIY”처럼 **기능·재질·특성을 나타내는 단어**는 브랜드가 아니라 속성으로 보고 유지한다.
- “/”, “·”, “,” 같은 기호는 되도록 사용하지 말고, 띄어쓰기로 대체한다.
  - 예: “텀블러/컵홀더” → “텀블러 컵홀더”

[추가 메타 정보]
- 이 상품의 판매형태는 “단품형” 또는 “옵션형” 중 하나이다.
  - "단품형"이면 옵션 없이 1개 구성,
  - "옵션형"이면 색상/사이즈/타입 등 여러 옵션 중 선택하는 구조이다.
- 이 정보는 참고용일 뿐, 상품명 안에 억지로 "단품형/옵션형" 같은 단어를 넣을 필요는 없다.

[최종 목표]
- 과장 표현 없이, 검색과 노출에 도움이 되는 “정보 위주 상품명”을 작성한다.
- 브랜드·상점 정보와 광고 문구는 제거하고, 제품의 정체(무엇인지)와 기능·상황·스펙 중심으로 정리한다.
- 최종 출력은 “정제된 상품명 한 줄”만 남도록 한다.

[작업 순서]

1단계. 원본 이해
- user 메시지로 주어지는 "카테고리명"과 "원본 상품명"을 함께 보고 카테고리/제품군을 먼저 파악한다.
- 다음 “핵심 정보”를 머릿속에 정리한다.
  - 핵심 제품군/타입 (예: 러닝화, 가랜드, 나노블럭, 선물주머니, 방한모자, 스노우체인, 쿠션폼벽지 등)
  - 핵심 기능·효과 또는 사용상황 (예: 방한, 단열, 곰팡이방지, 무소음, 안벗겨짐, 비상용, 캠핑용 등)
  - 규격/사이즈/용량/수량 (예: 12L, 50cm x 2.5m, 100개입 등)
  - 사용대상/핏/스타일 (예: 남성 세미일자, 여성 덧신, 수험생 시계, 캠핑용 하드쿨러 등)
  - 필요한 경우 색상/옵션, 시즌·테마 (겨울용, 크리스마스, 수능 등)

2단계. 노이즈(광고/상점/브랜드 정보) 제거
- 아래와 같은 광고성 표현 및 상점 관련 정보는 전부 제거한다.
  - 예시 광고 문구:
    “무료배송”, “당일배송”, “오늘출발”, “행사”, “인기”, “강력추천”, “최저가”, “빅세일”, “특가”, “인싸템”, “MD추천”, “한정수량”, “국민템”, “핫딜” 등
  - 이모티콘/기호:
    “♥”, “★”, “♬”, “※”, “!!”, “??” 등
  - 상점·몰 이름:
    “○○몰”, “○○샵”, “○○스토어”, “공식몰”, “전문몰”, “직영몰” 등
  - 일반적인 브랜드명:
    나이키, 아디다스, ○○마켓, ○○브랜드 등 위탁판매자가 경쟁 우위를 갖지 못하는 브랜드명
  - 연락처·홍보:
    전화번호, 카카오톡 ID, URL, SNS 아이디 등
- 위 예시는 참고용이며, 이와 유사한 모든 광고성·홍보성·브랜드 문구도 함께 제거한다.

3단계. 괄호·기호 정리
- 괄호는 필요한 최소 개수만 사용한다.
- 의미 있는 정보는 살리고, 괄호 기호만 정리한다.
  - 예: “(블루)” → “블루”, “(레터산타)” → “레터 산타”
- 이중 괄호·불필요한 괄호·빈 괄호는 제거한다.
  - 예: “(( ))”, “()” 등은 삭제.
- “/”, “·”, “,” 등 기호는 되도록 사용하지 말고, 단순한 띄어쓰기로 바꾼다.

4단계. 핵심 정보 유지 & 구조화
- 반드시 유지해야 할 정보:
  - 핵심 제품군/타입 (예: 러닝화, 가랜드, 나노블럭, 선물주머니, 방한모자, 스노우체인, 쿠션폼벽지 등)
  - 핵심 기능·효과/사용상황 (예: 방한용, 단열용, 곰팡이방지용, 무소음 시험용, 비상용, 캠핑용 등)
  - 규격·용량/세트·수량 (예: 12L, 100개입, 50cm x 2.5m, 4P 세트 등)
  - 사용대상·스타일/핏 (예: 남성 세미일자, 여성 페이크삭스, 수험생용, 캠핑·아웃도어용 등)
  - 필요한 경우 색상·옵션, 시즌·테마
- 의미 없는 중복 단어와 모호한 수식어는 줄인다.
- 핵심 키워드 위주로 자연스럽고 일관된 순서로 재배치한다.
  - 예:
    원본: “크리스마스파티가랜드(레터산타) 파티용품 장식 소품”  
    정제: “크리스마스 파티 가랜드 레터 산타 장식 소품”

5단계. 위험한 변조 금지
- 실제 원본에 없는 정보를 새로 만들어내지 않는다.
  - 재질, 용량, 기능, 인증 정보를 추측으로 추가하면 안 된다.
- 숫자, 모델명, 용량, 사이즈는 원본에 있는 그대로 사용한다.
- 한글/영문 표기는 원본을 존중하되, 불필요한 반복이나 어색한 표기는 정돈한다.
  - 예: “X-mas”와 “크리스마스”가 함께 있으면, 상황에 맞게 한 형태로만 통일.

6단계. 길이 및 형식 제한
- 정제된 상품명은 “최대 60자 이내”로 작성한다.
- 60자를 초과하면, 중요도가 낮은 수식어부터 순서대로 제거해 60자 이내로 줄인다.
- 문장이 아니라 “상품명 형태”로 작성한다.
  - “~입니다”, “~해요”, “어떠세요?” 같은 문장형 표현은 사용하지 않는다.
- 이모티콘, 과장된 느낌표, 의미 없는 기호는 모두 제거한다.

7단계. 입력 & 출력 형식
- user 메시지로 전달되는 아래 입력 정보를 참고해 상품명을 정제한다.
  - 카테고리명
  - 판매형태
  - 원본 상품명
- 출력 규칙:
  - 오직 “정제된 상품명 한 줄”만 출력한다.
  - 따옴표(“ ”, ' ')로 감싸지 않는다.
  - “정제된 상품명: …” 같은 라벨·설명 문구를 붙이지 않는다.
  - 단계 설명, 이유, 해설, 메모, 추가 문장은 절대 쓰지 않는다.
"""

# 각 행마다 동적으로 붙는 user 프롬프트(짧은 부분만 반복)
STAGE1_USER_PROMPT_TEMPLATE = """[입력 정보]
- 카테고리명: {category}
- 판매형태: {sale_type}
- 원본 상품명: {raw_name}

이제 위 규칙을 적용하여 아래 원본 상품명을 정제하라.
정제할 상품명 : "{raw_name}"
"""


# =========================
# 응답 텍스트 추출 유틸
# =========================

def looks_like_response_id(s: str) -> bool:
    """resp_xxxxx 형태의 Response ID 같은 문자열은 버리기."""
    if not s:
        return False
    if not s.startswith("resp_"):
        return False
    tail = s[5:]
    return bool(re.fullmatch(r"[0-9a-f]{16,}", tail))


def _extract_from_text_obj(text_obj: Any) -> str:
    """content[*].text 객체에서 문자열을 최대한 뽑아낸다."""
    # 이미 문자열이면 그대로
    if isinstance(text_obj, str):
        return text_obj.strip()

    # value 속성 시도 (Responses 공식 구조)
    val = getattr(text_obj, "value", None)
    if isinstance(val, str) and val.strip():
        return val.strip()

    # dict 비슷한 구조면 안쪽에서 value/text 찾기
    data = None
    try:
        if hasattr(text_obj, "model_dump"):
            data = text_obj.model_dump()
        elif isinstance(text_obj, dict):
            data = text_obj
        else:
            data = getattr(text_obj, "__dict__", None)
    except Exception:
        data = None

    if isinstance(data, dict):
        for key in ("value", "text", "content"):
            v = data.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()

    # 마지막 fallback: 그냥 str() 해보고 써먹을 수 있으면 사용
    try:
        s = str(text_obj)
        if s and not s.startswith("<") and not s.endswith(">"):
            return s.strip()
    except Exception:
        pass

    return ""


def extract_text_from_response(response: Any, prompt_hint: Optional[str] = None) -> str:
    """
    OpenAI responses.create 응답 객체에서 텍스트를 최대한 안전하게 추출.

    1순위: response.output[0].content[0].text(.value)
    2순위: response.output_text / response.text
    3순위: model_dump()로 JSON 전체를 훑어서 문자열 후보를 모은 뒤,
           프롬프트와의 유사도를 기준으로 '답변 텍스트'일 가능성이 높은 것을 선택.
    """

    # --- 1) 공식 output 구조 우선 시도 ---
    try:
        output = getattr(response, "output", None)
        if output:
            # 리스트 / 튜플 / 단일 객체 모두 대응
            items = output
            if not isinstance(items, (list, tuple)):
                items = [items]

            for item in items:
                # dict / 객체 모두 처리
                if isinstance(item, dict):
                    content = item.get("content")
                else:
                    content = getattr(item, "content", None)

                if not content:
                    continue

                # dict / 리스트 모두 처리
                if isinstance(content, dict):
                    content_list = [content]
                else:
                    content_list = list(content)

                for c in content_list:
                    if isinstance(c, dict):
                        text_obj = c.get("text")
                    else:
                        text_obj = getattr(c, "text", None)
                    if text_obj is None:
                        continue
                    s = _extract_from_text_obj(text_obj)
                    if s:
                        return s.strip()
    except Exception:
        pass

    # --- 2) SDK 편의 속성 시도 ---
    for attr in ("output_text", "text"):
        val = getattr(response, attr, None)
        if isinstance(val, str) and val.strip():
            return val.strip()

        if val is not None and not isinstance(val, str):
            v = getattr(val, "value", None)
            if isinstance(v, str) and v.strip():
                return v.strip()
            if isinstance(val, dict):
                for k in ("value", "text"):
                    txt = val.get(k)
                    if isinstance(txt, str) and txt.strip():
                        return txt.strip()

    # --- 3) 전체 JSON을 훑어서 후보 문자열 찾기 (마지막 fallback) ---
    data = None
    try:
        if hasattr(response, "model_dump"):
            data = response.model_dump()
        elif hasattr(response, "dict"):
            data = response.dict()
    except Exception:
        data = None

    if not isinstance(data, dict):
        return ""

    strings: list[str] = []

    def _collect_strings(obj, acc):
        """중첩된 dict/list 구조에서 모든 문자열을 수집."""
        if isinstance(obj, str):
            s = obj.strip()
            if s:
                acc.append(s)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                # 프롬프트/지시문/ID/모델명 등은 제외
                if k in ("prompt", "input", "instructions", "id", "model", "object", "type", "role"):
                    continue
                _collect_strings(v, acc)
        elif isinstance(obj, (list, tuple)):
            for x in obj:
                _collect_strings(x, acc)

    _collect_strings(data, strings)

    # 후보 정제
    uniq = []
    seen = set()
    for s in strings:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)

    candidates = []
    for s in uniq:
        # resp_... 같은 ID는 버리기
        if looks_like_response_id(s):
            continue

        # 프롬프트랑 완전히 같은 문자열은 제외
        if prompt_hint and s == prompt_hint:
            continue
        # 너무 짧거나(1글자) 너무 긴 것(설명문 등)은 제외
        if len(s) < 2 or len(s) > 200:
            continue
        # 프롬프트 안에 거의 그대로 포함된 긴 문장(프롬프트 일부)도 제외
        if prompt_hint and s in prompt_hint and len(s) > len(prompt_hint) * 0.5:
            continue

        candidates.append(s)

    if not candidates:
        return ""

    # 공백이 있고, 어느 정도 길이가 있는 문자열일수록 "문장/상품명"일 가능성이 크다고 가정
    best = max(candidates, key=lambda s: (s.count(" "), len(s)))
    return best.strip()


# =========================
# OpenAI 호출 함수
# =========================

def call_stage1_model(
    row_prompt: str,
    system_prompt: str,
    model: str = "gpt-5-nano",
    temperature: float = 0.2,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    max_retries: int = 3,
    debug_log=None,
) -> Tuple[str, int, int]:
    """
    Stage1 프롬프트 1개를 OpenAI에 보내고,
    정제된 상품명(텍스트)와 토큰 사용량을 반환.

    반환: (result_text, input_tokens, output_tokens)
    """
    global client

    row_prompt = safe_str(row_prompt)
    system_prompt = safe_str(system_prompt)
    if not row_prompt:
        return "", 0, 0

    if client is None:
        raise RuntimeError("OpenAI 클라이언트가 초기화되지 않았습니다.")

    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            # Responses API: instructions(=system), input(=user)
            kwargs = {
                "model": model,
                "instructions": system_prompt,
                "input": row_prompt,
                "max_output_tokens": max_output_tokens,
            }

            # gpt-5 계열은 reasoning 모델 → temperature 무시, reasoning.effort 사용
            if model.startswith("gpt-5"):
                kwargs["reasoning"] = {"effort": "medium"}
            else:
                kwargs["temperature"] = temperature

            response = client.responses.create(**kwargs)

            # === Debug: 전체 response 덤프 ===
            if debug_log is not None:
                try:
                    debug_log("[DEBUG] raw response 객체:")
                    debug_log(str(response))

                    if hasattr(response, "model_dump_json"):
                        debug_log("[DEBUG] response.model_dump_json():")
                        debug_log(response.model_dump_json(indent=2))
                    elif hasattr(response, "model_dump"):
                        debug_log("[DEBUG] response.model_dump():")
                        debug_log(json.dumps(response.model_dump(), ensure_ascii=False, indent=2))
                except Exception as e:
                    debug_log(f"[DEBUG] response 덤프 중 오류: {e}")

            # === 응답 텍스트 추출 ===
            result_text = extract_text_from_response(response, prompt_hint=row_prompt)

            usage = getattr(response, "usage", None)
            in_tokens = getattr(usage, "input_tokens", 0) if usage else 0
            out_tokens = getattr(usage, "output_tokens", 0) if usage else 0

            # 디버깅: 텍스트가 완전히 비었는데 토큰은 소비된 경우만 경고
            if (in_tokens or out_tokens) and not result_text:
                if debug_log is not None:
                    debug_log("[WARN] 응답에서 텍스트를 추출하지 못했습니다. JSON 구조가 예상과 다를 수 있습니다.")
                else:
                    print("[WARN] 응답에서 텍스트를 추출하지 못했습니다. JSON 구조가 예상과 다를 수 있습니다.")

            return result_text, in_tokens, out_tokens

        except Exception as e:
            last_err = e
            err_str = str(e)

            # 에러 문자열에서 status code 추출 (예: "Error code: 404 - {...}")
            m = re.search(r"Error code:\s*(\d+)", err_str)
            status_code = int(m.group(1)) if m else None

            if debug_log is not None:
                debug_log(f"[WARN] OpenAI 호출 실패({attempt}/{max_retries}) - {err_str}")
            else:
                print(f"[WARN] OpenAI 호출 실패({attempt}/{max_retries}) - {err_str}")

            # 모델 없음(404), quota 부족(429)은 재시도해도 의미 없음 → 바로 중단
            if status_code in (404, 429):
                break

    # 여기까지 왔으면 실패
    raise RuntimeError(f"OpenAI 호출 반복 실패: {last_err}")


# =========================
# 엑셀 처리 메인 로직
# =========================

def run_stage1_on_excel(
    excel_path: str,
    model: str,
    temperature: float,
    max_output_tokens: int,
    save_every: int,
    overwrite: bool,
    log_func=print,
) -> str:
    """
    Stage1 맵핑 엑셀을 읽어서 각 행의
    (카테고리명, 판매형태, 원본상품명)을 기반으로
    OpenAI를 호출하고, ST1_정제상품명을 채워 넣는다.
    """
    global STOP_REQUESTED

    log = log_func
    log(f"[INFO] 엑셀 로드: {excel_path}")

    df = pd.read_excel(excel_path, dtype=str)

    # 필수 컬럼 체크
    required_cols = ["카테고리명", "원본상품명", "판매형태"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"엑셀에 필수 컬럼이 없습니다: {missing}")

    # 결과 컬럼 없으면 새로 생성
    if "ST1_정제상품명" not in df.columns:
        df["ST1_정제상품명"] = ""

    # 출력 파일 경로
    base_dir = os.path.dirname(excel_path)
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    out_path = os.path.join(base_dir, f"{base_name}_stage1_completed.xlsx")

    total_rows = len(df)
    log(f"[INFO] 총 행 수: {total_rows}")
    if model.startswith("gpt-5"):
        log(f"[INFO] 사용할 모델: {model} (reasoning.effort=medium, temperature 무시)")
    else:
        log(f"[INFO] 사용할 모델: {model}")
        log(f"[INFO] temperature: {temperature:.2f}")
    log(f"[INFO] max_output_tokens: {max_output_tokens}")
    log(f"[INFO] save_every: {save_every}, overwrite: {overwrite}")

    processed = 0
    total_in_tokens = 0
    total_out_tokens = 0

    for idx, row in df.iterrows():
        # 사용자 중단 요청 확인
        if STOP_REQUESTED:
            log("[INFO] 사용자 중단 요청 감지. 현재까지 진행된 결과만 저장하고 종료합니다.")
            break

        category = safe_str(row.get("카테고리명", ""))
        raw_name = safe_str(row.get("원본상품명", ""))
        sale_type = safe_str(row.get("판매형태", ""))

        if not raw_name:
            continue

        current_result = safe_str(row.get("ST1_정제상품명", ""))
        if current_result and not overwrite:
            # 이미 결과가 있고 덮어쓰기 옵션이 아니면 스킵
            continue

        # 행별 user 프롬프트 구성
        row_prompt = STAGE1_USER_PROMPT_TEMPLATE.format(
            category=category,
            sale_type=sale_type,
            raw_name=raw_name,
        )

        log(f"\n[INFO] 행 {idx} 처리 중...")
        log(f"      원본상품명: {raw_name}")
        log(f"      카테고리명: {category}")
        log(f"      판매형태: {sale_type}")

        try:
            result_text, in_tok, out_tok = call_stage1_model(
                row_prompt=row_prompt,
                system_prompt=STAGE1_SYSTEM_PROMPT,
                model=model,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
                debug_log=log,   # 🔍 여기서 전체 response 로그로 확인
            )
        except Exception as e:
            msg = str(e)
            log(f"[ERROR] 행 {idx} 처리 실패: {msg}")

            # quota 부족 → 더 진행해도 전부 실패하므로 즉시 중단
            if "insufficient_quota" in msg or "You exceeded your current quota" in msg:
                log("[ERROR] OpenAI 크레딧/요금 한도를 초과했습니다.")
                log("[ERROR] platform.openai.com에서 결제/한도를 확인한 후 다시 시도해 주세요.")
                break

            # 모델 없음 / 권한 없음 → 즉시 중단
            if "model_not_found" in msg or "does not exist" in msg:
                log("[ERROR] 선택한 모델이 존재하지 않거나 접근 권한이 없습니다.")
                log("[ERROR] 다른 모델을 선택한 후 다시 실행해 주세요.")
                break

            # 그 외 에러는 해당 행만 건너뛰고 다음 행으로 진행
            continue

        # 결과 기록
        df.at[idx, "ST1_정제상품명"] = result_text
        processed += 1
        total_in_tokens += in_tok
        total_out_tokens += out_tok

        log(f"[OK] 행 {idx} 완료")
        log(f"     결과: {result_text}")
        if in_tok or out_tok:
            log(f"     tokens in/out = {in_tok}/{out_tok}")

        # 중간 저장
        if processed > 0 and processed % save_every == 0:
            log(f"[INFO] {processed}개 처리, 중간 저장: {out_path}")
            df.to_excel(out_path, index=False)

    # 최종 저장
    log(f"\n[INFO] 최종 저장: {out_path}")
    df.to_excel(out_path, index=False)

    log(f"[INFO] 처리 완료. 새로 처리된 행 수: {processed}")
    log(f"[INFO] 총 토큰 사용량: input={total_in_tokens}, output={total_out_tokens}")

    return out_path


# =========================
# Tkinter GUI
# =========================

class Stage1APIRunnerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Stage1 상품명 정제 API 실행기")
        root.geometry("920x650")

        self.selected_file: Optional[str] = None

        # ----- 상단 설명 -----
        desc = (
            "① 대상 파일\n"
            "   - Stage1 맵핑 툴(API 버전)로 생성한 '*_stage1_mapping_api.xlsx' 파일을 사용합니다.\n\n"
            "② 필수 컬럼\n"
            "   - 카테고리명, 원본상품명, 판매형태, ST1_정제상품명(없으면 자동 생성)\n\n"
            "③ 동작 방식\n"
            "   - 각 행의 (카테고리명/판매형태/원본상품명)을 OpenAI API로 보내고,\n"
            "     결과를 ST1_정제상품명에 채운 뒤 '*_stage1_completed.xlsx'로 저장합니다.\n"
            "   - '기존 결과 덮어쓰기'를 끄면 이미 채워진 행은 자동으로 건너뜁니다."
        )
        lbl_desc = tk.Label(root, text=desc, justify="left", anchor="w")
        lbl_desc.pack(fill=tk.X, padx=10, pady=(8, 4))

        # ----- 설정 영역 -----
        cfg_frame = tk.Frame(root)
        cfg_frame.pack(fill=tk.X, padx=10, pady=5)

        # API 키
        tk.Label(cfg_frame, text="OpenAI API 키:").grid(row=0, column=0, sticky="e", padx=5, pady=2)
        self.api_entry = tk.Entry(cfg_frame, width=55, show="*")
        self.api_entry.grid(row=0, column=1, columnspan=3, sticky="w", padx=5, pady=2)

        # 저장된 API 키 로드
        self.load_api_key()

        # 모델 선택  ✅ 기본값 gpt-5-nano
        tk.Label(cfg_frame, text="모델:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
        self.model_var = tk.StringVar(value="gpt-5-nano")
        self.cmb_model = ttk.Combobox(
            cfg_frame,
            textvariable=self.model_var,
            values=[
                "gpt-5-nano",   # GPT-5 nano
                "gpt-5-mini",   # 빠르고 저렴한 reasoning
                "gpt-5.1",      # 최고 품질 reasoning
            ],
            width=15,
            state="readonly",
        )
        self.cmb_model.grid(row=1, column=1, sticky="w", padx=5, pady=2)

        lbl_model_hint = tk.Label(
            cfg_frame,
            text="gpt-5-nano: 초저비용 / gpt-5-mini: 빠르고 저렴 / gpt-5.1: 최고 품질(비용↑)",
            fg="#555",
            anchor="w",
        )
        lbl_model_hint.grid(row=1, column=2, columnspan=2, sticky="w", padx=5, pady=2)

        # temperature 슬라이더
        tk.Label(cfg_frame, text="temperature:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
        self.temp_var = tk.DoubleVar(value=0.2)
        self.temp_scale = tk.Scale(
            cfg_frame,
            variable=self.temp_var,
            from_=0.0,
            to=1.0,
            resolution=0.05,
            orient=tk.HORIZONTAL,
            length=200,
        )
        self.temp_scale.grid(row=2, column=1, sticky="w", padx=5, pady=2)

        lbl_temp_hint = tk.Label(
            cfg_frame,
            text="낮을수록 결정적, 높을수록 랜덤성↑ (gpt-5 계열에서는 내부적으로 무시될 수 있음)",
            fg="#555",
            anchor="w",
        )
        lbl_temp_hint.grid(row=2, column=2, columnspan=2, sticky="w", padx=5, pady=2)

        # 중간 저장 간격
        tk.Label(cfg_frame, text="중간 저장 간격(행):").grid(row=3, column=0, sticky="e", padx=5, pady=2)
        self.save_every_var = tk.IntVar(value=10)
        self.spin_save_every = tk.Spinbox(
            cfg_frame,
            from_=1,
            to=1000,
            textvariable=self.save_every_var,
            width=6,
        )
        self.spin_save_every.grid(row=3, column=1, sticky="w", padx=5, pady=2)

        # 덮어쓰기 여부
        self.overwrite_var = tk.BooleanVar(value=False)
        chk_overwrite = tk.Checkbutton(
            cfg_frame,
            text="기존 ST1_정제상품명 덮어쓰기",
            variable=self.overwrite_var,
        )
        chk_overwrite.grid(row=3, column=2, sticky="w", padx=5, pady=2)

        # ----- 파일/실행 버튼 영역 -----
        btn_frame = tk.Frame(root)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)

        self.btn_select = tk.Button(
            btn_frame,
            text="Stage1 맵핑 엑셀 선택",
            command=self.on_select_file,
        )
        self.btn_select.pack(side=tk.LEFT, padx=5)

        self.btn_run = tk.Button(
            btn_frame,
            text="실행",
            command=self.on_run_click,
        )
        self.btn_run.pack(side=tk.LEFT, padx=5)

        self.btn_stop = tk.Button(
            btn_frame,
            text="중단 요청",
            command=self.on_stop_click,
        )
        self.btn_stop.pack(side=tk.LEFT, padx=5)

        self.lbl_file = tk.Label(btn_frame, text="선택된 파일: (없음)", anchor="w")
        self.lbl_file.pack(side=tk.LEFT, padx=10)

        # ----- 로그 영역 -----
        self.log_box = ScrolledText(root, wrap=tk.WORD, height=20)
        self.log_box.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    # 로그 출력 (UI 스레드에서 실행)
    def _append_log(self, msg: str):
        self.log_box.insert(tk.END, msg + "\n")
        self.log_box.see(tk.END)

    def log(self, msg: str):
        self.root.after(0, self._append_log, msg)

    # API 키 저장/로드
    def load_api_key(self):
        if os.path.exists(CONFIG_API_KEY_PATH):
            try:
                with open(CONFIG_API_KEY_PATH, "r", encoding="utf-8") as f:
                    key = f.read().strip()
                    if key:
                        self.api_entry.insert(0, key)
            except Exception:
                pass

    def save_api_key(self, api_key: str):
        api_key = api_key.strip()
        if not api_key:
            return
        try:
            with open(CONFIG_API_KEY_PATH, "w", encoding="utf-8") as f:
                f.write(api_key)
        except Exception as e:
            self.log(f"[WARN] API 키 저장 중 오류: {e}")

    # 파일 선택
    def on_select_file(self):
        filepath = filedialog.askopenfilename(
            title="Stage1 맵핑 엑셀 파일 선택 (*_stage1_mapping_api.xlsx)",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if not filepath:
            return

        base_name = os.path.basename(filepath)
        # 파일명 검사: '_stage1_mapping' 포함 여부 (stage1_mapping_api 도 포함됨)
        if "_stage1_mapping" not in base_name:
            messagebox.showerror(
                "파일 이름 오류",
                "파일 이름에 '_stage1_mapping' 이 포함된 Stage1 맵핑 파일만 사용할 수 있습니다.",
            )
            return

        self.selected_file = filepath
        self.lbl_file.config(text=f"선택된 파일: {filepath}")

    # 실행 버튼
    def on_run_click(self):
        global STOP_REQUESTED

        api_key = self.api_entry.get().strip()
        if not api_key:
            messagebox.showerror("입력 오류", "OpenAI API 키를 입력해 주세요.")
            return

        if not self.selected_file:
            messagebox.showerror("입력 오류", "Stage1 맵핑 엑셀 파일을 먼저 선택해 주세요.")
            return

        # 파일명 한 번 더 검증
        base_name = os.path.basename(self.selected_file)
        if "_stage1_mapping" not in base_name:
            messagebox.showerror(
                "파일 이름 오류",
                "파일 이름에 '_stage1_mapping' 이 포함된 Stage1 맵핑 파일만 사용할 수 있습니다.",
            )
            return

        model = self.model_var.get()
        temperature = float(self.temp_var.get())
        save_every = int(self.save_every_var.get())
        overwrite = bool(self.overwrite_var.get())

        # 중단 플래그 초기화
        STOP_REQUESTED = False

        # 실행 버튼 잠금
        self.btn_run.config(state=tk.DISABLED)

        # 백그라운드 스레드에서 API 작업 수행
        thread = threading.Thread(
            target=self.run_task,
            args=(api_key, self.selected_file, model, temperature, save_every, overwrite),
            daemon=True,
        )
        thread.start()

    # 중단 버튼
    def on_stop_click(self):
        global STOP_REQUESTED
        STOP_REQUESTED = True
        self.log("[INFO] 중단 요청 플래그 설정됨. 현재 처리 중인 행 이후부터 중단됩니다.")

    # 백그라운드 작업
    def run_task(
        self,
        api_key: str,
        filepath: str,
        model: str,
        temperature: float,
        save_every: int,
        overwrite: bool,
    ):
        global client
        try:
            # 클라이언트 초기화
            client = OpenAI(api_key=api_key)

            # 초기화 성공 시 키 저장
            self.save_api_key(api_key)

            self.log(f"[INFO] 선택된 파일: {filepath}")
            self.log(f"[INFO] 선택 모델: {model}")
            if model.startswith("gpt-5"):
                self.log("[INFO] temperature 설정값: %.2f (gpt-5 계열은 내부적으로 무시될 수 있음)" % temperature)
            else:
                self.log("[INFO] temperature 설정값: %.2f" % temperature)
            self.log(f"[INFO] 중간 저장 간격: {save_every}행")
            self.log(f"[INFO] 기존 결과 덮어쓰기: {overwrite}")

            out_path = run_stage1_on_excel(
                excel_path=filepath,
                model=model,
                temperature=temperature,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
                save_every=save_every,
                overwrite=overwrite,
                log_func=self.log,
            )

            self.log("[INFO] 모든 작업 완료")
            self.root.after(
                0,
                lambda: messagebox.showinfo(
                    "완료",
                    f"Stage1 API 실행이 완료되었습니다.\n\n{out_path}",
                ),
            )
        except Exception as e:
            self.log("[FATAL] 오류 발생:")
            self.log(str(e))
            self.root.after(
                0,
                lambda: messagebox.showerror("오류", f"작업 중 오류가 발생했습니다.\n\n{e}"),
            )
        finally:
            self.root.after(0, lambda: self.btn_run.config(state=tk.NORMAL))


def main():
    root = tk.Tk()
    app = Stage1APIRunnerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
