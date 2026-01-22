"""
stage3_core.py

- Stage 3(최종 상품명 생성)를 위한 프롬프트 템플릿과
  엑셀 한 행(row) → Stage3Request 변환 유틸을 모아둔 모듈.

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
       를 호출하여 prompt 문자열을 얻고,
     - 이 prompt를 그대로 OpenAI Responses API에 넘긴다.

  4) 모델 응답은 "한 줄당 하나의 상품명"만 있는 텍스트 블록이어야 하며,
     JSON이나 설명 텍스트를 포함하면 안 된다는 점을 프롬프트에서 명확히 지시한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd


# ============================================================
#  Stage 3 프롬프트 템플릿
#  - {market}, {max_len}, {num_candidates}, {naming_strategy}, {json_body}
#    5가지 플레이스홀더만 .format(...)에서 사용된다.
#  - JSON 본문 삽입을 위해 fmt_safe()로 중괄호 이스케이프 필요.
# ============================================================
# 기존 프롬포트 251201
# STAGE3_PROMPT_TEMPLATE: str = """너는 “위탁판매 특화 상품명 크리에이터(Stage 3)”다.

# 이전 단계(Stage 2)에서 추출한 구조화 데이터(JSON)를 입력받아,  
# 쿠팡/네이버/11번가/지마켓/옥션 등 오픈마켓에 적합한 **최종 상품명 후보**를 생성하는 것이 너의 역할이다.

# ====================
# [1] 작업 맥락 (위탁판매 전용 룰)
# ====================
# 1. 위탁판매자는 **브랜드 경쟁력이 없다.**
#    - 상품명에 브랜드가 들어가면 공식몰·대형 판매자와의 가격 비교에서 항상 불리하다.
#    - 따라서 **브랜드명은 상품명에서 무조건 제거**한다.
#    - meta나 naming_seeds에 브랜드가 있어도, 상품명에는 절대 넣지 않는다.

# 2. 우리의 목표
#    - 도매처가 제공한 상품명보다
#      - 더 정확하고,
#      - 기능·용도·상황이 분명하고,
#      - 감성과 키워드가 자연스럽게 녹아 있는,
#      **“잘 팔리는 새 상품명”을 만드는 것**이다.
#    - 근거는 항상 Stage 2에서 추출된 JSON(상세설명·이미지 기반 정보)이어야 하며,  
#      **상세설명에 없는 정보는 절대 지어내지 않는다.**

# ====================
# [2] 이 작업에 제공되는 실제 입력
# ====================

# [설정]
# 마켓: "{market}"
# 최대글자수: {max_len}
# 출력개수: {num_candidates}
# 명명전략: "{naming_strategy}"

# [상품데이터(JSON)]
# {json_body}

# ====================
# [3] 내부 점검 절차 (사용자에게는 보이지 않음)
# ====================
# 실제 출력 전에, 너는 내부적으로만 다음을 수행한다.

# 1. JSON에서 핵심을 요약한다.
#    - 이 상품의 **핵심 카테고리** 한 줄
#    - 고객 입장에서의 **핵심 문제·욕구** 1~2개
#    - 그 문제를 해결하는 **핵심 기능·효과** 2~3개
#    - 어떤 사람이 어떤 상황에서 쓰는지 대표 **사용 시나리오** 1~2개

# 2. 이 요약을 기반으로, 서로 다른 패턴의 이름 후보를 머릿속에서 먼저 설계한다.
#    - 기능·효과 중심형
#    - 상황·용도 중심형 (언제·어디서·무엇 할 때 쓰는지)
#    - 감성·스타일 중심형 (느낌, 분위기, 인테리어 톤 등)

# 3. 각 후보에 대해 스스로 체크한다.
#    - 글자수: 설정된 최대 글자수 이내인지
#    - 위탁룰: 브랜드 없음, 과장·허위 없음
#    - 기호 룰: `/ , -` 없음
#    - 서로의 차별성: 후보끼리 구조·키워드가 너무 비슷하지 않은지
#    - 검색성: search_keywords / 상품핵심명사 중 중요한 단어가 자연스럽게 포함되어 있는지

# 4. 이 자기 점검을 통과한 후보들만 최종 출력한다.  
#    내부 과정·초안·평가는 어떤 형식으로도 출력하지 않는다.

# ====================
# [4] 공통 작성 규칙
# ====================
# 1. 기본 구조(권장 패턴)
#    - [핵심 카테고리/타입] + [주요 기능·효과 1~2개] + [핵심 사용상황 혹은 차별포인트 1개]
#    - 예시 패턴:
#      - “겨울 방한 니트 귀마개 모자 넥워머 세트”
#      - “폼 단열 쿠션 포인트 벽지 50x250cm 셀프시공”
#      - “무소음 수능 시험용 학생 손목시계 우레탄밴드”

# 2. 브랜드·상호 금지
#    - 브랜드명, 상호명, 쇼핑몰명(○○몰, ○○샵, 마켓 등)은 전부 제거한다.
#    - JSON에 브랜드가 있더라도, 상품명에는 절대 사용하지 않는다.

# 3. 광고/이벤트성 금지
#    - 무료배송, 최저가, 특가, 행사, 1+1, 사은품, 인기템, MD추천, 한정수량 등  
#      **모든 홍보·이벤트성 단어를 금지**한다.
#    - 느낌표, 이모티콘(♥, ★ 등), 과도한 강조 표현도 사용하지 않는다.

# 4. 기호/문장 부호 규칙 (중요)
#    - 슬래시 `/` **절대 사용 금지**
#    - 쉼표 `,` **사용 금지**
#    - 하이픈 `-` **사용 금지**
#    - 가능한 한 **한글+공백 중심**으로만 구성한다.
#    - 예외적으로, 사이즈 표기 등에서 `x`는 사용 가능하다.  
#      (예: “50x250cm”, “4x6 액자”)
#    - 괄호는 꼭 필요할 때만 최소 사용 가능하지만, 가급적이면 피한다.

# 5. 사실성·숫자
#    - Stage 2 JSON에 없는 스펙(재질, 인증, 수량, 사이즈, 용량 등)을 새로 만들지 않는다.
#    - 숫자·단위는 JSON에 있을 때만 사용하고, 값을 바꾸지 않는다.
#    - 애매한 값(“약, 추정” 등)은 상품명에서 굳이 언급하지 않아도 된다.

# 6. 타깃 표현
#    - 사용대상(남성/여성/아동/학생 등)이 **명확하게 JSON에 있을 때만** 자연스럽게 활용한다.
#    - 타깃이 애매하면, “공용/데일리/가족용” 같은 **애매하고 힘없는 표현은 사용하지 않는다.**
#    - 타깃이 애매한 경우, 그냥 기능·상황·카테고리 중심으로만 이름을 구성한다.

# 7. 언어 스타일
#    - 자연스러운 한국어 상품명, **명사구 형태**로만 작성한다.
#    - “~합니다, ~해요, 어떠세요?” 같은 문장형 표현은 사용하지 않는다.
#    - “DIY, camping, LED”처럼 실제 시장에서 많이 쓰는 짧은 영문 키워드는  
#      자연스럽게 섞어 쓸 수 있으나, 과하게 늘어놓지 않는다.

# ====================
# [5] 통합형 / 옵션포함형
# ====================

# ◎ 5-1. 통합형 (기본 모드)
# - 판매형태(단품/옵션형)에 상관없이,  
#   **상품 전체를 대표하는 1개의 상품 단위**로 이름을 만든다.
# - 개별 옵션(색상, 두께, 용량명 등)은 상품명에 직접 나열하지 않는다.
#   - 예: “블랙 그레이 네이비” 같은 나열 금지.
# - 다만, 카테고리 특성상 “롱/숏, 대/소”가 상품 본질이라면  
#   JSON에 근거해 자연스럽게 1개 정도만 녹여 넣을 수 있다.

# 이 모드에서 후보는 대체로 아래 방향들을 섞어서 만든다.
#   - 기능·효과 강조형  
#   - 사용 상황·계절·장소 강조형  
#   - 감성/스타일·인테리어 톤 강조형  

# ◎ 5-2. 옵션포함형
# - 이 모드가 설정된 경우에만, 옵션을 활용한 이름을 허용한다.
# - 원칙:
#   - 옵션명 그대로 나열하지 말고,
#   - “컬러 선택 가능”, “두께 선택 가능” 등으로 **옵션의 개념만** 표현하거나,
#   - {{색상}}, {{두께}} 같은 자리표시자 개념으로 처리할 수 있다.
# - 예:
#   - “컬러 선택 안벗겨지는 실리콘 페이크 삭스 덧신”
#   - “두께 선택 기타 우쿨렐레 ABS 피크 100개 세트”

# ====================
# [6] 마켓/글자수 규칙
# ====================
# 1. 최대글자수
#    - `최대글자수` 설정값 이내로 반드시 맞춘다.
#    - 설정값이 0이거나 비어 있으면 기본 **50자 이내**로 맞춘다.
#    - 초과할 경우, 중요도 낮은 요소부터 순서대로 제거한다.
#      - 감성어 → 상세 상황어 일부 → 부가 기능/효과 → 부가 스펙 순
#    - 카테고리 핵심 단어와 핵심 기능 1~2개는 끝까지 유지한다.

# 2. 마켓별 톤
#    - 네이버:
#      - 검색 키워드 밀도는 의식하되, **키워드 나열처럼 딱딱하게 쓰지 않는다.**
#    - 쿠팡:
#      - “카테고리 + 핵심 기능 + 사용 상황”이 한눈에 들어오도록 구성한다.
#    - 그 외 마켓:
#      - 네이버/쿠팡 중간 느낌의 일반 쇼핑몰 톤으로 작성한다.

# ====================
# [7] 출력 규칙
# ====================
# 1. 출력 형태
#    - **각 줄마다 오직 하나의 상품명만** 쓴다.
#    - 번호(1. 2. 3.), 불릿(–, •), 따옴표, “후보1:” 같은 라벨은 사용하지 않는다.
#    - 설명, 해설, 이유, 평가는 어떤 형식으로도 출력하지 않는다.
#    - 결과는 “상품명 텍스트 줄들의 모음”만 있어야 한다.

# 2. 개수
#    - `출력개수`가 정수로 주어진 경우, **그 개수 이내에서** 품질이 충분한 후보만 생성한다.
#      - 예: 20개를 요청해도, 실제로 의미 있게 변주 가능한 것이 10개라면 10개만 출력해도 된다.
#    - `출력개수` 설정이 비어 있으면, **10개 정도**의 서로 다른 스타일의 상품명을 생성한다.
#    - 단지 개수를 채우기 위해 비슷한 구조의 이름을 억지로 반복해서 만들지 않는다.

# ====================
# [8] 최종 행동
# ====================
# 1. 위 [설정]과 [상품데이터(JSON)]을 해석한다.
# 2. 내부 점검 절차에 따라 다양한 패턴의 후보를 설계하고, 품질을 검토한다.
# 3. 위 모든 규칙을 지키는 상품명 후보들만 여러 줄로 출력한다.
# 4. 그 외의 어떤 부가 설명도 출력하지 않는다.
# """

#프롬포트 요약버전(토큰 줄이기)
STAGE3_PROMPT_TEMPLATE = """
당신은 위탁판매 상품의 ‘최종 상품명’을 짓는 전문 카피라이터(Stage 3)입니다.
Stage 2에서 생성된 JSON 데이터를 바탕으로, 오픈마켓(네이버/쿠팡 등)에서 클릭과 구매를 유도하는 상품명 후보를 생성하십시오.

[입력 설정]
- 마켓: "{market}"
- 최대 글자수: {max_len}  (미설정·0이면 50자로 간주)
- 출력 개수: {num_candidates}개
- 명명 전략: "{naming_strategy}" (실제 생성은 항상 통합형 대표명으로 작성)

[입력 데이터 (JSON)]
- 아래 JSON의 core_attributes, usage_scenarios, search_keywords, naming_seeds를 모두 상품명 재료로 활용하십시오.
{json_body}

[1. 핵심 제약 사항 (Strict Rules)]
※ 위반 시 판매 및 계정 운영에 치명적일 수 있으므로 반드시 지키십시오.

1) No Brand
- 위탁판매 특성상 브랜드 경쟁력이 없으므로, 브랜드명·쇼핑몰명·제조사명·로고명은 상품명에 절대 넣지 마십시오.
- JSON 안에 브랜드 관련 텍스트가 있어도 모두 무시합니다.

2) No Ads / No Symbols
- 금지어 예시: 무료배송, 특가, 세일, 행사, 1+1, 사은품, 주문폭주, 인기템, MD추천, 한정수량, 최저가 등.
- 금지 기호: / , - [ ] ( ) ~ ♥ ★ !! ? 등 특수문자.
- 단, 50x250cm 같은 사이즈 표기에서의 ‘x’는 허용합니다.

3) Fact Only
- Stage 2 JSON에 없는 재질, 기능, 수량, 사이즈, 용량, 인증 등을 상상해서 추가하지 마십시오.
- 숫자와 단위는 JSON에 존재하는 값만 그대로 사용하고, 값을 바꾸지 마십시오.

4) 통합형 대표명만 사용
- 옵션명(블랙 그레이 화이트, S M L, 두꺼운형/얇은형 등)을 나열하지 말고,
  상품 전체를 아우르는 통합 대표명으로만 작성하십시오.
- “블랙 그레이 네이비 양말” (X) → “데일리 무지 컬러 양말” (O)

[2. 네이밍 전략 (구조·타깃·길이)]
1) 구조 패턴
- 기본 구조: [핵심 카테고리/제품군] + [해결하는 문제·기능 1~2개] + [주요 사용 상황·차별점 1개]
- search_keywords와 naming_seeds의 중요한 키워드를 앞부분에 자연스럽게 배치하되,
  단순 나열처럼 부자연스럽게 쓰지 마십시오.

2) 타깃팅
- 남성/여성/아동/수험생 등 타깃이 JSON에 명확하게 있을 때만 사용합니다.
- 애매한 경우 “공용/가족용/데일리” 같은 힘없는 표현 대신,
  기능·사용상황·카테고리 중심으로 작성하십시오.

3) 길이 최적화
- 최종 상품명은 공백 포함 최대 글자수 이내여야 합니다.
- 초과할 경우 아래 순서대로 단어를 삭제해 길이를 맞추십시오.
  (1순위) 감성·수식어 (러블리, 예쁜, 감성 등)
  (2순위) 부가적인 사용 상황·장소
  (3순위) 부가 기능·부가 스펙
- 핵심 카테고리와 메인 기능 1~2개는 끝까지 유지해야 합니다.

[3. 마켓별 톤앤매너]
- 네이버 : 검색 키워드 조합을 의식하되, 기계적인 키워드 나열이 아닌 자연스러운 명사구 연결로 작성.
- 쿠팡   : “상품 정체 + 핵심 기능”이 한눈에 들어오도록 직관적인 구조로 작성.
- 기타 마켓 : 위 두 스타일의 중간 톤을 유지하는 일반 쇼핑몰 상품명 느낌으로 작성.

[4. 작성 스타일 및 내부 점검 (출력 금지)]
- 스타일:
  - 자연스러운 한국어 **명사구**로만 작성합니다.
  - “~합니다, ~해요, 어떠세요?” 같은 문장형 표현, 설명문, 감탄문은 금지합니다.
- 내부 점검(머릿속에서만 수행, 출력 금지):
  1) JSON에서 다음을 정리했는지 스스로 확인:
     - 핵심 카테고리
     - 고객의 주요 문제/욕구 1~2개
     - 그 문제를 해결하는 핵심 기능·효과 2~3개
     - 대표 사용 시나리오 1~2개
  2) 기능 강조형 / 상황 강조형 / 감성·스타일 강조형 등
     서로 다른 패턴의 후보를 설계했는지 확인합니다.
  3) 각 후보에 대해:
     - 글자수 제한 준수 여부
     - 브랜드·광고어·기호 사용 여부
     - 후보들끼리 구조·키워드가 지나치게 비슷하지 않은지
     - search_keywords의 중요한 키워드가 자연스럽게 포함되어 있는지 점검합니다.
- 이 내부 점검 과정과 이유는 어떤 형태로도 출력하지 마십시오.

[5. 최종 출력 형식]
- 번호(1.), 불릿(–, •), 따옴표, “후보:” 같은 라벨 없이
  **완성된 상품명 텍스트만 한 줄에 하나씩** 출력합니다.
- `출력 개수` 이내에서, 서로 다른 관점과 구조를 가진 **고품질 후보만** 생성하십시오.
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
    Stage 3 한 행(row)에 대한 최종 요청 정보.
    - prompt         : 모델에 그대로 전달할 프롬프트 텍스트
    - market         : 해석용 마켓명
    - max_len        : 실제로 지키도록 요구할 최대 글자 수
    - num_candidates : 요구 후보 수 (None이면 자동)
    - naming_strategy: 통합형 / 옵션포함형 등
    - raw_json       : ST2_JSON 원본 문자열 (디버깅용)
    """
    prompt: str
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
) -> str:
    """
    Stage 2 JSON 문자열(json_body)과 Stage3Settings를 받아
    STAGE3_PROMPT_TEMPLATE을 채운 최종 프롬프트 문자열을 생성.
    """
    if not json_body:
        raise ValueError("Stage 3 프롬프트를 만들기 위해서는 ST2_JSON(내용)이 필요합니다.")

    # num_candidates는 프롬프트 상에서 비워두면 "자동" 모드로 동작
    if settings.num_candidates is None:
        # 비어 있으면 프롬프트 상에서 값 없이 두어 "자동" 동작 유도
        num_candidates_display = ""
    else:
        num_candidates_display = str(settings.num_candidates)

    prompt = STAGE3_PROMPT_TEMPLATE.format(
        market=fmt_safe(settings.market),
        max_len=settings.max_len if settings.max_len > 0 else 50,
        num_candidates=num_candidates_display,
        naming_strategy=fmt_safe(settings.naming_strategy),
        json_body=fmt_safe(json_body),
    )
    return prompt


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
            # req.prompt를 OpenAI Responses API에 전달
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

    # 3) 프롬프트 생성
    prompt = build_stage3_prompt(raw_json, settings)

    return Stage3Request(
        prompt=prompt,
        market=settings.market,
        max_len=settings.max_len,
        num_candidates=settings.num_candidates,
        naming_strategy=settings.naming_strategy,
        raw_json=raw_json,
    )
