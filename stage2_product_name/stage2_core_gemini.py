"""
stage2_core_gemini.py

Stage 2: 상세정보/키워드/네이밍 재료 추출기 (Gemini 2.5 Flash-Lite 버전)
- Vision API 지원 (이미지 분석)
- Implicit Caching 자동 적용 (System Instruction 동일)
- Few-Shot 예시 포함
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Sequence, Any

import pandas as pd

# =========================
# Stage 2 프롬프트 템플릿 (Gemini 최적화 버전)
# =========================

# System Instruction (정적, 모든 요청에서 동일 - Implicit Caching 대상)
STAGE2_SYSTEM_INSTRUCTION = """당신은 온라인 쇼핑몰 *위탁판매* 상품을 위한
"Stage 2: 상세정보·키워드·네이밍 재료 추출기"입니다.
입력 텍스트와 상세이미지 정보를 분석해, Stage 3에서 최종 상품명을 만들 때 사용할 구조화 JSON을 생성하십시오.

[역할 및 제약]
1) 이 단계에서는 **최종 상품명/제목/타이틀을 절대 생성하지 않는다.**
   - "상품명:", "추천 제목:", "제목:", "타이틀:" 같은 필드는 만들지 말 것.
   - 오직 **정보 추출 + 검색 키워드 + 네이밍 재료**만 생성한다.
   - Stage 3에서 이 정보를 바탕으로 최종 상품명을 만들 것이므로, 여기서는 재료만 준비한다.
2) 위탁판매 특성
   - 브랜드명, 쇼핑몰명, 제조사명, 로고/워터마크에 보이는 이름은 모두 무시한다.
   - 예: "나이키", "아디다스", "○○몰", "○○샵", "○○스토어" 등은 추출하지 않는다.
   - 위탁판매자는 브랜드명으로 경쟁우위를 갖지 못하므로 브랜드 정보는 불필요하다.
3) meta Passthrough (중요: 완전 동일 복사)
   - 출력 JSON의 meta는 user가 제공한 meta(JSON)의 키/값을 **완전히 동일하게** 그대로 복사한다.
   - 공백, 특수문자, 대소문자, 줄바꿈 등 모든 것을 수정하지 말고 입력값 그대로 출력해야 한다.
   - meta 필드의 값은 절대 변경, 해석, 정규화, 정리하지 말고 입력값 그대로 출력한다.
4) 사실 기반 추출 (추측 금지)
   - 상세설명 텍스트와 이미지에 **명시된 재질·사이즈·구성·기능**만 쓴다.
   - 보이지 않는 정보는 **추측하지 않는다**.
   - 숫자 필드: 정보 없으면 null (0이나 빈 문자열이 아님)
   - 문자열 필드: 정보 없으면 "" (빈 문자열, null이 아님)
   - 배열 필드: 정보 없으면 [] (빈 배열)
5) JSON 출력 규칙
   - 아래 스키마를 따른 **단일 JSON 객체 한 개만** 출력한다.
   - JSON 앞뒤에 설명, 자연어 문장, 마크다운 코드블록(```json 등)은 붙이지 않는다.

[출력 스키마]

{
  "meta": {
    "기본상품명": "user가 제공한 값 그대로",
    "판매형태": "user가 제공한 값 그대로",
    "옵션_원본": "user가 제공한 값 그대로",
    "카테고리_경로": "user가 제공한 값 그대로"
  },
  "core_attributes": {
    "상품타입": "핵심 타입 한 줄",
    "상위카테고리": "1~2단어 축약",
    "사용대상": ["대상들"],
    "주요용도": ["용도 2~5개"],
    "재질": ["확인된 재질만"],
    "스타일/특징": ["디자인 키워드"],
    "사이즈": {
      "표기형식": "상세설명 그대로",
      "주요_길이_1_cm": null,
      "주요_길이_2_cm": null,
      "기타": ""
    },
    "색상/옵션_리스트": ["색상/패턴 옵션"],
    "옵션구분방식": "색상/패턴/사이즈 등",
    "세트구성": "단품/N개 세트 등",
    "기타기능": ["기능성 키워드"]
  },
  "usage_scenarios": ["사용 상황 문장 2~5개"],
  "search_keywords": ["검색용 키워드 15~25개"],
  "naming_seeds": {
    "상품핵심명사": ["명사 3~7개"],
    "스타일형용사": ["형용사 5~10개"],
    "기능/효과표현": ["~용 표현 3~7개"],
    "상황/장소/계절": ["상황 키워드 3~7개"],
    "타깃고객": "한 줄 요약",
    "차별화포인트": ["강점 3~5개"]
  }
}

## Few-Shot 예시

### 예시 1: 귀마개
입력:
- 기본상품명: 겨울 방한 니트 귀마개
- 판매형태: 옵션형
- 옵션_원본: 블랙,그레이,베이지
- 카테고리_경로: 패션잡화>모자/귀마개

출력:
{"meta":{"기본상품명":"겨울 방한 니트 귀마개","판매형태":"옵션형","옵션_원본":"블랙,그레이,베이지","카테고리_경로":"패션잡화>모자/귀마개"},"core_attributes":{"상품타입":"겨울 방한 니트 귀마개","상위카테고리":"귀마개","사용대상":["남녀공용","성인"],"주요용도":["겨울 외출","스키장","등산"],"재질":["니트","아크릴"],"스타일/특징":["심플","캐주얼","보온성"],"사이즈":{"표기형식":"FREE","주요_길이_1_cm":null,"주요_길이_2_cm":null,"기타":""},"색상/옵션_리스트":["블랙","그레이","베이지"],"옵션구분방식":"색상","세트구성":"단품","기타기능":["방한","보온"]},"usage_scenarios":["겨울철 외출 시 귀를 따뜻하게 보호합니다.","스키장이나 등산 시 방한용으로 활용합니다.","니트 소재로 부드럽고 편안한 착용감을 제공합니다."],"search_keywords":["겨울귀마개","방한귀마개","니트귀마개","귀덮개","귀보호대","방한용품","겨울악세사리","스키귀마개","등산귀마개","남녀공용귀마개","보온귀마개","귀마개","겨울모자","방한모자"],"naming_seeds":{"상품핵심명사":["귀마개","귀덮개","방한용품"],"스타일형용사":["따뜻한","포근한","심플한","캐주얼한"],"기능/효과표현":["방한용","보온용","외출용","스키용"],"상황/장소/계절":["겨울","외출","스키장","등산"],"타깃고객":"겨울철 외출이 잦은 남녀 성인","차별화포인트":["다양한 색상","니트 소재","편안한 착용감"]}}

### 예시 2: 아이스박스
입력:
- 기본상품명: 캠핑 아이스박스 12L
- 판매형태: 단품형
- 옵션_원본:
- 카테고리_경로: 스포츠/레저>캠핑용품>쿨러/아이스박스

출력:
{"meta":{"기본상품명":"캠핑 아이스박스 12L","판매형태":"단품형","옵션_원본":"","카테고리_경로":"스포츠/레저>캠핑용품>쿨러/아이스박스"},"core_attributes":{"상품타입":"캠핑용 하드쿨러 아이스박스 12L","상위카테고리":"아이스박스","사용대상":["성인","캠핑족"],"주요용도":["캠핑","낚시","피크닉","야외활동"],"재질":["PP","단열폼"],"스타일/특징":["하드케이스","휴대용"],"사이즈":{"표기형식":"12L","주요_길이_1_cm":null,"주요_길이_2_cm":null,"기타":"용량 12L"},"색상/옵션_리스트":[],"옵션구분방식":"","세트구성":"단품","기타기능":["보냉","단열"]},"usage_scenarios":["캠핑 시 음식과 음료를 신선하게 보관합니다.","낚시나 피크닉 시 보냉용으로 활용합니다.","12L 용량으로 적당한 양의 식음료를 보관할 수 있습니다."],"search_keywords":["아이스박스","캠핑쿨러","보냉박스","캠핑아이스박스","12L쿨러","하드쿨러","휴대용쿨러","피크닉쿨러","낚시쿨러","보냉가방","캠핑용품","아이스쿨러"],"naming_seeds":{"상품핵심명사":["아이스박스","쿨러","보냉박스"],"스타일형용사":["휴대용","하드케이스","실용적인"],"기능/효과표현":["보냉용","캠핑용","야외용"],"상황/장소/계절":["캠핑","낚시","피크닉","야외"],"타깃고객":"캠핑과 야외활동을 즐기는 성인","차별화포인트":["12L 적정 용량","하드케이스 내구성","휴대 편의성"]}}

위 스키마와 예시를 따라 **단일 JSON 객체만** 응답하십시오."""

# User 프롬프트 템플릿 (동적 데이터만 포함)
STAGE2_USER_PROMPT_TEMPLATE = """[입력 데이터]

1) meta 정보 (JSON)
{meta_json}

2) 참고 텍스트
   - 원본 상품명: {raw_name}
   - Stage1 정제상품명: {stage1_name}
   - 키워드(있다면): {keywords}

※ 상세이미지는 첨부된 이미지를 참고하세요."""


def safe_str(v: Any) -> str:
    """NaN/None 안전하게 문자열로 변환 + strip."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def fmt_safe(v: Any) -> str:
    """str.format()에 넣기 전에 { } 이스케이프용 헬퍼."""
    s = safe_str(v)
    return s.replace("{", "{{").replace("}", "}}")


def looks_like_option_from_name(raw_name: str) -> bool:
    """상품명이 봐도 '옵션형 같다' 싶은 패턴이면 True."""
    name = safe_str(raw_name)
    if not name:
        return False

    option_keywords = [
        "색상선택", "컬러선택", "옵션선택",
        "색상옵션", "컬러옵션", "옵션구성",
        "색상", "컬러", "옵션",
        "혼합발송", "랜덤", "랜덤발송",
        "택1", "택일", "중 택1", "중 택2",
    ]
    if any(k in name for k in option_keywords):
        return True

    color_tokens = [
        "블랙", "화이트", "아이보리", "베이지", "그레이", "회색",
        "네이비", "곤색", "브라운", "카키",
        "핑크", "레드", "와인", "버건디",
        "옐로", "노랑", "머스타드",
        "그린", "초록", "민트", "청록",
        "블루", "소라", "하늘", "스카이",
        "보라", "퍼플", "라벤더", "코랄", "오렌지",
    ]
    if "/" in name:
        color_count = sum(1 for c in color_tokens if c in name)
        if color_count >= 2:
            return True

    return False


def infer_sale_type(option_raw: str, raw_name: str) -> str:
    txt = safe_str(option_raw)
    name = safe_str(raw_name)

    if not txt:
        return "옵션형" if looks_like_option_from_name(name) else "단품형"

    if "," in txt:
        return "옵션형"

    if looks_like_option_from_name(name):
        return "옵션형"

    return "단품형"


@dataclass
class Stage2Request:
    """
    Gemini Vision API 요청 구조:
    - system_instruction: 시스템 지시문 (정적, 모든 요청에서 동일 - Implicit Caching)
    - user_prompt: 사용자 프롬프트 (동적 데이터 포함)
    - image_paths: 같이 업로드할 이미지 경로들 (썸네일/상세이미지 포함)
    """
    system_instruction: str
    user_prompt: str
    image_paths: List[str]


def build_stage2_request_from_row(
    row: pd.Series,
    detail_cols: Sequence[str],
) -> Stage2Request:
    """
    엑셀 한 행 + 상세이미지 컬럼 리스트 → Stage2Request
    (Gemini 최적화 버전: System Instruction + User Prompt 분리)
    """
    raw_name = fmt_safe(row.get("원본상품명", ""))
    stage1_name = fmt_safe(row.get("ST1_결과상품명", ""))
    basic_name = stage1_name if stage1_name else raw_name

    category_path = fmt_safe(row.get("카테고리명", ""))
    option_raw = fmt_safe(row.get("옵션1값", ""))
    keywords = fmt_safe(row.get("키워드", ""))

    sale_type_meta = safe_str(row.get("판매형태", ""))
    if sale_type_meta not in ("단품형", "옵션형"):
        sale_type_meta = infer_sale_type(option_raw, raw_name)
    sale_type = fmt_safe(sale_type_meta)

    # ===== 이미지 경로 모으기 =====
    image_paths: List[str] = []

    # 썸네일(대표 이미지) 먼저
    thumb = safe_str(row.get("이미지대", ""))
    if thumb:
        image_paths.append(thumb)

    # 상세이미지_1 ~ N
    for col in detail_cols:
        val_raw = row.get(col, "")
        if safe_str(val_raw):
            path = safe_str(val_raw)
            image_paths.append(path)

    # meta 정보를 JSON으로 구성
    meta_json_obj = {
        "기본상품명": basic_name,
        "판매형태": sale_type,
        "옵션_원본": option_raw,
        "카테고리_경로": category_path
    }
    import json as json_module
    meta_json_str = json_module.dumps(meta_json_obj, ensure_ascii=False, indent=2)

    # System Instruction (정적)
    system_instruction = STAGE2_SYSTEM_INSTRUCTION

    # User 프롬프트 (동적 데이터만 포함)
    user_prompt = STAGE2_USER_PROMPT_TEMPLATE.format(
        meta_json=meta_json_str,
        raw_name=raw_name,
        stage1_name=stage1_name,
        keywords=keywords,
    )

    return Stage2Request(
        system_instruction=system_instruction,
        user_prompt=user_prompt,
        image_paths=image_paths
    )


# ============================================================
#  Gemini Batch API 관련 함수 (Stage 2 Vision용)
#  - GPT Batch API와 동일한 워크플로우
#  - JSONL 업로드 → 배치 생성 → 상태 폴링 → 결과 다운로드 → 병합
#  - 이미지는 inline_data (base64)로 포함
# ============================================================

import os
import json
import base64
from io import BytesIO
from typing import Dict, List, Tuple, Optional, Any

# Gemini API Import
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# PIL for image processing
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# API 키 파일 경로
API_KEY_FILE = ".gemini_api_key_stage2_batch"

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
# 이미지 처리 (Batch API용)
# =====================================
def resize_image_to_base64(image_path: str, max_width: int = 384) -> Tuple[Optional[str], Optional[str]]:
    """
    이미지를 리사이즈하고 base64로 인코딩.

    상세설명 이미지는 세로로 긴 형태(860x1500~4000)이므로 가로 기준 리사이즈.

    max_width:
        384 - 토큰 절약 (세로 긴 이미지에서 타일 수 감소)
        768 - 고품질 (더 선명한 텍스트 인식)

    Returns: (base64_string, mime_type) or (None, None) if failed
    """
    if not PIL_AVAILABLE:
        return None, None

    if not os.path.exists(image_path):
        return None, None

    try:
        with Image.open(image_path) as img:
            # RGBA를 RGB로 변환
            if img.mode == 'RGBA':
                background = Image.new('RGB', img.size, (255, 255, 255))
                background.paste(img, mask=img.split()[3])
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')

            # 가로 기준 리사이즈 (비율 유지)
            width, height = img.size
            if width > max_width:
                ratio = max_width / width
                new_size = (int(width * ratio), int(height * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)

            # JPEG로 변환 및 base64 인코딩
            buffer = BytesIO()
            img.save(buffer, format='JPEG', quality=85)
            img_bytes = buffer.getvalue()
            img_base64 = base64.b64encode(img_bytes).decode('utf-8')

            return img_base64, "image/jpeg"
    except Exception as e:
        print(f"[WARN] 이미지 처리 실패: {image_path} - {e}")
        return None, None


def get_detail_image_cols(df) -> List[str]:
    """상세이미지 컬럼 목록 반환 (정렬됨)"""
    cols = []
    for col in df.columns:
        if str(col).startswith("상세이미지_"):
            cols.append(col)

    # 숫자 기준 정렬
    def sort_key(c):
        try:
            return int(str(c).split("_")[1])
        except:
            return 9999

    return sorted(cols, key=sort_key)


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
                # JSON 추출 (마크다운 코드블록 제거)
                import re
                text = re.sub(r"^```(?:json)?\n?", "", text.strip())
                text = re.sub(r"\n?```$", "", text)
                return text.strip()

        # 직접 text 필드가 있는 경우
        if response.get("text"):
            text = response["text"]
            import re
            text = re.sub(r"^```(?:json)?\n?", "", text.strip())
            text = re.sub(r"\n?```$", "", text)
            return text.strip()

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
# Batch API 핵심 로직 (Gemini Vision)
# =====================================
def create_batch_input_jsonl(
    excel_path: str,
    jsonl_path: str,
    max_images: int = 10,
    max_width: int = 384,
    skip_existing: bool = True,
    log_func=None,
):
    """
    엑셀 파일 → Gemini Batch API용 JSONL 생성 (이미지 inline_data 포함).

    Args:
        excel_path: 입력 엑셀 파일 경로
        jsonl_path: 출력 JSONL 파일 경로
        max_images: 최대 이미지 개수 (썸네일 + 상세이미지)
        max_width: 이미지 리사이즈 최대 가로 크기 (384 또는 768)
        skip_existing: ST2_JSON이 이미 있는 행 스킵 여부
        log_func: 로그 출력 함수
    """
    def log(msg):
        if log_func:
            log_func(msg)
        else:
            print(msg)

    df = pd.read_excel(excel_path)

    # 상세이미지 컬럼 찾기
    detail_cols = get_detail_image_cols(df)
    log(f"[Batch] 상세이미지 컬럼: {len(detail_cols)}개")

    total_rows = len(df)
    written_count = 0
    skipped_rows = []
    skipped_existing = 0

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for idx, row in df.iterrows():
            # 기존 결과가 있으면 스킵
            if skip_existing and "ST2_JSON" in df.columns:
                existing = safe_str(row.get("ST2_JSON", ""))
                if existing and existing != "nan" and existing != "{}":
                    skipped_existing += 1
                    continue

            # Stage2Request 빌드
            try:
                req = build_stage2_request_from_row(row, detail_cols)
            except Exception as e:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": str(e),
                })
                continue

            # 이미지 parts 구성
            image_parts = []
            image_count = 0

            for img_path in req.image_paths:
                if image_count >= max_images:
                    break

                img_base64, mime_type = resize_image_to_base64(img_path, max_width)
                if img_base64:
                    image_parts.append({
                        "inline_data": {
                            "mime_type": mime_type,
                            "data": img_base64
                        }
                    })
                    image_count += 1

            # 이미지가 하나도 없으면 스킵
            if not image_parts:
                skipped_rows.append({
                    "엑셀_인덱스": idx,
                    "누락항목": "유효한 이미지 없음",
                })
                continue

            # User prompt를 마지막에 추가
            user_parts = image_parts + [{"text": req.user_prompt}]

            # Gemini Batch JSONL 형식 (Vision)
            request_obj = {
                "key": f"row-{idx}",
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": user_parts
                        }
                    ],
                    "systemInstruction": {
                        "parts": [{"text": req.system_instruction}]
                    },
                    "generationConfig": {
                        "temperature": 0.3,
                        "maxOutputTokens": 4096,
                    }
                }
            }

            f.write(json.dumps(request_obj, ensure_ascii=False) + "\n")
            written_count += 1

            if written_count % 10 == 0:
                log(f"[Batch] JSONL 생성 중: {written_count}건")

    # 스킵된 행 저장
    skipped_path = None
    if skipped_rows:
        base, _ = os.path.splitext(excel_path)
        skipped_path = f"{base}_stage2_skipped_rows.xlsx"
        df_skipped = pd.DataFrame(skipped_rows)
        df_skipped.to_excel(skipped_path, index=False)

    log(f"[Batch] JSONL 생성 완료: {written_count}건 (스킵: {skipped_existing + len(skipped_rows)}건)")

    return {
        "total_rows": total_rows,
        "written_count": written_count,
        "skipped_count": len(skipped_rows),
        "skipped_existing": skipped_existing,
        "skipped_path": skipped_path,
        "jsonl_path": jsonl_path,
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
    temp_filename = f"stage2_batch_{timestamp}_{file_hash}.jsonl"

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

    if "ST2_JSON" not in df.columns:
        df["ST2_JSON"] = ""
    df["ST2_JSON"] = df["ST2_JSON"].astype(str)

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
                    df.at[idx, "ST2_JSON"] = text
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
