from __future__ import annotations
from dataclasses import dataclass
from typing import List, Sequence, Any

import pandas as pd

# =========================
# Stage 2 프롬프트 템플릿 (최종)
# =========================

STAGE2_PROMPT_TEMPLATE = """
당신은 온라인 쇼핑몰 *위탁판매* 상품을 위한
"Stage 2: 상세정보·키워드·네이밍 재료 추출기"입니다.
입력 텍스트와 상세이미지 정보를 분석해, Stage 3에서 최종 상품명을 만들 때 사용할 구조화 JSON을 생성하십시오.

[입력 데이터]
1) meta 정보
   - 기본상품명: {basic_name}
   - 판매형태: {sale_type}
   - 옵션_원본: {option_raw}
   - 카테고리_경로: {category_path}

2) 상세이미지 분석 정보
{detail_images_block}

3) 참고 텍스트
   - 원본 상품명: {raw_name}
   - Stage1 정제상품명: {stage1_name}
   - 키워드(있다면): {keywords}

[역할 및 제약]
1) 이 단계에서는 **최종 상품명/제목/타이틀을 절대 생성하지 않는다.**
   - "상품명:", "추천 제목:" 같은 필드는 만들지 말 것.
   - 오직 **정보 추출 + 검색 키워드 + 네이밍 재료**만 생성한다.
2) 위탁판매 특성
   - 브랜드명, 쇼핑몰명, 제조사명, 로고/워터마크에 보이는 이름은 모두 무시한다.
3) meta Passthrough
   - meta에 들어가는 값은 입력값을 **수정·해석하지 말고 그대로 복사**한다.
4) 사실 기반 추출
   - 상세설명 텍스트와 이미지에 **명시된 재질·사이즈·구성·기능**만 쓴다.
   - 보이지 않는 정보는 **추측하지 않는다**.
     · 숫자 필드: 정보 없으면 null  
     · 문자열 필드: 정보 없으면 "" (빈 문자열)
5) JSON 출력 규칙
   - 아래 스키마를 따른 **단일 JSON 객체 한 개만** 출력한다.
   - JSON 앞뒤에 설명, 자연어 문장, 마크다운 코드블록(````json 등)은 붙이지 않는다.

[출력 스키마]

{{
  "meta": {{
    "기본상품명": "{basic_name}",
    "판매형태": "{sale_type}",
    "옵션_원본": "{option_raw}",
    "카테고리_경로": "{category_path}"
  }},
  "core_attributes": {{
    "상품타입": "이 상품을 한 줄로 정의하는 핵심 타입. 예: 데일리 곱창 헤어밴드, 아크릴 탁상 사진 액자, 캠핑용 하드쿨러 아이스박스",
    "상위카테고리": "카테고리_경로를 1~2단어로 축약. 예: 헤어악세사리, 인테리어소품, 캠핑용품",
    "사용대상": [
      "텍스트/이미지 근거로 알 수 있는 대상들. 예: 여성, 남성, 공용, 아동, 반려견 등"
    ],
    "주요용도": [
      "실제 사용 장면 2~5개. 예: 데일리 묶음 머리끈, 캠핑/낚시 보냉, 인테리어 탁상 사진 전시"
    ],
    "재질": [
      "상세설명/이미지에서 확인되는 재질만. 예: 폴리에스터 100%, 아크릴, 도자재, PP"
    ],
    "스타일/특징": [
      "디자인·분위기·촉감 키워드. 예: 체크패턴, 벨벳, 골지, 러블리, 미니멀, 북유럽감성, 빈티지, 모던"
    ],
    "사이즈": {{
      "표기형식": "상세설명에 나온 사이즈 문구를 그대로. 예: one size(11×11cm), 42×31cm, 12L 등",
      "주요_길이_1_cm": null,
      "주요_길이_2_cm": null,
      "기타": "용량(L), 수량(입수), 두께 등 텍스트 기반 치수 정보 요약. 없으면 빈 문자열."
    }},
    "색상/옵션_리스트": [
      "옵션_원본과 이미지에서 실제로 확인되는 색상·패턴 옵션명 목록. 예: 체크블루, 벨벳레드, 플라워 등"
    ],
    "옵션구분방식": "색상 / 패턴 / 사이즈 / 용량 / 타입 등 중 하나. 애매하면 빈 문자열.",
    "세트구성": "단품 / N개 세트 / 본품+리필 / 10종 중 택1 등 실제 구성 요약",
    "기타기능": [
      "있다면 기능성 키워드. 예: 생활방수, 충격흡수, 보냉력, 미끄럼방지, 자가점착 등"
    ]
  }},
  "usage_scenarios": [
    "상세설명 내용을 바탕으로, 이 상품을 언제·어디서·어떻게 사용하는지 설명하는 자연어 문장 2~5개"
  ],
  "search_keywords": [
    "브랜드/쇼핑몰명·광고어(무료배송, 최저가 등)·특수문자 없이, 순수 검색용 키워드 15~25개를 생성한다.",
    "각 키워드는 최대 2단어로 작성한다. 예: 곱창머리끈, 체크곱창밴드, 캠핑아이스박스, 탁상사진액자"
  ],
  "naming_seeds": {{
    "상품핵심명사": [
      "나중에 상품명에 사용할 중심 명사 3~7개. 예: 곱창밴드, 헤어슈슈, 탁상액자, 캠핑아이스박스 등"
    ],
    "스타일형용사": [
      "상품 분위기를 표현하는 형용사 5~10개. 예: 데일리, 러블리, 모던, 심플, 감성, 빈티지, 포근한"
    ],
    "기능/효과표현": [
      "상품명이 '~용, ~기능' 형태로 붙일 수 있는 표현 3~7개. 예: 데일리묶음용, 선물용, 보냉보관용"
    ],
    "상황/장소/계절": [
      "사용 상황·장소·계절 키워드 3~7개. 예: 출근룩, 데이트룩, 캠핑, 나들이, 여행, 여름피크닉"
    ],
    "타깃고객": "예: 10~20대 여성, 직장인 여성, 캠핑·낚시 즐기는 성인, 반려견 보호자 등 한 줄 요약",
    "차별화포인트": [
      "유사 상품 대비 이 제품만의 강점 3~5개. 예: 10가지 패턴 구성, 생활방수 원단, 대용량 하드케이스 등"
    ]
  }}
}}

반드시 위 JSON 스키마를 따르는 **단일 JSON 객체만** 응답하십시오.
JSON 앞뒤에 다른 문장이나 마크다운(````json 등)을 절대 추가하지 마십시오.
"""

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
    채팅방에서 한 번에 보내는 요청 1건과 똑같은 구조:
    - prompt: 텍스트 프롬프트 전체
    - image_paths: 같이 업로드할 이미지 경로들 (썸네일/상세이미지 포함, 순서 O)
    """
    prompt: str
    image_paths: List[str]


def build_stage2_request_from_row(
    row: pd.Series,
    detail_cols: Sequence[str],
) -> Stage2Request:
    """
    엑셀 한 행 + 상세이미지 컬럼 리스트 → Stage2Request
    (프롬프트 + 이미지 경로 리스트)
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
    detail_path_text_lines: List[str] = []
    for col in detail_cols:
        val_raw = row.get(col, "")
        if safe_str(val_raw):
            path = safe_str(val_raw)
            image_paths.append(path)
            detail_path_text_lines.append(f"   - {fmt_safe(val_raw)}")

    if detail_path_text_lines:
        detail_images_block = "\n".join(detail_path_text_lines)
    else:
        detail_images_block = "   (상세이미지 없음)"

    prompt = STAGE2_PROMPT_TEMPLATE.format(
        basic_name=basic_name,
        sale_type=sale_type,
        option_raw=option_raw,
        category_path=category_path,
        detail_images_block=detail_images_block,
        raw_name=raw_name,
        stage1_name=stage1_name,
        keywords=keywords,
    )

    return Stage2Request(prompt=prompt, image_paths=image_paths)
