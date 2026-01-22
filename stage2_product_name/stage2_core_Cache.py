from __future__ import annotations
from dataclasses import dataclass
from typing import List, Sequence, Any

import pandas as pd

# =========================
# Stage 2 프롬프트 템플릿 (캐싱 최적화 버전)
# =========================
# OpenAI Prompt Caching 가이드에 맞게 재구성:
# - 정적 콘텐츠(역할, 제약, 스키마)를 앞쪽에 배치
# - 동적 콘텐츠(입력 데이터)를 뒤쪽에 배치
# - 프롬프트 프리픽스가 모든 요청에서 동일하도록 구성

# System 프롬프트 (완전히 정적, 모든 요청에서 동일)
# ⚠️ 중요: 프롬프트 캐싱 활성화를 위해 1024 토큰 이상이어야 함
STAGE2_SYSTEM_PROMPT = """당신은 온라인 쇼핑몰 *위탁판매* 상품을 위한
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
   - 예: 입력이 "블랙_5호"이면 "블랙 5호"로 바꾸지 말고 "블랙_5호" 그대로 출력.
   - 예: 입력이 "옵션형  " (뒤에 공백)이면 공백까지 그대로 유지.
4) 사실 기반 추출 (추측 금지)
   - 상세설명 텍스트와 이미지에 **명시된 재질·사이즈·구성·기능**만 쓴다.
   - 보이지 않는 정보는 **추측하지 않는다**.
   - 텍스트나 이미지에 나오지 않은 정보는 생성하지 않는다.
   - 숫자 필드: 정보 없으면 null (0이나 빈 문자열이 아님)
   - 문자열 필드: 정보 없으면 "" (빈 문자열, null이 아님)
   - 배열 필드: 정보 없으면 [] (빈 배열)
5) JSON 출력 규칙
   - 아래 스키마를 따른 **단일 JSON 객체 한 개만** 출력한다.
   - JSON 앞뒤에 설명, 자연어 문장, 마크다운 코드블록(````json 등)은 붙이지 않는다.
   - JSON만 출력하고, "다음은 JSON입니다" 같은 설명 문구는 절대 추가하지 않는다.

[출력 스키마 상세 설명]

{
  "meta": {
    "기본상품명": "user가 제공한 meta JSON의 '기본상품명' 값을 그대로 복사 (수정 금지)",
    "판매형태": "user가 제공한 meta JSON의 '판매형태' 값을 그대로 복사 (수정 금지)",
    "옵션_원본": "user가 제공한 meta JSON의 '옵션_원본' 값을 그대로 복사 (수정 금지)",
    "카테고리_경로": "user가 제공한 meta JSON의 '카테고리_경로' 값을 그대로 복사 (수정 금지)"
  },
  "core_attributes": {
    "상품타입": "이 상품을 한 줄로 정의하는 핵심 타입. 예: 데일리 곱창 헤어밴드, 아크릴 탁상 사진 액자, 캠핑용 하드쿨러 아이스박스, 여성 반팔 티셔츠, 어린이 미술 물감 세트",
    "상위카테고리": "카테고리_경로를 1~2단어로 축약. 예: 헤어악세사리, 인테리어소품, 캠핑용품, 의류, 문구용품",
    "사용대상": [
      "텍스트/이미지 근거로 알 수 있는 대상들. 예: 여성, 남성, 공용, 아동, 반려견, 어린이(3세 이상), 유아(0~3세) 등"
    ],
    "주요용도": [
      "실제 사용 장면 2~5개. 예: 데일리 묶음 머리끈, 캠핑/낚시 보냉, 인테리어 탁상 사진 전시, 여름 반팔 착용, 어린이 미술 수업"
    ],
    "재질": [
      "상세설명/이미지에서 확인되는 재질만. 예: 폴리에스터 100%, 아크릴, 도자재, PP, 면 100%, 폴리우레탄, 실리콘 등"
    ],
    "스타일/특징": [
      "디자인·분위기·촉감 키워드. 예: 체크패턴, 벨벳, 골지, 러블리, 미니멀, 북유럽감성, 빈티지, 모던, 심플, 캐주얼, 포멀 등"
    ],
    "사이즈": {
      "표기형식": "상세설명에 나온 사이즈 문구를 그대로. 예: one size(11×11cm), 42×31cm, 12L, FREE, 5호~13호 등",
      "주요_길이_1_cm": null,
      "주요_길이_2_cm": null,
      "기타": "용량(L), 수량(입수), 두께 등 텍스트 기반 치수 정보 요약. 없으면 빈 문자열."
    },
    "색상/옵션_리스트": [
      "옵션_원본과 이미지에서 실제로 확인되는 색상·패턴 옵션명 목록. 예: 체크블루, 벨벳레드, 플라워, 블랙, 화이트, 베이지 등"
    ],
    "옵션구분방식": "색상 / 패턴 / 사이즈 / 용량 / 타입 등 중 하나. 애매하면 빈 문자열.",
    "세트구성": "단품 / N개 세트 / 본품+리필 / 10종 중 택1 등 실제 구성 요약",
    "기타기능": [
      "있다면 기능성 키워드. 예: 생활방수, 충격흡수, 보냉력, 미끄럼방지, 자가점착, 항균, 방취, 탈취 등"
    ]
  },
  "usage_scenarios": [
    "상세설명 내용을 바탕으로, 이 상품을 언제·어디서·어떻게 사용하는지 설명하는 자연어 문장 2~5개"
  ],
  "search_keywords": [
    "브랜드/쇼핑몰명·광고어(무료배송, 최저가 등)·특수문자 없이, 순수 검색용 키워드 15~25개를 생성한다.",
    "각 키워드는 최대 2단어로 작성한다. 예: 곱창머리끈, 체크곱창밴드, 캠핑아이스박스, 탁상사진액자, 여성반팔티, 어린이물감세트"
  ],
  "naming_seeds": {
    "상품핵심명사": [
      "나중에 상품명에 사용할 중심 명사 3~7개. 예: 곱창밴드, 헤어슈슈, 탁상액자, 캠핑아이스박스, 반팔티셔츠, 미술물감 등"
    ],
    "스타일형용사": [
      "상품 분위기를 표현하는 형용사 5~10개. 예: 데일리, 러블리, 모던, 심플, 감성, 빈티지, 포근한, 캐주얼, 세련된 등"
    ],
    "기능/효과표현": [
      "상품명이 '~용, ~기능' 형태로 붙일 수 있는 표현 3~7개. 예: 데일리묶음용, 선물용, 보냉보관용, 여름착용용, 미술수업용"
    ],
    "상황/장소/계절": [
      "사용 상황·장소·계절 키워드 3~7개. 예: 출근룩, 데이트룩, 캠핑, 나들이, 여행, 여름피크닉, 실내장식, 야외활동"
    ],
    "타깃고객": "예: 10~20대 여성, 직장인 여성, 캠핑·낚시 즐기는 성인, 반려견 보호자, 어린이(3세 이상) 등 한 줄 요약",
    "차별화포인트": [
      "유사 상품 대비 이 제품만의 강점 3~5개. 예: 10가지 패턴 구성, 생활방수 원단, 대용량 하드케이스, 다양한 색상 옵션, 가벼운 무게 등"
    ]
  }
}

[출력 예시 형식 (학습용)]

올바른 출력 예시:
{
  "meta": {
    "기본상품명": "아동 캐주얼 티셔츠",
    "판매형태": "옵션형",
    "옵션_원본": "블랙_5호,화이트_7호",
    "카테고리_경로": "출산/육아>유아동의류>티셔츠"
  },
  "core_attributes": {
    "상품타입": "어린이 데일리 반팔 티셔츠",
    "상위카테고리": "유아동의류",
    "사용대상": ["어린이(3세 이상)", "남아", "여아"],
    "주요용도": ["일상 착용", "유치원/학교 복장", "여름 실내외 활동"],
    "재질": ["면 100%"],
    "스타일/특징": ["캐주얼", "심플", "편안한"],
    "사이즈": {
      "표기형식": "5호~13호",
      "주요_길이_1_cm": null,
      "주요_길이_2_cm": null,
      "기타": ""
    },
    "색상/옵션_리스트": ["블랙", "화이트"],
    "옵션구분방식": "색상/사이즈",
    "세트구성": "단품",
    "기타기능": []
  },
  "usage_scenarios": [
    "일상적인 외출이나 유치원, 학교 등에서 편안하게 착용할 수 있는 기본 티셔츠입니다.",
    "면 100% 소재로 피부에 자극이 적어 어린이에게 적합합니다.",
    "여름철 실내외 활동 시 시원하고 편안한 착용감을 제공합니다."
  ],
  "search_keywords": [
    "어린이티셔츠", "유아동티셔츠", "반팔티", "캐주얼티", "면티셔츠",
    "아동의류", "유치원복", "학교복", "데일리티", "기본티"
  ],
  "naming_seeds": {
    "상품핵심명사": ["티셔츠", "반팔티", "아동티"],
    "스타일형용사": ["캐주얼", "심플", "편안한", "기본"],
    "기능/효과표현": ["데일리착용용", "일상용", "학교용"],
    "상황/장소/계절": ["일상", "유치원", "학교", "여름"],
    "타깃고객": "3세 이상 어린이(남아/여아 공용)",
    "차별화포인트": ["면 100% 소재", "다양한 사이즈 구성", "기본 디자인"]
  }
}

잘못된 출력 예시 (피해야 할 것):
- "상품명: 어린이 티셔츠" 같은 필드 추가 (X)
- meta 값을 "옵션형" → "옵션 형"으로 정리 (X)
- 이미지에 없는 재질을 추측하여 추가 (X)
- JSON 외에 설명 문구 추가 (X)

반드시 위 JSON 스키마를 따르는 **단일 JSON 객체만** 응답하십시오.
JSON 앞뒤에 다른 문장이나 마크다운(````json 등)을 절대 추가하지 마십시오."""

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
    채팅방에서 한 번에 보내는 요청 1건과 똑같은 구조:
    - system_prompt: 시스템 프롬프트 (정적, 모든 요청에서 동일)
    - user_prompt: 사용자 프롬프트 (동적 데이터 포함)
    - image_paths: 같이 업로드할 이미지 경로들 (썸네일/상세이미지 포함, 순서 O)
    """
    system_prompt: str
    user_prompt: str
    image_paths: List[str]


def build_stage2_request_from_row(
    row: pd.Series,
    detail_cols: Sequence[str],
) -> Stage2Request:
    """
    엑셀 한 행 + 상세이미지 컬럼 리스트 → Stage2Request
    (프롬프트 + 이미지 경로 리스트)
    캐싱 최적화 버전: 동적 데이터를 프롬프트 뒤쪽에 배치
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

    # System 프롬프트는 항상 동일 (정적)
    system_prompt = STAGE2_SYSTEM_PROMPT

    # User 프롬프트는 동적 데이터만 포함
    user_prompt = STAGE2_USER_PROMPT_TEMPLATE.format(
        meta_json=meta_json_str,
        raw_name=raw_name,
        stage1_name=stage1_name,
        keywords=keywords,
    )

    return Stage2Request(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        image_paths=image_paths
    )
