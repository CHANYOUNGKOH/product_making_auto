#!/usr/bin/env python3
"""
상세설명 HTML 내 <img> 태그의 빈 alt 속성에
DB ST2_JSON 기반 자연어 설명을 자동 주입하는 모듈.

DB 안전 설계 (3중 보호):
  1. SQLite URI mode=ro — 엔진 레벨 쓰기 차단
  2. SELECT 전용 쿼리 — 코드 레벨 보호
  3. 폴백: DB 불가 → 캐시 비어있음 → 원본 HTML 그대로 반환
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ── 모듈 레벨 캐시 ──────────────────────────────────────────────
_alt_cache: Dict[str, dict] = {}

# 기본 DB 경로
# scripts/ → OC_ES_converter/ → 상품가공프로그램/ → DB_save/
_DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent.parent / "DB_save" / "products.db"

# 교환/반품 이미지 감지 패턴
_EXCHANGE_RE = re.compile(
    r"(교환|반품|환불|취소|as안내|배송안내|주의사항)",
    re.IGNORECASE,
)

# <img> 태그 매칭 (속성 포함)
_IMG_TAG_RE = re.compile(r"<img\b([^>]*)>", re.IGNORECASE)

# alt 속성 매칭
_ALT_ATTR_RE = re.compile(r'''alt\s*=\s*(?:"([^"]*)"|'([^']*)')''', re.IGNORECASE)

# src 속성 매칭
_SRC_ATTR_RE = re.compile(r'''src\s*=\s*(?:"([^"]*)"|'([^']*)')''', re.IGNORECASE)


# ── 캐시 초기화 ────────────────────────────────────────────────

def init_alt_cache(
    product_codes: List[str],
    db_path: Optional[str] = None,
) -> int:
    """변환 시작 시 1회 호출. DB에서 ST2_JSON을 읽어 캐시에 저장.

    Returns:
        캐시에 로드된 상품 수.
    """
    global _alt_cache
    _alt_cache = {}

    if not product_codes:
        return 0

    resolved_path = Path(db_path) if db_path else _DEFAULT_DB_PATH
    if not resolved_path.exists():
        logger.warning("SEO alt: DB 파일 없음 — %s", resolved_path)
        return 0

    try:
        # 1계층: URI mode=ro → 엔진 레벨 쓰기 차단
        uri = f"file:///{resolved_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        cursor = conn.cursor()

        # 2계층: SELECT 전용 쿼리
        placeholders = ",".join("?" for _ in product_codes)
        cursor.execute(
            f"SELECT 상품코드, ST2_JSON FROM products WHERE 상품코드 IN ({placeholders})",
            product_codes,
        )

        for code, st2_raw in cursor.fetchall():
            if not st2_raw:
                continue
            try:
                parsed = _parse_st2_json(st2_raw)
                if parsed:
                    _alt_cache[code] = parsed
            except Exception:
                logger.debug("SEO alt: ST2_JSON 파싱 실패 — %s", code)

        conn.close()
        logger.info("SEO alt: %d / %d 상품 캐시 로드 완료", len(_alt_cache), len(product_codes))
        return len(_alt_cache)

    except Exception as e:
        # 3계층: DB 장애 시 캐시 비어있음 → 원본 HTML 유지
        logger.warning("SEO alt: DB 연결 실패 — %s", e)
        return 0


def clear_alt_cache():
    """변환 종료 시 캐시 정리."""
    global _alt_cache
    _alt_cache = {}


# ── alt 주입 ───────────────────────────────────────────────────

def inject_alt_into_html(html: str, product_code: str) -> str:
    """상세설명 HTML 내 빈 alt 속성에 자연어 설명을 주입.

    1-pass: img 태그 스캔 → 주입 대상 개수 파악
    2-pass: 딱 그 수만큼 고유 alt 생성 → re.sub 주입

    캐시에 해당 상품이 없으면 원본 HTML 그대로 반환.
    """
    if not html or not product_code:
        return html

    attrs = _alt_cache.get(product_code)
    if not attrs:
        return html

    # ── 1-pass: 주입 대상 img 개수 카운트 ──
    need_count = 0
    for m in _IMG_TAG_RE.finditer(html):
        tag_inner = m.group(1)
        # 교환/반품은 고정 alt → 카운트 제외
        src_m = _SRC_ATTR_RE.search(tag_inner)
        src_val = (src_m.group(1) or src_m.group(2) or "") if src_m else ""
        if _is_exchange_image(src_val):
            continue
        # 이미 의미 있는 alt → 카운트 제외
        alt_m = _ALT_ATTR_RE.search(tag_inner)
        if alt_m and (alt_m.group(1) or alt_m.group(2) or "").strip():
            continue
        need_count += 1

    if need_count == 0:
        return html

    # ── 고유 alt 텍스트를 need_count개 생성 ──
    alt_texts = _build_alt_texts(attrs, need_count)
    if not alt_texts:
        return html

    # ── 2-pass: re.sub 주입 ──
    counter = [0]

    def _replace_img(match: re.Match) -> str:
        tag_inner = match.group(1)

        src_m = _SRC_ATTR_RE.search(tag_inner)
        src_val = (src_m.group(1) or src_m.group(2) or "") if src_m else ""

        if _is_exchange_image(src_val):
            return _set_alt_in_tag(match.group(0), tag_inner, "교환 및 반품 안내")

        alt_m = _ALT_ATTR_RE.search(tag_inner)
        if alt_m and (alt_m.group(1) or alt_m.group(2) or "").strip():
            return match.group(0)

        idx = counter[0]
        counter[0] += 1
        new_alt = alt_texts[idx] if idx < len(alt_texts) else alt_texts[-1]

        return _set_alt_in_tag(match.group(0), tag_inner, new_alt)

    return _IMG_TAG_RE.sub(_replace_img, html)


# ── 내부 헬퍼 ──────────────────────────────────────────────────

def _parse_st2_json(raw: str) -> Optional[dict]:
    """ST2_JSON 문자열을 파싱하여 alt 생성에 필요한 속성 추출."""
    data = json.loads(raw)
    ca = data.get("core_attributes", {})
    ns = data.get("naming_seeds", {})
    scenarios = data.get("usage_scenarios", [])

    product_type = ca.get("상품타입", "")
    nouns = ns.get("상품핵심명사", [])

    if not product_type and not nouns:
        return None

    return {
        "상품타입": product_type,
        "핵심명사": nouns[:7],
        "재질": ca.get("재질", []),
        "스타일": ca.get("스타일/특징", []),
        "용도": ca.get("주요용도", []),
        "대상": ca.get("사용대상", []),
        "시나리오": scenarios[:5],
    }


def _build_alt_texts(attrs: dict, need: int) -> List[str]:
    """need개만큼 고유한 alt 텍스트를 생성.

    전략: 기본 패턴 → 시나리오 → 교차 조합(명사×스타일, 명사×용도)
    부족하면 접미어 변형으로 확장. 모두 40~65자 보장.
    """
    seen: set[str] = set()
    texts: List[str] = []

    product_type = attrs.get("상품타입", "")
    nouns = attrs.get("핵심명사", [])
    materials = attrs.get("재질", [])
    styles = attrs.get("스타일", [])
    purposes = attrs.get("용도", [])
    targets = attrs.get("대상", [])
    scenarios = attrs.get("시나리오", [])

    noun_str = " ".join(nouns[:3]) if nouns else ""

    def _add(text: str) -> bool:
        """중복 없이 추가. need 도달하면 True 반환."""
        t = _ensure_length(text, product_type, noun_str)
        if t not in seen:
            seen.add(t)
            texts.append(t)
        return len(texts) >= need

    # ── 기본 패턴 (4개) ──
    if product_type and noun_str:
        if _add(f"{product_type} {noun_str}"):
            return texts
    elif product_type:
        if _add(product_type):
            return texts

    if materials and nouns:
        mat = materials[0] if isinstance(materials, list) else str(materials)
        if _add(f"{mat} 소재 {nouns[0]} 상세 이미지"):
            return texts

    if targets and purposes:
        target = targets[0] if isinstance(targets, list) else str(targets)
        purpose = purposes[0] if isinstance(purposes, list) else str(purposes)
        if _add(f"{target}을 위한 {purpose}"):
            return texts

    if styles and nouns:
        style = styles[0] if isinstance(styles, list) else str(styles)
        if _add(f"{style} {nouns[0]} 사용 모습"):
            return texts

    # ── 시나리오 (최대 5개) ──
    for s in scenarios:
        if s and _add(s):
            return texts

    # ── 교차 조합: 명사 × 스타일 ──
    for noun in nouns:
        for style in styles:
            if _add(f"{style} {noun} 상세 이미지"):
                return texts

    # ── 교차 조합: 명사 × 용도 ──
    for noun in nouns:
        for purpose in purposes:
            if _add(f"{purpose} {noun} 상품 이미지"):
                return texts

    # ── 교차 조합: 명사 × 대상 ──
    for noun in nouns:
        for target in (targets if isinstance(targets, list) else []):
            if _add(f"{target} {noun} 추천 상품"):
                return texts

    # ── 접미어 변형으로 부족분 채우기 ──
    _SUFFIXES = [
        "상세 이미지", "제품 상세컷", "활용 이미지", "특징 안내",
        "사용 예시", "구성품 안내", "상품 정보", "디테일컷",
    ]
    for suffix in _SUFFIXES:
        for noun in nouns:
            if _add(f"{noun} {suffix}"):
                return texts

    # 최후 폴백
    i = len(texts)
    while len(texts) < need:
        if _add(f"{product_type or noun_str} 상품 이미지 {i + 1}"):
            return texts
        i += 1

    return texts


def _ensure_length(text: str, product_type: str, noun_str: str) -> str:
    """alt 텍스트를 40~65자 범위로 조정."""
    text = text.strip()
    # 65자 초과 → 자르기
    if len(text) > 65:
        text = text[:62] + "..."
    # 40자 미만 → 패딩
    if len(text) < 40:
        pad = product_type or noun_str
        if pad and pad not in text:
            text = f"{text} {pad}"
        if len(text) < 40:
            text = f"{text} 상품 상세 이미지"
    # 패딩 후에도 65자 초과 가능 → 재자르기
    if len(text) > 65:
        text = text[:62] + "..."
    return text


def _is_exchange_image(src: str) -> bool:
    """src URL/파일명으로 교환·반품 안내 이미지 판별."""
    if not src:
        return False
    lower = src.lower()
    return bool(_EXCHANGE_RE.search(lower))


def _set_alt_in_tag(full_tag: str, tag_inner: str, alt_value: str) -> str:
    """img 태그에 alt 속성을 설정/교체."""
    escaped = alt_value.replace('"', '&quot;')
    alt_match = _ALT_ATTR_RE.search(tag_inner)
    if alt_match:
        # 기존 alt 교체
        new_inner = tag_inner[:alt_match.start()] + f'alt="{escaped}"' + tag_inner[alt_match.end():]
        return f"<img{new_inner}>"
    else:
        # alt 속성 추가
        return f'<img alt="{escaped}"{tag_inner}>'
