#!/usr/bin/env python3
"""
오너클랜 -> 이셀러스 변환 스크립트
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from seo_alt_injector import inject_alt_into_html

BASIC_INFO_COLUMNS = [
    "원본번호*",
    "판매자 관리코드",
    "폴더명",
    "카테고리 번호*",
    "상품명*",
    "판매가*",
    "수량*",
    "최대구매수량",
    "최소구매수량",
    "원산지*",
    "G마켓,옥션 원산지 유형",
    "수입사",
    "목록 이미지*",
    "이미지1(대표/기본이미지)*",
    "이미지2",
    "이미지3",
    "이미지4",
    "이미지5",
    "상세설명*",
    "ESM 추가구성 상세설명",
    "ESM 광고홍보 상세설명",
    "선택사항 타입",
    "선택사항 옵션명",
    "선택사항 상세정보",
    "선택사항 재고 사용여부",
    "작성형 선택사항",
    "추가구성 옵션명",
    "추가구성 상세정보",
    "브랜드",
    "모델명",
    "제조사",
    "과세여부",
    "나이제한",
    "제조일자",
    "유효일자",
    "홍보문구",
    "상품상태",
    "원가",
    "공급가",
    "도서정가",
    "ISBN",
    "문화비 소득공제",
    "검색어(태그)",
    "인증정보",
    "요약정보 상품군 코드*",
    "요약정보 전항목 상세설명 참조",
    "값1",
    "값2",
    "값3",
    "값4",
    "값5",
    "값6",
    "값7",
    "값8",
    "값9",
    "값10",
    "값11",
    "값12",
    "값13",
    "값14",
    "값15",
    "값16",
    "값17",
    "값18",
    "값19",
    "값20",
    "값21",
    "값22",
    "값23",
    "값24",
    "값25",
    "값26",
    "값27",
    "값28",
    "값29",
    "기본정보 오류메시지",
]

EXTENDED_INFO_COLUMNS = [
    "원본번호*",
    "마켓 카테고리번호",
    "위메프2.0 담당MD",
    "스마트스토어 전용 상품명 사용 여부",
    "스마트스토어 전용 상품명",
    "병행수입여부 및 수입신고필증",
    "롯데ON 수입형태",
    "인보이스",
    "기타 구비서류",
    "쿠팡 옵션 할인율기준가",
    "쿠팡 옵션 대표이미지",
    "쿠팡 옵션 상세설명",
    "쿠팡 옵션 바코드",
    "쿠팡 옵션 인증정보",
    "쿠팡 옵션 모델번호",
    "쿠팡 옵션 검색옵션명",
    "쿠팡 옵션 검색옵션값",
    "11번가 상품속성",
    "스마트스토어 상품속성",
    "위메프2.0 상품속성 라벨",
    "위메프2.0 제휴채널 검색키워드",
    "티몬 법적허가 및 신고대상 상품",
    "확장정보 오류메시지",
]

OUTPUT_MARKETS = [
    {"id": "coupang", "name": "쿠팡", "default": True, "max_stage": 2},
    {"id": "smartstore", "name": "스마트스토어", "default": True, "max_stage": 3},
    {"id": "st11", "name": "11번가", "default": False, "max_stage": 2},
    {"id": "gmarket", "name": "지마켓", "default": False, "max_stage": 2},
    {"id": "auction", "name": "옥션", "default": False, "max_stage": 2},
    {"id": "godomall", "name": "고도몰", "default": False, "max_stage": 0},
]

MARKET_CATEGORY_SHEETS = {
    "auction": "옥션",
    "gmarket": "G마켓",
    "st11": "11번가",
    "smartstore": "스마트스토어",
    "coupang": "쿠팡",
}

OWNERCLAN_TO_ESELLERS_MARKET = {
    "auction": "auction",
    "gmarket": "gmarket",
    "st11": "st11",
    "storefarm": "smartstore",
    "smartstore": "smartstore",
    "coupang": "coupang",
}

MARKET_NAME_MAP = {
    "auction": "옥션",
    "gmarket": "지마켓",
    "st11": "11번가",
    "storefarm": "스마트스토어",
    "smartstore": "스마트스토어",
    "coupang": "쿠팡",
    "wmp": "위메프",
    "tmon": "티몬",
    "interpark": "인터파크",
    "playauto": "플레이오토",
}

DEFAULT_CATEGORY_FILE = "cc-system/OpenMarketCategory.xlsx"
CHUNK_SIZE = 5000

DETAIL_NOTICE_HTML = (
    '<p style="font-size: 12px; color: #777777; display: block; margin: 20px 0;">'
    "본 제품을 구매하시면 원활한 배송을 위해 꼭 필요한 고객님의 개인정보를 "
    "(성함, 주소, 전화번호 등) 택배사 및 제 3업체에서 이용하는 것에 동의하시는 것으로 간주됩니다.<br/>"
    " 개인정보는 배송 외의 용도로는 절대 사용되지 않으니 안심하시기 바랍니다. 안전하게 배송해 드리겠습니다."
    "</p>"
)


def _safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_code(value) -> str:
    text = _safe_str(value)
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _to_int_string(value, default: str = "") -> str:
    text = _normalize_code(value).replace(",", "")
    if not text:
        return default
    try:
        return str(int(float(text)))
    except ValueError:
        return text


def extract_esellers_category(market_category_str) -> str:
    if pd.isna(market_category_str):
        return ""
    match = re.search(r"esellers,(\d+),", str(market_category_str))
    return match.group(1) if match else ""


def extract_market_categories(market_category_str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if pd.isna(market_category_str):
        return result
    for line in str(market_category_str).splitlines():
        parts = [part.strip() for part in line.split(",", 2)]
        if len(parts) >= 2 and parts[0] and parts[1]:
            result[parts[0]] = _normalize_code(parts[1])
    return result


def extract_category_name_from_ownerclan(market_category_str, market_id: str) -> str:
    if pd.isna(market_category_str):
        return ""
    for line in str(market_category_str).splitlines():
        parts = [part.strip() for part in line.split(",", 2)]
        if len(parts) >= 3 and parts[0] == market_id:
            return parts[2]
    return ""


def convert_origin(origin_str) -> str:
    origin = _safe_str(origin_str)
    if not origin:
        return "국산"
    if origin.startswith("국산/") or origin.startswith("수입/"):
        return origin
    if origin in ("국내", "국내산", "국산"):
        return "국산"
    if "|" in origin:
        parts = [part.strip() for part in origin.split("|") if part.strip()]
        if not parts:
            return "국산"
        head = parts[0]
        if head in ("국내", "국내산", "국산"):
            return "국산" if len(parts) == 1 else "국산/" + "/".join(parts[1:])
        if head.startswith("수입"):
            return "수입" if len(parts) == 1 else "수입/" + "/".join(parts[1:])
        return "/".join(parts)
    return origin.replace("|", "/")


def append_detail_notice(detail_html) -> str:
    detail = _safe_str(detail_html)
    if not detail:
        return ""
    if "본 제품을 구매하시면 원활한 배송을 위해 꼭 필요한 고객님의 개인정보를" in detail:
        return detail
    if detail.endswith("</center>"):
        return detail[:-9] + "<br>" + DETAIL_NOTICE_HTML + "</center>"
    return f"<center>{detail}<br>{DETAIL_NOTICE_HTML}</center>"


def parse_info_values(info_text, limit: int = 29) -> List[str]:
    raw = _safe_str(info_text)
    if not raw:
        return []

    values: List[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        for token in (" : ", ": ", ":", "："):
            if token in line:
                line = line.split(token, 1)[1].strip()
                break
        values.append(line)
        if len(values) >= limit:
            break
    return values


def build_option_fields(row: pd.Series) -> Tuple[str, str, str]:
    combo_text = _safe_str(row.get("조합형옵션", ""))
    if not combo_text:
        return "", "", ""

    option_names = [name for name in (_safe_str(row.get("옵션1명", "")), _safe_str(row.get("옵션2명", ""))) if name]
    main_image = _safe_str(row.get("이미지대", ""))
    details: List[str] = []

    for line in combo_text.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if not parts or not parts[0]:
            continue
        option_value = parts[0]
        option_price = parts[1] if len(parts) > 1 and parts[1] else "0"
        option_stock = parts[2] if len(parts) > 2 and parts[2] else "9999"
        details.append(f"{option_value}**{option_price}*{option_stock}*Y*{main_image}*")

    return "조합형", "\n".join(option_names), "\n".join(details)


def convert_option_to_2stage(opt_names: str, opt_details: str) -> Tuple[str, str]:
    return _safe_str(opt_names), _safe_str(opt_details)


def convert_option_to_max_stage(opt_names: str, opt_details: str, max_stage: int) -> Tuple[str, str]:
    names = [name for name in _safe_str(opt_names).split("\n") if name]
    if len(names) <= max_stage:
        return _safe_str(opt_names), _safe_str(opt_details)
    return "\n".join(names[:max_stage]), _safe_str(opt_details)


def apply_11st_option_logic(opt_type: str, opt_names: str, opt_details: str, main_image: str) -> Tuple[str, str, str]:
    return _safe_str(opt_type), _safe_str(opt_names), _safe_str(opt_details)


class ESMCategoryMapper:
    def __init__(self, category_file: str, selected_markets: Optional[List[str]] = None):
        self.category_file = category_file
        self.selected_markets = selected_markets or []
        self.loaded = False
        self.esm_to_market: Dict[Tuple[str, str], str] = {}
        self._load()

    def _load(self):
        if not self.category_file or not os.path.exists(self.category_file):
            return
        try:
            df = pd.read_excel(self.category_file, sheet_name="ESM2.0", engine="openpyxl")
        except Exception:
            return

        for _, row in df.iterrows():
            es_code = _normalize_code(row.get("ESM 카테고리코드", ""))
            market_key = _safe_str(row.get("마켓코드", "")).upper()
            market_code = _normalize_code(row.get("마켓 카테고리코드", ""))
            if es_code and market_key and market_code:
                self.esm_to_market[(es_code, market_key)] = market_code
        self.loaded = True

    def get_market_category(self, market_id: str, esellers_code: str) -> str:
        market_key = {"auction": "A", "gmarket": "G"}.get(market_id, "")
        if not market_key:
            return ""
        return self.esm_to_market.get((_normalize_code(esellers_code), market_key), "")


class CategoryValidator:
    def __init__(self, category_file: str, selected_markets: Optional[List[str]] = None):
        self.category_file = category_file
        self.selected_markets = selected_markets or []
        self.loaded = False
        self.market_categories: Dict[str, set[str]] = {}
        self._load()

    def _load(self):
        if not self.category_file or not os.path.exists(self.category_file):
            return

        target_markets = self.selected_markets or list(MARKET_CATEGORY_SHEETS.keys())
        loaded_any = False
        for market_id in target_markets:
            sheet_name = MARKET_CATEGORY_SHEETS.get(market_id)
            if not sheet_name:
                continue
            try:
                df = pd.read_excel(self.category_file, sheet_name=sheet_name, engine="openpyxl")
            except Exception:
                continue

            first_col = df.columns[0]
            codes = {_normalize_code(value) for value in df[first_col] if _normalize_code(value)}
            if codes:
                self.market_categories[market_id] = codes
                loaded_any = True

        self.loaded = loaded_any

    def validate_category(self, market_id: str, category_code: str) -> dict:
        normalized = _normalize_code(category_code)
        if not normalized:
            return {"valid": False, "reason": "카테고리 코드 없음"}

        codes = self.market_categories.get(market_id)
        if not codes:
            return {"valid": False, "reason": "검증 시트 없음"}

        if normalized in codes:
            return {"valid": True, "reason": ""}
        return {"valid": False, "reason": "OpenMarketCategory.xlsx에 없는 카테고리 코드"}


def format_market_category_for_market(market_cats: Dict[str, str], market_id: str, esm_mapper: Optional[ESMCategoryMapper] = None) -> str:
    ownerclan_market_id = next((oc_id for oc_id, es_id in OWNERCLAN_TO_ESELLERS_MARKET.items() if es_id == market_id), market_id)
    market_code = market_cats.get(ownerclan_market_id, "")
    if not market_code:
        esellers_code = market_cats.get("esellers", "")
        if esellers_code and esm_mapper:
            market_code = esm_mapper.get_market_category(market_id, esellers_code)
    if not market_code:
        return ""

    market_name = MARKET_NAME_MAP.get(ownerclan_market_id, MARKET_NAME_MAP.get(market_id, market_id))
    return f"{market_name}*{market_code}"


def convert_row_basic(row: pd.Series, idx: int) -> dict:
    result = {col: "" for col in BASIC_INFO_COLUMNS}
    result["원본번호*"] = idx + 1
    result["판매자 관리코드"] = _safe_str(row.get("상품코드", "")) or _safe_str(row.get("판매자관리코드1", ""))
    result["카테고리 번호*"] = extract_esellers_category(row.get("마켓카테고리", ""))
    result["상품명*"] = _safe_str(row.get("마켓상품명", ""))
    result["판매가*"] = _to_int_string(row.get("마켓실제판매가", ""), "0")
    result["수량*"] = "9999"
    result["최대구매수량"] = _to_int_string(row.get("최대구매수량", ""), "")
    result["최소구매수량"] = _to_int_string(row.get("최소구매수량", ""), "")
    result["원산지*"] = convert_origin(row.get("원산지", ""))
    result["목록 이미지*"] = _safe_str(row.get("이미지대", ""))
    result["이미지1(대표/기본이미지)*"] = _safe_str(row.get("이미지대", ""))
    result["이미지2"] = _safe_str(row.get("이미지중", ""))
    result["이미지3"] = _safe_str(row.get("이미지소", ""))
    _product_code = _safe_str(row.get("상품코드", ""))
    result["상세설명*"] = append_detail_notice(inject_alt_into_html(row.get("본문상세설명", ""), _product_code))

    option_type, option_names, option_details = build_option_fields(row)
    result["선택사항 타입"] = option_type
    result["선택사항 옵션명"] = option_names
    result["선택사항 상세정보"] = option_details
    result["선택사항 재고 사용여부"] = "N"

    result["브랜드"] = _safe_str(row.get("브랜드", ""))
    result["모델명"] = _safe_str(row.get("모델명", ""))
    result["제조사"] = _safe_str(row.get("제작/수입사", ""))
    result["과세여부"] = "과세"
    result["나이제한"] = "전체이용가"
    result["상품상태"] = _safe_str(row.get("상품상태", "")) or "신상품"
    result["원가"] = _to_int_string(row.get("오너클랜판매가", ""), "")
    result["검색어(태그)"] = _safe_str(row.get("키워드", ""))
    result["인증정보"] = _safe_str(row.get("인증정보", ""))
    result["요약정보 상품군 코드*"] = _to_int_string(row.get("정보고시코드", ""), "")
    result["요약정보 전항목 상세설명 참조"] = "Y"

    for info_idx, value in enumerate(parse_info_values(row.get("정보고시 항목정보", "")), start=1):
        key = f"값{info_idx}"
        if key in result:
            result[key] = value

    return result


def convert_row_extended(row: pd.Series, idx: int, esm_mapper: Optional[ESMCategoryMapper] = None) -> dict:
    result = {col: "" for col in EXTENDED_INFO_COLUMNS}
    result["원본번호*"] = idx + 1
    market_cats = extract_market_categories(row.get("마켓카테고리", ""))
    result["마켓 카테고리번호"] = format_market_category_for_market(market_cats, "coupang", esm_mapper)
    result["11번가 상품속성"] = _safe_str(row.get("11번가", ""))
    result["스마트스토어 상품속성"] = _safe_str(row.get("스마트스토어", ""))
    return result


def read_ownerclan(file_path: str) -> pd.DataFrame:
    return pd.read_excel(file_path, engine="openpyxl", header=1)


def convert_ownerclan_to_esellers(oc_df: pd.DataFrame, esm_mapper: Optional[ESMCategoryMapper] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    basic_rows = []
    extended_rows = []

    for idx, row in oc_df.iterrows():
        basic_rows.append(convert_row_basic(row, idx))
        extended_rows.append(convert_row_extended(row, idx, esm_mapper))

    basic_df = pd.DataFrame(basic_rows)
    for col in BASIC_INFO_COLUMNS:
        if col not in basic_df.columns:
            basic_df[col] = ""
    basic_df = basic_df[BASIC_INFO_COLUMNS]

    extended_df = pd.DataFrame(extended_rows)
    for col in EXTENDED_INFO_COLUMNS:
        if col not in extended_df.columns:
            extended_df[col] = ""
    extended_df = extended_df[EXTENDED_INFO_COLUMNS]
    return basic_df, extended_df


def convert_ownerclan_to_esellers_for_market(
    oc_df: pd.DataFrame,
    market_id: str,
    esm_mapper: Optional[ESMCategoryMapper] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    market_info = next((market for market in OUTPUT_MARKETS if market["id"] == market_id), None)
    if not market_info:
        return convert_ownerclan_to_esellers(oc_df, esm_mapper)

    max_stage = market_info["max_stage"]
    basic_df, extended_df = convert_ownerclan_to_esellers(oc_df, esm_mapper)

    if market_id == "st11":
        for idx in range(len(basic_df)):
            opt_type = basic_df.at[idx, "선택사항 타입"]
            opt_names = basic_df.at[idx, "선택사항 옵션명"]
            opt_details = basic_df.at[idx, "선택사항 상세정보"]
            main_image = basic_df.at[idx, "이미지1(대표/기본이미지)*"]
            new_opt_type, new_opt_names, new_opt_details = apply_11st_option_logic(opt_type, opt_names, opt_details, main_image)
            basic_df.at[idx, "선택사항 타입"] = new_opt_type
            basic_df.at[idx, "선택사항 옵션명"] = new_opt_names
            basic_df.at[idx, "선택사항 상세정보"] = new_opt_details
    elif market_id in ("auction", "gmarket"):
        for idx in range(len(basic_df)):
            opt_names = basic_df.at[idx, "선택사항 옵션명"]
            opt_details = basic_df.at[idx, "선택사항 상세정보"]
            if pd.notna(opt_names) and str(opt_names).strip():
                new_names, new_details = convert_option_to_2stage(str(opt_names), str(opt_details))
                basic_df.at[idx, "선택사항 옵션명"] = new_names
                basic_df.at[idx, "선택사항 상세정보"] = new_details
    elif max_stage < 99:
        for idx in range(len(basic_df)):
            opt_names = basic_df.at[idx, "선택사항 옵션명"]
            opt_details = basic_df.at[idx, "선택사항 상세정보"]
            if pd.notna(opt_names) and str(opt_names).strip():
                names = str(opt_names).strip().split("\n")
                if len(names) > max_stage:
                    new_names, new_details = convert_option_to_max_stage(str(opt_names), str(opt_details), max_stage)
                    basic_df.at[idx, "선택사항 옵션명"] = new_names
                    basic_df.at[idx, "선택사항 상세정보"] = new_details

    for idx, row in oc_df.iterrows():
        market_cat_val = row["마켓카테고리"] if "마켓카테고리" in row.index else ""
        market_cats = extract_market_categories(market_cat_val)
        new_cat = format_market_category_for_market(market_cats, market_id, esm_mapper)
        extended_df.at[idx, "마켓 카테고리번호"] = new_cat if new_cat else ""

    if market_id != "coupang":
        extended_df["쿠팡 옵션 인증정보"] = ""

    return basic_df, extended_df


def _write_sheet_xlsxwriter(ws, df: pd.DataFrame):
    for col_idx, col_name in enumerate(df.columns):
        ws.write(0, col_idx, col_name)
    for row_idx, (_, row) in enumerate(df.iterrows()):
        for col_idx, value in enumerate(row):
            if pd.isna(value):
                continue
            ws.write(row_idx + 2, col_idx, value)


def _save_single_file(basic_df: pd.DataFrame, extended_df: pd.DataFrame, file_path: str):
    import xlsxwriter

    workbook = xlsxwriter.Workbook(file_path)
    ws_basic = workbook.add_worksheet("기본정보")
    _write_sheet_xlsxwriter(ws_basic, basic_df)
    ws_extended = workbook.add_worksheet("확장정보")
    _write_sheet_xlsxwriter(ws_extended, extended_df)
    workbook.close()


def save_esellers(basic_df: pd.DataFrame, extended_df: pd.DataFrame, file_path: str, chunk_size: int = CHUNK_SIZE) -> List[dict]:
    output_dir = Path(file_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows = len(basic_df)
    if chunk_size is None or chunk_size <= 0 or total_rows <= chunk_size:
        _save_single_file(basic_df, extended_df, file_path)
        return [{"path": file_path, "count": total_rows}]

    saved_files = []
    base = Path(file_path)
    stem = base.stem
    suffix = base.suffix
    parent = base.parent

    chunk_num = 1
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk_basic = basic_df.iloc[start:end].reset_index(drop=True)
        chunk_extended = extended_df.iloc[start:end].reset_index(drop=True)
        chunk_file = parent / f"{stem}_{chunk_num:02d}{suffix}"
        _save_single_file(chunk_basic, chunk_extended, str(chunk_file))
        saved_files.append({"path": str(chunk_file), "count": len(chunk_basic)})
        chunk_num += 1

    return saved_files


def classify_products_by_category(
    oc_df: pd.DataFrame,
    basic_df: pd.DataFrame,
    extended_df: pd.DataFrame,
    target_market: str,
    category_validator: CategoryValidator,
) -> dict:
    oc_market_id = target_market
    for ownerclan_market_id, esellers_market_id in OWNERCLAN_TO_ESELLERS_MARKET.items():
        if esellers_market_id == target_market:
            oc_market_id = ownerclan_market_id
            break

    normal_indices = []
    fallback_indices = []
    missing_report = []

    for idx, row in oc_df.iterrows():
        row_num = idx + 1
        market_cat_str = row.get("마켓카테고리", "")
        market_cats = extract_market_categories(market_cat_str)
        market_cat_code = market_cats.get(oc_market_id, "")
        esellers_cat = extract_esellers_category(market_cat_str)
        target_market_name = MARKET_NAME_MAP.get(oc_market_id, target_market)

        if not market_cat_code:
            if esellers_cat:
                fallback_indices.append(idx)
                missing_report.append(
                    {
                        "원본번호": row_num,
                        "상품코드": row.get("상품코드", ""),
                        "판매자관리코드": row.get("판매자관리코드1", ""),
                        "상품명": row.get("마켓상품명", ""),
                        "대상 마켓": target_market_name,
                        "오너클랜 마켓카테고리": "",
                        "오너클랜 카테고리명": "",
                        "esellers 카테고리": esellers_cat,
                        "누락 사유": "오너클랜 원본에 마켓 카테고리 없음",
                    }
                )
            else:
                missing_report.append(
                    {
                        "원본번호": row_num,
                        "상품코드": row.get("상품코드", ""),
                        "판매자관리코드": row.get("판매자관리코드1", ""),
                        "상품명": row.get("마켓상품명", ""),
                        "대상 마켓": target_market_name,
                        "오너클랜 마켓카테고리": "",
                        "오너클랜 카테고리명": "",
                        "esellers 카테고리": "",
                        "누락 사유": "오너클랜 원본에 마켓 카테고리 없음 + esellers 카테고리 없음 (등록불가)",
                    }
                )
            continue

        validation = category_validator.validate_category(target_market, market_cat_code)
        if validation["valid"]:
            normal_indices.append(idx)
        else:
            cat_name = extract_category_name_from_ownerclan(market_cat_str, oc_market_id)
            if esellers_cat:
                fallback_indices.append(idx)
                missing_report.append(
                    {
                        "원본번호": row_num,
                        "상품코드": row.get("상품코드", ""),
                        "판매자관리코드": row.get("판매자관리코드1", ""),
                        "상품명": row.get("마켓상품명", ""),
                        "대상 마켓": target_market_name,
                        "오너클랜 마켓카테고리": market_cat_code,
                        "오너클랜 카테고리명": cat_name,
                        "esellers 카테고리": esellers_cat,
                        "누락 사유": validation["reason"],
                    }
                )
            else:
                missing_report.append(
                    {
                        "원본번호": row_num,
                        "상품코드": row.get("상품코드", ""),
                        "판매자관리코드": row.get("판매자관리코드1", ""),
                        "상품명": row.get("마켓상품명", ""),
                        "대상 마켓": target_market_name,
                        "오너클랜 마켓카테고리": market_cat_code,
                        "오너클랜 카테고리명": cat_name,
                        "esellers 카테고리": "",
                        "누락 사유": f"{validation['reason']} + esellers 카테고리 없음 (등록불가)",
                    }
                )

    return {
        "normal": {
            "indices": normal_indices,
            "basic_df": basic_df.iloc[normal_indices].reset_index(drop=True),
            "extended_df": extended_df.iloc[normal_indices].reset_index(drop=True),
        },
        "fallback": {
            "indices": fallback_indices,
            "basic_df": basic_df.iloc[fallback_indices].reset_index(drop=True),
            "extended_df": clear_market_category_for_fallback(extended_df.iloc[fallback_indices].reset_index(drop=True)),
        },
        "missing_report": missing_report,
    }


def clear_market_category_for_fallback(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "마켓 카테고리번호" in df.columns:
        df["마켓 카테고리번호"] = ""
    return df


def save_missing_report(report_data: list, file_path: str):
    if not report_data:
        return
    output_dir = Path(file_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    report_df = pd.DataFrame(report_data)
    columns = [
        "원본번호",
        "상품코드",
        "판매자관리코드",
        "상품명",
        "대상 마켓",
        "오너클랜 마켓카테고리",
        "오너클랜 카테고리명",
        "esellers 카테고리",
        "누락 사유",
    ]
    for col in columns:
        if col not in report_df.columns:
            report_df[col] = ""
    report_df = report_df[columns]
    report_df.to_excel(file_path, index=False, engine="openpyxl")


def save_esellers_with_validation(
    oc_df: pd.DataFrame,
    basic_df: pd.DataFrame,
    extended_df: pd.DataFrame,
    target_market: str,
    category_validator: CategoryValidator,
    output_path: Path,
    base_filename: str,
    chunk_size: int = CHUNK_SIZE,
) -> dict:
    result = {
        "normal_files": [],
        "fallback_files": [],
        "report_file": None,
        "normal_count": 0,
        "fallback_count": 0,
        "missing_count": 0,
    }

    if not category_validator or not category_validator.loaded:
        normal_file = output_path / f"{base_filename}.xlsx"
        saved_files = save_esellers(basic_df, extended_df, str(normal_file), chunk_size=chunk_size)
        result["normal_files"] = saved_files
        result["normal_count"] = len(basic_df)
        return result

    classification = classify_products_by_category(oc_df, basic_df, extended_df, target_market, category_validator)

    normal_data = classification["normal"]
    if len(normal_data["indices"]) > 0:
        normal_file = output_path / f"{base_filename}.xlsx"
        saved_files = save_esellers(normal_data["basic_df"], normal_data["extended_df"], str(normal_file), chunk_size=chunk_size)
        result["normal_files"] = saved_files
        result["normal_count"] = len(normal_data["indices"])

    fallback_data = classification["fallback"]
    if len(fallback_data["indices"]) > 0:
        fallback_file = output_path / f"{base_filename}_기본카테고리.xlsx"
        saved_files = save_esellers(fallback_data["basic_df"], fallback_data["extended_df"], str(fallback_file), chunk_size=chunk_size)
        result["fallback_files"] = saved_files
        result["fallback_count"] = len(fallback_data["indices"])

    missing_report = classification["missing_report"]
    if missing_report:
        unregistrable_dir = output_path / "등록불가"
        unregistrable_dir.mkdir(parents=True, exist_ok=True)
        report_file = unregistrable_dir / f"{base_filename}_누락카테고리.xlsx"
        save_missing_report(missing_report, str(report_file))
        result["report_file"] = str(report_file)
        result["missing_count"] = len(missing_report)

    return result


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "output/esellers_base.xlsx"
    category_file = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_CATEGORY_FILE

    if not os.path.exists(input_file):
        print(f"오류: 파일을 찾을 수 없습니다: {input_file}")
        sys.exit(1)

    esm_mapper = ESMCategoryMapper(category_file)
    oc_df = read_ownerclan(input_file)
    basic_df, extended_df = convert_ownerclan_to_esellers(oc_df, esm_mapper)
    save_esellers(basic_df, extended_df, output_file)
    print(f"총 상품 수: {len(basic_df)}")


if __name__ == "__main__":
    main()
