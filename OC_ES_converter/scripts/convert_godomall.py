#!/usr/bin/env python3
"""Ownerclan -> Godomall converter helpers."""

from pathlib import Path
import tempfile

import pandas as pd
from openpyxl import load_workbook

from seo_alt_injector import inject_alt_into_html

DEFAULT_GODOMALL_TEMPLATE = str(Path.home() / "Downloads" / "상품샘플파일.xlsx")

GODOMALL_DEFAULTS = {
    "category_code": "001",
    "brand_code": "001001",
    "deliverySno": "5",
    "fixed_price": 0,
    "option_yn": "n",
    "image_storage": "url",
}

GODOMALL_OPTION_LABEL = "옵션선택^|^세부옵션"


def _safe_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _safe_price(value):
    if pd.isna(value):
        return ""
    text = str(value).strip().replace(",", "")
    if not text:
        return ""
    try:
        return int(float(text))
    except ValueError:
        return text


def resolve_godomall_template_path() -> str:
    """Find a Godomall template in Downloads."""
    candidate = Path.home() / "Downloads" / "상품샘플파일.xlsx"
    if candidate.exists():
        return str(candidate)

    matches = list((Path.home() / "Downloads").glob("상품샘플파일*.xlsx"))
    if matches:
        return str(matches[0])

    return DEFAULT_GODOMALL_TEMPLATE


def build_godomall_image_name(image_url: str) -> str:
    """Build Godomall image_name using a single URL."""
    if pd.isna(image_url) or not str(image_url).strip():
        return ""

    image_url = str(image_url).strip()
    return "\n".join(
        [
            f"magnify^|^{image_url}^|^{image_url}^|^{image_url}",
            f"detail^|^{image_url}^|^{image_url}^|^{image_url}",
            f"list^|^{image_url}",
            f"main^|^{image_url}",
        ]
    )


def build_goods_must_info() -> str:
    lines = [
        "품명^|^품명:상세설명참조",
        "모델명^|^모델명:상세설명참조",
        "인증/허가 내용^|^법에 의한 인증허가 등을 받았음을 확인할 수 있는 경우 그에 대한 사항:상세설명참조",
        "제조국 또는 원산지^|^제조국 또는 원산지:상세설명참조",
        "제조자^|^제조자:상세설명참조",
        "수입품여부(Y/N)^|^Y",
        "수입자^|^수입자:상세설명참조",
        "A/S책임자와 전화번호^|^고객센터 010-5950-2949",
    ]
    return "\n".join(lines)


def build_naver_tag(keyword_text: str) -> str:
    keywords = [token.strip() for token in _safe_str(keyword_text).split(",") if token.strip()]
    return "|".join(keywords)


def parse_ownerclan_combo_options(combo_text: str) -> list[tuple[str, str, str]]:
    options: list[tuple[str, str, str]] = []
    for raw_line in _safe_str(combo_text).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split(",")]
        option_value = parts[0] if len(parts) > 0 else ""
        option_price = parts[1] if len(parts) > 1 and parts[1] else "0"
        option_stock = parts[2] if len(parts) > 2 and parts[2] else "999"
        if option_value:
            options.append((option_value, option_price, option_stock))
    return options


def build_option_block(row: pd.Series) -> dict:
    combo_text = row.get("조합형옵션", "")
    options = parse_ownerclan_combo_options(combo_text)
    if not options:
        return {
            "option_yn": "n",
            "option_display": "",
            "option_name": "",
            "option_value": "",
            "option_image": "",
            "option_cost_price": "",
            "option_price": "",
            "stock_cnt": "999",
            "option_view_fl": "",
            "option_sell_fl": "",
            "option_delivery_fl": "",
        }

    option_values = [value for value, _, _ in options]
    option_prices = [price for _, price, _ in options]
    option_rows = len(options)
    return {
        "option_yn": "y",
        "option_display": "s",
        "option_name": GODOMALL_OPTION_LABEL,
        "option_value": "\n".join(option_values),
        "option_image": "",
        "option_cost_price": "\n".join(["0"] * option_rows),
        "option_price": "\n".join(option_prices),
        "stock_cnt": "\n".join(["999"] * option_rows),
        "option_view_fl": "\n".join(["y"] * option_rows),
        "option_sell_fl": "\n".join(["y"] * option_rows),
        "option_delivery_fl": "\n".join(["y"] * option_rows),
    }


def convert_ownerclan_to_godomall(
    oc_df: pd.DataFrame,
    category_code: str = GODOMALL_DEFAULTS["category_code"],
    brand_code: str = GODOMALL_DEFAULTS["brand_code"],
    delivery_sno: str = GODOMALL_DEFAULTS["deliverySno"],
):
    """Convert Ownerclan rows into Godomall sheet rows."""
    rows = []

    for _, row in oc_df.iterrows():
        ownerclan_price = _safe_price(row.get("오너클랜판매가", ""))
        product_name = _safe_str(row.get("마켓상품명", ""))
        image_url = _safe_str(row.get("이미지대", ""))
        detail_html = _safe_str(row.get("본문상세설명", ""))
        detail_html = inject_alt_into_html(detail_html, _safe_str(row.get("상품코드", "")))
        option_block = build_option_block(row)

        rows.append(
            {
                "상품 상태": "n",
                "goods_no": "",
                "goods_name": product_name,
                "goods_cd": _safe_str(row.get("상품코드", "")),
                "category_code": str(category_code),
                "purchase_goods_name": product_name,
                "brand_code": str(brand_code),
                "pay_limit_fl": "n",
                "model_no": "edit" + _safe_str(row.get("상품코드", "")),
                "maker_name": _safe_str(row.get("제작/수입사", "")),
                "origin_name": _safe_str(row.get("원산지", "")),
                "search_word": _safe_str(row.get("키워드", "")),
                "deliverySno": str(delivery_sno),
                "goods_price": ownerclan_price,
                "fixed_price": GODOMALL_DEFAULTS["fixed_price"],
                "cost_price": ownerclan_price,
                "option_yn": option_block["option_yn"],
                "option_display": option_block["option_display"],
                "option_name": option_block["option_name"],
                "option_value": option_block["option_value"],
                "option_image": option_block["option_image"],
                "option_cost_price": option_block["option_cost_price"],
                "option_price": option_block["option_price"],
                "stock_cnt": option_block["stock_cnt"],
                "option_view_fl": option_block["option_view_fl"],
                "option_sell_fl": option_block["option_sell_fl"],
                "option_delivery_fl": option_block["option_delivery_fl"],
                "make_date": "0000-00-00",
                "launch_date": "0000-00-00",
                "effective_start_ymd": "0000-00-00 00:00:00",
                "effective_end_ymd": "0000-00-00 00:00:00",
                "goods_permission": "all",
                "goods_permission_price_string_fl": "n",
                "only_adult_fl": "n",
                "only_adult_display_fl": "y",
                "only_adult_image_fl": "n",
                "goods_access": "all",
                "goods_access_display_fl": "y",
                "goods_must_info": build_goods_must_info(),
                "kcmark_fl": "n",
                "weight": 0,
                "volume": 0,
                "stock_type": "y",
                "mileage_type": "c",
                "mileage_group": "all",
                "goods_discount_fl": "n",
                "goods_discount": 0.00,
                "goods_discount_unit": "percent",
                "fixed_sales": "option",
                "sales_unit": 1,
                "soldout_yn": "n",
                "tax_free_type": "t",
                "tax_percent": 10.0,
                "display_pc_yn": "y",
                "display_mobile_yn": "y",
                "sell_pc_yn": "y",
                "sell_mobile_yn": "y",
                "fixed_cnt": "goods",
                "min_cnt": 1,
                "max_cnt": 999,
                "sales_start_ymd": "0000-00-00 00:00:00",
                "sales_end_ymd": "0000-00-00 00:00:00",
                "culture_benefit_fl": "n",
                "external_video_fl": "n",
                "external_video_width": 0,
                "external_video_height": 0,
                "text_option_yn": "n",
                "delivery_schedule_yn": "n",
                "relation_yn": "a",
                "relation_same_fl": "y",
                "add_goods_fl": "n",
                "imgDetail_view_fl": "y",
                "image_storage": GODOMALL_DEFAULTS["image_storage"],
                "image_name": build_godomall_image_name(image_url),
                "event_description": "모든카드 3개월 무이자 할부!",
                "goods_desc_pc": detail_html,
                "goods_desc_mobile": detail_html,
                "goods_desc_same_flag": "y",
                "daum_flag": "y",
                "naver_flag": "y",
                "naver_age_group": "a",
                "naver_gender": "c",
                "naver_tag": build_naver_tag(row.get("키워드", "")),
                "naver_npay_able": "all",
                "naver_npay_acum_able": "all",
                "naver_brand_certification": "n",
                "naverbook_flag": "n",
                "goods_type": "P",
                "detail_delivery_fl": "selection",
                "detail_delivery": "002001",
                "detail_as_fl": "selection",
                "detail_as": "003001",
                "detail_refund_fl": "selection",
                "detail_refund": "004001",
                "detail_exchange_fl": "selection",
                "detail_exchange": "005001",
                "seo_tag_fl": "y",
                "set_tag_title": product_name,
                "set_tag_description": product_name,
                "set_tag_keyword": _safe_str(row.get("키워드", "")),
                "fb_vn": "n",
                "google_use_flag": "y",
            }
        )

    result_df = pd.DataFrame(rows)
    stats = {
        "converted_count": len(result_df),
        "skipped_option_count": 0,
        "skipped_option_products": [],
    }
    return result_df, stats


def save_godomall(godomall_df: pd.DataFrame, template_path: str, output_file: str) -> dict:
    """Fill a Godomall template workbook with generated rows."""
    template = Path(template_path)
    if not template.exists():
        template = Path(resolve_godomall_template_path())
    if not template.exists():
        raise FileNotFoundError(f"고도몰 템플릿을 찾을 수 없습니다: {template_path}")

    output = Path(output_file)
    output.parent.mkdir(parents=True, exist_ok=True)

    wb = load_workbook(template)
    ws = wb[wb.sheetnames[0]]

    if ws.max_row >= 4:
        ws.delete_rows(4, ws.max_row - 3)

    headers = [ws.cell(row=1, column=col_idx).value for col_idx in range(1, ws.max_column + 1)]
    field_codes = [ws.cell(row=2, column=col_idx).value for col_idx in range(1, ws.max_column + 1)]
    key_to_col = {}
    for idx, code in enumerate(field_codes, start=1):
        if code:
            key_to_col[str(code)] = idx
    for idx, header in enumerate(headers, start=1):
        if header:
            key_to_col[str(header).strip()] = idx

    for row_idx, (_, data_row) in enumerate(godomall_df.iterrows(), start=4):
        for field_code, value in data_row.items():
            col_idx = key_to_col.get(field_code)
            if not col_idx:
                continue
            if pd.isna(value):
                value = ""
            ws.cell(row=row_idx, column=col_idx, value=value)

    output_suffix = output.suffix.lower()
    if output_suffix == ".xls":
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_xlsx = Path(tmp.name)
        try:
            wb.save(temp_xlsx)
            try:
                import win32com.client  # type: ignore
            except ImportError as exc:
                raise RuntimeError("Excel 97-2003 저장을 위해 win32com.client가 필요합니다.") from exc

            excel = win32com.client.DispatchEx("Excel.Application")
            excel.Visible = False
            excel.DisplayAlerts = False
            workbook = excel.Workbooks.Open(str(temp_xlsx))
            try:
                workbook.SaveAs(str(output), FileFormat=56)
            finally:
                workbook.Close(SaveChanges=False)
                excel.Quit()
        finally:
            if temp_xlsx.exists():
                temp_xlsx.unlink()
    else:
        wb.save(output)
    return {"path": str(output), "count": len(godomall_df)}
