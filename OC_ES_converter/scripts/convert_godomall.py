#!/usr/bin/env python3
"""Ownerclan -> Godomall converter helpers."""

from pathlib import Path
import sys
import tempfile

import pandas as pd
from openpyxl import load_workbook

from seo_alt_injector import inject_alt_into_html
from convert_base import convert_origin

# price_engine 경로 (OC_ES_converter/scripts/ → 루트 2단계 위)
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

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
    keywords = _parse_keyword_list(keyword_text)
    return "|".join(keywords)


def _parse_keyword_list(keyword_text: str) -> list[str]:
    """키워드 텍스트/JSON array → 깨끗한 문자열 리스트."""
    import json as _json
    raw = _safe_str(keyword_text)
    if not raw:
        return []
    # JSON array인 경우 파싱
    if raw.startswith("["):
        try:
            parsed = _json.loads(raw)
            if isinstance(parsed, list):
                return [str(k).strip() for k in parsed if str(k).strip()]
        except (ValueError, TypeError):
            pass
    # 쉼표 구분 텍스트
    return [token.strip() for token in raw.split(",") if token.strip()]


# 브랜드 코드 ↔ 할인율 매핑 (고도몰 쿠폰 시스템)
BRAND_DISCOUNT_MAP = {
    "001": 35,  # A등급
    "002": 28,  # B등급
    "003": 18,  # C등급
    "004": 10,  # D등급
}


GODOMALL_DETAIL_NOTICE = (
    '<p style="font-size: 12px; color: #777777; display: block; margin: 20px 0;">'
    "본 제품을 구매하시면 원활한 배송을 위해 꼭 필요한 고객님의 개인정보를 "
    "(성함, 주소, 전화번호 등) 택배사 및 제 3업체에서 이용하는 것에 동의하시는 것으로 간주됩니다.<br/>"
    " 개인정보는 배송 외의 용도로는 절대 사용되지 않으니 안심하시기 바랍니다. 안전하게 배송해 드리겠습니다."
    "</p>"
)


def _wrap_detail_html(detail_html: str) -> str:
    """상세설명 앞뒤에 공통 콘텐츠 추가."""
    if not detail_html:
        return ""
    # 이미 notice가 포함되어 있으면 스킵
    if "원활한 배송을 위해" in detail_html:
        return detail_html
    if detail_html.endswith("</center>"):
        return detail_html[:-9] + "<br>" + GODOMALL_DETAIL_NOTICE + "</center>"
    return f"<center>{detail_html}<br>{GODOMALL_DETAIL_NOTICE}</center>"


def _select_brand_code(discount_pct: float) -> str:
    """할인율에 가장 가까운 브랜드 코드 반환."""
    best_code = "004"
    best_diff = float("inf")
    for code, pct in BRAND_DISCOUNT_MAP.items():
        diff = abs(pct - discount_pct)
        if diff < best_diff:
            best_diff = diff
            best_code = code
    return best_code


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


def build_option_block(row: pd.Series, market_price: int = 0) -> dict:
    """옵션 블록 생성. 스마트스토어 옵션가격 보정 정책 적용.

    - 단품(옵션 없음): 옵션 1개 생성, 추가금 0원
    - 옵션 있음: 최저가 옵션을 0원(기본)으로, 나머지는 추가금(차액)
    - OptionPriceCorrector로 추가금 상한/하한 보정
    """
    combo_text = row.get("조합형옵션", "")
    options = parse_ownerclan_combo_options(combo_text)

    if not options:
        # 단품 → 옵션 1개 (기본, +0원)
        return {
            "option_yn": "y",
            "option_display": "s",
            "option_name": "옵션선택",
            "option_value": "기본",
            "option_image": "",
            "option_cost_price": "0",
            "option_price": "0",
            "stock_cnt": "999",
            "option_view_fl": "y",
            "option_sell_fl": "y",
            "option_delivery_fl": "y",
        }

    # 옵션 가격 → 추가금 변환 (최저가 = 0원 기준)
    prices = []
    for _, price_str, _ in options:
        try:
            prices.append(int(float(str(price_str).replace(",", ""))))
        except (ValueError, TypeError):
            prices.append(0)

    min_price = min(prices) if prices else 0
    # 추가금 = 옵션가격 - 최저가
    deltas = [p - min_price for p in prices]

    # OptionPriceCorrector로 추가금 보정 (스마트스토어 정책)
    if market_price > 0 and any(d > 0 for d in deltas):
        try:
            _ROOT_path = Path(__file__).resolve().parent.parent.parent
            import sys as _sys
            if str(_ROOT_path) not in _sys.path:
                _sys.path.insert(0, str(_ROOT_path))
            from Upload_Mapper.rules.option_price_correction import OptionPriceCorrector
            has_zero = any(d == 0 for d in deltas)
            max_delta = OptionPriceCorrector.calculate_max_delta(market_price)
            deltas = OptionPriceCorrector.redistribute_deltas(
                deltas, max_delta, has_zero, market_price
            )
        except ImportError:
            pass  # 모듈 없으면 원본 deltas 사용

    option_values = [value for value, _, _ in options]
    option_rows = len(options)

    return {
        "option_yn": "y",
        "option_display": "s",
        "option_name": GODOMALL_OPTION_LABEL,
        "option_value": "\n".join(option_values),
        "option_image": "",
        "option_cost_price": "\n".join(["0"] * option_rows),
        "option_price": "\n".join(str(d) for d in deltas),
        "stock_cnt": "\n".join(["999"] * option_rows),
        "option_view_fl": "\n".join(["y"] * option_rows),
        "option_sell_fl": "\n".join(["y"] * option_rows),
        "option_delivery_fl": "\n".join(["y"] * option_rows),
    }


def _calc_price_with_strategy(cost_a: float, strategy_id: str) -> tuple[int, float]:
    """price_engine으로 고도몰 등록가와 할인율을 역산.

    Args:
        cost_a:      오너클랜 원가 (oc_price).
        strategy_id: "lowest_price" | "normal_sale" | "cpc_ad"

    Returns:
        (market_price, discount_pct): 마켓등록가(10원단위), 할인율J(%)
    """
    from DB_save.price_engine import solve_auto_c, _resolve_target_s
    from DB_save.pricing_strategies import get_pricing_strategy

    if cost_a <= 0:
        return int(cost_a), 0.0

    strategy = get_pricing_strategy("고도몰", strategy_id)

    # discount_bands → J (할인율) 결정
    discount_bands = strategy.get("discount_bands", [[999999999, 10]])
    J = float(_resolve_target_s(cost_a, discount_bands))

    policy = {
        "commission_rate": 4.0,
        "commission_base": "post_discount",
        "discount_rate": J,
        "coupon_amount": 0,
        "reward_rate": 0,
    }

    sol, _ = solve_auto_c(
        cost_a=cost_a,
        margin_c=strategy["margin_c"],
        policy=policy,
        target=_resolve_target_s(cost_a, strategy["bands"]),
        shipping_absorbed=0,
        min_h=strategy["min_h"],
        max_h=strategy["max_h"],
        round_h=strategy["round_h"],
        round_mode=strategy.get("round_mode", "nearest"),
        metric=strategy.get("metric", "absolute_s"),
    )

    fwd = sol.get("forward") or {}
    market_price = fwd.get("market_price") or fwd.get("I") or int(cost_a)
    return int(market_price), J


def convert_ownerclan_to_godomall(
    oc_df: pd.DataFrame,
    category_code: str = GODOMALL_DEFAULTS["category_code"],
    brand_code: str = GODOMALL_DEFAULTS["brand_code"],
    delivery_sno: str = GODOMALL_DEFAULTS["deliverySno"],
    strategy_id: str | None = None,
):
    """Convert Ownerclan rows into Godomall sheet rows.

    Args:
        oc_df:        오너클랜 형식 DataFrame.
        category_code: 고도몰 카테고리 코드.
        brand_code:   고도몰 브랜드 코드.
        delivery_sno: 고도몰 배송 정책 번호.
        strategy_id:  가격 전략 ID ("lowest_price" / "normal_sale" / "cpc_ad").
                      None이면 오너클랜 원가를 판매가로 그대로 사용.
    """
    rows = []

    for _, row in oc_df.iterrows():
        ownerclan_price = _safe_price(row.get("오너클랜판매가", ""))
        product_name = _safe_str(row.get("마켓상품명", ""))
        image_url = _safe_str(row.get("이미지대", ""))
        detail_html = _safe_str(row.get("본문상세설명", ""))
        detail_html = inject_alt_into_html(detail_html, _safe_str(row.get("상품코드", "")))
        # 상세설명 앞뒤 공통 콘텐츠 추가
        detail_html = _wrap_detail_html(detail_html)

        # ── 가격 계산 + 브랜드 코드 결정 ─────────────────────────────────
        if strategy_id and ownerclan_price > 0:
            goods_price, discount_pct = _calc_price_with_strategy(
                ownerclan_price, strategy_id
            )
            # 할인은 고도몰 브랜드 쿠폰으로 처리 → 브랜드 코드로 매핑
            selected_brand = _select_brand_code(discount_pct)
        else:
            goods_price = ownerclan_price
            discount_pct = 0.0
            selected_brand = "004"  # 기본 D등급(10%)

        option_block = build_option_block(row, market_price=goods_price)

        rows.append(
            {
                "상품 상태": "n",
                "goods_no": "",
                "goods_name": product_name,
                "goods_cd": _safe_str(row.get("상품코드", "")),
                "category_code": str(category_code),
                "purchase_goods_name": product_name,
                "brand_code": selected_brand,
                "pay_limit_fl": "n",
                "model_no": "edit" + _safe_str(row.get("상품코드", "")),
                "maker_name": _safe_str(row.get("제작/수입사", "")),
                "origin_name": convert_origin(row.get("원산지", "")),
                "search_word": ",".join(_parse_keyword_list(row.get("키워드", ""))),
                "deliverySno": str(delivery_sno),
                "goods_price": goods_price,
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
                "goods_discount": 0,
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
                "set_tag_keyword": ",".join(_parse_keyword_list(row.get("키워드", ""))),
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
