"""
다팔자 솔루션 구현
"""

import pandas as pd
from typing import Dict
import json as json_lib
import sys
from pathlib import Path

# 상대 import를 위한 경로 설정
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from solutions.base_solution import BaseSolution
from rules.shipping_fee import ShippingFeeCalculator
from rules.option_price_correction import OptionPriceCorrector, log_option_correction

class DafalzaSolution(BaseSolution):
    """다팔자 상품 등록 솔루션"""
    
    @property
    def name(self) -> str:
        return "다팔자"
    
    @property
    def columns(self) -> list:
        return [
            "상품코드", "상태", "작업", "오류 메시지", "마켓 상품코드", "소비자준수가", "노트",
            "관리코드", "상품명", "가격", "오너클랜 판매가격", "대표 이미지", "키워드",
            "브랜드", "제조사", "원산지", "면세", "카테고리", "성인전용 상품",
            "배송비", "배송타입", "반품배송비", "교환배송비", "판매시작일", "판매종료일",
            "판매자 부담 할인", "옵션", "상세정보"
        ]
    
    @property
    def description(self) -> str:
        return "다팔자 상품 등록 솔루션"
    
    @property
    def temp_path_template(self) -> str:
        return r"C:\Users\{username}\AppData\Roaming\dafalza\temp"
    
    def get_default_mapping(self) -> Dict[str, str]:
        """다팔자 기본 매핑 규칙"""
        return {
            "상품코드": "상품코드",
            "ST4_최종결과": "상품명",
            "사용URL": "대표 이미지",
            "search_keywords": "키워드"
        }
    
    def apply_solution_specific_rules(self, result_df: pd.DataFrame, 
                                     processed_df: pd.DataFrame, config: Dict,
                                     original_solution_df: pd.DataFrame = None) -> pd.DataFrame:
        """다팔자 특화 규칙 적용"""
        
        # 상품코드 기준 딕셔너리 생성
        processed_dict = {}
        if "상품코드" in processed_df.columns and "상품코드" in result_df.columns:
            processed_dict = processed_df.set_index("상품코드").to_dict("index")
        
        # base_dir 설정 (템플릿 로드용)
        parent_dir = Path(__file__).parent.parent
        
        # 상세설명 하단 문구 가져오기 (상단은 center 태그로 자동 처리)
        detail_bottom = config.get("detail_bottom_text", "")
        
        # 템플릿 사용 시 템플릿에서 가져오기
        if not detail_bottom and config.get("detail_bottom_template"):
            from config_manager import MapperConfig
            mapper_config = MapperConfig(parent_dir)
            detail_bottom = mapper_config.get_template("bottom", config["detail_bottom_template"])
        
        # 1. 기본 매핑 적용 (부모 클래스 메서드 호출)
        column_mapping = self.get_default_mapping()
        result_df = self.apply_mapping(result_df, processed_df, column_mapping, config)
        
        # 2. 특수 변환 규칙
        # 상품명 → ST4_최종결과
        if "ST4_최종결과" in processed_df.columns and "상품명" in result_df.columns:
            for idx, row in result_df.iterrows():
                product_code = row.get("상품코드", "")
                if product_code in processed_dict:
                    st4_value = processed_dict[product_code].get("ST4_최종결과", "")
                    if pd.notna(st4_value) and str(st4_value).strip():
                        result_df.at[idx, "상품명"] = str(st4_value).strip()
        
        # 대표 이미지/목록 이미지 → 대표 이미지 (마켓별 처리)
        detected_market = config.get("detected_market")
        
        # 이미지 컬럼 매핑 처리
        image_source_col = None
        if detected_market == "11번가":
            # 11번가: 대표 이미지 사용
            if "대표 이미지" in processed_df.columns:
                image_source_col = "대표 이미지"
        elif detected_market == "쿠팡":
            # 쿠팡: 목록 이미지 사용
            if "목록 이미지" in processed_df.columns:
                image_source_col = "목록 이미지"
        elif detected_market in ["옥션", "지마켓"]:
            # 옥션, 지마켓: 사용URL 또는 목록이미지 (띄어쓰기 없음) 또는 목록 이미지
            if "사용URL" in processed_df.columns:
                image_source_col = "사용URL"
            elif "목록이미지" in processed_df.columns:
                image_source_col = "목록이미지"
            elif "목록 이미지" in processed_df.columns:
                image_source_col = "목록 이미지"
        elif detected_market == "스스":
            # 스스: 사용URL을 대표 이미지로 매핑 (상세정보에는 원본 대표 이미지 사용)
            if "사용URL" in processed_df.columns:
                image_source_col = "사용URL"
        else:
            # 기본: 사용URL 또는 대표 이미지
            if "사용URL" in processed_df.columns:
                image_source_col = "사용URL"
            elif "대표 이미지" in processed_df.columns:
                image_source_col = "대표 이미지"
        
        # 등록 솔루션 엑셀의 이미지 컬럼 확인
        image_target_col = None
        if "목록 이미지" in result_df.columns:
            image_target_col = "목록 이미지"
        elif "대표 이미지" in result_df.columns:
            image_target_col = "대표 이미지"
        
        # 이미지 매핑 적용
        if image_source_col and image_target_col:
            for idx, row in result_df.iterrows():
                product_code = row.get("상품코드", "")
                if product_code in processed_dict:
                    url_value = processed_dict[product_code].get(image_source_col, "")
                    if pd.notna(url_value) and str(url_value).strip():
                        result_df.at[idx, image_target_col] = str(url_value).strip()
        
        # 키워드 → search_keywords (합치기: 가공키워드 + 등록솔루션 키워드, 중복 제거)
        if "search_keywords" in processed_df.columns and "키워드" in result_df.columns:
            for idx, row in result_df.iterrows():
                product_code = row.get("상품코드", "")
                
                # 가공된 엑셀의 키워드 가져오기
                processed_keywords = ""
                if product_code in processed_dict:
                    keywords_value = processed_dict[product_code].get("search_keywords", "")
                    if pd.notna(keywords_value) and str(keywords_value).strip():
                        processed_keywords = str(keywords_value).strip()
                
                # 등록 솔루션 엑셀의 기존 키워드 가져오기
                existing_keywords = row.get("키워드", "")
                if pd.notna(existing_keywords):
                    existing_keywords = str(existing_keywords).strip()
                else:
                    existing_keywords = ""
                
                # 키워드 합치기 및 중복 제거
                if processed_keywords or existing_keywords:
                    # 쉼표로 분리하여 리스트로 변환 (띄어쓰기 제거 후)
                    def normalize_keyword(k):
                        """키워드 정규화: 앞뒤 공백 제거 후 내부 띄어쓰기 제거"""
                        return k.strip().replace(" ", "")
                    
                    processed_list = [normalize_keyword(k) for k in processed_keywords.split(",") if normalize_keyword(k)] if processed_keywords else []
                    existing_list = [normalize_keyword(k) for k in existing_keywords.split(",") if normalize_keyword(k)] if existing_keywords else []
                    
                    # 순서: 가공키워드 먼저, 그 다음 등록솔루션 키워드
                    combined_list = processed_list + existing_list
                    
                    # 중복 제거 (순서 유지)
                    seen = set()
                    unique_list = []
                    for keyword in combined_list:
                        if keyword not in seen:
                            seen.add(keyword)
                            unique_list.append(keyword)
                    
                    # 쉼표로 다시 합치기
                    result_keywords = ",".join(unique_list)
                    result_df.at[idx, "키워드"] = result_keywords
        
        # 3. 가격은 사용자가 '마켓판매가격'을 '가격'에 매핑하면 apply_mapping에서 자동 처리됨
        # (별도 계산 로직 없음)
        
        # 4. 솔루션 엑셀 자체 변경
        # 브랜드 → 빈셀
        if "브랜드" in result_df.columns:
            result_df["브랜드"] = ""
        
        # 제조사 → "onerclan OEM"
        if "제조사" in result_df.columns:
            result_df["제조사"] = "onerclan OEM"
        
        # 배송비 계산 (공통 규칙: 형식 1 또는 형식 2)
        # 마켓별로 배송비 처리 방식이 다름
        # 옥션, 지마켓: 등록 솔루션에 배송비, 교환배송비 컬럼이 없음
        if detected_market in ["옥션", "지마켓"]:
            # 옥션, 지마켓: 반품배송비만 처리
            if "반품배송비" in result_df.columns:
                for idx, row in result_df.iterrows():
                    product_code = row.get("상품코드", "")
                    if product_code in processed_dict:
                        return_fee_value = processed_dict[product_code].get("반품배송비", 0)
                        if pd.notna(return_fee_value):
                            try:
                                return_fee_num = float(str(return_fee_value).replace(",", ""))
                                result_df.at[idx, "반품배송비"] = return_fee_num
                            except (ValueError, TypeError):
                                pass
        else:
            # 그 외 마켓: 배송비, 반품배송비, 교환배송비 모두 처리
            if "배송비" in result_df.columns:
                for idx, row in result_df.iterrows():
                    # 등록 솔루션 엑셀의 기존 배송비 값 가져오기
                    original_shipping = row.get("배송비", 0)
                    # 원본 반품배송비 값 가져오기 (배송비가 0일 때 사용)
                    original_return_fee = row.get("반품배송비", 0)
                    
                    try:
                        original_shipping_num = float(str(original_shipping).replace(",", "")) if pd.notna(original_shipping) else 0
                        original_return_fee_num = float(str(original_return_fee).replace(",", "")) if pd.notna(original_return_fee) else 0
                        
                        # 배송비 변환 (형식 1 또는 형식 2)
                        shipping_fee = ShippingFeeCalculator.calculate(original_shipping_num, config)
                        result_df.at[idx, "배송비"] = shipping_fee
                        
                        # 반품배송비 계산
                        if "반품배송비" in result_df.columns:
                            # 배송비가 0인 경우: 원본 반품배송비 + 1000
                            if shipping_fee == 0:
                                return_fee = original_return_fee_num + 1000
                            else:
                                return_fee = ShippingFeeCalculator.calculate_return_fee(
                                    shipping_fee, config, original_shipping_num
                                )
                            result_df.at[idx, "반품배송비"] = return_fee
                            
                            # 교환배송비 계산
                            if "교환배송비" in result_df.columns:
                                # 배송비가 0인 경우: (원본 반품배송비 + 1000) * 2
                                if shipping_fee == 0:
                                    exchange_fee = (original_return_fee_num + 1000) * 2
                                else:
                                    exchange_fee = ShippingFeeCalculator.calculate_exchange_fee(return_fee, config)
                                result_df.at[idx, "교환배송비"] = exchange_fee
                    except Exception as e:
                        # 오류 시 기본값
                        result_df.at[idx, "배송비"] = 0
                        if "반품배송비" in result_df.columns:
                            result_df.at[idx, "반품배송비"] = original_return_fee_num + 1000 if pd.notna(original_return_fee) else 1000
                        if "교환배송비" in result_df.columns:
                            result_df.at[idx, "교환배송비"] = (original_return_fee_num + 1000) * 2 if pd.notna(original_return_fee) else 2000
        
        # 5. 옵션추가금 자동 보정
        # 옵션금액 규칙 확인 (config에서 읽기)
        option_price_rule = config.get("option_price_rule", "smartstore")
        
        # 옵션금액 규칙이 "none"이 아니고, 필요한 컬럼이 있을 때만 보정 수행
        if option_price_rule != "none" and "옵션" in result_df.columns and "마켓판매가격" in processed_df.columns:
            for idx, row in result_df.iterrows():
                product_code = row.get("상품코드", "")
                option_text = row.get("옵션", "")
                
                # 옵션이 있는 행만 처리
                if pd.notna(option_text) and str(option_text).strip():
                    # 가공된 엑셀에서 마켓판매가격 가져오기
                    market_price = 0
                    if product_code in processed_dict:
                        price_value = processed_dict[product_code].get("마켓판매가격", 0)
                        if pd.notna(price_value):
                            try:
                                market_price = float(price_value)
                            except (ValueError, TypeError):
                                market_price = 0
                    
                    # 옵션 보정 수행
                    if market_price > 0:
                        corrected_option, change_info = OptionPriceCorrector.correct_option_text(
                            option_text, market_price
                        )
                        
                        # 로그 기록
                        log_option_correction(product_code, option_text, market_price, 
                                             corrected_option, change_info)
                        
                        # 보정된 옵션 적용
                        if change_info.get("changed", False):
                            result_df.at[idx, "옵션"] = corrected_option
        
        # 상세정보 → 상단 추가 + 원본 상세정보 + 하단 추가 (HTML 형식)
        if "상세정보" in result_df.columns:
            import random
            
            # 하단 이미지 URL 목록
            exchange_image_urls = [
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_0.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_1.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_2.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_3.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_4.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_5.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_6.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_7.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_8.jpg',
                'https://ai.esmplus.com/kohaz94/detailfolder/exchange_9.jpg'
            ]
            
            # 설정에서 하단 이미지 URL 가져오기 (날짜별/마켓별 변경 가능)
            bottom_image_urls = config.get("detail_bottom_image_urls", exchange_image_urls)
            if not bottom_image_urls:
                bottom_image_urls = exchange_image_urls
            
            # 엑셀 전체에 랜덤으로 1개 선택 (행마다 랜덤 아님)
            selected_bottom_image_url = random.choice(bottom_image_urls)
            
            # 상단 문구 설정 가져오기
            top_product_name_text = config.get("detail_top_product_name_text", "[상품명: {상품명}]")
            top_notice_text = config.get("detail_top_notice_text", "[필독] 제품명 및 상세설명에 기재된 '본품'만 발송됩니다.")
            
            # 이미지 사이즈 설정 가져오기
            image_width = config.get("detail_top_image_width", 500)
            image_height = config.get("detail_top_image_height", 500)
            
            # 상품명 스타일 설정 가져오기
            product_name_color = config.get("detail_top_product_name_color", "blue")
            product_name_font_size = config.get("detail_top_product_name_font_size", 10)
            
            # 필독 문구 스타일 설정 가져오기
            notice_bg_color = config.get("detail_top_notice_bg_color", "yellow")
            notice_padding = config.get("detail_top_notice_padding", "2px 5px")
            
            for idx, row in result_df.iterrows():
                # 등록 솔루션 엑셀의 원본 상세정보 가져오기
                original_detail = row.get("상세정보", "")
                if pd.notna(original_detail):
                    original_detail = str(original_detail).strip()
                else:
                    original_detail = ""
                
                # 등록 솔루션 엑셀의 상품명 가져오기 (원본상품명)
                # 원본 solution_df에서 가져오기 (매핑 전 원본 값)
                original_product_name = ""
                if "상품코드" in result_df.columns:
                    product_code = row.get("상품코드", "")
                    if pd.notna(product_code):
                        # 원본 solution_df에서 가져오기 (매핑 전 원본 상품명)
                        if original_solution_df is not None and "상품코드" in original_solution_df.columns and "상품명" in original_solution_df.columns:
                            original_row = original_solution_df[original_solution_df["상품코드"] == product_code]
                            if not original_row.empty:
                                original_product_name = original_row.iloc[0]["상품명"]
                                if pd.notna(original_product_name):
                                    original_product_name = str(original_product_name).strip()
                        # 원본 solution_df가 없으면 result_df에서 가져오기 (매핑 전 원본 값일 수도 있음)
                        elif "상품명" in result_df.columns:
                            original_product_name = result_df.loc[idx, "상품명"]
                            if pd.notna(original_product_name):
                                original_product_name = str(original_product_name).strip()                
                # 등록 솔루션 엑셀의 원본 대표 이미지 URL 가져오기 (매핑 후 값 사용)
                # 마켓별로 매핑된 이미지를 사용 (대표 이미지 또는 목록 이미지)
                # 컬럼명이 다를 수 있으므로 안전하게 처리
                # 상세정보 상단 이미지는 항상 원본 등록 솔루션 엑셀의 이미지를 사용
                # 가공된 엑셀의 이미지는 매핑 컬럼에만 반영되고, 상세정보에는 사용하지 않음
                main_image_url = ""
                if "상품코드" in result_df.columns:
                    product_code = row.get("상품코드", "")
                    if pd.notna(product_code) and original_solution_df is not None and "상품코드" in original_solution_df.columns:
                        try:
                            original_row = original_solution_df[original_solution_df["상품코드"] == product_code]
                            if not original_row.empty:
                                # 원본 등록 솔루션 엑셀에서 이미지 가져오기 (마켓별 우선순위)
                                possible_image_cols = []
                                if detected_market == "11번가":
                                    # 11번가: 대표 이미지 우선
                                    possible_image_cols = ["대표 이미지", "목록 이미지", "사용URL", "이미지", "대표이미지", "목록이미지"]
                                elif detected_market == "쿠팡":
                                    # 쿠팡: 목록 이미지 우선
                                    possible_image_cols = ["목록 이미지", "대표 이미지", "사용URL", "이미지", "대표이미지", "목록이미지"]
                                elif detected_market in ["옥션", "지마켓"]:
                                    # 옥션, 지마켓: 목록이미지(띄어쓰기 없음) 또는 목록 이미지 우선
                                    possible_image_cols = ["목록이미지", "목록 이미지", "대표 이미지", "사용URL", "이미지", "대표이미지"]
                                else:
                                    # 스스 등 기본: 대표 이미지 우선
                                    possible_image_cols = ["대표 이미지", "목록 이미지", "사용URL", "이미지", "대표이미지", "목록이미지"]
                                
                                for col_name in possible_image_cols:
                                    if col_name in original_solution_df.columns:
                                        img_value = original_row.iloc[0][col_name]
                                        if pd.notna(img_value) and str(img_value).strip():
                                            main_image_url = str(img_value).strip()
                                            break
                        except Exception:
                            pass
                
                # 상단 HTML 구성 (<center> 시작)
                top_html_parts = []
                
                # 대표 이미지 (설정된 사이즈, 중앙 정렬)
                if main_image_url:
                    top_html_parts.append(
                        f'<img src="{main_image_url}" style="width: {image_width}px; height: {image_height}px; object-fit: contain;" /><br>'
                    )
                
                # 상품명 표시 텍스트 (설정된 컬러, 폰트 사이즈)
                if original_product_name:
                    product_name_display = top_product_name_text.replace("{상품명}", original_product_name)
                    top_html_parts.append(
                        f'<span style="color: {product_name_color}; font-size: {product_name_font_size}px;">{product_name_display}</span><br>'
                    )
                
                # 필독 문구 (설정된 배경 컬러, 패딩)
                if top_notice_text:
                    top_html_parts.append(
                        f'<span style="background-color: {notice_bg_color}; padding: {notice_padding};">{top_notice_text}</span>'
                    )
                
                top_html = "<center>" + "".join(top_html_parts) + "</center>" if top_html_parts else ""
                
                # 하단 이미지 (엑셀 전체에 랜덤으로 1개)
                bottom_html = f'<center><img src="{selected_bottom_image_url}" /></center>'
                
                # 기존 하단 문구 추가
                if detail_bottom:
                    bottom_html = f"{bottom_html}<br><center>{detail_bottom}</center>"
                
                # 전체 조합: 상단 + 원본 상세정보 + 하단
                if top_html:
                    if original_detail:
                        detail_html = f"{top_html}<br>{original_detail}<br>{bottom_html}"
                    else:
                        detail_html = f"{top_html}<br>{bottom_html}"
                else:
                    if original_detail:
                        detail_html = f"{original_detail}<br>{bottom_html}"
                    else:
                        detail_html = bottom_html
                
                result_df.at[idx, "상세정보"] = detail_html
        
        return result_df

