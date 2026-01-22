"""
이셀러스 솔루션 구현
"""

import pandas as pd
from typing import Dict, Optional
import sys
from pathlib import Path
import os
import random

# 상대 import를 위한 경로 설정
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from solutions.base_solution import BaseSolution
from rules.option_price_correction import OptionPriceCorrector, log_option_correction

class EsellersSolution(BaseSolution):
    """이셀러스 상품 등록 솔루션"""
    
    @property
    def name(self) -> str:
        return "이셀러스"
    
    @property
    def columns(self) -> list:
        """기본정보 시트 컬럼 목록"""
        return [
            "원본번호*", "판매자 관리코드", "폴더명", "카테고리 번호*", "상품명*", "판매가*", "수량*",
            "최대구매수량", "최소구매수량", "원산지*", "G마켓,옥션 원산지 유형", "수입사",
            "목록 이미지*", "이미지1(대표/기본이미지)*", "이미지2", "이미지3", "이미지4", "이미지5",
            "상세설명*", "ESM 추가구성 상세설명", "ESM 광고홍보 상세설명",
            "선택사항 타입", "선택사항 옵션명", "선택사항 상세정보", "선택사항 재고 사용여부", "작성형 선택사항",
            "추가구성 옵션명", "추가구성 상세정보",
            "브랜드", "모델명", "제조사", "과세여부", "나이제한", "제조일자", "유효일자",
            "홍보문구", "상품상태", "원가", "공급가", "도서정가", "ISBN", "문화비 소득공제",
            "검색어(태그)", "인증정보",
            "요약정보 상품군 코드*", "요약정보 전항목 상세설명 참조",
            "값1", "값2", "값3", "값4", "값5", "값6", "값7", "값8", "값9", "값10",
            "값11", "값12", "값13", "값14", "값15", "값16", "값17", "값18", "값19", "값20",
            "값21", "값22", "값23", "값24", "값25", "값26", "값27", "값28", "값29",
            "기본정보 오류메시지"
        ]
    
    @property
    def description(self) -> str:
        return "이셀러스 상품 등록 솔루션"
    
    @property
    def temp_path_template(self) -> Optional[str]:
        """이셀러스 임시 파일 경로 (필요시 설정)"""
        return None
    
    def get_default_mapping(self) -> Dict[str, str]:
        """이셀러스 기본 매핑 규칙"""
        return {
            "상품코드": "판매자 관리코드",  # 가공엑셀의 상품코드 → 이셀러스의 판매자 관리코드
            "ST4_최종결과": "상품명*",
            "마켓판매가격": "판매가*",
            "사용URL": "목록 이미지*",  # 이미지1은 apply_solution_specific_rules에서 처리
            "search_keywords": "검색어(태그)"
        }
    
    def apply_mapping(self, result_df: pd.DataFrame, processed_df: pd.DataFrame, 
                     column_mapping: Dict[str, str], config: Dict) -> pd.DataFrame:
        """매핑 규칙 적용 (이셀러스는 상품코드 기준) - 성능 최적화 버전"""
        # 이셀러스는 상품코드 기준 매핑 (판매자 관리코드와 매칭)
        if "상품코드" in processed_df.columns and "판매자 관리코드" in result_df.columns:
            # 판매자 관리코드를 문자열로 변환 (인덱스로 사용)
            result_df = result_df.copy()
            result_df['판매자 관리코드_문자열'] = result_df['판매자 관리코드'].astype(str)
            
            # processed_df를 상품코드 기준 딕셔너리로 변환 (벡터화를 위해)
            processed_dict = processed_df.set_index("상품코드").to_dict("index")
            
            for proc_col, sol_col in column_mapping.items():
                if proc_col in processed_df.columns and sol_col in result_df.columns:
                    # 벡터화된 매핑 딕셔너리 생성
                    mapping_dict = {}
                    invalid_values = {'#N/A', '#NA', 'N/A', 'NA', 'NULL', 'NAN', 'NAN.0'}
                    
                    for code, data in processed_dict.items():
                        proc_value = data.get(proc_col, "")
                        if pd.notna(proc_value):
                            proc_value_str = str(proc_value).strip()
                            # #N/A 관련 문자열 필터링
                            if proc_value_str and proc_value_str.upper() not in invalid_values:
                                mapping_dict[str(code)] = proc_value_str
                    
                    # 벡터화된 매핑 적용 (map 사용 - iterrows보다 훨씬 빠름)
                    result_df[sol_col] = result_df['판매자 관리코드_문자열'].map(mapping_dict).fillna(result_df[sol_col])
            
            # 임시 컬럼 제거
            if '판매자 관리코드_문자열' in result_df.columns:
                result_df = result_df.drop(columns=['판매자 관리코드_문자열'])
        
        return result_df
    
    def apply_solution_specific_rules(self, result_df: pd.DataFrame, 
                                     processed_df: pd.DataFrame, config: Dict,
                                     original_solution_df: pd.DataFrame = None) -> pd.DataFrame:
        """이셀러스 특화 규칙 적용"""
        
        # 상품코드 기준 딕셔너리 생성 (판매자 관리코드와 매칭)
        processed_dict = {}
        if "상품코드" in processed_df.columns and "판매자 관리코드" in result_df.columns:
            processed_dict = processed_df.set_index("상품코드").to_dict("index")
        
        # base_dir 설정 (템플릿 로드용)
        parent_dir = Path(__file__).parent.parent
        
        # 상세설명 하단 문구 가져오기
        detail_bottom = config.get("detail_bottom_text", "")
        
        # 템플릿 사용 시 템플릿에서 가져오기
        if not detail_bottom and config.get("detail_bottom_template"):
            from config_manager import MapperConfig
            mapper_config = MapperConfig(parent_dir)
            detail_bottom = mapper_config.get_template("bottom", config["detail_bottom_template"])
        
        # 1. 기본 매핑 적용
        column_mapping = self.get_default_mapping()
        result_df = self.apply_mapping(result_df, processed_df, column_mapping, config)
        
        # 2. 폴더명 = 엑셀 파일명 (40byte 이내)
        # 스마트스토어(스스)인 경우: 날짜_마켓코드_배배송비값 형식
        # 솔루션엑셀 파일명에 '_기본카테고리'가 있으면 접두사 '이셀카테_' 추가
        if "폴더명" in result_df.columns:
            file_path = config.get("processed_file_path", "")
            solution_file_path = config.get("solution_file_path", "")
            detected_market = config.get("detected_market", "")
            
            # 솔루션엑셀 파일명에 '_기본카테고리'가 있는지 확인
            has_basic_category = False
            if solution_file_path:
                solution_filename = os.path.basename(solution_file_path)
                if '_기본카테고리' in solution_filename:
                    has_basic_category = True
            
            if file_path:
                filename = os.path.splitext(os.path.basename(file_path))[0]
                
                # 스마트스토어(스스)인 경우 특별 처리
                if detected_market == "스스":
                    # 날짜_마켓코드 부분만 추출 (예: "20260115_스스A1-0")
                    # '이셀카테_' 접두사 사용 시 압축 모드 활성화
                    base_folder_name = self._extract_date_market_code(filename, compress_for_category=has_basic_category)
                    
                    # 각 행별로 처리 (할인값 + 배송비)
                    for idx, row in result_df.iterrows():
                        manager_code = row.get("판매자 관리코드", "")
                        folder_name_parts = [base_folder_name]
                        
                        if manager_code in processed_dict:
                            # 할인값 처리 ("판매자 부담 할인" 컬럼)
                            discount_value = None
                            if "판매자 부담 할인" in processed_df.columns:
                                discount_raw = processed_dict[manager_code].get("판매자 부담 할인", "")
                                if pd.notna(discount_raw) and str(discount_raw).strip():
                                    discount_value = self._convert_discount_to_folder_format(str(discount_raw).strip())
                            
                            # 배송비 처리
                            shipping_fee_str = None
                            if "배송비" in processed_df.columns:
                                shipping_fee = processed_dict[manager_code].get("배송비", "")
                                if pd.notna(shipping_fee) and str(shipping_fee).strip():
                                    shipping_fee_str = str(shipping_fee).strip().replace(",", "")
                            
                            # 폴더명 구성: 날짜_마켓코드_할할인값_배배송비값
                            if discount_value:
                                folder_name_parts.append(f"할{discount_value}")
                            if shipping_fee_str:
                                folder_name_parts.append(f"배{shipping_fee_str}")
                            
                            folder_name = "_".join(folder_name_parts)
                            
                            # 솔루션엑셀에 '_기본카테고리'가 있으면 접두사 추가
                            if has_basic_category:
                                folder_name = f"이셀카테_{folder_name}"
                            
                            # 40byte 이내로 제한
                            folder_name = self._ensure_folder_name_byte_limit(folder_name)
                            result_df.at[idx, "폴더명"] = folder_name
                        else:
                            folder_name = base_folder_name
                            # 솔루션엑셀에 '_기본카테고리'가 있으면 접두사 추가
                            if has_basic_category:
                                folder_name = f"이셀카테_{folder_name}"
                            # 40byte 이내로 제한
                            folder_name = self._ensure_folder_name_byte_limit(folder_name)
                            result_df.at[idx, "폴더명"] = folder_name
                elif detected_market in ["옥션", "지마켓"]:
                    # 옥션/지마켓: 날짜_마켓코드_할숫자_숫자_스숫자 형식
                    # 가공엑셀의 배송비 컬럼을 사용 (스마트스토어와 동일한 로직)
                    # '이셀카테_' 접두사 사용 시 압축 모드 활성화
                    base_folder_name = self._extract_date_market_code(filename, compress_for_category=has_basic_category)
                    
                    # 각 행별로 처리 (할인값 + 배송비)
                    for idx, row in result_df.iterrows():
                        manager_code = row.get("판매자 관리코드", "")
                        folder_name_parts = [base_folder_name]
                        
                        if manager_code in processed_dict:
                            # 할인값 처리 ("판매자 부담 할인" 컬럼)
                            discount_value = None
                            if "판매자 부담 할인" in processed_df.columns:
                                discount_raw = processed_dict[manager_code].get("판매자 부담 할인", "")
                                if pd.notna(discount_raw) and str(discount_raw).strip():
                                    discount_value = self._convert_discount_to_folder_format(str(discount_raw).strip())
                            
                            # 배송비 처리
                            shipping_fee_str = None
                            if "배송비" in processed_df.columns:
                                shipping_fee = processed_dict[manager_code].get("배송비", "")
                                if pd.notna(shipping_fee) and str(shipping_fee).strip():
                                    shipping_fee_str = str(shipping_fee).strip().replace(",", "")
                            
                            # 폴더명 구성: 날짜_마켓코드_할할인값_배배송비값
                            if discount_value:
                                folder_name_parts.append(f"할{discount_value}")
                            if shipping_fee_str:
                                folder_name_parts.append(f"배{shipping_fee_str}")
                            
                            folder_name = "_".join(folder_name_parts)
                            
                            # 솔루션엑셀에 '_기본카테고리'가 있으면 접두사 추가
                            if has_basic_category:
                                folder_name = f"이셀카테_{folder_name}"
                            
                            # 40byte 이내로 제한
                            folder_name = self._ensure_folder_name_byte_limit(folder_name)
                            result_df.at[idx, "폴더명"] = folder_name
                        else:
                            folder_name = base_folder_name
                            # 솔루션엑셀에 '_기본카테고리'가 있으면 접두사 추가
                            if has_basic_category:
                                folder_name = f"이셀카테_{folder_name}"
                            # 40byte 이내로 제한
                            folder_name = self._ensure_folder_name_byte_limit(folder_name)
                            result_df.at[idx, "폴더명"] = folder_name
                else:
                    # 기타 마켓: 스마트스토어와 동일하게 할인값 + 배송비 컬럼 사용
                    # 날짜_마켓코드 부분만 추출
                    # '이셀카테_' 접두사 사용 시 압축 모드 활성화
                    base_folder_name = self._extract_date_market_code(filename, compress_for_category=has_basic_category)
                    
                    # 각 행별로 처리 (할인값 + 배송비)
                    for idx, row in result_df.iterrows():
                        manager_code = row.get("판매자 관리코드", "")
                        folder_name_parts = [base_folder_name]
                        
                        if manager_code in processed_dict:
                            # 할인값 처리 ("판매자 부담 할인" 컬럼)
                            discount_value = None
                            if "판매자 부담 할인" in processed_df.columns:
                                discount_raw = processed_dict[manager_code].get("판매자 부담 할인", "")
                                if pd.notna(discount_raw) and str(discount_raw).strip():
                                    discount_value = self._convert_discount_to_folder_format(str(discount_raw).strip())
                            
                            # 배송비 처리
                            shipping_fee_str = None
                            if "배송비" in processed_df.columns:
                                shipping_fee = processed_dict[manager_code].get("배송비", "")
                                if pd.notna(shipping_fee) and str(shipping_fee).strip():
                                    shipping_fee_str = str(shipping_fee).strip().replace(",", "")
                            
                            # 폴더명 구성: 날짜_마켓코드_할할인값_배배송비값
                            if discount_value:
                                folder_name_parts.append(f"할{discount_value}")
                            if shipping_fee_str:
                                folder_name_parts.append(f"배{shipping_fee_str}")
                            
                            folder_name = "_".join(folder_name_parts)
                            
                            # 솔루션엑셀에 '_기본카테고리'가 있으면 접두사 추가
                            if has_basic_category:
                                folder_name = f"이셀카테_{folder_name}"
                            
                            # 40byte 이내로 제한
                            folder_name = self._ensure_folder_name_byte_limit(folder_name)
                            result_df.at[idx, "폴더명"] = folder_name
                        else:
                            folder_name = base_folder_name
                            # 솔루션엑셀에 '_기본카테고리'가 있으면 접두사 추가
                            if has_basic_category:
                                folder_name = f"이셀카테_{folder_name}"
                            # 40byte 이내로 제한
                            folder_name = self._ensure_folder_name_byte_limit(folder_name)
                            result_df.at[idx, "폴더명"] = folder_name
        
        # 3. 상품명* = ST4_최종결과
        if "ST4_최종결과" in processed_df.columns and "상품명*" in result_df.columns:
            for idx, row in result_df.iterrows():
                manager_code = row.get("판매자 관리코드", "")
                if manager_code in processed_dict:
                    st4_value = processed_dict[manager_code].get("ST4_최종결과", "")
                    if pd.notna(st4_value) and str(st4_value).strip():
                        result_df.at[idx, "상품명*"] = str(st4_value).strip()
        
        # 4. 판매가* = 마켓판매가격
        if "마켓판매가격" in processed_df.columns and "판매가*" in result_df.columns:
            for idx, row in result_df.iterrows():
                manager_code = row.get("판매자 관리코드", "")
                if manager_code in processed_dict:
                    price_value = processed_dict[manager_code].get("마켓판매가격", "")
                    if pd.notna(price_value):
                        try:
                            price_num = float(str(price_value).replace(",", ""))
                            result_df.at[idx, "판매가*"] = price_num
                        except (ValueError, TypeError):
                            pass
        
        # 5. 이미지 매핑
        # 가공엑셀에 사용URL이 있으면: 목록 이미지*, 이미지1에 사용URL, 나머지는 뒤로 밀어서 재사용
        # 가공엑셀에 사용URL이 없으면: 원본 그대로 재사용
        for idx, row in result_df.iterrows():
            manager_code = row.get("판매자 관리코드", "")
            
            # 원본 솔루션 엑셀에서 해당 행 찾기
            original_row = None
            original_row_data = {}
            if original_solution_df is not None and "판매자 관리코드" in original_solution_df.columns:
                original_row = original_solution_df[original_solution_df["판매자 관리코드"] == manager_code]
                if not original_row.empty:
                    # 원본의 모든 이미지 값 가져오기
                    for img_col in ["목록 이미지*", "이미지1(대표/기본이미지)*", "이미지2", "이미지3", "이미지4", "이미지5"]:
                        if img_col in original_solution_df.columns:
                            img_value = original_row.iloc[0].get(img_col, "")
                            if pd.notna(img_value) and str(img_value).strip():
                                original_row_data[img_col] = str(img_value).strip()
            
            # 가공엑셀에 사용URL이 있는지 확인
            has_processed_url = False
            processed_url = ""
            if manager_code in processed_dict:
                url_value = processed_dict[manager_code].get("사용URL", "")
                if pd.notna(url_value) and str(url_value).strip():
                    processed_url = str(url_value).strip()
                    has_processed_url = True
            
            if has_processed_url:
                # 케이스 1: 가공엑셀에 사용URL이 존재하는 경우
                # 목록 이미지* = 사용URL
                if "목록 이미지*" in result_df.columns:
                    result_df.at[idx, "목록 이미지*"] = processed_url
                # 이미지1(대표/기본이미지)* = 사용URL
                if "이미지1(대표/기본이미지)*" in result_df.columns:
                    result_df.at[idx, "이미지1(대표/기본이미지)*"] = processed_url
                
                # 나머지는 뒤로 밀어서 재사용
                # 기존 '목록 이미지*' → 이미지2
                # 기존 '이미지2' → 이미지3
                # 기존 '이미지3' → 이미지4
                # 기존 '이미지4' → 이미지5
                # 기존 '이미지5' → 사용불가 (버림)
                if original_row_data:
                    image_shift_mapping = {
                        "목록 이미지*": "이미지2",
                        "이미지2": "이미지3",
                        "이미지3": "이미지4",
                        "이미지4": "이미지5"
                        # 이미지5는 버림
                    }
                    
                    for original_col, result_col in image_shift_mapping.items():
                        if original_col in original_row_data and result_col in result_df.columns:
                            result_df.at[idx, result_col] = original_row_data[original_col]
            else:
                # 케이스 2: 가공엑셀에 사용URL이 없는 경우
                # 원본 그대로 재사용
                if original_row_data:
                    for img_col in ["목록 이미지*", "이미지1(대표/기본이미지)*", "이미지2", "이미지3", "이미지4", "이미지5"]:
                        if img_col in original_row_data and img_col in result_df.columns:
                            result_df.at[idx, img_col] = original_row_data[img_col]
        
        # 6. 상세설명* = 다팔자와 동일한 HTML 생성 로직
        if "상세설명*" in result_df.columns:
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
            
            # 설정에서 하단 이미지 URL 가져오기
            bottom_image_urls = config.get("detail_bottom_image_urls", exchange_image_urls)
            if not bottom_image_urls:
                bottom_image_urls = exchange_image_urls
            
            # 엑셀 전체에 랜덤으로 1개 선택
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
                # 등록 솔루션 엑셀의 원본 상세설명 가져오기
                original_detail = row.get("상세설명*", "")
                if pd.notna(original_detail):
                    original_detail = str(original_detail).strip()
                else:
                    original_detail = ""
                
                # 등록 솔루션 엑셀의 상품명 가져오기 (원본상품명)
                original_product_name = ""
                if "판매자 관리코드" in result_df.columns:
                    manager_code = row.get("판매자 관리코드", "")
                    if pd.notna(manager_code) and original_solution_df is not None:
                        if "판매자 관리코드" in original_solution_df.columns and "상품명*" in original_solution_df.columns:
                            original_row = original_solution_df[original_solution_df["판매자 관리코드"] == manager_code]
                            if not original_row.empty:
                                original_product_name = original_row.iloc[0].get("상품명*", "")
                                if pd.notna(original_product_name):
                                    original_product_name = str(original_product_name).strip()
                
                # 등록 솔루션 엑셀의 원본 대표 이미지 URL 가져오기
                main_image_url = ""
                if "판매자 관리코드" in result_df.columns:
                    manager_code = row.get("판매자 관리코드", "")
                    if pd.notna(manager_code) and original_solution_df is not None:
                        if "판매자 관리코드" in original_solution_df.columns:
                            original_row = original_solution_df[original_solution_df["판매자 관리코드"] == manager_code]
                            if not original_row.empty:
                                # 원본 등록 솔루션 엑셀에서 이미지 가져오기
                                possible_image_cols = ["이미지1(대표/기본이미지)*", "목록 이미지*", "이미지2", "이미지3"]
                                for col_name in possible_image_cols:
                                    if col_name in original_solution_df.columns:
                                        img_value = original_row.iloc[0].get(col_name, "")
                                        if pd.notna(img_value) and str(img_value).strip():
                                            main_image_url = str(img_value).strip()
                                            break
                
                # 상단 HTML 구성
                top_html_parts = []
                
                # 대표 이미지
                if main_image_url:
                    top_html_parts.append(
                        f'<img src="{main_image_url}" style="width: {image_width}px; height: {image_height}px; object-fit: contain;" /><br>'
                    )
                
                # 상품명 표시 텍스트
                if original_product_name:
                    product_name_display = top_product_name_text.replace("{상품명}", original_product_name)
                    top_html_parts.append(
                        f'<span style="color: {product_name_color}; font-size: {product_name_font_size}px;">{product_name_display}</span><br>'
                    )
                
                # 필독 문구
                if top_notice_text:
                    top_html_parts.append(
                        f'<span style="background-color: {notice_bg_color}; padding: {notice_padding};">{top_notice_text}</span>'
                    )
                
                top_html = "<center>" + "".join(top_html_parts) + "</center>" if top_html_parts else ""
                
                # 하단 이미지
                bottom_html = f'<center><img src="{selected_bottom_image_url}" /></center>'
                
                # 기존 하단 문구 추가
                if detail_bottom:
                    bottom_html = f"{bottom_html}<br><center>{detail_bottom}</center>"
                
                # 전체 조합: 상단 + 원본 상세설명 + 하단
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
                
                result_df.at[idx, "상세설명*"] = detail_html
        
        # 7. 선택사항 상세정보 = 옵션가격조정 로직
        option_price_rule = config.get("option_price_rule", "smartstore")
        if option_price_rule != "none" and "선택사항 상세정보" in result_df.columns and "마켓판매가격" in processed_df.columns:
            for idx, row in result_df.iterrows():
                manager_code = row.get("판매자 관리코드", "")
                option_text = row.get("선택사항 상세정보", "")
                
                if pd.notna(option_text) and str(option_text).strip():
                    # 가공된 엑셀에서 마켓판매가격 가져오기
                    market_price = 0
                    if manager_code in processed_dict:
                        price_value = processed_dict[manager_code].get("마켓판매가격", 0)
                        if pd.notna(price_value):
                            try:
                                market_price = float(price_value)
                            except (ValueError, TypeError):
                                market_price = 0
                    
                    # 옵션 보정 수행 (이셀러스 형식)
                    if market_price > 0:
                        corrected_option = self._correct_esellers_option_price(option_text, market_price)
                        if corrected_option != option_text:
                            result_df.at[idx, "선택사항 상세정보"] = corrected_option
        
        # 8. 브랜드 = 공란
        if "브랜드" in result_df.columns:
            result_df["브랜드"] = ""
        
        # 9. 모델명 = "edit" + 판매자 관리코드
        if "모델명" in result_df.columns and "판매자 관리코드" in result_df.columns:
            for idx, row in result_df.iterrows():
                manager_code = row.get("판매자 관리코드", "")
                if pd.notna(manager_code):
                    result_df.at[idx, "모델명"] = f"edit{str(manager_code).strip()}"
        
        # 10. 제조사 = "onerclan OEM"
        if "제조사" in result_df.columns:
            result_df["제조사"] = "onerclan OEM"
        
        # 11. 홍보문구 = "모든카드 무이자 3개월!"
        if "홍보문구" in result_df.columns:
            result_df["홍보문구"] = "모든카드 무이자 3개월!"
        
        # 12. 검색어(태그) = 기존 태그 + search_keywords (중복 제거)
        if "search_keywords" in processed_df.columns and "검색어(태그)" in result_df.columns:
            for idx, row in result_df.iterrows():
                manager_code = row.get("판매자 관리코드", "")
                
                # 가공된 엑셀의 키워드 가져오기
                processed_keywords = ""
                if manager_code in processed_dict:
                    keywords_value = processed_dict[manager_code].get("search_keywords", "")
                    if pd.notna(keywords_value) and str(keywords_value).strip():
                        processed_keywords = str(keywords_value).strip()
                
                # 등록 솔루션 엑셀의 기존 키워드 가져오기
                existing_keywords = row.get("검색어(태그)", "")
                if pd.notna(existing_keywords):
                    existing_keywords = str(existing_keywords).strip()
                else:
                    existing_keywords = ""
                
                # 키워드 합치기 및 중복 제거
                if processed_keywords or existing_keywords:
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
                    result_df.at[idx, "검색어(태그)"] = result_keywords
        
        # 13. 요약정보 자동 설정
        if "요약정보 상품군 코드*" in result_df.columns and "요약정보 전항목 상세설명 참조" in result_df.columns:
            for idx, row in result_df.iterrows():
                summary_code = row.get("요약정보 상품군 코드*", "")
                summary_ref = row.get("요약정보 전항목 상세설명 참조", "")
                
                # 둘 중 하나라도 비어있으면 기본값 설정
                if pd.isna(summary_code) or str(summary_code).strip() == "" or pd.isna(summary_ref) or str(summary_ref).strip() == "":
                    result_df.at[idx, "요약정보 상품군 코드*"] = "35"
                    result_df.at[idx, "요약정보 전항목 상세설명 참조"] = "Y"
                    
                    # 값1~값14 = "상세설명별도표시"
                    for i in range(1, 15):
                        col_name = f"값{i}"
                        if col_name in result_df.columns:
                            result_df.at[idx, col_name] = "상세설명별도표시"
        
        # 14. 카테고리 번호* 처리
        # 일반 파일: 모든 값을 '431905000' (문자열)로 강제 변경 (이셀러스 업로드 시 문제 방지)
        # 기본카테고리 파일: 원본 값 유지
        if "카테고리 번호*" in result_df.columns:
            # 솔루션 엑셀 파일명에 '_기본카테고리'가 있는지 확인
            solution_file_path = config.get("solution_file_path", "")
            has_basic_category = False
            if solution_file_path:
                solution_filename = os.path.basename(solution_file_path)
                if '_기본카테고리' in solution_filename:
                    has_basic_category = True
            
            if not has_basic_category:
                # 일반 파일: 모든 값을 '431905000' (문자열)로 강제 변경
                result_df["카테고리 번호*"] = "431905000"
            else:
                # 기본카테고리 파일: 빈 값만 '431905000'으로 채움
                for idx, row in result_df.iterrows():
                    category_no = row.get("카테고리 번호*", "")
                    # 비어있거나 NaN이거나 공백 문자열인 경우
                    if pd.isna(category_no) or str(category_no).strip() == "":
                        result_df.at[idx, "카테고리 번호*"] = "431905000"
        
        return result_df
    
    def _extract_date_market_code(self, filename: str, compress_for_category: bool = False) -> str:
        """파일명에서 날짜_마켓코드 부분만 추출 (예: "20260115_스스A1-0")
        
        Args:
            filename: 파일명
            compress_for_category: '이셀카테_' 접두사 사용 시 지마켓 축약 (지마켓->지켓)
        
        Returns:
            날짜_마켓코드 형식의 문자열
        """
        import re
        # 언더스코어로 분리하여 첫 두 부분만 추출
        parts = filename.split('_')
        if len(parts) >= 2:
            date_part = parts[0]
            market_code_part = parts[1]
            
            # 날짜 축약: 항상 적용 (년도 앞 2자리 제거: 20260117 -> 260117)
            if re.match(r'^\d{8}$', date_part):
                date_part = date_part[2:]  # 앞 2자리 제거
            
            # 지마켓 축약: '이셀카테_' 접두사 사용 시에만 적용
            if compress_for_category:
                if market_code_part.startswith('지마켓'):
                    market_code_part = market_code_part.replace('지마켓', '지켓', 1)  # 첫 번째만 교체
            
            # 날짜 형식 검증 (6자리 숫자로 축약됨)
            if re.match(r'^\d{6}$', date_part) or re.match(r'^\d{8}$', parts[0]):
                return f"{date_part}_{market_code_part}"
            # 날짜 형식이 아니어도 첫 두 부분 반환
            return f"{date_part}_{market_code_part}"
        # 언더스코어가 없거나 1개만 있으면 첫 부분만 반환
        return parts[0] if parts else filename[:20]
    
    def _convert_discount_to_folder_format(self, discount_str: str) -> str:
        """할인값을 폴더명 형식으로 변환 (예: "0.5" → "50", "50%" → "50")
        
        입력 형식:
        - "0.5" (소수점 형식)
        - "50%" (퍼센트 형식)
        - "50" (정수 형식)
        
        출력: "50" (정수 문자열, 퍼센트 값)
        """
        import re
        # 퍼센트 기호 제거
        discount_str = discount_str.replace("%", "").strip()
        
        try:
            discount_num = float(discount_str)
            # 소수점 형식인 경우 (0.5 → 50)
            if discount_num < 1:
                discount_num = int(discount_num * 100)
            else:
                discount_num = int(discount_num)
            return str(discount_num)
        except (ValueError, TypeError):
            # 변환 실패 시 원본 반환 (숫자가 아닌 경우)
            return discount_str
    
    def _extract_discount_shipping_pattern(self, filename: str) -> str:
        """파일명에서 할인/배송 패턴 추출 (예: "할2_3000_스0.5")
        
        패턴: 할숫자_숫자_스숫자 (예: 할2_3000_스0.5, 할61_3000_스0.5)
        """
        import re
        # 할숫자_숫자_스숫자 패턴 찾기
        # 할 + 숫자 + _ + 숫자 + _ + 스 + 숫자(소수점 포함 가능)
        pattern = r'할\d+_\d+_스[\d.]+'
        match = re.search(pattern, filename)
        if match:
            return match.group(0)
        return ""
    
    def _lenb_excel(self, s: str) -> int:
        """엑셀의 LENB 함수와 동일한 바이트 계산 (DBCS 방식)
        
        Args:
            s: 문자열
        
        Returns:
            바이트 수 (영문/숫자: 1바이트, 한글: 2바이트)
        """
        count = 0
        for char in s:
            # 한글, 일본어, 중국어 등의 전각 문자는 2바이트 (엑셀 LENB 방식)
            if ord(char) > 127:  # ASCII가 아닌 문자
                count += 2
            else:
                count += 1
        return count
    
    def _ensure_folder_name_byte_limit(self, folder_name: str, max_bytes: int = 40) -> str:
        """폴더명이 40byte 이내인지 확인하고 초과 시 자르기 (엑셀 LENB 방식)
        
        Args:
            folder_name: 폴더명
            max_bytes: 최대 바이트 수 (기본값: 40, 엑셀 LENB 기준)
        
        Returns:
            40byte 이내로 제한된 폴더명
        """
        # 엑셀 LENB 방식으로 바이트 수 확인 (한글 2바이트, 영문/숫자 1바이트)
        folder_bytes = self._lenb_excel(folder_name)
        if folder_bytes <= max_bytes:
            return folder_name
        
        # 초과 시 자르기 (문자 단위로 제거)
        truncated = folder_name
        while self._lenb_excel(truncated) > max_bytes and len(truncated) > 0:
            truncated = truncated[:-1]
        
        return truncated
    
    def _sanitize_folder_name(self, filename: str) -> str:
        """폴더명 정제 (한글, 영문, 숫자, _, - 포함, 40byte 이내)"""
        # 한글, 영문, 숫자, _, - 만 추출
        import re
        sanitized = re.sub(r'[^가-힣a-zA-Z0-9_-]', '', filename)
        
        # 40byte 이내로 자르기
        byte_count = 0
        result = ""
        for char in sanitized:
            char_bytes = len(char.encode('utf-8'))
            if byte_count + char_bytes <= 40:
                result += char
                byte_count += char_bytes
            else:
                break
        
        return result if result else sanitized[:20]  # 최소 20자 보장
    
    def _correct_esellers_option_price(self, option_text: str, market_price: float) -> str:
        """이셀러스 옵션 형식의 추가금액 보정
        
        형식:
        - 독립형: 옵션명*옵션값**추가금액*수량*노출여부(Y/N)*이미지URL*판매자관리코드
        - 조합형: 옵션명1의 옵션값*옵션명2의 옵션값**추가금액*수량*노출여부(Y/N)*이미지URL*판매자관리코드*용량
        """
        import re
        from rules.option_price_correction import OptionPriceCorrector
        
        lines = option_text.strip().split('\n')
        corrected_lines = []
        
        # 최대 옵션추가금 계산
        max_delta = OptionPriceCorrector.calculate_max_delta(market_price)
        rounding_unit = OptionPriceCorrector.get_rounding_unit(market_price)
        
        # 각 옵션 라인 처리
        option_prices = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # **로 분리하여 추가금액 부분 찾기
            parts = line.split('**')
            if len(parts) >= 2:
                # 추가금액이 있는 경우
                after_double_star = parts[1]
                # 첫 번째 *로 분리하여 추가금액 추출
                price_parts = after_double_star.split('*', 1)
                if price_parts:
                    try:
                        original_price = int(price_parts[0])
                        option_prices.append((line, original_price, parts[0], after_double_star))
                    except (ValueError, TypeError):
                        corrected_lines.append(line)  # 파싱 실패 시 원본 유지
                else:
                    corrected_lines.append(line)
            else:
                corrected_lines.append(line)  # **가 없으면 원본 유지
        
        if not option_prices:
            return option_text  # 옵션 가격이 없으면 원본 반환
        
        # 옵션 가격 보정
        # 1. 음수는 0으로
        option_prices = [(line, max(0, price), prefix, suffix) for line, price, prefix, suffix in option_prices]
        
        # 2. 최대값 제한 및 단위 내림
        corrected_prices = []
        for line, price, prefix, suffix in option_prices:
            if price > max_delta:
                price = max_delta
            # 단위 내림
            price = (price // rounding_unit) * rounding_unit
            corrected_prices.append((line, price, prefix, suffix))
        
        # 3. 최소 1개는 0원 보장
        positive_count = sum(1 for _, price, _, _ in corrected_prices if price > 0)
        if positive_count == 0 and len(corrected_prices) > 0:
            # 첫 번째 옵션을 0원으로 설정
            line, _, prefix, suffix = corrected_prices[0]
            corrected_prices[0] = (line, 0, prefix, suffix)
        elif positive_count == 1:
            # 양수 값이 1개인 경우 max_delta로 고정
            for i, (line, price, prefix, suffix) in enumerate(corrected_prices):
                if price > 0:
                    corrected_price = (max_delta // rounding_unit) * rounding_unit
                    corrected_prices[i] = (line, corrected_price, prefix, suffix)
                    break
        elif positive_count >= 2:
            # 양수 값이 2개 이상인 경우 비율 유지 스케일링
            positive_items = [(i, price) for i, (_, price, _, _) in enumerate(corrected_prices) if price > 0]
            if positive_items:
                total_original = sum(price for _, price in positive_items)
                if total_original > 0:
                    # 비율 계산
                    max_original = max(price for _, price in positive_items)
                    scale_factor = max_delta / max_original if max_original > 0 else 1
                    
                    for i, original_price in positive_items:
                        line, _, prefix, suffix = corrected_prices[i]
                        scaled_price = original_price * scale_factor
                        rounded_price = (int(scaled_price) // rounding_unit) * rounding_unit
                        corrected_prices[i] = (line, rounded_price, prefix, suffix)
        
        # 보정된 옵션 라인 재구성
        for line, corrected_price, prefix, suffix in corrected_prices:
            # suffix에서 첫 번째 부분(추가금액)을 교체
            suffix_parts = suffix.split('*', 1)
            if len(suffix_parts) >= 1:
                # 정수로 변환하여 소수점 제거
                price_int = int(corrected_price)
                new_suffix = str(price_int) + '*' + '*'.join(suffix_parts[1:]) if len(suffix_parts) > 1 else str(price_int)
                corrected_line = prefix + '**' + new_suffix
                corrected_lines.append(corrected_line)
            else:
                corrected_lines.append(line)
        
        return '\n'.join(corrected_lines) if corrected_lines else option_text

