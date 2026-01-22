"""
기본 솔루션 클래스
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import pandas as pd

class BaseSolution(ABC):
    """등록 솔루션 기본 클래스"""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """솔루션 이름"""
        pass
    
    @property
    @abstractmethod
    def columns(self) -> List[str]:
        """솔루션 엑셀 컬럼 목록"""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """솔루션 설명"""
        pass
    
    @property
    def extensions(self) -> List[str]:
        """지원하는 파일 확장자"""
        return [".xlsx", ".xls"]
    
    @property
    def temp_path_template(self) -> Optional[str]:
        """임시 파일 경로 템플릿 (None이면 사용 안함)"""
        return None
    
    def get_default_mapping(self) -> Dict[str, str]:
        """기본 컬럼 매핑 규칙
        
        Returns:
            {가공엑셀컬럼: 솔루션엑셀컬럼} 딕셔너리
        """
        return {}
    
    def apply_mapping(self, result_df: pd.DataFrame, processed_df: pd.DataFrame, 
                     column_mapping: Dict[str, str], config: Dict) -> pd.DataFrame:
        """매핑 규칙 적용
        
        Args:
            result_df: 결과 데이터프레임 (솔루션 엑셀 구조)
            processed_df: 가공된 엑셀 데이터프레임
            column_mapping: 컬럼 매핑 딕셔너리
            config: 솔루션별 설정
        
        Returns:
            매핑이 적용된 결과 데이터프레임
        """
        # 기본 매핑: 상품코드 기준
        if "상품코드" in processed_df.columns and "상품코드" in result_df.columns:
            processed_dict = processed_df.set_index("상품코드").to_dict("index")
            
            for proc_col, sol_col in column_mapping.items():
                if proc_col in processed_df.columns and sol_col in result_df.columns:
                    for idx, row in result_df.iterrows():
                        product_code = row.get("상품코드", "")
                        if product_code in processed_dict:
                            proc_value = processed_dict[product_code].get(proc_col, "")
                            # #N/A, 빈 값 처리
                            if pd.notna(proc_value):
                                proc_value_str = str(proc_value).strip()
                                # #N/A 관련 문자열 필터링
                                if proc_value_str and proc_value_str.upper() not in ['#N/A', '#NA', 'N/A', 'NA', 'NULL', 'NAN', 'NAN.0']:
                                    # 판매자 부담 할인: 백분율 표기 유지 (49% -> 49%로 유지, 0.49 -> 49%로 변환)
                                    if sol_col == "판매자 부담 할인":
                                        # 소수로 변환된 경우 (0.49) 백분율로 변환
                                        try:
                                            float_value = float(proc_value_str)
                                            if 0 <= float_value <= 1:
                                                # 소수 형태면 백분율로 변환
                                                proc_value_str = f"{float_value * 100:.0f}%"
                                            elif float_value > 1 and float_value <= 100:
                                                # 이미 백분율 값이지만 %가 없는 경우
                                                proc_value_str = f"{float_value:.0f}%"
                                        except (ValueError, TypeError):
                                            # 숫자가 아니면 그대로 사용 (이미 %가 포함된 경우)
                                            if "%" not in proc_value_str:
                                                # %가 없으면 추가
                                                try:
                                                    float_value = float(proc_value_str)
                                                    if 0 <= float_value <= 1:
                                                        proc_value_str = f"{float_value * 100:.0f}%"
                                                    elif float_value > 1:
                                                        proc_value_str = f"{float_value:.0f}%"
                                                except (ValueError, TypeError):
                                                    pass
                                    result_df.at[idx, sol_col] = proc_value_str
        
        return result_df
    
    def apply_solution_specific_rules(self, result_df: pd.DataFrame, 
                                     processed_df: pd.DataFrame, config: Dict,
                                     original_solution_df: pd.DataFrame = None) -> pd.DataFrame:
        """솔루션별 특화 규칙 적용
        
        Args:
            result_df: 결과 데이터프레임
            processed_df: 가공된 엑셀 데이터프레임
            config: 솔루션별 설정
            original_solution_df: 원본 솔루션 엑셀 데이터프레임 (매핑 전 원본 값 가져오기용)
        
        Returns:
            규칙이 적용된 결과 데이터프레임
        """
        # 기본 구현: 아무것도 하지 않음
        # 하위 클래스에서 오버라이드
        return result_df
    
    def calculate_price(self, base_price: float, config: Dict) -> float:
        """가격 계산
        
        Args:
            base_price: 기준 가격
            config: 가격 계산 설정
        
        Returns:
            계산된 가격
        """
        # 기본 구현: 기준 가격 그대로 반환
        # 하위 클래스에서 오버라이드
        return base_price
    
    def calculate_shipping_fee(self, price: float, config: Dict) -> float:
        """배송비 계산
        
        Args:
            price: 상품 가격
            config: 배송비 계산 설정
        
        Returns:
            계산된 배송비
        """
        # 기본 구현: 설정에서 가져오거나 기본값
        shipping_rules = config.get("shipping_fee_rules", {})
        # TODO: 금액대별 규칙 적용
        return shipping_rules.get("default", 0)

