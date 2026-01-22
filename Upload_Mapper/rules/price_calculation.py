"""
가격 계산 규칙 모듈
마켓별, 마진별 가격 계산 로직
"""

from typing import Dict, Optional
import pandas as pd

class PriceCalculator:
    """가격 계산기"""
    
    @staticmethod
    def calculate(base_price: float, config: Dict) -> float:
        """
        가격 계산
        
        Args:
            base_price: 기준 가격
            config: 가격 계산 설정
                - market: 마켓 이름
                - margin_rate: 마진률 (%)
                - commission_rate: 수수료율 (%)
                - discount_rate: 할인률 (%)
        
        Returns:
            계산된 가격
        """
        margin_rate = config.get("margin_rate", 0.0)
        commission_rate = config.get("commission_rate", 0.0)
        discount_rate = config.get("discount_rate", 0.0)
        
        calculated_price = base_price
        
        # 마진 적용: 기준가격 * (1 + 마진률/100)
        if margin_rate:
            calculated_price = calculated_price * (1 + margin_rate / 100)
        
        # 수수료 적용: 가격 / (1 - 수수료율/100)
        if commission_rate:
            calculated_price = calculated_price / (1 - commission_rate / 100)
        
        # 할인 적용: 가격 * (1 - 할인률/100)
        if discount_rate:
            calculated_price = calculated_price * (1 - discount_rate / 100)
        
        return round(calculated_price, 0)
    
    @staticmethod
    def calculate_from_lookup(base_price: float, lookup_table: pd.DataFrame, 
                              price_col: str, result_col: str) -> Optional[float]:
        """
        VLOOKUP 방식으로 가격 계산
        
        Args:
            base_price: 기준 가격
            lookup_table: 조회 테이블 (DataFrame)
            price_col: 가격 컬럼명
            result_col: 결과 컬럼명
        
        Returns:
            조회된 가격 또는 None
        """
        if lookup_table is None or lookup_table.empty:
            return None
        
        # 기준 가격과 가장 가까운 행 찾기
        if price_col in lookup_table.columns and result_col in lookup_table.columns:
            # 정확히 일치하는 행 찾기
            exact_match = lookup_table[lookup_table[price_col] == base_price]
            if not exact_match.empty:
                return exact_match.iloc[0][result_col]
            
            # 범위 검색 (기준 가격보다 작거나 같은 최대값)
            lower_matches = lookup_table[lookup_table[price_col] <= base_price]
            if not lower_matches.empty:
                closest = lower_matches.loc[lower_matches[price_col].idxmax()]
                return closest[result_col]
        
        return None

