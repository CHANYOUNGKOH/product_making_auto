"""
배송비 계산 규칙 모듈
금액대별 배송비 계산 로직
"""

from typing import Dict, List, Optional

class ShippingFeeCalculator:
    """배송비 계산기"""
    
    @staticmethod
    def calculate_standard_transformation(cost: float) -> float:
        """
        기본 배송비 변환 로직 (형식 1)
        
        Args:
            cost: 기존 배송비
        
        Returns:
            변환된 배송비
        """
        cost = int(cost)
        if cost == 0:
            return 0
        elif 2000 <= cost <= 3000:
            return 3000
        elif 3000 < cost < 3500:
            return 3500
        elif 3500 <= cost < 4000:
            return 4000
        elif 4000 < cost < 5000:
            return 5000
        elif 5000 <= cost < 10000:
            return cost + 1000
        elif cost >= 10000:
            return cost + 2000
        else:
            return cost  # 그 외(2000 미만 등)는 기존 유지
    
    @staticmethod
    def calculate(original_shipping_fee: float, config: Dict) -> float:
        """
        배송비 계산 (공통 규칙)
        
        Args:
            original_shipping_fee: 기존 배송비 (가공 엑셀에서 가져온 값)
            config: 배송비 계산 설정
                - shipping_method: "standard" (형식 1) 또는 "free" (형식 2)
        
        Returns:
            계산된 배송비
        """
        shipping_method = config.get("shipping_method", "standard")
        
        if shipping_method == "free":
            # 형식 2: 무료배송으로 전환
            return 0
        else:
            # 형식 1: 기본 배송비 변환 로직
            return ShippingFeeCalculator.calculate_standard_transformation(original_shipping_fee)
    
    @staticmethod
    def calculate_return_fee(shipping_fee: float, config: Dict, original_shipping_fee: float = 0) -> float:
        """
        반품배송비 계산
        
        Args:
            shipping_fee: 변경된 배송비
            config: 설정
                - shipping_method: "standard" 또는 "free"
            original_shipping_fee: 기존 배송비 (무료배송일 때 사용)
        
        Returns:
            반품배송비
        """
        shipping_method = config.get("shipping_method", "standard")
        
        if shipping_method == "free":
            # 형식 2: 기존 배송비 + 1000
            return int(original_shipping_fee) + 1000
        else:
            # 형식 1: 변경된 배송비 + 1000
            return int(shipping_fee) + 1000
    
    @staticmethod
    def calculate_exchange_fee(return_fee: float, config: Dict) -> float:
        """
        교환배송비 계산
        
        Args:
            return_fee: 반품배송비
            config: 설정
        
        Returns:
            교환배송비 (반품배송비 * 2)
        """
        return int(return_fee) * 2

