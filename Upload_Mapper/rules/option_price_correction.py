"""
옵션추가금 자동 보정 모듈
다팔자 맵퍼에서 옵션 컬럼의 금액 부분을 오픈마켓 규칙에 맞게 자동 보정
"""

import re
from typing import List, Tuple, Optional


class OptionPriceCorrector:
    """옵션추가금 자동 보정 클래스"""
    
    @staticmethod
    def parse_option_line(line: str) -> Tuple[str, Optional[int]]:
        """
        옵션 라인에서 텍스트와 금액 토큰을 분리
        
        Args:
            line: 옵션 라인 (예: "색상:그린,+0원" 또는 "선택:대장금 올은수저 1벌,추가구성:수저세트,-100원")
        
        Returns:
            (텍스트 부분, 금액 delta) 튜플
            파싱 실패 시 (원본 라인, None)
        """
        line = line.strip()
        if not line:
            return (line, None)
        
        # 마지막 금액 토큰 패턴: +숫자원 또는 -숫자원 (콤마 포함/미포함, 공백 허용)
        # 예: +0원, -100원, +8,020원, +8020원, +3040원, , -100원
        # 권장 패턴: "마지막에 붙은 ...원"만 잡고, "콤마 유무 + 공백 유무" 다 허용
        pattern = r'(?:,\s*)?([+-]?)\s*(\d[\d,]*)\s*원\s*$'
        match = re.search(pattern, line)
        
        if match:
            sign = match.group(1) or ''
            number_str = match.group(2).replace(',', '')
            
            try:
                delta = int(number_str)
                if sign == '-':
                    delta = -delta
                elif sign == '+':
                    delta = delta
                # sign이 없으면 +로 간주
                
                # 텍스트 부분 추출 (금액 토큰 제외)
                text_part = line[:match.start()].rstrip()
                # 마지막 쉼표 제거
                if text_part.endswith(','):
                    text_part = text_part[:-1].rstrip()
                return (text_part, delta)
            except ValueError:
                return (line, None)
        else:
            # 금액 토큰이 없는 경우 (원본 유지)
            return (line, None)
    
    @staticmethod
    def calculate_max_delta(price: float) -> float:
        """
        마켓판매가격 기준 최대 옵션추가금 계산
        
        Args:
            price: 마켓판매가격 (P)
        
        Returns:
            최대 옵션추가금 (max_delta)
        """
        if price < 2000:
            # P < 2,000: 0 ~ +100% (0 ~ +P)
            return price
        elif price < 10000:
            # 2,000 ≤ P < 10,000: -50% ~ +100%
            # -값은 0으로 교정하므로 사실상 0 ~ +P
            # 요청서 규칙: max_delta = P
            return price
        else:
            # P ≥ 10,000: -50% ~ +50%
            # -값은 0으로 교정하므로 사실상 0 ~ +P*0.5
            return price * 0.5
    
    @staticmethod
    def get_rounding_unit(market_price: float) -> int:
        """
        마켓판매가격에 따른 단위 내림 규칙
        
        Args:
            market_price: 마켓판매가격
        
        Returns:
            내림 단위 (10, 100, 500, 1000)
        """
        if market_price > 60000:
            # 6만원 초과: 1000원 단위 내림
            return 1000
        elif market_price > 30000:
            # 3만원 초과 6만원 이하: 500원 단위 내림
            return 500
        elif market_price > 10000:
            # 1만원 초과 3만원 이하: 100원 단위 내림
            return 100
        else:
            # 1만원 이하: 10원 단위 내림 (기본)
            return 10
    
    @staticmethod
    def redistribute_deltas(deltas: List[int], max_delta: float, has_zero: bool, market_price: float) -> List[int]:
        """
        옵션추가금 리스트를 정책에 따라 재배정
        
        Args:
            deltas: 원본 옵션추가금 리스트
            max_delta: 최대 허용 옵션추가금
            has_zero: 원본에 +0원 옵션이 있는지 여부
            market_price: 마켓판매가격 (단위 내림 규칙 적용용)
        
        Returns:
            재배정된 옵션추가금 리스트
        """
        if not deltas:
            return []
        
        # 1. -값은 전부 0으로 변경
        corrected_deltas = [max(0, d) for d in deltas]
        
        # 2. 모든 값이 0인 경우 (예시1: 동급 옵션)
        if all(d == 0 for d in corrected_deltas):
            return corrected_deltas
        
        # 3. 0이 없는 경우, 최소값을 0으로 만들기
        if not has_zero and all(d > 0 for d in corrected_deltas):
            min_idx = corrected_deltas.index(min(corrected_deltas))
            corrected_deltas[min_idx] = 0
            has_zero = True
        
        # 4. 0 초과 값들만 추출
        positive_deltas = [d for d in corrected_deltas if d > 0]
        
        if not positive_deltas:
            # 모든 값이 0인 경우
            return corrected_deltas
        
        # 5. 최대값이 max_delta를 초과하는지 확인
        max_positive = max(positive_deltas)
        
        # 요청서 규칙에 따른 처리
        # 모든 경우에 분포 유지 스케일링 적용 (양수 값이 2개 이상이면 비율 유지)
        # 예시2는 양수 값이 1개인 경우에만 단순 보정 적용
        if max_positive > 0:
            positive_count = len(positive_deltas)
            
            # 단위 내림 규칙 적용
            rounding_unit = OptionPriceCorrector.get_rounding_unit(market_price)
            
            if positive_count == 1:
                # 예시2: 양수 값이 1개인 경우만 단순 보정 (max_delta로 고정)
                # 상한선 cap: 단위 내림 규칙 적용
                cap = (int(max_delta) // rounding_unit) * rounding_unit
                
                redistributed = []
                for d in corrected_deltas:
                    if d == 0:
                        redistributed.append(0)
                    else:
                        # 양수 값은 모두 cap으로 설정 (단위 내림 규칙 적용)
                        redistributed.append(cap)
                
                return redistributed
            else:
                # 양수 값이 2개 이상인 경우: 분포 유지 스케일링 (비율 유지)
                # max_delta를 초과하든 아니든, 최대값을 max_delta로 맞추고 나머지는 비율 유지
                scale_ratio = max_delta / max_positive
                
                # 상한선 cap: 단위 내림 규칙 적용
                cap = (int(max_delta) // rounding_unit) * rounding_unit
                
                # 재배정: 0은 유지, 양수는 스케일링 (단위 내림 규칙 적용)
                redistributed = []
                for d in corrected_deltas:
                    if d == 0:
                        redistributed.append(0)
                    else:
                        # 스케일링 후 단위 내림 (floor)
                        scaled = int((d * scale_ratio) // rounding_unit * rounding_unit)
                        # 상한선 cap 적용
                        scaled = min(scaled, cap)
                        redistributed.append(max(0, scaled))
                
                # 스케일링 후에도 0이 없는 경우, 최소값을 0으로 만들기
                if not has_zero and all(d > 0 for d in redistributed):
                    min_idx = redistributed.index(min(redistributed))
                    redistributed[min_idx] = 0
                
                return redistributed
        
        # 모든 값이 0인 경우
        return corrected_deltas
    
    @staticmethod
    def correct_option_text(option_text: str, market_price: float) -> Tuple[str, dict]:
        """
        옵션 텍스트의 금액 부분을 자동 보정
        
        Args:
            option_text: 원본 옵션 텍스트 (줄바꿈으로 구분된 여러 옵션 라인)
            market_price: 마켓판매가격 (옵션 보정 기준)
        
        Returns:
            (보정된 옵션 텍스트, 변경 정보 딕셔너리) 튜플
        """
        if not option_text or not str(option_text).strip():
            return (option_text, {"changed": False, "lines_changed": 0})
        
        option_text = str(option_text)
        lines = option_text.split('\n')
        
        # 파싱 단계: 각 라인에서 텍스트와 금액 분리
        parsed_lines = []
        deltas = []
        
        for line in lines:
            text_part, delta = OptionPriceCorrector.parse_option_line(line)
            parsed_lines.append((text_part, delta))
            if delta is not None:
                deltas.append(delta)
            else:
                # 금액 토큰이 없는 라인은 0으로 간주하지 않음 (원본 유지)
                deltas.append(None)
        
        # 재배정 단계
        max_delta = OptionPriceCorrector.calculate_max_delta(market_price)
        has_zero = any(d == 0 for d in deltas if d is not None)
        
        # None이 아닌 delta만 재배정
        valid_deltas = [d for d in deltas if d is not None]
        if valid_deltas:
            redistributed_valid = OptionPriceCorrector.redistribute_deltas(
                valid_deltas, max_delta, has_zero, market_price
            )
            
            # 재배정된 값으로 deltas 업데이트
            redistributed_idx = 0
            for i, delta in enumerate(deltas):
                if delta is not None:
                    deltas[i] = redistributed_valid[redistributed_idx]
                    redistributed_idx += 1
        
        # 재조립 단계: 원본 텍스트 유지하고 금액 토큰만 교체
        corrected_lines = []
        lines_changed = 0
        original_deltas_list = []
        corrected_deltas_list = []
        
        for i, (text_part, original_delta) in enumerate(parsed_lines):
            if original_delta is not None:
                original_deltas_list.append(original_delta)
                new_delta = deltas[i]
                if new_delta is not None:
                    corrected_deltas_list.append(new_delta)
                else:
                    # 재배정 실패 시 원본 사용
                    new_delta = original_delta
                    corrected_deltas_list.append(new_delta)
                    deltas[i] = new_delta
                
                if new_delta != original_delta:
                    lines_changed += 1
                
                # 금액 토큰 재조립 (빈 text_part 처리)
                if text_part:
                    if new_delta >= 0:
                        corrected_line = f"{text_part},+{new_delta}원"
                    else:
                        corrected_line = f"{text_part},{new_delta}원"
                else:
                    # text_part가 비어있는 경우 (금액만 있는 라인)
                    if new_delta >= 0:
                        corrected_line = f"+{new_delta}원"
                    else:
                        corrected_line = f"{new_delta}원"
                corrected_lines.append(corrected_line)
            else:
                # 금액 토큰이 없는 라인은 원본 유지
                corrected_lines.append(lines[i])
        
        corrected_text = '\n'.join(corrected_lines)
        
        return (corrected_text, {
            "changed": lines_changed > 0,
            "lines_changed": lines_changed,
            "max_delta": max_delta,
            "original_deltas": original_deltas_list,
            "corrected_deltas": corrected_deltas_list
        })


def log_option_correction(product_code: str, option_text: str, market_price: float, 
                         corrected_text: str, change_info: dict):
    """옵션 보정 로그 기록 (현재는 사용하지 않음)"""
    pass

