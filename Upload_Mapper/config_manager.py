"""
설정 관리 모듈
"""

import os
import json
import sys
from datetime import datetime
from typing import Dict, Optional
from pathlib import Path

def get_resource_path(relative_path):
    """PyInstaller로 빌드된 환경에서 리소스 경로 찾기"""
    if getattr(sys, 'frozen', False):
        # PyInstaller로 빌드된 실행 파일
        base_path = Path(sys._MEIPASS)  # 임시 폴더
    else:
        # 개발 환경
        base_path = Path(__file__).parent
    return base_path / relative_path

class MapperConfig:
    """맵퍼 설정 관리 클래스"""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """
        Args:
            base_dir: 설정 파일이 저장될 기본 디렉토리 (None이면 현재 디렉토리)
        """
        if base_dir is None:
            # PyInstaller 환경 고려
            if getattr(sys, 'frozen', False):
                # 실행 파일이 있는 디렉토리 사용 (사용자가 설정을 수정할 수 있도록)
                base_dir = Path(sys.executable).parent
            else:
                base_dir = Path(__file__).parent
        self.base_dir = Path(base_dir)
        self.config_file = self.base_dir / "upload_mapper_config.json"
        
        # PyInstaller 환경에서 임시 폴더의 템플릿 경로 설정 (리소스 읽기용)
        if getattr(sys, 'frozen', False):
            try:
                self._temp_templates_dir = Path(sys._MEIPASS) / "templates"
            except AttributeError:
                self._temp_templates_dir = None
        else:
            self._temp_templates_dir = None
    
    def load_config(self) -> Dict:
        """설정 파일 로드"""
        # 먼저 실행 파일 디렉토리에서 찾기 (사용자 설정 우선)
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"[경고] 설정 파일 로드 실패: {e}")
                return {}
        
        # PyInstaller 환경에서 임시 폴더의 기본 설정 파일 확인
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            temp_config = Path(sys._MEIPASS) / "upload_mapper_config.json"
            if temp_config.exists():
                try:
                    with open(temp_config, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception as e:
                    print(f"[경고] 기본 설정 파일 로드 실패: {e}")
        
        return {}
    
    def save_config(self, config: Dict):
        """설정 파일 저장"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[오류] 설정 파일 저장 실패: {e}")
    
    def get_solution_config(self, solution_name: str, market: Optional[str] = None) -> Dict:
        """특정 솔루션의 매핑 설정 가져오기
        
        Args:
            solution_name: 솔루션 이름
            market: 마켓 이름 (다팔자 솔루션의 경우 마켓별 설정 사용)
        """
        config = self.load_config()
        default_config = {
            "column_mapping": {},  # {가공엑셀컬럼: 솔루션엑셀컬럼}
            "default_values": {},  # {솔루션엑셀컬럼: 기본값}
            "transformations": {},  # {솔루션엑셀컬럼: 변환규칙}
            "detail_top_text": "",  # 상세설명 상단 문구 (사용 안함, center 태그로 자동 처리)
            "detail_bottom_text": "",  # 상세설명 하단 문구
            "detail_top_template": "",  # 상세설명 상단 템플릿 이름
            "detail_bottom_template": "",  # 상세설명 하단 템플릿 이름
            "detail_top_image_width": 500,  # 상단 이미지 가로 사이즈 (px)
            "detail_top_image_height": 500,  # 상단 이미지 세로 사이즈 (px)
            "detail_top_product_name_text": "[상품명: {상품명}]",  # 상품명 표시 텍스트 (등록 솔루션 엑셀의 상품명 사용)
            "detail_top_product_name_color": "blue",  # 상품명 텍스트 컬러
            "detail_top_product_name_font_size": 10,  # 상품명 텍스트 폰트 사이즈 (px)
            "detail_top_notice_text": "[필독] 제품명 및 상세설명에 기재된 '본품'만 발송됩니다.",  # 필독 문구
            "detail_top_notice_bg_color": "yellow",  # 필독 문구 배경 컬러
            "detail_top_notice_padding": "2px 5px",  # 필독 문구 패딩
            "detail_bottom_image_urls": [  # 하단 이미지 URL 목록 (랜덤 선택)
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
            ],
            "shipping_method": "standard",  # 배송비 계산 방식: "standard" (형식 1) 또는 "free" (형식 2)
            "shipping_fee_rules": {},  # 배송비 금액대별 규칙 (사용 안함, shipping_method로 대체)
            "option_price_rule": "smartstore",  # 옵션금액 규칙: "smartstore" (스마트스토어 기준, 기본값) 또는 다른 마켓 규칙
            "price_calculation": {  # 가격 계산 규칙 (사용 안함, 매핑으로 대체)
                "market": "",  # 마켓 이름
                "margin_rate": 0.0,  # 마진률
                "commission_rate": 0.0,  # 수수료율
                "discount_rate": 0.0  # 할인률
            }
        }
        
        # 다팔자 솔루션의 경우 마켓별 설정 지원
        if solution_name == "다팔자" and market:
            solution_config = config.get("solutions", {}).get(solution_name, {})
            market_mappings = solution_config.get("market_mappings", {})
            if market in market_mappings:
                # 마켓별 설정이 있으면 병합 (마켓별 설정 우선)
                market_config = market_mappings[market]
                merged_config = default_config.copy()
                merged_config.update(solution_config)  # 기본 설정
                merged_config.update(market_config)  # 마켓별 설정 (우선)
                return merged_config
        
        return config.get("solutions", {}).get(solution_name, default_config)
    
    def save_solution_config(self, solution_name: str, config_data: Dict, market: Optional[str] = None):
        """특정 솔루션의 매핑 설정 저장
        
        Args:
            solution_name: 솔루션 이름
            config_data: 설정 데이터
            market: 마켓 이름 (다팔자 솔루션의 경우 마켓별 설정으로 저장)
        """
        config = self.load_config()
        if "solutions" not in config:
            config["solutions"] = {}
        
        if solution_name not in config["solutions"]:
            config["solutions"][solution_name] = {}
        
        # 다팔자 솔루션의 경우 마켓별 설정 저장
        if solution_name == "다팔자" and market:
            if "market_mappings" not in config["solutions"][solution_name]:
                config["solutions"][solution_name]["market_mappings"] = {}
            
            # 마켓별 매핑 설정만 저장 (column_mapping, default_values)
            market_config = {
                "column_mapping": config_data.get("column_mapping", {}),
                "default_values": config_data.get("default_values", {})
            }
            config["solutions"][solution_name]["market_mappings"][market] = market_config
            
            # 공통 설정은 기본 솔루션 설정에 저장 (마켓별 설정이 아닌 것들)
            common_config = {k: v for k, v in config_data.items() 
                           if k not in ["column_mapping", "default_values"]}
            config["solutions"][solution_name].update(common_config)
        else:
            # 일반 솔루션은 기존대로 저장
            config["solutions"][solution_name] = config_data
        
        config["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_config(config)
    
    def get_template(self, template_type: str, template_name: str) -> str:
        """템플릿 불러오기
        
        Args:
            template_type: 'top' 또는 'bottom'
            template_name: 템플릿 이름
        
        Returns:
            템플릿 내용
        """
        # 먼저 base_dir에서 찾기
        template_file = self.base_dir / "templates" / f"detail_{template_type}_templates.json"
        
        # PyInstaller 환경에서는 임시 폴더도 확인
        if not template_file.exists() and self._temp_templates_dir:
            template_file = self._temp_templates_dir / f"detail_{template_type}_templates.json"
        
        if not template_file.exists():
            return ""
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                templates = json.load(f)
                return templates.get(template_name, "")
        except Exception as e:
            print(f"[경고] 템플릿 로드 실패: {e}")
            return ""
    
    def save_template(self, template_type: str, template_name: str, content: str):
        """템플릿 저장"""
        template_file = self.base_dir / "templates" / f"detail_{template_type}_templates.json"
        template_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            if template_file.exists():
                with open(template_file, 'r', encoding='utf-8') as f:
                    templates = json.load(f)
            else:
                templates = {}
            
            templates[template_name] = content
            
            with open(template_file, 'w', encoding='utf-8') as f:
                json.dump(templates, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[오류] 템플릿 저장 실패: {e}")
    
    def list_templates(self, template_type: str) -> list:
        """템플릿 목록 가져오기"""
        # 먼저 base_dir에서 찾기
        template_file = self.base_dir / "templates" / f"detail_{template_type}_templates.json"
        
        # PyInstaller 환경에서는 임시 폴더도 확인
        if not template_file.exists() and self._temp_templates_dir:
            template_file = self._temp_templates_dir / f"detail_{template_type}_templates.json"
        
        if not template_file.exists():
            return []
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                templates = json.load(f)
                return list(templates.keys())
        except Exception as e:
            print(f"[경고] 템플릿 목록 로드 실패: {e}")
            return []
    
    def get_market_prefixes(self) -> Dict[str, str]:
        """마켓 접두사 목록 가져오기
        
        Returns:
            {접두사: 마켓명} 딕셔너리
        """
        config = self.load_config()
        default_prefixes = {
            "스스": "스스",
            "스마트스토어": "스스",
            "쿠팡": "쿠팡",
            "11번가": "11번가",
            "옥션": "옥션",
            "지마켓": "지마켓",
            "토스": "토스",
            "톡스토어": "톡스토어"
        }
        
        # 설정에서 커스텀 접두사 가져오기
        custom_prefixes = config.get("market_prefixes", {})
        
        # 기본 접두사와 커스텀 접두사 병합 (커스텀 접두사 우선)
        merged = default_prefixes.copy()
        merged.update(custom_prefixes)
        
        return merged
    
    def save_market_prefixes(self, prefixes: Dict[str, str]):
        """마켓 접두사 목록 저장
        
        Args:
            prefixes: {접두사: 마켓명} 딕셔너리
        """
        config = self.load_config()
        config["market_prefixes"] = prefixes
        config["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_config(config)
    
    def get_market_mapping_rules(self, solution_name: str, market: str) -> Dict:
        """마켓별 기본 매핑 규칙 가져오기
        
        Args:
            solution_name: 솔루션 이름
            market: 마켓 이름
            
        Returns:
            마켓별 기본 매핑 규칙 딕셔너리
        """
        config = self.load_config()
        solution_config = config.get("solutions", {}).get(solution_name, {})
        market_rules = solution_config.get("market_mapping_rules", {})
        return market_rules.get(market, {})
    
    def save_market_mapping_rules(self, solution_name: str, market: str, rules: Dict):
        """마켓별 기본 매핑 규칙 저장
        
        Args:
            solution_name: 솔루션 이름
            market: 마켓 이름
            rules: 마켓별 기본 매핑 규칙 딕셔너리
        """
        config = self.load_config()
        if "solutions" not in config:
            config["solutions"] = {}
        if solution_name not in config["solutions"]:
            config["solutions"][solution_name] = {}
        if "market_mapping_rules" not in config["solutions"][solution_name]:
            config["solutions"][solution_name]["market_mapping_rules"] = {}
        
        config["solutions"][solution_name]["market_mapping_rules"][market] = rules
        config["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_config(config)

