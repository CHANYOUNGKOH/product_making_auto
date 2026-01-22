"""
markets/manager.py

업로드 총괄 관리
- 중복 체크 (check_business_duplicate)
- 업로드 전략 배정 (get_upload_strategy)
- 실제 마켓 API 호출
- 업로드 로그 기록
"""

import os
import sys
import json
import time
from typing import List, Dict, Any, Optional, Callable

# 상위 디렉토리에서 모듈 import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_handler import DBHandler
from config import AccountLoader


class UploadManager:
    """업로드 총괄 관리자"""
    
    def __init__(self, db_handler: DBHandler, account_loader: AccountLoader):
        """
        Args:
            db_handler: 데이터베이스 핸들러
            account_loader: 계정 로더
        """
        self.db_handler = db_handler
        self.account_loader = account_loader
    
    def check_business_duplicate(self, business_number: str, product_code: str) -> bool:
        """
        사업자 그룹에 이미 올린 상품인지 전수 조사
        
        Args:
            business_number: 사업자번호
            product_code: 상품코드
            
        Returns:
            True: 중복 존재, False: 중복 없음
        """
        return self.db_handler.check_business_duplicate(business_number, product_code)
    
    def get_upload_strategy(self, product: Dict, business_number: str) -> Dict[str, Any]:
        """
        안 쓴 이미지(누끼/연출)와 상품명 번호를 배정
        - 믹스url 우선 사용
        - ST3_결과상품명 상단 1줄 사용
        
        Args:
            product: 상품 정보 딕셔너리
            business_number: 사업자번호
            
        Returns:
            업로드 전략 딕셔너리
            {
                "image_nukki_index": 0,  # 사용할 누끼 이미지 인덱스
                "image_mix_index": 0,    # 사용할 연출 이미지 인덱스 (믹스url 우선)
                "product_name_index": 0, # 사용할 상품명 인덱스 (ST3 첫 줄)
                "strategy_id": "unique_id"
            }
        """
        # 상품명 JSON 파싱
        product_names_json = product.get("product_names_json", "[]")
        try:
            product_names = json.loads(product_names_json) if product_names_json else []
        except:
            product_names = []
        
        # 이미지 URL 파싱 (믹스url 우선)
        nukki_url = product.get("누끼url", "")
        mix_url = product.get("믹스url", "")
        
        # 업로드 로그에서 이미 사용한 전략 확인
        cursor = self.db_handler.conn.cursor()
        cursor.execute("""
            SELECT product_name_index, image_nukki_index, image_mix_index 
            FROM upload_logs 
            WHERE business_number = ? AND product_id = ? AND upload_status = 'SUCCESS'
        """, (business_number, product.get("id")))
        
        used_indices = cursor.fetchall()
        used_name_indices = {row[0] for row in used_indices if row[0] is not None}
        used_nukki_indices = {row[1] for row in used_indices if row[1] is not None}
        used_mix_indices = {row[2] for row in used_indices if row[2] is not None}
        
        # 사용 가능한 전략 찾기 (아직 사용하지 않은 인덱스 찾기)
        strategy = {
            "image_nukki_index": 0,
            "image_mix_index": 0,
            "product_name_index": 0,
            "strategy_id": f"{product.get('id')}_0_0_0"
        }
        
        # ST3_결과상품명에서 첫 번째 줄 추출 (상단 1줄)
        st3_value = product.get("ST3_결과상품명", "")
        if st3_value:
            first_line = str(st3_value).split('\n')[0].strip()
        else:
            # ST3_결과상품명이 없으면 product_names_json에서 첫 번째 추출
            first_line = product_names[0] if product_names else ""
        
        # 사용하지 않은 상품명 인덱스 찾기 (첫 번째 줄 우선)
        if product_names:
            # 첫 번째 상품명(인덱스 0)이 사용 가능한지 확인
            if 0 not in used_name_indices:
                strategy["product_name_index"] = 0
                strategy["product_name"] = first_line if first_line else product_names[0]
            else:
                # 첫 번째가 이미 사용됨 - 다음 사용 가능한 것 찾기
                for idx, name in enumerate(product_names):
                    if idx not in used_name_indices:
                        strategy["product_name_index"] = idx
                        strategy["product_name"] = name
                        break
                else:
                    # 모든 상품명을 사용했으면 첫 번째 사용 (재사용)
                    strategy["product_name_index"] = 0
                    strategy["product_name"] = first_line if first_line else product_names[0]
        else:
            # 상품명이 없으면 원본 상품명 사용
            strategy["product_name"] = product.get("원본상품명", product.get("ST1_정제상품명", ""))
            strategy["product_name_index"] = 0
        
        # 이미지 URL 처리 (믹스url 우선)
        strategy["nukki_url"] = nukki_url
        strategy["mix_url"] = mix_url  # 믹스url 우선 사용
        
        # 전략 ID 생성
        strategy["strategy_id"] = f"{product.get('id')}_{strategy['product_name_index']}_{strategy['image_nukki_index']}_{strategy['image_mix_index']}"
        
        return strategy
    
    def process_upload(self, category: str, selected_markets: List[str], 
                      log_callback: Optional[Callable[[str], None]] = None) -> Dict[str, int]:
        """
        업로드 프로세스 실행
        
        Args:
            category: 카테고리명
            selected_markets: 선택된 마켓명 리스트
            log_callback: 로그 출력 콜백 함수
            
        Returns:
            {"success": 성공 건수, "failed": 실패 건수}
        """
        def log(msg: str):
            if log_callback:
                log_callback(msg)
            else:
                print(msg)
        
        success_count = 0
        failed_count = 0
        
        # 1. 카테고리로 상품 조회
        log(f"카테고리 '{category}' 상품 조회 중...")
        products = self.db_handler.get_products_by_category(category)
        log(f"총 {len(products)}개 상품 발견")
        
        if not products:
            return {"success": 0, "failed": 0}
        
        # 2. 선택된 마켓별로 처리
        for market_name in selected_markets:
            log(f"\n=== 마켓: {market_name} ===")
            
            # 마켓 계정 정보 가져오기
            account = self.account_loader.get_account_by_market_name(market_name)
            if not account:
                log(f"⚠️ 마켓 계정을 찾을 수 없습니다: {market_name}")
                continue
            
            # 별칭에서 사업자번호 추출 (또는 별도 매핑 필요)
            business_number = account.get("business_number", "")
            user_id = account.get("user_id", "")
            password = account.get("password", "")
            
            if not user_id:
                log(f"⚠️ 아이디가 없습니다: {market_name}")
                continue
            
            # 3. 안전 검증: 중복 체크
            log(f"중복 체크 중... (사업자번호: {business_number})")
            
            # 4. 각 상품에 대해 업로드 처리
            for product in products:
                product_code = product.get("상품코드", "")
                product_id = product.get("id")
                
                try:
                    # 중복 체크
                    is_duplicate = self.check_business_duplicate(business_number, product_code)
                    if is_duplicate:
                        log(f"⏭️  상품 스킵 (중복): {product_code}")
                        continue
                    
                    # 업로드 전략 배정
                    strategy = self.get_upload_strategy(product, business_number)
                    log(f"📦 상품 처리: {product_code} (전략: {strategy.get('strategy_id')})")
                    
                    # 실제 마켓 API 호출 (여기서는 시뮬레이션)
                    # account에 user_id, password가 포함되어 있음
                    upload_status = self._upload_to_market(market_name, product, strategy, account)
                    
                    if upload_status == "SUCCESS":
                        # 업로드 로그 기록 (어떤 마켓, 어떤 상품명, 어떤 이미지를 사용했는지 기록)
                        market_id = self._get_market_id_by_name(market_name)
                        self.db_handler.log_upload(
                            business_number=business_number,
                            market_id=market_id,
                            market_name=market_name,  # 마켓명 (별칭)
                            product_id=product_id,
                            product_code=product_code,
                            used_product_name=strategy.get("product_name", ""),  # 사용한 상품명
                            used_nukki_url=strategy.get("nukki_url", ""),  # 사용한 누끼 이미지
                            used_mix_url=strategy.get("mix_url", ""),  # 사용한 연출 이미지
                            product_name_index=strategy.get("product_name_index", 0),  # 상품명 인덱스
                            image_nukki_index=strategy.get("image_nukki_index", 0),  # 누끼 이미지 인덱스
                            image_mix_index=strategy.get("image_mix_index", 0),  # 연출 이미지 인덱스
                            upload_strategy=json.dumps(strategy),
                            upload_status="SUCCESS",
                            notes=f"카테고리: {category}"
                        )
                        success_count += 1
                        log(f"✅ 업로드 성공: {product_code}")
                    else:
                        failed_count += 1
                        log(f"❌ 업로드 실패: {product_code}")
                    
                    # API 호출 간 딜레이 (rate limit 방지)
                    time.sleep(0.5)
                    
                except Exception as e:
                    failed_count += 1
                    log(f"❌ 오류 발생: {product_code} - {e}")
                    continue
        
        return {"success": success_count, "failed": failed_count}
    
    def _upload_to_market(self, market_name: str, product: Dict, strategy: Dict, 
                          account: Dict) -> str:
        """
        실제 마켓 API로 업로드 (시뮬레이션)
        
        Args:
            market_name: 마켓명
            product: 상품 정보
            strategy: 업로드 전략
            account: 계정 정보
            
        Returns:
            "SUCCESS" or "FAILED"
        """
        # 실제 구현에서는 markets/coupang.py, markets/naver.py 등을 호출
        # 여기서는 시뮬레이션만 수행
        
        # 예시: 쿠팡 API 호출
        if "쿠팡" in market_name:
            # return self._upload_to_coupang(product, strategy, account)
            pass
        # 예시: 네이버 API 호출
        elif "네이버" in market_name or "스마트스토어" in market_name:
            # return self._upload_to_naver(product, strategy, account)
            pass
        
        # 시뮬레이션: 성공으로 가정
        time.sleep(0.1)  # API 호출 시뮬레이션
        return "SUCCESS"
    
    def _get_market_id_by_name(self, market_name: str) -> Optional[int]:
        """마켓명으로 market_id 조회"""
        cursor = self.db_handler.conn.cursor()
        cursor.execute("SELECT id FROM markets WHERE market_name = ?", (market_name,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    # 실제 마켓별 업로드 함수들은 별도 파일로 분리 가능
    # def _upload_to_coupang(self, product, strategy, account):
    #     from markets.coupang import upload_product
    #     return upload_product(product, strategy, account)
    #
    # def _upload_to_naver(self, product, strategy, account):
    #     from markets.naver import upload_product
    #     return upload_product(product, strategy, account)

