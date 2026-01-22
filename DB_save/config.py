"""
config.py

설정 및 마켓 계정 관리
- 엑셀 파일에서 마켓 계정 정보 로드
- Market_id_pw.xlsx 파일 관리 (시트별로 마켓 구분)
"""

import os
import json
import pandas as pd
from typing import List, Dict, Any, Optional


class AccountLoader:
    """엑셀 파일에서 마켓 계정 정보를 로드하는 클래스
    
    엑셀 파일 구조:
    - 마켓별로 시트로 구분
    - 공통 컬럼: "사용여부", "별칭", "아이디", "비밀번호"
    - 사용여부: Y(사용), N(정지/사용불가)
    - 별칭: 마켓명 + 명의자(A,B) + 사업자번호(1,2,3) + 마켓번호(-0,-1,-2)
    """
    
    def __init__(self, excel_path: Optional[str] = None):
        """
        Args:
            excel_path: 마켓 계정 정보가 담긴 엑셀 파일 경로
                       None이면 설정 파일에서 로드
        """
        self.excel_path = excel_path
        self.accounts = []
        self.config_file = "config_settings.json"
        
        # 설정 파일에서 경로 로드
        if not self.excel_path:
            self.excel_path = self._load_excel_path_from_config()
    
    def _load_excel_path_from_config(self) -> Optional[str]:
        """설정 파일에서 엑셀 파일 경로 로드"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config.get("excel_accounts_path")
            except:
                pass
        return None
    
    def save_excel_path_to_config(self, excel_path: str):
        """엑셀 파일 경로를 설정 파일에 저장"""
        config = {}
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            except:
                pass
        
        config["excel_accounts_path"] = excel_path
        
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[WARN] 설정 파일 저장 실패: {e}")
    
    def set_excel_path(self, excel_path: str):
        """엑셀 파일 경로 설정 및 저장"""
        self.excel_path = excel_path
        self.save_excel_path_to_config(excel_path)
    
    def load_accounts(self) -> List[Dict[str, Any]]:
        """엑셀 파일에서 마켓 계정 정보 로드 (시트별로 읽기)"""
        if not self.excel_path:
            print(f"[WARN] 엑셀 파일 경로가 설정되지 않았습니다.")
            return []
        
        if not os.path.exists(self.excel_path):
            print(f"[WARN] 엑셀 파일을 찾을 수 없습니다: {self.excel_path}")
            return []
        
        try:
            # 엑셀 파일 열기 (모든 시트 읽기)
            excel_file = pd.ExcelFile(self.excel_path, engine='openpyxl')
            
            # 공통 컬럼
            required_columns = ["사용여부", "별칭", "아이디", "비밀번호"]
            
            accounts = []
            
            # 각 시트(마켓)별로 읽기
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    
                    # 필수 컬럼 확인
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    if missing_columns:
                        print(f"[WARN] 시트 '{sheet_name}'에 필수 컬럼이 없습니다: {missing_columns}")
                        continue
                    
                    # 각 행 처리
                    for idx, row in df.iterrows():
                        # 사용여부 체크 (Y와 N 모두 로드, N은 비활성화)
                        use_flag = str(row.get("사용여부", "Y")).strip().upper()
                        is_active = (use_flag == "Y")
                        
                        # 공통 컬럼만 추출
                        alias_raw = str(row.get("별칭", "")).strip()
                        # 대괄호 제거: "스스A1-0 [0]" -> "스스A1-0"
                        alias = self._clean_alias(alias_raw)
                        user_id = str(row.get("아이디", "")).strip()
                        password = str(row.get("비밀번호", "")).strip()
                        
                        # 필수 정보 검증
                        if not alias or not user_id:
                            continue
                        
                        # 별칭에서 사업자번호 추출
                        business_number = self._extract_business_number_from_alias(alias)
                        
                        account = {
                            "market_name": alias,  # 별칭을 마켓명으로 사용
                            "sheet_name": sheet_name,  # 원본 시트명 (마켓 타입)
                            "business_number": business_number,
                            "user_id": user_id,
                            "password": password,
                            "alias": alias,  # 정제된 별칭
                            "alias_raw": alias_raw,  # 원본 별칭 보관
                            "is_active": is_active,  # 사용여부 플래그
                        }
                        
                        accounts.append(account)
                
                except Exception as e:
                    print(f"[WARN] 시트 '{sheet_name}' 처리 중 오류: {e}")
                    continue
            
            self.accounts = accounts
            print(f"[INFO] {len(accounts)}개의 마켓 계정을 로드했습니다.")
            return accounts
            
        except Exception as e:
            print(f"[ERROR] 엑셀 파일 로드 중 오류: {e}")
            import traceback
            print(traceback.format_exc())
            return []
    
    def _clean_alias(self, alias: str) -> str:
        """
        별칭에서 대괄호 부분 제거
        예: "스스A1-0 [0]" -> "스스A1-0"
        예: "쿠팡A2" -> "쿠팡A2"
        """
        import re
        # 대괄호와 그 안의 내용 제거
        cleaned = re.sub(r'\s*\[.*?\]\s*', '', alias).strip()
        return cleaned
    
    def parse_alias(self, alias: str) -> Dict[str, str]:
        """
        별칭을 파싱하여 계층 구조 정보 추출
        
        별칭 형식 (두 가지):
        1. 하이픈 포함: "옥션A2-3" -> {"market": "옥션", "owner": "A", "biz_num": "2", "store_num": "3"}
        2. 하이픈 없음: "쿠팡A2" -> {"market": "쿠팡", "owner": "A", "biz_num": "2", "store_num": "0"}
        
        구조:
        - 오픈마켓명: 시트명으로 구분 (옥션, 쿠팡, 스스 등)
        - 명의자: 알파벳 (A, B 등)
        - 사업자번호: 알파벳 바로 뒤 숫자 (해당 명의자의 몇 번째 사업자인지)
        - 스토어번호: 하이픈 뒤 숫자 (복수 스토어인 경우, 없으면 "0")
        
        Returns:
            {"market": 마켓명, "owner": 명의자, "biz_num": 사업자번호, "store_num": 스토어번호}
        """
        import re
        
        # 먼저 대괄호 제거 (혹시 모를 경우 대비)
        alias = self._clean_alias(alias)
        
        # 패턴 1: 하이픈 포함 (스토어번호 있음)
        # 예: "스스A1-0", "옥션A2-3"
        pattern_with_dash = r'^([가-힣a-zA-Z]+)([A-Z])(\d+)-(\d+)$'
        match = re.match(pattern_with_dash, alias)
        
        if match:
            market = match.group(1)
            owner = match.group(2)
            biz_num = match.group(3)
            store_num = match.group(4)
            
            return {
                "market": market,
                "owner": owner,
                "biz_num": biz_num,
                "store_num": store_num
            }
        
        # 패턴 2: 하이픈 없음 (스토어번호 없음, 기본값 "0")
        # 예: "쿠팡A2", "쿠팡B1"
        pattern_without_dash = r'^([가-힣a-zA-Z]+)([A-Z])(\d+)$'
        match = re.match(pattern_without_dash, alias)
        
        if match:
            market = match.group(1)
            owner = match.group(2)
            biz_num = match.group(3)
            store_num = "0"  # 하이픈이 없으면 스토어번호는 0
            
            return {
                "market": market,
                "owner": owner,
                "biz_num": biz_num,
                "store_num": store_num
            }
        
        # 파싱 실패 시 기본값 반환
        return {
            "market": alias,
            "owner": "",
            "biz_num": "",
            "store_num": "0"
        }
    
    def _extract_business_number_from_alias(self, alias: str) -> str:
        """
        별칭에서 사업자번호 추출 (하위 호환성)
        """
        parsed = self.parse_alias(alias)
        return parsed.get("biz_num", "")
    
    def build_tree_structure(self) -> Dict:
        """
        계정 데이터를 트리 구조로 변환
        
        Returns:
            {
                "시트명": {
                    "명의자": {
                        "사업자번호": {
                            "스토어": [account_dict, ...]
                        }
                    }
                }
            }
        """
        tree = {}
        
        for account in self.accounts:
            sheet_name = account.get("sheet_name", "")
            alias = account.get("alias", "")
            parsed = self.parse_alias(alias)
            
            market = parsed.get("market", "")
            owner = parsed.get("owner", "")
            biz_num = parsed.get("biz_num", "")
            
            # 트리 구조 생성
            if sheet_name not in tree:
                tree[sheet_name] = {}
            
            if owner not in tree[sheet_name]:
                tree[sheet_name][owner] = {}
            
            if biz_num not in tree[sheet_name][owner]:
                tree[sheet_name][owner][biz_num] = {}
            
            # 스토어별로 그룹화 (별칭을 스토어명으로 사용)
            if alias not in tree[sheet_name][owner][biz_num]:
                tree[sheet_name][owner][biz_num][alias] = []
            
            tree[sheet_name][owner][biz_num][alias].append(account)
        
        return tree
    
    def get_accounts_by_business_number(self, business_number: str) -> List[Dict[str, Any]]:
        """사업자번호로 계정 조회"""
        return [acc for acc in self.accounts if acc.get("business_number") == business_number]
    
    def get_account_by_market_name(self, market_name: str) -> Optional[Dict[str, Any]]:
        """마켓명(별칭)으로 계정 조회"""
        for acc in self.accounts:
            if acc.get("market_name") == market_name or acc.get("alias") == market_name:
                return acc
        return None
    
    def get_all_market_names(self) -> List[str]:
        """모든 마켓명(별칭) 리스트 반환"""
        return [acc.get("market_name") for acc in self.accounts if acc.get("market_name")]


# 명의자 정보 매핑
OWNER_NAMES = {
    "A": "고찬영",
    "B": "최빛나"
}

# 명의자+사업자번호 -> 상호명 매핑
BUSINESS_NAMES = {
    "A1": "굿투굿",
    "A2": "마이유통",
    "A3": "마이비타민",
    "A4": "러블리몰",
    "A5": "럽미몰",
    "B1": "마이짐",
    "B2": "샤이닝몰",
    "B3": "샤인몰",
    "B4": "순수한노을",
    "B5": "포근한정원",
    "B6": "푸르른초원",
    "B7": "행복한저택"
}

# 전역 설정
DEFAULT_DB_PATH = "products.db"
DEFAULT_EXCEL_ACCOUNTS_PATH = r"C:\Users\kohaz\Desktop\Python\파이썬자동화파일\마켓마감기\_internal\Market_id_pw.xlsx"
FIXED_DB_PATH = r"C:\Users\kohaz\Desktop\Python\파이썬자동화파일\상품가공프로그램\DB_save\products.db"


def load_db_path_from_config() -> str:
    """설정 파일에서 DB 파일 경로 로드"""
    config_file = "config_settings.json"
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                db_path = config.get("db_path")
                if db_path and os.path.exists(db_path):
                    return db_path
        except:
            pass
    
    # 설정 파일에 없거나 파일이 없으면 고정 경로 반환
    if os.path.exists(FIXED_DB_PATH):
        return FIXED_DB_PATH
    
    # 고정 경로도 없으면 기본 경로
    return DEFAULT_DB_PATH


def save_db_path_to_config(db_path: str):
    """DB 파일 경로를 설정 파일에 저장"""
    config_file = "config_settings.json"
    config = {}
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
        except:
            pass
    
    config["db_path"] = db_path
    
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] 설정 파일 저장 실패: {e}")

# 마켓별 설정 (필요시 확장)
MARKET_SETTINGS = {
    "쿠팡": {
        "api_endpoint": "https://api.coupang.com",
        "rate_limit": 100,  # 시간당 요청 제한
    },
    "네이버": {
        "api_endpoint": "https://api.commerce.naver.com",
        "rate_limit": 200,
    },
    "11번가": {
        "api_endpoint": "https://api.11st.co.kr",
        "rate_limit": 150,
    },
}

