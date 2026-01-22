"""
database/db_handler.py

SQLite 데이터베이스 관리 클래스
- DB 연결 및 테이블 생성
- 마켓 정보 삽입/조회
- 상품 데이터 삽입/조회
- 중복 체크 기능
"""

import os
import json
import sqlite3
import re
from datetime import datetime
from typing import Optional, Dict, List, Any

import pandas as pd

# 시즌 필터링 통합
# season_filter_manager_gui.py에서 함수 import
try:
    import sys
    season_filter_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if season_filter_path not in sys.path:
        sys.path.insert(0, season_filter_path)
    from season_filter_manager_gui import load_season_config, filter_products_by_season
    SEASON_FILTER_AVAILABLE = True
except ImportError:
    SEASON_FILTER_AVAILABLE = False
    # 함수가 없으면 무시 (선택적 기능)


class DBHandler:
    """SQLite 데이터베이스 핸들러"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._last_season_filter_info = None  # 마지막 시즌 필터링 정보 저장
    
    def connect(self):
        """데이터베이스 연결 및 테이블 생성"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # 딕셔너리 형태로 결과 반환
        
        # 대용량 데이터 처리를 위한 성능 최적화 설정
        cursor = self.conn.cursor()
        # WAL 모드 활성화 (동시 읽기 성능 향상)
        cursor.execute("PRAGMA journal_mode=WAL")
        # 페이지 크기 증가 (대용량 데이터 처리 성능 향상)
        cursor.execute("PRAGMA page_size=4096")
        # 캐시 크기 증가 (메모리 사용량 증가, 성능 향상)
        cursor.execute("PRAGMA cache_size=-64000")  # 64MB
        # 동기화 모드 (성능과 안전성 균형)
        cursor.execute("PRAGMA synchronous=NORMAL")
        # 외래키 체크 (필요시에만 활성화)
        cursor.execute("PRAGMA foreign_keys=ON")
        self.conn.commit()
        
        # 테이블이 없으면 생성
        self.create_tables()
        return self.conn
    
    def close(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def create_tables(self):
        """필요한 테이블 생성"""
        cursor = self.conn.cursor()
        
        # 1. 마켓 정보 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS markets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_name TEXT NOT NULL UNIQUE,
                market_id TEXT,
                business_number TEXT,
                business_name TEXT,
                contact_email TEXT,
                contact_phone TEXT,
                address TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 2. 상품 데이터 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id INTEGER,
                상품코드 TEXT,
                카테고리명 TEXT,
                원본상품명 TEXT,
                ST1_정제상품명 TEXT,
                ST1_판매형태 TEXT,
                ST2_JSON TEXT,
                ST3_결과상품명 TEXT,
                product_names_json TEXT,
                IMG_S1_휴먼라벨 TEXT,
                IMG_S1_휴먼노트 TEXT,
                IMG_S1_AI라벨 TEXT,
                view_point TEXT,
                subject_position TEXT,
                subject_size TEXT,
                lighting_condition TEXT,
                color_tone TEXT,
                shadow_presence TEXT,
                background_simplicity TEXT,
                is_flat_lay TEXT,
                bg_layout_hint_en TEXT,
                bg_positive_en TEXT,
                bg_negative_en TEXT,
                video_motion_prompt_en TEXT,
                video_full_prompt_en TEXT,
                누끼url TEXT,
                믹스url TEXT,
                product_status TEXT DEFAULT 'ACTIVE',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (market_id) REFERENCES markets(id)
            )
        """)
        
        # 3. 메타데이터 테이블
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                excel_filename TEXT NOT NULL,
                excel_path TEXT,
                total_rows INTEGER,
                processed_rows INTEGER,
                import_date TEXT DEFAULT CURRENT_TIMESTAMP,
                notes TEXT
            )
        """)
        
        # 4. 업로드 로그 테이블 (업로드 이력 기록)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS upload_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_number TEXT,
                market_id INTEGER,
                market_name TEXT,
                product_id INTEGER,
                product_code TEXT,
                used_product_name TEXT,
                used_nukki_url TEXT,
                used_mix_url TEXT,
                product_name_index INTEGER,
                image_nukki_index INTEGER,
                image_mix_index INTEGER,
                upload_strategy TEXT,
                upload_status TEXT,
                uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                FOREIGN KEY (market_id) REFERENCES markets(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # 5. 출고 히스토리 테이블 (데이터 출고 이력 기록)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS export_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_date TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                store_name TEXT,
                store_alias TEXT,
                business_number TEXT,
                categories TEXT,
                product_count INTEGER,
                file_path TEXT,
                file_name TEXT,
                memo TEXT,
                export_mode TEXT,
                exclude_assigned INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 6. 상품 조합 테이블 (상품코드별 가능한 모든 조합을 미리 저장)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_combinations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code TEXT NOT NULL,
                product_id INTEGER,
                combination_index INTEGER NOT NULL,
                url_type TEXT NOT NULL,
                line_index INTEGER NOT NULL,
                product_name TEXT NOT NULL,
                nukki_url TEXT,
                mix_url TEXT,
                st2_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_code, combination_index),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # 7. 조합 할당 테이블 (시트별로 어떤 조합이 어떤 스토어에 할당되었는지 기록)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS combination_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_name TEXT NOT NULL,
                business_number TEXT NOT NULL,
                product_code TEXT NOT NULL,
                combination_index INTEGER NOT NULL,
                assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sheet_name, business_number, product_code, combination_index)
            )
        """)
        
        # 8. 성능 최적화를 위한 인덱스 생성 (대용량 데이터 처리용)
        # 카테고리명 검색 최적화 (LIKE 쿼리 성능 향상)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_products_category_status 
            ON products(카테고리명, product_status)
        """)
        
        # 상품코드 검색 최적화
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_products_code 
            ON products(상품코드)
        """)
        
        # 상품 조합 테이블 인덱스
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_product_combinations_code 
            ON product_combinations(product_code, combination_index)
        """)
        
        # 조합 할당 테이블 인덱스
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_combination_assignments_sheet_store 
            ON combination_assignments(sheet_name, business_number, product_code)
        """)
        
        # 조합 할당 테이블 추가 인덱스 (시트별 조합 조회 최적화)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_combination_assignments_sheet_code 
            ON combination_assignments(sheet_name, product_code, combination_index)
        """)
        
        # 조합 테이블 추가 인덱스 (url_type, line_index 조회 최적화)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_product_combinations_type_line 
            ON product_combinations(product_code, url_type, line_index, combination_index)
        """)
        
        # URL 존재 여부 체크 최적화
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_products_urls 
            ON products(누끼url, 믹스url) 
            WHERE 누끼url IS NOT NULL OR 믹스url IS NOT NULL
        """)
        
        # upload_logs 테이블 조회 최적화 (시트별 중복 체크)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_upload_logs_market_status 
            ON upload_logs(market_name, upload_status)
        """)
        
        # upload_logs 상품코드 검색 최적화
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_upload_logs_code 
            ON upload_logs(product_code)
        """)
        
        # 복합 인덱스: 시트별 조합 체크 최적화
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_upload_logs_combination 
            ON upload_logs(market_name, product_code, used_mix_url, used_nukki_url, used_product_name)
        """)
        
        # export_history 테이블 조회 최적화
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_export_history_date 
            ON export_history(export_date)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_export_history_sheet 
            ON export_history(sheet_name)
        """)
        
        # 상품 조합 테이블 인덱스
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_product_combinations_code 
            ON product_combinations(product_code, combination_index)
        """)
        
        # 조합 할당 테이블 인덱스
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_combination_assignments_sheet_store 
            ON combination_assignments(sheet_name, business_number, product_code)
        """)
        
        self.conn.commit()
    
    def insert_market(self, market_data: Dict[str, Any]) -> int:
        """마켓 정보 삽입 (중복 체크 후)"""
        cursor = self.conn.cursor()
        
        # 마켓명으로 중복 체크
        cursor.execute("SELECT id FROM markets WHERE market_name = ?", (market_data.get("market_name"),))
        existing = cursor.fetchone()
        
        if existing:
            return existing[0]
        
        # 새 마켓 추가
        cursor.execute("""
            INSERT INTO markets (market_name, market_id, business_number, business_name, 
                               contact_email, contact_phone, address, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            market_data.get("market_name"),
            market_data.get("market_id"),
            market_data.get("business_number"),
            market_data.get("business_name"),
            market_data.get("contact_email"),
            market_data.get("contact_phone"),
            market_data.get("address"),
            datetime.now().isoformat()
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def insert_products(self, products_df: pd.DataFrame, market_id: int, excel_filename: str):
        """상품 데이터 삽입"""
        cursor = self.conn.cursor()
        
        # 상품명 관련 컬럼 목록
        product_name_columns = [
            "ST3_결과상품명", "ST3_결과_상품명", "ST3결과상품명", "ST4_최종결과", "ST1_정제상품명"
        ]
        
        inserted_count = 0
        skipped_count = 0
        
        for idx, row in products_df.iterrows():
            try:
                # 기본 필수 컬럼 추출
                product_data = {
                    "market_id": market_id,
                    "상품코드": str(row.get("상품코드", row.get("코드", ""))).strip() if pd.notna(row.get("상품코드", row.get("코드", ""))) else "",
                    "product_status": "ACTIVE",
                }
                
                # 상품명 JSON 처리 (줄바꿈으로 구분된 상품명들을 JSON 배열로 변환)
                product_names_list = []
                for col_name in product_name_columns:
                    if col_name in products_df.columns:
                        val = row.get(col_name)
                        if pd.notna(val) and str(val).strip():
                            names = [name.strip() for name in str(val).split('\n') if name.strip()]
                            if names:
                                product_names_list.extend(names)
                
                # 순서 유지하며 중복 제거
                if product_names_list:
                    product_names_list = list(dict.fromkeys(product_names_list))
                    product_data["product_names_json"] = json.dumps(product_names_list, ensure_ascii=False)
                    # 원본 ST3_결과상품명도 저장
                    if "ST3_결과상품명" in products_df.columns:
                        st3_val = row.get("ST3_결과상품명")
                        if pd.notna(st3_val):
                            product_data["ST3_결과상품명"] = str(st3_val).strip()
                
                # 엑셀의 모든 컬럼을 동적으로 추가
                for col in products_df.columns:
                    if col in ["상품코드", "코드"]:
                        continue
                    
                    val = row.get(col)
                    if pd.notna(val):
                        safe_col = re.sub(r'[^\w가-힣]', '_', str(col))
                        safe_col = re.sub(r'_+', '_', safe_col)
                        safe_col = safe_col.strip('_')
                        if safe_col and safe_col not in product_data:
                            product_data[safe_col] = str(val).strip()
                
                # 기존 products 테이블에 없는 컬럼이면 ALTER TABLE로 추가
                cursor.execute("PRAGMA table_info(products)")
                existing_cols = [col[1] for col in cursor.fetchall()]
                
                for col in product_data.keys():
                    if col not in existing_cols and col not in ["id", "created_at", "updated_at", "market_id"]:
                        try:
                            cursor.execute(f'ALTER TABLE products ADD COLUMN "{col}" TEXT')
                        except sqlite3.OperationalError:
                            pass
                
                # 동적 INSERT 쿼리 생성
                columns = list(product_data.keys())
                placeholders = ", ".join(["?"] * len(columns))
                col_names = ", ".join([f'"{col}"' for col in columns])
                
                cursor.execute(f"""
                    INSERT INTO products ({col_names}, updated_at)
                    VALUES ({placeholders}, ?)
                """, list(product_data.values()) + [datetime.now().isoformat()])
                
                inserted_count += 1
                
            except Exception as e:
                skipped_count += 1
                continue
        
        self.conn.commit()
        return inserted_count, skipped_count
    
    def insert_metadata(self, excel_filename: str, excel_path: str, total_rows: int, processed_rows: int, notes: str = ""):
        """메타데이터 삽입"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO metadata (excel_filename, excel_path, total_rows, processed_rows, notes, import_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (excel_filename, excel_path, total_rows, processed_rows, notes, datetime.now().isoformat()))
        self.conn.commit()
    
    def get_all_categories(self) -> List[str]:
        """모든 카테고리 조회"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT 카테고리명 FROM products WHERE 카테고리명 IS NOT NULL AND 카테고리명 != '' AND product_status = 'ACTIVE'")
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    
    def get_category_product_counts(self) -> Dict[str, int]:
        """카테고리별 상품 수 조회"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 카테고리명, COUNT(*) as count 
            FROM products 
            WHERE 카테고리명 IS NOT NULL 
            AND 카테고리명 != '' 
            AND product_status = 'ACTIVE'
            GROUP BY 카테고리명
        """)
        rows = cursor.fetchall()
        return {row[0]: row[1] for row in rows}
    
    def get_category_tree(self) -> Dict[str, Any]:
        """
        카테고리 트리 구조 생성 (대>중 형식)
        
        Returns:
            {
                "대카테고리1": {
                    "중카테고리1": ["대>중>소>세부", ...],
                    "중카테고리2": [...]
                },
                ...
            }
        """
        categories = self.get_all_categories()
        tree = {}
        
        for category in categories:
            if not category:
                continue
            
            # '대>중>소>세부' 형식 파싱
            parts = [part.strip() for part in category.split('>')]
            if len(parts) >= 2:
                large = parts[0]
                medium = parts[1]
                
                if large not in tree:
                    tree[large] = {}
                if medium not in tree[large]:
                    tree[large][medium] = []
                
                tree[large][medium].append(category)
        
        return tree
    
    def get_products_by_category(self, category: str, market_ids: List[int] = None, status: str = 'ACTIVE') -> List[Dict]:
        """카테고리로 상품 조회"""
        cursor = self.conn.cursor()
        
        query = """
            SELECT * FROM products 
            WHERE 카테고리명 LIKE ? AND product_status = ?
        """
        params = [f"%{category}%", status]
        
        if market_ids:
            placeholders = ",".join(["?"] * len(market_ids))
            query += f" AND market_id IN ({placeholders})"
            params.extend(market_ids)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def get_products_for_upload(self, category: str, sheet_name: str, business_number: str, status: str = 'ACTIVE', exclude_assigned: bool = True, season_filter_enabled: bool = True) -> List[Dict]:
        """
        마켓 업로드용 상품 조회 (새로운 조합 테이블 사용)
        - product_combinations 테이블에서 조합 조회
        - combination_assignments 테이블에서 할당 정보 확인
        - 시트별로 중복 체크 (같은 시트 내에서만 중복 불가)
        - 시즌 필터링 적용 (선택적)
        
        Args:
            category: 카테고리명
            sheet_name: 시트명 (마켓 타입)
            business_number: 사업자번호
            status: 상품 상태 (기본: ACTIVE)
            exclude_assigned: 이미 배정된 상품코드 제외 여부 (기본: True)
            season_filter_enabled: 시즌 필터링 활성화 여부 (기본: True)
            
        Returns:
            업로드 가능한 상품 리스트 (조합 인덱스 순서로 정렬)
        """
        cursor = self.conn.cursor()
        
        # 1. 카테고리 패턴 생성
        category_parts = [part.strip() for part in category.split('>')]
        if len(category_parts) >= 2:
            large_cat = category_parts[0].strip()
            medium_cat = category_parts[1].strip()
            category_pattern = f"%{large_cat}%>%{medium_cat}%"
        else:
            category_pattern = f"%{category}%"
        
        # 2. 이미 할당된 조합 인덱스 조회 (시트 전체)
        # 중요: 시트별로 조합이 고유해야 하므로 시트 전체에서 사용된 조합을 확인
        cursor.execute("""
            SELECT DISTINCT combination_index, product_code
            FROM combination_assignments 
            WHERE sheet_name = ?
        """, (sheet_name,))
        
        sheet_used_combinations = {}  # {product_code: set(combination_indices)}
        for row in cursor.fetchall():
            combo_idx, pc = row
            if pc and combo_idx is not None:
                if pc not in sheet_used_combinations:
                    sheet_used_combinations[pc] = set()
                sheet_used_combinations[pc].add(combo_idx)
        
        # 3. 스토어별로 이미 사용한 상품코드 확인
        store_used_product_codes = set()
        if exclude_assigned and business_number:
            cursor.execute("""
                SELECT DISTINCT product_code
                FROM combination_assignments 
                WHERE sheet_name = ? AND business_number = ?
            """, (sheet_name, business_number))
            for row in cursor.fetchall():
                if row[0]:
                    store_used_product_codes.add(row[0])
        
        # 4. 카테고리로 상품 조회 (상품명과 카테고리 포함하여 조회 - 시즌 필터링용)
        # 출력 가능 기준: 상품명(product_names_json)만 있어도 가능
        cursor.execute("""
            SELECT DISTINCT p.상품코드, p.원본상품명, p.ST3_결과상품명, p.ST1_정제상품명, p.카테고리명
            FROM products p
            WHERE p.카테고리명 LIKE ? 
            AND p.product_status = ?
            AND p.product_names_json IS NOT NULL 
            AND p.product_names_json != '' 
            AND p.product_names_json != '[]'
        """, (category_pattern, status))
        
        products_with_info = []
        for row in cursor.fetchall():
            product_code = row[0]
            if product_code:
                # 상품명 추출 (시즌 필터링용)
                원본상품명 = row[1] or ""
                ST3_결과상품명 = row[2] or ""
                ST1_정제상품명 = row[3] or ""
                카테고리명 = row[4] or ""
                # 우선순위: 원본상품명 > ST3_결과상품명 > ST1_정제상품명
                product_name = 원본상품명 or ST3_결과상품명 or ST1_정제상품명
                
                products_with_info.append({
                    "상품코드": product_code,
                    "상품명": product_name,
                    "product_name": product_name,  # 시즌 필터링 함수 호환성
                    "카테고리명": 카테고리명,  # 카테고리도 포함
                })
        
        # 5. 시즌 필터링 적용 (활성화되어 있고 함수가 사용 가능한 경우)
        self._last_season_filter_info = None  # 초기화
        
        if season_filter_enabled and SEASON_FILTER_AVAILABLE:
            try:
                # 시즌 설정 파일 경로
                script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")
                json_path = os.path.join(script_dir, "season_filters.json")
                
                # 시즌 설정 로드
                season_config = load_season_config(excel_path, json_path)
                
                if season_config:
                    # 원본 상품 수 저장
                    original_count = len(products_with_info)
                    
                    # 시즌 필터링 적용
                    filtered_products, excluded_count, excluded_seasons, included_seasons, season_stats = filter_products_by_season(
                        products_with_info, season_config
                    )
                    
                    # 필터링된 상품코드만 추출
                    filtered_product_codes = {p.get("상품코드") for p in filtered_products if p.get("상품코드")}
                    
                    # 필터링 전 상품 수 저장 (검증용)
                    before_filter_count = len(products_with_info)
                    
                    # 필터링된 상품코드로 업데이트 (중요: 실제 필터링 적용)
                    products_with_info = [p for p in products_with_info if p.get("상품코드") in filtered_product_codes]
                    
                    # 필터링 적용 확인 (검증)
                    after_filter_count = len(products_with_info)
                    
                    # 시즌 필터링 정보를 결과에 포함 (나중에 로그 출력용)
                    self._last_season_filter_info = {
                        'excluded_count': excluded_count,
                        'excluded_seasons': excluded_seasons,
                        'included_seasons': included_seasons,
                        'season_stats': season_stats,
                        'original_count': original_count,
                        'filtered_count': after_filter_count,
                        'before_filter_count': before_filter_count  # 검증용
                    }
                    
                    # 디버깅: 필터링 적용 확인
                    # 제외할 상품이 없으면 필터링 전후가 동일한 것이 정상이므로 경고하지 않음
                    if before_filter_count != after_filter_count:
                        # 필터링이 정상적으로 적용됨
                        pass
                    elif excluded_count > 0:
                        # 제외할 상품이 있는데 필터링 전후가 동일하면 경고
                        import logging
                        logging.warning(
                            f"[시즌 필터링 검증] 필터링 전후 상품 수가 동일합니다: "
                            f"{before_filter_count}개 → {after_filter_count}개 "
                            f"(제외 예상: {excluded_count}개, 필터링이 적용되지 않았을 수 있음)"
                        )
                    # excluded_count == 0이면 제외할 상품이 없으므로 경고하지 않음 (정상)
                else:
                    # 시즌 설정 파일이 없을 때 - 필터링 없이 진행 (경고만)
                    self._last_season_filter_info = {
                        'error': '시즌 설정 파일을 찾을 수 없습니다. 필터링 없이 진행합니다.',
                        'season_stats': {'non_season': len(products_with_info), 'season_valid': 0, 'season_invalid': 0},
                        'original_count': len(products_with_info),
                        'filtered_count': len(products_with_info)  # 필터링 없음
                    }
            except Exception as e:
                # 시즌 필터링 오류 시 - 필터링 없이 진행 (경고만)
                import traceback
                error_detail = traceback.format_exc()
                self._last_season_filter_info = {
                    'error': f'시즌 필터링 오류: {str(e)}. 필터링 없이 진행합니다.',
                    'error_detail': error_detail,
                    'season_stats': {'non_season': len(products_with_info), 'season_valid': 0, 'season_invalid': 0},
                    'original_count': len(products_with_info),
                    'filtered_count': len(products_with_info)  # 필터링 없음
                }
        elif season_filter_enabled and not SEASON_FILTER_AVAILABLE:
            # 시즌 필터링 모듈이 사용 불가능한 경우 - 필터링 없이 진행
            self._last_season_filter_info = {
                'error': '시즌 필터링 모듈을 사용할 수 없습니다. 필터링 없이 진행합니다.',
                'season_stats': {'non_season': len(products_with_info), 'season_valid': 0, 'season_invalid': 0},
                'original_count': len(products_with_info),
                'filtered_count': len(products_with_info)  # 필터링 없음
            }
        
        # 필터링된 상품코드 추출 (시즌 필터링이 적용된 상태)
        product_codes = [p.get("상품코드") for p in products_with_info if p.get("상품코드")]
        
        # 필터링 적용 최종 검증 (시즌 필터링이 활성화된 경우)
        if season_filter_enabled and hasattr(self, '_last_season_filter_info') and self._last_season_filter_info:
            expected_filtered_count = self._last_season_filter_info.get('filtered_count', len(product_codes))
            actual_product_code_count = len(product_codes)
            
            if actual_product_code_count != expected_filtered_count:
                import logging
                logging.warning(
                    f"[시즌 필터링 최종 검증] 불일치 감지: "
                    f"예상 필터링 수={expected_filtered_count}개, "
                    f"실제 상품코드 수={actual_product_code_count}개 "
                    f"(조합 조회 시 필터링이 적용되지 않았을 수 있음)"
                )
        
        # 5. 각 상품코드별로 사용 가능한 조합 조회 (필터링된 상품코드만 사용)
        result = []
        for product_code in product_codes:
            # exclude_assigned가 True이고 해당 스토어에서 이미 사용한 상품코드면 제외
            if exclude_assigned and product_code in store_used_product_codes:
                continue
            
            # 사용 가능한 조합 조회 (시트 전체에서 사용되지 않은 조합)
            used_indices = sheet_used_combinations.get(product_code, set())
            
            # 성능 최적화: 필요한 만큼만 조회 (LIMIT 사용)
            # 실제로는 첫 번째 사용 가능한 조합만 필요하지만, 
            # 여러 조합을 미리 로드하여 선택의 여지 확보
            fetch_limit = 100  # 최대 100개 조합만 조회 (대용량 데이터 처리 성능 향상)
            
            if used_indices:
                # 사용된 인덱스가 많을 경우 성능 저하 방지
                if len(used_indices) > 1000:
                    # 너무 많은 경우 EXISTS 서브쿼리 사용
                    cursor.execute("""
                        SELECT * FROM product_combinations 
                        WHERE product_code = ? 
                        AND NOT EXISTS (
                            SELECT 1 FROM combination_assignments ca
                            WHERE ca.sheet_name = ?
                            AND ca.product_code = product_combinations.product_code
                            AND ca.combination_index = product_combinations.combination_index
                        )
                        ORDER BY combination_index ASC
                        LIMIT ?
                    """, (product_code, sheet_name, fetch_limit))
                else:
                    placeholders = ','.join('?' * len(used_indices))
                    cursor.execute(f"""
                        SELECT * FROM product_combinations 
                        WHERE product_code = ? 
                        AND combination_index NOT IN ({placeholders})
                        ORDER BY combination_index ASC
                        LIMIT ?
                    """, [product_code] + list(used_indices) + [fetch_limit])
            else:
                cursor.execute("""
                    SELECT * FROM product_combinations 
                    WHERE product_code = ? 
                    ORDER BY combination_index ASC
                    LIMIT ?
                """, (product_code, fetch_limit))
            
            for row in cursor.fetchall():
                combo = dict(row)
                # 반환 형식 통일 (기존 형식과 호환)
                result.append({
                    "상품코드": combo.get("product_code", ""),
                    "누끼url": combo.get("nukki_url", "") or "",
                    "믹스url": combo.get("mix_url", "") or "",
                    "ST4_최종결과": combo.get("product_name", ""),
                    "product_id": combo.get("product_id"),
                    "product_names_json": "",  # 조합 테이블에는 저장 안 함
                    "ST2_JSON": combo.get("st2_json", "") or "",
                    "url_type": combo.get("url_type", "mix"),
                    "line_index": combo.get("line_index", 0),
                    "combination_index": combo.get("combination_index", 0)  # 새로 추가
                })
        
        return result
    
    def get_incomplete_products(self, category: str = None, status: str = 'ACTIVE') -> List[Dict]:
        """
        미완료 DB 조회 (재가공용)
        - 완료 DB 기준: product_names_json이 있으면 완료
        - 미완료 DB 기준: 누끼url, 믹스url, product_names_json 중 1개라도 누락이면 미완료
        
        Args:
            category: 카테고리명 (None이면 전체)
            status: 상품 상태 (기본: ACTIVE)
            
        Returns:
            미완료 상품 리스트 (전체 컬럼)
        """
        cursor = self.conn.cursor()
        
        # 미완료 기준: 누끼url, 믹스url, product_names_json 중 1개라도 누락이면 미완료
        query = """
            SELECT * FROM products 
            WHERE product_status = ?
            AND (
                (누끼url IS NULL OR 누끼url = '')
                OR (믹스url IS NULL OR 믹스url = '')
                OR (product_names_json IS NULL OR product_names_json = '' OR product_names_json = '[]')
            )
        """
        params = [status]
        
        if category:
            query = query.replace("WHERE", "WHERE 카테고리명 LIKE ? AND")
            params.insert(0, f"%{category}%")
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def check_business_duplicate(self, business_number: str, product_code: str = None) -> bool:
        """
        사업자 그룹에 이미 올린 상품인지 중복 체크
        
        중복 판단 기준:
        - business_number (사업자번호)와 product_code (상품코드) 조합
        - upload_logs 테이블에서 upload_status = 'SUCCESS'인 레코드 확인
        
        Args:
            business_number: 사업자번호
            product_code: 상품코드
            
        Returns:
            True: 중복 존재 (이미 업로드됨), False: 중복 없음 (업로드 가능)
        """
        cursor = self.conn.cursor()
        
        if not business_number or not product_code:
            return False
        
        # upload_logs 테이블에서 중복 체크
        # 같은 사업자번호와 상품코드로 성공적으로 업로드된 이력이 있는지 확인
        cursor.execute("""
            SELECT COUNT(*) FROM upload_logs 
            WHERE business_number = ? 
            AND product_code = ? 
            AND upload_status = 'SUCCESS'
        """, (business_number, product_code))
        
        count = cursor.fetchone()[0]
        return count > 0
    
    def log_upload(self, business_number: str, market_id: int, market_name: str,
                   product_id: int, product_code: str,
                   used_product_name: str, used_nukki_url: str, used_mix_url: str,
                   product_name_index: int, image_nukki_index: int = None, image_mix_index: int = None,
                   upload_strategy: str = "", upload_status: str = "SUCCESS", notes: str = ""):
        """
        업로드 로그 기록
        
        Args:
            business_number: 사업자번호
            market_id: 마켓 ID
            market_name: 마켓명 (별칭)
            product_id: 상품 ID
            product_code: 상품코드
            used_product_name: 사용한 상품명
            used_nukki_url: 사용한 누끼 이미지 URL
            used_mix_url: 사용한 연출 이미지 URL
            product_name_index: 사용한 상품명 인덱스
            image_nukki_index: 사용한 누끼 이미지 인덱스
            image_mix_index: 사용한 연출 이미지 인덱스
            upload_strategy: 업로드 전략 (JSON)
            upload_status: 업로드 상태 (SUCCESS/FAILED)
            notes: 추가 메모
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO upload_logs (
                business_number, market_id, market_name, product_id, product_code,
                used_product_name, used_nukki_url, used_mix_url,
                product_name_index, image_nukki_index, image_mix_index,
                upload_strategy, upload_status, notes, uploaded_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            business_number, market_id, market_name, product_id, product_code,
            used_product_name, used_nukki_url, used_mix_url,
            product_name_index, image_nukki_index, image_mix_index,
            upload_strategy, upload_status, notes, datetime.now().isoformat()
        ))
        self.conn.commit()
    
    def get_upload_logs_by_product_code(self, product_code: str, sheet_name: str = None) -> List[Dict]:
        """
        상품코드별 업로드 로그 조회
        
        Args:
            product_code: 상품코드
            sheet_name: 시트명 (None이면 전체)
            
        Returns:
            업로드 로그 리스트
        """
        cursor = self.conn.cursor()
        
        if sheet_name:
            cursor.execute("""
                SELECT * FROM upload_logs 
                WHERE product_code = ? AND market_name = ? AND upload_status = 'SUCCESS'
                ORDER BY uploaded_at DESC
            """, (product_code, sheet_name))
        else:
            cursor.execute("""
                SELECT * FROM upload_logs 
                WHERE product_code = ? AND upload_status = 'SUCCESS'
                ORDER BY uploaded_at DESC
            """, (product_code,))
        
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def get_upload_logs_by_market(self, market_name: str) -> List[Dict]:
        """
        마켓별 업로드 로그 조회
        
        Args:
            market_name: 시트명 (마켓 타입)
            
        Returns:
            업로드 로그 리스트
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM upload_logs 
            WHERE market_name = ? AND upload_status = 'SUCCESS'
            ORDER BY uploaded_at DESC
        """, (market_name,))
        
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def log_export_history(self, export_date: str, sheet_name: str, store_name: str = None,
                           store_alias: str = None, business_number: str = None,
                           categories: str = None, product_count: int = 0,
                           file_path: str = None, file_name: str = None,
                           memo: str = None, export_mode: str = None,
                           exclude_assigned: bool = True) -> int:
        """
        출고 히스토리 기록
        
        Args:
            export_date: 출고 날짜 (YYYYMMDD 형식)
            sheet_name: 시트명 (오픈마켓명)
            store_name: 스토어명
            store_alias: 스토어 별칭
            business_number: 사업자번호
            categories: 카테고리 목록 (JSON 문자열 또는 쉼표 구분)
            product_count: 상품 개수
            file_path: 파일 경로
            file_name: 파일명
            memo: 메모
            export_mode: 출고 모드 (예: "마켓 업로드용", "미완료 DB")
            exclude_assigned: 새로운 DB만 출력 옵션
            
        Returns:
            기록된 히스토리 ID
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO export_history (
                export_date, sheet_name, store_name, store_alias, business_number,
                categories, product_count, file_path, file_name, memo,
                export_mode, exclude_assigned, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            export_date, sheet_name, store_name, store_alias, business_number,
            categories, product_count, file_path, file_name, memo,
            export_mode, 1 if exclude_assigned else 0, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def get_export_history(self, limit: int = 100, sheet_name: str = None, 
                         start_date: str = None, end_date: str = None) -> List[Dict]:
        """
        출고 히스토리 조회
        
        Args:
            limit: 조회 개수 제한
            sheet_name: 시트명 필터 (None이면 전체)
            start_date: 시작 날짜 (YYYYMMDD 형식)
            end_date: 종료 날짜 (YYYYMMDD 형식)
            
        Returns:
            출고 히스토리 리스트 (최신순)
        """
        cursor = self.conn.cursor()
        
        query = "SELECT * FROM export_history WHERE 1=1"
        params = []
        
        if sheet_name:
            query += " AND sheet_name = ?"
            params.append(sheet_name)
        
        if start_date:
            query += " AND export_date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND export_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def generate_and_save_product_combinations(self, product_code: str = None, force_regenerate: bool = False, update_existing: bool = True):
        """
        상품코드별로 가능한 모든 조합을 생성하여 DB에 저장
        
        Args:
            product_code: 특정 상품코드만 처리 (None이면 전체)
            force_regenerate: 이미 존재하는 조합도 재생성할지 여부
            update_existing: 기존 조합이 있으면 업데이트할지 여부 (상품명/URL 변경 대응)
            
        Returns:
            생성된 조합 개수
        """
        import json
        cursor = self.conn.cursor()
        
        # 처리할 상품 조회
        if product_code:
            cursor.execute("""
                SELECT * FROM products 
                WHERE 상품코드 = ? AND product_status = 'ACTIVE'
            """, (product_code,))
        else:
            # 조합 생성 기준: 상품명(product_names_json)만 있으면 조합 생성 가능
            # URL은 조합 생성 시에만 사용 (믹스url과 누끼url 둘 다 있으면 두 URL 모두 사용, 없으면 상품명만으로 조합)
            cursor.execute("""
                SELECT * FROM products 
                WHERE product_status = 'ACTIVE'
                AND product_names_json IS NOT NULL 
                AND product_names_json != '' 
                AND product_names_json != '[]'
            """)
        
        products = [dict(row) for row in cursor.fetchall()]
        
        total_combinations = 0
        batch_size = 1000  # 배치 처리 크기 (대용량 데이터 처리 성능 향상)
        batch_data = []  # 배치 INSERT용 데이터
        
        for product in products:
            pc = product.get("상품코드", "")
            if not pc:
                continue
            
            # 이미 조합이 존재하는지 확인
            cursor.execute("""
                SELECT COUNT(*) FROM product_combinations WHERE product_code = ?
            """, (pc,))
            existing_count = cursor.fetchone()[0]
            
            if existing_count > 0:
                if not force_regenerate and not update_existing:
                    continue  # 이미 존재하고 업데이트 안 함
                elif update_existing:
                    # 기존 조합 삭제 후 재생성 (상품명/URL 변경 대응)
                    # 단, 이미 할당된 조합은 유지하기 위해 combination_assignments 확인
                    cursor.execute("""
                        SELECT DISTINCT combination_index 
                        FROM combination_assignments 
                        WHERE product_code = ?
                    """, (pc,))
                    assigned_indices = {row[0] for row in cursor.fetchall()}
                    
                    # 할당되지 않은 조합만 삭제
                    if assigned_indices:
                        placeholders = ','.join('?' * len(assigned_indices))
                        cursor.execute(f"""
                            DELETE FROM product_combinations 
                            WHERE product_code = ? 
                            AND combination_index NOT IN ({placeholders})
                        """, [pc] + list(assigned_indices))
                    else:
                        # 할당된 조합이 없으면 모두 삭제
                        cursor.execute("DELETE FROM product_combinations WHERE product_code = ?", (pc,))
                elif force_regenerate:
                    # 강제 재생성 (기존 할당 기록도 무시)
                    cursor.execute("DELETE FROM product_combinations WHERE product_code = ?", (pc,))
            
            # 상품명 추출
            product_names_json = product.get("product_names_json", "[]")
            product_names = []
            try:
                names = json.loads(product_names_json) if product_names_json else []
                if names:
                    product_names = [str(name).strip() for name in names if str(name).strip()]
            except:
                pass
            
            if not product_names:
                # ST4_최종결과에서 추출
                st4_value = product.get("ST4_최종결과", "")
                if st4_value:
                    product_names = [line.strip() for line in str(st4_value).split('\n') if line.strip()]
            
            if not product_names:
                continue  # 상품명이 없으면 건너뛰기
            
            mix_url = (product.get("믹스url", "") or "").strip()
            nukki_url = (product.get("누끼url", "") or "").strip()
            product_id = product.get("id")
            st2_json = product.get("ST2_JSON", "") or ""
            
            # 기존 조합의 최대 인덱스 확인 (할당된 조합 유지)
            cursor.execute("""
                SELECT MAX(combination_index) FROM product_combinations WHERE product_code = ?
            """, (pc,))
            max_existing_index = cursor.fetchone()[0] or -1
            
            # 조합 생성 (상품명 중심, 우선순위 순서대로)
            # 상품명 개수만큼 반복하면서 URL 타입을 교차로 할당
            combination_index = max_existing_index + 1
            
            # URL 존재 여부 확인
            has_nukki = bool(nukki_url)
            has_mix = bool(mix_url)
            
            if has_nukki and has_mix:
                # 둘 다 존재: 누끼+1번, 믹스+2번, 누끼+3번, 믹스+4번, ..., 누끼+9번, 믹스+10번
                # 그 다음: 믹스+1번, 누끼+2번, ..., 믹스+9번, 누끼+10번
                num_names = len(product_names)
                
                # 첫 번째 라운드: 누끼+홀수번, 믹스+짝수번
                for line_index in range(num_names):
                    if line_index % 2 == 0:  # 홀수번 (0, 2, 4, 6, 8) -> 1, 3, 5, 7, 9번 상품명
                        # 누끼형
                        batch_data.append((pc, product_id, combination_index, "nukki", line_index, 
                                          product_names[line_index], nukki_url, mix_url, st2_json))
                        combination_index += 1
                    else:  # 짝수번 (1, 3, 5, 7, 9) -> 2, 4, 6, 8, 10번 상품명
                        # 믹스형
                        batch_data.append((pc, product_id, combination_index, "mix", line_index, 
                                          product_names[line_index], nukki_url, mix_url, st2_json))
                        combination_index += 1
                
                # 두 번째 라운드: 믹스+홀수번, 누끼+짝수번 (역순)
                for line_index in range(num_names):
                    if line_index % 2 == 0:  # 홀수번 (0, 2, 4, 6, 8) -> 1, 3, 5, 7, 9번 상품명
                        # 믹스형
                        batch_data.append((pc, product_id, combination_index, "mix", line_index, 
                                          product_names[line_index], nukki_url, mix_url, st2_json))
                        combination_index += 1
                    else:  # 짝수번 (1, 3, 5, 7, 9) -> 2, 4, 6, 8, 10번 상품명
                        # 누끼형
                        batch_data.append((pc, product_id, combination_index, "nukki", line_index, 
                                          product_names[line_index], nukki_url, mix_url, st2_json))
                        combination_index += 1
                        
            elif has_nukki:
                # 누끼url만 존재: 누끼+1번, 누끼+2번, ..., 누끼+10번
                for line_index in range(len(product_names)):
                    batch_data.append((pc, product_id, combination_index, "nukki", line_index, 
                                      product_names[line_index], nukki_url, mix_url, st2_json))
                    combination_index += 1
                    
            elif has_mix:
                # 믹스url만 존재: 믹스+1번, 믹스+2번, ..., 믹스+10번
                for line_index in range(len(product_names)):
                    batch_data.append((pc, product_id, combination_index, "mix", line_index, 
                                      product_names[line_index], nukki_url, mix_url, st2_json))
                    combination_index += 1
            
            # URL이 없는 경우: 상품명만
            if not mix_url and not nukki_url:
                for line_index in range(len(product_names)):
                    batch_data.append((pc, product_id, combination_index, "name_only", line_index, 
                                      product_names[line_index], "", "", st2_json))
                    combination_index += 1
            
            # 배치 크기에 도달하면 일괄 INSERT
            if len(batch_data) >= batch_size:
                try:
                    cursor.executemany("""
                        INSERT OR IGNORE INTO product_combinations 
                        (product_code, product_id, combination_index, url_type, line_index, 
                         product_name, nukki_url, mix_url, st2_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch_data)
                    total_combinations += len(batch_data)
                    batch_data = []
                    # 중간 커밋 (대용량 데이터 처리 시 메모리 사용량 감소)
                    if total_combinations % (batch_size * 10) == 0:
                        self.conn.commit()
                except Exception as e:
                    # 배치 실패 시 개별 처리로 폴백
                    for data in batch_data:
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO product_combinations 
                                (product_code, product_id, combination_index, url_type, line_index, 
                                 product_name, nukki_url, mix_url, st2_json)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, data)
                            total_combinations += 1
                        except:
                            pass
                    batch_data = []
        
        # 남은 배치 데이터 처리
        if batch_data:
            try:
                cursor.executemany("""
                    INSERT OR IGNORE INTO product_combinations 
                    (product_code, product_id, combination_index, url_type, line_index, 
                     product_name, nukki_url, mix_url, st2_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_data)
                total_combinations += len(batch_data)
            except Exception as e:
                # 배치 실패 시 개별 처리로 폴백
                for data in batch_data:
                    try:
                        cursor.execute("""
                            INSERT OR IGNORE INTO product_combinations 
                            (product_code, product_id, combination_index, url_type, line_index, 
                             product_name, nukki_url, mix_url, st2_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, data)
                        total_combinations += 1
                    except:
                        pass
        
        self.conn.commit()
        return total_combinations
    
    def migrate_existing_assignments(self):
        """
        기존 upload_logs에서 조합 할당 정보를 마이그레이션
        
        Returns:
            마이그레이션된 할당 개수
        """
        import json
        cursor = self.conn.cursor()
        
        # 1. 먼저 모든 상품의 조합 생성 (없는 경우만)
        self.generate_and_save_product_combinations(update_existing=False)
        
        # 2. upload_logs에서 할당 정보 읽기
        cursor.execute("""
            SELECT DISTINCT 
                market_name, business_number, product_code,
                used_product_name, used_nukki_url, used_mix_url,
                upload_strategy
            FROM upload_logs 
            WHERE upload_status = 'SUCCESS'
            AND product_code IS NOT NULL
            AND product_code != ''
        """)
        
        migrated_count = 0
        
        for row in cursor.fetchall():
            sheet_name, business_number, product_code, used_name, used_nukki, used_mix, strategy_json = row
            
            if not product_code or not sheet_name:
                continue
            
            # upload_strategy에서 line_index 추출
            line_index = None
            url_type = None
            try:
                if strategy_json:
                    strategy = json.loads(strategy_json) if isinstance(strategy_json, str) else strategy_json
                    line_index = strategy.get("line_index", strategy.get("product_name_index"))
                    url_type = strategy.get("url_type", "mix")
            except:
                # strategy가 없으면 URL로 판단
                if used_nukki:
                    url_type = "nukki"
                elif used_mix:
                    url_type = "mix"
                else:
                    url_type = "name_only"
            
            # product_combinations에서 해당 조합 찾기
            if line_index is not None:
                cursor.execute("""
                    SELECT combination_index FROM product_combinations
                    WHERE product_code = ? 
                    AND url_type = ? 
                    AND line_index = ?
                    AND product_name = ?
                """, (product_code, url_type, line_index, used_name))
            else:
                # line_index가 없으면 URL과 상품명으로만 찾기
                if url_type == "nukki":
                    cursor.execute("""
                        SELECT combination_index FROM product_combinations
                        WHERE product_code = ? 
                        AND url_type = 'nukki'
                        AND product_name = ?
                        AND nukki_url = ?
                    """, (product_code, used_name, used_nukki))
                elif url_type == "mix":
                    cursor.execute("""
                        SELECT combination_index FROM product_combinations
                        WHERE product_code = ? 
                        AND url_type = 'mix'
                        AND product_name = ?
                        AND mix_url = ?
                    """, (product_code, used_name, used_mix))
                else:
                    cursor.execute("""
                        SELECT combination_index FROM product_combinations
                        WHERE product_code = ? 
                        AND url_type = 'name_only'
                        AND product_name = ?
                    """, (product_code, used_name))
            
            combo_row = cursor.fetchone()
            if combo_row:
                combination_index = combo_row[0]
                
                # combination_assignments에 기록
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO combination_assignments 
                        (sheet_name, business_number, product_code, combination_index)
                        VALUES (?, ?, ?, ?)
                    """, (sheet_name, business_number, product_code, combination_index))
                    migrated_count += 1
                except:
                    pass  # 이미 존재하면 무시
        
        self.conn.commit()
        return migrated_count
    
    def get_next_available_combination(self, product_code: str, sheet_name: str, business_number: str = None) -> Optional[Dict]:
        """
        상품코드에 대해 시트 내에서 사용 가능한 다음 조합을 반환
        
        Args:
            product_code: 상품코드
            sheet_name: 시트명
            business_number: 사업자번호 (스토어별로 상품코드당 1개만 사용)
            
        Returns:
            사용 가능한 조합 정보 또는 None
        """
        cursor = self.conn.cursor()
        
        # 스토어별로 이미 사용한 상품코드 확인
        if business_number:
            cursor.execute("""
                SELECT DISTINCT product_code 
                FROM combination_assignments 
                WHERE sheet_name = ? AND business_number = ? AND product_code = ?
            """, (sheet_name, business_number, product_code))
            if cursor.fetchone():
                return None  # 해당 스토어에서 이미 사용한 상품코드
        
        # 이미 할당된 조합 인덱스 조회 (시트 전체)
        cursor.execute("""
            SELECT DISTINCT combination_index 
            FROM combination_assignments 
            WHERE sheet_name = ? AND product_code = ?
        """, (sheet_name, product_code))
        
        used_indices = {row[0] for row in cursor.fetchall()}
        
        # 사용 가능한 조합 조회 (가장 낮은 인덱스부터)
        if used_indices:
            placeholders = ','.join('?' * len(used_indices))
            cursor.execute(f"""
                SELECT * FROM product_combinations 
                WHERE product_code = ? 
                AND combination_index NOT IN ({placeholders})
                ORDER BY combination_index ASC
                LIMIT 1
            """, [product_code] + list(used_indices))
        else:
            cursor.execute("""
                SELECT * FROM product_combinations 
                WHERE product_code = ? 
                ORDER BY combination_index ASC
                LIMIT 1
            """, (product_code,))
        
        row = cursor.fetchone()
        if row:
            return dict(row)
        
        return None
    
    def assign_combination(self, product_code: str, combination_index: int, sheet_name: str, business_number: str):
        """
        조합을 스토어에 할당 기록
        
        Args:
            product_code: 상품코드
            combination_index: 조합 인덱스
            sheet_name: 시트명
            business_number: 사업자번호
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO combination_assignments 
            (sheet_name, business_number, product_code, combination_index)
            VALUES (?, ?, ?, ?)
        """, (sheet_name, business_number, product_code, combination_index))
        self.conn.commit()
    
    def sync_combinations_for_new_products(self, progress_callback=None):
        """
        새로운 상품이나 업데이트된 상품에 대해 조합을 동기화
        - 조합이 없는 상품: 조합 생성
        - 조합이 있지만 상품명/URL이 변경된 상품: 조합 업데이트 (할당된 조합은 유지)
        
        Args:
            progress_callback: 진행 상황 콜백 함수 (current, total) -> None
        
        Returns:
            동기화된 상품 개수
        """
        cursor = self.conn.cursor()
        
        # 조합이 없는 상품 찾기 (상품명만 있으면 조합 생성 가능)
        cursor.execute("""
            SELECT DISTINCT p.상품코드
            FROM products p
            WHERE p.product_status = 'ACTIVE'
            AND p.product_names_json IS NOT NULL 
            AND p.product_names_json != '' 
            AND p.product_names_json != '[]'
            AND NOT EXISTS (
                SELECT 1 FROM product_combinations pc 
                WHERE pc.product_code = p.상품코드
            )
        """)
        
        new_product_codes = [row[0] for row in cursor.fetchall()]
        total_count = len(new_product_codes)
        
        if total_count == 0:
            return 0
        
        # 진행 상황 콜백이 없으면 None으로 유지 (호출 시 체크)
        
        # 배치 처리로 성능 향상
        synced_count = 0
        
        # 배치 크기로 나누어 처리 (메모리 효율성 및 진행 상황 표시)
        batch_size = 50  # 50개씩 배치 처리 (더 자주 진행 상황 업데이트)
        
        if progress_callback:
            progress_callback(0, total_count)
        
        for i in range(0, total_count, batch_size):
            batch_codes = new_product_codes[i:i+batch_size]
            
            # 배치 내 각 상품에 대해 조합 생성
            for idx, pc in enumerate(batch_codes):
                current = i + idx + 1
                try:
                    count = self.generate_and_save_product_combinations(product_code=pc, update_existing=False)
                    if count > 0:
                        synced_count += 1
                except Exception as e:
                    # 개별 상품 오류는 무시하고 계속 진행
                    pass
                
                # 진행 상황 업데이트 (매 10개마다 또는 배치 마지막)
                if progress_callback and (current % 10 == 0 or current == i + len(batch_codes) or current == total_count):
                    progress_callback(current, total_count)
            
            # 배치마다 커밋 (성능 향상 및 진행 상황 저장)
            self.conn.commit()
        
        # 조합이 있지만 상품명/URL이 변경된 상품 확인 및 업데이트
        # (이 부분은 필요시 구현 - 현재는 조합이 없으면 생성만 함)
        
        return synced_count

