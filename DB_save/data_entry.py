"""
data_entry.py

데이터 입고 도구 (엑셀 → SQLite)
- 기능: 엑셀 파일을 불러와 SQLite DB로 변환
- 테이블: 마켓 정보 테이블, 상품 데이터 테이블
- 역할: 상품명/이미지 스테이지 완료된 신상 데이터를 DB에 저장
"""

import os
import sys
import json
import sqlite3
import re
import threading
import shutil
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple
from urllib.parse import urlparse

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText


# ========================================================
# 툴팁 클래스
# ========================================================
class ToolTip:
    """위젯에 마우스를 올렸을 때 도움말을 보여주는 툴팁 클래스"""
    def __init__(self, widget, text: str, wraplength: int = 400):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tipwindow = None
        self.widget.bind("<Enter>", self.show_tip)
        self.widget.bind("<Leave>", self.hide_tip)
    
    def show_tip(self, event=None):
        """툴팁 표시"""
        if self.tipwindow or not self.text:
            return
        
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 5
        
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        
        label = tk.Label(
            tw, text=self.text, justify="left",
            background="#ffffe0", relief="solid", borderwidth=1,
            font=("맑은 고딕", 9), wraplength=self.wraplength
        )
        label.pack(ipadx=6, ipady=4)
    
    def hide_tip(self, event=None):
        """툴팁 숨김"""
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None


# ========================================================
# SQLite DB 관리 클래스
# ========================================================
class SQLiteDBManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """데이터베이스 연결"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # 딕셔너리 형태로 결과 반환
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
        
        # 2. 상품 데이터 테이블 (동적 컬럼 지원을 위한 기본 구조)
        # 엑셀의 모든 컬럼을 저장할 수 있도록 TEXT 타입으로 유연하게 설계
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
                ST4_최종결과 TEXT,
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
        
        # 3. 메타데이터 테이블 (엑셀 파일 정보, 처리 이력 등)
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
    
    def validate_product_data(self, product_data: Dict, row_index: int, excel_product_codes: Dict[str, int]) -> Tuple[bool, List[str], List[str]]:
        """
        상품 데이터 검증
        
        Returns:
            (is_valid, critical_errors, warnings)
            - is_valid: True면 입고 가능, False면 입고 불가 (상품코드 문제 등)
            - critical_errors: 입고를 막는 치명적 오류 (상품코드 없음, 중복 등)
            - warnings: 경고 메시지 (JSON 형식 오류, URL 형식 오류 등 - 입고는 가능하지만 로그에 기록)
        """
        critical_errors = []  # 입고를 막는 치명적 오류
        warnings = []  # 경고 메시지 (입고는 가능)
        
        # 1. 상품코드 검증 (치명적 오류)
        product_code = product_data.get("상품코드", "").strip()
        if not product_code:
            critical_errors.append("상품코드가 없습니다")
        else:
            # 같은 엑셀 파일 내 중복 체크
            if product_code in excel_product_codes and excel_product_codes[product_code] != row_index:
                critical_errors.append(f"상품코드 중복: {product_code} (행 {excel_product_codes[product_code] + 1}에도 존재)")
        
        # 2. URL 형식 검증 (치명적 오류 - 상품코드가 URL에 포함되어야 함)
        product_code = product_data.get("상품코드", "").strip()
        if product_code:
            # 누끼url 검증
            nukki_url = product_data.get("누끼url", "").strip()
            if nukki_url:
                # URL 형식 검증
                try:
                    result = urlparse(nukki_url)
                    if not all([result.scheme, result.netloc]):
                        critical_errors.append(f"누끼url 형식이 올바르지 않습니다: {nukki_url[:50]}")
                    else:
                        # URL에 상품코드가 포함되어야 함 (예: WFG6BB6_01.jpg)
                        expected_suffix = f"{product_code}_01.jpg"
                        if expected_suffix not in nukki_url:
                            critical_errors.append(f"누끼url에 상품코드가 포함되지 않습니다. 예상: ...{expected_suffix} (실제: {nukki_url[:80]})")
                except Exception as e:
                    critical_errors.append(f"누끼url 형식 오류: {nukki_url[:50]} ({str(e)})")
            
            # 믹스url 검증
            mix_url = product_data.get("믹스url", "").strip()
            if mix_url:
                # URL 형식 검증
                try:
                    result = urlparse(mix_url)
                    if not all([result.scheme, result.netloc]):
                        critical_errors.append(f"믹스url 형식이 올바르지 않습니다: {mix_url[:50]}")
                    else:
                        # URL에 상품코드가 포함되어야 함 (예: WFG6BB6_02.jpg)
                        expected_suffix = f"{product_code}_02.jpg"
                        if expected_suffix not in mix_url:
                            critical_errors.append(f"믹스url에 상품코드가 포함되지 않습니다. 예상: ...{expected_suffix} (실제: {mix_url[:80]})")
                except Exception as e:
                    critical_errors.append(f"믹스url 형식 오류: {mix_url[:50]} ({str(e)})")
        
        # 3. JSON 형식 검증 (ST2_JSON) - 불완전하면 빈 문자열로 저장하고 입고
        st2_json = product_data.get("ST2_JSON", "")
        if st2_json:
            # 문자열이 아닌 경우 문자열로 변환
            if not isinstance(st2_json, str):
                st2_json = str(st2_json)
            st2_json_original = st2_json.strip()
            
            if st2_json_original:
                # 불완전한 JSON 감지 (토큰 부족으로 잘린 경우)
                is_truncated = False
                truncated_indicators = []
                
                # 1. JSON이 닫히지 않은 경우 감지
                if not st2_json_original.rstrip().endswith('}') and not st2_json_original.rstrip().endswith(']'):
                    is_truncated = True
                    truncated_indicators.append("JSON이 닫히지 않음 (마지막 문자가 } 또는 ]가 아님)")
                
                # 2. 중괄호/대괄호 짝 확인
                open_braces = st2_json_original.count('{')
                close_braces = st2_json_original.count('}')
                open_brackets = st2_json_original.count('[')
                close_brackets = st2_json_original.count(']')
                
                if open_braces > close_braces:
                    is_truncated = True
                    truncated_indicators.append(f"중괄호 불일치 (열림: {open_braces}, 닫힘: {close_braces})")
                if open_brackets > close_brackets:
                    is_truncated = True
                    truncated_indicators.append(f"대괄호 불일치 (열림: {open_brackets}, 닫힘: {close_brackets})")
                
                # 3. JSON 파싱 시도
                try:
                    json.loads(st2_json_original)
                    # 파싱 성공 → 정상 JSON, 그대로 사용
                except json.JSONDecodeError as e:
                    # 파싱 실패 → 불완전한 JSON
                    # 빈 문자열로 저장하고 입고 (경고만 기록)
                    warnings.append(f"⚠️ ST2_JSON이 불완전하여 빈 값으로 저장됩니다 (출력 토큰 부족으로 잘림 가능)")
                    if truncated_indicators:
                        for indicator in truncated_indicators:
                            warnings.append(f"  → {indicator}")
                    warnings.append(f"  → JSON 길이: {len(st2_json_original)}자")
                    warnings.append(f"  → 마지막 50자: ...{st2_json_original[-50:]}")
                    warnings.append(f"  → 해결 방법: Stage2 배치 작업을 재실행하거나 max_tokens를 늘려주세요.")
                    # product_data에서 ST2_JSON을 빈 문자열로 설정 (나중에 저장 시 반영)
                    product_data["ST2_JSON"] = ""
        
        # 치명적 오류가 없으면 입고 가능
        is_valid = len(critical_errors) == 0
        return is_valid, critical_errors, warnings
    
    def insert_products(self, products_df: pd.DataFrame, excel_filename: str, 
                       progress_callback=None, error_log_callback=None):
        """
        상품 데이터 삽입 (지정된 컬럼만 기록)
        
        Args:
            products_df: 상품 데이터프레임
            excel_filename: 엑셀 파일명
            progress_callback: 진행률 콜백 함수 (current, total)
            error_log_callback: 에러 로그 콜백 함수 (row_index, error_message)
        """
        cursor = self.conn.cursor()
        
        # 기록해야 할 컬럼 목록 (정확한 컬럼명)
        required_columns = [
            # 기본 정보
            "상품코드",
            "카테고리명",
            "원본상품명",
            # Stage 1
            "ST1_정제상품명",
            "ST1_판매형태",
            # Stage 2~4 및 이미지/프롬프트/URL (가공 결과 컬럼들)
            "ST2_JSON",
            "ST3_결과상품명",
            "ST4_최종결과",
            "IMG_S1_휴먼라벨",
            "IMG_S1_휴먼노트",
            "IMG_S1_AI라벨",
            "view_point",
            "subject_position",
            "subject_size",
            "lighting_condition",
            "color_tone",
            "shadow_presence",
            "background_simplicity",
            "is_flat_lay",
            "bg_layout_hint_en",
            "bg_positive_en",
            "bg_negative_en",
            "video_motion_prompt_en",
            "video_full_prompt_en",
            "누끼url",
            "믹스url"
        ]

        # "가공된 정보" 컬럼 목록 (이 중 하나라도 값이 있어야 입고/업데이트 대상이 됨)
        processed_columns = [
            # Stage 1 결과도 가공 데이터로 인정
            "ST1_정제상품명",
            "ST1_판매형태",
            "ST2_JSON",
            "ST3_결과상품명",
            "ST4_최종결과",
            "IMG_S1_휴먼라벨",
            "IMG_S1_휴먼노트",
            "IMG_S1_AI라벨",
            "view_point",
            "subject_position",
            "subject_size",
            "lighting_condition",
            "color_tone",
            "shadow_presence",
            "background_simplicity",
            "is_flat_lay",
            "bg_layout_hint_en",
            "bg_positive_en",
            "bg_negative_en",
            "video_motion_prompt_en",
            "video_full_prompt_en",
            "누끼url",
            "믹스url",
        ]
        
        # 기존 상품코드 조회 (중복 체크용)
        cursor.execute("SELECT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
        existing_product_codes = {row[0] for row in cursor.fetchall()}
        
        # 엑셀 파일 내 상품코드 중복 체크용
        excel_product_codes = {}  # {상품코드: 행번호}
        
        inserted_count = 0  # 신규 추가
        updated_count = 0   # 업데이트
        skipped_count = 0
        validation_error_count = 0  # 검증 오류
        updated_columns = set()  # 업데이트된 컬럼 추적
        error_log = []  # 에러 로그
        
        total_rows = len(products_df)
        
        for idx, row in products_df.iterrows():
            try:
                # 진행률 업데이트
                if progress_callback:
                    progress_callback(idx + 1, total_rows)
                # 상품코드 추출
                product_code = ""
                if "상품코드" in products_df.columns:
                    val = row.get("상품코드")
                    if pd.notna(val):
                        product_code = str(val).strip()
                
                # 기본 데이터 초기화
                product_data = {
                    "product_status": "ACTIVE",  # 기본 상태값
                }
                
                # 지정된 컬럼만 읽기 (공란 허용)
                for col_name in required_columns:
                    if col_name in products_df.columns:
                        val = row.get(col_name)
                        if pd.notna(val):
                            product_data[col_name] = str(val).strip()
                        else:
                            # 공란인 경우 빈 문자열로 저장
                            product_data[col_name] = ""
                    else:
                        # 컬럼이 없는 경우 빈 문자열로 저장
                        product_data[col_name] = ""
                
                # ST4_최종결과를 JSON 배열로 변환 (줄바꿈으로 구분)
                # 맨 상단 상품명이 가장 품질이 좋으므로 순서 유지가 중요
                st4_value = product_data.get("ST4_최종결과", "")
                if st4_value:
                    # 줄바꿈으로 구분된 상품명들을 리스트로 변환 (순서 유지)
                    product_names_list = [name.strip() for name in st4_value.split('\n') if name.strip()]
                    if product_names_list:
                        # 순서 유지하며 중복 제거 (첫 번째 항목 우선)
                        product_names_list = list(dict.fromkeys(product_names_list))
                        product_data["product_names_json"] = json.dumps(product_names_list, ensure_ascii=False)
                    else:
                        product_data["product_names_json"] = "[]"
                else:
                    product_data["product_names_json"] = "[]"
                
                # 데이터 검증
                is_valid, critical_errors, warnings = self.validate_product_data(product_data, idx, excel_product_codes)
                if not is_valid:
                    # 치명적 오류 (상품코드 없음, 중복 등) - 입고 불가
                    validation_error_count += 1
                    error_msg = f"행 {idx + 2}: {', '.join(critical_errors)}"
                    error_log.append(error_msg)
                    if error_log_callback:
                        error_log_callback(idx + 2, error_msg)
                    skipped_count += 1
                    continue
                
                # 경고 메시지가 있으면 로그에 기록 (입고는 계속 진행)
                # ST2_JSON이 불완전한 경우 빈 문자열로 저장하도록 설정
                if warnings:
                    warning_msg = f"행 {idx + 2}: [경고] {', '.join(warnings[:3])}"  # 처음 3개만 표시
                    if error_log_callback:
                        error_log_callback(idx + 2, warning_msg)
                    
                    # ST2_JSON 불완전 경고가 있으면 빈 문자열로 저장
                    if any("ST2_JSON이 불완전" in w for w in warnings):
                        product_data["ST2_JSON"] = ""

                # 가공된 컬럼 중 하나라도 값이 있는지 확인
                has_processed_value = False
                for col_name in processed_columns:
                    val = product_data.get(col_name, "")
                    if val is None:
                        continue
                    if isinstance(val, str):
                        if val.strip():
                            has_processed_value = True
                            break
                    else:
                        if str(val).strip():
                            has_processed_value = True
                            break

                # 모든 가공 컬럼이 비어 있으면 입고/업데이트 대상이 아님 → 스킵
                if not has_processed_value:
                    skipped_count += 1
                    continue
                
                # 검증 통과 후 엑셀 내 상품코드 추적 (중복 체크용)
                if product_code:
                    excel_product_codes[product_code] = idx
                
                # 기존 상품인지 확인
                is_existing = product_code and product_code in existing_product_codes
                
                if is_existing:
                    # 기존 상품 업데이트
                    # 기존 데이터 조회
                    cursor.execute("SELECT * FROM products WHERE 상품코드 = ?", (product_code,))
                    existing_row = cursor.fetchone()
                    
                    if existing_row:
                        existing_data = dict(existing_row)
                        # 변경된 컬럼 확인
                        changed_columns = []
                        for col_name in required_columns:
                            new_value = product_data.get(col_name, "")
                            old_value = existing_data.get(col_name, "")
                            if new_value != old_value:
                                changed_columns.append(col_name)
                                updated_columns.add(col_name)
                        
                        if changed_columns:
                            # 가공 컬럼 기준으로 실제로 업데이트가 필요한지 한 번 더 체크
                            changed_processed = [c for c in changed_columns if c in processed_columns]
                            if not changed_processed:
                                # 기본 정보(ST1 등)만 바뀐 경우 → 스킵 처리
                                skipped_count += 1
                                continue

                            # 업데이트 쿼리 생성 (변경된 컬럼만)
                            set_clauses = []
                            update_values = []
                            for col_name in changed_columns:
                                set_clauses.append(f'"{col_name}" = ?')
                                update_values.append(product_data[col_name])
                            
                            # product_names_json도 업데이트
                            if "product_names_json" in product_data:
                                set_clauses.append('"product_names_json" = ?')
                                update_values.append(product_data["product_names_json"])
                            
                            # updated_at 업데이트
                            set_clauses.append('"updated_at" = ?')
                            update_values.append(datetime.now().isoformat())
                            
                            # 업데이트 실행
                            update_query = f"""
                                UPDATE products 
                                SET {', '.join(set_clauses)}
                                WHERE 상품코드 = ?
                            """
                            update_values.append(product_code)
                            cursor.execute(update_query, update_values)
                            updated_count += 1
                        else:
                            # 변경사항 없음
                            skipped_count += 1
                    else:
                        # 상품코드는 있지만 데이터가 없는 경우 (이상 케이스)
                        # INSERT로 처리
                        columns = list(product_data.keys())
                        placeholders = ", ".join(["?"] * len(columns))
                        col_names = ", ".join([f'"{col}"' for col in columns])
                        
                        cursor.execute(f"""
                            INSERT INTO products ({col_names}, updated_at)
                            VALUES ({placeholders}, ?)
                        """, list(product_data.values()) + [datetime.now().isoformat()])
                        inserted_count += 1
                        existing_product_codes.add(product_code)  # 추가했으므로 목록에 추가
                else:
                    # 신규 상품 추가
                    columns = list(product_data.keys())
                    placeholders = ", ".join(["?"] * len(columns))
                    col_names = ", ".join([f'"{col}"' for col in columns])
                    
                    cursor.execute(f"""
                        INSERT INTO products ({col_names}, updated_at)
                        VALUES ({placeholders}, ?)
                    """, list(product_data.values()) + [datetime.now().isoformat()])
                    inserted_count += 1
                    if product_code:
                        existing_product_codes.add(product_code)  # 추가했으므로 목록에 추가
                
            except Exception as e:
                skipped_count += 1
                error_msg = f"행 {idx + 2}: {str(e)}"
                error_log.append(error_msg)
                if error_log_callback:
                    error_log_callback(idx + 2, error_msg)
                continue
        
        self.conn.commit()
        return inserted_count, updated_count, skipped_count, validation_error_count, updated_columns, error_log
    
    def check_excel_filename_exists(self, excel_filename: str) -> bool:
        """동일한 excel_filename이 metadata 테이블에 존재하는지 확인"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM metadata WHERE excel_filename = ?", (excel_filename,))
        count = cursor.fetchone()[0]
        return count > 0
    
    def insert_metadata(self, excel_filename: str, excel_path: str, total_rows: int, processed_rows: int, notes: str = ""):
        """메타데이터 삽입"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO metadata (excel_filename, excel_path, total_rows, processed_rows, notes, import_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (excel_filename, excel_path, total_rows, processed_rows, notes, datetime.now().isoformat()))
        self.conn.commit()


# ========================================================
# GUI Class
# ========================================================
class SQLiteConverterGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("데이터 입고 도구 (엑셀 → SQLite)")
        self.geometry("1200x900")
        
        # 변수 초기화
        self.excel_file_path = tk.StringVar()
        self.db_file_path = tk.StringVar()
        self.confirm_upload = tk.BooleanVar(value=False)  # DB 업로드 확인 체크박스
        
        # 데이터
        self.df = None
        self.preview_df = None  # 미리보기용 데이터
        self.before_data = {}  # 입고 전 데이터 (비교용)
        
        # 기본 DB 파일 경로 설정 (자동 생성/업데이트)
        default_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")
        self.db_file_path.set(default_db_path)
        
        # 진행률 관련
        self.progress_var = tk.DoubleVar()
        self.progress_label_var = tk.StringVar(value="대기 중...")
        
        # UI 구성
        self._configure_styles()
        self._init_ui()
    
    def _configure_styles(self):
        """스타일 설정"""
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        bg_color = "#f8f9fa"
        self.configure(background=bg_color)
        
        style.configure("TFrame", background=bg_color)
        style.configure("TLabelframe", background=bg_color, font=("맑은 고딕", 10, "bold"))
        style.configure("TLabelframe.Label", background=bg_color, foreground="#333333")
        style.configure("TLabel", background=bg_color, font=("맑은 고딕", 9))
        style.configure("Action.TButton", font=("맑은 고딕", 10, "bold"), padding=8)
        
        # 1단계 버튼 스타일 (초입 - 중복검사)
        style.configure("Step1.TButton", font=("맑은 고딕", 11, "bold"), padding=10,
                       background="#f39c12", foreground="white")
        style.map("Step1.TButton",
                 background=[("active", "#e67e22"), ("disabled", "#bdc3c7")],
                 foreground=[("disabled", "#7f8c8d")])
        
        # 2단계 버튼 스타일 (마무리 - DB 업로드)
        style.configure("Step2.TButton", font=("맑은 고딕", 11, "bold"), padding=10,
                       background="#27ae60", foreground="white")
        style.map("Step2.TButton",
                 background=[("active", "#229954"), ("disabled", "#bdc3c7")],
                 foreground=[("disabled", "#7f8c8d")])
    
    def _init_ui(self):
        """UI 초기화"""
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)
        
        # 상단: 사용법 버튼
        help_frame = ttk.Frame(main_frame)
        help_frame.pack(fill='x', pady=(0, 10))
        btn_help = ttk.Button(help_frame, text="❓ 사용법 가이드", command=self._show_help)
        btn_help.pack(side='right')
        ToolTip(btn_help, "데이터 입고 도구 사용법을 확인합니다.")
        
        # 1. 엑셀 파일 선택 (배치 처리 지원)
        frame_excel = ttk.LabelFrame(main_frame, text="📁 엑셀 파일 선택 (여러 파일 선택 가능)", padding=15)
        frame_excel.pack(fill='x', pady=(0, 10))
        
        rf1 = ttk.Frame(frame_excel)
        rf1.pack(fill='x', pady=5)
        ttk.Label(rf1, text="엑셀 파일:", width=12).pack(side='left')
        excel_entry = ttk.Entry(rf1, textvariable=self.excel_file_path, width=50)
        excel_entry.pack(side='left', fill='x', expand=True, padx=5)
        ToolTip(excel_entry, "가공할 엑셀 파일을 선택하세요.\n파일을 선택하면 자동으로 미리보기가 표시됩니다.")
        
        btn_select = ttk.Button(rf1, text="📂 파일 선택", command=self._select_excel_file)
        btn_select.pack(side='right', padx=(5, 0))
        ToolTip(btn_select, "단일 엑셀 파일을 선택합니다.")
        
        btn_multiple = ttk.Button(rf1, text="📂 여러 파일", command=self._select_multiple_excel_files)
        btn_multiple.pack(side='right')
        ToolTip(btn_multiple, "여러 엑셀 파일을 동시에 선택하여 배치 처리할 수 있습니다.")
        
        # 데이터 미리보기
        self.preview_frame = ttk.LabelFrame(main_frame, text="👁️ 데이터 미리보기 (최대 10행)", padding=10)
        self.preview_frame.pack(fill='both', expand=True, pady=(0, 10))
        self.preview_frame.pack_forget()  # 초기에는 숨김
        
        # 미리보기 트리뷰
        preview_tree_frame = ttk.Frame(self.preview_frame)
        preview_tree_frame.pack(fill='both', expand=True)
        
        # 스크롤바
        preview_scroll_y = ttk.Scrollbar(preview_tree_frame, orient="vertical")
        preview_scroll_x = ttk.Scrollbar(preview_tree_frame, orient="horizontal")
        
        self.preview_tree = ttk.Treeview(preview_tree_frame, 
                                        yscrollcommand=preview_scroll_y.set,
                                        xscrollcommand=preview_scroll_x.set,
                                        show='headings')
        preview_scroll_y.config(command=self.preview_tree.yview)
        preview_scroll_x.config(command=self.preview_tree.xview)
        
        self.preview_tree.pack(side='left', fill='both', expand=True)
        preview_scroll_y.pack(side='right', fill='y')
        preview_scroll_x.pack(side='bottom', fill='x')
        
        ttk.Label(self.preview_frame, text="※ 엑셀 파일을 선택하면 자동으로 미리보기가 표시됩니다.", 
                 foreground="#666", font=("맑은 고딕", 8)).pack(anchor='w', padx=5, pady=(5, 0))
        
        # 2. DB 파일 선택 (자동 생성/업데이트)
        frame_db = ttk.LabelFrame(main_frame, text="💾 SQLite DB 파일 (자동 생성/업데이트)", padding=15)
        frame_db.pack(fill='x', pady=(0, 10))
        
        rf6 = ttk.Frame(frame_db)
        rf6.pack(fill='x', pady=5)
        ttk.Label(rf6, text="DB 파일:", width=12).pack(side='left')
        db_entry = ttk.Entry(rf6, textvariable=self.db_file_path, width=50)
        db_entry.pack(side='left', fill='x', expand=True, padx=5)
        ToolTip(db_entry, "상품 데이터가 저장될 SQLite 데이터베이스 파일 경로입니다.\n기본값: products.db (자동 생성/업데이트)")
        
        btn_db_select = ttk.Button(rf6, text="📂 경로 변경", command=self._select_db_file)
        btn_db_select.pack(side='right')
        ToolTip(btn_db_select, "DB 파일 저장 경로를 변경합니다.")
        
        ttk.Label(frame_db, text="※ 기본 경로의 products.db 파일에 자동으로 누적 저장됩니다. (기존 데이터는 유지)", 
                 foreground="#666", font=("맑은 고딕", 8)).pack(anchor='w', padx=5)
        
        # 4. 진행률 표시
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill='x', pady=(0, 10))
        
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100, length=400)
        self.progress_bar.pack(side='left', fill='x', expand=True, padx=(0, 10))
        self.progress_label = ttk.Label(progress_frame, textvariable=self.progress_label_var, width=20)
        self.progress_label.pack(side='right')
        
        # 5. 작업 단계별 버튼 영역
        # 5-1. 1단계: 중복검사 및 필터링 (상품 가공 초입)
        step1_frame = ttk.LabelFrame(main_frame, text="1️⃣ 1단계: 상품 가공 초입 - 중복검사 및 필터링", padding=15)
        step1_frame.pack(fill='x', pady=(0, 10))
        
        step1_desc = ttk.Label(step1_frame, 
                 text="※ 가공할 엑셀 파일에서 DB에 이미 존재하는 상품코드를 제외하여 필터링합니다.\n   중복된 상품은 정제/가공하지 않도록 사전에 제거합니다.",
                 foreground="#666", font=("맑은 고딕", 8), justify='left')
        step1_desc.pack(anchor='w', pady=(0, 10))
        ToolTip(step1_desc, "가공 작업 전에 중복된 상품코드를 미리 제거하여 불필요한 작업을 방지합니다.")
        
        self.btn_check_duplicates = ttk.Button(step1_frame, text="🔍 중복검사 및 필터링 실행", 
                                               style="Step1.TButton", command=self._start_duplicate_check)
        self.btn_check_duplicates.pack(fill='x', ipady=12)
        ToolTip(self.btn_check_duplicates, "엑셀 파일의 상품코드를 DB와 비교하여 중복된 행을 제외한 새 엑셀 파일을 생성합니다.\n가공 작업 전에 먼저 실행하세요.")
        
        # 5-2. 2단계: DB 업로드 (상품 가공 마무리)
        step2_frame = ttk.LabelFrame(main_frame, text="2️⃣ 2단계: 상품 가공 마무리 - DB 업로드", padding=15)
        step2_frame.pack(fill='x', pady=(0, 10))
        
        step2_desc = ttk.Label(step2_frame, 
                 text="※ 가공이 완료된 엑셀 파일을 DB에 업로드합니다.\n   신규 상품은 추가되고, 기존 상품은 업데이트됩니다.",
                 foreground="#666", font=("맑은 고딕", 8), justify='left')
        step2_desc.pack(anchor='w', pady=(0, 10))
        ToolTip(step2_desc, "가공이 완료된 상품 데이터를 DB에 저장합니다.\n신규 상품은 추가되고, 기존 상품은 변경된 컬럼만 업데이트됩니다.")
        
        # 확인 체크박스
        confirm_frame = ttk.Frame(step2_frame)
        confirm_frame.pack(fill='x', pady=(0, 10))
        
        self.confirm_checkbox = ttk.Checkbutton(
            confirm_frame,
            text="⚠️ 가공이 완료된 파일임을 확인했습니다 (실수 방지)",
            variable=self.confirm_upload,
            command=self._on_confirm_check
        )
        self.confirm_checkbox.pack(anchor='w')
        ToolTip(self.confirm_checkbox, "DB 업로드는 가공이 완료된 파일에만 실행해야 합니다.\n실수로 미완성 파일을 업로드하지 않도록 확인해주세요.")
        
        self.btn_convert = ttk.Button(step2_frame, text="▶ DB 업로드 시작", 
                                      style="Step2.TButton", command=self._start_conversion,
                                      state='disabled')
        self.btn_convert.pack(fill='x', ipady=12)
        ToolTip(self.btn_convert, "엑셀 파일의 상품 데이터를 DB에 업로드합니다.\n가공이 완료된 후 실행하세요.\n신규 추가, 업데이트, 스킵 건수를 자세히 표시합니다.\n⚠️ 위의 확인 체크박스를 먼저 선택해야 버튼이 활성화됩니다.")
        
        # 6. 로그창
        self.log_frame = ttk.LabelFrame(main_frame, text="📝 진행 로그", padding=12)
        self.log_frame.pack(fill='both', expand=True)
        
        self.log_widget = ScrolledText(self.log_frame, height=15, state='disabled', 
                                       font=("Consolas", 9), wrap='word',
                                       bg="#ffffff", fg="#2c3e50",
                                       selectbackground="#3498db", selectforeground="#ffffff",
                                       borderwidth=1, relief="solid")
        self.log_widget.pack(fill='both', expand=True)
    
    def _log(self, msg: str):
        """로그 출력"""
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')
    
    def _show_help(self):
        """사용법 가이드 창 표시"""
        help_window = tk.Toplevel(self)
        help_window.title("📖 데이터 입고 도구 사용법 가이드")
        help_window.geometry("750x700")
        help_window.resizable(True, True)
        
        # 스크롤 가능한 텍스트 영역
        frame = ttk.Frame(help_window, padding=20)
        frame.pack(fill='both', expand=True)
        
        text_widget = ScrolledText(frame, wrap='word', font=("맑은 고딕", 10), 
                                   bg="#ffffff", fg="#2c3e50", padx=10, pady=10)
        text_widget.pack(fill='both', expand=True)
        
        help_text = """
╔══════════════════════════════════════════════════════════════╗
║            데이터 입고 도구 사용법 가이드                      ║
╚══════════════════════════════════════════════════════════════╝

【작업 흐름】

1️⃣ 상품 가공 초입 → 2️⃣ 상품 가공 작업 → 3️⃣ 상품 가공 마무리

【1단계: 상품 가공 초입 - 중복검사 및 필터링】

목적: 가공할 엑셀 파일에서 DB에 이미 존재하는 상품코드를 제외

1. 엑셀 파일 선택
   • "📂 파일 선택" 버튼으로 가공할 엑셀 파일 선택
   • 파일 선택 시 자동으로 미리보기 표시

2. DB 파일 확인
   • 기본값: products.db (자동 생성)
   • 필요시 "📂 경로 변경"으로 경로 변경

3. 중복검사 실행
   • "🔍 중복검사 및 필터링 실행" 버튼 클릭
   • 저장 경로 선택
   • 결과: 중복 제외된 새 엑셀 파일 생성

결과:
  ✓ 중복된 행: DB에 이미 존재하는 상품코드 (제외됨)
  ✓ 중복 없는 행: 새로 가공할 상품 (출력됨)


【2단계: 상품 가공 작업】

• 1단계에서 생성된 필터링된 엑셀 파일로 가공 작업 수행
• 이미지 처리, 상품명 생성 등 실제 가공 작업
• 이 단계는 이 도구에서 수행하지 않습니다


【3단계: 상품 가공 마무리 - DB 업로드】

목적: 가공이 완료된 엑셀 파일을 DB에 저장

1. 가공 완료된 엑셀 파일 선택
   • 가공 작업이 완료된 엑셀 파일 선택

2. DB 업로드 실행
   • "▶ DB 업로드 시작" 버튼 클릭
   • 자동으로 백업 생성
   • 데이터 처리 진행

결과:
  ✓ 신규 추가: DB에 없는 새로운 상품코드
  ✓ 업데이트: 기존 상품의 변경된 컬럼만 업데이트
  ✓ 스킵: 변경사항이 없는 상품
  ✓ 검증 오류: 데이터 형식 오류 등


【주요 기능】

✓ 자동 백업
  - DB 업로드 전 자동으로 백업 파일 생성
  - backups 폴더에 타임스탬프와 함께 저장

✓ 상세 통계
  - 신규 추가, 업데이트, 스킵 건수 표시
  - 업데이트된 컬럼 목록 표시
  - 데이터 통계 (카테고리, 완료/미완료 등)

✓ 데이터 검증
  - 상품코드 중복 체크
  - URL 형식 검증
  - JSON 형식 검증
  - 오류 로그 파일 자동 생성

✓ 배치 처리
  - 여러 엑셀 파일을 한 번에 처리 가능
  - "📂 여러 파일" 버튼으로 선택

✓ 미리보기
  - 엑셀 파일 선택 시 자동으로 최대 10행 미리보기
  - 컬럼 구조 확인 가능


【기록되는 컬럼】

다음 26개 컬럼만 DB에 기록됩니다:

• 상품코드, 카테고리명, 원본상품명
• ST1_정제상품명, ST1_판매형태, ST2_JSON
• ST3_결과상품명, ST4_최종결과 (ST4_최종결과는 줄바꿈 → JSON 배열로 변환하여 product_names_json에 저장)
• IMG_S1_휴먼라벨, IMG_S1_휴먼노트, IMG_S1_AI라벨
• view_point, subject_position, subject_size
• lighting_condition, color_tone, shadow_presence
• background_simplicity, is_flat_lay
• bg_layout_hint_en, bg_positive_en, bg_negative_en
• video_motion_prompt_en, video_full_prompt_en
• 누끼url, 믹스url


【팁】

• 각 버튼에 마우스를 올리면 상세 설명이 표시됩니다
• 로그창에서 진행 상황을 실시간으로 확인할 수 있습니다
• 중복검사는 가공 전에 반드시 실행하세요
• DB 업로드는 가공이 완료된 후에 실행하세요
• 오류가 발생하면 error_log 파일을 확인하세요
"""
        
        text_widget.insert('1.0', help_text)
        text_widget.config(state='disabled')
        
        # 닫기 버튼
        btn_close = ttk.Button(frame, text="닫기", command=help_window.destroy)
        btn_close.pack(pady=(10, 0))
    
    def _select_excel_file(self):
        """엑셀 파일 선택"""
        path = filedialog.askopenfilename(
            title="엑셀 파일 선택",
            filetypes=[("Excel files", "*.xlsx;*.xls"), ("All files", "*.*")]
        )
        if path:
            self._load_excel_file(path)
    
    def _select_multiple_excel_files(self):
        """여러 엑셀 파일 선택 (배치 처리)"""
        paths = filedialog.askopenfilenames(
            title="엑셀 파일 선택 (여러 파일 선택 가능)",
            filetypes=[("Excel files", "*.xlsx;*.xls"), ("All files", "*.*")]
        )
        if paths:
            if len(paths) == 1:
                self._load_excel_file(paths[0])
            else:
                # 여러 파일 선택 시 배치 처리
                self.excel_file_path.set(f"{len(paths)}개 파일 선택됨")
                self._log(f"📁 {len(paths)}개 파일 선택됨")
                # 배치 처리는 변환 시작 시 처리
                self.excel_files = list(paths)
    
    def _load_excel_file(self, path: str):
        """엑셀 파일 로드 및 미리보기"""
        try:
            self.excel_file_path.set(path)
            # 엑셀 파일 미리 읽어서 컬럼 확인
            self.df = pd.read_excel(path, nrows=0)  # 헤더만 읽기
            self._log(f"엑셀 파일 선택됨: {os.path.basename(path)}")
            self._log(f"컬럼 수: {len(self.df.columns)}개")
            
            # 미리보기 데이터 로드 (최대 10행)
            self.preview_df = pd.read_excel(path, nrows=10)
            self._update_preview()
            # 미리보기 프레임 표시
            self.preview_frame.pack(fill='both', expand=True, pady=(0, 10), before=self.log_frame)
            
            # 기록할 컬럼 확인
            required_columns = [
                "상품코드", "카테고리명", "원본상품명", "ST1_정제상품명", "ST1_판매형태",
                "ST2_JSON", "ST3_결과상품명", "ST4_최종결과", "IMG_S1_휴먼라벨", "IMG_S1_휴먼노트", "IMG_S1_AI라벨",
                "view_point", "subject_position", "subject_size", "lighting_condition",
                "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
                "bg_layout_hint_en", "bg_positive_en", "bg_negative_en",
                "video_motion_prompt_en", "video_full_prompt_en", "누끼url", "믹스url"
            ]
            found_columns = [col for col in required_columns if col in self.df.columns]
            missing_columns = [col for col in required_columns if col not in self.df.columns]
            
            if found_columns:
                self._log(f"✅ 기록할 컬럼 발견: {len(found_columns)}개")
            if missing_columns:
                self._log(f"⚠️ 기록할 컬럼 누락: {', '.join(missing_columns[:5])}{'...' if len(missing_columns) > 5 else ''} (공란으로 저장됨)")
        except Exception as e:
            messagebox.showerror("오류", f"엑셀 파일을 읽는 중 오류가 발생했습니다:\n{e}")
    
    def _update_preview(self):
        """미리보기 테이블 업데이트"""
        # 기존 데이터 삭제
        for item in self.preview_tree.get_children():
            self.preview_tree.delete(item)
        
        if self.preview_df is None or len(self.preview_df) == 0:
            return
        
        # 컬럼 설정
        columns = list(self.preview_df.columns)
        self.preview_tree['columns'] = columns
        self.preview_tree['show'] = 'headings'
        
        # 컬럼 헤더 설정
        for col in columns:
            self.preview_tree.heading(col, text=col)
            self.preview_tree.column(col, width=100, anchor='w')
        
        # 데이터 삽입
        for idx, row in self.preview_df.iterrows():
            values = [str(row.get(col, ""))[:50] for col in columns]  # 최대 50자
            self.preview_tree.insert('', 'end', values=values)
    
    def _select_db_file(self):
        """DB 파일 경로 변경 (기본값: products.db)"""
        path = filedialog.asksaveasfilename(
            title="SQLite DB 파일 경로 선택",
            defaultextension=".db",
            initialfile="products.db",
            filetypes=[("SQLite Database", "*.db"), ("All files", "*.*")]
        )
        if path:
            self.db_file_path.set(path)
            self._log(f"DB 파일 경로 변경: {os.path.basename(path)}")
    
    def _start_duplicate_check(self):
        """중복검사 및 필터링 시작"""
        if not self.excel_file_path.get():
            messagebox.showwarning("오류", "엑셀 파일을 선택해주세요.")
            return
        
        # DB 파일 경로가 없으면 기본 경로 사용
        if not self.db_file_path.get():
            default_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")
            self.db_file_path.set(default_db_path)
        
        # DB 파일 존재 확인
        db_path = self.db_file_path.get()
        if not os.path.exists(db_path):
            messagebox.showwarning("오류", f"DB 파일이 존재하지 않습니다.\n{db_path}")
            return
        
        # 저장 경로 미리 선택 (메인 스레드에서)
        excel_path = self.excel_file_path.get()
        save_path = filedialog.asksaveasfilename(
            title="필터링된 엑셀 파일 저장",
            defaultextension=".xlsx",
            initialfile=f"중복제거_{os.path.basename(excel_path)}",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        
        if not save_path:
            return  # 취소됨
        
        # 버튼 비활성화
        self.btn_check_duplicates.config(state='disabled')
        
        # 진행률 초기화
        self.progress_var.set(0)
        self.progress_label_var.set("대기 중...")
        
        # 백그라운드 스레드에서 실행
        threading.Thread(target=self._run_duplicate_check, args=(save_path,), daemon=True).start()
    
    def _run_duplicate_check(self, save_path: str):
        """중복검사 및 필터링 실행 (백그라운드 스레드)"""
        excel_path = self.excel_file_path.get()
        db_path = self.db_file_path.get()
        
        try:
            self._log("=== 상품코드 중복검사 시작 ===")
            
            # 1. DB에서 기존 상품코드 조회
            self.after(0, lambda: self.progress_label_var.set("DB 조회 중..."))
            self._log("DB에서 기존 상품코드 조회 중...")
            
            db_manager = SQLiteDBManager(db_path)
            db_manager.connect()
            # products 테이블이 없을 수도 있으므로 먼저 생성
            db_manager.create_tables()
            
            cursor = db_manager.conn.cursor()
            cursor.execute("SELECT DISTINCT 상품코드 FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
            existing_codes = {row[0] for row in cursor.fetchall()}
            
            self._log(f"DB에 등록된 상품코드: {len(existing_codes)}개")
            db_manager.close()
            
            # 2. 엑셀 파일 읽기
            self.after(0, lambda: self.progress_label_var.set("엑셀 파일 읽는 중..."))
            self._log("엑셀 파일 읽는 중...")
            
            df = pd.read_excel(excel_path)
            total_rows = len(df)
            self._log(f"엑셀 총 행 수: {total_rows}건")
            
            # 3. 상품코드 컬럼 확인
            if "상품코드" not in df.columns:
                self._log("❌ 오류: 엑셀에 '상품코드' 컬럼이 없습니다.")
                self.after(0, lambda: messagebox.showerror("오류", "엑셀에 '상품코드' 컬럼이 없습니다."))
                self.after(0, lambda: self.btn_check_duplicates.config(state='normal'))
                return
            
            # 4. 중복 검사 및 필터링
            self.after(0, lambda: self.progress_label_var.set("중복검사 중..."))
            self._log("중복검사 중...")
            
            duplicate_indices = []
            unique_indices = []
            
            for idx, row in df.iterrows():
                # 진행률 업데이트
                progress = ((idx + 1) / total_rows) * 100
                self.after(0, lambda p=progress: self.progress_var.set(p))
                self.after(0, lambda c=idx+1, t=total_rows: self.progress_label_var.set(f"검사 중: {c}/{t}"))
                
                product_code = str(row.get("상품코드", "")).strip() if pd.notna(row.get("상품코드")) else ""
                
                if product_code and product_code in existing_codes:
                    # 중복된 상품코드
                    duplicate_indices.append(idx)
                else:
                    # 중복되지 않은 행
                    unique_indices.append(idx)
            
            duplicate_count = len(duplicate_indices)
            unique_count = len(unique_indices)
            
            self._log("=" * 50)
            self._log("📊 중복검사 결과")
            self._log("=" * 50)
            self._log(f"총 행 수: {total_rows}건")
            self._log(f"중복된 행: {duplicate_count}건 (제외됨)")
            self._log(f"중복 없는 행: {unique_count}건 (출력됨)")
            self._log("=" * 50)
            
            # 5. 중복 없는 행만 필터링
            if unique_count == 0:
                self._log("⚠️ 중복 없는 행이 없습니다. 모든 행이 DB에 이미 존재합니다.")
                self.after(0, lambda: messagebox.showwarning("알림", "중복 없는 행이 없습니다.\n모든 행이 DB에 이미 존재합니다."))
                self.after(0, lambda: self.btn_check_duplicates.config(state='normal'))
                self.after(0, lambda: self.progress_var.set(0))
                self.after(0, lambda: self.progress_label_var.set("대기 중..."))
                return
            
            # 중복 없는 행만 추출
            filtered_df = df.iloc[unique_indices].copy()
            
            # 6. 필터링된 엑셀 파일 저장
            self.after(0, lambda: self.progress_label_var.set("엑셀 파일 저장 중..."))
            self._log("필터링된 엑셀 파일 저장 중...")
            filtered_df.to_excel(save_path, index=False, engine='openpyxl')
            
            self._log(f"✅ 필터링된 엑셀 파일 저장 완료: {os.path.basename(save_path)}")
            self._log(f"저장 위치: {save_path}")
            
            self.after(0, lambda: self.progress_var.set(100))
            self.after(0, lambda: self.progress_label_var.set("완료"))
            
            # 결과 메시지
            result_msg = f"중복검사가 완료되었습니다.\n\n"
            result_msg += f"총 행 수: {total_rows}건\n"
            result_msg += f"중복된 행: {duplicate_count}건 (제외됨)\n"
            result_msg += f"중복 없는 행: {unique_count}건 (출력됨)\n\n"
            result_msg += f"저장된 파일: {os.path.basename(save_path)}"
            
            self.after(0, lambda msg=result_msg: messagebox.showinfo("완료", msg))
            
            # 파일 열기 여부 확인
            def ask_open_file():
                if messagebox.askyesno("파일 열기", "저장된 엑셀 파일을 열까요?"):
                    try:
                        if os.name == 'nt':  # Windows
                            os.startfile(save_path)
                        else:
                            import subprocess
                            subprocess.run(['open' if os.name == 'posix' and sys.platform == 'darwin' else 'xdg-open', save_path])
                    except Exception as e:
                        self._log(f"[WARN] 파일 열기 실패: {e}")
            
            self.after(0, ask_open_file)
            
            # 버튼 다시 활성화
            self.after(0, lambda: self.btn_check_duplicates.config(state='normal'))
            self.after(0, lambda: self.progress_var.set(0))
            self.after(0, lambda: self.progress_label_var.set("대기 중..."))
            
            self._log("=== 중복검사 완료 ===")
            
        except Exception as e:
            self._log(f"❌ 오류 발생: {e}")
            import traceback
            self._log(traceback.format_exc())
            self.after(0, lambda: self.btn_check_duplicates.config(state='normal'))
            self.after(0, lambda: self.progress_label_var.set("오류 발생"))
            self.after(0, lambda e=e: messagebox.showerror("오류", f"중복검사 중 오류가 발생했습니다:\n{e}"))
    
    def _on_confirm_check(self):
        """확인 체크박스 상태 변경 시 버튼 활성화/비활성화"""
        if self.confirm_upload.get():
            self.btn_convert.config(state='normal')
        else:
            self.btn_convert.config(state='disabled')
    
    def _start_conversion(self):
        """DB 변환 시작"""
        # 추가 확인 다이얼로그
        if not self.confirm_upload.get():
            messagebox.showwarning("확인 필요", "먼저 확인 체크박스를 선택해주세요.")
            return
        
        # 최종 확인 다이얼로그
        excel_path = self.excel_file_path.get()
        if not excel_path:
            messagebox.showwarning("오류", "엑셀 파일을 선택해주세요.")
            return
        
        db_path = self.db_file_path.get()
        if not db_path:
            default_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")
            db_path = default_db_path
        
        confirm_msg = f"⚠️ DB 업로드를 시작하시겠습니까?\n\n"
        confirm_msg += f"엑셀 파일: {os.path.basename(excel_path)}\n"
        confirm_msg += f"DB 파일: {os.path.basename(db_path)}\n\n"
        confirm_msg += "※ 기존 데이터가 업데이트될 수 있습니다.\n"
        confirm_msg += "※ 자동으로 백업이 생성됩니다."
        
        if not messagebox.askyesno("최종 확인", confirm_msg, icon='warning'):
            return  # 취소됨
        
        # 배치 처리 확인
        if hasattr(self, 'excel_files') and self.excel_files:
            # 여러 파일 배치 처리
            if not self.db_file_path.get():
                default_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")
                self.db_file_path.set(default_db_path)
            
            self.btn_convert.config(state='disabled')
            threading.Thread(target=self._run_batch_conversion, daemon=True).start()
            return
        
        # 단일 파일 처리
        if not self.excel_file_path.get():
            messagebox.showwarning("오류", "엑셀 파일을 선택해주세요.")
            return
        
        # DB 파일 경로가 없으면 기본 경로 사용
        if not self.db_file_path.get():
            default_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")
            self.db_file_path.set(default_db_path)
        
        # 버튼 비활성화 (중복 실행 방지)
        self.btn_convert.config(state='disabled')
        
        # 진행률 초기화
        self.progress_var.set(0)
        self.progress_label_var.set("대기 중...")
        
        # 백그라운드 스레드에서 실행
        threading.Thread(target=self._run_conversion, daemon=True).start()
    
    def _backup_database(self, db_path: str) -> str:
        """DB 백업 생성"""
        if not os.path.exists(db_path):
            return None
        
        backup_dir = os.path.join(os.path.dirname(db_path), "backups")
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"products_backup_{timestamp}.db"
        backup_path = os.path.join(backup_dir, backup_filename)
        
        shutil.copy2(db_path, backup_path)
        return backup_path
    
    def _get_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """데이터 통계 계산"""
        stats = {
            "total_rows": len(df),
            "categories": {},
            "complete_count": 0,
            "incomplete_count": 0,
            "has_nukki_url": 0,
            "has_mix_url": 0,
            "has_st3": 0,
        }
        
        if "카테고리명" in df.columns:
            category_counts = df["카테고리명"].value_counts().to_dict()
            stats["categories"] = category_counts
        
        # 완료/미완료 체크
        for idx, row in df.iterrows():
            st4 = str(row.get("ST4_최종결과", "")).strip() if pd.notna(row.get("ST4_최종결과")) else ""
            nukki = str(row.get("누끼url", "")).strip() if pd.notna(row.get("누끼url")) else ""
            mix = str(row.get("믹스url", "")).strip() if pd.notna(row.get("믹스url")) else ""
            
            if st4 and nukki and mix:
                stats["complete_count"] += 1
            else:
                stats["incomplete_count"] += 1
            
            if nukki:
                stats["has_nukki_url"] += 1
            if mix:
                stats["has_mix_url"] += 1
            if st4:
                stats["has_st3"] += 1  # 변수명은 유지하되 ST4 체크
        
        return stats
    
    def _run_conversion(self):
        """DB 변환 실행 (백그라운드 스레드)"""
        excel_path = self.excel_file_path.get()
        db_path = self.db_file_path.get()
        
        try:
            self._log("=== DB 변환 시작 ===")
            
            # 0. DB 백업 생성
            self.after(0, lambda: self.progress_label_var.set("백업 생성 중..."))
            backup_path = self._backup_database(db_path)
            if backup_path:
                self._log(f"💾 DB 백업 생성: {os.path.basename(backup_path)}")
            
            # 1. 엑셀 파일 읽기
            self.after(0, lambda: self.progress_label_var.set("엑셀 파일 읽는 중..."))
            self._log("엑셀 파일 읽는 중...")
            self.df = pd.read_excel(excel_path)
            total_rows = len(self.df)
            self._log(f"총 {total_rows}행 읽기 완료")
            
            # 통계 계산
            stats = self._get_statistics(self.df)
            self._log("=" * 50)
            self._log("📊 데이터 통계")
            self._log("=" * 50)
            self._log(f"총 행 수: {stats['total_rows']}건")
            if stats['categories']:
                self._log(f"카테고리: {len(stats['categories'])}개")
                for cat, count in list(stats['categories'].items())[:5]:
                    self._log(f"  - {cat}: {count}건")
            self._log(f"완료된 데이터: {stats['complete_count']}건")
            self._log(f"미완료 데이터: {stats['incomplete_count']}건")
            self._log(f"누끼url 있음: {stats['has_nukki_url']}건")
            self._log(f"믹스url 있음: {stats['has_mix_url']}건")
            self._log(f"ST4_최종결과 있음: {stats['has_st3']}건")
            self._log("=" * 50)
            
            # 2. 입고 전 데이터 백업 (비교용)
            self.after(0, lambda: self.progress_label_var.set("입고 전 데이터 조회 중..."))
            self._log("입고 전 데이터 조회 중...")
            db_manager = SQLiteDBManager(db_path)
            db_manager.connect()
            # 새 DB 파일일 수 있으므로 먼저 테이블 생성
            self._log("테이블 생성 중...")
            db_manager.create_tables()
            self._log("테이블 생성 완료 (products, metadata)")
            
            # 2-1. 동일한 파일명 중복 체크
            excel_filename = os.path.basename(excel_path)
            self._log(f"파일명 중복 체크: {excel_filename}")
            if db_manager.check_excel_filename_exists(excel_filename):
                error_msg = f"❌ 입고 거부: 동일한 파일명이 이미 존재합니다.\n\n파일명: {excel_filename}\n\n이미 입고된 파일입니다. 다른 파일명을 사용하거나 기존 데이터를 확인해주세요."
                self._log(error_msg)
                db_manager.close()
                self.after(0, lambda: self.progress_var.set(0))
                self.after(0, lambda: self.progress_label_var.set("입고 거부됨"))
                self.after(0, lambda: self.btn_convert.config(state='normal'))
                self.after(0, lambda: messagebox.showerror("입고 거부", error_msg))
                return
            
            self._log(f"✅ 파일명 중복 없음: {excel_filename}")
            
            # 기존 상품 데이터 조회 (비교용)
            try:
                cursor = db_manager.conn.cursor()
                cursor.execute("SELECT 상품코드, ST4_최종결과, 누끼url, 믹스url FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
                for row in cursor.fetchall():
                    self.before_data[row[0]] = {
                        "ST4_최종결과": row[1] or "",
                        "누끼url": row[2] or "",
                        "믹스url": row[3] or ""
                    }
            except Exception as e:
                # products 테이블이 비어있거나 구조가 다른 경우에도 전체 플로우는 계속 진행
                self._log(f"[WARN] 기존 상품 데이터 조회 중 오류 (무시하고 계속 진행): {e}")
            
            # 3. 상품 데이터 삽입 (지정된 컬럼만 기록)
            self.after(0, lambda: self.progress_label_var.set("데이터 처리 중..."))
            self._log("상품 데이터 삽입 중...")
            
            # 진행률 콜백
            def progress_callback(current, total):
                progress = (current / total) * 100
                self.after(0, lambda: self.progress_var.set(progress))
                self.after(0, lambda: self.progress_label_var.set(f"처리 중: {current}/{total} ({progress:.1f}%)"))
            
            # 에러 로그 콜백
            error_log_list = []
            def error_log_callback(row_index, error_msg):
                error_log_list.append((row_index, error_msg))
                self._log(f"⚠️ {error_msg}")
            
            inserted_count, updated_count, skipped_count, validation_error_count, updated_columns, error_log = db_manager.insert_products(
                self.df, 
                os.path.basename(excel_path),
                progress_callback=progress_callback,
                error_log_callback=error_log_callback
            )
            
            # 상세 로그 출력
            self._log("=" * 50)
            self._log("📊 데이터 입고 결과")
            self._log("=" * 50)
            self._log(f"✅ 신규 추가: {inserted_count}건")
            if updated_count > 0:
                self._log(f"🔄 업데이트: {updated_count}건")
                if updated_columns:
                    updated_cols_str = ', '.join(sorted(updated_columns)[:10])
                    if len(updated_columns) > 10:
                        updated_cols_str += f" 외 {len(updated_columns) - 10}개"
                    self._log(f"   업데이트된 컬럼: {updated_cols_str}")
            if skipped_count > 0:
                self._log(f"⏭️  스킵: {skipped_count}건 (변경사항 없음)")
            if validation_error_count > 0:
                self._log(f"❌ 검증 오류: {validation_error_count}건")
            self._log(f"📦 총 처리: {inserted_count + updated_count + skipped_count + validation_error_count}건")
            self._log("=" * 50)
            
            # 에러 로그 파일 저장
            if error_log:
                error_log_path = os.path.join(os.path.dirname(db_path), f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
                with open(error_log_path, 'w', encoding='utf-8') as f:
                    f.write(f"데이터 입고 에러 로그\n")
                    f.write(f"엑셀 파일: {excel_path}\n")
                    f.write(f"처리 일시: {datetime.now().isoformat()}\n")
                    f.write("=" * 50 + "\n\n")
                    
                    # 불완전한 ST2_JSON 오류 통계
                    truncated_count = sum(1 for msg in error_log if "불완전합니다" in msg or "토큰 부족" in msg)
                    if truncated_count > 0:
                        f.write(f"⚠️ 불완전한 ST2_JSON (출력 토큰 부족으로 잘림): {truncated_count}건\n")
                        f.write("   → 이 행들은 Stage2 배치 작업을 재실행해야 합니다.\n")
                        f.write("   → 또는 Stage2 배치 API에서 max_tokens를 늘려주세요.\n")
                        f.write("   → 불완전한 JSON이 있으면 이후 Stage3, Stage4 작업도 문제가 발생할 수 있습니다.\n\n")
                    
                    f.write("상세 오류 목록:\n")
                    f.write("-" * 50 + "\n")
                    for error_msg in error_log:
                        f.write(f"{error_msg}\n")
                        # 여러 줄 오류 메시지인 경우 가독성 향상
                        if "\n" in error_msg or "→" in error_msg:
                            f.write("\n")
                
                self._log(f"📄 에러 로그 저장: {os.path.basename(error_log_path)}")
                if truncated_count > 0:
                    self._log(f"⚠️ 불완전한 ST2_JSON: {truncated_count}건 (토큰 부족으로 잘림)")
                    self._log(f"   → 이 행들은 Stage2 배치 작업을 재실행해야 합니다.")
            
            # 4. 메타데이터 삽입
            self._log("메타데이터 삽입 중...")
            db_manager.insert_metadata(
                excel_filename=os.path.basename(excel_path),
                excel_path=excel_path,
                total_rows=total_rows,
                processed_rows=inserted_count + updated_count,
                notes=f"데이터 입고 (신규: {inserted_count}, 업데이트: {updated_count}, 오류: {validation_error_count})"
            )
            self._log("메타데이터 삽입 완료")
            
            # 5. 데이터 비교 (입고 전후)
            self._log("데이터 비교 중...")
            comparison_result = self._compare_data()
            if comparison_result:
                self._log("=" * 50)
                self._log("📊 입고 전후 비교 결과")
                self._log("=" * 50)
                for key, value in comparison_result.items():
                    self._log(f"{key}: {value}")
                self._log("=" * 50)
            
            # 6. DB 연결 종료
            db_manager.close()
            
            self.after(0, lambda: self.progress_var.set(100))
            self.after(0, lambda: self.progress_label_var.set("완료"))
            
            self._log("=== DB 변환 완료 ===")
            self._log(f"DB 파일 위치: {db_path}")
            if backup_path:
                self._log(f"백업 파일 위치: {backup_path}")
            
            # 버튼 다시 활성화
            self.after(0, lambda: self.btn_convert.config(state='normal'))
            
            # 결과 메시지 구성
            result_msg = f"DB 변환이 완료되었습니다.\n\n"
            result_msg += f"총 행 수: {total_rows}건\n"
            result_msg += f"신규 추가: {inserted_count}건\n"
            if updated_count > 0:
                result_msg += f"업데이트: {updated_count}건\n"
            if skipped_count > 0:
                result_msg += f"스킵: {skipped_count}건\n"
            if validation_error_count > 0:
                result_msg += f"검증 오류: {validation_error_count}건\n"
                # 불완전한 ST2_JSON 통계 추가
                truncated_count = sum(1 for msg in error_log if "불완전합니다" in msg or "토큰 부족" in msg)
                if truncated_count > 0:
                    result_msg += f"  - 불완전한 ST2_JSON (토큰 부족): {truncated_count}건\n"
                    result_msg += f"  → Stage2 배치 작업 재실행 필요\n"
            result_msg += f"\nDB 파일: {os.path.basename(db_path)}"
            
            if updated_columns:
                result_msg += f"\n\n업데이트된 컬럼: {', '.join(sorted(updated_columns)[:5])}"
                if len(updated_columns) > 5:
                    result_msg += f" 외 {len(updated_columns) - 5}개"
            
            if backup_path:
                result_msg += f"\n\n백업 파일: {os.path.basename(backup_path)}"
            
            self.after(0, lambda: messagebox.showinfo("완료", result_msg))
            
            # DB 파일이 있는 폴더 열기 여부 확인
            self.after(0, lambda: self._ask_open_folder(db_path))
            
        except Exception as e:
            self._log(f"❌ 오류 발생: {e}")
            import traceback
            self._log(traceback.format_exc())
            # 버튼 다시 활성화
            self.after(0, lambda: self.btn_convert.config(state='normal'))
            self.after(0, lambda: self.progress_label_var.set("오류 발생"))
            self.after(0, lambda e=e: messagebox.showerror("오류", f"DB 변환 중 오류가 발생했습니다:\n{e}"))
    
    def _run_batch_conversion(self):
        """배치 변환 실행 (여러 파일)"""
        try:
            self._log("=== 배치 변환 시작 ===")
            self._log(f"총 {len(self.excel_files)}개 파일 처리")
            
            db_path = self.db_file_path.get()
            
            # DB 백업
            backup_path = self._backup_database(db_path)
            if backup_path:
                self._log(f"💾 DB 백업 생성: {os.path.basename(backup_path)}")
            
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            total_errors = 0
            
            # DB 연결을 한 번만 생성하여 모든 파일에 재사용
            db_manager = SQLiteDBManager(db_path)
            db_manager.connect()
            db_manager.create_tables()
            
            try:
                for idx, excel_path in enumerate(self.excel_files):
                    excel_filename = os.path.basename(excel_path)
                    self._log(f"\n[{idx + 1}/{len(self.excel_files)}] {excel_filename} 처리 중...")
                    self.after(0, lambda p=((idx + 1) / len(self.excel_files)) * 100: self.progress_var.set(p))
                    self.after(0, lambda f=excel_filename: self.progress_label_var.set(f"처리 중: {f}"))
                    
                    try:
                        # 중복 파일명 체크
                        if db_manager.check_excel_filename_exists(excel_filename):
                            self._log(f"❌ 입고 거부: {excel_filename} (동일한 파일명이 이미 존재함)")
                            total_errors += 1
                            continue
                        
                        df = pd.read_excel(excel_path)
                        
                        inserted, updated, skipped, validation_errors, _, _ = db_manager.insert_products(
                            df, excel_filename
                        )
                        
                        total_inserted += inserted
                        total_updated += updated
                        total_skipped += skipped
                        total_errors += validation_errors
                        
                        db_manager.insert_metadata(
                            excel_filename=excel_filename,
                            excel_path=excel_path,
                            total_rows=len(df),
                            processed_rows=inserted + updated,
                            notes="배치 처리"
                        )
                        
                        self._log(f"✅ 완료: 신규 {inserted}건, 업데이트 {updated}건")
                        
                    except Exception as e:
                        self._log(f"❌ 오류: {e}")
                        total_errors += 1
            finally:
                # 모든 파일 처리 후 DB 연결 종료
                db_manager.close()
            
            self.after(0, lambda: self.progress_var.set(100))
            self.after(0, lambda: self.progress_label_var.set("완료"))
            
            self._log("\n" + "=" * 50)
            self._log("📊 배치 처리 결과")
            self._log("=" * 50)
            self._log(f"총 파일: {len(self.excel_files)}개")
            self._log(f"신규 추가: {total_inserted}건")
            self._log(f"업데이트: {total_updated}건")
            self._log(f"스킵: {total_skipped}건")
            self._log(f"오류: {total_errors}건")
            self._log("=" * 50)
            
            self.after(0, lambda: self.btn_convert.config(state='normal'))
            self.after(0, lambda: messagebox.showinfo(
                "완료",
                f"배치 처리가 완료되었습니다.\n\n"
                f"처리 파일: {len(self.excel_files)}개\n"
                f"신규 추가: {total_inserted}건\n"
                f"업데이트: {total_updated}건\n"
                f"스킵: {total_skipped}건\n"
                f"오류: {total_errors}건"
            ))
            
        except Exception as e:
            self._log(f"❌ 배치 처리 오류: {e}")
            self.after(0, lambda: self.btn_convert.config(state='normal'))
            self.after(0, lambda e=e: messagebox.showerror("오류", f"배치 처리 중 오류가 발생했습니다:\n{e}"))
    
    def _compare_data(self) -> Dict[str, Any]:
        """입고 전후 데이터 비교"""
        if not self.before_data:
            return None
        
        # 입고 후 데이터 조회
        db_path = self.db_file_path.get()
        db_manager = SQLiteDBManager(db_path)
        db_manager.connect()
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT 상품코드, ST4_최종결과, 누끼url, 믹스url FROM products WHERE 상품코드 IS NOT NULL AND 상품코드 != ''")
        
        after_data = {}
        for row in cursor.fetchall():
            after_data[row[0]] = {
                "ST4_최종결과": row[1] or "",
                "누끼url": row[2] or "",
                "믹스url": row[3] or ""
            }
        
        db_manager.close()
        
        # 비교 결과
        result = {
            "입고 전 상품 수": len(self.before_data),
            "입고 후 상품 수": len(after_data),
            "신규 추가된 상품": len(after_data) - len(self.before_data),
        }
        
        # 업데이트된 상품 확인
        updated_count = 0
        for product_code in self.before_data:
            if product_code in after_data:
                before = self.before_data[product_code]
                after = after_data[product_code]
                if before != after:
                    updated_count += 1
        
        result["업데이트된 상품"] = updated_count
        
        return result
    
    def _ask_open_folder(self, db_path: str):
        """DB 파일이 있는 폴더 열기 여부 확인"""
        if messagebox.askyesno("폴더 열기", "DB 파일이 있는 폴더를 열까요?"):
            try:
                folder_path = os.path.dirname(os.path.abspath(db_path))
                if os.name == 'nt':  # Windows
                    os.startfile(folder_path)
                else:
                    import subprocess
                    subprocess.run(['open' if os.name == 'posix' and sys.platform == 'darwin' else 'xdg-open', folder_path])
            except Exception as e:
                self._log(f"[WARN] 폴더 열기 실패: {e}")


if __name__ == "__main__":
    import sys
    app = SQLiteConverterGUI()
    app.mainloop()

