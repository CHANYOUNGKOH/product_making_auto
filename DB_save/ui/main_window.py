"""
ui/main_window.py

메인 GUI 창
- 카테고리 검색
- 마켓/스토어 선택 (체크박스)
- 작업 시작 버튼
- 로그 출력
- 스레딩으로 GUI 멈춤 방지
"""

import os
import sys
import threading
import queue
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# 상위 디렉토리에서 모듈 import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_handler import DBHandler
from ui.export_helpers import (
    build_sheet_used_combinations_cache,
    build_store_used_product_codes_cache,
    log_category_season_result,
    normalize_store_categories,
    update_store_season_stats,
)
from config import (
    AccountLoader, DEFAULT_DB_PATH, DEFAULT_EXCEL_ACCOUNTS_PATH,
    OWNER_NAMES, BUSINESS_NAMES, FIXED_DB_PATH, SHEET_DISPLAY_NAMES, HIDDEN_SHEETS, WHOLESALE_SHEETS,
    load_db_path_from_config, save_db_path_to_config
)


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


class MainWindow(tk.Tk):
    """메인 GUI 창"""
    
    def __init__(self):
        super().__init__()
        self.title("상품 업로드 관리 시스템")
        # 기본 창 크기를 더 크게 설정 (세로 공간 확보)
        self.geometry("1400x1100")
        # 최소 창 크기 설정
        self.minsize(1200, 900)
        
        # 변수 초기화
        # 설정 파일에서 DB 경로 로드 (없으면 고정 경로 사용)
        self.db_path = load_db_path_from_config()
        self.db_handler: Optional[DBHandler] = None
        self.account_loader = AccountLoader()  # 경로는 설정 파일에서 로드
        self.selected_markets: List[str] = []
        self.market_tree_items: Dict[str, Any] = {}  # 트리뷰 아이템 저장
        self.tree_checkboxes: Dict[str, tk.BooleanVar] = {}  # 체크박스 변수
        self.selected_categories: List[str] = []  # 선택된 카테고리
        self.category_tree_items: Dict[str, Any] = {}  # 카테고리 트리뷰 아이템
        self.category_checkboxes: Dict[str, tk.BooleanVar] = {}  # 카테고리 체크박스 변수
        
        # 스토어별 메모 관리
        self.store_memos: Dict[str, Dict[str, Any]] = {}  # {store_key: {"memo": "", "categories": []}}
        self.store_memo_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "store_memos.json")
        
        # 편집 중인 Entry 추적 (중복 편집 방지)
        self._editing_entry = None
        self._editing_item = None
        self._editing_original_value = None
        
        # UI 업데이트를 위한 큐 (스레드 안전)
        self._ui_update_queue = queue.Queue()
        
        # UI 구성
        self._configure_styles()
        self._init_ui()
        
        # 큐 처리 시작 (메인 스레드에서 주기적으로 체크)
        self._process_ui_update_queue()
        
        # UI 생성 후 메모 로드 (로그 출력을 위해)
        self._load_store_memos()
        
        # 초기 모드에 따른 설명 텍스트 설정
        if hasattr(self, 'category_info_label'):
            mode = self.export_mode.get()
            if mode == "upload":
                self.category_info_label.config(
                    text="※ 마켓 업로드용 모드: 카테고리는 스토어 메모에서 지정합니다.\n※ 이 섹션은 상품수 확인용 보조자료로 사용됩니다.",
                    foreground="#666"
                )
            else:
                self.category_info_label.config(
                    text="※ 미완료 DB 모드: 카테고리를 선택하여 데이터를 출고합니다.",
                    foreground="#666"
                )
        
        # 마켓 계정 로드
        self._load_accounts()
        
        # 초기 출력 모드 설정
        self._on_mode_change()
        
        # 기본 DB 경로가 있으면 카테고리 자동 로드
        if self.db_path and os.path.exists(self.db_path):
            try:
                self._load_categories()
            except Exception as e:
                self._log(f"⚠️ 카테고리 자동 로드 실패: {e}")
        
        # 초기 출력 모드 설정
        self._on_mode_change()
        
        # 상품코드 필터 모드 변경 트레이스 설정 (모든 UI 초기화 완료 후)
        if hasattr(self, 'product_code_filter_mode'):
            try:
                self.product_code_filter_mode.trace_add('write', self._on_product_code_filter_mode_change)
            except Exception as e:
                self._log(f"⚠️ 상품코드 필터 트레이스 설정 실패: {e}")
    
    def _configure_styles(self):
        """스타일 설정"""
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        bg_color = "#f5f6fa"
        self.configure(background=bg_color)
        
        style.configure("TFrame", background=bg_color)
        style.configure("TLabelframe", background=bg_color, font=("맑은 고딕", 9, "bold"), 
                       borderwidth=1, relief="solid")
        style.configure("TLabelframe.Label", background=bg_color, foreground="#2c3e50", 
                       font=("맑은 고딕", 9, "bold"))
        style.configure("TLabel", background=bg_color, font=("맑은 고딕", 9))
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=10,
                       background="#3498db", foreground="white")
        style.map("Action.TButton",
                 background=[("active", "#2980b9"), ("disabled", "#bdc3c7")],
                 foreground=[("disabled", "#7f8c8d")])
        
        # 일반 버튼 스타일
        style.configure("TButton", font=("맑은 고딕", 9), padding=5)
        
        # 비활성화된 체크박스 스타일
        style.configure("Disabled.TCheckbutton", foreground="#999999")
        
        # Entry 스타일
        style.configure("TEntry", font=("맑은 고딕", 9), padding=3)
    
    def _init_ui(self):
        """UI 초기화"""
        main_frame = ttk.Frame(self, padding=10)  # padding을 15에서 10으로 줄여 공간 확보
        main_frame.pack(fill='both', expand=True)
        
        # 상단: 파일 선택 영역 (컴팩트하게)
        file_frame = ttk.Frame(main_frame)
        file_frame.pack(fill='x', pady=(0, 8))  # 여백을 10에서 8로 줄임
        
        # 1. 마켓 계정 엑셀 파일 선택 (컴팩트)
        frame_excel = ttk.LabelFrame(file_frame, text="📋 마켓 계정 엑셀 파일", padding=10)
        frame_excel.pack(fill='x', side='left', expand=True, padx=(0, 5))
        
        excel_frame = ttk.Frame(frame_excel)
        excel_frame.pack(fill='x')
        ttk.Label(excel_frame, text="엑셀:", width=8).pack(side='left')
        excel_path = self.account_loader.excel_path or ""
        self.excel_path_var = tk.StringVar(value=excel_path)
        excel_entry = ttk.Entry(excel_frame, textvariable=self.excel_path_var, state='readonly')
        excel_entry.pack(side='left', fill='x', expand=True, padx=5)
        ToolTip(excel_entry, "마켓 계정 정보가 담긴 엑셀 파일 경로입니다.\nMarket_id_pw.xlsx 파일을 선택하세요.")
        
        btn_excel_select = ttk.Button(excel_frame, text="📂", command=self._select_excel_file, width=3)
        btn_excel_select.pack(side='right')
        ToolTip(btn_excel_select, "마켓 계정 엑셀 파일을 선택합니다.")
        
        btn_excel_reload = ttk.Button(excel_frame, text="🔄", command=self._reload_accounts, width=3)
        btn_excel_reload.pack(side='right', padx=(5, 0))
        ToolTip(btn_excel_reload, "엑셀 파일을 다시 로드하여 마켓 계정 목록을 갱신합니다.")
        
        # 2. DB 파일 선택 (컴팩트)
        frame_db = ttk.LabelFrame(file_frame, text="💾 데이터베이스 파일", padding=8)  # padding 줄임
        frame_db.pack(fill='x', side='left', expand=True, padx=(5, 0))
        
        db_frame = ttk.Frame(frame_db)
        db_frame.pack(fill='x')
        ttk.Label(db_frame, text="DB:", width=8).pack(side='left')
        self.db_path_var = tk.StringVar(value=self.db_path)
        db_entry = ttk.Entry(db_frame, textvariable=self.db_path_var)
        db_entry.pack(side='left', fill='x', expand=True, padx=5)
        ToolTip(db_entry, "상품 데이터가 저장된 SQLite 데이터베이스 파일 경로입니다.\nDB 파일을 선택하면 카테고리가 자동으로 로드됩니다.")
        
        btn_db_select = ttk.Button(db_frame, text="📂", command=self._select_db_file, width=3)
        btn_db_select.pack(side='right', padx=(5, 0))
        ToolTip(btn_db_select, "데이터베이스 파일을 선택합니다.")
        
        btn_db_default = ttk.Button(db_frame, text="💾", command=self._set_default_db_path, width=3)
        btn_db_default.pack(side='right')
        ToolTip(btn_db_default, "현재 DB 경로를 기본 경로로 저장합니다.\n다음 실행 시 자동으로 이 경로가 사용됩니다.")
        
        # 3. 카테고리와 마켓 선택 영역을 좌우로 배치
        selection_frame = ttk.Frame(main_frame)
        selection_frame.pack(fill='both', expand=True, pady=(0, 6))  # 여백 줄임
        
        # 3-1. 카테고리 선택 (왼쪽)
        self.frame_category = ttk.LabelFrame(selection_frame, text="📂 카테고리 선택 (대>중)", padding=8)  # padding 줄임
        self.frame_category.pack(fill='both', expand=True, side='left', padx=(0, 5))
        
        # 카테고리 섹션 설명 (모드에 따라 다르게 표시)
        self.category_info_label = ttk.Label(self.frame_category, text="", font=("맑은 고딕", 8), foreground="#666")
        self.category_info_label.pack(anchor='w', pady=(0, 3))  # 여백 줄임
        
        # 카테고리 트리뷰와 스크롤바
        cat_tree_frame = ttk.Frame(self.frame_category)
        cat_tree_frame.pack(fill='both', expand=True)  # expand=True로 높이 확보
        
        self.category_tree = ttk.Treeview(cat_tree_frame, show='tree', selectmode='none', height=10)  # 높이 조정
        cat_tree_scrollbar = ttk.Scrollbar(cat_tree_frame, orient="vertical", command=self.category_tree.yview)
        self.category_tree.configure(yscrollcommand=cat_tree_scrollbar.set)
        
        self.category_tree.pack(side="left", fill="both", expand=True)
        cat_tree_scrollbar.pack(side="right", fill="y")
        
        # 카테고리 전체 선택/해제 버튼
        cat_btn_frame = ttk.Frame(self.frame_category)
        cat_btn_frame.pack(fill='x', pady=(5, 0))  # 여백 줄임
        
        btn_cat_select_all = ttk.Button(cat_btn_frame, text="전체 선택", command=self._select_all_category)
        btn_cat_select_all.pack(side='left', padx=5)
        ToolTip(btn_cat_select_all, "모든 카테고리를 선택합니다.")
        
        btn_cat_deselect_all = ttk.Button(cat_btn_frame, text="전체 해제", command=self._deselect_all_category)
        btn_cat_deselect_all.pack(side='left', padx=5)
        ToolTip(btn_cat_deselect_all, "모든 카테고리 선택을 해제합니다.")
        
        btn_cat_expand = ttk.Button(cat_btn_frame, text="📂 전체 열기", command=self._expand_all_categories)
        btn_cat_expand.pack(side='left', padx=5)
        ToolTip(btn_cat_expand, "모든 카테고리 트리를 펼칩니다.")
        
        btn_cat_collapse = ttk.Button(cat_btn_frame, text="📁 전체 닫기", command=self._collapse_all_categories)
        btn_cat_collapse.pack(side='left', padx=5)
        ToolTip(btn_cat_collapse, "모든 카테고리 트리를 접습니다.")
        
        btn_cat_refresh = ttk.Button(cat_btn_frame, text="🔄 새로고침", command=self._load_categories)
        btn_cat_refresh.pack(side='right', padx=5)
        ToolTip(btn_cat_refresh, "DB에서 카테고리 목록을 다시 불러옵니다.")
        
        ttk.Label(self.frame_category, text="※ DB 파일 선택 시 카테고리 자동 로드", 
                 foreground="#666", font=("맑은 고딕", 7)).pack(anchor='w', padx=5, pady=(5, 0))
        
        # 전체 출력 상품 수량 제한 설정 영역 (마켓 업로드용일 때만 표시)
        self.quantity_frame = ttk.LabelFrame(self.frame_category, text="📊 출력 상품 수량 제한", padding=5)
        # 초기 모드가 "upload"이므로 표시
        self.quantity_frame.pack(fill='x', pady=(5, 0))  # 여백 줄임
        
        quantity_info_frame = ttk.Frame(self.quantity_frame)
        quantity_info_frame.pack(fill='x', pady=(0, 3))  # 여백 줄임
        ttk.Label(quantity_info_frame, text="※ 선택된 스토어의 카테고리 총 상품코드 수가 자동으로 표시됩니다.", 
                 foreground="#666", font=("맑은 고딕", 7)).pack(anchor='w')
        ttk.Label(quantity_info_frame, text="※ 수량을 설정하면 각 스토어별로 해당 수량만큼만 상품코드가 출력됩니다. (비워두면 전체 출고)", 
                 foreground="#666", font=("맑은 고딕", 7)).pack(anchor='w')
        ttk.Label(quantity_info_frame, text="※ 상품코드 기준으로 제한되며, 조합 수는 상품코드당 여러 개일 수 있습니다.", 
                 foreground="#666", font=("맑은 고딕", 7)).pack(anchor='w')
        ttk.Label(quantity_info_frame, text="※ 출력 상품수 제한이 있으면 우선적으로 출고된 적 없는 상품코드가 먼저 출력됩니다.", 
                 foreground="#666", font=("맑은 고딕", 7)).pack(anchor='w')
        
        # 전체 수량 설정
        total_quantity_frame = ttk.Frame(self.quantity_frame)
        total_quantity_frame.pack(fill='x', pady=(5, 0))
        ttk.Label(total_quantity_frame, text="총 상품 수:", width=12).pack(side='left', padx=5)
        self.total_product_count_label = ttk.Label(total_quantity_frame, text="0개", width=15, foreground="#0066cc", font=("맑은 고딕", 9, "bold"))
        self.total_product_count_label.pack(side='left', padx=5)
        
        ttk.Label(total_quantity_frame, text="출력 제한:", width=12).pack(side='left', padx=5)
        self.total_quantity_var = tk.StringVar(value="")
        total_quantity_entry = ttk.Entry(total_quantity_frame, textvariable=self.total_quantity_var, width=15)
        total_quantity_entry.pack(side='left', padx=5)
        ToolTip(total_quantity_entry, "각 스토어별로 출력할 최대 상품코드 수를 입력하세요.\n비워두면 선택된 모든 카테고리의 전체 상품이 출력됩니다.\n예: 각 스토어당 100개씩만 필요하면 100 입력\n※ 출력 상품수 제한이 있으면 우선적으로 출고된 적 없는 상품코드가 먼저 출력됩니다.")
        
        # 카테고리 트리뷰 클릭 이벤트
        self.category_tree.bind("<ButtonRelease-1>", self._on_category_tree_click)
        
        # 3-2. 마켓/스토어 선택 (오른쪽) - 마켓 업로드용일 때만 표시
        self.frame_markets = ttk.LabelFrame(selection_frame, text="🏪 마켓/스토어 선택", padding=8)  # padding 줄임
        # 초기 모드가 "upload"이므로 표시
        self.frame_markets.pack(fill='both', expand=True, side='right', padx=(5, 0))
        
        # 등록된 상품수량 컬럼 사용 안내
        info_label = ttk.Label(self.frame_markets, 
                              text="※ 등록된 상품수량 컬럼을 더블클릭하면 편집할 수 있습니다. (빈 값이면 전체 출고)", 
                              foreground="#666", font=("맑은 고딕", 7))
        info_label.pack(anchor='w', pady=(0, 3))  # 여백 줄임
        
        # 트리뷰와 스크롤바
        tree_frame = ttk.Frame(self.frame_markets)
        tree_frame.pack(fill='both', expand=True)  # expand=True로 높이 확보
        
        # 트리뷰 생성 (selectmode='none'으로 설정하여 선택 방지, 접기/펼치기는 여전히 작동)
        # 컬럼을 추가하여 메모 정보, 상품수, 등록된 상품수량을 표시
        self.market_tree = ttk.Treeview(tree_frame, show='tree headings', selectmode='none', height=10, columns=("memo", "product_count", "registered_count"))
        self.market_tree.heading("#0", text="마켓/스토어", anchor='w')
        self.market_tree.heading("memo", text="메모/카테고리", anchor='w')
        self.market_tree.heading("product_count", text="상품수", anchor='w')
        self.market_tree.heading("registered_count", text="등록된 상품수량", anchor='w')
        self.market_tree.column("#0", width=200, minwidth=150)  # 기본 컬럼 너비
        self.market_tree.column("memo", width=300, minwidth=200)  # 메모 컬럼
        self.market_tree.column("product_count", width=100, minwidth=80)  # 상품수 컬럼
        self.market_tree.column("registered_count", width=130, minwidth=100)  # 등록된 상품수량 컬럼
        
        tree_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.market_tree.yview)
        self.market_tree.configure(yscrollcommand=tree_scrollbar.set)
        
        self.market_tree.pack(side="left", fill="both", expand=True)
        tree_scrollbar.pack(side="right", fill="y")
        
        # 트리뷰 스타일 설정
        style = ttk.Style()
        style.configure("Treeview", font=("맑은 고딕", 9))
        style.configure("Treeview.Item", padding=(5, 3))
        # 비활성화된 항목 스타일 (회색)
        style.map("Treeview", 
                 foreground=[("disabled", "#999999")],
                 background=[("disabled", "#f0f0f0")])
        
        # 편집 모드 Entry 스타일 (배경색 변경)
        style.configure("Edit.TEntry", fieldbackground="#fff9c4")  # 연한 노란색 배경
        
        # 중복 경고 태그 스타일 설정
        self.market_tree.tag_configure("duplicate", foreground="#d32f2f", background="#ffebee")
        
        # 전체 선택/해제 버튼
        btn_frame = ttk.Frame(self.frame_markets)
        btn_frame.pack(fill='x', pady=(5, 0))  # 여백 줄임
        
        btn_market_select_all = ttk.Button(btn_frame, text="전체 선택", command=self._select_all_tree)
        btn_market_select_all.pack(side='left', padx=5)
        ToolTip(btn_market_select_all, "모든 마켓/스토어를 선택합니다.")
        
        btn_market_deselect_all = ttk.Button(btn_frame, text="전체 해제", command=self._deselect_all_tree)
        btn_market_deselect_all.pack(side='left', padx=5)
        ToolTip(btn_market_deselect_all, "모든 마켓/스토어 선택을 해제합니다.")
        
        btn_market_expand = ttk.Button(btn_frame, text="📂 시트 전체 열기", command=self._expand_all_sheets)
        btn_market_expand.pack(side='left', padx=5)
        ToolTip(btn_market_expand, "모든 시트의 마켓 트리를 펼칩니다.")
        
        btn_market_collapse = ttk.Button(btn_frame, text="📁 시트 전체 닫기", command=self._collapse_all_sheets)
        btn_market_collapse.pack(side='left', padx=5)
        ToolTip(btn_market_collapse, "모든 시트의 마켓 트리를 접습니다.")
        
        # 5. 출력 모드 선택 및 실행 버튼
        self.btn_frame_action = ttk.Frame(main_frame)
        self.btn_frame_action.pack(fill='x', pady=(0, 8))
        
        # 출력 모드 선택
        mode_frame = ttk.Frame(self.btn_frame_action)
        mode_frame.pack(fill='x', pady=(0, 8))
        ttk.Label(mode_frame, text="출력 모드:", width=10).pack(side='left')
        self.export_mode = tk.StringVar(value="upload")
        
        radio_upload = ttk.Radiobutton(mode_frame, text="마켓 업로드용 (완료된 DB)", variable=self.export_mode, 
                        value="upload", command=self._on_mode_change)
        radio_upload.pack(side='left', padx=8)
        ToolTip(radio_upload, "완료된 상품 데이터를 마켓에 업로드하기 위해 내보냅니다.\n카테고리와 마켓/스토어를 선택해야 합니다.\n중복 방지를 위해 사용된 조합은 기록됩니다.")
        
        radio_incomplete = ttk.Radiobutton(mode_frame, text="미완료 DB (재가공용)", variable=self.export_mode, 
                        value="incomplete", command=self._on_mode_change)
        radio_incomplete.pack(side='left', padx=8)
        ToolTip(radio_incomplete, "미완료된 상품 데이터를 재가공하기 위해 내보냅니다.\n카테고리만 선택하면 됩니다.\nST3_결과상품명, 누끼url, 믹스url 중 하나라도 공백이면 미완료로 판단됩니다.")
        
        # 도움말 버튼 추가
        btn_help = ttk.Button(mode_frame, text="❓ 사용법", command=self._show_help)
        btn_help.pack(side='right', padx=5)
        ToolTip(btn_help, "사용법 가이드를 표시합니다.")
        
        # 출고 히스토리 버튼 추가
        btn_history = ttk.Button(mode_frame, text="📋 출고 히스토리", command=self._show_export_history)
        btn_history.pack(side='right', padx=5)
        ToolTip(btn_history, "데이터 출고 히스토리를 확인하고 재다운로드할 수 있습니다.")
        
        # 시즌 필터 설정 버튼 추가
        btn_season_filter = ttk.Button(mode_frame, text="🗓️ 시즌 필터 설정", command=self._open_season_filter_manager)
        btn_season_filter.pack(side='right', padx=5)
        ToolTip(btn_season_filter, "시즌 필터링 설정을 관리합니다.\n시즌 및 키워드를 추가/수정/삭제할 수 있습니다.")
        
        # 새로운 DB만 출력 옵션 (마켓 업로드용일 때만 표시)
        self.exclude_assigned_var = tk.BooleanVar(value=True)  # 기본값: True (제외)
        self.exclude_assigned_checkbox = ttk.Checkbutton(
            self.btn_frame_action, 
            text="✅ 새로운 DB만 출력 (이미 배정된 상품코드 제외)", 
            variable=self.exclude_assigned_var
        )
        self.exclude_assigned_checkbox.pack(fill='x', pady=(3, 0))  # 여백 줄임
        ToolTip(self.exclude_assigned_checkbox, 
                "체크됨: 이미 해당 스토어에서 배정받은 상품코드는 제외하고 출력합니다.\n"
                "체크 해제: 이미 배정받은 상품코드도 포함하여 재출력할 수 있습니다.\n"
                "⚠️ 체크 해제 시 중복 업로드 가능성이 있으니 주의하세요.")
        
        # 시즌 필터링 옵션 (마켓 업로드용일 때만 표시)
        self.season_filter_var = tk.BooleanVar(value=True)  # 기본값: True (활성화)
        self.season_filter_checkbox = ttk.Checkbutton(
            self.btn_frame_action,
            text="🗓️ 시즌 필터링 활성화 (현재 시즌이 지난 상품 자동 제외)",
            variable=self.season_filter_var
        )
        self.season_filter_checkbox.pack(fill='x', pady=(3, 0))  # 여백 줄임
        ToolTip(self.season_filter_checkbox,
                "체크됨: 시즌 필터링을 적용하여 현재 시즌이 지난 상품은 자동으로 제외합니다.\n"
                "체크 해제: 시즌 필터링을 비활성화하고 모든 상품을 포함합니다.\n"
                "※ 시즌 설정은 Season_Filter_Seasons_Keywords.xlsx 파일에서 관리됩니다.")
        
        # 상품코드 필터링 옵션 프레임 (기본적으로 숨김, 필요시에만 표시)
        self.product_code_filter_frame = ttk.LabelFrame(self.btn_frame_action, text="상품코드 필터링")
        # 기본적으로 숨김 (공간 확보)
        # self.product_code_filter_frame.pack(fill='x', pady=(5, 0))  # 주석 처리 - 기본 숨김
        
        # 제외/포함 선택 라디오 버튼 (간단한 형태로 별도 프레임에 배치)
        # 마켓 업로드용 모드일 때만 표시되도록 함 (기본적으로 숨김)
        self.filter_mode_compact_frame = ttk.Frame(self.btn_frame_action)
        # 기본적으로 숨김 (모드 변경 시 표시)
        
        ttk.Label(self.filter_mode_compact_frame, text="상품코드 필터:", font=("맑은 고딕", 9)).pack(side='left', padx=(0, 5))
        self.product_code_filter_mode = tk.StringVar(value="none")  # none, exclude, include
        
        ttk.Radiobutton(self.filter_mode_compact_frame, text="사용 안함", variable=self.product_code_filter_mode, value="none").pack(side='left', padx=(0, 10))
        ttk.Radiobutton(self.filter_mode_compact_frame, text="제외", variable=self.product_code_filter_mode, value="exclude").pack(side='left', padx=(0, 10))
        ttk.Radiobutton(self.filter_mode_compact_frame, text="포함", variable=self.product_code_filter_mode, value="include").pack(side='left')
        
        # 상품코드 입력 필드 (텍스트 영역) - 상품코드 필터링 프레임 내부
        code_input_frame = ttk.Frame(self.product_code_filter_frame)
        code_input_frame.pack(fill='both', expand=True, padx=5, pady=(5, 5))
        
        ttk.Label(code_input_frame, text="상품코드 (쉼표 또는 줄바꿈으로 구분):", font=("맑은 고딕", 8)).pack(anchor='w')
        
        # 텍스트 위젯과 스크롤바
        text_frame = ttk.Frame(code_input_frame)
        text_frame.pack(fill='both', expand=True)
        
        self.product_code_filter_text = tk.Text(text_frame, height=3, wrap=tk.WORD, font=("맑은 고딕", 9))  # 높이를 4에서 3으로 줄임
        scrollbar = ttk.Scrollbar(text_frame, orient='vertical', command=self.product_code_filter_text.yview)
        self.product_code_filter_text.configure(yscrollcommand=scrollbar.set)
        
        self.product_code_filter_text.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        # 라디오 버튼 변경 시 상품코드 입력 프레임 표시/숨김은 _init_ui 완료 후 설정
        # (메서드가 정의된 후에 trace_add 호출해야 함)
        
        ToolTip(self.filter_mode_compact_frame,
                "상품코드를 기준으로 필터링합니다.\n"
                "• 사용 안함: 필터링 없음\n"
                "• 제외: 입력한 상품코드는 출고에서 제외됩니다.\n"
                "• 포함: 입력한 상품코드만 출고됩니다.\n"
                "• 선택 시 상품코드 입력 필드가 표시됩니다.")
        
        # 시즌 관련 내보내기 버튼 프레임
        season_export_frame = ttk.Frame(self.btn_frame_action)
        season_export_frame.pack(fill='x', pady=(3, 0))  # 여백 줄임
        
        btn_export_sourcing = ttk.Button(season_export_frame, text="🔵 소싱 기간 상품 내보내기", 
                                         command=self._export_sourcing_products)
        btn_export_sourcing.pack(side='left', fill='x', expand=True, padx=(0, 3))
        ToolTip(btn_export_sourcing, 
                "소싱 기간 시즌 상품을 내보냅니다.\n"
                "상품코드 + 시즌단어 형식으로 Excel 파일로 저장됩니다.\n"
                "이미 가공 완료된 상품이므로 제외하고 DB 가공 준비에 사용합니다.")
        
        btn_export_expired = ttk.Button(season_export_frame, text="❌ 시즌 종료 상품 내보내기", 
                                        command=self._export_expired_products)
        btn_export_expired.pack(side='left', fill='x', expand=True, padx=(3, 0))
        ToolTip(btn_export_expired,
                "시즌 종료된 상품을 내보냅니다.\n"
                "상품코드만 Excel 파일로 저장됩니다.\n"
                "외부 오픈마켓에서 삭제하는 용도로 사용합니다.")
        
        # 전체 DB 출력 버튼 추가
        btn_export_all = ttk.Button(self.btn_frame_action, text="📦 전체 DB 출력", 
                                    command=self._export_all_products)
        btn_export_all.pack(fill='x', ipady=5, pady=(3, 0))  # ipady와 여백 줄임
        ToolTip(btn_export_all, "입고되어 있는 전체 DB의 모든 상품을 출력합니다.\n카테고리 필터링 없이 ACTIVE 상태의 모든 상품이 출력됩니다.")
        
        self.btn_start = ttk.Button(self.btn_frame_action, text="▶ 데이터 출고", style="Action.TButton", 
                                    command=self._start_export, state='disabled')
        self.btn_start.pack(fill='x', ipady=6, pady=(3, 0))  # ipady와 여백 줄임
        ToolTip(self.btn_start, "선택한 카테고리와 마켓에 따라 데이터를 출고합니다.\n카테고리와 마켓(업로드용 모드)을 선택해야 활성화됩니다.")
        
        # 6. 로그창 - 항상 보이도록 하단에 고정 (최우선)
        # 프로그레스 바 프레임
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill='x', pady=(0, 3))  # 여백 줄임
        
        self.progress_label = ttk.Label(progress_frame, text="진행률: 0%", font=("맑은 고딕", 9))
        self.progress_label.pack(side='left', padx=(0, 10))
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate', length=300)
        self.progress_bar.pack(side='left', fill='x', expand=True, padx=(0, 10))
        
        self.progress_status_label = ttk.Label(progress_frame, text="대기 중...", font=("맑은 고딕", 9), foreground="#666")
        self.progress_status_label.pack(side='right')
        
        self.log_frame = ttk.LabelFrame(main_frame, text="📝 진행 로그", padding=8)  # padding 줄임
        # 로그창이 남은 공간을 모두 차지하도록 설정 (expand=True)
        self.log_frame.pack(fill='both', expand=True, pady=(0, 0))
        
        # 로그창은 expand=True로 설정하여 남은 공간을 모두 사용
        # height는 최소 높이만 지정 (실제로는 expand로 공간 차지)
        self.log_widget = ScrolledText(self.log_frame, height=12, state='disabled',  # 최소 높이 줄임
                                       font=("Consolas", 9), wrap='word',
                                       bg="#ffffff", fg="#2c3e50",
                                       selectbackground="#3498db", selectforeground="#ffffff",
                                       borderwidth=1, relief="solid")
        self.log_widget.pack(fill='both', expand=True)
        
        # 상품코드 필터 모드 변경 트레이스 설정은 메서드 정의 후에 별도로 수행
        # (메서드가 정의되기 전에는 참조할 수 없으므로)
    
    def _log(self, msg: str):
        """로그 출력"""
        # log_widget이 아직 생성되지 않았으면 로그 출력하지 않음
        if not hasattr(self, 'log_widget') or self.log_widget is None:
            return
        
        ts = datetime.now().strftime("%H:%M:%S")
        try:
            self.log_widget.config(state='normal')
            self.log_widget.insert('end', f"[{ts}] {msg}\n")
            self.log_widget.see('end')
            self.log_widget.config(state='disabled')
        except Exception:
            # 로그 위젯이 아직 준비되지 않은 경우 무시
            pass
    
    def _update_progress(self, percentage: int, status: str = ""):
        """프로그레스 바 업데이트"""
        try:
            if percentage < 0:
                percentage = 0
            elif percentage > 100:
                percentage = 100
            
            self.progress_bar['value'] = percentage
            self.progress_label.config(text=f"진행률: {percentage}%")
            if status:
                self.progress_status_label.config(text=status)
            self.update_idletasks()  # UI 업데이트 강제
        except Exception:
            pass
    
    def _create_progress_dialog(self, title: str = "작업 진행 중"):
        """진행 상황 다이얼로그 생성 및 반환"""
        progress_window = tk.Toplevel(self)
        progress_window.title(title)
        progress_window.geometry("500x180")
        progress_window.transient(self)
        progress_window.resizable(False, False)
        
        # 중앙 배치
        progress_window.update_idletasks()
        x = (progress_window.winfo_screenwidth() // 2) - (500 // 2)
        y = (progress_window.winfo_screenheight() // 2) - (180 // 2)
        progress_window.geometry(f"500x180+{x}+{y}")
        
        # 프레임
        frame = ttk.Frame(progress_window, padding=20)
        frame.pack(fill='both', expand=True)
        
        # 제목 레이블
        title_label = ttk.Label(frame, text=title, font=("맑은 고딕", 11, "bold"))
        title_label.pack(pady=(0, 15))
        
        # 상태 메시지 레이블
        status_label = ttk.Label(frame, text="초기화 중...", font=("맑은 고딕", 10))
        status_label.pack(pady=(0, 10))
        
        # 진행률 바
        progress_var = tk.DoubleVar()
        progress_bar = ttk.Progressbar(frame, variable=progress_var, maximum=100, length=450, mode='determinate')
        progress_bar.pack(pady=(0, 10))
        
        # 진행률 텍스트 레이블
        progress_label = ttk.Label(frame, text="0%", font=("맑은 고딕", 9), foreground="#666")
        progress_label.pack()
        
        # 상세 정보 텍스트 영역 (스크롤 가능)
        detail_frame = ttk.Frame(frame)
        detail_frame.pack(fill='both', expand=True, pady=(10, 0))
        
        detail_text = ScrolledText(detail_frame, height=4, state='disabled', font=("Consolas", 8), wrap='word')
        detail_text.pack(fill='both', expand=True)
        
        # 창 닫기 방지 (X 버튼 비활성화는 복잡하므로 일단 허용)
        def on_closing():
            # 진행 중에는 닫기 방지 (사용자에게 알림)
            result = messagebox.askyesno(
                "확인",
                "작업이 진행 중입니다. 정말 취소하시겠습니까?\n\n"
                "작업은 백그라운드에서 계속 진행됩니다.",
                icon='warning'
            )
            if result:
                progress_window.destroy()
        
        progress_window.protocol("WM_DELETE_WINDOW", on_closing)
        
        return {
            'window': progress_window,
            'progress_var': progress_var,
            'progress_bar': progress_bar,
            'status_label': status_label,
            'progress_label': progress_label,
            'detail_text': detail_text
        }
    
    def _update_progress_dialog(self, progress_dialog: Dict, percentage: int, status: str = "", detail: str = ""):
        """진행 상황 다이얼로그 업데이트 (스레드 안전)"""
        if not progress_dialog or not progress_dialog.get('window'):
            return
        
        try:
            def update_ui():
                try:
                    if not progress_dialog['window'].winfo_exists():
                        return
                    
                    # 진행률 업데이트
                    if percentage >= 0 and percentage <= 100:
                        progress_dialog['progress_var'].set(percentage)
                        progress_dialog['progress_label'].config(text=f"{percentage}%")
                    
                    # 상태 메시지 업데이트
                    if status:
                        progress_dialog['status_label'].config(text=status)
                    
                    # 상세 정보 추가
                    if detail:
                        progress_dialog['detail_text'].config(state='normal')
                        progress_dialog['detail_text'].insert('end', f"[{datetime.now().strftime('%H:%M:%S')}] {detail}\n")
                        progress_dialog['detail_text'].see('end')
                        progress_dialog['detail_text'].config(state='disabled')
                    
                    progress_dialog['window'].update_idletasks()
                except (tk.TclError, AttributeError):
                    pass
            
            # 메인 스레드에서 실행
            self.after(0, update_ui)
        except Exception:
            pass
    
    def _close_progress_dialog(self, progress_dialog: Dict):
        """진행 상황 다이얼로그 닫기"""
        if not progress_dialog or not progress_dialog.get('window'):
            return
        
        def close_ui():
            try:
                if progress_dialog['window'].winfo_exists():
                    progress_dialog['window'].destroy()
            except (tk.TclError, AttributeError):
                pass
        
        self.after(0, close_ui)
    
    def _show_help(self):
        """사용법 가이드 창 표시"""
        help_window = tk.Toplevel(self)
        help_window.title("📖 사용법 가이드")
        help_window.geometry("700x600")
        help_window.resizable(True, True)
        
        # 스크롤 가능한 텍스트 영역
        frame = ttk.Frame(help_window, padding=20)
        frame.pack(fill='both', expand=True)
        
        text_widget = ScrolledText(frame, wrap='word', font=("맑은 고딕", 10), 
                                   bg="#ffffff", fg="#2c3e50", padx=10, pady=10)
        text_widget.pack(fill='both', expand=True)
        
        help_text = """
╔══════════════════════════════════════════════════════════════╗
║              데이터 출고 도구 사용법 가이드                   ║
╚══════════════════════════════════════════════════════════════╝

【1단계: 파일 설정】

1. 마켓 계정 엑셀 파일
   • Market_id_pw.xlsx 파일을 선택합니다
   • 시트별로 마켓이 구분되어 있습니다
   • 파일을 수정한 후 "🔄 다시 로드" 버튼을 클릭하세요

2. 데이터베이스 파일
   • 상품 데이터가 저장된 SQLite DB 파일을 선택합니다
   • DB 파일을 선택하면 카테고리가 자동으로 로드됩니다
   • "💾 기본 경로로 설정" 버튼으로 경로를 저장할 수 있습니다


【2단계: 카테고리 선택】

• 왼쪽 카테고리 트리에서 원하는 카테고리를 선택합니다
• 체크박스를 클릭하여 개별 선택/해제가 가능합니다
• "전체 선택" / "전체 해제" 버튼으로 일괄 선택이 가능합니다
• "📂 전체 열기" / "📁 전체 닫기"로 트리를 펼치거나 접을 수 있습니다


【3단계: 출력 모드 선택】

1. 마켓 업로드용 (완료된 DB)
   • 완료된 상품 데이터를 마켓에 업로드하기 위해 내보냅니다
   • 카테고리와 마켓/스토어를 모두 선택해야 합니다
   • 오른쪽 마켓/스토어 트리에서 업로드할 마켓을 선택합니다
   • 중복 방지: 사용된 조합(상품코드+URL+상품명)은 자동으로 기록됩니다
   • 우선순위: 믹스url > 누끼url, 첫 줄 상품명 > 다음 줄 상품명

2. 미완료 DB (재가공용)
   • 미완료된 상품 데이터를 재가공하기 위해 내보냅니다
   • 카테고리만 선택하면 됩니다 (마켓 선택 불필요)
   • 미완료 기준: ST3_결과상품명, 누끼url, 믹스url 중 하나라도 공백


【4단계: 데이터 출고】

• "▶ 데이터 출고" 버튼을 클릭합니다
• 저장 폴더를 선택하면 데이터가 엑셀 파일로 내보내집니다
• 진행 상황은 하단 로그창에서 확인할 수 있습니다


【주요 기능】

✓ 중복 방지
  - 같은 시트(마켓) 내에서 이미 사용한 조합은 자동으로 제외됩니다
  - upload_logs 테이블에 사용 이력이 기록됩니다

✓ 우선순위 시스템
  - 믹스url이 우선적으로 사용됩니다
  - 상품명은 첫 줄부터 순차적으로 사용됩니다
  - 각 스토어당 상품코드 1개만 할당됩니다

✓ 성능 최적화
  - 시트별로 사전 검증하여 불필요한 처리를 방지합니다
  - 대용량 데이터(5만~20만 개) 처리에 최적화되어 있습니다


【팁】

• 각 버튼에 마우스를 올리면 상세 설명이 표시됩니다
• 로그창에서 진행 상황을 실시간으로 확인할 수 있습니다
• 미완료 DB 모드에서는 마켓 선택이 필요 없습니다
• 카테고리 트리는 대>중 형식으로 표시됩니다
"""
        
        text_widget.insert('1.0', help_text)
        text_widget.config(state='disabled')
        
        # 닫기 버튼
        btn_close = ttk.Button(frame, text="닫기", command=help_window.destroy)
        btn_close.pack(pady=(10, 0))
    
    def _open_season_filter_manager(self):
        """시즌 필터 관리 창 열기"""
        try:
            # season_filter_manager_gui 모듈 import
            import sys
            season_filter_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if season_filter_path not in sys.path:
                sys.path.insert(0, season_filter_path)
            
            from season_filter_manager_gui import SeasonFilterManagerGUI
            
            # 시즌 필터 관리 GUI 창 열기
            manager_window = SeasonFilterManagerGUI()
            self._log("✅ 시즌 필터 관리 창이 열렸습니다.")
            
        except ImportError as e:
            error_msg = f"시즌 필터 관리 모듈을 불러올 수 없습니다:\n{e}\n\n시즌 필터링 기능을 사용할 수 없습니다."
            self._log(f"⚠️ {error_msg}")
            messagebox.showerror("오류", error_msg)
        except Exception as e:
            error_msg = f"시즌 필터 관리 창 열기 실패:\n{e}"
            self._log(f"❌ {error_msg}")
            messagebox.showerror("오류", error_msg)
    
    def _export_sourcing_products(self):
        """소싱 기간 시즌 상품 내보내기 (상품코드 + 시즌단어)"""
        if not self.db_handler or not self.db_handler.conn:
            messagebox.showwarning("경고", "DB 파일을 먼저 선택하세요.")
            return
        
        try:
            # 시즌 필터링 모듈 확인
            import sys
            season_filter_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if season_filter_path not in sys.path:
                sys.path.insert(0, season_filter_path)
            
            from season_filter_manager_gui import load_season_config, filter_products_by_season, _detect_seasons_from_product, _check_season_validity
            from datetime import datetime
            
            # 시즌 설정 로드
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")
            json_path = os.path.join(script_dir, "season_filters.json")
            
            season_config = load_season_config(excel_path, json_path)
            if not season_config:
                messagebox.showwarning("경고", "시즌 설정 파일을 찾을 수 없습니다.")
                return
            
            current_date = datetime.now()
            
            # 모든 ACTIVE 상품 조회
            cursor = self.db_handler.conn.cursor()
            cursor.execute("""
                SELECT DISTINCT p.상품코드, p.원본상품명, p.ST3_결과상품명, p.ST1_정제상품명, p.카테고리명
                FROM products p
                WHERE p.product_status = 'ACTIVE'
                AND p.product_names_json IS NOT NULL 
                AND p.product_names_json != '' 
                AND p.product_names_json != '[]'
            """)
            
            products_with_info = []
            for row in cursor.fetchall():
                product_code = row[0]
                if product_code:
                    원본상품명 = row[1] or ""
                    ST3_결과상품명 = row[2] or ""
                    ST1_정제상품명 = row[3] or ""
                    category_name = row[4] or ""
                    product_name = 원본상품명 or ST3_결과상품명 or ST1_정제상품명
                    
                    products_with_info.append({
                        "상품코드": product_code,
                        "상품명": product_name,
                        "product_name": product_name,
                        "카테고리명": category_name,
                    })
            
            # SOURCING 기간 시즌 상품 필터링
            # ⚠️ 정책: 소싱 기간 상품 내보내기에서는 예외단어를 적용하지 않음
            # 예외단어는 메인 데이터 출고(ACTIVE Event 타입 제외)와 시즌 종료 상품 내보내기에서만 적용
            sourcing_products = []
            for product in products_with_info:
                product_code = product.get("상품코드")
                product_name = str(product.get('상품명', product.get('product_name', ''))).lower()
                category_name = str(product.get('카테고리명', '')).lower()
                product_name_display = product.get('상품명', product.get('product_name', ''))  # 원본 상품명 (표시용)
                category_name_display = product.get('카테고리명', '')  # 원본 카테고리명 (표시용)
                
                if not product_name:
                    continue
                
                # 소싱 기간 상품 내보내기에서는 예외단어 체크 없이 시즌 감지 수행
                detected_seasons = _detect_seasons_from_product(product_name, season_config, category_name)
                
                for season_id, score in detected_seasons:
                    season_info = next((s for s in season_config.get("seasons", []) if s["id"] == season_id), None)
                    if not season_info:
                        continue
                    
                    validity = _check_season_validity(season_info, current_date, season_config)
                    
                    if validity == "SOURCING":
                        # 시즌 타입 상관없이 SOURCING이면 모두 내보내기 (예외단어 필터링 완료 후)
                        season_name = season_info.get("name", season_id)
                        season_type = season_info.get("type", "").strip() or "기타"
                        # 시즌 키워드 추출 (include 키워드)
                        keywords = season_info.get("keywords", {})
                        include_keywords = [kw.get("keyword", "") for kw in keywords.get("include", [])]
                        
                        # 매칭된 키워드와 위치 확인
                        matched_keywords = []
                        matched_in_product = []
                        matched_in_category = []
                        
                        product_name_lower = product_name_display.lower()
                        category_name_lower = category_name_display.lower()
                        
                        for kw in include_keywords:
                            kw_lower = kw.lower()
                            if kw_lower in product_name_lower:
                                matched_keywords.append(kw)
                                matched_in_product.append(kw)
                            elif kw_lower in category_name_lower:
                                matched_keywords.append(kw)
                                matched_in_category.append(kw)
                        
                        # 매칭 정보 문자열 생성
                        match_info_parts = []
                        if matched_in_product:
                            match_info_parts.append(f"상품명: {', '.join(matched_in_product)}")
                        if matched_in_category:
                            match_info_parts.append(f"카테고리: {', '.join(matched_in_category)}")
                        match_info = " | ".join(match_info_parts) if match_info_parts else "매칭 키워드 없음"
                        
                        sourcing_products.append({
                            "상품코드": product_code,
                            "카테고리명": category_name_display,
                            "상품명": product_name_display,
                            "시즌명": season_name,
                            "시즌타입": season_type,
                            "매칭키워드": ", ".join(matched_keywords) if matched_keywords else "",
                            "매칭위치": match_info
                        })
                        break  # 첫 번째 SOURCING 시즌만 사용
            
            if not sourcing_products:
                messagebox.showinfo("정보", "소싱 기간 시즌 상품이 없습니다.")
                return
            
            # Excel 파일로 저장
            from tkinter import filedialog
            save_path = filedialog.asksaveasfilename(
                title="소싱 기간 상품 저장",
                defaultextension=".xlsx",
                filetypes=[("Excel 파일", "*.xlsx"), ("모든 파일", "*.*")],
                initialfile=f"소싱기간_상품_{current_date.strftime('%Y%m%d')}.xlsx"
            )
            
            if not save_path:
                return
            
            # Excel 파일로 저장 (2개 시트: 상품목록 + 시즌단어)
            all_season_keywords = set()  # 모든 시즌 단어 (중복 제거)
            
            # 시즌 단어 수집 (매칭키워드에서)
            for item in sourcing_products:
                keywords = item.get("매칭키워드", "").split(", ")
                for kw in keywords:
                    kw = kw.strip()
                    if kw:
                        all_season_keywords.add(kw)
            
            try:
                with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                    # 시트1: 상품 목록
                    df_products = pd.DataFrame(sourcing_products)
                    df_products.to_excel(writer, sheet_name='상품목록', index=False)
                    
                    # 시트2: 시즌 단어 목록 (중복 제거, 정렬)
                    df_keywords = pd.DataFrame({
                        '시즌단어': sorted(list(all_season_keywords))
                    })
                    df_keywords.to_excel(writer, sheet_name='시즌단어', index=False)
            except PermissionError:
                messagebox.showerror("오류", f"파일을 저장할 수 없습니다.\n파일이 다른 프로그램에서 열려있는지 확인하세요.\n\n경로: {save_path}")
                return
            except Exception as e:
                messagebox.showerror("오류", f"Excel 파일 저장 중 오류가 발생했습니다:\n{str(e)}")
                return
            
            self._log(f"✅ 소싱 기간 상품 내보내기 완료: {len(sourcing_products)}개 (파일: {os.path.basename(save_path)})")
            messagebox.showinfo("완료", f"소싱 기간 상품 {len(sourcing_products)}개를 내보냈습니다.\n\n파일: {os.path.basename(save_path)}")
            
        except Exception as e:
            import traceback
            error_msg = f"소싱 기간 상품 내보내기 실패:\n{e}\n\n{traceback.format_exc()}"
            self._log(f"❌ {error_msg}")
            messagebox.showerror("오류", error_msg)
    
    def _export_expired_products(self):
        """시즌 종료 상품 내보내기 (상품코드만, 삭제용)"""
        if not self.db_handler or not self.db_handler.conn:
            messagebox.showwarning("경고", "DB 파일을 먼저 선택하세요.")
            return
        
        try:
            # 시즌 필터링 모듈 확인
            import sys
            season_filter_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if season_filter_path not in sys.path:
                sys.path.insert(0, season_filter_path)
            
            from season_filter_manager_gui import load_season_config, filter_products_by_season, _detect_seasons_from_product, _check_season_validity
            from datetime import datetime
            
            # 시즌 설정 로드
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")
            json_path = os.path.join(script_dir, "season_filters.json")
            
            season_config = load_season_config(excel_path, json_path)
            if not season_config:
                messagebox.showwarning("경고", "시즌 설정 파일을 찾을 수 없습니다.")
                return
            
            # 디버깅: 예외단어가 로드되었는지 확인하고, 없으면 JSON 캐시 재생성
            settings = season_config.get("settings", {})
            common_exclude_keywords = settings.get("common_exclude_keywords", [])
            if not common_exclude_keywords:
                # JSON 캐시가 오래되었을 수 있으므로 강제 재생성 시도
                self._log("[경고] JSON 캐시에 예외단어가 없습니다. 캐시를 재생성합니다.")
                # JSON 파일 삭제하여 강제 재생성
                if os.path.exists(json_path):
                    try:
                        os.remove(json_path)
                        season_config = load_season_config(excel_path, json_path)
                        if season_config:
                            settings = season_config.get("settings", {})
                            common_exclude_keywords = settings.get("common_exclude_keywords", [])
                            self._log(f"[정보] 캐시 재생성 완료. 예외단어 {len(common_exclude_keywords)}개 로드됨")
                    except Exception as e:
                        self._log(f"[오류] 캐시 재생성 실패: {e}")
            
            current_date = datetime.now()
            
            # 모든 ACTIVE 상품 조회
            cursor = self.db_handler.conn.cursor()
            cursor.execute("""
                SELECT DISTINCT p.상품코드, p.원본상품명, p.ST3_결과상품명, p.ST1_정제상품명, p.카테고리명
                FROM products p
                WHERE p.product_status = 'ACTIVE'
                AND p.product_names_json IS NOT NULL 
                AND p.product_names_json != '' 
                AND p.product_names_json != '[]'
            """)
            
            products_with_info = []
            for row in cursor.fetchall():
                product_code = row[0]
                if product_code:
                    원본상품명 = row[1] or ""
                    ST3_결과상품명 = row[2] or ""
                    ST1_정제상품명 = row[3] or ""
                    category_name = row[4] or ""
                    product_name = 원본상품명 or ST3_결과상품명 or ST1_정제상품명
                    
                    products_with_info.append({
                        "상품코드": product_code,
                        "상품명": product_name,
                        "product_name": product_name,
                        "카테고리명": category_name,
                    })
            
            # 공통 예외단어 로드 확인 및 디버깅
            if not common_exclude_keywords:
                self._log("[경고] 공통 예외단어가 로드되지 않았습니다. season_filter_config.json을 확인하세요.")
            else:
                self._log(f"[디버그] 공통 예외단어 {len(common_exclude_keywords)}개 로드됨")
                # 처음 5개 예외단어 출력 (디버깅용)
                sample_keywords = common_exclude_keywords[:5]
                sample_str = ', '.join(str(k) if not isinstance(k, dict) else k.get('keyword', k.get('key', '')) for k in sample_keywords)
                self._log(f"[디버그] 예시 예외단어: {sample_str}")
            
            # 디버깅: 예외단어 로드 확인
            if not common_exclude_keywords:
                self._log("[경고] 공통 예외단어가 로드되지 않았습니다.")
            else:
                self._log(f"[디버그] 공통 예외단어 {len(common_exclude_keywords)}개 로드됨")
            
            # EXPIRED 기간 시즌 상품 필터링 (중복 제거)
            expired_product_codes = set()
            expired_products_info = {}  # {상품코드: {'시즌명': str, '카테고리명': str, '상품명': str, ...}}
            excluded_by_keyword_count = 0  # 예외단어로 제외된 상품 수
            
            for product in products_with_info:
                product_code = product.get("상품코드")
                product_name = str(product.get('상품명', product.get('product_name', ''))).lower()
                category_name = str(product.get('카테고리명', '')).lower()
                product_name_display = product.get('상품명', product.get('product_name', ''))  # 원본 상품명 (표시용)
                category_name_display = product.get('카테고리명', '')  # 원본 카테고리명 (표시용)
                
                if not product_name:
                    continue
                
                # ⚠️ 우선순위: 예외단어 > 타입 > 시즌 > 키워드
                # 예외단어가 포함된 상품은 시즌 종료 상품 내보내기에서 제외
                search_text_for_exclude = product_name
                if category_name:
                    search_text_for_exclude = f"{product_name} {category_name}"
                
                has_exclude_keyword = False
                matched_exclude_keyword = None
                for kw in common_exclude_keywords:
                    if isinstance(kw, dict):
                        kw_str = kw.get("keyword", kw.get("key", "")).lower()
                    else:
                        kw_str = str(kw).lower()
                    
                    if kw_str and kw_str in search_text_for_exclude:
                        has_exclude_keyword = True
                        matched_exclude_keyword = kw_str
                        break
                
                # 예외단어가 포함된 상품은 시즌 종료 상품 내보내기에서 제외
                if has_exclude_keyword:
                    excluded_by_keyword_count += 1
                    # 디버깅: 처음 몇 개만 로그 출력
                    if excluded_by_keyword_count <= 5:
                        self._log(f"[디버그] 예외단어로 제외: {product_name_display[:50]}... (매칭 단어: {matched_exclude_keyword})")
                    continue
                
                detected_seasons = _detect_seasons_from_product(product_name, season_config, category_name)
                
                for season_id, score in detected_seasons:
                    season_info = next((s for s in season_config.get("seasons", []) if s["id"] == season_id), None)
                    if not season_info:
                        continue
                    
                    validity = _check_season_validity(season_info, current_date, season_config)
                    
                    if validity == "EXPIRED":
                        # Event 타입만 내보내기
                        season_type = season_info.get("type", "").strip().upper()
                        if season_type == "EVENT":
                            expired_product_codes.add(product_code)
                            if product_code not in expired_products_info:
                                expired_products_info[product_code] = {
                                    "상품코드": product_code,
                                    "카테고리명": category_name_display,
                                    "상품명": product_name_display,
                                    "시즌명": [],
                                    "시즌타입": [],
                                    "시즌단어": [],
                                    "매칭위치": []
                                }
                            season_name = season_info.get("name", season_id)
                            season_type = season_info.get("type", "").strip() or "기타"
                            if season_name not in expired_products_info[product_code]["시즌명"]:
                                expired_products_info[product_code]["시즌명"].append(season_name)
                                expired_products_info[product_code]["시즌타입"].append(season_type)
                            
                            # 매칭된 키워드와 위치 확인
                            keywords = season_info.get("keywords", {})
                            include_keywords = [kw.get("keyword", "") for kw in keywords.get("include", [])]
                            
                            product_name_lower = product_name_display.lower()
                            category_name_lower = category_name_display.lower()
                            
                            matched_keywords_for_season = []
                            matched_in_product_for_season = []
                            matched_in_category_for_season = []
                            
                            for kw in include_keywords:
                                if not kw:
                                    continue
                                kw_lower = kw.lower()
                                if kw_lower in product_name_lower:
                                    matched_keywords_for_season.append(kw)
                                    matched_in_product_for_season.append(kw)
                                    if kw not in expired_products_info[product_code]["시즌단어"]:
                                        expired_products_info[product_code]["시즌단어"].append(kw)
                                elif kw_lower in category_name_lower:
                                    matched_keywords_for_season.append(kw)
                                    matched_in_category_for_season.append(kw)
                                    if kw not in expired_products_info[product_code]["시즌단어"]:
                                        expired_products_info[product_code]["시즌단어"].append(kw)
                            
                            # 매칭 위치 정보 저장 (시즌별로)
                            match_info_parts = []
                            if matched_in_product_for_season:
                                match_info_parts.append(f"상품명: {', '.join(matched_in_product_for_season)}")
                            if matched_in_category_for_season:
                                match_info_parts.append(f"카테고리: {', '.join(matched_in_category_for_season)}")
                            match_info = " | ".join(match_info_parts) if match_info_parts else "매칭 키워드 없음"
                            
                            # 시즌별 매칭 정보 저장
                            season_match_info = f"{season_name}({match_info})"
                            if "매칭위치" not in expired_products_info[product_code]:
                                expired_products_info[product_code]["매칭위치"] = []
                            if season_match_info not in expired_products_info[product_code]["매칭위치"]:
                                expired_products_info[product_code]["매칭위치"].append(season_match_info)
            
            # 디버깅: 예외단어로 제외된 상품 수 출력
            if excluded_by_keyword_count > 0:
                self._log(f"[디버그] 예외단어로 제외된 상품: {excluded_by_keyword_count}개")
            
            # ⚠️ 중요: 이미 등록되어 있는 오픈마켓 상품만 필터링
            # combination_assignments 테이블에서 실제로 할당된 상품코드만 조회
            if expired_product_codes:
                try:
                    placeholders = ','.join('?' * len(expired_product_codes))
                    cursor.execute(f"""
                        SELECT DISTINCT product_code
                        FROM combination_assignments
                        WHERE product_code IN ({placeholders})
                    """, list(expired_product_codes))
                    
                    assigned_product_codes = {row[0] for row in cursor.fetchall()}
                    
                    # 실제로 할당된 상품만 필터링
                    expired_products_info = {
                        code: info 
                        for code, info in expired_products_info.items() 
                        if code in assigned_product_codes
                    }
                    
                    self._log(f"[정보] EXPIRED Event 타입 상품 중 실제로 할당된 상품: {len(assigned_product_codes)}개")
                    self._log(f"[정보] 할당되지 않은 상품 제외: {len(expired_product_codes) - len(assigned_product_codes)}개")
                except Exception as e:
                    self._log(f"[경고] combination_assignments 테이블 조회 실패: {e}")
                    # 오류 발생 시 모든 상품 포함 (기존 동작 유지)
            
            if not expired_products_info:
                messagebox.showinfo("정보", "시즌 종료된 상품 중 이미 등록되어 있는 오픈마켓 상품이 없습니다.")
                return
            
            # 리스트 생성 (상품코드별 시즌 단어 포함)
            expired_products = []
            all_season_keywords = set()  # 모든 시즌 단어 (중복 제거)
            
            for product_code, info in expired_products_info.items():
                season_names_str = ", ".join(info["시즌명"])
                season_types_str = ", ".join(info.get("시즌타입", [])) if info.get("시즌타입") else ""
                matched_keywords_str = ", ".join(info["시즌단어"]) if info["시즌단어"] else ""
                matched_location_str = " | ".join(info["매칭위치"]) if info.get("매칭위치") else ""
                
                expired_products.append({
                    "상품코드": product_code,
                    "카테고리명": info.get("카테고리명", ""),
                    "상품명": info.get("상품명", ""),
                    "시즌명": season_names_str,
                    "시즌타입": season_types_str,
                    "매칭키워드": matched_keywords_str,
                    "매칭위치": matched_location_str
                })
                # 시즌 단어 수집 (별도 시트용)
                for kw in info["시즌단어"]:
                    all_season_keywords.add(kw)
            
            # Excel 파일로 저장 (2개 시트: 상품목록 + 시즌단어)
            from tkinter import filedialog
            save_path = filedialog.asksaveasfilename(
                title="시즌 종료 상품 저장",
                defaultextension=".xlsx",
                filetypes=[("Excel 파일", "*.xlsx"), ("모든 파일", "*.*")],
                initialfile=f"시즌종료_상품코드_{current_date.strftime('%Y%m%d')}.xlsx"
            )
            
            if not save_path:
                return
            
            try:
                with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                    # 시트1: 상품 목록
                    df_products = pd.DataFrame(expired_products)
                    df_products.to_excel(writer, sheet_name='상품목록', index=False)
                    
                    # 시트2: 시즌 단어 목록 (중복 제거, 정렬)
                    df_keywords = pd.DataFrame({
                        '시즌단어': sorted(list(all_season_keywords))
                    })
                    df_keywords.to_excel(writer, sheet_name='시즌단어', index=False)
            except PermissionError:
                messagebox.showerror("오류", f"파일을 저장할 수 없습니다.\n파일이 다른 프로그램에서 열려있는지 확인하세요.\n\n경로: {save_path}")
                return
            except Exception as e:
                messagebox.showerror("오류", f"Excel 파일 저장 중 오류가 발생했습니다:\n{str(e)}")
                return
            
            self._log(f"✅ 시즌 종료 상품 내보내기 완료: {len(expired_products)}개 (파일: {os.path.basename(save_path)})")
            messagebox.showinfo("완료", f"시즌 종료 상품 {len(expired_products)}개를 내보냈습니다.\n\n파일: {os.path.basename(save_path)}")
            
        except Exception as e:
            import traceback
            error_msg = f"시즌 종료 상품 내보내기 실패:\n{e}\n\n{traceback.format_exc()}"
            self._log(f"❌ {error_msg}")
            messagebox.showerror("오류", error_msg)
    
    def _show_export_history(self):
        """출고 히스토리 조회 창 표시"""
        if not self.db_handler:
            messagebox.showwarning("경고", "DB 파일을 먼저 선택하세요.")
            return
        
        history_window = tk.Toplevel(self)
        history_window.title("📋 출고 히스토리")
        history_window.geometry("1200x700")
        history_window.resizable(True, True)
        
        # 메인 프레임
        main_frame = ttk.Frame(history_window, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # 필터 프레임
        filter_frame = ttk.LabelFrame(main_frame, text="필터", padding=10)
        filter_frame.pack(fill='x', pady=(0, 10))
        
        # 날짜 필터 - 빠른 선택 버튼
        date_quick_frame = ttk.Frame(filter_frame)
        date_quick_frame.pack(fill='x', pady=(0, 5))
        ttk.Label(date_quick_frame, text="기간 선택:", font=("맑은 고딕", 9, "bold")).pack(side='left', padx=5)
        
        from datetime import datetime, timedelta
        
        def set_date_range(days=None):
            """날짜 범위 설정 및 자동 조회"""
            today = datetime.now()
            end_date_str = today.strftime('%Y%m%d')
            
            if days is None:
                # 무제한
                start_date_var.set("")
                end_date_var.set("")
            else:
                start_date = today - timedelta(days=days)
                start_date_str = start_date.strftime('%Y%m%d')
                start_date_var.set(start_date_str)
                end_date_var.set(end_date_str)
            
            # 자동 조회
            refresh_history()
        
        # 빠른 선택 버튼들
        btn_1week = ttk.Button(date_quick_frame, text="최근 일주일", command=lambda: set_date_range(7), width=12)
        btn_1week.pack(side='left', padx=2)
        
        btn_2week = ttk.Button(date_quick_frame, text="최근 이주일", command=lambda: set_date_range(14), width=12)
        btn_2week.pack(side='left', padx=2)
        
        btn_1month = ttk.Button(date_quick_frame, text="최근 한달", command=lambda: set_date_range(30), width=12)
        btn_1month.pack(side='left', padx=2)
        
        btn_3month = ttk.Button(date_quick_frame, text="최근 3개월", command=lambda: set_date_range(90), width=12)
        btn_3month.pack(side='left', padx=2)
        
        btn_6month = ttk.Button(date_quick_frame, text="최근 6개월", command=lambda: set_date_range(180), width=12)
        btn_6month.pack(side='left', padx=2)
        
        btn_1year = ttk.Button(date_quick_frame, text="최근 1년", command=lambda: set_date_range(365), width=12)
        btn_1year.pack(side='left', padx=2)
        
        btn_2year = ttk.Button(date_quick_frame, text="최근 2년", command=lambda: set_date_range(730), width=12)
        btn_2year.pack(side='left', padx=2)
        
        btn_unlimited = ttk.Button(date_quick_frame, text="무제한", command=lambda: set_date_range(None), width=12)
        btn_unlimited.pack(side='left', padx=2)
        
        # 수동 날짜 입력 (선택사항)
        date_manual_frame = ttk.Frame(filter_frame)
        date_manual_frame.pack(fill='x', pady=(5, 0))
        ttk.Label(date_manual_frame, text="수동 입력:", font=("맑은 고딕", 9)).pack(side='left', padx=5)
        ttk.Label(date_manual_frame, text="시작:").pack(side='left', padx=2)
        start_date_var = tk.StringVar()
        ttk.Entry(date_manual_frame, textvariable=start_date_var, width=12).pack(side='left', padx=2)
        ttk.Label(date_manual_frame, text="(YYYYMMDD)").pack(side='left', padx=2)
        
        ttk.Label(date_manual_frame, text="종료:").pack(side='left', padx=5)
        end_date_var = tk.StringVar()
        ttk.Entry(date_manual_frame, textvariable=end_date_var, width=12).pack(side='left', padx=2)
        ttk.Label(date_manual_frame, text="(YYYYMMDD)").pack(side='left', padx=2)
        
        # 시트명 필터
        sheet_frame = ttk.Frame(filter_frame)
        sheet_frame.pack(fill='x', pady=(5, 0))
        ttk.Label(sheet_frame, text="오픈마켓:").pack(side='left', padx=5)
        sheet_var = tk.StringVar()
        sheet_combo = ttk.Combobox(sheet_frame, textvariable=sheet_var, width=20, state="readonly")
        sheet_combo.pack(side='left', padx=5)
        
        # 트리뷰 프레임
        tree_frame = ttk.Frame(main_frame)
        tree_frame.pack(fill='both', expand=True)
        
        # 트리뷰 생성 (체크박스 컬럼 추가)
        columns = ("선택", "날짜", "오픈마켓", "스토어", "카테고리", "상품수", "출고모드", "새DB만", "생성시간", "파일경로")
        tree = ttk.Treeview(tree_frame, columns=columns, show='headings', height=20, selectmode='extended')
        
        # 컬럼 설정
        tree.heading("선택", text="선택")
        tree.heading("날짜", text="출고날짜")
        tree.heading("오픈마켓", text="오픈마켓")
        tree.heading("스토어", text="스토어")
        tree.heading("카테고리", text="카테고리")
        tree.heading("상품수", text="상품수")
        tree.heading("출고모드", text="출고모드")
        tree.heading("새DB만", text="새DB만")
        tree.heading("생성시간", text="생성시간")
        tree.heading("파일경로", text="파일경로")
        
        tree.column("선택", width=60)
        tree.column("날짜", width=100)
        tree.column("오픈마켓", width=120)
        tree.column("스토어", width=100)
        tree.column("카테고리", width=200)
        tree.column("상품수", width=80)
        tree.column("출고모드", width=150)
        tree.column("새DB만", width=80)
        tree.column("생성시간", width=150)
        tree.column("파일경로", width=300)
        
        # 체크박스 상태 저장용 딕셔너리 (item_id -> BooleanVar)
        tree_checkboxes = {}
        
        # 스크롤바
        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        
        tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        # 히스토리 데이터 저장용 딕셔너리 (item_id -> history_data)
        history_data_map = {}
        
        # 선택 제어 함수들을 먼저 정의 (selection_count_label은 나중에 생성되므로 None으로 처리)
        selection_count_label_ref = {'widget': None}  # 딕셔너리로 래핑하여 참조 전달
        
        def update_selection_count():
            """선택된 항목 수 업데이트"""
            try:
                if selection_count_label_ref['widget']:
                    selected_count = sum(1 for item_id in tree.get_children() 
                                       if tree_checkboxes.get(item_id, tk.BooleanVar(value=False)).get())
                    total_count = len(tree.get_children())
                    selection_count_label_ref['widget'].config(text=f"선택: {selected_count}/{total_count}개")
            except:
                pass
        
        def select_all_items():
            """모든 항목 선택"""
            for item_id in tree.get_children():
                checkbox_var = tree_checkboxes.get(item_id)
                if checkbox_var:
                    checkbox_var.set(True)
                    current_values = list(tree.item(item_id, 'values'))
                    current_values[0] = "☑"
                    tree.item(item_id, values=current_values)
            update_selection_count()
        
        def deselect_all_items():
            """모든 항목 선택 해제"""
            for item_id in tree.get_children():
                checkbox_var = tree_checkboxes.get(item_id)
                if checkbox_var:
                    checkbox_var.set(False)
                    current_values = list(tree.item(item_id, 'values'))
                    current_values[0] = "☐"
                    tree.item(item_id, values=current_values)
            update_selection_count()
        
        def toggle_all_items():
            """모든 항목 선택/해제 토글"""
            # 현재 선택된 항목 수 확인
            selected_count = sum(1 for item_id in tree.get_children() 
                               if tree_checkboxes.get(item_id, tk.BooleanVar(value=False)).get())
            total_count = len(tree.get_children())
            
            # 대부분이 선택되어 있으면 전체 해제, 아니면 전체 선택
            if selected_count >= total_count / 2:
                deselect_all_items()
            else:
                select_all_items()
        
        # 조회 함수 정의 (tree 생성 후)
        def refresh_history():
            start_date = start_date_var.get().strip() or None
            end_date = end_date_var.get().strip() or None
            sheet_name = sheet_var.get() or None
            if sheet_name == "전체":
                sheet_name = None
            
            # 히스토리 조회
            history_list = self.db_handler.get_export_history(
                limit=500,
                sheet_name=sheet_name,
                start_date=start_date,
                end_date=end_date
            )
            
            # 트리뷰 초기화 및 데이터 맵 초기화
            for item in tree.get_children():
                tree.delete(item)
            history_data_map.clear()
            
            # 데이터 표시
            for hist in history_list:
                categories = hist.get("categories", "")
                try:
                    if categories:
                        cat_list = json.loads(categories) if isinstance(categories, str) else categories
                        if isinstance(cat_list, list):
                            categories_display = ", ".join(cat_list[:3])
                            if len(cat_list) > 3:
                                categories_display += f" 외 {len(cat_list)-3}개"
                        else:
                            categories_display = str(categories)[:50]
                    else:
                        categories_display = "-"
                except:
                    categories_display = str(categories)[:50] if categories else "-"
                
                export_date = hist.get("export_date", "")
                created_at = hist.get("created_at", "")
                
                # 트리뷰에 항목 추가 (체크박스 컬럼 포함)
                item_id = tree.insert('', 'end', values=(
                    "☐",  # 체크박스 표시 (☐: 미선택, ☑: 선택)
                    export_date,
                    hist.get("sheet_name", "-"),
                    hist.get("store_alias", hist.get("store_name", "-")),
                    categories_display,
                    hist.get("product_count", 0),
                    hist.get("export_mode", "-"),
                    "예" if hist.get("exclude_assigned", 0) else "아니오",
                    created_at,
                    hist.get("file_path", "")
                ))
                
                # 체크박스 변수 생성 및 저장
                checkbox_var = tk.BooleanVar(value=False)
                tree_checkboxes[item_id] = checkbox_var
                
                # 히스토리 데이터 저장 (다시 다운로드용)
                history_data_map[item_id] = hist
            
            # 선택된 항목 수 업데이트
            update_selection_count()
        
        # 조회 버튼
        btn_refresh = ttk.Button(sheet_frame, text="조회", command=refresh_history)
        btn_refresh.pack(side='left', padx=10)
        
        # 시트명 목록 가져오기
        try:
            cursor = self.db_handler.conn.cursor()
            cursor.execute("SELECT DISTINCT sheet_name FROM export_history ORDER BY sheet_name")
            sheets = [row[0] for row in cursor.fetchall() if row[0]]
            sheet_combo['values'] = ['전체'] + sheets
            sheet_combo.current(0)
        except:
            pass
        
        # 초기값: 최근 한달로 설정
        set_date_range(30)
        
        # 트리뷰 더블클릭 이벤트: 파일 다시 다운로드 (DB에서 재생성)
        def on_tree_double_click(event):
            """트리뷰 항목 더블클릭 시 DB에서 데이터를 다시 조회하여 파일 재생성"""
            selection = tree.selection()
            if not selection:
                return
            
            item_id = selection[0]
            
            # 히스토리 데이터 가져오기
            hist = history_data_map.get(item_id)
            if not hist:
                messagebox.showwarning("경고", "히스토리 데이터를 찾을 수 없습니다.")
                return
            
            # 마켓 업로드용 모드인지 확인
            # 히스토리에 저장된 export_mode는 "마켓 업로드용(완료된DB)" 형식이므로 이를 체크
            export_mode_display = hist.get("export_mode", "")
            if not export_mode_display or "마켓 업로드용" not in export_mode_display:
                messagebox.showinfo("알림", f"현재 '{export_mode_display}' 모드는 재다운로드를 지원하지 않습니다.\n마켓 업로드용 모드만 재다운로드가 가능합니다.")
                return
            
            # 실제 export_mode는 "upload"로 설정
            export_mode = "upload"
            
            # DB 연결 확인
            if not self.db_handler or not self.db_handler.conn:
                messagebox.showerror("오류", "DB가 연결되지 않았습니다. DB 파일을 먼저 선택해주세요.")
                return
            
            # 원본 파일 경로 확인
            original_file_path = hist.get("file_path", "")
            
            # 저장 위치 선택
            file_name = hist.get("file_name", "")
            if not file_name:
                # 파일명이 없으면 기본 파일명 생성
                sheet_name = hist.get("sheet_name", "")
                store_alias = hist.get("store_alias", hist.get("store_name", ""))
                file_name = f"{hist.get('export_date', '')}_{store_alias}.xlsx"
            
            save_path = filedialog.asksaveasfilename(
                title="파일 저장 위치 선택",
                defaultextension=".xlsx",
                initialfile=file_name,
                filetypes=[("Excel 파일", "*.xlsx"), ("모든 파일", "*.*")]
            )
            
            if not save_path:
                return  # 사용자가 취소
            
            # 원본 파일이 존재하면 복사, 없으면 DB에서 재생성 (upload_logs 기록 안 함)
            if original_file_path and os.path.exists(original_file_path):
                # 파일이 있으면 복사
                try:
                    import shutil
                    shutil.copy2(original_file_path, save_path)
                    messagebox.showinfo("완료", f"파일이 복사되었습니다:\n{save_path}")
                    self._log(f"✅ 히스토리 파일 복사 완료: {os.path.basename(save_path)}")
                except Exception as e:
                    messagebox.showerror("오류", f"파일 복사 실패:\n{str(e)}")
                    self._log(f"⚠️ 파일 복사 실패: {e}")
            else:
                # 파일이 없으면 DB에서 재생성 (upload_logs 기록 안 함)
                try:
                    # 카테고리 파싱
                    categories_str = hist.get("categories", "")
                    categories_list = []
                    if categories_str:
                        try:
                            categories_list = json.loads(categories_str) if isinstance(categories_str, str) else categories_str
                            if not isinstance(categories_list, list):
                                categories_list = []
                        except:
                            categories_list = []
                    
                    if not categories_list:
                        messagebox.showwarning("경고", "카테고리 정보가 없습니다.")
                        return
                    
                    # market_info 구성
                    sheet_name = hist.get("sheet_name", "")
                    store_name = hist.get("store_name", "")
                    store_alias = hist.get("store_alias", "")
                    business_number = hist.get("business_number", "")
                    
                    # owner 정보는 히스토리에 없으므로 빈 문자열로 처리
                    # 실제로는 account_loader에서 찾아야 하지만, 간단하게 처리
                    market_info = {
                        "sheet_name": sheet_name,
                        "market_name": sheet_name,  # sheet_name을 market_name으로 사용
                        "store_name": store_name,
                        "alias": store_alias,
                        "business_number": business_number,
                        "biz_num": business_number,
                        "owner": ""  # 히스토리에 없으므로 빈 문자열
                    }
                    
                    exclude_assigned = bool(hist.get("exclude_assigned", 1))
                    
                    # 백그라운드 스레드에서 실행
                    self._log(f"=== 히스토리에서 재다운로드 시작 (DB 재생성) ===")
                    self._log(f"원본 파일 없음: {original_file_path}")
                    self._log(f"시트: {sheet_name}, 스토어: {store_alias}")
                    self._log(f"카테고리: {len(categories_list)}개")
                    self._log(f"새DB만: {'예' if exclude_assigned else '아니오'}")
                    self._log(f"⚠️ 주의: upload_logs에 기록되지 않습니다 (재다운로드이므로)")
                    
                    # 저장 디렉토리 설정
                    save_dir = os.path.dirname(save_path)
                    if not save_dir:
                        save_dir = os.getcwd()
                    
                    # 파일명만 전달 (전체 경로는 _run_export_for_upload에서 생성)
                    custom_filename = os.path.basename(save_path)
                    
                    # 히스토리 재다운로드용 함수 호출 (skip_logging=True)
                    threading.Thread(
                        target=self._re_export_from_history,
                        args=(categories_list, [market_info], save_dir, exclude_assigned, export_mode, custom_filename, True),
                        daemon=True
                    ).start()
                    
                except Exception as e:
                    messagebox.showerror("오류", f"재다운로드 실패:\n{str(e)}")
                    self._log(f"⚠️ 히스토리 재다운로드 실패: {e}")
        
        # 체크박스 클릭 이벤트 처리
        def on_tree_click(event):
            """트리뷰 클릭 시 체크박스 토글"""
            region = tree.identify_region(event.x, event.y)
            if region == "cell":
                column = tree.identify_column(event.x)
                if column == "#1":  # 첫 번째 컬럼 (선택 컬럼)
                    item_id = tree.identify_row(event.y)
                    if item_id:
                        checkbox_var = tree_checkboxes.get(item_id)
                        if checkbox_var:
                            # 체크 상태 토글
                            checkbox_var.set(not checkbox_var.get())
                            # 트리뷰 표시 업데이트
                            current_values = list(tree.item(item_id, 'values'))
                            current_values[0] = "☑" if checkbox_var.get() else "☐"
                            tree.item(item_id, values=current_values)
                            # 선택된 항목 수 업데이트
                            update_selection_count()
            elif region == "heading":
                # 헤더 클릭 시 첫 번째 컬럼이면 전체 선택/해제 토글
                column = tree.identify_column(event.x)
                if column == "#1":  # 첫 번째 컬럼 헤더
                    toggle_all_items()
        
        tree.bind("<Button-1>", on_tree_click)
        tree.bind("<Double-1>", on_tree_double_click)
        
        # 선택 제어 버튼 프레임
        selection_frame = ttk.Frame(main_frame)
        selection_frame.pack(fill='x', pady=(10, 5))
        
        # 선택된 항목 수 표시 레이블
        selection_count_label = ttk.Label(selection_frame, text="선택: 0/0개", font=("맑은 고딕", 9))
        selection_count_label.pack(side='left', padx=10)
        
        # selection_count_label_ref에 실제 위젯 저장 (함수에서 참조 가능하도록)
        selection_count_label_ref['widget'] = selection_count_label
        
        # 전체 선택/해제 버튼
        btn_select_all = ttk.Button(selection_frame, text="✅ 전체 선택", command=select_all_items)
        btn_select_all.pack(side='left', padx=5)
        
        btn_deselect_all = ttk.Button(selection_frame, text="❌ 전체 해제", command=deselect_all_items)
        btn_deselect_all.pack(side='left', padx=5)
        
        btn_toggle_all = ttk.Button(selection_frame, text="🔄 토글", command=toggle_all_items)
        btn_toggle_all.pack(side='left', padx=5)
        
        # 액션 버튼 프레임
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill='x', pady=(5, 0))
        
        # 엑셀 내보내기 버튼
        def export_history_to_excel():
            """현재 조회된 히스토리 목록을 Excel로 내보내기"""
            if not history_data_map:
                messagebox.showinfo("알림", "내보낼 히스토리가 없습니다.")
                return
            
            save_path = filedialog.asksaveasfilename(
                title="히스토리 목록 저장",
                defaultextension=".xlsx",
                initialfile=f"출고히스토리_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                filetypes=[("Excel 파일", "*.xlsx"), ("모든 파일", "*.*")]
            )
            
            if not save_path:
                return
            
            try:
                import pandas as pd
                excel_data = []
                
                for item_id in tree.get_children():
                    hist = history_data_map.get(item_id)
                    if hist:
                        categories = hist.get("categories", "")
                        try:
                            if categories:
                                cat_list = json.loads(categories) if isinstance(categories, str) else categories
                                if isinstance(cat_list, list):
                                    categories_str = ", ".join(cat_list)
                                else:
                                    categories_str = str(categories)
                            else:
                                categories_str = ""
                        except:
                            categories_str = str(categories) if categories else ""
                        
                        excel_data.append({
                            "출고날짜": hist.get("export_date", ""),
                            "오픈마켓": hist.get("sheet_name", ""),
                            "스토어명": hist.get("store_name", ""),
                            "스토어별칭": hist.get("store_alias", ""),
                            "사업자번호": hist.get("business_number", ""),
                            "카테고리": categories_str,
                            "상품수": hist.get("product_count", 0),
                            "출고모드": hist.get("export_mode", ""),
                            "새DB만": "예" if hist.get("exclude_assigned", 0) else "아니오",
                            "생성시간": hist.get("created_at", ""),
                            "파일경로": hist.get("file_path", ""),
                            "파일명": hist.get("file_name", "")
                        })
                
                df = pd.DataFrame(excel_data)
                df.to_excel(save_path, index=False, engine='openpyxl')
                
                messagebox.showinfo("완료", f"히스토리 목록을 Excel 파일로 저장했습니다.\n\n파일: {os.path.basename(save_path)}")
                self._log(f"✅ 히스토리 목록 Excel 내보내기 완료: {len(excel_data)}개 항목 ({os.path.basename(save_path)})")
            except PermissionError:
                messagebox.showerror("오류", "파일을 저장할 수 없습니다.\n파일이 다른 프로그램에서 열려있는지 확인하세요.")
            except Exception as e:
                messagebox.showerror("오류", f"Excel 파일 저장 중 오류가 발생했습니다:\n{str(e)}")
                self._log(f"❌ 히스토리 Excel 내보내기 실패: {e}")
        
        btn_export_excel = ttk.Button(action_frame, text="📊 Excel 내보내기", command=export_history_to_excel)
        btn_export_excel.pack(side='left', padx=5)
        
        # 선택된 항목 병합 다운로드 버튼
        def merge_selected_history():
            """선택된 여러 히스토리를 하나의 Excel 파일로 병합하여 재생성"""
            selected_items = []
            for item_id in tree.get_children():
                checkbox_var = tree_checkboxes.get(item_id)
                if checkbox_var and checkbox_var.get():
                    hist = history_data_map.get(item_id)
                    if hist:
                        # 마켓 업로드용 모드만 지원
                        export_mode_display = hist.get("export_mode", "")
                        if export_mode_display and "마켓 업로드용" in export_mode_display:
                            selected_items.append((item_id, hist))
                        else:
                            messagebox.showwarning("경고", 
                                f"'{export_mode_display}' 모드는 병합 다운로드를 지원하지 않습니다.\n"
                                "마켓 업로드용 모드만 지원됩니다.")
                            return
            
            if not selected_items:
                messagebox.showinfo("알림", "병합할 항목을 선택해주세요.\n(항목의 첫 번째 컬럼을 클릭하여 선택)")
                return
            
            if len(selected_items) == 1:
                messagebox.showinfo("알림", "병합하려면 2개 이상의 항목을 선택해주세요.")
                return
            
            # 저장 위치 선택
            from datetime import datetime
            save_path = filedialog.asksaveasfilename(
                title="병합 파일 저장 위치 선택",
                defaultextension=".xlsx",
                initialfile=f"병합_출고히스토리_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                filetypes=[("Excel 파일", "*.xlsx"), ("모든 파일", "*.*")]
            )
            
            if not save_path:
                return
            
            # DB 연결 확인
            if not self.db_handler or not self.db_handler.conn:
                messagebox.showerror("오류", "DB가 연결되지 않았습니다. DB 파일을 먼저 선택해주세요.")
                return
            
            # 병합 처리 (백그라운드 스레드)
            self._log(f"=== 선택된 히스토리 병합 다운로드 시작 ({len(selected_items)}개 항목) ===")
            
            threading.Thread(
                target=self._merge_history_items,
                args=(selected_items, save_path),
                daemon=True
            ).start()
        
        btn_merge_download = ttk.Button(action_frame, text="🔗 선택 항목 병합 다운로드", command=merge_selected_history)
        btn_merge_download.pack(side='left', padx=5)
        
        # 안내 메시지 및 닫기 버튼 프레임
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.pack(fill='x', pady=(10, 0))
        
        # 안내 메시지
        ttk.Label(bottom_frame, text="※ 목록 항목을 더블클릭하면 해당 파일을 다시 다운로드할 수 있습니다. | 첫 번째 컬럼을 클릭하여 선택 후 병합 다운로드 가능", 
                 foreground="#666", font=("맑은 고딕", 8)).pack(side='left', padx=5)
        
        # 닫기 버튼
        btn_close = ttk.Button(bottom_frame, text="닫기", command=history_window.destroy)
        btn_close.pack(side='right', padx=5)
        
        # 초기 데이터 로드
        refresh_history()
    
    def _select_excel_file(self):
        """마켓 계정 엑셀 파일 선택"""
        path = filedialog.askopenfilename(
            title="마켓 계정 엑셀 파일 선택",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile="Market_id_pw.xlsx"
        )
        if path:
            self.account_loader.set_excel_path(path)
            self.excel_path_var.set(path)
            self._log(f"엑셀 파일 선택: {os.path.basename(path)}")
            # 자동으로 계정 다시 로드
            self._reload_accounts()
    
    def _reload_accounts(self):
        """마켓 계정 다시 로드"""
        self._log("마켓 계정 로드 중...")
        accounts = self.account_loader.load_accounts()
        if accounts:
            self._log(f"마켓 계정 {len(accounts)}개 로드 완료")
            self._update_market_tree(accounts)
        else:
            self._log("⚠️ 마켓 계정을 로드할 수 없습니다. 엑셀 파일을 확인하세요.")
    
    def _select_db_file(self):
        """DB 파일 선택"""
        path = filedialog.askopenfilename(
            title="SQLite DB 파일 선택",
            filetypes=[("SQLite Database", "*.db"), ("All files", "*.*")]
        )
        if path:
            self.db_path = path
            self.db_path_var.set(path)
            # 선택한 경로를 설정 파일에 저장
            save_db_path_to_config(path)
            self._log(f"DB 파일 선택: {os.path.basename(path)}")
            self._log(f"경로가 설정 파일에 저장되었습니다.")
            # 카테고리 트리 로드
            self._load_categories()
    
    def _set_default_db_path(self):
        """현재 선택한 DB 경로를 기본 경로로 저장"""
        # 현재 선택된 DB 경로 가져오기
        current_db_path = self.db_path_var.get()
        
        if not current_db_path:
            messagebox.showwarning("경고", "DB 파일을 먼저 선택해주세요.")
            return
        
        if not os.path.exists(current_db_path):
            messagebox.showerror("오류", f"선택한 DB 파일이 존재하지 않습니다.\n\n{current_db_path}\n\n파일 경로를 확인해주세요.")
            return
        
        # 현재 경로를 설정 파일에 저장
        save_db_path_to_config(current_db_path)
        self.db_path = current_db_path
        self._log(f"기본 DB 경로로 저장: {os.path.basename(current_db_path)}")
        messagebox.showinfo("완료", f"현재 DB 경로가 기본 경로로 저장되었습니다.\n\n{current_db_path}\n\n다음 실행 시 자동으로 이 경로가 사용됩니다.")
    
    def _load_accounts(self):
        """마켓 계정 로드 (초기화 시)"""
        if self.account_loader.excel_path:
            accounts = self.account_loader.load_accounts()
            if accounts:
                self._log(f"마켓 계정 {len(accounts)}개 로드 완료")
                self._update_market_tree(accounts)
            else:
                self._log("⚠️ 마켓 계정을 로드할 수 없습니다. 엑셀 파일을 선택해주세요.")
        else:
            self._log("ℹ️ 마켓 계정 엑셀 파일을 선택해주세요.")
    
    def _update_market_tree(self, accounts: List[Dict[str, Any]]):
        """마켓 트리뷰 업데이트"""
        # 기존 트리뷰 아이템 제거
        for item in self.market_tree.get_children():
            self.market_tree.delete(item)
        self.tree_checkboxes.clear()
        self.market_tree_items.clear()
        
        # 트리 구조 생성
        tree_structure = self.account_loader.build_tree_structure()
        
        # 루트 노드: 전체 해제 (초기 상태)
        root_id = self.market_tree.insert("", "end", text="☐ 전체 선택", open=False)
        root_var = tk.BooleanVar(value=False)
        self.tree_checkboxes[root_id] = root_var
        self.market_tree_items[root_id] = {"type": "root", "accounts": []}

        # 소매처/도매처 그룹 노드 생성
        retail_id = self.market_tree.insert(root_id, "end", text="☐ [소매처]", open=False)
        retail_var = tk.BooleanVar(value=False)
        self.tree_checkboxes[retail_id] = retail_var
        self.market_tree_items[retail_id] = {"type": "group", "group_name": "소매처", "accounts": []}

        wholesale_id = self.market_tree.insert(root_id, "end", text="☐ [도매처]", open=False)
        wholesale_var = tk.BooleanVar(value=False)
        self.tree_checkboxes[wholesale_id] = wholesale_var
        self.market_tree_items[wholesale_id] = {"type": "group", "group_name": "도매처", "accounts": []}

        # 시트별로 트리 구성
        for sheet_name, owners in sorted(tree_structure.items()):
            # 숨김 시트는 건너뛰기
            if sheet_name in HIDDEN_SHEETS:
                continue

            # 도매처/소매처 그룹 결정
            parent_id = wholesale_id if sheet_name in WHOLESALE_SHEETS else retail_id

            # 시트 노드 (마켓 타입) - 초기 상태: 닫힘, 해제
            # 시트명을 표시명으로 변환 (매핑이 없으면 원래 시트명 사용)
            display_name = SHEET_DISPLAY_NAMES.get(sheet_name, sheet_name)
            sheet_id = self.market_tree.insert(parent_id, "end", text=f"☐ {display_name}", open=False)
            sheet_var = tk.BooleanVar(value=False)
            self.tree_checkboxes[sheet_id] = sheet_var
            self.market_tree_items[sheet_id] = {"type": "sheet", "sheet_name": sheet_name, "display_name": display_name, "accounts": []}
            
            # 명의자별로 트리 구성 (정렬: A, B 순서)
            for owner in sorted(owners.keys()):
                biz_numbers = owners[owner]
                # 명의자 노드 (이름 표시) - 초기 상태: 닫힘, 해제
                owner_name = OWNER_NAMES.get(owner, "")
                if owner_name:
                    owner_display = f"명의자: {owner} ({owner_name})"
                else:
                    owner_display = f"명의자: {owner}"
                
                owner_id = self.market_tree.insert(sheet_id, "end", text=f"☐ {owner_display}", open=False)
                owner_var = tk.BooleanVar(value=False)
                self.tree_checkboxes[owner_id] = owner_var
                self.market_tree_items[owner_id] = {"type": "owner", "owner": owner, "owner_name": owner_name, "accounts": []}
                
                # 사업자번호별로 트리 구성 (정렬: 숫자 순서)
                for biz_num in sorted(biz_numbers.keys(), key=lambda x: int(x) if x.isdigit() else 999):
                    stores = biz_numbers[biz_num]
                    # 사업자번호 노드 (상호명 표시) - 초기 상태: 닫힘, 해제
                    biz_key = f"{owner}{biz_num}".upper()
                    business_name = BUSINESS_NAMES.get(biz_key, "")
                    if business_name:
                        biz_display = f"사업자: {biz_num} ({business_name})"
                    else:
                        biz_display = f"사업자: {biz_num}"
                    
                    biz_id = self.market_tree.insert(owner_id, "end", text=f"☐ {biz_display}", open=False)
                    biz_var = tk.BooleanVar(value=False)
                    self.tree_checkboxes[biz_id] = biz_var
                    self.market_tree_items[biz_id] = {
                        "type": "biz", 
                        "biz_num": biz_num, 
                        "business_name": business_name,
                        "accounts": []
                    }
                    
                    # 스토어별로 트리 구성 (정렬: 스토어번호 순서)
                    for store_alias in sorted(stores.keys(), key=lambda x: self._get_store_sort_key(x)):
                        store_accounts = stores[store_alias]
                        # 계정의 활성화 상태 확인
                        is_store_active = any(acc.get("is_active", True) for acc in store_accounts)
                        
                        # 첫 번째 활성화된 계정에서 정보 추출 (또는 첫 번째 계정)
                        first_account = None
                        for acc in store_accounts:
                            if acc.get("is_active", True):
                                first_account = acc
                                break
                        if not first_account and store_accounts:
                            first_account = store_accounts[0]
                        
                        # market_name과 business_number 추출 (빈 문자열 체크)
                        if first_account:
                            market_name = first_account.get("market_name", "") or store_alias
                            business_number = first_account.get("business_number", "") or biz_num
                        else:
                            # 계정이 없는 경우 (이론적으로는 발생하지 않아야 함)
                            market_name = store_alias
                            business_number = biz_num
                        
                        # business_number가 여전히 빈 문자열이면 별칭에서 추출 시도
                        if not business_number or business_number.strip() == "":
                            parsed = self.account_loader.parse_alias(store_alias)
                            business_number = parsed.get("business_number", "") or biz_num
                        
                        # 최종 검증: market_name과 business_number가 모두 있어야 함
                        if not market_name or market_name.strip() == "":
                            market_name = store_alias
                        if not business_number or business_number.strip() == "":
                            # 사업자번호를 찾을 수 없으면 스토어를 비활성화
                            self._log(f"⚠️ 스토어 '{store_alias}': 사업자번호를 찾을 수 없어 비활성화합니다.")
                            is_store_active = False
                        
                        # 스토어 노드 (실제 계정)
                        # 메모 정보 로드
                        store_key = self._get_store_key(sheet_name, owner, biz_num, store_alias)
                        store_memo_data = self.store_memos.get(store_key, {})
                        memo_text = store_memo_data.get("memo", "")
                        memo_categories = store_memo_data.get("categories", [])
                        
                        # 초기 표시 (나중에 _update_store_display로 업데이트됨)
                        store_display = f"스토어: {store_alias}"
                        checkbox_icon = "☐"  # 초기 상태: 모두 해제
                        store_id = self.market_tree.insert(biz_id, "end", text=f"{checkbox_icon} {store_display}")
                        store_var = tk.BooleanVar(value=False)  # 초기 상태: 모두 해제
                        self.tree_checkboxes[store_id] = store_var
                        self.market_tree_items[store_id] = {
                            "type": "store",
                            "alias": store_alias,
                            "accounts": store_accounts,
                            "is_active": is_store_active,
                            "sheet_name": sheet_name,
                            "owner": owner,
                            "biz_num": biz_num,
                            "market_name": market_name,
                            "business_number": business_number,
                            "store_key": store_key  # 메모 키 추가
                        }
                        
                        # 비활성화된 스토어는 회색으로 표시
                        if not is_store_active:
                            self.market_tree.item(store_id, tags=("disabled",))
                            store_var.set(False)
                        
                        # 메모 정보 반영하여 표시 업데이트
                        self._update_store_display(store_id)
                        
                        # 상위 노드에 계정 추가
                        for account in store_accounts:
                            if account not in self.market_tree_items[biz_id]["accounts"]:
                                self.market_tree_items[biz_id]["accounts"].append(account)
                            if account not in self.market_tree_items[owner_id]["accounts"]:
                                self.market_tree_items[owner_id]["accounts"].append(account)
                            if account not in self.market_tree_items[sheet_id]["accounts"]:
                                self.market_tree_items[sheet_id]["accounts"].append(account)
                            if account not in self.market_tree_items[root_id]["accounts"]:
                                self.market_tree_items[root_id]["accounts"].append(account)
        
        # 트리뷰 아이템 클릭 이벤트 바인딩
        # ButtonRelease-1로 체크박스 토글 처리
        self.market_tree.bind("<ButtonRelease-1>", self._on_tree_click)
        
        # 더블클릭 이벤트 (등록된 상품수량 편집용)
        self.market_tree.bind("<Double-1>", self._on_tree_double_click)
        
        # 우클릭 메뉴 (메모 편집용)
        self.market_tree.bind("<Button-3>", self._on_tree_right_click)  # Windows
        self.market_tree.bind("<Button-2>", self._on_tree_right_click)  # macOS/Linux
        
        # 체크박스 상태 업데이트
        self._update_tree_checkboxes()
    
    def _get_store_sort_key(self, alias: str) -> tuple:
        """스토어 별칭 정렬 키 (스토어번호 순서)"""
        parsed = self.account_loader.parse_alias(alias)
        store_num = parsed.get("store_num", "0")
        try:
            return (int(store_num), alias)
        except:
            return (999, alias)
    
    def _get_store_key(self, sheet_name: str, owner: str, biz_num: str, alias: str) -> str:
        """스토어 고유 키 생성 (메모 저장용)"""
        # 시트명_명의자_사업자번호_별칭 형식으로 고유 키 생성
        return f"{sheet_name}::{owner}::{biz_num}::{alias}"
    
    def _get_category_large_medium(self, full_category: str) -> str:
        """전체 카테고리를 '대>중' 형식으로 변환"""
        # "대>중>소>세부" 형식에서 "대>중" 추출
        parts = [part.strip() for part in full_category.split('>')]
        if len(parts) >= 2:
            return f"{parts[0]} > {parts[1]}"
        return full_category
    


    def _normalize_store_categories(self, categories: List[str]) -> List[str]:
        """스토어 카테고리 정규화(대>중 + 중복 제거)"""
        return normalize_store_categories(categories)

    def _build_sheet_used_combinations_cache(self, db_handler: DBHandler, sheet_name: str) -> Dict[str, set]:
        """시트 단위 사용 조합 캐시 생성"""
        return build_sheet_used_combinations_cache(db_handler.conn, sheet_name)

    def _build_store_used_product_codes_cache(
        self,
        db_handler: DBHandler,
        sheet_name: str,
        business_number: str,
    ) -> set:
        """스토어 단위 이미 사용된 상품코드 캐시 생성"""
        return build_store_used_product_codes_cache(db_handler.conn, sheet_name, business_number)
    def _check_category_duplicates(self, sheet_name: str, owner: str, store_key: str, selected_categories: List[str]) -> List[Dict[str, Any]]:
        """같은 명의자 내 다른 스토어와 카테고리 중복 체크"""
        duplicates = []
        
        # 선택된 카테고리를 '대>중' 형식으로 변환
        selected_large_medium = set()
        for cat in selected_categories:
            large_medium = self._get_category_large_medium(cat)
            selected_large_medium.add(large_medium)
        
        # 같은 명의자 내 다른 스토어들의 카테고리 확인
        for other_store_key, memo_data in self.store_memos.items():
            # 같은 시트, 같은 명의자인지 확인
            if not other_store_key.startswith(f"{sheet_name}::{owner}::"):
                continue
            
            # 자기 자신은 제외
            if other_store_key == store_key:
                continue
            
            other_categories = memo_data.get("categories", [])
            other_store_alias = other_store_key.split("::")[-1] if "::" in other_store_key else ""
            
            # 다른 스토어의 카테고리를 '대>중' 형식으로 변환하여 비교
            for other_cat in other_categories:
                other_large_medium = self._get_category_large_medium(other_cat)
                if other_large_medium in selected_large_medium:
                    duplicates.append({
                        "store_alias": other_store_alias,
                        "category": other_large_medium,
                        "full_category": other_cat
                    })
        
        return duplicates
    
    def _load_store_memos(self):
        """스토어 메모 파일 로드"""
        if os.path.exists(self.store_memo_file):
            try:
                with open(self.store_memo_file, 'r', encoding='utf-8') as f:
                    self.store_memos = json.load(f)
                self._log(f"스토어 메모 {len(self.store_memos)}개 로드 완료")
            except Exception as e:
                self._log(f"⚠️ 스토어 메모 로드 실패: {e}")
                self.store_memos = {}
        else:
            self.store_memos = {}
    
    def _save_store_memos(self):
        """스토어 메모 파일 저장"""
        try:
            os.makedirs(os.path.dirname(self.store_memo_file), exist_ok=True)
            with open(self.store_memo_file, 'w', encoding='utf-8') as f:
                json.dump(self.store_memos, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._log(f"⚠️ 스토어 메모 저장 실패: {e}")
    
    def _on_tree_right_click(self, event):
        """트리뷰 우클릭 이벤트 (메모 편집 메뉴)"""
        item = self.market_tree.identify_row(event.y)
        if not item:
            return
        
        item_data = self.market_tree_items.get(item, {})
        
        # 스토어 노드만 메모 편집 가능
        if item_data.get("type") != "store":
            return
        
        # 우클릭 메뉴 생성
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="📝 메모 편집", command=lambda: self._edit_store_memo(item))
        menu.add_separator()
        menu.add_command(label="카테고리 정보 보기", command=lambda: self._show_store_categories(item))
        
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()
    
    def _edit_store_memo(self, store_id: str):
        """스토어 메모 편집 창"""
        item_data = self.market_tree_items.get(store_id, {})
        if item_data.get("type") != "store":
            return
        
        store_key = item_data.get("store_key", "")
        if not store_key:
            return
        
        store_alias = item_data.get("alias", "")
        sheet_name = item_data.get("sheet_name", "")
        owner = item_data.get("owner", "")
        biz_num = item_data.get("biz_num", "")
        
        # 기존 메모 로드
        memo_data = self.store_memos.get(store_key, {})
        current_memo = memo_data.get("memo", "")
        current_categories = memo_data.get("categories", [])
        
        # 메모 편집 창 생성
        memo_window = tk.Toplevel(self)
        memo_window.title(f"스토어 메모 편집: {store_alias}")
        memo_window.geometry("700x750")
        memo_window.transient(self)
        memo_window.grab_set()
        memo_window.minsize(650, 700)

        # 메인 창 위치 기준으로 메모 창 위치 정렬
        try:
            self.update_idletasks()
            memo_window.update_idletasks()
            parent_x = self.winfo_rootx()
            parent_y = self.winfo_rooty()
            parent_w = self.winfo_width()
            parent_h = self.winfo_height()

            win_w = memo_window.winfo_reqwidth()
            win_h = memo_window.winfo_reqheight()

            # 부모 창 중앙 근처에 배치 (상단으로 더 올림)
            x = parent_x + max(0, (parent_w - win_w) // 2)
            # y 좌표를 상단으로 더 조정 (부모 높이의 15% 지점에 배치)
            y = parent_y + max(30, int(parent_h * 0.15))  # 최소 30px, 부모 높이의 15% 지점
            memo_window.geometry(f"+{x}+{y}")
        except Exception:
            # 위치 계산 실패 시 기본 위치 사용
            pass
        
        # 메인 컨테이너 (버튼 영역을 위한 공간 확보)
        content_container = ttk.Frame(memo_window)
        content_container.pack(fill='both', expand=True, padx=10, pady=(10, 0))
        
        # 스크롤 가능한 콘텐츠 영역
        canvas = tk.Canvas(content_container, bg="#ffffff")
        scrollbar = ttk.Scrollbar(content_container, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        frame = scrollable_frame
        
        # 스토어 정보 표시
        info_frame = ttk.LabelFrame(frame, text="스토어 정보", padding=10)
        info_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Label(info_frame, text=f"마켓: {SHEET_DISPLAY_NAMES.get(sheet_name, sheet_name)}", font=("맑은 고딕", 9)).pack(anchor='w')
        ttk.Label(info_frame, text=f"명의자: {owner}", font=("맑은 고딕", 9)).pack(anchor='w')
        ttk.Label(info_frame, text=f"사업자번호: {biz_num}", font=("맑은 고딕", 9)).pack(anchor='w')
        ttk.Label(info_frame, text=f"스토어 별칭: {store_alias}", font=("맑은 고딕", 9, "bold")).pack(anchor='w', pady=(5, 0))
        
        # 현재 선택된 카테고리 정보 표시 (동적 업데이트)
        category_info_label = ttk.Label(info_frame, text="선택된 카테고리: 없음", 
                                        font=("맑은 고딕", 9), foreground="#666", wraplength=600, justify='left')
        category_info_label.pack(anchor='w', pady=(5, 0))
        
        # 카테고리 선택 영역
        category_frame = ttk.LabelFrame(frame, text="업로드 예정 카테고리 (중복 방지용)", padding=10)
        category_frame.pack(fill='both', expand=False, pady=(0, 10))
        
        # 카테고리 분배 추천 버튼 추가
        recommend_btn_frame = ttk.Frame(category_frame)
        recommend_btn_frame.pack(fill='x', pady=(0, 5))
        
        def recommend_category_distribution():
            """명의자별 카테고리 분배 추천"""
            # 같은 명의자의 모든 스토어 정보 수집
            owner_stores = []
            for store_id, store_data in self.market_tree_items.items():
                if store_data.get("type") == "store":
                    store_sheet = store_data.get("sheet_name", "")
                    store_owner = store_data.get("owner", "")
                    if store_sheet == sheet_name and store_owner == owner:
                        store_key = store_data.get("store_key", "")
                        store_alias = store_data.get("alias", "")
                        memo_data = self.store_memos.get(store_key, {})
                        existing_categories = memo_data.get("categories", [])
                        owner_stores.append({
                            "store_id": store_id,
                            "store_key": store_key,
                            "alias": store_alias,
                            "existing_categories": existing_categories
                        })
            
            if len(owner_stores) <= 1:
                messagebox.showinfo("알림", "같은 명의자 내 다른 스토어가 없어 추천할 수 없습니다.\n명의자당 2개 이상의 스토어가 있어야 분배 추천이 가능합니다.")
                return
            
            # DB에서 사용 가능한 모든 카테고리와 상품수 조회
            if not self.db_handler or not self.db_handler.conn:
                messagebox.showwarning("경고", "DB가 연결되지 않았습니다. DB 파일을 먼저 선택해주세요.")
                return
            
            try:
                # 카테고리별 상품수 조회
                cursor = self.db_handler.conn.cursor()
                cursor.execute("""
                    SELECT 카테고리명, COUNT(DISTINCT 상품코드) as product_count
                    FROM products
                    WHERE product_status = 'ACTIVE'
                    AND product_names_json IS NOT NULL 
                    AND product_names_json != '' 
                    AND product_names_json != '[]'
                    GROUP BY 카테고리명
                    ORDER BY product_count DESC
                """)
                
                category_products = {}
                for row in cursor.fetchall():
                    cat_name = row[0]
                    count = row[1]
                    if cat_name:
                        # 대>중 형식으로 변환
                        large_medium = self._get_category_large_medium(cat_name)
                        if large_medium not in category_products:
                            category_products[large_medium] = 0
                        category_products[large_medium] += count
                
                if not category_products:
                    messagebox.showinfo("알림", "사용 가능한 카테고리가 없습니다.")
                    return
                
                # 이미 할당된 카테고리 확인 (같은 명의자 내에서만)
                assigned_categories = set()
                for store in owner_stores:
                    for cat in store["existing_categories"]:
                        large_medium = self._get_category_large_medium(cat)
                        assigned_categories.add(large_medium)
                
                # 사용 가능한 카테고리 (아직 할당되지 않은 것)
                # 주의: DB 카테고리 수는 전체 DB 기준이지만, 할당 체크는 같은 명의자 내에서만 수행
                # 따라서 다른 명의자와는 독립적으로 작동함
                available_categories = {k: v for k, v in category_products.items() if k not in assigned_categories}
                
                if not available_categories:
                    messagebox.showinfo("알림", "모든 카테고리가 이미 할당되었습니다.")
                    return
                
                # 분배 추천 생성
                # 전략: 상품수가 많은 카테고리부터 스토어에 균등하게 분배
                sorted_categories = sorted(available_categories.items(), key=lambda x: x[1], reverse=True)
                store_count = len(owner_stores)
                
                # 각 스토어별로 추천 카테고리 목록 생성
                recommendations = {}
                for idx, (cat, product_count) in enumerate(sorted_categories):
                    # 라운드로빈 방식으로 분배
                    store_idx = idx % store_count
                    store = owner_stores[store_idx]
                    store_alias = store["alias"]
                    
                    if store_alias not in recommendations:
                        recommendations[store_alias] = []
                    recommendations[store_alias].append({
                        "category": cat,
                        "product_count": product_count
                    })
                
                # 추천 결과 표시
                result_window = tk.Toplevel(memo_window)
                result_window.title("카테고리 분배 추천")
                result_window.geometry("600x500")
                result_window.transient(memo_window)
                
                result_frame = ttk.Frame(result_window, padding=20)
                result_frame.pack(fill='both', expand=True)
                
                # 스크롤 가능한 영역
                result_canvas = tk.Canvas(result_frame, bg="#ffffff")
                result_scrollbar = ttk.Scrollbar(result_frame, orient="vertical", command=result_canvas.yview)
                result_scrollable = ttk.Frame(result_canvas)
                
                result_scrollable.bind(
                    "<Configure>",
                    lambda e: result_canvas.configure(scrollregion=result_canvas.bbox("all"))
                )
                
                result_canvas.create_window((0, 0), window=result_scrollable, anchor="nw")
                result_canvas.configure(yscrollcommand=result_scrollbar.set)
                
                # 추천 정보 표시
                info_text = f"명의자 '{owner}' 내 {store_count}개 스토어에 대한 카테고리 분배 추천\n\n"
                info_text += f"총 사용 가능한 카테고리: {len(available_categories)}개\n"
                info_text += f"총 상품수: {sum(available_categories.values()):,}개\n\n"
                
                ttk.Label(result_scrollable, text=info_text, font=("맑은 고딕", 9, "bold"), justify='left').pack(anchor='w', pady=(0, 10))
                
                for store_alias in sorted(recommendations.keys()):
                    store_recs = recommendations[store_alias]
                    total_products = sum(r["product_count"] for r in store_recs)
                    
                    store_frame = ttk.LabelFrame(result_scrollable, text=f"스토어: {store_alias} (예상 상품수: {total_products:,}개)", padding=10)
                    store_frame.pack(fill='x', pady=(0, 10))
                    
                    for rec in store_recs[:10]:  # 최대 10개만 표시
                        cat_text = f"  • {rec['category']} ({rec['product_count']:,}개)"
                        ttk.Label(store_frame, text=cat_text, font=("맑은 고딕", 8), justify='left').pack(anchor='w')
                    
                    if len(store_recs) > 10:
                        ttk.Label(store_frame, text=f"  ... 외 {len(store_recs)-10}개", 
                                font=("맑은 고딕", 8), foreground="#666", justify='left').pack(anchor='w')
                
                result_canvas.pack(side="left", fill="both", expand=True)
                result_scrollbar.pack(side="right", fill="y")
                
                # 현재 스토어에 추천 적용 버튼
                def apply_recommendation():
                    current_store_recs = recommendations.get(store_alias, [])
                    if not current_store_recs:
                        messagebox.showinfo("알림", "현재 스토어에 추천할 카테고리가 없습니다.")
                        return
                    
                    # 현재 스토어의 기존 카테고리 확인
                    current_store_existing = set()
                    for cat in current_categories:
                        large_medium = self._get_category_large_medium(cat)
                        current_store_existing.add(large_medium)
                    
                    # 추천 카테고리 중 기존에 없는 것만 추가
                    recommended_cats = [rec["category"] for rec in current_store_recs if rec["category"] not in current_store_existing]
                    
                    if not recommended_cats:
                        messagebox.showinfo("알림", "추천된 모든 카테고리가 이미 선택되어 있습니다.")
                        return
                    
                    # 기존 선택이 있으면 확인
                    if current_categories:
                        response = messagebox.askyesno(
                            "확인",
                            f"현재 스토어에 이미 {len(current_categories)}개의 카테고리가 선택되어 있습니다.\n\n"
                            f"추천 카테고리 {len(recommended_cats)}개를 추가로 선택하시겠습니까?\n\n"
                            f"(기존 선택은 유지되고, 추천 카테고리만 추가됩니다)"
                        )
                        if not response:
                            return
                    
                    # 카테고리 트리뷰에서 해당 카테고리 선택 (추가)
                    added_count = 0
                    for item_id, item_data in category_tree_items.items():
                        if item_data.get("type") == "medium":
                            large = item_data.get("large", "")
                            medium = item_data.get("medium", "")
                            large_medium = f"{large} > {medium}"
                            if large_medium in recommended_cats:
                                if item_id in category_tree_checkboxes:
                                    if not category_tree_checkboxes[item_id].get():  # 이미 선택된 것은 건너뜀
                                        category_tree_checkboxes[item_id].set(True)
                                        category_treeview.item(item_id, text=f"☑ {large} > {medium}")
                                        added_count += 1
                    
                    # 부모 노드 상태 업데이트
                    def update_parent_state_local(item_id):
                        """로컬 함수로 부모 노드 상태 업데이트"""
                        parent_id = category_treeview.parent(item_id)
                        if parent_id and parent_id in category_tree_checkboxes:
                            children = category_treeview.get_children(parent_id)
                            all_checked = all(
                                category_tree_checkboxes.get(child, tk.BooleanVar()).get()
                                for child in children
                                if child in category_tree_checkboxes
                            )
                            category_tree_checkboxes[parent_id].set(all_checked)
                            if parent_id:
                                update_parent_state_local(parent_id)
                    
                    for item_id in category_tree_items.keys():
                        if category_tree_items[item_id].get("type") == "medium":
                            update_parent_state_local(item_id)
                    
                    # 카테고리 정보 업데이트
                    update_category_info()
                    messagebox.showinfo("완료", f"현재 스토어 '{store_alias}'에 추천 카테고리 {added_count}개가 추가되었습니다.\n\n(기존 {len(current_categories)}개 + 추가 {added_count}개 = 총 {len(current_categories) + added_count}개)")
                    result_window.destroy()
                
                btn_frame = ttk.Frame(result_window)
                btn_frame.pack(fill='x', padx=20, pady=10)
                ttk.Button(btn_frame, text="현재 스토어에 추천 적용", command=apply_recommendation).pack(side='left', padx=5)
                ttk.Button(btn_frame, text="닫기", command=result_window.destroy).pack(side='right', padx=5)
                
            except Exception as e:
                messagebox.showerror("오류", f"추천 생성 실패:\n{str(e)}")
                self._log(f"⚠️ 카테고리 분배 추천 실패: {e}")
        
        ttk.Button(recommend_btn_frame, text="📊 카테고리 분배 추천", command=recommend_category_distribution).pack(side='left', padx=5)
        ToolTip(recommend_btn_frame, "같은 명의자 내 모든 스토어를 고려하여 카테고리를 균등하게 분배하는 추천을 제공합니다.")
        
        ttk.Label(category_frame, text="대카테고리 또는 중카테고리를 선택하세요. (대카테고리 선택 시 해당 대카테고리의 모든 중카테고리가 선택됩니다)\n중카테고리 선택 시 해당 중카테고리의 모든 소카테고리가 포함됩니다.\n같은 명의자 내 다른 스토어와 중복되지 않도록 관리합니다.", 
                 font=("맑은 고딕", 8), foreground="#666").pack(anchor='w', pady=(5, 0))
        
        # 카테고리 트리뷰 버튼 영역
        category_btn_frame = ttk.Frame(category_frame)
        category_btn_frame.pack(fill='x', pady=(0, 5))
        
        # 카테고리 정보 업데이트 함수 정의
        def update_category_info():
            """선택된 카테고리 정보 업데이트"""
            selected_categories = []
            for item_id, var in category_tree_checkboxes.items():
                if var.get():
                    item_data = category_tree_items.get(item_id, {})
                    if item_data.get("type") == "medium":
                        large_cat = item_data.get("large", "")
                        medium_cat = item_data.get("medium", "")
                        selected_categories.append(f"{large_cat} > {medium_cat}")
            
            if selected_categories:
                # 카테고리를 그룹화하여 표시
                categories_text = ", ".join(selected_categories[:5])  # 최대 5개만 표시
                if len(selected_categories) > 5:
                    categories_text += f" 외 {len(selected_categories)-5}개"
                category_info_label.config(text=f"선택된 카테고리: {categories_text} ({len(selected_categories)}개)", 
                                         foreground="#2c3e50")
            else:
                category_info_label.config(text="선택된 카테고리: 없음", foreground="#666")
        
        def select_all_medium_categories():
            """모든 중카테고리 선택"""
            for item_id, item_data in category_tree_items.items():
                if item_data.get("type") == "medium":
                    var = category_tree_checkboxes.get(item_id)
                    if var:
                        var.set(True)
                        large_cat = item_data.get("large", "")
                        medium_cat = item_data.get("medium", "")
                        category_treeview.item(item_id, text=f"☑ {large_cat} > {medium_cat}")
            update_category_info()
        
        def deselect_all_medium_categories():
            """모든 중카테고리 해제"""
            for item_id, item_data in category_tree_items.items():
                if item_data.get("type") == "medium":
                    var = category_tree_checkboxes.get(item_id)
                    if var:
                        var.set(False)
                        large_cat = item_data.get("large", "")
                        medium_cat = item_data.get("medium", "")
                        category_treeview.item(item_id, text=f"☐ {large_cat} > {medium_cat}")
            update_category_info()
        
        def expand_all_categories():
            """모든 카테고리 펼치기"""
            for item_id in category_tree_items.keys():
                category_treeview.item(item_id, open=True)
        
        def collapse_all_categories():
            """모든 카테고리 접기"""
            for item_id in category_tree_items.keys():
                category_treeview.item(item_id, open=False)
        
        ttk.Button(category_btn_frame, text="전체 선택", command=select_all_medium_categories, width=10).pack(side='left', padx=2)
        ttk.Button(category_btn_frame, text="전체 해제", command=deselect_all_medium_categories, width=10).pack(side='left', padx=2)
        ttk.Button(category_btn_frame, text="전체 펼치기", command=expand_all_categories, width=10).pack(side='left', padx=2)
        ttk.Button(category_btn_frame, text="전체 접기", command=collapse_all_categories, width=10).pack(side='left', padx=2)
        
        # 카테고리 트리뷰 (체크박스 포함)
        category_tree_frame = ttk.Frame(category_frame)
        category_tree_frame.pack(fill='both', expand=False)
        
        category_treeview = ttk.Treeview(category_tree_frame, show='tree', selectmode='none', height=10)
        category_tree_scrollbar = ttk.Scrollbar(category_tree_frame, orient="vertical", command=category_treeview.yview)
        category_treeview.configure(yscrollcommand=category_tree_scrollbar.set)
        
        category_treeview.pack(side='left', fill='both', expand=True)
        category_tree_scrollbar.pack(side='right', fill='y')
        
        # 카테고리 트리뷰 체크박스 변수 저장
        category_tree_checkboxes = {}  # {item_id: BooleanVar}
        category_tree_items = {}  # {item_id: {"type": "large"/"medium", "large": ..., "medium": ..., "full_categories": [...]}}
        
        # 카테고리 목록 로드 (DB에서)
        category_tree = {}
        if self.db_handler:
            try:
                category_tree = self.db_handler.get_category_tree()
            except:
                pass
        
        # 성능 최적화: 먼저 트리뷰 구조만 생성 (상품수 없이)
        large_ids_map = {}  # {large_cat: large_id}
        medium_ids_map = {}  # {(large_cat, medium_cat): medium_id}
        
        for large_cat in sorted(category_tree.keys()):
            # 대카테고리 노드 (체크박스 포함) - 상품수는 나중에 업데이트
            large_id = category_treeview.insert('', 'end', text=f"☐ {large_cat}", open=False)
            large_var = tk.BooleanVar()
            category_tree_checkboxes[large_id] = large_var
            category_tree_items[large_id] = {"type": "large", "large": large_cat}
            large_ids_map[large_cat] = large_id
            
            for medium_cat in sorted(category_tree[large_cat].keys()):
                full_categories = category_tree[large_cat][medium_cat]
                
                # 중카테고리 노드 (선택 가능) - 상품수는 나중에 업데이트
                medium_id = category_treeview.insert(large_id, 'end', text=f"☐ {large_cat} > {medium_cat}")
                medium_var = tk.BooleanVar()
                category_tree_checkboxes[medium_id] = medium_var
                category_tree_items[medium_id] = {
                    "type": "medium",
                    "large": large_cat,
                    "medium": medium_cat,
                    "full_categories": full_categories
                }
                medium_ids_map[(large_cat, medium_cat)] = medium_id
                
                # 기존 선택된 카테고리 확인 (중카테고리 기준)
                is_selected = False
                for full_cat in full_categories:
                    if full_cat in current_categories:
                        is_selected = True
                        break
                if is_selected:
                    medium_var.set(True)
                    category_treeview.item(medium_id, text=f"☑ {large_cat} > {medium_cat}")
                
                # 소카테고리 노드들 (표시만, 선택 불가)
                small_categories = {}
                for full_cat in full_categories:
                    parts = [p.strip() for p in full_cat.split('>')]
                    if len(parts) >= 3:
                        small_cat = parts[2]  # 소카테고리
                        if small_cat not in small_categories:
                            small_categories[small_cat] = []
                        small_categories[small_cat].append(full_cat)
                
                for small_cat in sorted(small_categories.keys()):
                    small_count = len(small_categories[small_cat])
                    small_id = category_treeview.insert(medium_id, 'end', 
                                                       text=f"  └ {small_cat} ({small_count}개)", 
                                                       tags=('small',))
                    category_treeview.item(small_id, tags=('small',))
            
            # 대카테고리 체크박스 상태 업데이트 (하위 중카테고리 상태에 따라)
            # 해당 대카테고리의 모든 중카테고리가 선택되어 있으면 대카테고리도 체크
            all_medium_selected = all(
                category_tree_checkboxes.get(
                    mid, tk.BooleanVar()
                ).get() 
                for mid in category_treeview.get_children(large_id)
                if mid in category_tree_checkboxes and category_tree_items.get(mid, {}).get("type") == "medium"
            )
            if all_medium_selected and category_tree[large_cat]:
                large_var.set(True)
                category_treeview.item(large_id, text=f"☑ {large_cat}")
        
        # 성능 최적화: 상품수 조회 제거 (렉 발생 원인)
        # 상품수는 마켓/스토어 섹션의 상품수 컬럼에서 확인 가능
        
        # 소카테고리 스타일 (회색, 선택 불가)
        category_treeview.tag_configure('small', foreground="#999999")
        
        # 트리뷰 클릭 이벤트 (체크박스 토글)
        def on_category_tree_click(event):
            item = category_treeview.identify_row(event.y)
            if not item:
                return
            
            element = category_treeview.identify_element(event.x, event.y)
            
            # 화살표(indicator) 클릭 시: 접기/펼치기만 수행
            if "indicator" in element:
                return
            
            item_data = category_tree_items.get(item, {})
            item_type = item_data.get("type")
            
            # 대카테고리 또는 중카테고리 선택 가능
            if item_type not in ["large", "medium"]:
                return
            
            # 체크박스 토글
            if item in category_tree_checkboxes:
                var = category_tree_checkboxes[item]
                new_state = not var.get()
                var.set(new_state)
                
                if item_type == "large":
                    # 대카테고리 선택 시: 해당 대카테고리의 모든 중카테고리 선택/해제
                    large_cat = item_data.get("large", "")
                    children = category_treeview.get_children(item)
                    for child_id in children:
                        child_data = category_tree_items.get(child_id, {})
                        if child_data.get("type") == "medium":
                            child_var = category_tree_checkboxes.get(child_id)
                            if child_var:
                                child_var.set(new_state)
                                child_large = child_data.get("large", "")
                                child_medium = child_data.get("medium", "")
                                child_icon = "☑" if new_state else "☐"
                                category_treeview.item(child_id, text=f"{child_icon} {child_large} > {child_medium}")
                    
                    # 대카테고리 시각적 업데이트
                    checkbox_icon = "☑" if new_state else "☐"
                    category_treeview.item(item, text=f"{checkbox_icon} {large_cat}")
                
                elif item_type == "medium":
                    # 중카테고리 선택 시: 시각적 업데이트
                    large_cat = item_data.get("large", "")
                    medium_cat = item_data.get("medium", "")
                    checkbox_icon = "☑" if new_state else "☐"
                    category_treeview.item(item, text=f"{checkbox_icon} {large_cat} > {medium_cat}")
                    
                    # 대카테고리 체크박스 상태 업데이트 (하위 중카테고리 상태에 따라)
                    parent_id = category_treeview.parent(item)
                    if parent_id and parent_id in category_tree_checkboxes:
                        parent_children = category_treeview.get_children(parent_id)
                        all_selected = all(
                            category_tree_checkboxes.get(
                                cid, tk.BooleanVar()
                            ).get()
                            for cid in parent_children
                            if cid in category_tree_checkboxes and category_tree_items.get(cid, {}).get("type") == "medium"
                        )
                        parent_var = category_tree_checkboxes[parent_id]
                        parent_var.set(all_selected)
                        parent_data = category_tree_items.get(parent_id, {})
                        parent_large = parent_data.get("large", "")
                        parent_icon = "☑" if all_selected else "☐"
                        category_treeview.item(parent_id, text=f"{parent_icon} {parent_large}")
                
                # 카테고리 정보 업데이트
                update_category_info()
        
        category_treeview.bind("<ButtonRelease-1>", on_category_tree_click)
        
        # 초기 카테고리 정보 표시
        update_category_info()
        
        # 메모 입력 영역
        memo_frame = ttk.LabelFrame(frame, text="메모", padding=10)
        memo_frame.pack(fill='both', expand=False, pady=(0, 10))
        
        memo_text = ScrolledText(memo_frame, height=6, font=("맑은 고딕", 9), wrap='word')
        memo_text.pack(fill='both', expand=True)
        memo_text.insert('1.0', current_memo)
        
        # 스크롤 영역 업데이트
        canvas.update_idletasks()
        
        # 버튼 영역 (하단 고정)
        btn_frame = ttk.Frame(memo_window)
        btn_frame.pack(fill='x', padx=10, pady=10, side='bottom')
        
        def save_memo():
            # 선택된 카테고리 가져오기 (중카테고리 기준)
            selected_categories = []
            for item_id, var in category_tree_checkboxes.items():
                if var.get():
                    item_data = category_tree_items.get(item_id, {})
                    if item_data.get("type") == "medium":
                        # 중카테고리의 모든 소카테고리 포함
                        full_categories = item_data.get("full_categories", [])
                        selected_categories.extend(full_categories)
            
            # 중복 체크
            duplicates = self._check_category_duplicates(sheet_name, owner, store_key, selected_categories)
            
            if duplicates:
                # 중복 발견 시 경고 메시지 (간결하게 표시)
                duplicate_stores = sorted(set(dup['store_alias'] for dup in duplicates))
                duplicate_categories = sorted(set(dup['category'] for dup in duplicates))
                
                # 카테고리를 대>중 기준으로 그룹화하여 요약
                category_summary = {}
                for dup_cat in duplicate_categories:
                    parts = dup_cat.split('>')
                    if len(parts) >= 2:
                        large_medium = '>'.join(parts[:2])  # 대>중
                        if large_medium not in category_summary:
                            category_summary[large_medium] = []
                        if len(parts) > 2:
                            # 소카테고리 개수 세기
                            category_summary[large_medium].append(dup_cat)
                    else:
                        # 대>중 형식이 아닌 경우 그대로 표시
                        if dup_cat not in category_summary:
                            category_summary[dup_cat] = []
                
                # 요약 메시지 생성
                summary_lines = []
                for large_medium, sub_cats in sorted(category_summary.items()):
                    if sub_cats:
                        # 대>중 외 소 N개 형식
                        summary_lines.append(f"  • {large_medium} 외 소 {len(sub_cats)}개")
                    else:
                        # 대>중만 있는 경우
                        summary_lines.append(f"  • {large_medium}")
                
                # 가로로 표시하기 위해 줄바꿈 대신 쉼표 사용
                category_display = ", ".join(summary_lines[:10])  # 최대 10개만 표시
                if len(summary_lines) > 10:
                    category_display += f" 외 {len(summary_lines) - 10}개"
                
                warning_msg = (
                    f"⚠️ 카테고리 중복 경고\n\n"
                    f"같은 명의자 내 다른 스토어와 중복된 카테고리가 있습니다.\n\n"
                    f"중복 스토어: {', '.join(duplicate_stores)}\n"
                    f"중복 카테고리: {category_display}\n\n"
                    f"그래도 저장하시겠습니까?"
                )
                
                if not messagebox.askyesno("카테고리 중복 경고", warning_msg, icon='warning'):
                    return  # 저장 취소
            
            # 메모 텍스트 가져오기
            memo_content = memo_text.get('1.0', 'end-1c').strip()
            
            # 메모 저장
            self.store_memos[store_key] = {
                "memo": memo_content,
                "categories": selected_categories,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self._save_store_memos()
            
            # 트리뷰 업데이트 (메모 표시)
            self._update_store_display(store_id)
            
            # 중복이 있었으면 로그에 기록
            if duplicates:
                self._log(f"⚠️ 스토어 메모 저장 완료 (중복 경고): {store_alias} (카테고리 {len(selected_categories)}개, 중복 {len(duplicates)}개)")
            else:
                self._log(f"✅ 스토어 메모 저장 완료: {store_alias} (카테고리 {len(selected_categories)}개)")
            
            messagebox.showinfo("완료", f"메모가 저장되었습니다.\n\n카테고리: {len(selected_categories)}개\n메모: {len(memo_content)}자")
            memo_window.destroy()
        
        def delete_memo():
            if messagebox.askyesno("확인", "메모를 삭제하시겠습니까?"):
                if store_key in self.store_memos:
                    del self.store_memos[store_key]
                    self._save_store_memos()
                    self._update_store_display(store_id)
                    self._log(f"✅ 스토어 메모 삭제 완료: {store_alias}")
                    messagebox.showinfo("완료", "메모가 삭제되었습니다.")
                    memo_window.destroy()
        
        ttk.Button(btn_frame, text="💾 저장", command=save_memo).pack(side='right', padx=5)
        ttk.Button(btn_frame, text="🗑️ 삭제", command=delete_memo).pack(side='right', padx=5)
        ttk.Button(btn_frame, text="취소", command=memo_window.destroy).pack(side='right', padx=5)
    
    def _show_store_categories(self, store_id: str):
        """스토어 카테고리 정보 보기"""
        item_data = self.market_tree_items.get(store_id, {})
        if item_data.get("type") != "store":
            return
        
        store_key = item_data.get("store_key", "")
        store_alias = item_data.get("alias", "")
        sheet_name = item_data.get("sheet_name", "")
        owner = item_data.get("owner", "")
        
        memo_data = self.store_memos.get(store_key, {})
        categories = memo_data.get("categories", [])
        memo = memo_data.get("memo", "")
        
        info_window = tk.Toplevel(self)
        info_window.title(f"스토어 정보: {store_alias}")
        info_window.geometry("600x500")
        
        frame = ttk.Frame(info_window, padding=20)
        frame.pack(fill='both', expand=True)
        
        # 스크롤 가능한 영역
        canvas = tk.Canvas(frame, bg="#ffffff")
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        if categories:
            # 카테고리를 '대>중' 형식으로 그룹화
            category_groups = {}
            for cat in categories:
                large_medium = self._get_category_large_medium(cat)
                if large_medium not in category_groups:
                    category_groups[large_medium] = []
                category_groups[large_medium].append(cat)
            
            ttk.Label(scrollable_frame, text=f"업로드 예정 카테고리 ({len(categories)}개, {len(category_groups)}개 그룹):", 
                     font=("맑은 고딕", 10, "bold")).pack(anchor='w', pady=(0, 10))
            
            # 중복 체크 결과 표시
            duplicates = self._check_category_duplicates(sheet_name, owner, store_key, categories)
            if duplicates:
                duplicate_categories = sorted(set(dup['category'] for dup in duplicates))
                duplicate_stores = sorted(set(dup['store_alias'] for dup in duplicates))
                
                # 카테고리를 대>중 기준으로 그룹화하여 요약
                category_summary = {}
                for dup_cat in duplicate_categories:
                    parts = dup_cat.split('>')
                    if len(parts) >= 2:
                        large_medium = '>'.join(parts[:2])  # 대>중
                        if large_medium not in category_summary:
                            category_summary[large_medium] = []
                        if len(parts) > 2:
                            # 소카테고리 개수 세기
                            category_summary[large_medium].append(dup_cat)
                    else:
                        # 대>중 형식이 아닌 경우 그대로 표시
                        if dup_cat not in category_summary:
                            category_summary[dup_cat] = []
                
                # 요약 메시지 생성
                summary_lines = []
                for large_medium, sub_cats in sorted(category_summary.items()):
                    if sub_cats:
                        # 대>중 외 소 N개 형식
                        summary_lines.append(f"{large_medium} 외 소 {len(sub_cats)}개")
                    else:
                        # 대>중만 있는 경우
                        summary_lines.append(large_medium)
                
                # 가로로 표시 (최대 3줄로 제한)
                category_display = ", ".join(summary_lines[:15])  # 최대 15개만 표시
                if len(summary_lines) > 15:
                    category_display += f" 외 {len(summary_lines) - 15}개"
                
                warning_frame = ttk.LabelFrame(scrollable_frame, text="⚠️ 중복 경고", padding=10)
                warning_frame.pack(fill='x', pady=(0, 10))
                
                # 스토어 정보
                ttk.Label(warning_frame, 
                         text=f"중복 스토어: {', '.join(duplicate_stores)}",
                         font=("맑은 고딕", 9), foreground="#d32f2f", justify='left').pack(anchor='w', pady=(0, 5))
                
                # 카테고리 정보 (가로로 표시)
                category_label = ttk.Label(warning_frame, 
                         text=f"중복 카테고리: {category_display}",
                         font=("맑은 고딕", 9), foreground="#d32f2f", justify='left', wraplength=600)
                category_label.pack(anchor='w')
            
            # 카테고리 그룹별로 표시
            for large_medium in sorted(category_groups.keys()):
                group_frame = ttk.LabelFrame(scrollable_frame, text=f"📂 {large_medium}", padding=10)
                group_frame.pack(fill='x', pady=(0, 5))
                
                for cat in sorted(category_groups[large_medium]):
                    ttk.Label(group_frame, text=f"  • {cat}", font=("맑은 고딕", 8), foreground="#666", justify='left').pack(anchor='w')
        else:
            ttk.Label(scrollable_frame, text="등록된 카테고리 없음", font=("맑은 고딕", 9), foreground="#999").pack(anchor='w', pady=(0, 10))
        
        if memo:
            ttk.Label(scrollable_frame, text="메모:", font=("맑은 고딕", 10, "bold")).pack(anchor='w', pady=(10, 5))
            memo_label = ttk.Label(scrollable_frame, text=memo, font=("맑은 고딕", 9), justify='left', wraplength=550)
            memo_label.pack(anchor='w', fill='x')
        else:
            ttk.Label(scrollable_frame, text="메모 없음", font=("맑은 고딕", 9), foreground="#999").pack(anchor='w', pady=(10, 0))
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        ttk.Button(frame, text="닫기", command=info_window.destroy).pack(pady=(10, 0))
    
    def _get_category_product_count(self, category: str) -> int:
        """카테고리의 상품 수 조회 (완료된 DB 기준)"""
        if not self.db_handler or not self.db_handler.conn:
            return 0
        
        try:
            # 카테고리를 '대>중' 형식으로 변환
            category_parts = [part.strip() for part in category.split('>')]
            if len(category_parts) >= 2:
                large_cat = category_parts[0].strip()
                medium_cat = category_parts[1].strip()
                category_pattern = f"%{large_cat}%>%{medium_cat}%"
            else:
                category_pattern = f"%{category}%"
            
            cursor = self.db_handler.conn.cursor()
            cursor.execute("""
                SELECT COUNT(DISTINCT 상품코드) 
                FROM products 
                WHERE 카테고리명 LIKE ? 
                AND product_status = 'ACTIVE'
                AND product_names_json IS NOT NULL 
                AND product_names_json != '' 
                AND product_names_json != '[]'
            """, (category_pattern,))
            
            row = cursor.fetchone()
            return row[0] if row else 0
        except Exception as e:
            return 0
    
    def _update_store_display(self, store_id: str):
        """스토어 노드 표시 업데이트 (메모 반영)"""
        item_data = self.market_tree_items.get(store_id, {})
        if item_data.get("type") != "store":
            return
        
        store_key = item_data.get("store_key", "")
        store_alias = item_data.get("alias", "")
        is_active = item_data.get("is_active", True)
        sheet_name = item_data.get("sheet_name", "")
        owner = item_data.get("owner", "")
        
        # 메모 정보 로드
        memo_data = self.store_memos.get(store_key, {})
        memo_text = memo_data.get("memo", "")
        memo_categories = memo_data.get("categories", [])
        
        # 체크박스 상태 확인
        checkbox_icon = "☑" if (is_active and self.tree_checkboxes.get(store_id, tk.BooleanVar()).get()) else "☐"
        
        # 중복 체크
        duplicates = self._check_category_duplicates(sheet_name, owner, store_key, memo_categories) if memo_categories else []
        has_duplicate = len(duplicates) > 0
        
        # 기본 표시 텍스트 (첫 번째 컬럼)
        display_text = f"{checkbox_icon} 스토어: {store_alias}"
        
        # 메모/카테고리 정보 (두 번째 컬럼)
        memo_info = ""
        
        if memo_categories:
            # 카테고리를 대카테고리별로 그룹화
            # {대카테고리: {중카테고리: [전체 카테고리 목록]}}
            large_category_groups = {}
            total_product_count = 0
            
            for cat in memo_categories:
                large_medium = self._get_category_large_medium(cat)
                # 대카테고리와 중카테고리 추출
                category_parts = [part.strip() for part in large_medium.split('>')]
                if len(category_parts) >= 2:
                    large_cat = category_parts[0]
                    medium_cat = category_parts[1]
                    
                    if large_cat not in large_category_groups:
                        large_category_groups[large_cat] = {}
                    if medium_cat not in large_category_groups[large_cat]:
                        large_category_groups[large_cat][medium_cat] = []
                    large_category_groups[large_cat][medium_cat].append(large_medium)
                elif len(category_parts) >= 1:
                    # 대카테고리만 있는 경우
                    large_cat = category_parts[0]
                    if large_cat not in large_category_groups:
                        large_category_groups[large_cat] = {}
                    if "" not in large_category_groups[large_cat]:
                        large_category_groups[large_cat][""] = []
                    large_category_groups[large_cat][""].append(large_medium)
                
                # 상품수 조회는 비동기로 처리 (UI 멈춤 방지)
                # product_count = self._get_category_product_count(large_medium)
                # total_product_count += product_count
            
            # 표시할 카테고리 문자열 생성
            # 규칙:
            # 1. 대카테고리별로 중카테고리가 1개만 있으면 "대>중" 형식으로 표시
            # 2. 대카테고리별로 중카테고리가 여러 개 있으면 "대" 형식으로만 표시
            # 3. 최대 5개까지 표시하고 나머지는 "외 N개"로 표시
            display_items = []
            for large_cat in sorted(large_category_groups.keys()):
                medium_cats = large_category_groups[large_cat]
                total_medium_count = sum(len(cats) for cats in medium_cats.values())
                
                if total_medium_count == 1:
                    # 중카테고리가 1개만 있으면 "대>중" 형식으로 표시
                    for medium_cat, cats in medium_cats.items():
                        if medium_cat and cats:
                            display_items.append(f"{large_cat}>{medium_cat}")
                        else:
                            display_items.append(large_cat)
                else:
                    # 중카테고리가 여러 개 있으면 "대" 형식으로만 표시
                    display_items.append(large_cat)
            
            # 최대 5개까지 표시
            if len(display_items) <= 5:
                categories_str = ", ".join(display_items)
            else:
                categories_str = ", ".join(display_items[:5]) + f" 외 {len(display_items)-5}개"
            
            # 중복이 있으면 경고 표시
            if has_duplicate:
                memo_info += f"⚠️ [카테고리: {categories_str}]"
            else:
                memo_info += f"[카테고리: {categories_str}]"
        
        if memo_text:
            if memo_info:
                memo_info += " | "
            memo_preview = memo_text[:50] + "..." if len(memo_text) > 50 else memo_text
            memo_info += f"📝 {memo_preview}"
        
        # 상품수 계산은 비동기로 처리 (UI 멈춤 방지)
        # 먼저 "계산 중..." 표시하고, 백그라운드에서 계산
        product_count_str = "계산 중..."
        
        # 등록된 상품수량 정보 (store_memos에서 가져오기)
        registered_count = memo_data.get("registered_count", None)
        # 표시: 값이 있으면 숫자로, 없으면 "-"로 표시 (사용자 친화적)
        registered_count_str = str(registered_count) if registered_count is not None else "-"
        
        # 트리뷰 아이템 업데이트 (컬럼 지원) - 먼저 UI 업데이트
        self.market_tree.item(store_id, text=display_text, values=(memo_info, product_count_str, registered_count_str))
        
        # 상품수 계산을 백그라운드 스레드로 분리 (UI 멈춤 방지)
        if memo_categories:
            threading.Thread(
                target=self._calculate_store_product_count_background,
                args=(store_id, memo_categories),
                daemon=True
            ).start()
        
        # 중복이 있으면 색상 변경 (옵션)
        if has_duplicate:
            self.market_tree.item(store_id, tags=("duplicate",))
        else:
            self.market_tree.item(store_id, tags=())
    
    def _update_store_display_async(self, store_id: str):
        """스토어 노드 표시 업데이트 (비동기 버전 - UI 즉시 업데이트)"""
        item_data = self.market_tree_items.get(store_id, {})
        if item_data.get("type") != "store":
            return
        
        store_key = item_data.get("store_key", "")
        store_alias = item_data.get("alias", "")
        is_active = item_data.get("is_active", True)
        
        # 메모 정보 로드
        memo_data = self.store_memos.get(store_key, {})
        memo_text = memo_data.get("memo", "")
        memo_categories = memo_data.get("categories", [])
        
        # 체크박스 상태 확인
        checkbox_icon = "☑" if (is_active and self.tree_checkboxes.get(store_id, tk.BooleanVar()).get()) else "☐"
        
        # 기본 표시 텍스트 (첫 번째 컬럼)
        display_text = f"{checkbox_icon} 스토어: {store_alias}"
        
        # 메모/카테고리 정보 (두 번째 컬럼) - 카테고리 표시 형식 적용
        memo_info = ""
        
        if memo_categories:
            # 카테고리를 대카테고리별로 그룹화
            # {대카테고리: {중카테고리: [전체 카테고리 목록]}}
            large_category_groups = {}
            
            for cat in memo_categories:
                large_medium = self._get_category_large_medium(cat)
                # 대카테고리와 중카테고리 추출
                category_parts = [part.strip() for part in large_medium.split('>')]
                if len(category_parts) >= 2:
                    large_cat = category_parts[0]
                    medium_cat = category_parts[1]
                    
                    if large_cat not in large_category_groups:
                        large_category_groups[large_cat] = {}
                    if medium_cat not in large_category_groups[large_cat]:
                        large_category_groups[large_cat][medium_cat] = []
                    large_category_groups[large_cat][medium_cat].append(large_medium)
                elif len(category_parts) >= 1:
                    # 대카테고리만 있는 경우
                    large_cat = category_parts[0]
                    if large_cat not in large_category_groups:
                        large_category_groups[large_cat] = {}
                    if "" not in large_category_groups[large_cat]:
                        large_category_groups[large_cat][""] = []
                    large_category_groups[large_cat][""].append(large_medium)
            
            # 표시할 카테고리 문자열 생성
            # 규칙:
            # 1. 대카테고리별로 중카테고리가 1개만 있으면 "대>중" 형식으로 표시
            # 2. 대카테고리별로 중카테고리가 여러 개 있으면 "대" 형식으로만 표시
            # 3. 최대 5개까지 표시하고 나머지는 "외 N개"로 표시
            display_items = []
            for large_cat in sorted(large_category_groups.keys()):
                medium_cats = large_category_groups[large_cat]
                total_medium_count = sum(len(cats) for cats in medium_cats.values())
                
                if total_medium_count == 1:
                    # 중카테고리가 1개만 있으면 "대>중" 형식으로 표시
                    for medium_cat, cats in medium_cats.items():
                        if medium_cat and cats:
                            display_items.append(f"{large_cat}>{medium_cat}")
                        else:
                            display_items.append(large_cat)
                else:
                    # 중카테고리가 여러 개 있으면 "대" 형식으로만 표시
                    display_items.append(large_cat)
            
            # 최대 5개까지 표시
            if len(display_items) <= 5:
                categories_str = ", ".join(display_items)
            else:
                categories_str = ", ".join(display_items[:5]) + f" 외 {len(display_items)-5}개"
            
            # 중복 체크
            sheet_name = item_data.get("sheet_name", "")
            owner = item_data.get("owner", "")
            duplicates = self._check_category_duplicates(sheet_name, owner, store_key, memo_categories) if memo_categories else []
            has_duplicate = len(duplicates) > 0
            
            # 중복이 있으면 경고 표시
            if has_duplicate:
                memo_info += f"⚠️ [카테고리: {categories_str}]"
            else:
                memo_info += f"[카테고리: {categories_str}]"
        
        if memo_text:
            if memo_info:
                memo_info += " | "
            memo_preview = memo_text[:50] + "..." if len(memo_text) > 50 else memo_text
            memo_info += f"📝 {memo_preview}"
        
        # 상품수는 "계산 중..."으로 표시하고 백그라운드에서 계산
        product_count_str = "계산 중..."
        
        # 등록된 상품수량 정보
        registered_count = memo_data.get("registered_count", None)
        registered_count_str = str(registered_count) if registered_count is not None else "-"
        
        # 트리뷰 아이템 즉시 업데이트 (UI 멈춤 방지)
        self.market_tree.item(store_id, text=display_text, values=(memo_info, product_count_str, registered_count_str))
        
        # 중복이 있으면 색상 변경
        if memo_categories:
            sheet_name = item_data.get("sheet_name", "")
            owner = item_data.get("owner", "")
            duplicates = self._check_category_duplicates(sheet_name, owner, store_key, memo_categories) if memo_categories else []
            has_duplicate = len(duplicates) > 0
            if has_duplicate:
                self.market_tree.item(store_id, tags=("duplicate",))
            else:
                self.market_tree.item(store_id, tags=())
        
        # 상품수 계산을 백그라운드 스레드로 분리
        if memo_categories:
            threading.Thread(
                target=self._calculate_store_product_count_background,
                args=(store_id, memo_categories),
                daemon=True
            ).start()
    
    def _calculate_store_product_count_background(self, store_id: str, memo_categories: List[str]):
        """스토어 상품수 계산 (백그라운드 스레드)"""
        try:
            if not self.db_handler or not self.db_handler.conn:
                self._ui_update_queue.put(('update_store_count', store_id, "N/A"))
                return
            
            total_count = 0
            # 중복 제거된 카테고리 목록 (대>중 형식)
            unique_categories = set()
            for cat in memo_categories:
                large_medium = self._get_category_large_medium(cat)
                unique_categories.add(large_medium)
            
            # 각 카테고리의 상품수 합산 (DB 조회)
            for cat in unique_categories:
                count = self._get_category_product_count(cat)
                total_count += count
            
            # UI 업데이트를 큐에 추가 (메인 스레드에서 처리)
            self._ui_update_queue.put(('update_store_count', store_id, f"{total_count}개"))
        except Exception as e:
            # 오류 발생 시 "오류" 표시
            self._ui_update_queue.put(('update_store_count', store_id, "오류"))
    
    def _update_store_product_count_ui(self, store_id: str, product_count_str: str):
        """스토어 상품수 UI 업데이트 (메인 스레드에서 실행)"""
        try:
            if store_id not in self.market_tree_items:
                return
            
            item_data = self.market_tree_items.get(store_id, {})
            if item_data.get("type") != "store":
                return
            
            # 현재 트리뷰 아이템 정보 가져오기
            current_text = self.market_tree.item(store_id, "text")
            current_values = self.market_tree.item(store_id, "values")
            
            # 상품수만 업데이트 (다른 정보는 유지)
            if len(current_values) >= 3:
                new_values = (current_values[0], product_count_str, current_values[2])
            elif len(current_values) >= 2:
                new_values = (current_values[0], product_count_str, "-")
            else:
                new_values = ("", product_count_str, "-")
            
            self.market_tree.item(store_id, values=new_values)
        except Exception:
            pass
    
    def _process_ui_update_queue(self):
        """UI 업데이트 큐 처리 (메인 스레드에서 주기적으로 실행)"""
        try:
            while True:
                try:
                    # 큐에서 항목 가져오기 (non-blocking)
                    item = self._ui_update_queue.get_nowait()
                    
                    if item[0] == 'update_store_count':
                        _, store_id, product_count_str = item
                        self._update_store_product_count_ui(store_id, product_count_str)
                except queue.Empty:
                    break
        except Exception:
            pass
        
        # 100ms 후에 다시 체크
        self.after(100, self._process_ui_update_queue)
    
    def _update_tree_checkboxes(self):
        """트리뷰 체크박스 상태 업데이트 (시각적 표시)"""
        # 체크박스 아이콘: ☑ (체크됨) / ☐ (체크 해제됨)
        for item_id, var in self.tree_checkboxes.items():
            item_data = self.market_tree_items.get(item_id, {})
            
            # 스토어 노드는 메모 정보 포함하여 표시 (상품수 계산은 비동기로)
            if item_data.get("type") == "store":
                self._update_store_display_async(item_id)
            else:
                # 일반 노드는 기존 방식대로
                current_text = self.market_tree.item(item_id, "text")
                # 기존 체크박스 아이콘 제거
                clean_text = current_text.replace("☑ ", "").replace("☐ ", "").strip()
                
                if var.get():
                    # 체크됨
                    new_text = f"☑ {clean_text}"
                else:
                    # 체크 해제됨
                    new_text = f"☐ {clean_text}"
                
                self.market_tree.item(item_id, text=new_text, values=("", "", ""))  # 컬럼 지원 (메모, 상품수, 등록된 상품수량)
        
        # 버튼 활성화 상태 업데이트
        self._update_export_button_state()
        
        # 마켓 업로드용 모드일 때 상품수 업데이트 (지연 실행으로 렉 방지)
        mode = self.export_mode.get()
        if mode == "upload":
            # 이미 예약된 업데이트가 있으면 취소
            if hasattr(self, '_product_count_update_id'):
                self.after_cancel(self._product_count_update_id)
            # 500ms 후에 업데이트 (사용자가 여러 스토어를 빠르게 선택할 때 렉 방지)
            self._product_count_update_id = self.after(500, self._update_total_product_count)
    
    def _on_tree_click(self, event):
        """트리뷰 클릭 이벤트 (체크박스 토글)"""
        # 클릭한 위치의 아이템 확인
        item = self.market_tree.identify_row(event.y)
        if not item:
            return
        
        # 클릭한 컬럼 확인 (등록된 상품수량 컬럼 클릭 시 편집 모드로 진입하지 않도록)
        column = self.market_tree.identify_column(event.x)
        if column == "#3":  # 등록된 상품수량 컬럼은 더블클릭으로만 편집
            return
        
        # 클릭한 요소 확인
        element = self.market_tree.identify_element(event.x, event.y)
        
        # 화살표(indicator) 클릭 시: 접기/펼치기만 수행 (기본 동작 허용)
        if "indicator" in element:
            return  # 기본 동작 허용 (접기/펼치기)
        
        # 그 외 영역 클릭 시: 체크박스 토글
        item_data = self.market_tree_items.get(item, {})
        
        # 비활성화된 항목은 클릭 불가
        if item_data.get("type") == "store" and not item_data.get("is_active", True):
            return
        
        # 체크박스 토글
        if item in self.tree_checkboxes:
            var = self.tree_checkboxes[item]
            
            # 비활성화된 항목은 토글 불가
            if item_data.get("type") == "store" and not item_data.get("is_active", True):
                return
            
            # 스토어 노드 클릭 시 확인 메시지 표시 (실수 클릭 방지)
            if item_data.get("type") == "store":
                current_state = var.get()
                store_alias = item_data.get("alias", "스토어")
                action_text = "선택" if not current_state else "선택 해제"
                
                # 확인 메시지 표시
                result = messagebox.askyesno(
                    "스토어 선택 확인",
                    f"'{store_alias}' 스토어를 {action_text}하시겠습니까?\n\n"
                    f"※ 조합이 많을 경우 처리 시간이 소요될 수 있습니다.",
                    icon='question'
                )
                
                # 사용자가 "아니오"를 선택하면 취소
                if not result:
                    return
            
            # 체크 상태 토글
            new_state = not var.get()
            var.set(new_state)
            
            # 하위 노드도 함께 토글 (활성화된 항목만)
            self._toggle_children(item, new_state)
            
            # 상위 노드 상태 업데이트
            self._update_parent_state(item)
            
            # 시각적 업데이트
            self._update_tree_checkboxes()
        
        # 기본 선택 동작은 허용하지 않음
        return None
    
    def _on_tree_double_click(self, event):
        """트리뷰 더블클릭 이벤트 (등록된 상품수량 편집)"""
        # 클릭한 위치의 아이템 확인
        item = self.market_tree.identify_row(event.y)
        if not item:
            return
        
        # 클릭한 컬럼 확인
        column = self.market_tree.identify_column(event.x)
        if not column:
            return
        
        # 등록된 상품수량 컬럼인지 확인 (column은 "#3" 형식)
        if column != "#3":  # registered_count 컬럼
            return
        
        # 스토어 노드인지 확인
        item_data = self.market_tree_items.get(item, {})
        if item_data.get("type") != "store":
            return
        
        # 비활성화된 항목은 편집 불가
        if not item_data.get("is_active", True):
            return
        
        # 편집 가능한 Entry 위젯 생성
        self._edit_registered_count(item, event)
    
    def _edit_registered_count(self, item: str, event):
        """등록된 상품수량 편집"""
        # 이미 편집 중인 Entry가 있으면 제거 (중복 방지)
        if hasattr(self, '_editing_entry') and self._editing_entry:
            try:
                self._editing_entry.destroy()
            except:
                pass
        
        # 현재 값 가져오기
        store_key = self.market_tree_items.get(item, {}).get("store_key", "")
        memo_data = self.store_memos.get(store_key, {})
        current_value = memo_data.get("registered_count", None)
        original_value = current_value  # 원래 값 저장 (취소 시 복원용)
        
        # 컬럼 위치 계산
        bbox = self.market_tree.bbox(item, column="#3")
        if not bbox:
            return
        
        x, y, width, height = bbox
        
        # Entry 위젯 생성 (배경색 변경으로 시각적 피드백)
        entry = ttk.Entry(self.market_tree, width=10)
        entry.insert(0, str(current_value) if current_value is not None else "")
        entry.place(x=x, y=y, width=width, height=height)
        entry.configure(style="Edit.TEntry")  # 편집 모드 스타일
        entry.focus()
        entry.select_range(0, tk.END)
        
        # 편집 중인 Entry 추적
        self._editing_entry = entry
        self._editing_item = item
        self._editing_original_value = original_value
        
        def save_value():
            try:
                new_value = entry.get().strip()
                # 숫자만 허용 (빈 문자열도 허용)
                if new_value:
                    # 숫자 검증
                    int_value = int(new_value)
                    if int_value < 0:
                        messagebox.showwarning("입력 오류", 
                            "0 이상의 숫자만 입력 가능합니다.\n\n"
                            "• 0 또는 빈 값: 필터링 없이 전체 출고 (신규 마켓)\n"
                            "• 양수: 해당 수량만큼만 출고")
                        entry.destroy()
                        self._editing_entry = None
                        return
                else:
                    int_value = None
                
                # store_memos에 저장 (None이면 저장하지 않음, 빈 문자열도 저장하지 않음)
                if store_key not in self.store_memos:
                    self.store_memos[store_key] = {}
                if int_value is not None:
                    self.store_memos[store_key]["registered_count"] = int_value
                else:
                    # 빈 값이면 키 자체를 삭제 (저장 공간 절약)
                    if "registered_count" in self.store_memos[store_key]:
                        del self.store_memos[store_key]["registered_count"]
                
                # 메모 저장
                self._save_store_memos()
                
                # 트리뷰 업데이트
                self._update_store_display(item)
                
                entry.destroy()
                self._editing_entry = None
                self._editing_item = None
            except ValueError:
                messagebox.showwarning("입력 오류", 
                    "숫자만 입력 가능합니다.\n\n"
                    "• 빈 값: 필터링 없이 전체 출고 (신규 마켓)\n"
                    "• 0: 필터링 없이 전체 출고 (신규 마켓)\n"
                    "• 양수: 해당 수량만큼만 출고")
                entry.destroy()
                self._editing_entry = None
            except Exception as e:
                messagebox.showerror("오류", f"값 저장 중 오류가 발생했습니다: {e}")
                entry.destroy()
                self._editing_entry = None
        
        def cancel_edit(event=None):
            # 원래 값으로 복원하지 않고 그냥 취소 (현재 값 유지)
            entry.destroy()
            self._editing_entry = None
            self._editing_item = None
        
        # 이벤트 바인딩
        entry.bind("<Return>", lambda e: save_value())
        entry.bind("<FocusOut>", lambda e: save_value())
        entry.bind("<Escape>", cancel_edit)
        entry.bind("<Button-1>", lambda e: "break")  # Entry 내부 클릭 시 이벤트 전파 방지
    
    def _toggle_children(self, parent_id: str, checked: bool):
        """하위 노드 체크 상태 변경 (활성화된 항목만)"""
        children = self.market_tree.get_children(parent_id)
        if not children:
            return  # 하위 노드가 없으면 종료
        
        for child_id in children:
            if child_id not in self.tree_checkboxes:
                continue  # 체크박스가 없는 노드는 건너뛰기
            
            child_data = self.market_tree_items.get(child_id, {})
            child_type = child_data.get("type", "")
            
            # 비활성화된 스토어는 제외
            if child_type == "store" and not child_data.get("is_active", True):
                continue
            
            # 체크 상태 변경
            self.tree_checkboxes[child_id].set(checked)
            
            # 재귀적으로 하위 노드도 변경 (스토어가 아닌 경우에만)
            if child_type != "store":
                self._toggle_children(child_id, checked)
    
    def _update_parent_state(self, item_id: str):
        """상위 노드 체크 상태 업데이트"""
        item_data = self.market_tree_items.get(item_id, {})
        
        if item_data.get("type") == "store":
            # 스토어의 부모: 사업자
            parent_id = self.market_tree.parent(item_id)
            if parent_id and parent_id in self.tree_checkboxes:
                # 해당 사업자의 모든 스토어 확인
                children = self.market_tree.get_children(parent_id)
                all_checked = all(
                    self.tree_checkboxes.get(child, tk.BooleanVar()).get()
                    for child in children
                    if child in self.tree_checkboxes and 
                    self.market_tree_items.get(child, {}).get("is_active", True)
                )
                self.tree_checkboxes[parent_id].set(all_checked)
                self._update_parent_state(parent_id)
        
        elif item_data.get("type") == "biz":
            # 사업자의 부모: 명의자
            parent_id = self.market_tree.parent(item_id)
            if parent_id and parent_id in self.tree_checkboxes:
                children = self.market_tree.get_children(parent_id)
                all_checked = all(
                    self.tree_checkboxes.get(child, tk.BooleanVar()).get()
                    for child in children
                    if child in self.tree_checkboxes
                )
                self.tree_checkboxes[parent_id].set(all_checked)
                self._update_parent_state(parent_id)
        
        elif item_data.get("type") == "owner":
            # 명의자의 부모: 시트
            parent_id = self.market_tree.parent(item_id)
            if parent_id and parent_id in self.tree_checkboxes:
                children = self.market_tree.get_children(parent_id)
                all_checked = all(
                    self.tree_checkboxes.get(child, tk.BooleanVar()).get()
                    for child in children
                    if child in self.tree_checkboxes
                )
                self.tree_checkboxes[parent_id].set(all_checked)
                self._update_parent_state(parent_id)
        
        elif item_data.get("type") == "sheet":
            # 시트의 부모: 그룹 (소매처/도매처)
            parent_id = self.market_tree.parent(item_id)
            if parent_id and parent_id in self.tree_checkboxes:
                children = self.market_tree.get_children(parent_id)
                all_checked = all(
                    self.tree_checkboxes.get(child, tk.BooleanVar()).get()
                    for child in children
                    if child in self.tree_checkboxes
                )
                self.tree_checkboxes[parent_id].set(all_checked)
                self._update_parent_state(parent_id)

        elif item_data.get("type") == "group":
            # 그룹의 부모: 루트
            parent_id = self.market_tree.parent(item_id)
            if parent_id and parent_id in self.tree_checkboxes:
                children = self.market_tree.get_children(parent_id)
                all_checked = all(
                    self.tree_checkboxes.get(child, tk.BooleanVar()).get()
                    for child in children
                    if child in self.tree_checkboxes
                )
                self.tree_checkboxes[parent_id].set(all_checked)
    
    def _select_all_tree(self):
        """전체 스토어 선택 (확인 메시지 포함)"""
        # 활성화된 스토어 개수 확인
        active_store_count = 0
        for item_id, item_data in self.market_tree_items.items():
            if item_data.get("type") == "store" and item_data.get("is_active", True):
                active_store_count += 1
        
        if active_store_count == 0:
            messagebox.showinfo("알림", "선택 가능한 스토어가 없습니다.")
            return
        
        # 확인 메시지 표시
        result = messagebox.askyesno(
            "전체 선택 확인",
            f"모든 활성화된 스토어({active_store_count}개)를 선택하시겠습니까?\n\n"
            f"※ 조합이 많을 경우 처리 시간이 소요될 수 있습니다.",
            icon='question'
        )
        
        if not result:
            return
        
        # 트리 전체 선택 실행
        root_children = self.market_tree.get_children()
        if root_children:
            root_id = root_children[0]  # 전체 선택 노드
            if root_id in self.tree_checkboxes:
                self.tree_checkboxes[root_id].set(True)
                self._toggle_children(root_id, True)
                self._update_tree_checkboxes()
                # 버튼 활성화 상태 업데이트
                self._update_export_button_state()
    
    def _deselect_all_tree(self):
        """전체 스토어 해제 (확인 메시지 포함)"""
        # 선택된 스토어 개수 확인
        selected_store_count = 0
        for item_id, item_data in self.market_tree_items.items():
            if item_data.get("type") == "store" and item_data.get("is_active", True):
                if item_id in self.tree_checkboxes and self.tree_checkboxes[item_id].get():
                    selected_store_count += 1
        
        if selected_store_count == 0:
            messagebox.showinfo("알림", "선택된 스토어가 없습니다.")
            return
        
        # 확인 메시지 표시
        result = messagebox.askyesno(
            "전체 해제 확인",
            f"선택된 모든 스토어({selected_store_count}개)를 해제하시겠습니까?",
            icon='question'
        )
        
        if not result:
            return
        
        # 트리 전체 해제 실행
        root_children = self.market_tree.get_children()
        if root_children:
            root_id = root_children[0]  # 전체 선택 노드
            if root_id in self.tree_checkboxes:
                self.tree_checkboxes[root_id].set(False)
                self._toggle_children(root_id, False)
                self._update_tree_checkboxes()
                # 버튼 활성화 상태 업데이트
                self._update_export_button_state()
    
    def _expand_all_sheets(self):
        """모든 시트 노드 열기 (시트 단위)"""
        root_children = self.market_tree.get_children()
        if root_children:
            root_id = root_children[0]  # 전체 선택 노드
            # 그룹 노드 (소매처/도매처) 열기
            for group_id in self.market_tree.get_children(root_id):
                item_data = self.market_tree_items.get(group_id, {})
                if item_data.get("type") == "group":
                    self.market_tree.item(group_id, open=True)
                    # 그룹 내 시트 노드들 열기
                    for sheet_id in self.market_tree.get_children(group_id):
                        sheet_data = self.market_tree_items.get(sheet_id, {})
                        if sheet_data.get("type") == "sheet":
                            self.market_tree.item(sheet_id, open=True)

    def _collapse_all_sheets(self):
        """모든 시트 노드 닫기 (시트 단위)"""
        root_children = self.market_tree.get_children()
        if root_children:
            root_id = root_children[0]  # 전체 선택 노드
            # 그룹 노드 (소매처/도매처) 닫기
            for group_id in self.market_tree.get_children(root_id):
                item_data = self.market_tree_items.get(group_id, {})
                if item_data.get("type") == "group":
                    # 그룹 내 시트 노드들 닫기
                    for sheet_id in self.market_tree.get_children(group_id):
                        sheet_data = self.market_tree_items.get(sheet_id, {})
                        if sheet_data.get("type") == "sheet":
                            self.market_tree.item(sheet_id, open=False)
                    self.market_tree.item(group_id, open=False)
    
    def _load_categories(self):
        """카테고리 트리 로드"""
        db_path = self.db_path_var.get() if hasattr(self, 'db_path_var') else self.db_path
        
        if not db_path:
            self._log("⚠️ DB 파일을 선택해주세요.")
            return
        
        if not os.path.exists(db_path):
            self._log(f"⚠️ DB 파일이 존재하지 않습니다: {db_path}")
            return
        
        try:
            # DB 연결
            if not self.db_handler:
                self.db_handler = DBHandler(db_path)
                self.db_handler.connect()
            elif self.db_handler.db_path != db_path:
                if self.db_handler.conn:
                    self.db_handler.close()
                self.db_handler = DBHandler(db_path)
                self.db_handler.connect()
            
            # 조합 테이블 초기화 및 동기화 (백그라운드에서 실행)
            def init_combinations():
                try:
                    # 백그라운드 스레드에서는 새로운 DB 연결 생성 (SQLite 스레드 안전성)
                    db_path = self.db_handler.db_path
                    temp_db_handler = DBHandler(db_path)
                    temp_db_handler.connect()
                    
                    try:
                        # 조합이 있는지 확인
                        cursor = temp_db_handler.conn.cursor()
                        cursor.execute("SELECT COUNT(*) FROM product_combinations")
                        combo_count = cursor.fetchone()[0]
                        
                        if combo_count == 0:
                            self._log("📊 상품 조합 생성 중... (최초 1회, 시간이 걸릴 수 있습니다)")
                            total = temp_db_handler.generate_and_save_product_combinations()
                            self._log(f"✅ 상품 조합 생성 완료: {total}개 조합")
                            
                            # 기존 upload_logs에서 할당 정보 마이그레이션
                            self._log("📊 기존 조합 할당 정보 마이그레이션 중...")
                            migrated = temp_db_handler.migrate_existing_assignments()
                            self._log(f"✅ 기존 조합 할당 정보 마이그레이션 완료: {migrated}개")
                        else:
                            self._log(f"✅ 상품 조합 테이블 확인 완료: {combo_count}개 조합 존재")
                            
                            # 새로운 상품이나 업데이트된 상품에 대해 조합 동기화
                            self._log("📊 새로운 상품 조합 동기화 중...")
                            
                            # 진행 상황 콜백 함수
                            def progress_callback(current, total):
                                if current % 50 == 0 or current == total:
                                    self._log(f"  진행 중: {current}/{total}개 상품 처리 중 ({current*100//total if total > 0 else 0}%)")
                            
                            synced = temp_db_handler.sync_combinations_for_new_products(progress_callback=progress_callback)
                            if synced > 0:
                                self._log(f"✅ 새로운 상품 조합 동기화 완료: {synced}개 상품")
                            else:
                                self._log("✅ 동기화할 새로운 상품 없음")
                    finally:
                        # 백그라운드 스레드의 DB 연결 종료
                        temp_db_handler.close()
                except Exception as e:
                    self._log(f"⚠️ 조합 초기화 실패: {e}")
            
            # 백그라운드에서 실행 (UI 블로킹 방지)
            threading.Thread(target=init_combinations, daemon=True).start()
            
            # 카테고리 트리 구조 가져오기
            category_tree = self.db_handler.get_category_tree()
            
            if not category_tree:
                self._log("⚠️ 카테고리 데이터가 없습니다.")
                return
            
            # 카테고리별 상품 수 조회
            category_counts = self.db_handler.get_category_product_counts()
            
            # 전체 상품 수 계산
            total_products = sum(category_counts.values())
            
            # 기존 트리뷰 아이템 삭제
            for item in self.category_tree.get_children():
                self.category_tree.delete(item)
            self.category_tree_items = {}
            self.category_checkboxes = {}
            
            # 전체 상품 수 표시 (최상단)
            total_id = self.category_tree.insert('', 0, text=f"📊 전체 상품: {total_products:,}개", open=False)
            self.category_tree_items[total_id] = {"type": "total"}
            var_total = tk.BooleanVar()
            self.category_checkboxes[total_id] = var_total
            self._update_category_checkbox(total_id)
            
            # 트리뷰에 카테고리 추가 (대>중 형식)
            for large_cat, medium_cats in sorted(category_tree.items()):
                # 대카테고리별 상품 수 계산
                large_cat_count = 0
                for medium_cat, full_categories in medium_cats.items():
                    for full_category in full_categories:
                        large_cat_count += category_counts.get(full_category, 0)
                
                # 대카테고리 추가 (상품 수 포함, 초기에는 닫힘)
                large_text = f"📁 {large_cat} ({large_cat_count:,}개)"
                large_id = self.category_tree.insert('', 'end', text=large_text, open=False)
                self.category_tree_items[large_id] = {"type": "large", "name": large_cat}
                var_large = tk.BooleanVar()
                self.category_checkboxes[large_id] = var_large
                self._update_category_checkbox(large_id)
                
                # 중카테고리 추가
                for medium_cat, full_categories in sorted(medium_cats.items()):
                    # 중카테고리별 상품 수 계산
                    medium_cat_count = sum(category_counts.get(full_cat, 0) for full_cat in full_categories)
                    
                    # 중카테고리 노드 (대>중 형식으로 표시, 상품 수 포함)
                    medium_text = f"{large_cat} > {medium_cat} ({medium_cat_count:,}개)"
                    medium_id = self.category_tree.insert(large_id, 'end', text=medium_text)
                    self.category_tree_items[medium_id] = {
                        "type": "medium",
                        "large": large_cat,
                        "medium": medium_cat,
                        "full_categories": full_categories
                    }
                    var_medium = tk.BooleanVar()
                    self.category_checkboxes[medium_id] = var_medium
                    
                    # 체크박스 아이콘 추가
                    self._update_category_checkbox(medium_id)
            
            self._log(f"✅ 카테고리 트리 로드 완료: {len(category_tree)}개 대카테고리, 총 {total_products:,}개 상품")
            
            # 시즌 필터링 통계 조회 및 표시
            self._show_season_filter_statistics()
            
            # 버튼 활성화 상태 업데이트
            self._update_export_button_state()
            
        except Exception as e:
            self._log(f"❌ 카테고리 로드 오류: {e}")
            import traceback
            self._log(traceback.format_exc())
            # 오류 발생 시 버튼 비활성화
            self.btn_start.config(state='disabled')
    
    def _update_category_checkbox(self, item_id: str):
        """카테고리 체크박스 아이콘 업데이트"""
        if item_id in self.category_checkboxes:
            var = self.category_checkboxes[item_id]
            checked = "☑" if var.get() else "☐"
            current_text = self.category_tree.item(item_id, "text")
            # 기존 체크박스 아이콘 제거 후 새로 추가
            if current_text.startswith("☑") or current_text.startswith("☐"):
                text_without_checkbox = current_text[2:].lstrip()
            else:
                text_without_checkbox = current_text
            self.category_tree.item(item_id, text=f"{checked} {text_without_checkbox}")
    
    def _on_category_tree_click(self, event):
        """카테고리 트리뷰 클릭 이벤트"""
        item = self.category_tree.identify_row(event.y)
        if not item:
            return
        
        element = self.category_tree.identify_element(event.x, event.y)
        
        # 화살표(indicator) 클릭 시: 접기/펼치기만 수행
        if "indicator" in element:
            return
        
        # 체크박스 토글
        if item in self.category_checkboxes:
            var = self.category_checkboxes[item]
            new_state = not var.get()
            var.set(new_state)
            
            # 하위 노드도 함께 토글
            self._toggle_category_children(item, new_state)
            
            # 상위 노드 상태 업데이트
            self._update_category_parent_state(item)
            
            # 시각적 업데이트
            self._update_all_category_checkboxes()
            
            # 카테고리 선택 변경 시 총 상품 수 업데이트
            self._update_total_product_count()
    
    def _toggle_category_children(self, parent_id: str, checked: bool):
        """카테고리 하위 노드 체크 상태 변경"""
        children = self.category_tree.get_children(parent_id)
        for child_id in children:
            if child_id in self.category_checkboxes:
                self.category_checkboxes[child_id].set(checked)
                self._toggle_category_children(child_id, checked)
    
    def _update_category_parent_state(self, item_id: str):
        """카테고리 상위 노드 상태 업데이트"""
        parent_id = self.category_tree.parent(item_id)
        if parent_id and parent_id in self.category_checkboxes:
            children = self.category_tree.get_children(parent_id)
            all_checked = all(
                self.category_checkboxes.get(child, tk.BooleanVar()).get()
                for child in children
                if child in self.category_checkboxes
            )
            self.category_checkboxes[parent_id].set(all_checked)
            self._update_category_parent_state(parent_id)
    
    def _update_all_category_checkboxes(self):
        """모든 카테고리 체크박스 아이콘 업데이트"""
        for item_id in self.category_checkboxes:
            self._update_category_checkbox(item_id)
        # 버튼 활성화 상태 업데이트
        self._update_export_button_state()
    
    def _select_all_category(self):
        """카테고리 전체 선택"""
        for item_id in self.category_checkboxes:
            self.category_checkboxes[item_id].set(True)
        self._update_all_category_checkboxes()
        # 카테고리 선택 변경 시 총 상품 수 업데이트
        self._update_total_product_count()
    
    def _deselect_all_category(self):
        """카테고리 전체 해제"""
        for item_id in self.category_checkboxes:
            self.category_checkboxes[item_id].set(False)
        self._update_all_category_checkboxes()
        # 카테고리 선택 변경 시 총 상품 수 업데이트
        self._update_total_product_count()
    
    def _expand_all_categories(self):
        """모든 카테고리 트리뷰 열기"""
        def expand_item(item_id):
            self.category_tree.item(item_id, open=True)
            for child_id in self.category_tree.get_children(item_id):
                expand_item(child_id)
        
        for item_id in self.category_tree.get_children():
            expand_item(item_id)
    
    def _collapse_all_categories(self):
        """모든 카테고리 트리뷰 닫기"""
        def collapse_item(item_id):
            for child_id in self.category_tree.get_children(item_id):
                collapse_item(child_id)
            self.category_tree.item(item_id, open=False)
        
        for item_id in self.category_tree.get_children():
            collapse_item(item_id)
    
    def _get_selected_categories(self) -> List[str]:
        """선택된 카테고리 목록 가져오기 (대>중 형식만 반환)"""
        selected = []
        for item_id, var in self.category_checkboxes.items():
            if var.get():
                item_data = self.category_tree_items.get(item_id, {})
                item_type = item_data.get("type")
                
                if item_type == "total":
                    # 전체 선택 시 모든 중카테고리 추가 (대>중 형식)
                    for item_id2, item_data2 in self.category_tree_items.items():
                        if item_data2.get("type") == "medium":
                            large_cat = item_data2.get("large", "")
                            medium_cat = item_data2.get("medium", "")
                            if large_cat and medium_cat:
                                large_medium = f"{large_cat} > {medium_cat}"
                                selected.append(large_medium)
                    # 중복 제거
                    selected = list(set(selected))
                elif item_type == "large":
                    # 대카테고리 선택 시 해당 대카테고리의 모든 중카테고리 추가 (대>중 형식)
                    large_name = item_data.get("name")
                    for item_id2, item_data2 in self.category_tree_items.items():
                        if item_data2.get("type") == "medium" and item_data2.get("large") == large_name:
                            large_cat = item_data2.get("large", "")
                            medium_cat = item_data2.get("medium", "")
                            if large_cat and medium_cat:
                                large_medium = f"{large_cat} > {medium_cat}"
                                selected.append(large_medium)
                elif item_type == "medium":
                    # 중카테고리 선택 시 대>중 형식으로 추가
                    large_cat = item_data.get("large", "")
                    medium_cat = item_data.get("medium", "")
                    if large_cat and medium_cat:
                        large_medium = f"{large_cat} > {medium_cat}"
                        selected.append(large_medium)
        
        # 중복 제거
        return list(set(selected))
    
    def _update_total_product_count(self):
        """선택된 모든 카테고리의 총 상품코드 수 업데이트 (최적화 버전 사용)"""
        # 최적화된 버전 사용
        self._update_total_product_count_optimized()
    
    def _update_total_product_count_original(self):
        """선택된 모든 카테고리의 총 상품코드 수 업데이트 (원본 - 백그라운드 스레드에서 실행)"""
        mode = self.export_mode.get()
        
        # 기존에 실행 중인 계산이 있으면 취소
        if hasattr(self, '_product_count_thread') and self._product_count_thread.is_alive():
            # 취소 플래그 설정 (실제로는 스레드를 강제 종료할 수 없으므로 플래그만 설정)
            self._product_count_cancelled = True
        
        # 메인 스레드에서 데이터 준비 (UI 위젯 접근)
        unique_categories = None
        error_msg = None
        
        if mode == "upload":
            # 마켓 업로드용: 선택된 스토어의 카테고리 상품수 계산
            selected_markets = self._get_selected_markets()
            if not selected_markets:
                self.total_product_count_label.config(text="0개")
                return
            
            # 선택된 스토어들의 카테고리 상품수 합산
            category_set = set()
            
            for market_info in selected_markets:
                sheet_name = market_info.get("sheet_name", "")
                owner = market_info.get("owner", "")
                biz_num = market_info.get("biz_num", "")
                alias = market_info.get("alias", "")
                
                store_key = self._get_store_key(sheet_name, owner, biz_num, alias)
                store_memo_data = self.store_memos.get(store_key, {})
                memo_categories = store_memo_data.get("categories", [])
                
                # 중복 제거된 카테고리 목록 (대>중 형식)
                for cat in memo_categories:
                    large_medium = self._get_category_large_medium(cat)
                    category_set.add(large_medium)
            
            # 카테고리 섹션에서 선택한 카테고리도 포함 (보조자료)
            selected_categories = self._get_selected_categories()
            for cat in selected_categories:
                category_set.add(cat)
            
            if not category_set:
                self.total_product_count_label.config(text="0개")
                return
            
            unique_categories = list(category_set)
        else:
            # 미완료 DB: 카테고리 섹션에서 선택한 카테고리 사용
            selected_categories = self._get_selected_categories()
            
            if not selected_categories:
                self.total_product_count_label.config(text="0개")
                return
            
            unique_categories = list(set(selected_categories))
        
        # DB 연결 확인
        if not self.db_handler or not self.db_handler.conn:
            self.total_product_count_label.config(text="DB 미연결")
            return
        
        # 계산 중 표시
        self.total_product_count_label.config(text="계산 중...")
        
        # 백그라운드 스레드에서 계산 실행
        self._product_count_cancelled = False
        db_path = self.db_path_var.get()
        self._product_count_thread = threading.Thread(
            target=self._calculate_product_count_background,
            args=(unique_categories, db_path),
            daemon=True
        )
        self._product_count_thread.start()
    
    def _update_total_product_count_optimized(self):
        """선택된 모든 카테고리의 총 상품코드 수 업데이트 (최적화 버전 - 배치 처리 + 메모리 최적화)"""
        mode = self.export_mode.get()
        
        # 기존에 실행 중인 계산이 있으면 취소
        if hasattr(self, '_product_count_thread_opt') and self._product_count_thread_opt.is_alive():
            self._product_count_cancelled_opt = True
        
        # 메인 스레드에서 데이터 준비 (UI 위젯 접근)
        unique_categories = None
        
        if mode == "upload":
            # 마켓 업로드용: 선택된 스토어의 카테고리 상품수 계산
            selected_markets = self._get_selected_markets()
            if not selected_markets:
                self.total_product_count_label.config(text="0개")
                return
            
            # 선택된 스토어들의 카테고리 상품수 합산
            category_set = set()
            
            for market_info in selected_markets:
                sheet_name = market_info.get("sheet_name", "")
                owner = market_info.get("owner", "")
                biz_num = market_info.get("biz_num", "")
                alias = market_info.get("alias", "")
                
                store_key = self._get_store_key(sheet_name, owner, biz_num, alias)
                store_memo_data = self.store_memos.get(store_key, {})
                memo_categories = store_memo_data.get("categories", [])
                
                # 중복 제거된 카테고리 목록 (대>중 형식)
                for cat in memo_categories:
                    large_medium = self._get_category_large_medium(cat)
                    category_set.add(large_medium)
            
            # 카테고리 섹션에서 선택한 카테고리도 포함 (보조자료)
            selected_categories = self._get_selected_categories()
            for cat in selected_categories:
                category_set.add(cat)
            
            if not category_set:
                self.total_product_count_label.config(text="0개")
                return
            
            unique_categories = list(category_set)
        else:
            # 미완료 DB: 카테고리 섹션에서 선택한 카테고리 사용
            selected_categories = self._get_selected_categories()
            
            if not selected_categories:
                self.total_product_count_label.config(text="0개")
                return
            
            unique_categories = list(set(selected_categories))
        
        # DB 연결 확인
        if not self.db_handler or not self.db_handler.conn:
            self.total_product_count_label.config(text="DB 미연결")
            return
        
        # 계산 중 표시
        self.total_product_count_label.config(text="계산 중...")
        
        # 백그라운드 스레드에서 최적화된 계산 실행
        self._product_count_cancelled_opt = False
        db_path = self.db_path_var.get()
        self._product_count_thread_opt = threading.Thread(
            target=self._calculate_product_count_optimized,
            args=(unique_categories, db_path),
            daemon=True
        )
        self._product_count_thread_opt.start()
    
    def _calculate_product_count_background(self, unique_categories: list, db_path: str):
        """백그라운드에서 상품 수 계산 (원본)"""
        try:
            
            # DB 조회는 별도 연결 사용 (스레드 안전성을 위해)
            import sqlite3
            conn = sqlite3.connect(db_path, check_same_thread=False)
            cursor = conn.cursor()
            
            try:
                all_product_codes = set()
                
                # 카테고리를 배치로 나눠서 처리 (한 번에 100개씩)
                # SQLite의 Expression tree depth 제한(1000)을 고려하여 안전하게 처리
                category_list = list(unique_categories)
                batch_size = 100
                for i in range(0, len(category_list), batch_size):
                    # 취소 체크
                    if getattr(self, '_product_count_cancelled', False):
                        conn.close()
                        return
                    
                    batch_categories = category_list[i:i + batch_size]
                    # 카테고리 필터링 패턴 생성 (대>중 형식 지원)
                    category_patterns = []
                    for cat in batch_categories:
                        # '대>중' 형식에서 '대'와 '중' 추출
                        category_parts = [part.strip() for part in cat.split('>')]
                        if len(category_parts) >= 2:
                            large_cat = category_parts[0].strip()
                            medium_cat = category_parts[1].strip()
                            # '%대%>%중%' 패턴으로 검색
                            category_pattern = f"%{large_cat}%>%{medium_cat}%"
                        else:
                            category_pattern = f"%{cat}%"
                        category_patterns.append(category_pattern)
                    
                    # 배치별로 쿼리 실행
                    # 완료된 DB 기준: product_names_json이 있으면 완료
                    query = f"""
                        SELECT DISTINCT 상품코드 
                        FROM products 
                        WHERE product_status = 'ACTIVE'
                        AND product_names_json IS NOT NULL 
                        AND product_names_json != '' 
                        AND product_names_json != '[]'
                        AND (
                            {' OR '.join(['카테고리명 LIKE ?' for _ in batch_categories])}
                        )
                    """
                    cursor.execute(query, category_patterns)
                    rows = cursor.fetchall()
                    for row in rows:
                        product_code = row[0]
                        if product_code:
                            all_product_codes.add(product_code)
                
                total_count = len(all_product_codes)
                
                # 취소 체크
                if getattr(self, '_product_count_cancelled', False):
                    conn.close()
                    return
                
                # 메인 스레드에서 UI 업데이트
                self.after(0, lambda count=total_count: self.total_product_count_label.config(text=f"{count:,}개"))
            finally:
                conn.close()
        except Exception as e:
            # 메인 스레드에서 UI 업데이트
            self.after(0, lambda: self.total_product_count_label.config(text="조회 실패"))
            self.after(0, lambda: self._log(f"⚠️ 총 상품 수 조회 실패: {e}"))
    
    def _calculate_product_count_optimized(self, unique_categories: list, db_path: str):
        """백그라운드에서 상품 수 계산 (속도 최적화 버전 - 대용량 배치 처리 + 병렬 처리 + DB 최적화)"""
        try:
            import sqlite3
            import time
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            # DB 조회는 별도 연결 사용 (스레드 안전성을 위해)
            conn = sqlite3.connect(db_path, check_same_thread=False)
            # 성능 최적화 설정 (RAM 충분하므로 더 큰 캐시 사용)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")  # 성능 향상
            cursor.execute("PRAGMA cache_size=-500000")  # 500MB 캐시 (RAM 충분하므로 대용량 캐시)
            cursor.execute("PRAGMA temp_store=MEMORY")
            cursor.execute("PRAGMA mmap_size=268435456")  # 256MB 메모리 맵 (대용량 데이터 처리)
            cursor.execute("PRAGMA threads=4")  # 멀티스레드 쿼리 처리
            
            try:
                # 전체 중복 제거를 위한 set (RAM 충분하므로 메모리에 모두 보관)
                all_product_codes = set()
                
                # 카테고리를 대용량 배치로 나눠서 처리 (RAM 충분하므로 배치 크기 증가)
                category_list = list(unique_categories)
                batch_size = 300  # 원본 100에서 300으로 증가 (속도 향상, RAM 충분하므로 가능)
                total_batches = (len(category_list) + batch_size - 1) // batch_size
                
                # 진행 상황 업데이트 주기
                update_interval = 2  # 더 자주 업데이트 (2배치마다)
                last_update_time = time.time()
                
                # 병렬 처리용 함수 (각 배치를 병렬로 처리)
                def process_batch(batch_categories, batch_num):
                    """단일 배치 처리 (병렬 실행용)"""
                    try:
                        # 각 배치마다 별도 DB 연결 생성 (병렬 처리 안전성)
                        batch_conn = sqlite3.connect(db_path, check_same_thread=False)
                        batch_cursor = batch_conn.cursor()
                        # 성능 최적화 설정
                        batch_cursor.execute("PRAGMA journal_mode=WAL")
                        batch_cursor.execute("PRAGMA synchronous=NORMAL")
                        batch_cursor.execute("PRAGMA cache_size=-500000")
                        
                        batch_product_codes = set()
                        
                        # 배치 내 카테고리 패턴 생성
                        category_patterns = []
                        for cat in batch_categories:
                            category_parts = [part.strip() for part in cat.split('>')]
                            if len(category_parts) >= 2:
                                large_cat = category_parts[0].strip()
                                medium_cat = category_parts[1].strip()
                                category_pattern = f"%{large_cat}%>%{medium_cat}%"
                            else:
                                category_pattern = f"%{cat}%"
                            category_patterns.append(category_pattern)
                        
                        # 배치별로 쿼리 실행 (대용량 배치)
                        query = f"""
                            SELECT DISTINCT 상품코드 
                            FROM products 
                            WHERE product_status = 'ACTIVE'
                            AND product_names_json IS NOT NULL 
                            AND product_names_json != '' 
                            AND product_names_json != '[]'
                            AND (
                                {' OR '.join(['카테고리명 LIKE ?' for _ in batch_categories])}
                            )
                        """
                        batch_cursor.execute(query, category_patterns)
                        
                        # 결과를 바로 set에 추가 (메모리 효율적)
                        for row in batch_cursor.fetchall():
                            product_code = row[0]
                            if product_code:
                                batch_product_codes.add(product_code)
                        
                        batch_conn.close()
                        return batch_product_codes, batch_num
                    except Exception as e:
                        batch_conn.close()
                        return set(), batch_num
                
                # 병렬 처리로 여러 배치를 동시에 처리 (속도 향상)
                # 단, DB 연결 수가 너무 많아지지 않도록 최대 4개 스레드로 제한
                max_workers = min(4, total_batches)  # 최대 4개 병렬 처리
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 모든 배치 작업 제출
                    futures = {}
                    for batch_idx in range(0, len(category_list), batch_size):
                        if getattr(self, '_product_count_cancelled_opt', False):
                            break
                        
                        batch_categories = category_list[batch_idx:batch_idx + batch_size]
                        current_batch = (batch_idx // batch_size) + 1
                        future = executor.submit(process_batch, batch_categories, current_batch)
                        futures[future] = current_batch
                    
                    # 완료된 배치 결과 수집
                    completed_batches = 0
                    for future in as_completed(futures):
                        if getattr(self, '_product_count_cancelled_opt', False):
                            break
                        
                        batch_product_codes, batch_num = future.result()
                        all_product_codes.update(batch_product_codes)
                        completed_batches += 1
                        
                        # 진행 상황 업데이트 (더 자주)
                        current_time = time.time()
                        if current_time - last_update_time >= 0.3 or completed_batches % update_interval == 0:
                            progress_pct = int((completed_batches / total_batches) * 100)
                            self.after(0, lambda pct=progress_pct, batch=completed_batches, total=total_batches: 
                                       self.total_product_count_label.config(text=f"계산 중... ({pct}%, {batch}/{total} 배치)"))
                            last_update_time = current_time
                
                # 최종 카운트 계산 (전체 중복 제거 완료)
                if not getattr(self, '_product_count_cancelled_opt', False):
                    final_count = len(all_product_codes)
                    
                    # 취소 체크
                    if getattr(self, '_product_count_cancelled_opt', False):
                        conn.close()
                        return
                    
                    # 메인 스레드에서 UI 업데이트
                    self.after(0, lambda count=final_count: self.total_product_count_label.config(text=f"{count:,}개"))
            finally:
                conn.close()
        except Exception as e:
            # 메인 스레드에서 UI 업데이트
            self.after(0, lambda: self.total_product_count_label.config(text="조회 실패"))
            self.after(0, lambda: self._log(f"⚠️ 총 상품 수 조회 실패: {e}"))
    
    def _on_product_code_filter_mode_change(self, *args):
        """상품코드 필터 모드 변경 시 입력 필드 표시/숨김"""
        if not hasattr(self, 'product_code_filter_mode'):
            return
        
        mode = self.product_code_filter_mode.get()
        # 모드가 변경될 때 현재 export_mode 확인
        if not hasattr(self, 'export_mode'):
            return
            
        current_export_mode = self.export_mode.get()
        
        if current_export_mode != "upload":
            # 업로드 모드가 아니면 필터 프레임 숨김
            if hasattr(self, 'product_code_filter_frame') and self.product_code_filter_frame.winfo_ismapped():
                self.product_code_filter_frame.pack_forget()
            return
        
        if not hasattr(self, 'product_code_filter_frame'):
            return
        
        if mode == "none":
            # 사용 안함: 입력 필드 숨김
            if self.product_code_filter_frame.winfo_ismapped():
                self.product_code_filter_frame.pack_forget()
        else:
            # 제외 또는 포함: 입력 필드 표시
            if not self.product_code_filter_frame.winfo_ismapped():
                # 시즌 필터링 옵션 다음, 데이터 출고 버튼 전에 표시
                # before 옵션으로 정확한 위치에 배치
                try:
                    if hasattr(self, 'btn_start'):
                        self.product_code_filter_frame.pack(fill='x', pady=(5, 0), before=self.btn_start)
                    else:
                        self.product_code_filter_frame.pack(fill='x', pady=(5, 0))
                except:
                    # before가 실패하면 그냥 append
                    self.product_code_filter_frame.pack(fill='x', pady=(5, 0))
    
    def _on_mode_change(self):
        """출력 모드 변경 시 UI 업데이트"""
        mode = self.export_mode.get()
        if mode == "upload":
            # 마켓 업로드용: 마켓 선택 영역 표시 (좌우 배치)
            if not self.frame_markets.winfo_ismapped():
                # selection_frame 내에서 오른쪽에 배치
                self.frame_markets.pack(fill='both', expand=True, side='right', padx=(5, 0))
            
            # 새로운 DB만 출력 옵션 표시
            if not self.exclude_assigned_checkbox.winfo_ismapped():
                self.exclude_assigned_checkbox.pack(fill='x', pady=(5, 0))
            
            # 시즌 필터링 옵션 표시
            if hasattr(self, 'season_filter_checkbox') and not self.season_filter_checkbox.winfo_ismapped():
                self.season_filter_checkbox.pack(fill='x', pady=(5, 0))
            
            # 상품코드 필터링 옵션 (컴팩트 프레임) 표시
            if hasattr(self, 'filter_mode_compact_frame') and not self.filter_mode_compact_frame.winfo_ismapped():
                self.filter_mode_compact_frame.pack(fill='x', pady=(5, 0))
            
            # 상품코드 입력 필드는 모드 변경 시 자동으로 표시/숨김 처리됨
            
            # 출력 상품 수량 제한 섹션 표시
            if not self.quantity_frame.winfo_ismapped():
                self.quantity_frame.pack(fill='x', pady=(8, 0))
            
            # 카테고리 섹션 설명 업데이트
            if hasattr(self, 'category_info_label'):
                self.category_info_label.config(
                    text="※ 마켓 업로드용 모드: 카테고리는 스토어 메모에서 지정합니다.\n※ 이 섹션은 상품수 확인용 보조자료로 사용됩니다.",
                    foreground="#666"
                )
        elif mode == "incomplete":
            # 미완료 DB: 마켓 선택 영역 숨김
            if self.frame_markets.winfo_ismapped():
                self.frame_markets.pack_forget()
                # 카테고리 창이 전체 너비 사용 (좌우 배치이므로 자동으로 확장됨)
            
            # 새로운 DB만 출력 옵션 숨김
            if self.exclude_assigned_checkbox.winfo_ismapped():
                self.exclude_assigned_checkbox.pack_forget()
            
            # 시즌 필터링 옵션 숨김
            if hasattr(self, 'season_filter_checkbox') and self.season_filter_checkbox.winfo_ismapped():
                self.season_filter_checkbox.pack_forget()
            
            # 상품코드 필터링 옵션 (컴팩트 프레임) 숨김
            if hasattr(self, 'filter_mode_compact_frame') and self.filter_mode_compact_frame.winfo_ismapped():
                self.filter_mode_compact_frame.pack_forget()
            
            # 상품코드 필터링 입력 필드 숨김
            if hasattr(self, 'product_code_filter_frame') and self.product_code_filter_frame.winfo_ismapped():
                self.product_code_filter_frame.pack_forget()
            
            # 상품코드 필터 모드도 초기화 (다음 업로드 모드 진입 시 깔끔한 상태로)
            if hasattr(self, 'product_code_filter_mode'):
                self.product_code_filter_mode.set("none")
            
            # 출력 상품 수량 제한 섹션 숨김
            if self.quantity_frame.winfo_ismapped():
                self.quantity_frame.pack_forget()
            
            # 카테고리 섹션 설명 업데이트
            if hasattr(self, 'category_info_label'):
                self.category_info_label.config(
                    text="※ 미완료 DB 모드: 카테고리를 선택하여 데이터를 출고합니다.",
                    foreground="#666"
                )
        
        # 버튼 활성화 상태 업데이트
        self._update_export_button_state()
        
        # 상품수 업데이트
        self._update_total_product_count()
    
    def _show_season_filter_statistics(self):
        """시즌 필터링 통계 조회 및 표시"""
        if not self.db_handler or not self.db_handler.conn:
            return
        
        try:
            # 시즌 필터링 모듈 확인
            try:
                import sys
                import os
                import pandas as pd
                season_filter_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if season_filter_path not in sys.path:
                    sys.path.insert(0, season_filter_path)
                from season_filter_manager_gui import load_season_config, filter_products_by_season
                SEASON_FILTER_AVAILABLE = True
            except ImportError:
                SEASON_FILTER_AVAILABLE = False
            
            if not SEASON_FILTER_AVAILABLE:
                return
            
            # 전체 상품 수 확인
            cursor = self.db_handler.conn.cursor()
            cursor.execute("""
                SELECT COUNT(DISTINCT p.상품코드) as total
                FROM products p
                WHERE p.product_status = 'ACTIVE'
            """)
            total_products_all = cursor.fetchone()[0]
            
            # 출력 가능한 상품만 조회 (상품명만 있어도 가능)
            cursor.execute("""
                SELECT DISTINCT p.상품코드, p.원본상품명, p.ST3_결과상품명, p.ST1_정제상품명
                FROM products p
                WHERE p.product_status = 'ACTIVE'
                AND p.product_names_json IS NOT NULL 
                AND p.product_names_json != '' 
                AND p.product_names_json != '[]'
            """)
            
            products_with_info = []
            for row in cursor.fetchall():
                product_code = row[0]
                if product_code:
                    # 상품명 추출 (시즌 필터링용)
                    원본상품명 = row[1] or ""
                    ST3_결과상품명 = row[2] or ""
                    ST1_정제상품명 = row[3] or ""
                    # 우선순위: 원본상품명 > ST3_결과상품명 > ST1_정제상품명
                    product_name = 원본상품명 or ST3_결과상품명 or ST1_정제상품명
                    
                    # 카테고리명도 조회 (시즌 필터링용)
                    cursor.execute("""
                        SELECT 카테고리명 FROM products WHERE 상품코드 = ? LIMIT 1
                    """, (product_code,))
                    category_row = cursor.fetchone()
                    카테고리명 = category_row[0] if category_row and category_row[0] else ""
                    
                    products_with_info.append({
                        "상품코드": product_code,
                        "상품명": product_name,
                        "product_name": product_name,
                        "카테고리명": 카테고리명,
                    })
            
            if not products_with_info:
                return
            
            # 시즌 설정 로드
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")
            json_path = os.path.join(script_dir, "season_filters.json")
            
            # 디버깅: 파일 존재 확인
            if not os.path.exists(excel_path):
                self._log(f"❌ 시즌 필터링 Excel 파일을 찾을 수 없습니다: {excel_path}")
                return
            if not os.path.exists(json_path):
                self._log(f"⚠️ 시즌 필터링 JSON 파일이 없습니다: {json_path} (자동 생성 시도)")
            
            season_config = load_season_config(excel_path, json_path)
            
            if not season_config:
                self._log("❌ 시즌 설정을 로드할 수 없습니다. Excel 파일과 JSON 파일을 확인하세요.")
                return
            
            # 디버깅: 시즌 설정 구조 확인
            seasons_list = season_config.get("seasons", [])
            if not seasons_list:
                self._log("⚠️ 시즌 설정에 시즌이 없습니다. JSON 캐시를 재생성합니다...")
                # JSON 캐시 삭제하고 재생성 시도
                if os.path.exists(json_path):
                    try:
                        os.remove(json_path)
                        self._log(f"🗑️ 기존 JSON 캐시 삭제: {json_path}")
                    except Exception as e:
                        self._log(f"⚠️ JSON 캐시 삭제 실패: {e}")
                
                # 재생성 시도 (직접 파싱)
                try:
                    from season_filter_manager_gui import _parse_excel_to_config_static
                    import pandas as pd
                    import json as json_module
                    
                    xl = pd.ExcelFile(excel_path, engine='openpyxl')
                    season_config = _parse_excel_to_config_static(xl)
                    
                    # JSON 저장
                    os.makedirs(os.path.dirname(json_path), exist_ok=True)
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json_module.dump(season_config, f, ensure_ascii=False, indent=2)
                    
                    seasons_list = season_config.get("seasons", [])
                    if seasons_list:
                        self._log(f"✅ JSON 캐시 재생성 완료: {len(seasons_list)}개 시즌 로드됨")
                    else:
                        self._log("❌ JSON 캐시 재생성 후에도 시즌이 없습니다. Excel 파일 구조를 확인하세요.")
                        return
                except Exception as e:
                    self._log(f"❌ JSON 캐시 재생성 실패: {e}")
                    import traceback
                    self._log(traceback.format_exc())
                    return
            
            # 시즌 필터링 적용
            from datetime import datetime
            current_date = datetime.now()
            filtered_products, excluded_count, excluded_seasons, included_seasons, season_stats = filter_products_by_season(
                products_with_info, season_config, current_date
            )
            
            # 디버깅: 시즌 설정 확인
            keywords_dict = season_config.get("keywords", {})
            
            # 키워드가 있는 시즌 개수 확인
            seasons_with_keywords = 0
            total_keywords = 0
            for season in seasons_list:
                season_id = season.get("id")
                season_keywords = season.get("keywords", {})
                include_count = len(season_keywords.get("include", []))
                exclude_count = len(season_keywords.get("exclude", []))
                allowed_count = len(season_keywords.get("allowed", []))
                if include_count + exclude_count + allowed_count > 0:
                    seasons_with_keywords += 1
                    total_keywords += include_count + exclude_count + allowed_count
            
            self._log(f"🔍 디버깅: 시즌 설정 로드됨 - {len(seasons_list)}개 시즌")
            self._log(f"🔍 디버깅: 키워드 설정된 시즌 - {seasons_with_keywords}개 (총 {total_keywords}개 키워드)")
            
            # 시즌별 유효성 체크 및 분류
            from season_filter_manager_gui import _check_season_validity, _detect_seasons_from_product
            
            # 샘플 상품명으로 시즌 감지 테스트 (최대 100개)
            test_products = products_with_info[:100]
            detected_season_counts = {}
            detected_examples = {}  # {season_id: [상품명 예시]}
            
            for product in test_products:
                product_name = str(product.get('상품명', product.get('product_name', '')))
                category_name = str(product.get('카테고리명', ''))
                if not product_name:
                    continue
                    
                detected = _detect_seasons_from_product(product_name.lower(), season_config, category_name)
                for season_id, score in detected:
                    if season_id not in detected_season_counts:
                        detected_season_counts[season_id] = 0
                        detected_examples[season_id] = []
                    detected_season_counts[season_id] += 1
                    if len(detected_examples[season_id]) < 3:  # 시즌당 최대 3개 예시
                        detected_examples[season_id].append(product_name[:50])
            
            if detected_season_counts:
                self._log(f"🔍 디버깅: 샘플 상품({len(test_products)}개) 중 시즌 감지 결과:")
                for season_id, count in sorted(detected_season_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                    season = next((s for s in seasons_list if s.get("id") == season_id), None)
                    season_name = season.get("name", season_id) if season else season_id
                    examples = detected_examples.get(season_id, [])
                    self._log(f"  - {season_name}: {count}개 상품 감지됨 (예시: {examples[0] if examples else 'N/A'})")
            else:
                self._log(f"⚠️ 디버깅: 샘플 상품({len(test_products)}개) 중 시즌이 감지되지 않았습니다.")
                # 키워드 예시 출력
                sample_season = seasons_list[0] if seasons_list else None
                if sample_season:
                    sample_season_id = sample_season.get("id")
                    sample_keywords = keywords_dict.get(sample_season_id, {})
                    include_kws = [kw.get("keyword", "") for kw in sample_keywords.get("include", [])[:3]]
                    if include_kws:
                        self._log(f"  예시: '{sample_season.get('name', '')}' 시즌의 키워드 - {', '.join(include_kws)}")
            
            # 현재 날짜 기준으로 각 시즌의 유효성 체크 (3단계)
            season_status = {}  # {season_id: 'SOURCING'|'ACTIVE'|'EXPIRED'}
            season_dates = {}  # {season_id: {'start_date': str, 'end_date': str, 'sourcing_start': str}}
            upcoming_seasons = []  # 1개월 이내 다가오는 시즌
            
            for season in seasons_list:
                season_id = season.get("id")
                validity = _check_season_validity(season, current_date, season_config)
                season_status[season_id] = validity
                
                # 시즌 날짜 정보 저장
                start_date_str = season.get("start_date", "")
                end_date_str = season.get("end_date", "")
                sourcing_start_days = season.get("sourcing_start_days", season.get("prep_days", 30))
                
                # 날짜 파싱하여 실제 날짜 계산
                from season_filter_manager_gui import _parse_date_string
                start_date = _parse_date_string(start_date_str)
                end_date = _parse_date_string(end_date_str)
                
                if start_date and end_date:
                    # 연도 처리 (cross_year 고려)
                    cross_year = season.get("cross_year", False)
                    if cross_year and end_date.month < start_date.month:
                        if current_date.month < start_date.month:
                            start_date = start_date.replace(year=current_date.year - 1)
                            end_date = end_date.replace(year=current_date.year)
                        else:
                            start_date = start_date.replace(year=current_date.year)
                            end_date = end_date.replace(year=current_date.year + 1)
                    else:
                        start_date = start_date.replace(year=current_date.year)
                        end_date = end_date.replace(year=current_date.year)
                    
                    sourcing_start = start_date - pd.Timedelta(days=int(sourcing_start_days))
                    
                    season_dates[season_id] = {
                        'start_date': start_date.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'sourcing_start': sourcing_start.strftime('%Y-%m-%d')
                    }
                    
                    # 다가오는 시즌 확인 (소싱 시작일이 1개월 이내이고 아직 시작 전)
                    days_until_sourcing = (sourcing_start - current_date).days
                    if 0 <= days_until_sourcing <= 30 and validity not in ['SOURCING', 'ACTIVE']:
                        upcoming_seasons.append({
                            'season_id': season_id,
                            'name': season.get("name", season_id),
                            'sourcing_start': sourcing_start.strftime('%Y-%m-%d'),
                            'start_date': start_date.strftime('%Y-%m-%d'),
                            'end_date': end_date.strftime('%Y-%m-%d'),
                            'days_until': days_until_sourcing
                        })
            
            # 통계 요약 출력
            total_count = len(products_with_info)
            valid_count = len(filtered_products)
            invalid_count = excluded_count
            non_season_count = season_stats.get('non_season', 0)
            season_valid_count = season_stats.get('season_valid', 0)
            season_invalid_count = season_stats.get('season_invalid', 0)
            missing_url_or_name = total_products_all - total_count
            
            current_date_str = current_date.strftime('%Y-%m-%d')
            
            self._log("=" * 60)
            self._log(f"🗓️ 시즌 필터링 통계 (기준일: {current_date_str})")
            self._log(f"  📦 전체 ACTIVE 상품: {total_products_all:,}개")
            self._log(f"  📋 출력 가능 조건 충족 상품: {total_count:,}개")
            self._log(f"    (상품명이 있는 상품 - URL은 조합 생성 시에만 사용)")
            self._log(f"  ⚠️ 출력 불가 상품 (상품명 없음): {missing_url_or_name:,}개")
            self._log("")
            self._log(f"  📊 시즌 필터링 후 분류:")
            self._log(f"    ✅ 출력 가능 상품: {valid_count:,}개")
            self._log(f"    ❌ 출력 제외 상품: {invalid_count:,}개")
            self._log("")
            self._log(f"  📊 상품 분류 상세:")
            self._log(f"    - 일반 상품 (시즌 없음): {non_season_count:,}개")
            self._log(f"    - 시즌 상품 (포함): {season_valid_count:,}개")
            self._log(f"    - 시즌 지난 상품 (제외): {season_invalid_count:,}개")
            
            # 포함된 시즌 종류별 통계 (ACTIVE만 표시, SOURCING은 제외됨)
            active_seasons = []
            if included_seasons:
                self._log("")
                sorted_included = sorted(included_seasons.items(), key=lambda x: x[1].get('count', 0), reverse=True)
                
                for season_id, info in sorted_included:
                    name = info.get('name', season_id)
                    count = info.get('count', 0)
                    status = season_status.get(season_id, 'EXPIRED')
                    
                    if status == 'ACTIVE':
                        active_seasons.append((name, count))
                
                if active_seasons:
                    self._log(f"  ✅ 출력 가능 시즌 (지금 출력할 수 있는, 종류별 상품수):")
                    for season_id, info in sorted_included:
                        name = info.get('name', season_id)
                        count = info.get('count', 0)
                        status = season_status.get(season_id, 'EXPIRED')
                        
                        if status == 'ACTIVE':
                            # 날짜 정보 추가
                            dates = season_dates.get(season_id, {})
                            start_date_display = dates.get('start_date', '')
                            end_date_display = dates.get('end_date', '')
                            if start_date_display and end_date_display:
                                self._log(f"    - {name}: {count:,}개 (기간: {start_date_display} ~ {end_date_display})")
                            else:
                                self._log(f"    - {name}: {count:,}개")
            
            # 제외된 시즌 종류별 통계 (SOURCING + EXPIRED)
            sourcing_excluded_seasons = []
            expired_seasons = []
            if excluded_seasons:
                self._log("")
                sorted_excluded = sorted(excluded_seasons.items(), key=lambda x: x[1].get('count', 0), reverse=True)
                
                for season_id, info in sorted_excluded:
                    name = info.get('name', season_id)
                    count = info.get('count', 0)
                    reason = info.get('reason', '시즌 기간 외')
                    status = season_status.get(season_id, 'EXPIRED')
                    
                    # Event 타입만 제외 표시 (정책: Event 타입만 출력 제한)
                    season = next((s for s in seasons_list if s.get("id") == season_id), None)
                    season_type = season.get("type", "").strip().upper() if season else ""
                    
                    if status == 'SOURCING' and season_type == "EVENT":
                        # 소싱 기간: Event 타입만 제외 표시
                        sourcing_excluded_seasons.append((name, count, reason))
                    elif status == 'EXPIRED' and season_type == "EVENT":
                        # 시즌 종료: Event 타입만 제외 표시 (삭제해야 하는 상품)
                        expired_seasons.append((name, count, reason))
                
                if sourcing_excluded_seasons:
                    self._log(f"  🔵 소싱 기간 시즌 (Event 타입만 - 이미 가공 완료 - 제외, 종류별 상품수):")
                    for season_id, info in sorted_excluded:
                        name = info.get('name', season_id)
                        count = info.get('count', 0)
                        reason = info.get('reason', '시즌 기간 외')
                        status = season_status.get(season_id, 'EXPIRED')
                        
                        # Event 타입만 표시
                        season = next((s for s in seasons_list if s.get("id") == season_id), None)
                        season_type = season.get("type", "").strip().upper() if season else ""
                        
                        if status == 'SOURCING' and season_type == "EVENT":
                            # 날짜 정보 추가
                            dates = season_dates.get(season_id, {})
                            start_date_display = dates.get('start_date', '')
                            end_date_display = dates.get('end_date', '')
                            reason_clean = reason.replace(f"{name}(", "").replace(")", "").strip()
                            if start_date_display and end_date_display:
                                self._log(f"    - {name} - {reason_clean} - {count:,}개 (기간: {start_date_display} ~ {end_date_display})")
                            else:
                                self._log(f"    - {name} - {reason_clean} - {count:,}개")
                
                if expired_seasons:
                    self._log(f"  ❌ 제외 시즌 (Event 타입만 - 시즌 종료 - 삭제해야 하는, 종류별 상품수):")
                    for season_id, info in sorted_excluded:
                        name = info.get('name', season_id)
                        count = info.get('count', 0)
                        reason = info.get('reason', '시즌 기간 외')
                        status = season_status.get(season_id, 'EXPIRED')
                        
                        # Event 타입만 표시
                        season = next((s for s in seasons_list if s.get("id") == season_id), None)
                        season_type = season.get("type", "").strip().upper() if season else ""
                        
                        if status == 'EXPIRED' and season_type == "EVENT":
                            # 날짜 정보 추가
                            dates = season_dates.get(season_id, {})
                            start_date_display = dates.get('start_date', '')
                            end_date_display = dates.get('end_date', '')
                            reason_clean = reason.replace(f"{name}(", "").replace(")", "").strip()
                            if start_date_display and end_date_display:
                                self._log(f"    - {name} - {reason_clean} - {count:,}개 (기간: {start_date_display} ~ {end_date_display})")
                            else:
                                self._log(f"    - {name} - {reason_clean} - {count:,}개")
            
            # 현재 시점에서 각 상태의 시즌 목록
            sourcing_season_ids = [s for s, status in season_status.items() if status == 'SOURCING']
            active_season_ids = [s for s, status in season_status.items() if status == 'ACTIVE']
            expired_season_ids = [s for s, status in season_status.items() if status == 'EXPIRED']
            
            # 소싱 기간 시즌 목록 (Event 타입만 표시)
            sourcing_event_season_ids = []
            for season_id in sourcing_season_ids:
                season = next((s for s in seasons_list if s.get("id") == season_id), None)
                if season:
                    season_type = season.get("type", "").strip().upper()
                    if season_type == "EVENT":
                        sourcing_event_season_ids.append(season_id)
            
            if sourcing_event_season_ids:
                self._log("")
                self._log(f"  🔵 소싱 기간 시즌 목록 (Event 타입만 - 이미 가공 완료 - 제외):")
                for season_id in sourcing_event_season_ids[:10]:  # 최대 10개만
                    season = next((s for s in seasons_list if s.get("id") == season_id), None)
                    if season:
                        name = season.get("name", season_id)
                        # 제외된 상품 수 확인
                        excluded_count_for_season = excluded_seasons.get(season_id, {}).get('count', 0)
                        self._log(f"    - {name} ({excluded_count_for_season:,}개 제외)")
            
            if active_season_ids:
                self._log("")
                self._log(f"  ✅ 출력 가능 시즌 목록 (지금 출력할 수 있는):")
                for season_id in active_season_ids[:10]:  # 최대 10개만
                    season = next((s for s in seasons_list if s.get("id") == season_id), None)
                    if season:
                        name = season.get("name", season_id)
                        dates = season_dates.get(season_id, {})
                        start_date_display = dates.get('start_date', '')
                        end_date_display = dates.get('end_date', '')
                        if start_date_display and end_date_display:
                            self._log(f"    - {name} (기간: {start_date_display} ~ {end_date_display})")
                        else:
                            self._log(f"    - {name}")
            
            # 다가오는 시즌 표시 (1개월 이내)
            if upcoming_seasons:
                self._log("")
                self._log(f"  🔮 다가오는 시즌 (1개월 이내, 소싱 준비 필요):")
                # 날짜순으로 정렬
                upcoming_seasons_sorted = sorted(upcoming_seasons, key=lambda x: x['days_until'])
                for upcoming in upcoming_seasons_sorted[:10]:  # 최대 10개만
                    name = upcoming['name']
                    days = upcoming['days_until']
                    sourcing_start = upcoming['sourcing_start']
                    start_date = upcoming['start_date']
                    end_date = upcoming['end_date']
                    self._log(f"    - {name}: {days}일 후 소싱 시작 ({sourcing_start}), 기간: {start_date} ~ {end_date}")
            
            self._log("=" * 60)
            
        except Exception as e:
            # 오류 발생 시 무시 (선택적 기능이므로)
            import traceback
            self._log(f"⚠️ 시즌 필터링 통계 조회 실패: {e}")
    
    def _update_export_button_state(self):
        """데이터 출고 버튼 활성화 상태 업데이트"""
        # 버튼이 아직 생성되지 않았으면 무시
        if not hasattr(self, 'btn_start'):
            return
        
        mode = self.export_mode.get()
        
        if mode == "upload":
            # 마켓 업로드용: 마켓/스토어 선택 확인 (카테고리는 스토어 메모에서 가져옴)
            selected_markets = self._get_selected_markets()
            has_market = len(selected_markets) > 0
            
            # 선택된 스토어 중 카테고리가 지정된 스토어가 있는지 확인
            has_store_with_categories = False
            if has_market:
                for market_info in selected_markets:
                    sheet_name = market_info.get("sheet_name", "")
                    owner = market_info.get("owner", "")
                    biz_num = market_info.get("biz_num", "")
                    alias = market_info.get("alias", "")
                    store_key = self._get_store_key(sheet_name, owner, biz_num, alias)
                    store_memo_data = self.store_memos.get(store_key, {})
                    memo_categories = store_memo_data.get("categories", [])
                    if memo_categories:
                        has_store_with_categories = True
                        break
            
            if not has_market:
                self.btn_start.config(state='disabled')
                return
            elif not has_store_with_categories:
                # 스토어는 선택했지만 카테고리가 없는 경우
                self.btn_start.config(state='disabled')
                return
            
            # 디버깅: 선택 상태 로그
            if not has_market:
                # 선택된 마켓이 없을 때 왜 없는지 확인
                checked_stores = []
                checked_non_stores = []
                for item_id, var in self.tree_checkboxes.items():
                    if var.get():
                        item_data = self.market_tree_items.get(item_id, {})
                        item_type = item_data.get("type", "unknown")
                        if item_type == "store":
                            is_active = item_data.get("is_active", True)
                            market_name = item_data.get("market_name", "")
                            business_number = item_data.get("business_number", "")
                            alias = item_data.get("alias", "")
                            checked_stores.append({
                                "alias": alias,
                                "is_active": is_active,
                                "has_market_name": bool(market_name),
                                "has_business_number": bool(business_number)
                            })
                        else:
                            checked_non_stores.append(f"{item_type}: {item_data.get('alias', item_data.get('market_name', item_id))}")
                
                # 디버깅 로그
                if checked_stores:
                    self._log(f"[DEBUG] 체크된 스토어 {len(checked_stores)}개 중 선택 가능한 스토어 없음")
                    for store in checked_stores:
                        reason = []
                        if not store["is_active"]:
                            reason.append("비활성화됨")
                        if not store["has_market_name"]:
                            reason.append("마켓명 없음")
                        if not store["has_business_number"]:
                            reason.append("사업자번호 없음")
                        self._log(f"  - 별칭: {store['alias']}, 이유: {', '.join(reason) if reason else '알 수 없음'}")
                elif checked_non_stores:
                    self._log(f"[DEBUG] 체크된 항목: {checked_non_stores} (스토어가 아닌 상위 노드만 체크됨)")
            
            # 모든 조건 충족 시 활성화
            self.btn_start.config(state='normal')
        elif mode == "incomplete":
            # 미완료 DB: 카테고리 섹션에서 선택한 카테고리 확인
            selected_categories = self._get_selected_categories()
            has_category = len(selected_categories) > 0
            
            if has_category:
                self.btn_start.config(state='normal')
            else:
                self.btn_start.config(state='disabled')
    
    def _start_export(self):
        """데이터 출고 시작"""
        mode = self.export_mode.get()
        
        if mode == "upload":
            # 마켓 업로드용: 마켓 선택 확인
            selected_markets = self._get_selected_markets()
            if not selected_markets:
                messagebox.showwarning("오류", "마켓/스토어를 선택해주세요.")
                return
            
            # 스토어별 카테고리 확인 (스토어 메모에 카테고리가 있는지)
            stores_with_categories = []
            stores_without_categories = []
            
            for market_info in selected_markets:
                sheet_name = market_info.get("sheet_name", "")
                owner = market_info.get("owner", "")
                biz_num = market_info.get("biz_num", "")
                alias = market_info.get("alias", "")
                
                store_key = self._get_store_key(sheet_name, owner, biz_num, alias)
                store_memo_data = self.store_memos.get(store_key, {})
                memo_categories = store_memo_data.get("categories", [])
                
                if memo_categories:
                    stores_with_categories.append(market_info)
                else:
                    stores_without_categories.append(market_info)
            
            # 카테고리 섹션에서 선택한 카테고리 (보조자료, 마켓 업로드용에서는 사용 안 함)
            selected_categories = self._get_selected_categories()
            
            # 마켓 업로드용: 스토어 메모의 카테고리를 우선 사용
            if stores_without_categories:
                # 카테고리가 지정되지 않은 스토어가 있는 경우
                store_names = [m.get("alias", "") for m in stores_without_categories]
                messagebox.showwarning("오류", 
                    f"다음 스토어에 업로드 예정 카테고리가 지정되지 않았습니다:\n" + 
                    "\n".join(store_names[:5]) + 
                    (f"\n외 {len(store_names)-5}개" if len(store_names) > 5 else "") +
                    "\n\n스토어 메모 편집에서 카테고리를 지정해주세요.")
                return
            
            # 모든 스토어에 카테고리가 지정되어 있으면 데이터 출고 진행
            # 카테고리 섹션에서 선택한 카테고리는 무시 (스토어 메모의 카테고리만 사용)
            self._export_for_upload([], selected_markets)  # 빈 리스트 전달, 스토어별로 카테고리 처리
        elif mode == "incomplete":
            # 미완료 DB: 카테고리 섹션에서 선택한 카테고리 사용
            selected_categories = self._get_selected_categories()
            if not selected_categories:
                messagebox.showwarning("오류", "카테고리를 선택해주세요.")
                return
            self._export_incomplete(selected_categories)
    
    def _get_selected_markets(self) -> List[Dict[str, str]]:
        """선택된 마켓 정보 가져오기"""
        selected_markets = []
        checked_stores_without_info = []  # 정보가 없는 체크된 스토어
        
        # 체크된 상위 노드에서 하위 스토어를 재귀적으로 찾는 함수
        def get_stores_from_node(node_id: str) -> List[str]:
            """노드에서 하위 스토어 ID 목록 가져오기"""
            store_ids = []
            children = self.market_tree.get_children(node_id)
            for child_id in children:
                child_data = self.market_tree_items.get(child_id, {})
                child_type = child_data.get("type", "")
                
                if child_type == "store":
                    # 스토어 노드인 경우
                    if child_data.get("is_active", True):
                        store_ids.append(child_id)
                else:
                    # 상위 노드인 경우 재귀적으로 하위 스토어 찾기
                    store_ids.extend(get_stores_from_node(child_id))
            
            return store_ids
        
        # 직접 체크된 스토어와 상위 노드가 체크된 경우 하위 스토어 찾기
        checked_node_ids = []
        for item_id, var in self.tree_checkboxes.items():
            if var.get():
                item_data = self.market_tree_items.get(item_id, {})
                item_type = item_data.get("type", "")
                
                if item_type == "store":
                    # 스토어가 직접 체크된 경우
                    checked_node_ids.append(item_id)
                elif item_type in ["sheet", "owner", "biz", "root"]:
                    # 상위 노드가 체크된 경우 하위 스토어 찾기
                    checked_node_ids.extend(get_stores_from_node(item_id))
        
        # 중복 제거
        checked_node_ids = list(set(checked_node_ids))
        
        # 체크된 스토어에서 정보 추출
        for store_id in checked_node_ids:
            item_data = self.market_tree_items.get(store_id, {})
            
            # 스토어 타입이고 활성화된 경우만 선택
            if item_data.get("type") == "store" and item_data.get("is_active", True):
                market_name = item_data.get("market_name", "")
                business_number = item_data.get("business_number", "")
                alias = item_data.get("alias", "")
                sheet_name = item_data.get("sheet_name", "")
                owner = item_data.get("owner", "")
                biz_num = item_data.get("biz_num", "")
                
                # market_name과 business_number가 모두 있어야 함
                if market_name and business_number:
                    selected_markets.append({
                        "market_name": market_name,
                        "business_number": business_number,
                        "alias": alias,
                        "sheet_name": sheet_name,
                        "owner": owner,
                        "biz_num": biz_num
                    })
                else:
                    # 정보가 없는 경우 기록 (디버깅용)
                    checked_stores_without_info.append({
                        "alias": alias,
                        "market_name": market_name,
                        "business_number": business_number,
                        "sheet_name": sheet_name
                    })
        
        # 디버깅: 정보가 없는 체크된 스토어가 있으면 로그 출력
        if checked_stores_without_info:
            self._log(f"[DEBUG] 체크되었지만 정보가 부족한 스토어: {len(checked_stores_without_info)}개")
            for store_info in checked_stores_without_info:
                self._log(f"  - 별칭: {store_info.get('alias')}, 마켓명: {store_info.get('market_name')}, 사업자번호: {store_info.get('business_number')}")
        
        return selected_markets
    
    def _parse_product_code_filter(self) -> tuple:
        """상품코드 필터 입력을 파싱하여 (mode, codes_set) 반환
        
        Returns:
            (mode, codes_set): 
                - mode: "none", "exclude", "include"
                - codes_set: 상품코드 set (빈 set일 수 있음)
        """
        mode = self.product_code_filter_mode.get()
        if mode == "none":
            return ("none", set())
        
        # 텍스트 영역에서 입력 읽기
        text_content = self.product_code_filter_text.get("1.0", tk.END).strip()
        if not text_content:
            return ("none", set())
        
        # 쉼표와 줄바꿈으로 분리
        codes = []
        for line in text_content.split('\n'):
            for part in line.split(','):
                code = part.strip()
                if code:  # 빈 문자열 제외
                    codes.append(code)
        
        return (mode, set(codes))
    
    def _export_for_upload(self, selected_categories: List[str], selected_markets: List[Dict[str, str]]):
        """마켓 업로드용 데이터 출고"""
        # 새로운 DB만 출력 옵션 확인
        exclude_assigned = self.exclude_assigned_var.get()
        
        # 상품코드 필터 확인
        filter_mode, filter_codes = self._parse_product_code_filter()
        if filter_mode != "none" and not filter_codes:
            messagebox.showwarning("경고", "상품코드 필터를 사용하려면 상품코드를 입력해주세요.")
            return
        
        # 체크박스가 해제되어 있으면 경고 메시지 표시
        if not exclude_assigned:
            response = messagebox.askyesno(
                "⚠️ 경고",
                "이미 배정받은 상품코드 제외 옵션이 해제되어 있습니다.\n\n"
                "이 경우 이미 해당 스토어에서 배정받은 상품코드도 포함하여 재출력될 수 있습니다.\n"
                "중복 업로드 가능성이 있으니 주의하세요.\n\n"
                "계속하시겠습니까?",
                icon='warning'
            )
            if not response:
                return  # 사용자가 취소한 경우
        
        # 버튼 비활성화
        self.btn_start.config(state='disabled')
        
        # 저장 디렉토리 선택 (메인 스레드에서 호출)
        save_dir = filedialog.askdirectory(title="데이터 출고 파일 저장 폴더 선택")
        if not save_dir:
            self._log("⚠️ 저장 폴더를 선택하지 않아 취소되었습니다.")
            self.btn_start.config(state='normal')
            return
        
        # 백그라운드 스레드에서 실행 (옵션 전달)
        # export_mode를 파라미터로 전달
        export_mode = self.export_mode.get()
        threading.Thread(target=self._run_export_for_upload, args=(selected_categories, selected_markets, save_dir, exclude_assigned, export_mode, filter_mode, filter_codes), daemon=True).start()
    
    def _export_incomplete(self, selected_categories: List[str]):
        """미완료 DB 출고"""
        # 버튼 비활성화
        self.btn_start.config(state='disabled')
        
        # 백그라운드 스레드에서 실행
        threading.Thread(target=self._run_export_incomplete, args=(selected_categories,), daemon=True).start()
    
    def _re_export_from_history(self, selected_categories: List[str], selected_markets: List[Dict[str, str]], save_dir: str, exclude_assigned: bool = True, export_mode: str = "upload", custom_filename: str = None, skip_logging: bool = False):
        """히스토리에서 재다운로드 (DB에서 데이터를 다시 조회하여 파일 재생성)
        
        Args:
            skip_logging: True이면 upload_logs에 기록하지 않음 (재다운로드이므로)
        """
        # _run_export_for_upload를 호출하되, 파일명만 커스터마이징하고 upload_logs 기록은 건너뜀
        # 히스토리 재다운로드는 상품코드 필터링 사용 안함 (원본 그대로 재생성)
        self._history_custom_filename = custom_filename
        self._history_skip_logging = skip_logging
        try:
            self._run_export_for_upload(selected_categories, selected_markets, save_dir, exclude_assigned, export_mode, "none", set(), custom_filename, skip_logging)
        finally:
            # 정리
            if hasattr(self, '_history_custom_filename'):
                delattr(self, '_history_custom_filename')
            if hasattr(self, '_history_skip_logging'):
                delattr(self, '_history_skip_logging')
    
    def _merge_history_items(self, selected_items: List[tuple], save_path: str):
        """선택된 여러 히스토리 항목을 하나의 Excel 파일로 병합하여 재생성
        
        Args:
            selected_items: [(item_id, history_data), ...] 리스트
            save_path: 병합된 파일 저장 경로
        """
        try:
            import pandas as pd
            import json
            from datetime import datetime
            
            self._log(f"=== 히스토리 병합 다운로드 시작 ({len(selected_items)}개 항목) ===")
            
            # DB 연결 확인
            if not self.db_handler or not self.db_handler.conn:
                messagebox.showerror("오류", "DB가 연결되지 않았습니다.")
                return
            
            # 백그라운드 스레드에서는 새로운 DB 연결 생성
            db_path = self.db_path_var.get()
            db_handler = DBHandler(db_path)
            db_handler.connect()
            
            try:
                all_data = []  # 모든 스토어의 데이터를 저장할 리스트
                
                # 전체 카테고리 수 계산 (진행률 표시용)
                total_categories = 0
                for item_id, hist in selected_items:
                    categories_str = hist.get("categories", "")
                    if categories_str:
                        try:
                            cat_list = json.loads(categories_str) if isinstance(categories_str, str) else categories_str
                            if isinstance(cat_list, list):
                                total_categories += len(cat_list)
                        except:
                            pass
                
                processed_categories = 0
                
                # 각 히스토리 항목별로 데이터 재생성
                for idx, (item_id, hist) in enumerate(selected_items, 1):
                    sheet_name = hist.get("sheet_name", "")
                    store_name = hist.get("store_name", "")
                    store_alias = hist.get("store_alias", "")
                    business_number = hist.get("business_number", "")
                    
                    self._log(f"[{idx}/{len(selected_items)}] {sheet_name} - {store_alias} 처리 중...")
                    
                    # 카테고리 파싱
                    categories_str = hist.get("categories", "")
                    categories_list = []
                    if categories_str:
                        try:
                            categories_list = json.loads(categories_str) if isinstance(categories_str, str) else categories_str
                            if not isinstance(categories_list, list):
                                categories_list = []
                        except:
                            categories_list = []
                    
                    if not categories_list:
                        self._log(f"  ⚠️ 카테고리 정보가 없어 건너뜀")
                        continue
                    
                    exclude_assigned = bool(hist.get("exclude_assigned", 1))
                    
                    # 시즌 필터링 활성화 여부 가져오기
                    season_filter_enabled = getattr(self, 'season_filter_var', tk.BooleanVar(value=True)).get()
                    
                    # combination_assignments 조회 캐싱 (성능 최적화) - 시트별, 스토어별로 한 번만 조회
                    cursor = db_handler.conn.cursor()
                    
                    # 시트별 사용된 조합 조회 (한 번만)
                    cursor.execute("""
                        SELECT DISTINCT combination_index, product_code
                        FROM combination_assignments 
                        WHERE sheet_name = ?
                    """, (sheet_name,))
                    
                    sheet_used_combinations_cache = {}  # {product_code: set(combination_indices)}
                    for row in cursor.fetchall():
                        combo_idx, pc = row
                        if pc and combo_idx is not None:
                            if pc not in sheet_used_combinations_cache:
                                sheet_used_combinations_cache[pc] = set()
                            sheet_used_combinations_cache[pc].add(combo_idx)
                    
                    # 스토어별 사용된 상품코드 조회 (한 번만)
                    store_used_product_codes_cache = set()
                    if exclude_assigned and business_number:
                        cursor.execute("""
                            SELECT DISTINCT product_code
                            FROM combination_assignments 
                            WHERE sheet_name = ? AND business_number = ?
                        """, (sheet_name, business_number))
                        for row in cursor.fetchall():
                            if row[0]:
                                store_used_product_codes_cache.add(row[0])
                    
                    # 각 카테고리별로 상품 조회 및 export_row 형식으로 변환
                    for cat_idx, category in enumerate(categories_list, 1):
                        processed_categories += 1
                        
                        # 진행률 표시 (10개마다 또는 마지막 카테고리)
                        if processed_categories % 10 == 0 or cat_idx == len(categories_list):
                            progress_pct = int((processed_categories / total_categories) * 100) if total_categories > 0 else 0
                            self._log(f"  📊 진행률: {processed_categories}/{total_categories} 카테고리 ({progress_pct}%) - {category} 처리 중...")
                        
                        # 캐시된 데이터를 전달하여 중복 조회 방지 (성능 최적화)
                        # 히스토리 병합은 원본 그대로 재생성하므로 상품코드 필터링 사용 안함
                        products = db_handler.get_products_for_upload(
                            category, sheet_name, business_number,
                            exclude_assigned=exclude_assigned,
                            season_filter_enabled=season_filter_enabled,
                            sheet_used_combinations=sheet_used_combinations_cache,
                            store_used_product_codes=store_used_product_codes_cache if exclude_assigned else None,
                            product_code_filter_mode="none",
                            product_code_filter_codes=None
                        )
                        
                        # export_row 형식으로 변환 (기존 상품조합 형태 유지)
                        for product in products:
                            mix_url = product.get("믹스url", "") or ""
                            nukki_url = product.get("누끼url", "") or ""
                            final_name = product.get("ST4_최종결과", "") or ""
                            product_code = product.get("상품코드", "")
                            url_type = product.get("url_type", "mix")
                            line_index = product.get("line_index", 0)
                            st2_json = product.get("ST2_JSON", "") or ""
                            
                            # URL 타입에 따라 사용할 URL 결정
                            if url_type == "mix":
                                used_url = mix_url
                            elif url_type == "nukki":
                                used_url = nukki_url
                            else:  # "name_only"
                                used_url = ""
                            
                            # URL 타입 표시 문자열
                            if url_type == "mix":
                                url_type_display = "믹스"
                            elif url_type == "nukki":
                                url_type_display = "누끼"
                            else:
                                url_type_display = "상품명만"
                            
                            # search_keywords 추출
                            search_keywords_str = ""
                            if st2_json:
                                try:
                                    st2_data = json.loads(st2_json) if isinstance(st2_json, str) else st2_json
                                    search_keywords = st2_data.get("search_keywords", [])
                                    if search_keywords and isinstance(search_keywords, list):
                                        search_keywords_str = ",".join([str(kw).strip() for kw in search_keywords if kw])
                                except:
                                    search_keywords_str = ""
                            
                            # export_row 형식 (기존 상품조합 형태 유지)
                            export_row = {
                                "상품코드": product_code,
                                "시트명": sheet_name,
                                "스토어명": store_name,
                                "스토어별칭": store_alias,
                                "카테고리": category,
                                "사용URL": used_url,
                                "URL타입": url_type_display,
                                "누끼url": nukki_url,
                                "믹스url": mix_url,
                                "ST4_최종결과": final_name,
                                "줄번호": line_index + 1,
                                "ST2_JSON": st2_json,
                                "search_keywords": search_keywords_str,
                            }
                            all_data.append(export_row)
                        
                        # 카테고리별 상품 수 로그 (진행 상황 확인용)
                        if len(products) > 0:
                            self._log(f"    ✓ {category}: {len(products)}개 상품 수집")
                    
                    self._log(f"  ✅ [{idx}/{len(selected_items)}] {store_alias} 완료: 총 {len(all_data)}개 상품 수집")
                
                if not all_data:
                    messagebox.showwarning("경고", "병합할 데이터가 없습니다.")
                    return
                
                self._log(f"📊 총 {len(all_data)}개 상품 데이터 수집 완료. Excel 파일 생성 중...")
                
                # DataFrame 생성
                df = pd.DataFrame(all_data)
                
                # Excel 파일로 저장 (시트별로 분리)
                self._log(f"💾 Excel 파일 저장 중: {os.path.basename(save_path)}")
                with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                    existing_sheet_names = []  # 이미 저장된 시트명 추적
                    
                    # 시트별로 그룹화하여 저장
                    for sheet_name in sorted(df['시트명'].unique()):
                        sheet_df = df[df['시트명'] == sheet_name].copy()
                        
                        # export_row 형식의 컬럼 순서대로 정렬
                        column_order = [
                            '상품코드', '시트명', '스토어명', '스토어별칭', '카테고리',
                            '사용URL', 'URL타입', '누끼url', '믹스url', 
                            'ST4_최종결과', '줄번호', 'ST2_JSON', 'search_keywords'
                        ]
                        
                        # 존재하는 컬럼만 선택하고 순서대로 정렬
                        available_columns = [col for col in column_order if col in sheet_df.columns]
                        # 순서에 없는 컬럼도 추가
                        for col in sheet_df.columns:
                            if col not in available_columns:
                                available_columns.append(col)
                        
                        sheet_df_save = sheet_df[available_columns]
                        
                        # 시트명으로 저장 (Excel 시트명 제한 고려, 중복 방지)
                        safe_sheet_name = sheet_name[:31] if sheet_name else "Sheet1"  # Excel 시트명 최대 31자
                        # 시트명 중복 체크 및 처리
                        original_sheet_name = safe_sheet_name
                        counter = 1
                        while safe_sheet_name in existing_sheet_names:
                            safe_sheet_name = f"{original_sheet_name[:28]}_{counter}"
                            counter += 1
                        existing_sheet_names.append(safe_sheet_name)
                        
                        sheet_df_save.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                        self._log(f"  ✓ 시트 '{safe_sheet_name}' 저장 완료 ({len(sheet_df_save)}개 행)")
                
                self._log(f"✅ 병합 파일 생성 완료: {os.path.basename(save_path)}")
                messagebox.showinfo("완료", 
                    f"병합된 파일이 생성되었습니다.\n\n"
                    f"총 {len(all_data)}개 상품\n"
                    f"시트 수: {len(df['시트명'].unique())}개\n\n"
                    f"파일: {os.path.basename(save_path)}")
                
                self._log(f"✅ 히스토리 병합 다운로드 완료: {len(all_data)}개 상품 ({os.path.basename(save_path)})")
                
            finally:
                db_handler.close()
                
        except Exception as e:
            import traceback
            error_msg = f"병합 다운로드 실패:\n{str(e)}\n\n{traceback.format_exc()}"
            messagebox.showerror("오류", error_msg)
            self._log(f"❌ 히스토리 병합 다운로드 실패: {e}")
    
    def _run_export_for_upload(self, selected_categories: List[str], selected_markets: List[Dict[str, str]], save_dir: str, exclude_assigned: bool = True, export_mode: str = "upload", product_code_filter_mode: str = "none", product_code_filter_codes: set = None, custom_filename: str = None, skip_logging: bool = False):
        """마켓 업로드용 데이터 출고 실행 (백그라운드 스레드)
        
        Args:
            product_code_filter_mode: "none", "exclude", "include"
            product_code_filter_codes: 필터링할 상품코드 set
        """
        db_handler = None
        progress_dialog = None
        try:
            import pandas as pd
            import json
            from datetime import datetime
            
            # 진행 상황 다이얼로그 생성
            progress_dialog = self._create_progress_dialog("데이터 출고 진행 중")
            self._update_progress_dialog(progress_dialog, 0, "초기화 중...", "데이터 출고를 시작합니다...")
            
            self._log("=== 마켓 업로드용 데이터 출고 시작 ===")
            if selected_categories:
                self._log(f"카테고리 섹션에서 선택된 카테고리: {len(selected_categories)}개")
            else:
                self._log("카테고리 섹션에서 선택하지 않음 (스토어 메모의 카테고리 사용)")
            self._log(f"선택된 마켓: {len(selected_markets)}개")
            self._log(f"이미 배정된 상품코드 제외: {'예' if exclude_assigned else '아니오 (재출력 가능)'}")
            
            # 상품코드 필터 로그
            if product_code_filter_mode == "exclude":
                self._log(f"상품코드 필터링: 제외 모드 ({len(product_code_filter_codes or set())}개 상품코드 제외)")
            elif product_code_filter_mode == "include":
                self._log(f"상품코드 필터링: 포함 모드 ({len(product_code_filter_codes or set())}개 상품코드만 포함)")
            else:
                self._log("상품코드 필터링: 사용 안함")
            
            # 프로그레스 바 초기화
            self._update_progress(0, "초기화 중...")
            
            # 백그라운드 스레드에서는 새로운 DB 연결 생성
            db_path = self.db_path_var.get()
            db_handler = DBHandler(db_path)
            db_handler.connect()
            
            # 전체 통계
            total_export_count = 0
            
            # 전체 스토어 수 계산 (진행률 계산용)
            total_stores = len(selected_markets)
            processed_stores = 0
            total_logged_count = 0
            exported_files = []
            
            # 1단계: 시트별로 마켓 그룹화
            from collections import defaultdict
            markets_by_sheet = defaultdict(list)
            for market_info in selected_markets:
                sheet_name = market_info.get("sheet_name", "")
                if sheet_name:
                    markets_by_sheet[sheet_name].append(market_info)
            
            # 전체 스토어 수 계산 (진행률 계산용)
            total_stores = len(selected_markets)
            processed_stores = 0
            
            self._log(f"시트별 그룹화: {len(markets_by_sheet)}개 시트")
            self._update_progress(5, f"시트별 그룹화 완료 ({len(markets_by_sheet)}개 시트)")
            self._update_progress_dialog(progress_dialog, 5, f"시트별 그룹화 완료 ({len(markets_by_sheet)}개 시트)", f"{len(markets_by_sheet)}개 시트, {total_stores}개 스토어 처리 준비 완료")
            self._update_progress_dialog(progress_dialog, 5, f"시트별 그룹화 완료 ({len(markets_by_sheet)}개 시트)", f"{len(markets_by_sheet)}개 시트, {total_stores}개 스토어 처리 준비 완료")
            
            # 스토어별 수량 제한 확인 (각 스토어별로 엑셀에 제공될 개수 제한)
            total_quantity_limit = None
            qty_str = self.total_quantity_var.get().strip()
            if qty_str:
                try:
                    total_quantity_limit = int(qty_str)
                    if total_quantity_limit < 0:
                        total_quantity_limit = None
                except ValueError:
                    total_quantity_limit = None
            
            # 도매처/소매처별 조합 추적 (도매처↔소매처 간 조합 별도 추적)
            # DB에서 실제 할당된 조합을 도매처/소매처별로 분리 로드
            retail_used_combinations_db = set()    # 소매처 조합 (DB)
            wholesale_used_combinations_db = set() # 도매처 조합 (DB)
            try:
                cursor = db_handler.conn.cursor()
                cursor.execute("""
                    SELECT ca.product_code, ca.combination_index, ca.sheet_name,
                           pc.url_type, pc.line_index, pc.product_name,
                           COALESCE(pc.nukki_url, ''), COALESCE(pc.mix_url, '')
                    FROM combination_assignments ca
                    JOIN product_combinations pc
                    ON ca.product_code = pc.product_code
                    AND ca.combination_index = pc.combination_index
                """)

                for row in cursor.fetchall():
                    pc, combo_idx, sn, url_type, line_idx, prod_name, nukki, mix = row
                    used_url = nukki if url_type == "nukki" else (mix if url_type == "mix" else "")
                    combo_key = (pc, url_type, line_idx, prod_name, used_url)
                    # 도매처/소매처 분류
                    if sn in WHOLESALE_SHEETS:
                        wholesale_used_combinations_db.add(combo_key)
                    else:
                        retail_used_combinations_db.add(combo_key)
            except Exception as e:
                self._log(f"  ⚠️ 조합 조회 실패: {e}")

            # 메모리에서도 추적 (이번 출고에서 할당한 조합)
            retail_used_combinations = retail_used_combinations_db.copy()
            wholesale_used_combinations = wholesale_used_combinations_db.copy()

            self._log(f"[조합 추적] 소매처: {len(retail_used_combinations_db)}개, 도매처: {len(wholesale_used_combinations_db)}개 (DB 로드)")
            
            # 2단계: 시트별로 처리
            # 중요: 전체 시트에 대해 동일 조합 추적 (시트별 독립 추적 제거)
            # 새로운 DB만 출력 옵션 체크시: 같은 스토어 내 같은 상품코드 출력 불가
            # 옵션 체크 해제시: 같은 스토어 내 같은 상품코드 출력 가능
            
            # market_id 캐싱 (성능 최적화) - 시트별로 한 번만 조회
            market_id_cache = {}
            for sheet_name, sheet_markets in markets_by_sheet.items():
                if sheet_name not in market_id_cache:
                    cursor = db_handler.conn.cursor()
                    cursor.execute("SELECT id FROM markets WHERE market_name = ?", (sheet_name,))
                    row = cursor.fetchone()
                    market_id_cache[sheet_name] = row[0] if row else None
            
            for sheet_name, sheet_markets in markets_by_sheet.items():
                self._log("")
                # 도매처/소매처 구분
                is_wholesale = sheet_name in WHOLESALE_SHEETS
                group_type = "도매처" if is_wholesale else "소매처"
                # 해당 그룹의 조합 세트 선택
                global_used_combinations = wholesale_used_combinations if is_wholesale else retail_used_combinations
                global_used_combinations_db = wholesale_used_combinations_db if is_wholesale else retail_used_combinations_db

                display_name = SHEET_DISPLAY_NAMES.get(sheet_name, sheet_name)
                self._log(f"=== {display_name} [{group_type}] 처리 시작 ===")
                self._log(f"  선택된 스토어: {len(sheet_markets)}개")
                self._log(f"  [중요] {group_type} 내 동일 조합 추적 (도매처↔소매처 분리)")
                if exclude_assigned:
                    self._log(f"  [중요] 새로운 DB만 출력 옵션 체크: 같은 스토어 내 같은 상품코드 출력 불가")
                else:
                    self._log(f"  [중요] 옵션 체크 해제: 같은 스토어 내 같은 상품코드 출력 가능")
                
                # 시트 단위 캐시는 한 번만 생성하고 스토어 루프에서 재사용
                sheet_used_combinations_cache = self._build_sheet_used_combinations_cache(db_handler, sheet_name)
                store_used_product_codes_cache_map: Dict[str, set] = {}
                
                # 스토어별로 처리
                # 시트별로 선택된 카테고리 목록 (스토어 메모에 카테고리가 있는 스토어는 해당 카테고리 사용)
                for market_info in sheet_markets:
                    market_name = market_info.get("market_name", "")
                    business_number = market_info.get("business_number", "")
                    alias = market_info.get("alias", "")
                    sheet_name = market_info.get("sheet_name", "")
                    owner = market_info.get("owner", "")
                    biz_num = market_info.get("biz_num", "")
                    
                    if not market_name or not business_number:
                        continue
                    
                    # 스토어 메모 및 카테고리 정보 가져오기 (파일명 생성용)
                    store_key = self._get_store_key(sheet_name, owner, biz_num, alias)
                    store_memo_data = self.store_memos.get(store_key, {})
                    memo_text = store_memo_data.get("memo", "")
                    memo_categories = store_memo_data.get("categories", [])
                    registered_count = store_memo_data.get("registered_count", None)
                    
                    # 스토어별 카테고리 결정: 스토어 메모에 카테고리가 있으면 그것을 우선 사용, 없으면 선택된 카테고리 사용
                    raw_store_categories = memo_categories if memo_categories else selected_categories
                    store_categories = self._normalize_store_categories(raw_store_categories)
                    
                    if not store_categories:
                        self._log(f"  ⚠️ 스토어 '{market_name}' (별칭: {alias}): 카테고리가 지정되지 않음")
                        continue
                    
                    # 등록된 상품수량 필터링 (출력 상품수량 제한 필터 전에 검증)
                    # 주의: 등록된 상품수량이 없거나 0이어도 출고 가능 (신규 마켓 대응)
                    # 등록된 상품수량이 입력된 경우에만 필터링 적용
                    store_registered_limit = None
                    if registered_count is not None:
                        try:
                            # registered_count가 이미 정수인지 확인
                            if isinstance(registered_count, int):
                                store_registered_limit = registered_count
                            else:
                                # 문자열인 경우 변환 시도
                                store_registered_limit = int(str(registered_count).strip())
                            
                            if store_registered_limit < 0:
                                store_registered_limit = None
                            elif store_registered_limit == 0:
                                # 등록된 상품수량이 0이면 필터링하지 않음 (신규 마켓 대응)
                                store_registered_limit = None
                                self._log(f"  ℹ️ 스토어 '{market_name}' (별칭: {alias}): 등록된 상품수량이 0개이므로 필터링 없이 전체 출고 (신규 마켓)")
                            else:
                                self._log(f"  📊 스토어 '{market_name}' (별칭: {alias}): 등록된 상품수량 {store_registered_limit}개 기준으로 필터링")
                        except (ValueError, TypeError):
                            store_registered_limit = None
                    
                    # 스토어별 수량 제한 로그
                    if total_quantity_limit is not None:
                        self._log(f"  📊 스토어 '{market_name}' (별칭: {alias}): 스토어별 수량 제한 {total_quantity_limit}개 적용")
                    
                    # 해당 스토어에서 사용 가능한 조합 조회 (스토어별로 business_number로 필터링)
                    available_combinations_by_category = {}
                    all_products_by_code = {}  # {product_code: [product1, product2, ...]} - 우선순위 순서
                    
                    # 스토어별 사용 상품코드 캐시(필요한 경우만) 조회
                    store_used_product_codes_cache = set()
                    if exclude_assigned:
                        if business_number not in store_used_product_codes_cache_map:
                            store_used_product_codes_cache_map[business_number] = self._build_store_used_product_codes_cache(
                                db_handler, sheet_name, business_number
                            )
                        store_used_product_codes_cache = store_used_product_codes_cache_map.get(business_number, set())
                    
                    # 시즌 필터링 활성화 여부 가져오기 (스토어별로 동일)
                    season_filter_enabled = getattr(self, 'season_filter_var', tk.BooleanVar(value=True)).get() if export_mode == "upload" else False
                    season_config_for_log = None
                    check_season_validity = None
                    if season_filter_enabled:
                        try:
                            from season_filter_manager_gui import load_season_config, _check_season_validity
                            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                            excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")
                            json_path = os.path.join(script_dir, "season_filters.json")
                            season_config_for_log = load_season_config(excel_path, json_path)
                            check_season_validity = _check_season_validity
                        except Exception:
                            season_config_for_log = None
                            check_season_validity = None
                    
                    # 스토어별 시즌 필터링 통계 수집 (요약 로그용)
                    store_season_stats = {
                        'total_categories': len(store_categories),
                        'total_products_before': 0,
                        'total_products_after': 0,
                        'total_combinations': 0,
                        'season_excluded_count': 0,
                        'included_seasons': {},
                        'excluded_seasons': {}
                    }
                    
                    for category in store_categories:
                        # 스토어별로 사용 가능한 조합 조회 (business_number로 필터링하여 스토어별로 독립적으로 관리)
                        # 캐시된 데이터를 전달하여 중복 조회 방지 (성능 최적화)
                        # 상품코드 필터링도 내부에서 처리하여 성능 최적화 (시즌 필터링 전에 적용)
                        products = db_handler.get_products_for_upload(
                            category, sheet_name, business_number, 
                            exclude_assigned=exclude_assigned,
                            season_filter_enabled=season_filter_enabled,
                            sheet_used_combinations=sheet_used_combinations_cache,
                            store_used_product_codes=store_used_product_codes_cache if exclude_assigned else None,
                            product_code_filter_mode=product_code_filter_mode,
                            product_code_filter_codes=product_code_filter_codes
                        )
                        
                        # 상품코드 필터링 결과 로그 출력 (db_handler 내부에서 필터링 완료)
                        if product_code_filter_mode != "none" and product_code_filter_codes:
                            if hasattr(db_handler, '_last_product_code_filter_info') and db_handler._last_product_code_filter_info:
                                filter_info = db_handler._last_product_code_filter_info.get(category)
                                if filter_info:
                                    mode = filter_info.get('mode')
                                    original_codes = filter_info.get('original_product_codes_count', 0)
                                    filtered_codes = filter_info.get('filtered_product_codes_count', 0)
                                    excluded_codes = filter_info.get('excluded_codes_count', 0)
                                    original_combinations = filter_info.get('original_count', 0)
                                    filtered_combinations = filter_info.get('filtered_count', 0)
                                    
                                    if mode == "exclude" and excluded_codes > 0:
                                        self._log(f"    🔍 상품코드 필터링 (제외): {excluded_codes}개 상품코드 제외됨 (상품코드: {original_codes}개 → {filtered_codes}개, 조합: {original_combinations}개 → {filtered_combinations}개)")
                                    elif mode == "include" and filtered_codes > 0:
                                        self._log(f"    🔍 상품코드 필터링 (포함): {filtered_codes}개 상품코드만 포함됨 (원본: {original_codes}개 상품코드, 조합: {original_combinations}개 → {filtered_combinations}개)")
                        
                        # 시즌 필터링 통계 수집
                        season_info = None
                        if season_filter_enabled and hasattr(db_handler, '_last_season_filter_info'):
                            season_info = db_handler._last_season_filter_info

                        if season_info:
                            update_store_season_stats(store_season_stats, season_info)

                        if season_filter_enabled:
                            log_category_season_result(
                                log_fn=self._log,
                                category=category,
                                season_info=season_info,
                                products_count=len(products),
                                season_config_for_log=season_config_for_log,
                                check_season_validity=check_season_validity,
                            )

                        if products:
                            available_combinations_by_category[category] = products
                            
                            # 상품코드별로 그룹화
                            for product in products:
                                product_code = product.get("상품코드", "")
                                if not product_code:
                                    continue
                                
                                if product_code not in all_products_by_code:
                                    all_products_by_code[product_code] = []
                                all_products_by_code[product_code].append(product)
                    
                    # 사용 가능한 조합이 없으면 해당 스토어 스킵
                    if not all_products_by_code:
                        self._log(f"  ⚠️ 스토어 '{market_name}' (별칭: {alias}): 사용 가능한 조합 없음")
                        continue
                    
                    # 해당 스토어의 데이터 수집
                    market_export_data = []
                    market_logged_count = 0
                    store_used_product_codes = set()  # 해당 스토어에서 이미 사용한 상품코드 추적 (새로운 DB만 출력 옵션 체크시만 사용)
                    store_used_combinations = set()  # 해당 스토어에서 이미 사용한 조합 추적 (exclude_assigned=False일 때 사용)
                    store_processed_codes = set()  # 해당 스토어에서 처리한 상품코드 추적 (스토어별 수량 제한용)
                    skipped_count = 0  # 스킵된 조합 개수
                    registered_count_skipped = 0  # 등록된 상품수량 제한으로 스킵된 개수
                    
                    # 배치 INSERT를 위한 리스트 (성능 최적화)
                    combination_assignments_batch = []  # [(sheet_name, business_number, product_code, combination_index), ...]
                    upload_logs_batch = []  # [(business_number, market_id, market_name, product_id, product_code, ...), ...]
                    
                    # exclude_assigned=False일 때, 해당 스토어에서 이미 사용한 조합 확인
                    # 새로운 combination_assignments 테이블과 upload_logs 모두 확인
                    if not exclude_assigned:
                        try:
                            cursor = db_handler.conn.cursor()
                            
                            # 1. combination_assignments에서 조합 인덱스 확인
                            cursor.execute("""
                                SELECT ca.product_code, ca.combination_index, 
                                       pc.url_type, pc.product_name, pc.nukki_url, pc.mix_url
                                FROM combination_assignments ca
                                JOIN product_combinations pc 
                                ON ca.product_code = pc.product_code 
                                AND ca.combination_index = pc.combination_index
                                WHERE ca.sheet_name = ? 
                                AND ca.business_number = ?
                            """, (sheet_name, business_number))
                            
                            for row in cursor.fetchall():
                                used_pc, combo_idx, url_type, used_name, used_nukki, used_mix = row
                                if used_pc:
                                    # 조합 키 생성
                                    if url_type == "nukki" and used_nukki:
                                        store_used_combinations.add((used_pc, "nukki", used_name, used_nukki))
                                    elif url_type == "mix" and used_mix:
                                        store_used_combinations.add((used_pc, "mix", used_name, used_mix))
                                    elif url_type == "name_only":
                                        store_used_combinations.add((used_pc, "name_only", used_name, ""))
                            
                            # 2. upload_logs에서도 확인 (하위 호환성)
                            cursor.execute("""
                                SELECT DISTINCT product_code, used_nukki_url, used_mix_url, used_product_name
                                FROM upload_logs 
                                WHERE market_name = ? 
                                AND business_number = ?
                                AND upload_status = 'SUCCESS'
                                AND product_code IS NOT NULL
                                AND NOT EXISTS (
                                    SELECT 1 FROM combination_assignments ca
                                    WHERE ca.sheet_name = upload_logs.market_name
                                    AND ca.business_number = upload_logs.business_number
                                    AND ca.product_code = upload_logs.product_code
                                )
                            """, (sheet_name, business_number))
                            
                            for row in cursor.fetchall():
                                used_pc, used_nukki, used_mix, used_name = row
                                if used_pc:
                                    # 조합 키 생성 (상품코드, url_type, line_index는 정확히 알 수 없으므로 URL과 상품명으로만 판단)
                                    if used_nukki:
                                        store_used_combinations.add((used_pc, "nukki", used_name, used_nukki))
                                    if used_mix:
                                        store_used_combinations.add((used_pc, "mix", used_name, used_mix))
                        except Exception as e:
                            self._log(f"    ⚠️ 스토어 조합 조회 실패: {e}")
                    
                    # 상품코드 중심으로 처리 (카테고리 순서가 아닌 상품코드 순서로)
                    # 우선순위: 출력 상품수 제한이 있으면 우선적으로 출고된 적 없는 상품코드 먼저
                    # exclude_assigned=True일 때만 우선순위 적용 (출고된 적 없는 상품코드 먼저)
                    if exclude_assigned and total_quantity_limit is not None:
                        # 출고된 적 없는 상품코드와 출고된 적 있는 상품코드 분리
                        unexported_codes = []
                        exported_codes = []
                        
                        # 출고 이력 확인 (전체 시트에서 확인 - global_used_combinations 기준)
                        # global_used_combinations_db에 있는 상품코드는 이미 출고된 것으로 간주
                        try:
                            # global_used_combinations_db에서 상품코드 추출
                            exported_product_codes = {combo[0] for combo in global_used_combinations_db if combo[0]}
                            
                            for product_code in all_products_by_code.keys():
                                if product_code in exported_product_codes:
                                    exported_codes.append(product_code)
                                else:
                                    unexported_codes.append(product_code)
                            
                            # 출고된 적 없는 상품코드를 먼저, 그 다음 출고된 적 있는 상품코드
                            product_codes_list = sorted(unexported_codes) + sorted(exported_codes)
                            if unexported_codes:
                                self._log(f"    📋 우선순위 적용: 출고된 적 없는 상품코드 {len(unexported_codes)}개를 먼저 처리")
                        except Exception as e:
                            self._log(f"    ⚠️ 출고 이력 조회 실패, 기본 정렬 사용: {e}")
                            product_codes_list = sorted(all_products_by_code.keys())
                    else:
                        # 우선순위 적용 안 함 (기본 정렬)
                        product_codes_list = sorted(all_products_by_code.keys())  # 상품코드 리스트 (정렬하여 일관성 유지)
                    
                    for product_code in product_codes_list:
                        # 등록된 상품수량 필터링 (출력 상품수량 제한 필터 전에 검증)
                        if store_registered_limit is not None:
                            if len(store_processed_codes) >= store_registered_limit:
                                # 등록된 상품수량 제한 초과 (이미 할당된 상품코드 수가 등록된 상품수량에 도달)
                                registered_count_skipped += 1
                                continue

                        # 스토어별 수량 제한 확인 (각 스토어별로 엑셀에 제공될 개수 제한)
                        # 주의: 실제로 조합이 할당된 후에만 카운트에 포함되므로, 여기서는 예비 체크만 수행
                        if total_quantity_limit is not None:
                            if len(store_processed_codes) >= total_quantity_limit:
                                # 스토어별 수량 제한 초과 (이미 할당된 상품코드 수가 제한에 도달)
                                skipped_count += 1
                                continue

                        # 새로운 DB만 출력 옵션 체크시: 같은 스토어 내 같은 상품코드는 1개 조합만 사용
                        # 옵션 체크 해제시: 같은 스토어 내 같은 상품코드 출력 가능
                        if exclude_assigned and product_code in store_used_product_codes:
                            skipped_count += 1
                            continue

                        # === 성능 최적화: 이미 조회된 조합 사용 (이중 조회 제거) ===
                        # get_products_for_upload에서 이미 조합을 조회하여 all_products_by_code에 저장함
                        # get_next_combination_for_store 호출 제거 → DB 쿼리 대폭 감소
                        available_combos = all_products_by_code.get(product_code, [])

                        # 사용 가능한 조합 선택 (global_used_combinations 체크)
                        found_product = None
                        for combo in available_combos:
                            combo_url_type = combo.get("url_type", "mix")
                            combo_line_index = combo.get("line_index", 0)
                            combo_final_name = combo.get("ST4_최종결과", "") or ""
                            combo_nukki_url = combo.get("누끼url", "") or ""
                            combo_mix_url = combo.get("믹스url", "") or ""

                            # URL 결정
                            if combo_url_type == "mix":
                                combo_used_url = combo_mix_url
                            elif combo_url_type == "nukki":
                                combo_used_url = combo_nukki_url
                            else:
                                combo_used_url = ""

                            # 전체 시트 중복 체크
                            combo_key = (product_code, combo_url_type, combo_line_index, combo_final_name, combo_used_url)
                            if global_used_combinations and combo_key in global_used_combinations:
                                continue

                            # 스토어 중복 체크 (exclude_assigned=False일 때)
                            if not exclude_assigned and store_used_combinations:
                                store_combo_key = (product_code, combo_url_type, combo_final_name, combo_used_url)
                                if store_combo_key in store_used_combinations:
                                    continue

                            # 사용 가능한 조합 발견
                            found_product = combo
                            break

                        # 사용 가능한 조합이 없으면 스킵
                        if not found_product:
                            continue
                        
                        # 조합 정보 추출
                        mix_url = found_product.get("믹스url", "") or ""
                        nukki_url = found_product.get("누끼url", "") or ""
                        final_name = found_product.get("ST4_최종결과", "") or ""
                        url_type = found_product.get("url_type", "mix")
                        line_index = found_product.get("line_index", 0)
                        
                        # URL 타입에 따라 사용할 URL 결정
                        if url_type == "mix":
                            used_url = mix_url
                        elif url_type == "nukki":
                            used_url = nukki_url
                        else:  # "name_only"
                            used_url = ""  # URL 없음
                        
                        # 조합 키 생성 (전체 시트 동일 조합 추적용)
                        found_combination_key = (product_code, url_type, line_index, final_name, used_url)
                        
                        # 조합 사용 표시
                        if exclude_assigned:
                            store_used_product_codes.add(product_code)  # 새로운 DB만 출력 옵션 체크시만 스토어별 상품코드 추적
                        global_used_combinations.add(found_combination_key)  # 전체 시트에서 사용된 조합으로 표시
                        if not exclude_assigned:
                            candidate_combination_key = (product_code, url_type, final_name, used_url)
                            store_used_combinations.add(candidate_combination_key)
                        
                        # 새로운 조합 할당 테이블에 기록 (배치 INSERT로 변경 - 성능 최적화)
                        combination_index = found_product.get("combination_index")
                        if combination_index is not None:
                            # 배치 INSERT를 위해 리스트에 추가 (커밋은 나중에 일괄 처리)
                            combination_assignments_batch.append(
                                (sheet_name, business_number, product_code, combination_index)
                            )
                        
                        # 조합 정보 추가 추출 (product_id 등)
                        product = found_product
                        product_id = product.get("product_id")
                        
                        # 사용한 URL과 인덱스 결정 (이미 위에서 used_url 계산됨)
                        used_mix_url = mix_url if url_type == "mix" else ""
                        used_nukki_url = nukki_url if url_type == "nukki" else ""
                        
                        # URL 타입 표시 문자열
                        if url_type == "mix":
                            url_type_display = "믹스"
                        elif url_type == "nukki":
                            url_type_display = "누끼"
                        else:  # "name_only"
                            url_type_display = "상품명만"
                        
                        # ST2_JSON 및 search_keywords 추출
                        st2_json = product.get("ST2_JSON", "") or ""
                        search_keywords_str = ""
                        
                        if st2_json:
                            try:
                                # ST2_JSON 파싱
                                st2_data = json.loads(st2_json) if isinstance(st2_json, str) else st2_json
                                # search_keywords 추출
                                search_keywords = st2_data.get("search_keywords", [])
                                if search_keywords and isinstance(search_keywords, list):
                                    # 배열을 쉼표로 구분된 문자열로 변환
                                    search_keywords_str = ",".join([str(kw).strip() for kw in search_keywords if kw])
                            except (json.JSONDecodeError, AttributeError, TypeError) as e:
                                # JSON 파싱 실패 시 빈 문자열
                                search_keywords_str = ""
                                self._log(f"    ⚠️ ST2_JSON 파싱 실패 (상품코드: {product_code}): {e}")
                        
                        # 출력 필드 구성 (실제 사용한 URL 명확히 표시)
                        export_row = {
                            "상품코드": product_code,
                            "사용URL": used_url,  # 실제 사용한 URL (믹스 또는 누끼, 없으면 빈 문자열)
                            "URL타입": url_type_display,  # 사용한 URL 타입
                            "누끼url": nukki_url,  # 참고용
                            "믹스url": mix_url,  # 참고용
                            "ST4_최종결과": final_name,
                            "줄번호": line_index + 1,  # 1부터 시작 (사용자 친화적)
                            "ST2_JSON": st2_json,  # ST2_JSON 전체 (값이 없으면 빈 문자열)
                            "search_keywords": search_keywords_str,  # search_keywords 배열을 쉼표로 구분된 문자열
                        }
                        market_export_data.append(export_row)
                        
                        # 실제로 export_row가 추가된 후에만 스토어별 수량 제한용 카운터 업데이트
                        if product_code not in store_processed_codes:
                            store_processed_codes.add(product_code)
                        
                        # DB에 기록 준비 (배치 INSERT로 변경 - 성능 최적화)
                        # market_id는 캐시에서 가져오기 (이미 조회됨)
                        market_id = market_id_cache.get(sheet_name)
                        
                        # 상품명 인덱스는 실제 사용한 줄 번호
                        product_name_index = line_index
                        image_mix_index = 0 if url_type == "mix" else None
                        image_nukki_index = 0 if url_type == "nukki" else None
                        # name_only인 경우 둘 다 None
                        
                        # 업로드 전략 정보
                        strategy = {
                            "url_type": url_type,
                            "product_name_index": product_name_index,
                            "line_index": line_index,
                            "image_mix_index": image_mix_index,
                            "image_nukki_index": image_nukki_index
                        }
                        
                        # upload_logs 기록 조건:
                        # 1. exclude_assigned가 True일 때만 기록 (새로운 DB만 출력 옵션이 켜져있을 때만)
                        # 2. skip_logging이 False일 때만 기록 (히스토리 재다운로드인 경우 기록 안 함)
                        # exclude_assigned가 False이면 이미 배정된 조합도 재출력 가능하므로 기록하지 않음
                        if exclude_assigned and not skip_logging:
                            # 배치 INSERT를 위해 리스트에 추가 (커밋은 나중에 일괄 처리)
                            upload_logs_batch.append((
                                business_number,
                                market_id,
                                sheet_name,  # 시트명으로 기록
                                product_id,
                                product_code,
                                final_name,
                                used_nukki_url,
                                used_mix_url,
                                product_name_index,
                                image_nukki_index,
                                image_mix_index,
                                json.dumps(strategy, ensure_ascii=False),
                                "SUCCESS",
                                f"카테고리: {', '.join(store_categories) if store_categories else 'N/A'}, 마켓: {market_name}, 스토어별칭: {alias}, 줄번호: {line_index}",
                                datetime.now().isoformat()
                            ))
                            market_logged_count += 1
                        
                        # DB 기록 확인 로그 (상세 정보)
                        if not skip_logging:
                            self._log(f"      ✓ DB 기록: 상품코드 '{product_code}' → 마켓 '{sheet_name}' / 스토어 '{market_name}' / 조합: {url_type}url + 상품명({line_index+1}번째줄)")
                        else:
                            self._log(f"      ⏭️ DB 기록 건너뜀 (재다운로드): 상품코드 '{product_code}' → 마켓 '{sheet_name}' / 스토어 '{market_name}' / 조합: {url_type}url + 상품명({line_index+1}번째줄)")
                    
                    # 배치 INSERT 실행 (성능 최적화)
                    try:
                        cursor = db_handler.conn.cursor()
                        
                        # combination_assignments 배치 INSERT
                        if combination_assignments_batch:
                            cursor.executemany("""
                                INSERT OR IGNORE INTO combination_assignments 
                                (sheet_name, business_number, product_code, combination_index)
                                VALUES (?, ?, ?, ?)
                            """, combination_assignments_batch)
                        
                        # upload_logs 배치 INSERT
                        if upload_logs_batch:
                            cursor.executemany("""
                                INSERT INTO upload_logs (
                                    business_number, market_id, market_name, product_id, product_code,
                                    used_product_name, used_nukki_url, used_mix_url,
                                    product_name_index, image_nukki_index, image_mix_index,
                                    upload_strategy, upload_status, notes, uploaded_at
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, upload_logs_batch)
                        
                        # 한 번에 커밋
                        db_handler.conn.commit()
                        self._log(f"    ✓ 배치 DB 기록 완료: combination_assignments {len(combination_assignments_batch)}건, upload_logs {len(upload_logs_batch)}건")
                    except Exception as e:
                        db_handler.conn.rollback()
                        self._log(f"    ⚠️ 배치 DB 기록 실패: {e}")
                        import traceback
                        self._log(traceback.format_exc())
                    
                    # 배치 리스트 초기화 (다음 스토어를 위해)
                    combination_assignments_batch.clear()
                    upload_logs_batch.clear()
                
                    # 해당 마켓/스토어에 대한 엑셀 파일 저장
                    if market_export_data:
                        # 파일명 생성 (날짜_스토어명_카테고리_메모)
                        date_str = datetime.now().strftime('%Y%m%d')
                        
                        # 스토어명 안전하게 변환
                        safe_alias = alias.replace("/", "_").replace("\\", "_").replace(">", "_").replace("<", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("|", "_")[:30]
                        
                        # 카테고리 정보 처리 (대>중 형식으로 변환하여 첫 번째 카테고리만 사용)
                        category_str = ""
                        if memo_categories:
                            # 첫 번째 카테고리를 '대>중' 형식으로 변환
                            first_category = memo_categories[0]
                            category_large_medium = self._get_category_large_medium(first_category)
                            # 안전하게 변환 ('>'는 '-'로 대체)
                            category_str = category_large_medium.replace("/", "_").replace("\\", "_").replace(">", "-").replace("<", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("|", "_")[:30]
                            # 여러 카테고리가 있으면 개수 표시
                            if len(memo_categories) > 1:
                                category_str += f"_외{len(memo_categories)-1}개"
                        
                        # 메모 정보 처리
                        memo_str = ""
                        if memo_text:
                            # 메모를 안전하게 변환 (특수문자 제거, 길이 제한)
                            memo_str = memo_text.replace("/", "_").replace("\\", "_").replace(">", "_").replace("<", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("|", "_").replace("\n", "_").replace("\r", "_")[:20]
                        
                        # 파일명 조합 (날짜_스토어명_카테고리_메모)
                        # 히스토리 재다운로드인 경우 사용자가 지정한 파일명 사용
                        if custom_filename:
                            filename = custom_filename
                            if not filename.endswith('.xlsx'):
                                filename += '.xlsx'
                        else:
                            filename_parts = [date_str, safe_alias]
                            if category_str:
                                filename_parts.append(category_str)
                            if memo_str:
                                filename_parts.append(memo_str)
                            filename = "_".join(filename_parts) + ".xlsx"
                        
                        filepath = os.path.join(save_dir, filename)
                        
                        try:
                            df = pd.DataFrame(market_export_data)
                            # ExcelWriter를 사용하여 권한 문제 해결 (임시 파일 사용 안 함)
                            with pd.ExcelWriter(filepath, engine='openpyxl', mode='w') as writer:
                                df.to_excel(writer, index=False, sheet_name='Sheet1')
                            total_export_count += len(market_export_data)
                            total_logged_count += market_logged_count
                            exported_files.append(filename)
                            log_msg = f"✅ 파일 저장 완료: {filename} ({len(market_export_data)}건)"
                            if store_registered_limit is not None:
                                log_msg += f" (등록된 상품수량: {store_registered_limit}개 기준)"
                            if total_quantity_limit is not None:
                                log_msg += f" (스토어별 수량 제한: {total_quantity_limit}개)"
                            if registered_count_skipped > 0:
                                log_msg += f" (등록된 상품수량 제한으로 {registered_count_skipped}건 스킵)"
                            if skipped_count > 0:
                                if exclude_assigned:
                                    log_msg += f" (중복 방지/수량 제한으로 {skipped_count}건 스킵 - 정상 동작)"
                                else:
                                    log_msg += f" (전체 시트에서 이미 사용된 조합/수량 제한으로 {skipped_count}건 스킵 - 정상 동작)"
                            self._log(log_msg)
                            
                            # 스토어별 출고 요약 로그
                            self._log(f"")
                            self._log(f"  📋 스토어 출고 요약: {alias} ({market_name})")
                            self._log(f"    - 카테고리: {store_season_stats['total_categories']}개")
                            self._log(f"    - 출고된 조합: {len(market_export_data)}건")
                            self._log(f"    - DB 기록: {market_logged_count}건")
                            
                            if skipped_count > 0 or registered_count_skipped > 0:
                                total_skipped = skipped_count + registered_count_skipped
                                self._log(f"    - 스킵된 조합: {total_skipped}건 (중복 방지/수량 제한)")
                            
                            if season_filter_enabled and store_season_stats['total_products_before'] > 0:
                                self._log(f"    - 시즌 필터링:")
                                self._log(f"      • 필터링 전 상품 코드: {store_season_stats['total_products_before']}개")
                                self._log(f"      • 필터링 후 상품 코드: {store_season_stats['total_products_after']}개")
                                if store_season_stats['season_excluded_count'] > 0:
                                    self._log(f"      • 제외된 시즌 상품: {store_season_stats['season_excluded_count']}개")
                                if store_season_stats['included_seasons']:
                                    included_seasons_str = ", ".join([f"{name}({count}개)" for name, count in store_season_stats['included_seasons'].items()])
                                    self._log(f"      • 포함된 시즌: {included_seasons_str}")
                            
                            if store_registered_limit is not None:
                                self._log(f"    - 등록된 상품수량 제한: {store_registered_limit}개")
                            if total_quantity_limit is not None:
                                self._log(f"    - 스토어별 수량 제한: {total_quantity_limit}개")
                            
                            self._log(f"")
                            
                            # 출고 히스토리 기록
                            try:
                                categories_json = json.dumps(memo_categories, ensure_ascii=False) if memo_categories else None
                                export_mode_display = "마켓 업로드용(완료된DB)" if export_mode == "upload" else "미완료 DB"
                                db_handler.log_export_history(
                                    export_date=date_str,
                                    sheet_name=sheet_name,
                                    store_name=market_name,
                                    store_alias=alias,
                                    business_number=business_number,
                                    categories=categories_json,
                                    product_count=len(market_export_data),
                                    file_path=filepath,
                                    file_name=filename,
                                    memo=memo_text if memo_text else None,
                                    export_mode=export_mode_display,
                                    exclude_assigned=exclude_assigned
                                )
                            except Exception as e:
                                self._log(f"    ⚠️ 히스토리 기록 실패: {e}")
                            
                            # 진행률 업데이트
                            processed_stores += 1
                            if total_stores > 0:
                                progress = int((processed_stores / total_stores) * 95) + 5  # 5% ~ 100%
                                self._update_progress(progress, f"스토어 처리 중: {processed_stores}/{total_stores} ({alias})")
                        except Exception as e:
                            self._log(f"❌ 파일 저장 실패: {filename} - {e}")
                            # 진행률 업데이트 (실패해도 카운트)
                            processed_stores += 1
                            if total_stores > 0:
                                progress = int((processed_stores / total_stores) * 95) + 5
                                self._update_progress(progress, f"스토어 처리 중: {processed_stores}/{total_stores} ({alias})")
                                self._update_progress_dialog(progress_dialog, progress, f"스토어 처리 중: {processed_stores}/{total_stores}", f"⚠️ {alias}: 파일 저장 실패 - {e}")
                    else:
                        self._log(f"  ⚠️ 스토어 '{market_name}' (별칭: {alias}): 할당된 조합 없음")
                        # 디버깅: 왜 할당되지 않았는지 확인
                        if registered_count_skipped > 0:
                            self._log(f"    [원인] 등록된 상품수량 제한({store_registered_limit}개)으로 {registered_count_skipped}건 스킵됨 (정상 동작)")
                        if skipped_count > 0:
                            if exclude_assigned:
                                self._log(f"    [원인] 중복 방지/수량 제한으로 {skipped_count}건 스킵됨 (정상 동작)")
                            else:
                                self._log(f"    [원인] 전체 시트에서 이미 사용된 조합/수량 제한으로 {skipped_count}건 스킵됨 (정상 동작)")
                        # 사용 가능한 조합이 있었는지 확인
                        total_available = sum(len(products) for products in available_combinations_by_category.values())
                        if store_registered_limit is not None:
                            self._log(f"    [원인] 사용 가능한 총 조합: {total_available}개 / 등록된 상품수량: {store_registered_limit}개 (이미 {len(store_processed_codes)}개 할당됨)")
                        elif total_quantity_limit is not None:
                            self._log(f"    [원인] 사용 가능한 총 조합: {total_available}개 / 스토어별 수량 제한: {total_quantity_limit}개")
                        else:
                            self._log(f"    [원인] 사용 가능한 총 조합: {total_available}개 (모든 조합이 이미 전체 시트에서 사용됨)")
                        # 진행률 업데이트 (할당 없어도 카운트)
                        processed_stores += 1
                        if total_stores > 0:
                            progress = int((processed_stores / total_stores) * 95) + 5
                            self._update_progress(progress, f"스토어 처리 중: {processed_stores}/{total_stores} ({alias})")
                            self._update_progress_dialog(progress_dialog, progress, f"스토어 처리 중: {processed_stores}/{total_stores}", f"ℹ️ {alias}: 할당된 조합 없음 (스킵)")
                
                self._log(f"=== {display_name} [{group_type}] 처리 완료 ===")
            
            # 전체 결과 요약
            self._update_progress(100, "완료")
            if total_export_count > 0:
                self._log(f"✅ 전체 데이터 출고 완료: {total_export_count}건")
                self._log(f"✅ 전체 DB 기록 완료: {total_logged_count}건")
                self._log(f"✅ 저장된 파일: {len(exported_files)}개")
                self._log(f"저장 위치: {save_dir}")
                self._update_progress_dialog(progress_dialog, 100, "완료", f"✅ 전체 데이터 출고 완료: {total_export_count}건, 파일: {len(exported_files)}개")
                
                # 진행 상황 창 닫기 (메시지 박스 표시 전에)
                self._close_progress_dialog(progress_dialog)
                
                self.after(0, lambda: messagebox.showinfo(
                    "완료",
                    f"데이터 출고가 완료되었습니다.\n\n"
                    f"출고 건수: {total_export_count}건\n"
                    f"DB 기록: {total_logged_count}건\n"
                    f"저장된 파일: {len(exported_files)}개\n"
                    f"저장 위치: {save_dir}"
                ))
            else:
                self._log("⚠️ 출고할 데이터가 없습니다.")
                self._update_progress_dialog(progress_dialog, 100, "완료", "⚠️ 출고할 데이터가 없습니다.")
                self._close_progress_dialog(progress_dialog)
                self.after(0, lambda: messagebox.showwarning("알림", "출고할 데이터가 없습니다."))
            
            # 버튼 다시 활성화
            self.after(0, lambda: self.btn_start.config(state='normal'))
            
        except Exception as e:
            self._log(f"❌ 오류 발생: {e}")
            import traceback
            self._log(traceback.format_exc())
            error_msg = str(e)
            self._update_progress_dialog(progress_dialog, 0, "오류 발생", f"❌ 오류: {error_msg}")
            self._close_progress_dialog(progress_dialog)
            self.after(0, lambda: self.btn_start.config(state='normal'))
            self.after(0, lambda msg=error_msg: messagebox.showerror("오류", f"데이터 출고 중 오류가 발생했습니다:\n{msg}"))
        finally:
            # 진행 상황 창이 아직 열려있으면 닫기
            if progress_dialog:
                self._close_progress_dialog(progress_dialog)
            # DB 연결 종료 (항상 실행)
            if db_handler:
                try:
                    db_handler.close()
                except Exception as e:
                    self._log(f"⚠️ DB 연결 종료 중 오류: {e}")
    
    def _run_export_incomplete(self, selected_categories: List[str], export_path: str):
        """미완료 DB 출고 실행 (백그라운드 스레드)"""
        db_handler = None
        try:
            import pandas as pd
            from datetime import datetime
            
            self._log("=== 미완료 DB 출고 시작 ===")
            self._log(f"선택된 카테고리: {len(selected_categories)}개")
            
            # 백그라운드 스레드에서는 새로운 DB 연결 생성
            db_path = self.db_path_var.get()
            db_handler = DBHandler(db_path)
            db_handler.connect()
            
            # 선택된 카테고리별로 미완료 상품 조회
            all_products = []
            for category in selected_categories:
                products = db_handler.get_incomplete_products(category)
                all_products.extend(products)
                self._log(f"카테고리 '{category}': {len(products)}건")
            
            # 중복 제거 (상품코드 기준)
            seen_codes = set()
            unique_products = []
            for product in all_products:
                product_code = product.get("상품코드", "")
                if product_code and product_code not in seen_codes:
                    seen_codes.add(product_code)
                    unique_products.append(product)
            
            self._log(f"총 미완료 상품: {len(unique_products)}건 (중복 제거 후)")
            
            if unique_products:
                # 전체 컬럼을 엑셀 파일로 저장
                df = pd.DataFrame(unique_products)
                
                if export_path:
                    # ExcelWriter를 사용하여 권한 문제 해결 (임시 파일 사용 안 함)
                    try:
                        with pd.ExcelWriter(export_path, engine='openpyxl', mode='w') as writer:
                            df.to_excel(writer, index=False, sheet_name='Sheet1')
                    except PermissionError:
                        self._log(f"❌ 파일 저장 실패: 파일이 다른 프로그램에서 열려있거나 권한이 없습니다.")
                        self.after(0, lambda: messagebox.showerror("오류", f"파일을 저장할 수 없습니다.\n파일이 다른 프로그램에서 열려있는지 확인하세요.\n\n경로: {export_path}"))
                        return
                    self._log(f"✅ 미완료 DB 출고 완료: {len(unique_products)}건")
                    self._log(f"저장 위치: {export_path}")
                    
                    # 파일 열기 여부 확인
                    def ask_open_file():
                        result = messagebox.askyesno(
                            "완료",
                            f"미완료 DB 출고가 완료되었습니다.\n\n"
                            f"출고 건수: {len(unique_products)}건\n"
                            f"파일: {os.path.basename(export_path)}\n\n"
                            f"파일을 여시겠습니까?"
                        )
                        if result:
                            try:
                                if os.name == 'nt':  # Windows
                                    os.startfile(export_path)
                                else:  # macOS, Linux
                                    import subprocess
                                    if sys.platform == 'darwin':  # macOS
                                        subprocess.run(['open', export_path])
                                    else:  # Linux
                                        subprocess.run(['xdg-open', export_path])
                            except Exception as e:
                                self._log(f"⚠️ 파일 열기 실패: {e}")
                                messagebox.showerror("오류", f"파일을 열 수 없습니다:\n{e}")
                    
                    self.after(0, ask_open_file)
            else:
                self._log("✅ 미완료 DB가 없습니다.")
                self.after(0, lambda: messagebox.showinfo("알림", "미완료 DB가 없습니다."))
            
            # 버튼 다시 활성화
            self.after(0, lambda: self.btn_start.config(state='normal'))
            
        except Exception as e:
            self._log(f"❌ 오류 발생: {e}")
            import traceback
            self._log(traceback.format_exc())
            error_msg = str(e)
            self.after(0, lambda: self.btn_start.config(state='normal'))
            self.after(0, lambda msg=error_msg: messagebox.showerror("오류", f"미완료 DB 출고 중 오류가 발생했습니다:\n{msg}"))
        finally:
            # DB 연결 종료 (항상 실행)
            if db_handler:
                try:
                    db_handler.close()
                except Exception as e:
                    self._log(f"⚠️ DB 연결 종료 중 오류: {e}")
    
    def _export_all_products(self):
        """입고되어 있는 전체 DB 출력"""
        # DB 파일 확인
        db_path = self.db_path_var.get()
        if not db_path or not os.path.exists(db_path):
            messagebox.showerror("오류", "DB 파일을 먼저 선택해주세요.")
            return
        
        # 저장 파일 경로 선택
        from datetime import datetime
        
        default_filename = f"전체DB출력_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        export_path = filedialog.asksaveasfilename(
            title="전체 DB 출력 파일 저장",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile=default_filename
        )
        
        if not export_path:
            self._log("⚠️ 저장 파일을 선택하지 않아 취소되었습니다.")
            return
        
        # 백그라운드 스레드에서 실행
        threading.Thread(target=self._run_export_all_products, args=(export_path,), daemon=True).start()
    
    def _run_export_all_products(self, export_path: str):
        """전체 DB 출력 실행 (백그라운드 스레드)"""
        db_handler = None
        try:
            from datetime import datetime
            
            self._log("=== 전체 DB 출력 시작 ===")
            
            # 백그라운드 스레드에서는 새로운 DB 연결 생성
            db_path = self.db_path_var.get()
            db_handler = DBHandler(db_path)
            db_handler.connect()
            
            # 전체 ACTIVE 상품 조회
            cursor = db_handler.conn.cursor()
            cursor.execute("""
                SELECT * FROM products 
                WHERE product_status = 'ACTIVE'
                ORDER BY 상품코드
            """)
            
            # 컬럼명 가져오기
            columns = [description[0] for description in cursor.description]
            
            # 모든 데이터 가져오기
            rows = cursor.fetchall()
            
            # 딕셔너리 리스트로 변환
            all_products = []
            for row in rows:
                product_dict = dict(zip(columns, row))
                all_products.append(product_dict)
            
            self._log(f"전체 ACTIVE 상품: {len(all_products):,}건")
            
            if all_products:
                # 전체 컬럼을 엑셀 파일로 저장
                df = pd.DataFrame(all_products)
                # ExcelWriter를 사용하여 권한 문제 해결 (임시 파일 사용 안 함)
                with pd.ExcelWriter(export_path, engine='openpyxl', mode='w') as writer:
                    df.to_excel(writer, index=False, sheet_name='Sheet1')
                
                self._log(f"✅ 전체 DB 출력 완료: {len(all_products):,}건")
                self._log(f"저장 위치: {export_path}")
                
                # 파일 열기 여부 확인
                def ask_open_file():
                    result = messagebox.askyesno(
                        "완료",
                        f"전체 DB 출력이 완료되었습니다.\n\n"
                        f"출력 건수: {len(all_products):,}건\n"
                        f"파일: {os.path.basename(export_path)}\n\n"
                        f"파일을 여시겠습니까?"
                    )
                    if result:
                        try:
                            if os.name == 'nt':  # Windows
                                os.startfile(export_path)
                            else:  # macOS, Linux
                                import subprocess
                                if sys.platform == 'darwin':  # macOS
                                    subprocess.run(['open', export_path])
                                else:  # Linux
                                    subprocess.run(['xdg-open', export_path])
                        except Exception as e:
                            self._log(f"⚠️ 파일 열기 실패: {e}")
                            messagebox.showerror("오류", f"파일을 열 수 없습니다:\n{e}")
                
                self.after(0, ask_open_file)
            else:
                self._log("✅ 출력할 상품이 없습니다.")
                self.after(0, lambda: messagebox.showinfo("알림", "출력할 상품이 없습니다."))
            
        except Exception as e:
            self._log(f"❌ 오류 발생: {e}")
            import traceback
            self._log(traceback.format_exc())
            error_msg = str(e)
            self.after(0, lambda msg=error_msg: messagebox.showerror("오류", f"전체 DB 출력 중 오류가 발생했습니다:\n{msg}"))
        finally:
            # DB 연결 종료 (항상 실행)
            if db_handler:
                try:
                    db_handler.close()
                except Exception as e:
                    self._log(f"⚠️ DB 연결 종료 중 오류: {e}")


if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()
