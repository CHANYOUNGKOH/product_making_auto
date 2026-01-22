"""
stage3_batch_api_Casche.py

Stage 3 Batch API 실행 스크립트 (GUI) - 캐싱 최적화 버전
- 기능: Batch JSONL 생성 -> 업로드 -> 실행 -> 결과 병합 -> [NEW] 통합 리포트 & 휴지통
- [Fix] 배치 목록 및 휴지통에 'Effort' 컬럼 추가
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
  * 동적 콘텐츠(설정, JSON 데이터)를 user 프롬프트에 배치
  * prompt_cache_key 사용으로 캐시 히트율 향상 (토큰 비용 최대 90% 절감 가능)
"""

import os
import sys
import json
import threading
import subprocess
import re
from datetime import datetime
from dataclasses import asdict

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸 (Stage3: Text)
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 상품_T2_I0.xlsx -> 상품.xlsx
    예: 상품_T3_I1.xlsx -> 상품.xlsx
    예: 상품_T2_I0(업완).xlsx -> 상품.xlsx
    예: 상품_T2_I0_T3_I1.xlsx -> 상품.xlsx (여러 버전 패턴 제거)
    예: 상품_T2_I5(업완).xlsx -> 상품.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)

    # 1. 버전 패턴 (_T숫자_I숫자(괄호)? 또는 _t숫자_i숫자(괄호)?) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거, 괄호가 붙은 경우도 포함
    while True:
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+(\([^)]+\))?", "", base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등) - 버전 패턴의 괄호는 이미 제거됨
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")

    return base + ext

def get_excel_name_from_path(excel_path: str) -> str:
    """
    엑셀 파일 경로에서 파일명만 추출
    예: C:/Users/.../상품_T1_I0.xlsx -> 상품_T1_I0.xlsx
    """
    if not excel_path:
        return "-"
    return os.path.basename(excel_path)


def get_next_version_path(current_path: str, task_type: str = "text") -> str:
    """
    현재 파일명을 분석해서 다음 단계의 파일명을 생성합니다.
    파일명 형식: 원본명_T{숫자}_I{숫자}.xlsx 또는 원본명_T{숫자}_I{숫자}(괄호).xlsx
    - task_type='text'  → T 버전 +1 (Stage1: T1, Stage2: T2, Stage3: T3, ...)
    - task_type='image' → I 버전 +1
    
    주의: 파일명에 여러 버전 패턴이 있어도 마지막 패턴만 사용합니다.
    예: 상품_T2_I5(업완).xlsx -> 상품_T3_I5(업완).xlsx (text) 또는 상품_T2_I6(업완).xlsx (image)
    """
    dir_name = os.path.dirname(current_path)
    base_name = os.path.basename(current_path)
    name_only, ext = os.path.splitext(base_name)

    # 마지막 _T*_I*(괄호)? 패턴 찾기 (대소문자 구분 없음, 여러 패턴이 있어도 마지막 것만)
    # 괄호가 붙은 경우도 인식 (예: _I5(업완))
    all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)(\([^)]+\))?", name_only, re.IGNORECASE))
    
    if all_matches:
        # 마지막 매칭 사용
        match = all_matches[-1]
        current_t = int(match.group(2))
        current_i = int(match.group(4))
        i_suffix = match.group(5) or ""  # 괄호 부분이 있으면 유지 (예: (업완))
        # 원본명은 마지막 패턴 이전까지
        original_name = name_only[: match.start()].rstrip("_")
    else:
        # 패턴이 없으면 원본명에서 버전 정보 제거 후 사용
        original_name = name_only
        # 기존 버전 패턴 제거 (괄호 포함)
        while True:
            new_name = re.sub(r"_[Tt]\d+_[Ii]\d+(\([^)]+\))?", "", original_name, flags=re.IGNORECASE)
            if new_name == original_name:
                break
            original_name = new_name
        original_name = original_name.rstrip("_")
        current_t = 0
        current_i = 0
        i_suffix = ""

    if task_type == "text":
        new_t = current_t + 1
        new_i = current_i
    elif task_type == "image":
        new_t = current_t
        new_i = current_i + 1
    else:
        return current_path

    # 괄호 부분 유지 (예: (업완))
    new_filename = f"{original_name}_T{new_t}_I{new_i}{i_suffix}{ext}"
    return os.path.join(dir_name, new_filename)


class JobManager:
    DB_FILE = None

    @classmethod
    def find_db_path(cls):
        if cls.DB_FILE and os.path.exists(cls.DB_FILE):
            return cls.DB_FILE

        current_dir = os.path.dirname(os.path.abspath(__file__))
        search_dirs = [
            current_dir,
            os.path.abspath(os.path.join(current_dir, "..")),
            os.path.abspath(os.path.join(current_dir, "..", "..")),
        ]

        for d in search_dirs:
            target = os.path.join(d, "job_history.json")
            if os.path.exists(target):
                cls.DB_FILE = target
                print(f"[JobManager] DB Found: {target}")
                return target

        default_path = os.path.abspath(os.path.join(current_dir, "..", "job_history.json"))
        cls.DB_FILE = default_path
        return default_path

    @classmethod
    def load_jobs(cls):
        db_path = cls.find_db_path()
        if not os.path.exists(db_path):
            return {}
        try:
            with open(db_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None):
        """메인 런처 현황판 상태 업데이트 (Stage1/2/3 공용)."""
        db_path = cls.find_db_path()
        data = cls.load_jobs()
        now = datetime.now().strftime("%m-%d %H:%M")

        if filename not in data:
            data[filename] = {
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "text_status": "대기",
                "text_time": "-",
                "image_status": "대기",
                "image_time": "-",
                "memo": "",
            }

        if text_msg:
            data[filename]["text_status"] = text_msg
            data[filename]["text_time"] = now
        if img_msg:
            data[filename]["image_status"] = img_msg
            data[filename]["image_time"] = now

        data[filename]["last_update"] = now

        try:
            with open(db_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[JobManager Error] {e}")


def safe_save_excel(df: pd.DataFrame, path: str) -> bool:
    """엑셀 파일이 열려 있어 저장이 안 될 때 재시도를 유도하는 함수"""
    while True:
        try:
            df.to_excel(path, index=False)
            return True
        except PermissionError:
            if not messagebox.askretrycancel(
                "저장 실패",
                f"엑셀 파일이 열려있습니다!\n[{os.path.basename(path)}]\n\n"
                "파일을 닫고 '다시 시도'를 눌러주세요.",
            ):
                return False
        except Exception as e:
            messagebox.showerror("오류", f"저장 중 알 수 없는 오류: {e}")
            return False


# [필수 의존성] stage3_core_Casche.py / stage3_run_history.py
# 캐싱 최적화 버전 사용 (stage3_core_Casche.py)
try:
    from stage3_core_Casche import (
        safe_str,
        Stage3Settings,
        Stage3Request,
        build_stage3_request_from_row,  # Row -> Request 객체(프롬프트+설정) 변환 (캐싱 최적화)
    )
    CACHE_MODE_CORE = True
    from stage3_run_history import append_run_history
    _HISTORY_AVAILABLE = True
except ImportError:
    # 캐싱 버전이 없으면 일반 버전 사용
    try:
        from stage3_core import (
            safe_str,
            Stage3Settings,
            Stage3Request,
            build_stage3_request_from_row,
        )
        CACHE_MODE_CORE = False
        from stage3_run_history import append_run_history
        _HISTORY_AVAILABLE = True
    except ImportError as e:
        # 의존성 파일 부재 시 비상용 더미
        CACHE_MODE_CORE = False
        _HISTORY_AVAILABLE = False
        MODEL_PRICING_USD_PER_MTOK = {}
        def safe_str(x): return str(x) if x is not None else ""
        def load_api_key_from_file(x): return ""
        def save_api_key_to_file(x, y): pass
        def append_run_history(*args, **kwargs): 
            # 더미 함수: 히스토리 기록 실패 시 조용히 무시
            pass

# === 기본 설정 ===
API_KEY_FILE = ".openai_api_key_stage3_batch"
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage3_batch_jobs.json")

# Stage 3용 Batch 모델/가격 (gpt-5 계열만 사용)
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5": {
        "input": 1.25,
        "output": 10.0,
    },
    "gpt-5-mini": {
        "input": 0.25,
        "output": 2.00,
    },
    "gpt-5-nano": {
        "input": 0.05,
        "output": 0.40,
    },
}

# === API Key 유틸리티 함수 ===
def load_api_key_from_file(path: str = API_KEY_FILE) -> str:
    """텍스트 파일에서 API 키를 읽는다."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""

def save_api_key_to_file(key: str, path: str = API_KEY_FILE) -> None:
    """API 키를 텍스트 파일에 저장한다."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")

# --- UI 색상 팔레트 ---
COLOR_BG = "#F8F9FA"
COLOR_WHITE = "#FFFFFF"
COLOR_PRIMARY = "#4A90E2"
COLOR_PRIMARY_HOVER = "#357ABD"
COLOR_SUCCESS = "#28A745"
COLOR_SUCCESS_HOVER = "#218838"
COLOR_DANGER = "#DC3545"
COLOR_DANGER_HOVER = "#C82333"
COLOR_TEXT = "#333333"
COLOR_HEADER = "#E9ECEF"

def get_seoul_now():
    try:
        from pytz import timezone
        return datetime.now(timezone("Asia/Seoul"))
    except:
        return datetime.now()

# ========================================================
# 배치 잡 관리 (JSON DB)
# ========================================================
def load_batch_jobs():
    if not os.path.exists(BATCH_JOBS_FILE):
        return []
    try:
        with open(BATCH_JOBS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def save_batch_jobs(jobs):
    try:
        with open(BATCH_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[Error] 잡 저장 실패: {e}")

def upsert_batch_job(batch_id, **kwargs):
    jobs = load_batch_jobs()
    found = False
    now_str = datetime.now().isoformat()
    
    for j in jobs:
        if j["batch_id"] == batch_id:
            if kwargs.get("status") == "completed" and j.get("status") != "completed":
                if "completed_at" not in kwargs:
                    j["completed_at"] = now_str
            j.update(kwargs)
            j["updated_at"] = now_str
            found = True
            break
            
    if not found:
        new_job = {
            "batch_id": batch_id,
            "created_at": now_str,
            "updated_at": now_str,
            "completed_at": "",
            "archived": False,
            **kwargs
        }
        jobs.insert(0, new_job)
    save_batch_jobs(jobs)

def archive_batch_job(batch_ids, archive=True):
    if isinstance(batch_ids, str): batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    for j in jobs:
        if j["batch_id"] in batch_ids:
            j["archived"] = archive
    save_batch_jobs(jobs)

def hard_delete_batch_job(batch_ids):
    if isinstance(batch_ids, str): batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    jobs = [j for j in jobs if j["batch_id"] not in batch_ids]
    save_batch_jobs(jobs)

# ========================================================
# GUI Class
# ========================================================
class Stage3BatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 3: Batch API Manager (Production Generator) 🚀 캐싱 최적화 버전")
        self.geometry("1250x1000") # 높이 약간 증가하여 로그 창 공간 확보
        
        self.api_key_var = tk.StringVar()
        
        # 파일 변수
        self.src_file_var = tk.StringVar()
        self.skip_exist_var = tk.BooleanVar(value=True)
        
        # [중요] Stage 3 전용 설정 변수
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="medium")
        self.market_var = tk.StringVar(value="네이버 50자")
        self.max_len_var = tk.IntVar(value=50)
        self.num_cand_var = tk.IntVar(value=10)
        self.naming_strategy_var = tk.StringVar(value="통합형")
        
        # 탭 3 변수
        self.batch_id_var = tk.StringVar()
        
        self._configure_styles()
        self._init_ui()
        self._load_key()

    def _configure_styles(self):
        style = ttk.Style()
        try: style.theme_use('clam')
        except: pass
        
        self.configure(background=COLOR_BG)
        
        style.configure("TFrame", background=COLOR_BG)
        style.configure("TLabel", background=COLOR_BG, foreground=COLOR_TEXT, font=("맑은 고딕", 10))
        style.configure("Header.TLabel", font=("맑은 고딕", 11, "bold"), foreground="#444")
        
        style.configure("TLabelframe", background=COLOR_BG, bordercolor="#D0D7DE")
        style.configure("TLabelframe.Label", background=COLOR_BG, foreground="#0056b3", font=("맑은 고딕", 10, "bold"))

        style.configure("TNotebook", background=COLOR_BG, borderwidth=0)
        style.configure("TNotebook.Tab", background="#E1E4E8", padding=[12, 5], font=("맑은 고딕", 10))
        style.map("TNotebook.Tab", background=[("selected", COLOR_WHITE)], foreground=[("selected", COLOR_PRIMARY)])
        
        style.configure("Treeview", background=COLOR_WHITE, fieldbackground=COLOR_WHITE, font=("맑은 고딕", 9), rowheight=28)
        style.configure("Treeview.Heading", background=COLOR_HEADER, foreground="#333", font=("맑은 고딕", 9, "bold"))
        style.map("Treeview", background=[('selected', '#CCE5FF')], foreground=[('selected', 'black')])

        style.configure("TButton", font=("맑은 고딕", 9), padding=5, borderwidth=1)
        style.configure("Primary.TButton", background=COLOR_PRIMARY, foreground="white", bordercolor=COLOR_PRIMARY)
        style.map("Primary.TButton", background=[("active", COLOR_PRIMARY_HOVER)])
        style.configure("Success.TButton", background=COLOR_SUCCESS, foreground="white", bordercolor=COLOR_SUCCESS)
        style.map("Success.TButton", background=[("active", COLOR_SUCCESS_HOVER)])
        style.configure("Danger.TButton", background=COLOR_DANGER, foreground="white", bordercolor=COLOR_DANGER)
        style.map("Danger.TButton", background=[("active", COLOR_DANGER_HOVER)])

    def _init_ui(self):
        main_container = ttk.Frame(self, padding=15)
        main_container.pack(fill='both', expand=True)
        
        # Grid를 사용하여 비율 제어 (로그 창이 더 큰 공간 차지)
        main_container.grid_rowconfigure(1, weight=1)  # 메인 탭 행 (작은 비중)
        main_container.grid_rowconfigure(2, weight=2)  # 로그 창 행 (더 큰 비중, 2배)
        main_container.grid_columnconfigure(0, weight=1)

        # 1. 상단 API Key
        f_top = ttk.LabelFrame(main_container, text="🔑 API 설정", padding=10)
        f_top.grid(row=0, column=0, sticky='ew', pady=(0, 10))
        ttk.Label(f_top, text="Batch API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        entry_key = ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10))
        entry_key.pack(side='left', padx=10)
        ttk.Button(f_top, text="저장", command=self._save_key, style="Primary.TButton").pack(side='left')

        # 2. 메인 탭 (비율 조정: 작은 공간)
        self.main_tabs = ttk.Notebook(main_container)
        self.main_tabs.grid(row=1, column=0, sticky='nsew', pady=5)  # grid 사용, weight=1
        
        self.tab_create = ttk.Frame(self.main_tabs)
        self.tab_manage = ttk.Frame(self.main_tabs) 
        self.tab_merge = ttk.Frame(self.main_tabs)
        
        self.main_tabs.add(self.tab_create, text=" 1. 배치 생성 & 업로드 ")
        self.main_tabs.add(self.tab_manage, text=" 2. 배치 관리 (목록/병합/리포트) ")
        self.main_tabs.add(self.tab_merge, text=" 3. 개별 병합 (수동) ")
        
        self._init_tab_create()
        self._init_tab_manage()
        self._init_tab_merge()
        
        # 3. 로그 (더 큰 공간 할당: weight=2)
        f_log = ttk.LabelFrame(main_container, text="📋 시스템 로그", padding=10)
        f_log.grid(row=2, column=0, sticky='nsew', pady=(10, 0))  # grid 사용, weight=2로 더 큰 공간
        self.log_widget = ScrolledText(f_log, height=25, state='disabled', font=("Consolas", 9), bg="#F1F3F5")  # height를 15에서 25로 증가
        self.log_widget.pack(fill='both', expand=True)

    def _load_key(self):
        loaded = load_api_key_from_file(API_KEY_FILE)
        if loaded: self.api_key_var.set(loaded)

    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k, API_KEY_FILE)
            messagebox.showinfo("저장", "API Key 저장 완료")

    def append_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')

    # ----------------------------------------------------
    # Tab 1: Create (생성)
    # ----------------------------------------------------
    def _init_tab_create(self):
        container = ttk.Frame(self.tab_create, padding=20)
        container.pack(fill='both', expand=True)
        
        # Step 1: 파일
        f_file = ttk.LabelFrame(container, text="1. 작업 대상 파일 (ST2_JSON 포함)", padding=15)
        f_file.pack(fill='x', pady=(0, 15))
        ttk.Entry(f_file, textvariable=self.src_file_var, font=("맑은 고딕", 10)).pack(side='left', fill='x', expand=True)
        ttk.Button(f_file, text="📂 파일 찾기", command=self._select_src_file).pack(side='right', padx=5)
        
        # Step 2: Stage 3 옵션
        f_opt = ttk.LabelFrame(container, text="2. Stage 3 생성 옵션", padding=15)
        f_opt.pack(fill='x', pady=5)

        # 모델 & Effort
        fr1 = ttk.Frame(f_opt)
        fr1.pack(fill='x', pady=5)
        ttk.Label(fr1, text="모델 (Model):", width=12).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys()) if MODEL_PRICING_USD_PER_MTOK else ["gpt-5-mini", "gpt-5", "gpt-5-nano"]
        cb_model = ttk.Combobox(fr1, textvariable=self.model_var, values=models, state="readonly", width=20)
        cb_model.pack(side='left', padx=5)
        
        ttk.Label(fr1, text="추론 강도:", width=10).pack(side='left', padx=(20, 5))
        ttk.Combobox(fr1, textvariable=self.effort_var, values=["none", "low", "medium", "high"], state="readonly", width=12).pack(side='left', padx=5)
        
        # 마켓 설정
        fr2 = ttk.Frame(f_opt)
        fr2.pack(fill='x', pady=5)
        ttk.Label(fr2, text="타겟 마켓:", width=12).pack(side='left')
        markets = ["네이버 50자", "쿠팡 100자", "지마켓/옥션 45자", "기타"]
        cb_mk = ttk.Combobox(fr2, textvariable=self.market_var, values=markets, state="readonly", width=18)
        cb_mk.pack(side='left', padx=5)
        cb_mk.bind("<<ComboboxSelected>>", self._on_market_change)
        
        ttk.Label(fr2, text="최대 글자:", width=10).pack(side='left', padx=(20, 5))
        ttk.Spinbox(fr2, from_=10, to=200, textvariable=self.max_len_var, width=10).pack(side='left', padx=5)

        # 출력 개수 & 전략
        fr3 = ttk.Frame(f_opt)
        fr3.pack(fill='x', pady=5)
        ttk.Label(fr3, text="출력 개수:", width=12).pack(side='left')
        ttk.Spinbox(fr3, from_=1, to=30, textvariable=self.num_cand_var, width=10).pack(side='left', padx=5)

        ttk.Label(fr3, text="명명 전략:", width=10).pack(side='left', padx=(25, 5))
        ttk.Combobox(fr3, textvariable=self.naming_strategy_var, values=["통합형", "옵션포함형"], state="readonly", width=12).pack(side='left', padx=5)
        
        # 체크박스
        f_row_chk = ttk.Frame(f_opt)
        f_row_chk.pack(fill='x', pady=10)
        ttk.Checkbutton(f_row_chk, text=" 이미 ST3_결과가 있는 행 건너뛰기", variable=self.skip_exist_var).pack(side='left')
        
        # Step 3: 실행
        f_step3 = ttk.LabelFrame(container, text="3. 실행", padding=15)
        f_step3.pack(fill='x', pady=15)

        btn = ttk.Button(f_step3, text="🚀 JSONL 생성 및 배치 업로드 (Start Batch)", command=self._start_create_batch, style="Success.TButton")
        btn.pack(fill='x', ipady=8)
        ttk.Label(container, text="※ 배치 API는 결과 수신까지 최대 24시간이 소요됩니다. (비용 50% 절감)", foreground="#666").pack()

    def _on_market_change(self, event=None):
        val = self.market_var.get()
        if "네이버" in val: self.max_len_var.set(50)
        elif "쿠팡" in val: self.max_len_var.set(100)
        elif "지마켓" in val: self.max_len_var.set(45)

    def _select_src_file(self):
        p = filedialog.askopenfilename(
            title="Stage3 엑셀 선택 (T2 버전만 가능)",
            filetypes=[("Excel", "*.xlsx;*.xls")]
        )
        if p:
            # T2 포함 여부 검증
            base_name = os.path.splitext(os.path.basename(p))[0]
            if not re.search(r"_T2_[Ii]\d+", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 T2 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {os.path.basename(p)}\n"
                    f"파일명에 '_T2_I*' 패턴이 포함되어 있어야 합니다."
                )
                return
            self.src_file_var.set(p)

    def _start_create_batch(self):
        if not self.api_key_var.get():
            messagebox.showwarning("오류", "API Key가 필요합니다.")
            return
        if not self.src_file_var.get():
            messagebox.showwarning("오류", "파일을 선택해주세요.")
            return
        
        # T2 포함 여부 검증
        src = self.src_file_var.get().strip()
        base_name = os.path.splitext(os.path.basename(src))[0]
        if not re.search(r"_T2_[Ii]\d+", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 T2 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {os.path.basename(src)}\n"
                f"파일명에 '_T2_I*' 패턴이 포함되어 있어야 합니다."
            )
            return
        
        t = threading.Thread(target=self._run_create_batch)
        t.daemon = True
        t.start()

    def _run_create_batch(self):
        key = self.api_key_var.get().strip()
        src = self.src_file_var.get().strip()
        jsonl_path = None  # 에러 처리에서 사용하기 위해 미리 선언
        
        # Stage3Settings는 model_name과 reasoning_effort를 포함하지 않으므로 별도 관리
        model_name = self.model_var.get().strip() or "gpt-5-mini"
        reasoning_effort = self.effort_var.get().strip() or "medium"
        
        settings = Stage3Settings(
            market=self.market_var.get(),
            max_len=self.max_len_var.get(),
            num_candidates=self.num_cand_var.get(),
            naming_strategy=self.naming_strategy_var.get()
        )
        
        try:
            client = OpenAI(api_key=key)
            self.append_log(f"엑셀 로드 중... {os.path.basename(src)}")
            df = pd.read_excel(src)
            
            if "ST2_JSON" not in df.columns:
                raise ValueError("필수 컬럼(ST2_JSON)이 누락되었습니다. Stage 2를 먼저 완료하세요.")
            
            self.append_log(f"설정: {settings.market} / {settings.max_len}자 / {model_name}")
            
            # 캐싱 모드 확인 및 로그
            if CACHE_MODE_CORE:
                self.append_log(f"[INFO] 🚀 프롬프트 캐싱 최적화 모드 활성화 (stage3_core_Casche.py)")
                # System prompt 토큰 수 확인 (선택적)
                try:
                    import tiktoken
                    from stage3_core_Casche import STAGE3_SYSTEM_PROMPT
                    enc = tiktoken.encoding_for_model('gpt-4o')
                    token_count = len(enc.encode(STAGE3_SYSTEM_PROMPT))
                    status = "✅ 충분" if token_count > 1024 else "⚠️ 부족"
                    self.append_log(f"[INFO] System prompt 토큰 수: {token_count} 토큰 ({status}, 기준: 1024 토큰)")
                except ImportError:
                    # tiktoken이 설치되지 않은 경우 조용히 건너뜀 (선택적 기능)
                    pass
                except Exception as e:
                    # 기타 오류는 디버깅용으로만 로그 출력
                    self.append_log(f"[DEBUG] System prompt 토큰 수 확인 실패: {e}")
            else:
                self.append_log(f"[INFO] ⚠️ 일반 모드 (stage3_core.py) - 캐싱 최적화 미적용")

            # 먼저 전체 대상 요청 수를 계산 (버킷 수 결정용)
            target_rows = 0
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get() and "ST3_결과상품명" in df.columns:
                    val = str(row.get("ST3_결과상품명", "")).strip()
                    if val and val != "nan":
                        continue
                # ST2_JSON 확인
                st2_json = safe_str(row.get("ST2_JSON", ""))
                if not st2_json or st2_json.strip().lower() in ("", "nan", "none", "null"):
                    continue
                target_rows += 1

            # 버킷 수를 미리 계산 (모든 요청에 동일하게 적용)
            if CACHE_MODE_CORE and target_rows > 0:
                # [프롬프트 캐싱 최적화 전략 - 수정됨]
                # 
                # [문제 분석 (실제 테스트 결과)]
                # - 버킷 분산 시나리오: 486개 요청을 49개 버킷(b00~b48)으로 분산
                # - 결과: 캐시 히트율 10.08% (49건/486건), 대부분의 버킷은 히트 0건
                # - 원인: prompt_cache_key가 분산되면 캐시 라우팅/저장 풀도 분산되어
                #   Batch 처리 특성상(병렬/분산 실행) 같은 키끼리 연속 재사용이 잘 안 걸림
                #
                # [해결책: 키 고정]
                # - prompt_cache_key를 하나로 고정: "stage3_v1" (b00~b48로 쪼개지 않기)
                # - 효과: 모든 요청이 같은 캐시 풀을 공유하여 히트율 대폭 향상 예상
                # - Batch API는 24시간에 걸쳐 처리되므로 overflow 우려는 낮음
                # - 실제 테스트에서 같은 키를 10~17번씩 쓰는 경우도 있었지만 히트율이 낮았던 이유는
                #   키 분산 + 배치 내부 스케줄링/병렬 처리 때문
                #
                # [참고: OpenAI 공식 문서]
                # - 일반 API(동기 요청): 같은 prefix + prompt_cache_key 조합이 분당 약 15건을 초과하면
                #   일부가 추가 머신으로 overflow되어 캐시 효율이 떨어질 수 있음
                #   (참고: https://platform.openai.com/docs/guides/prompt-caching)
                # - Batch API: 공식 문서에 prompt_cache_key 버킷 분배 기준이 명시되어 있지 않음
                #   → 실제 테스트 결과를 바탕으로 키 고정 전략 채택
                #
                # [구현]
                # - PROMPT_CACHE_BUCKETS = 1로 고정하여 모든 요청이 동일한 prompt_cache_key 사용
                # - 가능하면 같은 키끼리 요청을 묶어서(연속되게) 배치로 넣는 것이 이상적이지만,
                #   Batch API는 자체적으로 최적화하므로 단순히 키를 고정하는 것만으로도 효과적
                PROMPT_CACHE_BUCKETS = 1
                
                self.append_log(f"[INFO] 프롬프트 캐싱: 키 고정 전략 사용 (모든 요청이 'stage3_v1' 키 공유)")
                self.append_log(f"[INFO] 예상 요청 수: {target_rows}개, 캐시 히트율 향상 예상")
            else:
                PROMPT_CACHE_BUCKETS = 1

            jsonl_lines = []
            skipped_cnt = 0
            seen_custom_ids = set()
            duplicate_count = 0
            
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get() and "ST3_결과상품명" in df.columns:
                    val = str(row.get("ST3_결과상품명", "")).strip()
                    if val and val != "nan":
                        continue
                
                try:
                    req = build_stage3_request_from_row(row, settings)
                except Exception:
                    skipped_cnt += 1
                    continue

                # 캐싱 최적화: system/user 프롬프트 분리
                if CACHE_MODE_CORE:
                    system_prompt = safe_str(getattr(req, "system_prompt", ""))
                    user_prompt = safe_str(getattr(req, "user_prompt", ""))
                    
                    if not system_prompt or not user_prompt:
                        skipped_cnt += 1
                        continue
                    
                    # System 메시지 (텍스트만, 정적)
                    system_content = [{"type": "input_text", "text": system_prompt}]
                    
                    # User 메시지 (텍스트만, 동적)
                    user_content = [{"type": "input_text", "text": user_prompt}]
                    
                    body = {
                        "model": model_name,
                        "input": [
                            {
                                "role": "system",
                                "content": system_content,
                            },
                            {
                                "role": "user",
                                "content": user_content,
                            }
                        ],
                    }
                else:
                    # 일반 모드: 기존 방식 유지
                    prompt = safe_str(getattr(req, "prompt", ""))
                    if not prompt:
                        skipped_cnt += 1
                        continue
                    
                    body = {
                        "model": model_name,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                
                # reasoning.effort (Responses API)
                is_reasoning = any(x in model_name for x in ["gpt-5", "o1", "o3"])
                if is_reasoning and reasoning_effort != "none":
                    if CACHE_MODE_CORE:
                        body["reasoning"] = {"effort": reasoning_effort}
                    else:
                        body["reasoning_effort"] = reasoning_effort
                elif not is_reasoning:
                    if not CACHE_MODE_CORE:
                        body["temperature"] = 0.7

                custom_id = f"row-{idx}"
                
                # 중복 custom_id 체크
                if custom_id in seen_custom_ids:
                    duplicate_count += 1
                    continue
                seen_custom_ids.add(custom_id)

                # Prompt Caching 최적화 (캐싱 모드일 때만)
                if CACHE_MODE_CORE:
                    # prompt_cache_key: 키 고정 전략 (모든 요청이 동일한 키 사용)
                    # 버킷 분산 대신 키를 하나로 고정하여 캐시 히트율 최대화
                    body["prompt_cache_key"] = "stage3_v1"
                    
                    # prompt_cache_retention: 모델이 지원하는 경우에만 추가
                    # Extended retention 지원 모델: gpt-5.1, gpt-5.1-codex, gpt-5.1-codex-mini, gpt-5.1-chat-latest, gpt-5, gpt-5-codex, gpt-4.1
                    # gpt-5-mini, gpt-5-nano는 prompt_cache_retention 파라미터를 지원하지 않음
                    if model_name in ["gpt-5.1", "gpt-5.1-codex", "gpt-5.1-codex-mini", "gpt-5.1-chat-latest", "gpt-5", "gpt-5-codex", "gpt-4.1"]:
                        body["prompt_cache_retention"] = "extended"  # 24시간 retention
                    elif model_name not in ["gpt-5-mini", "gpt-5-nano"]:
                        # 기타 모델은 in-memory 사용 (5~10분 inactivity, 최대 1시간)
                        body["prompt_cache_retention"] = "in_memory"
                    
                    # Responses API 사용 (system/user role)
                    url = "/v1/responses"
                else:
                    # 일반 모드: Chat Completions API 사용
                    url = "/v1/chat/completions"

                request_obj = {
                    "custom_id": custom_id,
                    "method": "POST",
                    "url": url,
                    "body": body
                }
                
                jsonl_lines.append(json.dumps(request_obj, ensure_ascii=False))
            
            if duplicate_count > 0:
                self.append_log(f"[WARN] ⚠️ 중복 요청 {duplicate_count}개가 감지되어 제외되었습니다.")
            
            if not jsonl_lines:
                self.append_log("생성할 요청 없음.")
                return

            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage3_batch_input.jsonl"
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.write("\n".join(jsonl_lines))
            
            self.append_log(f"JSONL 생성 완료: {len(jsonl_lines)}건 (스킵 {skipped_cnt}건)")
            self.append_log(f"[INFO] JSONL 파일 저장 위치: {jsonl_path}")
            
            # 파일 크기 및 요청 수 확인
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            info = {
                'num_requests': len(jsonl_lines),
                'file_size_mb': jsonl_size_mb
            }
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 요청 수: {info['num_requests']}개")
            
            # 청크 분할 기준 (OpenAI Batch API 제한: 200MB)
            # 실제 분할은 파일 크기 기준으로만 수행되므로, 파일 크기만 체크
            MAX_FILE_SIZE_MB = 190
            
            # 예상 청크 수 계산 (파일 크기 기준)
            estimated_chunks = max(1, int(jsonl_size_mb / MAX_FILE_SIZE_MB) + 1) if jsonl_size_mb > MAX_FILE_SIZE_MB else 1
            
            if jsonl_size_mb > MAX_FILE_SIZE_MB:
                self.append_log(f"[INFO] 파일 크기 ({jsonl_size_mb:.2f}MB > {MAX_FILE_SIZE_MB}MB)로 인해 분할 처리합니다... (OpenAI 제한: 200MB)")
                # 타임아웃 설정: 대용량 파일 업로드를 위해 10분으로 설정
                import httpx
                timeout = httpx.Timeout(600.0, connect=60.0)  # 10분 타임아웃
                client_with_timeout = OpenAI(api_key=key, timeout=timeout, max_retries=3)
                batch_ids = self._create_batch_chunks(
                    client=client_with_timeout,
                    jsonl_path=jsonl_path,
                    excel_path=src,
                    model_name=model_name,
                    effort=reasoning_effort,
                    settings=settings,
                    max_size_mb=MAX_FILE_SIZE_MB,
                    max_requests=999999,  # 요청 수 제한은 실제로 사용하지 않으므로 큰 값으로 설정
                )
                self.append_log(f"✅ 총 {len(batch_ids)}개의 배치가 생성되었습니다: {', '.join(batch_ids)}")
                messagebox.showinfo("성공", f"{len(batch_ids)}개의 배치가 생성되었습니다:\n{', '.join(batch_ids)}")
            else:
                # 기존 방식: 단일 배치 생성
                # 타임아웃 설정: 대용량 파일 업로드를 위해 10분으로 설정
                import httpx
                timeout = httpx.Timeout(600.0, connect=60.0)  # 10분 타임아웃
                client = OpenAI(api_key=key, timeout=timeout, max_retries=3)
                batch = self._create_batch_from_jsonl(
                    client=client,
                    jsonl_path=jsonl_path,
                    excel_path=src,
                    model_name=model_name,
                    reasoning_effort=reasoning_effort,
                    settings=settings,
                )

                batch_id = batch.id
                self.append_log(f"✅ 배치 시작! ID: {batch_id}, status={batch.status}")

                # 3) 작업 이력 기록
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=src,
                    jsonl_path=jsonl_path,
                    model=model_name,
                    effort=reasoning_effort,
                    status=batch.status,
                    output_file_id=None,
                    market=settings.market,
                    strategy=settings.naming_strategy
                )

                # 메인 런처 현황판에 Stage3(Text) 작업 시작 상태 기록: T3 (진행중) (img 상태는 변경하지 않음)
                try:
                    root_name = get_root_filename(src)
                    JobManager.update_status(root_name, text_msg="T3 (진행중)")
                    self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T3 (진행중)")
                except Exception:
                    # 런처나 job_history.json 이 없을 수 있으므로 조용히 무시
                    pass
                messagebox.showinfo("성공", f"배치 시작됨: {batch_id}")
            
            self._load_jobs_all()
            self._load_archive_list()

        except Exception as e:
            error_str = str(e)
            error_lower = error_str.lower()
            
            # 결제 한도 초과 에러 감지
            if "billing_hard_limit_reached" in error_lower or "billing" in error_lower and "limit" in error_lower:
                self.append_log(f"❌ [결제 한도 초과] OpenAI 계정의 결제 한도에 도달했습니다.")
                if jsonl_path and os.path.exists(jsonl_path):
                    self.append_log(f"   💾 생성된 JSONL 파일: {jsonl_path}")
                    self.append_log(f"      → 결제 한도 해결 후 이 파일을 재사용할 수 있습니다.")
                self.append_log(f"   → 해결 방법:")
                self.append_log(f"      1. OpenAI 대시보드(https://platform.openai.com/account/billing)에서 결제 한도 확인")
                self.append_log(f"      2. 결제 한도 증가 또는 결제 정보 업데이트")
                self.append_log(f"      3. 또는 더 작은 배치로 분할하여 처리")
                self.append_log(f"   원본 에러: {error_str}")
                
                msg = "OpenAI 계정의 결제 한도에 도달했습니다.\n\n"
                if jsonl_path and os.path.exists(jsonl_path):
                    msg += f"✅ JSONL 파일은 이미 생성되었습니다:\n{os.path.basename(jsonl_path)}\n"
                    msg += "   결제 한도 해결 후 재사용 가능합니다.\n\n"
                msg += "해결 방법:\n"
                msg += "1. OpenAI 대시보드에서 결제 한도 확인\n"
                msg += "   (https://platform.openai.com/account/billing)\n"
                msg += "2. 결제 한도 증가 또는 결제 정보 업데이트\n"
                msg += "3. 더 작은 배치로 분할하여 처리\n\n"
                msg += f"상세 에러: {error_str[:200]}"
                
                messagebox.showerror("결제 한도 초과", msg)
            else:
                self.append_log(f"❌ 에러: {error_str}")
                if jsonl_path and os.path.exists(jsonl_path):
                    self.append_log(f"   💾 생성된 JSONL 파일: {jsonl_path}")
                messagebox.showerror("에러", error_str)
    
    def _create_batch_from_jsonl(self, client, jsonl_path, excel_path, model_name, reasoning_effort, settings):
        """JSONL 파일을 업로드하고 배치를 생성하는 헬퍼 함수"""
        if not os.path.exists(jsonl_path):
            raise FileNotFoundError(f"입력 JSONL 파일을 찾을 수 없습니다: {jsonl_path}")

        with open(jsonl_path, "rb") as f:
            # 타임아웃 설정: 대용량 파일 업로드를 위해 10분으로 설정
            up_file = client.files.create(file=f, purpose="batch", timeout=600)

        # 캐싱 모드에 따라 endpoint 결정
        endpoint = "/v1/responses" if CACHE_MODE_CORE else "/v1/chat/completions"

        batch = client.batches.create(
            input_file_id=up_file.id,
            endpoint=endpoint,
            completion_window="24h"
        )
        return batch
    
    def _create_batch_chunks(self, client, jsonl_path, excel_path, model_name, effort, settings, max_size_mb=180, max_requests=999999):
        """
        큰 JSONL 파일을 청크로 분할하여 여러 배치를 생성합니다.
        같은 그룹의 배치들은 batch_group_id로 관리됩니다.
        """
        import uuid
        
        # 배치 그룹 ID 생성 (같은 엑셀에서 분할된 배치들을 묶음)
        batch_group_id = f"group_{uuid.uuid4().hex[:8]}"
        
        # JSONL 파일 읽기
        requests = []
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    requests.append(json.loads(line))
        
        total_requests = len(requests)
        # 예상 청크 수 계산 (용량 기준만 사용, 요청 수는 용량 제한 내에서 가능한 만큼 포함)
        original_file_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
        estimated_total_chunks = max(1, int(original_file_size_mb / max_size_mb) + 1)
        
        # 실제로 분할이 필요한지 확인 (파일 크기가 제한보다 작으면 1개 청크로 처리)
        if original_file_size_mb <= max_size_mb:
            # 파일 크기가 제한보다 작으면 분할 불필요, 단일 배치로 처리
            self.append_log(f"[INFO] 파일 크기 ({original_file_size_mb:.2f}MB ≤ {max_size_mb}MB)로 단일 배치로 처리합니다. (요청 수: {total_requests}개)")
            estimated_total_chunks = 1
        else:
            self.append_log(f"[INFO] 총 {total_requests}개 요청을 분할합니다... (예상: 약 {estimated_total_chunks}개 청크, 그룹 ID: {batch_group_id})")
        
        # base 변수를 미리 정의 (실패 정보 저장 시 사용)
        base, ext = os.path.splitext(jsonl_path)
        
        batch_ids = []
        chunk_num = 0
        chunk_files_created = []  # 생성된 청크 파일 목록 (정리용)
        failed_chunk_files = []  # 실패한 청크 파일 목록 (재시도용)
        
        i = 0
        while i < total_requests:
            chunk_num += 1
            chunk_requests = []
            chunk_size_bytes = 0  # 바이트 단위로 정확히 계산
            
            # 청크 생성 (용량 기준 우선, 요청 수는 용량 제한 내에서 가능한 만큼 포함)
            while i < total_requests:
                req_json = json.dumps(requests[i], ensure_ascii=False)
                req_size_bytes = len(req_json.encode('utf-8')) + 1  # +1 for newline
                
                # 다음 요청을 추가하면 제한을 초과하는지 확인 (안전 마진 5% 포함)
                if chunk_size_bytes + req_size_bytes > max_size_mb * 1024 * 1024 * 0.95:
                    break
                
                chunk_requests.append(requests[i])
                chunk_size_bytes += req_size_bytes
                i += 1
            
            if not chunk_requests:
                break
            
            # 청크 JSONL 파일 생성
            chunk_jsonl_path = f"{base}_chunk{chunk_num:03d}{ext}"
            chunk_files_created.append(chunk_jsonl_path)
            
            with open(chunk_jsonl_path, "w", encoding="utf-8") as f:
                for req in chunk_requests:
                    f.write(json.dumps(req, ensure_ascii=False) + "\n")
            
            chunk_size_mb = os.path.getsize(chunk_jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] 청크 {chunk_num}: {len(chunk_requests)}개 요청, {chunk_size_mb:.2f} MB")
            
            # 배치 생성 (재시도 로직 포함)
            max_retries = 3
            retry_count = 0
            batch_created = False
            
            while retry_count < max_retries and not batch_created:
                try:
                    self.append_log(f"[INFO] 청크 {chunk_num} 배치 생성 시도 중... (시도 {retry_count + 1}/{max_retries})")
                    batch = self._create_batch_from_jsonl(
                        client=client,
                        jsonl_path=chunk_jsonl_path,
                        excel_path=excel_path,
                        model_name=model_name,
                        reasoning_effort=effort,
                        settings=settings,
                    )
                    
                    batch_id = batch.id
                    batch_ids.append(batch_id)
                    self.append_log(f"✅ 청크 {chunk_num} 배치 생성 완료: {batch_id}")
                    
                    batch_created = True
                    
                    # 작업 이력 기록 (그룹 정보 포함)
                    upsert_batch_job(
                        batch_id=batch_id,
                        src_excel=excel_path,
                        jsonl_path=chunk_jsonl_path,
                        model=model_name,
                        effort=effort,
                        status=batch.status,
                        output_file_id=None,
                        batch_group_id=batch_group_id,  # 그룹 ID 추가
                        chunk_index=chunk_num,  # 청크 번호
                        total_chunks=chunk_num,  # 현재까지 생성된 청크 수 (나중에 업데이트됨)
                        market=settings.market,
                        strategy=settings.naming_strategy
                    )
                except Exception as e:
                    error_str = str(e).lower()
                    is_token_limit_error = "enqueued token limit" in error_str or "token limit reached" in error_str
                    
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = retry_count * 5  # 5초, 10초, 15초 대기
                        self.append_log(f"⚠️ 청크 {chunk_num} 배치 생성 실패 (재시도 {retry_count}/{max_retries}): {e}")
                        if is_token_limit_error:
                            self.append_log(f"[WARN] 토큰 제한 오류 감지. 더 긴 대기 시간이 필요할 수 있습니다.")
                            wait_time = max(wait_time, 30)  # 토큰 제한 오류는 최소 30초 대기
                        self.append_log(f"[INFO] {wait_time}초 후 재시도합니다...")
                        import time
                        time.sleep(wait_time)
                    else:
                        self.append_log(f"❌ 청크 {chunk_num} 배치 생성 최종 실패: {e}")
                        if is_token_limit_error:
                            self.append_log(f"[INFO] 토큰 제한 오류입니다. 일부 배치가 완료된 후 재시도하세요.")
                            self.append_log(f"[INFO] 실패한 청크 파일: {chunk_jsonl_path}")
                            self.append_log(f"[INFO] 나중에 '3. 개별 병합 (수동)' 탭에서 이 파일을 사용하여 재시도할 수 있습니다.")
                        import traceback
                        self.append_log(traceback.format_exc())
                        # 실패한 청크 파일은 유지 (수동 재시도 가능하도록)
                        failed_chunk_files.append({
                            "chunk_num": chunk_num,
                            "chunk_file": chunk_jsonl_path,
                            "error": str(e),
                            "is_token_limit": is_token_limit_error,
                            "excel_path": excel_path,
                            "model_name": model_name,
                            "effort": effort,
                            "batch_group_id": batch_group_id,
                            "settings": asdict(settings) if hasattr(settings, '__dict__') else (settings if isinstance(settings, dict) else {}),
                        })
                        # 하지만 배치 ID는 추가되지 않았으므로 batch_ids에는 포함되지 않음
        
        # 모든 청크 생성 완료 후, total_chunks를 실제 생성된 배치 수로 업데이트
        actual_total_chunks = len(batch_ids)
        if actual_total_chunks > 0:
            self.append_log(f"[INFO] 총 {actual_total_chunks}개 배치 생성 완료. 작업 이력 업데이트 중...")
            jobs = load_batch_jobs()
            updated_count = 0
            for j in jobs:
                if j.get("batch_group_id") == batch_group_id:
                    j["total_chunks"] = actual_total_chunks
                    updated_count += 1
            if updated_count > 0:
                save_batch_jobs(jobs)
                self.append_log(f"[INFO] {updated_count}개 작업의 total_chunks를 {actual_total_chunks}로 업데이트했습니다.")
        else:
            self.append_log(f"⚠️ 생성된 배치가 없습니다. 모든 청크 생성이 실패했을 수 있습니다.")
        
        # 성공한 배치가 있는 경우에만 청크 파일 정리 옵션 제공 (현재는 유지)
        # 실패한 청크 파일은 수동 재시도를 위해 유지
        if actual_total_chunks < chunk_num:
            failed_chunks = chunk_num - actual_total_chunks
            self.append_log(f"⚠️ {failed_chunks}개 청크의 배치 생성이 실패했습니다. 청크 파일은 유지됩니다.")
            
            # 실패한 청크 파일 목록을 명확히 표시
            if failed_chunk_files:
                self.append_log(f"[INFO] 실패한 청크 파일 목록:")
                for failed_info in failed_chunk_files:
                    self.append_log(f"  - 청크 {failed_info['chunk_num']}: {os.path.basename(failed_info['chunk_file'])}")
                    if failed_info['is_token_limit']:
                        self.append_log(f"    → 토큰 제한 오류. 일부 배치 완료 후 재시도하세요.")
                
                # 실패 정보를 JSON 파일로 저장 (나중에 재시도 가능하도록)
                failed_info_path = f"{base}_failed_chunks.json"
                try:
                    with open(failed_info_path, "w", encoding="utf-8") as f:
                        json.dump(failed_chunk_files, f, ensure_ascii=False, indent=2)
                    self.append_log(f"[INFO] 실패한 청크 정보 저장: {os.path.basename(failed_info_path)}")
                    self.append_log(f"[INFO] 나중에 이 정보를 사용하여 실패한 청크만 재시도할 수 있습니다.")
                    
                    # GUI에 자동으로 실패 정보 파일 경로 설정 및 알림
                    self.after(0, lambda: self._handle_failed_chunks(failed_info_path, failed_chunk_files))
                except Exception as e:
                    self.append_log(f"[WARN] 실패 정보 저장 실패: {e}")
        
        # 메인 런처 현황판에 Stage3(Text) 작업 시작 상태 기록: T3 (진행중)
        try:
            root_name = get_root_filename(excel_path)
            JobManager.update_status(root_name, text_msg="T3 (진행중)")
            self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T3 (진행중)")
        except Exception:
            pass
        
        return batch_ids

    # ----------------------------------------------------
    # Tab 2: Manage (List & Trash)
    # ----------------------------------------------------
    def _init_tab_manage(self):
        container = ttk.Frame(self.tab_manage, padding=10)
        container.pack(fill='both', expand=True)

        sub_tabs = ttk.Notebook(container)
        sub_tabs.pack(fill='both', expand=True)
        
        self.sub_active = ttk.Frame(sub_tabs, padding=10)
        self.sub_archive = ttk.Frame(sub_tabs, padding=10)
        
        sub_tabs.add(self.sub_active, text=" ▶ 진행중 / 완료 (Active) ")
        sub_tabs.add(self.sub_archive, text=" 🗑 휴지통 (Archive) ")
        
        # --- Active Tab UI ---
        f_ctrl = ttk.Frame(self.sub_active)
        f_ctrl.pack(fill='x', pady=(0, 10))
        
        ttk.Button(f_ctrl, text="🔄 선택 갱신", command=lambda: self._refresh_selected(self.tree_active)).pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="📥 선택 일괄 병합", command=self._merge_selected, style="Primary.TButton").pack(side='left', padx=10)
        ttk.Button(f_ctrl, text="📊 선택 일괄 통합 리포트", command=self._report_selected_unified, style="Success.TButton").pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="🗑 휴지통 이동", command=self._archive_selected, style="Danger.TButton").pack(side='right', padx=2)
        
        # 그룹 접기/펼치기 버튼 추가
        f_group_ctrl = ttk.Frame(self.sub_active)
        f_group_ctrl.pack(fill='x', pady=(0, 5))
        ttk.Label(f_group_ctrl, text="💡 그룹 헤더를 더블클릭하면 접기/펼치기가 됩니다.", 
                 font=("맑은 고딕", 8), foreground="#666").pack(side='left', padx=5)
        ttk.Button(f_group_ctrl, text="📂 모든 그룹 펼치기", command=lambda: self._expand_all_groups(self.tree_active)).pack(side='right', padx=2)
        ttk.Button(f_group_ctrl, text="📁 모든 그룹 접기", command=lambda: self._collapse_all_groups(self.tree_active)).pack(side='right', padx=2)
        
        # [NEW] Effort 컬럼 및 그룹 컬럼 추가, 엑셀명과 메모 컬럼 추가
        cols = ("batch_id", "excel_name", "memo", "status", "created", "completed", "model", "effort", "market", "counts", "group")
        # 계층 구조를 위해 show='tree headings' 사용 (트리 아이콘 + 컬럼 헤더)
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='tree headings', height=18, selectmode='extended')
        
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF')
        self.tree_active.tag_configure('group', background='#E8F4FD')  # 그룹 배치 강조
        self.tree_active.tag_configure('group_header', background='#C8E6C9', font=("맑은 고딕", 9, "bold"))  # 그룹 헤더 강조
        
        # 컬럼 헤더 한글화
        self.tree_active.heading("batch_id", text="배치 ID")
        self.tree_active.heading("excel_name", text="엑셀명")
        self.tree_active.heading("memo", text="메모")
        self.tree_active.heading("status", text="상태")
        self.tree_active.heading("created", text="생성일")
        self.tree_active.heading("completed", text="완료일")
        self.tree_active.heading("model", text="모델")
        self.tree_active.heading("effort", text="Effort")
        self.tree_active.heading("market", text="마켓")
        self.tree_active.heading("counts", text="요청수")
        self.tree_active.heading("group", text="그룹")
        
        # 컬럼 너비 조정: 그룹명이 길어서 트리 컬럼 확대
        self.tree_active.column("#0", width=350, anchor="w")  # 트리 컬럼 (그룹명 표시)
        self.tree_active.column("batch_id", width=180)
        self.tree_active.column("excel_name", width=200, anchor="w")  # 엑셀 파일명
        self.tree_active.column("memo", width=150, anchor="w")  # 메모
        self.tree_active.column("status", width=80, anchor="center")
        self.tree_active.column("created", width=120, anchor="center")
        self.tree_active.column("completed", width=120, anchor="center")
        self.tree_active.column("model", width=80, anchor="center")
        self.tree_active.column("effort", width=60, anchor="center")
        self.tree_active.column("market", width=80, anchor="center")
        self.tree_active.column("counts", width=80, anchor="center")
        self.tree_active.column("group", width=80, anchor="center")
        
        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)
        
        # 우클릭 메뉴
        self.menu_active = Menu(self, tearoff=0)
        self.menu_active.add_command(label="상태 갱신", command=lambda: self._refresh_selected(self.tree_active))
        self.menu_active.add_separator()
        self.menu_active.add_command(label="결과 병합", command=self._merge_selected)
        self.menu_active.add_command(label="통합 리포트 생성", command=self._report_selected_unified)
        self.menu_active.add_separator()
        self.menu_active.add_command(label="메모 편집", command=lambda: self._edit_memo(self.tree_active))
        self.menu_active.add_separator()
        self.menu_active.add_command(label="휴지통으로 이동", command=self._archive_selected)
        
        self.tree_active.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_active, self.menu_active))
        self.tree_active.bind("<Double-1>", self._on_tree_double_click)

        # --- Archive Tab UI ---
        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 선택 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 선택 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')
        
        # 그룹 접기/펼치기 버튼 추가
        f_arch_group_ctrl = ttk.Frame(self.sub_archive)
        f_arch_group_ctrl.pack(fill='x', pady=(0, 5))
        ttk.Label(f_arch_group_ctrl, text="💡 그룹 헤더를 더블클릭하면 접기/펼치기가 됩니다.", 
                 font=("맑은 고딕", 8), foreground="#666").pack(side='left', padx=5)
        ttk.Button(f_arch_group_ctrl, text="📂 모든 그룹 펼치기", command=lambda: self._expand_all_groups(self.tree_arch)).pack(side='right', padx=2)
        ttk.Button(f_arch_group_ctrl, text="📁 모든 그룹 접기", command=lambda: self._collapse_all_groups(self.tree_arch)).pack(side='right', padx=2)
        
        # 계층 구조를 위해 show='tree headings' 사용
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='tree headings', height=18, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2')
        self.tree_arch.tag_configure('group', background='#FFE8E8')  # 그룹 배치 강조
        self.tree_arch.tag_configure('group_header', background='#FFCDD2', font=("맑은 고딕", 9, "bold"))  # 그룹 헤더 강조 

        # 컬럼 헤더 한글화
        self.tree_arch.heading("batch_id", text="배치 ID")
        self.tree_arch.heading("excel_name", text="엑셀명")
        self.tree_arch.heading("memo", text="메모")
        self.tree_arch.heading("status", text="상태")
        self.tree_arch.heading("created", text="생성일")
        self.tree_arch.heading("completed", text="완료일")
        self.tree_arch.heading("model", text="모델")
        self.tree_arch.heading("effort", text="Effort")
        self.tree_arch.heading("market", text="마켓")
        self.tree_arch.heading("counts", text="요청수")
        self.tree_arch.heading("group", text="그룹")
        
        self.tree_arch.column("#0", width=350, anchor="w")  # 트리 컬럼 (그룹명 표시)
        self.tree_arch.column("batch_id", width=200, anchor="w")
        self.tree_arch.column("excel_name", width=200, anchor="w")  # 엑셀 파일명
        self.tree_arch.column("memo", width=150, anchor="w")  # 메모
        self.tree_arch.column("status", width=80, anchor="center")
        self.tree_arch.column("created", width=120, anchor="center")
        self.tree_arch.column("completed", width=120, anchor="center")
        self.tree_arch.column("model", width=80, anchor="center")
        self.tree_arch.column("effort", width=60, anchor="center")
        self.tree_arch.column("market", width=80, anchor="center")
        self.tree_arch.column("counts", width=80, anchor="center")
        self.tree_arch.column("group", width=80, anchor="center")
        
        self.tree_arch.pack(fill='both', expand=True)
        
        # Archive 우클릭 메뉴
        self.menu_arch = Menu(self, tearoff=0)
        self.menu_arch.add_command(label="메모 편집", command=lambda: self._edit_memo(self.tree_arch))
        self.menu_arch.add_separator()
        self.menu_arch.add_command(label="♻️ 선택 복구", command=self._restore_selected)
        self.menu_arch.add_command(label="🔥 선택 영구 삭제", command=self._hard_delete_selected)
        self.tree_arch.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_arch, self.menu_arch))
        
        self._load_jobs_all()
        self._load_archive_list()

    def _show_context_menu(self, event, tree, menu):
        item = tree.identify_row(event.y)
        if item:
            if item not in tree.selection():
                tree.selection_set(item)
            menu.post(event.x_root, event.y_root)

    def _expand_all_groups(self, tree):
        """모든 그룹 헤더를 펼칩니다."""
        for item in tree.get_children():
            vals = tree.item(item)['values']
            if not vals or not vals[0]:  # 그룹 헤더인 경우 (batch_id가 비어있음)
                tree.item(item, open=True)
    
    def _collapse_all_groups(self, tree):
        """모든 그룹 헤더를 접습니다."""
        for item in tree.get_children():
            vals = tree.item(item)['values']
            if not vals or not vals[0]:  # 그룹 헤더인 경우 (batch_id가 비어있음)
                tree.item(item, open=False)
    
    def _get_selected_ids(self, tree):
        """
        선택된 항목에서 배치 ID를 추출합니다.
        그룹 헤더를 선택하면 그룹 내 모든 배치 ID를 반환합니다.
        """
        selection = tree.selection()
        ids = []
        
        for item in selection:
            vals = tree.item(item)['values']
            batch_id = vals[0] if vals else ""
            
            # 그룹 헤더인 경우 (batch_id가 비어있음)
            if not batch_id:
                # 그룹 헤더의 자식 노드들(배치들)을 모두 가져오기
                children = tree.get_children(item)
                for child in children:
                    child_vals = tree.item(child)['values']
                    if child_vals and child_vals[0]:
                        ids.append(child_vals[0])
            else:
                # 개별 배치인 경우
                ids.append(batch_id)
        
        # 중복 제거
        return list(set(ids))
    
    def _edit_memo(self, tree):
        """선택된 배치의 메모를 편집합니다."""
        selection = tree.selection()
        if not selection:
            messagebox.showwarning("경고", "메모를 편집할 배치를 선택해주세요.")
            return
        
        # 첫 번째 선택된 항목의 배치 ID 가져오기
        item = selection[0]
        vals = tree.item(item)['values']
        batch_id = vals[0] if vals else ""
        
        if not batch_id:
            messagebox.showinfo("안내", "배치를 선택해주세요.")
            return
        
        # 현재 메모 가져오기
        jobs = load_batch_jobs()
        current_memo = ""
        for j in jobs:
            if j["batch_id"] == batch_id:
                current_memo = j.get("memo", "") or ""
                break
        
        # 메모 편집 다이얼로그
        dialog = tk.Toplevel(self)
        dialog.title("메모 편집")
        dialog.geometry("500x200")
        dialog.transient(self)
        dialog.grab_set()
        
        # 배치 ID 표시
        tk.Label(dialog, text=f"배치 ID: {batch_id[:30]}...", font=("맑은 고딕", 9), anchor="w").pack(fill="x", padx=10, pady=(10, 5))
        
        # 메모 입력 필드
        tk.Label(dialog, text="메모:", font=("맑은 고딕", 9), anchor="w").pack(fill="x", padx=10, pady=(5, 0))
        memo_entry = tk.Text(dialog, height=5, width=60, font=("맑은 고딕", 9))
        memo_entry.pack(fill="both", expand=True, padx=10, pady=5)
        memo_entry.insert("1.0", current_memo)
        memo_entry.focus()
        
        # 버튼
        btn_frame = tk.Frame(dialog)
        btn_frame.pack(fill="x", padx=10, pady=10)
        
        def save_memo():
            new_memo = memo_entry.get("1.0", "end-1c").strip()
            upsert_batch_job(batch_id, memo=new_memo)
            self.append_log(f"[INFO] 배치 {batch_id[:20]}... 메모 업데이트: {new_memo[:50]}...")
            self._load_jobs_all()
            self._load_archive_list()
            dialog.destroy()
            messagebox.showinfo("완료", "메모가 저장되었습니다.")
        
        tk.Button(btn_frame, text="저장", command=save_memo, bg="#4CAF50", fg="white", font=("맑은 고딕", 9), width=10).pack(side="right", padx=5)
        tk.Button(btn_frame, text="취소", command=dialog.destroy, bg="#f44336", fg="white", font=("맑은 고딕", 9), width=10).pack(side="right", padx=5)

    def _load_jobs_all(self):
        if not hasattr(self, 'tree_active'): return
        for i in self.tree_active.get_children(): self.tree_active.delete(i)
        jobs = load_batch_jobs()
        idx = 0
        
        # 그룹별로 정렬하여 표시
        grouped_jobs = {}
        ungrouped_jobs = []
        for j in jobs:
            if j.get("archived", False): continue
            group_id = j.get("batch_group_id")
            if group_id:
                if group_id not in grouped_jobs:
                    grouped_jobs[group_id] = []
                grouped_jobs[group_id].append(j)
            else:
                ungrouped_jobs.append(j)
        
        # 그룹별 배치 표시 (계층 구조)
        for group_id, group_jobs in sorted(grouped_jobs.items()):
            # 그룹 내 배치들을 청크 번호 순으로 정렬
            group_jobs.sort(key=lambda x: x.get("chunk_index", 0))
            total_chunks = group_jobs[0].get("total_chunks", len(group_jobs))
            
            # 그룹 상태 집계
            statuses = {}
            total_completed = 0
            total_requests = 0
            for j in group_jobs:
                status = j.get("status", "unknown")
                statuses[status] = statuses.get(status, 0) + 1
                if "request_counts" in j and j["request_counts"]:
                    rc = j["request_counts"]
                    total_completed += rc.get('completed', 0)
                    total_requests += rc.get('total', 0)
            
            # 그룹 헤더 생성 (요약 정보 - 간소화)
            completed_count = statuses.get("completed", 0) + statuses.get("merged", 0)
            status_summary = f"완료: {completed_count}/{total_chunks}"
            if total_requests > 0:
                status_summary += f" | 요청: {total_completed}/{total_requests}"
            
            # 그룹 생성 날짜 (첫 번째 배치의 생성 시간 사용 - values에만 포함)
            first_job = group_jobs[0]
            created_at = first_job.get("created_at", "")
            if created_at:
                try:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    date_str = dt.strftime("%m-%d %H:%M")
                except:
                    date_str = created_at[:16].replace("T", " ") if len(created_at) >= 16 else created_at[:10]
            else:
                date_str = "-"
            
            # 그룹 헤더 (부모 노드) - 간소화된 텍스트
            group_header_text = f"📦 {group_id[:12]} | {status_summary}"
            excel_name = get_excel_name_from_path(first_job.get("src_excel", ""))
            memo = first_job.get("memo", "") or "-"
            group_node = self.tree_active.insert("", "end", 
                text=group_header_text,
                values=("", excel_name, memo, "", date_str, "", first_job.get("model", "-"), first_job.get("effort", "-"), first_job.get("market", "-"), "", f"그룹 {total_chunks}개"),
                tags=('group_header',),
                open=False  # 기본적으로 접힌 상태
            )
            
            # 개별 배치 (자식 노드)
            for j in group_jobs:
                cnt = "-"
                if "request_counts" in j and j["request_counts"]:
                    rc = j["request_counts"]
                    cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
                c_at = (j.get("created_at") or "")[:16].replace("T", " ")
                f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
                chunk_info = f"{j.get('chunk_index', 0)}/{total_chunks}"
                group_display = f"청크 {chunk_info}"
                excel_name = get_excel_name_from_path(j.get("src_excel", ""))
                memo = j.get("memo", "") or "-"
                tag = 'group'
                self.tree_active.insert(group_node, "end", 
                    text=f"  └─ {j['batch_id'][:20]}...",
                    values=(
                        j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, group_display
                    ), 
                    tags=(tag,))
        
        # 그룹 없는 배치 표시
        for j in ungrouped_jobs:
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            excel_name = get_excel_name_from_path(j.get("src_excel", ""))
            memo = j.get("memo", "") or "-"
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_active.insert("", "end", 
                text=j["batch_id"][:30],
                values=(
                    j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, "-"
                ), 
                tags=(tag,))
            idx += 1

    def _load_archive_list(self):
        if not hasattr(self, 'tree_arch'): return
        for i in self.tree_arch.get_children(): self.tree_arch.delete(i)
        jobs = load_batch_jobs()
        idx = 0
        
        # 그룹별로 정렬하여 표시
        grouped_jobs = {}
        ungrouped_jobs = []
        for j in jobs:
            if not j.get("archived", False): continue
            group_id = j.get("batch_group_id")
            if group_id:
                if group_id not in grouped_jobs:
                    grouped_jobs[group_id] = []
                grouped_jobs[group_id].append(j)
            else:
                ungrouped_jobs.append(j)
        
        # 그룹별 배치 표시 (계층 구조)
        for group_id, group_jobs in sorted(grouped_jobs.items()):
            group_jobs.sort(key=lambda x: x.get("chunk_index", 0))
            total_chunks = group_jobs[0].get("total_chunks", len(group_jobs)) if group_jobs else len(group_jobs)
            
            # 그룹 상태 집계
            statuses = {}
            total_completed = 0
            total_requests = 0
            for j in group_jobs:
                status = j.get("status", "unknown")
                statuses[status] = statuses.get(status, 0) + 1
                if "request_counts" in j and j["request_counts"]:
                    rc = j["request_counts"]
                    total_completed += rc.get('completed', 0)
                    total_requests += rc.get('total', 0)
            
            # 그룹 헤더 생성 (요약 정보 - 간소화)
            completed_count = statuses.get("completed", 0) + statuses.get("merged", 0)
            status_summary = f"완료: {completed_count}/{total_chunks}"
            if total_requests > 0:
                status_summary += f" | 요청: {total_completed}/{total_requests}"
            
            # 그룹 생성 날짜 (첫 번째 배치의 생성 시간 사용 - values에만 포함)
            first_job = group_jobs[0]
            created_at = first_job.get("created_at", "")
            if created_at:
                try:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    date_str = dt.strftime("%m-%d %H:%M")
                except:
                    date_str = created_at[:16].replace("T", " ") if len(created_at) >= 16 else created_at[:10]
            else:
                date_str = "-"
            
            # 그룹 헤더 (부모 노드) - 간소화된 텍스트
            group_header_text = f"📦 {group_id[:12]} | {status_summary}"
            excel_name = get_excel_name_from_path(first_job.get("src_excel", ""))
            memo = first_job.get("memo", "") or "-"
            group_node = self.tree_arch.insert("", "end", 
                text=group_header_text,
                values=("", excel_name, memo, "", date_str, "", first_job.get("model", "-"), first_job.get("effort", "-"), first_job.get("market", "-"), "", f"그룹 {total_chunks}개"),
                tags=('group_header',),
                open=False  # 기본적으로 접힌 상태
            )
            
            # 개별 배치 (자식 노드)
            for j in group_jobs:
                cnt = "-"
                if "request_counts" in j and j["request_counts"]:
                    rc = j["request_counts"]
                    cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
                c_at = (j.get("created_at") or "")[:16].replace("T", " ")
                f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
                chunk_info = f"{j.get('chunk_index', 0)}/{total_chunks}"
                group_display = f"청크 {chunk_info}"
                excel_name = get_excel_name_from_path(j.get("src_excel", ""))
                memo = j.get("memo", "") or "-"
                tag = 'group'
                self.tree_arch.insert(group_node, "end", 
                    text=f"  └─ {j['batch_id'][:20]}...",
                    values=(
                        j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, group_display
                    ), 
                    tags=(tag,))
        
        # 그룹 없는 배치 표시
        for j in ungrouped_jobs:
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            excel_name = get_excel_name_from_path(j.get("src_excel", ""))
            memo = j.get("memo", "") or "-"
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_arch.insert("", "end", 
                text=j["batch_id"][:30],
                values=(
                    j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, "-"
                ), 
                tags=(tag,))
            idx += 1

    # --- Batch Actions ---
    def _refresh_selected(self, tree):
        ids = self._get_selected_ids(tree)
        if not ids: return
        
        # API 키 확인
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key가 필요합니다.\n상단의 API 설정에서 API Key를 입력해주세요.")
            return
        
        jobs = load_batch_jobs()
        completed = [bid for bid in ids if next((x for x in jobs if x["batch_id"] == bid), {}).get("status") in ["completed", "merged"]]
        
        if completed:
            if messagebox.askyesno("확인", f"{len(completed)}건은 이미 완료되었습니다.\n제외하고 미완료 건만 갱신할까요?"):
                ids = [i for i in ids if i not in completed]
        
        if not ids:
            messagebox.showinfo("취소", "갱신할 대상이 없습니다.")
            return

        t = threading.Thread(target=self._run_refresh_ids, args=(ids,))
        t.daemon = True
        t.start()

    def _run_refresh_ids(self, ids):
        key = self.api_key_var.get().strip()
        if not key:
            self.append_log("❌ API Key가 없습니다. 상단의 API 설정에서 API Key를 입력해주세요.")
            self.after(0, lambda: messagebox.showwarning("오류", "API Key가 필요합니다."))
            return
        
        try:
            client = OpenAI(api_key=key)
        except Exception as e:
            self.append_log(f"❌ OpenAI 클라이언트 생성 실패: {e}")
            self.after(0, lambda: messagebox.showerror("오류", f"API Key가 유효하지 않습니다.\n{e}"))
            return
        
        self.append_log(f"선택된 {len(ids)}건 갱신 중...")
        success_cnt = 0
        fail_cnt = 0
        
        for bid in ids:
            try:
                remote = client.batches.retrieve(bid)
                rc = None
                if remote.request_counts:
                    rc = {"total": remote.request_counts.total, "completed": remote.request_counts.completed, "failed": remote.request_counts.failed}
                
                # expired 상태도 갱신 가능 (output_file_id 확인을 위해)
                output_file_id = getattr(remote, "output_file_id", None)
                upsert_batch_job(bid, status=remote.status, output_file_id=output_file_id, request_counts=rc)
                
                if remote.status == "expired" and output_file_id:
                    self.append_log(f"ℹ️ {bid}: 만료된 배치이지만 output_file_id가 있습니다. (다운로드 가능)")
                self.append_log(f"✅ {bid}: {remote.status}")
                success_cnt += 1
            except Exception as e:
                error_msg = str(e)
                # 401 오류인 경우 더 명확한 메시지
                if "401" in error_msg or "authentication" in error_msg.lower():
                    self.append_log(f"❌ {bid} 갱신 실패: API Key 인증 오류 (401)")
                    self.append_log(f"   → 상단의 API 설정에서 올바른 API Key를 확인해주세요.")
                else:
                    self.append_log(f"❌ {bid} 갱신 실패: {error_msg}")
                fail_cnt += 1
        
        self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])
        if fail_cnt > 0:
            self.append_log(f"갱신 완료 (성공: {success_cnt}, 실패: {fail_cnt})")
            if fail_cnt == len(ids):
                self.after(0, lambda: messagebox.showwarning("경고", f"모든 배치 갱신이 실패했습니다.\nAPI Key를 확인해주세요."))
        else:
            self.append_log(f"갱신 완료 (성공: {success_cnt}건)")

    def _merge_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        
        # API 키 확인
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key가 필요합니다.\n상단의 API 설정에서 API Key를 입력해주세요.")
            return
        
        jobs = load_batch_jobs()
        
        # 같은 그룹의 모든 배치를 자동으로 포함
        group_ids = set()
        for bid in ids:
            job = next((j for j in jobs if j["batch_id"] == bid), None)
            if job:
                group_id = job.get("batch_group_id")
                if group_id:
                    group_ids.add(group_id)
        
        # 같은 그룹의 모든 배치를 자동으로 포함
        all_target_ids = set(ids)
        group_info = ""  # 초기화
        
        for group_id in group_ids:
            if group_id:
                # completed, expired 또는 merged 상태인 배치 포함 (expired 상태에서도 output_file_id가 있으면 다운로드 가능)
                group_batches = [j for j in jobs if j.get("batch_group_id") == group_id and j.get("status") in ["completed", "expired"]]
                for j in group_batches:
                    all_target_ids.add(j["batch_id"])
        
        # 그룹 정보 메시지 생성
        if len(all_target_ids) > len(ids):
            group_info = f"\n\n같은 그룹의 배치 {len(all_target_ids) - len(ids)}개가 자동으로 포함됩니다."
        
        # completed, expired 또는 merged 상태인 배치 모두 선택 가능 (expired 상태에서도 output_file_id가 있으면 다운로드 가능)
        targets = [bid for bid in all_target_ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") in ["completed", "expired", "merged"]]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed', 'expired' 또는 'merged' 상태의 작업이 없습니다.")
            return
        
        completed_cnt = sum(1 for bid in targets if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "completed")
        merged_cnt = len(targets) - completed_cnt
        
        msg = f"선택한 {len(targets)}건을 처리하시겠습니까?{group_info}"
        if merged_cnt > 0:
            msg += f"\n\n({completed_cnt}건: 병합 + 히스토리 기록)\n({merged_cnt}건: 히스토리 기록만)"
        
        if messagebox.askyesno("병합", msg):
            t = threading.Thread(target=self._run_merge_multi, args=(targets,))
            t.daemon = True
            t.start()

    def _run_merge_multi(self, ids):
        key = self.api_key_var.get().strip()
        if not key:
            self.append_log("❌ API Key가 없습니다. 상단의 API 설정에서 API Key를 입력해주세요.")
            self.after(0, lambda: messagebox.showwarning("오류", "API Key가 필요합니다."))
            return
        
        try:
            client = OpenAI(api_key=key)
        except Exception as e:
            self.append_log(f"❌ OpenAI 클라이언트 생성 실패: {e}")
            self.after(0, lambda: messagebox.showerror("오류", f"API Key가 유효하지 않습니다.\n{e}"))
            return
        
        jobs = load_batch_jobs()
        
        # 그룹별로 분류
        groups_to_merge = {}  # {group_id: [batch_ids]}
        ungrouped_batches = []  # 그룹 없는 배치들
        
        for bid in ids:
            job = next((j for j in jobs if j["batch_id"] == bid), None)
            if not job:
                continue
            
            group_id = job.get("batch_group_id")
            if group_id:
                if group_id not in groups_to_merge:
                    groups_to_merge[group_id] = []
                groups_to_merge[group_id].append(bid)
            else:
                ungrouped_batches.append(bid)
        
        success_cnt = 0
        total_cost = 0.0
        
        # 그룹별 병합 처리
        for group_id, batch_ids in groups_to_merge.items():
            self.append_log(f"--- 그룹 병합 시작: {group_id} ({len(batch_ids)}개 배치) ---")
            try:
                # 그룹 내 첫 번째 배치의 원본 엑셀 경로 사용
                first_job = next((j for j in jobs if j["batch_id"] == batch_ids[0]), None)
                if not first_job:
                    continue
                
                src_path = first_job.get("src_excel") or ""
                if not src_path or not os.path.exists(src_path):
                    self.append_log(f"⚠️ 그룹 {group_id}: 원본 엑셀 경로가 없거나 존재하지 않아 건너뜁니다.")
                    continue
                
                # 그룹 내 모든 배치의 결과를 임시 JSONL에 수집
                all_output_lines = []
                model_name = first_job.get("model", "gpt-5-mini")
                total_group_cost = 0.0
                
                # 청크 번호 순으로 정렬 (chunk_index가 없는 경우는 맨 뒤로)
                def get_chunk_index(bid):
                    job = next((j for j in jobs if j["batch_id"] == bid), None)
                    if job:
                        idx = job.get("chunk_index")
                        return idx if idx is not None else 999999
                    return 999999
                
                batch_ids_sorted = sorted(batch_ids, key=get_chunk_index)
                
                for bid in batch_ids_sorted:
                    self.append_log(f"  [그룹] 배치 {bid} 결과 다운로드 중...")
                    try:
                        local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                        if not local_job:
                            continue
                        
                        # 이미 병합된 배치도 output 파일이 있으면 재병합 가능
                        is_already_merged = local_job.get("status") == "merged"
                        if is_already_merged:
                            # 로컬에 output JSONL 파일이 있는지 확인
                            base_dir = os.path.dirname(src_path)
                            base_name, _ = os.path.splitext(os.path.basename(src_path))
                            out_jsonl = os.path.join(base_dir, f"{base_name}_stage3_batch_output_{bid}.jsonl")
                            
                            if os.path.exists(out_jsonl):
                                # 로컬 파일이 있으면 다운로드 없이 바로 사용
                                self.append_log(f"  ℹ️ {bid}: 이미 병합된 작업이지만 로컬 output 파일이 있어 재병합합니다.")
                                # 아래 다운로드 로직을 건너뛰고 JSONL 읽기로 이동
                            else:
                                # 로컬 파일이 없으면 원격에서 다운로드 시도
                                self.append_log(f"  ℹ️ {bid}: 이미 병합된 작업이지만 output 파일을 다시 다운로드합니다.")
                        
                        # Batch 상태 확인
                        remote = client.batches.retrieve(bid)
                        output_file_id = getattr(remote, "output_file_id", None)
                        
                        # completed 또는 expired 상태에서 output_file_id가 있으면 다운로드 시도
                        if remote.status == "completed":
                            if not output_file_id:
                                self.append_log(f"  ⚠️ {bid}: 완료 상태지만 output_file_id를 찾을 수 없어 건너뜁니다.")
                                upsert_batch_job(bid, status=remote.status, output_file_id=None)
                                continue
                        elif remote.status == "expired":
                            if not output_file_id:
                                self.append_log(f"  ⚠️ {bid}: 만료된 배치이며 output_file_id를 찾을 수 없어 건너뜁니다. 수동으로 다운로드해주세요.")
                                upsert_batch_job(bid, status=remote.status, output_file_id=None)
                                continue
                            else:
                                self.append_log(f"  ℹ️ {bid}: 배치가 만료되었지만 output_file_id가 있어 다운로드를 시도합니다.")
                        else:
                            self.append_log(f"  ⚠️ {bid}: 아직 completed 상태가 아니어서 건너뜁니다. (status={remote.status})")
                            upsert_batch_job(bid, status=remote.status, output_file_id=output_file_id)
                            continue
                        
                        # 결과 다운로드 (로컬 파일이 없을 때만)
                        base_dir = os.path.dirname(src_path)
                        base_name, _ = os.path.splitext(os.path.basename(src_path))
                        out_jsonl = os.path.join(base_dir, f"{base_name}_stage3_batch_output_{bid}.jsonl")
                        
                        if not os.path.exists(out_jsonl):
                            # 로컬 파일이 없으면 다운로드
                            try:
                                content = client.files.content(output_file_id).content
                            except AttributeError:
                                file_content = client.files.content(output_file_id)
                                if hasattr(file_content, "read"):
                                    content = file_content.read()
                                elif hasattr(file_content, "iter_bytes"):
                                    chunks = []
                                    for ch in file_content.iter_bytes():
                                        chunks.append(ch)
                                    content = b"".join(chunks)
                                else:
                                    content = file_content
                            
                            with open(out_jsonl, "wb") as f:
                                f.write(content)
                            
                            upsert_batch_job(bid, status="completed", output_file_id=output_file_id, output_jsonl=out_jsonl)
                        else:
                            # 로컬 파일이 있으면 다운로드 건너뛰기
                            self.append_log(f"  [그룹] 로컬 output 파일 사용: {os.path.basename(out_jsonl)}")
                            if not is_already_merged:
                                upsert_batch_job(bid, status="completed", output_file_id=output_file_id, output_jsonl=out_jsonl)
                        
                        # JSONL 파일 읽어서 수집
                        if os.path.exists(out_jsonl):
                            with open(out_jsonl, "r", encoding="utf-8") as f:
                                for line in f:
                                    line = line.strip()
                                    if line:
                                        all_output_lines.append(line)
                        
                    except Exception as e:
                        self.append_log(f"  ❌ {bid} 결과 다운로드 실패: {e}")
                        continue
                
                if not all_output_lines:
                    self.append_log(f"⚠️ 그룹 {group_id}: 병합할 결과가 없습니다.")
                    continue
                
                # 그룹의 전체 청크 수 확인 및 검증
                expected_total_chunks = first_job.get("total_chunks")
                if expected_total_chunks:
                    # 실제로 결과를 다운로드한 배치 수 계산 (completed 또는 expired 상태 포함)
                    downloaded_batch_ids = []
                    for bid in batch_ids_sorted:
                        local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                        if local_job and local_job.get("status") in ["completed", "expired"]:
                            out_jsonl = local_job.get("output_jsonl") or os.path.join(
                                os.path.dirname(src_path),
                                f"{os.path.splitext(os.path.basename(src_path))[0]}_stage3_batch_output_{bid}.jsonl"
                            )
                            if os.path.exists(out_jsonl):
                                downloaded_batch_ids.append(bid)
                    
                    if len(downloaded_batch_ids) < expected_total_chunks:
                        missing = expected_total_chunks - len(downloaded_batch_ids)
                        self.append_log(f"⚠️ 그룹 {group_id}: 예상 {expected_total_chunks}개 중 {len(downloaded_batch_ids)}개만 다운로드되었습니다. ({missing}개 누락 가능)")
                
                # 임시 통합 JSONL 파일 생성
                base_dir = os.path.dirname(src_path)
                base_name, _ = os.path.splitext(os.path.basename(src_path))
                merged_jsonl = os.path.join(base_dir, f"{base_name}_stage3_batch_output_merged_{group_id}.jsonl")
                
                with open(merged_jsonl, "w", encoding="utf-8") as f:
                    for line in all_output_lines:
                        f.write(line + "\n")
                
                self.append_log(f"  [그룹] 통합 JSONL 생성: {len(all_output_lines)}개 결과")
                
                # 통합 JSONL을 엑셀에 병합
                results_map = {}
                total_group_in = 0
                total_group_out = 0
                total_group_cached = 0
                total_group_requests = 0
                total_group_cache_hits = 0
                
                with open(merged_jsonl, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            data = json.loads(line)
                        except json.JSONDecodeError as e:
                            self.append_log(f"[WARN] JSON 파싱 실패: {e}")
                            continue
                        
                        cid = data.get("custom_id")
                        if not cid:
                            continue
                        
                        # /v1/responses API 형식 처리
                        # response가 있으면 body에서, 없으면 error에서 토큰 정보 확인
                        response = data.get("response")
                        error = data.get("error")
                        
                        # 토큰 사용량 추출 (response 또는 error 모두에서)
                        usage = {}
                        if response:
                            response_body = response.get("body", {}) if isinstance(response, dict) else {}
                            usage = response_body.get("usage", {})
                        elif error:
                            # 에러 응답에도 usage가 있을 수 있음
                            if isinstance(error, dict):
                                usage = error.get("usage", {})
                        
                        input_tokens = usage.get("input_tokens", 0) or usage.get("prompt_tokens", 0)  # 호환성
                        output_tokens = usage.get("output_tokens", 0) or usage.get("completion_tokens", 0)  # 호환성
                        total_group_in += input_tokens
                        total_group_out += output_tokens
                        
                        # 캐싱 통계 수집
                        input_tokens_details = usage.get("input_tokens_details", {})
                        cached_tokens = input_tokens_details.get("cached_tokens", 0)
                        total_group_cached += cached_tokens
                        total_group_requests += 1
                        if cached_tokens > 0:
                            total_group_cache_hits += 1
                        
                        # 결과 텍스트 추출 (에러가 아닌 경우만)
                        if response and not error:
                            try:
                                # 새로운 API 응답 형식: response.body.output 배열 사용
                                response_body = response.get("body", {}) if isinstance(response, dict) else {}
                                output_array = response_body.get("output", [])
                                
                                # output 배열에서 type="message"인 항목 찾기
                                text_content = ""
                                for item in output_array:
                                    if item.get("type") == "message":
                                        content_array = item.get("content", [])
                                        for content_item in content_array:
                                            if content_item.get("type") == "output_text":
                                                text_content = content_item.get("text", "").strip()
                                                break
                                        if text_content:
                                            break
                                
                                if text_content:
                                    results_map[cid] = text_content
                                else:
                                    # 기존 형식 호환: choices 사용 (fallback)
                                    val = response_body.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                                    if val:
                                        results_map[cid] = val
                                    else:
                                        results_map[cid] = ""
                            except Exception as e:
                                self.append_log(f"[WARN] 결과 추출 실패 (custom_id: {cid}): {e}")
                                results_map[cid] = ""
                        elif error:
                            # 에러 케이스: 빈 문자열로 처리
                            results_map[cid] = ""
                        else:
                            # response도 error도 없는 경우
                            results_map[cid] = ""
                
                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    if "ST3_결과상품명" not in df.columns:
                        df["ST3_결과상품명"] = ""
                    df["ST3_결과상품명"] = df["ST3_결과상품명"].astype(str)
                    cnt = 0
                    for cid, val in results_map.items():
                        try:
                            # custom_id 형식: "row-0", "row-1" 등
                            if cid.startswith("row-"):
                                idx = int(cid.split("-")[1])
                            else:
                                # 기존 형식 호환: "row_0" 등
                                idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                df.at[idx, "ST3_결과상품명"] = val
                                cnt += 1
                        except Exception as e:
                            self.append_log(f"[WARN] custom_id 파싱 실패: {cid} - {e}")
                            pass

                    # 코어 완료 파일(out_excel)을 먼저 저장
                    base, _ = os.path.splitext(src_path)
                    out_excel = f"{base}_stage3_batch_done.xlsx"
                    if not safe_save_excel(df, out_excel):
                        self.append_log(f"[WARN] 기본 완료 파일 저장 실패: {out_excel}")

                    # Stage3 최종 파일명: *_T3_... 형식으로 버전 업
                    try:
                        final_out_path = get_next_version_path(src_path, task_type="text")
                        df_done = pd.read_excel(out_excel)
                        
                        # ST3_결과상품명이 있는 행과 없는 행 분리
                        if "ST3_결과상품명" in df_done.columns:
                            df_with_st3 = df_done[df_done["ST3_결과상품명"].notna() & (df_done["ST3_결과상품명"] != '') & (df_done["ST3_결과상품명"].astype(str) != 'nan')].copy()
                            df_no_st3 = df_done[(df_done["ST3_결과상품명"].isna()) | (df_done["ST3_결과상품명"] == '') | (df_done["ST3_결과상품명"].astype(str) == 'nan')].copy()
                        else:
                            df_with_st3 = pd.DataFrame()
                            df_no_st3 = df_done.copy()
                        
                        # ST3_결과상품명이 없는 행들을 T3(실패) 버전으로 별도 파일 저장
                        no_st3_path = None
                        if len(df_no_st3) > 0:
                            base_dir = os.path.dirname(src_path)
                            base_name, ext = os.path.splitext(os.path.basename(src_path))
                            
                            name_only_clean = re.sub(r"\([^)]*\)", "", base_name)
                            all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)", name_only_clean, re.IGNORECASE))
                            
                            if all_matches:
                                match = all_matches[-1]
                                original_name = name_only_clean[: match.start()].rstrip("_")
                                current_i = int(match.group(4))
                                new_filename = f"{original_name}_T3_I{current_i}(실패){ext}"
                            else:
                                new_filename = f"{base_name}_T3(실패)_I0{ext}"
                            
                            no_st3_path = os.path.join(base_dir, new_filename)
                            df_no_st3.to_excel(no_st3_path, index=False)
                            
                            self.append_log(f"  [그룹] T3(실패) 분리 파일: {os.path.basename(no_st3_path)} ({len(df_no_st3)}개 행)")
                            self.append_log(f"         ※ 이 파일은 T3 작업에 실패한 항목입니다.")
                            
                            try:
                                no_st3_root_name = get_root_filename(no_st3_path)
                                JobManager.update_status(no_st3_root_name, text_msg="T3(실패)")
                                self.append_log(f"[Launcher] 분리 파일 상태 업데이트: {no_st3_root_name} -> T3(실패)")
                            except Exception as e:
                                self.append_log(f"[Launcher] 분리 파일 상태 업데이트 실패: {e}")
                        
                        if len(df_with_st3) > 0:
                            df_done = df_with_st3
                        else:
                            self.append_log(f"  ⚠️ 그룹 {group_id}: ST3_결과상품명이 있는 행이 없습니다.")
                        
                        if safe_save_excel(df_done, final_out_path):
                            out_path_for_history = final_out_path
                            if out_excel != final_out_path and os.path.exists(out_excel):
                                try:
                                    os.remove(out_excel)
                                    self.append_log(f"[INFO] 중간 파일 삭제: {os.path.basename(out_excel)}")
                                except Exception as e:
                                    self.append_log(f"[WARN] 중간 파일 삭제 실패: {e}")
                        else:
                            out_path_for_history = out_excel
                    except Exception as e:
                        self.append_log(f"[WARN] T3 버전 파일 저장 중 오류: {e}")
                        out_path_for_history = out_excel
                    
                    # 디버깅: 토큰 수집 상태 확인
                    if total_group_requests == 0:
                        self.append_log(f"⚠️ [디버깅] 그룹 {group_id}: JSONL에서 요청을 찾을 수 없습니다. 파일 구조를 확인하세요.")
                    elif total_group_in == 0 and total_group_out == 0:
                        self.append_log(f"⚠️ [디버깅] 그룹 {group_id}: 토큰 정보가 모두 0입니다. usage 필드 구조를 확인하세요.")
                        # 첫 번째 라인 샘플 출력
                        try:
                            with open(merged_jsonl, "r", encoding="utf-8") as f:
                                first_line = f.readline()
                                if first_line.strip():
                                    sample = json.loads(first_line)
                                    self.append_log(f"  [샘플] 첫 번째 라인 구조: response={bool(sample.get('response'))}, error={bool(sample.get('error'))}, usage={bool(sample.get('response', {}).get('body', {}).get('usage'))}")
                        except Exception as e:
                            self.append_log(f"  [샘플 확인 실패]: {e}")
                    
                    # 그룹 전체 캐싱 통계 출력
                    group_cache_hit_rate = (total_group_cache_hits / total_group_requests * 100) if total_group_requests > 0 else 0
                    group_cache_savings_pct = (total_group_cached / total_group_in * 100) if total_group_in > 0 else 0
                    pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                    group_cache_savings = (total_group_cached / 1_000_000) * pricing["input"] * 0.5
                    
                    # 비용 계산 (50% 할인)
                    cost_in = (total_group_in / 1_000_000) * pricing["input"] * 0.5
                    cost_out = (total_group_out / 1_000_000) * pricing["output"] * 0.5
                    cost_total = cost_in + cost_out
                    total_group_cost = cost_total
                    total_cost += total_group_cost
                    
                    # 그룹 내 모든 배치를 merged 상태로 업데이트
                    for bid in batch_ids:
                        upsert_batch_job(
                            batch_id=bid,
                            out_excel=out_path_for_history,
                            status="merged",
                        )
                    
                    # 실행 이력 기록
                    try:
                        if first_job:
                            c_at_str = first_job.get("created_at", "")
                            if c_at_str:
                                try:
                                    # ISO 형식 파싱 (Z를 +00:00으로 변환)
                                    c_at = datetime.fromisoformat(c_at_str.replace('Z', '+00:00'))
                                    # 시간대 정보가 있으면 naive로 변환 (datetime.now()와 일치)
                                    if c_at.tzinfo is not None:
                                        c_at = c_at.replace(tzinfo=None)
                                except:
                                    c_at = datetime.now()
                            else:
                                c_at = datetime.now()
                            finish_dt = datetime.now()
                            elapsed = (finish_dt - c_at).total_seconds()
                            
                            append_run_history(
                                stage="Stage 3 Batch (Grouped)",
                                model_name=model_name,
                                reasoning_effort=first_job.get("effort", "medium"),
                                src_file=src_path,
                                out_file=out_path_for_history,
                                total_rows=len(df),
                                api_rows=len(results_map),
                                elapsed_seconds=elapsed,
                                total_in_tok=total_group_in,
                                total_out_tok=total_group_out,
                                total_reasoning_tok=0,
                                input_cost_usd=cost_in,
                                output_cost_usd=cost_out,
                                total_cost_usd=cost_total,
                                start_dt=c_at,
                                finish_dt=finish_dt,
                                api_type="batch",
                                batch_id=f"{group_id} ({len(batch_ids)} batches)",
                                success_rows=cnt,
                                fail_rows=len(results_map)-cnt,
                            )
                    except Exception as hist_e:
                        self.append_log(f"[WARN] 그룹 {group_id} 히스토리 기록 실패: {hist_e}")
                    
                    try:
                        root_name = get_root_filename(src_path)
                        JobManager.update_status(root_name, text_msg="T3(생성완료)")
                        self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T3(생성완료)")
                    except Exception as e:
                        self.append_log(f"[WARN] 현황판 연동 실패: {e}")
                    
                    # 그룹 전체 캐싱 통계 출력
                    group_cache_hit_rate = (total_group_cache_hits / total_group_requests * 100) if total_group_requests > 0 else 0
                    group_cache_savings_pct = (total_group_cached / total_group_in * 100) if total_group_in > 0 else 0
                    group_cache_savings = (total_group_cached / 1_000_000) * pricing["input"] * 0.5
                    
                    self.append_log(f"✅ 그룹 {group_id} 병합 완료 ({cnt}건): {os.path.basename(out_path_for_history)}")
                    self.append_log(f"  [그룹 캐싱 통계] 요청 {total_group_requests:,}건, 히트 {total_group_cache_hits:,}건 ({group_cache_hit_rate:.1f}%), 캐시 토큰 {total_group_cached:,} ({group_cache_savings_pct:.1f}%)")
                    if group_cache_savings > 0:
                        self.append_log(f"  [그룹 비용절감] 캐싱으로 총 ${group_cache_savings:.4f} 절감")
                    success_cnt += 1
                else:
                    self.append_log(f"⚠️ 그룹 {group_id}: 원본 엑셀 경로가 없습니다.")
            except Exception as e:
                self.append_log(f"❌ 그룹 {group_id} 병합 실패: {e}")
                import traceback
                self.append_log(traceback.format_exc())
        
        # 그룹 없는 배치 처리 (기존 로직)
        for bid in ungrouped_batches:
            self.append_log(f"--- 병합 시작: {bid} ---")
            try:
                jobs = load_batch_jobs()
                local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                
                if not local_job:
                    self.append_log(f"❌ {bid} 병합 실패: 작업 이력을 찾을 수 없습니다.")
                    continue

                # 이미 병합된 배치도 output 파일이 있으면 재병합 가능
                is_already_merged = local_job.get("status") == "merged"
                if is_already_merged:
                    # 로컬에 output JSONL 파일이 있는지 확인
                    src_path = local_job.get("src_excel")
                    if src_path:
                        base, _ = os.path.splitext(src_path)
                        out_jsonl = f"{base}_stage3_batch_output.jsonl"
                        
                        if os.path.exists(out_jsonl):
                            # 로컬 파일이 있으면 재병합 진행
                            self.append_log(f"ℹ️ {bid}: 이미 병합된 작업이지만 로컬 output 파일이 있어 재병합합니다.")
                            is_already_merged = False  # 재병합 플래그 해제
                        else:
                            # 로컬 파일이 없으면 히스토리 기록만 수행
                            self.append_log(f"⏭️ {bid}: 이미 병합 완료된 작업입니다. 히스토리 기록만 수행합니다.")
                            out_path_for_history = local_job.get("out_excel")
                            if not src_path or not out_path_for_history or not os.path.exists(out_path_for_history):
                                self.append_log(f"⚠️ {bid}: 병합된 파일을 찾을 수 없어 히스토리 기록을 건너뜁니다.")
                                continue
                    else:
                        # src_excel이 없으면 재병합 불가
                        self.append_log(f"⚠️ {bid}: 원본 엑셀 경로가 없어 재병합할 수 없습니다.")
                        continue
                
                if is_already_merged:
                    # 히스토리 기록만 수행하는 경우
                    
                    # 기존 파일에서 토큰 정보 추출 (JSONL 파일이 있으면)
                    base, _ = os.path.splitext(src_path) if src_path else ("", "")
                    out_jsonl = f"{base}_stage3_batch_output.jsonl"
                    batch_in_tok = 0
                    batch_out_tok = 0
                    batch_cached_tok = 0
                    batch_total_requests = 0
                    batch_cache_hits = 0
                    results_map = {}
                    
                    if os.path.exists(out_jsonl):
                        try:
                            with open(out_jsonl, "r", encoding="utf-8") as f:
                                for line in f:
                                    if not line.strip(): continue
                                    data = json.loads(line)
                                    # /v1/responses API 형식 처리
                                    response_body = data.get("response", {}).get("body", {})
                                    usage = response_body.get("usage", {})
                                    input_tokens = usage.get("input_tokens", 0) or usage.get("prompt_tokens", 0)  # 호환성
                                    output_tokens = usage.get("output_tokens", 0) or usage.get("completion_tokens", 0)  # 호환성
                                    batch_in_tok += input_tokens
                                    batch_out_tok += output_tokens
                                    
                                    # 캐싱 통계 수집
                                    input_tokens_details = usage.get("input_tokens_details", {})
                                    cached_tokens = input_tokens_details.get("cached_tokens", 0)
                                    batch_cached_tok += cached_tokens
                                    batch_total_requests += 1
                                    if cached_tokens > 0:
                                        batch_cache_hits += 1
                                    cid = data.get("custom_id")
                                    try:
                                        # 새로운 API 응답 형식: response.body.output 배열 사용
                                        body = response_body
                                        output_array = body.get("output", [])
                                        
                                        text_content = ""
                                        for item in output_array:
                                            if item.get("type") == "message":
                                                content_array = item.get("content", [])
                                                for content_item in content_array:
                                                    if content_item.get("type") == "output_text":
                                                        text_content = content_item.get("text", "").strip()
                                                        break
                                                if text_content:
                                                    break
                                        
                                        if text_content:
                                            results_map[cid] = text_content
                                        else:
                                            # 기존 형식 호환: choices 사용 (fallback)
                                            val = body.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                                            if val:
                                                results_map[cid] = val
                                    except Exception as e:
                                        self.append_log(f"[WARN] {bid}: 결과 추출 실패 (custom_id: {cid}): {e}")
                                        pass
                        except Exception as e:
                            self.append_log(f"[WARN] {bid}: JSONL 파일 읽기 실패: {e}")
                    
                    # 캐싱 통계 출력
                    cache_hit_rate = (batch_cache_hits / batch_total_requests * 100) if batch_total_requests > 0 else 0
                    cache_savings_pct = (batch_cached_tok / batch_in_tok * 100) if batch_in_tok > 0 else 0
                    self.append_log(f"  [캐싱] {bid}: 요청 {batch_total_requests}건, 히트 {batch_cache_hits}건 ({cache_hit_rate:.1f}%), 캐시 토큰 {batch_cached_tok:,} ({cache_savings_pct:.1f}%)")
                    
                    # 비용 계산 (50% 할인)
                    model_name = local_job.get("model", "gpt-5-mini")
                    pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                    cost_in = (batch_in_tok / 1_000_000) * pricing["input"] * 0.5
                    cost_out = (batch_out_tok / 1_000_000) * pricing["output"] * 0.5
                    cost_total = cost_in + cost_out
                    
                    # 캐시로 절감된 비용 계산
                    cache_savings = (batch_cached_tok / 1_000_000) * pricing["input"] * 0.5
                    if cache_savings > 0:
                        self.append_log(f"  [비용절감] {bid}: 캐싱으로 ${cache_savings:.4f} 절감")
                    
                    # 출력 파일에서 행 수 확인
                    try:
                        df_out = pd.read_excel(out_path_for_history)
                        total_rows = len(df_out)
                        api_rows = len(results_map) if results_map else total_rows
                        cnt = api_rows  # merged 상태에서는 성공 건수 추정
                    except:
                        total_rows = 0
                        api_rows = 0
                        cnt = 0
                    
                    # 히스토리 기록만 수행
                    try:
                        c_at_str = local_job.get("created_at", "")
                        if c_at_str:
                            try:
                                # ISO 형식 파싱 (Z를 +00:00으로 변환)
                                c_at = datetime.fromisoformat(c_at_str.replace('Z', '+00:00'))
                                # 시간대 정보가 있으면 naive로 변환 (datetime.now()와 일치)
                                if c_at.tzinfo is not None:
                                    c_at = c_at.replace(tzinfo=None)
                            except:
                                c_at = datetime.now()
                        else:
                            c_at = datetime.now()
                        finish_dt = datetime.now()
                        elapsed = (finish_dt - c_at).total_seconds()
                        
                        # 히스토리 기록 전에 파일 경로 확인
                        from stage3_run_history import RUN_LOG_PATH
                        self.append_log(f"[DEBUG] 히스토리 파일 경로: {RUN_LOG_PATH}")
                        
                        result = append_run_history(
                            stage="Stage 3 Batch",
                            model_name=model_name,
                            reasoning_effort=local_job.get("effort", "medium"),
                            src_file=src_path,
                            out_file=out_path_for_history,
                            total_rows=total_rows,
                            api_rows=api_rows,
                            elapsed_seconds=elapsed,
                            total_in_tok=batch_in_tok,
                            total_out_tok=batch_out_tok,
                            total_reasoning_tok=0,
                            input_cost_usd=cost_in,
                            output_cost_usd=cost_out,
                            total_cost_usd=cost_total,
                            start_dt=c_at,
                            finish_dt=finish_dt,
                            api_type="batch",
                            batch_id=bid,
                            success_rows=cnt,
                            fail_rows=api_rows - cnt if api_rows > 0 else 0,
                        )
                        if result:
                            # 파일이 실제로 저장되었는지 확인
                            if os.path.exists(RUN_LOG_PATH):
                                self.append_log(f"[INFO] ✅ 실행 이력 기록 완료: {RUN_LOG_PATH} (배치 ID: {bid})")
                            else:
                                self.append_log(f"[ERROR] ❌ 실행 이력 파일이 저장되지 않았습니다: {RUN_LOG_PATH}")
                        else:
                            self.append_log(f"[INFO] 실행 이력 기록 건너뜀: 배치 {bid}는 이미 기록되어 있습니다.")
                    except Exception as hist_e:
                        import traceback
                        error_detail = traceback.format_exc()
                        self.append_log(f"[WARN] 실행 이력 기록 실패")
                        self.append_log(f"[WARN] 오류 상세: {str(hist_e)}")
                        self.append_log(f"[WARN] {error_detail}")
                    
                    continue

                # Batch 상태 및 결과 파일 ID 조회 (기존 방식 사용)
                remote = client.batches.retrieve(bid)
                output_file_id = getattr(remote, "output_file_id", None)
                
                # completed 또는 expired 상태에서 output_file_id가 있으면 다운로드 시도
                if remote.status == "completed":
                    if not output_file_id:
                        self.append_log(f"❌ {bid} 병합 실패: 완료 상태지만 output_file_id를 찾을 수 없습니다.")
                        upsert_batch_job(bid, status=remote.status, output_file_id=None)
                        continue
                elif remote.status == "expired":
                    if not output_file_id:
                        self.append_log(f"⚠️ {bid}: 만료된 배치이며 output_file_id를 찾을 수 없어 건너뜁니다. 수동으로 다운로드해주세요.")
                        upsert_batch_job(bid, status=remote.status, output_file_id=None)
                        continue
                    else:
                        self.append_log(f"ℹ️ {bid}: 배치가 만료되었지만 output_file_id가 있어 다운로드를 시도합니다.")
                else:
                    self.append_log(f"⚠️ {bid}: 아직 completed 또는 expired 상태가 아니어서 병합을 건너뜁니다. (status={remote.status})")
                    # 상태 업데이트
                    upsert_batch_job(bid, status=remote.status, output_file_id=output_file_id)
                    continue
                
                if local_job and local_job.get("src_excel"):
                    src_path = local_job["src_excel"]
                    base, _ = os.path.splitext(src_path)
                    out_jsonl = f"{base}_stage3_batch_output.jsonl"
                    out_excel = f"{base}_stage3_batch_done.xlsx"
                else:
                    out_jsonl = f"output_{bid}.jsonl"
                    out_excel = f"output_{bid}.xlsx"
                    src_path = None

                # 로컬 파일이 없을 때만 다운로드
                if not os.path.exists(out_jsonl):
                    try:
                        content = client.files.content(output_file_id).content
                    except AttributeError:
                        # 신버전 클라이언트 대응
                        file_content = client.files.content(output_file_id)
                        if hasattr(file_content, "read"):
                            content = file_content.read()
                        elif hasattr(file_content, "iter_bytes"):
                            chunks = []
                            for ch in file_content.iter_bytes():
                                chunks.append(ch)
                            content = b"".join(chunks)
                        else:
                            content = file_content  # type: ignore
                    
                    with open(out_jsonl, "wb") as f:
                        f.write(content)
                    self.append_log(f"  [다운로드 완료] {os.path.basename(out_jsonl)}")
                else:
                    self.append_log(f"  [로컬 파일 사용] {os.path.basename(out_jsonl)} (다운로드 건너뜀)")
                
                results_map = {}
                batch_in_tok = 0
                batch_out_tok = 0
                
                with open(out_jsonl, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip(): continue
                        data = json.loads(line)
                        cid = data.get("custom_id")
                        
                        usage = data.get("response", {}).get("body", {}).get("usage", {})
                        batch_in_tok += usage.get("prompt_tokens", 0)
                        batch_out_tok += usage.get("completion_tokens", 0)
                        
                        try:
                            # 새로운 API 응답 형식: response.body.output 배열 사용
                            body = data.get("response", {}).get("body", {})
                            output_array = body.get("output", [])
                            
                            # output 배열에서 type="message"인 항목 찾기
                            text_content = ""
                            for item in output_array:
                                if item.get("type") == "message":
                                    content_array = item.get("content", [])
                                    for content_item in content_array:
                                        if content_item.get("type") == "output_text":
                                            text_content = content_item.get("text", "").strip()
                                            break
                                    if text_content:
                                        break
                            
                            if text_content:
                                results_map[cid] = text_content
                            else:
                                # 기존 형식 호환: choices 사용 (fallback)
                                val = body.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                                if val:
                                    results_map[cid] = val
                                else:
                                    results_map[cid] = ""
                        except Exception as e:
                            self.append_log(f"[WARN] 결과 추출 실패 (custom_id: {cid}): {e}")
                            results_map[cid] = ""
                
                # 비용 계산 (50% 할인)
                model_name = local_job.get("model", "gpt-5-mini") if local_job else "gpt-5-mini"
                pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                cost_in = (batch_in_tok / 1_000_000) * pricing["input"] * 0.5
                cost_out = (batch_out_tok / 1_000_000) * pricing["output"] * 0.5
                cost_total = cost_in + cost_out
                total_cost += cost_total

                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    if "ST3_결과상품명" not in df.columns:
                        df["ST3_결과상품명"] = ""
                    df["ST3_결과상품명"] = df["ST3_결과상품명"].astype(str)
                    cnt = 0
                    for cid, val in results_map.items():
                        try:
                            # custom_id 형식: "row-0", "row-1" 등
                            if cid.startswith("row-"):
                                idx = int(cid.split("-")[1])
                            else:
                                # 기존 형식 호환: "row_0" 등
                                idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                df.at[idx, "ST3_결과상품명"] = val
                                cnt += 1
                        except Exception as e:
                            self.append_log(f"[WARN] custom_id 파싱 실패: {cid} - {e}")
                            pass

                    # 코어 완료 파일(out_excel)을 먼저 저장
                    if not safe_save_excel(df, out_excel):
                        self.append_log(f"[WARN] 기본 완료 파일 저장 실패: {out_excel}")

                    # Stage3 최종 파일명: *_T3_... 형식으로 버전 업
                    try:
                        final_out_path = get_next_version_path(src_path, task_type="text")
                        df_done = pd.read_excel(out_excel)
                        
                        # ST3_결과상품명이 있는 행과 없는 행 분리
                        if "ST3_결과상품명" in df_done.columns:
                            # ST3_결과상품명이 비어있거나 None인 행 찾기
                            df_with_st3 = df_done[df_done["ST3_결과상품명"].notna() & (df_done["ST3_결과상품명"] != '') & (df_done["ST3_결과상품명"].astype(str) != 'nan')].copy()
                            df_no_st3 = df_done[(df_done["ST3_결과상품명"].isna()) | (df_done["ST3_결과상품명"] == '') | (df_done["ST3_결과상품명"].astype(str) == 'nan')].copy()
                        else:
                            # 컬럼이 없으면 모든 행이 ST3_결과상품명 없음으로 처리
                            df_with_st3 = pd.DataFrame()
                            df_no_st3 = df_done.copy()
                        
                        # ST3_결과상품명이 없는 행들을 T3(실패) 버전으로 별도 파일 저장
                        no_st3_path = None
                        if len(df_no_st3) > 0:
                            base_dir = os.path.dirname(src_path)
                            base_name, ext = os.path.splitext(os.path.basename(src_path))
                            
                            # 현재 파일명에서 버전 정보 추출 (예: _T2_I0)
                            # T3(실패) 버전으로 변경
                            name_only_clean = re.sub(r"\([^)]*\)", "", base_name)  # 기존 괄호 제거
                            all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)", name_only_clean, re.IGNORECASE))
                            
                            if all_matches:
                                # 마지막 버전 패턴 사용
                                match = all_matches[-1]
                                original_name = name_only_clean[: match.start()].rstrip("_")
                                current_i = int(match.group(4))
                                # T3(실패) 버전으로 생성
                                new_filename = f"{original_name}_T3_I{current_i}(실패){ext}"
                            else:
                                # 버전 패턴이 없으면 기본적으로 T3(실패)_I0로 생성
                                new_filename = f"{base_name}_T3(실패)_I0{ext}"
                            
                            no_st3_path = os.path.join(base_dir, new_filename)
                            df_no_st3.to_excel(no_st3_path, index=False)
                            
                            self.append_log(f"  T3(실패) 분리 파일: {os.path.basename(no_st3_path)} ({len(df_no_st3)}개 행)")
                            self.append_log(f"  ※ 이 파일은 T3 작업에 실패한 항목입니다.")
                            
                            # 분리된 파일의 런처 상태 업데이트
                            try:
                                no_st3_root_name = get_root_filename(no_st3_path)
                                JobManager.update_status(no_st3_root_name, text_msg="T3(실패)")
                                self.append_log(f"[Launcher] 분리 파일 상태 업데이트: {no_st3_root_name} -> T3(실패)")
                            except Exception as e:
                                self.append_log(f"[Launcher] 분리 파일 상태 업데이트 실패: {e}")
                        
                        # ST3_결과상품명이 있는 행들만 저장
                        if len(df_with_st3) > 0:
                            df_done = df_with_st3
                        else:
                            self.append_log(f"⚠️ {bid}: ST3_결과상품명이 있는 행이 없습니다.")
                        
                        if safe_save_excel(df_done, final_out_path):
                            out_path_for_history = final_out_path
                            # T3 버전 파일 저장 성공 시, 코어가 생성한 중간 파일(_stage3_batch_done) 삭제
                            if out_excel != final_out_path and os.path.exists(out_excel):
                                try:
                                    os.remove(out_excel)
                                    self.append_log(f"[INFO] 중간 파일 삭제: {os.path.basename(out_excel)}")
                                except Exception as e:
                                    self.append_log(f"[WARN] 중간 파일 삭제 실패: {e}")
                        else:
                            out_path_for_history = out_excel
                    except Exception as e:
                        self.append_log(f"[WARN] T3 버전 파일 저장 중 오류: {e}")
                        out_path_for_history = out_excel

                    upsert_batch_job(bid, out_excel=out_path_for_history, status="merged")

                    # History 기록 (naive datetime 기준)
                    try:
                        c_at_str = local_job.get("created_at", "")
                        if c_at_str:
                            try:
                                # ISO 형식 파싱 (Z를 +00:00으로 변환)
                                c_at = datetime.fromisoformat(c_at_str.replace('Z', '+00:00'))
                                # 시간대 정보가 있으면 naive로 변환 (datetime.now()와 일치)
                                if c_at.tzinfo is not None:
                                    c_at = c_at.replace(tzinfo=None)
                            except:
                                c_at = datetime.now()
                        else:
                            c_at = datetime.now()
                        finish_dt = datetime.now()
                        elapsed = (finish_dt - c_at).total_seconds()

                        # 히스토리 기록 전에 파일 경로 확인
                        from stage3_run_history import RUN_LOG_PATH
                        self.append_log(f"[DEBUG] 히스토리 파일 경로: {RUN_LOG_PATH}")
                        
                        result = append_run_history(
                            stage="Stage 3 Batch",
                            model_name=model_name,
                            reasoning_effort=local_job.get("effort", "medium"),
                            src_file=src_path,
                            out_file=out_path_for_history,
                            total_rows=len(df),
                            api_rows=len(results_map),
                            elapsed_seconds=elapsed,
                            total_in_tok=batch_in_tok,
                            total_out_tok=batch_out_tok,
                            total_reasoning_tok=0,
                            input_cost_usd=cost_in,
                            output_cost_usd=cost_out,
                            total_cost_usd=cost_total,
                            start_dt=c_at,
                            finish_dt=finish_dt,
                            api_type="batch",
                            batch_id=bid,
                            success_rows=cnt,
                            fail_rows=len(results_map)-cnt,
                        )
                        if result:
                            # 파일이 실제로 저장되었는지 확인
                            if os.path.exists(RUN_LOG_PATH):
                                self.append_log(f"[INFO] ✅ 실행 이력 기록 완료: {RUN_LOG_PATH} (배치 ID: {bid})")
                            else:
                                self.append_log(f"[ERROR] ❌ 실행 이력 파일이 저장되지 않았습니다: {RUN_LOG_PATH}")
                        else:
                            self.append_log(f"[INFO] 실행 이력 기록 건너뜀: 배치 {bid}는 이미 기록되어 있습니다.")
                    except Exception as hist_e:
                        # 히스토리 기록 실패해도 병합은 성공한 것으로 처리
                        import traceback
                        error_detail = traceback.format_exc()
                        self.append_log(f"[WARN] 실행 이력 기록 실패 (병합은 정상 완료)")
                        self.append_log(f"[WARN] 오류 상세: {str(hist_e)}")
                        self.append_log(f"[WARN] {error_detail}")

                    # 메인 런처 현황판에 Stage3(Text) 완료 상태 기록: T3(생성완료) (img 상태는 변경하지 않음)
                    try:
                        root_name = get_root_filename(src_path)
                        JobManager.update_status(root_name, text_msg="T3(생성완료)")
                        self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T3(생성완료)")
                    except Exception as e:
                        self.append_log(f"[WARN] 현황판 연동 실패: {e}")

                    self.append_log(f"✅ 병합 완료 ({cnt}건): {os.path.basename(out_path_for_history)}")
                    success_cnt += 1
                else:
                    self.append_log(f"⚠️ 원본 없음. JSONL만 저장.")
            except Exception as e:
                self.append_log(f"❌ {bid} 병합 실패: {e}")
        
        self.append_log(f"=== 일괄 병합 끝 (성공: {success_cnt}, 비용: ${total_cost:.4f}) ===")
        self._load_jobs_all()
        messagebox.showinfo("완료", f"{success_cnt}건 병합 완료.\n총 비용: ${total_cost:.4f}")

    def _report_selected_unified(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        jobs = load_batch_jobs()
        targets = [bid for bid in ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "merged"]
        if not targets:
            messagebox.showinfo("알림", "상태가 'merged'인 작업이 없습니다.")
            return
        if messagebox.askyesno("리포트", f"선택한 {len(targets)}건의 통합 리포트를 생성합니까?"):
            t = threading.Thread(target=self._run_report_unified, args=(targets,))
            t.daemon = True
            t.start()

    def _run_report_unified(self, ids):
        self.append_log(f"--- 통합 리포트 생성 ({len(ids)}건) ---")
        jobs = load_batch_jobs()
        all_reps = []
        for bid in ids:
            local_job = next((j for j in jobs if j["batch_id"] == bid), None)
            if not local_job: continue
            src = local_job.get("src_excel")
            out = local_job.get("out_excel")
            if not src or not out or not os.path.exists(src) or not os.path.exists(out): 
                self.append_log(f"⚠️ 파일 누락: {bid}")
                continue
            try:
                df_in = pd.read_excel(src)
                df_out = pd.read_excel(out)
                
                # 원본 상품명 컬럼 찾기 (우선순위: ST1_결과상품명 > 원본상품명)
                orig_col = None
                for col in ["ST1_결과상품명", "원본상품명", "공급사상품명"]:
                    if col in df_in.columns:
                        orig_col = col
                        break
                
                # Stage 3 리포트 로직 (원본과 비교)
                for idx, row in df_in.iterrows():
                    # 원본 상품명
                    orig_name = safe_str(row.get(orig_col, "")) if orig_col else ""
                    
                    # ST3 결과
                    st3 = ""
                    if idx < len(df_out):
                        st3 = safe_str(df_out.iloc[idx].get("ST3_결과상품명", ""))
                    
                    cands = [x for x in st3.split('\n') if x.strip()]
                    first_line = cands[0] if cands else "(생성실패)"
                    
                    # 비교 정보
                    is_same = (orig_name.strip() == first_line.strip()) if orig_name and first_line != "(생성실패)" else False
                    length_diff = len(first_line) - len(orig_name) if orig_name else None
                    
                    all_reps.append({
                        "Batch_ID": bid,
                        "행번호": idx + 2,
                        "상품코드": safe_str(row.get("상품코드", "")),
                        "원본상품명": orig_name,
                        "ST3_첫줄": first_line,
                        "동일여부": "✅ 동일" if is_same else "❌ 다름",
                        "길이차이": length_diff if length_diff is not None else "-",
                        "생성후보수": len(cands),
                        "ST2_길이": len(safe_str(row.get("ST2_JSON", "")))
                    })
            except Exception as e:
                self.append_log(f"❌ 리포트 생성 오류 ({bid}): {e}")
                import traceback
                self.append_log(traceback.format_exc())

        if not all_reps:
            messagebox.showinfo("알림", "데이터 없음")
            return

        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_dir = os.path.dirname(os.path.abspath(__file__))
            report_path = os.path.join(save_dir, f"Stage3_Unified_Report_{ts}.xlsx")
            pd.DataFrame(all_reps).to_excel(report_path, index=False)
            self.append_log(f"📊 리포트 완료: {os.path.basename(report_path)}")
            if messagebox.askyesno("완료", "파일을 여시겠습니까?"):
                os.startfile(report_path)
        except Exception as e:
            self.append_log(f"실패: {e}")
            messagebox.showerror("오류", str(e))

    def _archive_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if ids and messagebox.askyesno("보관", f"{len(ids)}건 휴지통 이동?"):
            archive_batch_job(ids, True)
            self._load_jobs_all()
            self._load_archive_list()

    def _restore_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if ids and messagebox.askyesno("복구", f"{len(ids)}건 복구?"):
            archive_batch_job(ids, False)
            self._load_jobs_all()
            self._load_archive_list()

    def _hard_delete_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if ids and messagebox.askyesno("삭제", "영구 삭제하시겠습니까?"):
            hard_delete_batch_job(ids)
            self._load_archive_list()

    def _on_tree_double_click(self, event):
        """더블클릭 시: 그룹 헤더면 접기/펼치기, 배치면 병합 탭으로 이동"""
        sel = self.tree_active.selection()
        if not sel: return
        
        item = sel[0]
        vals = self.tree_active.item(item)['values']
        batch_id = vals[0] if vals else ""
        
        # 그룹 헤더인 경우: 접기/펼치기 토글
        if not batch_id:
            # 현재 상태 확인
            children = self.tree_active.get_children(item)
            if children:
                # 자식이 있으면 접기/펼치기 토글
                if self.tree_active.item(item, 'open'):
                    self.tree_active.item(item, open=False)
                else:
                    self.tree_active.item(item, open=True)
        else:
            # 개별 배치인 경우: 병합 탭으로 이동
            self.batch_id_var.set(batch_id)
            self.main_tabs.select(self.tab_merge)

    # ----------------------------------------------------
    # Tab 3: Merge (Manual)
    # ----------------------------------------------------
    def _init_tab_merge(self):
        container = ttk.Frame(self.tab_merge, padding=20)
        container.pack(fill='both', expand=True)
        
        # 실패한 청크 재시도 섹션
        f_retry = ttk.LabelFrame(container, text="🔄 실패한 청크 재시도", padding=15)
        f_retry.pack(fill='x', pady=(0, 15))
        
        # 실패한 청크 목록 표시
        f_list = ttk.Frame(f_retry)
        f_list.pack(fill='both', expand=True, pady=(0, 10))
        ttk.Label(f_list, text="실패한 청크 목록:", font=("맑은 고딕", 9, "bold")).pack(anchor='w')
        
        # Treeview로 실패한 청크 목록 표시
        list_frame = ttk.Frame(f_list)
        list_frame.pack(fill='both', expand=True, pady=5)
        
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side='right', fill='y')
        
        self.failed_chunks_tree = ttk.Treeview(list_frame, columns=("chunk_num", "file_name", "error_type"), 
                                                show='headings', height=4, yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.failed_chunks_tree.yview)
        
        self.failed_chunks_tree.heading("chunk_num", text="청크 번호")
        self.failed_chunks_tree.heading("file_name", text="파일명")
        self.failed_chunks_tree.heading("error_type", text="오류 유형")
        
        self.failed_chunks_tree.column("chunk_num", width=80, anchor="center")
        self.failed_chunks_tree.column("file_name", width=300, anchor="w")
        self.failed_chunks_tree.column("error_type", width=150, anchor="center")
        
        self.failed_chunks_tree.pack(fill='both', expand=True)
        
        # 실패 정보 JSON 파일 입력
        f_file = ttk.Frame(f_retry)
        f_file.pack(fill='x', pady=(10, 0))
        ttk.Label(f_file, text="실패 정보 JSON 파일:", font=("맑은 고딕", 9)).pack(side='left')
        self.failed_chunks_file_var = tk.StringVar()
        ttk.Entry(f_file, textvariable=self.failed_chunks_file_var, width=50, font=("Consolas", 9)).pack(side='left', padx=5, fill='x', expand=True)
        btn_select = ttk.Button(f_file, text="📂 찾기", command=self._select_failed_chunks_file)
        btn_select.pack(side='left', padx=5)
        btn_retry = ttk.Button(f_file, text="🔄 실패한 청크 재시도", command=self._retry_failed_chunks, style="Success.TButton")
        btn_retry.pack(side='left', padx=5)
        
        f_in = ttk.LabelFrame(container, text="개별 작업", padding=15)
        f_in.pack(fill='x', pady=(0, 15))
        ttk.Label(f_in, text="Batch ID:").pack(side='left')
        ttk.Entry(f_in, textvariable=self.batch_id_var, width=45, font=("Consolas", 10)).pack(side='left', padx=10)
        
        f_btn = ttk.Frame(container)
        f_btn.pack(fill='x', pady=20)
        ttk.Button(f_btn, text="1. 결과 병합", command=self._start_merge, style="Primary.TButton").pack(fill='x', pady=5)
        ttk.Button(f_btn, text="2. 단일 리포트", command=self._start_diff_report).pack(fill='x', pady=5)

    def _start_merge(self):
        # API 키 확인
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key가 필요합니다.\n상단의 API 설정에서 API Key를 입력해주세요.")
            return
        
        bid = self.batch_id_var.get().strip()
        if not bid:
            messagebox.showwarning("오류", "Batch ID를 입력해주세요.")
            return
        
        t = threading.Thread(target=self._run_merge)
        t.daemon = True
        t.start()

    def _run_merge(self):
        bid = self.batch_id_var.get().strip()
        if bid: self._run_merge_multi([bid])

    def _start_diff_report(self):
        t = threading.Thread(target=self._run_diff_report)
        t.daemon = True
        t.start()

    def _run_diff_report(self):
        bid = self.batch_id_var.get().strip()
        if bid: self._run_report_unified([bid])
    
    def _handle_failed_chunks(self, failed_info_path, failed_chunk_files):
        """실패한 청크가 있을 때 GUI에 자동으로 표시하고 알림"""
        # 실패 정보 파일 경로 자동 설정
        self.failed_chunks_file_var.set(failed_info_path)
        
        # 실패한 청크 목록을 Treeview에 표시
        for item in self.failed_chunks_tree.get_children():
            self.failed_chunks_tree.delete(item)
        
        for failed_info in failed_chunk_files:
            chunk_num = failed_info.get("chunk_num", 0)
            chunk_file = failed_info.get("chunk_file", "")
            error_type = "토큰 제한" if failed_info.get("is_token_limit", False) else "일반 오류"
            file_name = os.path.basename(chunk_file)
            
            self.failed_chunks_tree.insert("", "end", values=(chunk_num, file_name, error_type))
        
        # 알림 메시지 및 탭 전환
        failed_count = len(failed_chunk_files)
        token_limit_count = sum(1 for f in failed_chunk_files if f.get("is_token_limit", False))
        
        msg = f"⚠️ {failed_count}개 청크의 배치 생성이 실패했습니다.\n\n"
        if token_limit_count > 0:
            msg += f"• 토큰 제한 오류: {token_limit_count}개\n"
        msg += f"• 실패 정보가 자동으로 로드되었습니다.\n"
        msg += f"• '3. 개별 병합 (수동)' 탭에서 재시도할 수 있습니다."
        
        messagebox.showwarning("배치 생성 실패", msg)
        
        # 재시도 탭으로 자동 전환
        self.main_tabs.select(self.tab_merge)
    
    def _select_failed_chunks_file(self):
        """실패한 청크 정보 JSON 파일 선택"""
        path = filedialog.askopenfilename(
            title="실패한 청크 정보 JSON 파일 선택",
            filetypes=[("JSON 파일", "*.json"), ("모든 파일", "*.*")]
        )
        if path:
            self.failed_chunks_file_var.set(path)
            # 파일을 선택하면 목록도 업데이트
            self._load_failed_chunks_from_file(path)
    
    def _load_failed_chunks_from_file(self, file_path):
        """실패 정보 JSON 파일을 읽어서 목록에 표시"""
        if not os.path.exists(file_path):
            return
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                failed_chunks = json.load(f)
            
            # 기존 목록 삭제
            for item in self.failed_chunks_tree.get_children():
                self.failed_chunks_tree.delete(item)
            
            # 목록에 추가
            for failed_info in failed_chunks:
                chunk_num = failed_info.get("chunk_num", 0)
                chunk_file = failed_info.get("chunk_file", "")
                error_type = "토큰 제한" if failed_info.get("is_token_limit", False) else "일반 오류"
                file_name = os.path.basename(chunk_file)
                
                self.failed_chunks_tree.insert("", "end", values=(chunk_num, file_name, error_type))
        except Exception as e:
            self.append_log(f"[WARN] 실패 정보 파일 읽기 실패: {e}")
    
    def _retry_failed_chunks(self):
        """실패한 청크 재시도"""
        failed_file = self.failed_chunks_file_var.get().strip()
        if not failed_file:
            messagebox.showwarning("경고", "실패한 청크 정보 JSON 파일을 선택해주세요.")
            return
        
        if not os.path.exists(failed_file):
            messagebox.showerror("오류", f"파일을 찾을 수 없습니다:\n{failed_file}")
            return
        
        if not self.api_key_var.get():
            messagebox.showwarning("경고", "API Key를 입력해주세요.")
            return
        
        try:
            with open(failed_file, "r", encoding="utf-8") as f:
                failed_chunks = json.load(f)
            
            # 목록도 업데이트
            self._load_failed_chunks_from_file(failed_file)
        except Exception as e:
            messagebox.showerror("오류", f"JSON 파일 읽기 실패:\n{e}")
            return
        
        if not failed_chunks:
            messagebox.showinfo("알림", "재시도할 실패한 청크가 없습니다.")
            return
        
        if messagebox.askyesno("확인", f"{len(failed_chunks)}개 실패한 청크를 재시도하시겠습니까?"):
            t = threading.Thread(target=self._run_retry_failed_chunks, args=(failed_chunks,))
            t.daemon = True
            t.start()
    
    def _run_retry_failed_chunks(self, failed_chunks):
        """실패한 청크 재시도 실행"""
        key = self.api_key_var.get().strip()
        import httpx
        timeout = httpx.Timeout(600.0, connect=60.0)  # 10분 타임아웃
        client = OpenAI(api_key=key, timeout=timeout, max_retries=3)
        
        self.append_log(f"[RETRY] 실패한 청크 {len(failed_chunks)}개 재시도 시작...")
        
        retry_batch_ids = []
        for failed_info in failed_chunks:
            chunk_num = failed_info.get("chunk_num", 0)
            chunk_file = failed_info.get("chunk_file", "")
            excel_path = failed_info.get("excel_path", "")
            model_name = failed_info.get("model_name", "gpt-5-mini")
            effort = failed_info.get("effort", "medium")
            batch_group_id = failed_info.get("batch_group_id", "")
            settings_dict = failed_info.get("settings", {})
            
            # settings를 dataclass로 복원 (필요한 경우)
            from stage3_core import Stage3Settings
            if settings_dict:
                try:
                    settings = Stage3Settings(**settings_dict)
                except Exception:
                    # 기본값 사용
                    settings = Stage3Settings()
            else:
                settings = Stage3Settings()
            
            if not os.path.exists(chunk_file):
                self.append_log(f"⚠️ 청크 {chunk_num}: 파일을 찾을 수 없습니다: {chunk_file}")
                continue
            
            self.append_log(f"[RETRY] 청크 {chunk_num} 재시도 중... ({os.path.basename(chunk_file)})")
            
            try:
                batch = self._create_batch_from_jsonl(
                    client=client,
                    jsonl_path=chunk_file,
                    excel_path=excel_path,
                    model_name=model_name,
                    reasoning_effort=effort,
                    settings=settings,
                )
                
                batch_id = batch.id
                retry_batch_ids.append(batch_id)
                self.append_log(f"✅ 청크 {chunk_num} 재시도 성공: {batch_id}")
                
                # 작업 이력 기록
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=excel_path,
                    jsonl_path=chunk_file,
                    model=model_name,
                    effort=effort,
                    status=batch.status,
                    output_file_id=None,
                    batch_group_id=batch_group_id,
                    chunk_index=chunk_num,
                    market=settings.market,
                    strategy=settings.naming_strategy
                )
                
            except Exception as e:
                self.append_log(f"❌ 청크 {chunk_num} 재시도 실패: {e}")
                import traceback
                self.append_log(traceback.format_exc())
        
        if retry_batch_ids:
            self.append_log(f"✅ 재시도 완료: {len(retry_batch_ids)}개 배치 생성됨")
            # 배치 목록 갱신 및 배치 관리 탭으로 자동 전환
            self.after(0, lambda: [
                self._load_jobs_all(),
                self._load_archive_list(),
                self.main_tabs.select(self.tab_manage),  # 배치 관리 탭으로 자동 전환
                messagebox.showinfo("완료", f"{len(retry_batch_ids)}개 청크 재시도 성공:\n{', '.join(retry_batch_ids[:5])}{'...' if len(retry_batch_ids) > 5 else ''}\n\n배치 관리 탭에서 진행 상황을 확인하세요.")
            ])
        else:
            self.append_log(f"⚠️ 재시도된 배치가 없습니다.")
            self.after(0, lambda: messagebox.showwarning("경고", "재시도된 배치가 없습니다."))

if __name__ == "__main__":
    app = Stage3BatchGUI()
    app.mainloop()