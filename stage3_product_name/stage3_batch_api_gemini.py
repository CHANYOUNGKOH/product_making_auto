"""
stage3_batch_api_gemini.py

Stage 3 Batch API 실행 스크립트 (GUI) - Gemini 2.5 Flash-Lite 버전
- 기능: Batch JSONL 생성 -> 업로드 -> 실행 -> 결과 병합 -> 통합 리포트 & 휴지통
- Gemini Batch Prediction API 사용 (비용 약 3.7배 절감)
- Implicit Caching 자동 적용 (System Instruction 동일)
"""

import os
import sys
import json
import threading
import subprocess
import re
import time
from datetime import datetime
from dataclasses import asdict

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText

# Gemini API
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("[WARN] google-genai 패키지가 설치되지 않았습니다. pip install google-genai")

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸 (Stage3: Text)
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)

    while True:
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+(\([^)]+\))?", "", base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base

    base = re.sub(r"\([^)]*\)", "", base)

    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")

    base = base.rstrip("_")

    return base + ext

def get_excel_name_from_path(excel_path: str) -> str:
    if not excel_path:
        return "-"
    return os.path.basename(excel_path)


def get_next_version_path(current_path: str, task_type: str = "text") -> str:
    """현재 파일명을 분석해서 다음 단계의 파일명을 생성합니다."""
    dir_name = os.path.dirname(current_path)
    base_name = os.path.basename(current_path)
    name_only, ext = os.path.splitext(base_name)

    all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)(\([^)]+\))?", name_only, re.IGNORECASE))

    if all_matches:
        match = all_matches[-1]
        current_t = int(match.group(2))
        current_i = int(match.group(4))
        i_suffix = match.group(5) or ""
        original_name = name_only[: match.start()].rstrip("_")
    else:
        original_name = name_only
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


# [필수 의존성] stage3_core_gemini.py / stage3_run_history.py
try:
    from stage3_core_gemini import (
        safe_str,
        Stage3Settings,
        Stage3Request,
        build_stage3_request_from_row,
        # Batch API 관련 함수
        get_gemini_client,
        create_batch_input_jsonl,
        upload_jsonl_file,
        create_batch_job,
        get_batch_status,
        download_batch_results,
        parse_batch_results,
        merge_results_to_excel,
        extract_text_from_response_dict,
        extract_usage_from_response_dict,
        compute_cost_usd,
        is_batch_completed,
        is_batch_succeeded,
        BATCH_STATE_PENDING,
        BATCH_STATE_RUNNING,
        BATCH_STATE_SUCCEEDED,
        BATCH_STATE_FAILED,
        BATCH_STATE_CANCELLED,
        DEFAULT_MODEL,
    )
    GEMINI_CORE_AVAILABLE = True
    from stage3_run_history import append_run_history
    _HISTORY_AVAILABLE = True
except ImportError as e:
    print(f"[WARN] stage3_core_gemini.py 임포트 실패: {e}")
    GEMINI_CORE_AVAILABLE = False
    _HISTORY_AVAILABLE = False
    MODEL_PRICING_USD_PER_MTOK = {}
    def safe_str(x): return str(x) if x is not None else ""
    def append_run_history(*args, **kwargs): pass

# === 기본 설정 ===
API_KEY_FILE = ".gemini_api_key_stage3_batch"
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage3_batch_jobs_gemini.json")

# Gemini 2.5 Flash-Lite Batch 가격 (50% 할인 적용)
MODEL_PRICING_USD_PER_MTOK = {
    "gemini-2.5-flash-lite": {
        "input": 0.05,   # $0.05 / 1M tokens (Batch: 50% discount)
        "output": 0.20,  # $0.20 / 1M tokens (Batch: 50% discount)
    },
    "gemini-2.5-flash-preview-05-20": {
        "input": 0.15,
        "output": 0.60,
    },
    "gemini-2.0-flash": {
        "input": 0.10,
        "output": 0.40,
    },
}

# === API Key 유틸리티 함수 ===
def load_api_key_from_file(path: str = API_KEY_FILE) -> str:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""

def save_api_key_to_file(key: str, path: str = API_KEY_FILE) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")

# --- UI 색상 팔레트 ---
COLOR_BG = "#F8F9FA"
COLOR_WHITE = "#FFFFFF"
COLOR_PRIMARY = "#4285F4"  # Google Blue
COLOR_PRIMARY_HOVER = "#3367D6"
COLOR_SUCCESS = "#34A853"  # Google Green
COLOR_SUCCESS_HOVER = "#2E7D32"
COLOR_DANGER = "#EA4335"   # Google Red
COLOR_DANGER_HOVER = "#C62828"
COLOR_TEXT = "#333333"
COLOR_HEADER = "#E8F0FE"

# Gemini Batch 상태 한글 매핑
JOB_STATE_KR = {
    "JOB_STATE_PENDING": "⏳ 대기중 (큐에서 처리 순서 대기)",
    "JOB_STATE_RUNNING": "🔄 처리중 (요청 처리 진행 중)",
    "JOB_STATE_SUCCEEDED": "✅ 완료 (결과 다운로드 가능)",
    "JOB_STATE_FAILED": "❌ 실패 (오류 발생)",
    "JOB_STATE_CANCELLED": "🚫 취소됨 (사용자 취소)",
}

JOB_STATE_SHORT = {
    "JOB_STATE_PENDING": "⏳ 대기중",
    "JOB_STATE_RUNNING": "🔄 처리중",
    "JOB_STATE_SUCCEEDED": "✅ 완료",
    "JOB_STATE_FAILED": "❌ 실패",
    "JOB_STATE_CANCELLED": "🚫 취소됨",
}

def get_state_display(state: str) -> str:
    """영어 상태를 한글 설명 포함 형태로 변환 (로그용)"""
    kr = JOB_STATE_KR.get(state, "")
    return f"{state} - {kr}" if kr else state

def get_state_short(state: str) -> str:
    """영어 상태를 짧은 한글 형식으로 변환 (트리뷰용)"""
    short = JOB_STATE_SHORT.get(state, "")
    return f"{state} {short}" if short else state

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
            if kwargs.get("status") == "SUCCEEDED" and j.get("status") != "SUCCEEDED":
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
class Stage3BatchGeminiGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 3: Gemini Batch API Manager (Gemini 2.5 Flash-Lite)")
        self.geometry("1250x1000")

        self.api_key_var = tk.StringVar()

        # 파일 변수
        self.src_file_var = tk.StringVar()
        self.skip_exist_var = tk.BooleanVar(value=True)

        # Stage 3 전용 설정 변수 (Gemini용)
        self.model_var = tk.StringVar(value="gemini-2.5-flash-lite")
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
        style.configure("TLabelframe.Label", background=COLOR_BG, foreground="#1A73E8", font=("맑은 고딕", 10, "bold"))

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

        main_container.grid_rowconfigure(1, weight=1)
        main_container.grid_rowconfigure(2, weight=2)
        main_container.grid_columnconfigure(0, weight=1)

        # 1. 상단 API Key
        f_top = ttk.LabelFrame(main_container, text="🔑 Gemini API 설정", padding=10)
        f_top.grid(row=0, column=0, sticky='ew', pady=(0, 10))
        ttk.Label(f_top, text="Gemini API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        entry_key = ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10))
        entry_key.pack(side='left', padx=10)
        ttk.Button(f_top, text="저장", command=self._save_key, style="Primary.TButton").pack(side='left')

        # 2. 메인 탭
        self.main_tabs = ttk.Notebook(main_container)
        self.main_tabs.grid(row=1, column=0, sticky='nsew', pady=5)

        self.tab_create = ttk.Frame(self.main_tabs)
        self.tab_manage = ttk.Frame(self.main_tabs)
        self.tab_merge = ttk.Frame(self.main_tabs)

        self.main_tabs.add(self.tab_create, text=" 1. 배치 생성 & 업로드 ")
        self.main_tabs.add(self.tab_manage, text=" 2. 배치 관리 (목록/병합/리포트) ")
        self.main_tabs.add(self.tab_merge, text=" 3. 개별 병합 (수동) ")

        self._init_tab_create()
        self._init_tab_manage()
        self._init_tab_merge()

        # 3. 로그
        f_log = ttk.LabelFrame(main_container, text="📋 시스템 로그", padding=10)
        f_log.grid(row=2, column=0, sticky='nsew', pady=(10, 0))
        self.log_widget = ScrolledText(f_log, height=25, state='disabled', font=("Consolas", 9), bg="#F1F3F5")
        self.log_widget.pack(fill='both', expand=True)

    def _load_key(self):
        loaded = load_api_key_from_file(API_KEY_FILE)
        if loaded: self.api_key_var.set(loaded)

    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k, API_KEY_FILE)
            messagebox.showinfo("저장", "Gemini API Key 저장 완료")

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

        # Step 2: Stage 3 옵션 (Gemini용)
        f_opt = ttk.LabelFrame(container, text="2. Stage 3 설정 (Gemini)", padding=15)
        f_opt.pack(fill='x', pady=(0, 15))

        opt_row1 = ttk.Frame(f_opt)
        opt_row1.pack(fill='x', pady=3)

        ttk.Label(opt_row1, text="모델:").pack(side='left', padx=(0, 5))
        model_combo = ttk.Combobox(opt_row1, textvariable=self.model_var, width=35, state="readonly")
        model_combo['values'] = [
            "gemini-2.5-flash-lite",
            "gemini-2.5-flash-preview-05-20",
            "gemini-2.0-flash",
        ]
        model_combo.pack(side='left', padx=(0, 20))

        ttk.Label(opt_row1, text="마켓:").pack(side='left', padx=(0, 5))
        market_combo = ttk.Combobox(opt_row1, textvariable=self.market_var, width=12, state="readonly")
        market_combo['values'] = ["네이버 50자", "쿠팡 100자", "기타 50자"]
        market_combo.pack(side='left', padx=(0, 20))

        opt_row2 = ttk.Frame(f_opt)
        opt_row2.pack(fill='x', pady=3)

        ttk.Label(opt_row2, text="글자수:").pack(side='left', padx=(0, 5))
        ttk.Spinbox(opt_row2, from_=20, to=150, textvariable=self.max_len_var, width=5).pack(side='left', padx=(0, 20))

        ttk.Label(opt_row2, text="후보수:").pack(side='left', padx=(0, 5))
        ttk.Spinbox(opt_row2, from_=1, to=30, textvariable=self.num_cand_var, width=5).pack(side='left', padx=(0, 20))

        ttk.Label(opt_row2, text="전략:").pack(side='left', padx=(0, 5))
        strategy_combo = ttk.Combobox(opt_row2, textvariable=self.naming_strategy_var, width=10, state="readonly")
        strategy_combo['values'] = ["통합형", "옵션포함형"]
        strategy_combo.pack(side='left')

        # 옵션 행
        opt_row3 = ttk.Frame(f_opt)
        opt_row3.pack(fill='x', pady=3)
        ttk.Checkbutton(opt_row3, text="ST3_결과상품명 이미 있으면 스킵", variable=self.skip_exist_var).pack(side='left')

        # Step 3: 실행 버튼
        f_btn = ttk.Frame(container)
        f_btn.pack(fill='x', pady=15)
        ttk.Button(f_btn, text="🚀 JSONL 생성 & 배치 업로드", command=self._run_create,
                   style="Success.TButton", width=30).pack(side='left', padx=10)

    def _select_src_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx")])
        if p: self.src_file_var.set(p)

    def _run_create(self):
        if not GEMINI_AVAILABLE:
            messagebox.showerror("오류", "google-genai 패키지가 설치되지 않았습니다.\npip install google-genai")
            return
        if not GEMINI_CORE_AVAILABLE:
            messagebox.showerror("오류", "stage3_core_gemini.py 파일을 찾을 수 없습니다.")
            return

        t = threading.Thread(target=self._thread_create)
        t.daemon = True
        t.start()

    def _thread_create(self):
        """배치 생성 스레드 (Gemini Batch API - 실제 Batch API 사용)"""
        jsonl_path = None

        try:
            key = self.api_key_var.get().strip()
            src = self.src_file_var.get().strip()

            if not key:
                messagebox.showwarning("오류", "Gemini API Key를 입력하세요.")
                return
            if not src or not os.path.exists(src):
                messagebox.showwarning("오류", "유효한 엑셀 파일을 선택하세요.")
                return

            model_name = self.model_var.get()
            skip_exist = self.skip_exist_var.get()

            # 마켓 설정 파싱
            market_sel = self.market_var.get()
            if "네이버" in market_sel:
                market = "네이버"
            elif "쿠팡" in market_sel:
                market = "쿠팡"
            else:
                market = "기타"

            settings = Stage3Settings(
                market=market,
                max_len=self.max_len_var.get(),
                num_candidates=self.num_cand_var.get(),
                naming_strategy=self.naming_strategy_var.get()
            )

            self.append_log(f"[Gemini Batch API] 파일 로드: {src}")

            # JSONL 파일 경로 생성
            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage3_gemini_batch_input.jsonl"

            # 1. JSONL 생성 (core 모듈 사용)
            self.append_log(f"[Step 1/4] JSONL 생성 중...")
            result = create_batch_input_jsonl(
                excel_path=src,
                jsonl_path=jsonl_path,
                settings=settings,
                skip_existing=skip_exist,
            )

            written_count = result["written_count"]
            skipped_count = result["skipped_count"]
            skipped_existing = result["skipped_existing"]

            self.append_log(f"JSONL 생성 완료: {written_count}건 (스킵 {skipped_count}건, 기존결과 {skipped_existing}건)")

            if written_count == 0:
                self.append_log("생성할 요청 없음.")
                return

            # 2. Gemini 클라이언트 생성
            self.append_log(f"[Step 2/4] Gemini 클라이언트 생성...")
            client = get_gemini_client(key)

            # 3. JSONL 파일 업로드
            self.append_log(f"[Step 3/4] JSONL 파일 업로드 중...")
            display_name = f"stage3_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            uploaded_file_name = upload_jsonl_file(
                client=client,
                jsonl_path=jsonl_path,
                display_name=display_name
            )
            self.append_log(f"업로드 완료: {uploaded_file_name}")

            # 4. Batch Job 생성
            self.append_log(f"[Step 4/4] Batch Job 생성 중...")
            batch_info = create_batch_job(
                client=client,
                model_name=model_name,
                src_file_name=uploaded_file_name,
                display_name=display_name
            )

            batch_id = batch_info["name"]
            batch_state = batch_info["state"]

            self.append_log(f"✅ [Gemini Batch API] 배치 생성 완료!")
            self.append_log(f"   배치 ID: {batch_id}")
            self.append_log(f"   상태: {get_state_display(batch_state)}")

            # 배치 정보 저장
            upsert_batch_job(
                batch_id=batch_id,
                src_excel=src,
                jsonl_path=jsonl_path,
                uploaded_file_name=uploaded_file_name,
                model=model_name,
                status=batch_state,
                output_file_id=None,
                market=settings.market,
                strategy=settings.naming_strategy,
                request_counts={"total": written_count, "completed": 0, "failed": 0}
            )

            try:
                root_name = get_root_filename(src)
                JobManager.update_status(root_name, text_msg="T3 (진행중)")
                self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T3 (진행중)")
            except Exception:
                pass

            self.after(0, self._load_jobs_all)
            self.after(0, self._load_archive_list)

            messagebox.showinfo("성공",
                f"Gemini Batch API 작업이 시작되었습니다.\n\n"
                f"배치 ID: {batch_id}\n"
                f"상태: {batch_state}\n"
                f"요청 수: {written_count}건\n\n"
                f"[배치 관리] 탭에서 상태를 확인하고\n"
                f"완료 후 [병합] 버튼을 눌러주세요."
            )

        except Exception as e:
            error_str = str(e)
            self.append_log(f"❌ 에러: {error_str}")
            import traceback
            self.append_log(traceback.format_exc())
            if jsonl_path and os.path.exists(jsonl_path):
                self.append_log(f"   💾 생성된 JSONL 파일: {jsonl_path}")
            messagebox.showerror("에러", error_str)

    # ----------------------------------------------------
    # Tab 2: Manage (관리)
    # ----------------------------------------------------
    def _init_tab_manage(self):
        container = ttk.Frame(self.tab_manage, padding=10)
        container.pack(fill='both', expand=True)

        # 상단 컨트롤
        f_ctrl = ttk.Frame(container)
        f_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_ctrl, text="🔄 목록 새로고침", command=self._load_jobs_all, style="Primary.TButton").pack(side='left', padx=5)
        ttk.Button(f_ctrl, text="🔄 선택 상태 갱신", command=lambda: self._refresh_selected(self.tree_active), style="Primary.TButton").pack(side='left', padx=5)
        ttk.Button(f_ctrl, text="📥 선택 병합", command=self._merge_selected, style="Success.TButton").pack(side='left', padx=5)
        ttk.Button(f_ctrl, text="🗑️ 휴지통", command=self._archive_selected, style="Danger.TButton").pack(side='right', padx=5)

        # 서브 탭
        self.sub_tabs = ttk.Notebook(container)
        self.sub_tabs.pack(fill='both', expand=True)

        self.sub_active = ttk.Frame(self.sub_tabs)
        self.sub_archive = ttk.Frame(self.sub_tabs)

        self.sub_tabs.add(self.sub_active, text=" 진행중/완료 ")
        self.sub_tabs.add(self.sub_archive, text=" 휴지통 ")

        # --- Active Tab UI ---
        cols = ("batch_id", "excel_name", "memo", "status", "created", "completed", "model", "market", "counts", "cost")

        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='tree headings', height=18, selectmode='extended')
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F0F4F8')

        self.tree_active.heading("batch_id", text="배치 ID")
        self.tree_active.heading("excel_name", text="엑셀명")
        self.tree_active.heading("memo", text="메모")
        self.tree_active.heading("status", text="상태")
        self.tree_active.heading("created", text="생성일")
        self.tree_active.heading("completed", text="완료일")
        self.tree_active.heading("model", text="모델")
        self.tree_active.heading("market", text="마켓")
        self.tree_active.heading("counts", text="요청수")
        self.tree_active.heading("cost", text="비용($)")

        self.tree_active.column("#0", width=300, anchor="w")
        self.tree_active.column("batch_id", width=180)
        self.tree_active.column("excel_name", width=180, anchor="w")
        self.tree_active.column("memo", width=120, anchor="w")
        self.tree_active.column("status", width=80, anchor="center")
        self.tree_active.column("created", width=110, anchor="center")
        self.tree_active.column("completed", width=110, anchor="center")
        self.tree_active.column("model", width=200, anchor="center")
        self.tree_active.column("market", width=60, anchor="center")
        self.tree_active.column("counts", width=70, anchor="center")
        self.tree_active.column("cost", width=70, anchor="center")

        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)

        # Archive Tab
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='tree headings', height=18, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2')

        for col in cols:
            self.tree_arch.heading(col, text=self.tree_active.heading(col)["text"])
            self.tree_arch.column(col, width=self.tree_active.column(col)["width"], anchor=self.tree_active.column(col)["anchor"])
        self.tree_arch.column("#0", width=300, anchor="w")

        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 선택 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 선택 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')

        self.tree_arch.pack(fill='both', expand=True)

        self._load_jobs_all()
        self._load_archive_list()

    def _get_selected_ids(self, tree):
        selection = tree.selection()
        ids = []
        for item in selection:
            vals = tree.item(item)['values']
            batch_id = vals[0] if vals else ""
            if batch_id:
                ids.append(batch_id)
        return list(set(ids))

    def _load_jobs_all(self):
        if not hasattr(self, 'tree_active'): return
        for i in self.tree_active.get_children(): self.tree_active.delete(i)
        jobs = load_batch_jobs()
        idx = 0

        for j in jobs:
            if j.get("archived", False): continue
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            excel_name = get_excel_name_from_path(j.get("src_excel", ""))
            memo = j.get("memo", "") or "-"
            cost = f"${j.get('total_cost_usd', 0):.4f}" if j.get('total_cost_usd') else "-"
            tag = 'even' if idx % 2 == 0 else 'odd'

            # 모델명 줄이기
            model_display = j.get("model", "-")
            if "gemini-2.5-flash-lite" in model_display:
                model_display = "gemini-2.5-flash-lite"
            elif "gemini-2.5-flash" in model_display:
                model_display = "gemini-2.5-flash"

            self.tree_active.insert("", "end",
                text=j["batch_id"][:35],
                values=(
                    j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at,
                    model_display, j.get("market", "-"), cnt, cost
                ),
                tags=(tag,))
            idx += 1

    def _load_archive_list(self):
        if not hasattr(self, 'tree_arch'): return
        for i in self.tree_arch.get_children(): self.tree_arch.delete(i)
        jobs = load_batch_jobs()
        idx = 0

        for j in jobs:
            if not j.get("archived", False): continue
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            excel_name = get_excel_name_from_path(j.get("src_excel", ""))
            memo = j.get("memo", "") or "-"
            cost = f"${j.get('total_cost_usd', 0):.4f}" if j.get('total_cost_usd') else "-"
            tag = 'even' if idx % 2 == 0 else 'odd'

            model_display = j.get("model", "-")
            if "gemini-2.5-flash-lite" in model_display:
                model_display = "gemini-2.5-flash-lite"

            self.tree_arch.insert("", "end",
                text=j["batch_id"][:35],
                values=(
                    j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at,
                    model_display, j.get("market", "-"), cnt, cost
                ),
                tags=(tag,))
            idx += 1

    def _refresh_selected(self, tree):
        """선택된 배치들의 상태를 Gemini Batch API에서 조회하여 갱신"""
        ids = self._get_selected_ids(tree)
        if not ids:
            messagebox.showinfo("안내", "갱신할 배치를 선택하세요.")
            return

        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "Gemini API Key를 입력하세요.")
            return

        t = threading.Thread(target=self._thread_refresh, args=(ids, key))
        t.daemon = True
        t.start()

    def _thread_refresh(self, batch_ids, api_key):
        """배치 상태 갱신 스레드 (Gemini Batch API)"""
        try:
            client = get_gemini_client(api_key)

            for batch_id in batch_ids:
                try:
                    self.append_log(f"[갱신] {batch_id} 상태 조회 중...")

                    # Gemini Batch API 상태 조회
                    status_info = get_batch_status(client, batch_id)

                    state = status_info.get("state", "UNKNOWN")
                    total_count = status_info.get("total_count", 0)
                    succeeded_count = status_info.get("succeeded_count", 0)
                    failed_count = status_info.get("failed_count", 0)
                    output_file_name = status_info.get("output_file_name")

                    self.append_log(f"   상태: {get_state_display(state)}")
                    if total_count:
                        self.append_log(f"   진행: {succeeded_count}/{total_count} (실패: {failed_count})")

                    # DB 업데이트
                    update_data = {
                        "status": state,
                        "request_counts": {
                            "total": total_count,
                            "completed": succeeded_count,
                            "failed": failed_count
                        }
                    }

                    if output_file_name:
                        update_data["gemini_output_file"] = output_file_name

                    upsert_batch_job(batch_id=batch_id, **update_data)

                    # 완료된 경우 결과 다운로드
                    if is_batch_succeeded(state) and output_file_name:
                        jobs = load_batch_jobs()
                        job = next((j for j in jobs if j["batch_id"] == batch_id), None)

                        if job and not job.get("output_file_id"):
                            src_excel = job.get("src_excel", "")
                            if src_excel:
                                base, _ = os.path.splitext(src_excel)
                                local_output_path = f"{base}_stage3_gemini_batch_output.jsonl"

                                self.append_log(f"[다운로드] 결과 파일 다운로드 중...")
                                download_batch_results(
                                    client=client,
                                    output_file_name=output_file_name,
                                    local_path=local_output_path
                                )
                                self.append_log(f"   저장 위치: {local_output_path}")

                                # 토큰 사용량 계산
                                results = parse_batch_results(local_output_path)
                                total_input = 0
                                total_output = 0
                                for r in results:
                                    in_tok, out_tok, _ = extract_usage_from_response_dict(r)
                                    total_input += in_tok
                                    total_output += out_tok

                                # 비용 계산
                                model_name = job.get("model", DEFAULT_MODEL)
                                cost_info = compute_cost_usd(model_name, total_input, total_output)
                                total_cost = cost_info["total_cost"] if cost_info else 0

                                upsert_batch_job(
                                    batch_id=batch_id,
                                    output_file_id=local_output_path,
                                    total_input_tokens=total_input,
                                    total_output_tokens=total_output,
                                    total_cost_usd=total_cost
                                )

                                self.append_log(f"   토큰: Input {total_input:,}, Output {total_output:,}")
                                self.append_log(f"   비용: ${total_cost:.4f}")

                except Exception as e:
                    self.append_log(f"❌ {batch_id} 갱신 실패: {e}")

            self.after(0, self._load_jobs_all)
            self.after(0, self._load_archive_list)
            self.append_log("✅ 상태 갱신 완료")

        except Exception as e:
            self.append_log(f"❌ 갱신 오류: {e}")
            import traceback
            self.append_log(traceback.format_exc())

    def _merge_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids:
            messagebox.showinfo("안내", "병합할 배치를 선택하세요.")
            return

        t = threading.Thread(target=self._run_merge, args=(ids,))
        t.daemon = True
        t.start()

    def _run_merge(self, batch_ids):
        """선택된 배치들의 결과를 엑셀에 병합 (Gemini Batch API)"""
        self.append_log(f"병합 시작: {len(batch_ids)}건")

        key = self.api_key_var.get().strip()
        client = None
        if key:
            try:
                client = get_gemini_client(key)
            except Exception:
                pass

        for bid in batch_ids:
            try:
                jobs = load_batch_jobs()
                job = next((j for j in jobs if j["batch_id"] == bid), None)

                if not job:
                    self.append_log(f"❌ {bid}: 작업 정보를 찾을 수 없습니다.")
                    continue

                status = job.get("status", "")
                # SUCCEEDED 또는 JOB_STATE_SUCCEEDED 둘 다 허용
                if status not in ["SUCCEEDED", "JOB_STATE_SUCCEEDED", "merged"]:
                    self.append_log(f"⏭️ {bid}: 완료되지 않은 작업입니다. (상태: {status})")
                    continue

                src_excel = job.get("src_excel")
                if not src_excel or not os.path.exists(src_excel):
                    self.append_log(f"❌ {bid}: 원본 엑셀 파일을 찾을 수 없습니다.")
                    continue

                # 결과 파일 확인 (로컬에 없으면 다운로드)
                output_file = job.get("output_file_id")
                gemini_output_file = job.get("gemini_output_file")

                if not output_file or not os.path.exists(output_file):
                    # Gemini에서 결과 파일 다운로드 필요
                    if gemini_output_file and client:
                        base, _ = os.path.splitext(src_excel)
                        output_file = f"{base}_stage3_gemini_batch_output.jsonl"

                        self.append_log(f"[다운로드] {bid} 결과 파일 다운로드 중...")
                        try:
                            download_batch_results(
                                client=client,
                                output_file_name=gemini_output_file,
                                local_path=output_file
                            )
                            upsert_batch_job(batch_id=bid, output_file_id=output_file)
                        except Exception as e:
                            self.append_log(f"❌ 다운로드 실패: {e}")
                            continue
                    else:
                        self.append_log(f"❌ {bid}: 결과 파일을 찾을 수 없습니다. 상태 갱신을 먼저 실행하세요.")
                        continue

                # 결과 파싱 및 병합 (core 모듈 사용)
                self.append_log(f"[병합] {bid} 결과 파일 처리 중...")
                results = parse_batch_results(output_file)

                if not results:
                    self.append_log(f"❌ {bid}: 결과 데이터가 없습니다.")
                    continue

                # 출력 파일 경로 생성
                out_excel = get_next_version_path(src_excel, task_type="text")

                # 병합 실행
                cnt, total_input, total_output = merge_results_to_excel(
                    excel_path=src_excel,
                    results=results,
                    output_path=out_excel
                )

                # 비용 계산
                model_name = job.get("model", DEFAULT_MODEL)
                cost_info = compute_cost_usd(model_name, total_input, total_output)
                total_cost = cost_info["total_cost"] if cost_info else 0

                self.append_log(f"✅ {bid} 병합 완료: {cnt}건 -> {os.path.basename(out_excel)}")
                self.append_log(f"   토큰: Input {total_input:,}, Output {total_output:,}")
                self.append_log(f"   비용: ${total_cost:.4f}")

                upsert_batch_job(
                    batch_id=bid,
                    status="merged",
                    out_excel=out_excel,
                    total_input_tokens=total_input,
                    total_output_tokens=total_output,
                    total_cost_usd=total_cost
                )

                try:
                    root_name = get_root_filename(src_excel)
                    JobManager.update_status(root_name, text_msg="T3(생성완료)")
                except Exception:
                    pass

            except Exception as e:
                self.append_log(f"❌ {bid} 병합 실패: {e}")
                import traceback
                self.append_log(traceback.format_exc())

        self.after(0, self._load_jobs_all)
        self.after(0, self._load_archive_list)
        self.append_log("병합 작업 완료")

    def _archive_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        if messagebox.askyesno("확인", f"{len(ids)}건을 휴지통으로 이동하시겠습니까?"):
            archive_batch_job(ids, archive=True)
            self._load_jobs_all()
            self._load_archive_list()

    def _restore_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if not ids: return
        archive_batch_job(ids, archive=False)
        self._load_jobs_all()
        self._load_archive_list()

    def _hard_delete_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if not ids: return
        if messagebox.askyesno("경고", f"{len(ids)}건을 영구 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다."):
            hard_delete_batch_job(ids)
            self._load_archive_list()

    # ----------------------------------------------------
    # Tab 3: Manual Merge (수동 병합)
    # ----------------------------------------------------
    def _init_tab_merge(self):
        container = ttk.Frame(self.tab_merge, padding=20)
        container.pack(fill='both', expand=True)

        ttk.Label(container, text="배치 ID:").pack(anchor='w')
        ttk.Entry(container, textvariable=self.batch_id_var, width=60).pack(fill='x', pady=5)

        ttk.Button(container, text="병합 실행", command=self._manual_merge, style="Success.TButton").pack(pady=10)

    def _manual_merge(self):
        bid = self.batch_id_var.get().strip()
        if not bid:
            messagebox.showwarning("오류", "배치 ID를 입력하세요.")
            return

        t = threading.Thread(target=self._run_merge, args=([bid],))
        t.daemon = True
        t.start()


# ========================================================
# 메인 실행
# ========================================================
if __name__ == "__main__":
    app = Stage3BatchGeminiGUI()
    app.mainloop()
