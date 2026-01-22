"""
stage2_batch_api_gemini.py

Stage 2 Batch API 실행 스크립트 (GUI) - Gemini 2.5 Flash-Lite 버전
- 기능: Batch JSONL 생성 -> 업로드 -> 실행 -> 결과 병합 -> 통합 리포트 & 휴지통
- Gemini Batch Prediction API 사용 (비용 50% 절감)
- Vision API 지원 (이미지 inline_data로 포함)
"""

import os
import sys
import json
import threading
import re
import time
from datetime import datetime

# ========================================================
# 경로 강제 설정
# ========================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)
# ========================================================

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# Gemini API
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("[WARN] google-genai 패키지가 설치되지 않았습니다. pip install google-genai")

# PIL for image processing
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("[WARN] Pillow 패키지가 설치되지 않았습니다. pip install Pillow")

# Stage 2 Core import (Batch API 함수 포함)
try:
    from stage2_core_gemini import (
        safe_str,
        fmt_safe,
        Stage2Request,
        build_stage2_request_from_row,
        STAGE2_SYSTEM_INSTRUCTION,
        STAGE2_USER_PROMPT_TEMPLATE,
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
        get_detail_image_cols,
        BATCH_STATE_PENDING,
        BATCH_STATE_RUNNING,
        BATCH_STATE_SUCCEEDED,
        BATCH_STATE_FAILED,
        BATCH_STATE_CANCELLED,
        DEFAULT_MODEL,
    )
    GEMINI_CORE_AVAILABLE = True
except ImportError as e:
    print(f"[WARN] stage2_core_gemini.py 임포트 실패: {e}")
    GEMINI_CORE_AVAILABLE = False

    def safe_str(x):
        if x is None:
            return ""
        try:
            if pd.isna(x):
                return ""
        except:
            pass
        return str(x).strip()

    def fmt_safe(x):
        s = safe_str(x)
        return s.replace("{", "{{").replace("}", "}}")

# ========================================================
# 메인 런처 연동용 유틸
# ========================================================
def get_root_filename(filename):
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    while True:
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+(\([^)]+\))?", "", base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base
    base = re.sub(r"\([^)]*\)", "", base)
    base = base.rstrip("_")
    return base + ext


def get_next_version_path(current_path, task_type="text"):
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
        for d in [current_dir, os.path.join(current_dir, ".."), os.path.join(current_dir, "..", "..")]:
            target = os.path.join(os.path.abspath(d), "job_history.json")
            if os.path.exists(target):
                cls.DB_FILE = target
                return target
        cls.DB_FILE = os.path.join(os.path.abspath(os.path.join(current_dir, "..")), "job_history.json")
        return cls.DB_FILE

    @classmethod
    def load_jobs(cls):
        db_path = cls.find_db_path()
        if not os.path.exists(db_path):
            return {}
        try:
            with open(db_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None):
        db_path = cls.find_db_path()
        data = cls.load_jobs()
        now = datetime.now().strftime("%m-%d %H:%M")
        if filename not in data:
            data[filename] = {
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "text_status": "대기", "text_time": "-",
                "image_status": "대기", "image_time": "-", "memo": ""
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
        except:
            pass


def safe_save_excel(df, path):
    while True:
        try:
            df.to_excel(path, index=False)
            return True
        except PermissionError:
            if not messagebox.askretrycancel("저장 실패", f"엑셀 파일이 열려있습니다!\n파일을 닫고 '다시 시도'를 눌러주세요."):
                return False
        except Exception as e:
            messagebox.showerror("오류", f"저장 오류: {e}")
            return False


# ========================================================
# 기본 설정
# ========================================================
API_KEY_FILE = ".gemini_api_key_stage2_batch"
BATCH_JOBS_FILE = os.path.join(CURRENT_DIR, "stage2_batch_jobs_gemini.json")

MODEL_PRICING_USD_PER_MTOK = {
    "gemini-2.5-flash-lite": {"input": 0.05, "output": 0.20},
    "gemini-2.5-flash-preview-05-20": {"input": 0.15, "output": 0.60},
    "gemini-2.0-flash": {"input": 0.10, "output": 0.40},
}

# UI Colors
COLOR_BG = "#F8F9FA"
COLOR_WHITE = "#FFFFFF"
COLOR_PRIMARY = "#4285F4"
COLOR_PRIMARY_HOVER = "#3367D6"
COLOR_SUCCESS = "#34A853"
COLOR_SUCCESS_HOVER = "#2E7D32"
COLOR_DANGER = "#EA4335"
COLOR_DANGER_HOVER = "#C62828"
COLOR_TEXT = "#333333"
COLOR_HEADER = "#E8F0FE"

# Gemini Batch 상태 한글 매핑
JOB_STATE_KR = {
    "JOB_STATE_PENDING": "⏳ 대기중",
    "JOB_STATE_RUNNING": "🔄 처리중",
    "JOB_STATE_SUCCEEDED": "✅ 완료",
    "JOB_STATE_FAILED": "❌ 실패",
    "JOB_STATE_CANCELLED": "🚫 취소됨",
}


def get_state_short(state: str) -> str:
    """영어 상태를 짧은 한글 형식으로 변환"""
    return JOB_STATE_KR.get(state, state)


def load_api_key_from_file(path=API_KEY_FILE):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except:
            return ""
    return ""


def save_api_key_to_file(key, path=API_KEY_FILE):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except:
        pass


# ========================================================
# 배치 잡 관리
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
    except:
        pass


def upsert_batch_job(batch_id, **kwargs):
    jobs = load_batch_jobs()
    now_str = datetime.now().isoformat()
    found = False
    for j in jobs:
        if j["batch_id"] == batch_id:
            if kwargs.get("status") == "JOB_STATE_SUCCEEDED" and j.get("status") != "JOB_STATE_SUCCEEDED":
                j["completed_at"] = now_str
            j.update(kwargs)
            j["updated_at"] = now_str
            found = True
            break
    if not found:
        jobs.insert(0, {"batch_id": batch_id, "created_at": now_str, "updated_at": now_str, "completed_at": "", "archived": False, **kwargs})
    save_batch_jobs(jobs)


def archive_batch_job(batch_ids, archive=True):
    if isinstance(batch_ids, str):
        batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    for j in jobs:
        if j["batch_id"] in batch_ids:
            j["archived"] = archive
    save_batch_jobs(jobs)


def hard_delete_batch_job(batch_ids):
    if isinstance(batch_ids, str):
        batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    jobs = [j for j in jobs if j["batch_id"] not in batch_ids]
    save_batch_jobs(jobs)


# ========================================================
# GUI Class
# ========================================================
class Stage2BatchGeminiGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 2: Gemini Batch API Manager (Vision - Gemini 2.5 Flash-Lite)")
        self.geometry("1250x950")

        self.api_key_var = tk.StringVar()
        self.src_file_var = tk.StringVar()
        self.model_var = tk.StringVar(value="gemini-2.5-flash-lite")
        self.skip_exist_var = tk.BooleanVar(value=True)
        self.max_images_var = tk.IntVar(value=10)
        self.resize_mode_var = tk.StringVar(value="384")  # 384: 토큰 절약, 768: 고품질
        self.batch_id_var = tk.StringVar()

        # 상세이미지 통계 정보 저장
        self.detail_image_stats = {
            "max_columns": 0,
            "row_counts": {},
        }

        # 폴링 스레드 관리
        self.polling_threads = {}

        self._configure_styles()
        self._init_ui()
        self._load_key()

    def _configure_styles(self):
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        self.configure(background=COLOR_BG)

        style.configure("TFrame", background=COLOR_BG)
        style.configure("TLabel", background=COLOR_BG, foreground=COLOR_TEXT, font=("맑은 고딕", 10))
        style.configure("TLabelframe", background=COLOR_BG, bordercolor="#D0D7DE")
        style.configure("TLabelframe.Label", background=COLOR_BG, foreground="#1A73E8", font=("맑은 고딕", 10, "bold"))
        style.configure("TNotebook", background=COLOR_BG, borderwidth=0)
        style.configure("TNotebook.Tab", background="#E1E4E8", padding=[12, 5], font=("맑은 고딕", 10))
        style.map("TNotebook.Tab", background=[("selected", COLOR_WHITE)], foreground=[("selected", COLOR_PRIMARY)])
        style.configure("Treeview", background=COLOR_WHITE, fieldbackground=COLOR_WHITE, font=("맑은 고딕", 9), rowheight=28)
        style.configure("Treeview.Heading", background=COLOR_HEADER, foreground="#333", font=("맑은 고딕", 9, "bold"))
        style.map("Treeview", background=[('selected', '#CCE5FF')], foreground=[('selected', 'black')])
        style.configure("TButton", font=("맑은 고딕", 9), padding=5)
        style.configure("Primary.TButton", background=COLOR_PRIMARY, foreground="white")
        style.map("Primary.TButton", background=[("active", COLOR_PRIMARY_HOVER)])
        style.configure("Success.TButton", background=COLOR_SUCCESS, foreground="white")
        style.map("Success.TButton", background=[("active", COLOR_SUCCESS_HOVER)])
        style.configure("Danger.TButton", background=COLOR_DANGER, foreground="white")
        style.map("Danger.TButton", background=[("active", COLOR_DANGER_HOVER)])

    def _init_ui(self):
        main_container = ttk.Frame(self, padding=15)
        main_container.pack(fill='both', expand=True)
        main_container.grid_rowconfigure(1, weight=1)
        main_container.grid_rowconfigure(2, weight=2)
        main_container.grid_columnconfigure(0, weight=1)

        # API Key
        f_top = ttk.LabelFrame(main_container, text="🔑 Gemini API 설정", padding=10)
        f_top.grid(row=0, column=0, sticky='ew', pady=(0, 10))
        ttk.Label(f_top, text="Gemini API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10)).pack(side='left', padx=10)
        ttk.Button(f_top, text="저장", command=self._save_key, style="Primary.TButton").pack(side='left')

        # Tabs
        self.main_tabs = ttk.Notebook(main_container)
        self.main_tabs.grid(row=1, column=0, sticky='nsew', pady=5)

        self.tab_create = ttk.Frame(self.main_tabs)
        self.tab_manage = ttk.Frame(self.main_tabs)
        self.tab_merge = ttk.Frame(self.main_tabs)

        self.main_tabs.add(self.tab_create, text=" 1. 배치 생성 & 실행 ")
        self.main_tabs.add(self.tab_manage, text=" 2. 배치 관리 (목록/병합) ")
        self.main_tabs.add(self.tab_merge, text=" 3. 개별 병합 (수동) ")

        self._init_tab_create()
        self._init_tab_manage()
        self._init_tab_merge()

        # Log
        f_log = ttk.LabelFrame(main_container, text="📋 시스템 로그", padding=10)
        f_log.grid(row=2, column=0, sticky='nsew', pady=(10, 0))
        self.log_widget = ScrolledText(f_log, height=25, state='disabled', font=("Consolas", 9), bg="#F1F3F5")
        self.log_widget.pack(fill='both', expand=True)

    def _load_key(self):
        loaded = load_api_key_from_file(API_KEY_FILE)
        if loaded:
            self.api_key_var.set(loaded)

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
    # Tab 1: Create
    # ----------------------------------------------------
    def _init_tab_create(self):
        container = ttk.Frame(self.tab_create, padding=20)
        container.pack(fill='both', expand=True)

        # File
        f_file = ttk.LabelFrame(container, text="1. 작업 대상 파일 (ST1_결과상품명, 이미지대, 상세이미지 포함)", padding=15)
        f_file.pack(fill='x', pady=(0, 15))
        ttk.Entry(f_file, textvariable=self.src_file_var, font=("맑은 고딕", 10)).pack(side='left', fill='x', expand=True)
        ttk.Button(f_file, text="📂 파일 찾기", command=self._select_src_file).pack(side='right', padx=5)

        # Options
        f_opt = ttk.LabelFrame(container, text="2. Stage 2 설정 (Gemini Vision Batch)", padding=15)
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

        ttk.Label(opt_row1, text="최대 이미지 수:").pack(side='left', padx=(0, 5))
        ttk.Spinbox(opt_row1, from_=1, to=100, textvariable=self.max_images_var, width=5).pack(side='left', padx=(0, 20))

        ttk.Label(opt_row1, text="가로 리사이즈:").pack(side='left', padx=(0, 5))
        resize_combo = ttk.Combobox(opt_row1, textvariable=self.resize_mode_var, width=6, state="readonly")
        resize_combo['values'] = ["384", "768"]
        resize_combo.pack(side='left')
        ttk.Label(opt_row1, text="px").pack(side='left', padx=(2, 0))

        opt_row2 = ttk.Frame(f_opt)
        opt_row2.pack(fill='x', pady=3)
        ttk.Checkbutton(opt_row2, text="ST2_JSON 이미 있으면 스킵", variable=self.skip_exist_var).pack(side='left')
        ttk.Label(opt_row2, text="  (상세설명: 384=토큰절약 40%, 768=고품질)", foreground="#666").pack(side='left')

        # 상세이미지 통계 표시
        fr_stats = ttk.Frame(f_opt)
        fr_stats.pack(fill='x', pady=(8, 3))

        self.detail_stats_frame = tk.Frame(fr_stats, bg="#E3F2FD", relief="solid", bd=1, padx=10, pady=8)
        self.detail_stats_frame.pack(fill='x', padx=5)

        self.detail_stats_label = tk.Label(
            self.detail_stats_frame,
            text="📊 엑셀 파일을 선택하면 상세이미지 통계가 표시됩니다.",
            font=("맑은 고딕", 10, "bold"),
            bg="#E3F2FD",
            fg="#1976D2",
            anchor="w",
            justify="left"
        )
        self.detail_stats_label.pack(side='left', fill='x', expand=True)

        btn_stats = ttk.Button(self.detail_stats_frame, text="📊 통계 상세", command=self._show_detail_stats_popup, width=12)
        btn_stats.pack(side='right', padx=(10, 0))

        # Spinbox 값 변경 시 통계 업데이트
        self.max_images_var.trace('w', lambda *args: self._update_detail_stats_display())

        # Run
        f_btn = ttk.Frame(container)
        f_btn.pack(fill='x', pady=15)
        ttk.Button(f_btn, text="🚀 Gemini Vision 배치 생성 & 실행", command=self._run_create,
                   style="Success.TButton", width=35).pack(side='left', padx=10)

    def _select_src_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx")])
        if p:
            self._analyze_detail_images(p)
            self.src_file_var.set(p)

    def _analyze_detail_images(self, excel_path):
        """엑셀 파일의 상세이미지 컬럼 통계를 분석합니다."""
        try:
            df = pd.read_excel(excel_path)

            detail_cols = [c for c in df.columns if str(c).startswith("상세이미지_")]
            if detail_cols:
                def sort_key(c):
                    try:
                        return int(str(c).split("_")[1])
                    except Exception:
                        return 9999
                detail_cols.sort(key=sort_key)
                max_col_num = max([int(str(c).split("_")[1]) for c in detail_cols if str(c).split("_")[1].isdigit()], default=0)
            else:
                max_col_num = 0

            row_counts = {}
            for idx, row in df.iterrows():
                count = 0
                for col in detail_cols:
                    val = row.get(col, "")
                    if pd.notna(val) and str(val).strip():
                        count += 1
                if count > 0:
                    row_counts[count] = row_counts.get(count, 0) + 1

            self.detail_image_stats = {
                "max_columns": max_col_num,
                "row_counts": row_counts,
            }

            self._update_detail_stats_display()

            if max_col_num > 0:
                max_limit = self.max_images_var.get()
                rows_exceeding = sum(count for col_count, count in row_counts.items() if col_count > max_limit)
                if rows_exceeding > 0:
                    messagebox.showinfo(
                        "상세이미지 통계",
                        f"📊 엑셀 파일 분석 완료\n\n"
                        f"최대 상세이미지 컬럼: {max_col_num}개\n"
                        f"현재 설정값: {max_limit}개\n"
                        f"⚠️ 설정값 초과 행: {rows_exceeding}개\n\n"
                        f"상세 통계는 옵션 설정 영역에서 확인할 수 있습니다."
                    )

        except Exception as e:
            self.append_log(f"[WARN] 상세이미지 통계 분석 실패: {e}")
            self.detail_image_stats = {"max_columns": 0, "row_counts": {}}
            self._update_detail_stats_display()

    def _update_detail_stats_display(self):
        """상세이미지 통계를 화면에 표시합니다."""
        stats = self.detail_image_stats
        max_cols = stats.get("max_columns", 0)
        row_counts = stats.get("row_counts", {})
        max_limit = self.max_images_var.get()

        if max_cols == 0:
            self.detail_stats_label.config(
                text="📊 엑셀 파일을 선택하면 상세이미지 통계가 표시됩니다.",
                fg="#666",
                bg="#E3F2FD"
            )
            if hasattr(self, 'detail_stats_frame'):
                self.detail_stats_frame.config(bg="#E3F2FD")
            return

        rows_exceeding_limit = sum(count for col_count, count in row_counts.items() if col_count > max_limit)

        if rows_exceeding_limit > 0:
            stats_text = f"📊 최대 상세이미지: {max_cols}개 | ⚠️ {max_limit}개 초과 행: {rows_exceeding_limit}개"
            color = "#d32f2f"
            bg_color = "#FFEBEE"
        else:
            stats_text = f"📊 최대 상세이미지: {max_cols}개 | ✅ 모든 행이 {max_limit}개 이하"
            color = "#388e3c"
            bg_color = "#E8F5E9"

        if row_counts:
            max_row_count = max(row_counts.keys(), default=0)
            if max_row_count > max_limit:
                stats_text += f" (최대 {max_row_count}개)"

        self.detail_stats_label.config(text=stats_text, fg=color, bg=bg_color)
        if hasattr(self, 'detail_stats_frame'):
            self.detail_stats_frame.config(bg=bg_color)

    def _show_detail_stats_popup(self):
        """상세이미지 통계를 팝업 창으로 표시합니다."""
        stats = self.detail_image_stats
        max_cols = stats.get("max_columns", 0)
        row_counts = stats.get("row_counts", {})
        max_limit = self.max_images_var.get()

        if max_cols == 0:
            messagebox.showinfo("상세이미지 통계", "엑셀 파일을 먼저 선택해주세요.")
            return

        msg_lines = [
            "📊 상세이미지 통계",
            "",
            f"최대 상세이미지 컬럼 개수: {max_cols}개",
            f"현재 설정값: {max_limit}개",
            "",
        ]

        rows_exceeding = sum(count for col_count, count in row_counts.items() if col_count > max_limit)
        if rows_exceeding > 0:
            msg_lines.append(f"⚠️ {max_limit}개 초과 행: {rows_exceeding}개")
            msg_lines.append("")

        if row_counts:
            msg_lines.append("상세이미지 개수별 행 분포:")
            sorted_counts = sorted(row_counts.items(), key=lambda x: x[0], reverse=True)
            for col_count, row_count in sorted_counts[:20]:
                marker = "⚠️" if col_count > max_limit else "  "
                msg_lines.append(f"  {marker} {col_count}개 상세이미지: {row_count}개 행")
            if len(sorted_counts) > 20:
                msg_lines.append(f"  ... (총 {len(sorted_counts)}개 그룹)")

        messagebox.showinfo("상세이미지 통계", "\n".join(msg_lines))

    def _run_create(self):
        if not GEMINI_AVAILABLE:
            messagebox.showerror("오류", "google-genai 패키지가 설치되지 않았습니다.")
            return
        if not PIL_AVAILABLE:
            messagebox.showerror("오류", "Pillow 패키지가 설치되지 않았습니다.")
            return
        if not GEMINI_CORE_AVAILABLE:
            messagebox.showerror("오류", "stage2_core_gemini.py를 찾을 수 없습니다.")
            return

        t = threading.Thread(target=self._thread_create)
        t.daemon = True
        t.start()

    def _thread_create(self):
        """Gemini Vision Batch 생성 및 실행"""
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
            max_images = self.max_images_var.get()
            resize_max = int(self.resize_mode_var.get())

            self.append_log(f"[Batch] 파일 로드: {src}")
            self.append_log(f"[Batch] 이미지 리사이즈: {resize_max}px, 최대 이미지: {max_images}개")

            # 1. JSONL 생성
            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage2_batch_input.jsonl"

            self.append_log("[Batch] JSONL 파일 생성 중 (이미지 인코딩 포함)...")
            result = create_batch_input_jsonl(
                excel_path=src,
                jsonl_path=jsonl_path,
                max_images=max_images,
                max_width=resize_max,
                skip_existing=skip_exist,
                log_func=self.append_log,
            )

            if result["written_count"] == 0:
                self.append_log("[Batch] 처리할 요청이 없습니다.")
                return

            self.append_log(f"[Batch] JSONL 생성 완료: {result['written_count']}건")

            # 2. Gemini 클라이언트 생성
            client = get_gemini_client(key)

            # 3. JSONL 파일 업로드
            self.append_log("[Batch] JSONL 파일 업로드 중...")
            uploaded_file_name = upload_jsonl_file(client, jsonl_path)
            self.append_log(f"[Batch] 업로드 완료: {uploaded_file_name}")

            # 4. 배치 작업 생성
            self.append_log(f"[Batch] 배치 작업 생성 중 (모델: {model_name})...")
            batch_info = create_batch_job(
                client=client,
                model_name=model_name,
                src_file_name=uploaded_file_name,
                display_name=f"stage2_{os.path.basename(src)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

            batch_name = batch_info["name"]
            self.append_log(f"[Batch] 배치 작업 생성 완료: {batch_name}")
            self.append_log(f"[Batch] 상태: {get_state_short(batch_info['state'])}")

            # 5. 로컬 DB에 저장
            upsert_batch_job(
                batch_id=batch_name,
                src_excel=src,
                jsonl_path=jsonl_path,
                model=model_name,
                status=batch_info["state"],
                request_count=result["written_count"],
            )

            try:
                root_name = get_root_filename(src)
                JobManager.update_status(root_name, text_msg="T2 (배치 진행중)")
            except:
                pass

            # 6. 폴링 시작
            self._start_polling(batch_name, key, src)

            messagebox.showinfo("성공", f"Gemini Vision Batch 작업 시작됨\n\nBatch ID: {batch_name}\n\n배치 완료까지 수 분~수 시간 소요될 수 있습니다.")
            self.after(0, self._load_jobs_all)
            self.after(0, self._load_archive_list)

        except Exception as e:
            self.append_log(f"❌ 에러: {e}")
            import traceback
            self.append_log(traceback.format_exc())
            messagebox.showerror("에러", str(e))

    def _start_polling(self, batch_name, api_key, src_excel):
        """배치 상태 폴링 시작"""
        if batch_name in self.polling_threads:
            return

        def poll():
            try:
                client = get_gemini_client(api_key)
                poll_interval = 30  # 30초마다 폴링

                while True:
                    status = get_batch_status(client, batch_name)
                    state = status.get("state", "")

                    self.after(0, lambda s=state: self.append_log(f"[Batch] {batch_name[:30]}... 상태: {get_state_short(s)}"))

                    # 로컬 DB 업데이트
                    upsert_batch_job(
                        batch_id=batch_name,
                        status=state,
                        total_count=status.get("total_count", 0),
                        succeeded_count=status.get("succeeded_count", 0),
                        failed_count=status.get("failed_count", 0),
                    )
                    self.after(0, self._load_jobs_all)

                    if is_batch_completed(state):
                        if is_batch_succeeded(state):
                            self.after(0, lambda: self.append_log(f"[Batch] ✅ 배치 완료: {batch_name[:30]}..."))
                            # 결과 파일 정보 저장
                            output_file = status.get("output_file_name", "")
                            if output_file:
                                upsert_batch_job(
                                    batch_id=batch_name,
                                    output_file_name=output_file,
                                )
                            self.after(0, self._load_jobs_all)
                        else:
                            self.after(0, lambda: self.append_log(f"[Batch] ❌ 배치 실패/취소: {batch_name[:30]}..."))
                        break

                    time.sleep(poll_interval)

            except Exception as e:
                self.after(0, lambda: self.append_log(f"[Batch] 폴링 오류: {e}"))
            finally:
                if batch_name in self.polling_threads:
                    del self.polling_threads[batch_name]

        thread = threading.Thread(target=poll, daemon=True)
        self.polling_threads[batch_name] = thread
        thread.start()

    # ----------------------------------------------------
    # Tab 2: Manage
    # ----------------------------------------------------
    def _init_tab_manage(self):
        container = ttk.Frame(self.tab_manage, padding=10)
        container.pack(fill='both', expand=True)

        f_ctrl = ttk.Frame(container)
        f_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_ctrl, text="🔄 목록 새로고침", command=self._load_jobs_all, style="Primary.TButton").pack(side='left', padx=5)
        ttk.Button(f_ctrl, text="📥 선택 다운로드 & 병합", command=self._merge_selected, style="Success.TButton").pack(side='left', padx=5)
        ttk.Button(f_ctrl, text="🔍 상태 확인", command=self._check_selected_status, style="Primary.TButton").pack(side='left', padx=5)
        ttk.Button(f_ctrl, text="🗑️ 휴지통", command=self._archive_selected, style="Danger.TButton").pack(side='right', padx=5)

        self.sub_tabs = ttk.Notebook(container)
        self.sub_tabs.pack(fill='both', expand=True)

        self.sub_active = ttk.Frame(self.sub_tabs)
        self.sub_archive = ttk.Frame(self.sub_tabs)
        self.sub_tabs.add(self.sub_active, text=" 진행중/완료 ")
        self.sub_tabs.add(self.sub_archive, text=" 휴지통 ")

        cols = ("batch_id", "excel_name", "status", "created", "completed", "model", "counts", "cost")

        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='tree headings', height=18, selectmode='extended')
        for col, w in [("batch_id", 180), ("excel_name", 180), ("status", 100), ("created", 110), ("completed", 110), ("model", 180), ("counts", 70), ("cost", 70)]:
            self.tree_active.heading(col, text=col.replace("_", " ").title())
            self.tree_active.column(col, width=w)
        self.tree_active.column("#0", width=280)
        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)

        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='tree headings', height=18, selectmode='extended')
        for col, w in [("batch_id", 180), ("excel_name", 180), ("status", 100), ("created", 110), ("completed", 110), ("model", 180), ("counts", 70), ("cost", 70)]:
            self.tree_arch.heading(col, text=col.replace("_", " ").title())
            self.tree_arch.column(col, width=w)
        self.tree_arch.column("#0", width=280)

        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')
        self.tree_arch.pack(fill='both', expand=True)

        self._load_jobs_all()
        self._load_archive_list()

    def _get_selected_ids(self, tree):
        return [tree.item(item)['values'][0] for item in tree.selection() if tree.item(item)['values']]

    def _load_jobs_all(self):
        if not hasattr(self, 'tree_active'):
            return
        for i in self.tree_active.get_children():
            self.tree_active.delete(i)
        for j in load_batch_jobs():
            if j.get("archived"):
                continue
            cnt = "-"
            if j.get("succeeded_count") is not None:
                cnt = f"{j.get('succeeded_count', 0)}/{j.get('total_count', j.get('request_count', 0))}"
            elif j.get("request_count"):
                cnt = f"0/{j.get('request_count', 0)}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            cost = f"${j.get('total_cost_usd', 0):.4f}" if j.get('total_cost_usd') else "-"
            model_display = j.get("model", "-")
            if "gemini-2.5-flash-lite" in model_display:
                model_display = "gemini-2.5-flash-lite"
            status_display = get_state_short(j.get("status", ""))
            self.tree_active.insert("", "end", text=j["batch_id"][:35], values=(
                j["batch_id"], os.path.basename(j.get("src_excel", "")), status_display, c_at, f_at, model_display, cnt, cost))

    def _load_archive_list(self):
        if not hasattr(self, 'tree_arch'):
            return
        for i in self.tree_arch.get_children():
            self.tree_arch.delete(i)
        for j in load_batch_jobs():
            if not j.get("archived"):
                continue
            cnt = "-"
            if j.get("succeeded_count") is not None:
                cnt = f"{j.get('succeeded_count', 0)}/{j.get('total_count', j.get('request_count', 0))}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            cost = f"${j.get('total_cost_usd', 0):.4f}" if j.get('total_cost_usd') else "-"
            model_display = j.get("model", "-")
            status_display = get_state_short(j.get("status", ""))
            self.tree_arch.insert("", "end", text=j["batch_id"][:35], values=(
                j["batch_id"], os.path.basename(j.get("src_excel", "")), status_display, c_at, f_at, model_display, cnt, cost))

    def _check_selected_status(self):
        """선택된 배치의 상태를 API에서 확인"""
        ids = self._get_selected_ids(self.tree_active)
        if not ids:
            messagebox.showinfo("안내", "상태를 확인할 배치를 선택하세요.")
            return

        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key를 입력하세요.")
            return

        threading.Thread(target=self._thread_check_status, args=(ids, key), daemon=True).start()

    def _thread_check_status(self, batch_ids, api_key):
        try:
            client = get_gemini_client(api_key)
            for batch_name in batch_ids:
                try:
                    status = get_batch_status(client, batch_name)
                    state = status.get("state", "")
                    self.append_log(f"[상태 확인] {batch_name[:40]}... → {get_state_short(state)}")

                    upsert_batch_job(
                        batch_id=batch_name,
                        status=state,
                        total_count=status.get("total_count", 0),
                        succeeded_count=status.get("succeeded_count", 0),
                        failed_count=status.get("failed_count", 0),
                        output_file_name=status.get("output_file_name", ""),
                    )
                except Exception as e:
                    self.append_log(f"[상태 확인 오류] {batch_name[:30]}...: {e}")

            self.after(0, self._load_jobs_all)
        except Exception as e:
            self.append_log(f"[오류] {e}")

    def _merge_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids:
            messagebox.showinfo("안내", "병합할 배치를 선택하세요.")
            return

        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key를 입력하세요.")
            return

        threading.Thread(target=self._run_merge, args=(ids, key), daemon=True).start()

    def _run_merge(self, batch_ids, api_key):
        self.append_log(f"[병합] 시작: {len(batch_ids)}건")
        try:
            client = get_gemini_client(api_key)

            for bid in batch_ids:
                try:
                    job = next((j for j in load_batch_jobs() if j["batch_id"] == bid), None)
                    if not job:
                        self.append_log(f"[병합] {bid[:30]}... - 작업 정보 없음")
                        continue

                    # 상태 확인
                    if job.get("status") != "JOB_STATE_SUCCEEDED":
                        status = get_batch_status(client, bid)
                        state = status.get("state", "")
                        upsert_batch_job(batch_id=bid, status=state, output_file_name=status.get("output_file_name", ""))

                        if state != "JOB_STATE_SUCCEEDED":
                            self.append_log(f"[병합] {bid[:30]}... - 아직 완료되지 않음 ({get_state_short(state)})")
                            continue

                        job["output_file_name"] = status.get("output_file_name", "")

                    output_file_name = job.get("output_file_name", "")
                    src_excel = job.get("src_excel", "")

                    if not output_file_name:
                        self.append_log(f"[병합] {bid[:30]}... - 결과 파일 정보 없음")
                        continue

                    if not src_excel or not os.path.exists(src_excel):
                        self.append_log(f"[병합] {bid[:30]}... - 원본 엑셀 없음")
                        continue

                    # 결과 파일 다운로드
                    base, _ = os.path.splitext(src_excel)
                    local_results_path = f"{base}_stage2_batch_results.jsonl"

                    self.append_log(f"[병합] {bid[:30]}... - 결과 다운로드 중...")
                    download_batch_results(client, output_file_name, local_results_path)

                    # 결과 파싱
                    results = parse_batch_results(local_results_path)
                    self.append_log(f"[병합] {bid[:30]}... - {len(results)}건 결과 파싱 완료")

                    # 엑셀에 병합
                    out_excel = get_next_version_path(src_excel, "text")
                    cnt, total_in, total_out = merge_results_to_excel(src_excel, results, out_excel)

                    # 비용 계산
                    model = job.get("model", "gemini-2.5-flash-lite")
                    cost_info = compute_cost_usd(model, total_in, total_out)
                    total_cost = cost_info["total_cost"] if cost_info else 0

                    self.append_log(f"[병합] ✅ {bid[:30]}... 완료: {cnt}건, 비용: ${total_cost:.4f}")

                    upsert_batch_job(
                        batch_id=bid,
                        status="MERGED",
                        out_excel=out_excel,
                        total_input_tokens=total_in,
                        total_output_tokens=total_out,
                        total_cost_usd=total_cost,
                    )

                    try:
                        JobManager.update_status(get_root_filename(src_excel), text_msg="T2(분석완료)")
                    except:
                        pass

                except Exception as e:
                    self.append_log(f"[병합] ❌ {bid[:30]}... 실패: {e}")

            self.after(0, self._load_jobs_all)
            self.after(0, self._load_archive_list)
            self.append_log("[병합] 완료")

        except Exception as e:
            self.append_log(f"[병합 오류] {e}")

    def _archive_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if ids and messagebox.askyesno("확인", f"{len(ids)}건을 휴지통으로 이동?"):
            archive_batch_job(ids, True)
            self._load_jobs_all()
            self._load_archive_list()

    def _restore_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if ids:
            archive_batch_job(ids, False)
            self._load_jobs_all()
            self._load_archive_list()

    def _hard_delete_selected(self):
        ids = self._get_selected_ids(self.tree_arch)
        if ids and messagebox.askyesno("경고", f"{len(ids)}건 영구 삭제?"):
            hard_delete_batch_job(ids)
            self._load_archive_list()

    # ----------------------------------------------------
    # Tab 3: Manual Merge
    # ----------------------------------------------------
    def _init_tab_merge(self):
        container = ttk.Frame(self.tab_merge, padding=20)
        container.pack(fill='both', expand=True)
        ttk.Label(container, text="배치 ID (Gemini Batch Name):").pack(anchor='w')
        ttk.Entry(container, textvariable=self.batch_id_var, width=80).pack(fill='x', pady=5)
        ttk.Button(container, text="상태 확인 & 병합 실행", command=self._manual_merge, style="Success.TButton").pack(pady=10)

    def _manual_merge(self):
        bid = self.batch_id_var.get().strip()
        if not bid:
            messagebox.showwarning("오류", "배치 ID를 입력하세요.")
            return

        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key를 입력하세요.")
            return

        threading.Thread(target=self._run_merge, args=([bid], key), daemon=True).start()


# ========================================================
# Main
# ========================================================
if __name__ == "__main__":
    app = Stage2BatchGeminiGUI()
    app.mainloop()
