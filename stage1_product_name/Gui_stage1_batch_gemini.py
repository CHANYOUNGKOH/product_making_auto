"""
Gui_stage1_batch_gemini.py

Stage 1 Gemini Batch API 실행 스크립트 (GUI)
- 기능: 엑셀 원본 -> JSONL 생성 -> Gemini Batch 업로드 -> 상태 폴링 -> 결과 다운로드 -> 병합
- Gemini Batch API 사용 (비용 50% 절감)
- GPT Batch API와 동일한 워크플로우
"""

import os
import sys
import json
import re
import threading
from datetime import datetime

# ========================================================
# [CRITICAL] 경로 강제 설정 (Import 에러 방지)
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
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText

# Gemini API
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# Core 모듈 import
try:
    from batch_stage1_core_gemini import (
        create_batch_input_jsonl,
        upload_jsonl_file,
        create_batch_job,
        get_batch_status,
        download_batch_results,
        parse_batch_results,
        merge_results_to_excel,
        compute_cost_usd,
        get_gemini_client,
        safe_str,
        is_batch_completed,
        is_batch_succeeded,
        BATCH_STATE_SUCCEEDED,
        BATCH_STATE_FAILED,
        DEFAULT_MODEL,
        MODEL_PRICING,
    )
    CORE_AVAILABLE = True
except ImportError as e:
    print(f"[WARN] Core 모듈 로드 실패: {e}")
    CORE_AVAILABLE = False


# ========================================================
# 파일명 유틸리티
# ========================================================
def get_root_filename(filename):
    """파일명에서 버전 정보 제거"""
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


def get_next_version_path(current_path, task_type: str = "text"):
    """다음 버전 파일명 생성"""
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


# ========================================================
# 경로 및 설정 관리
# ========================================================
def get_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
API_KEY_FILE = os.path.join(BASE_DIR, ".gemini_api_key_stage1_batch")
BATCH_JOBS_FILE = os.path.join(BASE_DIR, "stage1_batch_jobs_gemini.json")

# Gemini 모델 목록
MODEL_LIST = list(MODEL_PRICING.keys()) if CORE_AVAILABLE else ["gemini-2.5-flash-lite"]

# UI Colors
COLOR_BG = "#F8F9FA"
COLOR_WHITE = "#FFFFFF"
COLOR_PRIMARY = "#4285F4"  # Google Blue
COLOR_PRIMARY_HOVER = "#3367D6"
COLOR_SUCCESS = "#34A853"  # Google Green
COLOR_SUCCESS_HOVER = "#2E8B47"
COLOR_DANGER = "#EA4335"  # Google Red
COLOR_DANGER_HOVER = "#C5221F"
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

# 트리뷰용 짧은 형식
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
    if kr:
        return f"{state} - {kr}"
    return state

def get_state_short(state: str) -> str:
    """영어 상태를 짧은 한글 형식으로 변환 (트리뷰용)"""
    short = JOB_STATE_SHORT.get(state, "")
    if short:
        return f"{state} {short}"
    return state


def load_api_key_from_file(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""


def save_api_key_to_file(key, path):
    with open(path, "w", encoding="utf-8") as f:
        f.write(key)


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


def upsert_batch_job(batch_name, **kwargs):
    """배치 작업 정보 업데이트 또는 추가"""
    jobs = load_batch_jobs()
    found = False
    now_str = datetime.now().isoformat()

    for j in jobs:
        if j.get("batch_name") == batch_name:
            if kwargs.get("state") == BATCH_STATE_SUCCEEDED and j.get("state") != BATCH_STATE_SUCCEEDED:
                if "completed_at" not in kwargs:
                    j["completed_at"] = now_str
            j.update(kwargs)
            j["updated_at"] = now_str
            found = True
            break

    if not found:
        new_job = {
            "batch_name": batch_name,
            "created_at": now_str,
            "updated_at": now_str,
            "completed_at": "",
            "archived": False,
            **kwargs
        }
        jobs.insert(0, new_job)

    save_batch_jobs(jobs)


def archive_batch_job(batch_names, archive=True):
    if isinstance(batch_names, str):
        batch_names = [batch_names]
    jobs = load_batch_jobs()
    for j in jobs:
        if j.get("batch_name") in batch_names:
            j["archived"] = archive
    save_batch_jobs(jobs)


def hard_delete_batch_job(batch_names):
    if isinstance(batch_names, str):
        batch_names = [batch_names]
    jobs = load_batch_jobs()
    jobs = [j for j in jobs if j.get("batch_name") not in batch_names]
    save_batch_jobs(jobs)


# ========================================================
# 툴팁 클래스
# ========================================================
class ToolTip:
    def __init__(self, widget, text, wraplength=400):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tipwindow = None
        self.widget.bind("<Enter>", self.show_tip)
        self.widget.bind("<Leave>", self.hide_tip)

    def show_tip(self, event=None):
        if self.tipwindow or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left', background="#ffffe0",
                        relief='solid', borderwidth=1, font=("맑은 고딕", 9),
                        wraplength=self.wraplength)
        label.pack(ipadx=4, ipady=2)

    def hide_tip(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None


# ========================================================
# GUI Class
# ========================================================
class Stage1GeminiBatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 1: Gemini Batch API Manager (50% 할인)")
        self.geometry("1250x950")

        if not GEMINI_AVAILABLE:
            messagebox.showerror("오류", "google-genai 패키지가 설치되지 않았습니다.\npip install google-genai")
            self.destroy()
            return

        if not CORE_AVAILABLE:
            messagebox.showerror("오류", "batch_stage1_core_gemini.py 모듈을 찾을 수 없습니다.")
            self.destroy()
            return

        self.api_key_var = tk.StringVar()
        self.src_file_var = tk.StringVar()
        self.model_var = tk.StringVar(value=DEFAULT_MODEL)
        self.skip_exist_var = tk.BooleanVar(value=True)

        # 자동 갱신 관련
        self.auto_refresh_var = tk.BooleanVar(value=False)
        self.refresh_interval_var = tk.IntVar(value=30)
        self.is_refreshing = False

        self._configure_styles()
        self._init_ui()
        self._load_key()

        # 자동 갱신 루프 시작
        self._auto_refresh_loop()

    def _configure_styles(self):
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass

        self.configure(background=COLOR_BG)
        style.configure("TFrame", background=COLOR_BG)
        style.configure("TLabel", background=COLOR_BG, foreground=COLOR_TEXT, font=("맑은 고딕", 10))
        style.configure("Header.TLabel", font=("맑은 고딕", 11, "bold"), foreground="#444")
        style.configure("TLabelframe", background=COLOR_BG, bordercolor="#D0D7DE")
        style.configure("TLabelframe.Label", background=COLOR_BG, foreground="#1967D2", font=("맑은 고딕", 10, "bold"))
        style.configure("TNotebook", background=COLOR_BG, borderwidth=0)
        style.configure("TNotebook.Tab", background="#E1E4E8", padding=[12, 5], font=("맑은 고딕", 10))
        style.map("TNotebook.Tab", background=[("selected", COLOR_WHITE)], foreground=[("selected", COLOR_PRIMARY)])
        style.configure("Treeview", background=COLOR_WHITE, fieldbackground=COLOR_WHITE, font=("맑은 고딕", 9), rowheight=28)
        style.configure("Treeview.Heading", background=COLOR_HEADER, foreground="#333", font=("맑은 고딕", 9, "bold"))
        style.map("Treeview", background=[('selected', '#CCE5FF')], foreground=[('selected', 'black')])
        style.configure("TButton", font=("맑은 고딕", 9), padding=5, borderwidth=1)

    def _init_ui(self):
        main_container = ttk.Frame(self, padding=15)
        main_container.pack(fill='both', expand=True)

        # 1. 상단 API Key
        f_top = ttk.LabelFrame(main_container, text="🔑 Gemini API 설정", padding=10)
        f_top.pack(fill='x', pady=(0, 10))
        ttk.Label(f_top, text="API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        entry_key = ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10))
        entry_key.pack(side='left', padx=10)
        btn_save = tk.Button(f_top, text="저장", command=self._save_key, bg=COLOR_PRIMARY, fg="white", font=("맑은 고딕", 9))
        btn_save.pack(side='left')

        btn_help = ttk.Button(f_top, text="❓ 사용 가이드", command=self._show_help_dialog)
        btn_help.pack(side='right')

        # 2. 메인 탭
        self.main_tabs = ttk.Notebook(main_container)
        self.main_tabs.pack(fill='both', expand=True, pady=5)

        self.tab_create = ttk.Frame(self.main_tabs)
        self.tab_manage = ttk.Frame(self.main_tabs)

        self.main_tabs.add(self.tab_create, text=" 1. 배치 생성 & 업로드 ")
        self.main_tabs.add(self.tab_manage, text=" 2. 배치 관리 (목록/갱신/병합) ")

        self._init_tab_create()
        self._init_tab_manage()

        # 3. 로그
        f_log = ttk.LabelFrame(main_container, text="📋 시스템 로그", padding=10)
        f_log.pack(fill='both', expand=True, pady=(10, 0))
        self.log_widget = ScrolledText(f_log, height=12, state='disabled', font=("Consolas", 9), bg="#F1F3F5")
        self.log_widget.pack(fill='both', expand=True)

    def _load_key(self):
        loaded = load_api_key_from_file(API_KEY_FILE)
        if loaded:
            self.api_key_var.set(loaded)

    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k, API_KEY_FILE)
            messagebox.showinfo("저장", "API Key 저장 완료")

    def append_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        full_msg = f"[{ts}] {msg}"
        def _update():
            if not hasattr(self, 'log_widget'):
                return
            try:
                self.log_widget.config(state='normal')
                self.log_widget.insert('end', full_msg + "\n")
                self.log_widget.see('end')
                self.log_widget.config(state='disabled')
            except:
                pass
        self.after(0, _update)

    def _safe_msgbox(self, type_, title, msg):
        self.after(0, lambda: getattr(messagebox, type_)(title, msg))

    def _show_help_dialog(self):
        msg = (
            "[Stage 1 Gemini Batch API 사용 가이드]\n\n"
            "1. [배치 생성 탭]:\n"
            "   - 원본 엑셀 파일을 선택하고 'Start Batch'를 클릭하세요.\n"
            "   - JSONL 파일이 생성되고 Gemini에 업로드됩니다.\n\n"
            "2. [배치 관리 탭]:\n"
            "   - [자동 갱신]을 켜두면 주기적으로 상태를 확인합니다.\n"
            "   - 'SUCCEEDED' 상태가 되면 [결과 병합]을 클릭하세요.\n\n"
            "※ Gemini Batch API는 비용 50% 할인이 적용됩니다.\n"
            "※ 결과는 원본 엑셀의 'ST1_결과상품명' 컬럼에 병합됩니다."
        )
        messagebox.showinfo("사용법", msg)

    # ----------------------------------------------------
    # Tab 1: Create
    # ----------------------------------------------------
    def _init_tab_create(self):
        container = ttk.Frame(self.tab_create, padding=20)
        container.pack(fill='both', expand=True)

        # Step 1: 파일
        f_file = ttk.LabelFrame(container, text="1. 작업 대상 파일 선택", padding=15)
        f_file.pack(fill='x', pady=(0, 15))
        ttk.Entry(f_file, textvariable=self.src_file_var, font=("맑은 고딕", 10)).pack(side='left', fill='x', expand=True)
        btn_file = ttk.Button(f_file, text="📂 파일 찾기", command=self._select_src_file)
        btn_file.pack(side='right', padx=5)
        ToolTip(btn_file, "Stage 1을 수행할 원본 엑셀 파일을 선택하세요.\n(카테고리명, 원본상품명, 판매형태 컬럼 필수)")

        # Step 2: 옵션
        f_opt = ttk.LabelFrame(container, text="2. 배치 옵션 설정", padding=15)
        f_opt.pack(fill='x', pady=5)

        fr1 = ttk.Frame(f_opt)
        fr1.pack(fill='x', pady=5)
        ttk.Label(fr1, text="모델:", width=10).pack(side='left')
        cb_model = ttk.Combobox(fr1, textvariable=self.model_var, values=MODEL_LIST, state="readonly", width=35)
        cb_model.pack(side='left', padx=5)
        ToolTip(cb_model, "gemini-2.5-flash-lite가 가장 저렴합니다.")

        fr2 = ttk.Frame(f_opt)
        fr2.pack(fill='x', pady=5)
        chk_skip = ttk.Checkbutton(fr2, text=" 이미 ST1_결과상품명이 있는 행은 건너뛰기", variable=self.skip_exist_var)
        chk_skip.pack(side='left', padx=5)
        ToolTip(chk_skip, "중복 과금 방지를 위해 이미 결과가 있는 행은 제외합니다.")

        # Step 3: 실행
        f_step3 = ttk.LabelFrame(container, text="3. 실행", padding=15)
        f_step3.pack(fill='x', pady=15)
        btn_run = tk.Button(f_step3, text="🚀 JSONL 생성 및 Gemini Batch 업로드",
                           command=self._start_create_batch, bg=COLOR_SUCCESS, fg="white",
                           font=("맑은 고딕", 11, "bold"), height=2)
        btn_run.pack(fill='x')

        ttk.Label(container, text="※ Gemini Batch API: 비용 50% 할인, 최대 24시간 소요", foreground="#666").pack()

    def _select_src_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx;*.xls")])
        if p:
            self.src_file_var.set(p)

    def _start_create_batch(self):
        if not self.api_key_var.get():
            messagebox.showwarning("오류", "API Key 필요")
            return
        if not self.src_file_var.get():
            messagebox.showwarning("오류", "파일 선택 필요")
            return
        t = threading.Thread(target=self._run_create_batch)
        t.daemon = True
        t.start()

    def _run_create_batch(self):
        key = self.api_key_var.get().strip()
        src = self.src_file_var.get().strip()
        model = self.model_var.get()
        skip_existing = self.skip_exist_var.get()

        try:
            self.append_log(f"엑셀 로드 중... {os.path.basename(src)}")

            # 1. JSONL 생성
            base_name, _ = os.path.splitext(os.path.basename(src))
            jsonl_path = os.path.join(os.path.dirname(src), f"{base_name}_stage1_gemini_batch_input.jsonl")

            result = create_batch_input_jsonl(
                excel_path=src,
                jsonl_path=jsonl_path,
                model_name=model,
                skip_existing=skip_existing
            )

            self.append_log(f"JSONL 생성 완료: {result['written_count']}건 (스킵: {result['skipped_count']}건, 기존결과: {result['skipped_existing']}건)")

            if result['written_count'] == 0:
                self.append_log("생성할 요청이 없습니다.")
                return

            # 2. Gemini 클라이언트 생성
            client = get_gemini_client(key)

            # 3. JSONL 파일 업로드
            self.append_log("Gemini에 JSONL 파일 업로드 중...")
            uploaded_file_name = upload_jsonl_file(
                client=client,
                jsonl_path=jsonl_path,
                display_name=f"{base_name}_stage1_batch"
            )
            self.append_log(f"업로드 완료: {uploaded_file_name}")

            # 4. Batch Job 생성
            self.append_log("Batch Job 생성 중...")
            batch_info = create_batch_job(
                client=client,
                model_name=model,
                src_file_name=uploaded_file_name,
                display_name=f"stage1_{base_name}"
            )

            batch_name = batch_info["name"]
            self.append_log(f"✅ 배치 시작! Name: {batch_name}")
            self.append_log(f"   상태: {batch_info['state']}")

            # 5. 로컬 DB에 저장
            upsert_batch_job(
                batch_name=batch_name,
                src_excel=src,
                jsonl_path=jsonl_path,
                uploaded_file_name=uploaded_file_name,
                model=model,
                state=batch_info["state"],
                request_count=result['written_count'],
            )

            self._safe_msgbox("showinfo", "성공", f"배치 시작됨:\n{batch_name}")
            self.after(0, self._load_jobs_list)

        except Exception as e:
            self.append_log(f"에러: {e}")
            self._safe_msgbox("showerror", "에러", str(e))

    # ----------------------------------------------------
    # Tab 2: Manage
    # ----------------------------------------------------
    def _init_tab_manage(self):
        container = ttk.Frame(self.tab_manage, padding=10)
        container.pack(fill='both', expand=True)

        # 컨트롤 바
        f_ctrl = ttk.Frame(container)
        f_ctrl.pack(fill='x', pady=(0, 10))

        btn_refresh = tk.Button(f_ctrl, text="🔄 상태 갱신", command=self._refresh_selected,
                               bg=COLOR_PRIMARY, fg="white", font=("맑은 고딕", 9))
        btn_refresh.pack(side='left', padx=2)

        btn_merge = tk.Button(f_ctrl, text="📥 결과 병합", command=self._merge_selected,
                             bg=COLOR_SUCCESS, fg="white", font=("맑은 고딕", 9))
        btn_merge.pack(side='left', padx=2)

        btn_archive = tk.Button(f_ctrl, text="🗑️ 휴지통", command=self._archive_selected,
                               bg=COLOR_DANGER, fg="white", font=("맑은 고딕", 9))
        btn_archive.pack(side='left', padx=2)

        # 자동 갱신
        ttk.Separator(f_ctrl, orient='vertical').pack(side='left', fill='y', padx=10)
        chk_auto = ttk.Checkbutton(f_ctrl, text="자동 갱신", variable=self.auto_refresh_var)
        chk_auto.pack(side='left')
        ttk.Label(f_ctrl, text="간격(초):").pack(side='left', padx=(10, 2))
        spin_interval = ttk.Spinbox(f_ctrl, from_=10, to=300, width=5, textvariable=self.refresh_interval_var)
        spin_interval.pack(side='left')

        # Treeview
        cols = ("batch_name", "excel_name", "model", "state", "counts", "created", "completed")
        self.tree_jobs = ttk.Treeview(container, columns=cols, show='headings', height=20, selectmode='extended')

        self.tree_jobs.heading("batch_name", text="배치 Name")
        self.tree_jobs.heading("excel_name", text="엑셀명")
        self.tree_jobs.heading("model", text="모델")
        self.tree_jobs.heading("state", text="상태")
        self.tree_jobs.heading("counts", text="성공/전체")
        self.tree_jobs.heading("created", text="생성일시")
        self.tree_jobs.heading("completed", text="완료일시")

        self.tree_jobs.column("batch_name", width=280, anchor="w")
        self.tree_jobs.column("excel_name", width=200, anchor="w")
        self.tree_jobs.column("model", width=200, anchor="w")
        self.tree_jobs.column("state", width=120, anchor="center")
        self.tree_jobs.column("counts", width=80, anchor="center")
        self.tree_jobs.column("created", width=120, anchor="center")
        self.tree_jobs.column("completed", width=120, anchor="center")

        self.tree_jobs.tag_configure('odd', background=COLOR_WHITE)
        self.tree_jobs.tag_configure('even', background='#F2F7FF')
        self.tree_jobs.tag_configure('succeeded', background='#D4EDDA')
        self.tree_jobs.tag_configure('failed', background='#F8D7DA')

        self.tree_jobs.pack(fill='both', expand=True, padx=5, pady=5)

        # 우클릭 메뉴
        self.menu_jobs = Menu(self, tearoff=0)
        self.menu_jobs.add_command(label="상태 갱신", command=self._refresh_selected)
        self.menu_jobs.add_command(label="결과 병합", command=self._merge_selected)
        self.menu_jobs.add_separator()
        self.menu_jobs.add_command(label="휴지통으로 이동", command=self._archive_selected)
        self.menu_jobs.add_command(label="영구 삭제", command=self._hard_delete_selected)
        self.tree_jobs.bind("<Button-3>", self._show_context_menu)

        self._load_jobs_list()

    def _show_context_menu(self, event):
        item = self.tree_jobs.identify_row(event.y)
        if item:
            if item not in self.tree_jobs.selection():
                self.tree_jobs.selection_set(item)
            self.menu_jobs.post(event.x_root, event.y_root)

    def _get_selected_names(self):
        selection = self.tree_jobs.selection()
        names = []
        for item in selection:
            vals = self.tree_jobs.item(item)['values']
            if vals and vals[0]:
                names.append(vals[0])
        return names

    def _load_jobs_list(self):
        if not hasattr(self, 'tree_jobs'):
            return
        for i in self.tree_jobs.get_children():
            self.tree_jobs.delete(i)

        jobs = load_batch_jobs()
        idx = 0
        for j in jobs:
            if j.get("archived", False):
                continue

            counts = "-"
            if "succeeded_count" in j and "total_count" in j:
                counts = f"{j.get('succeeded_count', 0)}/{j.get('total_count', 0)}"
            elif "request_count" in j:
                counts = f"-/{j['request_count']}"

            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            excel_name = os.path.basename(j.get("src_excel", "")) if j.get("src_excel") else "-"

            state = j.get("state", "")
            state_display = get_state_short(state)

            if state == BATCH_STATE_SUCCEEDED:
                tag = 'succeeded'
            elif state == BATCH_STATE_FAILED:
                tag = 'failed'
            else:
                tag = 'even' if idx % 2 == 0 else 'odd'

            self.tree_jobs.insert("", "end",
                values=(
                    j.get("batch_name", ""),
                    excel_name,
                    j.get("model", ""),
                    state_display,
                    counts,
                    c_at,
                    f_at
                ),
                tags=(tag,))
            idx += 1

    def _auto_refresh_loop(self):
        """자동 상태 갱신 루프"""
        if self.auto_refresh_var.get() and not self.is_refreshing:
            jobs = load_batch_jobs()
            active_names = [
                j['batch_name'] for j in jobs
                if not j.get('archived') and not is_batch_completed(j.get('state', ''))
            ]
            if active_names:
                t = threading.Thread(target=self._run_refresh_names, args=(active_names, True))
                t.daemon = True
                t.start()

        interval = max(10, self.refresh_interval_var.get()) * 1000
        self.after(interval, self._auto_refresh_loop)

    def _refresh_selected(self):
        names = self._get_selected_names()
        if not names:
            # 전체 미완료 작업 갱신
            jobs = load_batch_jobs()
            names = [
                j['batch_name'] for j in jobs
                if not j.get('archived') and not is_batch_completed(j.get('state', ''))
            ]
        if not names:
            messagebox.showinfo("알림", "갱신할 작업이 없습니다.")
            return
        t = threading.Thread(target=self._run_refresh_names, args=(names,))
        t.daemon = True
        t.start()

    def _run_refresh_names(self, names, silent=False):
        if self.is_refreshing:
            return
        self.is_refreshing = True

        key = self.api_key_var.get().strip()
        if not key:
            self.is_refreshing = False
            return

        if not silent:
            self.append_log(f"선택된 {len(names)}건 갱신 중...")

        try:
            client = get_gemini_client(key)

            for batch_name in names:
                try:
                    status = get_batch_status(client, batch_name)

                    update_data = {"state": status["state"]}
                    if "total_count" in status:
                        update_data["total_count"] = status["total_count"]
                    if "succeeded_count" in status:
                        update_data["succeeded_count"] = status["succeeded_count"]
                    if "failed_count" in status:
                        update_data["failed_count"] = status["failed_count"]
                    if "output_file_name" in status:
                        update_data["output_file_name"] = status["output_file_name"]

                    upsert_batch_job(batch_name, **update_data)

                    if not silent:
                        state_display = get_state_display(status['state'])
                        self.append_log(f"✅ {batch_name[:30]}...: {state_display}")

                except Exception as e:
                    if not silent:
                        self.append_log(f"❌ {batch_name[:30]}... 갱신 실패: {e}")

        finally:
            self.is_refreshing = False
            self.after(0, self._load_jobs_list)
            if not silent:
                self.append_log("갱신 완료")

    def _merge_selected(self):
        names = self._get_selected_names()
        if not names:
            messagebox.showinfo("알림", "병합할 배치를 선택하세요.")
            return

        jobs = load_batch_jobs()
        succeeded_names = [
            name for name in names
            if any(j.get("batch_name") == name and is_batch_succeeded(j.get("state", "")) for j in jobs)
        ]

        if not succeeded_names:
            messagebox.showinfo("알림", "병합 가능한 'SUCCEEDED' 상태의 작업이 없습니다.")
            return

        if messagebox.askyesno("병합", f"선택한 {len(succeeded_names)}건을 병합하시겠습니까?"):
            t = threading.Thread(target=self._run_merge, args=(succeeded_names,))
            t.daemon = True
            t.start()

    def _run_merge(self, names):
        key = self.api_key_var.get().strip()

        try:
            client = get_gemini_client(key)
            jobs = load_batch_jobs()

            for batch_name in names:
                job = next((j for j in jobs if j.get("batch_name") == batch_name), None)
                if not job:
                    continue

                self.append_log(f"--- {batch_name[:30]}... 병합 시작 ---")

                # 원본 엑셀 경로 확인
                src_excel = job.get("src_excel")
                if not src_excel or not os.path.exists(src_excel):
                    self.append_log(f"⚠️ 원본 엑셀을 찾을 수 없습니다: {src_excel}")
                    continue

                # 출력 파일 이름 확인
                output_file_name = job.get("output_file_name")
                if not output_file_name:
                    # 다시 상태 조회
                    status = get_batch_status(client, batch_name)
                    output_file_name = status.get("output_file_name")
                    if output_file_name:
                        upsert_batch_job(batch_name, output_file_name=output_file_name)

                if not output_file_name:
                    self.append_log(f"⚠️ 출력 파일을 찾을 수 없습니다.")
                    continue

                # 결과 다운로드
                base_dir = os.path.dirname(src_excel)
                base_name = os.path.splitext(os.path.basename(src_excel))[0]
                output_jsonl = os.path.join(base_dir, f"{base_name}_stage1_gemini_batch_output.jsonl")

                self.append_log(f"결과 다운로드 중...")
                download_batch_results(client, output_file_name, output_jsonl)
                self.append_log(f"다운로드 완료: {output_jsonl}")

                # 결과 파싱
                results = parse_batch_results(output_jsonl)
                self.append_log(f"파싱된 결과: {len(results)}건")

                # 병합
                output_excel = get_next_version_path(src_excel, "text")
                merged_count, total_in, total_out = merge_results_to_excel(src_excel, results, output_excel)

                # 비용 계산
                model = job.get("model", DEFAULT_MODEL)
                cost_info = compute_cost_usd(model, total_in, total_out)
                cost_str = f"${cost_info['total_cost']:.4f}" if cost_info else "N/A"

                self.append_log(f"✅ 병합 완료: {merged_count}건")
                self.append_log(f"   토큰: 입력 {total_in:,} / 출력 {total_out:,}")
                self.append_log(f"   비용: {cost_str} (Batch 50% 할인 적용)")
                self.append_log(f"   저장: {os.path.basename(output_excel)}")

                # 상태 업데이트
                upsert_batch_job(batch_name, state="merged", merged_excel=output_excel)

            self._safe_msgbox("showinfo", "완료", "병합이 완료되었습니다.")
            self.after(0, self._load_jobs_list)

        except Exception as e:
            self.append_log(f"에러: {e}")
            self._safe_msgbox("showerror", "에러", str(e))

    def _archive_selected(self):
        names = self._get_selected_names()
        if not names:
            return
        if messagebox.askyesno("확인", f"{len(names)}건을 휴지통으로 이동하시겠습니까?"):
            archive_batch_job(names, True)
            self._load_jobs_list()

    def _hard_delete_selected(self):
        names = self._get_selected_names()
        if not names:
            return
        if messagebox.askyesno("확인", f"{len(names)}건을 영구 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다."):
            hard_delete_batch_job(names)
            self._load_jobs_list()


# ========================================================
# Main
# ========================================================
if __name__ == "__main__":
    app = Stage1GeminiBatchGUI()
    app.mainloop()
