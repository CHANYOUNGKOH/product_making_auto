"""
Gui_stage1_batch.py

Stage 1 Batch API 실행 스크립트 (GUI) - Path Fixed
- 기능: 엑셀 원본 -> Batch JSONL 생성(상품명 정제) -> 업로드 -> 실행 -> 병합 -> 정제 리포트
- [Fix] 런처 실행 시 모듈 경로(ModuleNotFoundError) 문제 완벽 해결
"""

import os
import sys
import json
import math
import threading
import subprocess
from datetime import datetime

# ========================================================
# [CRITICAL] 경로 강제 설정 (Import 에러 방지)
# ========================================================
# 현재 파일(Gui_stage1_batch.py)이 있는 폴더를 구합니다.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 해당 폴더를 파이썬 검색 경로(sys.path)의 맨 앞에 추가합니다.
# 이렇게 하면 같은 폴더에 있는 'batch_stage1_core.py'를 무조건 찾습니다.
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# 혹시 core 파일이 상위 폴더에 있을 경우를 대비해 상위 폴더도 추가합니다.
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)
# ========================================================

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI

# [핵심 의존성] Stage1 프롬프트 모듈 (core와 동일한 프롬프트 사용)
try:
    from prompts_stage1 import build_stage1_prompt, safe_str
except ImportError:
    # prompts_stage1 이 정말 없는 경우 안전한 fallback
    def safe_str(x):
        if x is None:
            return ""
        try:
            if isinstance(x, float) and math.isnan(x):
                return ""
        except Exception:
            pass
        return str(x).strip()

    def build_stage1_prompt(category, sale_type, raw_name):
        raise RuntimeError(
            "필수 모듈 'prompts_stage1.py'를 찾을 수 없습니다.\n"
            "Stage1에서 사용하던 프롬프트 정의 파일이 같은 폴더나 파이썬 경로에 있는지 확인해주세요."
        )

# 여기서 GUI 전용 batch payload 빌더를 구현 (core는 손대지 않음)
def build_stage1_batch_payload(idx, row, model, effort):
    """
    한 행(row)을 Batch API용 요청 한 줄(JSONL)로 만드는 함수.
    - core의 create_batch_input_jsonl 과 동일한 필드(카테고리명, 판매형태, 원본상품명)를 사용
    - 프롬프트는 prompts_stage1.build_stage1_prompt 재사용
    - Batch endpoint: /v1/chat/completions (Gui_stage1_batch.py 현재 구조 유지)
    """
    # 필수 필드 안전하게 문자열로 변환
    raw_name = safe_str(row.get("원본상품명", ""))
    category = safe_str(row.get("카테고리명", ""))
    sale_type = safe_str(row.get("판매형태", ""))

    # 필수값이 하나라도 비어 있으면 이 행은 스킵
    if not raw_name or not category or not sale_type:
        return None

    # core와 동일한 프롬프트 생성
    prompt_text = build_stage1_prompt(category, sale_type, raw_name)

    # Chat Completions용 body
    body = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt_text,
            }
        ],
        # 필요하면 온도/토큰 제한 등 조절 가능
        "temperature": 0.3,
        "max_tokens": 64,
    }

    # reasoning effort 옵션이 필요하면 붙여줌 (gpt-5 계열 reasoning 지원용)
    if effort:
        body["reasoning"] = {"effort": effort}

    # Batch JSONL 한 줄 구조
    return {
        "custom_id": f"row-{idx}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": body,
    }


# History 모듈 (있으면 사용, 없으면 조용히 패스)
try:
    from stage1_run_history import append_run_history
except ImportError:
    def append_run_history(*args, **kwargs):
        pass


# ========================================================
# [CORE] 경로 및 설정 관리
# ========================================================
def get_base_dir():
    """PyInstaller 등으로 패키징된 경우와 일반 실행을 구분하여 기본 경로 반환"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
API_KEY_FILE = os.path.join(BASE_DIR, ".openai_api_key_stage1_batch")
BATCH_JOBS_FILE = os.path.join(BASE_DIR, "stage1_batch_jobs.json")

# [수정] GPT-5 계열 모델만 유지
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5":       {"input": 1.25, "output": 10.00},
    "gpt-5-mini":  {"input": 0.25, "output": 2.00},
    "gpt-5-nano":  {"input": 0.05, "output": 0.40},
}

# UI Colors
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

def load_api_key_from_file(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f: return f.read().strip()
    return ""

def save_api_key_to_file(key, path):
    with open(path, "w", encoding="utf-8") as f: f.write(key)

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
        if self.tipwindow or not self.text: return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left', background="#ffffe0", relief='solid', borderwidth=1, font=("맑은 고딕", 9), wraplength=self.wraplength)
        label.pack(ipadx=4, ipady=2)

    def hide_tip(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None

# ========================================================
# 배치 잡 관리 (JSON DB)
# ========================================================
def load_batch_jobs():
    if not os.path.exists(BATCH_JOBS_FILE): return []
    try:
        with open(BATCH_JOBS_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return []

def save_batch_jobs(jobs):
    try:
        with open(BATCH_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
    except Exception as e: print(f"[Error] 잡 저장 실패: {e}")

def upsert_batch_job(batch_id, **kwargs):
    jobs = load_batch_jobs()
    found = False
    now_str = datetime.now().isoformat()
    for j in jobs:
        if j["batch_id"] == batch_id:
            if kwargs.get("status") == "completed" and j.get("status") != "completed":
                if "completed_at" not in kwargs: j["completed_at"] = now_str
            j.update(kwargs)
            j["updated_at"] = now_str
            found = True
            break
    if not found:
        new_job = {
            "batch_id": batch_id, "created_at": now_str, "updated_at": now_str,
            "completed_at": "", "archived": False, **kwargs
        }
        jobs.insert(0, new_job)
    save_batch_jobs(jobs)

def archive_batch_job(batch_ids, archive=True):
    if isinstance(batch_ids, str): batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    for j in jobs:
        if j["batch_id"] in batch_ids: j["archived"] = archive
    save_batch_jobs(jobs)

def hard_delete_batch_job(batch_ids):
    if isinstance(batch_ids, str): batch_ids = [batch_ids]
    jobs = load_batch_jobs()
    jobs = [j for j in jobs if j["batch_id"] not in batch_ids]
    save_batch_jobs(jobs)

# ========================================================
# GUI Class
# ========================================================
class Stage1BatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 1: Batch API Manager (Path Fixed)")
        self.geometry("1250x950")
        
        self.api_key_var = tk.StringVar()
        self.src_file_var = tk.StringVar()
        
        # [설정] 기본값: gpt-5-mini / low
        self.model_var = tk.StringVar(value="gpt-5-mini") 
        self.effort_var = tk.StringVar(value="low") 
        self.skip_exist_var = tk.BooleanVar(value=True)
        
        # 자동 갱신 관련
        self.auto_refresh_var = tk.BooleanVar(value=False)
        self.refresh_interval_var = tk.IntVar(value=30)
        self.is_refreshing = False

        self.batch_id_var = tk.StringVar()
        
        self._configure_styles()
        self._init_ui()
        self._load_key()
        
        # 자동 갱신 루프 시작
        self._auto_refresh_loop()

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

        # 1. 상단 API Key
        f_top = ttk.LabelFrame(main_container, text="🔑 API 설정", padding=10)
        f_top.pack(fill='x', pady=(0, 10))
        ttk.Label(f_top, text="Batch API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        entry_key = ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10))
        entry_key.pack(side='left', padx=10)
        btn_save = ttk.Button(f_top, text="저장", command=self._save_key, style="Primary.TButton")
        btn_save.pack(side='left')
        ToolTip(btn_save, "입력한 API Key를 로컬에 저장합니다.")

        btn_help = ttk.Button(f_top, text="❓ 사용 가이드", command=self._show_help_dialog)
        btn_help.pack(side='right')

        # 2. 메인 탭
        self.main_tabs = ttk.Notebook(main_container)
        self.main_tabs.pack(fill='both', expand=True, pady=5)
        
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
        f_log.pack(fill='both', expand=True, pady=(10, 0))
        self.log_widget = ScrolledText(f_log, height=12, state='disabled', font=("Consolas", 9), bg="#F1F3F5")
        self.log_widget.pack(fill='both', expand=True)

    def _load_key(self):
        loaded = load_api_key_from_file(API_KEY_FILE)
        if loaded: self.api_key_var.set(loaded)

    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k, API_KEY_FILE)
            messagebox.showinfo("저장", "API Key 저장 완료")

    # [Thread-Safe Log]
    def append_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        full_msg = f"[{ts}] {msg}"
        def _update():
            if not hasattr(self, 'log_widget'): return
            try:
                self.log_widget.config(state='normal')
                self.log_widget.insert('end', full_msg + "\n")
                self.log_widget.see('end')
                self.log_widget.config(state='disabled')
            except: pass
        self.after(0, _update)

    # [Thread-Safe Messagebox]
    def _safe_msgbox(self, type_, title, msg):
        self.after(0, lambda: getattr(messagebox, type_)(title, msg))

    def _show_help_dialog(self):
        msg = (
            "[Stage 1 Batch API 사용 가이드]\n\n"
            "1. [배치 생성 탭]:\n"
            "   - 원본 엑셀 파일을 선택하고 'Start Batch'를 클릭하세요.\n"
            "   - 'gpt-5-mini' 모델 사용 시 비용 효율과 속도가 좋습니다.\n\n"
            "2. [배치 관리 탭]:\n"
            "   - [자동 갱신]을 켜두면 주기적으로 상태를 확인합니다.\n"
            "   - 'completed' 상태가 되면 [선택 일괄 병합] -> [정제 리포트] 순으로 진행하세요.\n"
            "   - 리포트에서 원본 vs 정제결과의 글자 수 변화를 확인할 수 있습니다.\n\n"
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
        ToolTip(btn_file, "Stage 1을 수행할 원본 엑셀 파일을 선택하세요.\n(카테고리명, 원본상품명 컬럼 필수)")
        
        # Step 2: 옵션
        f_opt = ttk.LabelFrame(container, text="2. 배치 옵션 설정", padding=15)
        f_opt.pack(fill='x', pady=5)
        
        # 모델
        fr1 = ttk.Frame(f_opt)
        fr1.pack(fill='x', pady=5)
        ttk.Label(fr1, text="모델 (Model):", width=12).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys())
        cb_model = ttk.Combobox(fr1, textvariable=self.model_var, values=models, state="readonly", width=20)
        cb_model.pack(side='left', padx=5)
        ToolTip(cb_model, "Stage 1은 gpt-5-mini가 가장 효율적입니다.")
        
        # Effort
        ttk.Label(fr1, text="Reasoning Effort:", width=15).pack(side='left', padx=(20, 5))
        cb_effort = ttk.Combobox(fr1, textvariable=self.effort_var, values=["low", "medium", "high"], state="readonly", width=12)
        cb_effort.pack(side='left', padx=5)
        ToolTip(cb_effort, "텍스트 정제는 'low'만으로 충분합니다.")
        
        # 체크박스
        fr2 = ttk.Frame(f_opt)
        fr2.pack(fill='x', pady=5)
        chk_skip = ttk.Checkbutton(fr2, text=" 이미 ST1_결과상품명이 있는 행은 건너뛰기 (Skip)", variable=self.skip_exist_var)
        chk_skip.pack(side='left', padx=5)
        ToolTip(chk_skip, "중복 과금 방지를 위해 이미 결과가 있는 행은 제외합니다.")

        # Step 3: 실행
        f_step3 = ttk.LabelFrame(container, text="3. 실행", padding=15)
        f_step3.pack(fill='x', pady=15)
        btn_run = ttk.Button(f_step3, text="🚀 JSONL 생성 및 배치 업로드 (Start Batch)", command=self._start_create_batch, style="Success.TButton")
        btn_run.pack(fill='x', ipady=8)
        ttk.Label(container, text="※ 배치 API는 결과 수신까지 최대 24시간 소요 (비용 50% 절감)", foreground="#666").pack()

    def _select_src_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx;*.xls")])
        if p: self.src_file_var.set(p)

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
        effort = self.effort_var.get()
        
        try:
            client = OpenAI(api_key=key)
            self.append_log(f"엑셀 로드 중... {os.path.basename(src)}")
            df = pd.read_excel(src)
            
            # 필수 컬럼 확인
            req_cols = ['카테고리명', '원본상품명']
            for c in req_cols:
                if c not in df.columns:
                    raise ValueError(f"필수 컬럼({c})이 누락되었습니다.")
            
            jsonl_lines = []
            skipped_cnt = 0
            
            for idx, row in df.iterrows():
                if self.skip_exist_var.get() and "ST1_결과상품명" in df.columns:
                    val = safe_str(row.get("ST1_결과상품명", ""))
                    if val and val != "nan":
                        continue
                
                # Core 함수 호출
                payload = build_stage1_batch_payload(idx, row, model, effort)
                if payload:
                    jsonl_lines.append(json.dumps(payload, ensure_ascii=False))
                else:
                    skipped_cnt += 1
            
            if not jsonl_lines:
                self.append_log("생성할 요청 없음.")
                return

            # [Fix] BASE_DIR 사용 (Exe 환경 대응)
            base_name, _ = os.path.splitext(os.path.basename(src))
            # 같은 폴더에 JSONL 생성
            jsonl_path = os.path.join(os.path.dirname(src), f"{base_name}_stage1_batch_input.jsonl")
            
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.write("\n".join(jsonl_lines))
            
            self.append_log(f"JSONL 생성 완료: {len(jsonl_lines)}건 (스킵 {skipped_cnt}건)")
            self.append_log("OpenAI 업로드 중...")
            
            with open(jsonl_path, "rb") as f:
                batch_input_file = client.files.create(file=f, purpose="batch")
            
            file_id = batch_input_file.id
            self.append_log(f"업로드 완료 ID: {file_id}")
            
            batch_job = client.batches.create(
                input_file_id=file_id,
                endpoint="/v1/chat/completions",
                completion_window="24h"
            )
            
            batch_id = batch_job.id
            self.append_log(f"✅ 배치 시작! ID: {batch_id}")
            
            upsert_batch_job(
                batch_id=batch_id,
                src_excel=src,
                jsonl_path=jsonl_path,
                model=model,
                effort=effort,
                status=batch_job.status,
                output_file_id=None
            )
            self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])
            self._safe_msgbox("showinfo", "성공", f"배치 시작됨: {batch_id}")

        except Exception as e:
            self.append_log(f"에러: {e}")
            self._safe_msgbox("showerror", "에러", str(e))

    # ----------------------------------------------------
    # Tab 2: Manage
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
        
        # Active UI
        f_ctrl = ttk.Frame(self.sub_active)
        f_ctrl.pack(fill='x', pady=(0, 10))
        
        # 자동 갱신 옵션
        f_auto = ttk.Frame(f_ctrl)
        f_auto.pack(side='left', padx=5)
        ttk.Checkbutton(f_auto, text="자동 상태 갱신", variable=self.auto_refresh_var).pack(side='left')
        ttk.Spinbox(f_auto, from_=10, to=600, textvariable=self.refresh_interval_var, width=4).pack(side='left', padx=2)
        ttk.Label(f_auto, text="초").pack(side='left')
        
        ttk.Button(f_ctrl, text="🔄 수동 갱신", command=lambda: self._refresh_selected(self.tree_active)).pack(side='left', padx=10)
        ttk.Button(f_ctrl, text="📥 일괄 병합", command=self._merge_selected, style="Primary.TButton").pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="📊 정제 리포트", command=self._report_selected_unified, style="Success.TButton").pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="🗑 휴지통 이동", command=self._archive_selected, style="Danger.TButton").pack(side='right', padx=2)
        
        # [Model / Effort 컬럼 포함]
        cols = ("batch_id", "status", "created", "completed", "model", "effort", "counts")
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='headings', height=15, selectmode='extended')
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF') 
        for c in cols: self.tree_active.heading(c, text=c.capitalize())
        self.tree_active.column("batch_id", width=200)
        self.tree_active.column("effort", width=80, anchor="center")
        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)
        
        self.menu_active = Menu(self, tearoff=0)
        self.menu_active.add_command(label="상태 갱신", command=lambda: self._refresh_selected(self.tree_active))
        self.menu_active.add_command(label="결과 병합", command=self._merge_selected)
        self.menu_active.add_command(label="정제 리포트 생성", command=self._report_selected_unified)
        self.menu_active.add_separator()
        self.menu_active.add_command(label="휴지통으로 이동", command=self._archive_selected)
        self.tree_active.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_active, self.menu_active))
        self.tree_active.bind("<Double-1>", self._on_tree_double_click)

        # Archive UI
        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 선택 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 선택 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='headings', height=15, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2') 
        for c in cols: self.tree_arch.heading(c, text=c.capitalize())
        self.tree_arch.column("batch_id", width=200)
        self.tree_arch.column("effort", width=80, anchor="center")
        self.tree_arch.pack(fill='both', expand=True)
        
        self._load_jobs_all()
        self._load_archive_list()

    def _auto_refresh_loop(self):
        """자동 상태 갱신 루프"""
        if self.auto_refresh_var.get() and not self.is_refreshing:
            # merged, failed 등 이미 끝난 상태는 조회 대상에서 제외
            jobs = load_batch_jobs()
            active_ids = [
                j['batch_id'] for j in jobs 
                if not j.get('archived') and j.get('status') not in ['completed', 'failed', 'expired', 'cancelled', 'merged']
            ]
            if active_ids:
                t = threading.Thread(target=self._run_refresh_ids, args=(active_ids, True))
                t.daemon = True
                t.start()
        
        interval = max(10, self.refresh_interval_var.get()) * 1000
        self.after(interval, self._auto_refresh_loop)

    def _show_context_menu(self, event, tree, menu):
        item = tree.identify_row(event.y)
        if item:
            if item not in tree.selection(): tree.selection_set(item)
            menu.post(event.x_root, event.y_root)

    def _get_selected_ids(self, tree):
        selection = tree.selection()
        ids = []
        for item in selection:
            vals = tree.item(item)['values']
            if vals: ids.append(vals[0])
        return ids

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
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_active.insert("", "end", values=(
                j["batch_id"], j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt
            ), tags=(tag,))
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
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_arch.insert("", "end", values=(
                j["batch_id"], j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt
            ), tags=(tag,))
            idx += 1

    # --- Actions ---
    def _refresh_selected(self, tree):
        ids = self._get_selected_ids(tree)
        if not ids: return
        jobs = load_batch_jobs()
        completed = [bid for bid in ids if next((x for x in jobs if x["batch_id"] == bid), {}).get("status") in ["completed", "merged"]]
        if completed:
            if messagebox.askyesno("확인", f"{len(completed)}건은 이미 완료되었습니다.\n미완료 건만 갱신할까요?"):
                ids = [i for i in ids if i not in completed]
        if not ids: return
        t = threading.Thread(target=self._run_refresh_ids, args=(ids,))
        t.daemon = True
        t.start()

    def _run_refresh_ids(self, ids, silent=False):
        if self.is_refreshing: return
        self.is_refreshing = True
        
        key = self.api_key_var.get().strip()
        if not key:
            self.is_refreshing = False
            return
            
        if not silent: self.append_log(f"선택된 {len(ids)}건 갱신 중...")
        
        try:
            client = OpenAI(api_key=key)
            for bid in ids:
                try:
                    remote = client.batches.retrieve(bid)
                    rc = None
                    if remote.request_counts:
                        rc = {"total": remote.request_counts.total, "completed": remote.request_counts.completed, "failed": remote.request_counts.failed}
                    upsert_batch_job(bid, status=remote.status, output_file_id=remote.output_file_id, request_counts=rc)
                except Exception as e:
                    if not silent: self.append_log(f"{bid} 갱신 실패: {e}")
        finally:
            self.is_refreshing = False
            self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])
            if not silent: self.append_log("갱신 완료")

    def _merge_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        jobs = load_batch_jobs()
        targets = [bid for bid in ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "completed"]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed' 상태의 작업이 없습니다.")
            return
        if messagebox.askyesno("병합", f"선택한 {len(targets)}건을 병합하시겠습니까?"):
            t = threading.Thread(target=self._run_merge_multi, args=(targets,))
            t.daemon = True
            t.start()

    def _run_merge_multi(self, ids):
        key = self.api_key_var.get().strip()
        client = OpenAI(api_key=key)
        success_cnt = 0
        total_cost = 0.0
        
        for bid in ids:
            self.append_log(f"--- 병합 시작: {bid} ---")
            try:
                jobs = load_batch_jobs()
                local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                remote = client.batches.retrieve(bid)
                content = client.files.content(remote.output_file_id).content
                
                if local_job and local_job.get("src_excel"):
                    src_path = local_job["src_excel"]
                    base, _ = os.path.splitext(src_path)
                    out_jsonl = os.path.join(BASE_DIR, f"{base}_stage1_batch_output.jsonl")
                    out_excel = os.path.join(os.path.dirname(src_path), f"{base}_stage1_batch_done.xlsx")
                else:
                    out_jsonl = os.path.join(BASE_DIR, f"output_{bid}.jsonl")
                    out_excel = os.path.join(BASE_DIR, f"output_{bid}.xlsx")
                    src_path = None

                with open(out_jsonl, "wb") as f: f.write(content)
                
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
                            val = data["response"]["body"]["choices"][0]["message"]["content"].strip()
                            results_map[cid] = val
                        except: results_map[cid] = ""
                
                model_name = local_job.get("model", "gpt-5-mini") if local_job else "gpt-5-mini"
                pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0.25, "output": 2.0})
                cost = ((batch_in_tok * pricing["input"] + batch_out_tok * pricing["output"]) / 1_000_000) * 0.5
                total_cost += cost

                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    target_col = "ST1_결과상품명"
                    if target_col not in df.columns: df[target_col] = ""
                    df[target_col] = df[target_col].astype(str)
                    cnt = 0
                    for cid, val in results_map.items():
                        try:
                            idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                df.at[idx, target_col] = val
                                cnt += 1
                        except: pass
                    df.to_excel(out_excel, index=False)
                    upsert_batch_job(bid, out_excel=out_excel, status="merged")
                    
                    # History 기록 (Stage 1)
                    c_at_str = local_job.get("created_at", "")
                    c_at = datetime.fromisoformat(c_at_str) if c_at_str else get_seoul_now()
                    append_run_history(
                        stage="Stage 1 Batch",
                        model_name=model_name,
                        reasoning_effort=local_job.get("effort", "low"),
                        src_file=src_path,
                        out_file=out_excel,
                        total_rows=len(df),
                        api_rows=len(results_map),
                        elapsed_seconds=(get_seoul_now() - c_at).total_seconds(),
                        total_in_tok=batch_in_tok,
                        total_out_tok=batch_out_tok,
                        total_reasoning_tok=0,
                        input_cost_usd=0, output_cost_usd=0,
                        total_cost_usd=cost,
                        start_dt=c_at, finish_dt=get_seoul_now(),
                        api_type="batch", batch_id=bid,
                        success_rows=cnt, fail_rows=len(results_map)-cnt
                    )
                    self.append_log(f"✅ 병합 완료: {os.path.basename(out_excel)}")
                    success_cnt += 1
                else:
                    self.append_log(f"⚠️ 원본 없음. JSONL만 저장.")
            except Exception as e:
                self.append_log(f"❌ {bid} 병합 실패: {e}")
        
        self.append_log(f"=== 일괄 병합 끝 (성공: {success_cnt}, 비용: ${total_cost:.4f}) ===")
        self._load_jobs_all()
        self._safe_msgbox("showinfo", "완료", f"{success_cnt}건 병합 완료.\n총 비용: ${total_cost:.4f}")

    def _report_selected_unified(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        jobs = load_batch_jobs()
        targets = [bid for bid in ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "merged"]
        if not targets:
            messagebox.showinfo("알림", "상태가 'merged'인 작업이 없습니다.")
            return
        if messagebox.askyesno("리포트", f"선택한 {len(targets)}건의 정제 리포트를 생성합니까?"):
            t = threading.Thread(target=self._run_report_unified, args=(targets,))
            t.daemon = True
            t.start()

    def _run_report_unified(self, ids):
        self.append_log(f"--- 정제 리포트 생성 ({len(ids)}건) ---")
        jobs = load_batch_jobs()
        all_reps = []
        for bid in ids:
            local_job = next((j for j in jobs if j["batch_id"] == bid), None)
            if not local_job: continue
            out_path = local_job.get("out_excel")
            if not out_path or not os.path.exists(out_path):
                self.append_log(f"❌ 파일 누락: {bid}")
                continue
            
            try:
                df = pd.read_excel(out_path)
                if "ST1_결과상품명" not in df.columns or "원본상품명" not in df.columns: continue
                for idx, row in df.iterrows():
                    raw = safe_str(row.get("원본상품명", ""))
                    res = safe_str(row.get("ST1_결과상품명", ""))
                    
                    is_changed = "O" if raw != res else "X"
                    len_diff = len(res) - len(raw)
                    
                    all_reps.append({
                        "Batch_ID": bid,
                        "행번호": idx+2,
                        "상품코드": safe_str(row.get("상품코드", "")),
                        "원본상품명": raw,
                        "정제상품명": res,
                        "변경여부": is_changed,
                        "길이변화": f"{len(raw)} -> {len(res)} ({len_diff:+d})"
                    })
            except: pass

        if not all_reps:
            self._safe_msgbox("showinfo", "알림", "데이터 없음")
            return

        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = os.path.join(BASE_DIR, f"Stage1_Clean_Report_{ts}.xlsx")
            pd.DataFrame(all_reps).to_excel(path, index=False)
            self.append_log(f"📊 리포트 완료: {os.path.basename(path)}")
            
            self.after(0, lambda: self._ask_open_file(path))
            
        except Exception as e: self._safe_msgbox("showerror", "오류", str(e))

    def _ask_open_file(self, path):
        if messagebox.askyesno("완료", "리포트 파일을 여시겠습니까?"):
            try: os.startfile(path)
            except: pass

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
        sel = self.tree_active.selection()
        if not sel: return
        bid = self.tree_active.item(sel[0])['values'][0]
        self.batch_id_var.set(bid)
        self.main_tabs.select(self.tab_merge)

    # ----------------------------------------------------
    # Tab 3: Manual
    # ----------------------------------------------------
    def _init_tab_merge(self):
        container = ttk.Frame(self.tab_merge, padding=20)
        container.pack(fill='both', expand=True)
        f_in = ttk.LabelFrame(container, text="개별 작업", padding=15)
        f_in.pack(fill='x')
        ttk.Label(f_in, text="Batch ID:").pack(side='left')
        ttk.Entry(f_in, textvariable=self.batch_id_var, width=45, font=("Consolas", 10)).pack(side='left', padx=10)
        
        f_btn = ttk.Frame(container)
        f_btn.pack(fill='x', pady=20)
        ttk.Button(f_btn, text="1. 결과 병합", command=self._start_merge, style="Primary.TButton").pack(fill='x', pady=5)
        ttk.Button(f_btn, text="2. 단일 리포트", command=self._start_diff_report).pack(fill='x', pady=5)

    def _start_merge(self):
        bid = self.batch_id_var.get().strip()
        if not bid:
            messagebox.showwarning("경고", "Batch ID를 입력하세요.")
            return
        
        # 상태 체크 (안전장치)
        jobs = load_batch_jobs()
        job = next((j for j in jobs if j["batch_id"] == bid), None)
        if job and job.get("status") != "completed":
            if not messagebox.askyesno("경고", f"현재 상태가 '{job.get('status')}'입니다.\n그래도 병합을 시도하시겠습니까?"):
                return

        t = threading.Thread(target=self._run_merge_multi, args=([bid],))
        t.daemon = True
        t.start()

    def _start_diff_report(self):
        t = threading.Thread(target=self._run_diff_report)
        t.daemon = True
        t.start()

    def _run_diff_report(self):
        bid = self.batch_id_var.get().strip()
        if bid: self._run_report_unified([bid])

if __name__ == "__main__":
    app = Stage1BatchGUI()
    app.mainloop()