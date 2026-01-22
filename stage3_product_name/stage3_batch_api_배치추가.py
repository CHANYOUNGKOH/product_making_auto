"""
stage3_batch_api.py

Stage 3 Batch API 실행 스크립트 (GUI)
- 기능: Batch JSONL 생성 -> 업로드 -> 실행 -> 결과 병합 -> [NEW] 통합 리포트 & 휴지통
- [Fix] 배치 목록 및 휴지통에 'Effort' 컬럼 추가
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
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 상품_T2_I0.xlsx -> 상품.xlsx
    예: 상품_T3_I1.xlsx -> 상품.xlsx
    예: 상품_T2_I0(업완).xlsx -> 상품.xlsx
    예: 상품_T2_I0_T3_I1.xlsx -> 상품.xlsx (여러 버전 패턴 제거)
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)

    # 1. 버전 패턴 (_T숫자_I숫자 또는 _t숫자_i숫자) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거
    while True:
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+", "", base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등)
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")

    return base + ext


def get_next_version_path(current_path: str, task_type: str = "text") -> str:
    """
    현재 파일명을 분석해서 다음 단계의 파일명을 생성합니다.
    파일명 형식: 원본명_T{숫자}_I{숫자}.xlsx
    - task_type='text'  → T 버전 +1 (Stage1: T1, Stage2: T2, Stage3: T3, ...)
    - task_type='image' → I 버전 +1
    
    주의: 파일명에 여러 버전 패턴이 있어도 마지막 패턴만 사용합니다.
    """
    dir_name = os.path.dirname(current_path)
    base_name = os.path.basename(current_path)
    name_only, ext = os.path.splitext(base_name)

    # 괄호 안의 텍스트 제거 (예: (업완))
    name_only_clean = re.sub(r"\([^)]*\)", "", name_only)
    
    # 마지막 _T*_I* 패턴 찾기 (대소문자 구분 없음, 여러 패턴이 있어도 마지막 것만)
    all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)", name_only_clean, re.IGNORECASE))
    
    if all_matches:
        # 마지막 매칭 사용
        match = all_matches[-1]
        current_t = int(match.group(2))
        current_i = int(match.group(4))
        # 원본명은 마지막 패턴 이전까지
        original_name = name_only_clean[: match.start()].rstrip("_")
    else:
        # 패턴이 없으면 원본명에서 버전 정보 제거 후 사용
        original_name = name_only_clean
        # 기존 버전 패턴 제거
        while True:
            new_name = re.sub(r"_[Tt]\d+_[Ii]\d+", "", original_name, flags=re.IGNORECASE)
            if new_name == original_name:
                break
            original_name = new_name
        original_name = original_name.rstrip("_")
        current_t = 0
        current_i = 0

    if task_type == "text":
        new_t = current_t + 1
        new_i = current_i
    elif task_type == "image":
        new_t = current_t
        new_i = current_i + 1
    else:
        return current_path

    new_filename = f"{original_name}_T{new_t}_I{new_i}{ext}"
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


# [필수 의존성] stage3_core.py / stage3_run_history.py
try:
    from stage3_core import (
        safe_str,
        Stage3Settings,
        Stage3Request,
        build_stage3_request_from_row,
        MODEL_PRICING_USD_PER_MTOK,
        load_api_key_from_file,
        save_api_key_to_file,
    )
    from stage3_run_history import append_run_history
    _HISTORY_AVAILABLE = True
except ImportError as e:
    # 의존성 파일이 없을 경우를 대비한 더미
    _HISTORY_AVAILABLE = False
    MODEL_PRICING_USD_PER_MTOK = {}
    def safe_str(x): return str(x)
    def load_api_key_from_file(x): return ""
    def save_api_key_to_file(x, y): pass
    def append_run_history(*args, **kwargs): 
        # 더미 함수: 히스토리 기록 실패 시 조용히 무시
        pass

# === 기본 설정 ===
API_KEY_FILE = ".openai_api_key_stage3_batch"
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage3_batch_jobs.json")

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
        self.title("Stage 3: Batch API Manager (Production Generator)")
        self.geometry("1250x950") # 너비 살짝 증가
        
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

        # 1. 상단 API Key
        f_top = ttk.LabelFrame(main_container, text="🔑 API 설정", padding=10)
        f_top.pack(fill='x', pady=(0, 10))
        ttk.Label(f_top, text="Batch API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        entry_key = ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10))
        entry_key.pack(side='left', padx=10)
        ttk.Button(f_top, text="저장", command=self._save_key, style="Primary.TButton").pack(side='left')

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
        self.log_widget = ScrolledText(f_log, height=15, state='disabled', font=("Consolas", 9), bg="#F1F3F5")
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

            jsonl_lines = []
            skipped_cnt = 0
            
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get() and "ST3_결과상품명" in df.columns:
                    val = str(row.get("ST3_결과상품명", "")).strip()
                    if val and val != "nan":
                        continue
                
                try:
                    req = build_stage3_request_from_row(row, settings)
                    prompt = req.prompt
                except Exception:
                    skipped_cnt += 1
                    continue

                body = {
                    "model": model_name,
                    "messages": [{"role": "user", "content": prompt}],
                }
                
                is_reasoning = any(x in model_name for x in ["gpt-5", "o1", "o3"])
                if is_reasoning and reasoning_effort != "none":
                    body["reasoning_effort"] = reasoning_effort
                elif not is_reasoning:
                    body["temperature"] = 0.7

                request_obj = {
                    "custom_id": f"row_{idx}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": body
                }
                
                jsonl_lines.append(json.dumps(request_obj, ensure_ascii=False))
            
            if not jsonl_lines:
                self.append_log("생성할 요청 없음.")
                return

            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage3_batch_input.jsonl"
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.write("\n".join(jsonl_lines))
            
            self.append_log(f"JSONL 생성 완료: {len(jsonl_lines)}건 (스킵 {skipped_cnt}건)")
            
            # 파일 크기 및 요청 수 확인
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            info = {
                'num_requests': len(jsonl_lines),
                'file_size_mb': jsonl_size_mb
            }
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 요청 수: {info['num_requests']}개")
            
            # 청크 분할 기준 (OpenAI Batch API 제한: 200MB)
            MAX_FILE_SIZE_MB = 190
            MAX_REQUESTS_PER_BATCH = 500
            
            if jsonl_size_mb > MAX_FILE_SIZE_MB or info['num_requests'] > MAX_REQUESTS_PER_BATCH:
                reason = []
                if jsonl_size_mb > MAX_FILE_SIZE_MB:
                    reason.append(f"파일 크기 ({jsonl_size_mb:.2f}MB > {MAX_FILE_SIZE_MB}MB)")
                if info['num_requests'] > MAX_REQUESTS_PER_BATCH:
                    reason.append(f"요청 수 ({info['num_requests']}개 > {MAX_REQUESTS_PER_BATCH}개)")
                self.append_log(f"[INFO] {' 및 '.join(reason)}로 인해 분할 처리합니다... (OpenAI 제한: 200MB)")
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
                    max_requests=MAX_REQUESTS_PER_BATCH,
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
            self.append_log(f"에러: {e}")
            messagebox.showerror("에러", str(e))
    
    def _create_batch_from_jsonl(self, client, jsonl_path, excel_path, model_name, reasoning_effort, settings):
        """JSONL 파일을 업로드하고 배치를 생성하는 헬퍼 함수"""
        if not os.path.exists(jsonl_path):
            raise FileNotFoundError(f"입력 JSONL 파일을 찾을 수 없습니다: {jsonl_path}")

        with open(jsonl_path, "rb") as f:
            # 타임아웃 설정: 대용량 파일 업로드를 위해 10분으로 설정
            up_file = client.files.create(file=f, purpose="batch", timeout=600)

        batch = client.batches.create(
            input_file_id=up_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        return batch
    
    def _create_batch_chunks(self, client, jsonl_path, excel_path, model_name, effort, settings, max_size_mb=190, max_requests=500):
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
        # 예상 청크 수 계산 (파일 크기와 요청 수 모두 고려)
        original_file_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
        estimated_chunks_by_size = max(1, int(original_file_size_mb / max_size_mb) + 1)
        estimated_chunks_by_count = (total_requests + max_requests - 1) // max_requests
        estimated_total_chunks = max(estimated_chunks_by_size, estimated_chunks_by_count)
        
        self.append_log(f"[INFO] 총 {total_requests}개 요청을 분할합니다... (예상: 약 {estimated_total_chunks}개 청크, 그룹 ID: {batch_group_id})")
        
        batch_ids = []
        chunk_num = 0
        chunk_files_created = []  # 생성된 청크 파일 목록 (정리용)
        
        i = 0
        while i < total_requests:
            chunk_num += 1
            chunk_requests = []
            chunk_size_bytes = 0  # 바이트 단위로 정확히 계산
            
            # 청크 생성 (크기 또는 개수 제한)
            while i < total_requests and len(chunk_requests) < max_requests:
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
            base, ext = os.path.splitext(jsonl_path)
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
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = retry_count * 5  # 5초, 10초, 15초 대기
                        self.append_log(f"⚠️ 청크 {chunk_num} 배치 생성 실패 (재시도 {retry_count}/{max_retries}): {e}")
                        self.append_log(f"[INFO] {wait_time}초 후 재시도합니다...")
                        import time
                        time.sleep(wait_time)
                    else:
                        self.append_log(f"❌ 청크 {chunk_num} 배치 생성 최종 실패: {e}")
                        import traceback
                        self.append_log(traceback.format_exc())
        
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
        
        # [NEW] Effort 컬럼 및 그룹 컬럼 추가
        cols = ("batch_id", "status", "created", "completed", "model", "effort", "market", "counts", "group")
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='headings', height=18, selectmode='extended')
        
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF')
        self.tree_active.tag_configure('group', background='#E8F4FD')
        
        self.tree_active.heading("batch_id", text="Batch ID")
        self.tree_active.heading("status", text="상태")
        self.tree_active.heading("created", text="생성일시")
        self.tree_active.heading("completed", text="완료일시")
        self.tree_active.heading("model", text="모델")
        self.tree_active.heading("effort", text="Effort")
        self.tree_active.heading("market", text="마켓")
        self.tree_active.heading("counts", text="완료/전체")
        self.tree_active.heading("group", text="그룹")
        
        self.tree_active.column("batch_id", width=180)
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
        self.menu_active.add_command(label="휴지통으로 이동", command=self._archive_selected)
        
        self.tree_active.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_active, self.menu_active))
        self.tree_active.bind("<Double-1>", self._on_tree_double_click)

        # --- Archive Tab UI ---
        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 선택 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 선택 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')
        
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='headings', height=18, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2') 

        for col in cols: 
            self.tree_arch.heading(col, text=col.capitalize())
            self.tree_arch.column(col, anchor="center")
        self.tree_arch.column("batch_id", width=200, anchor="w")
        
        self.tree_arch.pack(fill='both', expand=True)
        
        self._load_jobs_all()
        self._load_archive_list()

    def _show_context_menu(self, event, tree, menu):
        item = tree.identify_row(event.y)
        if item:
            if item not in tree.selection():
                tree.selection_set(item)
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
        
        # 그룹별 배치 표시
        for group_id, group_jobs in sorted(grouped_jobs.items()):
            # 그룹 내 배치들을 청크 번호 순으로 정렬
            group_jobs.sort(key=lambda x: x.get("chunk_index", 0))
            total_chunks = group_jobs[0].get("total_chunks", len(group_jobs))
            
            for j in group_jobs:
                cnt = "-"
                if "request_counts" in j and j["request_counts"]:
                    rc = j["request_counts"]
                    cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
                c_at = (j.get("created_at") or "")[:16].replace("T", " ")
                f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
                chunk_info = f"{j.get('chunk_index', 0)}/{total_chunks}"
                group_display = f"그룹 {chunk_info}"
                tag = 'group' if idx % 2 == 0 else 'group'
                self.tree_active.insert("", "end", values=(
                    j["batch_id"], j.get("status"), 
                    c_at, f_at, 
                    j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, group_display
                ), tags=(tag,))
                idx += 1
        
        # 그룹 없는 배치 표시
        for j in ungrouped_jobs:
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_active.insert("", "end", values=(
                j["batch_id"], j.get("status"), 
                c_at, f_at, 
                j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, "-"
            ), tags=(tag,))
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
        
        # 그룹별 배치 표시
        for group_id, group_jobs in sorted(grouped_jobs.items()):
            group_jobs.sort(key=lambda x: x.get("chunk_index", 0))
            total_chunks = group_jobs[0].get("total_chunks", len(group_jobs))
            
            for j in group_jobs:
                cnt = "-"
                if "request_counts" in j and j["request_counts"]:
                    rc = j["request_counts"]
                    cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
                c_at = (j.get("created_at") or "")[:16].replace("T", " ")
                f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
                chunk_info = f"{j.get('chunk_index', 0)}/{total_chunks}"
                group_display = f"그룹 {chunk_info}"
                tag = 'group' if idx % 2 == 0 else 'group'
                self.tree_arch.insert("", "end", values=(
                    j["batch_id"], j.get("status"), 
                    c_at, f_at, 
                    j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, group_display
                ), tags=(tag,))
                idx += 1
        
        # 그룹 없는 배치 표시
        for j in ungrouped_jobs:
            cnt = "-"
            if "request_counts" in j and j["request_counts"]:
                rc = j["request_counts"]
                cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
            c_at = (j.get("created_at") or "")[:16].replace("T", " ")
            f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_arch.insert("", "end", values=(
                j["batch_id"], j.get("status"), 
                c_at, f_at, 
                j.get("model"), j.get("effort", "-"), j.get("market", "-"), cnt, "-"
            ), tags=(tag,))
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
                upsert_batch_job(bid, status=remote.status, output_file_id=remote.output_file_id, request_counts=rc)
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
        for group_id in group_ids:
            if group_id:
                group_batches = [j for j in jobs if j.get("batch_group_id") == group_id and j.get("status") == "completed"]
                for j in group_batches:
                    all_target_ids.add(j["batch_id"])
        
        if len(all_target_ids) > len(ids):
            group_info = f"\n\n같은 그룹의 배치 {len(all_target_ids) - len(ids)}개가 자동으로 포함됩니다."
        else:
            group_info = ""
        
        # completed 또는 merged 상태인 배치 모두 선택 가능
        targets = [bid for bid in all_target_ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") in ["completed", "merged"]]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed' 또는 'merged' 상태의 작업이 없습니다.")
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
                        
                        # 이미 병합된 배치는 건너뛰기
                        if local_job.get("status") == "merged":
                            self.append_log(f"  ⏭️ {bid}: 이미 병합 완료된 작업입니다. 건너뜁니다.")
                            continue
                        
                        # Batch 상태 확인
                        remote = client.batches.retrieve(bid)
                        if remote.status != "completed":
                            self.append_log(f"  ⚠️ {bid}: 아직 completed 상태가 아니어서 건너뜁니다. (status={remote.status})")
                            upsert_batch_job(bid, status=remote.status, output_file_id=getattr(remote, "output_file_id", None))
                            continue
                        
                        output_file_id = getattr(remote, "output_file_id", None)
                        if not output_file_id:
                            self.append_log(f"  ⚠️ {bid}: output_file_id를 찾을 수 없어 건너뜁니다.")
                            continue
                        
                        # 결과 다운로드
                        base_dir = os.path.dirname(src_path)
                        base_name, _ = os.path.splitext(os.path.basename(src_path))
                        out_jsonl = os.path.join(base_dir, f"{base_name}_stage3_batch_output_{bid}.jsonl")
                        
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
                    downloaded_batch_ids = []
                    for bid in batch_ids_sorted:
                        local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                        if local_job and local_job.get("status") == "completed":
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
                batch_in_tok = 0
                batch_out_tok = 0
                
                with open(merged_jsonl, "r", encoding="utf-8") as f:
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
                
                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    if "ST3_결과상품명" not in df.columns:
                        df["ST3_결과상품명"] = ""
                    df["ST3_결과상품명"] = df["ST3_결과상품명"].astype(str)
                    cnt = 0
                    for cid, val in results_map.items():
                        try:
                            idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                df.at[idx, "ST3_결과상품명"] = val
                                cnt += 1
                        except:
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
                    
                    # 비용 계산
                    pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                    cost_in = (batch_in_tok / 1_000_000) * pricing["input"] * 0.5
                    cost_out = (batch_out_tok / 1_000_000) * pricing["output"] * 0.5
                    cost_total = cost_in + cost_out
                    total_group_cost += cost_total
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
                                c_at = datetime.fromisoformat(c_at_str)
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
                                total_in_tok=batch_in_tok,
                                total_out_tok=batch_out_tok,
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
                    
                    self.append_log(f"✅ 그룹 {group_id} 병합 완료 ({cnt}건): {os.path.basename(out_path_for_history)}")
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

                # 이미 병합된 배치는 파일 재병합은 건너뛰되, 히스토리 기록은 수행
                is_already_merged = local_job.get("status") == "merged"
                if is_already_merged:
                    self.append_log(f"⏭️ {bid}: 이미 병합 완료된 작업입니다. 히스토리 기록만 수행합니다.")
                    # merged 상태인 경우 기존 파일 경로 사용
                    src_path = local_job.get("src_excel")
                    out_path_for_history = local_job.get("out_excel")
                    if not src_path or not out_path_for_history or not os.path.exists(out_path_for_history):
                        self.append_log(f"⚠️ {bid}: 병합된 파일을 찾을 수 없어 히스토리 기록을 건너뜁니다.")
                        continue
                    
                    # 기존 파일에서 토큰 정보 추출 (JSONL 파일이 있으면)
                    base, _ = os.path.splitext(src_path) if src_path else ("", "")
                    out_jsonl = f"{base}_stage3_batch_output.jsonl"
                    batch_in_tok = 0
                    batch_out_tok = 0
                    results_map = {}
                    
                    if os.path.exists(out_jsonl):
                        try:
                            with open(out_jsonl, "r", encoding="utf-8") as f:
                                for line in f:
                                    if not line.strip(): continue
                                    data = json.loads(line)
                                    usage = data.get("response", {}).get("body", {}).get("usage", {})
                                    batch_in_tok += usage.get("prompt_tokens", 0)
                                    batch_out_tok += usage.get("completion_tokens", 0)
                                    cid = data.get("custom_id")
                                    try:
                                        val = data["response"]["body"]["choices"][0]["message"]["content"].strip()
                                        results_map[cid] = val
                                    except: pass
                        except Exception as e:
                            self.append_log(f"[WARN] {bid}: JSONL 파일 읽기 실패: {e}")
                    
                    # 비용 계산
                    model_name = local_job.get("model", "gpt-5-mini")
                    pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                    cost_in = (batch_in_tok / 1_000_000) * pricing["input"] * 0.5
                    cost_out = (batch_out_tok / 1_000_000) * pricing["output"] * 0.5
                    cost_total = cost_in + cost_out
                    
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
                            c_at = datetime.fromisoformat(c_at_str)
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
                
                # 최신 상태 확인 (갱신 후 상태가 변경되었을 수 있음)
                if remote.status != "completed":
                    self.append_log(f"⚠️ {bid}: 아직 completed 상태가 아니어서 병합을 건너뜁니다. (status={remote.status})")
                    # 상태 업데이트
                    upsert_batch_job(bid, status=remote.status, output_file_id=getattr(remote, "output_file_id", None))
                    continue
                
                # output_file_id 확인
                output_file_id = getattr(remote, "output_file_id", None)
                if not output_file_id:
                    self.append_log(f"❌ {bid} 병합 실패: output_file_id 를 찾을 수 없습니다. (status={remote.status})")
                    upsert_batch_job(bid, status=remote.status)
                    continue
                
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
                
                if local_job and local_job.get("src_excel"):
                    src_path = local_job["src_excel"]
                    base, _ = os.path.splitext(src_path)
                    out_jsonl = f"{base}_stage3_batch_output.jsonl"
                    out_excel = f"{base}_stage3_batch_done.xlsx"
                else:
                    out_jsonl = f"output_{bid}.jsonl"
                    out_excel = f"output_{bid}.xlsx"
                    src_path = None

                with open(out_jsonl, "wb") as f:
                    f.write(content)
                
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
                            idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                df.at[idx, "ST3_결과상품명"] = val
                                cnt += 1
                        except:
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
                            c_at = datetime.fromisoformat(c_at_str)
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
        sel = self.tree_active.selection()
        if not sel: return
        bid = self.tree_active.item(sel[0])['values'][0]
        self.batch_id_var.set(bid)
        self.main_tabs.select(self.tab_merge)

    # ----------------------------------------------------
    # Tab 3: Merge (Manual)
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

if __name__ == "__main__":
    app = Stage3BatchGUI()
    app.mainloop()