"""
bg_Batch_prompt_gui_Casche.py

Stage 3-2: 전처리 -> AI배경생성 분석 작업 (배치/대량) - 캐싱 최적화 버전
- 기능: Batch JSONL 생성 -> 업로드 -> 실행 -> 결과 병합
- bg_prompt_core_Casche.py를 사용하여 배치 처리
- 입력: I2 또는 I3 파일만 허용
- 출력: 항상 I4로 고정
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
  * 동적 콘텐츠(입력 데이터)를 user 프롬프트에 배치
  * prompt_cache_key 사용으로 캐시 히트율 향상 (토큰 비용 최대 90% 절감 가능)
"""

import os
import json
import re
import threading
import subprocess
import platform
from datetime import datetime
from typing import Optional

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI

# [필수 의존성] bg_prompt_core_Casche.py
# 캐싱 최적화 버전 사용 (bg_prompt_core_Casche.py)
try:
    from bg_prompt_core_Casche import (
        API_KEY_FILE,
        DEFAULT_MODEL,
        load_api_key_from_file,
        save_api_key_to_file,
        build_bg_prompt_messages,
        build_bg_prompt_batch_payload,  # Batch API용 payload 빌더 (캐싱 최적화)
        MODEL_PRICING_USD_PER_MTOK,
    )
    CACHE_MODE_CORE = True
except ImportError:
    # 캐싱 버전이 없으면 일반 버전 사용
    try:
        from bg_prompt_core import (
            API_KEY_FILE,
            DEFAULT_MODEL,
            load_api_key_from_file,
            save_api_key_to_file,
            build_bg_prompt_messages,
            MODEL_PRICING_USD_PER_MTOK,
        )
        CACHE_MODE_CORE = False
        def build_bg_prompt_batch_payload(*args, **kwargs): return None
    except ImportError:
        # 의존성 파일 부재 시 비상용 더미
        CACHE_MODE_CORE = False
        API_KEY_FILE = ".openai_api_key_bg_prompt"
        DEFAULT_MODEL = "gpt-5-mini"
        MODEL_PRICING_USD_PER_MTOK = {}
        def load_api_key_from_file(x): return ""
        def save_api_key_to_file(x, y): pass
        def build_bg_prompt_messages(*args, **kwargs): return []
        def build_bg_prompt_batch_payload(*args, **kwargs): return None

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, T4(완)_I* 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T3_I3.xlsx -> 아디다스.xlsx
    예: 나이키_T0_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T0_I0_T1_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 나이키_T4(완)_I3.xlsx -> 나이키.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # 1. 버전 패턴 (_T숫자(괄호)?_I숫자 또는 _t숫자(괄호)?_i숫자) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거, T4(완)_I* 패턴도 포함
    while True:
        new_base = re.sub(r"_[Tt]\d+\([^)]*\)_[Ii]\d+", "", base, flags=re.IGNORECASE)  # T4(완)_I* 패턴 제거
        new_base = re.sub(r"_[Tt]\d+_[Ii]\d+", "", new_base, flags=re.IGNORECASE)  # 일반 T*_I* 패턴 제거
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등) - 버전 패턴의 괄호는 이미 제거됨
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_bg_prompt_done", "_bg_prompt_batch_done", "_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext


def get_i4_output_path(input_path: str) -> str:
    """
    입력 파일명을 분석해서 I4로 고정된 출력 파일명을 생성합니다.
    입력: I2 또는 I3 파일 (예: 상품_T3_I2.xlsx, 상품_T3_I3.xlsx, 상품_T4(완)_I3.xlsx)
    출력: 항상 I4 (예: 상품_T3_I4.xlsx, 상품_T4(완)_I4.xlsx)
    """
    dir_name = os.path.dirname(input_path)
    base_name = os.path.basename(input_path)
    name_only, ext = os.path.splitext(base_name)

    # T4(완)_I* 또는 일반 _T*_I* 패턴 매칭
    pattern = r"_T(\d+)(\([^)]+\))?_I(\d+)$"
    match = re.search(pattern, name_only, re.IGNORECASE)

    if match:
        current_t = int(match.group(1))
        t_suffix = match.group(2) or ""  # (완) 부분이 있으면 유지
        original_name = name_only[: match.start()]
    else:
        # 버전 정보가 없으면 T 버전 추출 시도 (괄호 포함 가능)
        t_match = re.search(r"_T(\d+)(\([^)]+\))?", name_only, re.IGNORECASE)
        if t_match:
            current_t = int(t_match.group(1))
            t_suffix = t_match.group(2) or ""
            original_name = name_only[: t_match.start()]
        else:
            current_t = 0
            t_suffix = ""
            original_name = name_only

    # 항상 I4로 고정, T 부분은 그대로 유지 (예: T4(완) 또는 T4)
    new_filename = f"{original_name}_T{current_t}{t_suffix}_I4{ext}"
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
    def update_status(cls, filename, text_msg=None, img_msg=None, img_s3_1_msg=None, img_s3_2_msg=None):
        """
        작업 상태를 업데이트합니다.
        
        Args:
            filename: 파일명 (root filename)
            text_msg: 텍스트 상태 메시지
            img_msg: 이미지 전체 상태 메시지 (하위 호환성)
            img_s3_1_msg: Stage 3-1 (썸네일 분석) 상태 메시지
            img_s3_2_msg: Stage 3-2 (전처리) 상태 메시지
        """
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
                "image_s3_1_status": "-",  # Stage 3-1: 썸네일 분석
                "image_s3_1_time": "-",
                "image_s3_2_status": "-",  # Stage 3-2: 전처리
                "image_s3_2_time": "-",
                "memo": "",
            }

        if text_msg:
            data[filename]["text_status"] = text_msg
            data[filename]["text_time"] = now
        
        if img_msg:
            # 하위 호환성: 기존 image_status도 업데이트
            data[filename]["image_status"] = img_msg
            data[filename]["image_time"] = now
        
        if img_s3_1_msg:
            data[filename]["image_s3_1_status"] = img_s3_1_msg
            data[filename]["image_s3_1_time"] = now
            # image_status 통합 업데이트 (S3-1, S3-2 접두사 제거)
            parts = []
            if data[filename].get("image_s3_1_status", "-") != "-":
                parts.append(data[filename]['image_s3_1_status'])  # "I3-1 (진행중)" 형식 그대로
            if data[filename].get("image_s3_2_status", "-") != "-":
                parts.append(data[filename]['image_s3_2_status'])  # "I3-2 (완료)" 형식 그대로
            if parts:
                data[filename]["image_status"] = " / ".join(parts)
                data[filename]["image_time"] = now
        
        if img_s3_2_msg:
            data[filename]["image_s3_2_status"] = img_s3_2_msg
            data[filename]["image_s3_2_time"] = now
            # image_status 통합 업데이트 (S3-1, S3-2 접두사 제거)
            parts = []
            if data[filename].get("image_s3_1_status", "-") != "-":
                parts.append(data[filename]['image_s3_1_status'])  # "I3-1 (진행중)" 형식 그대로
            if data[filename].get("image_s3_2_status", "-") != "-":
                parts.append(data[filename]['image_s3_2_status'])  # "I3-2 (완료)" 형식 그대로
            if parts:
                data[filename]["image_status"] = " / ".join(parts)
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


# === 기본 설정 ===
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "bg_prompt_batch_jobs.json")

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

def get_excel_name_from_path(path: str) -> str:
    """전체 경로에서 파일명만 추출"""
    if not path:
        return "-"
    return os.path.basename(path)

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
class BGPromptBatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 3-2: 전처리 -> AI배경생성 분석 작업 (배치/대량) 🚀 캐싱 최적화 버전")
        self.geometry("1250x950")
        
        self.api_key_var = tk.StringVar()
        
        # 파일 변수
        self.src_file_var = tk.StringVar()
        self.skip_exist_var = tk.BooleanVar(value=True)
        self.jsonl_file_var = tk.StringVar()  # 생성된 JSONL 파일 경로
        
        # 모델 설정 변수
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="low")
        
        # 탭 3 변수
        self.batch_id_var = tk.StringVar()
        self.failed_chunks_file_var = tk.StringVar()
        
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
        self.main_tabs.add(self.tab_manage, text=" 2. 배치 관리 (목록/병합) ")
        self.main_tabs.add(self.tab_merge, text=" 3. 개별 병합 (수동) ")
        
        self._init_tab_create()
        self._init_tab_manage()
        self._init_tab_merge()
        
        # 3. 로그
        f_log = ttk.LabelFrame(main_container, text="📋 시스템 로그", padding=10)
        f_log.pack(fill='both', expand=True, pady=(10, 0))
        self.log_widget = ScrolledText(f_log, height=22, state='disabled', font=("Consolas", 9), bg="#F1F3F5")
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
        f_file = ttk.LabelFrame(container, text="1. 작업 대상 파일 (ST2_JSON 포함, I2 또는 I3)", padding=15)
        f_file.pack(fill='x', pady=(0, 15))
        ttk.Entry(f_file, textvariable=self.src_file_var, font=("맑은 고딕", 10)).pack(side='left', fill='x', expand=True)
        ttk.Button(f_file, text="📂 파일 찾기", command=self._select_src_file).pack(side='right', padx=5)
        
        # Step 2: 모델 설정
        f_opt = ttk.LabelFrame(container, text="2. 모델 설정", padding=15)
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
        
        # 체크박스
        f_row_chk = ttk.Frame(f_opt)
        f_row_chk.pack(fill='x', pady=10)
        ttk.Checkbutton(f_row_chk, text=" 이미 bg_positive_en 등이 있는 행 건너뛰기", variable=self.skip_exist_var).pack(side='left')
        
        # Step 3: JSONL 생성
        f_step3 = ttk.LabelFrame(container, text="3. JSONL 생성", padding=15)
        f_step3.pack(fill='x', pady=15)
        
        # 생성된 JSONL 파일 경로 표시
        f_jsonl = ttk.Frame(f_step3)
        f_jsonl.pack(fill='x', pady=(0, 10))
        ttk.Label(f_jsonl, text="JSONL 파일 경로:", font=("맑은 고딕", 9)).pack(side='left')
        ttk.Entry(f_jsonl, textvariable=self.jsonl_file_var, font=("Consolas", 9), width=60).pack(side='left', padx=5, fill='x', expand=True)
        ttk.Button(f_jsonl, text="📂 찾기", command=self._select_jsonl_file).pack(side='right', padx=5)
        
        # 분리된 버튼들
        f_btn_separated = ttk.Frame(f_step3)
        f_btn_separated.pack(fill='x', pady=5)
        btn_create = ttk.Button(f_btn_separated, text="📄 JSONL 생성만 (Create JSONL)", command=self._create_jsonl_only, style="Primary.TButton")
        btn_create.pack(side='left', fill='x', expand=True, padx=(0, 5), ipady=6)
        btn_upload = ttk.Button(f_btn_separated, text="⬆️ 배치 업로드 (Upload Batch)", command=self._upload_batch_from_jsonl, style="Success.TButton")
        btn_upload.pack(side='right', fill='x', expand=True, padx=(5, 0), ipady=6)
        
        # 구분선
        ttk.Separator(f_step3, orient='horizontal').pack(fill='x', pady=15)
        
        # 통합 버튼 (기존 기능 유지)
        f_step4 = ttk.LabelFrame(container, text="4. 통합 실행 (기존 방식)", padding=15)
        f_step4.pack(fill='x', pady=15)
        btn_integrated = ttk.Button(f_step4, text="🚀 JSONL 생성 및 배치 업로드 (통합)", command=self._start_create_batch, style="Success.TButton")
        btn_integrated.pack(fill='x', ipady=8)
        ttk.Label(container, text="※ 배치 API는 결과 수신까지 최대 24시간이 소요됩니다. (비용 50% 절감)", foreground="#666").pack(pady=(5, 0))

    def _select_jsonl_file(self):
        """JSONL 파일 선택"""
        p = filedialog.askopenfilename(
            title="JSONL 파일 선택",
            filetypes=[("JSONL", "*.jsonl"), ("모든 파일", "*.*")]
        )
        if p:
            self.jsonl_file_var.set(p)
            self.append_log(f"JSONL 파일 선택됨: {os.path.basename(p)}")
    
    def _select_src_file(self):
        p = filedialog.askopenfilename(
            title="배경 분석 엑셀 선택 (T2 이상 _I3 버전만 가능)",
            filetypes=[("Excel", "*.xlsx;*.xls")]
        )
        if p:
            base_name = os.path.basename(p)
            
            # T 버전과 I 버전 검증
            t_match = re.search(r"_T(\d+)", base_name, re.IGNORECASE)
            i_match = re.search(r"_I(\d+)", base_name, re.IGNORECASE)
            
            if not t_match or not i_match:
                messagebox.showerror(
                    "오류",
                    f"파일명에 버전 정보(_T*_I*)가 없습니다.\n\n"
                    f"선택한 파일: {base_name}\n\n"
                    f"T2 이상 _I3 버전 파일을 선택해주세요.\n"
                    f"(예: T2_I3, T4_I3, T4(완)_I3)"
                )
                return
            
            t_version = int(t_match.group(1))
            i_version = int(i_match.group(1))
            
            # T2 이상이고 I3인지 검증
            if t_version < 2:
                messagebox.showerror(
                    "오류",
                    f"입력 파일은 T2 이상 _I3 버전만 허용됩니다.\n\n"
                    f"선택한 파일: {base_name}\n"
                    f"현재 버전: T{t_version}_I{i_version}\n\n"
                    f"T2 이상 _I3 버전 파일을 선택해주세요.\n"
                    f"(예: T2_I3, T4_I3, T4(완)_I3)"
                )
                return
            
            if i_version != 3:
                messagebox.showerror(
                    "오류",
                    f"입력 파일은 T2 이상 _I3 버전만 허용됩니다.\n\n"
                    f"선택한 파일: {base_name}\n"
                    f"현재 버전: T{t_version}_I{i_version}\n\n"
                    f"T2 이상 _I3 버전 파일을 선택해주세요.\n"
                    f"(예: T2_I3, T4_I3, T4(완)_I3)"
                )
                return
            
            # 엑셀 파일 검증
            try:
                df_check = pd.read_excel(p)
                
                # view_point 컬럼 필수 체크
                if "view_point" not in df_check.columns:
                    messagebox.showerror(
                        "필수 컬럼 누락",
                        f"'{base_name}' 파일에 'view_point' 컬럼이 없습니다.\n\n"
                        f"I3 썸네일 구도 분석 작업을 먼저 완료해주세요."
                    )
                    return
                
                # view_point가 비어있는 행이 있는지 체크
                view_point_empty = df_check["view_point"].isna() | (df_check["view_point"].astype(str).str.strip() == "")
                if view_point_empty.any():
                    empty_count = view_point_empty.sum()
                    if messagebox.askyesno(
                        "경고",
                        f"'{base_name}' 파일에 'view_point'가 비어있는 행이 {empty_count}개 있습니다.\n\n"
                        f"이 행들은 처리되지 않습니다.\n\n"
                        f"계속 진행하시겠습니까?"
                    ) == False:
                        return
                
                # bg_positive_en 컬럼에 이미 내용이 있는지 체크
                if "bg_positive_en" in df_check.columns:
                    has_content = df_check["bg_positive_en"].notna() & (df_check["bg_positive_en"].astype(str).str.strip() != "")
                    if has_content.any():
                        content_count = has_content.sum()
                        if messagebox.askyesno(
                            "경고",
                            f"'{base_name}' 파일에 'bg_positive_en'에 이미 내용이 있는 행이 {content_count}개 있습니다.\n\n"
                            f"이미 완료된 작업입니다. 건너뛰기 옵션이 활성화되어 있으면 해당 행은 처리되지 않습니다.\n\n"
                            f"계속 진행하시겠습니까?"
                        ) == False:
                            return
                
                # 검증 통과
                self.src_file_var.set(p)
                self.append_log(f"파일 선택됨: {base_name} (I3)")
            except Exception as e:
                messagebox.showerror("파일 읽기 오류", f"엑셀 파일을 읽는 중 오류가 발생했습니다:\n{e}")
                return

    def _create_jsonl_only(self):
        """JSONL 생성만 수행 (분리된 버튼)"""
        if not self.src_file_var.get():
            messagebox.showwarning("오류", "파일을 선택해주세요.")
            return
        t = threading.Thread(target=self._run_create_jsonl)
        t.daemon = True
        t.start()
    
    def _upload_batch_from_jsonl(self):
        """JSONL 파일에서 배치 업로드만 수행 (분리된 버튼)"""
        if not self.api_key_var.get():
            messagebox.showwarning("오류", "API Key가 필요합니다.")
            return
        
        # JSONL 파일 경로 확인
        jsonl_path = self.jsonl_file_var.get().strip()
        if not jsonl_path or not os.path.exists(jsonl_path):
            messagebox.showwarning("오류", "JSONL 파일을 선택해주세요.\n\n먼저 'JSONL 생성만' 버튼을 실행하거나, 기존 JSONL 파일을 선택해주세요.")
            return
        
        # 원본 엑셀 파일 경로 확인 (메타데이터 저장용)
        if not self.src_file_var.get():
            # JSONL 파일명에서 원본 파일 경로 추론 시도
            jsonl_basename = os.path.basename(jsonl_path)
            if "_bg_prompt_batch_input.jsonl" in jsonl_basename:
                base_path = jsonl_path.replace("_bg_prompt_batch_input.jsonl", "")
                # 가능한 확장자 시도
                for ext in [".xlsx", ".xls"]:
                    candidate = base_path + ext
                    if os.path.exists(candidate):
                        self.src_file_var.set(candidate)
                        break
        
        t = threading.Thread(target=self._run_upload_batch, args=(jsonl_path,))
        t.daemon = True
        t.start()
    
    def _start_create_batch(self):
        """통합 버튼: JSONL 생성 및 배치 업로드 (기존 기능 유지)"""
        if not self.api_key_var.get():
            messagebox.showwarning("오류", "API Key가 필요합니다.")
            return
        if not self.src_file_var.get():
            messagebox.showwarning("오류", "파일을 선택해주세요.")
            return
        t = threading.Thread(target=self._run_create_batch)
        t.daemon = True
        t.start()
    
    def _run_create_jsonl(self):
        """JSONL 생성만 수행하는 로직"""
        src = self.src_file_var.get().strip()
        model_name = self.model_var.get().strip() or "gpt-5-mini"
        reasoning_effort = self.effort_var.get().strip() or "low"
        
        try:
            self.append_log(f"엑셀 로드 중... {os.path.basename(src)}")
            df = pd.read_excel(src)
            
            if "ST2_JSON" not in df.columns:
                raise ValueError("필수 컬럼(ST2_JSON)이 누락되었습니다. Stage 2를 먼저 완료하세요.")
            
            # 캐싱 모드 확인 및 로그
            if CACHE_MODE_CORE:
                self.append_log(f"[INFO] 🚀 프롬프트 캐싱 최적화 모드 활성화 (bg_prompt_core_Casche.py)")
            else:
                self.append_log(f"[INFO] ⚠️ 일반 모드 (bg_prompt_core.py) - 캐싱 최적화 미적용")
            
            self.append_log(f"설정: 모델={model_name}, effort={reasoning_effort}")

            # 전체 대상 요청 수 계산
            target_rows = 0
            result_cols = ["bg_positive_en", "bg_negative_en", "video_motion_prompt_en", "video_full_prompt_en"]
            
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get():
                    has_result = False
                    for col in result_cols:
                        val = str(row.get(col, "")).strip()
                        if val and val != "nan":
                            has_result = True
                            break
                    if has_result:
                        continue
                
                # ST2_JSON 확인
                st2_json_raw = str(row.get("ST2_JSON", "")).strip()
                if not st2_json_raw or st2_json_raw == "nan":
                    continue
                
                # view_point 필수 체크
                view_point_val = str(row.get("view_point", "")).strip()
                if not view_point_val or view_point_val == "nan":
                    continue
                
                target_rows += 1

            # 버킷 수 계산 (프롬프트 캐싱 최적화)
            if CACHE_MODE_CORE and target_rows > 0:
                PROMPT_CACHE_BUCKETS = 1
                self.append_log(f"[INFO] 프롬프트 캐싱: 키 고정 전략 사용 (모든 요청이 'bg_prompt_v1' 키 공유)")
                self.append_log(f"[INFO] 예상 요청 수: {target_rows}개, 캐시 히트율 향상 예상")
            else:
                PROMPT_CACHE_BUCKETS = 1

            jsonl_lines = []
            skipped_cnt = 0
            seen_custom_ids = set()
            duplicate_count = 0
            
            # JSONL 라인 생성
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get():
                    has_result = False
                    for col in result_cols:
                        val = str(row.get(col, "")).strip()
                        if val and val != "nan":
                            has_result = True
                            break
                    if has_result:
                        skipped_cnt += 1
                        continue
                
                # ST2_JSON 확인
                st2_json_raw = str(row.get("ST2_JSON", "")).strip()
                if not st2_json_raw or st2_json_raw == "nan":
                    skipped_cnt += 1
                    continue
                
                # view_point 필수 체크
                view_point_val = str(row.get("view_point", "")).strip()
                if not view_point_val or view_point_val == "nan":
                    self.append_log(f"[Row {idx+1}] view_point가 비어있습니다. 썸네일 구도 분석이 필요합니다. 건너뜁니다.")
                    skipped_cnt += 1
                    continue
                
                try:
                    # ST2_JSON 파싱 시도
                    try:
                        st2_parsed = json.loads(st2_json_raw)
                        st2_for_model = json.dumps(st2_parsed, ensure_ascii=False, indent=2)
                    except json.JSONDecodeError:
                        st2_for_model = st2_json_raw
                    
                    # IMG_ANALYSIS_JSON 구성
                    img_analysis_data = None
                    img_analysis_cols = [
                        "view_point", "subject_position", "subject_size", "lighting_condition",
                        "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
                        "bg_layout_hint_en"
                    ]
                    if any(col in df.columns for col in img_analysis_cols):
                        img_analysis_data = {}
                        for col in img_analysis_cols:
                            if col in df.columns:
                                val = str(row.get(col, "")).strip()
                                if val and val != "nan":
                                    if col == "is_flat_lay":
                                        img_analysis_data[col] = val.lower() in ["true", "1", "yes", "y"]
                                    else:
                                        img_analysis_data[col] = val
                    
                    # 캐싱 최적화 모드: build_bg_prompt_batch_payload 사용
                    if CACHE_MODE_CORE:
                        request_obj = build_bg_prompt_batch_payload(
                            row_index=idx,
                            st2_json_raw=st2_for_model,
                            model_name=model_name,
                            reasoning_effort=reasoning_effort,
                            img_analysis_data=img_analysis_data,
                            use_cache_optimization=True
                        )
                        
                        if request_obj and "body" in request_obj:
                            custom_id = request_obj.get("custom_id", f"row_{idx}")
                            
                            # 중복 custom_id 체크
                            if custom_id in seen_custom_ids:
                                duplicate_count += 1
                                continue
                            seen_custom_ids.add(custom_id)
                            
                            # prompt_cache_key: 키 고정 전략
                            request_obj["body"]["prompt_cache_key"] = "bg_prompt_v1"
                            
                            # prompt_cache_retention 설정
                            if model_name in ["gpt-5.1", "gpt-5.1-codex", "gpt-5.1-codex-mini", "gpt-5.1-chat-latest", "gpt-5", "gpt-5-codex", "gpt-4.1"]:
                                request_obj["body"]["prompt_cache_retention"] = "extended"
                            elif model_name not in ["gpt-5-mini", "gpt-5-nano"]:
                                request_obj["body"]["prompt_cache_retention"] = "in_memory"
                            
                            # text.format: JSON 출력 강제
                            request_obj["body"]["text"] = {"format": {"type": "json_object"}}
                    else:
                        # 일반 모드: 기존 방식 유지
                        messages = build_bg_prompt_messages(st2_for_model, img_analysis_data)
                        
                        body = {
                            "model": model_name,
                            "messages": messages,
                        }
                        
                        is_reasoning = any(x in model_name for x in ["gpt-5", "o1", "o3"])
                        if is_reasoning and reasoning_effort != "none":
                            body["reasoning_effort"] = reasoning_effort

                        request_obj = {
                            "custom_id": f"row_{idx}",
                            "method": "POST",
                            "url": "/v1/chat/completions",
                            "body": body
                        }
                    
                    jsonl_lines.append(json.dumps(request_obj, ensure_ascii=False))
                except Exception as e:
                    self.append_log(f"[Row {idx+1}] 스킵: {e}")
                    skipped_cnt += 1
                    continue
            
            if duplicate_count > 0:
                self.append_log(f"[WARN] ⚠️ 중복 요청 {duplicate_count}개가 감지되어 제외되었습니다.")
            
            if not jsonl_lines:
                self.append_log("생성할 요청 없음.")
                messagebox.showinfo("알림", "생성할 요청이 없습니다.")
                return

            # JSONL 파일 저장
            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_bg_prompt_batch_input.jsonl"
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.write("\n".join(jsonl_lines))
            
            self.append_log(f"✅ JSONL 생성 완료: {len(jsonl_lines)}건 (스킵 {skipped_cnt}건)")
            
            # 파일 크기 확인
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 요청 수: {len(jsonl_lines)}개")
            
            # JSONL 파일 경로를 변수에 저장
            self.after(0, lambda: self.jsonl_file_var.set(jsonl_path))
            
            # 성공 메시지
            self.after(0, lambda: messagebox.showinfo(
                "JSONL 생성 완료",
                f"JSONL 파일이 생성되었습니다.\n\n"
                f"파일: {os.path.basename(jsonl_path)}\n"
                f"요청 수: {len(jsonl_lines)}건\n"
                f"크기: {jsonl_size_mb:.2f} MB\n\n"
                f"이제 '배치 업로드' 버튼을 눌러 업로드하세요."
            ))
            
        except Exception as e:
            self.append_log(f"❌ JSONL 생성 실패: {e}")
            import traceback
            self.append_log(traceback.format_exc())
            self.after(0, lambda: messagebox.showerror("오류", f"JSONL 생성 중 오류가 발생했습니다:\n{e}"))
    
    def _run_upload_batch(self, jsonl_path):
        """JSONL 파일로부터 배치 업로드만 수행하는 로직"""
        key = self.api_key_var.get().strip()
        src = self.src_file_var.get().strip()
        model_name = self.model_var.get().strip() or "gpt-5-mini"
        reasoning_effort = self.effort_var.get().strip() or "low"
        
        try:
            if not os.path.exists(jsonl_path):
                raise FileNotFoundError(f"JSONL 파일을 찾을 수 없습니다: {jsonl_path}")
            
            self.append_log(f"JSONL 파일 로드 중... {os.path.basename(jsonl_path)}")
            
            # 파일 크기 확인
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB")
            
            MAX_FILE_SIZE_MB = 180
            
            # 타임아웃 설정
            import httpx
            timeout = httpx.Timeout(600.0, connect=60.0)  # 10분 타임아웃
            client = OpenAI(api_key=key, timeout=timeout, max_retries=3)
            
            if jsonl_size_mb > MAX_FILE_SIZE_MB:
                self.append_log(f"[INFO] 파일 크기가 제한을 초과하여 청크로 분할합니다: {jsonl_size_mb:.2f}MB > {MAX_FILE_SIZE_MB}MB")
                batch_ids = self._create_batch_chunks(
                    client=client,
                    jsonl_path=jsonl_path,
                    excel_path=src if src else "",
                    model_name=model_name,
                    effort=reasoning_effort,
                    max_size_mb=MAX_FILE_SIZE_MB,
                    max_requests=999999,
                )
                self.append_log(f"✅ 총 {len(batch_ids)}개의 배치가 생성되었습니다: {', '.join(batch_ids)}")
                self.after(0, lambda: messagebox.showinfo("성공", f"{len(batch_ids)}개의 배치가 생성되었습니다:\n{', '.join(batch_ids)}"))
            else:
                # 단일 배치 생성
                batch = self._create_batch_from_jsonl(
                    client=client,
                    jsonl_path=jsonl_path,
                    excel_path=src if src else "",
                    model_name=model_name,
                    reasoning_effort=reasoning_effort,
                )

                batch_id = batch.id
                self.append_log(f"✅ 배치 시작! ID: {batch_id}, status={batch.status}")

                # 작업 이력 기록
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=src if src else "",
                    jsonl_path=jsonl_path,
                    model=model_name,
                    effort=reasoning_effort,
                    status=batch.status,
                    output_file_id=None,
                )

                # 메인 런처 현황판 업데이트
                if src:
                    try:
                        root_name = get_root_filename(src)
                        JobManager.update_status(root_name, img_s3_2_msg="I3-2 (진행중)")
                        self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> I3-2 (진행중)")
                    except Exception:
                        pass
                
                self.after(0, lambda bid=batch_id: messagebox.showinfo("성공", f"배치 시작됨: {bid}"))
            
            self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])

        except Exception as e:
            self.append_log(f"❌ 배치 업로드 실패: {e}")
            import traceback
            self.append_log(traceback.format_exc())
            self.after(0, lambda err=str(e): messagebox.showerror("오류", f"배치 업로드 중 오류가 발생했습니다:\n{err}"))

    def _run_create_batch(self):
        key = self.api_key_var.get().strip()
        src = self.src_file_var.get().strip()
        
        model_name = self.model_var.get().strip() or "gpt-5-mini"
        reasoning_effort = self.effort_var.get().strip() or "low"
        
        try:
            client = OpenAI(api_key=key)
            self.append_log(f"엑셀 로드 중... {os.path.basename(src)}")
            df = pd.read_excel(src)
            
            if "ST2_JSON" not in df.columns:
                raise ValueError("필수 컬럼(ST2_JSON)이 누락되었습니다. Stage 2를 먼저 완료하세요.")
            
            # 캐싱 모드 확인 및 로그
            if CACHE_MODE_CORE:
                self.append_log(f"[INFO] 🚀 프롬프트 캐싱 최적화 모드 활성화 (bg_prompt_core_Casche.py)")
            else:
                self.append_log(f"[INFO] ⚠️ 일반 모드 (bg_prompt_core.py) - 캐싱 최적화 미적용")
            
            self.append_log(f"설정: 모델={model_name}, effort={reasoning_effort}")

            # 먼저 전체 대상 요청 수를 계산 (버킷 수 결정용)
            target_rows = 0
            result_cols = ["bg_positive_en", "bg_negative_en", "video_motion_prompt_en", "video_full_prompt_en"]
            
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get():
                    has_result = False
                    for col in result_cols:
                        val = str(row.get(col, "")).strip()
                        if val and val != "nan":
                            has_result = True
                            break
                    if has_result:
                        continue
                
                # ST2_JSON 확인
                st2_json_raw = str(row.get("ST2_JSON", "")).strip()
                if not st2_json_raw or st2_json_raw == "nan":
                    continue
                
                # view_point 필수 체크
                view_point_val = str(row.get("view_point", "")).strip()
                if not view_point_val or view_point_val == "nan":
                    continue
                
                target_rows += 1

            # 버킷 수를 미리 계산 (모든 요청에 동일하게 적용)
            if CACHE_MODE_CORE and target_rows > 0:
                # [버킷 수 계산 전략 - 주의: OpenAI 공식 기준이 아닌 추정치입니다]
                # [프롬프트 캐싱 최적화 전략 - 키 고정]
                # 실제 테스트 결과: 버킷 분산 시 캐시 히트율이 낮아짐 (10% 수준)
                # 해결책: prompt_cache_key를 하나로 고정하여 모든 요청이 같은 캐시 풀 공유
                # Batch API는 24시간에 걸쳐 처리되므로 overflow 우려는 낮음
                PROMPT_CACHE_BUCKETS = 1
                
                self.append_log(f"[INFO] 프롬프트 캐싱: 키 고정 전략 사용 (모든 요청이 'bg_prompt_v1' 키 공유)")
                self.append_log(f"[INFO] 예상 요청 수: {target_rows}개, 캐시 히트율 향상 예상")
            else:
                PROMPT_CACHE_BUCKETS = 1

            jsonl_lines = []
            skipped_cnt = 0
            seen_custom_ids = set()
            duplicate_count = 0
            
            for idx, row in df.iterrows():
                # 스킵 로직
                if self.skip_exist_var.get():
                    has_result = False
                    for col in result_cols:
                        val = str(row.get(col, "")).strip()
                        if val and val != "nan":
                            has_result = True
                            break
                    if has_result:
                        skipped_cnt += 1
                        continue
                
                # ST2_JSON 확인
                st2_json_raw = str(row.get("ST2_JSON", "")).strip()
                if not st2_json_raw or st2_json_raw == "nan":
                    skipped_cnt += 1
                    continue
                
                # view_point 필수 체크
                view_point_val = str(row.get("view_point", "")).strip()
                if not view_point_val or view_point_val == "nan":
                    self.append_log(f"[Row {idx+1}] view_point가 비어있습니다. 썸네일 구도 분석이 필요합니다. 건너뜁니다.")
                    skipped_cnt += 1
                    continue
                
                try:
                    # ST2_JSON 파싱 시도
                    try:
                        st2_parsed = json.loads(st2_json_raw)
                        st2_for_model = json.dumps(st2_parsed, ensure_ascii=False, indent=2)
                    except json.JSONDecodeError:
                        st2_for_model = st2_json_raw
                    
                    # IMG_ANALYSIS_JSON 구성 (이미지 분석 컬럼에서 읽기)
                    img_analysis_data = None
                    img_analysis_cols = [
                        "view_point", "subject_position", "subject_size", "lighting_condition",
                        "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
                        "bg_layout_hint_en"
                    ]
                    if any(col in df.columns for col in img_analysis_cols):
                        img_analysis_data = {}
                        for col in img_analysis_cols:
                            if col in df.columns:
                                val = str(row.get(col, "")).strip()
                                if val and val != "nan":
                                    # is_flat_lay는 boolean으로 변환 시도
                                    if col == "is_flat_lay":
                                        img_analysis_data[col] = val.lower() in ["true", "1", "yes", "y"]
                                    else:
                                        img_analysis_data[col] = val
                    
                    # 캐싱 최적화 모드: build_bg_prompt_batch_payload 사용
                    if CACHE_MODE_CORE:
                        request_obj = build_bg_prompt_batch_payload(
                            row_index=idx,
                            st2_json_raw=st2_for_model,
                            model_name=model_name,
                            reasoning_effort=reasoning_effort,
                            img_analysis_data=img_analysis_data,
                            use_cache_optimization=True
                        )
                        
                        if request_obj and "body" in request_obj:
                            custom_id = request_obj.get("custom_id", f"row_{idx}")
                            
                            # 중복 custom_id 체크
                            if custom_id in seen_custom_ids:
                                duplicate_count += 1
                                continue
                            seen_custom_ids.add(custom_id)
                            
                            # prompt_cache_key: 키 고정 전략 (모든 요청이 동일한 키 사용)
                            request_obj["body"]["prompt_cache_key"] = "bg_prompt_v1"
                            
                            # prompt_cache_retention: 모델이 지원하는 경우에만 추가
                            # Extended retention 지원 모델: gpt-5.1, gpt-5.1-codex, gpt-5.1-codex-mini, gpt-5.1-chat-latest, gpt-5, gpt-5-codex, gpt-4.1
                            # gpt-5-mini, gpt-5-nano는 prompt_cache_retention 파라미터를 지원하지 않음
                            if model_name in ["gpt-5.1", "gpt-5.1-codex", "gpt-5.1-codex-mini", "gpt-5.1-chat-latest", "gpt-5", "gpt-5-codex", "gpt-4.1"]:
                                request_obj["body"]["prompt_cache_retention"] = "extended"  # 24시간 retention
                            elif model_name not in ["gpt-5-mini", "gpt-5-nano"]:
                                # 기타 모델은 in-memory 사용 (5~10분 inactivity, 최대 1시간)
                                request_obj["body"]["prompt_cache_retention"] = "in_memory"
                            
                            # text.format: JSON 출력 강제 (Structured Outputs)
                            request_obj["body"]["text"] = {"format": {"type": "json_object"}}
                    else:
                        # 일반 모드: 기존 방식 유지
                        messages = build_bg_prompt_messages(st2_for_model, img_analysis_data)
                        
                        body = {
                            "model": model_name,
                            "messages": messages,
                        }
                        
                        is_reasoning = any(x in model_name for x in ["gpt-5", "o1", "o3"])
                        if is_reasoning and reasoning_effort != "none":
                            body["reasoning_effort"] = reasoning_effort

                        request_obj = {
                            "custom_id": f"row_{idx}",
                            "method": "POST",
                            "url": "/v1/chat/completions",
                            "body": body
                        }
                    
                    jsonl_lines.append(json.dumps(request_obj, ensure_ascii=False))
                except Exception as e:
                    self.append_log(f"[Row {idx+1}] 스킵: {e}")
                    skipped_cnt += 1
                    continue
            
            if duplicate_count > 0:
                self.append_log(f"[WARN] ⚠️ 중복 요청 {duplicate_count}개가 감지되어 제외되었습니다.")
            
            if not jsonl_lines:
                self.append_log("생성할 요청 없음.")
                return

            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_bg_prompt_batch_input.jsonl"
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.write("\n".join(jsonl_lines))
            
            self.append_log(f"JSONL 생성 완료: {len(jsonl_lines)}건 (스킵 {skipped_cnt}건)")
            
            # JSONL 파일 경로를 변수에 저장 (통합 기능에서도)
            self.after(0, lambda: self.jsonl_file_var.set(jsonl_path))
            
            # 파일 크기 및 요청 수 확인
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            info = {
                'num_requests': len(jsonl_lines),
                'file_size_mb': jsonl_size_mb
            }
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 요청 수: {info['num_requests']}개")
            
            # 용량 기준 우선: 180MB 이상이면 분할 처리 (OpenAI Batch API 제한: 200MB)
            # 요청 수는 용량 제한 내에서 가능한 만큼 포함 (500개 제한 제거)
            MAX_FILE_SIZE_MB = 180
            
            if jsonl_size_mb > MAX_FILE_SIZE_MB:
                self.append_log(f"[INFO] 파일 크기가 제한을 초과하여 청크로 분할합니다: {jsonl_size_mb:.2f}MB > {MAX_FILE_SIZE_MB}MB")
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
                    max_size_mb=MAX_FILE_SIZE_MB,
                    max_requests=999999,  # 요청 수 제한 거의 제거 (용량이 우선)
                )
                self.append_log(f"✅ 총 {len(batch_ids)}개의 배치가 생성되었습니다: {', '.join(batch_ids)}")
                self.after(0, lambda: messagebox.showinfo("성공", f"{len(batch_ids)}개의 배치가 생성되었습니다:\n{', '.join(batch_ids)}"))
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
                )

                batch_id = batch.id
                self.append_log(f"✅ 배치 시작! ID: {batch_id}, status={batch.status}")

                # 작업 이력 기록
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=src,
                    jsonl_path=jsonl_path,
                    model=model_name,
                    effort=reasoning_effort,
                    status=batch.status,
                    output_file_id=None,
                )

                # 메인 런처 현황판에 I3-2 작업 시작 상태 기록 - img 상태만 업데이트 (text 상태는 변경하지 않음)
                try:
                    root_name = get_root_filename(src)
                    JobManager.update_status(root_name, img_s3_2_msg="I3-2 (진행중)")
                    self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> I3-2 (진행중)")
                except Exception:
                    pass
                self.after(0, lambda bid=batch_id: messagebox.showinfo("성공", f"배치 시작됨: {bid}"))
            
            self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])

        except Exception as e:
            self.append_log(f"에러: {e}")
            import traceback
            self.append_log(traceback.format_exc())
            self.after(0, lambda err=str(e): messagebox.showerror("에러", err))
    
    def _create_batch_from_jsonl(self, client, jsonl_path, excel_path, model_name, reasoning_effort):
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
    
    def _create_batch_chunks(self, client, jsonl_path, excel_path, model_name, effort, max_size_mb=180, max_requests=999999):
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
                
                # 용량이 우선: 다음 요청을 추가하면 제한을 초과하는지 확인 (안전 마진 5% 포함)
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
        
        # 메인 런처 현황판 업데이트
        try:
            root_name = get_root_filename(excel_path)
            JobManager.update_status(root_name, img_s3_2_msg="I3-2 (진행중)")
            self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> I3-2 (진행중)")
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
        ttk.Button(f_ctrl, text="🗑 휴지통 이동", command=self._archive_selected, style="Danger.TButton").pack(side='right', padx=2)
        
        # 컬럼 정의: batch_id | excel_name | memo | status | created | completed | model | effort | counts | group
        cols = ("batch_id", "excel_name", "memo", "status", "created", "completed", "model", "effort", "counts", "group")
        # 계층 구조를 위해 show='tree headings' 사용 (트리 아이콘 + 컬럼 헤더)
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='tree headings', height=12, selectmode='extended')
        
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF')
        self.tree_active.tag_configure('group', background='#E8F4FD')
        self.tree_active.tag_configure('group_header', background='#C8E6C9', font=("맑은 고딕", 9, "bold"))
        
        # 컬럼 헤더 한글화
        self.tree_active.heading("batch_id", text="배치 ID")
        self.tree_active.heading("excel_name", text="엑셀명")
        self.tree_active.heading("memo", text="메모")
        self.tree_active.heading("status", text="상태")
        self.tree_active.heading("created", text="생성일시")
        self.tree_active.heading("completed", text="완료일시")
        self.tree_active.heading("model", text="모델")
        self.tree_active.heading("effort", text="Effort")
        self.tree_active.heading("counts", text="완료/전체")
        self.tree_active.heading("group", text="그룹")
        
        # 컬럼 너비 설정
        self.tree_active.column("batch_id", width=180, anchor="w")
        self.tree_active.column("excel_name", width=200, anchor="w")
        self.tree_active.column("memo", width=150, anchor="w")
        self.tree_active.column("status", width=80, anchor="center")
        self.tree_active.column("created", width=120, anchor="center")
        self.tree_active.column("completed", width=120, anchor="center")
        self.tree_active.column("model", width=80, anchor="center")
        self.tree_active.column("effort", width=60, anchor="center")
        self.tree_active.column("counts", width=80, anchor="center")
        self.tree_active.column("group", width=80, anchor="center")
        
        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)
        
        # 그룹 접기/펼치기 버튼
        f_group_ctrl = ttk.Frame(self.sub_active)
        f_group_ctrl.pack(fill='x', padx=5, pady=(0, 5))
        ttk.Button(f_group_ctrl, text="📂 모든 그룹 펼치기", command=lambda: self._expand_all_groups(self.tree_active)).pack(side='left', padx=2)
        ttk.Button(f_group_ctrl, text="📁 모든 그룹 접기", command=lambda: self._collapse_all_groups(self.tree_active)).pack(side='left', padx=2)
        
        # 우클릭 메뉴
        self.menu_active = Menu(self, tearoff=0)
        self.menu_active.add_command(label="상태 갱신", command=lambda: self._refresh_selected(self.tree_active))
        self.menu_active.add_separator()
        self.menu_active.add_command(label="결과 병합", command=self._merge_selected)
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
        
        # 계층 구조를 위해 show='tree headings' 사용
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='tree headings', height=12, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2')
        self.tree_arch.tag_configure('group_header', background='#C8E6C9', font=("맑은 고딕", 9, "bold"))
        
        # 컬럼 헤더 한글화
        self.tree_arch.heading("batch_id", text="배치 ID")
        self.tree_arch.heading("excel_name", text="엑셀명")
        self.tree_arch.heading("memo", text="메모")
        self.tree_arch.heading("status", text="상태")
        self.tree_arch.heading("created", text="생성일시")
        self.tree_arch.heading("completed", text="완료일시")
        self.tree_arch.heading("model", text="모델")
        self.tree_arch.heading("effort", text="Effort")
        self.tree_arch.heading("counts", text="완료/전체")
        self.tree_arch.heading("group", text="그룹")
        
        # 컬럼 너비 설정
        self.tree_arch.column("batch_id", width=180, anchor="w")
        self.tree_arch.column("excel_name", width=200, anchor="w")
        self.tree_arch.column("memo", width=150, anchor="w")
        self.tree_arch.column("status", width=80, anchor="center")
        self.tree_arch.column("created", width=120, anchor="center")
        self.tree_arch.column("completed", width=120, anchor="center")
        self.tree_arch.column("model", width=80, anchor="center")
        self.tree_arch.column("effort", width=60, anchor="center")
        self.tree_arch.column("counts", width=80, anchor="center")
        self.tree_arch.column("group", width=80, anchor="center")
        
        self.tree_arch.pack(fill='both', expand=True)
        
        # Archive 우클릭 메뉴
        self.menu_arch = Menu(self, tearoff=0)
        self.menu_arch.add_command(label="복구", command=self._restore_selected)
        self.menu_arch.add_separator()
        self.menu_arch.add_command(label="메모 편집", command=lambda: self._edit_memo(self.tree_arch))
        self.menu_arch.add_separator()
        self.menu_arch.add_command(label="영구 삭제", command=self._hard_delete_selected)
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
            messagebox.showinfo("안내", "배치를 선택해주세요.")
            return
        
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
                values=("", excel_name, memo, "", date_str, "", first_job.get("model", "-"), first_job.get("effort", "-"), "", f"그룹 {total_chunks}개"),
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
                        j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, 
                        j.get("model"), j.get("effort", "-"), cnt, group_display
                    ), 
                    tags=(tag,))
                idx += 1
        
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
                    j["batch_id"], excel_name, memo, j.get("status"), 
                    c_at, f_at, 
                    j.get("model"), j.get("effort", "-"), cnt, "-"
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
            first_job = group_jobs[0] if group_jobs else None
            if first_job:
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
                    values=("", excel_name, memo, "", date_str, "", first_job.get("model", "-"), first_job.get("effort", "-"), "", f"그룹 {total_chunks}개"),
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
                            j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, 
                            j.get("model"), j.get("effort", "-"), cnt, group_display
                        ), 
                        tags=(tag,))
                    idx += 1
        
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
                    j["batch_id"], excel_name, memo, j.get("status"), 
                    c_at, f_at, 
                    j.get("model"), j.get("effort", "-"), cnt, "-"
                ), 
                tags=(tag,))
            idx += 1

    # --- Batch Actions ---
    def _refresh_selected(self, tree):
        ids = self._get_selected_ids(tree)
        if not ids: return
        
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key가 필요합니다.")
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
            self.append_log("❌ API Key가 없습니다.")
            return
        
        try:
            client = OpenAI(api_key=key)
        except Exception as e:
            self.append_log(f"❌ OpenAI 클라이언트 생성 실패: {e}")
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
                # output_file_id 추출: 여러 경로 시도
                output_file_id = getattr(remote, "output_file_id", None)
                if not output_file_id:
                    # output_file 객체가 있는 경우
                    output_file = getattr(remote, "output_file", None)
                    if output_file:
                        if isinstance(output_file, str):
                            output_file_id = output_file
                        else:
                            output_file_id = getattr(output_file, "id", None) or getattr(output_file, "file_id", None)
                
                # model_dump()를 통한 추가 확인 (갱신 시에도 적용)
                if not output_file_id and remote.status == "completed":
                    try:
                        if hasattr(remote, "model_dump"):
                            dump = remote.model_dump()
                            if "output_file_id" in dump and dump["output_file_id"]:
                                output_file_id = dump["output_file_id"]
                            elif "output_file" in dump:
                                of = dump["output_file"]
                                if isinstance(of, str) and of:
                                    output_file_id = of
                                elif isinstance(of, dict) and "id" in of:
                                    output_file_id = of["id"]
                    except Exception:
                        pass
                
                upsert_batch_job(bid, status=remote.status, output_file_id=output_file_id, request_counts=rc)
                
                if remote.status == "expired" and output_file_id:
                    self.append_log(f"ℹ️ {bid}: 만료된 배치이지만 output_file_id가 있습니다. (다운로드 가능)")
                elif remote.status == "completed":
                    if output_file_id:
                        self.append_log(f"✅ {bid}: {remote.status} (output_file_id: {output_file_id})")
                    else:
                        self.append_log(f"⚠️ {bid}: {remote.status} (output_file_id 없음 - 디버깅 필요)")
                else:
                    self.append_log(f"✅ {bid}: {remote.status}")
                success_cnt += 1
            except Exception as e:
                self.append_log(f"❌ {bid} 갱신 실패: {e}")
                fail_cnt += 1
        
        self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])
        if fail_cnt > 0:
            self.append_log(f"갱신 완료 (성공: {success_cnt}, 실패: {fail_cnt})")
            self.after(0, lambda: messagebox.showinfo("갱신 완료", f"갱신 완료 (성공: {success_cnt}건, 실패: {fail_cnt}건)\n\n[2. 배치 관리] 탭에서 '선택 일괄 병합'을 진행하세요."))
        else:
            self.append_log(f"갱신 완료 (성공: {success_cnt}건)")
            self.after(0, lambda: messagebox.showinfo("갱신 완료", f"갱신 완료 (성공: {success_cnt}건)\n\n[2. 배치 관리] 탭에서 '선택 일괄 병합'을 진행하세요."))

    def _merge_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key가 필요합니다.")
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
                # completed 또는 expired 상태인 배치 포함 (expired 상태에서도 output_file_id가 있으면 다운로드 가능)
                group_batches = [j for j in jobs if j.get("batch_group_id") == group_id and j.get("status") in ["completed", "expired"]]
                for j in group_batches:
                    all_target_ids.add(j["batch_id"])
        
        if len(all_target_ids) > len(ids):
            group_info = f"\n\n같은 그룹의 배치 {len(all_target_ids) - len(ids)}개가 자동으로 포함됩니다."
        else:
            group_info = ""
        
        # completed, expired 또는 merged 상태인 배치 모두 선택 가능 (expired 상태에서도 output_file_id가 있으면 다운로드 가능)
        targets = [bid for bid in all_target_ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") in ["completed", "expired", "merged"]]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed', 'expired' 또는 'merged' 상태의 작업이 없습니다.")
            return
        
        msg = f"선택한 {len(targets)}건을 처리하시겠습니까?{group_info}"
        if messagebox.askyesno("병합", msg):
            t = threading.Thread(target=self._run_merge_multi, args=(targets,))
            t.daemon = True
            t.start()

    def _run_merge_multi(self, ids):
        key = self.api_key_var.get().strip()
        if not key:
            self.append_log("❌ API Key가 없습니다.")
            return
        
        try:
            client = OpenAI(api_key=key)
        except Exception as e:
            self.append_log(f"❌ OpenAI 클라이언트 생성 실패: {e}")
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
        success_folders = set()  # 성공한 파일들이 저장된 폴더들 추적
        
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
                        # output_file_id 추출: 여러 경로 시도
                        output_file_id = getattr(remote, "output_file_id", None)
                        if not output_file_id:
                            # output_file 객체가 있는 경우
                            output_file = getattr(remote, "output_file", None)
                            if output_file:
                                if isinstance(output_file, str):
                                    output_file_id = output_file
                                else:
                                    output_file_id = getattr(output_file, "id", None) or getattr(output_file, "file_id", None)
                        
                        # model_dump()를 통한 추가 확인
                        if not output_file_id and remote.status == "completed":
                            try:
                                if hasattr(remote, "model_dump"):
                                    dump = remote.model_dump()
                                    if "output_file_id" in dump and dump["output_file_id"]:
                                        output_file_id = dump["output_file_id"]
                                    elif "output_file" in dump:
                                        of = dump["output_file"]
                                        if isinstance(of, str) and of:
                                            output_file_id = of
                                        elif isinstance(of, dict) and "id" in of:
                                            output_file_id = of["id"]
                            except Exception:
                                pass
                        
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
                        
                        # 결과 다운로드
                        base_dir = os.path.dirname(src_path)
                        base_name, _ = os.path.splitext(os.path.basename(src_path))
                        out_jsonl = os.path.join(base_dir, f"{base_name}_bg_prompt_batch_output_{bid}.jsonl")
                        
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
                        
                        # JSONL 파일 읽어서 수집 및 캐싱 통계 수집
                        batch_cached_tok = 0
                        batch_total_requests = 0
                        batch_cache_hits = 0
                        if os.path.exists(out_jsonl):
                            with open(out_jsonl, "r", encoding="utf-8") as f:
                                for line in f:
                                    line = line.strip()
                                    if line:
                                        all_output_lines.append(line)
                                        # 캐싱 통계 수집
                                        try:
                                            data = json.loads(line)
                                            response_body = data.get("response", {}).get("body", {})
                                            usage = response_body.get("usage", {})
                                            input_tokens_details = usage.get("input_tokens_details", {})
                                            cached_tokens = input_tokens_details.get("cached_tokens", 0)
                                            batch_cached_tok += cached_tokens
                                            batch_total_requests += 1
                                            if cached_tokens > 0:
                                                batch_cache_hits += 1
                                        except:
                                            pass
                        
                        # 배치별 캐싱 통계 출력
                        if batch_total_requests > 0:
                            cache_hit_rate = (batch_cache_hits / batch_total_requests * 100)
                            cache_savings_pct = (batch_cached_tok / (batch_cached_tok + (batch_total_requests * 1000))) * 100 if batch_cached_tok > 0 else 0  # 대략적 계산
                            self.append_log(f"  [캐싱] {bid}: 요청 {batch_total_requests}건, 히트 {batch_cache_hits}건 ({cache_hit_rate:.1f}%), 캐시 토큰 {batch_cached_tok:,}")
                        
                    except Exception as e:
                        self.append_log(f"  ❌ {bid} 결과 다운로드 실패: {e}")
                        continue
                
                if not all_output_lines:
                    self.append_log(f"⚠️ 그룹 {group_id}: 병합할 결과가 없습니다.")
                    continue
                
                # 임시 통합 JSONL 파일 생성
                base_dir = os.path.dirname(src_path)
                base_name, _ = os.path.splitext(os.path.basename(src_path))
                merged_jsonl = os.path.join(base_dir, f"{base_name}_bg_prompt_batch_output_merged_{group_id}.jsonl")
                
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
                        data = json.loads(line)
                        cid = data.get("custom_id")
                        
                        # /v1/responses API 형식 처리
                        response_body = data.get("response", {}).get("body", {})
                        usage = response_body.get("usage", {})
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
                        
                        try:
                            # /v1/responses API: output 배열에서 message 타입 찾기
                            output_array = response_body.get("output", [])
                            content_str = None
                            
                            for output_item in output_array:
                                if output_item.get("type") == "message":
                                    content_array = output_item.get("content", [])
                                    for content_item in content_array:
                                        if content_item.get("type") == "output_text":
                                            content_str = content_item.get("text", "").strip()
                                            break
                                    if content_str:
                                        break
                            
                            # Fallback: 기존 choices 형식 (호환성)
                            if not content_str:
                                content_str = data.get("response", {}).get("body", {}).get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                            
                            if content_str:
                                result_data = json.loads(content_str)
                                results_map[cid] = result_data
                            else:
                                results_map[cid] = {}
                        except Exception as e:
                            results_map[cid] = {}
                            self.append_log(f"  [WARN] {cid} 파싱 실패: {e}")
                
                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    result_cols = ["bg_positive_en", "bg_negative_en", "video_motion_prompt_en", "video_full_prompt_en"]
                    for col in result_cols:
                        if col not in df.columns:
                            df[col] = ""
                        df[col] = df[col].astype(str)
                    
                    cnt = 0
                    for cid, result_data in results_map.items():
                        try:
                            idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                for col in result_cols:
                                    if col in result_data:
                                        df.at[idx, col] = str(result_data[col])
                                cnt += 1
                        except:
                            pass

                    # 중간 파일 저장
                    base, ext = os.path.splitext(src_path)
                    out_excel = f"{base}_bg_prompt_batch_done{ext}"
                    if not safe_save_excel(df, out_excel):
                        self.append_log(f"[WARN] 기본 완료 파일 저장 실패: {out_excel}")
                        continue

                    # I4 버전 파일로 저장
                    try:
                        final_out_path = get_i4_output_path(src_path)
                        df_done = pd.read_excel(out_excel)
                        if safe_save_excel(df_done, final_out_path):
                            # 중간 파일 삭제
                            if out_excel != final_out_path and os.path.exists(out_excel):
                                try:
                                    os.remove(out_excel)
                                    self.append_log(f"[INFO] 중간 파일 삭제: {os.path.basename(out_excel)}")
                                except Exception as e:
                                    self.append_log(f"[WARN] 중간 파일 삭제 실패: {e}")
                        else:
                            final_out_path = out_excel
                    except Exception as e:
                        self.append_log(f"[WARN] I4 버전 파일 저장 중 오류: {e}")
                        final_out_path = out_excel
                    
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
                            out_excel=final_out_path,
                            status="merged",
                        )
                    
                    # 메인 런처 현황판 업데이트
                    try:
                        root_name = get_root_filename(src_path)
                        JobManager.update_status(root_name, img_s3_2_msg="I3-2(배경분석완료)")
                        self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I3-2(배경분석완료)")
                    except Exception as e:
                        self.append_log(f"[WARN] 현황판 연동 실패: {e}")
                    
                    self.append_log(f"✅ 그룹 {group_id} 병합 완료 ({cnt}건): {os.path.basename(final_out_path)}")
                    self.append_log(f"  [그룹 캐싱 통계] 요청 {total_group_requests:,}건, 히트 {total_group_cache_hits:,}건 ({group_cache_hit_rate:.1f}%), 캐시 토큰 {total_group_cached:,} ({group_cache_savings_pct:.1f}%)")
                    if group_cache_savings > 0:
                        self.append_log(f"  [그룹 비용절감] 캐싱으로 총 ${group_cache_savings:.4f} 절감")
                    success_cnt += 1
                    success_folders.add(os.path.dirname(final_out_path))
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
                local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                
                if not local_job:
                    self.append_log(f"❌ {bid} 병합 실패: 작업 이력을 찾을 수 없습니다.")
                    continue

                # 이미 병합된 배치는 건너뛰기
                is_already_merged = local_job.get("status") == "merged"
                if is_already_merged:
                    self.append_log(f"⏭️ {bid}: 이미 병합 완료된 작업입니다.")
                    continue

                # Batch 상태 및 결과 파일 ID 조회
                remote = client.batches.retrieve(bid)
                # output_file_id 추출: 여러 경로 시도
                output_file_id = getattr(remote, "output_file_id", None)
                if not output_file_id:
                    # output_file 객체가 있는 경우
                    output_file = getattr(remote, "output_file", None)
                    if output_file:
                        if isinstance(output_file, str):
                            output_file_id = output_file
                        else:
                            output_file_id = getattr(output_file, "id", None) or getattr(output_file, "file_id", None)
                
                # model_dump()를 통한 추가 확인
                if not output_file_id and remote.status == "completed":
                    try:
                        if hasattr(remote, "model_dump"):
                            dump = remote.model_dump()
                            if "output_file_id" in dump and dump["output_file_id"]:
                                output_file_id = dump["output_file_id"]
                            elif "output_file" in dump:
                                of = dump["output_file"]
                                if isinstance(of, str) and of:
                                    output_file_id = of
                                elif isinstance(of, dict) and "id" in of:
                                    output_file_id = of["id"]
                    except Exception:
                        pass
                
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
                    upsert_batch_job(bid, status=remote.status, output_file_id=output_file_id)
                    continue
                
                # 결과 파일 다운로드
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
                
                if local_job and local_job.get("src_excel"):
                    src_path = local_job["src_excel"]
                    base, _ = os.path.splitext(src_path)
                    out_jsonl = f"{base}_bg_prompt_batch_output.jsonl"
                else:
                    out_jsonl = f"output_{bid}.jsonl"
                    src_path = None

                with open(out_jsonl, "wb") as f:
                    f.write(content)
                
                # JSONL 파싱 및 엑셀 병합
                results_map = {}
                batch_in_tok = 0
                batch_out_tok = 0
                batch_cached_tok = 0
                batch_total_requests = 0
                batch_cache_hits = 0
                
                with open(out_jsonl, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip(): continue
                        data = json.loads(line)
                        cid = data.get("custom_id")
                        
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
                        
                        try:
                            # /v1/responses API: output 배열에서 message 타입 찾기
                            output_array = response_body.get("output", [])
                            content_str = None
                            
                            for output_item in output_array:
                                if output_item.get("type") == "message":
                                    content_array = output_item.get("content", [])
                                    for content_item in content_array:
                                        if content_item.get("type") == "output_text":
                                            content_str = content_item.get("text", "").strip()
                                            break
                                    if content_str:
                                        break
                            
                            # Fallback: 기존 choices 형식 (호환성)
                            if not content_str:
                                content_str = data.get("response", {}).get("body", {}).get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                            
                            if content_str:
                                result_data = json.loads(content_str)
                                results_map[cid] = result_data
                            else:
                                results_map[cid] = {}
                        except Exception as e:
                            results_map[cid] = {}
                            self.append_log(f"  [WARN] {cid} 파싱 실패: {e}")

                # 캐싱 통계 출력
                cache_hit_rate = (batch_cache_hits / batch_total_requests * 100) if batch_total_requests > 0 else 0
                cache_savings_pct = (batch_cached_tok / batch_in_tok * 100) if batch_in_tok > 0 else 0
                self.append_log(f"  [캐싱] {bid}: 요청 {batch_total_requests}건, 히트 {batch_cache_hits}건 ({cache_hit_rate:.1f}%), 캐시 토큰 {batch_cached_tok:,} ({cache_savings_pct:.1f}%)")
                
                # 비용 계산 (50% 할인)
                model_name = local_job.get("model", "gpt-5-mini") if local_job else "gpt-5-mini"
                pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                cost_in = (batch_in_tok / 1_000_000) * pricing["input"] * 0.5
                cost_out = (batch_out_tok / 1_000_000) * pricing["output"] * 0.5
                cost_total = cost_in + cost_out
                total_cost += cost_total
                
                # 캐시로 절감된 비용 계산 (캐시된 토큰은 비용이 0)
                cache_savings = (batch_cached_tok / 1_000_000) * pricing["input"] * 0.5
                if cache_savings > 0:
                    self.append_log(f"  [비용절감] {bid}: 캐싱으로 ${cache_savings:.4f} 절감")

                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    result_cols = ["bg_positive_en", "bg_negative_en", "video_motion_prompt_en", "video_full_prompt_en"]
                    for col in result_cols:
                        if col not in df.columns:
                            df[col] = ""
                        df[col] = df[col].astype(str)
                    
                    cnt = 0
                    for cid, result_data in results_map.items():
                        try:
                            idx = int(cid.split("_")[1])
                            if 0 <= idx < len(df):
                                for col in result_cols:
                                    if col in result_data:
                                        df.at[idx, col] = str(result_data[col])
                                cnt += 1
                        except:
                            pass

                    # 중간 파일 저장
                    base, ext = os.path.splitext(src_path)
                    out_excel = f"{base}_bg_prompt_batch_done{ext}"
                    if not safe_save_excel(df, out_excel):
                        self.append_log(f"[WARN] 기본 완료 파일 저장 실패: {out_excel}")
                        continue

                    # I4 버전 파일로 저장
                    try:
                        final_out_path = get_i4_output_path(src_path)
                        df_done = pd.read_excel(out_excel)
                        if safe_save_excel(df_done, final_out_path):
                            # 중간 파일 삭제
                            if out_excel != final_out_path and os.path.exists(out_excel):
                                try:
                                    os.remove(out_excel)
                                    self.append_log(f"[INFO] 중간 파일 삭제: {os.path.basename(out_excel)}")
                                except Exception as e:
                                    self.append_log(f"[WARN] 중간 파일 삭제 실패: {e}")
                        else:
                            final_out_path = out_excel
                    except Exception as e:
                        self.append_log(f"[WARN] I4 버전 파일 저장 중 오류: {e}")
                        final_out_path = out_excel

                    upsert_batch_job(bid, out_excel=final_out_path, status="merged")

                    # 메인 런처 현황판에 I3-2 완료 상태 기록 - img 상태만 I3-2(배경분석완료)로 업데이트 (text 상태는 변경하지 않음)
                    try:
                        root_name = get_root_filename(src_path)
                        JobManager.update_status(root_name, img_s3_2_msg="I3-2(배경분석완료)")
                        self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I3-2(배경분석완료)")
                    except Exception as e:
                        self.append_log(f"[WARN] 현황판 연동 실패: {e}")

                    self.append_log(f"✅ 병합 완료 ({cnt}건): {os.path.basename(final_out_path)}")
                    success_cnt += 1
                    # 성공한 파일의 폴더 경로 저장
                    success_folders.add(os.path.dirname(final_out_path))
                else:
                    self.append_log(f"⚠️ 원본 없음. JSONL만 저장.")
            except Exception as e:
                self.append_log(f"❌ {bid} 병합 실패: {e}")
        
        self.append_log(f"=== 일괄 병합 끝 (성공: {success_cnt}, 비용: ${total_cost:.4f}) ===")
        self._load_jobs_all()
        
        # 성공한 파일이 있으면 폴더 열기 여부 확인
        if success_folders:
            folder_path = list(success_folders)[0]  # 첫 번째 성공한 폴더
            if messagebox.askyesno("완료", f"{success_cnt}건 병합 완료.\n총 비용: ${total_cost:.4f}\n\n결과 파일이 저장된 폴더를 열까요?"):
                try:
                    if platform.system() == "Windows":
                        os.startfile(folder_path)
                    elif platform.system() == "Darwin":  # macOS
                        subprocess.run(["open", folder_path])
                    else:  # Linux
                        subprocess.run(["xdg-open", folder_path])
                    self.append_log(f"📂 결과 폴더 열기: {folder_path}")
                except Exception as e:
                    self.append_log(f"[WARN] 폴더 열기 실패: {e}")
                    messagebox.showerror("오류", f"폴더를 열 수 없습니다:\n{e}")
        else:
            messagebox.showinfo("완료", f"{success_cnt}건 병합 완료.\n총 비용: ${total_cost:.4f}")

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

    def _start_merge(self):
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("오류", "API Key가 필요합니다.")
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
            model_name = failed_info.get("model_name", DEFAULT_MODEL)
            effort = failed_info.get("effort", "low")
            batch_group_id = failed_info.get("batch_group_id", "")
            
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
    app = BGPromptBatchGUI()
    app.mainloop()

