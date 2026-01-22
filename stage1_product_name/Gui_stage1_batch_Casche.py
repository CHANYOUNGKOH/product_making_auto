"""
Gui_stage1_batch_Casche.py

Stage 1 Batch API 실행 스크립트 (GUI) - 캐싱 최적화 버전
- 기능: 엑셀 원본 -> Batch JSONL 생성(상품명 정제) -> 업로드 -> 실행 -> 병합 -> 정제 리포트
- [Fix] 런처 실행 시 모듈 경로(ModuleNotFoundError) 문제 완벽 해결
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
  * 동적 콘텐츠(입력 데이터)를 user 프롬프트에 배치
  * prompt_cache_key 사용으로 캐시 히트율 향상 (토큰 비용 최대 90% 절감 가능)
"""

import os
import sys
import json
import math
import threading
import subprocess
import re
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

# ========================================================
# [NEW] 메인 런처 연동용 JobManager & 파일명 유틸
# ========================================================
def get_root_filename(filename):
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T2_I1.xlsx -> 아디다스.xlsx
    예: 나이키_T1_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T1_I0_T2_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 나이키_T1_I5(업완).xlsx -> 나이키.xlsx
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
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")

    return base + ext


def get_next_version_path(current_path, task_type: str = "text"):
    """
    현재 파일명을 분석해서 다음 단계의 파일명을 생성합니다.
    파일명 형식: 원본명_T{숫자}_I{숫자}.xlsx 또는 원본명_T{숫자}_I{숫자}(괄호).xlsx
    - task_type='text'  → T 버전 +1 (T1 의미: 텍스트 1단계 완료)
    - task_type='image' → I 버전 +1
    
    주의: 파일명에 여러 버전 패턴이 있어도 마지막 패턴만 사용합니다.
    예: 나이키_T1_I0_T2_I1.xlsx -> 나이키_T2_I2.xlsx (text) 또는 나이키_T2_I2.xlsx (image)
    예: 나이키_T1_I5(업완).xlsx -> 나이키_T2_I5(업완).xlsx (text) 또는 나이키_T1_I6(업완).xlsx (image)
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
        """메인 런처 현황판 상태 업데이트"""
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


def safe_save_excel(df, path: str) -> bool:
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


# core 모듈의 응답 파서 재사용 (Batch JSONL 형식 통일)
# 캐싱 최적화 버전 사용 (batch_stage1_core_Casche.py)
try:
    from batch_stage1_core_Casche import (
        extract_text_from_response_dict,
        extract_usage_from_response_dict,
        STAGE1_SYSTEM_PROMPT,
        STAGE1_USER_PROMPT_TEMPLATE,
        fmt_safe,
    )
    CACHE_MODE_CORE = True
except ImportError:
    # 캐싱 버전이 없으면 일반 버전 사용
    try:
        from batch_stage1_core import (
            extract_text_from_response_dict,
            extract_usage_from_response_dict,
        )
        CACHE_MODE_CORE = False
        STAGE1_SYSTEM_PROMPT = ""
        STAGE1_USER_PROMPT_TEMPLATE = ""
        def fmt_safe(x): return str(x) if x is not None else ""
    except ImportError:
        # 구버전/누락 환경에서는 조용히 패스하지만,
        # 이 경우 Batch 병합 시 토큰/텍스트 파싱이 동작하지 않을 수 있음.
        def extract_text_from_response_dict(resp):  # type: ignore[override]
            body = resp.get("body") if isinstance(resp, dict) and "body" in resp else resp
            # 가장 단순한 fallback: output_text 또는 문자열 변환
            if isinstance(body, dict) and isinstance(body.get("output_text"), str):
                return body["output_text"].strip()
            return ""

        def extract_usage_from_response_dict(resp):  # type: ignore[override]
            return 0, 0, 0
        
        CACHE_MODE_CORE = False
        STAGE1_SYSTEM_PROMPT = ""
        STAGE1_USER_PROMPT_TEMPLATE = ""
        def fmt_safe(x): return str(x) if x is not None else ""

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

# 여기서 GUI 전용 batch payload 빌더를 구현 (캐싱 최적화 버전)
def build_stage1_batch_payload(idx, row, model, effort):
    """
    한 행(row)을 Batch API용 요청 한 줄(JSONL)로 만드는 함수.
    - core의 create_batch_input_jsonl 과 동일한 필드(카테고리명, 판매형태, 원본상품명)를 사용
    - 프롬프트 캐싱 최적화: system/user 분리
    - Batch endpoint: /v1/responses
    """
    # 필수 필드 안전하게 문자열로 변환
    raw_name = safe_str(row.get("원본상품명", ""))
    category = safe_str(row.get("카테고리명", ""))
    sale_type = safe_str(row.get("판매형태", ""))

    # 필수값이 하나라도 비어 있으면 이 행은 스킵
    if not raw_name or not category or not sale_type:
        return None

    if CACHE_MODE_CORE:
        # 캐싱 최적화 모드: system/user 분리
        system_content = [{"type": "input_text", "text": STAGE1_SYSTEM_PROMPT}]
        user_prompt = STAGE1_USER_PROMPT_TEMPLATE.format(
            category=fmt_safe(category),
            sale_type=fmt_safe(sale_type),
            raw_name=fmt_safe(raw_name)
        )
        user_content = [{"type": "input_text", "text": user_prompt}]

        body = {
            "model": model,
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
            "reasoning": {"effort": effort or "low"},
        }
    else:
        # 일반 모드: 기존 방식 유지
        prompt_text = build_stage1_prompt(category, sale_type, raw_name)
        body = {
            "model": model,
            "input": [
                {"role": "user", "content": prompt_text}
            ],
            "reasoning": {"effort": effort or "low"},
        }

    # Batch JSONL 한 줄 구조 (Responses 엔드포인트)
    return {
        "custom_id": f"row-{idx}",
        "method": "POST",
        "url": "/v1/responses",
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
        self.title("Stage 1: Batch API Manager (Path Fixed) 🚀 캐싱 최적화 버전")
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
        self.failed_chunks_file_var = tk.StringVar()
        
        # 그룹 접기/펼치기 상태 추적
        self.collapsed_groups = set()  # 접힌 그룹 ID 집합
        
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
            
            # 캐싱 모드 확인 및 로그
            if CACHE_MODE_CORE:
                self.append_log(f"[INFO] 🚀 프롬프트 캐싱 최적화 모드 활성화 (batch_stage1_core_Casche.py)")
            else:
                self.append_log(f"[INFO] ⚠️ 일반 모드 (batch_stage1_core.py) - 캐싱 최적화 미적용")
            
            # 먼저 전체 대상 요청 수를 계산 (버킷 수 결정용)
            target_rows = 0
            for idx, row in df.iterrows():
                if self.skip_exist_var.get() and "ST1_결과상품명" in df.columns:
                    val = safe_str(row.get("ST1_결과상품명", ""))
                    if val and val != "nan":
                        continue
                
                raw_name = safe_str(row.get("원본상품명", ""))
                category = safe_str(row.get("카테고리명", ""))
                sale_type = safe_str(row.get("판매형태", ""))
                
                if not raw_name or not category or not sale_type:
                    continue
                
                target_rows += 1

            # 버킷 수를 미리 계산 (모든 요청에 동일하게 적용)
            if CACHE_MODE_CORE and target_rows > 0:
                # [프롬프트 캐싱 최적화 전략 - 키 고정]
                # 실제 테스트 결과: 버킷 분산 시 캐시 히트율이 낮아짐 (10% 수준)
                # 해결책: prompt_cache_key를 하나로 고정하여 모든 요청이 같은 캐시 풀 공유
                # Batch API는 24시간에 걸쳐 처리되므로 overflow 우려는 낮음
                PROMPT_CACHE_BUCKETS = 1
                
                self.append_log(f"[INFO] 프롬프트 캐싱: 키 고정 전략 사용 (모든 요청이 'stage1_v1' 키 공유)")
                self.append_log(f"[INFO] 예상 요청 수: {target_rows}개, 캐시 히트율 향상 예상")
            else:
                PROMPT_CACHE_BUCKETS = 1
            
            jsonl_lines = []
            skipped_cnt = 0
            seen_custom_ids = set()
            duplicate_count = 0
            
            for idx, row in df.iterrows():
                if self.skip_exist_var.get() and "ST1_결과상품명" in df.columns:
                    val = safe_str(row.get("ST1_결과상품명", ""))
                    if val and val != "nan":
                        skipped_cnt += 1
                        continue
                
                # Core 함수 호출
                payload = build_stage1_batch_payload(idx, row, model, effort)
                if not payload:
                    skipped_cnt += 1
                    continue
                
                # Prompt Caching 최적화 (캐싱 모드일 때만)
                if CACHE_MODE_CORE and "body" in payload:
                    custom_id = payload.get("custom_id", f"row-{idx}")
                    
                    # 중복 custom_id 체크
                    if custom_id in seen_custom_ids:
                        duplicate_count += 1
                        continue
                    seen_custom_ids.add(custom_id)
                    
                    # prompt_cache_key: 키 고정 전략 (모든 요청이 동일한 키 사용)
                    payload["body"]["prompt_cache_key"] = "stage1_v1"
                    
                    # prompt_cache_retention은 모델이 지원하지 않을 수 있으므로 제거
                    # (prompt_cache_key만으로도 프롬프트 캐싱이 작동할 수 있음)
                
                jsonl_lines.append(json.dumps(payload, ensure_ascii=False))
            
            if duplicate_count > 0:
                self.append_log(f"[WARN] ⚠️ 중복 요청 {duplicate_count}개가 감지되어 제외되었습니다.")
            
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
            
            # 파일 크기 확인 및 분할 처리
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 요청 수: {len(jsonl_lines)}개")
            
            # 190MB 이상이면 분할 처리 (OpenAI Batch API 제한: 200MB)
            MAX_FILE_SIZE_MB = 190
            
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
                    model_name=model,
                    effort=effort,
                    max_size_mb=MAX_FILE_SIZE_MB,
                )
                self.append_log(f"✅ 총 {len(batch_ids)}개의 배치가 생성되었습니다: {', '.join(batch_ids)}")
                self._safe_msgbox("showinfo", "성공", f"{len(batch_ids)}개의 배치가 생성되었습니다:\n{', '.join(batch_ids)}")
            else:
                # 기존 방식: 단일 배치 생성
                self.append_log("OpenAI 업로드 중...")
                # 타임아웃 설정: 대용량 파일 업로드를 위해 10분으로 설정
                import httpx
                timeout = httpx.Timeout(600.0, connect=60.0)  # 10분 타임아웃
                client = OpenAI(api_key=key, timeout=timeout, max_retries=3)
                
                with open(jsonl_path, "rb") as f:
                    batch_input_file = client.files.create(file=f, purpose="batch")
                
                file_id = batch_input_file.id
                self.append_log(f"업로드 완료 ID: {file_id}")
                
                # Responses 엔드포인트로 Batch 생성 (core와 동일)
                batch_job = client.batches.create(
                    input_file_id=file_id,
                    endpoint="/v1/responses",
                    completion_window="24h",
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
                    output_file_id=None,
                )
                
                # 메인 런처 현황판에 T1 작업 시작 상태 기록
                try:
                    root_name = get_root_filename(src)
                    JobManager.update_status(root_name, text_msg="T1 (진행중)")
                    self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T1 (진행중)")
                except Exception:
                    # 런처가 없거나 job_history.json 이 없을 수 있으므로 조용히 무시
                    pass
                
                self._safe_msgbox("showinfo", "성공", f"배치 시작됨: {batch_id}")
            
            self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])

        except Exception as e:
            self.append_log(f"에러: {e}")
            self._safe_msgbox("showerror", "에러", str(e))
    
    def _create_batch_chunks(self, client, jsonl_path, excel_path, model_name, effort, max_size_mb=190):
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
        # 예상 청크 수 계산 (용량 기준으로만 계산)
        original_file_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
        estimated_total_chunks = max(1, int(original_file_size_mb / max_size_mb) + 1)
        
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
            chunk_size_bytes = 0
            
            # 청크 생성 (용량 기준으로만 분할, 500개 제한 없음)
            while i < total_requests:
                req_json = json.dumps(requests[i], ensure_ascii=False)
                req_size_bytes = len(req_json.encode('utf-8')) + 1  # +1 for newline
                
                # 다음 요청을 추가하면 크기 제한을 초과하는지 확인 (안전 마진 5% 포함)
                if chunk_size_bytes + req_size_bytes > max_size_mb * 1024 * 1024 * 0.95:
                    break
                
                chunk_requests.append(requests[i])
                chunk_size_bytes += req_size_bytes
                i += 1
            
            if not chunk_requests:
                # 첫 번째 요청 하나도 크기 제한을 초과하면 경고하고 건너뜀
                if i < total_requests:
                    self.append_log(f"⚠️ 청크 {chunk_num}: 첫 번째 요청이 크기 제한을 초과합니다. 건너뜁니다.")
                    i += 1  # 다음 요청으로 이동
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
                    
                    # 파일 업로드
                    with open(chunk_jsonl_path, "rb") as f:
                        batch_input_file = client.files.create(file=f, purpose="batch")
                    file_id = batch_input_file.id
                    
                    # 배치 생성
                    batch_job = client.batches.create(
                        input_file_id=file_id,
                        endpoint="/v1/responses",
                        completion_window="24h",
                    )
                    
                    batch_id = batch_job.id
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
                        status=batch_job.status,
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
            JobManager.update_status(root_name, text_msg="T1 (진행중)")
            self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T1 (진행중)")
        except Exception:
            pass
        
        return batch_ids

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
        
        # 컬럼 정의: batch_id | excel_name | group | memo | status | created | completed | model | effort | counts
        cols = ("batch_id", "excel_name", "group", "memo", "status", "created", "completed", "model", "effort", "counts")
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='headings', height=15, selectmode='extended')
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF')
        self.tree_active.tag_configure('group_header', background='#E8F4FD', font=("맑은 고딕", 9, "bold"))
        
        # 컬럼 헤더 한글화
        self.tree_active.heading("batch_id", text="배치 ID")
        self.tree_active.heading("excel_name", text="엑셀명")
        self.tree_active.heading("group", text="그룹")
        self.tree_active.heading("memo", text="메모")
        self.tree_active.heading("status", text="상태")
        self.tree_active.heading("created", text="생성일시")
        self.tree_active.heading("completed", text="완료일시")
        self.tree_active.heading("model", text="모델")
        self.tree_active.heading("effort", text="Effort")
        self.tree_active.heading("counts", text="완료/전체")
        
        # 컬럼 너비 설정
        self.tree_active.column("batch_id", width=180, anchor="w")
        self.tree_active.column("excel_name", width=200, anchor="w")
        self.tree_active.column("group", width=120, anchor="w")
        self.tree_active.column("memo", width=150, anchor="w")
        self.tree_active.column("status", width=80, anchor="center")
        self.tree_active.column("created", width=120, anchor="center")
        self.tree_active.column("completed", width=120, anchor="center")
        self.tree_active.column("model", width=80, anchor="center")
        self.tree_active.column("effort", width=60, anchor="center")
        self.tree_active.column("counts", width=80, anchor="center")
        
        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)
        
        self.menu_active = Menu(self, tearoff=0)
        self.menu_active.add_command(label="상태 갱신", command=lambda: self._refresh_selected(self.tree_active))
        self.menu_active.add_command(label="결과 병합", command=self._merge_selected)
        self.menu_active.add_command(label="정제 리포트 생성", command=self._report_selected_unified)
        self.menu_active.add_separator()
        self.menu_active.add_command(label="메모 편집", command=lambda: self._edit_memo(self.tree_active))
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

    def _auto_refresh_loop(self):
        """자동 상태 갱신 루프"""
        if self.auto_refresh_var.get() and not self.is_refreshing:
            # merged, failed 등 이미 끝난 상태는 조회 대상에서 제외
            # expired는 제외하지 않음 (output_file_id 확인을 위해 갱신 필요)
            jobs = load_batch_jobs()
            active_ids = [
                j['batch_id'] for j in jobs 
                if not j.get('archived') and j.get('status') not in ['completed', 'failed', 'cancelled', 'merged']
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
            if not vals:
                continue
            
            # 그룹 헤더인 경우 (batch_id가 비어있음)
            if not vals[0] or vals[0] == "":
                # 그룹 헤더의 자식들(배치들) 모두 가져오기
                children = tree.get_children(item)
                for child in children:
                    child_vals = tree.item(child)['values']
                    if child_vals and child_vals[0]:
                        ids.append(child_vals[0])
            else:
                # 일반 배치인 경우
                ids.append(vals[0])
        return ids
    
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
        
        # 그룹별로 분류
        groups = {}  # {group_id: [jobs]}
        ungrouped = []  # 그룹 없는 배치들
        
        for j in jobs:
            if j.get("archived", False): continue
            group_id = j.get("batch_group_id")
            if group_id:
                if group_id not in groups:
                    groups[group_id] = []
                groups[group_id].append(j)
            else:
                ungrouped.append(j)
        
        # 그룹별로 정렬 (그룹 ID 기준)
        sorted_groups = sorted(groups.items())
        
        idx = 0
        # 그룹별로 표시
        for group_id, group_jobs in sorted_groups:
            # 그룹 내 배치들을 chunk_index 순으로 정렬
            group_jobs.sort(key=lambda x: (x.get("chunk_index") or 999999, x.get("created_at") or ""))
            
            # 그룹 헤더 추가
            total_chunks = group_jobs[0].get("total_chunks", len(group_jobs))
            is_collapsed = group_id in self.collapsed_groups
            collapse_icon = "▶" if is_collapsed else "▼"
            group_header = f"{collapse_icon} 그룹: {group_id[:12]}... ({len(group_jobs)}/{total_chunks}개 배치)"
            header_item = self.tree_active.insert("", "end",
                text=group_header,
                values=("", "", group_id[:20], "", "", "", "", "", "", ""),
                tags=("group_header",))
            
            # 접혀있지 않으면 그룹 내 배치들 표시
            if not is_collapsed:
                # 그룹 내 배치들 표시
                for j in group_jobs:
                    cnt = "-"
                    if "request_counts" in j and j["request_counts"]:
                        rc = j["request_counts"]
                        cnt = f"{rc.get('completed',0)}/{rc.get('total',0)}"
                    c_at = (j.get("created_at") or "")[:16].replace("T", " ")
                    f_at = (j.get("completed_at") or "")[:16].replace("T", " ")
                    excel_name = get_excel_name_from_path(j.get("src_excel", ""))
                    memo = j.get("memo", "") or "-"
                    chunk_idx = j.get("chunk_index", "")
                    group_display = f"청크 {chunk_idx}" if chunk_idx else group_id[:12] if group_id else "-"
                    tag = 'even' if idx % 2 == 0 else 'odd'
                    self.tree_active.insert(header_item, "end",
                        text=j["batch_id"][:30],
                        values=(
                            j["batch_id"], excel_name, group_display, memo, j.get("status"),
                            c_at, f_at, j.get("model"), j.get("effort", "-"), cnt
                        ),
                        tags=(tag,))
                    idx += 1
        
        # 그룹 없는 배치들 표시
        for j in ungrouped:
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
                    j["batch_id"], excel_name, "-", memo, j.get("status"),
                    c_at, f_at, j.get("model"), j.get("effort", "-"), cnt
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
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.tree_arch.insert("", "end", 
                text=j["batch_id"][:30],
                values=(
                    j["batch_id"], excel_name, memo, j.get("status"), 
                    c_at, f_at, j.get("model"), j.get("effort", "-"), cnt
                ), 
                tags=(tag,))
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
                    
                    if not silent:
                        if remote.status == "expired" and output_file_id:
                            self.append_log(f"ℹ️ {bid}: 만료된 배치이지만 output_file_id가 있습니다. (다운로드 가능)")
                        elif remote.status == "completed":
                            if output_file_id:
                                self.append_log(f"✅ {bid}: {remote.status} (output_file_id: {output_file_id})")
                            else:
                                self.append_log(f"⚠️ {bid}: {remote.status} (output_file_id 없음 - 디버깅 필요)")
                        else:
                            self.append_log(f"✅ {bid}: {remote.status}")
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
        # completed 또는 expired 상태인 배치도 포함 (expired 상태에서도 output_file_id가 있으면 다운로드 가능)
        targets = [bid for bid in ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") in ["completed", "expired"]]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed' 또는 'expired' 상태의 작업이 없습니다.")
            return
        if messagebox.askyesno("병합", f"선택한 {len(targets)}건을 병합하시겠습니까?"):
            t = threading.Thread(target=self._run_merge_multi, args=(targets,))
            t.daemon = True
            t.start()

    def _run_merge_multi(self, ids):
        """
        선택된 Batch 들에 대해 결과 JSONL 다운로드 + 엑셀 병합을 수행.
        같은 그룹의 배치들은 하나의 엑셀로 병합됩니다.
        """
        key = self.api_key_var.get().strip()
        client = OpenAI(api_key=key)
        success_cnt = 0
        total_cost = 0.0
        
        jobs = load_batch_jobs()
        
        # 그룹별로 배치 분류
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
                
                # 그룹 내 모든 배치의 결과를 수집
                all_results_map = {}  # {custom_id: content}
                total_group_in = 0
                total_group_out = 0
                total_group_cost = 0.0
                total_group_cached = 0
                total_group_requests = 0
                total_group_cache_hits = 0
                model_name = first_job.get("model", "gpt-5-mini")
                
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
                        
                        # completed 또는 expired 상태에서 output_file_id가 있으면 다운로드 시도
                        output_file_id = getattr(remote, "output_file_id", None)
                        if not output_file_id:
                            output_ids = getattr(remote, "output_file_ids", None)
                            if output_ids and isinstance(output_ids, (list, tuple)) and len(output_ids) > 0:
                                output_file_id = output_ids[0]
                        
                        if remote.status == "expired":
                            if not output_file_id:
                                self.append_log(f"  ⚠️ {bid}: 만료된 배치이며 output_file_id를 찾을 수 없어 건너뜁니다. 수동으로 다운로드해주세요.")
                                upsert_batch_job(bid, status=remote.status, output_file_id=None)
                                continue
                            else:
                                self.append_log(f"  ℹ️ {bid}: 배치가 만료되었지만 output_file_id가 있어 다운로드를 시도합니다.")
                        elif remote.status not in ["completed", "expired"]:
                            self.append_log(f"  ⚠️ {bid}: 아직 completed 또는 expired 상태가 아니어서 건너뜁니다. (status={remote.status})")
                            upsert_batch_job(bid, status=remote.status, output_file_id=output_file_id)
                            continue
                        
                        if not output_file_id:
                            self.append_log(f"  ⚠️ {bid}: output_file_id를 찾을 수 없어 건너뜁니다.")
                            continue
                        
                        # 결과 다운로드
                        base_name, _ = os.path.splitext(os.path.basename(src_path))
                        base_dir = os.path.dirname(src_path)
                        out_jsonl = os.path.join(base_dir, f"{base_name}_stage1_batch_output_{bid}.jsonl")
                        
                        file_content = client.files.content(output_file_id)
                        if hasattr(file_content, "read"):
                            content_bytes = file_content.read()
                        elif hasattr(file_content, "iter_bytes"):
                            chunks = []
                            for ch in file_content.iter_bytes():
                                chunks.append(ch)
                            content_bytes = b"".join(chunks)
                        else:
                            content_bytes = file_content
                        
                        with open(out_jsonl, "wb") as f:
                            f.write(content_bytes)
                        
                        upsert_batch_job(bid, status=remote.status, output_file_id=output_file_id, output_jsonl=out_jsonl)
                        
                        # JSONL 파일 읽어서 결과 수집
                        batch_in_tok = 0
                        batch_out_tok = 0
                        batch_cached_tok = 0
                        batch_total_requests = 0
                        batch_cache_hits = 0
                        with open(out_jsonl, "r", encoding="utf-8") as f:
                            for line in f:
                                if not line.strip():
                                    continue
                                data = json.loads(line)
                                cid = data.get("custom_id")
                                resp = data.get("response")
                                error = data.get("error")
                                if error is not None or not resp or not cid:
                                    continue
                                
                                # core 의 파서 사용 (Responses 포맷 기준)
                                refined = extract_text_from_response_dict(resp)
                                in_tok, out_tok, _ = extract_usage_from_response_dict(resp)
                                batch_in_tok += in_tok
                                batch_out_tok += out_tok
                                all_results_map[cid] = refined
                                
                                # 캐싱 통계 수집
                                body = resp.get("body", {}) if isinstance(resp, dict) else {}
                                usage = body.get("usage", {})
                                input_tokens_details = usage.get("input_tokens_details", {})
                                cached_tokens = input_tokens_details.get("cached_tokens", 0)
                                batch_cached_tok += cached_tokens
                                batch_total_requests += 1
                                if cached_tokens > 0:
                                    batch_cache_hits += 1
                        
                        # 배치별 캐싱 통계 출력
                        if batch_total_requests > 0:
                            cache_hit_rate = (batch_cache_hits / batch_total_requests * 100)
                            cache_savings_pct = (batch_cached_tok / batch_in_tok * 100) if batch_in_tok > 0 else 0
                            self.append_log(f"  [캐싱] {bid}: 요청 {batch_total_requests}건, 히트 {batch_cache_hits}건 ({cache_hit_rate:.1f}%), 캐시 토큰 {batch_cached_tok:,} ({cache_savings_pct:.1f}%)")
                        
                        total_group_in += batch_in_tok
                        total_group_out += batch_out_tok
                        total_group_cached += batch_cached_tok
                        total_group_requests += batch_total_requests
                        total_group_cache_hits += batch_cache_hits
                        
                        # 비용 계산 (50% 할인)
                        pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0.25, "output": 2.0})
                        cost = ((batch_in_tok * pricing["input"] + batch_out_tok * pricing["output"]) / 1_000_000) * 0.5
                        total_group_cost += cost
                        
                        # 캐시로 절감된 비용 계산
                        cache_savings = (batch_cached_tok / 1_000_000) * pricing["input"] * 0.5
                        if cache_savings > 0:
                            self.append_log(f"  [비용절감] {bid}: 캐싱으로 ${cache_savings:.4f} 절감")
                        
                    except Exception as e:
                        self.append_log(f"  ❌ {bid} 결과 다운로드 실패: {e}")
                        continue
                
                if not all_results_map:
                    self.append_log(f"⚠️ 그룹 {group_id}: 병합할 결과가 없습니다.")
                    continue
                
                # 그룹의 전체 청크 수 확인 및 검증
                expected_total_chunks = first_job.get("total_chunks")
                if expected_total_chunks:
                    downloaded_batch_ids = []
                    for bid in batch_ids_sorted:
                        local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                        if local_job and local_job.get("status") in ["completed", "expired"]:
                            out_jsonl = local_job.get("output_jsonl") or os.path.join(
                                os.path.dirname(src_path),
                                f"{os.path.splitext(os.path.basename(src_path))[0]}_stage1_batch_output_{bid}.jsonl"
                            )
                            if os.path.exists(out_jsonl):
                                downloaded_batch_ids.append(bid)
                    
                    if len(downloaded_batch_ids) < expected_total_chunks:
                        missing = expected_total_chunks - len(downloaded_batch_ids)
                        self.append_log(f"⚠️ 그룹 {group_id}: 예상 {expected_total_chunks}개 중 {len(downloaded_batch_ids)}개만 다운로드되었습니다. ({missing}개 누락 가능)")
                
                # 통합 결과를 엑셀에 병합
                base_name, _ = os.path.splitext(os.path.basename(src_path))
                base_dir = os.path.dirname(src_path)
                
                # 텍스트 파이프라인 1단계 완료 파일명: T0 → T1로 버전 업
                pattern = r"_T(\d+)_I(\d+)"
                match = re.search(pattern, base_name, re.IGNORECASE)
                if match:
                    current_t = int(match.group(1))
                    current_i = int(match.group(2))
                    original_name = re.sub(r"_T\d+_I\d+.*$", "", base_name, flags=re.IGNORECASE).rstrip("_")
                    new_t = current_t + 1
                    new_i = current_i
                    out_filename = f"{original_name}_T{new_t}_I{new_i}.xlsx"
                else:
                    out_filename = f"{base_name}_T1_I0.xlsx"
                out_excel = os.path.join(base_dir, out_filename)
                
                df = pd.read_excel(src_path)
                target_col = "ST1_결과상품명"
                if target_col not in df.columns:
                    df[target_col] = ""
                df[target_col] = df[target_col].astype(str)
                
                cnt = 0
                for cid, val in all_results_map.items():
                    try:
                        # custom_id 형식: row-123  → 인덱스 123
                        idx = int(str(cid).split("-")[1])
                        if 0 <= idx < len(df):
                            df.at[idx, target_col] = val
                            cnt += 1
                    except Exception:
                        continue
                
                # 엑셀 저장
                if safe_save_excel(df, out_excel):
                    # 그룹 내 모든 배치를 merged 상태로 업데이트
                    for bid in batch_ids_sorted:
                        upsert_batch_job(bid, out_excel=out_excel, status="merged")
                    
                    # History 기록 (첫 번째 배치 기준)
                    c_at_str = first_job.get("created_at", "")
                    if c_at_str:
                        c_at = datetime.fromisoformat(c_at_str)
                    else:
                        c_at = datetime.now()
                    finish_dt = datetime.now()
                    append_run_history(
                        stage="Stage 1 Batch",
                        model_name=model_name,
                        reasoning_effort=first_job.get("effort", "low"),
                        src_file=src_path,
                        out_file=out_excel,
                        total_rows=len(df),
                        api_rows=len(all_results_map),
                        elapsed_seconds=(finish_dt - c_at).total_seconds(),
                        total_in_tok=total_group_in,
                        total_out_tok=total_group_out,
                        total_reasoning_tok=0,
                        input_cost_usd=0,
                        output_cost_usd=0,
                        total_cost_usd=total_group_cost,
                        start_dt=c_at,
                        finish_dt=finish_dt,
                        api_type="batch",
                        batch_id=batch_ids[0],  # 첫 번째 배치 ID 사용
                        success_rows=cnt,
                        fail_rows=len(all_results_map) - cnt,
                    )
                    
                    # 메인 런처 현황판 업데이트
                    try:
                        root_name = get_root_filename(src_path)
                        JobManager.update_status(root_name, text_msg="T1(완료)")
                        self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T1(완료)")
                    except Exception as e:
                        self.append_log(f"[WARN] 현황판 연동 실패: {e}")
                    
                    # 그룹 전체 캐싱 통계 출력
                    group_cache_hit_rate = (total_group_cache_hits / total_group_requests * 100) if total_group_requests > 0 else 0
                    group_cache_savings_pct = (total_group_cached / total_group_in * 100) if total_group_in > 0 else 0
                    group_cache_savings = (total_group_cached / 1_000_000) * pricing["input"] * 0.5
                    
                    self.append_log(f"  [그룹] 병합 완료 ({cnt}건): {os.path.basename(out_excel)}")
                    self.append_log(f"  [그룹 캐싱 통계] 요청 {total_group_requests:,}건, 히트 {total_group_cache_hits:,}건 ({group_cache_hit_rate:.1f}%), 캐시 토큰 {total_group_cached:,} ({group_cache_savings_pct:.1f}%)")
                    if group_cache_savings > 0:
                        self.append_log(f"  [그룹 비용절감] 캐싱으로 총 ${group_cache_savings:.4f} 절감")
                    
                    success_cnt += 1
                    total_cost += total_group_cost
                else:
                    self.append_log(f"  [그룹] 엑셀 저장 실패: {out_excel}")
                
            except Exception as e:
                self.append_log(f"❌ 그룹 {group_id} 병합 실패: {e}")
                import traceback
                self.append_log(traceback.format_exc())
                continue
        
        # 그룹 없는 배치들 처리 (기존 로직)
        for bid in ungrouped_batches:
            self.append_log(f"--- 병합 시작: {bid} ---")
            try:
                jobs = load_batch_jobs()
                local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                remote = client.batches.retrieve(bid)
                
                # completed 또는 expired 상태에서 output_file_id가 있으면 다운로드 시도
                if remote.status == "expired":
                    self.append_log(f"ℹ️ {bid}: 배치가 만료되었지만 output_file_id가 있으면 다운로드를 시도합니다.")

                # output_file_id / output_file_ids 처리 (신버전 호환)
                output_file_id = getattr(remote, "output_file_id", None)
                if not output_file_id:
                    output_ids = getattr(remote, "output_file_ids", None)
                    if output_ids and isinstance(output_ids, (list, tuple)) and len(output_ids) > 0:
                        output_file_id = output_ids[0]
                if not output_file_id:
                    if remote.status == "expired":
                        self.append_log(f"⚠️ {bid}: 만료된 배치이며 output_file_id를 찾을 수 없어 건너뜁니다. 수동으로 다운로드해주세요.")
                    else:
                        self.append_log(f"❌ output_file_id 없음: {bid}")
                    continue

                file_content = client.files.content(output_file_id)
                if hasattr(file_content, "read"):
                    content_bytes = file_content.read()
                elif hasattr(file_content, "iter_bytes"):
                    chunks = []
                    for ch in file_content.iter_bytes():
                        chunks.append(ch)
                    content_bytes = b"".join(chunks)
                else:
                    content_bytes = file_content  # type: ignore

                if local_job and local_job.get("src_excel"):
                    src_path = local_job["src_excel"]
                    base_name, _ = os.path.splitext(os.path.basename(src_path))
                    base_dir = os.path.dirname(src_path)
                    # JSONL은 원본과 같은 폴더에 저장
                    out_jsonl = os.path.join(base_dir, f"{base_name}_stage1_batch_output.jsonl")
                    # 텍스트 파이프라인 1단계 완료 파일명: T0 → T1로 버전 업 (stage1_api_ver_runner.py와 동일한 방식)
                    pattern = r"_T(\d+)_I(\d+)"
                    match = re.search(pattern, base_name, re.IGNORECASE)
                    if match:
                        current_t = int(match.group(1))
                        current_i = int(match.group(2))
                        # 원본명 추출 (버전 정보 제거)
                        original_name = re.sub(r"_T\d+_I\d+.*$", "", base_name, flags=re.IGNORECASE).rstrip("_")
                        # T 버전만 +1 (I는 유지)
                        new_t = current_t + 1
                        new_i = current_i
                        out_filename = f"{original_name}_T{new_t}_I{new_i}.xlsx"
                    else:
                        # 버전 정보가 없으면 T1_I0으로 생성
                        out_filename = f"{base_name}_T1_I0.xlsx"
                    out_excel = os.path.join(base_dir, out_filename)
                else:
                    out_jsonl = os.path.join(BASE_DIR, f"output_{bid}.jsonl")
                    out_excel = os.path.join(BASE_DIR, f"output_{bid}.xlsx")
                    src_path = None

                with open(out_jsonl, "wb") as f:
                    f.write(content_bytes)

                results_map = {}
                batch_in_tok = 0
                batch_out_tok = 0
                batch_cached_tok = 0
                batch_total_requests = 0
                batch_cache_hits = 0

                with open(out_jsonl, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        data = json.loads(line)
                        cid = data.get("custom_id")
                        resp = data.get("response")
                        error = data.get("error")
                        if error is not None or not resp or not cid:
                            continue
                        
                        # core 의 파서 사용 (Responses 포맷 기준)
                        refined = extract_text_from_response_dict(resp)
                        in_tok, out_tok, _ = extract_usage_from_response_dict(resp)
                        batch_in_tok += in_tok
                        batch_out_tok += out_tok
                        results_map[cid] = refined
                        
                        # 캐싱 통계 수집
                        body = resp.get("body", {}) if isinstance(resp, dict) else {}
                        usage = body.get("usage", {})
                        input_tokens_details = usage.get("input_tokens_details", {})
                        cached_tokens = input_tokens_details.get("cached_tokens", 0)
                        batch_cached_tok += cached_tokens
                        batch_total_requests += 1
                        if cached_tokens > 0:
                            batch_cache_hits += 1
                
                # 캐싱 통계 출력
                cache_hit_rate = (batch_cache_hits / batch_total_requests * 100) if batch_total_requests > 0 else 0
                cache_savings_pct = (batch_cached_tok / batch_in_tok * 100) if batch_in_tok > 0 else 0
                self.append_log(f"  [캐싱] {bid}: 요청 {batch_total_requests}건, 히트 {batch_cache_hits}건 ({cache_hit_rate:.1f}%), 캐시 토큰 {batch_cached_tok:,} ({cache_savings_pct:.1f}%)")
                
                model_name = local_job.get("model", "gpt-5-mini") if local_job else "gpt-5-mini"
                pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0.25, "output": 2.0})
                # Batch 할인(50%) 반영
                cost = ((batch_in_tok * pricing["input"] + batch_out_tok * pricing["output"]) / 1_000_000) * 0.5
                total_cost += cost
                
                # 캐시로 절감된 비용 계산
                cache_savings = (batch_cached_tok / 1_000_000) * pricing["input"] * 0.5
                if cache_savings > 0:
                    self.append_log(f"  [비용절감] {bid}: 캐싱으로 ${cache_savings:.4f} 절감")

                if src_path and os.path.exists(src_path):
                    df = pd.read_excel(src_path)
                    target_col = "ST1_결과상품명"
                    if target_col not in df.columns:
                        df[target_col] = ""
                    df[target_col] = df[target_col].astype(str)
                    cnt = 0
                    for cid, val in results_map.items():
                        try:
                            # custom_id 형식: row-123  → 인덱스 123
                            idx = int(str(cid).split("-")[1])
                            if 0 <= idx < len(df):
                                df.at[idx, target_col] = val
                                cnt += 1
                        except Exception:
                            continue
                    # 엑셀 저장 (열려 있을 수 있으므로 안전 저장 유틸 사용)
                    if safe_save_excel(df, out_excel):
                        upsert_batch_job(bid, out_excel=out_excel, status="merged")

                        # History 기록 (Stage 1) - 타임존 정보 혼합 방지를 위해 naive datetime 사용
                        c_at_str = local_job.get("created_at", "")
                        if c_at_str:
                            # isoformat()으로 저장된 naive datetime 가정
                            c_at = datetime.fromisoformat(c_at_str)
                        else:
                            c_at = datetime.now()
                        finish_dt = datetime.now()
                        append_run_history(
                            stage="Stage 1 Batch",
                            model_name=model_name,
                            reasoning_effort=local_job.get("effort", "low"),
                            src_file=src_path,
                            out_file=out_excel,
                            total_rows=len(df),
                            api_rows=len(results_map),
                            elapsed_seconds=(finish_dt - c_at).total_seconds(),
                            total_in_tok=batch_in_tok,
                            total_out_tok=batch_out_tok,
                            total_reasoning_tok=0,
                            input_cost_usd=0,
                            output_cost_usd=0,
                            total_cost_usd=cost,
                            start_dt=c_at,
                            finish_dt=finish_dt,
                            api_type="batch",
                            batch_id=bid,
                            success_rows=cnt,
                            fail_rows=len(results_map) - cnt,
                        )

                        # 메인 런처 현황판에 Stage1 텍스트(T1) 완료 상태 기록 (img 상태는 변경하지 않음)
                        try:
                            root_name = get_root_filename(src_path)
                            JobManager.update_status(root_name, text_msg="T1(완료)")
                            self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T1(완료)")
                        except Exception as e:
                            self.append_log(f"[WARN] 현황판 연동 실패: {e}")

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
        item = sel[0]
        item_tags = self.tree_active.item(item)['tags']
        item_values = self.tree_active.item(item)['values']
        
        # 그룹 헤더인 경우 접기/펼치기
        if 'group_header' in item_tags:
            group_id = item_values[2] if len(item_values) > 2 else ""  # 그룹 ID는 3번째 컬럼
            if group_id:
                # 접기/펼치기 상태 토글
                if group_id in self.collapsed_groups:
                    self.collapsed_groups.discard(group_id)
                else:
                    self.collapsed_groups.add(group_id)
                # 목록 다시 로드하여 상태 반영
                self._load_jobs_all()
            return
        
        # 일반 배치인 경우 기존 동작
        bid = item_values[0] if item_values else ""
        if bid:
            self.batch_id_var.set(bid)
            self.main_tabs.select(self.tab_merge)

    # ----------------------------------------------------
    # Tab 3: Manual
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
        
        self._safe_msgbox("showwarning", "배치 생성 실패", msg)
        
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
            self._safe_msgbox("showwarning", "경고", "실패한 청크 정보 JSON 파일을 선택해주세요.")
            return
        
        if not os.path.exists(failed_file):
            self._safe_msgbox("showerror", "오류", f"파일을 찾을 수 없습니다:\n{failed_file}")
            return
        
        if not self.api_key_var.get():
            self._safe_msgbox("showwarning", "경고", "API Key를 입력해주세요.")
            return
        
        try:
            with open(failed_file, "r", encoding="utf-8") as f:
                failed_chunks = json.load(f)
            
            # 목록도 업데이트
            self._load_failed_chunks_from_file(failed_file)
        except Exception as e:
            self._safe_msgbox("showerror", "오류", f"JSON 파일 읽기 실패:\n{e}")
            return
        
        if not failed_chunks:
            self._safe_msgbox("showinfo", "알림", "재시도할 실패한 청크가 없습니다.")
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
            effort = failed_info.get("effort", "low")
            batch_group_id = failed_info.get("batch_group_id", "")
            
            if not os.path.exists(chunk_file):
                self.append_log(f"⚠️ 청크 {chunk_num}: 파일을 찾을 수 없습니다: {chunk_file}")
                continue
            
            self.append_log(f"[RETRY] 청크 {chunk_num} 재시도 중... ({os.path.basename(chunk_file)})")
            
            try:
                # 파일 업로드
                with open(chunk_file, "rb") as f:
                    batch_input_file = client.files.create(file=f, purpose="batch")
                file_id = batch_input_file.id
                
                # 배치 생성
                batch_job = client.batches.create(
                    input_file_id=file_id,
                    endpoint="/v1/responses",
                    completion_window="24h",
                )
                
                batch_id = batch_job.id
                retry_batch_ids.append(batch_id)
                self.append_log(f"✅ 청크 {chunk_num} 재시도 성공: {batch_id}")
                
                # 작업 이력 기록
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=excel_path,
                    jsonl_path=chunk_file,
                    model=model_name,
                    effort=effort,
                    status=batch_job.status,
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
                self._safe_msgbox("showinfo", "완료", f"{len(retry_batch_ids)}개 청크 재시도 성공:\n{', '.join(retry_batch_ids[:5])}{'...' if len(retry_batch_ids) > 5 else ''}\n\n배치 관리 탭에서 진행 상황을 확인하세요.")
            ])
        else:
            self.append_log(f"⚠️ 재시도된 배치가 없습니다.")
            self.after(0, lambda: self._safe_msgbox("showwarning", "경고", "재시도된 배치가 없습니다."))

if __name__ == "__main__":
    app = Stage1BatchGUI()
    app.mainloop()