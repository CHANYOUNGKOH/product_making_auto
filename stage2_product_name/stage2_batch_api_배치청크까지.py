"""
stage2_batch_api.py

Stage 2 Batch API 실행 스크립트 (GUI) - Final Version
- 기능: 엑셀(이미지+텍스트) -> Batch JSONL 생성(Vision API) -> 업로드 -> 실행 -> 병합 -> JSON 분석 리포트
- 특징: GPT-5/4o 모델 지원, 이미지 옵션 처리, 상세 툴팁 및 가이드 포함
"""

import os
import sys
import json
import threading
import subprocess
import re
from datetime import datetime

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Menu
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI


# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸 (Stage2 전용)
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 상품_T0_I0.xlsx -> 상품.xlsx
    예: 상품_T2_I1.xlsx -> 상품.xlsx
    예: 상품_T1_I0(업완).xlsx -> 상품.xlsx
    예: 상품_T1_I0_T2_I1.xlsx -> 상품.xlsx (여러 버전 패턴 제거)
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
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")

    return base + ext


def get_next_version_path(current_path: str, task_type: str = "text") -> str:
    """
    현재 파일명을 분석해서 다음 단계의 파일명을 생성합니다.
    파일명 형식: 원본명_T{숫자}_I{숫자}.xlsx
    - task_type='text'  → T 버전 +1 (Stage1: T1, Stage2: T2, ...)
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
        """메인 런처 현황판 상태 업데이트 (Stage1/Stage2 공용)."""
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


# [필수 의존성] stage2_core.py / stage2_run_history.py
try:
    from stage2_core import (
        safe_str,
        build_stage2_request_from_row, # Row -> Request 객체(프롬프트+이미지경로) 변환
    )
    from stage2_run_history import append_run_history
except ImportError:
    # 의존성 파일 부재 시 비상용 더미
    def safe_str(x): return str(x) if x is not None else ""
    def build_stage2_request_from_row(*args, **kwargs): return None
    def append_run_history(*args, **kwargs): pass

# 안정적으로 동작하는 기존 Stage2 Batch 코어 로직 재사용
try:
    from stage2_batch_api_기존gpt import (
        create_stage2_batch_input_jsonl,
        create_batch_from_jsonl,
        download_batch_output_if_ready,
        merge_batch_output_to_excel,
    )
except ImportError:
    # 구버전 환경에서는 이 모듈이 없을 수 있으므로, 이 경우에는 아래 새 로직만 사용
    create_stage2_batch_input_jsonl = None  # type: ignore
    create_batch_from_jsonl = None  # type: ignore
    download_batch_output_if_ready = None  # type: ignore
    merge_batch_output_to_excel = None  # type: ignore

# === 기본 설정 ===
API_KEY_FILE = ".openai_api_key_stage2_batch"
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage2_batch_jobs.json")

# Stage 2용 Batch 모델/가격 (stage2_core 와 동일한 gpt-5 계열만 사용)
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

# --- UI 색상 팔레트 (Modern Blue) ---
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
# 툴팁 클래스 (최상단 정의)
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
# Payload Builder (Stage 2 전용)
# ========================================================
def build_stage2_batch_payload(row_index, row, model, effort, use_thumb, allow_url):
    """
    Stage 2 Core의 로직을 활용하여 Batch Payload 생성
    - use_thumb: 썸네일(이미지대) 포함 여부
    - allow_url: URL 이미지 다운로드 허용 여부 (Core 로직에 따라 동작)
    """
    try:
        # Core 함수 호출 (Core가 옵션을 지원하지 않으면 기본 로직 수행)
        # 여기서는 Core가 row 전체를 받아 판단한다고 가정하고, 옵션에 따라 이미지 경로 리스트를 필터링할 수도 있음
        # 하지만 일반적인 Core 구현상 row에 있는 정보를 다 가져오므로, 
        # Core 수정 없이 여기서 request 객체의 image_paths를 조작하는 것이 안전함.
        req = build_stage2_request_from_row(row)
    except Exception:
        return None

    if not req: return None

    # 1. 이미지 필터링 로직 (옵션 적용)
    final_messages = []
    
    # Prompt 메시지 (System/User text)
    # Core가 만든 messages 리스트를 순회하며 이미지 부분만 필터링
    for msg in req.messages:
        if not isinstance(msg.get('content'), list):
            final_messages.append(msg) # 텍스트만 있는 경우 그대로 사용
            continue
            
        new_content = []
        for item in msg['content']:
            if item['type'] == 'text':
                new_content.append(item)
            elif item['type'] == 'image_url':
                url = item['image_url']['url']
                
                # 썸네일 제외 로직 (이미지대 컬럼값과 비교)
                thumb_val = safe_str(row.get("이미지대", ""))
                if not use_thumbnail and thumb_val and url == thumb_val:
                    continue # 썸네일이면 건너뜀
                
                # URL 허용 여부 로직
                if not allow_url and (url.startswith("http://") or url.startswith("https://")):
                    continue # URL 비허용이면 건너뜀
                
                new_content.append(item)
        
        if new_content:
            final_messages.append({"role": msg['role'], "content": new_content})

    # 2. Body 구성
    body = {
        "model": model,
        "messages": final_messages,
        "response_format": {"type": "json_object"} # JSON 출력 강제
    }
    
    # 3. 추론 모델 파라미터
    is_reasoning = any(x in model for x in ["gpt-5", "o1", "o3"])
    if is_reasoning and effort in ["low", "medium", "high"]:
        body["reasoning_effort"] = effort
    elif not is_reasoning:
        body["temperature"] = 0.0 # 정밀도 우선

    # 4. Batch Request 객체
    request_obj = {
        "custom_id": f"row_{row_index}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": body
    }
    return request_obj

# ========================================================
# GUI Class
# ========================================================
class Stage2BatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 2: Batch API Manager (Multimodal & Analytics)")
        self.geometry("1280x950")
        
        self.api_key_var = tk.StringVar()
        self.src_file_var = tk.StringVar()
        
        # 기본값
        self.model_var = tk.StringVar(value="gpt-5-mini") 
        self.effort_var = tk.StringVar(value="medium")
        self.skip_exist_var = tk.BooleanVar(value=True)
        
        # Stage 2 옵션
        self.use_thumbnail_var = tk.BooleanVar(value=False)  # 기본값: 썸네일 제외 (성능 최적화)
        self.allow_url_var = tk.BooleanVar(value=False)

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

        # 1. 상단 API
        f_top = ttk.LabelFrame(main_container, text="🔑 API 설정", padding=10)
        f_top.pack(fill='x', pady=(0, 10))
        ttk.Label(f_top, text="Batch API Key:", font=("맑은 고딕", 9, "bold")).pack(side='left')
        entry_key = ttk.Entry(f_top, textvariable=self.api_key_var, show="*", width=50, font=("Consolas", 10))
        entry_key.pack(side='left', padx=10)
        btn_save = ttk.Button(f_top, text="저장", command=self._save_key, style="Primary.TButton")
        btn_save.pack(side='left')
        ToolTip(btn_save, "입력한 API Key를 로컬에 저장합니다.")

        btn_help = ttk.Button(f_top, text="❓ 사용법 / 워크플로우", command=self._show_help_dialog)
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

    def append_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')

    def _show_help_dialog(self):
        msg = (
            "[Stage 2 Batch API 워크플로우]\n\n"
            "1. [생성 탭]: 엑셀 파일을 선택하고 'Start Batch'를 클릭.\n"
            "   - 이미지가 포함된 경우 Vision API 요청을 자동으로 생성합니다.\n"
            "   - 비용은 실시간 API 대비 50% 절감됩니다.\n\n"
            "2. [관리 탭]: 진행 상황을 확인하고 '완료(completed)' 시 병합합니다.\n"
            "   - [선택 갱신]: OpenAI 서버에서 최신 상태를 가져옵니다.\n"
            "   - [선택 병합]: 결과를 다운로드하여 원본 엑셀(ST2_JSON)에 저장합니다.\n"
            "   - [분석 리포트]: JSON 파싱 성공률, 키워드 수 등을 분석합니다.\n\n"
            "※ 이미지가 없는 행은 텍스트만 분석하거나, 설정에 따라 건너뜁니다."
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
        ToolTip(btn_file, "Stage 2를 수행할 엑셀 파일을 선택하세요.\n이미지 컬럼이 없어도 텍스트 분석이 가능하면 진행됩니다.")
        
        # Step 2: 옵션
        f_opt = ttk.LabelFrame(container, text="2. 배치 옵션 설정", padding=15)
        f_opt.pack(fill='x', pady=5)
        
        # 모델/Effort
        fr1 = ttk.Frame(f_opt)
        fr1.pack(fill='x', pady=5)
        ttk.Label(fr1, text="모델 (Model):", width=12).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys())
        cb_model = ttk.Combobox(fr1, textvariable=self.model_var, values=models, state="readonly", width=20)
        cb_model.pack(side='left', padx=5)
        ToolTip(cb_model, "이미지 분석(Vision) 성능이 뛰어난 GPT-5 계열 모델 권장.")
        
        ttk.Label(fr1, text="추론 강도:", width=10).pack(side='left', padx=(20, 5))
        cb_effort = ttk.Combobox(fr1, textvariable=self.effort_var, values=["low", "medium", "high"], state="readonly", width=12)
        cb_effort.pack(side='left', padx=5)
        ToolTip(cb_effort, "이미지 분석의 깊이를 결정합니다.\nMedium 권장.")
        
        # Stage 2 전용 옵션
        fr2 = ttk.Frame(f_opt)
        fr2.pack(fill='x', pady=5)
        chk_skip = ttk.Checkbutton(fr2, text=" 이미 ST2_JSON이 있는 행은 건너뛰기 (Skip)", variable=self.skip_exist_var)
        chk_skip.pack(side='left', padx=5)
        ToolTip(chk_skip, "중복 과금 방지를 위해 이미 결과가 있는 행은 제외합니다.")

        chk_thumb = ttk.Checkbutton(fr2, text=" 썸네일(이미지대) 포함", variable=self.use_thumbnail_var)
        chk_thumb.pack(side='left', padx=20)
        ToolTip(chk_thumb, "체크 시: 대표 이미지(이미지대)도 AI에게 보여줍니다.\n해제 시: 상세 페이지만 분석합니다.")

        chk_url = ttk.Checkbutton(fr2, text=" URL 이미지 허용", variable=self.allow_url_var)
        chk_url.pack(side='left', padx=20)
        ToolTip(chk_url, "체크 시: 웹 링크(http) 이미지도 다운로드하여 분석합니다.\n해제 시: 로컬 파일 경로만 인식합니다.")

        # Step 3: 실행
        f_step3 = ttk.LabelFrame(container, text="3. 실행", padding=15)
        f_step3.pack(fill='x', pady=15)
        btn_run = ttk.Button(f_step3, text="🚀 JSONL 생성 및 배치 업로드 (Start Batch)", command=self._start_create_batch, style="Success.TButton")
        btn_run.pack(fill='x', ipady=8)
        ToolTip(btn_run, "1. 엑셀 읽기 (이미지 포함)\n2. JSONL 생성\n3. 배치 시작 요청 (24시간 내 완료)")
        
        ttk.Label(container, text="※ 배치 API는 결과 수신까지 최대 24시간이 소요됩니다. (비용 50% 절감)", foreground="#666").pack()

    def _select_src_file(self):
        p = filedialog.askopenfilename(
            title="Stage2 엑셀 선택 (T1 버전만 가능)",
            filetypes=[("Excel", "*.xlsx;*.xls")]
        )
        if p:
            # T1 포함 여부 검증
            base_name = os.path.splitext(os.path.basename(p))[0]
            if not re.search(r"_T1_[Ii]\d+", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 T1 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {os.path.basename(p)}\n"
                    f"파일명에 '_T1_I*' 패턴이 포함되어 있어야 합니다."
                )
                return
            self.src_file_var.set(p)

    def _start_create_batch(self):
        if not self.api_key_var.get():
            messagebox.showwarning("오류", "API Key 필요")
            return
        if not self.src_file_var.get():
            messagebox.showwarning("오류", "파일 선택 필요")
            return
        
        # T1 포함 여부 검증
        src = self.src_file_var.get().strip()
        base_name = os.path.splitext(os.path.basename(src))[0]
        if not re.search(r"_T1_[Ii]\d+", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 T1 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {os.path.basename(src)}\n"
                f"파일명에 '_T1_I*' 패턴이 포함되어 있어야 합니다."
            )
            return
        
        t = threading.Thread(target=self._run_create_batch)
        t.daemon = True
        t.start()

    def _run_create_batch(self):
        """
        1단계: 엑셀 → /v1/responses용 JSONL 생성 (기존 안정 버전 코어 사용)
        2단계: JSONL 업로드 → Batch 생성
        3단계: 현재 GUI의 작업 이력에 기록
        """
        key = self.api_key_var.get().strip()
        src = self.src_file_var.get().strip()
        model = self.model_var.get().strip() or "gpt-5-mini"
        effort = self.effort_var.get().strip() or "medium"

        use_thumb = self.use_thumbnail_var.get()
        allow_url = self.allow_url_var.get()

        try:
            if create_stage2_batch_input_jsonl is None or create_batch_from_jsonl is None:
                raise RuntimeError("stage2_batch_api_기존gpt 모듈을 찾을 수 없어 Batch 코어를 사용할 수 없습니다.")

            self.append_log(f"[RUN] Stage2: JSONL 생성 + Batch 생성 시작 → {os.path.basename(src)}")

            # 1) 엑셀 → JSONL 생성
            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage2_batch_input.jsonl"

            info = create_stage2_batch_input_jsonl(
                excel_path=src,
                jsonl_path=jsonl_path,
                model_name=model,
                effort=effort,
                skip_filled=self.skip_exist_var.get(),
                use_thumbnail=use_thumb,
                allow_url=allow_url,
                log_func=self.append_log,
            )

            self.append_log(
                f"[DONE] 요청 JSONL 생성: total_rows={info['total_rows']}, "
                f"target_rows={info['target_rows']}, num_requests={info['num_requests']}"
            )

            # 2) 배치 파일 크기 확인 및 분할 처리
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 요청 수: {info['num_requests']}개")
            
            # 190MB 이상이거나 요청이 500개 이상이면 분할 처리 (OpenAI Batch API 제한: 200MB)
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
                    model_name=model,
                    effort=effort,
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
                batch = create_batch_from_jsonl(
                    client=client,
                    jsonl_path=jsonl_path,
                    excel_path=src,
                    model_name=model,
                    log_func=self.append_log,
                )

                batch_id = batch.id
                self.append_log(f"✅ 배치 시작! ID: {batch_id}, status={batch.status}")

                # 3) 작업 이력 기록
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=src,
                    jsonl_path=jsonl_path,
                    model=model,
                    effort=effort,
                    status=batch.status,
                    output_file_id=None,
                )

                # 메인 런처 현황판에 Stage2(Text) 작업 시작 상태 기록: T2 (진행중) (img 상태는 변경하지 않음)
                try:
                    root_name = get_root_filename(src)
                    JobManager.update_status(root_name, text_msg="T2 (진행중)")
                    self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T2 (진행중)")
                except Exception:
                    # 런처나 job_history.json 이 없을 수 있으므로 조용히 무시
                    pass
                messagebox.showinfo("성공", f"배치 시작됨: {batch_id}")
            
            self._load_jobs_all()
            self._load_archive_list()

        except Exception as e:
            self.append_log(f"에러: {e}")
            import traceback
            self.append_log(traceback.format_exc())
            messagebox.showerror("에러", str(e))
    
    def _create_batch_chunks(self, client, jsonl_path, excel_path, model_name, effort, max_size_mb=190, max_requests=500):
        """
        큰 JSONL 파일을 청크로 분할하여 여러 배치를 생성합니다.
        같은 그룹의 배치들은 batch_group_id로 관리됩니다.
        """
        import json
        import uuid
        
        # 배치 그룹 ID 생성 (같은 엑셀에서 분할된 배치들을 묶음)
        batch_group_id = f"group_{uuid.uuid4().hex[:8]}"
        
        # JSONL 파일 읽기 (메모리 효율성을 위해 스트리밍 방식 고려, 하지만 현재는 전체 로드)
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
            # 실제 파일 크기를 정확히 예측하기 위해 JSON 직렬화 + 줄바꿈 문자 고려
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
            # 실제 생성된 청크 수로 표시 (나중에 업데이트될 수 있음)
            self.append_log(f"[INFO] 청크 {chunk_num}: {len(chunk_requests)}개 요청, {chunk_size_mb:.2f} MB")
            
            # 배치 생성 (재시도 로직 포함)
            max_retries = 3
            retry_count = 0
            batch_created = False
            
            while retry_count < max_retries and not batch_created:
                try:
                    self.append_log(f"[INFO] 청크 {chunk_num} 배치 생성 시도 중... (시도 {retry_count + 1}/{max_retries})")
                    batch = create_batch_from_jsonl(
                        client=client,
                        jsonl_path=chunk_jsonl_path,
                        excel_path=excel_path,
                        model_name=model_name,
                        log_func=self.append_log,
                    )
                    
                    batch_id = batch.id
                    batch_ids.append(batch_id)
                    self.append_log(f"✅ 청크 {chunk_num} 배치 생성 완료: {batch_id}")
                    batch_created = True
                    
                    # 작업 이력 기록 (그룹 정보 포함)
                    # total_chunks는 나중에 업데이트되므로 일단 chunk_num으로 설정
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
                        # 실패한 청크 파일은 유지 (수동 재시도 가능하도록)
                        # 하지만 배치 ID는 추가되지 않았으므로 batch_ids에는 포함되지 않음
        
        # 모든 청크 생성 완료 후, total_chunks를 실제 생성된 배치 수로 업데이트
        actual_total_chunks = len(batch_ids)
        if actual_total_chunks > 0:
            self.append_log(f"[INFO] 총 {actual_total_chunks}개 배치 생성 완료. 작업 이력 업데이트 중...")
            # 경쟁 조건 방지를 위해 원자적 업데이트
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
        
        # 메인 런처 현황판 업데이트
        try:
            root_name = get_root_filename(excel_path)
            JobManager.update_status(root_name, text_msg="T2 (진행중)")
            self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T2 (진행중)")
        except Exception:
            pass
        
        self._load_jobs_all()
        self._load_archive_list()
        
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
        ttk.Button(f_ctrl, text="🔄 선택 갱신", command=lambda: self._refresh_selected(self.tree_active)).pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="📥 선택 일괄 병합", command=self._merge_selected, style="Primary.TButton").pack(side='left', padx=10)
        ttk.Button(f_ctrl, text="📊 선택 일괄 분석 리포트", command=self._report_selected_unified, style="Success.TButton").pack(side='left', padx=2)
        ttk.Button(f_ctrl, text="🗑 휴지통 이동", command=self._archive_selected, style="Danger.TButton").pack(side='right', padx=2)
        
        cols = ("batch_id", "status", "created", "completed", "model", "effort", "counts", "group")
        # 계층 구조를 위해 show='tree headings' 사용 (트리 아이콘 + 컬럼 헤더)
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='tree headings', height=15, selectmode='extended')
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF')
        self.tree_active.tag_configure('group', background='#E8F5E9')  # 그룹 배치 강조
        self.tree_active.tag_configure('group_header', background='#C8E6C9', font=("맑은 고딕", 9, "bold"))  # 그룹 헤더 강조
        for c in cols: self.tree_active.heading(c, text=c.capitalize())
        self.tree_active.column("#0", width=250, anchor="w")  # 트리 컬럼 (그룹명 표시)
        self.tree_active.column("batch_id", width=200)
        self.tree_active.column("effort", width=80, anchor="center")
        self.tree_active.column("group", width=120, anchor="center")
        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)
        
        self.menu_active = Menu(self, tearoff=0)
        self.menu_active.add_command(label="상태 갱신", command=lambda: self._refresh_selected(self.tree_active))
        self.menu_active.add_command(label="결과 병합", command=self._merge_selected)
        self.menu_active.add_command(label="분석 리포트 생성", command=self._report_selected_unified)
        self.menu_active.add_separator()
        self.menu_active.add_command(label="휴지통으로 이동", command=self._archive_selected)
        self.tree_active.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_active, self.menu_active))
        self.tree_active.bind("<Double-1>", self._on_tree_double_click)

        # Archive UI
        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 선택 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 선택 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')
        # 계층 구조를 위해 show='tree headings' 사용
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='tree headings', height=15, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2')
        self.tree_arch.tag_configure('group', background='#FFE8E8')  # 그룹 배치 강조
        self.tree_arch.tag_configure('group_header', background='#FFCDD2', font=("맑은 고딕", 9, "bold"))  # 그룹 헤더 강조
        for c in cols: self.tree_arch.heading(c, text=c.capitalize())
        self.tree_arch.column("#0", width=250, anchor="w")  # 트리 컬럼 (그룹명 표시)
        self.tree_arch.column("batch_id", width=200)
        self.tree_arch.column("effort", width=80, anchor="center")
        self.tree_arch.column("group", width=120, anchor="center")
        self.tree_arch.pack(fill='both', expand=True)
        
        self._load_jobs_all()
        self._load_archive_list()

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
            
            # 그룹 헤더 생성 (요약 정보)
            completed_count = statuses.get("completed", 0) + statuses.get("merged", 0)
            status_summary = f"완료: {completed_count}/{total_chunks}"
            if total_requests > 0:
                status_summary += f" | 요청: {total_completed}/{total_requests}"
            
            # 그룹 헤더 (부모 노드)
            group_header_text = f"📦 그룹 {group_id[:8]}... ({total_chunks}개 배치) - {status_summary}"
            group_node = self.tree_active.insert("", "end", 
                text=group_header_text,
                values=("", "", "", "", "", "", "", f"그룹 {total_chunks}개"),
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
                tag = 'group'
                self.tree_active.insert(group_node, "end", 
                    text=f"  └─ {j['batch_id'][:20]}...",
                    values=(
                        j["batch_id"], j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, group_display
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
            tag = 'even'
            self.tree_active.insert("", "end", 
                text=j["batch_id"][:30],
                values=(
                    j["batch_id"], j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, "-"
                ), 
                tags=(tag,))

    def _load_archive_list(self):
        if not hasattr(self, 'tree_arch'): return
        for i in self.tree_arch.get_children(): self.tree_arch.delete(i)
        jobs = load_batch_jobs()
        
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
            
            # 그룹 헤더 생성 (요약 정보)
            completed_count = statuses.get("completed", 0) + statuses.get("merged", 0)
            status_summary = f"완료: {completed_count}/{total_chunks}"
            if total_requests > 0:
                status_summary += f" | 요청: {total_completed}/{total_requests}"
            
            # 그룹 헤더 (부모 노드)
            group_header_text = f"📦 그룹 {group_id[:8]}... ({total_chunks}개 배치) - {status_summary}"
            group_node = self.tree_arch.insert("", "end", 
                text=group_header_text,
                values=("", "", "", "", "", "", "", f"그룹 {total_chunks}개"),
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
                tag = 'group'
                self.tree_arch.insert(group_node, "end", 
                    text=f"  └─ {j['batch_id'][:20]}...",
                    values=(
                        j["batch_id"], j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, group_display
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
            tag = 'even'
            self.tree_arch.insert("", "end", 
                text=j["batch_id"][:30],
                values=(
                    j["batch_id"], j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, "-"
                ), 
                tags=(tag,))

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

    def _run_refresh_ids(self, ids):
        key = self.api_key_var.get().strip()
        client = OpenAI(api_key=key)
        self.append_log(f"선택된 {len(ids)}건 갱신 중...")
        for bid in ids:
            try:
                remote = client.batches.retrieve(bid)
                rc = None
                if remote.request_counts:
                    rc = {"total": remote.request_counts.total, "completed": remote.request_counts.completed, "failed": remote.request_counts.failed}
                upsert_batch_job(bid, status=remote.status, output_file_id=remote.output_file_id, request_counts=rc)
            except Exception as e:
                self.append_log(f"{bid} 갱신 실패: {e}")
        self.after(0, lambda: [self._load_jobs_all(), self._load_archive_list()])
        self.append_log("갱신 완료")

    def _merge_selected(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        jobs = load_batch_jobs()
        
        # 선택된 배치들의 그룹 정보 확인
        selected_jobs = [j for j in jobs if j["batch_id"] in ids]
        group_ids = set()
        for j in selected_jobs:
            group_id = j.get("batch_group_id")
            if group_id:
                group_ids.add(group_id)
        
        # 같은 그룹의 모든 배치를 자동으로 포함
        all_target_ids = set(ids)
        if group_ids:
            for group_id in group_ids:
                group_batches = [j for j in jobs if j.get("batch_group_id") == group_id and j.get("status") == "completed"]
                for j in group_batches:
                    all_target_ids.add(j["batch_id"])
            
            if len(all_target_ids) > len(ids):
                group_info = f"\n\n같은 그룹의 배치 {len(all_target_ids) - len(ids)}개가 자동으로 포함됩니다."
            else:
                group_info = ""
        else:
            group_info = ""
        
        targets = [bid for bid in all_target_ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "completed"]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed' 상태의 작업이 없습니다.")
            return
        
        if messagebox.askyesno("병합", f"총 {len(targets)}건을 병합하시겠습니까?{group_info}"):
            t = threading.Thread(target=self._run_merge_multi, args=(list(targets),))
            t.daemon = True
            t.start()

    def _run_merge_multi(self, ids):
        """
        안정적으로 동작하는 기존 Stage2 Batch 코어(download_batch_output_if_ready + merge_batch_output_to_excel)를
        이용해 선택된 Batch 들에 대해 결과 JSONL 다운로드 + 엑셀 병합을 수행.
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
                
                # 그룹 내 모든 배치의 결과를 임시 JSONL에 수집
                all_output_lines = []
                model_name = first_job.get("model", "gpt-5-mini")
                total_group_cost = 0.0
                
                # 청크 번호 순으로 정렬 (chunk_index가 없는 경우는 맨 뒤로)
                def get_chunk_index(bid):
                    job = next((j for j in jobs if j["batch_id"] == bid), None)
                    if job:
                        idx = job.get("chunk_index")
                        return idx if idx is not None else 999999  # chunk_index가 없으면 맨 뒤로
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
                        
                        base_dir = os.path.dirname(src_path)
                        base_name, _ = os.path.splitext(os.path.basename(src_path))
                        out_jsonl = os.path.join(base_dir, f"{base_name}_stage2_batch_output_{bid}.jsonl")
                        
                        if download_batch_output_if_ready is None:
                            raise RuntimeError("stage2_batch_api_기존gpt 모듈을 찾을 수 없어 Batch 병합 코어를 사용할 수 없습니다.")
                        
                        # 배치 결과 다운로드
                        ok, status = download_batch_output_if_ready(
                            client=client,
                            batch_id=bid,
                            output_jsonl_path=out_jsonl,
                            log_func=self.append_log,
                        )
                        
                        upsert_batch_job(
                            batch_id=bid,
                            status=status,
                            output_jsonl=out_jsonl if ok else local_job.get("output_jsonl", ""),
                        )
                        
                        if not ok or status != "completed":
                            self.append_log(f"  ⚠️ {bid}: 아직 completed 상태가 아니어서 건너뜁니다. (status={status})")
                            continue
                        
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
                    # 실제로 결과를 다운로드한 배치 수 계산
                    downloaded_batch_ids = []
                    for bid in batch_ids_sorted:
                        local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                        if local_job and local_job.get("status") == "completed":
                            out_jsonl = local_job.get("output_jsonl") or os.path.join(
                                os.path.dirname(src_path),
                                f"{os.path.splitext(os.path.basename(src_path))[0]}_stage2_batch_output_{bid}.jsonl"
                            )
                            if os.path.exists(out_jsonl):
                                downloaded_batch_ids.append(bid)
                    
                    if len(downloaded_batch_ids) < expected_total_chunks:
                        missing = expected_total_chunks - len(downloaded_batch_ids)
                        self.append_log(f"⚠️ 그룹 {group_id}: 예상 {expected_total_chunks}개 중 {len(downloaded_batch_ids)}개만 다운로드되었습니다. ({missing}개 누락 가능)")
                
                # 임시 통합 JSONL 파일 생성
                base_dir = os.path.dirname(src_path)
                base_name, _ = os.path.splitext(os.path.basename(src_path))
                merged_jsonl = os.path.join(base_dir, f"{base_name}_stage2_batch_output_merged_{group_id}.jsonl")
                
                with open(merged_jsonl, "w", encoding="utf-8") as f:
                    for line in all_output_lines:
                        f.write(line + "\n")
                
                self.append_log(f"  [그룹] 통합 JSONL 생성: {len(all_output_lines)}개 결과")
                
                # 통합 JSONL을 엑셀에 병합
                if merge_batch_output_to_excel is None:
                    raise RuntimeError("stage2_batch_api_기존gpt 모듈을 찾을 수 없어 Batch 병합 코어를 사용할 수 없습니다.")
                
                info = merge_batch_output_to_excel(
                    excel_path=src_path,
                    output_jsonl_path=merged_jsonl,
                    model_name=model_name,
                    skip_filled=self.skip_exist_var.get(),
                    log_func=self.append_log,
                )
                
                total_group_cost += info.get("total_cost_usd") or 0.0
                total_cost += total_group_cost
                
                # Stage2 최종 파일명: *_T2_... 형식으로 버전 업
                core_out_path = info["out_excel_path"]
                final_out_path = None
                try:
                    df_done = pd.read_excel(core_out_path)
                    
                    # ST2_JSON이 있는 행과 없는 행 분리
                    if "ST2_JSON" in df_done.columns:
                        # ST2_JSON이 비어있거나 None인 행 찾기
                        df_with_st2 = df_done[df_done["ST2_JSON"].notna() & (df_done["ST2_JSON"] != '')].copy()
                        df_no_st2 = df_done[df_done["ST2_JSON"].isna() | (df_done["ST2_JSON"] == '')].copy()
                    else:
                        # 컬럼이 없으면 모든 행이 ST2_JSON 없음으로 처리
                        df_with_st2 = pd.DataFrame()
                        df_no_st2 = df_done.copy()
                    
                    # ST2_JSON이 없는 행들을 T2-2(실패) 버전으로 별도 파일 저장
                    no_st2_path = None
                    if len(df_no_st2) > 0:
                        base_dir = os.path.dirname(src_path)
                        base_name, ext = os.path.splitext(os.path.basename(src_path))
                        
                        # 현재 파일명에서 버전 정보 추출 (예: _T1_I0)
                        # T2-2(실패) 버전으로 변경
                        name_only_clean = re.sub(r"\([^)]*\)", "", base_name)  # 기존 괄호 제거
                        all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)", name_only_clean, re.IGNORECASE))
                        
                        if all_matches:
                            # 마지막 버전 패턴 사용
                            match = all_matches[-1]
                            original_name = name_only_clean[: match.start()].rstrip("_")
                            current_i = int(match.group(4))
                            # T2-2(실패) 버전으로 생성
                            new_filename = f"{original_name}_T2-2(실패)_I{current_i}{ext}"
                        else:
                            # 버전 패턴이 없으면 기본적으로 T2-2(실패)_I0로 생성
                            new_filename = f"{base_name}_T2-2(실패)_I0{ext}"
                        
                        no_st2_path = os.path.join(base_dir, new_filename)
                        df_no_st2.to_excel(no_st2_path, index=False)
                        
                        self.append_log(f"  [그룹] T2-2(실패) 분리 파일: {os.path.basename(no_st2_path)} ({len(df_no_st2)}개 행)")
                        self.append_log(f"         ※ 이 파일은 T2-1 단계까지만 작업 가능합니다.")
                        
                        # 분리된 파일의 런처 상태 업데이트
                        try:
                            no_st2_root_name = get_root_filename(no_st2_path)
                            JobManager.update_status(no_st2_root_name, text_msg="T2-2(실패)")
                            self.append_log(f"[Launcher] 분리 파일 상태 업데이트: {no_st2_root_name} -> T2-2(실패)")
                        except Exception as e:
                            self.append_log(f"[Launcher] 분리 파일 상태 업데이트 실패: {e}")
                    
                    # ST2_JSON이 있는 행들만 저장
                    if len(df_with_st2) > 0:
                        df_done = df_with_st2
                    else:
                        self.append_log(f"  ⚠️ 그룹 {group_id}: ST2_JSON이 있는 행이 없습니다.")
                    
                    final_out_path = get_next_version_path(src_path, task_type="text")
                    
                    if safe_save_excel(df_done, final_out_path):
                        info["out_excel_path"] = final_out_path
                        # T2 버전 파일 저장 성공 시, 코어가 생성한 중간 파일 삭제
                        if core_out_path != final_out_path and os.path.exists(core_out_path):
                            try:
                                os.remove(core_out_path)
                                self.append_log(f"[INFO] 중간 파일 삭제: {os.path.basename(core_out_path)}")
                            except Exception as e:
                                self.append_log(f"[WARN] 중간 파일 삭제 실패: {e}")
                    else:
                        final_out_path = core_out_path
                except Exception as e:
                    final_out_path = core_out_path
                    self.append_log(f"[WARN] T2 버전 파일 저장 중 오류: {e}")
                
                # 그룹 내 모든 배치를 merged 상태로 업데이트
                for bid in batch_ids:
                    upsert_batch_job(
                        batch_id=bid,
                        out_excel=final_out_path,
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
                            stage="Stage 2 Batch (Grouped)",
                            model_name=model_name,
                            reasoning_effort=first_job.get("effort", "medium"),
                            src_file=src_path,
                            out_file=final_out_path,
                            total_rows=info["total_rows"],
                            api_rows=info["merged"],
                            elapsed_seconds=elapsed,
                            total_in_tok=info["total_in_tok"],
                            total_out_tok=info["total_out_tok"],
                            total_reasoning_tok=info["total_reasoning_tok"],
                            input_cost_usd=info["input_cost_usd"],
                            output_cost_usd=info["output_cost_usd"],
                            total_cost_usd=total_group_cost,
                            start_dt=c_at,
                            finish_dt=finish_dt,
                            api_type="batch",
                            batch_id=f"{group_id} ({len(batch_ids)} batches)",
                            success_rows=info["merged"],
                            fail_rows=info["missing"],
                        )
                except Exception as e:
                    self.append_log(f"[WARN] 실행 이력 기록 실패: {e}")
                
                # 메인 런처 현황판 업데이트
                try:
                    root_name = get_root_filename(src_path)
                    JobManager.update_status(root_name, text_msg="T2-2(분석완료)")
                    self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T2-2(분석완료)")
                except Exception as e:
                    self.append_log(f"[WARN] 현황판 연동 실패: {e}")
                
                self.append_log(f"✅ 그룹 병합 완료: {os.path.basename(final_out_path)} ({len(batch_ids)}개 배치)")
                success_cnt += 1
                
            except Exception as e:
                self.append_log(f"❌ 그룹 {group_id} 병합 실패: {e}")
                import traceback
                self.append_log(traceback.format_exc())
        
        # 그룹 없는 배치 개별 병합 (기존 로직)
        for bid in ungrouped_batches:
            self.append_log(f"--- 병합 시작: {bid} ---")
            try:
                jobs = load_batch_jobs()
                local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                if not local_job:
                    self.append_log(f"❌ {bid} 병합 실패: 작업 이력을 찾을 수 없습니다.")
                    continue

                # 이미 병합된 배치는 건너뛰기 (중복 병합 방지)
                if local_job.get("status") == "merged":
                    self.append_log(f"⏭️ {bid}: 이미 병합 완료된 작업입니다. 건너뜁니다.")
                    continue

                src_path = local_job.get("src_excel") or ""
                if not src_path or not os.path.exists(src_path):
                    self.append_log(f"⚠️ {bid}: 원본 엑셀 경로가 없거나 존재하지 않아 건너뜁니다.")
                    continue

                base_dir = os.path.dirname(src_path)
                base_name, _ = os.path.splitext(os.path.basename(src_path))
                out_jsonl = os.path.join(base_dir, f"{base_name}_stage2_batch_output.jsonl")

                if download_batch_output_if_ready is None or merge_batch_output_to_excel is None:
                    raise RuntimeError("stage2_batch_api_기존gpt 모듈을 찾을 수 없어 Batch 병합 코어를 사용할 수 없습니다.")

                # 1) Batch 결과 JSONL 다운로드
                ok, status = download_batch_output_if_ready(
                    client=client,
                    batch_id=bid,
                    output_jsonl_path=out_jsonl,
                    log_func=self.append_log,
                )

                upsert_batch_job(
                    batch_id=bid,
                    status=status,
                    output_jsonl=out_jsonl if ok else local_job.get("output_jsonl", ""),
                )

                if not ok or status != "completed":
                    self.append_log(f"⚠️ {bid}: 아직 completed 상태가 아니어서 병합을 건너뜁니다. (status={status})")
                    continue

                # 2) JSONL → 엑셀 병합 + 비용/토큰 계산 (기존 코어 사용)
                model_name = local_job.get("model", "gpt-5-mini")
                info = merge_batch_output_to_excel(
                    excel_path=src_path,
                    output_jsonl_path=out_jsonl,
                    model_name=model_name,
                    skip_filled=self.skip_exist_var.get(),
                    log_func=self.append_log,
                )

                total_cost += info.get("total_cost_usd") or 0.0

                # 3) Stage2 최종 파일명: *_T2_... 형식으로 버전 업
                core_out_path = info["out_excel_path"]
                final_out_path = None
                try:
                    # 코어가 만든 완료 파일을 다시 읽어와서 T2 버전 파일로 저장
                    df_done = pd.read_excel(core_out_path)
                    
                    # ST2_JSON이 있는 행과 없는 행 분리
                    if "ST2_JSON" in df_done.columns:
                        # ST2_JSON이 비어있거나 None인 행 찾기
                        df_with_st2 = df_done[df_done["ST2_JSON"].notna() & (df_done["ST2_JSON"] != '')].copy()
                        df_no_st2 = df_done[df_done["ST2_JSON"].isna() | (df_done["ST2_JSON"] == '')].copy()
                    else:
                        # 컬럼이 없으면 모든 행이 ST2_JSON 없음으로 처리
                        df_with_st2 = pd.DataFrame()
                        df_no_st2 = df_done.copy()
                    
                    # ST2_JSON이 없는 행들을 T2-2(실패) 버전으로 별도 파일 저장
                    no_st2_path = None
                    if len(df_no_st2) > 0:
                        base_dir = os.path.dirname(src_path)
                        base_name, ext = os.path.splitext(os.path.basename(src_path))
                        
                        # 현재 파일명에서 버전 정보 추출 (예: _T1_I0)
                        # T2-2(실패) 버전으로 변경
                        name_only_clean = re.sub(r"\([^)]*\)", "", base_name)  # 기존 괄호 제거
                        all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)", name_only_clean, re.IGNORECASE))
                        
                        if all_matches:
                            # 마지막 버전 패턴 사용
                            match = all_matches[-1]
                            original_name = name_only_clean[: match.start()].rstrip("_")
                            current_i = int(match.group(4))
                            # T2-2(실패) 버전으로 생성
                            new_filename = f"{original_name}_T2-2(실패)_I{current_i}{ext}"
                        else:
                            # 버전 패턴이 없으면 기본적으로 T2-2(실패)_I0로 생성
                            new_filename = f"{base_name}_T2-2(실패)_I0{ext}"
                        
                        no_st2_path = os.path.join(base_dir, new_filename)
                        df_no_st2.to_excel(no_st2_path, index=False)
                        
                        self.append_log(f"  T2-2(실패) 분리 파일: {os.path.basename(no_st2_path)} ({len(df_no_st2)}개 행)")
                        self.append_log(f"  ※ 이 파일은 T2-1 단계까지만 작업 가능합니다.")
                        
                        # 분리된 파일의 런처 상태 업데이트
                        try:
                            no_st2_root_name = get_root_filename(no_st2_path)
                            JobManager.update_status(no_st2_root_name, text_msg="T2-2(실패)")
                            self.append_log(f"[Launcher] 분리 파일 상태 업데이트: {no_st2_root_name} -> T2-2(실패)")
                        except Exception as e:
                            self.append_log(f"[Launcher] 분리 파일 상태 업데이트 실패: {e}")
                    
                    # ST2_JSON이 있는 행들만 저장
                    if len(df_with_st2) > 0:
                        df_done = df_with_st2
                    else:
                        self.append_log(f"⚠️ {bid}: ST2_JSON이 있는 행이 없습니다.")
                    
                    final_out_path = get_next_version_path(src_path, task_type="text")

                    if safe_save_excel(df_done, final_out_path):
                        info["out_excel_path"] = final_out_path
                        # T2 버전 파일 저장 성공 시, 코어가 생성한 중간 파일(_stage2_batch_완료) 삭제
                        if core_out_path != final_out_path and os.path.exists(core_out_path):
                            try:
                                os.remove(core_out_path)
                                self.append_log(f"[INFO] 중간 파일 삭제: {os.path.basename(core_out_path)}")
                            except Exception as e:
                                self.append_log(f"[WARN] 중간 파일 삭제 실패: {e}")
                    else:
                        # 저장 실패 시, 코어 완료 파일을 그대로 사용
                        final_out_path = core_out_path
                except Exception as e:
                    final_out_path = core_out_path
                    self.append_log(f"[WARN] T2 버전 파일 저장 중 오류: {e}")

                upsert_batch_job(
                    batch_id=bid,
                    out_excel=final_out_path,
                    status="merged",
                )

                # 실행 이력 기록 (naive datetime 기준)
                try:
                    c_at_str = local_job.get("created_at", "")
                    if c_at_str:
                        c_at = datetime.fromisoformat(c_at_str)
                    else:
                        c_at = datetime.now()
                    finish_dt = datetime.now()
                    elapsed = (finish_dt - c_at).total_seconds()

                    append_run_history(
                        stage="Stage 2 Batch",
                        model_name=model_name,
                        reasoning_effort=local_job.get("effort", "medium"),
                        src_file=src_path,
                        out_file=info["out_excel_path"],
                        total_rows=info["total_rows"],
                        api_rows=info["merged"],
                        elapsed_seconds=elapsed,
                        total_in_tok=info["total_in_tok"],
                        total_out_tok=info["total_out_tok"],
                        total_reasoning_tok=info["total_reasoning_tok"],
                        input_cost_usd=info["input_cost_usd"],
                        output_cost_usd=info["output_cost_usd"],
                        total_cost_usd=info["total_cost_usd"],
                        start_dt=c_at,
                        finish_dt=finish_dt,
                        api_type="batch",
                        batch_id=bid,
                        success_rows=info["merged"],
                        fail_rows=info["missing"],
                    )
                except Exception as e:
                    self.append_log(f"[WARN] 실행 이력 기록 실패: {e}")

                # 메인 런처 현황판에 Stage2(Text) 완료 상태 기록: T2-2(분석완료) (img 상태는 변경하지 않음)
                try:
                    root_name = get_root_filename(src_path)
                    JobManager.update_status(root_name, text_msg="T2-2(분석완료)")
                    self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T2-2(분석완료)")
                except Exception as e:
                    self.append_log(f"[WARN] 현황판 연동 실패: {e}")

                self.append_log(f"✅ 병합 완료: {os.path.basename(final_out_path)}")
                success_cnt += 1
            except Exception as e:
                self.append_log(f"❌ {bid} 병합 실패: {e}")

        self.append_log(f"=== 일괄 병합 끝 (성공: {success_cnt}, 총 비용 추정: ${total_cost:.4f}) ===")
        self._load_jobs_all()
        messagebox.showinfo("완료", f"{success_cnt}건 병합 완료.\n총 비용(추정): ${total_cost:.4f}")

    def _report_selected_unified(self):
        ids = self._get_selected_ids(self.tree_active)
        if not ids: return
        jobs = load_batch_jobs()
        targets = [bid for bid in ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") == "merged"]
        if not targets:
            messagebox.showinfo("알림", "상태가 'merged'인 작업이 없습니다.")
            return
        if messagebox.askyesno("리포트", f"선택한 {len(targets)}건의 JSON 분석 리포트를 생성합니까?"):
            t = threading.Thread(target=self._run_report_unified, args=(targets,))
            t.daemon = True
            t.start()

    def _run_report_unified(self, ids):
        self.append_log(f"--- JSON 분석 리포트 생성 ({len(ids)}건) ---")
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
                if "ST2_JSON" not in df.columns: continue
                for idx, row in df.iterrows():
                    st2 = safe_str(row.get("ST2_JSON", ""))
                    parsed = "❌ 실패"
                    kw_cnt = 0
                    if st2.strip().startswith("{"):
                        try:
                            js = json.loads(st2)
                            kw_cnt = len(js.get("search_keywords", []))
                            parsed = "✅ 성공"
                        except: pass
                    
                    all_reps.append({
                        "Batch_ID": bid,
                        "행번호": idx+2,
                        "상품코드": safe_str(row.get("상품코드", "")),
                        "JSON상태": parsed,
                        "키워드수": kw_cnt
                    })
            except: pass

        if not all_reps:
            messagebox.showinfo("알림", "데이터 없음")
            return

        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = os.path.join(os.path.dirname(__file__), f"Stage2_Analysis_Report_{ts}.xlsx")
            pd.DataFrame(all_reps).to_excel(path, index=False)
            self.append_log(f"📊 리포트 완료: {os.path.basename(path)}")
            if messagebox.askyesno("완료", "파일을 여시겠습니까?"): os.startfile(path)
        except Exception as e: messagebox.showerror("오류", str(e))

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
    app = Stage2BatchGUI()
    app.mainloop()