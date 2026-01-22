# stage2_llm_gui.py
import os
import re
import time
import threading
import base64
import mimetypes
import subprocess
import json
from datetime import datetime

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI
from stage2_core import (
    safe_str,
    build_stage2_request_from_row,  # ✅ 행 → Stage2Request (프롬프트 + 이미지 경로)
)
from stage2_run_history import append_run_history  # ✅ Stage2 실행 이력 기록

API_KEY_FILE = ".openai_api_key_stage2_llm"

# =========================================================
# [런처 연동] JobManager & 유틸 (표준화됨)
# =========================================================
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

class JobManager:
    DB_FILE = None

    @classmethod
    def find_db_path(cls):
        if cls.DB_FILE and os.path.exists(cls.DB_FILE): return cls.DB_FILE
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        search_dirs = [
            current_dir,
            os.path.abspath(os.path.join(current_dir, "..")), 
            os.path.abspath(os.path.join(current_dir, "..", ".."))
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
        if not os.path.exists(db_path): return {}
        try:
            with open(db_path, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None):
        """런처 현황판 상태 업데이트"""
        db_path = cls.find_db_path()
        data = cls.load_jobs()
        now = datetime.now().strftime("%m-%d %H:%M")
        
        # 파일명 Key로 사용 (확장자 포함 or 제외 통일 필요, 여기선 get_root_filename 결과 사용)
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
            with open(db_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[JobManager Error] {e}")

# =======================
#  모델별 가격 (USD)
#  - 단위: 100만(1,000,000) 토큰당 가격
#  - reasoning 토큰은 출력 단가로 계산
# =======================
MODEL_PRICING_USD_PER_MTOK = {
    "gpt-5": {
        "input": 1.25,   # $1.25 / 1M input tokens
        "output": 10.0,  # $10.00 / 1M output+reasoning tokens
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

# =======================
#  서울 시간 헬퍼
# =======================
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def get_seoul_now() -> datetime:
    """가능하면 Asia/Seoul 기준 현재 시각, 실패하면 로컬 현재 시각."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Asia/Seoul"))
        except Exception:
            pass
    return datetime.now()


# =========================================================
#  간단 Tooltip 구현 (마우스 오버 시 도움말 팝업)
# =========================================================
class ToolTip:
    def __init__(self, widget, text: str, wraplength: int = 420):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tipwindow: tk.Toplevel | None = None

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

        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            wraplength=self.wraplength,
        )
        label.pack(ipadx=4, ipady=2)

    def hide_tip(self, event=None):
        tw = self.tipwindow
        if tw is not None:
            tw.destroy()
            self.tipwindow = None


# =========================================================
#  이미지 → data URL 변환 (로컬 이미지를 직접 보내기)
# =========================================================
def encode_image_to_data_url(path: str) -> str:
    """로컬 이미지 파일을 data:[mime];base64,... 형태 문자열로 변환."""
    mime, _ = mimetypes.guess_type(path)
    if mime is None:
        mime = "image/jpeg"
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"


def load_api_key_from_file() -> str:
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""


def save_api_key_to_file(key: str) -> None:
    try:
        with open(API_KEY_FILE, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")


# =========================================================
#  이미지 경로 리스트 → 이미지 input 노드 리스트
# =========================================================
def build_image_nodes_from_paths(paths, log_func=None, cache=None, allow_url: bool = True):
    """
    이미지 경로 리스트 → Responses API용 input_image 노드 리스트.
    (http/https URL 또는 로컬 파일 경로 모두 지원)
    cache: {경로: data_url} 캐시 딕셔너리 (있으면 재사용)
    allow_url: False면 http/https URL은 모두 스킵하고 로컬 파일만 사용.
    """
    image_nodes = []
    seen = set()  # 같은 행에서 동일 이미지 중복 업로드 방지

    for raw_p in paths:
        p = safe_str(raw_p)
        if not p:
            continue
        if p in seen:
            continue
        seen.add(p)

        lower = p.lower()

        # 1) http/https면 URL 사용 여부에 따라 처리
        if lower.startswith("http://") or lower.startswith("https://"):
            if not allow_url:
                msg = f"[INFO] URL 이미지 사용 안 함 설정으로 스킵: {p}"
                if log_func:
                    log_func(msg)
                else:
                    print(msg)
                continue

            image_nodes.append(
                {
                    "type": "input_image",
                    "image_url": p,  # 🔵 문자열 URL 그대로
                }
            )
            continue

        # 2) 로컬 파일이면 data URL로 변환
        if os.path.exists(p):
            try:
                if cache is not None and p in cache:
                    data_url = cache[p]
                else:
                    data_url = encode_image_to_data_url(p)
                    if cache is not None:
                        cache[p] = data_url

                image_nodes.append(
                    {
                        "type": "input_image",
                        "image_url": data_url,  # 🔵 data:...;base64,... 문자열 그대로
                    }
                )
            except Exception as e:
                msg = f"[WARN] 이미지 인코딩 실패: {p} ({e})"
                if log_func:
                    log_func(msg)
                else:
                    print(msg)
        else:
            msg = f"[WARN] 이미지 파일을 찾을 수 없습니다: {p}"
            if log_func:
                log_func(msg)
            else:
                print(msg)

    return image_nodes


# (호환용) 엑셀 한 행 → 이미지 input 노드 리스트
def build_image_nodes_from_row(row, detail_cols, log_func=None, cache=None, allow_url: bool = True):
    """
    엑셀 한 행에서 상세이미지 경로들을 뽑아서
    build_image_nodes_from_paths()로 넘긴다.
    (기존 시그니처 유지용 헬퍼)
    """
    paths = []
    for col in detail_cols:
        raw = row.get(col, "")
        p = safe_str(raw)
        if p:
            paths.append(p)

    return build_image_nodes_from_paths(paths, log_func=log_func, cache=cache, allow_url=allow_url)


# =========================================================
#  메인 GUI 클래스
# =========================================================
class Stage2LLMApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage2 LLM 실행기 (엑셀 → 프롬프트+이미지 → ST2_JSON)")
        self.geometry("960x720")

        # 상태 변수
        self.api_key_var = tk.StringVar(value=load_api_key_from_file())
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="medium")   # reasoning medium 모드 기본값
        self.src_excel_var = tk.StringVar(value="")
        self.skip_filled_var = tk.BooleanVar(value=True)

        # 이미지 옵션
        self.use_thumbnail_var = tk.BooleanVar(value=False)   # 대표 썸네일 사용 여부 (기본값: 해제)
        self.allow_url_var = tk.BooleanVar(value=False)      # URL 이미지 사용 여부
        self.max_detail_images_var = tk.IntVar(value=10)  # 기본값: 상세이미지 10개까지만 사용
        
        # 상세이미지 통계 정보 저장
        self.detail_image_stats = {
            "max_columns": 0,  # 엑셀에 있는 최대 상세이미지 컬럼 개수
            "row_counts": {},  # {컬럼개수: 행개수} 예: {15: 5, 20: 3}
        }

        # 진행/상태 표시용
        self.progress_var = tk.DoubleVar(value=0.0)
        self.progress_text_var = tk.StringVar(value="진행률: 0% (0 / 0)")
        self.time_info_var = tk.StringVar(value="시작: -   종료: -   경과: -")
        self.token_info_var = tk.StringVar(value="토큰: -")
        self.cost_info_var = tk.StringVar(value="비용: -")
        self.summary_info_var = tk.StringVar(
            value="처리 요약: 전체=0, 처리행=0, 성공=0, 건너뜀=0, 실패=0"
        )

        self._worker_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # ✅ 이미지 base64 캐시 (같은 파일 여러 번 인코딩 방지)
        self._image_cache: dict[str, str] = {}

        # ✅ 시작 시각(서울 기준)
        self._start_dt: datetime | None = None

        self._build_widgets()

    # ---------------- UI 구성 ----------------
    def _build_widgets(self):
        # ===== 공통 스타일 =====
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("Title.TLabel", font=("맑은 고딕", 14, "bold"))
        style.configure("SmallGray.TLabel", font=("맑은 고딕", 9), foreground="#666666")

        # 초록색 Progressbar 스타일
        style.configure(
            "green.Horizontal.TProgressbar",
            troughcolor="#f0f0f0",
            bordercolor="#f0f0f0",
            background="#4caf50",
            lightcolor="#81c784",
            darkcolor="#388e3c",
        )

        # ===== 상단 타이틀/서브타이틀 =====
        header = ttk.Label(
            self,
            text="Stage2 LLM 실행기",
            style="Title.TLabel",
        )
        header.pack(fill="x", padx=10, pady=(8, 0))

        sub_header = ttk.Label(
            self,
            text="엑셀 메타 + 이미지 → Stage2 프롬프트 자동 생성 → ST2_JSON 채우기",
            style="SmallGray.TLabel",
        )
        sub_header.pack(fill="x", padx=10, pady=(0, 8))

        # 🔹 툴팁에 들어갈 사용 안내 텍스트
        help_text = (
            "[Stage2 LLM 실행기 사용법]\n\n"
            "- 입력 엑셀 예시:\n"
            "  '*_stage1_mapping_stage1_completed_with_detail_images.xlsx'\n"
            "  (Stage1 정제명 + 상세이미지 매핑까지 완료된 파일)\n\n"
            "- 주요 메타 컬럼(예시):\n"
            "  * 원본상품명, ST1_결과상품명, 카테고리명, 옵션1값, 키워드 등\n\n"
            "- 이미지 컬럼:\n"
            "  * 이미지대: 대표 썸네일(URL 또는 로컬 경로)\n"
            "  * 상세이미지_1 ~ 상세이미지_N : 로컬 이미지 경로 또는 URL\n"
            "  * 존재하는 모든 상세이미지를 LLM에 전달하며, 같은 경로는 자동 중복 제거.\n\n"
            "- 결과 컬럼:\n"
            "  * ST2_JSON : LLM 결과 JSON이 저장될 컬럼 (없으면 자동 생성)\n\n"
            "- Reasoning Effort:\n"
            "  * none : reasoning 미사용 (가장 빠르고 저렴, 기본 추천)\n"
            "  * low/medium/high : 점점 더 깊게 사고하지만 비용/시간 증가\n"
        )

        # ===== API 설정 =====
        frame_api = ttk.LabelFrame(self, text="OpenAI API 설정")
        frame_api.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_api, text="API Key:").grid(row=0, column=0, sticky="w", padx=5, pady=3)
        entry_key = ttk.Entry(frame_api, textvariable=self.api_key_var, width=50, show="*")
        entry_key.grid(row=0, column=1, sticky="w", padx=5, pady=3)
        btn_save = ttk.Button(frame_api, text="키 저장", command=self.on_save_api_key)
        btn_save.grid(row=0, column=2, sticky="w", padx=5, pady=3)

        # 도움말 아이콘
        help_icon = ttk.Label(
            frame_api,
            text="❓사용법",
            foreground="blue",
            cursor="question_arrow",
        )
        help_icon.grid(row=0, column=3, sticky="e", padx=5, pady=3)
        ToolTip(help_icon, help_text)

        ttk.Label(frame_api, text="모델:").grid(row=1, column=0, sticky="w", padx=5, pady=3)
        combo_model = ttk.Combobox(
            frame_api,
            textvariable=self.model_var,
            values=["gpt-5", "gpt-5-mini", "gpt-5-nano"],
            state="readonly",
            width=20,
        )
        combo_model.grid(row=1, column=1, sticky="w", padx=5, pady=3)

        ttk.Label(frame_api, text="Reasoning Effort:").grid(row=2, column=0, sticky="w", padx=5, pady=3)
        combo_effort = ttk.Combobox(
            frame_api,
            textvariable=self.effort_var,
            values=["none", "low", "medium", "high"],
            state="readonly",
            width=20,
        )
        combo_effort.grid(row=2, column=1, sticky="w", padx=5, pady=3)

        # ===== 파일 선택 + 이미지 옵션 =====
        frame_file = ttk.LabelFrame(self, text="입력 엑셀(Stage2 메타 + 이미지 설정)")
        frame_file.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_file, text="입력 엑셀:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        entry_src = ttk.Entry(frame_file, textvariable=self.src_excel_var, width=70)
        entry_src.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        btn_browse = ttk.Button(frame_file, text="찾기...", command=self.on_browse_excel)
        btn_browse.grid(row=0, column=2, sticky="w", padx=5, pady=5)

        chk_skip = ttk.Checkbutton(
            frame_file,
            text="이미 ST2_JSON 값이 있는 행은 건너뛰기",
            variable=self.skip_filled_var,
        )
        chk_skip.grid(row=1, column=1, sticky="w", padx=5, pady=3)

        chk_thumb = ttk.Checkbutton(
            frame_file,
            text="대표 썸네일(이미지대)도 LLM 이미지로 사용",
            variable=self.use_thumbnail_var,
        )
        chk_thumb.grid(row=2, column=1, sticky="w", padx=5, pady=2)

        ToolTip(
            chk_thumb,
            "- 체크: 엑셀의 '이미지대' 컬럼 값(대표 썸네일)을 이미지로 함께 전송합니다.\n"
            "- 해제: '이미지대' 컬럼 값은 Stage2 이미지에서 제외합니다.\n\n"
            "※ 이미지대가 URL(http/https)이면 OpenAI 서버가 직접 이미지를 다운로드하려 시도합니다."
        )

        chk_url = ttk.Checkbutton(
            frame_file,
            text="URL 이미지 사용 허용 (http/https)",
            variable=self.allow_url_var,
        )
        chk_url.grid(row=3, column=1, sticky="w", padx=5, pady=2)

        ToolTip(
            chk_url,
            "- 체크: http:// 또는 https:// 로 시작하는 이미지 URL도 그대로 LLM에 전송합니다.\n"
            "- 해제: URL 이미지는 모두 스킵하고 로컬 파일만 사용합니다.\n\n"
            "※ 외부 서버가 느리거나 막힌 경우 URL 사용을 끄면 타임아웃 에러를 줄일 수 있습니다."
        )
        
        # 상세이미지 개수 제한 옵션
        ttk.Label(frame_file, text="상세이미지 개수 제한:").grid(row=4, column=0, sticky="w", padx=5, pady=5)
        spin_max_detail = ttk.Spinbox(frame_file, from_=1, to=100, textvariable=self.max_detail_images_var, width=10)
        spin_max_detail.grid(row=4, column=1, sticky="w", padx=5, pady=5)
        ToolTip(spin_max_detail, "상세이미지_1부터 지정한 개수까지만 사용합니다.\n예: 10으로 설정하면 상세이미지_1~상세이미지_10까지만 사용합니다.\n토큰 사용량을 줄이기 위한 옵션입니다.")
        ttk.Label(frame_file, text="개 (기본값: 10)", font=("맑은 고딕", 9)).grid(row=4, column=1, sticky="w", padx=(120, 0), pady=5)
        
        # 상세이미지 통계 표시
        self.detail_stats_frame = tk.Frame(frame_file, bg="#E3F2FD", relief="solid", bd=1, padx=10, pady=8)
        self.detail_stats_frame.grid(row=5, column=0, columnspan=3, sticky="ew", padx=5, pady=5)
        frame_file.columnconfigure(1, weight=1)
        
        self.detail_stats_label = tk.Label(self.detail_stats_frame, 
                                           text="📊 엑셀 파일을 선택하면 상세이미지 통계가 표시됩니다.", 
                                           font=("맑은 고딕", 10, "bold"), 
                                           bg="#E3F2FD",
                                           fg="#1976D2",
                                           anchor="w",
                                           justify="left")
        self.detail_stats_label.pack(side='left', fill='x', expand=True)
        
        # 통계 상세 확인 버튼
        btn_stats = ttk.Button(self.detail_stats_frame, text="📊 통계 상세 확인", command=self._show_detail_stats_popup, width=18)
        btn_stats.pack(side='right', padx=(10, 0))
        ToolTip(btn_stats, "상세이미지 통계를 팝업 창으로 확인합니다.")
        
        # Spinbox 값 변경 시 통계 업데이트
        self.max_detail_images_var.trace('w', lambda *args: self._update_detail_stats_display())

        # ===== 진행 상태 =====
        frame_progress = ttk.LabelFrame(self, text="진행 상태")
        frame_progress.pack(fill="x", padx=10, pady=5)

        self.progress_bar = ttk.Progressbar(
            frame_progress,
            variable=self.progress_var,
            maximum=100.0,
            mode="determinate",
            length=500,
            style="green.Horizontal.TProgressbar",
        )
        self.progress_bar.grid(row=0, column=0, columnspan=2, sticky="we", padx=10, pady=5)

        lbl_progress = ttk.Label(frame_progress, textvariable=self.progress_text_var)
        lbl_progress.grid(row=1, column=0, sticky="w", padx=10, pady=2)

        lbl_time = ttk.Label(frame_progress, textvariable=self.time_info_var)
        lbl_time.grid(row=1, column=1, sticky="e", padx=10, pady=2)

        lbl_summary = ttk.Label(
            frame_progress,
            textvariable=self.summary_info_var,
            style="SmallGray.TLabel",
        )
        lbl_summary.grid(row=2, column=0, columnspan=2, sticky="w", padx=10, pady=2)

        lbl_tokens = ttk.Label(frame_progress, textvariable=self.token_info_var)
        lbl_tokens.grid(row=3, column=0, columnspan=2, sticky="w", padx=10, pady=2)

        lbl_cost = ttk.Label(frame_progress, textvariable=self.cost_info_var)
        lbl_cost.grid(row=4, column=0, columnspan=2, sticky="w", padx=10, pady=2)

        # ===== 실행 버튼 =====
        frame_run = ttk.Frame(self)
        frame_run.pack(fill="x", padx=10, pady=5)

        self.btn_start = ttk.Button(
            frame_run,
            text="Stage2 LLM 실행 (프롬프트 자동 생성 + 이미지)",
            command=self.on_start,
        )
        self.btn_start.pack(side="left", padx=5, pady=3)

        self.btn_stop = ttk.Button(
            frame_run,
            text="중단 요청",
            command=self.on_stop,
            state="disabled",
        )
        self.btn_stop.pack(side="left", padx=5, pady=3)

        # ===== 로그 =====
        frame_log = ttk.LabelFrame(self, text="로그")
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_widget = ScrolledText(frame_log, height=15)
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)

    # ---------------- 유틸 ----------------
    def append_log(self, msg: str):
        def _do():
            self.log_widget.insert("end", msg + "\n")
            self.log_widget.see("end")

        self.after(0, _do)

    def _save_df_with_backup(self, df: pd.DataFrame, excel_path: str) -> str:
        """
        엑셀 저장 시 파일이 열려 있어서 실패하는 경우,
        *_stage2_partial_타임스탬프.xlsx 로 백업 저장.
        반환값: 실제로 저장에 성공한 파일 경로 (T1 → T2로 버전 업)
        """
        # T1 → T2로 버전 업 파일명 생성
        base_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        
        # 입력 파일명에서 버전 정보 추출 (괄호 포함 가능, 예: _I5(업완))
        pattern = r"_T(\d+)_I(\d+)(\([^)]+\))?"
        match = re.search(pattern, base_name, re.IGNORECASE)
        if match:
            current_t = int(match.group(1))
            current_i = int(match.group(2))
            i_suffix = match.group(3) or ""  # 괄호 부분이 있으면 유지 (예: (업완))
            # 원본명 추출 (버전 정보 제거, 괄호 포함)
            original_name = re.sub(r"_T\d+_I\d+(\([^)]+\))?.*$", "", base_name, flags=re.IGNORECASE).rstrip("_")
            # T 버전만 +1 (I는 유지, 괄호도 유지)
            new_t = current_t + 1
            new_i = current_i
            out_filename = f"{original_name}_T{new_t}_I{new_i}{i_suffix}.xlsx"
        else:
            # 버전 정보가 없으면 T2_I0으로 생성
            out_filename = f"{base_name}_T2_I0.xlsx"
        out_path = os.path.join(base_dir, out_filename)
        
        try:
            df.to_excel(out_path, index=False)
            self.append_log(f"[DONE] 엑셀 저장 완료 (ST2_JSON 포함): {out_path}")
            return out_path
        except Exception as e:
            base, ext = os.path.splitext(out_path)
            ts = get_seoul_now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{base}_stage2_partial_{ts}{ext}"
            try:
                df.to_excel(backup_path, index=False)
                self.append_log(
                    f"[WARN] 원본 엑셀 저장 실패(열려있을 수 있음): {e}\n"
                    f"       대신 임시 파일로 저장: {backup_path}"
                )
                return backup_path
            except Exception as e2:
                self.append_log(
                    f"[FATAL] 엑셀 저장에 완전히 실패했습니다. ({e2})\n"
                    f"       현재 DataFrame이 메모리에만 존재합니다."
                )
                return out_path  # 그래도 원래 경로 리턴

    def _open_file(self, path: str) -> None:
        """완료 후 결과 엑셀 열기용 헬퍼."""
        if not path or not os.path.exists(path):
            messagebox.showwarning("경고", f"파일을 찾을 수 없습니다:\n{path}")
            return

        try:
            if os.name == "nt":
                os.startfile(path)  # type: ignore[attr-defined]
            elif os.name == "posix":
                if "darwin" in os.sys.platform:
                    subprocess.run(["open", path], check=False)
                else:
                    subprocess.run(["xdg-open", path], check=False)
            else:
                messagebox.showinfo(
                    "알림",
                    f"이 OS에서는 자동 열기가 지원되지 않을 수 있습니다.\n\n{path}",
                )
        except Exception as e:
            messagebox.showerror("오류", f"파일을 여는 중 오류가 발생했습니다.\n\n{e}")

    def _show_completion_dialog(
        self,
        saved_path: str,
        total_rows: int,
        done_rows: int,
        success: int,
        skipped: int,
        failed: int,
        total_tokens: int,
        total_cost_usd: float | None,
    ) -> None:
        """작업 완료 후 요약 팝업 + 엑셀 열기 여부 확인."""
        def _do():
            msg = (
                "Stage2 LLM 처리가 완료되었습니다.\n\n"
                f"전체 행: {total_rows}\n"
                f"처리된 행: {done_rows} (성공={success}, 건너뜀={skipped}, 실패={failed})\n"
                f"총 토큰: {total_tokens}\n"
            )
            if total_cost_usd is not None:
                msg += f"예상 비용(USD): 약 {total_cost_usd:.6f}\n"

            if saved_path and os.path.exists(saved_path):
                msg += f"\n결과 엑셀을 지금 여시겠습니까?\n{saved_path}"
                if messagebox.askyesno("완료", msg):
                    self._open_file(saved_path)
            else:
                messagebox.showinfo("완료", msg)

        self.after(0, _do)

    # ---------------- 이벤트 핸들러 ----------------
    def on_save_api_key(self):
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("경고", "API Key를 입력하세요.")
            return
        save_api_key_to_file(key)
        self.append_log("[INFO] API 키 저장 완료.")

    def on_browse_excel(self):
        path = filedialog.askopenfilename(
            title="Stage2 메타/이미지 포함 엑셀 선택 (T1 버전만 가능)",
            filetypes=[("Excel files", "*.xlsx;*.xls")],
        )
        if path:
            # T1 포함 여부 검증
            base_name = os.path.splitext(os.path.basename(path))[0]
            if not re.search(r"_T1_[Ii]\d+", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 T1 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {os.path.basename(path)}\n"
                    f"파일명에 '_T1_I*' 패턴이 포함되어 있어야 합니다."
                )
                return
            
            # 엑셀 파일을 읽어서 상세이미지 통계 계산
            self._analyze_detail_images(path)
            
            self.src_excel_var.set(path)
    
    def _analyze_detail_images(self, excel_path):
        """엑셀 파일의 상세이미지 컬럼 통계를 분석합니다."""
        try:
            df = pd.read_excel(excel_path)
            
            # 상세이미지 컬럼 찾기
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
            
            # 각 행에서 실제로 값이 있는 상세이미지 컬럼 개수 계산
            row_counts = {}  # {컬럼개수: 행개수}
            for idx, row in df.iterrows():
                count = 0
                for col in detail_cols:
                    val = row.get(col, "")
                    if pd.notna(val) and str(val).strip():
                        count += 1
                if count > 0:
                    row_counts[count] = row_counts.get(count, 0) + 1
            
            # 통계 저장
            self.detail_image_stats = {
                "max_columns": max_col_num,
                "row_counts": row_counts,
            }
            
            # 통계 표시 업데이트
            self._update_detail_stats_display()
            
            # 파일 선택 시 통계를 메시지박스로도 표시 (선택적)
            if max_col_num > 0:
                rows_exceeding = sum(count for col_count, count in row_counts.items() if col_count > self.max_detail_images_var.get())
                if rows_exceeding > 0:
                    # 설정값 초과 행이 있으면 경고 메시지
                    messagebox.showinfo(
                        "상세이미지 통계",
                        f"📊 엑셀 파일 분석 완료\n\n"
                        f"최대 상세이미지 컬럼: {max_col_num}개\n"
                        f"현재 설정값: {self.max_detail_images_var.get()}개\n"
                        f"⚠️ 설정값 초과 행: {rows_exceeding}개\n\n"
                        f"상세 통계는 옵션 설정 영역에서 확인할 수 있습니다."
                    )
                else:
                    # 모든 행이 설정값 이하면 정보 메시지
                    messagebox.showinfo(
                        "상세이미지 통계",
                        f"📊 엑셀 파일 분석 완료\n\n"
                        f"최대 상세이미지 컬럼: {max_col_num}개\n"
                        f"현재 설정값: {self.max_detail_images_var.get()}개\n"
                        f"✅ 모든 행이 설정값 이하입니다.\n\n"
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
        max_limit = self.max_detail_images_var.get()
        
        if max_cols == 0:
            self.detail_stats_label.config(
                text="📊 엑셀 파일을 선택하면 상세이미지 통계가 표시됩니다.",
                fg="#666",
                bg="#E3F2FD"
            )
            return
        
        # 설정한 개수보다 많은 상세이미지를 가진 행 개수 계산
        rows_exceeding_limit = 0
        for col_count, row_count in row_counts.items():
            if col_count > max_limit:
                rows_exceeding_limit += row_count
        
        # 통계 메시지 생성
        if rows_exceeding_limit > 0:
            stats_text = (
                f"📊 최대 상세이미지 컬럼: {max_cols}개 | "
                f"⚠️ {max_limit}개 초과 행: {rows_exceeding_limit}개"
            )
            color = "#d32f2f"  # 빨간색 (경고)
            bg_color = "#FFEBEE"  # 연한 빨간색 배경
        else:
            stats_text = f"📊 최대 상세이미지 컬럼: {max_cols}개 | ✅ 모든 행이 {max_limit}개 이하"
            color = "#388e3c"  # 초록색 (정상)
            bg_color = "#E8F5E9"  # 연한 초록색 배경
        
        # 상세 통계 추가 (옵션)
        if row_counts:
            # 가장 많은 상세이미지를 가진 행의 개수
            max_row_count = max(row_counts.keys(), default=0)
            if max_row_count > max_limit:
                stats_text += f" (최대 {max_row_count}개 상세이미지 행 포함)"
        
        # 배경색도 함께 업데이트
        self.detail_stats_label.config(text=stats_text, fg=color, bg=bg_color)
        # 프레임 배경색도 업데이트
        if hasattr(self, 'detail_stats_frame'):
            self.detail_stats_frame.config(bg=bg_color)
    
    def _show_detail_stats_popup(self):
        """상세이미지 통계를 팝업 창으로 표시합니다."""
        stats = self.detail_image_stats
        max_cols = stats.get("max_columns", 0)
        row_counts = stats.get("row_counts", {})
        max_limit = self.max_detail_images_var.get()
        
        if max_cols == 0:
            messagebox.showinfo("상세이미지 통계", "엑셀 파일을 먼저 선택해주세요.")
            return
        
        # 통계 메시지 생성
        msg_lines = [
            "📊 상세이미지 통계",
            "",
            f"최대 상세이미지 컬럼 개수: {max_cols}개",
            f"현재 설정값: {max_limit}개",
            "",
        ]
        
        # 설정값 초과 행 개수
        rows_exceeding = sum(count for col_count, count in row_counts.items() if col_count > max_limit)
        if rows_exceeding > 0:
            msg_lines.append(f"⚠️ {max_limit}개 초과 행: {rows_exceeding}개")
            msg_lines.append("")
        
        # 상세 통계 (컬럼 개수별 행 개수)
        if row_counts:
            msg_lines.append("상세이미지 개수별 행 분포:")
            sorted_counts = sorted(row_counts.items(), key=lambda x: x[0], reverse=True)
            for col_count, row_count in sorted_counts[:20]:  # 상위 20개만 표시
                marker = "⚠️" if col_count > max_limit else "  "
                msg_lines.append(f"  {marker} {col_count}개 상세이미지: {row_count}개 행")
            if len(sorted_counts) > 20:
                msg_lines.append(f"  ... (총 {len(sorted_counts)}개 그룹)")
        
        messagebox.showinfo("상세이미지 통계", "\n".join(msg_lines))

    def on_start(self):
        excel_path = self.src_excel_var.get().strip()
        model_name = self.model_var.get().strip()
        effort = self.effort_var.get().strip()
        api_key = self.api_key_var.get().strip()
        skip_filled = self.skip_filled_var.get()

        if not excel_path:
            messagebox.showwarning("경고", "입력 엑셀 경로를 먼저 지정하세요.")
            return
        if not os.path.exists(excel_path):
            messagebox.showwarning("경고", f"입력 엑셀을 찾을 수 없습니다:\n{excel_path}")
            return
        
        # T1 포함 여부 검증
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        if not re.search(r"_T1_[Ii]\d+", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 T1 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {os.path.basename(excel_path)}\n"
                f"파일명에 '_T1_I*' 패턴이 포함되어 있어야 합니다."
            )
            return
        if not api_key:
            messagebox.showwarning("경고", "API Key를 입력하거나 저장하세요.")
            return
        if not model_name:
            messagebox.showwarning("경고", "모델을 선택하세요.")
            return

        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self._stop_event.clear()

        # 진행/토큰/비용 초기화
        self.progress_var.set(0.0)
        self.progress_text_var.set("진행률: 0% (0 / 0)")
        self.time_info_var.set("시작: -   종료: -   경과: -")
        self.token_info_var.set("토큰: -")
        self.cost_info_var.set("비용: -")
        self.summary_info_var.set(
            "처리 요약: 전체=0, 처리행=0, 성공=0, 건너뜀=0, 실패=0"
        )
        self._image_cache.clear()

        # 시작 시각(서울 기준)
        self._start_dt = get_seoul_now()
        start_dt = self._start_dt

        self.after(
            0,
            lambda: self.time_info_var.set(
                f"시작: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}   종료: -   경과: 0초"
            ),
        )

        def worker():
            try:
                client = OpenAI(api_key=api_key)
                self.append_log(
                    f"[RUN] Stage2 LLM 처리 시작. model={model_name}, effort={effort}, "
                    f"use_thumbnail={self.use_thumbnail_var.get()}, allow_url={self.allow_url_var.get()}"
                )
                self._run_stage2_llm(
                    client=client,
                    excel_path=excel_path,
                    model_name=model_name,
                    effort=effort,
                    skip_filled=skip_filled,
                    start_dt=start_dt,
                )
                self.append_log("[RUN] 작업이 종료되었습니다.")
            except Exception as e:
                self.append_log(f"[FATAL] 작업 중 예외 발생: {e}")
                self.after(
                    0,
                    lambda: messagebox.showerror(
                        "에러",
                        f"Stage2 LLM 작업 실패:\n{e}",
                    ),
                )
            finally:
                self.after(0, lambda: self.btn_start.config(state="normal"))
                self.after(0, lambda: self.btn_stop.config(state="disabled"))

        th = threading.Thread(target=worker, daemon=True)
        th.start()
        self._worker_thread = th

    def on_stop(self):
        self._stop_event.set()
        self.append_log("[INFO] 중단 요청 플래그를 설정했습니다. 현재 작업이 끝나면 멈춥니다.")

    # ---------------- 응답에서 텍스트 뽑는 함수 ----------------
    def _extract_text_from_response(self, resp) -> str:
        """
        Responses API 응답 객체에서 텍스트(JSON 문자열)를 안전하게 추출.
        - 1차: resp.output[*].content[*].text.value (SDK 객체 방식)
        - 2차: model_dump() 해서 dict 모드로 파싱
        """
        text_chunks = []

        # 1) SDK 객체 방식
        outputs = getattr(resp, "output", None)
        if outputs:
            try:
                for out in outputs:
                    content_list = getattr(out, "content", None)
                    if not content_list:
                        continue
                    for item in content_list:
                        txt_obj = getattr(item, "text", None)
                        if txt_obj is None:
                            continue
                        val = getattr(txt_obj, "value", None)
                        if isinstance(val, str) and val.strip():
                            text_chunks.append(val.strip())
                        elif isinstance(txt_obj, str) and txt_obj.strip():
                            text_chunks.append(txt_obj.strip())
            except TypeError:
                pass

        # 2) dict로 fallback
        if not text_chunks:
            try:
                resp_dict = resp.model_dump()
            except Exception:
                resp_dict = None

            if isinstance(resp_dict, dict):
                out_list = resp_dict.get("output") or []
                if isinstance(out_list, list):
                    for out in out_list:
                        if not isinstance(out, dict):
                            continue
                        content_list = out.get("content") or []
                        if not isinstance(content_list, list):
                            continue
                        for c in content_list:
                            if not isinstance(c, dict):
                                continue
                            txt_obj = c.get("text")
                            if isinstance(txt_obj, str) and txt_obj.strip():
                                text_chunks.append(txt_obj.strip())
                            elif isinstance(txt_obj, dict):
                                val = txt_obj.get("value")
                                if isinstance(val, str) and val.strip():
                                    text_chunks.append(val.strip())

        full_text = "\n".join(text_chunks).strip()
        if not full_text:
            raise RuntimeError("응답에서 텍스트를 추출하지 못했습니다.")
        return full_text

    # ---------------- usage(토큰) 뽑는 함수 ----------------
    def _extract_usage_tokens(self, resp):
        """
        Responses API 응답에서 토큰 사용량 추출.
        - resp.model_dump()["usage"] 기준으로 input / output / reasoning 토큰 합산
        """
        in_tok = 0
        out_tok = 0
        reasoning_tok = 0

        try:
            if hasattr(resp, "model_dump"):
                data = resp.model_dump()
            else:
                data = resp

            if isinstance(data, dict):
                usage = data.get("usage")
            else:
                usage = getattr(data, "usage", None)

            def add_from_usage(u):
                nonlocal in_tok, out_tok, reasoning_tok
                if not isinstance(u, dict):
                    return
                it = int(u.get("input_tokens") or u.get("prompt_tokens") or 0)
                ot = int(u.get("output_tokens") or u.get("completion_tokens") or 0)
                rt = int(u.get("reasoning_tokens") or 0)

                details = u.get("output_tokens_details")
                if isinstance(details, dict):
                    rt_detail = details.get("reasoning_tokens")
                    if rt_detail is not None:
                        rt = int(rt_detail)

                in_tok += it
                out_tok += ot
                reasoning_tok += rt

            if isinstance(usage, dict):
                add_from_usage(usage)
            elif isinstance(usage, list):
                for u in usage:
                    add_from_usage(u)

        except Exception as e:
            self.append_log(f"[WARN] usage 파싱 실패: {e}")

        return in_tok, out_tok, reasoning_tok

    # ---------------- 비용 계산 함수 ----------------
    def _calc_costs(
        self,
        model_name: str,
        total_in_tok: int,
        total_out_tok: int,
        total_reasoning_tok: int,
    ):
        """
        모델별 단가(MODEL_PRICING_USD_PER_MTOK)에 따라
        input / output / total 비용(USD)을 계산.
        """
        price = MODEL_PRICING_USD_PER_MTOK.get(model_name)
        if not price:
            return None, None, None

        in_rate = float(price.get("input", 0.0))
        out_rate = float(price.get("output", 0.0))

        input_cost = (total_in_tok / 1_000_000.0) * in_rate
        output_tokens_for_cost = total_out_tok + total_reasoning_tok
        output_cost = (output_tokens_for_cost / 1_000_000.0) * out_rate
        total_cost = input_cost + output_cost

        return input_cost, output_cost, total_cost

    # ---------------- LLM 실행 메인 로직 ----------------
    def _run_stage2_llm(
        self,
        client: OpenAI,
        excel_path: str,
        model_name: str,
        effort: str,
        skip_filled: bool,
        start_dt: datetime,
    ) -> None:
        """
        주의:
        - 루프 도중 중단 요청이 들어오면 break 후, 지금까지 채운 ST2_JSON을 엑셀에 저장하고 종료.
        - 다음 실행 시 'skip_filled=True' 옵션 덕분에 이어서 처리 가능.
        """
        self.append_log(f"[INFO] 엑셀 읽는 중: {excel_path}")
        df = pd.read_excel(excel_path)

        # ST2_프롬프트 컬럼이 없어도 동작. (Stage2 프롬프트는 코드에서 자동 생성)
        if "ST2_JSON" not in df.columns:
            df["ST2_JSON"] = ""

        detail_cols = [c for c in df.columns if str(c).startswith("상세이미지_")]
        if detail_cols:
            def sort_key(c):
                try:
                    return int(str(c).split("_")[1])
                except Exception:
                    return 9999

            detail_cols.sort(key=sort_key)
            
            # 상세이미지 개수 제한 적용
            max_detail_images = self.max_detail_images_var.get()
            if max_detail_images > 0:
                detail_cols = detail_cols[:max_detail_images]
                self.append_log(f"[INFO] 상세이미지 컬럼 제한 적용: {max_detail_images}개까지만 사용")
            else:
                self.append_log(f"[INFO] 상세이미지 컬럼 제한 없음: 모든 컬럼 사용")
        
        self.append_log(f"[INFO] 사용할 상세이미지 컬럼: {detail_cols}")

        use_thumbnail = self.use_thumbnail_var.get()
        allow_url = self.allow_url_var.get()
        self.append_log(f"[INFO] 옵션 - 썸네일 사용: {use_thumbnail}, URL 이미지 사용: {allow_url}")

        total_rows = len(df)
        done_rows = 0
        success = 0
        skipped = 0
        failed = 0

        total_in_tok = 0
        total_out_tok = 0
        total_reasoning_tok = 0

        for idx, row in df.iterrows():
            if self._stop_event.is_set():
                self.append_log("[INFO] 사용자가 중단을 요청했습니다. 현재까지의 결과를 저장하고 종료합니다.")
                break

            existing_json = safe_str(row.get("ST2_JSON", ""))
            if skip_filled and existing_json:
                skipped += 1
                self.append_log(f"[SKIP] idx={idx} : 이미 ST2_JSON 값이 있어 건너뜀.")
                done_rows += 1
                self._update_progress(done_rows, total_rows, start_dt)
                continue

            # Stage2 요청(프롬프트 + 이미지 경로) 생성
            try:
                req = build_stage2_request_from_row(row, detail_cols)
            except Exception as e:
                failed += 1
                self.append_log(f"[ERROR] idx={idx} : Stage2 프롬프트 생성 중 예외 발생 → 건너뜀. ({e})")
                done_rows += 1
                self._update_progress(done_rows, total_rows, start_dt)
                continue

            prompt = safe_str(getattr(req, "prompt", ""))
            if not prompt:
                skipped += 1
                self.append_log(f"[SKIP] idx={idx} : Stage2 프롬프트 생성 결과가 비어 있어 건너뜀.")
                done_rows += 1
                self._update_progress(done_rows, total_rows, start_dt)
                continue

            if "ST2_프롬프트" in df.columns:
                df.at[idx, "ST2_프롬프트"] = prompt

            상품코드 = safe_str(row.get("상품코드", ""))
            self.append_log(f"[CALL] idx={idx} (상품코드={상품코드}) → 모델={model_name}, effort={effort}")

            image_paths = list(getattr(req, "image_paths", []) or [])

            # 썸네일(이미지대) 제외 옵션 처리
            if not use_thumbnail:
                thumb_val = safe_str(row.get("이미지대", ""))
                if thumb_val:
                    before_len = len(image_paths)
                    image_paths = [p for p in image_paths if safe_str(p) != thumb_val]
                    if len(image_paths) != before_len:
                        self.append_log(
                            f"[INFO] idx={idx} : 옵션에 따라 썸네일(이미지대) 제외: {thumb_val}"
                        )

            image_entries = build_image_nodes_from_paths(
                image_paths,
                log_func=self.append_log,
                cache=self._image_cache,
                allow_url=allow_url,
            )

            if not image_entries:
                self.append_log(f"[WARN] idx={idx} : 사용할 이미지가 없습니다. 텍스트만으로 호출합니다.")

            content = [{"type": "input_text", "text": prompt}]
            content.extend(image_entries)

            extra_args = {}
            if effort in ("low", "medium", "high"):
                extra_args["reasoning"] = {"effort": effort}

            t0 = time.time()
            try:
                resp = client.responses.create(
                    model=model_name,
                    input=[{"role": "user", "content": content}],
                    **extra_args,
                )

                full_text = self._extract_text_from_response(resp)

                in_tok, out_tok, r_tok = self._extract_usage_tokens(resp)
                total_in_tok += in_tok
                total_out_tok += out_tok
                total_reasoning_tok += r_tok

                df.at[idx, "ST2_JSON"] = full_text
                success += 1
                elapsed = time.time() - t0
                self.append_log(
                    f"[OK] idx={idx} 처리 완료 ({elapsed:.1f}초) "
                    f"[tokens in={in_tok}, out={out_tok}, reasoning={r_tok}]"
                )

            except Exception as e:
                failed += 1
                self.append_log(f"[ERROR] idx={idx} 처리 중 예외 발생: {e}")

            done_rows += 1
            self._update_progress(done_rows, total_rows, start_dt)
            time.sleep(0.2)

        # ST2_JSON이 있는 행과 없는 행 분리
        if "ST2_JSON" in df.columns:
            # ST2_JSON이 비어있거나 None인 행 찾기
            df_with_st2 = df[df["ST2_JSON"].notna() & (df["ST2_JSON"] != '')].copy()
            df_no_st2 = df[df["ST2_JSON"].isna() | (df["ST2_JSON"] == '')].copy()
        else:
            # 컬럼이 없으면 모든 행이 ST2_JSON 없음으로 처리
            df_with_st2 = pd.DataFrame()
            df_no_st2 = df.copy()
        
        # ST2_JSON이 없는 행들을 T2-2(실패) 버전으로 별도 파일 저장
        no_st2_path = None
        if len(df_no_st2) > 0:
            base_dir = os.path.dirname(excel_path)
            base_name, ext = os.path.splitext(os.path.basename(excel_path))
            
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
            
            self.append_log(f" - T2-2(실패) 분리 파일: {os.path.basename(no_st2_path)} ({len(df_no_st2)}개 행)")
            self.append_log(f"   ※ 이 파일은 T2-1 단계까지만 작업 가능합니다.")
            
            # 분리된 파일의 런처 상태 업데이트
            try:
                no_st2_root_name = get_root_filename(no_st2_path)
                JobManager.update_status(no_st2_root_name, text_msg="T2-2(실패)")
                self.append_log(f"[Launcher] 분리 파일 상태 업데이트: {no_st2_root_name} -> T2-2(실패)")
            except Exception as e:
                self.append_log(f"[Launcher] 분리 파일 상태 업데이트 실패: {e}")
        
        # ST2_JSON이 있는 행들만 저장
        if len(df_with_st2) > 0:
            df = df_with_st2
        else:
            self.append_log("⚠️ ST2_JSON이 있는 행이 없습니다.")
        
        # 최종 저장 (엑셀 열려 있을 때 대비)
        saved_path = self._save_df_with_backup(df, excel_path)
        self.append_log(
            f"[SUMMARY] 전체={total_rows}, 처리행={done_rows}, 성공={success}, "
            f"건너뜀={skipped}, 실패={failed}"
        )

        total_tokens = total_in_tok + total_out_tok + total_reasoning_tok
        self.append_log(
            f"[TOKENS] input={total_in_tok}, output={total_out_tok}, "
            f"reasoning={total_reasoning_tok}, total={total_tokens}"
        )

        # GUI 요약 표시
        self.after(
            0,
            lambda: self.summary_info_var.set(
                f"처리 요약: 전체={total_rows}, 처리행={done_rows}, "
                f"성공={success}, 건너뜀={skipped}, 실패={failed}"
            ),
        )
        self.after(
            0,
            lambda: self.token_info_var.set(
                f"토큰: input={total_in_tok}, output={total_out_tok}, "
                f"reasoning={total_reasoning_tok}, total={total_tokens}"
            ),
        )

        # 비용 계산
        input_cost_usd, output_cost_usd, total_cost_usd = self._calc_costs(
            model_name, total_in_tok, total_out_tok, total_reasoning_tok
        )

        if total_cost_usd is not None:
            self.append_log(
                "[COST] model={}  input=${:.6f}, output=${:.6f}, total=${:.6f}".format(
                    model_name,
                    input_cost_usd,
                    output_cost_usd,
                    total_cost_usd,
                )
            )
            self.after(
                0,
                lambda: self.cost_info_var.set(
                    f"비용(USD): input={input_cost_usd:.6f}, "
                    f"output={output_cost_usd:.6f}, total={total_cost_usd:.6f}"
                ),
            )
        else:
            self.append_log(
                f"[COST] 모델 '{model_name}' 에 대한 가격 정보가 없어 비용 계산을 생략했습니다."
            )
            self.after(0, lambda: self.cost_info_var.set("비용: - (가격 정보 없음)"))

        # 실행 이력 기록
        finish_dt = get_seoul_now()
        elapsed_seconds = (finish_dt - start_dt).total_seconds()
        api_rows = success + failed  # 실제 API 호출한 행 수

        append_run_history(
            stage="stage2_llm",
            model_name=model_name,
            reasoning_effort=effort,
            src_file=excel_path,
            total_rows=total_rows,
            api_rows=api_rows,
            elapsed_seconds=elapsed_seconds,
            total_in_tok=total_in_tok,
            total_out_tok=total_out_tok,
            total_reasoning_tok=total_reasoning_tok,
            input_cost_usd=input_cost_usd,
            output_cost_usd=output_cost_usd,
            total_cost_usd=total_cost_usd,
            start_dt=start_dt,
            finish_dt=finish_dt,
            api_type="per_call",
            batch_id=None,
            out_file=saved_path,
            success_rows=success,
            fail_rows=failed,
        )
        
        # 메인 런처 현황판에 T2-2 완료 상태 기록 (img 상태는 변경하지 않음)
        try:
            root_name = get_root_filename(saved_path)
            JobManager.update_status(root_name, text_msg="T2-2(분석완료)")
            self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T2-2(분석완료)")
        except Exception as e:
            self.append_log(f"[WARN] 런처 현황판 업데이트 실패: {e}")

        # 완료 팝업 + 엑셀 열기 여부
        self._show_completion_dialog(
            saved_path=saved_path,
            total_rows=total_rows,
            done_rows=done_rows,
            success=success,
            skipped=skipped,
            failed=failed,
            total_tokens=total_tokens,
            total_cost_usd=total_cost_usd,
        )

    def _update_progress(self, done: int, total: int, start_dt: datetime):
        if total <= 0:
            return
        ratio = done / total
        pct = round(ratio * 100, 1)

        now = get_seoul_now()
        elapsed = int((now - start_dt).total_seconds())

        self.after(0, lambda: self.progress_var.set(pct))
        self.after(
            0,
            lambda: self.progress_text_var.set(
                f"진행률: {pct}% ({done} / {total})"
            ),
        )
        self.after(
            0,
            lambda: self.time_info_var.set(
                f"시작: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}   "
                f"종료: {now.strftime('%Y-%m-%d %H:%M:%S')}   "
                f"경과: {elapsed}초"
            ),
        )


def main():
    app = Stage2LLMApp()
    app.mainloop()


if __name__ == "__main__":
    main()
