"""
IMG_analysis_gui_Casche.py

썸네일 이미지 분석용 엑셀 기반 GUI (캐싱 최적화 버전)
- 엑셀 파일에서 "IMG_S1_누끼" 컬럼을 읽어
- 각 행의 이미지를 512x512로 리사이즈하여 base64로 전달
- OpenAI Vision API를 호출하여 카메라 각도, 레이아웃, 조명 분석 JSON 생성
- 결과를 엑셀에 저장 (view_point, subject_position, subject_size, lighting_condition,
  color_tone, shadow_presence, background_simplicity, is_flat_lay, bg_layout_hint_en)
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 규칙)를 system 프롬프트에 배치
  * 동적 콘텐츠(이미지)를 user 프롬프트에 배치
  * prompt_cache_key 사용으로 캐시 히트율 향상 (토큰 비용 최대 90% 절감 가능)
"""

import os
import json
import re
import time
import threading
from datetime import datetime
from typing import Optional

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

# [필수 의존성] IMG_analysis_core_Casche.py
# 캐싱 최적화 버전 사용 (IMG_analysis_core_Casche.py)
try:
    from IMG_analysis_core_Casche import (
        API_KEY_FILE,
        DEFAULT_MODEL,
        load_api_key_from_file,
        save_api_key_to_file,
        call_image_analysis_api,
        build_analysis_messages,
        build_analysis_batch_payload,  # Batch API용 payload 빌더 (캐싱 최적화)
        get_openai_client,
        MODEL_PRICING_USD_PER_MTOK,
    )
    CACHE_MODE_CORE = True
except ImportError:
    # 캐싱 버전이 없으면 일반 버전 사용
    try:
        from IMG_analysis_core import (
            API_KEY_FILE,
            DEFAULT_MODEL,
            load_api_key_from_file,
            save_api_key_to_file,
            call_image_analysis_api,
            get_openai_client,
            MODEL_PRICING_USD_PER_MTOK,
        )
        CACHE_MODE_CORE = False
        def build_analysis_messages(*args, **kwargs): return []
        def build_analysis_batch_payload(*args, **kwargs): return None
    except ImportError:
        # 의존성 파일 부재 시 비상용 더미
        CACHE_MODE_CORE = False
        API_KEY_FILE = ".openai_api_key_img_analysis"
        DEFAULT_MODEL = "gpt-5-mini"
        MODEL_PRICING_USD_PER_MTOK = {}
        def load_api_key_from_file(x): return ""
        def save_api_key_to_file(x, y): pass
        def call_image_analysis_api(*args, **kwargs): return {}, None
        def build_analysis_messages(*args, **kwargs): return []
        def build_analysis_batch_payload(*args, **kwargs): return None
        def get_openai_client(*args, **kwargs): return None

# =======================
#  파일명 버전 관리 유틸
# =======================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, T4(완)_I* 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T3_I2.xlsx -> 아디다스.xlsx
    예: 나이키_T0_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T0_I0_T1_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 나이키_T4(완)_I2.xlsx -> 나이키.xlsx
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
    suffixes = ["_img_analysis_done", "_img_analysis_batch_done", "_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext


def get_i3_output_path(input_path: str) -> str:
    """
    입력 파일명을 분석해서 I3로 고정된 출력 파일명을 생성합니다.
    입력: I2 또는 I3 파일 (예: 상품_T3_I2.xlsx, 상품_T3_I3.xlsx, 상품_T4(완)_I2.xlsx)
    출력: 항상 I3 (예: 상품_T3_I3.xlsx, 상품_T4(완)_I3.xlsx)
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

    # 항상 I3로 고정, T 부분은 그대로 유지 (예: T4(완) 또는 T4)
    new_filename = f"{original_name}_T{current_t}{t_suffix}_I3{ext}"
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


# 서울 시간 헬퍼
try:
    from zoneinfo import ZoneInfo
    def get_seoul_now() -> datetime:
        return datetime.now(ZoneInfo("Asia/Seoul"))
except ImportError:
    try:
        import pytz
        def get_seoul_now() -> datetime:
            return datetime.now(pytz.timezone("Asia/Seoul"))
    except ImportError:
        def get_seoul_now() -> datetime:
            return datetime.now()


# =======================
#  UI 헬퍼 클래스 (ToolTip)
# =======================
class ToolTip:
    """마우스 오버 시 도움말 팝업"""
    def __init__(self, widget, text: str, wraplength: int = 400):
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
        label = tk.Label(
            tw, text=self.text, justify="left",
            background="#ffffe0", relief="solid", borderwidth=1,
            wraplength=self.wraplength, font=("맑은 고딕", 9)
        )
        label.pack(ipadx=4, ipady=2)

    def hide_tip(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None


# =======================
#  메인 GUI 클래스
# =======================
class ImageAnalysisGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("썸네일 이미지 분석 - 카메라 각도/레이아웃/조명 분석기 (엑셀 기반) 🚀 캐싱 최적화 버전")
        self.geometry("1000x850")
        
        # 스타일 설정
        self._configure_styles()

        # --- 변수 초기화 ---
        self.api_key_var = tk.StringVar(value=load_api_key_from_file(API_KEY_FILE) or "")
        self.input_file_path = tk.StringVar()
        self.output_file_path = ""

        # 옵션 변수
        self.model_var = tk.StringVar(value=DEFAULT_MODEL)
        self.effort_var = tk.StringVar(value="low")  # none/low/medium/high
        self.skip_filled_var = tk.BooleanVar(value=True)
        self.skip_bad_label_var = tk.BooleanVar(value=True)  # 'bad' 라벨 행 제외 (기본값: True)

        # 상태 및 통계 변수
        self.is_running = False
        self.stop_requested = False
        
        self.stat_progress = tk.StringVar(value="0.0%")
        self.stat_count = tk.StringVar(value="0 / 0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_fail = tk.StringVar(value="0")
        self.stat_cost = tk.StringVar(value="$0.0000")
        self.stat_time = tk.StringVar(value="00:00:00")
        self.status_msg = tk.StringVar(value="파일을 선택하고 작업을 시작하세요.")

        # UI 구성
        self._init_ui()

    def _configure_styles(self):
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        bg_color = "#f5f5f5"
        self.configure(background=bg_color)
        
        style.configure("TFrame", background=bg_color)
        style.configure("TLabelframe", background=bg_color, font=("맑은 고딕", 10, "bold"))
        style.configure("TLabelframe.Label", background=bg_color, foreground="#333333")
        style.configure("TLabel", background=bg_color, font=("맑은 고딕", 10))
        
        style.configure("Header.TLabel", font=("맑은 고딕", 11, "bold"), foreground="#444")
        style.configure("Stat.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#0052cc")
        style.configure("Cost.TLabel", font=("맑은 고딕", 12, "bold"), foreground="#d32f2f")
        
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=5)
        style.configure("Stop.TButton", font=("맑은 고딕", 11, "bold"), foreground="red", padding=5)

    def _init_ui(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)

        # 1. 상단: API & Model 설정
        frame_top = ttk.LabelFrame(main_frame, text="API 및 모델 설정", padding=15)
        frame_top.pack(fill='x', pady=(0, 10))

        # Row 1: API Key
        r1 = ttk.Frame(frame_top)
        r1.pack(fill='x', pady=2)
        ttk.Label(r1, text="API Key:", width=10).pack(side='left')
        entry_key = ttk.Entry(r1, textvariable=self.api_key_var, show="*", width=50)
        entry_key.pack(side='left', padx=5)
        ttk.Button(r1, text="저장", command=self._save_key, width=8).pack(side='left')
        
        # 도움말 아이콘
        lbl_help = ttk.Label(r1, text="❓도움말", foreground="blue", cursor="hand2")
        lbl_help.pack(side='right', padx=5)
        ToolTip(lbl_help, "Vision API 지원 모델 사용 권장.\n모델별 가격 정책 확인 필요.")

        # Row 2: Model & Effort
        r2 = ttk.Frame(frame_top)
        r2.pack(fill='x', pady=5)
        ttk.Label(r2, text="모델:", width=10).pack(side='left')
        models = list(MODEL_PRICING_USD_PER_MTOK.keys())
        ttk.Combobox(r2, textvariable=self.model_var, values=models, state="readonly", width=18).pack(side='left', padx=5)
        
        ttk.Label(r2, text="Effort:", width=8).pack(side='left', padx=(20,0))
        ttk.Combobox(r2, textvariable=self.effort_var, values=["none", "low", "medium", "high"], state="readonly", width=12).pack(side='left', padx=5)

        # 2. 파일 선택 & 옵션
        frame_file = ttk.LabelFrame(main_frame, text="작업 대상 파일", padding=15)
        frame_file.pack(fill='x', pady=(0, 10))
        
        rf = ttk.Frame(frame_file)
        rf.pack(fill='x')
        ttk.Entry(rf, textvariable=self.input_file_path).pack(side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(rf, text="📂 파일 선택", command=self._select_file).pack(side='right')
        
        # 건너뛰기 체크박스
        ttk.Checkbutton(
            frame_file, 
            text="이미 결과(view_point 등)가 있는 행은 건너뛰기", 
            variable=self.skip_filled_var
        ).pack(anchor='w', pady=(5,0))
        
        ttk.Checkbutton(
            frame_file, 
            text="'bad' 라벨이 있는 행 제외 (IMG_S1_휴먼라벨 또는 IMG_S1_AI라벨이 'bad'인 경우)", 
            variable=self.skip_bad_label_var
        ).pack(anchor='w', pady=(2,0))

        # 3. 대시보드 (Dashboard)
        dash_frame = ttk.LabelFrame(main_frame, text="실시간 현황 (Dashboard)", padding=15)
        dash_frame.pack(fill='x', pady=(0, 10))

        # 1행: 진행률
        d1 = ttk.Frame(dash_frame)
        d1.pack(fill='x', pady=5)
        ttk.Label(d1, text="진행률:", style="Header.TLabel", width=10).pack(side='left')
        self.pb = ttk.Progressbar(d1, maximum=100, mode='determinate')
        self.pb.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Label(d1, textvariable=self.stat_progress, style="Stat.TLabel", width=8).pack(side='right')

        # 2행: 통계
        d2 = ttk.Frame(dash_frame)
        d2.pack(fill='x', pady=5)
        ttk.Label(d2, text="처리 건수:", width=10).pack(side='left')
        ttk.Label(d2, textvariable=self.stat_count, width=15, foreground="blue", font=("맑은 고딕", 10, "bold")).pack(side='left')
        
        ttk.Label(d2, text="성공/실패:", width=10).pack(side='left')
        lbl_succ = ttk.Label(d2, textvariable=self.stat_success, foreground="green", font=("맑은 고딕", 10, "bold"))
        lbl_succ.pack(side='left')
        ttk.Label(d2, text=" / ").pack(side='left')
        lbl_fail = ttk.Label(d2, textvariable=self.stat_fail, foreground="red", font=("맑은 고딕", 10, "bold"))
        lbl_fail.pack(side='left')

        # 3행: 비용/시간
        d3 = ttk.Frame(dash_frame)
        d3.pack(fill='x', pady=5)
        ttk.Label(d3, text="예상 비용:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_cost, style="Cost.TLabel", width=15).pack(side='left')
        
        ttk.Label(d3, text="경과 시간:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_time).pack(side='left')

        # 4. 액션 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))
        
        self.btn_start = ttk.Button(btn_frame, text="▶ 작업 시작", style="Action.TButton", command=self._start_thread)
        self.btn_start.pack(side='left', fill='x', expand=True, padx=(0, 5))
        
        self.btn_stop = ttk.Button(btn_frame, text="⏹ 중단 (Safe Stop)", style="Stop.TButton", command=self._request_stop, state='disabled')
        self.btn_stop.pack(side='right', fill='x', expand=True, padx=(5, 0))

        ttk.Label(main_frame, textvariable=self.status_msg, foreground="#555", anchor='center').pack(fill='x', pady=(0, 5))

        # 5. 로그창
        log_frame = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        log_frame.pack(fill='both', expand=True)
        self.log_widget = ScrolledText(log_frame, height=10, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    # --- UI 이벤트 핸들러 ---
    def _save_key(self):
        k = self.api_key_var.get().strip()
        if k:
            save_api_key_to_file(k, API_KEY_FILE)
            messagebox.showinfo("저장", "API Key가 저장되었습니다.")

    def _select_file(self):
        p = filedialog.askopenfilename(
            title="썸네일 분석 엑셀 선택 (I2 버전만 가능)",
            filetypes=[("Excel Files", "*.xlsx;*.xls")]
        )
        if p:
            base_name = os.path.basename(p)
            # I2 포함 여부 검증
            if not re.search(r"_I2", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 I2 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {base_name}\n"
                    f"파일명에 '_I2' 패턴이 포함되어 있어야 합니다."
                )
                return
            
            out_path = get_i3_output_path(p)
            out_name = os.path.basename(out_path)
            self.input_file_path.set(p)
            self._log(f"파일 선택됨: {base_name} (I2)")
            self._log(f"출력 파일: {out_name} (I3로 고정 저장)")
            self.status_msg.set(f"준비 완료. 저장 시 {out_name}로 저장됩니다.")

    def _log(self, msg):
        self.log_widget.after(0, self._append_log, msg)

    def _append_log(self, msg):
        t = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert("end", f"[{t}] {msg}\n")
        self.log_widget.see("end")
        self.log_widget.config(state='disabled')

    def _request_stop(self):
        if self.is_running:
            self.stop_requested = True
            self.status_msg.set("⚠️ 중단 요청됨! 현재 행 처리 후 멈춥니다.")
            self.btn_stop.config(state='disabled')

    # --- 핵심 로직 ---
    def _safe_str(self, v) -> str:
        """NaN/None 안전하게 문자열로 변환"""
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        return str(v).strip()

    def _save_df_with_backup(self, df: pd.DataFrame, excel_path: str) -> str:
        """엑셀 저장 실패 시 백업 생성"""
        try:
            df.to_excel(excel_path, index=False)
            return excel_path
        except Exception as e:
            base, ext = os.path.splitext(excel_path)
            ts = get_seoul_now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{base}_img_analysis_partial_{ts}{ext}"
            try:
                df.to_excel(backup_path, index=False)
                self._log(f"⚠️ 원본 저장 실패(열림 등). 백업 저장: {os.path.basename(backup_path)}")
                return backup_path
            except Exception as e2:
                self._log(f"🔥 백업 저장도 실패: {e2}")
                return excel_path

    def _extract_usage_tokens(self, resp):
        """API 응답에서 토큰 사용량 추출"""
        usage = getattr(resp, "usage", None)
        if not usage:
            return 0, 0, 0
        
        i = getattr(usage, "prompt_tokens", 0) or 0
        o = getattr(usage, "completion_tokens", 0) or 0
        r = 0
        details = getattr(usage, "completion_tokens_details", None)
        if details:
            r = getattr(details, "reasoning_tokens", 0) or 0
        return i, o, r

    def _calc_cost(self, model, i, o, r):
        """비용 계산"""
        price = MODEL_PRICING_USD_PER_MTOK.get(model, {"input": 0, "output": 0})
        i_cost = (i / 1_000_000) * price["input"]
        o_cost = ((o + r) / 1_000_000) * price["output"]
        return i_cost + o_cost

    def _open_file(self, path: str):
        """파일 열기"""
        if path and os.path.exists(path):
            try:
                os.startfile(path)
            except:
                pass

    # --- 작업 스레드 ---
    def _start_thread(self):
        if self.is_running:
            return
        key = self.api_key_var.get().strip()
        path = self.input_file_path.get().strip()
        
        if not key:
            messagebox.showwarning("오류", "API Key가 없습니다.")
            return
        if not path or not os.path.exists(path):
            messagebox.showwarning("오류", "파일이 없습니다.")
            return
            
        self.is_running = True
        self.stop_requested = False
        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        self.status_msg.set("작업 초기화 중...")
        
        t = threading.Thread(target=self._run_process, args=(key, path))
        t.daemon = True
        t.start()

    def _run_process(self, api_key, input_path):
        try:
            # 캐싱 모드 확인 및 로그
            if CACHE_MODE_CORE:
                self._log(f"[INFO] 🚀 프롬프트 캐싱 최적화 모드 활성화 (IMG_analysis_core_Casche.py)")
            else:
                self._log(f"[INFO] ⚠️ 일반 모드 (IMG_analysis_core.py) - 캐싱 최적화 미적용")
            
            client = get_openai_client(api_key=api_key)
            model = self.model_var.get().strip() or DEFAULT_MODEL

            df = pd.read_excel(input_path)
            if "IMG_S1_누끼" not in df.columns:
                raise ValueError("'IMG_S1_누끼' 컬럼이 없습니다. 이미지 경로 정보가 필요합니다.")

            # 결과 컬럼 준비
            result_cols = [
                "view_point", "subject_position", "subject_size", "lighting_condition",
                "color_tone", "shadow_presence", "background_simplicity", "is_flat_lay",
                "bg_layout_hint_en"
            ]
            for col in result_cols:
                if col not in df.columns:
                    df[col] = ""
                df[col] = df[col].astype(str)

            # 저장 경로 (항상 I3로 고정)
            out_path = get_i3_output_path(input_path)
            self.output_file_path = out_path
            self._log(f"출력 파일: {os.path.basename(out_path)} (I3 고정)")
            
            # 메인 런처 현황판에 I3-1 작업 시작 상태 기록 - img 상태만 업데이트 (text 상태는 변경하지 않음)
            try:
                root_name = get_root_filename(input_path)
                JobManager.update_status(root_name, img_s3_1_msg="I3-1 (진행중)")
                self._log(f"[INFO] 런처 상태 업데이트: {root_name} -> I3-1 (진행중)")
            except Exception as e:
                self._log(f"[WARN] 런처 연동 실패: {e}")

            total_rows = len(df)
            start_dt = get_seoul_now()
            self._update_timer(start_dt)

            stats = {
                "in": 0, "out": 0, "reason": 0, "cost": 0.0,
                "success": 0, "fail": 0, "skip": 0, "api": 0
            }
            processed_now = 0
            
            self._log(f"▶ 시작: {len(df)}행, 모델={model}")

            for idx, row in df.iterrows():
                if self.stop_requested:
                    self._log("⛔ 사용자 중단 요청.")
                    break

                # 건너뛰기 체크
                if self.skip_filled_var.get():
                    has_result = False
                    val = self._safe_str(row.get("view_point", ""))
                    if val and val != "nan" and val.strip():
                        has_result = True
                    if has_result:
                        stats["skip"] += 1
                        self._update_ui_stats(idx+1, total_rows, stats)
                        continue

                # 'bad' 라벨 체크 (기본값: True)
                if self.skip_bad_label_var.get():
                    human_label = self._safe_str(row.get("IMG_S1_휴먼라벨", "")).lower()
                    ai_label = self._safe_str(row.get("IMG_S1_AI라벨", "")).lower()
                    
                    if human_label == "bad" or ai_label == "bad":
                        self._log(f"[Row {idx+1}] 'bad' 라벨이 있어 건너뜁니다. (휴먼라벨: {row.get('IMG_S1_휴먼라벨', '')}, AI라벨: {row.get('IMG_S1_AI라벨', '')})")
                        stats["skip"] += 1
                        self._update_ui_stats(idx+1, total_rows, stats)
                        continue

                # 누끼 이미지 경로 확인
                thumbnail_path = self._safe_str(row.get("IMG_S1_누끼", ""))
                if not thumbnail_path or thumbnail_path == "nan":
                    self._log(f"[Row {idx+1}] IMG_S1_누끼 경로가 비어있습니다. 건너뜁니다.")
                    stats["fail"] += 1
                    self._update_ui_stats(idx+1, total_rows, stats)
                    continue

                # 절대 경로 확인
                if not os.path.isabs(thumbnail_path):
                    # 상대 경로인 경우 처리 (필요시)
                    self._log(f"[Row {idx+1}] 경고: 상대 경로입니다. 절대 경로를 사용하세요: {thumbnail_path}")
                
                if not os.path.exists(thumbnail_path):
                    self._log(f"[Row {idx+1}] 이미지 파일을 찾을 수 없습니다: {thumbnail_path}")
                    stats["fail"] += 1
                    self._update_ui_stats(idx+1, total_rows, stats)
                    continue

                # 실행
                try:
                    # API 호출
                    effort = self.effort_var.get().strip()
                    # 캐싱 최적화 모드 사용
                    result_data, resp = call_image_analysis_api(
                        use_cache_optimization=CACHE_MODE_CORE,
                        image_path=thumbnail_path,
                        model=model,
                        api_key=api_key,
                        reasoning_effort=effort if effort != "none" else None
                    )

                    # 필드 검증 및 저장
                    for col in result_cols:
                        if col in result_data:
                            # is_flat_lay는 boolean이므로 문자열로 변환
                            val = result_data[col]
                            if col == "is_flat_lay":
                                df.at[idx, col] = str(val).lower() if isinstance(val, bool) else str(val)
                            else:
                                df.at[idx, col] = str(val)
                        else:
                            df.at[idx, col] = ""
                            self._log(f"[Row {idx+1}] 경고: 필드 '{col}'가 응답에 없습니다.")

                    # 비용 계산
                    i, o, r = self._extract_usage_tokens(resp)
                    cost = self._calc_cost(model, i, o, r)
                    
                    stats["in"] += i
                    stats["out"] += o
                    stats["reason"] += r
                    stats["cost"] += cost
                    stats["api"] += 1
                    stats["success"] += 1
                    
                except Exception as e:
                    self._log(f"[Row {idx+1}] 오류: {e}")
                    import traceback
                    self._log(traceback.format_exc())
                    stats["fail"] += 1

                processed_now += 1
                self._update_ui_stats(idx+1, total_rows, stats)

                # 자동 저장
                if processed_now % 10 == 0:
                    self._save_df_with_backup(df, out_path)
                    self._log(f"💾 자동 저장 ({processed_now}건)")

            finish_dt = get_seoul_now()
            
            # 최종 저장
            self._save_df_with_backup(df, out_path)
            self._log(f"💾 저장 완료: {os.path.basename(out_path)}")

            # 메인 런처 현황판에 I3-1 완료 상태 기록 (중단이 아닌 경우만) - img 상태만 업데이트 (text 상태는 변경하지 않음)
            if not self.stop_requested:
                try:
                    root_name = get_root_filename(input_path)
                    JobManager.update_status(root_name, img_s3_1_msg="I3-1(썸네일분석완료)")
                    self._log(f"[INFO] 런처 상태 업데이트: {root_name} -> I3-1(썸네일분석완료)")
                except Exception as e:
                    self._log(f"[WARN] 런처 연동 실패: {e}")

            msg = "작업 중단됨" if self.stop_requested else "작업 완료됨"
            self.status_msg.set(msg)
            self._show_completion(msg, stats, out_path)

        except Exception as e:
            self._log(f"🔥 오류: {e}")
            import traceback
            self._log(traceback.format_exc())
            messagebox.showerror("오류", str(e))
        finally:
            self.is_running = False
            self.stop_requested = False
            self.btn_start.config(state='normal')
            self.btn_stop.config(state='disabled')

    def _update_ui_stats(self, curr, total, stats):
        pct = (curr / total) * 100
        self.pb['value'] = pct
        self.stat_progress.set(f"{pct:.1f}%")
        self.stat_count.set(f"{curr} / {total}")
        self.stat_success.set(str(stats['success']))
        self.stat_fail.set(str(stats['fail']))
        self.stat_cost.set(f"${stats['cost']:.4f}")
        
        msg = f"처리 중... {curr}/{total}"
        if stats['skip'] > 0:
            msg += f" (Skip: {stats['skip']})"
        self.status_msg.set(msg)
        self.update_idletasks()

    def _update_timer(self, start_dt):
        if not self.is_running:
            return
        now = get_seoul_now()
        diff = int((now - start_dt).total_seconds())
        h, r = divmod(diff, 3600)
        m, s = divmod(r, 60)
        self.stat_time.set(f"{h:02}:{m:02}:{s:02}")
        self.after(500, lambda: self._update_timer(start_dt))

    def _show_completion(self, title, stats, path):
        msg = (
            f"[{title}]\n\n"
            f"성공: {stats['success']}\n"
            f"실패: {stats['fail']}\n"
            f"건너뜀: {stats['skip']}\n"
            f"총 비용: ${stats['cost']:.4f}\n\n"
            f"파일: {os.path.basename(path)}"
        )
        if messagebox.askyesno("완료", msg + "\n\n파일을 여시겠습니까?"):
            self._open_file(path)


if __name__ == "__main__":
    app = ImageAnalysisGUI()
    app.mainloop()

