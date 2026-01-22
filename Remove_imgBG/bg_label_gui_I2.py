#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
bg_label_gui_v4.py (최종: 별도 윈도우 뷰어 적용)

[업데이트 내역]
1. 상세페이지(HTML) 뷰어 방식 변경: 
   - 기존: 메인 화면 분할 (작업 방해됨)
   - 변경: 메인 윈도우 우측에 '별도 창'으로 팝업 (Dual Window)
2. 메인 윈도우 레이아웃 고정 (HTML 열어도 이미지 크기 변함 없음)
3. 팝업 창 자동 위치 계산 (메인 윈도우 바로 옆에 붙어서 열림)

"""

import os
import sys
import json
import csv
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
import re
from datetime import datetime

# --- Drag & Drop 라이브러리 체크 ---
try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
    DND_AVAILABLE = True
except ImportError:
    DND_AVAILABLE = False
    print("Tip: 'pip install tkinterdnd2'를 입력하면 파일 드래그앤드롭 기능을 사용할 수 있습니다.")

from PIL import Image, ImageTk
from bs4 import BeautifulSoup
import pandas as pd

# GPU 메모리 정리 유틸
def clear_gpu_cache_safe():
    """
    GPU 메모리 캐시를 안전하게 정리합니다.
    PyTorch, ONNX Runtime 등에서 사용하는 CUDA 메모리를 정리합니다.
    """
    cleared = False
    
    # 1. PyTorch CUDA 캐시 정리
    try:
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            torch.cuda.synchronize()
            cleared = True
    except ImportError:
        pass
    except Exception:
        pass
    
    # 2. ONNX Runtime CUDA 캐시 정리 (rembg 등에서 사용)
    try:
        import onnxruntime as ort
        # ONNX Runtime의 CUDA provider 캐시 정리
        providers = ort.get_available_providers()
        if 'CUDAExecutionProvider' in providers:
            # 세션을 종료하고 캐시를 정리하는 것은 어렵지만,
            # Python GC를 통해 간접적으로 정리
            import gc
            gc.collect()
            cleared = True
    except ImportError:
        pass
    except Exception:
        pass
    
    # 3. Python GC 실행 (메모리 정리)
    import gc
    gc.collect()
    
    return cleared

def check_gpu_memory_safe():
    """
    GPU 메모리 사용량을 안전하게 확인합니다.
    Returns: (allocated_mb, reserved_mb, total_mb) 또는 (None, None, None)
    """
    try:
        import torch
        if not torch.cuda.is_available():
            return None, None, None
        
        allocated = torch.cuda.memory_allocated() / 1024**2  # MB
        reserved = torch.cuda.memory_reserved() / 1024**2   # MB
        total = torch.cuda.get_device_properties(0).total_memory / 1024**2  # MB
        return allocated, reserved, total
    except ImportError:
        return None, None, None
    except Exception:
        return None, None, None


# =========================================================
#  [NEW] 버전 관리 및 런처 연동 유틸
# =========================================================
def get_root_filename(filename):
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, T4(완)_I* 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T3_I1.xlsx -> 아디다스.xlsx
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
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_with_images", "_bg"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext

def get_next_version_path(current_path, task_type='image'):
    """I 버전을 1 증가시킨 파일명 생성 (예: T0_I1 -> T0_I2, T4(완)_I2 -> T4(완)_I3)"""
    dir_name = os.path.dirname(current_path)
    base_name = os.path.basename(current_path)
    name_only, ext = os.path.splitext(base_name)

    # T4(완)_I* 또는 일반 _T*_I* 패턴 매칭
    pattern = r"_T(\d+)(\([^)]+\))?_I(\d+)$"
    match = re.search(pattern, name_only, re.IGNORECASE)

    if match:
        current_t = int(match.group(1))
        t_suffix = match.group(2) or ""  # (완) 부분이 있으면 유지
        current_i = int(match.group(3))
        original_name = name_only[:match.start()]
    else:
        current_t = 0
        t_suffix = ""
        current_i = 0
        original_name = name_only

    # 라벨링은 이미지 작업의 연장이므로 I 버전 증가
    new_t = current_t
    new_i = current_i + 1

    # T 부분 유지 (예: T4(완) 또는 T4)
    new_filename = f"{original_name}_T{new_t}{t_suffix}_I{new_i}{ext}"
    return os.path.join(dir_name, new_filename)

class JobManager:
    DB_FILE = None
    @classmethod
    def find_db_path(cls):
        if cls.DB_FILE and os.path.exists(cls.DB_FILE): return cls.DB_FILE
        current_dir = os.path.dirname(os.path.abspath(__file__))
        search_dirs = [current_dir, os.path.abspath(os.path.join(current_dir, "..")), os.path.abspath(os.path.join(current_dir, "..", ".."))]
        for d in search_dirs:
            target = os.path.join(d, "job_history.json")
            if os.path.exists(target):
                cls.DB_FILE = target
                return target
        return os.path.join(current_dir, "job_history.json")

    @classmethod
    def update_status(cls, filename, text_msg=None, img_msg=None):
        db_path = cls.find_db_path()
        try:
            if os.path.exists(db_path):
                with open(db_path, 'r', encoding='utf-8') as f: data = json.load(f)
            else: data = {}
        except: data = {}

        now = datetime.now().strftime("%m-%d %H:%M")
        if filename not in data:
            data[filename] = {"start_time": datetime.now().strftime("%Y-%m-%d %H:%M"), "text_status": "대기", "image_status": "대기"}

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
        except Exception as e: print(f"[JobManager Error] {e}")

# -------------------------------------------------------------
# Pillow 10+ 대응
# -------------------------------------------------------------
if not hasattr(Image, "ANTIALIAS"):
    try:
        from PIL import Image as _Img
        if hasattr(_Img, "Resampling"):
            Image.ANTIALIAS = _Img.Resampling.LANCZOS
        else:
            Image.ANTIALIAS = _Img.LANCZOS
    except Exception:
        pass

# HTML 렌더링용
try:
    from tkhtmlview import HTMLScrolledText
    HTML_AVAILABLE = True
except Exception:
    HTML_AVAILABLE = False


# --------------------------------------------------------------------
#  [변경 없음] 데이터 처리 로직 유지
# --------------------------------------------------------------------
NOISE_URL_KEYWORDS = ["logo", "notice", "event", "delivery", "shipping", "banner", "coupon", "gift", "guide", "info"]

def _parse_px(value: str) -> Optional[int]:
    if not value: return None
    v = value.strip()
    m = re.search(r"(\d+)", v)
    if not m: return None
    try: return int(m.group(1))
    except Exception: return None

def clean_detail_html(raw_html: str) -> str:
    if not isinstance(raw_html, str) or not raw_html.strip(): return "<p>(상세설명 없음)</p>"
    try: soup = BeautifulSoup(raw_html, "html.parser")
    except Exception: return raw_html
    for tag in soup(["script", "style"]): tag.decompose()
    for img in list(soup.find_all("img")):
        src = (img.get("src") or "").strip()
        if not src:
            img.decompose()
            continue
        lower_src = src.lower()
        if any(key in lower_src for key in NOISE_URL_KEYWORDS):
            img.decompose()
            continue
        w = _parse_px(str(img.get("width") or ""))
        h = _parse_px(str(img.get("height") or ""))
        style = img.get("style") or ""
        m_w = re.search(r"width\s*:\s*([0-9]+)px", style)
        m_h = re.search(r"height\s*:\s*([0-9]+)px", style)
        if m_w and not w: w = int(m_w.group(1))
        if m_h and not h: h = int(m_h.group(1))
        if w is not None and h is not None:
            if (w <= 10 or h <= 10) or (w <= 80 and h <= 80):
                img.decompose()
                continue
    for tag in list(soup.find_all(["p", "div", "span"])):
        if tag.name in ("html", "body"): continue
        if not tag.get_text(strip=True) and not tag.find("img"): tag.decompose()
    return str(soup)

@lru_cache(maxsize=512)
def get_cleaned_html_cached(raw_html: str) -> str: return clean_detail_html(raw_html)

@lru_cache(maxsize=256)
def load_preview_image(path: str, size: int):
    try: img = Image.open(path).convert("RGB")
    except Exception: return None
    w, h = img.size
    if w == 0 or h == 0: return None
    scale = min(size / w, size / h)
    new_w = max(1, int(w * scale))
    new_h = max(1, int(h * scale))
    img_resized = img.resize((new_w, new_h), Image.LANCZOS)
    return img_resized

# 상수 정의
CATEGORY_COL, RESULT_NAME_COL, ORIG_NAME_COL, MARKET_NAME_COL = "카테고리명", "ST1_결과상품명", "원본상품명", "마켓상품명"
CODE_COL, DETAIL_HTML_COL = "상품코드", "본문상세설명"
INPUT_PATH_COL, OUTPUT_PATH_COL, INPUT_REL_COL, OUTPUT_REL_COL = "input_abs", "output_abs", "input_rel", "output_rel"
LABEL_COL, NOTE_COL = "IMG_S1_휴먼라벨", "IMG_S1_휴먼노트"

LABEL_VALUE_GOOD, LABEL_VALUE_MEDIUM, LABEL_VALUE_BAD = "good", "medium", "bad"
LABEL_VALUE_TO_KO = {LABEL_VALUE_GOOD: "좋음", LABEL_VALUE_MEDIUM: "애매", LABEL_VALUE_BAD: "불량"}
LABEL_KO_TO_VALUE = {v: k for k, v in LABEL_VALUE_TO_KO.items()}

# 엑셀 파싱 로직
def safe_get(d: Dict[str, Any], key: str, default: str = "") -> str:
    v = d.get(key, default)
    return str(v) if v is not None and str(v).lower() != "nan" else default

def _cell_to_str(v: Any) -> str: return str(v) if v is not None and str(v).lower() != "nan" else ""

def _build_rows_from_excel(path: str) -> List[Dict[str, Any]]:
    df = pd.read_excel(path)
    if df.empty: return []
    
    def find_col(cands):
        for c in cands:
            if c in df.columns: return c
        return None

    c_cat = find_col([CATEGORY_COL, "카테고리명", "카테고리"])
    c_res = find_col([RESULT_NAME_COL, "ST1_결과상품명", "결과상품명"])
    c_orig = find_col([ORIG_NAME_COL, "원본상품명", "공급사상품명"])
    c_mkt = find_col([MARKET_NAME_COL, "마켓상품명", "노출상품명"])
    c_code = find_col([CODE_COL, "상품코드", "판매자관리코드1"])
    c_html = find_col([DETAIL_HTML_COL, "본문상세설명", "상세설명", "상세페이지"])
    c_in = find_col([INPUT_PATH_COL, "썸네일경로", "대표이미지"])
    c_out = find_col([OUTPUT_PATH_COL, "IMG_S1_누끼", "썸네일_BG"])

    rows = []
    for i in range(len(df)):
        r = {}
        r[CATEGORY_COL] = _cell_to_str(df.at[i, c_cat]) if c_cat else ""
        r[RESULT_NAME_COL] = _cell_to_str(df.at[i, c_res]) if c_res else ""
        r[ORIG_NAME_COL] = _cell_to_str(df.at[i, c_orig]) if c_orig else ""
        r[MARKET_NAME_COL] = _cell_to_str(df.at[i, c_mkt]) if c_mkt else ""
        r[CODE_COL] = _cell_to_str(df.at[i, c_code]) if c_code else ""
        r[DETAIL_HTML_COL] = _cell_to_str(df.at[i, c_html]) if c_html else ""
        r[INPUT_PATH_COL] = _cell_to_str(df.at[i, c_in]) if c_in else ""
        r[OUTPUT_PATH_COL] = _cell_to_str(df.at[i, c_out]) if c_out else ""
        r[LABEL_COL] = _cell_to_str(df.at[i, LABEL_COL]) if LABEL_COL in df.columns else ""
        r[NOTE_COL] = _cell_to_str(df.at[i, NOTE_COL]) if NOTE_COL in df.columns else ""
        rows.append(r)
    return rows

def load_mapping_file(path: str) -> List[Dict[str, Any]]:
    if not path.lower().endswith(".xlsx"): raise ValueError(".xlsx 파일만 지원")
    return _build_rows_from_excel(path)

   
def save_mapping_file(source_path: str, target_path: str, rows: List[Dict[str, Any]]):
    # 원본 구조 유지를 위해 source 읽기
    df = pd.read_excel(source_path)
    n = min(len(df), len(rows))

    if LABEL_COL not in df.columns: df[LABEL_COL] = ""
    else: df[LABEL_COL] = df[LABEL_COL].astype(object)

    if NOTE_COL not in df.columns: df[NOTE_COL] = ""
    else: df[NOTE_COL] = df[NOTE_COL].astype(object)

    for i in range(n):
        r = rows[i]
        df.at[i, LABEL_COL] = r.get(LABEL_COL, "")
        df.at[i, NOTE_COL] = r.get(NOTE_COL, "")

    # [중요] target_path로 저장
    df.to_excel(target_path, index=False)
    return target_path


# --------------------------------------------------------------------
#  [UI 테마] 다크모드 색상
# --------------------------------------------------------------------
COLOR_BG_MAIN = "#202124"
COLOR_BG_CARD = "#292a2d"
COLOR_FG_TEXT = "#e8eaed"
COLOR_FG_SUB  = "#9aa0a6"
COLOR_BORDER  = "#3c4043"
COLOR_BTN     = "#303134"
COLOR_ACCENT  = "#8ab4f8"
COLOR_INPUT_BG = "#3c4043"

# --------------------------------------------------------------------
#  [NEW] 툴팁 클래스 추가
# --------------------------------------------------------------------
class CreateToolTip(object):
    """
    위젯에 마우스를 올리면 툴팁(설명)을 띄워주는 클래스
    """
    def __init__(self, widget, text='widget info'):
        self.waittime = 500     # 0.5초 후 표시
        self.wraplength = 300   # 툴팁 너비 제한
        self.widget = widget
        self.text = text
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)
        self.widget.bind("<ButtonPress>", self.leave)
        self.id = None
        self.tw = None

    def enter(self, event=None):
        self.schedule()

    def leave(self, event=None):
        self.unschedule()
        self.hidetip()

    def schedule(self):
        self.unschedule()
        self.id = self.widget.after(self.waittime, self.showtip)

    def unschedule(self):
        id = self.id
        self.id = None
        if id:
            self.widget.after_cancel(id)

    def showtip(self, event=None):
        x = y = 0
        x, y, cx, cy = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 20
        # creates a toplevel window
        self.tw = tk.Toplevel(self.widget)
        self.tw.wm_overrideredirect(True) # 타이틀바 제거
        self.tw.wm_geometry("+%d+%d" % (x, y))
        
        label = tk.Label(self.tw, text=self.text, justify='left',
                       background="#ffffe0", relief='solid', borderwidth=1,
                       wraplength = self.wraplength, font=("맑은 고딕", 9, "normal"))
        label.pack(ipadx=4, ipady=2)

    def hidetip(self):
        tw = self.tw
        self.tw= None
        if tw:
            tw.destroy()
# --------------------------------------------------------------------
#  메인 GUI 클래스
# --------------------------------------------------------------------
class BgLabelApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("BG 라벨링 도구 - Dark Mode (Windowed)")
        # 1. 화면 크기 줄임 (1400x950 -> 1250x800)
        self.root.geometry("1000x650")
        
        # 폰트
        self.font_main = ("맑은 고딕", 10)
        self.font_bold = ("맑은 고딕", 10, "bold")
        self.font_large = ("맑은 고딕", 14, "bold")
        self.font_small = ("맑은 고딕", 9)

        self._setup_theme()
        
        # DnD
        if DND_AVAILABLE:
            self.root.drop_target_register(DND_FILES)
            self.root.dnd_bind('<<Drop>>', self.on_drop)

        # Data
        self.file_path = None
        self.target_path = None  # [추가] 저장될 경로 변수
        self.rows = []
        self.filtered_indices = []
        self.current_index = 0
        
        # 별도 창 관리를 위한 변수
        self.html_window: Optional[tk.Toplevel] = None
        self.html_view_widget = None

        # 이미지 사이즈
        self.preview_size = 400
        self.left_photo = None
        self.right_photo = None

        # Variables
        self.label_filter_var = tk.StringVar(value="unlabeled")
        self.current_label_var = tk.StringVar(value="-")
        self.path_var = tk.StringVar(value=" 파일을 드래그하거나 열어주세요.")
        self.position_var = tk.StringVar(value="0 / 0")
        self.base_dir_var = tk.StringVar(value="")
        self.mapping_dir = None
        self.autosave_var = tk.BooleanVar(value=True)
        self.change_since_save = 0
        
        # 로딩 상태 관리
        self.is_loading = False
        self.loading_label = None
        self.label_buttons = []  # 라벨 버튼들을 저장할 리스트

        self._build_ui()
        self._bind_keys()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _setup_theme(self):
        self.root.configure(bg=COLOR_BG_MAIN)
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("TCombobox", background=COLOR_INPUT_BG, foreground=COLOR_FG_TEXT, 
                        fieldbackground=COLOR_INPUT_BG, arrowcolor=COLOR_FG_TEXT)
        style.map("TCombobox", fieldbackground=[("readonly", COLOR_INPUT_BG)], 
                  selectbackground=[("readonly", COLOR_INPUT_BG)], 
                  selectforeground=[("readonly", COLOR_FG_TEXT)])

    def _build_ui(self):
        # 전체 레이아웃 (2열 구조: 이미지 | 컨트롤) 
        # HTML은 별도 창이므로 여기선 공간 차지 안함
        self.root.columnconfigure(0, weight=1) # Main (Image)
        self.root.columnconfigure(1, weight=0) # Controls (Fixed)
        self.root.rowconfigure(1, weight=1)

        # === 1. Top Bar ===
        top_frame = tk.Frame(self.root, bg=COLOR_BG_CARD, height=50)
        top_frame.grid(row=0, column=0, columnspan=2, sticky="ew")
        top_frame.columnconfigure(1, weight=1)


        # tk.Button(top_frame, text="📂 엑셀 열기", command=self.on_open_file,
        #           bg=COLOR_BTN, fg=COLOR_FG_TEXT, font=self.font_bold, relief="flat", padx=10, pady=5).grid(row=0, column=0, padx=10, pady=5)


        # [툴팁 추가] 엑셀 열기
        btn_open = tk.Button(top_frame, text="📂 엑셀 열기", command=self.on_open_file,
                  bg=COLOR_BTN, fg=COLOR_FG_TEXT, font=self.font_bold, relief="flat", padx=10, pady=5)
        btn_open.grid(row=0, column=0, padx=10, pady=5)
        CreateToolTip(btn_open, "작업할 엑셀 파일(.xlsx)을 불러옵니다.")

        tk.Label(top_frame, textvariable=self.path_var, bg=COLOR_BG_CARD, fg=COLOR_ACCENT, font=self.font_main, anchor="w").grid(row=0, column=1, sticky="ew", padx=10)

        filter_frame = tk.Frame(top_frame, bg=COLOR_BG_CARD)
        filter_frame.grid(row=0, column=2, padx=10)
        tk.Label(filter_frame, text="필터:", bg=COLOR_BG_CARD, fg=COLOR_FG_SUB, font=self.font_small).pack(side="left")
        
        # [툴팁 추가] 필터
        self.filter_combo = ttk.Combobox(filter_frame, width=10, state="readonly", textvariable=self.label_filter_var, values=["전체", "unlabeled", "좋음", "애매", "불량"])
        self.filter_combo.pack(side="left", padx=5)
        self.filter_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filter())
        CreateToolTip(self.filter_combo, "작업할 항목을 필터링합니다.\n'unlabeled' 선택 시 아직 라벨링 안 한 상품만 보여줍니다.")

        tk.Label(top_frame, textvariable=self.position_var, bg=COLOR_BTN, fg=COLOR_FG_TEXT, font=self.font_bold, padx=10).grid(row=0, column=3, padx=10)

        # === 2. Center Area (Image Comparison) ===
        self.center_frame = tk.Frame(self.root, bg=COLOR_BG_MAIN)
        self.center_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        self.center_frame.columnconfigure(0, weight=1)
        self.center_frame.rowconfigure(1, weight=1) # 이미지 영역 최대 확장

        # (1) Info Header
        info_header = tk.Frame(self.center_frame, bg=COLOR_BG_MAIN)
        info_header.grid(row=0, column=0, sticky="ew", pady=(0, 5))
        self.title_label = tk.Label(info_header, text="", font=self.font_large, bg=COLOR_BG_MAIN, fg=COLOR_FG_TEXT, anchor="w")
        self.title_label.pack(fill="x")
        self.subtitle_label = tk.Label(info_header, text="", font=self.font_main, bg=COLOR_BG_MAIN, fg=COLOR_FG_SUB, anchor="w")
        self.subtitle_label.pack(fill="x")

        # (2) Image Canvas Area
        img_container = tk.Frame(self.center_frame, bg=COLOR_BG_CARD, bd=1, relief="solid")
        img_container.grid(row=1, column=0, sticky="nsew")
        img_container.columnconfigure(0, weight=1)
        img_container.columnconfigure(1, weight=1)
        img_container.rowconfigure(0, weight=1)

        self.left_canvas = tk.Canvas(img_container, bg="#000000", highlightthickness=0)
        self.left_canvas.grid(row=0, column=0, sticky="nsew", padx=1, pady=1)
        tk.Label(img_container, text="원본", bg="#000000", fg="white", font=self.font_small).grid(row=0, column=0, sticky="nw", padx=5, pady=5)

        self.right_canvas = tk.Canvas(img_container, bg="#000000", highlightthickness=0)
        self.right_canvas.grid(row=0, column=1, sticky="nsew", padx=1, pady=1)
        tk.Label(img_container, text="결과(누끼)", bg="#000000", fg="white", font=self.font_small).grid(row=0, column=1, sticky="nw", padx=5, pady=5)
        
        # 로딩 인디케이터 (이미지 영역 중앙에 오버레이)
        self.loading_overlay = tk.Label(img_container, text="처리 중...", 
                                        bg="#000000", fg="#8ab4f8", 
                                        font=("맑은 고딕", 16, "bold"),
                                        relief="flat")
        self.loading_overlay.place(relx=0.5, rely=0.5, anchor="center")
        self.loading_overlay.place_forget()  # 초기에는 숨김

        # === 3. Right Control Panel ===
        self.ctrl_panel = tk.Frame(self.root, bg=COLOR_BG_CARD, width=280, padx=15, pady=15)
        self.ctrl_panel.grid(row=1, column=1, sticky="ns", padx=(0, 10), pady=10)
        self.ctrl_panel.grid_propagate(False)

        # 라벨링
        tk.Label(self.ctrl_panel, text="현재 라벨", font=self.font_small, bg=COLOR_BG_CARD, fg=COLOR_FG_SUB).pack(anchor="w")
        tk.Label(self.ctrl_panel, textvariable=self.current_label_var, font=("맑은 고딕", 22, "bold"), bg=COLOR_BG_CARD, fg=COLOR_ACCENT).pack(anchor="w", pady=(0, 15))

        btn_opt = {"bg": COLOR_BTN, "fg": COLOR_FG_TEXT, "font": self.font_bold, "relief": "flat", "height": 2, "cursor": "hand2"}

        # [툴팁 추가] 좋음
        btn_good = tk.Button(self.ctrl_panel, text="[1] 좋음 (Good)", command=lambda: self.set_label(LABEL_VALUE_GOOD), **btn_opt)
        btn_good.pack(fill="x", pady=4)
        CreateToolTip(btn_good, "✅ 바로 썸네일 사용 가능\n(오픈마켓 규정에 맞음 또는 좋은 이미지)")
        self.label_buttons.append(btn_good)

        # [툴팁 추가] 애매
        btn_mid = tk.Button(self.ctrl_panel, text="[2] 애매 (Mid)", command=lambda: self.set_label(LABEL_VALUE_MEDIUM), **btn_opt)
        btn_mid.pack(fill="x", pady=4)
        CreateToolTip(btn_mid, "⚠️ 약간 부족하나 사용 가능\n(오픈마켓 기준 위반 X, 사용은 가능해야 함)")
        self.label_buttons.append(btn_mid)

        # [툴팁 추가] 불량
        btn_bad = tk.Button(self.ctrl_panel, text="[3] 불량 (Bad)", command=lambda: self.set_label(LABEL_VALUE_BAD), **btn_opt)
        btn_bad.pack(fill="x", pady=4)
        CreateToolTip(btn_bad, "❌ 사용 불가\n- 원본 이미지 불량\n- 누끼 품질 이상\n- 비율/중앙정렬 문제")
        self.label_buttons.append(btn_bad)

        tk.Frame(self.ctrl_panel, bg=COLOR_BORDER, height=1).pack(fill="x", pady=15)
        
        # [툴팁 추가] 삭제
        btn_del = tk.Button(self.ctrl_panel, text="[0] 라벨 삭제", command=self.clear_label, bg=COLOR_BG_CARD, fg="#ff6b6b", relief="flat")
        btn_del.pack(fill="x")
        CreateToolTip(btn_del, "현재 라벨을 초기화합니다.")
        self.label_buttons.append(btn_del)
        
        # 로딩 상태 표시 레이블 (컨트롤 패널 상단)
        self.loading_status_label = tk.Label(self.ctrl_panel, text="", 
                                             bg=COLOR_BG_CARD, fg=COLOR_ACCENT, 
                                             font=("맑은 고딕", 10, "bold"))
        self.loading_status_label.pack(anchor="w", pady=(0, 5))

        
        # [툴팁 추가] HTML 버튼
        self.btn_toggle_html = tk.Button(self.ctrl_panel, text="📄 상세페이지 창 열기", command=self.toggle_html_window,
                                         bg=COLOR_BTN, fg=COLOR_FG_TEXT, font=self.font_bold, relief="flat", height=2)
        self.btn_toggle_html.pack(fill="x")
        CreateToolTip(self.btn_toggle_html, "상세페이지를 우측 별도 창으로 띄웁니다.\n(듀얼 모니터나 넓은 화면에서 유용합니다)")
        
        tk.Label(self.ctrl_panel, text="* 메인화면 옆 새 창으로 뜸", font=("맑은 고딕", 8), bg=COLOR_BG_CARD, fg=COLOR_FG_SUB).pack(anchor="e", pady=(2,0))


        # [툴팁 추가] 메모
        self.memo_entry = tk.Entry(self.ctrl_panel, bg=COLOR_INPUT_BG, fg=COLOR_FG_TEXT, insertbackground="white", relief="flat")
        self.memo_entry.pack(fill="x", ipady=4, pady=(0, 10))
        CreateToolTip(self.memo_entry, "특이사항이 있다면 여기에 적어주세요.")

        # [툴팁 추가] 자동저장
        chk_autosave = tk.Checkbutton(self.ctrl_panel, text="자동 저장", variable=self.autosave_var, bg=COLOR_BG_CARD, fg=COLOR_FG_TEXT, selectcolor=COLOR_BG_CARD, activebackground=COLOR_BG_CARD)
        chk_autosave.pack(anchor="w", pady=5)
        CreateToolTip(chk_autosave, "체크 시 10건마다 자동으로 저장합니다.")

        # [툴팁 추가] 저장
        btn_save = tk.Button(self.ctrl_panel, text="💾 저장 (Ctrl+S)", command=self.on_save, bg=COLOR_ACCENT, fg="white", font=self.font_bold, relief="flat", height=2)
        btn_save.pack(fill="x")
        CreateToolTip(btn_save, "현재까지 작업한 내용을 엑셀 파일에 덮어씁니다.")

        # === 4. Bottom Navigation ===
        nav_frame = tk.Frame(self.root, bg=COLOR_BG_MAIN, height=45)
        nav_frame.grid(row=2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 10))
        
        tk.Button(nav_frame, text="◀ 이전", command=self.prev_item, bg=COLOR_BTN, fg=COLOR_FG_TEXT, font=self.font_bold, relief="flat", width=12).pack(side="left")
        tk.Button(nav_frame, text="다음 ▶", command=self.next_item, bg=COLOR_BTN, fg=COLOR_FG_TEXT, font=self.font_bold, relief="flat", width=12).pack(side="left", padx=10)
        
        j_frame = tk.Frame(nav_frame, bg=COLOR_BG_MAIN)
        j_frame.pack(side="right")
        self.goto_entry = tk.Entry(j_frame, width=5, bg=COLOR_INPUT_BG, fg=COLOR_FG_TEXT, justify="center")
        self.goto_entry.pack(side="left", padx=5)
        self.goto_entry.bind("<Return>", lambda e: self.goto_index())
        tk.Button(j_frame, text="Go", command=self.goto_index, bg=COLOR_BTN, fg=COLOR_FG_TEXT, relief="flat").pack(side="left")

    # ------------------------------------------------------------------
    #  [UI 로직] 별도 창(Toplevel) 관리
    # ------------------------------------------------------------------
    def toggle_html_window(self):
        # 이미 열려있으면 닫기
        if self.html_window is not None and tk.Toplevel.winfo_exists(self.html_window):
            self.html_window.destroy()
            self.html_window = None
            self.html_view_widget = None
            self.btn_toggle_html.config(text="📄 상세페이지 창 열기", bg=COLOR_BTN)
        else:
            # 창 생성
            self.html_window = tk.Toplevel(self.root)
            self.html_window.title("상세페이지 뷰어")
            
            # 메인 윈도우 우측에 붙이기
            root_x = self.root.winfo_x()
            root_y = self.root.winfo_y()
            root_w = self.root.winfo_width()
            root_h = self.root.winfo_height()
            
            # 새 창의 좌표 및 크기
            new_x = root_x + root_w + 10 # 10px 간격
            new_y = root_y
            new_w = 860
            # new_h = root_h # gui창과 똑같게
            new_h = 950
            self.html_window.geometry(f"{new_w}x{new_h}+{new_x}+{new_y}")
            
            # 뷰어 위젯 배치
            if HTML_AVAILABLE:
                self.html_view_widget = HTMLScrolledText(self.html_window, html="<p>로딩 중...</p>")
                self.html_view_widget.pack(fill="both", expand=True)
            else:
                self.html_view_widget = tk.Text(self.html_window)
                self.html_view_widget.pack(fill="both", expand=True)

            self.btn_toggle_html.config(text="📄 상세페이지 창 닫기", bg=COLOR_ACCENT)
            
            # 창 닫힐 때 이벤트 처리
            self.html_window.protocol("WM_DELETE_WINDOW", self.on_close_html_window)
            
            # 내용 렌더링
            self.render_html()

    def on_close_html_window(self):
        if self.html_window:
            self.html_window.destroy()
        self.html_window = None
        self.html_view_widget = None
        self.btn_toggle_html.config(text="📄 상세페이지 창 열기", bg=COLOR_BTN)

    def render_html(self):
        # 뷰어 창이 없으면 스킵
        if not self.html_window or not self.html_view_widget:
            return

        if not self.filtered_indices:
            content = "<p>표시할 항목이 없습니다.</p>"
        else:
            row = self.rows[self.filtered_indices[self.current_index]]
            raw = safe_get(row, DETAIL_HTML_COL)
            content = get_cleaned_html_cached(raw) if raw.strip() else "<p>내용 없음</p>"

        if HTML_AVAILABLE:
            try:
                self.html_view_widget.set_html(content)
            except:
                self.html_view_widget.set_html(f"<pre>{content}</pre>")
        else:
            self.html_view_widget.config(state="normal")
            self.html_view_widget.delete("1.0", tk.END)
            self.html_view_widget.insert("1.0", content)
            self.html_view_widget.config(state="disabled")

    # ------------------------------------------------------------------
    #  [기능 로직]
    # ------------------------------------------------------------------
    def _bind_keys(self):
        self.root.bind("<Left>", lambda e: self.prev_item())
        self.root.bind("<Right>", lambda e: self.next_item())
        self.root.bind("1", lambda e: self.set_label(LABEL_VALUE_GOOD))
        self.root.bind("2", lambda e: self.set_label(LABEL_VALUE_MEDIUM))
        self.root.bind("3", lambda e: self.set_label(LABEL_VALUE_BAD))
        self.root.bind("0", lambda e: self.clear_label())
        self.root.bind("<BackSpace>", lambda e: self.clear_label())
        self.root.bind("<Control-s>", lambda e: self.on_save())
        self.root.bind("<space>", lambda e: self.toggle_filter_space())

    def on_drop(self, event):
        files = self.root.tk.splitlist(event.data)
        if files and files[0].lower().endswith(".xlsx"): self.load_file(files[0])

    def on_open_file(self):
        path = filedialog.askopenfilename(
            title="배경 라벨링 엑셀 선택 (I1 또는 I2 버전)",
            filetypes=[("Excel 파일", "*.xlsx")]
        )
        if path:
            # I1 또는 I2 포함 여부 검증
            base_name = os.path.basename(path)
            if not re.search(r"_I[12]", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 I1 또는 I2 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {base_name}\n"
                    f"파일명에 '_I1' 또는 '_I2' 패턴이 포함되어 있어야 합니다."
                )
                return
            self.load_file(path)

    def load_file(self, path):
        # I1 또는 I2 포함 여부 검증
        base_name = os.path.basename(path)
        if not re.search(r"_I[12]", base_name, re.IGNORECASE):
            messagebox.showerror(
                "오류", 
                f"이 도구는 I1 또는 I2 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {base_name}\n"
                f"파일명에 '_I1' 또는 '_I2' 패턴이 포함되어 있어야 합니다."
            )
            return
        
        # I2 파일인 경우, 'IMG_S1_휴먼라벨' 컬럼에 빈 값이 있는지 확인
        is_i2 = bool(re.search(r"_I2", base_name, re.IGNORECASE))
        if is_i2:
            try:
                # 엑셀 파일을 먼저 읽어서 빈 라벨 확인
                df = pd.read_excel(path)
                if LABEL_COL in df.columns:
                    empty_labels = df[LABEL_COL].isna() | (df[LABEL_COL].astype(str).str.strip() == "")
                    if not empty_labels.any():
                        # 모든 라벨이 채워져 있으면 경고
                        if not messagebox.askyesno(
                            "확인",
                            f"선택한 I2 파일의 모든 항목이 이미 라벨링되어 있습니다.\n\n"
                            f"이어서 작업하시겠습니까?"
                        ):
                            return
                else:
                    # LABEL_COL이 없으면 새로 시작
                    messagebox.showinfo(
                        "정보",
                        f"선택한 I2 파일에 '{LABEL_COL}' 컬럼이 없습니다.\n"
                        f"새로 라벨링을 시작합니다."
                    )
            except Exception as e:
                messagebox.showwarning(
                    "경고",
                    f"I2 파일 검증 중 오류가 발생했습니다:\n{e}\n\n"
                    f"계속 진행합니다."
                )
        
        # 작업 시작 전 GPU 메모리 정리
        try:
            allocated, reserved, total = check_gpu_memory_safe()
            if allocated is not None and allocated > 0:
                # GPU 메모리가 사용 중이면 정리
                cleared = clear_gpu_cache_safe()
                if cleared:
                    # 정리 후 다시 확인
                    allocated_after, _, _ = check_gpu_memory_safe()
                    if allocated_after is not None:
                        freed_mb = allocated - allocated_after
                        if freed_mb > 10:  # 10MB 이상 해제된 경우에만 로그
                            print(f"[GPU 정리] 메모리 해제: {freed_mb:.1f}MB (정리 전: {allocated:.1f}MB, 정리 후: {allocated_after:.1f}MB)")
        except Exception as e:
            # GPU 정리 실패해도 작업은 계속 진행
            print(f"[GPU 정리] 경고: {e}")
        
        try:
            self.rows = load_mapping_file(path)
        except Exception as e:
            messagebox.showerror("오류", str(e))
            return
        if not self.rows:
            messagebox.showwarning("주의", "데이터 없음")
            return
        self.file_path = path
        
        # I2 파일인 경우, 같은 파일에 저장 (이어서 작업)
        # I1 파일인 경우, I2로 버전 업
        is_i2 = bool(re.search(r"_I2", base_name, re.IGNORECASE))
        if is_i2:
            # I2 파일이면 같은 파일에 저장 (이어서 작업)
            self.target_path = path
        else:
            # I1 파일이면 I2로 버전 업
            self.target_path = get_next_version_path(path, task_type='image')
        
        self.mapping_dir = os.path.dirname(path)
        self.base_dir_var.set(self.mapping_dir)
        load_preview_image.cache_clear()
        get_cleaned_html_cached.cache_clear()
        self.label_filter_var.set("unlabeled")
        # UI에 저장될 파일명 표시
        target_name = os.path.basename(self.target_path)
        if is_i2:
            self.path_var.set(f"이어서 작업: {target_name}")
        else:
            self.path_var.set(f"원본: {os.path.basename(path)} -> 저장: {target_name}")

        # [NEW] JobManager 시작 알림 - img 상태만 업데이트 (text 상태는 변경하지 않음)
        try:
            root_name = get_root_filename(path)
            JobManager.update_status(root_name, img_msg="I2-1 (진행중)")
        except: pass
        self.apply_filter()

    def apply_filter(self):
        self.filtered_indices = []
        if not self.rows: return
        mode = self.label_filter_var.get()
        for idx, row in enumerate(self.rows):
            val = safe_get(row, LABEL_COL, "")
            if mode == "전체": self.filtered_indices.append(idx)
            elif mode == "unlabeled":
                if val == "": self.filtered_indices.append(idx)
            elif mode in LABEL_KO_TO_VALUE:
                if val == LABEL_KO_TO_VALUE[mode]: self.filtered_indices.append(idx)
            else: self.filtered_indices.append(idx)
        
        self.current_index = 0
        if not self.filtered_indices: self.update_empty_view()
        else: self.show_current()

    def update_empty_view(self):
        self.title_label.config(text="표시할 항목 없음")
        self.subtitle_label.config(text="")
        self.current_label_var.set("-")
        self.left_canvas.delete("all")
        self.right_canvas.delete("all")
        self.position_var.set("0 / 0")
        if self.html_window: self.render_html()
        self._stop_loading()  # 빈 뷰일 때도 로딩 종료

    def show_current(self):
        if not self.filtered_indices: 
            self._stop_loading()
            return
        
        row = self.rows[self.filtered_indices[self.current_index]]
        
        # Info
        title = f"[{safe_get(row, CATEGORY_COL)}] {safe_get(row, ORIG_NAME_COL) or safe_get(row, RESULT_NAME_COL)}"
        self.title_label.config(text=title)
        self.subtitle_label.config(text=f"코드: {safe_get(row, CODE_COL)} | 마켓명: {safe_get(row, MARKET_NAME_COL)}")
        
        # Label
        l_val = safe_get(row, LABEL_COL)
        self.current_label_var.set(LABEL_VALUE_TO_KO.get(l_val, l_val) if l_val else "(없음)")
        self.memo_entry.delete(0, tk.END)
        self.memo_entry.insert(0, safe_get(row, NOTE_COL))
        
        # Images
        self.show_img(self.resolve_path(row, "input"), self.left_canvas, True)
        self.show_img(self.resolve_path(row, "output"), self.right_canvas, False)
        
        # HTML Render (새 창이 떠 있을 경우)
        if self.html_window: self.render_html()
        
        self.position_var.set(f"{self.current_index+1} / {len(self.filtered_indices)}")
        
        # 로딩 종료 (이미지 로딩 완료 후)
        self.root.after(50, self._stop_loading)  # 약간의 딜레이를 두어 이미지 렌더링 완료 보장

    def resolve_path(self, row, kind):
        base = self.base_dir_var.get() or self.mapping_dir or os.getcwd()
        k_abs = INPUT_PATH_COL if kind == "input" else OUTPUT_PATH_COL
        k_rel = INPUT_REL_COL if kind == "input" else OUTPUT_REL_COL
        p_abs, p_rel = safe_get(row, k_abs), safe_get(row, k_rel)
        
        cands = []
        if p_abs: cands.append(p_abs)
        if p_abs and not os.path.isabs(p_abs): cands.append(os.path.join(base, p_abs))
        if p_rel: cands.append(os.path.join(base, p_rel))
        
        for p in cands:
            if p and os.path.exists(p): return p
        return cands[0] if cands else ""

    def show_img(self, path, canvas, is_left):
        canvas.delete("all")
        if not path or not os.path.exists(path):
            canvas.create_text(self.preview_size//2, self.preview_size//2, text="No Image", fill="white")
            return
        img = load_preview_image(path, self.preview_size)
        if not img: return
        photo = ImageTk.PhotoImage(img)
        canvas.create_image(self.preview_size//2, self.preview_size//2, image=photo)
        if is_left: self.left_photo = photo
        else: self.right_photo = photo

    def set_label(self, val):
        if not self.filtered_indices: return
        if self.is_loading: return  # 로딩 중이면 무시
        
        # 로딩 시작
        self._start_loading()
        
        row = self.rows[self.filtered_indices[self.current_index]]
        row[LABEL_COL] = val
        row[NOTE_COL] = self.memo_entry.get().strip()
        self.current_label_var.set(LABEL_VALUE_TO_KO.get(val, val))
        self._maybe_autosave()
        
        # 다음 항목으로 이동 (필터 모드에 따라)
        if self.label_filter_var.get() == "unlabeled":
            # unlabeled 모드: 필터 적용 후 다음 항목으로 자동 이동
            self.apply_filter()
            if self.filtered_indices and self.current_index < len(self.filtered_indices) - 1:
                self.current_index += 1
                self.show_current()
            else:
                self._stop_loading()
        else:
            # 전체 모드: 다음 항목으로 자동 이동
            if self.current_index < len(self.filtered_indices) - 1:
                self.current_index += 1
                self.show_current()
            else:
                self._stop_loading()

    def clear_label(self):
        if not self.filtered_indices: return
        if self.is_loading: return  # 로딩 중이면 무시
        
        # 로딩 시작
        self._start_loading()
        
        row = self.rows[self.filtered_indices[self.current_index]]
        row[LABEL_COL] = ""
        row[NOTE_COL] = self.memo_entry.get().strip()
        self.current_label_var.set("(없음)")
        self._maybe_autosave()
        self.show_current()

    def _start_loading(self):
        """로딩 상태 시작 - 버튼 비활성화 및 로딩 인디케이터 표시"""
        if self.is_loading: return
        self.is_loading = True
        
        # 버튼 비활성화
        for btn in self.label_buttons:
            btn.config(state="disabled", cursor="wait")
        
        # 로딩 인디케이터 표시
        self.loading_overlay.place(relx=0.5, rely=0.5, anchor="center")
        self.loading_status_label.config(text="⏳ 처리 중...")
        self.root.update_idletasks()  # UI 즉시 업데이트

    def _stop_loading(self):
        """로딩 상태 종료 - 버튼 활성화 및 로딩 인디케이터 숨김"""
        if not self.is_loading: return
        self.is_loading = False
        
        # 버튼 활성화
        for btn in self.label_buttons:
            btn.config(state="normal", cursor="hand2")
        
        # 로딩 인디케이터 숨김
        self.loading_overlay.place_forget()
        self.loading_status_label.config(text="")

    def _maybe_autosave(self):
        if self.autosave_var.get():
            self.change_since_save += 1
            if self.change_since_save >= 10:
                try: 
                    # [수정] 원본(file_path) 읽어서 타겟(target_path)에 저장
                    save_mapping_file(self.file_path, self.target_path, self.rows)
                    self.change_since_save = 0
                except: pass

    def on_save(self):
        if self.file_path and self.target_path:
            # [수정] 원본(file_path) 읽어서 타겟(target_path)에 저장
            save_mapping_file(self.file_path, self.target_path, self.rows)
            messagebox.showinfo("저장", f"저장 완료\n[{os.path.basename(self.target_path)}]")
            self.change_since_save = 0

            # [NEW] JobManager 완료 업데이트 - img 상태만 I2-1(휴먼완료)로 업데이트 (text 상태는 변경하지 않음)
            try:
                root_name = get_root_filename(self.file_path)
                # 모든 라벨링이 완료되었는지 확인
                all_labeled = all(safe_get(row, LABEL_COL, "") for row in self.rows)
                if all_labeled:
                    JobManager.update_status(root_name, img_msg="I2-1(휴먼완료)")
                else:
                    JobManager.update_status(root_name, img_msg="I2-1 (저장됨)")
            except: pass

    def prev_item(self):
        if self.is_loading: return  # 로딩 중이면 무시
        if self.current_index > 0: 
            self.current_index -= 1
            self.show_current()
    def next_item(self):
        if self.is_loading: return  # 로딩 중이면 무시
        if self.filtered_indices and self.current_index < len(self.filtered_indices)-1: 
            self.current_index += 1
            self.show_current()
    def goto_index(self):
        try:
            idx = int(self.goto_entry.get())
            if 1 <= idx <= len(self.filtered_indices): self.current_index = idx-1; self.show_current()
        except: pass
    def toggle_filter_space(self):
        self.label_filter_var.set("전체" if self.label_filter_var.get() == "unlabeled" else "unlabeled")
        self.apply_filter()
    def on_close(self):
        # 팝업 창도 닫아주기
        if self.html_window: self.html_window.destroy()
        if self.change_since_save > 0 and messagebox.askyesno("종료", "저장하시겠습니까?"): self.on_save()
        self.root.destroy()

# --------------------------------------------------------------------
if __name__ == "__main__":
    if DND_AVAILABLE: root = TkinterDnD.Tk()
    else: root = tk.Tk()
    app = BgLabelApp(root)
    root.mainloop()