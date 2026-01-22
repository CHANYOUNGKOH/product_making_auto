#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
bg_label_gui.py

배경제거 결과(bg_mapping.csv/json)를 보면서
썸네일 품질 라벨(좋음/애매/불량)을 붙이는 전용 GUI.

고정 컬럼명(정답 컬럼명):
  - 카테고리      : excel_카테고리명
  - ST1_상품명    : excel_ST1_결과상품명
  - 원본상품명    : excel_원본상품명
  - 마켓상품명    : excel_마켓상품명
  - 상품코드      : excel_상품코드
  - 상세설명(HTML): excel_본문상세설명

이미지 경로:
  - 우선: input_abs / output_abs
  - 그 다음: input_rel / output_rel(있다면)
  - 상대경로는 항상 "이미지 기준 폴더"를 기준으로 해석
    (기본값 = bg_mapping 파일이 있는 폴더)

특징:
  - LRU 캐시로 이미지 리사이즈 + HTML 정제 결과 재사용 (되돌리기 시 빠름)
  - pandas 제거, 표준 csv/json 만 사용
  - 인덱스로 점프(Go To) 기능
  - 자동 저장 옵션 (라벨 10건마다 + 종료 시 자동저장)
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

from PIL import Image, ImageTk
from bs4 import BeautifulSoup  # HTML 정제용

# -------------------------------------------------------------
# Pillow 10+ 대응: tkhtmlview 내부에서 사용하는 Image.ANTIALIAS 정의
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

# HTML 렌더링용 (pip install tkhtmlview)
try:
    from tkhtmlview import HTMLScrolledText
    HTML_AVAILABLE = True
except Exception:
    HTML_AVAILABLE = False


# --------------------------------------------------------------------
#  상세설명 노이즈 제거 관련 설정
# --------------------------------------------------------------------
NOISE_URL_KEYWORDS = [
    "logo", "notice", "event", "delivery", "shipping",
    "banner", "coupon", "gift", "guide", "info",
]


def _parse_px(value: str) -> Optional[int]:
    """'20px', ' 30 ' 같은 문자열에서 정수 px만 추출."""
    if not value:
        return None
    v = value.strip()
    m = re.search(r"(\d+)", v)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def clean_detail_html(raw_html: str) -> str:
    """
    상세설명 HTML에서 불필요한 부분(로고/배너/쿠폰/공지/배송 아이콘 등)을 제거하고
    tkhtmlview 에 던질 수 있는 '가벼운' HTML 로 정제.
    """
    if not isinstance(raw_html, str) or not raw_html.strip():
        return "<p>(상세설명 없음)</p>"

    try:
        soup = BeautifulSoup(raw_html, "html.parser")
    except Exception:
        return raw_html

    # 1) script, style 제거
    for tag in soup(["script", "style"]):
        tag.decompose()

    # 2) 이미지 필터링
    for img in list(soup.find_all("img")):
        src = (img.get("src") or "").strip()
        if not src:
            img.decompose()
            continue

        lower_src = src.lower()

        # (1) URL 키워드로 노이즈 판단 (로고, 배너, 쿠폰, 공지 등)
        if any(key in lower_src for key in NOISE_URL_KEYWORDS):
            img.decompose()
            continue

        # (2) 너무 작은 아이콘으로 추정 되는 경우 제거
        w = _parse_px(str(img.get("width") or ""))
        h = _parse_px(str(img.get("height") or ""))

        style = img.get("style") or ""
        m_w = re.search(r"width\s*:\s*([0-9]+)px", style)
        m_h = re.search(r"height\s*:\s*([0-9]+)px", style)
        if m_w and not w:
            w = int(m_w.group(1))
        if m_h and not h:
            h = int(m_h.group(1))

        # 작은 아이콘/뱃지 → 제거
        if w is not None and h is not None:
            if (w <= 10 or h <= 10) or (w <= 80 and h <= 80):
                img.decompose()
                continue

    # 3) 완전히 비어있는 p/div/span 제거
    for tag in list(soup.find_all(["p", "div", "span"])):
        if tag.name in ("html", "body"):
            continue
        if not tag.get_text(strip=True) and not tag.find("img"):
            tag.decompose()

    return str(soup)


# --- HTML 정제 결과 캐시 -------------------------------------------------
@lru_cache(maxsize=512)
def get_cleaned_html_cached(raw_html: str) -> str:
    return clean_detail_html(raw_html)


# --- 이미지 리사이즈 캐시 ------------------------------------------------
@lru_cache(maxsize=256)
def load_preview_image(path: str, size: int):
    """
    path 에서 이미지를 열어 size x size 안에 맞게 축소한 PIL.Image 반환.
    실패 시 None.
    """
    try:
        img = Image.open(path).convert("RGB")
    except Exception:
        return None

    w, h = img.size
    if w == 0 or h == 0:
        return None

    scale = min(size / w, size / h)
    new_w = max(1, int(w * scale))
    new_h = max(1, int(h * scale))
    img_resized = img.resize((new_w, new_h), Image.LANCZOS)
    return img_resized


# --------------------------------------------------------------------
#  고정 컬럼명 정의
# --------------------------------------------------------------------
CATEGORY_COL      = "excel_카테고리명"
RESULT_NAME_COL   = "excel_ST1_결과상품명"
ORIG_NAME_COL     = "excel_원본상품명"
MARKET_NAME_COL   = "excel_마켓상품명"
CODE_COL          = "excel_상품코드"
DETAIL_HTML_COL   = "excel_본문상세설명"

INPUT_PATH_COL    = "input_abs"
OUTPUT_PATH_COL   = "output_abs"
INPUT_REL_COL     = "input_rel"
OUTPUT_REL_COL    = "output_rel"

LABEL_COL         = "human_label"
NOTE_COL          = "human_notes"

LABEL_VALUE_GOOD   = "good"
LABEL_VALUE_MEDIUM = "medium"
LABEL_VALUE_BAD    = "bad"

LABEL_VALUE_TO_KO = {
    LABEL_VALUE_GOOD:   "좋음",
    LABEL_VALUE_MEDIUM: "애매",
    LABEL_VALUE_BAD:    "불량",
}
LABEL_KO_TO_VALUE = {v: k for k, v in LABEL_VALUE_TO_KO.items()}


# --------------------------------------------------------------------
#  유틸
# --------------------------------------------------------------------
def safe_get(d: Dict[str, Any], key: str, default: str = "") -> str:
    v = d.get(key, default)
    if v is None:
        return default
    s = str(v)
    if s.lower() == "nan":
        return default
    return s


def load_mapping_file(path: str) -> List[Dict[str, Any]]:
    """
    CSV: 표준 csv 모듈 사용 (pandas 미사용)
    JSON: json.load 사용
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".tsv"]:
        rows: List[Dict[str, Any]] = []
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                row: Dict[str, Any] = {}
                for k, v in raw.items():
                    if v is None:
                        row[k] = ""
                    else:
                        s = str(v)
                        row[k] = "" if s.lower() == "nan" else s
                rows.append(row)
        return rows

    elif ext in [".json", ".jsonl"]:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = data.get("data", [])
        if not isinstance(data, list):
            raise ValueError("json 형식이 올바르지 않습니다 (리스트가 아님)")
        rows: List[Dict[str, Any]] = []
        for row in data:
            clean: Dict[str, Any] = {}
            for k, v in row.items():
                clean[k] = "" if v is None else str(v)
            rows.append(clean)
        return rows
    else:
        raise ValueError("지원하지 않는 파일 형식입니다 (csv / json 만 가능)")


def save_mapping_file(path: str, rows: List[Dict[str, Any]]):
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".tsv"]:
        all_keys = set()
        for r in rows:
            all_keys.update(r.keys())
        fieldnames = sorted(all_keys)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
    elif ext in [".json", ".jsonl"]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
    else:
        raise ValueError("지원하지 않는 파일 형식입니다 (csv / json 만 가능)")


# --------------------------------------------------------------------
#  메인 GUI 클래스
# --------------------------------------------------------------------
class BgLabelApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("BG 라벨링 도구 (bg_mapping 기반)")
        # 전체화면 X, 고정 기본 크기
        self.root.geometry("1400x900")

        # 데이터
        self.file_path: Optional[str] = None
        self.rows: List[Dict[str, Any]] = []
        self.filtered_indices: List[int] = []
        self.current_index: int = 0

        # 이미지 프리뷰 사이즈
        self.preview_size = 380
        self.left_photo: Optional[ImageTk.PhotoImage] = None
        self.right_photo: Optional[ImageTk.PhotoImage] = None

        # UI 변수
        self.label_filter_var = tk.StringVar(value="unlabeled")  # 시작은 unlabeled
        self.current_label_var = tk.StringVar(value="-")
        self.path_var = tk.StringVar(value="bg_mapping 파일을 열어주세요.")
        self.position_var = tk.StringVar(value="0 / 0")

        # 이미지 기준 폴더 (기본: mapping 파일 폴더)
        self.base_dir_var = tk.StringVar(value="")
        self.mapping_dir: Optional[str] = None

        # 자동 저장 관련
        self.autosave_var = tk.BooleanVar(value=False)
        self.change_since_save = 0

        self._build_ui()
        self._bind_keys()

        # 창 닫기 훅
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    # ------------------------------------------------------------------
    #  UI 구성
    # ------------------------------------------------------------------
    def _build_ui(self):
        self.root.rowconfigure(2, weight=1)
        self.root.columnconfigure(0, weight=1)

        # 상단: 파일/필터/위치 + 이미지 기준 폴더
        top_frame = tk.Frame(self.root, pady=4)
        top_frame.grid(row=0, column=0, sticky="ew")
        top_frame.columnconfigure(1, weight=1)

        # 0행: 파일/필터/위치
        tk.Button(
            top_frame,
            text="bg_mapping 열기(CSV/JSON)",
            command=self.on_open_file,
            width=22,
        ).grid(row=0, column=0, padx=(6, 4), sticky="w")

        tk.Label(top_frame, textvariable=self.path_var, anchor="w").grid(
            row=0, column=1, sticky="ew"
        )

        tk.Label(top_frame, text="라벨 필터:", padx=6).grid(
            row=0, column=2, sticky="e"
        )
        self.filter_combo = ttk.Combobox(
            top_frame,
            width=10,
            state="readonly",
            textvariable=self.label_filter_var,
            values=["전체", "unlabeled", "좋음", "애매", "불량"],
        )
        self.filter_combo.grid(row=0, column=3, padx=(0, 4), sticky="e")
        self.filter_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filter())

        tk.Label(top_frame, textvariable=self.position_var, padx=8).grid(
            row=0, column=4, sticky="e", padx=(0, 8)
        )

        # 1행: 이미지 기준 폴더
        tk.Label(
            top_frame,
            text="이미지 기준 폴더:",
            anchor="w",
        ).grid(row=1, column=0, padx=(6, 0), sticky="w")

        base_entry = tk.Entry(top_frame, textvariable=self.base_dir_var)
        base_entry.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(0, 4))

        tk.Button(
            top_frame,
            text="폴더 선택...",
            command=self.choose_base_dir,
            width=12,
        ).grid(row=1, column=3, sticky="e", padx=(0, 4))

        # 두 번째 줄: 상품 타이틀
        title_frame = tk.Frame(self.root, pady=4)
        title_frame.grid(row=1, column=0, sticky="ew")
        title_frame.columnconfigure(0, weight=1)

        self.title_label = tk.Label(
            title_frame,
            text="",
            font=("맑은 고딕", 14, "bold"),
            anchor="w",
        )
        self.title_label.grid(row=0, column=0, sticky="ew", padx=8)

        self.subtitle_label = tk.Label(
            title_frame,
            text="",
            font=("맑은 고딕", 10),
            fg="#555555",
            anchor="w",
        )
        self.subtitle_label.grid(row=1, column=0, sticky="ew", padx=8)

        # 메인 영역: 좌측(이미지+HTML) / 우측(라벨패널)
        main_frame = tk.Frame(self.root)
        main_frame.grid(row=2, column=0, sticky="nsew", padx=4, pady=4)
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=0)
        main_frame.rowconfigure(0, weight=1)

        # ---------------- 좌측: 이미지 + HTML ----------------
        visual_frame = tk.Frame(main_frame)
        visual_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        visual_frame.rowconfigure(0, weight=1)
        visual_frame.columnconfigure(0, weight=1)

        # 세로 분할 PanedWindow: 위(이미지), 아래(HTML)
        splitter = tk.PanedWindow(visual_frame, orient=tk.VERTICAL, sashrelief="raised")
        splitter.grid(row=0, column=0, sticky="nsew")

        # 위쪽: 이미지 2개
        top_images_frame = tk.Frame(splitter)
        top_images_frame.columnconfigure(0, weight=0)
        top_images_frame.columnconfigure(1, weight=0)

        # 원본 이미지
        left_frame = tk.LabelFrame(top_images_frame, text="원본 이미지", padx=4, pady=4)
        left_frame.grid(row=0, column=0, sticky="n", padx=(0, 2))
        self.left_canvas = tk.Canvas(
            left_frame,
            width=self.preview_size,
            height=self.preview_size,
            bg="#202020",
            highlightthickness=0,
        )
        self.left_canvas.pack()

        # 배경제거 결과
        mid_frame = tk.LabelFrame(top_images_frame, text="배경제거 결과", padx=4, pady=4)
        mid_frame.grid(row=0, column=1, sticky="n", padx=(2, 0))
        self.right_canvas = tk.Canvas(
            mid_frame,
            width=self.preview_size,
            height=self.preview_size,
            bg="#202020",
            highlightthickness=0,
        )
        self.right_canvas.pack()

        # 아래쪽: HTML 상세설명
        html_outer = tk.LabelFrame(splitter, text="상세설명 (HTML)", padx=4, pady=4)
        html_outer.rowconfigure(0, weight=1)
        html_outer.columnconfigure(0, weight=1)

        if HTML_AVAILABLE:
            self.html_view = HTMLScrolledText(
                html_outer,
                html="<p style='color:#888'>상세설명이 여기에 표시됩니다.</p>",
                background="white",
            )
            self.html_view.config(width=110)
            self.html_view.grid(row=0, column=0, sticky="nsew")
        else:
            self.html_view = tk.Text(html_outer, wrap="word")
            self.html_view.grid(row=0, column=0, sticky="nsew")
            self.html_view.insert(
                "1.0",
                "tkhtmlview 패키지가 없어 HTML을 텍스트로만 표시합니다.\n"
                "설치: pip install tkhtmlview",
            )
            self.html_view.config(state="disabled")

        splitter.add(top_images_frame, minsize=self.preview_size + 80)
        splitter.add(html_outer, minsize=150)
        splitter.sash_place(0, 0, self.preview_size + 90)

        # ---------------- 우측: 라벨 / 메모 / 저장 ----------------
        right_panel = tk.Frame(main_frame)
        right_panel.grid(row=0, column=1, sticky="ns", padx=(4, 0))
        right_panel.columnconfigure(0, weight=1)
        right_panel.rowconfigure(0, weight=1)

        label_frame = tk.LabelFrame(right_panel, text="라벨 / 메모", padx=4, pady=4)
        label_frame.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)
        label_frame.columnconfigure(0, weight=1)

        label_top = tk.Frame(label_frame)
        label_top.pack(fill="x", padx=2, pady=(0, 4))

        tk.Label(label_top, text="라벨:", width=5, anchor="w").grid(
            row=0, column=0, sticky="w"
        )
        tk.Label(
            label_top,
            textvariable=self.current_label_var,
            width=8,
            anchor="w",
        ).grid(row=0, column=1, sticky="w")

        tk.Button(
            label_top,
            text="1: 좋음",
            width=7,
            command=lambda: self.set_label(LABEL_VALUE_GOOD),
        ).grid(row=1, column=0, padx=1, pady=1, sticky="w")

        tk.Button(
            label_top,
            text="2: 애매",
            width=7,
            command=lambda: self.set_label(LABEL_VALUE_MEDIUM),
        ).grid(row=1, column=1, padx=1, pady=1, sticky="w")

        tk.Button(
            label_top,
            text="3: 불량",
            width=7,
            command=lambda: self.set_label(LABEL_VALUE_BAD),
        ).grid(row=1, column=2, padx=1, pady=1, sticky="w")

        tk.Button(
            label_top,
            text="라벨 삭제(0)",
            width=10,
            command=self.clear_label,
        ).grid(row=2, column=0, columnspan=3, pady=(4, 0), sticky="w")

        # 메모
        tk.Label(label_frame, text="메모 (human_notes):", anchor="w").pack(
            fill="x", padx=2, pady=(6, 0)
        )
        self.memo_entry = tk.Entry(label_frame)
        self.memo_entry.pack(fill="x", padx=2, pady=(0, 4))

        # 저장 버튼 + 자동 저장 옵션
        tk.Button(
            right_panel,
            text="저장 (CSV/JSON)  Ctrl+S",
            command=self.on_save,
            width=20,
        ).grid(row=1, column=0, sticky="e", padx=4, pady=(0, 2))

        tk.Checkbutton(
            right_panel,
            text="자동 저장 (라벨 10건마다)",
            variable=self.autosave_var,
        ).grid(row=2, column=0, sticky="e", padx=4, pady=(0, 4))

        # 단축키 안내 + 이전/다음 버튼 + 점프
        help_frame = tk.Frame(self.root)
        help_frame.grid(row=3, column=0, sticky="ew", pady=(0, 4))
        help_frame.columnconfigure(0, weight=0)
        help_frame.columnconfigure(1, weight=1)

        nav_frame = tk.Frame(help_frame)
        nav_frame.grid(row=0, column=0, padx=8, sticky="w")

        tk.Button(
            nav_frame,
            text="← 이전",
            width=6,
            command=self.prev_item,
        ).pack(side="left", padx=(0, 4))

        tk.Button(
            nav_frame,
            text="다음 →",
            width=6,
            command=self.next_item,
        ).pack(side="left")

        # 인덱스 점프
        tk.Label(nav_frame, text="  이동:", anchor="w").pack(
            side="left", padx=(8, 0)
        )
        self.goto_entry = tk.Entry(nav_frame, width=5)
        self.goto_entry.pack(side="left", padx=(0, 2))
        self.goto_entry.bind("<Return>", lambda e: self.goto_index())

        tk.Button(
            nav_frame,
            text="Go",
            width=4,
            command=self.goto_index,
        ).pack(side="left")

        help_text = (
            "단축키:  ← : 이전    → : 다음    "
            "1 : 좋음    2 : 애매    3 : 불량    "
            "0 또는 Backspace : 라벨 삭제    "
            "Ctrl+S : 저장    "
            "Space : 라벨 필터(unlabeled/전체) 토글"
        )
        tk.Label(
            help_frame,
            text=help_text,
            anchor="w",
            fg="#555555",
        ).grid(row=0, column=1, padx=8, sticky="w")

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

        self.root.bind("<space>", self.toggle_filter_space)

    # ------------------------------------------------------------------
    # 경로/폴더 관련
    # ------------------------------------------------------------------
    def choose_base_dir(self):
        d = filedialog.askdirectory(
            title="이미지 기준 폴더 선택",
            initialdir=self.base_dir_var.get() or self.mapping_dir or os.getcwd(),
        )
        if d:
            self.base_dir_var.set(d)

    def get_base_dir(self) -> str:
        base = self.base_dir_var.get().strip()
        if base:
            return base
        if self.mapping_dir:
            return self.mapping_dir
        return os.getcwd()

    def resolve_image_path(self, row: Dict[str, Any], which: str) -> str:
        """
        which: "input" / "output"
        우선순위:
          1) *_abs 컬럼 (input_abs/output_abs)
          2) *_rel 컬럼 (input_rel/output_rel) 를 base_dir 기준으로
          3) *_abs 값이 절대경로인데 없으면 base_dir + basename 으로 한 번 더 시도
        """
        base_dir = self.get_base_dir()
        candidates: List[str] = []

        if which == "input":
            raw_abs = safe_get(row, INPUT_PATH_COL, "")
            raw_rel = safe_get(row, INPUT_REL_COL, "")
        else:
            raw_abs = safe_get(row, OUTPUT_PATH_COL, "")
            raw_rel = safe_get(row, OUTPUT_REL_COL, "")

        # 1) abs 그대로/또는 base_dir 기준
        if raw_abs:
            if os.path.isabs(raw_abs):
                candidates.append(raw_abs)
                # 파일이 없으면 base_dir + basename 도 후보
                candidates.append(os.path.join(base_dir, os.path.basename(raw_abs)))
            else:
                candidates.append(os.path.join(base_dir, raw_abs))

        # 2) rel
        if raw_rel:
            if os.path.isabs(raw_rel):
                candidates.append(raw_rel)
            else:
                candidates.append(os.path.join(base_dir, raw_rel))

        # 3) 마지막 fallback: raw 자체
        if not candidates and raw_abs:
            candidates.append(raw_abs)

        for cand in candidates:
            if cand and os.path.exists(cand):
                return cand

        # 전부 실패하면 첫 후보(또는 빈 문자열) 리턴
        return candidates[0] if candidates else ""

    # ------------------------------------------------------------------
    # 파일 열기 / 저장 / 종료
    # ------------------------------------------------------------------
    def on_open_file(self):
        path = filedialog.askopenfilename(
            title="bg_mapping 파일 선택 (csv/json)",
            filetypes=[
                ("CSV / JSON", "*.csv *.tsv *.json *.jsonl"),
                ("모든 파일", "*.*"),
            ],
        )
        if not path:
            return

        try:
            rows = load_mapping_file(path)
        except Exception as e:
            messagebox.showerror("오류", f"파일을 여는 중 오류:\n{e}")
            return

        if not rows:
            messagebox.showwarning("주의", "행이 하나도 없는 파일입니다.")
            return

        self.file_path = path
        self.mapping_dir = os.path.dirname(path)
        self.base_dir_var.set(self.mapping_dir)

        # 캐시 초기화 (다른 파일 열 때 메모리/내용 리셋)
        load_preview_image.cache_clear()
        get_cleaned_html_cached.cache_clear()

        self.rows = rows

        # 시작 시 unlabeled 기준
        self.label_filter_var.set("unlabeled")
        self.apply_filter()

        self.path_var.set(path)
        self.change_since_save = 0
        messagebox.showinfo("완료", f"{len(self.rows)}개 항목을 불러왔습니다.")

    def on_save(self):
        if not self.file_path or not self.rows:
            messagebox.showwarning("주의", "먼저 bg_mapping 파일을 여세요.")
            return
        try:
            save_mapping_file(self.file_path, self.rows)
        except Exception as e:
            messagebox.showerror("오류", f"저장 중 오류:\n{e}")
            return
        self.change_since_save = 0
        messagebox.showinfo("저장 완료", "라벨/메모 상태를 저장했습니다.")

    def on_close(self):
        """창 닫을 때: 자동저장 옵션이면 바로 저장, 아니면 저장 여부 물어봄."""
        if self.file_path and self.rows:
            if self.autosave_var.get():
                try:
                    save_mapping_file(self.file_path, self.rows)
                except Exception as e:
                    messagebox.showerror("오류", f"자동 저장 중 오류:\n{e}")
                    # 그래도 창은 닫는다.
            else:
                if messagebox.askyesno("종료", "종료하기 전에 저장하시겠습니까?"):
                    try:
                        save_mapping_file(self.file_path, self.rows)
                    except Exception as e:
                        messagebox.showerror("오류", f"저장 중 오류:\n{e}")
                        return  # 저장 실패 시 종료 취소
        self.root.destroy()

    # ------------------------------------------------------------------
    # 필터
    # ------------------------------------------------------------------
    def apply_filter(self):
        self.filtered_indices = []

        if not self.rows:
            self.current_index = 0
            self.update_position_label()
            self.clear_view()
            return

        mode = self.label_filter_var.get()

        for idx, row in enumerate(self.rows):
            label_val = safe_get(row, LABEL_COL, "")

            if mode == "전체":
                self.filtered_indices.append(idx)
            elif mode == "unlabeled":
                if label_val == "":
                    self.filtered_indices.append(idx)
            elif mode in ("좋음", "애매", "불량"):
                want = LABEL_KO_TO_VALUE.get(mode)
                if want and label_val == want:
                    self.filtered_indices.append(idx)
            else:
                self.filtered_indices.append(idx)

        if not self.filtered_indices:
            self.current_index = 0
            self.update_position_label()
            self.clear_view()
            return

        self.current_index = 0
        self.show_current()

    def toggle_filter_space(self, event=None):
        cur = self.label_filter_var.get()
        if cur == "unlabeled":
            self.label_filter_var.set("전체")
        else:
            self.label_filter_var.set("unlabeled")
        self.apply_filter()

    def update_position_label(self):
        total = len(self.filtered_indices)
        if total == 0:
            self.position_var.set("0 / 0")
        else:
            self.position_var.set(f"{self.current_index+1} / {total}")

    # ------------------------------------------------------------------
    # 화면 지우기 / 표시
    # ------------------------------------------------------------------
    def clear_view(self):
        self.title_label.config(text="")
        self.subtitle_label.config(text="")
        self.current_label_var.set("-")
        self.memo_entry.delete(0, tk.END)

        self.left_canvas.delete("all")
        self.right_canvas.delete("all")

        if HTML_AVAILABLE:
            try:
                self.html_view.set_html(
                    "<p style='color:#888'>표시할 행이 없습니다.</p>"
                )
            except Exception:
                pass
        else:
            self.html_view.config(state="normal")
            self.html_view.delete("1.0", tk.END)
            self.html_view.insert("1.0", "표시할 행이 없습니다.")
            self.html_view.config(state="disabled")

    def show_current(self):
        if not self.filtered_indices:
            self.clear_view()
            return

        row_idx = self.filtered_indices[self.current_index]
        row = self.rows[row_idx]

        # 제목: [카테고리] 원본상품명 (코드: xxx)
        category   = safe_get(row, CATEGORY_COL, "")
        orig_name  = safe_get(row, ORIG_NAME_COL, "")
        result_nm  = safe_get(row, RESULT_NAME_COL, "")
        market_nm  = safe_get(row, MARKET_NAME_COL, "")
        code       = safe_get(row, CODE_COL, "")

        title_parts = []
        if category:
            title_parts.append(f"[{category}]")
        main_name = orig_name or result_nm or "(상품명 없음)"
        title_parts.append(main_name)
        title_text = " ".join(title_parts)
        if code:
            title_text += f"   (코드: {code})"
        self.title_label.config(text=title_text)

        # 서브 텍스트: 가공(ST1) / 마켓명
        sub_parts = []
        if result_nm:
            sub_parts.append(f"가공(ST1): {result_nm}")
        if market_nm:
            sub_parts.append(f"마켓: {market_nm}")
        self.subtitle_label.config(text="  ·  ".join(sub_parts))

        # 라벨 / 메모
        label_val = safe_get(row, LABEL_COL, "")
        if label_val:
            self.current_label_var.set(LABEL_VALUE_TO_KO.get(label_val, label_val))
        else:
            self.current_label_var.set("(없음)")

        memo = safe_get(row, NOTE_COL, "")
        self.memo_entry.delete(0, tk.END)
        if memo:
            self.memo_entry.insert(0, memo)

        # 이미지
        in_path = self.resolve_image_path(row, "input")
        out_path = self.resolve_image_path(row, "output")
        self.show_image_on_canvas(in_path, self.left_canvas, is_left=True)
        self.show_image_on_canvas(out_path, self.right_canvas, is_left=False)

        # HTML 상세설명 (정제 후 렌더링)
        html_raw = safe_get(row, DETAIL_HTML_COL, "")
        if not html_raw.strip():
            if HTML_AVAILABLE:
                try:
                    self.html_view.set_html(
                        "<p style='color:#888'>상세설명(HTML)이 비어 있습니다.</p>"
                    )
                except Exception:
                    pass
            else:
                self.html_view.config(state="normal")
                self.html_view.delete("1.0", tk.END)
                self.html_view.insert("1.0", "상세설명(HTML)이 비어 있습니다.")
                self.html_view.config(state="disabled")
        else:
            cleaned_html = get_cleaned_html_cached(html_raw)
            wrapped = (
                "<html><head><meta charset='utf-8'></head>"
                "<body style='font-size:12px;'>"
                + cleaned_html +
                "</body></html>"
            )
            if HTML_AVAILABLE:
                try:
                    self.html_view.set_html(wrapped)
                except Exception:
                    safe_txt = cleaned_html.replace("<", "&lt;").replace(">", "&gt;")
                    self.html_view.set_html(
                        "<pre style='white-space:pre-wrap; font-size:11px;'>"
                        + safe_txt +
                        "</pre>"
                    )
            else:
                self.html_view.config(state="normal")
                self.html_view.delete("1.0", tk.END)
                try:
                    soup = BeautifulSoup(cleaned_html, "html.parser")
                    plain = soup.get_text("\n", strip=True)
                except Exception:
                    plain = cleaned_html
                self.html_view.insert("1.0", plain)
                self.html_view.config(state="disabled")

        self.update_position_label()

    def show_image_on_canvas(self, path: str, canvas: tk.Canvas, is_left: bool):
        canvas.delete("all")
        if not path or not os.path.exists(path):
            canvas.create_text(
                self.preview_size // 2,
                self.preview_size // 2,
                text="이미지 없음",
                fill="white",
            )
            if is_left:
                self.left_photo = None
            else:
                self.right_photo = None
            return

        img_resized = load_preview_image(path, self.preview_size)
        if img_resized is None:
            canvas.create_text(
                self.preview_size // 2,
                self.preview_size // 2,
                text="이미지 열기 오류",
                fill="red",
            )
            if is_left:
                self.left_photo = None
            else:
                self.right_photo = None
            return

        photo = ImageTk.PhotoImage(img_resized)
        x = self.preview_size // 2
        y = self.preview_size // 2
        canvas.config(width=self.preview_size, height=self.preview_size)
        canvas.create_image(x, y, image=photo)

        if is_left:
            self.left_photo = photo
        else:
            self.right_photo = photo

    # ------------------------------------------------------------------
    # 라벨 조작 + 자동 저장
    # ------------------------------------------------------------------
    def _maybe_autosave(self):
        if not self.file_path or not self.rows:
            return
        if not self.autosave_var.get():
            return

        self.change_since_save += 1
        if self.change_since_save >= 10:
            try:
                save_mapping_file(self.file_path, self.rows)
                self.change_since_save = 0
            except Exception as e:
                # 자동 저장 실패는 stderr 에만 찍고 조용히 넘어감
                print(f"[AUTOSAVE ERROR] {e}", file=sys.stderr)

    def set_label(self, label_value: str):
        if not self.filtered_indices:
            return
        row_idx = self.filtered_indices[self.current_index]
        row = self.rows[row_idx]

        row[LABEL_COL] = label_value
        row[NOTE_COL] = self.memo_entry.get().strip()

        self.current_label_var.set(LABEL_VALUE_TO_KO.get(label_value, label_value))

        self._maybe_autosave()

        if self.label_filter_var.get() == "unlabeled":
            self.apply_filter()
        else:
            self.show_current()

    def clear_label(self):
        if not self.filtered_indices:
            return
        row_idx = self.filtered_indices[self.current_index]
        row = self.rows[row_idx]

        row[LABEL_COL] = ""
        row[NOTE_COL] = self.memo_entry.get().strip()

        self.current_label_var.set("(없음)")

        self._maybe_autosave()
        self.show_current()

    # ------------------------------------------------------------------
    # 이전 / 다음 / 점프
    # ------------------------------------------------------------------
    def prev_item(self):
        if not self.filtered_indices:
            return
        if self.current_index > 0:
            self.current_index -= 1
            self.show_current()

    def next_item(self):
        if not self.filtered_indices:
            return
        if self.current_index < len(self.filtered_indices) - 1:
            self.current_index += 1
            self.show_current()

    def goto_index(self):
        if not self.filtered_indices:
            return
        val = self.goto_entry.get().strip()
        if not val:
            return
        try:
            idx = int(val)
        except ValueError:
            messagebox.showwarning("입력 오류", "이동할 인덱스를 정수로 입력해주세요.")
            return
        if idx < 1 or idx > len(self.filtered_indices):
            messagebox.showwarning(
                "범위 오류",
                f"1 이상 {len(self.filtered_indices)} 이하의 값만 가능합니다.",
            )
            return
        self.current_index = idx - 1
        self.show_current()


# --------------------------------------------------------------------
def run():
    root = tk.Tk()
    app = BgLabelApp(root)
    root.mainloop()


if __name__ == "__main__":
    run()
