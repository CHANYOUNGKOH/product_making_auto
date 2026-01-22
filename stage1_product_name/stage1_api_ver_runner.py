import os
import sys            # ★ 파일 열기용 (mac / linux 구분)
import subprocess     # ★ 파일 열기용
import threading
import time
import json
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any, Dict

import re  # 출력 후처리용

import pandas as pd
from openai import OpenAI

from stage1_run_history import append_run_history
from prompts_stage1 import safe_str, build_stage1_prompt  # ★ 프롬프트/유틸 분리 모듈 사용

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

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
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_with_images", "_stage1_완료"]
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

# =========================
# 전역 설정
# =========================

client: Optional[OpenAI] = None
STOP_REQUESTED = False

# API 키 저장 파일 (스크립트와 같은 폴더)
CONFIG_API_KEY_PATH = os.path.join(os.path.dirname(__file__), ".openai_api_key")

# 사용 모델: GPT-5 계열 3종만
MODEL_ORDER = [
    "gpt-5",        # 5세대 풀사이즈 reasoning, 최고 품질/고비용
    "gpt-5-mini",   # 5세대 경량 reasoning, 품질/비용 밸런스
    "gpt-5-nano",   # 5세대 초경량, 최저가·최고속도
]

MODEL_DESCRIPTIONS: Dict[str, str] = {
    "gpt-5-nano": (
        "gpt-5-nano : 5세대 중 가장 저렴하고 빠른 경량 모델\n"
        "- 대량 처리, 간단한 정제/분류 작업, 실시간 응답에 적합\n"
        "- 품질보다는 속도·비용을 우선할 때 사용"
    ),
    "gpt-5-mini": (
        "gpt-5-mini : 5세대 경량 reasoning 모델\n"
        "- gpt-5보다 싸고 빠르면서도 높은 품질의 추론 제공\n"
        "- 정제/명명처럼 규칙이 명확한 작업에서 품질·속도·비용 밸런스가 좋음"
    ),
    "gpt-5": (
        "gpt-5 : 5세대 풀사이즈 reasoning 모델\n"
        "- 가장 강력한 추론과 품질, 비용도 가장 높은 편\n"
        "- 예외 케이스가 많거나, 품질을 최우선할 때, 규칙 튜닝 단계에서 테스트용으로 사용"
    ),
}

# OpenAI 공식 가격 (USD, /1M tokens 기준)  - reasoning 토큰은 output에 포함된다고 보고 계산
MODEL_PRICING: Dict[str, Dict[str, float]] = {
    "gpt-5": {
        "input_per_million": 1.250,
        "cached_input_per_million": 0.125,
        "output_per_million": 10.000,
    },
    "gpt-5-mini": {
        "input_per_million": 0.250,
        "cached_input_per_million": 0.025,
        "output_per_million": 2.000,
    },
    "gpt-5-nano": {
        "input_per_million": 0.050,
        "cached_input_per_million": 0.005,
        "output_per_million": 0.400,
    },
}

# 서울 시간 헬퍼
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


# =========================
# Tkinter Tooltip 유틸
# =========================

class ToolTip:
    """위젯에 마우스를 올렸을 때 작은 설명창을 띄우는 툴팁."""

    def __init__(self, widget, text: str = "", wraplength: int = 420):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tipwindow: Optional[tk.Toplevel] = None

        self.widget.bind("<Enter>", self._show_tip)
        self.widget.bind("<Leave>", self._hide_tip)

    def _show_tip(self, event=None):
        if self.tipwindow or not self.text:
            return

        # 화면 위치 계산
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 8

        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)  # 윈도우 테두리 제거
        tw.wm_geometry(f"+{x}+{y}")

        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            wraplength=self.wraplength,
            font=("맑은 고딕", 9),
        )
        label.pack(ipadx=6, ipady=4)

    def _hide_tip(self, event=None):
        tw = self.tipwindow
        if tw is not None:
            tw.destroy()
        self.tipwindow = None


# =========================
# STAGE1 프롬프트
#  - 실제 규칙/템플릿은 prompts_stage1.py 에서 관리
#  - 여기서는 safe_str, build_stage1_prompt 만 사용
# =========================


# =========================
# OpenAI 관련
# =========================

def load_api_key_from_file() -> str:
    if os.path.exists(CONFIG_API_KEY_PATH):
        try:
            with open(CONFIG_API_KEY_PATH, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""


def save_api_key_to_file(key: str) -> None:
    try:
        with open(CONFIG_API_KEY_PATH, "w", encoding="utf-8") as f:
            f.write(key.strip())
    except Exception as e:
        print(f"[WARN] API 키 저장 실패: {e}")


def init_openai_client(api_key: str) -> None:
    global client
    api_key = api_key.strip()
    if not api_key:
        raise ValueError("API 키가 비어 있습니다.")
    client = OpenAI(api_key=api_key)


def normalize_reasoning_effort(gui_value: str) -> Optional[str]:
    """
    GUI에서 받은 reasoning effort 값을 API에 넘길 값으로 정규화.

    반환값 의미:
    - "low" / "medium" / "high"  → reasoning={"effort": 그 값} 으로 전송
    - None                       → reasoning 매개변수를 아예 보내지 않음 (모델 기본 동작)
    """
    if gui_value is None:
        return None

    val = gui_value.strip().lower()

    # "(자동)" → low로 고정 (기본 추천)
    if not val or val in ("(자동)", "auto", "기본값", ""):
        return "low"

    # none/없음/off → reasoning 사용 안 함
    if val in ("none", "없음", "off"):
        return None

    if val in ("low", "medium", "high"):
        return val

    # 알 수 없는 값이면 그냥 None (reasoning 안 보냄)
    return None


def extract_text_from_response(resp: Any) -> str:
    """
    responses.create() 결과에서 사람이 읽을 텍스트만 안전하게 추출.
    - resp.output[*].content[*].text.value 형태를 우선 사용.
    - reasoning 아이템은 제외하고 message 타입만 사용.
    """
    if resp is None:
        return ""

    try:
        chunks = []
        output_list = getattr(resp, "output", None)
        if output_list:
            for out in output_list:
                o_type = getattr(out, "type", None)
                if o_type not in (None, "message"):
                    continue

                content_list = getattr(out, "content", None)
                if not content_list:
                    continue
                for c in content_list:
                    t_obj = getattr(c, "text", None)
                    if isinstance(t_obj, str):
                        chunks.append(t_obj)
                    elif t_obj is not None:
                        val = getattr(t_obj, "value", None)
                        if isinstance(val, str):
                            chunks.append(val)
        if chunks:
            return "\n".join(chunks).strip()
    except Exception:
        pass

    try:
        output_text = getattr(resp, "output_text", None)
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()
    except Exception:
        pass

    return ""


def clean_stage1_output(text: str) -> str:
    """
    Stage1 결과를 Stage2에서 바로 '기본상품명'으로 쓸 수 있도록
    최소한의 정리를 적용한다.
    - 라벨 제거 ("정제된 상품명:" 등)
    - 양 끝 따옴표 제거
    - 금지 기호(/, ·, ,)를 공백으로 치환
    - 공백 정리

    ※ 60자 이내 하드 컷은 하지 않는다. 길이 제한은 프롬프트에만 의존.
    """
    if not text:
        return ""

    s = text.strip()

    # 1) 가장 흔한 라벨 패턴 제거
    s = re.sub(r'^\s*(정제된\s*상품명|상품명)\s*[:：]\s*', '', s, flags=re.IGNORECASE)

    # 2) 바깥쪽에만 있는 큰따옴표/작은따옴표 제거
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("“") and s.endswith("”")):
        s = s[1:-1].strip()
    elif (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # 3) 금지 기호(/, ·, ,)를 공백으로 치환
    s = s.replace("/", " ").replace("·", " ").replace(",", " ")

    # 4) 공백 2개 이상 → 1개로
    s = re.sub(r"\s+", " ", s).strip()

    return s


def call_stage1_api(
    category: str,
    sale_type: str,
    raw_name: str,
    model_name: str,
    reasoning_effort: Optional[str] = None,
) -> Tuple[str, int, int, int, str]:
    """
    한 행에 대해 Stage1 API 호출.
    - prompts_stage1.build_stage1_prompt()를 사용해 전체 프롬프트를 생성.
    - 단일 user 메시지로 호출 (규칙/예시/입력정보 모두 포함).

    반환: (정제상품명, input_tokens, output_tokens, reasoning_tokens, reasoning_summary_text)
    reasoning_summary_text는 현재 항상 "" (사용 안 함)
    """
    if client is None:
        raise RuntimeError("OpenAI client가 초기화되지 않았습니다. 먼저 API 키를 설정하세요.")

    # prompts_stage1에서 가져온 템플릿/규칙으로 전체 프롬프트 생성
    prompt_text = build_stage1_prompt(
        category=category,
        sale_type=sale_type,
        raw_name=raw_name,
    )

    is_gpt5 = model_name.startswith("gpt-5")

    # 단일 user 메시지 구조
    kwargs: Dict[str, Any] = {
        "model": model_name,
        "input": [
            {
                "role": "user",
                "content": prompt_text,
            },
        ],
    }

    if is_gpt5:
        # normalize_reasoning_effort() 결과가 None이면 reasoning 자체를 보내지 않음
        if reasoning_effort is not None:
            kwargs["reasoning"] = {"effort": reasoning_effort}

    try:
        resp = client.responses.create(**kwargs)
    except Exception as e:
        msg = str(e)
        # 예전 버전 호환용 방어 코드 (현재 summary는 사용하지 않음)
        if "param': 'reasoning.summary'" in msg or 'param": "reasoning.summary"' in msg:
            r = kwargs.get("reasoning")
            if isinstance(r, dict):
                r = dict(r)
                r.pop("summary", None)
                if r:
                    kwargs["reasoning"] = r
                else:
                    kwargs.pop("reasoning", None)
            else:
                kwargs.pop("reasoning", None)
            resp = client.responses.create(**kwargs)
        else:
            raise

    full_text = extract_text_from_response(resp)
    reasoning_summary_text = ""

    status = getattr(resp, "status", "")
    if status == "incomplete" and not full_text:
        reason = ""
        try:
            incomplete = getattr(resp, "incomplete_details", None)
            if incomplete is not None:
                reason = getattr(incomplete, "reason", "") or ""
        except Exception:
            pass
        full_text = f"[INCOMPLETE: {reason or 'unknown'}]"

    in_tok = 0
    out_tok = 0
    reasoning_tok = 0
    try:
        usage = getattr(resp, "usage", None)
        if usage is not None:
            in_tok = int(getattr(usage, "input_tokens", 0) or 0)
            out_tok = int(getattr(usage, "output_tokens", 0) or 0)
            out_details = getattr(usage, "output_tokens_details", None)
            if out_details is not None:
                reasoning_tok = int(getattr(out_details, "reasoning_tokens", 0) or 0)
    except Exception:
        pass

    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    raw_refined = lines[0] if lines else ""
    refined = clean_stage1_output(raw_refined)

    return refined, in_tok, out_tok, reasoning_tok, reasoning_summary_text


def compute_cost_usd(
    model_name: str,
    total_in_tokens: int,
    total_out_tokens: int,
) -> Optional[Tuple[float, float, float]]:
    """
    단건 API 기준 비용 계산.
    reasoning_tokens는 별도 단가가 없다고 보고 output 단가에 포함된 것으로 처리.
    """
    pricing = MODEL_PRICING.get(model_name)
    if pricing is None:
        return None

    in_rate = pricing["input_per_million"] / 1_000_000.0
    out_rate = pricing["output_per_million"] / 1_000_000.0

    input_cost = total_in_tokens * in_rate
    output_cost = total_out_tokens * out_rate
    total_cost = input_cost + output_cost
    return input_cost, output_cost, total_cost


# =========================
# GUI 관련
# =========================

class Stage1GUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Stage1-a 상품명 정제 도구 - GPT-5")
        self.geometry("900x880")

        # 기본 스타일 살짝 꾸미기
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Title.TLabel", font=("맑은 고딕", 14, "bold"))
        style.configure("SubTitle.TLabel", font=("맑은 고딕", 10), foreground="#555555")
        style.configure("SmallGray.TLabel", font=("맑은 고딕", 9), foreground="#777777")

        # ★ 초록색 프로그레스바 스타일 추가
        style.configure(
            "Green.Horizontal.TProgressbar",
            troughcolor="#f0f0f0",
            bordercolor="#f0f0f0",
            background="#4caf50",   # 채워지는 바 색
            lightcolor="#4caf50",
            darkcolor="#388e3c",
        )

        self.api_key_var = tk.StringVar(value=load_api_key_from_file())

        # 기본 모델: gpt-5-mini
        self.model_var = tk.StringVar(value="gpt-5-mini")

        self.save_every_var = tk.IntVar(value=50)

        self.file_path_var = tk.StringVar(value="")

        # reasoning 옵션용 변수
        self.reasoning_effort_var = tk.StringVar(value="(자동)")  # → low로 매핑

        # 덮어쓰기 옵션 (기본: True = 항상 덮어쓰기)
        self.overwrite_var = tk.BooleanVar(value=True)

        # 진행률 표시용 변수
        self.progress_var = tk.DoubleVar(value=0.0)          # 0~100 (%)
        self.progress_text_var = tk.StringVar(value="0 / 0 (0.0%)")

        # 남은 시간/시작/완료예상 표시용 변수
        self.eta_var = tk.StringVar(value="남은 예상 시간: -")
        self.start_time_var = tk.StringVar(value="시작 시각: -")
        self.finish_time_var = tk.StringVar(value="완료 예상 시각: -")

        # Tooltip 객체
        self.model_tooltip: Optional[ToolTip] = None
        self.reasoning_tooltip: Optional[ToolTip] = None
        self.overwrite_tooltip: Optional[ToolTip] = None  # ★ 덮어쓰기 체크박스 툴팁

        self._build_widgets()

        # 초기 설명 텍스트를 툴팁에 주입
        self._update_model_help_label(self.model_var.get())
        self._update_reasoning_help_label(self.reasoning_effort_var.get())

    # ---- UI 구성 ----
    def _build_widgets(self) -> None:
        # 상단 타이틀
        header = ttk.Label(
            self,
            text="Stage1 상품명 정제 도구",
            style="Title.TLabel",
        )
        header.pack(fill="x", padx=10, pady=(10, 0))

        sub_header = ttk.Label(
            self,
            text="도매 원본상품명 → 위탁판매용 정보 중심 정제상품명",
            style="SubTitle.TLabel",
        )
        sub_header.pack(fill="x", padx=10, pady=(0, 8))

        # 상단 프레임: OpenAI 설정
        frame_top = ttk.LabelFrame(self, text="OpenAI 설정")
        frame_top.pack(fill="x", padx=10, pady=5)

        # API 키
        ttk.Label(frame_top, text="API Key:").grid(row=0, column=0, sticky="w", padx=5, pady=4)
        entry_key = ttk.Entry(frame_top, textvariable=self.api_key_var, width=50, show="*")
        entry_key.grid(row=0, column=1, sticky="w", padx=5, pady=4)

        btn_save_key = ttk.Button(frame_top, text="키 저장", command=self.on_save_api_key)
        btn_save_key.grid(row=0, column=2, sticky="w", padx=5, pady=4)

        # 모델 선택
        ttk.Label(frame_top, text="Model:").grid(row=1, column=0, sticky="w", padx=5, pady=4)
        combo_model = ttk.Combobox(
            frame_top,
            textvariable=self.model_var,
            values=MODEL_ORDER,
            width=20,
            state="readonly",
        )
        combo_model.grid(row=1, column=1, sticky="w", padx=5, pady=4)
        combo_model.bind("<<ComboboxSelected>>", self._on_model_selected)
        # 모델 설명 툴팁 아이콘
        self.model_info_icon = ttk.Label(
            frame_top,
            text="ⓘ",
            foreground="#666666",
            cursor="question_arrow",
        )
        self.model_info_icon.grid(row=1, column=2, sticky="w", padx=(2, 0), pady=4)
        self.model_tooltip = ToolTip(self.model_info_icon, "")

        # ★ 모델 요약 안내 (항상 보이는 한 줄)
        self.model_inline_label = ttk.Label(
            frame_top,
            text="※ 모델 옆 ⓘ 아이콘에 마우스를 올리면 요금·특징을 자세히 볼 수 있습니다.",
            style="SmallGray.TLabel",
        )
        self.model_inline_label.grid(row=2, column=0, columnspan=3, sticky="w", padx=24, pady=(0, 2))

        # 저장 주기
        ttk.Label(frame_top, text="엑셀 저장 주기(행):").grid(
            row=3, column=0, sticky="w", padx=5, pady=4
        )
        spin_save = ttk.Spinbox(
            frame_top,
            from_=1,
            to=1000,
            increment=1,
            textvariable=self.save_every_var,
            width=7,
        )
        spin_save.grid(row=3, column=1, sticky="w", padx=5, pady=4)

        # Reasoning Effort
        ttk.Label(frame_top, text="Reasoning Effort (GPT-5):").grid(
            row=4, column=0, sticky="w", padx=5, pady=4
        )
        combo_reasoning = ttk.Combobox(
            frame_top,
            textvariable=self.reasoning_effort_var,
            values=["(자동)", "none", "low", "medium", "high"],
            width=20,
            state="readonly",
        )
        combo_reasoning.grid(row=4, column=1, sticky="w", padx=5, pady=4)
        combo_reasoning.bind("<<ComboboxSelected>>", self._on_reasoning_selected)

        # Reasoning 설명 툴팁 아이콘
        self.reasoning_info_icon = ttk.Label(
            frame_top,
            text="ⓘ",
            foreground="#666666",
            cursor="question_arrow",
        )
        self.reasoning_info_icon.grid(row=4, column=2, sticky="w", padx=(2, 0), pady=4)
        self.reasoning_tooltip = ToolTip(self.reasoning_info_icon, "")

        # ★ Reasoning Effort 요약 안내 (항상 보이는 한 줄)
        self.reasoning_inline_label = ttk.Label(
            frame_top,
            text="※ (자동=low, none=미사용, medium/high=품질↑·비용↑)",
            style="SmallGray.TLabel",
        )
        self.reasoning_inline_label.grid(row=5, column=0, columnspan=3, sticky="w", padx=24, pady=(0, 4))

        # 추천 프리셋 안내
        preset_hint = (
            "추천 프로필\n"
            "- 기본: gpt-5-mini + low  (속도·비용 균형)\n"
            "- 품질: gpt-5-mini + medium  (품질 우선, 조금 더 비쌈)\n"
            "- 자세한 요금·역할은 모델/Reasoning 옆 ⓘ 아이콘 툴팁 참고"
        )
        self.preset_label = ttk.Label(
            frame_top,
            text=preset_hint,
            style="SmallGray.TLabel",
            justify="left",
        )
        self.preset_label.grid(row=6, column=0, columnspan=3, sticky="w", padx=5, pady=(2, 4))

        # 덮어쓰기 옵션 체크박스
        chk_overwrite = ttk.Checkbutton(
            frame_top,
            text="ST1_정제상품명 기존값도 덮어쓰기 (OFF면 기존값 있는 행은 건너뜀)",
            variable=self.overwrite_var,
        )
        chk_overwrite.grid(row=7, column=0, columnspan=3, sticky="w", padx=5, pady=(2, 4))

        # ★ 덮어쓰기 체크박스 툴팁 추가
        self.overwrite_tooltip = ToolTip(
            chk_overwrite,
            text=(
                "ST1_정제상품명 기존값 덮어쓰기 설명\n"
                "- 체크 ON: 기존에 값이 있어도 새로 생성된 정제상품명으로 덮어씁니다.\n"
                "  · 규칙을 바꾸고 전체를 다시 돌릴 때 사용\n\n"
                "- 체크 OFF: ST1_정제상품명에 값이 있는 행은 건너뜁니다.\n"
                "  · 이미 검수한 결과는 남기고, 빈 셀만 새로 채울 때 사용"
            ),
        )

        for i in range(3):
            frame_top.grid_columnconfigure(i, weight=0)

        # 파일 선택 영역
        frame_file = ttk.LabelFrame(self, text="도매처 원본 엑셀 선택")
        frame_file.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_file, text="엑셀 파일 경로:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        entry_file = ttk.Entry(frame_file, textvariable=self.file_path_var, width=70)
        entry_file.grid(row=0, column=1, sticky="w", padx=5, pady=5)

        btn_browse = ttk.Button(frame_file, text="찾기...", command=self.on_browse_file)
        btn_browse.grid(row=0, column=2, sticky="w", padx=5, pady=5)

        # 실행/중지 버튼 + 진행률
        frame_run = ttk.Frame(self)
        frame_run.pack(fill="x", padx=10, pady=5)

        btn_run = ttk.Button(frame_run, text="Stage1 실행", command=self.on_run_stage1)
        btn_run.pack(side="left", padx=5)

        btn_stop = ttk.Button(frame_run, text="중지", command=self.on_stop)
        btn_stop.pack(side="left", padx=5)

        # 진행률 프로그레스바 + 퍼센트 텍스트
        progress_bar = ttk.Progressbar(
            frame_run,
            variable=self.progress_var,
            maximum=100,
            mode="determinate",
            length=250,
            style="Green.Horizontal.TProgressbar",   # ★ 초록색 스타일 적용
        )
        progress_bar.pack(side="left", padx=10)

        lbl_progress = ttk.Label(frame_run, textvariable=self.progress_text_var)
        lbl_progress.pack(side="left", padx=5)

        lbl_eta = ttk.Label(frame_run, textvariable=self.eta_var, style="SmallGray.TLabel")
        lbl_eta.pack(side="left", padx=5)

        # 시작/완료 예상 시각 라벨
        frame_time = ttk.Frame(self)
        frame_time.pack(fill="x", padx=10, pady=2)

        lbl_start = ttk.Label(frame_time, textvariable=self.start_time_var, style="SmallGray.TLabel")
        lbl_start.pack(side="left", padx=5)

        lbl_finish = ttk.Label(frame_time, textvariable=self.finish_time_var, style="SmallGray.TLabel")
        lbl_finish.pack(side="left", padx=5)

        # 로그 영역
        frame_log = ttk.LabelFrame(self, text="로그")
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_widget = ScrolledText(frame_log, height=20)
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)

    # ---- 모델/Reasoning 도움말 → 툴팁 텍스트로 세팅 ----
    def _update_model_help_label(self, model_id: str) -> None:
        if self.model_tooltip is None:
            return

        desc = MODEL_DESCRIPTIONS.get(model_id, "")

        pricing = MODEL_PRICING.get(model_id)
        if pricing:
            desc += (
                "\n\n[요금 안내] (공식 단가 기준)\n"
                f"- 입력:     ${pricing['input_per_million']:.3f} / 100만 tokens\n"
                f"- 출력:     ${pricing['output_per_million']:.3f} / 100만 tokens\n"
                f"- 캐시입력: ${pricing['cached_input_per_million']:.3f} / 100만 tokens\n"
                "※ 이 도구의 비용 계산은 '캐시 미사용 기준 상한값'으로만 잡습니다."
            )

        self.model_tooltip.text = desc

    def _update_reasoning_help_label(self, gui_value: str) -> None:
        if self.reasoning_tooltip is None:
            return

        norm = normalize_reasoning_effort(gui_value)
        gv = (gui_value or "").strip().lower()

        if gv in ("(자동)", "auto", "기본값", "") and norm == "low":
            text = (
                "Reasoning Effort: (자동 → low)\n"
                "- gpt-5 계열에서 effort='low'로 호출됩니다.\n"
                "- 일반적인 상품명 정제 작업에 적당한 품질/비용 밸런스.\n\n"
                "[Tip]\n"
                "- 대량 처리·속도·비용 우선: gpt-5-mini + low\n"
                "- 품질 우선: gpt-5-mini + medium"
            )
        elif gv in ("none", "없음", "off") and norm is None:
            text = (
                "Reasoning Effort: none (미사용)\n"
                "- reasoning 매개변수를 아예 보내지 않습니다.\n"
                "- gpt-5 기본 동작으로 응답하며, reasoning 토큰 사용량을 최소화합니다."
            )
        elif norm == "low":
            text = (
                "Reasoning Effort: low\n"
                "- 추론 토큰을 적게 사용하여 빠르고 저렴합니다.\n"
                "- 대량 정제 작업에서 기본 추천."
            )
        elif norm == "medium":
            text = (
                "Reasoning Effort: medium\n"
                "- 'low'보다 추론량이 많아 예외 케이스 처리에 유리하지만,\n"
                "- reasoning 토큰 사용량과 비용이 증가합니다.\n"
                "- 품질을 조금 더 우선하고 싶을 때 gpt-5-mini와 함께 사용을 추천합니다."
            )
        elif norm == "high":
            text = (
                "Reasoning Effort: high\n"
                "- 가장 많은 추론 토큰을 사용합니다.\n"
                "- 규칙이 복잡하거나 품질을 최우선으로 보는 테스트/튜닝 단계에 적합.\n"
                "- 토큰 사용량(특히 reasoning_tokens)을 로그에서 꼭 확인하세요."
            )
        else:
            text = (
                "Reasoning Effort: 알 수 없는 값\n"
                "- 기본적으로 모델 기본값으로 동작합니다."
            )

        text += "\n\n※ Reasoning Summary 기능은 현재 이 도구에서 사용하지 않습니다."
        self.reasoning_tooltip.text = text

    def _on_model_selected(self, event=None) -> None:
        model_id = self.model_var.get().strip()
        self._update_model_help_label(model_id)

    def _on_reasoning_selected(self, event=None) -> None:
        self._update_reasoning_help_label(self.reasoning_effort_var.get())

    # ---- 로그 & 진행률 & 시간표시 유틸 (스레드 안전) ----
    def _append_log_on_main_thread(self, msg: str) -> None:
        self.log_widget.insert("end", msg + "\n")
        self.log_widget.see("end")
        self.update_idletasks()

    def append_log(self, msg: str) -> None:
        """백그라운드 스레드에서도 안전하게 로그 추가."""
        self.after(0, self._append_log_on_main_thread, msg)

    def _set_progress_on_main_thread(self, current: int, total: int) -> None:
        if total <= 0:
            percent = 0.0
        else:
            percent = current * 100.0 / total
        self.progress_var.set(percent)
        self.progress_text_var.set(f"{current} / {total} ({percent:.1f}%)")
        self.update_idletasks()

    def set_progress(self, current: int, total: int) -> None:
        """백그라운드 스레드에서 호출해도 되는 진행률 업데이트."""
        self.after(0, self._set_progress_on_main_thread, current, total)

    def _set_eta_on_main_thread(self, text: str) -> None:
        self.eta_var.set(text)
        self.update_idletasks()

    def set_eta(self, text: str) -> None:
        """남은 예상 시간 텍스트 업데이트."""
        self.after(0, self._set_eta_on_main_thread, text)

    def _set_start_time_on_main_thread(self, text: str) -> None:
        self.start_time_var.set(text)
        self.update_idletasks()

    def _set_finish_time_on_main_thread(self, text: str) -> None:
        self.finish_time_var.set(text)
        self.update_idletasks()

    def set_start_time(self, dt: datetime) -> None:
        """시작 시각 라벨 업데이트 (서울 기준)."""
        text = "시작 시각: " + dt.strftime("%Y-%m-%d %H:%M:%S")
        self.after(0, self._set_start_time_on_main_thread, text)

    def set_finish_time(self, dt: datetime) -> None:
        """완료 예상 시각 라벨 업데이트 (서울 기준)."""
        text = "완료 예상 시각: " + dt.strftime("%Y-%m-%d %H:%M:%S")
        self.after(0, self._set_finish_time_on_main_thread, text)

    # ★ 완료된 파일 열기 헬퍼
    def _open_file(self, path: str) -> None:
        try:
            if not os.path.exists(path):
                messagebox.showerror("파일 없음", f"파일을 찾을 수 없습니다:\n{path}")
                return
            if os.name == "nt":          # Windows
                os.startfile(path)      # type: ignore[attr-defined]
            elif sys.platform == "darwin":  # macOS
                subprocess.Popen(["open", path])
            else:                        # Linux 등
                subprocess.Popen(["xdg-open", path])
        except Exception as e:
            messagebox.showerror(
                "파일 열기 실패",
                f"다음 파일을 여는 중 오류가 발생했습니다.\n\n{path}\n\n{e}"
            )

    def _show_done_message_on_main(self, out_path: str, stopped: bool) -> None:
        """
        작업 완료/중단 시 알림.
        - 중단: 기존처럼 안내만.
        - 정상 완료: '파일을 지금 열까요?' 예/아니요 선택 후, 예면 파일 열기.
        """
        if stopped:
            messagebox.showinfo(
                "작업 중단",
                f"사용자 요청으로 작업이 중단되었습니다.\n\n"
                f"현재까지 저장된 파일:\n{out_path}"
            )
        else:
            answer = messagebox.askyesno(
                "완료",
                f"Stage1 상품명 정제가 모두 완료되었습니다.\n\n"
                f"저장 파일:\n{out_path}\n\n"
                f"이 파일을 지금 열까요?"
            )
            if answer:
                self._open_file(out_path)

    # ---- 엑셀 닫기 안내 ----
    def _show_excel_close_notice(self, file_path: str) -> bool:
        """
        선택된 엑셀 파일 기준으로
        - 원본 파일
        - 최종 저장 파일
        안내 문구를 깔끔하게 보여주고
        사용자가 '예'를 눌렀을 때만 True를 반환한다.
        """
        base_dir = os.path.dirname(file_path)
        base_name = os.path.splitext(os.path.basename(file_path))[0]

        src_name = os.path.basename(file_path)
        out_name = f"{base_name}_stage1_완료.xlsx"

        msg = (
            "작업 중에는 아래 엑셀 파일들이 사용/생성됩니다.\n\n"
            "작업 폴더:\n"
            f"  {base_dir}\n\n"
            "아래 파일이 Excel에서 열려 있으면\n"
            "저장 시 Permission denied 오류가 발생할 수 있습니다.\n\n"
            f"- 원본 파일 : {src_name}\n"
            f"- 최종 저장 : {out_name}\n\n"
            "위 엑셀 파일을 모두 닫은 후 계속 진행해 주세요.\n\n"
            "모든 관련 엑셀 파일을 닫았습니까?"
        )

        return messagebox.askyesno("엑셀 파일 닫기 안내", msg)

    # ---- 이벤트 핸들러 ----
    def on_save_api_key(self) -> None:
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("경고", "API 키가 비어 있습니다.")
            return
        save_api_key_to_file(key)
        try:
            init_openai_client(key)
        except Exception as e:
            messagebox.showerror("에러", f"OpenAI 클라이언트 초기화 실패: {e}")
            return
        messagebox.showinfo("완료", "API 키 저장 및 클라이언트 초기화 완료.")

    def on_browse_file(self) -> None:
        path = filedialog.askopenfilename(
            title="도매처 원본 엑셀 선택",
            filetypes=[("Excel files", "*.xlsx;*.xls")],
        )
        if path:
            self.file_path_var.set(path)

    def on_stop(self) -> None:
        global STOP_REQUESTED
        STOP_REQUESTED = True
        self.append_log("[INFO] 중지 요청됨. 현재 진행 중인 행까지만 처리합니다.")
        self.set_eta("남은 예상 시간: 작업 중단 요청됨")
        # 메인 스레드에서 바로 호출해도 안전
        self._set_finish_time_on_main_thread("완료 예상 시각: 작업 중단 요청됨")

    def on_run_stage1(self) -> None:
        global STOP_REQUESTED
        STOP_REQUESTED = False

        file_path = self.file_path_var.get().strip()
        if not file_path:
            messagebox.showwarning("경고", "엑셀 파일을 먼저 선택하세요.")
            return
        if not os.path.exists(file_path):
            messagebox.showerror("에러", f"파일을 찾을 수 없습니다:\n{file_path}")
            return

        api_key = self.api_key_var.get().strip()
        if not api_key:
            messagebox.showwarning("경고", "먼저 API 키를 입력/저장하세요.")
            return

        # ---- 엑셀 닫기 안내 ----
        ok = self._show_excel_close_notice(file_path)
        if not ok:
            self.append_log("[INFO] 엑셀 파일 미종료로 Stage1 실행을 취소했습니다.")
            return
        # ---- 안내 끝 ----

        # 시작 시각(서울) & 모노토닉 타임스탬프 기록
        start_dt = get_seoul_now()
        start_time_monotonic = time.time()

        # 라벨 초기화
        self.set_progress(0, 1)
        self.set_eta("남은 예상 시간: 계산 중...")
        self.set_start_time(start_dt)
        self._set_finish_time_on_main_thread("완료 예상 시각: 계산 중...")

        self.append_log("[INFO] Stage1 작업을 시작합니다.")

        th = threading.Thread(
            target=self._run_stage1_thread,
            args=(file_path, start_dt, start_time_monotonic),
        )
        th.daemon = True
        th.start()

    def _run_stage1_thread(
        self,
        file_path: str,
        start_dt: datetime,
        start_time_monotonic: float,
    ) -> None:
        try:
            init_openai_client(self.api_key_var.get())

            model_name = self.model_var.get().strip()
            save_every = int(self.save_every_var.get())

            raw_effort = self.reasoning_effort_var.get()
            reasoning_effort = normalize_reasoning_effort(raw_effort)

            gv = (raw_effort or "").strip().lower()
            if gv in ("(자동)", "auto", "기본값", "") and reasoning_effort == "low":
                effective_effort = "low (자동)"
            elif gv in ("none", "없음", "off") and reasoning_effort is None:
                effective_effort = "none (미사용)"
            elif reasoning_effort:
                effective_effort = reasoning_effort
            else:
                effective_effort = "default(모델 기본)"

            overwrite_results = self.overwrite_var.get()

            self.append_log(f"[INFO] 선택된 파일: {file_path}")
            self.append_log(f"[INFO] 모델: {model_name}")
            self.append_log(f"[INFO] reasoning_effort(effective)={effective_effort}")
            self.append_log(
                f"[INFO] ST1_정제상품명 기존값 덮어쓰기: {overwrite_results}"
            )
            self.append_log(f"[INFO] 엑셀 저장 주기: {save_every} 행마다 저장")

            df = pd.read_excel(file_path)
            required_cols = ["원본상품명", "카테고리명", "판매형태"]

            for col in required_cols:
                if col not in df.columns:
                    messagebox.showerror(
                        "에러",
                        f"엑셀에 필수 컬럼이 없습니다: {col}\n"
                        f"현재 컬럼 목록: {list(df.columns)}"
                    )
                    return

            if "ST1_정제상품명" not in df.columns:
                df["ST1_정제상품명"] = ""
            if "ST1_판매형태" not in df.columns:
                df["ST1_판매형태"] = ""

            total_rows = len(df)
            self.append_log(f"[INFO] 총 행 수: {total_rows}")
            self.set_progress(0, total_rows if total_rows > 0 else 1)

            total_in_tok = 0
            total_out_tok = 0
            total_reasoning_tok = 0

            # 시간 & 토큰 통계용
            processed_rows_total = 0      # 스킵 포함, 루프에서 방문한 행 수
            api_rows = 0                  # 실제 OpenAI API를 호출한 행 수
            success_rows = 0              # API 호출 성공한 행 수
            fail_rows = 0                 # API 호출 실패한 행 수

            base_dir = os.path.dirname(file_path)
            base_name = os.path.splitext(os.path.basename(file_path))[0]

            stopped_by_user = False

            # ETA 계산은 start_time_monotonic 기준
            for idx in range(total_rows):
                if STOP_REQUESTED:
                    self.append_log("[INFO] 중지 플래그 감지, 루프 종료.")
                    stopped_by_user = True
                    break

                raw_name = safe_str(df.at[idx, "원본상품명"])
                category = safe_str(df.at[idx, "카테고리명"])
                sale_type = safe_str(df.at[idx, "판매형태"])  # 이미 엑셀에서 지정된 단품형/옵션형

                # 이 행은 방문했으므로 처리된 행 수 카운트
                processed_rows_total = idx + 1

                # 덮어쓰기 옵션 OFF이고, 기존 정제상품명이 있으면 스킵
                existing_refined = safe_str(df.at[idx, "ST1_정제상품명"])
                if existing_refined and not overwrite_results:
                    self.append_log(
                        f"[SKIP] 행 {idx}: ST1_정제상품명 값이 이미 있어 덮어쓰기 옵션 OFF 상태로 스킵."
                    )
                    self.set_progress(idx + 1, total_rows)
                    # ETA 및 완료 예상 시각 업데이트
                    try:
                        processed = idx + 1
                        if processed > 0 and total_rows > 0:
                            elapsed = time.time() - start_time_monotonic
                            per_row = elapsed / processed
                            remaining = max(total_rows - processed, 0)
                            eta_seconds = max(per_row * remaining, 0)
                            eta_min = int(eta_seconds // 60)
                            eta_sec = int(round(eta_seconds % 60))
                            if remaining == 0:
                                eta_text = "남은 예상 시간: 거의 완료"
                            else:
                                if eta_min > 0:
                                    eta_text = f"남은 예상 시간: 약 {eta_min}분 {eta_sec}초"
                                else:
                                    eta_text = f"남은 예상 시간: 약 {eta_sec}초"
                            self.set_eta(eta_text)

                            expected_end_dt = start_dt + timedelta(
                                seconds=elapsed + eta_seconds
                            )
                            self.set_finish_time(expected_end_dt)
                    except Exception:
                        pass
                    continue

                if not raw_name:
                    self.append_log(f"[SKIP] 행 {idx}: 원본상품명이 비어 있음.")
                    self.set_progress(idx + 1, total_rows)
                    # ETA 및 완료 예상 시각 업데이트
                    try:
                        processed = idx + 1
                        if processed > 0 and total_rows > 0:
                            elapsed = time.time() - start_time_monotonic
                            per_row = elapsed / processed
                            remaining = max(total_rows - processed, 0)
                            eta_seconds = max(per_row * remaining, 0)
                            eta_min = int(eta_seconds // 60)
                            eta_sec = int(round(eta_seconds % 60))
                            if remaining == 0:
                                eta_text = "남은 예상 시간: 거의 완료"
                            else:
                                if eta_min > 0:
                                    eta_text = f"남은 예상 시간: 약 {eta_min}분 {eta_sec}초"
                                else:
                                    eta_text = f"남은 예상 시간: 약 {eta_sec}초"
                            self.set_eta(eta_text)

                            expected_end_dt = start_dt + timedelta(
                                seconds=elapsed + eta_seconds
                            )
                            self.set_finish_time(expected_end_dt)
                    except Exception:
                        pass
                    continue

                self.append_log(f"[INFO] 행 {idx} 처리 중...")
                self.append_log(f"       원본상품명: {raw_name}")
                self.append_log(f"       카테고리명: {category}")
                self.append_log(f"       판매형태(엑셀): {sale_type}")

                # 실제 OpenAI API를 호출하는 행 카운트
                api_rows += 1

                try:
                    refined, in_tok, out_tok, r_tok, r_summary = call_stage1_api(
                        category=category,
                        sale_type=sale_type,
                        raw_name=raw_name,
                        model_name=model_name,
                        reasoning_effort=reasoning_effort,
                    )
                    total_in_tok += in_tok
                    total_out_tok += out_tok
                    total_reasoning_tok += r_tok

                    df.at[idx, "ST1_정제상품명"] = refined
                    df.at[idx, "ST1_판매형태"] = sale_type

                    success_rows += 1

                    self.append_log(f"[OK] 행 {idx} 완료")
                    self.append_log(f"     정제상품명: {refined}")
                    self.append_log(f"     tokens in/out/reason = {in_tok}/{out_tok}/{r_tok}")

                    if r_summary:
                        self.append_log("     [Reasoning Summary]")
                        self.append_log("     " + r_summary.replace("\n", "\n     "))

                except Exception as e:
                    fail_rows += 1
                    self.append_log(f"[ERROR] 행 {idx} 처리 중 예외 발생: {e}")

                # 진행률 업데이트
                self.set_progress(idx + 1, total_rows)

                # ETA 계산 및 완료 예상 시각 업데이트
                try:
                    processed = idx + 1
                    if processed > 0 and total_rows > 0:
                        elapsed = time.time() - start_time_monotonic
                        per_row = elapsed / processed
                        remaining = max(total_rows - processed, 0)
                        eta_seconds = max(per_row * remaining, 0)

                        eta_min = int(eta_seconds // 60)
                        eta_sec = int(round(eta_seconds % 60))

                        if remaining == 0:
                            eta_text = "남은 예상 시간: 거의 완료"
                        else:
                            if eta_min > 0:
                                eta_text = f"남은 예상 시간: 약 {eta_min}분 {eta_sec}초"
                            else:
                                eta_text = f"남은 예상 시간: 약 {eta_sec}초"
                        self.set_eta(eta_text)

                        expected_end_dt = start_dt + timedelta(
                            seconds=elapsed + eta_seconds
                        )
                        self.set_finish_time(expected_end_dt)
                except Exception:
                    # ETA 계산 실패해도 전체 작업에는 영향 없음
                    pass

                # 중간 저장
                if (idx + 1) % save_every == 0:
                    out_path_tmp = os.path.join(base_dir, f"{base_name}_stage1_중간저장.xlsx")
                    try:
                        df.to_excel(out_path_tmp, index=False)
                        self.append_log(f"[SAVE] {idx+1}행까지 중간 저장: {out_path_tmp}")
                    except Exception as e:
                        self.append_log(f"[WARN] 중간 저장 실패: {e}")

            # 최종 저장 (T0 → T1로 버전 업)
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
                # 버전 정보가 없으면 T1_I0으로 생성
                out_filename = f"{base_name}_T1_I0.xlsx"
            out_path = os.path.join(base_dir, out_filename)
            try:
                df.to_excel(out_path, index=False)
                self.append_log(f"[DONE] 최종 엑셀 저장 완료: {out_path}")
                self.append_log(
                    f"[USAGE] total tokens in/out/reason = "
                    f"{total_in_tok}/{total_out_tok}/{total_reasoning_tok}"
                )

                # 토큰 평균 (API 호출 기준)
                total_api_tokens = total_in_tok + total_out_tok + total_reasoning_tok
                if api_rows > 0:
                    avg_in = total_in_tok / api_rows
                    avg_out = total_out_tok / api_rows
                    avg_reason = total_reasoning_tok / api_rows
                    avg_total = total_api_tokens / api_rows
                    self.append_log(
                        f"[TOKENS] OpenAI API 호출 {api_rows}행 기준 평균 tokens "
                        f"in/out/reason/합계 = "
                        f"{avg_in:.1f} / {avg_out:.1f} / {avg_reason:.1f} / {avg_total:.1f}"
                    )
                else:
                    self.append_log(
                        "[TOKENS] OpenAI API가 호출된 행이 없어 토큰 평균을 계산할 수 없습니다."
                    )

                cost_info = compute_cost_usd(model_name, total_in_tok, total_out_tokens=total_out_tok)
                if cost_info is not None:
                    in_cost, out_cost, total_cost = cost_info
                    self.append_log(
                        f"[COST] model={model_name} (캐시 미사용 기준 상한)\n"
                        f"       input_cost=${in_cost:.6f}, "
                        f"output_cost=${out_cost:.6f}, "
                        f"total=${total_cost:.6f}"
                    )
                    total_tokens_for_cost = total_in_tok + total_out_tok
                    if total_tokens_for_cost > 0:
                        cost_per_1k = total_cost / (total_tokens_for_cost / 1000.0)
                        self.append_log(
                            f"[COST] 총 토큰당 평균 비용 ≈ ${cost_per_1k:.6f} / 1K tokens"
                        )
                    if api_rows > 0:
                        cost_per_row = total_cost / api_rows
                        self.append_log(
                            f"[COST] OpenAI API 호출 행당 평균 비용 ≈ ${cost_per_row:.6f} / 행"
                        )
                else:
                    self.append_log(
                        "[COST] 이 모델은 비용 테이블이 정의되어 있지 않아, "
                        "토큰당 비용을 계산하지 못했습니다."
                    )

                # 실행 이력 기록용 비용 변수 정리
                if cost_info is not None:
                    in_cost, out_cost, total_cost = cost_info
                else:
                    in_cost = out_cost = total_cost = None

                # ---- 시간 통계 계산 ----
                elapsed = time.time() - start_time_monotonic
                elapsed_min = int(elapsed // 60)
                elapsed_sec = elapsed % 60

                self.append_log(
                    f"[TIME] 총 경과 시간: {elapsed_min}분 {elapsed_sec:.1f}초 "
                    f"(총 {elapsed:.1f}초)"
                )

                if processed_rows_total > 0:
                    avg_all = elapsed / processed_rows_total
                    self.append_log(
                        f"[TIME] 전체 {processed_rows_total}행 기준 평균 처리 시간: "
                        f"{avg_all:.3f}초/행"
                    )

                if api_rows > 0:
                    avg_api = elapsed / api_rows
                    self.append_log(
                        f"[TIME] OpenAI API 호출 {api_rows}행 기준 평균 처리 시간: "
                        f"{avg_api:.3f}초/행"
                    )

                # ---- 실행 이력 엑셀에 기록 ----
                finish_dt = get_seoul_now()
                append_run_history(
                    stage="ST1",
                    model_name=model_name,
                    reasoning_effort=effective_effort,
                    src_file=file_path,
                    total_rows=total_rows,
                    api_rows=api_rows,
                    elapsed_seconds=elapsed,
                    total_in_tok=total_in_tok,
                    total_out_tok=total_out_tok,
                    total_reasoning_tok=total_reasoning_tok,
                    input_cost_usd=in_cost,
                    output_cost_usd=out_cost,
                    total_cost_usd=total_cost,
                    start_dt=start_dt,
                    finish_dt=finish_dt,
                    api_type="per_call",
                    batch_id=None,
                    out_file=out_path,
                    success_rows=success_rows,
                    fail_rows=fail_rows,
                )
                
                # 메인 런처 현황판에 T1 완료 상태 기록 (img 상태는 변경하지 않음)
                try:
                    root_name = get_root_filename(out_path)
                    JobManager.update_status(root_name, text_msg="T1(완료)")
                    self.append_log(f"[INFO] 런처 현황판 업데이트: {root_name} -> T1(완료)")
                except Exception as e:
                    self.append_log(f"[WARN] 런처 현황판 업데이트 실패: {e}")

                # 완료 시 100%로 세팅 + 남은 시간/완료 시각
                if not stopped_by_user:
                    self.set_progress(total_rows, total_rows if total_rows > 0 else 1)
                    self.set_eta("남은 예상 시간: 작업 완료")
                    self.set_finish_time(finish_dt)
                else:
                    self.set_eta("남은 예상 시간: 작업 중단됨")
                    self._set_finish_time_on_main_thread("완료 예상 시각: 작업 중단됨")

                # 작업 완료/중단 안내 메시지를 메인 스레드에서 띄움
                self.after(0, self._show_done_message_on_main, out_path, stopped_by_user)

            except Exception as e:
                self.append_log(f"[ERROR] 최종 저장 실패: {e}")

        except Exception as e:
            self.append_log(f"[FATAL] 전체 작업 중 에러: {e}")


def main() -> None:
    app = Stage1GUI()
    app.mainloop()


if __name__ == "__main__":
    main()
