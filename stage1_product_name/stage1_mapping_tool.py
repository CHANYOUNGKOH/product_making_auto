from __future__ import annotations

"""
Stage1 상품명 정제용 맵핑 파일 생성 도구 (대화방용)

1단계 (Stage1 맵핑):
- 도매처 원본 엑셀을 읽어서 Stage1 작업에 필요한 최소 컬럼만 추려낸
  `*_stage1_mapping.xlsx` 파일을 만들어 준다.
- 각 행에는 LLM 채팅방에 그대로 붙여넣어 사용할 `ST1_프롬프트`가 포함된다.
- 사용자는 채팅방에서 받은 정제 상품명을 `ST1_결과상품명` 컬럼에 채워 넣으면 된다.

2단계 (썸네일 다운로드/맵핑):
- Stage1 맵핑 엑셀(또는 동일 구조의 엑셀)을 읽어서
  `이미지대` URL을 통해 1000x1000 흰 배경 썸네일 JPG를 다운로드/생성한다.
- 파일 이름: `판매자관리코드1 + "_01.jpg"` (파일명에 사용할 수 없는 문자는 '_' 처리)
- 폴더: `XXX_stage1_IMG`
- 엑셀: `XXX_stage1_img_mapping.xlsx` (새 컬럼 `썸네일경로` 에 이미지 전체 경로 기록)

※ 의존 라이브러리
- pandas
- requests
- pillow (PIL)
"""

import os
import io
import json
import pprint
import threading
import importlib.util
from datetime import datetime
from typing import Callable, Tuple, Any
import re
import requests
import pandas as pd
from PIL import Image

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

# =========================================================
# [런처 연동] JobManager & 유틸 (표준화됨)
# =========================================================
def get_root_filename(filename):
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T2_I1.xlsx -> 아디다스.xlsx
    예: 나이키_T1_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T1_I0_T2_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
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

def safe_save_excel(df, path):
    """파일이 열려있어 저장이 안 될 때 재시도를 유도하는 함수"""
    while True:
        try:
            df.to_excel(path, index=False)
            return True
        except PermissionError:
            if not messagebox.askretrycancel("저장 실패", f"엑셀 파일이 열려있습니다!\n[{os.path.basename(path)}]\n\n파일을 닫고 '다시 시도'를 눌러주세요."):
                return False
        except Exception as e:
            messagebox.showerror("오류", f"저장 중 알 수 없는 오류: {e}")
            return False

# =========================================================
#  설정 및 유틸
# =========================================================
# 전체 배경 색
BG_MAIN = "#f4f4f7"

# 썸네일 설정
THUMB_SIZE = 1000
THUMB_COL_NAME = "썸네일경로"

# Stage1 프롬프트 정의/유틸
try:
    from prompts_stage1 import safe_str, build_stage1_prompt  # type: ignore
except ImportError:
    # 테스트/개발용 더미 구현 (실제 사용 시에는 별도 모듈이 있어야 함)
    def safe_str(x: Any) -> str:
        return "" if x is None else str(x)

    def build_stage1_prompt(category: str, sale_type: str, raw_name: str) -> str:
        return f"[테스트 프롬프트] {category} | {sale_type} | {raw_name}"


LogFunc = Callable[[str], None]
SourceType = str


# =========================================================
#  판매형태 추론 유틸
# =========================================================
def looks_like_option_from_name(raw_name: str) -> bool:
    """원본 상품명만 봐도 옵션이 많을 것 같은 패턴을 간단히 감지."""
    name = safe_str(raw_name)
    if not name:
        return False

    option_keywords = [
        "색상", "컬러", "사이즈", "타입", "옵션",
        "선택", "호수", "호수선택",
        "세트", "구성", "혼합",
    ]
    hit = sum(1 for kw in option_keywords if kw in name)
    return hit >= 2


def infer_sale_type(option_raw: str, raw_name: str) -> str:
    """옵션1값 + 상품명으로 판매형태(단품형/옵션형)를 추론."""
    opt = safe_str(option_raw)
    name = safe_str(raw_name)

    if not opt:
        if looks_like_option_from_name(name):
            return "옵션형"
        return "단품형"

    # 여러 값이 나열된 것 같은 패턴
    multi_sep = any(ch in opt for ch in ["/", "|", ",", ";", "·"])
    if multi_sep:
        return "옵션형"

    # 쉼표/파이프 등으로 나뉘는 값이 2개 이상이면 옵션형
    parts = [p.strip() for p in opt.replace("|", ",").split(",") if p.strip()]
    if len(parts) >= 2:
        return "옵션형"

    if looks_like_option_from_name(name):
        return "옵션형"

    return "단품형"


# =========================================================
#  논리 컬럼 정의 & 내장 프리셋
# =========================================================
LOGICAL_COLS = [
    "판매자관리코드1",
    "상품코드",
    "카테고리명",
    "원본상품명",
    "마켓상품명",
    "옵션1값",
    "이미지대",
    "키워드",
    "본문상세설명",
]

# 오른쪽 힌트용 텍스트(실제 도매처 기준 설명)
OWNERCLAN_EXAMPLE_COLS: dict[str, str] = {
    "판매자관리코드1": "도매처 상품코드/관리코드",
    "상품코드": "도매처 상품코드",
    "카테고리명": "도매처 카테고리",
    "원본상품명": "도매처 상품명",
    "마켓상품명": "도매처 상품명",
    "옵션1값": "도매처 옵션값 (,로 구분된 형태)",
    "이미지대": "도매처 썸네일(대표 이미지 URL)",
    "키워드": "도매처 키워드",
    "본문상세설명": "도매처 상세설명 HTML",
}

# 내장 도매처 양식
SOURCE_PRESETS_BUILTIN: dict[str, dict[str, Any]] = {
    "ownerclan": {
        "display": "ownerclan · 오너클랜 기본 양식",
        "header_row": 2,  # 1-based
        "data_row": 3,    # 1-based
        "mapping": {
            "판매자관리코드1": "판매자관리코드1",
            "상품코드": "상품코드",
            "카테고리명": "카테고리명",
            "원본상품명": "원본상품명",
            "마켓상품명": "마켓상품명",
            "옵션1값": "옵션1값",
            "이미지대": "이미지대",
            "키워드": "키워드",
            "본문상세설명": "본문상세설명",
        },
    },
    "domeme": {
        "display": "domeme · 도매매 마이박스",
        "header_row": 1,  # 1-based
        "data_row": 3,    # 1-based
        "mapping": {
            "판매자관리코드1": "도매매 상품번호",
            "상품코드": "도매매 상품번호",
            "카테고리명": None,
            "원본상품명": "상품명",
            "마켓상품명": "상품명",
            "옵션1값": None,
            "이미지대": "대표이미지링크",
            "키워드": "키워드",
            "본문상세설명": None,
        },
    },
}


def load_source_presets() -> dict[str, dict[str, Any]]:
    """
    내장 프리셋 + 외부(stage1_source_presets.py/json) 프리셋을 합쳐서 반환.
    - ownerclan, domeme 키는 내장 프리셋이 우선 적용된다.
    """
    presets: dict[str, dict[str, Any]] = dict(SOURCE_PRESETS_BUILTIN)
    base_dir = os.path.dirname(__file__)

    # 1) 파이썬 파일 로드
    py_path = os.path.join(base_dir, "stage1_source_presets.py")
    if os.path.exists(py_path):
        try:
            spec = importlib.util.spec_from_file_location(
                "stage1_source_presets_custom", py_path
            )
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)  # type: ignore

                custom = getattr(mod, "SOURCE_PRESETS", None)
                if callable(custom):
                    custom_dict = custom()
                else:
                    custom_dict = custom
                if isinstance(custom_dict, dict):
                    for key, val in custom_dict.items():
                        if key in presets:
                            continue
                        if isinstance(val, dict):
                            presets[key] = val
        except Exception as e:
            print(f"[WARN] stage1_source_presets.py 로부터 도매처 양식 로드 실패: {e}")

    # 2) JSON 파일 로드
    json_path = os.path.join(base_dir, "stage1_source_presets.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            sources = data.get("sources", data)
            if isinstance(sources, dict):
                for key, val in sources.items():
                    if key in presets:
                        continue
                    if isinstance(val, dict):
                        presets[key] = val
        except Exception as e:
            print(f"[WARN] stage1_source_presets.json 로부터 도매처 양식 로드 실패: {e}")

    return presets


# 실제 사용되는 프리셋 딕셔너리
SOURCE_PRESETS: dict[str, dict[str, Any]] = load_source_presets()


# =========================================================
#  stage1_source_presets.py 저장/수정/삭제 유틸
# =========================================================
def _load_presets_from_py_file(py_path: str) -> dict[str, Any]:
    existing: dict[str, Any] = {}
    if not os.path.exists(py_path):
        return existing
    try:
        spec = importlib.util.spec_from_file_location(
            "stage1_source_presets_for_edit", py_path
        )
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            mod.__file__ = py_path  # type: ignore[attr-defined]
            spec.loader.exec_module(mod)  # type: ignore[arg-type]
            custom = getattr(mod, "SOURCE_PRESETS", None)
            if callable(custom):
                custom_dict = custom()
            else:
                custom_dict = custom
            if isinstance(custom_dict, dict):
                existing.update(custom_dict)
    except Exception as e:
        print(f"[WARN] stage1_source_presets.py 로부터 기존 도매처 양식 로드 실패: {e}")
    return existing


def save_preset_to_py_file(
    preset_id: str,
    display: str,
    header_row: int,
    data_row: int,
    mapping: dict[str, str],
) -> None:
    """도매처 양식을 stage1_source_presets.py 에 저장/갱신."""
    base_dir = os.path.dirname(__file__)
    py_path = os.path.join(base_dir, "stage1_source_presets.py")

    existing = _load_presets_from_py_file(py_path)
    existing[preset_id] = {
        "display": display,
        "header_row": int(header_row),
        "data_row": int(data_row),
        "mapping": dict(mapping),
    }

    content = (
        "# -*- coding: utf-8 -*-\n"
        "# 이 파일은 stage1_mapping_tool.py 에서 자동 생성/수정했습니다.\n"
        "# 필요하면 백업 후 직접 편집할 수 있습니다.\n\n"
        "SOURCE_PRESETS = "
        + pprint.pformat(existing, width=120, sort_dicts=False)
        + "\n"
    )

    with open(py_path, "w", encoding="utf-8") as f:
        f.write(content)


def delete_preset_from_py_file(preset_id: str) -> bool:
    """stage1_source_presets.py 에서 해당 프리셋을 제거."""
    base_dir = os.path.dirname(__file__)
    py_path = os.path.join(base_dir, "stage1_source_presets.py")

    existing = _load_presets_from_py_file(py_path)
    if preset_id not in existing:
        return False

    del existing[preset_id]

    content = (
        "# -*- coding: utf-8 -*-\n"
        "# 이 파일은 stage1_mapping_tool.py 에서 자동 생성/수정했습니다.\n"
        "# 필요하면 백업 후 직접 편집할 수 있습니다.\n\n"
        "SOURCE_PRESETS = "
        + pprint.pformat(existing, width=120, sort_dicts=False)
        + "\n"
    )

    with open(py_path, "w", encoding="utf-8") as f:
        f.write(content)

    return True


# =========================================================
#  엑셀 → Stage1 맵핑 파일 변환
# =========================================================
def _read_with_preset(
    excel_path: str,
    preset: dict[str, Any],
    log: LogFunc,
) -> pd.DataFrame:
    """프리셋(헤더행/데이터행)에 맞게 엑셀을 읽어 DataFrame 반환."""
    header_row_1based = int(preset.get("header_row", 1))
    data_row_1based = int(preset.get("data_row", header_row_1based + 1))

    header_idx = header_row_1based - 1  # pandas header 인덱스(0-based)
    data_idx = data_row_1based - 1      # 실제 데이터 시작 행(0-based, 원본 기준)

    log(f"[INFO] 헤더행={header_row_1based}행, 데이터 시작={data_row_1based}행 기준으로 읽기")
    df_raw = pd.read_excel(excel_path, header=header_idx, dtype=str)

    skip_data_rows = max(0, data_idx - (header_idx + 1))
    if skip_data_rows > 0:
        df_raw = df_raw.iloc[skip_data_rows:, :].reset_index(drop=True)
        log(f"[INFO] 헤더 이후 상단 {skip_data_rows}개 행을 건너뛰고 데이터 시작.")

    return df_raw


def process_excel_for_stage1(
    excel_path: str,
    source_type: SourceType = "ownerclan",
    log_func: LogFunc = print,
    progress_cb: Callable[[int, int], None] | None = None,
) -> Tuple[str, pd.DataFrame]:
    """원본 엑셀을 Stage1 맵핑용 엑셀로 변환."""
    log = log_func
    log(f"[INFO] 엑셀 읽는 중: {excel_path}")

    if source_type not in SOURCE_PRESETS:
        raise ValueError(
            f"지원하지 않는 도매처 양식입니다: {source_type}\n\n"
            f"사용 가능 양식: {list(SOURCE_PRESETS.keys())}"
        )

    preset = SOURCE_PRESETS[source_type]
    display_name = preset.get("display", source_type)
    mapping: dict[str, str | None] = preset.get("mapping", {})

    log(f"[INFO] 선택된 도매처 양식: {display_name} ({source_type})")
    log(f"[INFO] 사용 가능 도매처 양식 목록: {list(SOURCE_PRESETS.keys())}")

    # 1) 원본 읽기
    try:
        df_raw = _read_with_preset(excel_path, preset, log)
    except Exception as e:
        raise RuntimeError(f"엑셀 파일을 읽는 중 오류가 발생했습니다: {e}") from e

    # 2) 원본에서 반드시 있어야 하는 실제 컬럼 (None 제외)
    raw_required = sorted({col for col in mapping.values() if col is not None})
    missing_raw = [c for c in raw_required if c not in df_raw.columns]
    if missing_raw:
        cols_preview = ", ".join(list(df_raw.columns)[:30])
        raise ValueError(
            f"[{display_name}] 선택한 도매처 양식에서 요구하는 컬럼을 원본 엑셀에서 찾지 못했습니다.\n\n"
            f"- 누락된 컬럼(엑셀 헤더명): {missing_raw}\n\n"
            f"[도움말]\n"
            f"1) 도매처 엑셀의 실제 헤더 이름이 양식에 설정한 값과 일치하는지 확인하세요.\n"
            f"2) 헤더 행 번호(헤더 행 / 데이터 시작 행)가 맞는지 확인하세요.\n"
            f"3) 필요하다면 '도매처 양식 추가/수정'에서 매핑을 다시 설정해주세요.\n\n"
            f"현재 엑셀의 컬럼 목록 일부:\n{cols_preview}"
        )

    # 3) 논리 컬럼 기준으로 Stage1용 DF 구성
    df = pd.DataFrame()
    for logical_col in LOGICAL_COLS:
        actual = mapping.get(logical_col)

        if not actual:
            # 제공 안 되는 정보는 빈 값으로 생성
            df[logical_col] = ""
            log(f"[INFO] '{logical_col}' 은(는) 원본에서 제공되지 않아 빈 값으로 채웁니다.")
            continue

        if actual in df_raw.columns:
            df[logical_col] = df_raw[actual]
        else:
            df[logical_col] = ""
            log(
                f"[WARN] 논리컬럼 '{logical_col}'에 매핑된 실제 컬럼 '{actual}'을(를) "
                "원본에서 찾지 못해 빈 값으로 채웁니다."
            )

    # 4) 판매형태 / ST1 컬럼 준비
    if "판매형태" not in df.columns:
        df["판매형태"] = ""
    if "ST1_프롬프트" not in df.columns:
        df["ST1_프롬프트"] = ""
    if "ST1_결과상품명" not in df.columns:
        df["ST1_결과상품명"] = ""

    log(f"[INFO] 변환된 행 수: {len(df.index)}")
    log("[INFO] 판매형태 추론 및 ST1_프롬프트 생성 중...")

    total = len(df.index)
    if progress_cb:
        progress_cb(0, max(total, 1))

    for i, (idx, row) in enumerate(df.iterrows(), start=1):
        try:
            sale_type = safe_str(row.get("판매형태", ""))
            if not sale_type:
                option_raw = row.get("옵션1값", "")
                raw_name_for_type = row.get("원본상품명", "") or row.get("마켓상품명", "")
                sale_type = infer_sale_type(option_raw, raw_name_for_type)
                df.at[idx, "판매형태"] = sale_type

            category = safe_str(row.get("카테고리명", ""))
            raw_name = safe_str(row.get("원본상품명", "") or row.get("마켓상품명", ""))

            prompt = build_stage1_prompt(
                category=category,
                sale_type=sale_type,
                raw_name=raw_name,
            )
            df.at[idx, "ST1_프롬프트"] = prompt
        except Exception as e:
            log(f"[WARN] idx={idx} 프롬프트 생성 실패: {e}")

        if progress_cb:
            progress_cb(i, total)

    # 5) 엑셀 저장
    base_dir = os.path.dirname(excel_path)
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    # [수정] 파일명을 '_T0_I0.xlsx'로 지정 (초기 상태)
    out_path = os.path.join(base_dir, f"{base_name}_T0_I0.xlsx")


    if safe_save_excel(df, out_path):
        log(f"[완료] 마스터 파일 생성 (초기 버전): {os.path.basename(out_path)}")
        return out_path, df
    else:
        raise RuntimeError("파일 저장 실패")


# =========================================================
#  썸네일 다운로드 유틸
# =========================================================
def safe_filename(value: str) -> str:
    """윈도우 파일명에 쓸 수 없는 문자 치환."""
    if not value:
        return ""
    bad_chars = '\\/:*?"<>|'
    for ch in bad_chars:
        value = value.replace(ch, "_")
    value = value.strip()
    if not value:
        value = "no_code"
    return value


def extract_first_url(url_field: Any) -> str:
    """이미지대 컬럼에서 첫 번째 URL 비슷한 것만 뽑아내기."""
    s = safe_str(url_field).strip()
    if not s:
        return ""

    # 여러개가 | 나 , 로 묶여 있을 수 있음 → 첫 번째만 사용
    for sep in ["|", ",", " "]:
        if sep in s:
            s = s.split(sep)[0].strip()
    if not s:
        return ""

    if s.startswith("//"):
        s = "https:" + s
    return s


def make_square_thumbnail(img: Image.Image, size: int = THUMB_SIZE) -> Image.Image:
    """이미지를 비율 유지하면서 흰 배경 정사각형 썸네일로 변환."""
    img = img.convert("RGB")
    w, h = img.size
    if w == 0 or h == 0:
        raise ValueError("잘못된 이미지 크기")

    scale = min(size / w, size / h)
    new_w = max(1, int(w * scale))
    new_h = max(1, int(h * scale))

    resized = img.resize((new_w, new_h), Image.LANCZOS)
    canvas = Image.new("RGB", (size, size), (255, 255, 255))
    offset_x = (size - new_w) // 2
    offset_y = (size - new_h) // 2
    canvas.paste(resized, (offset_x, offset_y))
    return canvas


def download_and_build_thumbnail(
    url: str,
    out_dir: str,
    base_name: str,
    log: LogFunc,
) -> str:
    """
    단일 URL을 받아 1000x1000 썸네일을 만들어 저장하고 경로를 반환.
    실패 시 빈 문자열 반환.
    """
    if not url:
        return ""

    os.makedirs(out_dir, exist_ok=True)
    file_base = safe_filename(base_name)
    filename = f"{file_base}_01.jpg"
    out_path = os.path.join(out_dir, filename)

    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code != 200:
            log(f"[WARN] 이미지 다운로드 실패 ({resp.status_code}): {url}")
            return ""
        img = Image.open(io.BytesIO(resp.content))
        thumb = make_square_thumbnail(img, THUMB_SIZE)
        thumb.save(out_path, "JPEG", quality=90, optimize=True)
        return out_path
    except Exception as e:
        log(f"[WARN] 썸네일 생성 실패 ({base_name}): {e}")
        return ""


# =========================================================
#  GUI: 도매처 양식 추가/수정 다이얼로그
# =========================================================
class AddPresetDialog(tk.Toplevel):
    """도매처 엑셀 양식을 GUI에서 추가/수정하기 위한 다이얼로그."""

    def __init__(
        self,
        master: tk.Misc,
        logical_cols: list[str],
        initial: dict[str, Any] | None = None,
        edit_mode: bool = False,
    ):
        super().__init__(master)
        self.title("새 도매처 양식 추가" if not edit_mode else "도매처 양식 수정")
        self.resizable(False, True)
        self.configure(bg=BG_MAIN)

        self.result: dict[str, Any] | None = None
        self.logical_cols = logical_cols
        self.initial = initial or {}
        self.edit_mode = edit_mode

        self._build_ui()
        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.on_cancel)

    def _build_ui(self) -> None:
        frm = tk.Frame(self, bg=BG_MAIN)
        frm.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        row = 0
        title_id = "도매처 양식 ID (내부용 키)"
        tk.Label(frm, text=title_id, anchor="w", bg=BG_MAIN).grid(
            row=row, column=0, sticky="w"
        )
        self.entry_id = tk.Entry(frm, width=25)
        self.entry_id.grid(row=row, column=1, sticky="we", padx=(5, 0))

        if "id" in self.initial:
            self.entry_id.insert(0, str(self.initial["id"]))

        if self.edit_mode:
            help_text = (
                "※ 이미 생성된 도매처 양식의 ID 입니다. 수정할 수 없습니다.\n"
                "   예: myshop, coupang, naver_mall"
            )
            self.entry_id.config(state="readonly")
        else:
            help_text = (
                "※ stage1_source_presets.py 의 SOURCE_PRESETS 딕셔너리에서 사용되는 키입니다.\n"
                "   예: myshop, coupang, naver_mall"
            )

        tk.Label(
            frm,
            text=help_text,
            anchor="w",
            fg="#555555",
            justify="left",
            bg=BG_MAIN,
        ).grid(row=row + 1, column=0, columnspan=3, sticky="w", pady=(2, 6))
        row += 2

        tk.Label(frm, text="표시 이름 (콤보박스에 보일 이름)", anchor="w", bg=BG_MAIN).grid(
            row=row, column=0, sticky="w"
        )
        self.entry_display = tk.Entry(frm, width=30)
        self.entry_display.grid(row=row, column=1, sticky="we", padx=(5, 0))
        if "display" in self.initial:
            self.entry_display.insert(0, str(self.initial["display"]))
        row += 1

        tk.Label(
            frm,
            text="헤더 행 / 데이터 시작 행 (1-based)",
            anchor="w",
            bg=BG_MAIN,
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(8, 0))
        row += 1

        tk.Label(frm, text="헤더 행 번호:", anchor="w", bg=BG_MAIN).grid(
            row=row, column=0, sticky="w"
        )
        self.entry_header_row = tk.Entry(frm, width=8)
        hdr_default = self.initial.get("header_row", 1)
        self.entry_header_row.insert(0, str(hdr_default))
        self.entry_header_row.grid(row=row, column=1, sticky="w", padx=(5, 0))
        row += 1

        tk.Label(frm, text="데이터 시작 행 번호:", anchor="w", bg=BG_MAIN).grid(
            row=row, column=0, sticky="w"
        )
        self.entry_data_row = tk.Entry(frm, width=8)
        data_default = self.initial.get("data_row", 2)
        self.entry_data_row.insert(0, str(data_default))
        self.entry_data_row.grid(row=row, column=1, sticky="w", padx=(5, 0))
        row += 1

        # 필수 컬럼 규칙 안내
        tk.Label(
            frm,
            text=(
                "도매처 엑셀의 실제 컬럼명을 아래에 적어주세요.\n"
                "※ 필수 항목\n"
                "   - [판매자관리코드1] 또는 [상품코드] 중 1개 이상 (도매처 상품코드)\n"
                "   - [카테고리명] (도매처 카테고리)\n"
                "   - [원본상품명] 또는 [마켓상품명] 중 1개 이상 (도매처 상품명)\n"
                "   - [이미지대] (도매처 썸네일 / 대표 이미지 URL)\n"
                "   - [본문상세설명] (도매처 상세 설명 HTML/텍스트)"
            ),
            anchor="w",
            justify="left",
            fg="#aa0000",
            bg=BG_MAIN,
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(10, 4))
        row += 1

        tk.Label(
            frm,
            text="오른쪽 설명은 오너클랜 기본 양식을 기준으로 한 예시입니다.",
            anchor="w",
            fg="#555555",
            bg=BG_MAIN,
        ).grid(row=row, column=0, columnspan=3, sticky="w", pady=(0, 4))
        row += 1

        # 논리 컬럼별 매핑 입력 + 오른쪽 설명
        self.entry_mapping: dict[str, tk.Entry] = {}
        initial_mapping: dict[str, str] = self.initial.get("mapping", {}) or {}

        for logical_col in self.logical_cols:
            tk.Label(frm, text=f"{logical_col}:", anchor="w", bg=BG_MAIN).grid(
                row=row, column=0, sticky="w", pady=1
            )
            e = tk.Entry(frm, width=30)
            e.grid(row=row, column=1, sticky="we", padx=(5, 0), pady=1)
            self.entry_mapping[logical_col] = e

            if logical_col in initial_mapping:
                e.insert(0, str(initial_mapping[logical_col]))

            hint = OWNERCLAN_EXAMPLE_COLS.get(logical_col, "")
            tk.Label(
                frm,
                text=hint,
                anchor="w",
                fg="#777777",
                bg=BG_MAIN,
            ).grid(row=row, column=2, sticky="w", padx=(6, 0))
            row += 1

        # 버튼
        btn_frame = tk.Frame(frm, bg=BG_MAIN)
        btn_frame.grid(row=row, column=0, columnspan=3, pady=(10, 0), sticky="e")

        btn_ok = tk.Button(
            btn_frame,
            text="저장" if not self.edit_mode else "수정 내용 저장",
            command=self.on_ok,
            width=12,
        )
        btn_ok.pack(side=tk.LEFT, padx=5)
        btn_cancel = tk.Button(btn_frame, text="취소", command=self.on_cancel, width=10)
        btn_cancel.pack(side=tk.LEFT, padx=5)

    def on_ok(self) -> None:
        preset_id = self.entry_id.get().strip()
        display = self.entry_display.get().strip() or preset_id

        if not preset_id:
            messagebox.showwarning("경고", "도매처 양식 ID를 입력해주세요.")
            return

        try:
            header_row = int(self.entry_header_row.get().strip() or "1")
            data_row = int(self.entry_data_row.get().strip() or "2")
        except ValueError:
            messagebox.showwarning("경고", "헤더 행/데이터 시작 행은 숫자로 입력해주세요.")
            return

        # 전체 매핑 임시 수집
        mapping_full: dict[str, str] = {}
        for logical_col, entry in self.entry_mapping.items():
            actual = entry.get().strip()
            if actual:
                mapping_full[logical_col] = actual

        # 필수 컬럼 검증
        def has(col: str) -> bool:
            return bool(mapping_full.get(col))

        errors: list[str] = []
        if not (has("판매자관리코드1") or has("상품코드")):
            errors.append("- '판매자관리코드1' 또는 '상품코드' 중 하나는 반드시 매핑해야 합니다.")
        if not has("카테고리명"):
            errors.append("- '카테고리명' 매핑은 필수입니다.")
        if not (has("원본상품명") or has("마켓상품명")):
            errors.append("- '원본상품명' 또는 '마켓상품명' 중 하나는 반드시 매핑해야 합니다.")
        if not has("이미지대"):
            errors.append("- '이미지대' 매핑은 필수입니다. (도매처 썸네일/대표 이미지)")
        if not has("본문상세설명"):
            errors.append("- '본문상세설명' 매핑은 필수입니다. (도매처 상세설명 HTML)")

        if errors:
            messagebox.showwarning(
                "필수 컬럼 누락",
                "다음 필수 매핑이 누락되었습니다:\n\n" + "\n".join(errors),
            )
            return

        mapping: dict[str, str] = dict(mapping_full)

        self.result = {
            "id": preset_id,
            "display": display,
            "header_row": header_row,
            "data_row": data_row,
            "mapping": mapping,
        }
        self.destroy()

    def on_cancel(self) -> None:
        self.result = None
        self.destroy()


# =========================================================
#  Tkinter GUI (메인)
# =========================================================
class Stage1MappingApp:
    """Stage1 맵핑 파일 생성 + 썸네일 다운로드/맵핑 + 프롬프트 미리보기/복사 GUI."""

    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("STEP.0 상품명/이미지 맵핑 파일만들기)")

        # 화면 크기에 맞게 기본 사이즈 조정
        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()
        default_w, default_h = 1080, 820
        width = min(default_w, max(960, screen_w - 200))
        height = min(default_h, max(700, screen_h - 200))
        root.geometry(f"{width}x{height}")
        root.minsize(960, 700)
        root.configure(bg=BG_MAIN)

        # 기본 폰트
        root.option_add("*Font", ("맑은 고딕", 9))
        root.option_add("*Label.font", ("맑은 고딕", 9))
        root.option_add("*Button.font", ("맑은 고딕", 9))

        # ttk 스타일
        try:
            style = ttk.Style()
            style.theme_use("clam")
            style.configure("TButton", padding=6)
            style.configure(
                "Accent.TButton",
                padding=6,
                foreground="white",
                background="#2563eb",
            )
            style.map("Accent.TButton", background=[("active", "#1d4ed8")])
        except tk.TclError:
            # 일부 환경에서는 theme_use 가 실패할 수 있으므로 무시
            pass

        # 상태 값
        self.current_df: pd.DataFrame | None = None
        self.current_out_path: str | None = None
        self.current_index: int = 0

        self.excel_path_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="대기 중")
        self.progress_var = tk.IntVar(value=0)
        self.progress_text_var = tk.StringVar(value="")
        self.is_running = False

        self.source_type_var = tk.StringVar(value="")
        self.source_display_to_key: dict[str, str] = {}
        self.source_key_to_display: dict[str, str] = {}

        # 위젯 레퍼런스
        self.source_combo: ttk.Combobox | None = None
        self.preview_text: ScrolledText | None = None
        self.log_text: ScrolledText | None = None
        self.btn_run_stage1: ttk.Button | None = None
        self.btn_run_thumb: ttk.Button | None = None

        self._build_ui()
        self._reload_presets_into_combo()

    # [누락된 함수 추가]
    def _worker_stage1(self, path, key):
        self.status_var.set("Step-0 맵핑 생성 중...")
        self.progress_var.set(0)
        try:
            # 진행률 콜백 함수 정의
            def log_cb(msg: str) -> None:
                self.root.after(0, lambda: self.append_log(msg))

            def progress_cb(cur: int, tot: int) -> None:
                self.root.after(0, lambda: self.set_progress(cur, tot))

            # 엑셀 변환 실행
            out_path, df = process_excel_for_stage1(
                path,
                source_type=key,
                log_func=log_cb,
                progress_cb=progress_cb,
            )
            
            # 완료 후 처리
            def done():
                self.current_df = df
                self.current_out_path = out_path
                # 맵핑 완료 후 자동으로 *_T0_I0.xlsx 선택
                self.excel_path_var.set(out_path)
                self._set_running_state(False)
                self.set_status(f"Step-0  맵핑 완료: {out_path}")
                self.set_progress(len(df), len(df))
                self.refresh_preview()

                # [런처 연동] 상태 업데이트
                root_name = get_root_filename(out_path)
                # T0_I0 상태 알림
                JobManager.update_status(root_name, text_msg="T0 (대기)", img_msg="I0 (대기)")
                
                messagebox.showinfo("완료", f"마스터 파일이 생성되었습니다.\n[{os.path.basename(out_path)}]\n이제 '썸네일 다운로드'를 진행하세요.")
                self.status_var.set("Step-0(매핑) 완료")

            self.root.after(0, done)

        except Exception as e:
            def err():
                self._set_running_state(False)
                self.set_status("Step-0 맵핑 실행 중 오류 발생")
                messagebox.showerror("오류", f"Step-0 맵핑 실행 중 오류가 발생했습니다:\n{e}")
            self.root.after(0, err)
    # ---------- UI ----------
    def _build_ui(self) -> None:
        # 상단 헤더 바
        header = tk.Frame(self.root, bg="#e5edff")
        header.pack(fill=tk.X)
        tk.Label(
            header,
            text="StEP-0 도매처 원본 정제 도구",
            bg="#e5edff",
            font=("맑은 고딕", 11, "bold"),
        ).pack(side=tk.LEFT, padx=10, pady=6)
        tk.Label(
            header,
            text="도매처 엑셀 → STEP 0 실행 → 썸네일 다운",
            bg="#e5edff",
            fg="#555555",
        ).pack(side=tk.LEFT, pady=6)

        desc = (
            "① 도매처 양식을 선택한 뒤, 원본 엑셀 파일을 선택하세요.\n"
            "② Stage1 용 엑셀(*_T0_I0.xlsx)을 생성합니다.\n"
            "   (완료 시 자동으로 *_T0_I0.xlsx 파일이 선택됩니다.)\n"
            "③ '썸네일 다운로드/맵핑'을 누르면 이미지대 URL을 이용해\n"
            "   1000x1000 썸네일 JPG와 *_T0_I0.xlsx 을 생성합니다.\n"
            "   (완료 시 *_T0_I0.xlsx 파일이 자동 선택됩니다.)\n"
        )
        tk.Label(
            self.root,
            text=desc,
            justify="left",
            anchor="w",
            bg=BG_MAIN,
        ).pack(fill=tk.X, padx=10, pady=(8, 4))

        # 상단: 도매처 양식 선택 + 파일 선택
        top_frame = tk.Frame(self.root, bg=BG_MAIN)
        top_frame.pack(fill=tk.X, padx=10, pady=4)

        # 도매처 양식 선택
        source_frame = tk.LabelFrame(top_frame, text="도매처 양식 선택")
        source_frame.pack(side=tk.LEFT, padx=(0, 10), pady=2, fill=tk.Y)

        tk.Label(source_frame, text="도매처 양식:", anchor="w").pack(
            fill=tk.X, padx=6, pady=(4, 0)
        )
        self.source_combo = ttk.Combobox(
            source_frame,
            state="readonly",
            values=[],
        )
        self.source_combo.pack(fill=tk.X, padx=6, pady=(0, 4))
        self.source_combo.bind("<<ComboboxSelected>>", self.on_source_combo_changed)

        tk.Button(
            source_frame,
            text="도매처 양식 추가...",
            command=self.on_add_preset,
        ).pack(fill=tk.X, padx=6, pady=(0, 2))

        tk.Button(
            source_frame,
            text="선택한 양식 수정...",
            command=self.on_edit_preset,
        ).pack(fill=tk.X, padx=6, pady=(0, 2))

        tk.Button(
            source_frame,
            text="선택한 양식 삭제...",
            command=self.on_delete_preset,
        ).pack(fill=tk.X, padx=6, pady=(0, 6))

        tk.Label(
            source_frame,
            text="※ ownerclan, domeme 등 기본 제공 양식은 삭제/수정할 수 없습니다.\n"
                 "   필요하면 복사해서 새 양식을 만들어 사용해주세요.",
            justify="left",
            fg="#555555",
        ).pack(fill=tk.X, padx=6, pady=(0, 8))

        # 파일 선택 + Stage1/썸네일 버튼
        file_frame = tk.LabelFrame(top_frame, text="원본 엑셀 선택 및 변환")
        file_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, pady=2)

        file_inner = tk.Frame(file_frame)
        file_inner.pack(fill=tk.BOTH, expand=True, padx=8, pady=(6, 8))

        tk.Label(file_inner, text="엑셀 파일", anchor="w").grid(
            row=0, column=0, sticky="w"
        )
        tk.Button(
            file_inner,
            text="찾아보기...",
            command=self.on_browse_file,
            width=12,
        ).grid(row=0, column=1, sticky="e")

        tk.Label(
            file_inner,
            textvariable=self.excel_path_var,
            anchor="w",
            fg="#555555",
            wraplength=500,
            justify="left",
        ).grid(row=1, column=0, columnspan=2, sticky="we", pady=(4, 8))

        file_inner.columnconfigure(0, weight=1)

        # STEP 0 원본엑셀 변환 실행
        self.btn_run_stage1 = ttk.Button(
            file_inner,
            text="TEXT STEP 0 : 원본엑셀 변환 실행",
            command=self.on_run_stage1,
            style="Accent.TButton",
        )
        self.btn_run_stage1.grid(row=2, column=0, columnspan=2, sticky="ew")

        # 썸네일 다운로드/맵핑 버튼
        self.btn_run_thumb = ttk.Button(
            file_inner,
            text="IMG STEP0 : 썸네일 다운로드/맵핑",
            command=self.on_run_thumbnail_mapping,
        )
        self.btn_run_thumb.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(6, 0))

        # 진행 상태
        status_frame = tk.LabelFrame(self.root, text="진행 상태")
        status_frame.pack(fill=tk.X, padx=10, pady=(0, 4))

        tk.Label(
            status_frame,
            textvariable=self.status_var,
            anchor="w",
        ).pack(fill=tk.X, padx=8, pady=(4, 2))

        ttk.Progressbar(
            status_frame,
            orient="horizontal",
            mode="determinate",
            maximum=100,
            variable=self.progress_var,
        ).pack(fill=tk.X, padx=8, pady=(0, 2))

        tk.Label(
            status_frame,
            textvariable=self.progress_text_var,
            anchor="e",
            fg="#555555",
        ).pack(fill=tk.X, padx=8, pady=(0, 4))

        # 프리뷰 + 로그 (가로 분할)
        main_paned = tk.PanedWindow(self.root, orient=tk.HORIZONTAL, sashrelief=tk.RAISED)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=(4, 10))

        # 왼쪽: 프리뷰
        preview_frame = tk.LabelFrame(main_paned, text="ST1 프롬프트 / 행 정보 미리보기")
        main_paned.add(preview_frame, stretch="always")

        nav_frame = tk.Frame(preview_frame)
        nav_frame.pack(fill=tk.X, padx=6, pady=(4, 2))

        tk.Button(nav_frame, text="◀ 이전", width=8, command=self.on_prev_row).pack(
            side=tk.LEFT
        )
        tk.Button(nav_frame, text="다음 ▶", width=8, command=self.on_next_row).pack(
            side=tk.LEFT, padx=(4, 0)
        )
        tk.Button(
            nav_frame,
            text="현재 행 프롬프트 복사",
            width=18,
            command=self.on_copy_prompt,
        ).pack(side=tk.RIGHT)

        self.preview_text = ScrolledText(preview_frame, height=20)
        self.preview_text.pack(fill=tk.BOTH, expand=True, padx=6, pady=(0, 6))

        # 오른쪽: 로그
        log_frame = tk.LabelFrame(main_paned, text="로그")
        main_paned.add(log_frame, stretch="always")

        self.log_text = ScrolledText(log_frame, height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        self.append_log("[INFO] 프로그램이 준비되었습니다.")

    # ---------- 프리셋 관련 ----------
    def _reload_presets_into_combo(self) -> None:
        global SOURCE_PRESETS
        SOURCE_PRESETS = load_source_presets()

        self.source_display_to_key.clear()
        self.source_key_to_display.clear()

        items: list[str] = []
        for key, val in SOURCE_PRESETS.items():
            display = val.get("display", key)
            items.append(display)
            self.source_display_to_key[display] = key
            self.source_key_to_display[key] = display

        if self.source_combo is not None:
            self.source_combo["values"] = items

            # 기본 선택: ownerclan 또는 첫 번째
            default_key = "ownerclan" if "ownerclan" in SOURCE_PRESETS else None
            if default_key is None and items:
                default_key = self.source_display_to_key[items[0]]

            if default_key:
                display = self.source_key_to_display.get(default_key, default_key)
                self.source_combo.set(display)
                self.source_type_var.set(default_key)

    def on_source_combo_changed(self, event=None) -> None:
        if self.source_combo is None:
            return
        display = self.source_combo.get()
        key = self.source_display_to_key.get(display, "")
        self.source_type_var.set(key)
        self.append_log(f"[INFO] 선택한 도매처 양식: {display} ({key})")

    def on_add_preset(self) -> None:
        dlg = AddPresetDialog(self.root, LOGICAL_COLS, initial=None, edit_mode=False)
        self.root.wait_window(dlg)
        if dlg.result:
            res = dlg.result
            try:
                save_preset_to_py_file(
                    preset_id=res["id"],
                    display=res["display"],
                    header_row=res["header_row"],
                    data_row=res["data_row"],
                    mapping=res["mapping"],
                )
                self.append_log(f"[INFO] 도매처 양식 추가/저장 완료: {res['id']}")
                self._reload_presets_into_combo()
            except Exception as e:
                messagebox.showerror("오류", f"도매처 양식 저장 중 오류가 발생했습니다.\n\n{e}")

    def on_edit_preset(self) -> None:
        key = self.source_type_var.get().strip()
        if not key:
            messagebox.showwarning("경고", "수정할 도매처 양식을 먼저 선택해주세요.")
            return
        if key in SOURCE_PRESETS_BUILTIN:
            messagebox.showwarning(
                "경고",
                "ownerclan, domeme 등 기본 제공 양식은 직접 수정할 수 없습니다.\n"
                "복사해서 새 양식을 만들어 사용해주세요.",
            )
            return

        preset = SOURCE_PRESETS.get(key)
        if not preset:
            messagebox.showerror("오류", "선택한 도매처 양식을 찾을 수 없습니다.")
            return

        initial = {
            "id": key,
            "display": preset.get("display", key),
            "header_row": preset.get("header_row", 1),
            "data_row": preset.get("data_row", 2),
            "mapping": preset.get("mapping", {}),
        }
        dlg = AddPresetDialog(self.root, LOGICAL_COLS, initial=initial, edit_mode=True)
        self.root.wait_window(dlg)
        if dlg.result:
            res = dlg.result
            try:
                save_preset_to_py_file(
                    preset_id=key,
                    display=res["display"],
                    header_row=res["header_row"],
                    data_row=res["data_row"],
                    mapping=res["mapping"],
                )
                self.append_log(f"[INFO] 도매처 양식 수정 완료: {key}")
                self._reload_presets_into_combo()
            except Exception as e:
                messagebox.showerror("오류", f"도매처 양식 수정 중 오류가 발생했습니다.\n\n{e}")

    def on_delete_preset(self) -> None:
        key = self.source_type_var.get().strip()
        if not key:
            messagebox.showwarning("경고", "삭제할 도매처 양식을 먼저 선택해주세요.")
            return
        if key in SOURCE_PRESETS_BUILTIN:
            messagebox.showwarning(
                "경고",
                "ownerclan, domeme 등 기본 제공 양식은 삭제할 수 없습니다.",
            )
            return

        display = self.source_key_to_display.get(key, key)
        if not messagebox.askyesno(
            "확인",
            f"정말로 도매처 양식 '{display}' ({key}) 을(를) 삭제하시겠습니까?\n"
            f"stage1_source_presets.py 파일에서도 제거됩니다.",
        ):
            return

        try:
            ok = delete_preset_from_py_file(key)
            if ok:
                self.append_log(f"[INFO] 도매처 양식 삭제 완료: {key}")
            else:
                self.append_log(f"[WARN] 도매처 양식 삭제 실패 (존재하지 않음): {key}")
            self._reload_presets_into_combo()
        except Exception as e:
            messagebox.showerror("오류", f"도매처 양식 삭제 중 오류가 발생했습니다.\n\n{e}")

    # ---------- 공통 유틸 ----------
    def append_log(self, msg: str) -> None:
        ts = datetime.now().strftime("%m-%d %H:%M")
        line = f"[{ts}] {msg}"
        print(line)
        if self.log_text is not None:
            self.log_text.insert(tk.END, line + "\n")
            self.log_text.see(tk.END)

    def set_status(self, text: str) -> None:
        self.status_var.set(text)

    def set_progress(self, current: int, total: int) -> None:
        total = max(total, 1)
        percent = int(current / total * 100)
        self.progress_var.set(percent)
        self.progress_text_var.set(f"{current} / {total}  ({percent}%)")

    def _set_running_state(self, running: bool) -> None:
        self.is_running = running
        state = tk.DISABLED if running else tk.NORMAL
        if self.btn_run_stage1 is not None:
            self.btn_run_stage1.config(state=state)
        if self.btn_run_thumb is not None:
            self.btn_run_thumb.config(state=state)

    # ---------- 파일 선택 ----------
    def on_browse_file(self) -> None:
        path = filedialog.askopenfilename(
            title="엑셀 파일 선택",
            filetypes=[
                ("Excel files", "*.xlsx;*.xlsm;*.xls"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.excel_path_var.set(path)

    # ---------- Stage1 맵핑 실행 ----------
    def on_run_stage1(self) -> None:
        if self.is_running:
            messagebox.showwarning("경고", "이미 작업이 진행 중입니다.")
            return

        excel_path = self.excel_path_var.get().strip()
        if not excel_path:
            messagebox.showwarning("주의", "원본 도매처 엑셀 파일을 선택해주세요.")
            return
        if not os.path.exists(excel_path):
            messagebox.showerror("오류", f"파일을 찾을 수 없습니다:\n{excel_path}")
            return

        source_key = self.source_type_var.get().strip()
        if not source_key:
            messagebox.showwarning("주의", "도매처 양식을 먼저 선택해주세요.")
            return

        self._set_running_state(True)
        self.set_status("Step-0 맵핑 실행 중...")
        self.set_progress(0, 1)
        self.append_log(f"[INFO] Step-0 맵핑 실행 시작: {excel_path} ({source_key})")

        def worker():
            try:
                def log_cb(msg: str) -> None:
                    self.root.after(0, lambda: self.append_log(msg))

                def progress_cb(cur: int, tot: int) -> None:
                    self.root.after(0, lambda: self.set_progress(cur, tot))

                out_path, df = process_excel_for_stage1(
                    excel_path,
                    source_type=source_key,
                    log_func=log_cb,
                    progress_cb=progress_cb,
                )

                def done():
                    self.current_df = df
                    self.current_out_path = out_path
                    # 맵핑 완료 후 자동으로 *_T0_I0.xlsx 선택
                    self.excel_path_var.set(out_path)
                    self._set_running_state(False)
                    self.set_status(f"Step-0  맵핑 완료: {out_path}")
                    self.set_progress(len(df), len(df))
                    self.refresh_preview()

                    # ▼▼▼ [수정된 부분] ▼▼▼
                    # 1. 뿌리 이름 추출
                    root_name = get_root_filename(out_path)
                    # 2. 뿌리 이름으로 상태 업데이트
                    JobManager.update_status(root_name, text_msg="T0(완료)", img_msg="I0(완료)")  
                    messagebox.showinfo("완료", f"Step-0 원본 변환 완료:\n{out_path}")

                self.root.after(0, done)
            except Exception as e:
                def err():
                    self._set_running_state(False)
                    self.set_status("Step-0 맵핑 실행 중 오류 발생")
                    messagebox.showerror("오류", f"Step-0 맵핑 실행 중 오류가 발생했습니다:\n{e}")
                self.root.after(0, err)

        threading.Thread(target=worker, daemon=True).start()

    # ---------- 썸네일 다운로드/맵핑 ----------
    def on_run_thumbnail_mapping(self) -> None:
        if self.is_running:
            messagebox.showwarning("경고", "이미 작업이 진행 중입니다.")
            return

        excel_path = self.excel_path_var.get().strip()
        if not excel_path:
            messagebox.showwarning("주의", "썸네일을 생성할 Step-0 맵핑 엑셀(또는 동일 구조의 엑셀)을 선택해주세요.")
            return
        if not os.path.exists(excel_path):
            messagebox.showerror("오류", f"파일을 찾을 수 없습니다:\n{excel_path}")
            return

        self._set_running_state(True)
        self.set_status("썸네일 다운로드/맵핑 실행 중...")
        self.set_progress(0, 1)
        self.append_log(f"[INFO] 썸네일 다운로드/맵핑 시작: {excel_path}")

        def worker():
            try:
                df = pd.read_excel(excel_path, dtype=str)
                total = len(df.index)

                if "이미지대" not in df.columns:
                    raise ValueError("엑셀에 '이미지대' 컬럼이 없습니다.")
                if "판매자관리코드1" not in df.columns and "상품코드" not in df.columns:
                    raise ValueError(
                        "엑셀에 '판매자관리코드1' 또는 '상품코드' 컬럼이 없습니다."
                    )

                # 썸네일경로 컬럼 준비
                if THUMB_COL_NAME not in df.columns:
                    df[THUMB_COL_NAME] = ""

                base_dir = os.path.dirname(excel_path)
                mapping_base = os.path.splitext(os.path.basename(excel_path))[0]

                # XXX_stage1_mapping → XXX 추출
                stem = mapping_base
                if stem.endswith("_stage1_mapping"):
                    stem = stem[: -len("_stage1_mapping")]

                img_dir = os.path.join(base_dir, f"{stem}_stage1_IMG")
                out_excel = os.path.join(base_dir, f"{stem}_stage1_img_mapping.xlsx")

                self.root.after(
                    0,
                    lambda: self.append_log(
                        f"[INFO] 썸네일 출력 폴더: {img_dir}\n"
                        f"[INFO] 썸네일 맵핑 엑셀: {out_excel}"
                    ),
                )

                for i, (idx, row) in enumerate(df.iterrows(), start=1):
                    seller_code = safe_str(
                        row.get("판매자관리코드1", "") or row.get("상품코드", "")
                    )
                    url_field = row.get("이미지대", "")
                    url = extract_first_url(url_field)

                    thumb_path = ""
                    if url and seller_code:
                        thumb_path = download_and_build_thumbnail(
                            url=url,
                            out_dir=img_dir,
                            base_name=seller_code,
                            log=lambda m: self.root.after(0, lambda: self.append_log(m)),
                        )

                    df.at[idx, THUMB_COL_NAME] = thumb_path

                    # 진행 표시
                    def update_progress(i=i, total=total):
                        self.set_progress(i, total)
                    self.root.after(0, update_progress)


                
                # 1. 엑셀 저장 (기존 파일에 덮어쓰기)
                df.to_excel(excel_path, index=False)

                def done():
                    self.current_df = df
                    self.current_out_path = excel_path
                    self._set_running_state(False)
                    self.set_status(f"썸네일 완료: {excel_path}")
                    self.refresh_preview()


                    # 1. 뿌리 이름 추출
                    root_name = get_root_filename(excel_path)
                    # 2. 뿌리 이름으로 상태 업데이트
                    JobManager.update_status(root_name, text_msg="T0(완료)", img_msg="I0(완료)")
                    # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

                    messagebox.showinfo(
                        "완료",
                        "썸네일 추출 및 맵핑이 완료되었습니다.\n(기존 파일에 저장됨)"
                    )

                self.root.after(0, done)
                # ▲▲▲▲▲ [여기까지 수정된 부분입니다] ▲▲▲▲▲
                
            except Exception as e:
                def err():
                    self._set_running_state(False)
                    self.set_status("썸네일 다운로드/맵핑 중 오류 발생")
                    messagebox.showerror(
                        "오류",
                        f"썸네일 다운로드/맵핑 중 오류가 발생했습니다:\n{e}",
                    )
                self.root.after(0, err)

        threading.Thread(target=worker, daemon=True).start()

    # ---------- 프리뷰 / 네비게이션 ----------
    def refresh_preview(self) -> None:
        if self.preview_text is None:
            return

        self.preview_text.delete("1.0", tk.END)

        if self.current_df is None or self.current_df.empty:
            self.preview_text.insert(tk.END, "아직 로드된 Stage1 맵핑 데이터가 없습니다.\n")
            return

        total = len(self.current_df.index)
        if total <= 0:
            self.preview_text.insert(tk.END, "행이 없습니다.\n")
            return

        if self.current_index < 0:
            self.current_index = 0
        if self.current_index >= total:
            self.current_index = total - 1

        row = self.current_df.iloc[self.current_index]
        idx_display = self.current_index + 1

        lines = []
        lines.append(f"[행 번호] {idx_display} / {total}")
        lines.append(f"[판매자관리코드1] {safe_str(row.get('판매자관리코드1', ''))}")
        lines.append(f"[상품코드] {safe_str(row.get('상품코드', ''))}")
        lines.append(f"[카테고리명] {safe_str(row.get('카테고리명', ''))}")
        lines.append(f"[원본상품명] {safe_str(row.get('원본상품명', ''))}")
        lines.append(f"[마켓상품명] {safe_str(row.get('마켓상품명', ''))}")
        lines.append(f"[판매형태] {safe_str(row.get('판매형태', ''))}")
        lines.append(f"[옵션1값] {safe_str(row.get('옵션1값', ''))}")
        lines.append(f"[이미지대] {safe_str(row.get('이미지대', ''))}")
        lines.append(f"[썸네일경로] {safe_str(row.get(THUMB_COL_NAME, ''))}")
        lines.append("")
        lines.append("[ST1_프롬프트]")
        lines.append(safe_str(row.get("ST1_프롬프트", "")))
        lines.append("")
        lines.append("[ST1_결과상품명]")
        lines.append(safe_str(row.get("ST1_결과상품명", "")))

        self.preview_text.insert(tk.END, "\n".join(lines))
        self.preview_text.see("1.0")

    def on_prev_row(self) -> None:
        if self.current_df is None or self.current_df.empty:
            return
        self.current_index -= 1
        if self.current_index < 0:
            self.current_index = 0
        self.refresh_preview()

    def on_next_row(self) -> None:
        if self.current_df is None or self.current_df.empty:
            return
        self.current_index += 1
        if self.current_index >= len(self.current_df.index):
            self.current_index = len(self.current_df.index) - 1
        self.refresh_preview()

    def on_copy_prompt(self) -> None:
        if self.current_df is None or self.current_df.empty:
            messagebox.showwarning("주의", "복사할 프롬프트가 없습니다.")
            return
        row = self.current_df.iloc[self.current_index]
        text = safe_str(row.get("ST1_프롬프트", ""))
        if not text:
            messagebox.showwarning("주의", "현재 행의 ST1_프롬프트가 비어 있습니다.")
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.append_log(f"[INFO] 현재 행 프롬프트를 클립보드에 복사했습니다. (행 {self.current_index + 1})")


# =========================================================
#  실행
# =========================================================
def main() -> None:
    root = tk.Tk()
    app = Stage1MappingApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
