"""
stage2_batch_api_Cachever.py

Stage 2 Batch API 실행 스크립트 (GUI) - 캐싱 최적화 버전
- 기능: 엑셀(이미지+텍스트) -> Batch JSONL 생성(Vision API) -> 업로드 -> 실행 -> 병합 -> JSON 분석 리포트
- 특징: GPT-5/4o 모델 지원, 이미지 옵션 처리, 상세 툴팁 및 가이드 포함
- 🚀 프롬프트 캐싱 최적화: OpenAI Prompt Caching 가이드에 맞게 프롬프트 구조 재구성
  * 정적 콘텐츠(역할, 제약, 스키마)를 앞쪽에 배치
  * 동적 콘텐츠(입력 데이터)를 뒤쪽에 배치
  * prompt_cache_key 사용으로 캐시 히트율 향상 (토큰 비용 최대 90% 절감 가능)
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
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _I*(업완) 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 상품_T0_I0.xlsx -> 상품.xlsx
    예: 상품_T2_I1.xlsx -> 상품.xlsx
    예: 상품_T1_I0(업완).xlsx -> 상품.xlsx
    예: 상품_T1_I0_T2_I1.xlsx -> 상품.xlsx (여러 버전 패턴 제거)
    예: 상품_T1_I5(업완).xlsx -> 상품.xlsx
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

def get_excel_name_from_path(excel_path: str) -> str:
    """
    엑셀 파일 경로에서 파일명만 추출
    예: C:/Users/.../상품_T1_I0.xlsx -> 상품_T1_I0.xlsx
    """
    if not excel_path:
        return "-"
    return os.path.basename(excel_path)


def get_next_version_path(current_path: str, task_type: str = "text") -> str:
    """
    현재 파일명을 분석해서 다음 단계의 파일명을 생성합니다.
    파일명 형식: 원본명_T{숫자}_I{숫자}.xlsx 또는 원본명_T{숫자}_I{숫자}(괄호).xlsx
    - task_type='text'  → T 버전 +1 (Stage1: T1, Stage2: T2, ...)
    - task_type='image' → I 버전 +1
    
    주의: 파일명에 여러 버전 패턴이 있어도 마지막 패턴만 사용합니다.
    예: 상품_T1_I5(업완).xlsx -> 상품_T2_I5(업완).xlsx (text) 또는 상품_T1_I6(업완).xlsx (image)
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


# [필수 의존성] stage2_core_Cache.py / stage2_run_history.py
# 캐싱 최적화 버전 강제 사용 (stage2_core_Cache.py)
try:
    from stage2_core_Cache import (
        safe_str,
        build_stage2_request_from_row, # Row -> Request 객체(프롬프트+이미지경로) 변환 (캐싱 최적화)
    )
    CACHE_MODE_CORE = True
except ImportError:
    # 캐싱 버전이 필수이므로 오류 발생
    raise ImportError(
        "stage2_core_Cache.py를 찾을 수 없습니다. "
        "캐싱 최적화 버전을 사용하려면 stage2_core_Cache.py 파일이 필요합니다."
    )

try:
    from stage2_run_history import append_run_history
except ImportError:
    def append_run_history(*args, **kwargs): pass

# 안정적으로 동작하는 기존 Stage2 Batch 코어 로직 재사용
# 주의: Batch JSONL 생성 로직(create_stage2_batch_input_jsonl)은 이 파일(Cachever)에서 별도로 구현하여
#       프롬프트 캐싱 구조(system/user 분리, meta JSON, prompt_cache_key 고정)를 강제합니다.
try:
    from stage2_batch_api_기존gpt import (
        create_batch_from_jsonl,
        download_batch_output_if_ready,
        merge_batch_output_to_excel,
        build_image_nodes_from_paths,
        CACHE_MODE as BATCH_CACHE_MODE,  # stage2_batch_api_기존gpt.py의 CACHE_MODE 가져오기
    )
    CACHE_MODE_BATCH = BATCH_CACHE_MODE
    if not CACHE_MODE_BATCH:
        # CACHE_MODE가 False인 경우 경고
        print("[WARN] stage2_batch_api_기존gpt.py의 CACHE_MODE가 False입니다. 캐싱이 활성화되지 않았을 수 있습니다.")
except ImportError:
    # 구버전 환경에서는 이 모듈이 없을 수 있으므로, 이 경우에는 아래 새 로직만 사용
    create_batch_from_jsonl = None  # type: ignore
    download_batch_output_if_ready = None  # type: ignore
    merge_batch_output_to_excel = None  # type: ignore
    build_image_nodes_from_paths = None  # type: ignore
    CACHE_MODE_BATCH = False
except AttributeError:
    # CACHE_MODE가 없는 경우 (구버전)
    CACHE_MODE_BATCH = False
    print("[WARN] stage2_batch_api_기존gpt.py에 CACHE_MODE가 없습니다. 캐싱이 활성화되지 않았을 수 있습니다.")

# === 기본 설정 ===
API_KEY_FILE = ".openai_api_key_stage2_batch"
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage2_batch_jobs.json")
DEFAULT_SETTINGS_FILE = os.path.join(os.path.dirname(__file__), ".stage2_batch_defaults.json")

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


# =========================================================
# 엑셀 → Batch 요청 JSONL 생성 (Cachever 전용 구현)
#  - 프롬프트 캐싱 구조를 이 파일에서 강제:
#    * system / user 메시지 분리
#    * meta를 JSON으로만 전달
#    * detail_images_block 텍스트 제거 (이미지는 첨부로만 사용)
#    * prompt_cache_key 고정값("stage2_v1") 사용
# =========================================================

def create_stage2_batch_input_jsonl(
    excel_path: str,
    jsonl_path: str,
    model_name: str,
    effort: str,
    skip_filled: bool,
    use_thumbnail: bool,
    allow_url: bool,
    max_detail_images: int = 10,
    resize_mode: str = "A",
    log_func=None,
):
    """
    Stage2 엑셀을 읽어서 Batch API용 요청 JSONL을 만든다. (Cachever 전용)
    - ST2_JSON 이 비어 있는 행만 대상으로 한다(skip_filled=True일 때).
    - stage2_core_Cache.Stage2Request(system_prompt + user_prompt + image_paths)를 이용해 요청을 구성.
    - custom_id 는 "row-{index}-{resize_mode}" 형식 (결과 비교를 위해 resize_mode 포함).
    - OpenAI Prompt Caching 가이드에 맞게 system/user 분리 및 prompt_cache_key 고정.
    
    Args:
        resize_mode: 리사이즈 모드. "A"(기본/리사이즈 안 함), "B"(가로 512px), "C"(가로 448px)
    """

    if build_image_nodes_from_paths is None:
        raise RuntimeError("build_image_nodes_from_paths를 stage2_batch_api_기존gpt 에서 가져올 수 없습니다.")

    def log(msg: str):
        if log_func:
            log_func(msg)
        else:
            print(msg)

    df = pd.read_excel(excel_path)

    # ST2_JSON 컬럼 없으면 만들어 둔다 (병합 시 쓰임)
    if "ST2_JSON" not in df.columns:
        df["ST2_JSON"] = ""

    # ST2_프롬프트 컬럼이 없으면 생성 (디버깅용)
    if "ST2_프롬프트" not in df.columns:
        df["ST2_프롬프트"] = ""

    # 상세이미지 컬럼들 정렬 및 개수 제한
    detail_cols = [c for c in df.columns if str(c).startswith("상세이미지_")]
    if detail_cols:
        def sort_key(c):
            try:
                return int(str(c).split("_")[1])
            except Exception:
                return 9999
        detail_cols.sort(key=sort_key)

        # max_detail_images 개수만큼만 사용 (예: 10이면 상세이미지_1~상세이미지_10까지만)
        if max_detail_images > 0:
            detail_cols = detail_cols[:max_detail_images]
            log(f"[INFO] 상세이미지 컬럼 제한 적용: {max_detail_images}개까지만 사용")
        else:
            log(f"[INFO] 상세이미지 컬럼 제한 없음: 모든 컬럼 사용")

    log(f"[INFO] 사용할 상세이미지 컬럼: {detail_cols}")

    image_cache: dict[str, str] = {}

    requests = []
    total_rows = len(df)
    target_rows = 0

    # 썸네일 제외 통계 (성능 최적화를 위한 요약 로그용)
    thumbnail_exclude_count = 0
    thumbnail_exclude_logs: list[str] = []  # 처음 몇 개만 저장 (디버깅용)

    # 중복 요청 방지: custom_id 추적
    seen_custom_ids: set[str] = set()
    duplicate_count = 0

    # 먼저 전체 대상 요청 수를 계산 (버킷 수 결정용)
    for idx, row in df.iterrows():
        existing_json = safe_str(row.get("ST2_JSON", ""))
        existing_json_clean = existing_json.strip().lower() if existing_json else ""
        if not (skip_filled and existing_json_clean and existing_json_clean not in ("", "nan", "none", "null")):
            target_rows += 1

    # 버킷 수를 미리 계산 (모든 요청에 동일하게 적용)
    if CACHE_MODE_CORE and target_rows > 0:
        # [버킷 수 계산 전략 - 주의: OpenAI 공식 기준이 아닌 추정치입니다]
        # 
        # [OpenAI 공식 문서 기준]
        # - 일반 API(동기 요청): 같은 prefix + prompt_cache_key 조합이 분당 약 15건을 초과하면
        #   일부가 추가 머신으로 overflow되어 캐시 효율이 떨어질 수 있음
        #   (참고: https://platform.openai.com/docs/guides/prompt-caching)
        # - Batch API: 공식 문서에 prompt_cache_key 버킷 분배 기준이 명시되어 있지 않음
        #
        # [현재 구현 전략 (추정치)]
        # - Batch API는 24시간에 걸쳐 처리되므로, 실제 처리 시점에는 더 분산될 수 있음
        # - 일반 API 기준(분당 15건)을 참고하여, 안전 마진을 포함하여 분당 10건 기준으로 계산
        # - 대량 배치(5000~10000개)를 고려하여 버킷 수를 충분히 확보
        # - 실제 overflow 가능성은 Batch API의 처리 분산 특성상 낮을 것으로 예상
        #
        # [버킷 수 계산 개선]
        # - 분당 15건 제한을 고려하여, 각 버킷당 분당 10건 이하가 되도록 계산 (안전 마진 포함)
        # - 대량 배치(5000~10000개)의 경우 버킷당 요청률을 낮추기 위해 더 많은 버킷 필요
        # - 최대 버킷 수를 500개로 확대 (5000개 요청: 500개 버킷, 10000개 요청: 500개 버킷)
        #
        # [참고]
        # - 이 버킷 분산은 overflow 방지를 위한 것이며, 프롬프트 캐싱 자체는 system 프롬프트의
        #   동일성에 의존하므로 요청 수와 무관하게 작동합니다
        # - "대량이면 버킷 10개로 부족할 때가 많아서 20~50개로 늘리는 게 실제로 체감에 도움됩니다"
        #   → 5천~1만개 요청의 경우 훨씬 더 많은 버킷이 필요
        # [프롬프트 캐싱 최적화 전략 - 적당히 샤딩]
        # stage2는 용량 문제로 청크를 여러 개로 분배하므로, 적당히 샤딩하여 캐시 효율 향상
        # 최대 8개 버킷으로 제한 (청크별로 다른 키를 사용하되, 과도한 분산 방지)
        # 500건 이하: 1개, 500~1000건: 2개, 1000~2000건: 4개, 2000건 이상: 8개
        if target_rows <= 500:
            PROMPT_CACHE_BUCKETS = 1
        elif target_rows <= 1000:
            PROMPT_CACHE_BUCKETS = 2
        elif target_rows <= 2000:
            PROMPT_CACHE_BUCKETS = 4
        else:
            PROMPT_CACHE_BUCKETS = 8
        
        avg_per_bucket = target_rows // PROMPT_CACHE_BUCKETS if PROMPT_CACHE_BUCKETS > 0 else target_rows
        log(f"[INFO] 프롬프트 캐싱: 적당히 샤딩 전략 사용 (버킷 수: {PROMPT_CACHE_BUCKETS}개)")
        log(f"[INFO] 예상 요청 수: {target_rows}개, 각 버킷당 평균 ~{avg_per_bucket}건")
    else:
        PROMPT_CACHE_BUCKETS = 1

    # target_rows 초기화 (실제 처리 시 다시 계산)
    target_rows = 0

    for idx, row in df.iterrows():
        # ST2_JSON 중복 체크 (skip_filled 옵션)
        existing_json = safe_str(row.get("ST2_JSON", ""))
        # 빈 문자열, "nan", None 등을 모두 빈 값으로 처리
        existing_json_clean = existing_json.strip().lower() if existing_json else ""
        if skip_filled and existing_json_clean and existing_json_clean not in ("", "nan", "none", "null"):
            log(f"[SKIP] idx={idx}: 이미 ST2_JSON 값이 있어 건너뜀.")
            continue

        target_rows += 1

        try:
            # stage2_core_Cache 의 캐싱 최적화 프롬프트 빌더 사용
            req = build_stage2_request_from_row(row, detail_cols)
        except Exception as e:
            log(f"[ERROR] idx={idx}: Stage2 프롬프트 생성 실패 → 스킵. ({e})")
            continue

        system_prompt = safe_str(getattr(req, "system_prompt", ""))
        user_prompt = safe_str(getattr(req, "user_prompt", ""))

        if not system_prompt or not user_prompt:
            log(f"[SKIP] idx={idx}: Stage2 프롬프트가 비어 있어 건너뜀.")
            continue

        # 디버깅용 ST2_프롬프트 기록 (system + user 결합)
        full_prompt = f"[System]\n{system_prompt}\n\n[User]\n{user_prompt}"
        df.at[idx, "ST2_프롬프트"] = full_prompt

        image_paths = list(getattr(req, "image_paths", []) or [])

        # 썸네일(이미지대) 제외 옵션 (성능 최적화: 썸네일 제외 옵션이 활성화된 경우에만 체크)
        if not use_thumbnail:
            thumb_val = safe_str(row.get("이미지대", ""))
            if thumb_val:  # 이미지대 값이 있을 때만 필터링
                before_len = len(image_paths)
                if before_len > 0:  # 이미지가 있을 때만 필터링
                    image_paths = [p for p in image_paths if safe_str(p) != thumb_val]
                    if len(image_paths) != before_len:
                        thumbnail_exclude_count += 1
                        # 처음 5개만 로그 저장 (디버깅용)
                        if thumbnail_exclude_count <= 5:
                            thumbnail_exclude_logs.append(f"idx={idx}: {thumb_val[:50]}...")

        # stage2_batch_api_기존gpt 의 이미지 인코딩/캐시 로직 재사용
        # resize_mode에 따라 가로 기준 리사이즈 적용
        image_nodes = build_image_nodes_from_paths(
            image_paths,
            log_func=log,
            cache=image_cache,
            allow_url=allow_url,
            resize_mode=resize_mode,
        )

        # System 메시지 (텍스트만, 정적)
        system_content = [{"type": "input_text", "text": system_prompt}]

        # User 메시지 (텍스트 + 이미지, 동적)
        user_content = [{"type": "input_text", "text": user_prompt}]
        user_content.extend(image_nodes)

        body = {
            "model": model_name,
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
        }

        # reasoning.effort
        if effort in ("low", "medium", "high"):
            body["reasoning"] = {"effort": effort}

        # custom_id에 resize_mode 포함 (결과 비교를 위해)
        custom_id = f"row-{idx}-{resize_mode}"

        # Prompt Caching 최적화 (캐싱 모드일 때만)
        if CACHE_MODE_CORE:
            # prompt_cache_key: 버킷 분산으로 라우팅 효율 향상
            # 
            # [버킷 분산의 목적]
            # - 같은 prefix+key 조합이 분당 ~15건을 넘으면 overflow로 라우팅이 퍼져 캐시 효율이 급감
            # - 배치 API는 시간에 걸쳐 처리되지만, 실제 처리 시점에 분당 15건 제한이 적용됨
            # - 버킷으로 분산하면 각 버킷당 요청 수가 줄어들어 overflow 방지
            #
            # [버킷 수 결정 - 주의: OpenAI 공식 기준이 아닌 추정치입니다]
            # - 예상 요청 수를 고려하여 버킷 수를 동적으로 계산 (위에서 미리 계산됨)
            # - 각 버킷당 분당 10건 이하가 되도록 설정 (일반 API 기준 15건의 안전 마진 포함)
            # - 최소 1개, 최대 200개 버킷 (대량 배치 대응: 1000~10000개 요청)
            # - stage2_v2: system 프롬프트에 meta 복사 명시 추가 (2024-12-15)
            #
            # [프롬프트 캐싱 작동 여부]
            # - 프롬프트 캐싱은 요청 수와 무관하게 작동합니다 (system 프롬프트가 동일하면 캐시 히트)
            # - 버킷 분산은 overflow 방지를 위한 것이며, 캐싱 자체는 system 프롬프트의 동일성에 의존
            # - 배치 API는 24시간에 걸쳐 처리되므로, 실제 처리 시점에는 더 분산되어 overflow 가능성 감소
            #
            # [중요: 프롬프트 캐싱이 결과값에 미치는 영향]
            # - 프롬프트 캐싱 자체는 결과값을 변경하지 않습니다 (비용/지연만 영향)
            # - 다만 이번 수정에서 "system/user 분리" + "meta를 user JSON에서 복사" 같은 프롬프트 구조 변화는
            #   모델 행동(특히 meta 누락/복사 정확도)에 영향을 줄 수 있습니다
            # - system 프롬프트에 "meta(JSON)를 키/값 완전히 동일 복사" 문구가 포함되어 있어
            #   meta 누락 리스크는 많이 줄어든 상태입니다
            #
            # [참고: OpenAI 공식 문서]
            # - 일반 API: 같은 prefix + prompt_cache_key 조합이 분당 약 15건 초과 시 overflow 가능
            # - Batch API: 공식 문서에 prompt_cache_key 버킷 분배 기준이 명시되어 있지 않음
            # - 현재 구현은 일반 API 기준을 참고한 추정치이며, 실제 Batch API 동작은 다를 수 있음
            
            # prompt_cache_key: 적당히 샤딩 (청크 분배 고려)
            bucket_num = hash(custom_id) % PROMPT_CACHE_BUCKETS
            body["prompt_cache_key"] = f"stage2_v2_b{bucket_num:02d}"
            
            # prompt_cache_retention: 모델이 지원하는 경우에만 추가
            # Extended retention 지원 모델: gpt-5.1, gpt-5.1-codex, gpt-5.1-codex-mini, gpt-5.1-chat-latest, gpt-5, gpt-5-codex, gpt-4.1
            # gpt-5-mini, gpt-5-nano는 prompt_cache_retention 파라미터를 지원하지 않음
            if model_name in ["gpt-5.1", "gpt-5.1-codex", "gpt-5.1-codex-mini", "gpt-5.1-chat-latest", "gpt-5", "gpt-5-codex", "gpt-4.1"]:
                body["prompt_cache_retention"] = "extended"  # 24시간 retention
            elif model_name not in ["gpt-5-mini", "gpt-5-nano"]:
                # 기타 모델은 in-memory 사용 (5~10분 inactivity, 최대 1시간)
                body["prompt_cache_retention"] = "in_memory"
        
        # Responses API: text.format으로 JSON 모드 강제 (Structured Outputs)
        # 프롬프트만으로 JSON 강제하는 대신, text.format으로 파싱 안정성 향상
        # 문서 스펙에 맞춰 format을 객체 형태로 설정 (향후 호환성 보장)
        body["text"] = {
            "format": {
                "type": "json_object"  # JSON 모드 강제 (JSON Schema는 필요 시 추가 가능)
            }
        }

        # 중복 custom_id 체크
        if custom_id in seen_custom_ids:
            duplicate_count += 1
            log(f"[WARN] 중복 요청 감지: custom_id={custom_id} (idx={idx}) - 건너뜀.")
            continue

        seen_custom_ids.add(custom_id)

        # 용량 분석용: 각 요청의 이미지 데이터 크기 추정
        image_data_size = 0
        for node in image_nodes:
            if node.get("type") == "input_image" and "image_url" in node:
                img_url = node["image_url"]
                if img_url.startswith("data:"):
                    # Base64 인코딩된 이미지: data:image/jpeg;base64,{base64_string}
                    # Base64 문자열 길이로 크기 추정 (약 4/3 배율)
                    base64_part = img_url.split(",", 1)[1] if "," in img_url else ""
                    image_data_size += len(base64_part)  # Base64 문자열 길이 (바이트 단위)

        requests.append(
            {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/responses",
                "body": body,
                "_size_estimate": {  # 디버깅용 (실제 JSONL에는 포함되지 않음)
                    "image_data_bytes": image_data_size,
                    "image_count": len(image_nodes),
                }
            }
        )

    # 썸네일 제외 요약 로그 (성능 최적화)
    if not use_thumbnail and thumbnail_exclude_count > 0:
        if thumbnail_exclude_count <= 10:
            # 10개 이하면 개별 로그 출력
            for log_msg in thumbnail_exclude_logs:
                log(f"[INFO] {log_msg}")
            if thumbnail_exclude_count > len(thumbnail_exclude_logs):
                log(f"[INFO] ... 외 {thumbnail_exclude_count - len(thumbnail_exclude_logs)}개 행에서 썸네일 제외됨")
        else:
            # 10개 초과면 요약 로그만
            if thumbnail_exclude_logs:
                for log_msg in thumbnail_exclude_logs[:3]:  # 처음 3개만
                    log(f"[INFO] {log_msg}")
            log(f"[INFO] 총 {thumbnail_exclude_count}개 행에서 썸네일(이미지대)이 제외되었습니다. (옵션: 썸네일 제외)")

    # 중복 요청 감지 로그
    if duplicate_count > 0:
        log(f"[WARN] ⚠️ 중복 요청 {duplicate_count}개가 감지되어 제외되었습니다. (같은 행이 여러 번 요청되는 것을 방지)")
        log(f"[WARN] ⚠️ 중복 요청으로 인해 실제 요청 수가 {len(requests)}개입니다. (예상: {target_rows}개)")

    # 최종 통계 로그
    log(f"[INFO] 최종 요청 통계:")
    log(f"  - 전체 행 수: {total_rows}개")
    log(f"  - 대상 행 수 (ST2_JSON 비어있음): {target_rows}개")
    log(f"  - 실제 생성된 요청 수: {len(requests)}개")
    if duplicate_count > 0:
        log(f"  - 중복 제외: {duplicate_count}개")
    if target_rows != len(requests):
        log(f"  - ⚠️ 차이: {target_rows - len(requests)}개 (중복 제외 또는 프롬프트 생성 실패)")

    if not requests:
        raise RuntimeError("Batch 요청에 사용할 유효한 행이 없습니다.")

    # 용량 분석: 이미지 데이터 크기 통계
    total_image_data_bytes = 0
    total_image_count = 0
    requests_with_images = 0
    for req in requests:
        size_est = req.get("_size_estimate", {})
        if size_est:
            total_image_data_bytes += size_est.get("image_data_bytes", 0)
            total_image_count += size_est.get("image_count", 0)
            if size_est.get("image_count", 0) > 0:
                requests_with_images += 1
    
    # JSONL 파일 생성 (용량 분석용 필드 제거)
    import json as json_module
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for item in requests:
            # _size_estimate 필드는 실제 JSONL에 포함하지 않음
            item_clean = {k: v for k, v in item.items() if k != "_size_estimate"}
            f.write(json_module.dumps(item_clean, ensure_ascii=False) + "\n")
    
    # 용량 분석 로그
    jsonl_size_bytes = os.path.getsize(jsonl_path)
    jsonl_size_mb = jsonl_size_bytes / (1024 * 1024)
    image_data_mb = total_image_data_bytes / (1024 * 1024)
    image_data_ratio = (total_image_data_bytes / jsonl_size_bytes * 100) if jsonl_size_bytes > 0 else 0
    
    log(f"[INFO] 📊 JSONL 용량 분석:")
    log(f"  - 전체 파일 크기: {jsonl_size_mb:.2f} MB ({jsonl_size_bytes:,} bytes)")
    log(f"  - Base64 이미지 데이터: {image_data_mb:.2f} MB ({total_image_data_bytes:,} bytes, {image_data_ratio:.1f}%)")
    log(f"  - 텍스트/메타데이터: {jsonl_size_mb - image_data_mb:.2f} MB ({(100 - image_data_ratio):.1f}%)")
    log(f"  - 총 이미지 개수: {total_image_count}개 (평균 {total_image_count / len(requests):.1f}개/요청)")
    log(f"  - 이미지 포함 요청: {requests_with_images}개 / {len(requests)}개")
    if total_image_data_bytes > 0:
        avg_image_size_mb = (total_image_data_bytes / total_image_count) / (1024 * 1024) if total_image_count > 0 else 0
        log(f"  - 평균 이미지 크기: {avg_image_size_mb:.2f} MB (Base64 인코딩 후)")
        log(f"[INFO] 💡 참고: Base64 인코딩은 원본 이미지보다 약 33% 크기가 증가합니다.")

    # ST2_프롬프트 기록을 위해 엑셀 덮어쓰기 (열려있으면 실패해도 무방)
    try:
        df.to_excel(excel_path, index=False)
        log(f"[INFO] 엑셀에 ST2_프롬프트 갱신 완료: {excel_path}")
    except Exception as e:
        log(f"[WARN] 엑셀 저장 실패(열려있을 수 있음): {e}")

    log(
        f"[DONE] Batch 입력 JSONL 생성 완료: {jsonl_path} "
        f"(전체 {total_rows}행 중 대상 {target_rows}행, 요청 {len(requests)}개)"
    )

    return {
        "total_rows": total_rows,
        "target_rows": target_rows,
        "num_requests": len(requests),
    }

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

def load_default_settings():
    """기본 설정값 불러오기"""
    default_settings = {
        "model": "gpt-5-mini",
        "effort": "medium",
        "resize_mode": "B"
    }
    
    if os.path.exists(DEFAULT_SETTINGS_FILE):
        try:
            with open(DEFAULT_SETTINGS_FILE, "r", encoding="utf-8") as f:
                saved_settings = json.load(f)
                # 저장된 값으로 업데이트 (유효한 값만)
                if "model" in saved_settings:
                    default_settings["model"] = saved_settings["model"]
                if "effort" in saved_settings:
                    default_settings["effort"] = saved_settings["effort"]
                if "resize_mode" in saved_settings:
                    default_settings["resize_mode"] = saved_settings["resize_mode"]
        except Exception as e:
            print(f"[WARN] 기본 설정 파일 읽기 실패: {e}, 기본값 사용")
    
    return default_settings

def save_default_settings(model, effort, resize_mode):
    """기본 설정값 저장"""
    settings = {
        "model": model,
        "effort": effort,
        "resize_mode": resize_mode
    }
    try:
        with open(DEFAULT_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"[ERROR] 기본 설정 저장 실패: {e}")
        return False

# ========================================================
# 툴팁 클래스 (최상단 정의)
# ========================================================
class ToolTip:
    def __init__(self, widget, text, wraplength=400):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tipwindow = None
        # 마우스 이벤트를 가로채지 않도록 지연 시간 추가
        self.widget.bind("<Enter>", self._on_enter)
        self.widget.bind("<Leave>", self.hide_tip)
        self.widget.bind("<Button-1>", self.hide_tip)  # 클릭 시 툴팁 숨김
        self._after_id = None

    def _on_enter(self, event=None):
        """마우스 진입 시 지연 후 툴팁 표시"""
        # 기존 타이머 취소
        if self._after_id:
            self.widget.after_cancel(self._after_id)
        # 500ms 후 툴팁 표시 (클릭 이벤트를 방해하지 않도록)
        self._after_id = self.widget.after(500, self.show_tip)

    def show_tip(self, event=None):
        if self.tipwindow or not self.text: return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        # 툴팁이 클릭 이벤트를 받지 않도록 설정
        tw.attributes("-topmost", True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left', background="#ffffe0", relief='solid', borderwidth=1, font=("맑은 고딕", 9), wraplength=self.wraplength)
        label.pack(ipadx=4, ipady=2)
        # 툴팁 클릭 시 즉시 숨김
        tw.bind("<Button-1>", lambda e: self.hide_tip())

    def hide_tip(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None
        if self._after_id:
            self.widget.after_cancel(self._after_id)
            self._after_id = None

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
                if not use_thumb and thumb_val and url == thumb_val:
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
        self.title("Stage 2: Batch API Manager (Multimodal & Analytics) - 🚀 캐싱 최적화 버전")
        self.geometry("1500x950")  # 가로 길이 증가 (1280 → 1500)
        
        self.api_key_var = tk.StringVar()
        self.src_file_var = tk.StringVar()
        
        # 기본값 (저장된 설정 불러오기)
        default_settings = load_default_settings()
        self.model_var = tk.StringVar(value=default_settings.get("model", "gpt-5-mini"))
        self.effort_var = tk.StringVar(value=default_settings.get("effort", "medium"))
        self.skip_exist_var = tk.BooleanVar(value=True)
        
        # Stage 2 옵션
        self.use_thumbnail_var = tk.BooleanVar(value=False)  # 기본값: 썸네일 제외 (성능 최적화)
        self.allow_url_var = tk.BooleanVar(value=False)
        self.max_detail_images_var = tk.IntVar(value=10)  # 기본값: 상세이미지 10개까지만 사용
        self.resize_mode_var = tk.StringVar(value=default_settings.get("resize_mode", "B"))  # 기본값: B(512px)

        self.batch_id_var = tk.StringVar()
        
        # 상세이미지 통계 정보 저장
        self.detail_image_stats = {
            "max_columns": 0,  # 엑셀에 있는 최대 상세이미지 컬럼 개수
            "row_counts": {},  # {컬럼개수: 행개수} 예: {15: 5, 20: 3}
        }
        
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
        
        btn_save_defaults = ttk.Button(f_top, text="💾 기본값 저장", command=self._save_defaults, style="Success.TButton")
        btn_save_defaults.pack(side='left', padx=(5, 0))
        ToolTip(btn_save_defaults, "현재 설정된 모델, effort, 리사이즈 모드를 기본값으로 저장합니다.\n다음 실행 시 자동으로 불러옵니다.")

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
    
    def _save_defaults(self):
        """현재 설정을 기본값으로 저장"""
        model = self.model_var.get().strip()
        effort = self.effort_var.get().strip()
        resize_mode = self.resize_mode_var.get().strip()
        
        if save_default_settings(model, effort, resize_mode):
            messagebox.showinfo(
                "저장 완료", 
                f"기본값이 저장되었습니다:\n\n"
                f"• 모델: {model}\n"
                f"• Effort: {effort}\n"
                f"• 리사이즈 모드: {resize_mode}\n\n"
                f"다음 실행 시 자동으로 불러옵니다."
            )
        else:
            messagebox.showerror("저장 실패", "기본값 저장에 실패했습니다.")

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
        
        # 상세이미지 개수 제한 옵션
        fr3 = ttk.Frame(f_opt)
        fr3.pack(fill='x', pady=5)
        ttk.Label(fr3, text="상세이미지 개수 제한:", width=18).pack(side='left')
        spin_max_detail = ttk.Spinbox(fr3, from_=1, to=100, textvariable=self.max_detail_images_var, width=10)
        spin_max_detail.pack(side='left', padx=5)
        ToolTip(spin_max_detail, "상세이미지_1부터 지정한 개수까지만 사용합니다.\n예: 10으로 설정하면 상세이미지_1~상세이미지_10까지만 사용합니다.\n배치 용량과 토큰 사용량을 줄이기 위한 옵션입니다.")
        ttk.Label(fr3, text="개 (기본값: 10)", font=("맑은 고딕", 9)).pack(side='left', padx=5)
        
        # 상세이미지 통계 표시
        fr3_stats = ttk.Frame(f_opt)
        fr3_stats.pack(fill='x', pady=(5, 5))
        
        # 통계 표시 영역 (더 눈에 띄게)
        self.detail_stats_frame = tk.Frame(fr3_stats, bg="#E3F2FD", relief="solid", bd=1, padx=10, pady=8)
        self.detail_stats_frame.pack(fill='x', padx=5)
        
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
        
        # 이미지 리사이즈 모드 옵션
        fr_resize = ttk.Frame(f_opt)
        fr_resize.pack(fill='x', pady=5)
        ttk.Label(fr_resize, text="이미지 리사이즈 모드:", width=18).pack(side='left')
        resize_mode_combo = ttk.Combobox(
            fr_resize, 
            textvariable=self.resize_mode_var, 
            values=["A", "B", "C"], 
            state="readonly", 
            width=8
        )
        resize_mode_combo.pack(side='left', padx=5)
        ttk.Label(
            fr_resize, 
            text="(A: 기본/리사이즈 안 함, B: 가로 512px, C: 가로 448px)", 
            font=("맑은 고딕", 9)
        ).pack(side='left', padx=5)
        ToolTip(
            resize_mode_combo, 
            "이미지 리사이즈 모드 선택:\n"
            "• A (기본): 리사이즈 하지 않음 (현행 유지)\n"
            "• B (512px): 가로 512px로 축소, 비율 유지, 크롭/패딩 금지\n"
            "• C (448px): 가로 448px로 축소, 비율 유지, 크롭/패딩 금지\n\n"
            "모든 모드는 고품질 리샘플링(Lanczos) 사용"
        )

        # Step 3: 실행
        f_step3 = ttk.LabelFrame(container, text="3. 실행", padding=15)
        f_step3.pack(fill='x', pady=15)
        
        # 통합 버튼 (기존)
        btn_run = ttk.Button(f_step3, text="🚀 JSONL 생성 및 배치 업로드 (Start Batch)", command=self._start_create_batch, style="Success.TButton")
        btn_run.pack(fill='x', ipady=8, pady=(0, 10))
        ToolTip(btn_run, "1. 엑셀 읽기 (이미지 포함)\n2. JSONL 생성\n3. 배치 시작 요청 (24시간 내 완료)")
        
        # 분리된 버튼들 (테스트용)
        f_step3_separated = ttk.Frame(f_step3)
        f_step3_separated.pack(fill='x', pady=(5, 0))
        
        btn_jsonl_only = ttk.Button(f_step3_separated, text="📝 JSONL 생성만 (테스트용)", command=self._start_create_jsonl_only, style="Primary.TButton")
        btn_jsonl_only.pack(side='left', fill='x', expand=True, padx=(0, 5), ipady=6)
        ToolTip(btn_jsonl_only, "엑셀 파일을 읽어서 JSONL 파일만 생성합니다.\n생성된 JSONL 파일을 확인한 후, '배치 업로드만' 버튼으로 배치를 생성할 수 있습니다.")
        
        btn_upload_only = ttk.Button(f_step3_separated, text="📤 배치 업로드만 (JSONL 파일 선택)", command=self._start_upload_batch_only, style="Primary.TButton")
        btn_upload_only.pack(side='left', fill='x', expand=True, padx=(5, 0), ipady=6)
        ToolTip(btn_upload_only, "이미 생성된 JSONL 파일을 선택하여 배치를 업로드합니다.\n'JSONL 생성만' 버튼으로 먼저 JSONL을 생성한 후 사용하세요.")
        
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
            
            # 엑셀 파일을 읽어서 상세이미지 통계 계산
            self._analyze_detail_images(p)
            
            self.src_file_var.set(p)
    
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

    def _start_create_jsonl_only(self):
        """JSONL 생성만 수행 (테스트용)"""
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
        
        t = threading.Thread(target=self._run_create_jsonl_only)
        t.daemon = True
        t.start()

    def _start_upload_batch_only(self):
        """배치 업로드만 수행 (JSONL 파일 선택)"""
        if not self.api_key_var.get():
            messagebox.showwarning("오류", "API Key 필요")
            return
        
        # JSONL 파일 선택
        jsonl_path = filedialog.askopenfilename(
            title="배치 업로드할 JSONL 파일 선택",
            filetypes=[("JSONL 파일", "*.jsonl"), ("모든 파일", "*.*")]
        )
        
        if not jsonl_path:
            return
        
        # 원본 엑셀 파일 경로 추론 (JSONL 파일명에서)
        base_name = os.path.splitext(os.path.basename(jsonl_path))[0]
        # 예: "파일명_stage2_batch_input.jsonl" -> "파일명.xlsx"
        excel_path = jsonl_path.replace("_stage2_batch_input.jsonl", ".xlsx")
        if not os.path.exists(excel_path):
            # 엑셀 파일을 찾을 수 없으면 사용자에게 물어봄
            excel_path = filedialog.askopenfilename(
                title="원본 엑셀 파일 선택 (배치 이력 기록용)",
                filetypes=[("Excel 파일", "*.xlsx;*.xls"), ("모든 파일", "*.*")]
            )
            if not excel_path:
                return
        
        t = threading.Thread(target=self._run_upload_batch_only, args=(jsonl_path, excel_path))
        t.daemon = True
        t.start()

    def _run_create_jsonl_only(self):
        """JSONL 파일만 생성 (배치 업로드는 하지 않음)"""
        src = self.src_file_var.get().strip()
        model = self.model_var.get().strip() or "gpt-5-mini"
        effort = self.effort_var.get().strip() or "medium"

        use_thumb = self.use_thumbnail_var.get()
        allow_url = self.allow_url_var.get()
        max_detail_images = self.max_detail_images_var.get()
        resize_mode = self.resize_mode_var.get().strip() or "A"

        try:
            if create_stage2_batch_input_jsonl is None:
                raise RuntimeError("create_stage2_batch_input_jsonl 함수를 찾을 수 없습니다.")

            self.append_log(f"[RUN] Stage2: JSONL 생성만 수행 → {os.path.basename(src)}")
            self.append_log(f"[INFO] 🚀 프롬프트 캐싱 최적화 버전 (Cachever) 사용 중")
            if CACHE_MODE_CORE:
                self.append_log(f"[INFO] ✅ stage2_core_Cache.py 로드 완료 (프롬프트 구조 최적화)")
            else:
                self.append_log(f"[WARN] ⚠️ stage2_core_Cache.py 로드 실패 - 캐싱이 작동하지 않을 수 있습니다")
            if CACHE_MODE_BATCH:
                self.append_log(f"[INFO] ✅ stage2_batch_api_기존gpt.py 캐싱 모드 활성화 (prompt_cache_key 사용)")
                self.append_log(f"[INFO] 💰 토큰 비용 최대 90% 절감 가능 (캐시 히트율에 따라 다름)")
            else:
                self.append_log(f"[WARN] ⚠️ stage2_batch_api_기존gpt.py 캐싱 모드 비활성화 - prompt_cache_key가 추가되지 않을 수 있습니다")
            self.append_log(f"[INFO] 상세이미지 개수 제한: {max_detail_images}개 (상세이미지_1 ~ 상세이미지_{max_detail_images})")
            
            # 리사이즈 모드 설명
            resize_mode_desc = {
                "A": "기본 모드 (리사이즈 안 함)",
                "B": "리사이즈 모드 512px (가로 512px로 축소, 비율 유지)",
                "C": "리사이즈 모드 448px (가로 448px로 축소, 비율 유지)",
            }.get(resize_mode, f"알 수 없는 모드: {resize_mode}")
            self.append_log(f"[INFO] 이미지 리사이즈 모드: {resize_mode} ({resize_mode_desc})")

            # 엑셀 → JSONL 생성
            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage2_batch_input.jsonl"

            # 함수 시그니처 확인 후 파라미터 전달
            import inspect
            sig = inspect.signature(create_stage2_batch_input_jsonl)
            params = list(sig.parameters.keys())
            
            call_kwargs = {
                "excel_path": src,
                "jsonl_path": jsonl_path,
                "model_name": model,
                "effort": effort,
                "skip_filled": self.skip_exist_var.get(),
                "use_thumbnail": use_thumb,
                "allow_url": allow_url,
                "log_func": self.append_log,
            }
            
            if "max_detail_images" in params:
                call_kwargs["max_detail_images"] = max_detail_images
            
            if "resize_mode" in params:
                call_kwargs["resize_mode"] = resize_mode
            
            info = create_stage2_batch_input_jsonl(**call_kwargs)

            self.append_log(
                f"[DONE] 요청 JSONL 생성: total_rows={info['total_rows']}, "
                f"target_rows={info['target_rows']}, num_requests={info['num_requests']}"
            )

            # JSONL 파일 크기 확인
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 처리 건수(행): {info['num_requests']}개")
            self.append_log(f"[INFO] JSONL 파일 경로: {jsonl_path}")
            self.append_log(f"[INFO] ✅ JSONL 생성 완료! 이제 '배치 업로드만' 버튼으로 배치를 생성할 수 있습니다.")
            
            # 파일 열기 옵션 제공
            self.after(0, lambda: messagebox.showinfo(
                "JSONL 생성 완료",
                f"JSONL 파일이 생성되었습니다.\n\n"
                f"파일: {os.path.basename(jsonl_path)}\n"
                f"크기: {jsonl_size_mb:.2f} MB\n"
                f"요청 수: {info['num_requests']}개\n\n"
                f"파일을 확인한 후, '배치 업로드만' 버튼으로 배치를 생성하세요."
            ))

        except Exception as e:
            self.append_log(f"에러: {e}")
            import traceback
            self.append_log(traceback.format_exc())
            self.after(0, lambda: messagebox.showerror("에러", str(e)))

    def _run_upload_batch_only(self, jsonl_path: str, excel_path: str):
        """JSONL 파일을 사용하여 배치만 업로드"""
        key = self.api_key_var.get().strip()
        model = self.model_var.get().strip() or "gpt-5-mini"
        effort = self.effort_var.get().strip() or "medium"

        try:
            if create_batch_from_jsonl is None:
                raise RuntimeError("create_batch_from_jsonl 함수를 찾을 수 없습니다.")

            self.append_log(f"[RUN] 배치 업로드 시작 → {os.path.basename(jsonl_path)}")
            
            # JSONL 파일 크기 확인
            if not os.path.exists(jsonl_path):
                raise FileNotFoundError(f"JSONL 파일을 찾을 수 없습니다: {jsonl_path}")
            
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB")
            
            # 용량 기준 우선: 180MB 이상이면 분할 처리
            MAX_FILE_SIZE_MB = 180
            
            if jsonl_size_mb > MAX_FILE_SIZE_MB:
                self.append_log(f"[INFO] 파일 크기가 제한을 초과하여 청크로 분할합니다: {jsonl_size_mb:.2f}MB > {MAX_FILE_SIZE_MB}MB")
                import httpx
                timeout = httpx.Timeout(600.0, connect=60.0)
                client_with_timeout = OpenAI(api_key=key, timeout=timeout, max_retries=3)
                batch_ids = self._create_batch_chunks(
                    client=client_with_timeout,
                    jsonl_path=jsonl_path,
                    excel_path=excel_path,
                    model_name=model,
                    effort=effort,
                    max_size_mb=MAX_FILE_SIZE_MB,
                    max_requests=999999,
                    resize_mode=None,  # 이미 생성된 JSONL 업로드 시에는 None (JSONL 파일에서 추론 가능하지만 일단 None)
                )
                self.append_log(f"✅ 총 {len(batch_ids)}개의 배치가 생성되었습니다: {', '.join(batch_ids)}")
                self.after(0, lambda: messagebox.showinfo("성공", f"{len(batch_ids)}개의 배치가 생성되었습니다:\n{', '.join(batch_ids)}"))
            else:
                # 단일 배치 생성
                import httpx
                timeout = httpx.Timeout(600.0, connect=60.0)
                client = OpenAI(api_key=key, timeout=timeout, max_retries=3)
                batch = create_batch_from_jsonl(
                    client=client,
                    jsonl_path=jsonl_path,
                    excel_path=excel_path,
                    model_name=model,
                    log_func=self.append_log,
                )

                batch_id = batch.id
                self.append_log(f"✅ 배치 시작! ID: {batch_id}, status={batch.status}")

                # 작업 이력 기록 (resize_mode는 JSONL 파일에서 추론 불가하므로 None으로 설정)
                # resize_mode는 JSONL 생성 시에만 저장되며, 이미 생성된 JSONL 업로드 시에는 custom_id에서 추출 가능
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=excel_path,
                    jsonl_path=jsonl_path,
                    model=model,
                    effort=effort,
                    status=batch.status,
                    output_file_id=None,
                    resize_mode=None,  # 이미 생성된 JSONL 업로드 시에는 None
                )

                # 메인 런처 현황판 업데이트
                try:
                    root_name = get_root_filename(excel_path)
                    JobManager.update_status(root_name, text_msg="T2 (진행중)")
                    self.append_log(f"[INFO] 런처 상태 업데이트: {root_name} -> T2 (진행중)")
                except Exception:
                    pass
                
                self.after(0, lambda: messagebox.showinfo("성공", f"배치 시작됨: {batch_id}"))
            
            self._load_jobs_all()
            self._load_archive_list()

        except Exception as e:
            self.append_log(f"에러: {e}")
            import traceback
            self.append_log(traceback.format_exc())
            self.after(0, lambda: messagebox.showerror("에러", str(e)))

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
        max_detail_images = self.max_detail_images_var.get()
        resize_mode = self.resize_mode_var.get().strip() or "A"

        try:
            if create_stage2_batch_input_jsonl is None or create_batch_from_jsonl is None:
                raise RuntimeError("stage2_batch_api_기존gpt 모듈을 찾을 수 없어 Batch 코어를 사용할 수 없습니다.")

            self.append_log(f"[RUN] Stage2: JSONL 생성 + Batch 생성 시작 → {os.path.basename(src)}")
            self.append_log(f"[INFO] 🚀 프롬프트 캐싱 최적화 버전 (Cachever) 사용 중")
            if CACHE_MODE_CORE:
                self.append_log(f"[INFO] ✅ stage2_core_Cache.py 로드 완료 (프롬프트 구조 최적화)")
            else:
                self.append_log(f"[WARN] ⚠️ stage2_core_Cache.py 로드 실패 - 캐싱이 작동하지 않을 수 있습니다")
            if CACHE_MODE_BATCH:
                self.append_log(f"[INFO] ✅ stage2_batch_api_기존gpt.py 캐싱 모드 활성화 (prompt_cache_key 사용)")
                self.append_log(f"[INFO] 💰 토큰 비용 최대 90% 절감 가능 (캐시 히트율에 따라 다름)")
            else:
                self.append_log(f"[WARN] ⚠️ stage2_batch_api_기존gpt.py 캐싱 모드 비활성화 - prompt_cache_key가 추가되지 않을 수 있습니다")
            self.append_log(f"[INFO] 상세이미지 개수 제한: {max_detail_images}개 (상세이미지_1 ~ 상세이미지_{max_detail_images})")
            
            # 리사이즈 모드 설명
            resize_mode_desc = {
                "A": "기본 모드 (리사이즈 안 함)",
                "B": "리사이즈 모드 512px (가로 512px로 축소, 비율 유지)",
                "C": "리사이즈 모드 448px (가로 448px로 축소, 비율 유지)",
            }.get(resize_mode, f"알 수 없는 모드: {resize_mode}")
            self.append_log(f"[INFO] 이미지 리사이즈 모드: {resize_mode} ({resize_mode_desc})")

            # 1) 엑셀 → JSONL 생성
            base, _ = os.path.splitext(src)
            jsonl_path = f"{base}_stage2_batch_input.jsonl"

            # 함수 시그니처 확인 후 파라미터 전달 (캐시 문제 대응)
            import inspect
            sig = inspect.signature(create_stage2_batch_input_jsonl)
            params = list(sig.parameters.keys())
            
            # max_detail_images 파라미터 전달
            call_kwargs = {
                "excel_path": src,
                "jsonl_path": jsonl_path,
                "model_name": model,
                "effort": effort,
                "skip_filled": self.skip_exist_var.get(),
                "use_thumbnail": use_thumb,
                "allow_url": allow_url,
                "log_func": self.append_log,
            }
            
            if "max_detail_images" in params:
                call_kwargs["max_detail_images"] = max_detail_images
            
            if "resize_mode" in params:
                call_kwargs["resize_mode"] = resize_mode
            
            info = create_stage2_batch_input_jsonl(**call_kwargs)

            self.append_log(
                f"[DONE] 요청 JSONL 생성: total_rows={info['total_rows']}, "
                f"target_rows={info['target_rows']}, num_requests={info['num_requests']}"
            )

            # 2) 배치 파일 크기 확인 및 분할 처리
            jsonl_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
            self.append_log(f"[INFO] JSONL 파일 크기: {jsonl_size_mb:.2f} MB, 처리 건수(행): {info['num_requests']}개")
            
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
                    model_name=model,
                    effort=effort,
                    max_size_mb=MAX_FILE_SIZE_MB,
                    max_requests=999999,  # 요청 수 제한 거의 제거 (용량이 우선)
                    resize_mode=resize_mode,
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

                # 3) 작업 이력 기록 (resize_mode 포함)
                upsert_batch_job(
                    batch_id=batch_id,
                    src_excel=src,
                    jsonl_path=jsonl_path,
                    model=model,
                    effort=effort,
                    status=batch.status,
                    output_file_id=None,
                    resize_mode=resize_mode,
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
    
    def _create_batch_chunks(self, client, jsonl_path, excel_path, model_name, effort, max_size_mb=180, max_requests=999999, resize_mode=None):
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
        # 예상 청크 수 계산 (용량 기준만 사용, 요청 수는 용량 제한 내에서 가능한 만큼 포함)
        original_file_size_mb = os.path.getsize(jsonl_path) / (1024 * 1024)
        estimated_total_chunks = max(1, int(original_file_size_mb / max_size_mb) + 1)
        
        self.append_log(f"[INFO] 총 {total_requests}건(행)을 분할합니다... (예상: 약 {estimated_total_chunks}개 청크, 그룹 ID: {batch_group_id})")
        
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
            # 실제 파일 크기를 정확히 예측하기 위해 JSON 직렬화 + 줄바꿈 문자 고려
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
            # 실제 생성된 청크 수로 표시 (나중에 업데이트될 수 있음)
            self.append_log(f"[INFO] 청크 {chunk_num}: {len(chunk_requests)}건(행) 포함, {chunk_size_mb:.2f} MB")
            
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
                        resize_mode=resize_mode,  # 리사이즈 모드
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
        
        # 그룹 접기/펼치기 버튼 추가
        f_group_ctrl = ttk.Frame(self.sub_active)
        f_group_ctrl.pack(fill='x', pady=(0, 5))
        ttk.Label(f_group_ctrl, text="💡 그룹 헤더를 더블클릭하면 접기/펼치기가 됩니다.", 
                 font=("맑은 고딕", 8), foreground="#666").pack(side='left', padx=5)
        ttk.Button(f_group_ctrl, text="📂 모든 그룹 펼치기", command=lambda: self._expand_all_groups(self.tree_active)).pack(side='right', padx=2)
        ttk.Button(f_group_ctrl, text="📁 모든 그룹 접기", command=lambda: self._collapse_all_groups(self.tree_active)).pack(side='right', padx=2)
        
        cols = ("batch_id", "excel_name", "memo", "status", "created", "completed", "model", "effort", "counts", "group")
        # 계층 구조를 위해 show='tree headings' 사용 (트리 아이콘 + 컬럼 헤더)
        self.tree_active = ttk.Treeview(self.sub_active, columns=cols, show='tree headings', height=15, selectmode='extended')
        self.tree_active.tag_configure('odd', background=COLOR_WHITE)
        self.tree_active.tag_configure('even', background='#F2F7FF')
        self.tree_active.tag_configure('group', background='#E8F5E9')  # 그룹 배치 강조
        self.tree_active.tag_configure('group_header', background='#C8E6C9', font=("맑은 고딕", 9, "bold"))  # 그룹 헤더 강조
        # 컬럼 헤더 한글화
        self.tree_active.heading("batch_id", text="배치 ID")
        self.tree_active.heading("excel_name", text="엑셀명")
        self.tree_active.heading("memo", text="메모")
        self.tree_active.heading("status", text="상태")
        self.tree_active.heading("created", text="생성일")
        self.tree_active.heading("completed", text="완료일")
        self.tree_active.heading("model", text="모델")
        self.tree_active.heading("effort", text="Effort")
        self.tree_active.heading("counts", text="건수")
        self.tree_active.heading("group", text="그룹")
        # 컬럼 너비 조정: 그룹명이 길어서 트리 컬럼 확대, 일부 컬럼은 축소
        self.tree_active.column("#0", width=350, anchor="w")  # 트리 컬럼 (그룹명 표시) - 250 → 350
        self.tree_active.column("batch_id", width=200, anchor="w")
        self.tree_active.column("excel_name", width=200, anchor="w")  # 엑셀 파일명
        self.tree_active.column("memo", width=150, anchor="w")  # 메모
        self.tree_active.column("status", width=100, anchor="center")
        self.tree_active.column("created", width=120, anchor="center")  # 줄임
        self.tree_active.column("completed", width=120, anchor="center")  # 줄임
        self.tree_active.column("model", width=100, anchor="center")  # 줄임
        self.tree_active.column("effort", width=70, anchor="center")  # 80 → 70
        self.tree_active.column("counts", width=80, anchor="center")  # 줄임
        self.tree_active.column("group", width=100, anchor="center")  # 120 → 100
        self.tree_active.pack(fill='both', expand=True, padx=5, pady=5)
        
        self.menu_active = Menu(self, tearoff=0)
        self.menu_active.add_command(label="상태 갱신", command=lambda: self._refresh_selected(self.tree_active))
        self.menu_active.add_command(label="결과 병합", command=self._merge_selected)
        self.menu_active.add_command(label="분석 리포트 생성", command=self._report_selected_unified)
        self.menu_active.add_separator()
        self.menu_active.add_command(label="메모 편집", command=lambda: self._edit_memo(self.tree_active))
        self.menu_active.add_separator()
        self.menu_active.add_command(label="🔄 실패한 청크로 이동 (재시도)", command=self._move_failed_to_retry)
        self.menu_active.add_separator()
        self.menu_active.add_command(label="휴지통으로 이동", command=self._archive_selected)
        self.tree_active.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_active, self.menu_active))
        self.tree_active.bind("<Double-1>", self._on_tree_double_click)

        # Archive UI
        f_arch_ctrl = ttk.Frame(self.sub_archive)
        f_arch_ctrl.pack(fill='x', pady=(0, 10))
        ttk.Button(f_arch_ctrl, text="♻️ 선택 복구", command=self._restore_selected, style="Primary.TButton").pack(side='left')
        ttk.Button(f_arch_ctrl, text="🔥 선택 영구 삭제", command=self._hard_delete_selected, style="Danger.TButton").pack(side='right')
        
        # 그룹 접기/펼치기 버튼 추가
        f_arch_group_ctrl = ttk.Frame(self.sub_archive)
        f_arch_group_ctrl.pack(fill='x', pady=(0, 5))
        ttk.Label(f_arch_group_ctrl, text="💡 그룹 헤더를 더블클릭하면 접기/펼치기가 됩니다.", 
                 font=("맑은 고딕", 8), foreground="#666").pack(side='left', padx=5)
        ttk.Button(f_arch_group_ctrl, text="📂 모든 그룹 펼치기", command=lambda: self._expand_all_groups(self.tree_arch)).pack(side='right', padx=2)
        ttk.Button(f_arch_group_ctrl, text="📁 모든 그룹 접기", command=lambda: self._collapse_all_groups(self.tree_arch)).pack(side='right', padx=2)
        # 계층 구조를 위해 show='tree headings' 사용
        self.tree_arch = ttk.Treeview(self.sub_archive, columns=cols, show='tree headings', height=15, selectmode='extended')
        self.tree_arch.tag_configure('odd', background=COLOR_WHITE)
        self.tree_arch.tag_configure('even', background='#FFF2F2')
        self.tree_arch.tag_configure('group', background='#FFE8E8')  # 그룹 배치 강조
        self.tree_arch.tag_configure('group_header', background='#FFCDD2', font=("맑은 고딕", 9, "bold"))  # 그룹 헤더 강조
        # 컬럼 헤더 한글화
        self.tree_arch.heading("batch_id", text="배치 ID")
        self.tree_arch.heading("excel_name", text="엑셀명")
        self.tree_arch.heading("memo", text="메모")
        self.tree_arch.heading("status", text="상태")
        self.tree_arch.heading("created", text="생성일")
        self.tree_arch.heading("completed", text="완료일")
        self.tree_arch.heading("model", text="모델")
        self.tree_arch.heading("effort", text="Effort")
        self.tree_arch.heading("counts", text="건수")
        self.tree_arch.heading("group", text="그룹")
        # 컬럼 너비 조정: 그룹명이 길어서 트리 컬럼 확대, 일부 컬럼은 축소
        self.tree_arch.column("#0", width=350, anchor="w")  # 트리 컬럼 (그룹명 표시) - 250 → 350
        self.tree_arch.column("batch_id", width=200, anchor="w")
        self.tree_arch.column("excel_name", width=200, anchor="w")  # 엑셀 파일명
        self.tree_arch.column("memo", width=150, anchor="w")  # 메모
        self.tree_arch.column("status", width=100, anchor="center")
        self.tree_arch.column("created", width=120, anchor="center")  # 줄임
        self.tree_arch.column("completed", width=120, anchor="center")  # 줄임
        self.tree_arch.column("model", width=100, anchor="center")  # 줄임
        self.tree_arch.column("effort", width=70, anchor="center")  # 80 → 70
        self.tree_arch.column("counts", width=80, anchor="center")  # 줄임
        self.tree_arch.column("group", width=100, anchor="center")  # 120 → 100
        self.tree_arch.pack(fill='both', expand=True)
        
        self.menu_arch = Menu(self, tearoff=0)
        self.menu_arch.add_command(label="메모 편집", command=lambda: self._edit_memo(self.tree_arch))
        self.menu_arch.add_separator()
        self.menu_arch.add_command(label="♻️ 선택 복구", command=self._restore_selected)
        self.menu_arch.add_command(label="🔥 선택 영구 삭제", command=self._hard_delete_selected)
        self.tree_arch.bind("<Button-3>", lambda event: self._show_context_menu(event, self.tree_arch, self.menu_arch))
        
        self._load_jobs_all()
        self._load_archive_list()

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
    
    def _show_context_menu(self, event, tree, menu):
        item = tree.identify_row(event.y)
        if item:
            if item not in tree.selection(): tree.selection_set(item)
            menu.post(event.x_root, event.y_root)

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
            messagebox.showwarning("경고", "메모를 편집할 배치를 선택해주세요.")
            return
        
        # 첫 번째 선택된 항목의 배치 ID 가져오기
        item = selection[0]
        vals = tree.item(item)['values']
        batch_id = vals[0] if vals else ""
        
        # 그룹 헤더인 경우 처리
        if not batch_id:
            messagebox.showinfo("안내", "그룹 헤더가 아닌 개별 배치를 선택해주세요.")
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
                status_summary += f" | 건: {total_completed}/{total_requests}"
            
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
                        j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, group_display
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
            excel_name = get_excel_name_from_path(j.get("src_excel", ""))
            memo = j.get("memo", "") or "-"
            tag = 'even'
            self.tree_active.insert("", "end", 
                text=j["batch_id"][:30],
                values=(
                    j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, "-"
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
            
            # 그룹 헤더 생성 (요약 정보 - 간소화)
            completed_count = statuses.get("completed", 0) + statuses.get("merged", 0)
            status_summary = f"완료: {completed_count}/{total_chunks}"
            if total_requests > 0:
                status_summary += f" | 건: {total_completed}/{total_requests}"
            
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
                        j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, group_display
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
            excel_name = get_excel_name_from_path(j.get("src_excel", ""))
            memo = j.get("memo", "") or "-"
            tag = 'even'
            self.tree_arch.insert("", "end", 
                text=j["batch_id"][:30],
                values=(
                    j["batch_id"], excel_name, memo, j.get("status"), c_at, f_at, j.get("model"), j.get("effort", "-"), cnt, "-"
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
                # completed 또는 expired 상태인 배치 포함 (expired 상태에서도 output_file_id가 있으면 다운로드 가능)
                group_batches = [j for j in jobs if j.get("batch_group_id") == group_id and j.get("status") in ["completed", "expired"]]
                for j in group_batches:
                    all_target_ids.add(j["batch_id"])
            
            if len(all_target_ids) > len(ids):
                group_info = f"\n\n같은 그룹의 배치 {len(all_target_ids) - len(ids)}개가 자동으로 포함됩니다."
            else:
                group_info = ""
        else:
            group_info = ""
        
        # completed 또는 expired 상태인 배치 포함 (expired 상태에서도 output_file_id가 있으면 다운로드 가능)
        targets = [bid for bid in all_target_ids if next((j for j in jobs if j["batch_id"] == bid), {}).get("status") in ["completed", "expired"]]
        if not targets:
            messagebox.showinfo("알림", "병합할 수 있는 'completed' 또는 'expired' 상태의 작업이 없습니다.")
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
                        
                        # completed 또는 expired 상태에서 다운로드 성공한 경우만 수집
                        if not ok or status not in ["completed", "expired"]:
                            self.append_log(f"  ⚠️ {bid}: 다운로드할 수 없어서 건너뜁니다. (status={status}, ok={ok})")
                            continue
                        
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
                            self.append_log(f"  [캐싱] {bid}: 요청 {batch_total_requests}건, 히트 {batch_cache_hits}건 ({cache_hit_rate:.1f}%), 캐시 토큰 {batch_cached_tok:,}")
                        
                    except Exception as e:
                        self.append_log(f"  ❌ {bid} 결과 다운로드 실패: {e}")
                        continue
                
                if not all_output_lines:
                    self.append_log(f"⚠️ 그룹 {group_id}: 병합할 결과가 없습니다.")
                    continue
                
                # 그룹의 전체 청크 수 확인 및 검증
                expected_total_chunks = first_job.get("total_chunks")
                if expected_total_chunks:
                    # 실제로 결과를 다운로드한 배치 수 계산 (completed 또는 expired 상태 포함)
                    downloaded_batch_ids = []
                    for bid in batch_ids_sorted:
                        local_job = next((j for j in jobs if j["batch_id"] == bid), None)
                        if local_job and local_job.get("status") in ["completed", "expired"]:
                            out_jsonl = local_job.get("output_jsonl") or os.path.join(
                                os.path.dirname(src_path),
                                f"{os.path.splitext(os.path.basename(src_path))[0]}_stage2_batch_output_{bid}.jsonl"
                            )
                            if os.path.exists(out_jsonl):
                                downloaded_batch_ids.append(bid)
                    
                    if len(downloaded_batch_ids) < expected_total_chunks:
                        missing = expected_total_chunks - len(downloaded_batch_ids)
                        self.append_log(f"⚠️ 그룹 {group_id}: 예상 {expected_total_chunks}개 중 {len(downloaded_batch_ids)}개만 다운로드되었습니다. ({missing}개 누락 가능)")
                
                # 임시 통합 JSONL 파일 생성 (중복 제거: custom_id 기준)
                base_dir = os.path.dirname(src_path)
                base_name, _ = os.path.splitext(os.path.basename(src_path))
                merged_jsonl = os.path.join(base_dir, f"{base_name}_stage2_batch_output_merged_{group_id}.jsonl")
                
                # custom_id 기준으로 중복 제거 (같은 custom_id가 여러 번 나오면 마지막 것만 사용)
                result_map = {}  # {custom_id: json_line}
                duplicate_count = 0
                
                for line in all_output_lines:
                    try:
                        obj = json.loads(line)
                        custom_id = obj.get("custom_id", "")
                        if custom_id:
                            if custom_id in result_map:
                                duplicate_count += 1
                            result_map[custom_id] = line  # 마지막 것만 유지
                    except Exception as e:
                        self.append_log(f"  [WARN] JSONL 라인 파싱 실패: {e}")
                        continue
                
                # 중복 제거된 결과를 JSONL 파일로 저장
                with open(merged_jsonl, "w", encoding="utf-8") as f:
                    for line in result_map.values():
                        f.write(line + "\n")
                
                if duplicate_count > 0:
                    self.append_log(f"  [그룹] 통합 JSONL 생성: {len(all_output_lines)}개 결과 중 {duplicate_count}개 중복 제거 → {len(result_map)}개 고유 결과")
                else:
                    self.append_log(f"  [그룹] 통합 JSONL 생성: {len(result_map)}개 고유 결과 (중복 없음)")
                
                # 통합 JSONL에서 캐싱 통계 수집
                total_group_cached = 0
                total_group_requests = 0
                total_group_cache_hits = 0
                with open(merged_jsonl, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            data = json.loads(line)
                            response_body = data.get("response", {}).get("body", {})
                            usage = response_body.get("usage", {})
                            input_tokens_details = usage.get("input_tokens_details", {})
                            cached_tokens = input_tokens_details.get("cached_tokens", 0)
                            total_group_cached += cached_tokens
                            total_group_requests += 1
                            if cached_tokens > 0:
                                total_group_cache_hits += 1
                        except:
                            pass
                
                # 그룹 전체 캐싱 통계 출력
                group_cache_hit_rate = (total_group_cache_hits / total_group_requests * 100) if total_group_requests > 0 else 0
                self.append_log(f"  [그룹 캐싱 통계] 요청 {total_group_requests:,}건, 히트 {total_group_cache_hits:,}건 ({group_cache_hit_rate:.1f}%), 캐시 토큰 {total_group_cached:,}")
                
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
                
                # 캐시로 절감된 비용 계산
                pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                group_cache_savings = (total_group_cached / 1_000_000) * pricing["input"] * 0.5
                if group_cache_savings > 0:
                    self.append_log(f"  [그룹 비용절감] 캐싱으로 총 ${group_cache_savings:.4f} 절감")
                
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
                            try:
                                # ISO 형식 파싱 (Z를 +00:00으로 변환)
                                c_at = datetime.fromisoformat(c_at_str.replace('Z', '+00:00'))
                                # 시간대 정보가 있으면 naive로 변환 (datetime.now()와 일치)
                                if c_at.tzinfo is not None:
                                    c_at = c_at.replace(tzinfo=None)
                            except:
                                c_at = datetime.now()
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
                
                # 그룹 전체 캐싱 통계 출력 (통합 JSONL에서 수집)
                total_group_cached = 0
                total_group_requests = 0
                total_group_cache_hits = 0
                with open(merged_jsonl, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            data = json.loads(line)
                            response_body = data.get("response", {}).get("body", {})
                            usage = response_body.get("usage", {})
                            input_tokens_details = usage.get("input_tokens_details", {})
                            cached_tokens = input_tokens_details.get("cached_tokens", 0)
                            total_group_cached += cached_tokens
                            total_group_requests += 1
                            if cached_tokens > 0:
                                total_group_cache_hits += 1
                        except:
                            pass
                
                group_cache_hit_rate = (total_group_cache_hits / total_group_requests * 100) if total_group_requests > 0 else 0
                group_cache_savings_pct = (total_group_cached / (info.get("total_in_tok", 0) or 1)) * 100 if info.get("total_in_tok", 0) > 0 else 0
                pricing = MODEL_PRICING_USD_PER_MTOK.get(model_name, {"input": 0, "output": 0})
                group_cache_savings = (total_group_cached / 1_000_000) * pricing["input"] * 0.5
                
                self.append_log(f"✅ 그룹 병합 완료: {os.path.basename(final_out_path)} ({len(batch_ids)}개 배치)")
                self.append_log(f"  [그룹 캐싱 통계] 요청 {total_group_requests:,}건, 히트 {total_group_cache_hits:,}건 ({group_cache_hit_rate:.1f}%), 캐시 토큰 {total_group_cached:,} ({group_cache_savings_pct:.1f}%)")
                if group_cache_savings > 0:
                    self.append_log(f"  [그룹 비용절감] 캐싱으로 총 ${group_cache_savings:.4f} 절감")
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

                # completed 또는 expired 상태에서 다운로드 성공한 경우만 병합
                if not ok or status not in ["completed", "expired"]:
                    self.append_log(f"⚠️ {bid}: 다운로드할 수 없어서 병합을 건너뜁니다. (status={status}, ok={ok})")
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
                        try:
                            # ISO 형식 파싱 (Z를 +00:00으로 변환)
                            c_at = datetime.fromisoformat(c_at_str.replace('Z', '+00:00'))
                            # 시간대 정보가 있으면 naive로 변환 (datetime.now()와 일치)
                            if c_at.tzinfo is not None:
                                c_at = c_at.replace(tzinfo=None)
                        except:
                            c_at = datetime.now()
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
        
        # 그룹별로 분류하여 중복 제거 (같은 그룹의 배치들은 하나의 병합된 파일만 사용)
        processed_groups = set()  # 이미 처리한 그룹 ID
        processed_files = set()  # 이미 처리한 파일 경로 (그룹 없는 경우)
        
        for bid in ids:
            local_job = next((j for j in jobs if j["batch_id"] == bid), None)
            if not local_job: continue
            
            # 그룹이 있는 경우: 그룹별로 한 번만 처리
            group_id = local_job.get("batch_group_id")
            if group_id:
                if group_id in processed_groups:
                    self.append_log(f"⏭️ {bid}: 그룹 {group_id}는 이미 처리되었습니다. 건너뜁니다.")
                    continue
                processed_groups.add(group_id)
            
            out_path = local_job.get("out_excel")
            if not out_path or not os.path.exists(out_path):
                self.append_log(f"❌ 파일 누락: {bid}")
                continue
            
            # 그룹 없는 경우: 파일 경로 기준으로 중복 제거
            if not group_id:
                if out_path in processed_files:
                    self.append_log(f"⏭️ {bid}: 파일 {os.path.basename(out_path)}는 이미 처리되었습니다. 건너뜁니다.")
                    continue
                processed_files.add(out_path)
            
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
            except Exception as e:
                self.append_log(f"⚠️ {bid} 리포트 생성 중 오류: {e}")
                pass

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

    def _move_failed_to_retry(self):
        """선택된 failed 배치를 실패한 청크 재시도 목록으로 이동"""
        ids = self._get_selected_ids(self.tree_active)
        if not ids:
            messagebox.showwarning("경고", "배치를 선택해주세요.")
            return
        
        jobs = load_batch_jobs()
        failed_jobs = []
        for bid in ids:
            job = next((j for j in jobs if j["batch_id"] == bid), None)
            if job and job.get("status") == "failed":
                failed_jobs.append(job)
        
        if not failed_jobs:
            messagebox.showinfo("알림", "선택한 배치 중 'failed' 상태인 배치가 없습니다.")
            return
        
        if not messagebox.askyesno("확인", f"{len(failed_jobs)}개의 failed 배치를 실패한 청크 재시도 목록으로 이동하시겠습니까?"):
            return
        
        # 실패한 청크 형식으로 변환
        failed_chunks = []
        for job in failed_jobs:
            batch_id = job["batch_id"]
            jsonl_path = job.get("jsonl_path", "")
            
            # jsonl_path가 없으면 원본 엑셀 경로에서 추론
            if not jsonl_path or not os.path.exists(jsonl_path):
                src_excel = job.get("src_excel", "")
                if src_excel and os.path.exists(src_excel):
                    base, _ = os.path.splitext(src_excel)
                    jsonl_path = f"{base}_stage2_batch_input.jsonl"
                    # 청크 파일인 경우도 확인
                    if not os.path.exists(jsonl_path):
                        # 청크 파일 패턴 찾기
                        base_dir = os.path.dirname(src_excel)
                        base_name = os.path.basename(src_excel)
                        base_name_only, _ = os.path.splitext(base_name)
                        # 청크 파일 패턴: *_chunk001.jsonl
                        chunk_index = job.get("chunk_index", 1)
                        jsonl_path = os.path.join(base_dir, f"{base_name_only}_stage2_batch_input_chunk{chunk_index:03d}.jsonl")
            
            # jsonl_path가 여전히 없거나 존재하지 않으면 건너뛰기
            if not jsonl_path or not os.path.exists(jsonl_path):
                self.append_log(f"⚠️ {batch_id}: JSONL 파일을 찾을 수 없어 건너뜁니다. (jsonl_path: {jsonl_path})")
                continue
            
            chunk_num = job.get("chunk_index", 1)
            error_msg = job.get("error", "알 수 없는 오류") or "알 수 없는 오류"
            is_token_limit = "token limit" in error_msg.lower() or "enqueued token limit" in error_msg.lower()
            
            failed_chunk_info = {
                "chunk_num": chunk_num,
                "chunk_file": jsonl_path,
                "error": error_msg,
                "is_token_limit": is_token_limit,
                "excel_path": job.get("src_excel", ""),
                "model_name": job.get("model", "gpt-5-mini"),
                "effort": job.get("effort", "medium"),
                "batch_group_id": job.get("batch_group_id", ""),
            }
            failed_chunks.append(failed_chunk_info)
        
        if not failed_chunks:
            messagebox.showwarning("경고", "이동할 수 있는 failed 배치가 없습니다.\n(JSONL 파일을 찾을 수 없음)")
            return
        
        # 실패 정보 JSON 파일 생성 또는 기존 파일에 추가
        # 원본 엑셀 경로에서 기본 경로 추론
        if failed_chunks[0].get("excel_path"):
            base_dir = os.path.dirname(failed_chunks[0]["excel_path"])
            base_name, _ = os.path.splitext(os.path.basename(failed_chunks[0]["excel_path"]))
            failed_info_path = os.path.join(base_dir, f"{base_name}_failed_chunks.json")
        else:
            # 엑셀 경로가 없으면 스크립트 디렉토리에 저장
            failed_info_path = os.path.join(os.path.dirname(__file__), "failed_chunks.json")
        
        # 기존 파일이 있으면 읽어서 병합 (중복 제거)
        existing_chunks = []
        if os.path.exists(failed_info_path):
            try:
                with open(failed_info_path, "r", encoding="utf-8") as f:
                    existing_chunks = json.load(f)
            except Exception as e:
                self.append_log(f"[WARN] 기존 실패 정보 파일 읽기 실패: {e}")
        
        # 중복 제거 (chunk_file 기준)
        existing_files = {chunk.get("chunk_file", "") for chunk in existing_chunks}
        new_chunks = [chunk for chunk in failed_chunks if chunk.get("chunk_file", "") not in existing_files]
        
        if new_chunks:
            # 기존 항목과 새 항목 병합
            all_chunks = existing_chunks + new_chunks
            try:
                with open(failed_info_path, "w", encoding="utf-8") as f:
                    json.dump(all_chunks, f, ensure_ascii=False, indent=2)
                
                # 재시도 목록 업데이트
                self.failed_chunks_file_var.set(failed_info_path)
                self._load_failed_chunks_from_file(failed_info_path)
                
                self.append_log(f"[INFO] {len(new_chunks)}개의 failed 배치를 실패한 청크 재시도 목록에 추가했습니다.")
                self.append_log(f"[INFO] 실패 정보 파일: {os.path.basename(failed_info_path)}")
                
                # 재시도 탭으로 자동 전환
                self.main_tabs.select(self.tab_merge)
                
                messagebox.showinfo("완료", f"{len(new_chunks)}개의 failed 배치를 실패한 청크 재시도 목록에 추가했습니다.\n\n파일: {os.path.basename(failed_info_path)}\n\n재시도 탭에서 확인하세요.")
            except Exception as e:
                self.append_log(f"[ERROR] 실패 정보 파일 저장 실패: {e}")
                messagebox.showerror("오류", f"실패 정보 파일 저장 실패:\n{e}")
        else:
            messagebox.showinfo("알림", "모든 failed 배치가 이미 재시도 목록에 있습니다.")

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
        ToolTip(btn_retry, "토큰 제한 오류 등으로 실패한 청크를 재시도합니다.\n실패 정보 JSON 파일을 선택하고 재시도 버튼을 클릭하세요.")
        
        f_in = ttk.LabelFrame(container, text="개별 작업", padding=15)
        f_in.pack(fill='x', pady=(0, 15))
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
            model_name = failed_info.get("model_name", "gpt-5-mini")
            effort = failed_info.get("effort", "medium")
            batch_group_id = failed_info.get("batch_group_id", "")
            
            if not os.path.exists(chunk_file):
                self.append_log(f"⚠️ 청크 {chunk_num}: 파일을 찾을 수 없습니다: {chunk_file}")
                continue
            
            self.append_log(f"[RETRY] 청크 {chunk_num} 재시도 중... ({os.path.basename(chunk_file)})")
            
            try:
                batch = create_batch_from_jsonl(
                    client=client,
                    jsonl_path=chunk_file,
                    excel_path=excel_path,
                    model_name=model_name,
                    log_func=self.append_log,
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
            # 배치 목록 갱신
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
    app = Stage2BatchGUI()
    app.mainloop()