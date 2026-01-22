# stage2_batch_api.py
"""
Stage2 Batch API 실행 스크립트

- 엑셀(Stage2 메타 + 이미지 경로)을 읽어서
  → Batch API용 요청 JSONL을 만든 뒤
  → OpenAI Batch를 생성하고
  → Batch 결과 JSONL을 받아
  → 다시 엑셀의 ST2_JSON 컬럼에 병합하는 도구.

추가 기능:
- stage2_batch_jobs.json 에 전체 배치 이력 저장
- GUI에서 배치 작업 목록 관리 + 보관함(Archive) 기능
  - 선택 배치를 보관함으로 이동 (GUI 목록에서만 숨김)
  - 보관함 창에서 선택 배치 복구 / 완전 삭제
- 배치 생성 시 사용한 model / effort 도 목록에서 확인 가능
"""

import os
import sys
import json
import re
import time
import base64
import mimetypes
import subprocess
from datetime import datetime

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

from openai import OpenAI
# 캐싱 최적화 버전 사용 (stage2_core_Cache.py)
try:
    from stage2_core_Cache import safe_str, build_stage2_request_from_row, infer_sale_type
    CACHE_MODE = True
except ImportError:
    # 캐싱 버전이 없으면 일반 버전 사용
    try:
        from stage2_core import safe_str, build_stage2_request_from_row, infer_sale_type
    except ImportError:
        from stage2_core import safe_str, build_stage2_request_from_row
        # infer_sale_type이 없으면 간단한 대체 함수 사용
        def infer_sale_type(option_raw, raw_name):
            if option_raw and "," in option_raw:
                return "옵션형"
            return "단품형"
    CACHE_MODE = False


# -------------------------------------------------------------------
# 설정 파일/경로
# -------------------------------------------------------------------

# Stage2 LLM GUI와는 다른 키 파일 사용 (배치 전용)
API_KEY_FILE = ".openai_api_key_stage2_batch"

# Stage2 배치 작업 이력 JSON 파일 (이 .py 파일이 있는 폴더 기준)
BATCH_JOBS_FILE = os.path.join(os.path.dirname(__file__), "stage2_batch_jobs.json")


# =======================
# 모델별 가격 (USD)
# - 단위: 1,000,000 토큰
# - reasoning 토큰은 출력 단가로 계산
# =======================
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


# =========================================================
# API 키 로드/저장
# =========================================================

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
# 배치 작업 이력(JSON) 관리
#   - stage2_batch_jobs.json 에 모든 배치 작업을 누적 관리
#   - archived 플래그로 보관함 여부 표시
#   - result_dir 보정 로직 포함
# =========================================================

def _ensure_result_dir(job: dict) -> None:
    """result_dir 필드가 없으면 src_excel/out_excel 기준으로 채워준다."""
    if job.get("result_dir"):
        return
    rd = None
    src_excel = job.get("src_excel")
    out_excel = job.get("out_excel")
    if isinstance(src_excel, str) and src_excel:
        rd = os.path.dirname(src_excel)
    elif isinstance(out_excel, str) and out_excel:
        rd = os.path.dirname(out_excel)
    job["result_dir"] = rd


def _fill_job_defaults(job: dict) -> dict:
    """로드/저장 시 각 job dict에 기본 필드를 보정."""
    now_iso = datetime.now().isoformat()

    # created_at 기본값
    if not job.get("created_at"):
        job["created_at"] = now_iso

    # archived 기본값
    job["archived"] = bool(job.get("archived", False))

    # result_dir 보정
    _ensure_result_dir(job)

    # model / effort 키가 없으면 공백으로 맞춰두기 (GUI에서 접근 용이)
    job.setdefault("model", "")
    job.setdefault("effort", "")

    return job


def load_all_batch_jobs(log_func=None):
    """
    stage2_batch_jobs.json 에 저장된 전체 배치 작업 목록 반환.
    각 항목은 dict (batch_id, src_excel, jsonl_path, out_excel, model, effort, archived, ...).
    """
    if not os.path.exists(BATCH_JOBS_FILE):
        return []

    try:
        with open(BATCH_JOBS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        jobs = []
        if isinstance(data, list):
            for job in data:
                if isinstance(job, dict):
                    jobs.append(_fill_job_defaults(job))
        else:
            if log_func:
                log_func(f"[WARN] 배치 이력 JSON 포맷이 리스트가 아닙니다: {BATCH_JOBS_FILE}")
        return jobs
    except Exception as e:
        if log_func:
            log_func(f"[WARN] 배치 이력 JSON 로드 실패: {BATCH_JOBS_FILE} ({e})")
        return []


def save_all_batch_jobs(jobs, log_func=None):
    """jobs 리스트를 stage2_batch_jobs.json 에 저장."""
    try:
        norm_jobs = []
        for job in jobs:
            if isinstance(job, dict):
                norm_jobs.append(_fill_job_defaults(job.copy()))
        with open(BATCH_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(norm_jobs, f, ensure_ascii=False, indent=2)
        if log_func:
            log_func(f"[JOBS] 배치 이력 {len(norm_jobs)}개 저장: {BATCH_JOBS_FILE}")
    except Exception as e:
        if log_func:
            log_func(f"[WARN] 배치 이력 저장 실패: {BATCH_JOBS_FILE} ({e})")


def upsert_batch_job(batch_id: str, log_func=None, **fields):
    """
    batch_id 기준으로 배치 작업 이력을 추가/갱신.
    - 존재하면 update, 없으면 append.
    - archived, result_dir, created_at 등을 보정.
    """
    if not batch_id:
        return None

    jobs = load_all_batch_jobs(log_func=log_func)
    now_iso = datetime.now().isoformat()

    found = None  # type: ignore
    for job in jobs:
        if job.get("batch_id") == batch_id:
            found = job
            break

    if found is None:
        job = {"batch_id": batch_id}
        job.update(fields)

        if "created_at" not in job or not job.get("created_at"):
            job["created_at"] = now_iso

        # archived 기본값 False
        if "archived" not in job:
            job["archived"] = False

        _fill_job_defaults(job)
        jobs.append(job)
        if log_func:
            log_func(f"[JOBS] 새 배치 작업 추가: batch_id={batch_id}")
        result = job
    else:
        # 기존 job 업데이트
        found.update(fields)
        _fill_job_defaults(found)
        if log_func:
            log_func(f"[JOBS] 배치 작업 업데이트: batch_id={batch_id}")
        result = found

    save_all_batch_jobs(jobs, log_func=log_func)
    return result


# =========================================================
# Tkinter Tooltip Helper
# =========================================================

class ToolTip:
    """간단한 툴팁 유틸리티 (위젯에 마우스 올리면 설명 표시)"""

    def __init__(self, widget, text: str, wraplength: int = 320):
        self.widget = widget
        self.text = text
        self.wraplength = wraplength
        self.tip_window = None
        self.widget.bind("<Enter>", self._on_enter)
        self.widget.bind("<Leave>", self._on_leave)

    def _on_enter(self, event=None):
        self.show_tip()

    def _on_leave(self, event=None):
        self.hide_tip()

    def show_tip(self):
        if self.tip_window or not self.text:
            return

        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + 20

        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")

        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            font=("tahoma", 8),
            wraplength=self.wraplength,
        )
        label.pack(ipadx=4, ipady=2)

    def hide_tip(self):
        tw = self.tip_window
        if tw is not None:
            self.tip_window = None
            tw.destroy()


# =========================================================
# 폴더 열기 Helper
# =========================================================

def open_folder(path: str, log_func=None):
    """OS 기본 탐색기로 폴더 열기"""
    if not path:
        if log_func:
            log_func("[WARN] 폴더 경로가 비어 있어 열 수 없습니다.")
        return
    path = os.path.abspath(path)
    if not os.path.exists(path):
        if log_func:
            log_func(f"[WARN] 폴더를 찾을 수 없습니다: {path}")
        return
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
        if log_func:
            log_func(f"[INFO] 탐색기로 폴더 열기: {path}")
    except Exception as e:
        if log_func:
            log_func(f"[WARN] 폴더 열기 실패: {path} ({e})")


# =========================================================
# 이미지 → data URL 변환 (로컬 이미지 직접 첨부용)
# =========================================================

def encode_image_to_data_url(path: str, max_height: int = None, max_width: int = None, jpeg_quality: int = 85, log_func=None) -> str:
    """
    로컬 이미지 파일을 data:[mime];base64,... 형태 문자열로 변환.
    
    Args:
        path: 이미지 파일 경로
        max_height: 최대 높이 (px). None이면 리사이징 안 함. 세로가 긴 이미지(4000-6000px)를 줄여서 토큰 절감.
        max_width: 최대 가로 (px). None이면 리사이징 안 함. 가로 기준 비율 유지 리사이즈 (크롭/패딩 금지).
                   max_width가 지정되면 max_height보다 우선적으로 사용됨.
        jpeg_quality: JPEG 품질 (1-100). 낮을수록 파일 크기 작아짐. 기본값 85.
    
    Returns:
        data URL 문자열
    """
    mime, _ = mimetypes.guess_type(path)
    if mime is None:
        mime = "image/jpeg"
    
    # 리사이징이 필요하면 PIL 사용
    # 가로 기준 리사이즈가 우선 (max_width가 지정된 경우)
    if max_width is not None or max_height is not None:
        try:
            from PIL import Image
            import io
            
            with Image.open(path) as img:
                # 원본 크기 확인
                original_width, original_height = img.size
                need_resize = False
                new_width = original_width
                new_height = original_height
                
                # 가로 기준 리사이즈 우선 (비율 유지, 크롭/패딩 금지)
                if max_width is not None:
                    if original_width > max_width:
                        # 비율 유지하면서 가로 기준으로 리사이징
                        ratio = max_width / original_width
                        new_width = max_width
                        new_height = round(original_height * ratio)
                        need_resize = True
                    # 디버깅: 리사이즈 정보 로그 (로컬 파일인 경우에만)
                    if log_func and not path.startswith('http'):
                        reduction_pct = (1 - (new_width * new_height) / (original_width * original_height)) * 100
                        log_func(f"[리사이즈 적용] {os.path.basename(path)}: {original_width}x{original_height} → {new_width}x{new_height} (가로 기준 {max_width}px, 비율 유지, 면적 {reduction_pct:.1f}% 감소)")
                    else:
                        # 이미지가 이미 max_width 이하이면 리사이즈 불필요
                        if log_func and not path.startswith('http'):
                            log_func(f"[리사이즈] {os.path.basename(path)}: {original_width}x{original_height} (이미 {max_width}px 이하, 리사이즈 불필요)")
                # 세로 기준 리사이즈 (호환성 유지, 가로 기준이 지정되지 않은 경우만)
                elif max_width is None and max_height is not None:
                    if original_height > max_height:
                        # 비율 유지하면서 세로 기준으로 리사이징
                        ratio = max_height / original_height
                        new_width = round(original_width * ratio)
                        new_height = max_height
                        need_resize = True
                    else:
                        # 이미지가 이미 max_height 이하이면 리사이즈 불필요
                        if log_func and not path.startswith('http'):
                            log_func(f"[리사이즈] {os.path.basename(path)}: {original_width}x{original_height} (이미 {max_height}px 이하, 리사이즈 불필요)")
                
                if need_resize:
                    # RGB 모드로 변환 (RGBA 등도 지원)
                    if img.mode not in ('RGB', 'L'):
                        img = img.convert('RGB')
                    
                    # 고품질 리샘플링 (Lanczos 권장)
                    img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    
                    # JPEG로 저장 (메모리)
                    output = io.BytesIO()
                    img_resized.save(output, format='JPEG', quality=jpeg_quality, optimize=True)
                    output.seek(0)
                    
                    # base64 인코딩
                    b64 = base64.b64encode(output.read()).decode("ascii")
                    mime = "image/jpeg"  # 리사이징 후에는 항상 JPEG
                    
                    return f"data:{mime};base64,{b64}"
        except ImportError:
            # PIL이 없으면 원본 그대로 사용
            if log_func and not path.startswith('http'):
                log_func(f"[WARN] PIL(Pillow)이 설치되지 않아 리사이즈를 수행할 수 없습니다: {os.path.basename(path)} (원본 사용)")
        except Exception as e:
            # 리사이징 실패 시 원본 사용
            error_msg = f"[WARN] 이미지 리사이징 실패 ({os.path.basename(path)}): {e}, 원본 사용"
            if log_func:
                log_func(error_msg)
            else:
                print(error_msg)
    
    # 리사이징 없이 원본 사용
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"


def build_image_nodes_from_paths(paths, log_func=None, cache=None, allow_url: bool = False, max_image_height: int = None, max_image_width: int = None, resize_mode: str = "A", jpeg_quality: int = 85):
    """
    이미지 경로 리스트 → Responses API용 input_image 노드 리스트.
    (http/https URL 또는 로컬 파일 경로 모두 지원)
    
    Args:
        paths: 이미지 경로 리스트
        log_func: 로그 함수
        cache: {경로: data_url} 캐시 딕셔너리 (있으면 재사용)
        allow_url: False면 http/https URL은 모두 스킵하고 로컬 파일만 사용.
        max_image_height: 최대 이미지 높이 (px). None이면 리사이징 안 함. (호환성 유지, resize_mode가 우선)
        max_image_width: 최대 이미지 가로 (px). resize_mode가 B(512) 또는 C(448)일 때 사용됨.
        resize_mode: 리사이즈 모드. "A"(기본/리사이즈 안 함), "B"(가로 512px), "C"(가로 448px)
        jpeg_quality: JPEG 품질 (1-100). 낮을수록 파일 크기 작아짐. 기본값 85.
    """
    image_nodes = []
    seen = set()
    url_skip_count = 0  # URL 스킵 카운터
    
    # resize_mode에 따라 max_image_width 결정
    if resize_mode == "B":
        target_width = 512
    elif resize_mode == "C":
        target_width = 448
    else:  # "A" 또는 기타
        target_width = None
    
    # max_image_width가 명시적으로 지정된 경우 우선 사용 (resize_mode 무시)
    if max_image_width is not None:
        target_width = max_image_width

    for raw_p in paths:
        p = safe_str(raw_p)
        if not p:
            continue
        if p in seen:
            continue
        seen.add(p)

        lower = p.lower()

        # 1) URL 이미지
        if lower.startswith("http://") or lower.startswith("https://"):
            if not allow_url:
                url_skip_count += 1
                continue

            image_nodes.append(
                {
                    "type": "input_image",
                    "image_url": p,
                }
            )
            continue

        # 2) 로컬 파일
        if os.path.exists(p):
            try:
                # 캐시 키에 리사이징 옵션 포함 (다른 옵션으로 리사이징된 경우 구분)
                # resize_mode와 target_width 모두 포함하여 정확히 구분
                if target_width is not None:
                    cache_key = f"{p}:w{target_width}:q{jpeg_quality}:m{resize_mode}"
                elif max_image_height is not None:
                    cache_key = f"{p}:h{max_image_height}:q{jpeg_quality}:m{resize_mode}"
                else:
                    cache_key = f"{p}:m{resize_mode}"
                
                if cache is not None and cache_key in cache:
                    data_url = cache[cache_key]
                else:
                    # 가로 기준 리사이즈 우선 (resize_mode B/C)
                    data_url = encode_image_to_data_url(
                        p, 
                        max_height=max_image_height, 
                        max_width=target_width,
                        jpeg_quality=jpeg_quality,
                        log_func=log_func
                    )
                    if cache is not None:
                        cache[cache_key] = data_url

                image_nodes.append(
                    {
                        "type": "input_image",
                        "image_url": data_url,
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
    
    # URL 스킵 요약 로그 (100개 이상일 때만 출력)
    if url_skip_count > 0:
        if url_skip_count >= 100:
            msg = f"[INFO] URL 이미지 {url_skip_count}개가 'URL 이미지 허용' 옵션으로 인해 스킵되었습니다."
        else:
            # 100개 미만일 때는 개별 로그 출력 (디버깅용)
            msg = f"[INFO] URL 이미지 {url_skip_count}개 스킵됨 (URL 허용 옵션 비활성화)"
        if log_func:
            log_func(msg)
        else:
            print(msg)

    return image_nodes


# =========================================================
# Batch 응답(JSON dict)에서 텍스트 추출
# =========================================================

def extract_text_from_response_dict(resp_dict: dict) -> str:
    """
    Batch Output JSONL의 각 줄에 들어있는
    response(dict) 객체에서 텍스트(JSON 문자열)를 추출한다.
    - 구조는 responses.create().model_dump() 와 거의 동일하다고 가정.
    """
    if not isinstance(resp_dict, dict):
        raise RuntimeError("response가 dict 형태가 아닙니다.")

    text_chunks = []

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


def extract_usage_tokens_from_response_dict(resp_dict: dict):
    """
    response(dict)["usage"] 에서 input/output/reasoning 토큰 합계를 추출.
    """
    in_tok = 0
    out_tok = 0
    reasoning_tok = 0

    try:
        usage = resp_dict.get("usage")
        if not isinstance(usage, dict):
            return 0, 0, 0

        it = int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
        ot = int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
        rt = int(usage.get("reasoning_tokens") or 0)

        details = usage.get("output_tokens_details")
        if isinstance(details, dict):
            rt_detail = details.get("reasoning_tokens")
            if rt_detail is not None:
                rt = int(rt_detail)

        in_tok += it
        out_tok += ot
        reasoning_tok += rt

    except Exception:
        pass

    return in_tok, out_tok, reasoning_tok


# =========================================================
# 비용 계산
# =========================================================

def calc_costs_usd(
    model_name: str,
    total_in_tok: int,
    total_out_tok: int,
    total_reasoning_tok: int,
):
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


# =========================================================
# 엑셀 → Batch 요청 JSONL 생성
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
    max_image_height: int = None,
    jpeg_quality: int = 85,
    log_func=None,
):
    """
    Stage2 엑셀을 읽어서 Batch API용 요청 JSONL을 만든다.
    - ST2_JSON 이 비어 있는 행만 대상으로 한다(skip_filled=True일 때).
    - Stage2Request(prompt + image_paths)를 이용해 요청을 구성.
    - custom_id 는 "row-{index}" 형식.
    """
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

    image_cache = {}

    requests = []
    total_rows = len(df)
    target_rows = 0
    
    # 썸네일 제외 통계 (성능 최적화를 위한 요약 로그용)
    thumbnail_exclude_count = 0
    thumbnail_exclude_logs = []  # 처음 몇 개만 저장 (디버깅용)
    
    # 중복 요청 방지: custom_id 추적
    seen_custom_ids = set()
    duplicate_count = 0

    # 먼저 전체 대상 요청 수를 계산 (버킷 수 결정용)
    for idx, row in df.iterrows():
        existing_json = safe_str(row.get("ST2_JSON", ""))
        existing_json_clean = existing_json.strip().lower() if existing_json else ""
        if not (skip_filled and existing_json_clean and existing_json_clean not in ("", "nan", "none", "null")):
            target_rows += 1

    # 버킷 수를 미리 계산 (모든 요청에 동일하게 적용)
    if CACHE_MODE and target_rows > 0:
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
        min_buckets = max(1, (target_rows + 9) // 10)  # 분당 10건 기준 (추정치, 공식 기준 아님)
        # 최대 500개 버킷으로 확대 (대량 배치 대응: 5000~10000개 요청)
        PROMPT_CACHE_BUCKETS = min(500, max(1, min_buckets))
        
        avg_per_bucket = target_rows // PROMPT_CACHE_BUCKETS if PROMPT_CACHE_BUCKETS > 0 else target_rows
        log(f"[INFO] 프롬프트 캐싱 버킷 수: {PROMPT_CACHE_BUCKETS}개 (예상 요청 수: {target_rows}개, 각 버킷당 평균 ~{avg_per_bucket}건)")
        log(f"[INFO] ⚠️ 참고: 버킷 분배는 OpenAI 공식 기준이 아닌 추정치입니다. Batch API는 24시간에 걸쳐 처리되므로 실제 overflow 가능성은 낮습니다.")
        if PROMPT_CACHE_BUCKETS >= 500:
            log(f"[INFO] ⚠️ 버킷 수가 최대치(500개)에 도달했습니다. 대량 배치의 경우 overflow 가능성이 있습니다.")
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

        image_nodes = build_image_nodes_from_paths(
            image_paths,
            log_func=log,
            cache=image_cache,
            allow_url=allow_url,
            max_image_height=max_image_height,
            jpeg_quality=jpeg_quality,
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
        
        custom_id = f"row-{idx}"
        
        # Prompt Caching 최적화 (캐싱 모드일 때만)
        if CACHE_MODE:
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
        
        requests.append(
            {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/responses",
                "body": body,
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

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for item in requests:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

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


# =========================================================
# Batch 생성 / 상태 조회 / 결과 다운로드
# =========================================================

def create_batch_from_jsonl(
    client: OpenAI,
    jsonl_path: str,
    excel_path: str,
    model_name: str,
    log_func=None,
    completion_window: str = "24h",
):
    def log(msg: str):
        if log_func:
            log_func(msg)
        else:
            print(msg)

    if not os.path.exists(jsonl_path):
        raise FileNotFoundError(f"입력 JSONL 파일을 찾을 수 없습니다: {jsonl_path}")

    with open(jsonl_path, "rb") as f:
        up_file = client.files.create(file=f, purpose="batch")

    meta = {
        "stage": "stage2_batch",
        "src_excel": os.path.basename(excel_path),
        "model": model_name,
    }

    batch = client.batches.create(
        input_file_id=up_file.id,
        endpoint="/v1/responses",
        completion_window=completion_window,
        metadata=meta,
    )

    log(
        f"[BATCH] 생성 완료: id={batch.id}, "
        f"input_file_id={batch.input_file_id}, status={batch.status}"
    )
    return batch


def download_batch_output_if_ready(
    client: OpenAI,
    batch_id: str,
    output_jsonl_path: str,
    log_func=None,
):
    """
    Batch 상태를 조회하고,
    - status가 completed 또는 expired이고 output_file_id가 있으면 JSONL 다운로드 후 True, status 반환
    - 그 외의 경우 False, 현재 status 반환
    
    Note: expired 상태에서도 output_file_id가 있으면 다운로드 가능 (배치 만료 후에도 결과 파일은 일정 기간 유지됨)
    """
    def log(msg: str):
        if log_func:
            log_func(msg)
        else:
            print(msg)

    batch = client.batches.retrieve(batch_id)
    log(
        f"[STATUS] batch_id={batch.id}, status={batch.status}, "
        f"requests={getattr(batch, 'request_counts', 'N/A')}"
    )

    # completed 또는 expired 상태에서 output_file_id가 있으면 다운로드 시도
    output_file_id = getattr(batch, "output_file_id", None)
    
    if batch.status == "completed":
        if not output_file_id:
            log("[ERROR] 완료 상태지만 output_file_id 를 찾을 수 없습니다.")
            return False, batch.status
    elif batch.status == "expired":
        if not output_file_id:
            log("[WARN] 만료된 배치이며 output_file_id 를 찾을 수 없습니다. 수동으로 다운로드해주세요.")
            return False, batch.status
        else:
            log("[INFO] 배치가 만료되었지만 output_file_id가 있어 다운로드를 시도합니다.")
    else:
        log(f"[INFO] 아직 완료되지 않았습니다. (status={batch.status}) 나중에 다시 조회하세요.")
        return False, batch.status

    # output_file_id가 있으면 다운로드 시도
    try:
        file_content = client.files.content(output_file_id)
        try:
            with open(output_jsonl_path, "wb") as f:
                f.write(file_content.read())
        except AttributeError:
            with open(output_jsonl_path, "wb") as f:
                f.write(file_content)
        
        log(f"[DOWNLOAD] Batch 결과 JSONL 저장 완료: {output_jsonl_path}")
        return True, batch.status
    except Exception as e:
        log(f"[ERROR] 파일 다운로드 실패: {e}")
        if batch.status == "expired":
            log("[INFO] 만료된 배치의 파일 다운로드가 실패했습니다. OpenAI 웹사이트에서 수동으로 다운로드해주세요.")
        return False, batch.status


# =========================================================
# Batch 결과 JSONL → 엑셀 병합 (ST2_JSON)
# =========================================================

def merge_batch_output_to_excel(
    excel_path: str,
    output_jsonl_path: str,
    model_name: str,
    skip_filled: bool,
    log_func=None,
):
    def log(msg: str):
        if log_func:
            log_func(msg)
        else:
            print(msg)

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일을 찾을 수 없습니다: {excel_path}")
    if not os.path.exists(output_jsonl_path):
        raise FileNotFoundError(f"Batch 결과 JSONL 파일을 찾을 수 없습니다: {output_jsonl_path}")

    result_map = {}
    total_in_tok = 0
    total_out_tok = 0
    total_reasoning_tok = 0

    with open(output_jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            custom_id = safe_str(obj.get("custom_id", ""))
            if not custom_id:
                continue

            error = obj.get("error")
            if error:
                log(f"[WARN] custom_id={custom_id} 에 에러 존재: {error}")
                continue

            resp_wrapper = obj.get("response")
            if not isinstance(resp_wrapper, dict):
                log(f"[WARN] custom_id={custom_id}: response 객체 없음")
                continue

            resp_dict = resp_wrapper.get("body")
            if not isinstance(resp_dict, dict):
                log(f"[WARN] custom_id={custom_id}: response.body 가 dict 아님")
                continue

            try:
                text = extract_text_from_response_dict(resp_dict)
            except Exception as e:
                log(f"[WARN] custom_id={custom_id}: 텍스트 추출 실패 ({e})")
                continue

            in_tok, out_tok, r_tok = extract_usage_tokens_from_response_dict(resp_dict)
            result_map[custom_id] = {
                "text": text,
                "in_tok": in_tok,
                "out_tok": out_tok,
                "reasoning_tok": r_tok,
            }

            total_in_tok += in_tok
            total_out_tok += out_tok
            total_reasoning_tok += r_tok

    log(f"[INFO] Batch 결과 개수: {len(result_map)}")

    df = pd.read_excel(excel_path)
    if "ST2_JSON" not in df.columns:
        df["ST2_JSON"] = ""

    total_rows = len(df)
    merged = 0
    skipped = 0
    missing = 0

    for idx in range(total_rows):
        # custom_id 형식: "row-{idx}" 또는 "row-{idx}-{resize_mode}" (예: "row-0-C", "row-1-B")
        # resize_mode가 포함된 경우에도 매칭할 수 있도록 처리
        custom_id_base = f"row-{idx}"
        
        # 먼저 기본 형식으로 매칭 시도
        row_result = result_map.get(custom_id_base)
        
        # 기본 형식이 없으면 resize_mode가 포함된 형식으로 매칭 시도
        # 정규표현식으로 "row-{idx}" 또는 "row-{idx}-{문자}" 형식 매칭
        if not row_result:
            pattern = re.compile(rf"^row-{idx}(?:-[A-Za-z0-9]+)?$")
            for key in result_map.keys():
                if pattern.match(key):
                    row_result = result_map.get(key)
                    if row_result:
                        log(f"[INFO] custom_id 매칭: {custom_id_base} → {key}")
                        break
        
        if not row_result:
            missing += 1
            # 디버깅: 매칭 실패한 경우 사용 가능한 키 확인 (처음 5개만)
            if missing <= 5:
                available_keys = [k for k in result_map.keys() if k.startswith("row-")][:5]
                log(f"[WARN] custom_id={custom_id_base} 매칭 실패. 사용 가능한 키 예시: {available_keys}")
            continue

        existing_json = safe_str(df.at[idx, "ST2_JSON"])
        if skip_filled and existing_json:
            skipped += 1
            continue

        # JSON 파싱 및 meta 강제 덮어쓰기 (B안: 완전무결)
        try:
            parsed_json = json.loads(row_result["text"])
            
            # 원본 엑셀에서 meta 정보 추출
            row = df.iloc[idx]
            original_meta = {
                "기본상품명": safe_str(row.get("ST1_정제상품명", "") or row.get("원본상품명", "")),
                "판매형태": safe_str(row.get("판매형태", "")),
                "옵션_원본": safe_str(row.get("옵션1값", "")),
                "카테고리_경로": safe_str(row.get("카테고리명", ""))
            }
            
            # 판매형태가 없거나 유효하지 않으면 추론
            if original_meta["판매형태"] not in ("단품형", "옵션형"):
                original_meta["판매형태"] = infer_sale_type(
                    original_meta["옵션_원본"],
                    original_meta["기본상품명"]
                )
            
            # meta 강제 덮어쓰기 (LLM 실수 제거)
            if isinstance(parsed_json, dict):
                parsed_json["meta"] = original_meta
                # 덮어쓰기 후 다시 JSON 문자열로 변환
                df.at[idx, "ST2_JSON"] = json.dumps(parsed_json, ensure_ascii=False)
            else:
                # 파싱 실패 시 원본 텍스트 그대로 사용
                df.at[idx, "ST2_JSON"] = row_result["text"]
        except (json.JSONDecodeError, Exception) as e:
            # JSON 파싱 실패 시 원본 텍스트 그대로 사용
            log(f"[WARN] idx={idx}: JSON 파싱 실패, meta 덮어쓰기 건너뜀 ({e})")
            df.at[idx, "ST2_JSON"] = row_result["text"]
        
        merged += 1

    base_dir = os.path.dirname(excel_path)
    base_name, ext = os.path.splitext(os.path.basename(excel_path))
    out_path = os.path.join(base_dir, f"{base_name}_stage2_batch_완료{ext}")

    try:
        df.to_excel(out_path, index=False)
        log(f"[DONE] 병합된 엑셀 저장 완료: {out_path}")
    except Exception as e:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(base_dir, f"{base_name}_stage2_batch_완료_{ts}{ext}")
        df.to_excel(backup_path, index=False)
        log(f"[WARN] 기본 경로 저장 실패({e}) → 백업 파일로 저장: {backup_path}")
        out_path = backup_path

    log(
        f"[SUMMARY] 총행={total_rows}, 병합성공={merged}, "
        f"기존값으로 스킵={skipped}, 결과없음(미매칭)={missing}"
    )

    total_tokens = total_in_tok + total_out_tok + total_reasoning_tok
    log(
        f"[TOKENS] input={total_in_tok}, output={total_out_tok}, "
        f"reasoning={total_reasoning_tok}, total={total_tokens}"
    )

    input_cost_usd, output_cost_usd, total_cost_usd = calc_costs_usd(
        model_name, total_in_tok, total_out_tok, total_reasoning_tok
    )

    if total_cost_usd is not None:
        log(
            "[COST] model={}  input=${:.6f}, output=${:.6f}, total=${:.6f}".format(
                model_name,
                input_cost_usd,
                output_cost_usd,
                total_cost_usd,
            )
        )
    else:
        log(f"[COST] 모델 '{model_name}' 에 대한 가격 정보가 없어 비용 계산 생략.")

    return {
        "out_excel_path": out_path,
        "total_rows": total_rows,
        "merged": merged,
        "skipped": skipped,
        "missing": missing,
        "total_in_tok": total_in_tok,
        "total_out_tok": total_out_tok,
        "total_reasoning_tok": total_reasoning_tok,
        "input_cost_usd": input_cost_usd,
        "output_cost_usd": output_cost_usd,
        "total_cost_usd": total_cost_usd,
    }


# =========================================================
# Tkinter GUI
# =========================================================

class Stage2BatchGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("스테이지2-B BatchAPI 실행기 (엑셀+이미지 → Batch → ST2_JSON)")

        # 화면 크기 계산 (작은 노트북도 고려)
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()

        base_w = min(1300, max(1150, screen_w - 120))
        base_h = min(900, max(650, screen_h - 120))

        self.geometry(f"{base_w}x{base_h}")
        min_w = min(1150, max(1000, screen_w - 80))
        min_h = min(700, max(620, screen_h - 80))
        self.minsize(min_w, min_h)

        # 컬러 팔레트 (파스텔 톤)
        colors = {
            "bg": "#f6f7fb",
            "panel": "#ffffff",
            "header": "#e8f0ff",
            "border": "#d0d7e2",
            "accent": "#4f7cff",
            "accent_dark": "#3557d6",
        }
        self._colors = colors
        self.configure(bg=colors["bg"])

        # ttk 스타일 설정
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("Main.TFrame", background=colors["bg"])
        style.configure("Inner.TFrame", background=colors["panel"])

        style.configure(
            "TLabelframe",
            background=colors["bg"],
            bordercolor=colors["border"],
            relief="groove",
        )
        style.configure(
            "TLabelframe.Label",
            background=colors["header"],
            foreground="#111827",
            padding=(6, 2, 6, 2),
        )
        style.configure("TLabel", background=colors["bg"])

        style.configure(
            "Treeview",
            background="white",
            fieldbackground="white",
            rowheight=22,
            bordercolor=colors["border"],
        )
        style.map(
            "Treeview",
            background=[("selected", colors["accent"])],
            foreground=[("selected", "white")],
        )

        style.configure("TButton", padding=(8, 2))

        style.configure(
            "Big.Horizontal.TProgressbar",
            thickness=18,
            troughcolor=colors["panel"],
            bordercolor=colors["border"],
            background=colors["accent"],
        )

        style.configure(
            "Accent.TButton",
            padding=(12, 4),
            background=colors["accent"],
            foreground="white",
            relief="flat",
            borderwidth=0,
        )
        style.map(
            "Accent.TButton",
            background=[
                ("active", colors["accent_dark"]),
                ("disabled", "#a5b4fc"),
            ],
            foreground=[("disabled", "#e5e7eb")],
        )

        # 상태 변수들
        self.api_key_var = tk.StringVar(value=load_api_key_from_file())
        self.model_var = tk.StringVar(value="gpt-5-mini")
        self.effort_var = tk.StringVar(value="medium")  # none / low / medium / high

        self.src_excel_var = tk.StringVar(value="")
        self.batch_input_jsonl_var = tk.StringVar(value="")
        self.batch_output_jsonl_var = tk.StringVar(value="")
        self.batch_id_var = tk.StringVar(value="")

        self.skip_filled_var = tk.BooleanVar(value=True)
        self.use_thumbnail_var = tk.BooleanVar(value=True)
        self.allow_url_var = tk.BooleanVar(value=False)

        self.progress_var = tk.DoubleVar(value=0.0)
        self.progress_text_var = tk.StringVar(value="진행률: 0%")

        self.token_info_var = tk.StringVar(value="토큰: -")
        self.cost_info_var = tk.StringVar(value="비용: -")

        self.batch_jobs = []          # 메모리 상의 배치 작업 목록
        self.batch_tree = None        # 메인 목록 Treeview

        # 보관함(Archive) 창 관련
        self.archive_window = None    # tk.Toplevel
        self.archive_tree = None      # ttk.Treeview

        main = ttk.Frame(self, style="Main.TFrame")
        main.pack(fill="both", expand=True)

        self._build_widgets(main, colors["bg"])

    # ---------- UI ----------
    def _build_widgets(self, main: ttk.Frame, BG_MAIN: str):
        colors = self._colors

        # API 설정
        frame_api_outer = ttk.LabelFrame(main, text="OpenAI API 설정 (Stage2 배치)")
        frame_api_outer.pack(fill="x", padx=10, pady=(10, 5))

        frame_api = ttk.Frame(frame_api_outer, style="Inner.TFrame")
        frame_api.pack(fill="x", padx=8, pady=6)

        ttk.Label(frame_api, text="API Key:").grid(row=0, column=0, sticky="w", padx=5, pady=3)
        entry_key = ttk.Entry(frame_api, textvariable=self.api_key_var, width=50, show="*")
        entry_key.grid(row=0, column=1, sticky="we", padx=5, pady=3)
        frame_api.columnconfigure(1, weight=1)

        btn_save = ttk.Button(frame_api, text="키 저장", command=self.on_save_api_key)
        btn_save.grid(row=0, column=2, sticky="w", padx=5, pady=3)

        ttk.Label(frame_api, text="모델:").grid(row=1, column=0, sticky="w", padx=5, pady=3)
        combo_model = ttk.Combobox(
            frame_api,
            textvariable=self.model_var,
            values=["gpt-5", "gpt-5-mini", "gpt-5-nano"],
            state="readonly",
            width=15,
        )
        combo_model.grid(row=1, column=1, sticky="w", padx=5, pady=3)

        ttk.Label(frame_api, text="Reasoning Effort:").grid(row=2, column=0, sticky="w", padx=5, pady=3)
        combo_effort = ttk.Combobox(
            frame_api,
            textvariable=self.effort_var,
            values=["none", "low", "medium", "high"],
            state="readonly",
            width=15,
        )
        combo_effort.grid(row=2, column=1, sticky="w", padx=5, pady=3)

        ToolTip(
            combo_model,
            "사용할 OpenAI 모델을 선택합니다.\n\n"
            "- 가성비: gpt-5-mini (기본값)\n"
            "- 최고 품질: gpt-5\n"
            "- 아주 가벼운 테스트: gpt-5-nano"
        )
        ToolTip(
            combo_effort,
            "추론 강도(Reasoning Effort)를 선택합니다.\n\n"
            "- none: 추론 없음 (가장 저렴)\n"
            "- low: 기본 추론(추천)\n"
            "- medium, high: 더 깊은 추론 (비용 증가)"
        )
        ToolTip(
            btn_save,
            "현재 입력한 API Key를 stage2_batch_api 전용 파일에 저장합니다.\n"
            "한 번 저장해두면 다음 실행 시 자동으로 불러옵니다."
        )

        # 엑셀 & 옵션
        frame_excel_outer = ttk.LabelFrame(main, text="입력 엑셀 및 옵션")
        frame_excel_outer.pack(fill="x", padx=10, pady=5)

        frame_excel = ttk.Frame(frame_excel_outer, style="Inner.TFrame")
        frame_excel.pack(fill="x", padx=8, pady=6)

        ttk.Label(frame_excel, text="Stage2 엑셀 파일:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        entry_excel = ttk.Entry(frame_excel, textvariable=self.src_excel_var, width=70)
        entry_excel.grid(row=0, column=1, sticky="we", padx=5, pady=5)
        frame_excel.columnconfigure(1, weight=1)

        btn_browse_excel = ttk.Button(frame_excel, text="찾기...", command=self.on_browse_excel)
        btn_browse_excel.grid(row=0, column=2, sticky="w", padx=5, pady=5)

        chk_skip = ttk.Checkbutton(
            frame_excel,
            text="이미 ST2_JSON 값이 있는 행은 건너뛰기",
            variable=self.skip_filled_var,
        )
        chk_skip.grid(row=1, column=1, sticky="w", padx=5, pady=2)

        chk_thumb = ttk.Checkbutton(
            frame_excel,
            text="대표 썸네일(이미지대)도 이미지로 사용",
            variable=self.use_thumbnail_var,
        )
        chk_thumb.grid(row=2, column=1, sticky="w", padx=5, pady=2)

        chk_url = ttk.Checkbutton(
            frame_excel,
            text="URL 이미지 사용 허용 (http/https)",
            variable=self.allow_url_var,
        )
        chk_url.grid(row=3, column=1, sticky="w", padx=5, pady=2)

        # JSONL / Batch 설정
        frame_batch_outer = ttk.LabelFrame(main, text="Batch 설정")
        frame_batch_outer.pack(fill="x", padx=10, pady=5)

        frame_batch = ttk.Frame(frame_batch_outer, style="Inner.TFrame")
        frame_batch.pack(fill="x", padx=8, pady=6)

        ttk.Label(frame_batch, text="입력 JSONL(요청):").grid(row=0, column=0, sticky="w", padx=5, pady=3)
        entry_in_jsonl = ttk.Entry(frame_batch, textvariable=self.batch_input_jsonl_var, width=70)
        entry_in_jsonl.grid(row=0, column=1, sticky="we", padx=5, pady=3)
        frame_batch.columnconfigure(1, weight=1)

        btn_in_jsonl = ttk.Button(frame_batch, text="찾기...", command=self.on_browse_input_jsonl)
        btn_in_jsonl.grid(row=0, column=2, sticky="w", padx=5, pady=3)

        ttk.Label(frame_batch, text="출력 JSONL(응답):").grid(row=1, column=0, sticky="w", padx=5, pady=3)
        entry_out_jsonl = ttk.Entry(frame_batch, textvariable=self.batch_output_jsonl_var, width=70)
        entry_out_jsonl.grid(row=1, column=1, sticky="we", padx=5, pady=3)
        btn_out_jsonl = ttk.Button(frame_batch, text="찾기...", command=self.on_browse_output_jsonl)
        btn_out_jsonl.grid(row=1, column=2, sticky="w", padx=5, pady=3)

        ttk.Label(frame_batch, text="Batch ID:").grid(row=2, column=0, sticky="w", padx=5, pady=3)
        entry_batch_id = ttk.Entry(frame_batch, textvariable=self.batch_id_var, width=40)
        entry_batch_id.grid(row=2, column=1, sticky="w", padx=5, pady=3)

        # 단계 버튼
        frame_steps_outer = ttk.Frame(main, style="Main.TFrame")
        frame_steps_outer.pack(fill="x", padx=10, pady=5)

        frame_steps = ttk.Frame(frame_steps_outer, style="Inner.TFrame")
        frame_steps.pack(fill="x", padx=8, pady=6)

        btn_step1 = ttk.Button(
            frame_steps,
            text="1단계: 엑셀 → 요청 JSONL 생성",
            command=self.on_step1_create_jsonl,
            style="Accent.TButton",
        )
        btn_step1.pack(side="left", padx=5, pady=3, expand=True, fill="x")

        btn_step2 = ttk.Button(
            frame_steps,
            text="2단계: Batch 생성",
            command=self.on_step2_create_batch,
            style="Accent.TButton",
        )
        btn_step2.pack(side="left", padx=5, pady=3, expand=True, fill="x")

        btn_step3 = ttk.Button(
            frame_steps,
            text="3단계: 상태 조회 / 결과 다운로드",
            command=self.on_step3_check_and_download,
            style="Accent.TButton",
        )
        btn_step3.pack(side="left", padx=5, pady=3, expand=True, fill="x")

        btn_step4 = ttk.Button(
            frame_steps,
            text="4단계: 응답 JSONL → 엑셀 병합",
            command=self.on_step4_merge_to_excel,
            style="Accent.TButton",
        )
        btn_step4.pack(side="left", padx=5, pady=3, expand=True, fill="x")

        # 위/아래 분할 (배치 목록 / 진행+로그)
        splitter = tk.PanedWindow(
            self,
            orient="vertical",
            sashwidth=5,
            sashrelief="raised",
            bd=0,
            bg=colors["bg"],
        )
        splitter.pack(fill="both", expand=True, padx=10, pady=5)

        top_area = ttk.Frame(splitter, style="Main.TFrame")
        bottom_area = ttk.Frame(splitter, style="Main.TFrame")

        splitter.add(top_area, minsize=200)
        splitter.add(bottom_area, minsize=220)

        # 배치 작업 목록
        frame_jobs = ttk.LabelFrame(top_area, text="배치 작업 목록 (여러 파일 동시 관리)")
        frame_jobs.pack(fill="both", expand=True, padx=0, pady=(0, 4))

        columns = (
            "batch_id",
            "status",
            "model",
            "effort",
            "src_excel",
            "jsonl",
            "output_jsonl",
            "out_excel",
            "created_at",
            "completed_at",
        )
        self.batch_tree = ttk.Treeview(
            frame_jobs,
            columns=columns,
            show="headings",
            height=5,
        )
        self.batch_tree.grid(row=0, column=0, columnspan=6, sticky="nsew")

        scrollbar = ttk.Scrollbar(
            frame_jobs,
            orient="vertical",
            command=self.batch_tree.yview,
        )
        self.batch_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=0, column=6, sticky="ns")

        frame_jobs.rowconfigure(0, weight=1)
        frame_jobs.columnconfigure(0, weight=1)

        self.batch_tree.heading("batch_id", text="Batch ID")
        self.batch_tree.heading("status", text="상태")
        self.batch_tree.heading("model", text="모델")
        self.batch_tree.heading("effort", text="Effort")
        self.batch_tree.heading("src_excel", text="원본 엑셀")
        self.batch_tree.heading("jsonl", text="입력 JSONL")
        self.batch_tree.heading("output_jsonl", text="출력 JSONL")
        self.batch_tree.heading("out_excel", text="결과 엑셀")
        self.batch_tree.heading("created_at", text="생성 시각")
        self.batch_tree.heading("completed_at", text="완료 시각")

        self.batch_tree.column("batch_id", width=170, anchor="w")
        self.batch_tree.column("status", width=80, anchor="center")
        self.batch_tree.column("model", width=110, anchor="w")
        self.batch_tree.column("effort", width=80, anchor="center")
        self.batch_tree.column("src_excel", width=150, anchor="w")
        self.batch_tree.column("jsonl", width=140, anchor="w")
        self.batch_tree.column("output_jsonl", width=140, anchor="w")
        self.batch_tree.column("out_excel", width=150, anchor="w")
        self.batch_tree.column("created_at", width=130, anchor="w")
        self.batch_tree.column("completed_at", width=130, anchor="w")

        btn_jobs_reload = ttk.Button(
            frame_jobs,
            text="목록 새로고침",
            command=self.on_reload_jobs,
        )
        btn_jobs_reload.grid(row=1, column=0, sticky="w", padx=5, pady=3)

        btn_jobs_apply = ttk.Button(
            frame_jobs,
            text="선택 행 ①~④에 반영",
            command=self.on_apply_job_selection,
        )
        btn_jobs_apply.grid(row=1, column=1, sticky="w", padx=5, pady=3)

        btn_jobs_step3 = ttk.Button(
            frame_jobs,
            text="선택 Batch들 3-4단계 실행",
            command=self.on_step3_for_selected_jobs,
        )
        btn_jobs_step3.grid(row=1, column=2, sticky="w", padx=5, pady=3)

        btn_open_folder = ttk.Button(
            frame_jobs,
            text="선택 작업 폴더 열기",
            command=self.on_open_selected_job_folder,
        )
        btn_open_folder.grid(row=1, column=3, sticky="w", padx=5, pady=3)

        btn_jobs_archive = ttk.Button(
            frame_jobs,
            text="선택 배치 보관함으로 이동",
            command=self.on_archive_selected_jobs,
        )
        btn_jobs_archive.grid(row=1, column=4, sticky="w", padx=5, pady=3)

        btn_jobs_archive_open = ttk.Button(
            frame_jobs,
            text="보관함 열기",
            command=self.on_open_archive_window,
        )
        btn_jobs_archive_open.grid(row=1, column=5, sticky="w", padx=5, pady=3)

        self.batch_tree.bind("<Double-1>", lambda e: self.on_apply_job_selection())

        # 진행 상태 / 로그
        frame_progress = ttk.LabelFrame(bottom_area, text="진행 상태")
        frame_progress.pack(fill="x", padx=0, pady=(0, 4))

        self.progress_bar = ttk.Progressbar(
            frame_progress,
            variable=self.progress_var,
            maximum=100.0,
            mode="determinate",
            style="Big.Horizontal.TProgressbar",
        )
        self.progress_bar.grid(row=0, column=0, columnspan=2, sticky="we", padx=10, pady=5)

        lbl_progress = ttk.Label(frame_progress, textvariable=self.progress_text_var)
        lbl_progress.grid(row=1, column=0, sticky="w", padx=10, pady=2)

        lbl_tokens = ttk.Label(frame_progress, textvariable=self.token_info_var)
        lbl_tokens.grid(row=2, column=0, columnspan=2, sticky="w", padx=10, pady=2)

        lbl_cost = ttk.Label(frame_progress, textvariable=self.cost_info_var)
        lbl_cost.grid(row=3, column=0, columnspan=2, sticky="w", padx=10, pady=2)

        frame_log = ttk.LabelFrame(bottom_area, text="로그")
        frame_log.pack(fill="both", expand=True, padx=0, pady=(0, 5))

        self.log_widget = ScrolledText(frame_log, height=10)
        self.log_widget.pack(fill="both", expand=True, padx=5, pady=5)
        self.log_widget.configure(bg="white", fg="#111827")

        # 초기 배치 목록 로드 + 분할 위치 조정
        self._load_jobs_for_current_excel()

        def _init_sash():
            try:
                splitter.update_idletasks()
                total_h = splitter.winfo_height()
                # 위: 목록, 아래: 진행+로그 — 로그가 가려지지 않도록 56% 정도로 분할
                splitter.sash_place(0, 0, int(total_h * 0.56))
            except Exception:
                pass

        self.after(200, _init_sash)

    # ---------- 유틸 ----------
    def append_log(self, msg: str):
        self.log_widget.insert("end", msg + "\n")
        self.log_widget.see("end")

    def set_progress(self, ratio: float):
        pct = max(0.0, min(100.0, ratio * 100.0))
        self.progress_var.set(pct)
        self.progress_text_var.set(f"진행률: {pct:.1f}%")
        self.update_idletasks()

    def _update_default_paths_for_excel(self, excel_path: str):
        """
        새 엑셀을 선택할 때마다
        - 입력 JSONL: <엑셀명>_stage2_batch_input.jsonl
        - 출력 JSONL: <엑셀명>_stage2_batch_output.jsonl
        로 자동 세팅.
        """
        if not excel_path:
            return
        base_dir = os.path.dirname(excel_path)
        base_name, _ = os.path.splitext(os.path.basename(excel_path))
        in_jsonl = os.path.join(base_dir, f"{base_name}_stage2_batch_input.jsonl")
        out_jsonl = os.path.join(base_dir, f"{base_name}_stage2_batch_output.jsonl")
        self.batch_input_jsonl_var.set(in_jsonl)
        self.batch_output_jsonl_var.set(out_jsonl)
        self.batch_id_var.set("")

    # ---------- 배치 작업 목록 / 보관함 ----------
    def _load_jobs_for_current_excel(self):
        """
        stage2_batch_jobs.json 전체를 self.batch_jobs 에 로딩하고
        - 특정 엑셀 선택 시: 해당 src_excel 만 필터링
        - 미선택 시: 전체 표시
        """
        try:
            all_jobs = load_all_batch_jobs(log_func=self.append_log)
        except Exception as e:
            self.append_log(f"[ERROR] 배치 작업 이력 로드 중 예외 발생: {e}")
            all_jobs = []

        current_excel = self.src_excel_var.get().strip()
        if current_excel:
            jobs = [job for job in all_jobs if job.get("src_excel") == current_excel]
        else:
            jobs = all_jobs

        self.batch_jobs = jobs
        self._refresh_job_tree()
        self._sync_archive_window_when_exists()

    def _refresh_job_tree(self):
        """self.batch_jobs 내용을 Treeview에 그려넣기 (archived=False 만)."""
        if self.batch_tree is None:
            return

        self.batch_tree.delete(*self.batch_tree.get_children())

        def sort_key(job):
            return job.get("created_at") or ""

        for job in sorted(self.batch_jobs, key=sort_key, reverse=True):
            if job.get("archived"):
                continue

            batch_id = job.get("batch_id", "")
            status = job.get("status", "")
            model = job.get("model", "")
            effort = job.get("effort", "")
            src_excel = job.get("src_excel", "")
            jsonl_path = job.get("jsonl_path", "")
            output_jsonl = job.get("output_jsonl", "")
            out_excel = job.get("out_excel", "")
            created_at = job.get("created_at", "")
            completed_at = job.get("completed_at", "")

            self.batch_tree.insert(
                "",
                "end",
                iid=batch_id,
                values=(
                    batch_id,
                    status,
                    model,
                    effort,
                    os.path.basename(src_excel) if src_excel else "",
                    os.path.basename(jsonl_path) if jsonl_path else "",
                    os.path.basename(output_jsonl) if output_jsonl else "",
                    os.path.basename(out_excel) if out_excel else "",
                    created_at,
                    completed_at or "-",
                ),
            )

    def _ensure_archive_window(self):
        """보관함(Archive) 창이 없으면 새로 띄우고, 있으면 앞으로 가져오기."""
        if self.archive_window is not None:
            try:
                if self.archive_window.winfo_exists():
                    self.archive_window.lift()
                    self.archive_window.focus_force()
                    return
            except tk.TclError:
                self.archive_window = None
                self.archive_tree = None

        win = tk.Toplevel(self)
        win.title("삭제된 배치(보관함) - Stage2")
        win.geometry("1000x420")

        self.archive_window = win

        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True, padx=10, pady=10)

        columns = ("batch_id", "status", "model", "effort", "src_excel", "out_excel", "created_at", "completed_at")
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=12)
        self.archive_tree = tree

        tree.heading("batch_id", text="Batch ID")
        tree.heading("status", text="상태")
        tree.heading("model", text="모델")
        tree.heading("effort", text="Effort")
        tree.heading("src_excel", text="원본 엑셀")
        tree.heading("out_excel", text="결과 엑셀")
        tree.heading("created_at", text="생성 시각")
        tree.heading("completed_at", text="완료 시각")

        tree.column("batch_id", width=160, anchor="w")
        tree.column("status", width=70, anchor="w")
        tree.column("model", width=90, anchor="w")
        tree.column("effort", width=80, anchor="w")
        tree.column("src_excel", width=200, anchor="w")
        tree.column("out_excel", width=220, anchor="w")
        tree.column("created_at", width=120, anchor="w")
        tree.column("completed_at", width=120, anchor="w")


        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill="x", padx=10, pady=(0, 10))

        btn_restore = ttk.Button(
            btn_frame,
            text="선택 배치 복구",
            command=self.on_restore_selected_jobs,
        )
        btn_restore.pack(side="left", padx=5)

        btn_delete = ttk.Button(
            btn_frame,
            text="선택 배치 완전 삭제",
            command=self.on_delete_selected_jobs_permanently,
        )
        btn_delete.pack(side="left", padx=5)

        btn_close = ttk.Button(
            btn_frame,
            text="닫기",
            command=win.destroy,
        )
        btn_close.pack(side="right", padx=5)

        def _on_close():
            self.archive_window = None
            self.archive_tree = None
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", _on_close)

        self._refresh_archive_tree()

    def _refresh_archive_tree(self):
        """보관함 Treeview(archive_tree) 내용 갱신."""
        if self.archive_tree is None:
            return

        jobs = load_all_batch_jobs(log_func=self.append_log)
        tree = self.archive_tree

        for item in tree.get_children():
            tree.delete(item)

        # created_at 기준 오름차순 정렬
        jobs = sorted(
            jobs,
            key=lambda j: (j.get("created_at") or "", j.get("batch_id") or ""),
        )

        for job in jobs:
            if not job.get("archived"):
                continue
            batch_id = job.get("batch_id", "")
            status = job.get("status", "")
            model = job.get("model", "")
            effort = job.get("effort", "")
            src_excel = job.get("src_excel", "")
            out_excel = job.get("out_excel", "")
            created_at = job.get("created_at", "")
            completed_at = job.get("completed_at", "") or "-"

            self.archive_tree.insert(
                "",
                "end",
                iid=batch_id,
                values=(
                    batch_id,
                    status,
                    model,
                    effort,
                    os.path.basename(src_excel) if src_excel else "",
                    os.path.basename(out_excel) if out_excel else "",
                    created_at,
                    completed_at,
                ),
            )


    def _sync_archive_window_when_exists(self):
        """보관함 창이 열려 있으면 그쪽 목록도 같이 갱신."""
        if self.archive_window is None or self.archive_tree is None:
            return
        try:
            if self.archive_window.winfo_exists():
                self._refresh_archive_tree()
            else:
                self.archive_window = None
                self.archive_tree = None
        except tk.TclError:
            self.archive_window = None
            self.archive_tree = None

    def on_reload_jobs(self):
        """배치 작업 목록 새로고침."""
        self._load_jobs_for_current_excel()
        self.append_log("[INFO] 배치 작업 목록을 새로고침했습니다.")

    def on_apply_job_selection(self):
        """선택된 목록(1건)을 위의 ①~④, 모델/effort 입력란으로 되살리기."""
        if self.batch_tree is None:
            return

        selected = self.batch_tree.selection()
        if not selected:
            messagebox.showwarning("경고", "먼저 배치 작업을 하나 선택하세요.")
            return

        batch_id = selected[0]
        job = None
        for j in self.batch_jobs:
            if j.get("batch_id") == batch_id:
                job = j
                break

        if not job:
            messagebox.showwarning("경고", "선택한 배치 작업 정보를 찾을 수 없습니다.")
            return

        src_excel = job.get("src_excel") or ""
        jsonl_path = job.get("jsonl_path") or ""
        output_jsonl = job.get("output_jsonl") or ""
        model = job.get("model") or ""
        effort = job.get("effort") or ""

        if src_excel:
            self.src_excel_var.set(src_excel)
            self._update_default_paths_for_excel(src_excel)
        if jsonl_path:
            self.batch_input_jsonl_var.set(jsonl_path)
        if output_jsonl:
            self.batch_output_jsonl_var.set(output_jsonl)
        self.batch_id_var.set(batch_id)
        if model:
            self.model_var.set(model)
        if effort:
            self.effort_var.set(effort)

        self.append_log(
            f"[INFO] 선택한 배치 작업을 입력란에 반영했습니다. batch_id={batch_id}, src_excel={src_excel}"
        )

    def on_open_selected_job_folder(self):
        """선택된 배치 작업의 결과 폴더 열기."""
        if self.batch_tree is None:
            return
        selected = self.batch_tree.selection()
        if not selected:
            messagebox.showwarning("경고", "먼저 배치 작업을 하나 선택하세요.")
            return

        batch_id = selected[0]
        job = None
        for j in self.batch_jobs:
            if j.get("batch_id") == batch_id:
                job = j
                break
        if not job:
            messagebox.showwarning("경고", "선택한 배치 작업 정보를 찾을 수 없습니다.")
            return

        out_excel = job.get("out_excel") or ""
        src_excel = job.get("src_excel") or ""
        result_dir = job.get("result_dir") or ""

        folder = ""
        if out_excel:
            folder = os.path.dirname(out_excel)
        elif src_excel:
            folder = os.path.dirname(src_excel)
        elif result_dir:
            folder = result_dir

        if not folder:
            messagebox.showwarning("경고", "열 수 있는 폴더 정보가 없습니다.")
            return

        open_folder(folder, log_func=self.append_log)

    def on_archive_selected_jobs(self):
        """
        메인 목록에서 선택된 배치 작업을 '보관함'으로 이동.
        (JSON에서는 archived=True로 표시만 바꾸고 실제 OpenAI Batch 는 삭제하지 않음)
        """
        tree = self.batch_tree
        if tree is None:
            return

        selected = tree.selection()
        if not selected:
            messagebox.showwarning("경고", "보관함으로 이동할 배치 작업을 먼저 선택하세요.")
            return

        selected_ids = list(selected)

        msg_lines = [
            f"선택된 배치 작업 {len(selected_ids)}개를 보관함으로 이동합니다.",
            "",
            "※ 이 동작은 OpenAI 서버의 실제 Batch 작업을 삭제하지 않습니다.",
            "   - GUI 목록에서만 숨기기 위한 기능입니다.",
            "",
            "계속하시겠습니까?",
        ]
        if not messagebox.askyesno("보관함으로 이동", "\n".join(msg_lines)):
            return

        changed = 0
        for bid in selected_ids:
            for job in self.batch_jobs:
                if job.get("batch_id") == bid:
                    if not job.get("archived", False):
                        job["archived"] = True
                        changed += 1
                    break

        if changed == 0:
            return

        save_all_batch_jobs(self.batch_jobs, log_func=self.append_log)
        self._refresh_job_tree()
        self._sync_archive_window_when_exists()
        self.append_log(f"[INFO] 배치 작업 {changed}개를 보관함으로 이동했습니다.")

    def on_open_archive_window(self):
        """보관함(Archive) 창 열기."""
        self._ensure_archive_window()

    def on_restore_selected_jobs(self):
        """보관함 창에서 선택한 배치 작업 복구."""
        if self.archive_window is None or self.archive_tree is None:
            messagebox.showwarning("경고", "먼저 보관함 창을 열어주세요.")
            return

        try:
            if not self.archive_window.winfo_exists():
                self.archive_window = None
                self.archive_tree = None
                messagebox.showwarning("경고", "보관함 창이 이미 닫혔습니다.")
                return
        except tk.TclError:
            self.archive_window = None
            self.archive_tree = None
            messagebox.showwarning("경고", "보관함 창이 이미 닫혔습니다.")
            return

        selected = self.archive_tree.selection()
        if not selected:
            messagebox.showwarning("경고", "복구할 배치 작업을 먼저 선택하세요.")
            return

        selected_ids = list(selected)

        msg = (
            "선택한 배치 작업을 보관함에서 복구합니다.\n\n"
            "※ 실제 OpenAI Batch 상태에는 영향을 주지 않고,\n"
            "   이 GUI 목록에서만 다시 보이게 합니다.\n\n"
            "계속하시겠습니까?"
        )
        if not messagebox.askyesno("선택 배치 복구", msg):
            return

        changed = 0
        for bid in selected_ids:
            for job in self.batch_jobs:
                if job.get("batch_id") == bid:
                    if job.get("archived"):
                        job["archived"] = False
                        changed += 1
                    break

        if changed == 0:
            return

        save_all_batch_jobs(self.batch_jobs, log_func=self.append_log)
        self._load_jobs_for_current_excel()
        self.append_log(f"[INFO] 보관함에서 배치 작업 {changed}개를 복구했습니다.")

    def on_delete_selected_jobs_permanently(self):
        """보관함에서 선택한 배치 작업을 완전히 삭제 (되돌릴 수 없음)."""
        if self.archive_window is None or self.archive_tree is None:
            messagebox.showwarning("경고", "먼저 보관함 창을 열어주세요.")
            return

        try:
            if not self.archive_window.winfo_exists():
                self.archive_window = None
                self.archive_tree = None
                messagebox.showwarning("경고", "보관함 창이 이미 닫혔습니다.")
                return
        except tk.TclError:
            self.archive_window = None
            self.archive_tree = None
            messagebox.showwarning("경고", "보관함 창이 이미 닫혔습니다.")
            return

        selected = self.archive_tree.selection()
        if not selected:
            messagebox.showwarning("경고", "완전히 삭제할 배치 작업을 먼저 선택하세요.")
            return

        selected_ids = set(selected)

        msg = (
            f"선택된 배치 작업 {len(selected_ids)}개를 보관함에서 완전히 삭제합니다.\n\n"
            "※ 이 작업은 되돌릴 수 없습니다.\n"
            "   OpenAI 서버 상의 Batch 객체는 삭제되지 않지만,\n"
            "   이 프로그램의 작업 이력(stage2_batch_jobs.json)에서는 사라집니다.\n\n"
            "정말로 계속하시겠습니까?"
        )
        if not messagebox.askyesno("선택 배치 완전 삭제", msg):
            return

        before = len(self.batch_jobs)
        self.batch_jobs = [
            job for job in self.batch_jobs
            if job.get("batch_id") not in selected_ids
        ]
        deleted = before - len(self.batch_jobs)

        if deleted <= 0:
            return

        save_all_batch_jobs(self.batch_jobs, log_func=self.append_log)
        self._load_jobs_for_current_excel()
        self.append_log(f"[INFO] 보관함에서 배치 작업 {deleted}개를 완전히 삭제했습니다.")

    # ---------- 이벤트 ----------
    def on_save_api_key(self):
        key = self.api_key_var.get().strip()
        if not key:
            messagebox.showwarning("경고", "API Key를 입력하세요.")
            return
        save_api_key_to_file(key)
        self.append_log("[INFO] API 키 저장 완료.")

    def on_browse_excel(self):
        path = filedialog.askopenfilename(
            title="Stage2 엑셀 선택",
            filetypes=[("Excel files", "*.xlsx;*.xls")],
        )
        if path:
            self.src_excel_var.set(path)
            self._update_default_paths_for_excel(path)
            self._load_jobs_for_current_excel()

    def on_browse_input_jsonl(self):
        path = filedialog.askopenfilename(
            title="입력 JSONL(요청) 선택",
            filetypes=[("JSON Lines", "*.jsonl"), ("All files", "*.*")],
        )
        if path:
            self.batch_input_jsonl_var.set(path)

    def on_browse_output_jsonl(self):
        path = filedialog.askopenfilename(
            title="출력 JSONL(응답) 선택",
            filetypes=[("JSON Lines", "*.jsonl"), ("All files", "*.*")],
        )
        if path:
            self.batch_output_jsonl_var.set(path)

    def _ensure_excel_and_paths(self):
        excel_path = self.src_excel_var.get().strip()
        if not excel_path:
            raise RuntimeError("Stage2 엑셀 파일을 먼저 선택하세요.")
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"엑셀 파일을 찾을 수 없습니다: {excel_path}")
        return excel_path

    def on_step1_create_jsonl(self):
        """엑셀 → Batch 요청 JSONL 생성"""
        try:
            excel_path = self._ensure_excel_and_paths()
        except Exception as e:
            messagebox.showwarning("경고", str(e))
            return

        model_name = self.model_var.get().strip() or "gpt-5-mini"
        effort = self.effort_var.get().strip()

        jsonl_path = self.batch_input_jsonl_var.get().strip()
        if not jsonl_path:
            base_dir = os.path.dirname(excel_path)
            base_name, _ = os.path.splitext(os.path.basename(excel_path))
            jsonl_path = os.path.join(base_dir, f"{base_name}_stage2_batch_input.jsonl")
            self.batch_input_jsonl_var.set(jsonl_path)

        self.append_log(
            f"[RUN] 1단계 시작: 엑셀={excel_path}, jsonl={jsonl_path}, "
            f"model={model_name}, effort={effort}"
        )
        self.set_progress(0.0)

        try:
            info = create_stage2_batch_input_jsonl(
                excel_path=excel_path,
                jsonl_path=jsonl_path,
                model_name=model_name,
                effort=effort,
                skip_filled=self.skip_filled_var.get(),
                use_thumbnail=self.use_thumbnail_var.get(),
                allow_url=self.allow_url_var.get(),
                log_func=self.append_log,
            )
            self.append_log(
                f"[DONE] 1단계 완료: total_rows={info['total_rows']}, "
                f"target_rows={info['target_rows']}, num_requests={info['num_requests']}"
            )
            self.set_progress(1.0)
            messagebox.showinfo(
                "완료",
                f"요청 JSONL 생성 완료.\n\n"
                f"파일: {jsonl_path}\n"
                f"요청 개수: {info['num_requests']}",
            )
        except Exception as e:
            self.append_log(f"[ERROR] 1단계 중 오류: {e}")
            messagebox.showerror("에러", f"1단계 처리 중 오류가 발생했습니다.\n\n{e}")
            self.set_progress(0.0)

    def _build_client(self) -> OpenAI:
        api_key = self.api_key_var.get().strip()
        if not api_key:
            raise RuntimeError("API Key를 먼저 입력하거나 저장하세요.")
        return OpenAI(api_key=api_key)

    def on_step2_create_batch(self):
        """입력 JSONL 업로드 → Batch 생성 + 작업 이력 기록"""
        try:
            excel_path = self._ensure_excel_and_paths()
        except Exception as e:
            messagebox.showwarning("경고", str(e))
            return

        jsonl_path = self.batch_input_jsonl_var.get().strip()
        if not jsonl_path:
            messagebox.showwarning("경고", "입력 JSONL(요청) 파일 경로를 지정하세요.")
            return
        if not os.path.exists(jsonl_path):
            messagebox.showwarning("경고", f"입력 JSONL 파일을 찾을 수 없습니다:\n{jsonl_path}")
            return

        model_name = self.model_var.get().strip() or "gpt-5-mini"
        effort = self.effort_var.get().strip() or "low"

        try:
            client = self._build_client()
        except Exception as e:
            messagebox.showerror("에러", f"OpenAI 클라이언트 생성 실패:\n{e}")
            return

        self.append_log(
            f"[RUN] 2단계: Batch 생성 시작. jsonl={jsonl_path}, model={model_name}, effort={effort}"
        )

        try:
            batch = create_batch_from_jsonl(
                client=client,
                jsonl_path=jsonl_path,
                excel_path=excel_path,
                model_name=model_name,
                log_func=self.append_log,
            )
            self.batch_id_var.set(batch.id)

            upsert_batch_job(
                batch_id=batch.id,
                log_func=self.append_log,
                src_excel=excel_path,
                jsonl_path=jsonl_path,
                out_excel="",
                model=model_name,
                effort=effort,
                created_at=datetime.now().isoformat(),
                status=batch.status,
                completed_at=None,
                output_jsonl="",
                archived=False,
            )

            self._load_jobs_for_current_excel()

            messagebox.showinfo(
                "Batch 생성 완료",
                f"Batch ID: {batch.id}\n"
                f"현재 상태: {batch.status}",
            )
        except Exception as e:
            self.append_log(f"[ERROR] 2단계 중 오류: {e}")
            messagebox.showerror("에러", f"Batch 생성 중 오류가 발생했습니다.\n\n{e}")

    def on_step3_check_and_download(self):
        """
        Batch 상태 조회 후 완료 시 결과 JSONL 다운로드 + 작업 이력 갱신
        - 목록에서 행 선택 시 그 정보를 ①~④에 반영해 사용
        """
        if self.batch_tree is not None:
            selected = self.batch_tree.selection()
            if selected:
                self.on_apply_job_selection()

        batch_id = self.batch_id_var.get().strip()
        if not batch_id:
            messagebox.showwarning("경고", "Batch ID를 먼저 입력하거나 2단계에서 생성하세요.")
            return

        try:
            client = self._build_client()
        except Exception as e:
            messagebox.showerror("에러", f"OpenAI 클라이언트 생성 실패:\n{e}")
            return

        output_jsonl_path = self.batch_output_jsonl_var.get().strip()

        try:
            excel_path = self._ensure_excel_and_paths()
        except Exception:
            excel_path = None

        if not output_jsonl_path:
            if excel_path:
                base_dir = os.path.dirname(excel_path)
                base_name, _ = os.path.splitext(os.path.basename(excel_path))
                output_jsonl_path = os.path.join(base_dir, f"{base_name}_stage2_batch_output.jsonl")
            else:
                output_jsonl_path = f"stage2_batch_output_{batch_id}.jsonl"
            self.batch_output_jsonl_var.set(output_jsonl_path)

        self.append_log(
            f"[RUN] 3단계: Batch 상태 조회 및 결과 다운로드 시도. "
            f"batch_id={batch_id}, output_jsonl={output_jsonl_path}"
        )

        try:
            ok, status = download_batch_output_if_ready(
                client=client,
                batch_id=batch_id,
                output_jsonl_path=output_jsonl_path,
                log_func=self.append_log,
            )

            # 기존 job의 archived 상태를 유지
            jobs = load_all_batch_jobs(log_func=self.append_log)
            archived_flag = False
            for j in jobs:
                if j.get("batch_id") == batch_id:
                    archived_flag = j.get("archived", False)
                    break

            upsert_batch_job(
                batch_id=batch_id,
                log_func=self.append_log,
                status=status,
                output_jsonl=output_jsonl_path if ok else "",
                completed_at=(
                    datetime.now().isoformat()
                    if ok and status == "completed"
                    else None
                ),
                archived=archived_flag,
            )

            self._load_jobs_for_current_excel()

            if ok:
                messagebox.showinfo(
                    "다운로드 완료",
                    f"Batch 결과를 다운로드했습니다.\n\n파일: {output_jsonl_path}",
                )
            else:
                messagebox.showinfo(
                    "진행 중",
                    "Batch가 아직 완료되지 않았습니다.\n"
                    "조금 뒤에 다시 시도하세요.",
                )
        except Exception as e:
            self.append_log(f"[ERROR] 3단계 중 오류: {e}")
            messagebox.showerror("에러", f"Batch 상태 조회/다운로드 중 오류가 발생했습니다.\n\n{e}")

    def on_step4_merge_to_excel(self):
        """Batch 결과 JSONL → 엑셀 ST2_JSON 병합 + 작업 이력 out_excel 갱신"""
        try:
            excel_path = self._ensure_excel_and_paths()
        except Exception as e:
            messagebox.showwarning("경고", str(e))
            return

        output_jsonl_path = self.batch_output_jsonl_var.get().strip()
        if not output_jsonl_path:
            messagebox.showwarning(
                "경고",
                "출력 JSONL(응답) 파일 경로를 먼저 지정하거나 3단계에서 다운로드를 완료하세요.",
            )
            return
        if not os.path.exists(output_jsonl_path):
            messagebox.showwarning("경고", f"출력 JSONL 파일을 찾을 수 없습니다:\n{output_jsonl_path}")
            return

        model_name = self.model_var.get().strip() or "gpt-5-mini"

        self.append_log(
            f"[RUN] 4단계: 응답 JSONL → 엑셀 병합 시작. "
            f"excel={excel_path}, output_jsonl={output_jsonl_path}"
        )
        self.set_progress(0.0)

        try:
            info = merge_batch_output_to_excel(
                excel_path=excel_path,
                output_jsonl_path=output_jsonl_path,
                model_name=model_name,
                skip_filled=self.skip_filled_var.get(),
                log_func=self.append_log,
            )
            self.set_progress(1.0)

            total_tokens = (
                (info["total_in_tok"] or 0)
                + (info["total_out_tok"] or 0)
                + (info["total_reasoning_tok"] or 0)
            )
            self.token_info_var.set(
                f"토큰: input={info['total_in_tok']}, "
                f"output={info['total_out_tok']}, "
                f"reasoning={info['total_reasoning_tok']}, "
                f"total={total_tokens}"
            )

            if info["total_cost_usd"] is not None:
                self.cost_info_var.set(
                    f"비용(USD): input={info['input_cost_usd']:.6f}, "
                    f"output={info['output_cost_usd']:.6f}, "
                    f"total={info['total_cost_usd']:.6f}"
                )
            else:
                self.cost_info_var.set("비용: - (가격 정보 없음)")

            batch_id = self.batch_id_var.get().strip()
            if batch_id:
                jobs = load_all_batch_jobs(log_func=self.append_log)
                archived_flag = False
                for j in jobs:
                    if j.get("batch_id") == batch_id:
                        archived_flag = j.get("archived", False)
                        break

                upsert_batch_job(
                    batch_id=batch_id,
                    log_func=self.append_log,
                    out_excel=info["out_excel_path"],
                    archived=archived_flag,
                )

            self._load_jobs_for_current_excel()

            msg = (
                f"엑셀 병합이 완료되었습니다.\n\n"
                f"저장 파일: {info['out_excel_path']}\n"
                f"병합 성공 행: {info['merged']}\n"
                f"기존값으로 스킵: {info['skipped']}\n"
                f"결과 없음(미매칭): {info['missing']}\n\n"
                f"해당 폴더를 바로 여시겠습니까?"
            )
            if messagebox.askyesno("병합 완료", msg):
                folder = os.path.dirname(info["out_excel_path"])
                open_folder(folder, log_func=self.append_log)

        except Exception as e:
            self.append_log(f"[ERROR] 4단계 중 오류: {e}")
            messagebox.showerror("에러", f"응답 병합 중 오류가 발생했습니다.\n\n{e}")
            self.set_progress(0.0)

    def on_step3_for_selected_jobs(self):
        """
        배치 목록에서 선택된 여러 Batch에 대해
        3단계(상태 조회 + 결과 JSONL 다운로드) + 4단계(엑셀 병합)를 순차적으로 실행.
        """
        if self.batch_tree is None:
            self.on_step3_check_and_download()
            return

        selected = self.batch_tree.selection()
        if not selected:
            self.on_step3_check_and_download()
            return

        if len(selected) == 1:
            self.batch_tree.selection_set(selected[0])
            self.on_step3_check_and_download()
            return

        try:
            client = self._build_client()
        except Exception as e:
            messagebox.showerror("에러", f"OpenAI 클라이언트 생성 실패:\n{e}")
            return

        total = len(selected)
        downloaded = 0
        pending = 0
        failed_download = 0
        merged_success = 0
        merged_fail = 0
        error_msgs = []

        self.append_log(
            f"[RUN] 3-4단계(목록 모드): 선택된 {total}개 Batch에 대해 상태 조회 + 결과 다운로드 + 엑셀 병합 실행."
        )

        for batch_id in selected:
            job = None
            for j in self.batch_jobs:
                if j.get("batch_id") == batch_id:
                    job = j
                    break
            if not job:
                failed_download += 1
                error_msgs.append(f"- {batch_id}: 작업 이력 없음")
                continue

            if job.get("archived"):
                pending += 1
                error_msgs.append(f"- {batch_id}: 보관함(archived) 상태라 건너뜀")
                continue

            src_excel = job.get("src_excel") or ""
            jsonl_path = job.get("jsonl_path") or ""
            output_jsonl = job.get("output_jsonl") or ""
            model = job.get("model") or "" or "gpt-5-mini"
            effort = job.get("effort") or ""

            if src_excel:
                self.src_excel_var.set(src_excel)
                self._update_default_paths_for_excel(src_excel)
            if jsonl_path:
                self.batch_input_jsonl_var.set(jsonl_path)
            self.batch_id_var.set(batch_id)
            if model:
                self.model_var.set(model)
            if effort:
                self.effort_var.set(effort)

            if output_jsonl:
                output_jsonl_path = output_jsonl
            else:
                if src_excel:
                    base_dir = os.path.dirname(src_excel)
                    base_name, _ = os.path.splitext(os.path.basename(src_excel))
                    output_jsonl_path = os.path.join(base_dir, f"{base_name}_stage2_batch_output.jsonl")
                else:
                    output_jsonl_path = f"stage2_batch_output_{batch_id}.jsonl"
            self.batch_output_jsonl_var.set(output_jsonl_path)

            self.append_log(f"[RUN] 3-4단계(목록 모드) 단일 실행: batch_id={batch_id}")

            # 3단계
            try:
                ok, status = download_batch_output_if_ready(
                    client=client,
                    batch_id=batch_id,
                    output_jsonl_path=output_jsonl_path,
                    log_func=self.append_log,
                )

                upsert_batch_job(
                    batch_id=batch_id,
                    log_func=self.append_log,
                    status=status,
                    output_jsonl=output_jsonl_path if ok else job.get("output_jsonl", ""),
                    completed_at=(
                        datetime.now().isoformat()
                        if ok and status == "completed"
                        else job.get("completed_at")
                    ),
                    archived=job.get("archived", False),
                )

                if ok:
                    downloaded += 1
                else:
                    if status == "completed":
                        failed_download += 1
                        error_msgs.append(f"- {batch_id}: completed 상태지만 output_file 없음")
                    else:
                        pending += 1
                        continue

            except Exception as e:
                failed_download += 1
                msg = f"- {batch_id}: 3단계 예외 발생 ({e})"
                error_msgs.append(msg)
                self.append_log(f"[ERROR] {msg}")
                continue

            # 4단계
            if not src_excel:
                merged_fail += 1
                error_msgs.append(f"- {batch_id}: src_excel 경로가 없어 병합 불가")
                continue
            if not os.path.exists(src_excel):
                merged_fail += 1
                error_msgs.append(f"- {batch_id}: src_excel 파일이 존재하지 않음 ({src_excel})")
                continue

            try:
                info = merge_batch_output_to_excel(
                    excel_path=src_excel,
                    output_jsonl_path=output_jsonl_path,
                    model_name=model,
                    skip_filled=self.skip_filled_var.get(),
                    log_func=self.append_log,
                )
                merged_success += 1

                total_tokens = (
                    (info["total_in_tok"] or 0)
                    + (info["total_out_tok"] or 0)
                    + (info["total_reasoning_tok"] or 0)
                )
                self.token_info_var.set(
                    f"토큰: input={info['total_in_tok']}, "
                    f"output={info['total_out_tok']}, "
                    f"reasoning={info['total_reasoning_tok']}, "
                    f"total={total_tokens}"
                )
                if info["total_cost_usd"] is not None:
                    self.cost_info_var.set(
                        f"비용(USD): input={info['input_cost_usd']:.6f}, "
                        f"output={info['output_cost_usd']:.6f}, "
                        f"total={info['total_cost_usd']:.6f}"
                    )
                else:
                    self.cost_info_var.set("비용: - (가격 정보 없음)")

                upsert_batch_job(
                    batch_id=batch_id,
                    log_func=self.append_log,
                    out_excel=info["out_excel_path"],
                    archived=job.get("archived", False),
                )

            except Exception as e:
                merged_fail += 1
                msg = f"- {batch_id}: 4단계(병합) 예외 발생 ({e})"
                error_msgs.append(msg)
                self.append_log(f"[ERROR] {msg}")

        # 실행 후 목록 갱신
        try:
            self._load_jobs_for_current_excel()
        except Exception as e:
            self.append_log(f"[WARN] 배치 목록 재로딩 실패: {e}")

        summary_lines = [
            f"선택 Batch 수: {total}",
            f"다운로드 완료(완료 상태): {downloaded}",
            f"아직 진행 중(미완료 또는 보관함): {pending}",
            f"다운로드 실패/에러: {failed_download}",
            f"엑셀 병합 성공: {merged_success}",
            f"엑셀 병합 실패: {merged_fail}",
        ]
        if error_msgs:
            summary_lines.append("\n상세 오류(일부):")
            for line in error_msgs[:5]:
                summary_lines.append(line)

        messagebox.showinfo("3-4단계 일괄 실행 결과", "\n".join(summary_lines))


# =========================================================
# main
# =========================================================

def main():
    app = Stage2BatchGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
