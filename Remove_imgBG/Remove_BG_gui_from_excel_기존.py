#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hybrid_remove_BG_gui_from_excel.py

[역할]

Stage1 썸네일 맵핑 엑셀(예: *_stage1_img_mapping.xlsx)을 입력으로 받아,
각 행의 `썸네일경로`에 있는 이미지를 하이브리드 배경제거(CarveKit + rembg) 파이프라인으로 처리하고,
결과 이미지 + 알파 마스크 + 라벨링/AI용 매핑(json/csv)을 생성하는 GUI 도구.

입력:
  - Stage1 썸네일 맵핑 엑셀 파일
    * 필수 컬럼: `썸네일경로`
    * 그 외 컬럼 (판매자관리코드1, 원본상품명, ST1_결과상품명 등)은 있으면 모두 메타정보로 매핑에 포함됨.

출력:
  - output_root/_auto_ok/**.jpg     : 자동으로 OK 판정된 컷 (1000x1000 흰 배경 JPG)
  - output_root/_need_manual/**.jpg : 사람이 다시 확인해야 할 컷
  - output_root/_alpha/**.png       : 최종 알파 마스크
  - output_root/bg_mapping.json     : 원본/결과/마스크/프리셋/라벨 + 엑셀 메타정보까지 포함한 매핑
  - output_root/bg_mapping.csv      : 동일 정보를 CSV로 저장 (라벨링 툴에서 쓰기 좋게)

엑셀 메타정보:
  - 각 엑셀 컬럼은 `excel_컬럼명` 형태의 키로 json/csv에 함께 기록됨.
  - 예: excel_판매자관리코드1, excel_원본상품명, excel_ST1_결과상품명 ...
  - 추가로:
      excel_row_index : 엑셀 내에서의 1-based 행 인덱스 (데이터 기준, 헤더 제외)
      excel_file      : 사용한 엑셀 파일 경로

GUI:
  - 입력 엑셀 파일 선택
  - 출력 폴더 선택 (또는 엑셀 파일명 + "_bg" 자동 생성 옵션)
  - 품질 프리셋(공격적/균형/보수적) + 설명 툴팁 라벨
  - 진행 개수 / 총 개수 / %
  - 시작 / 예상 종료 시각, 경과 시간 / 남은 시간
  - 실시간 로그
  - 작업 완료 시 "출력 폴더를 여시겠습니까?" 예/아니오 → 예 선택 시 폴더 자동 오픈

필수 패키지:
    pip install carvekit rembg pillow opencv-python numpy pandas
"""

import os
import io
import time
import threading
import queue
import json
import csv
import subprocess
import sys
from datetime import datetime, timedelta
import inspect
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import cv2
from PIL import Image
import pandas as pd

# --- CarveKit ------------------------------------------------------------
from carvekit.api.high import HiInterface

# --- rembg ---------------------------------------------------------------
from rembg import remove, new_session

# Tkinter GUI
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk


# -------------------------------------------------------------------------
#  상수: 엑셀에서 사용할 썸네일 경로 컬럼명
# -------------------------------------------------------------------------
THUMB_COL_NAME = "썸네일경로"

# -------------------------------------------------------------------------
#  품질 프리셋 정의
# -------------------------------------------------------------------------
@dataclass
class QualityConfig:
    name: str
    fg_ratio_min: float         # 전경 비율 최소
    fg_ratio_max: float         # 전경 비율 최대
    big_component_ratio: float  # "큰 블랍" 기준 (전체 대비)
    edge_touch_threshold: int   # 몇 변(상/하/좌/우) 이상 닿으면 수상
    alpha_hard_cutoff: int      # 이 값보다 작으면 alpha=0


# 프리셋별 내부 세팅
QUALITY_PRESETS: dict[str, QualityConfig] = {
    # 자동OK 많이 / 약간 과감하게
    "공격적": QualityConfig(
        name="공격적",
        fg_ratio_min=0.01,
        fg_ratio_max=0.995,
        big_component_ratio=0.12,
        edge_touch_threshold=3,
        alpha_hard_cutoff=15,
    ),
    # 기본 추천값
    "균형": QualityConfig(
        name="균형",
        fg_ratio_min=0.03,
        fg_ratio_max=0.98,
        big_component_ratio=0.08,
        edge_touch_threshold=2,
        alpha_hard_cutoff=20,
    ),
    # 웬만하면 수동확인으로 보내는 보수적 모드
    "보수적": QualityConfig(
        name="보수적",
        fg_ratio_min=0.05,
        fg_ratio_max=0.95,
        big_component_ratio=0.05,
        edge_touch_threshold=1,
        alpha_hard_cutoff=25,
    ),
}

# 프리셋 설명 (GUI 툴팁 라벨에 노출)
PRESET_DESCRIPTIONS: dict[str, str] = {
    "공격적": "자동OK 비율↑ · 애매한 컷도 과감히 자동 통과 · 잘못 잘리는 컷이 조금 늘 수 있어요.",
    "균형":   "기본 추천값 · 자동OK / 수동확인 비율을 적당히 맞춘 프리셋.",
    "보수적": "조금만 수상해도 수동확인으로 보냄 · 품질 우선 · 자동OK는 줄어듭니다.",
}

# 현재 품질 설정 (GUI에서 프리셋 선택 시 바뀜)
CONFIG: QualityConfig = QUALITY_PRESETS["균형"]


def set_quality_config(cfg: QualityConfig):
    """전역 품질 설정 갱신"""
    global CONFIG
    CONFIG = cfg


# 출력 사이즈
OUTPUT_SIZE = (1000, 1000)  # (width, height)

# 폴더 이름
AUTO_OK_DIR = "_auto_ok"
NEED_MANUAL_DIR = "_need_manual"
ALPHA_DIR = "_alpha"        # 알파 마스크 저장 루트

# rembg 세션 (birefnet-general)
REMBG_SESSION = new_session("birefnet-general")


# -------------------------------------------------------------------------
#  CarveKit 인터페이스 초기화 (버전별 시그니처 차이 자동 대응)
# -------------------------------------------------------------------------
def build_carvekit_interface() -> HiInterface:
    """
    설치된 CarveKit 버전의 HiInterface.__init__ 시그니처를 보고
    지원하는 인자만 넣어서 생성.
    """
    params = inspect.signature(HiInterface).parameters
    kwargs: Dict[str, Any] = {}

    # 공통적으로 거의 항상 있는 인자들
    if "object_type" in params:
        kwargs["object_type"] = "object"

    # 세그멘테이션 / 매팅 모델 지정 (있을 때만)
    if "segm_model" in params:
        kwargs["segm_model"] = "tracer_b7"
    if "matting_model" in params:
        kwargs["matting_model"] = "fba"

    # 디바이스 관련 인자 (CPU 강제)
    if "device" in params:
        kwargs["device"] = "cpu"
    else:
        if "segm_device" in params:
            kwargs["segm_device"] = "cpu"
        if "matting_device" in params:
            kwargs["matting_device"] = "cpu"

    # batch_size 가 있는 버전이라면 1로 제한
    if "batch_size" in params:
        kwargs["batch_size"] = 1

    return HiInterface(**kwargs)


CARVEKIT_IF = build_carvekit_interface()


# -------------------------------------------------------------------------
#  공통 유틸
# -------------------------------------------------------------------------
def format_time(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}시간 {m}분 {s}초"
    elif m > 0:
        return f"{m}분 {s}초"
    else:
        return f"{s}초"


def render_to_1000x1000_rgb(img_rgba: Image.Image) -> Image.Image:
    """
    알파 포함 이미지를 1000x1000 흰 배경 RGB로 센터 맞춰 렌더.

    - 객체가 현재 이미지의 네 변과 닿지 않는 경우:
        → 객체 bounding box 가 정사각형 한 변의 약 80% 정도가 되도록
          스케일을 조정해서 배치
    - 이미 변에 닿아 있으면:
        → 기존처럼 최대 크기로만 맞추고 추가 확대는 하지 않음
    """
    target_w, target_h = OUTPUT_SIZE
    ratio_target = 0.85  # 한 변의 80%

    if img_rgba.mode != "RGBA":
        img_rgba = img_rgba.convert("RGBA")

    # 알파에서 객체 위치 추출
    a = img_rgba.split()[-1]
    alpha = np.array(a, dtype=np.uint8)
    h, w = alpha.shape
    cfg = CONFIG

    bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
    fg_pixels = int(bin_mask.sum())

    use_ratio = False
    if fg_pixels > 0:
        ys, xs = np.where(bin_mask > 0)
        y_min, y_max = ys.min(), ys.max()
        x_min, x_max = xs.min(), xs.max()

        # 현재 이미지 상에서 네 변과 모두 떨어져 있는 경우만 80% 룰 적용
        if (
            x_min > 0
            and y_min > 0
            and x_max < w - 1
            and y_max < h - 1
        ):
            bbox_w = x_max - x_min + 1
            bbox_h = y_max - y_min + 1
            obj_max = max(bbox_w, bbox_h)
            if obj_max > 0:
                # 캔버스가 혹시라도 비정사각형이 되어도 안전하게 동작하도록 최소 변 기준
                target_side = int(min(target_w, target_h) * ratio_target)
                scale_obj = target_side / obj_max
                use_ratio = True
            else:
                scale_obj = 1.0
        else:
            scale_obj = 1.0
    else:
        scale_obj = 1.0

    src_w, src_h = img_rgba.size

    # 캔버스에 들어갈 수 있는 최대 스케일
    scale_fit_canvas = min(target_w / src_w, target_h / src_h)

    if use_ratio:
        # 80%에 맞추는 스케일과 캔버스 한계 중 작은 값 사용
        scale = min(scale_obj, scale_fit_canvas)
    else:
        # 예전처럼 최대한 꽉 차게
        scale = scale_fit_canvas

    if scale <= 0:
        scale = scale_fit_canvas

    new_w = max(1, int(round(src_w * scale)))
    new_h = max(1, int(round(src_h * scale)))

    resized = img_rgba.resize((new_w, new_h), Image.LANCZOS)

    canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 255))
    offset_x = (target_w - new_w) // 2
    offset_y = (target_h - new_h) // 2
    canvas.paste(resized, (offset_x, offset_y), resized)

    return canvas.convert("RGB")


def open_folder(path: str):
    """플랫폼별로 폴더 열기"""
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception as e:
        print(f"[WARN] 폴더 열기 실패: {e}")


# -------------------------------------------------------------------------
#  품질 분석 (CarveKit / rembg 공통)
# -------------------------------------------------------------------------
def analyze_mask(alpha: np.ndarray) -> bool:
    """
    알파 마스크를 보고 '수상한' 이미지인지 판별.
    True  = 수동 확인 필요 (또는 rembg로 재처리)
    False = 자동 통과
    """
    cfg = CONFIG

    h, w = alpha.shape
    bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)

    total_pixels = h * w
    fg_pixels = int(bin_mask.sum())

    if fg_pixels == 0:
        return True  # 완전 비어 있음

    fg_ratio = fg_pixels / total_pixels

    # 전경 비율이 너무 작거나/너무 크면 수상
    if fg_ratio < cfg.fg_ratio_min or fg_ratio > cfg.fg_ratio_max:
        return True

    # bounding box
    ys, xs = np.where(bin_mask > 0)
    y_min, y_max = ys.min(), ys.max()
    x_min, x_max = xs.min(), xs.max()

    touch_left = x_min == 0
    touch_right = x_max == w - 1
    touch_top = y_min == 0
    touch_bottom = y_max == h - 1
    touches = sum([touch_left, touch_right, touch_top, touch_bottom])

    if touches >= cfg.edge_touch_threshold:
        return True

    # 연결 컴포넌트 분석
    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(bin_mask)
    if num_labels > 1:
        areas = stats[1:, cv2.CC_STAT_AREA]  # 배경 제외
        large_areas = [
            a for a in areas if a > (total_pixels * cfg.big_component_ratio)
        ]
        if len(large_areas) >= 2:
            # 큰 덩어리가 2개 이상이면 수상
            return True

    return False


# -------------------------------------------------------------------------
#  안전할 때만 중앙 크롭 & 정렬
# -------------------------------------------------------------------------
def center_crop_if_safe(
    img_rgba: Image.Image,
    alpha: np.ndarray,
    margin: int = 5,
):
    """
    객체가 화면 가장자리와 닿지 않을 때만,
    객체 bounding box(+margin)로 크롭해서 중앙 정렬에 쓰기 위한 전처리.

    - 네 변(상/하/좌/우) 중 하나라도 닿으면: 원본 그대로 반환
    - 안 닿으면: bbox(+margin)으로 crop 후 반환
    """
    cfg = CONFIG
    h, w = alpha.shape

    bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
    if bin_mask.sum() == 0:
        # 객체가 없으면 손대지 않음
        return img_rgba, alpha

    ys, xs = np.where(bin_mask > 0)
    y_min, y_max = ys.min(), ys.max()
    x_min, x_max = xs.min(), xs.max()

    # 한 변이라도 닿으면 중앙정렬용 크롭은 하지 않음
    if (
        x_min <= 0
        or y_min <= 0
        or x_max >= w - 1
        or y_max >= h - 1
    ):
        return img_rgba, alpha

    # 여유 margin 주고 잘라내기
    x_min_c = max(0, x_min - margin)
    y_min_c = max(0, y_min - margin)
    x_max_c = min(w - 1, x_max + margin)
    y_max_c = min(h - 1, y_max + margin)

    # PIL은 (left, upper, right, lower) 이고 right/lower는 +1 필요
    img_cropped = img_rgba.crop((x_min_c, y_min_c, x_max_c + 1, y_max_c + 1))
    alpha_cropped = alpha[y_min_c : y_max_c + 1, x_min_c : x_max_c + 1].copy()

    return img_cropped, alpha_cropped


# -------------------------------------------------------------------------
#  CarveKit / rembg 개별 처리 함수
# -------------------------------------------------------------------------
def remove_bg_carvekit(input_path: str):
    """
    CarveKit으로 배경 제거.
    반환: (RGBA 이미지, alpha numpy 배열)
    """
    img = Image.open(input_path).convert("RGB")
    result_list = CARVEKIT_IF([img])  # List[Image.Image] 기대
    if not result_list:
        raise RuntimeError("CarveKit 결과가 비어 있습니다.")
    out = result_list[0].convert("RGBA")
    alpha = np.array(out.split()[-1], dtype=np.uint8)
    return out, alpha


def remove_bg_rembg(input_path: str):
    """
    rembg(birefnet-general)로 배경 제거.
    반환: (RGBA 이미지, alpha numpy 배열)
    CarveKit 대비 더 정교한 보정 역할.
    """
    cfg = CONFIG

    with open(input_path, "rb") as f:
        data = f.read()

    result_bytes = remove(
        data,
        session=REMBG_SESSION,
        alpha_matting=True,
        alpha_matting_foreground_threshold=240,
        alpha_matting_background_threshold=10,
        alpha_matting_erode_size=5,
        alpha_matting_base_size=1500,
        force_return_bytes=True,
    )

    img = Image.open(io.BytesIO(result_bytes)).convert("RGBA")
    r, g, b, a = img.split()
    alpha_raw = np.array(a, dtype=np.uint8)

    # 가장 큰 객체만 남김
    soft_mask = (alpha_raw > 5).astype(np.uint8)
    num_labels, labels = cv2.connectedComponents(soft_mask)
    if num_labels <= 1:
        keep_mask = soft_mask
    else:
        areas = np.bincount(labels.flatten())
        areas[0] = 0
        largest_label = areas.argmax()
        keep_mask = (labels == largest_label).astype(np.uint8)

    final_alpha = alpha_raw * keep_mask
    final_alpha[final_alpha < cfg.alpha_hard_cutoff] = 0

    final_a = Image.fromarray(final_alpha, mode="L")
    final_img = Image.merge("RGBA", (r, g, b, final_a))

    return final_img, final_alpha


# -------------------------------------------------------------------------
#  한 장에 대한 하이브리드 파이프라인
# -------------------------------------------------------------------------
def process_one_image(
    input_path: str,
    output_root: str,
    rel_root: str = "",
    log=None,
    preset_name: str = "",
    mapping_collector: Optional[List[Dict[str, Any]]] = None,
    input_root: Optional[str] = None,
    extra_meta: Optional[Dict[str, Any]] = None,
):
    """
    1) CarveKit으로 배경제거
    2) 마스크 품질 검사 → 수상하면 rembg로 재처리
    3) 최종 결과를 품질에 따라 _auto_ok / _need_manual 로 분류 저장
    4) 알파 마스크 PNG 및 라벨링용 mapping 정보를 생성
    5) extra_meta가 주어지면 매핑 정보에 함께 기록
    """
    if log is None:
        log = print

    base_name, _ = os.path.splitext(os.path.basename(input_path))
    output_name = f"{base_name}.jpg"

    # 1차: CarveKit
    try:
        img_rgba_ck, alpha_ck = remove_bg_carvekit(input_path)
        suspicious_ck = analyze_mask(alpha_ck)
    except Exception as e:
        log(f"[CarveKit ERROR] {input_path}: {e}")
        # CarveKit이 아예 실패하면 그냥 rembg로 시도
        img_rgba_ck, alpha_ck = None, None
        suspicious_ck = True

    # 수상하지 않으면 CarveKit 결과 그대로 사용
    if not suspicious_ck and img_rgba_ck is not None:
        final_rgba = img_rgba_ck
        final_alpha = alpha_ck
        method = "CarveKit"
        suspicious_final = False
    else:
        # 2차: rembg로 재처리
        try:
            img_rgba_rm, alpha_rm = remove_bg_rembg(input_path)
            suspicious_rm = analyze_mask(alpha_rm)
            final_rgba = img_rgba_rm
            final_alpha = alpha_rm
            method = "rembg"
            suspicious_final = suspicious_rm
        except Exception as e:
            log(f"[rembg ERROR] {input_path}: {e}")
            # rembg까지 실패하면 그냥 CarveKit 결과라도 쓰되 수동확인으로 보냄
            if img_rgba_ck is not None:
                final_rgba = img_rgba_ck
                final_alpha = alpha_ck
                method = "CarveKit(Fallback)"
            else:
                raise RuntimeError("CarveKit과 rembg 모두 실패했습니다.")
            suspicious_final = True

    result_flag = "need_manual" if suspicious_final else "auto_ok"
    result_category = "수동확인" if suspicious_final else "자동OK"
    subdir = NEED_MANUAL_DIR if suspicious_final else AUTO_OK_DIR

    # 안전 중앙 크롭 (제품이 화면 네 변에 닿지 않을 때만 bbox로 크롭)
    final_rgba, final_alpha = center_crop_if_safe(final_rgba, final_alpha)

    # 상대 경로까지 보존하고 싶다면 rel_root 사용
    target_root = os.path.join(output_root, subdir, rel_root)
    os.makedirs(target_root, exist_ok=True)
    output_path = os.path.join(target_root, output_name)

    # 최종 이미지 저장 (1000x1000 흰 배경)
    output_img = render_to_1000x1000_rgb(final_rgba)
    output_img.save(output_path, "JPEG", quality=90, optimize=True)

    # 알파 마스크도 별도 PNG로 저장
    alpha_root = os.path.join(output_root, ALPHA_DIR, rel_root)
    os.makedirs(alpha_root, exist_ok=True)
    alpha_path = os.path.join(alpha_root, f"{base_name}.png")
    Image.fromarray(final_alpha, mode="L").save(alpha_path)

    log(f"[{method}] {input_path} -> {output_path} [{result_category}]")

    # mapping 정보 수집 (라벨링/AI용)
    if mapping_collector is not None:
        input_abs = os.path.abspath(input_path)
        output_abs = os.path.abspath(output_path)
        mask_abs = os.path.abspath(alpha_path)

        # input_root 기준 상대경로
        if input_root is not None and os.path.isdir(input_root):
            try:
                input_rel = os.path.relpath(input_path, input_root)
            except ValueError:
                input_rel = os.path.basename(input_path)
        else:
            input_rel = os.path.basename(input_path)

        output_rel = os.path.relpath(output_path, output_root)
        mask_rel = os.path.relpath(alpha_path, output_root)

        # 슬래시 통일
        input_rel = input_rel.replace("\\", "/")
        output_rel = output_rel.replace("\\", "/")
        mask_rel = mask_rel.replace("\\", "/")

        entry: Dict[str, Any] = {
            "input_abs": input_abs,
            "input_rel": input_rel,
            "output_abs": output_abs,
            "output_rel": output_rel,
            "mask_abs": mask_abs,
            "mask_rel": mask_rel,
            "result_category": result_category,   # "자동OK" / "수동확인"
            "result_flag": result_flag,           # "auto_ok" / "need_manual"
            "method": method,                     # "CarveKit" / "rembg" / "CarveKit(Fallback)"
            "preset": preset_name,                # 품질 프리셋 이름

            # 라벨링/AI용 필드 (초기엔 비어 있음)
            "human_label": None,
            "human_notes": "",
            "ai_label": None,
            "ai_score": None,
            "ai_model": None,
        }

        # 엑셀 메타정보 병합
        if extra_meta:
            entry.update(extra_meta)

        mapping_collector.append(entry)


# -------------------------------------------------------------------------
#  GUI 클래스 (엑셀 기반)
# -------------------------------------------------------------------------
class HybridBGApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("하이브리드 배경제거 (엑셀 기반 · CarveKit + rembg)")
        self.root.geometry("900x750")

        # 상태 변수
        self.excel_path_var = tk.StringVar()
        self.output_path_var = tk.StringVar()

        # 품질 프리셋
        self.preset_var = tk.StringVar(value="공격적")
        self.current_preset: QualityConfig = QUALITY_PRESETS["공격적"]

        # 출력 폴더 자동 생성 옵션
        self.auto_output_var = tk.BooleanVar(value=True)

        # UI 업데이트용 큐 / 쓰레드 상태
        self.ui_queue: "queue.Queue[tuple]" = queue.Queue()
        self.job_thread: Optional[threading.Thread] = None
        self.job_running = False
        self.start_timestamp: Optional[float] = None
        self.total_files: int = 0

        # 라벨링/AI용 매핑 정보 누적
        self.mapping_rows: List[Dict[str, Any]] = []

        # UI 구성
        self._build_ui()

        # 큐 폴링 시작
        self.root.after(100, self.poll_queue)

        # 초기 프리셋 적용
        self.apply_preset(self.preset_var.get())

    # ----------------- UI 구성 -----------------
    def _build_ui(self):
        # 설명
        desc_frame = tk.Frame(self.root)
        desc_frame.pack(fill="x", padx=10, pady=(8, 4))

        tk.Label(
            desc_frame,
            text=(
                "1) Stage1 썸네일 맵핑 엑셀(*_stage1_img_mapping.xlsx)을 선택합니다.\n"
                f"   - 반드시 '{THUMB_COL_NAME}' 컬럼에 썸네일 이미지 경로가 있어야 합니다.\n"
                "2) 출력 폴더를 선택하거나, '자동 생성' 옵션을 사용합니다.\n"
                "3) 품질 프리셋을 선택하고 [배경제거 시작]을 눌러 주세요.\n\n"
                "※ 각 행의 썸네일 이미지를 CarveKit + rembg로 처리하여\n"
                "   _auto_ok / _need_manual / _alpha + bg_mapping.json/csv 를 생성합니다."
            ),
            justify="left",
            anchor="w",
        ).pack(fill="x")

        # 입력 엑셀
        in_frame = tk.LabelFrame(self.root, text="입력 Stage1 이미지 맵핑 엑셀")
        in_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(in_frame, text="엑셀 파일:").pack(side="left")
        tk.Entry(in_frame, textvariable=self.excel_path_var, width=60).pack(
            side="left", padx=5
        )
        tk.Button(in_frame, text="찾기...", command=self.select_excel).pack(side="left")

        # 출력 경로 + 자동 생성 옵션
        out_frame = tk.LabelFrame(self.root, text="출력 설정")
        out_frame.pack(fill="x", padx=10, pady=5)

        tk.Label(out_frame, text="출력 폴더:").pack(side="left")
        self.output_entry = tk.Entry(out_frame, textvariable=self.output_path_var, width=45)
        self.output_entry.pack(side="left", padx=5)
        self.output_button = tk.Button(out_frame, text="찾기...", command=self.select_output)
        self.output_button.pack(side="left")

        self.auto_output_check = tk.Checkbutton(
            out_frame,
            text="엑셀 파일과 같은 위치에 자동 생성 (파일명 + '_bg')",
            variable=self.auto_output_var,
        )
        self.auto_output_check.pack(side="left", padx=(10, 0))

        # 품질 프리셋 + 설명
        preset_frame = tk.Frame(self.root)
        preset_frame.pack(fill="x", padx=10, pady=(0, 5))

        tk.Label(preset_frame, text="품질 프리셋:").pack(side="left")

        self.preset_combo = ttk.Combobox(
            preset_frame,
            textvariable=self.preset_var,
            values=list(QUALITY_PRESETS.keys()),
            state="readonly",
            width=8,
        )
        self.preset_combo.pack(side="left", padx=5)
        self.preset_combo.bind("<<ComboboxSelected>>", self.on_preset_changed)

        self.preset_desc_label = tk.Label(
            preset_frame,
            text="",
            anchor="w",
            fg="#555555",
        )
        self.preset_desc_label.pack(side="left", padx=10)

        # 실행 버튼
        btn_frame = tk.Frame(self.root)
        btn_frame.pack(fill="x", padx=10, pady=5)

        self.start_button = tk.Button(
            btn_frame,
            text="배경제거 시작",
            command=self.start_processing,
            bg="#4caf50",
            fg="white",
        )
        self.start_button.pack(side="right")

        # 진행 상태 영역
        status_frame = tk.LabelFrame(self.root, text="진행 상태")
        status_frame.pack(fill="x", padx=10, pady=5)

        self.status_label = tk.Label(
            status_frame, text="대기 중", anchor="w"
        )
        self.status_label.pack(fill="x", padx=5, pady=(3, 0))

        self.time_label = tk.Label(
            status_frame,
            text="시작: - / 예상 종료: - / 경과: - / 남은: -",
            anchor="w",
            fg="#555555",
        )
        self.time_label.pack(fill="x", padx=5, pady=(0, 3))

        self.progress_bar = ttk.Progressbar(status_frame, maximum=100, value=0)
        self.progress_bar.pack(fill="x", padx=5, pady=(0, 5))

        # 로그 창
        log_frame = tk.LabelFrame(self.root, text="로그")
        log_frame.pack(fill="both", expand=True, padx=10, pady=(5, 10))

        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=25, state="disabled"
        )
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)

        # 초기 안내
        self.log(
            "하이브리드 배경제거 도구 (엑셀 기반 · CarveKit + rembg)\n"
            f"입력: Stage1 썸네일 맵핑 엑셀 / 사용 컬럼: '{THUMB_COL_NAME}'\n"
            f"출력 크기: {OUTPUT_SIZE[0]}x{OUTPUT_SIZE[1]} 흰 배경 JPG\n"
            f"_auto_ok      : 자동 통과 컷\n"
            f"_need_manual  : 수동 확인 필요 컷\n"
        )
        self.log(
            "※ 품질 프리셋에 따라 자동OK/수동확인 기준이 달라집니다.\n"
            "   - 공격적: 자동OK 최대화, 약간 과감\n"
            "   - 균형  : 기본 추천 값\n"
            "   - 보수적: 품질 우선, 수동확인 많음\n"
        )
        self.log(
            "※ 라벨링/AI 작업을 위해 bg_mapping.json / bg_mapping.csv / _alpha 마스크가 같이 생성됩니다.\n"
            "   각 행의 엑셀 값은 excel_컬럼명 형태로 매핑에 기록됩니다.\n"
        )

    # ----------------- 프리셋 관련 -----------------
    def apply_preset(self, name: str):
        cfg = QUALITY_PRESETS.get(name)
        if not cfg:
            return
        set_quality_config(cfg)
        self.current_preset = cfg
        desc = PRESET_DESCRIPTIONS.get(name, "")
        self.preset_desc_label.config(text=desc)
        self.log(f"[프리셋 변경] '{name}' 적용: {desc}")

    def on_preset_changed(self, event=None):
        self.apply_preset(self.preset_var.get())

    # ----------------- 로그 / 진행 업데이트 -----------------
    def log(self, message: str):
        """메인 스레드에서 직접 호출하는 로거"""
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def update_progress(
        self,
        current: int,
        total: int,
        start_ts: float,
        elapsed: float,
        remaining: float,
    ):
        percent = 0.0
        if total > 0:
            percent = current / total * 100.0

        self.progress_bar["value"] = percent
        self.status_label.config(
            text=f"진행: {current}/{total} ({percent:5.1f}%)"
        )

        start_dt = datetime.fromtimestamp(start_ts)
        now_dt = datetime.now()
        est_end_dt = now_dt + timedelta(seconds=max(remaining, 0))

        self.time_label.config(
            text=(
                f"시작: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}  /  "
                f"예상 종료: {est_end_dt.strftime('%Y-%m-%d %H:%M:%S')}  /  "
                f"경과: {format_time(elapsed)}  /  남은: {format_time(remaining)}"
            )
        )

    # ----------------- 큐 폴링 (UI 업데이트) -----------------
    def poll_queue(self):
        """백그라운드 스레드에서 보낸 메시지를 UI에 반영"""
        try:
            while True:
                item = self.ui_queue.get_nowait()
                kind = item[0]

                if kind == "log":
                    msg = item[1]
                    self.log(msg)

                elif kind == "progress":
                    _, current, total, start_ts, elapsed, remaining = item
                    self.update_progress(current, total, start_ts, elapsed, remaining)

                elif kind == "init":
                    _, total, est_total, est_end_str, out_root, excel_path = item
                    self.total_files = total
                    self.status_label.config(
                        text=f"준비 중... 전체 {total}개 / 예상 소요: {format_time(est_total)}"
                    )
                    self.time_label.config(
                        text=f"예상 종료(초기 추정): {est_end_str}"
                    )
                    self.log(f"[설정] 입력 엑셀: {excel_path}")
                    self.log(f"[설정] 결과 폴더: {out_root}")

                elif kind == "done":
                    _, out_root = item
                    self.job_running = False
                    self.start_button.config(state="normal")
                    self.log("=== 작업 완료 ===")
                    if messagebox.askyesno(
                        "완료",
                        "배경제거 작업이 완료되었습니다.\n\n출력 폴더를 여시겠습니까?",
                    ):
                        open_folder(out_root)

                elif kind == "mapping_saved":
                    _, json_path, csv_path, count = item
                    self.log(
                        f"[MAPPING] {count}개 항목을\n"
                        f"  - {json_path}\n"
                        f"  - {csv_path}\n"
                        f"로 저장했습니다."
                    )

        except queue.Empty:
            pass

        self.root.after(100, self.poll_queue)

    # ----------------- 경로 선택 -----------------
    def select_excel(self):
        path = filedialog.askopenfilename(
            title="Stage1 이미지 맵핑 엑셀 선택",
            filetypes=[
                ("Excel files", "*.xlsx;*.xlsm;*.xls"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.excel_path_var.set(path)

    def select_output(self):
        path = filedialog.askdirectory(title="출력 폴더 선택")
        if path:
            self.output_path_var.set(path)
            self.auto_output_var.set(False)

    # ----------------- 작업 시작 -----------------
    def start_processing(self):
        if self.job_running:
            messagebox.showwarning(
                "진행 중",
                "이미 작업이 진행 중입니다. 작업이 끝난 후 다시 실행해주세요.",
            )
            return

        excel_path = self.excel_path_var.get().strip()
        out_path = self.output_path_var.get().strip()

        if not excel_path:
            messagebox.showwarning("주의", "입력 엑셀 파일을 선택해주세요.")
            return
        if not os.path.exists(excel_path):
            messagebox.showerror("오류", f"엑셀 파일을 찾을 수 없습니다:\n{excel_path}")
            return

        # 엑셀 로드
        try:
            df = pd.read_excel(excel_path, dtype=str)
        except Exception as e:
            messagebox.showerror("오류", f"엑셀을 읽는 중 오류가 발생했습니다:\n{e}")
            return

        if THUMB_COL_NAME not in df.columns:
            messagebox.showerror(
                "오류",
                f"엑셀에 '{THUMB_COL_NAME}' 컬럼이 없습니다.\n"
                "Stage1 썸네일 맵핑 엑셀(*_stage1_img_mapping.xlsx)인지 확인해주세요.",
            )
            return

        excel_dir = os.path.dirname(excel_path)

        # 엑셀 → 처리 대상 리스트 구성
        items = []
        skipped_no_path = 0
        skipped_missing_file = 0

        for idx, row in df.iterrows():
            raw_val = row.get(THUMB_COL_NAME, "")
            if pd.isna(raw_val):
                s = ""
            else:
                s = str(raw_val).strip()

            if not s:
                skipped_no_path += 1
                continue

            if os.path.isabs(s):
                img_path = s
            else:
                img_path = os.path.normpath(os.path.join(excel_dir, s))

            if not os.path.exists(img_path):
                skipped_missing_file += 1
                self.ui_queue.put(
                    ("log", f"[SKIP] 행 {idx+1}: 이미지 파일을 찾을 수 없습니다: {img_path}")
                )
                continue

            meta: Dict[str, Any] = {
                "excel_row_index": idx + 1,
                "excel_file": os.path.abspath(excel_path),
            }
            for col_name, value in row.items():
                if pd.isna(value):
                    v = None
                else:
                    v = str(value)
                meta[f"excel_{col_name}"] = v

            items.append({"image_path": img_path, "meta": meta})

        if not items:
            msg = "처리할 이미지가 없습니다.\n"
            if skipped_no_path:
                msg += f"- 썸네일경로 비어 있음: {skipped_no_path}행\n"
            if skipped_missing_file:
                msg += f"- 이미지 파일 없음: {skipped_missing_file}행\n"
            messagebox.showwarning("주의", msg)
            return

        total = len(items)

        # 출력 루트 결정
        if self.auto_output_var.get() or not out_path:
            base_name = os.path.splitext(os.path.basename(excel_path))[0]
            out_root = os.path.join(excel_dir, base_name + "_bg")
            self.output_path_var.set(out_root)
        else:
            out_root = out_path

        # 초기 ETA
        est_seconds_per_image = 2.5
        est_total = est_seconds_per_image * total
        est_end = datetime.now() + timedelta(seconds=est_total)

        self.job_running = True
        self.start_button.config(state="disabled")
        self.start_timestamp = time.time()
        self.total_files = total
        self.progress_bar["value"] = 0
        self.mapping_rows = []

        preset_name = self.preset_var.get()
        self.apply_preset(preset_name)

        self.ui_queue.put(
            (
                "init",
                total,
                est_total,
                est_end.strftime("%Y-%m-%d %H:%M:%S"),
                out_root,
                excel_path,
            )
        )
        self.ui_queue.put(
            (
                "log",
                f"=== 작업 설정 ===\n"
                f"입력 엑셀: {excel_path}\n"
                f"유효 썸네일 행 수: {total}개\n"
                f"(썸네일경로 비어 있음: {skipped_no_path}행, "
                f"파일 없음: {skipped_missing_file}행)\n"
                f"프리셋: {preset_name}\n"
                f"대략 {est_seconds_per_image:.1f}초/이미지 기준, "
                f"예상 소요: {format_time(est_total)}\n"
                f"예상 종료 시각: {est_end.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"결과 폴더: {out_root}\n"
                f"  - 자동 통과: {os.path.join(out_root, AUTO_OK_DIR)}\n"
                f"  - 수동 확인: {os.path.join(out_root, NEED_MANUAL_DIR)}\n"
                f"  - 알파 마스크: {os.path.join(out_root, ALPHA_DIR)}\n"
                f"=================",
            )
        )

        t = threading.Thread(
            target=self.worker,
            args=(excel_path, out_root, items, preset_name),
            daemon=True,
        )
        self.job_thread = t
        t.start()

    # ----------------- 매핑 파일 저장 -----------------
    def save_mapping_files(self, out_root: str):
        if not self.mapping_rows:
            return

        json_path = os.path.join(out_root, "bg_mapping.json")
        csv_path = os.path.join(out_root, "bg_mapping.csv")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.mapping_rows, f, ensure_ascii=False, indent=2)

        base_field_order = [
            "input_abs",
            "input_rel",
            "output_abs",
            "output_rel",
            "mask_abs",
            "mask_rel",
            "result_category",
            "result_flag",
            "method",
            "preset",
            "human_label",
            "human_notes",
            "ai_label",
            "ai_score",
            "ai_model",
        ]

        all_keys = set()
        for row in self.mapping_rows:
            all_keys.update(row.keys())

        fieldnames: List[str] = []
        for k in base_field_order:
            if k in all_keys:
                fieldnames.append(k)
                all_keys.discard(k)
        fieldnames.extend(sorted(all_keys))

        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.mapping_rows:
                writer.writerow(row)

        self.ui_queue.put(
            ("mapping_saved", json_path, csv_path, len(self.mapping_rows))
        )

    # ----------------- 백그라운드 워커 -----------------
    def worker(self, excel_path: str, out_root: str, items, preset_name: str):
        start_ts = time.time()
        total = len(items)
        os.makedirs(out_root, exist_ok=True)

        for idx, item in enumerate(items, start=1):
            input_path = item["image_path"]
            extra_meta = item.get("meta") or {}

            try:
                process_one_image(
                    input_path,
                    out_root,
                    rel_root="",
                    log=lambda m: self.ui_queue.put(("log", m)),
                    preset_name=preset_name,
                    mapping_collector=self.mapping_rows,
                    input_root=None,
                    extra_meta=extra_meta,
                )
            except Exception as e:
                self.ui_queue.put(
                    ("log", f"[FATAL] {input_path} 처리 중 오류: {e}")
                )

            elapsed = time.time() - start_ts
            avg = elapsed / idx
            remaining = avg * (total - idx)

            self.ui_queue.put(
                ("progress", idx, total, start_ts, elapsed, remaining)
            )

        self.save_mapping_files(out_root)
        self.ui_queue.put(("done", out_root))


# -------------------------------------------------------------------------
#  실행
# -------------------------------------------------------------------------
def run_gui():
    root = tk.Tk()
    app = HybridBGApp(root)
    root.mainloop()


if __name__ == "__main__":
    run_gui()
