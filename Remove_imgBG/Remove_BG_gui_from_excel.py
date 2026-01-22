#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hybrid_remove_BG_gui_optimized.py

[업데이트 내역]
1. GPU 가속 지원 (CUDA 사용 가능 시 자동 적용)
2. 모델 로딩 최적화 (앱 실행 시가 아니라, 작업 시작 시 로딩)
3. 작업 중단(Stop) 기능 추가
4. 메모리 누수 방지 (gc.collect 주기적 호출)

필수 패키지:
    pip install carvekit rembg pillow opencv-python numpy pandas torch
    (GPU 사용 시 torch 버전에 맞는 CUDA 설정 필요)
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
import gc  # 메모리 관리용
from datetime import datetime, timedelta
import inspect
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import cv2
from PIL import Image
import pandas as pd
import torch  # GPU 체크용

# --- CarveKit ------------------------------------------------------------
from carvekit.api.high import HiInterface

# --- rembg ---------------------------------------------------------------
from rembg import remove, new_session

# Tkinter GUI
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from tkinter import font as tkfont
# -------------------------------------------------------------------------
#  전역 변수 (모델 지연 로딩을 위해 None으로 초기화)
# -------------------------------------------------------------------------
CARVEKIT_IF = None
REMBG_SESSION = None
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# -------------------------------------------------------------------------
#  상수: 엑셀에서 사용할 썸네일 경로 컬럼명
# -------------------------------------------------------------------------
THUMB_COL_NAME = "썸네일경로"
# 엑셀에 쓸 결과 컬럼명들
COL_IMG_OUT = "IMG_S1_누끼"
COL_HUMAN_LABEL = "IMG_S1_휴먼라벨"
COL_HUMAN_NOTES = "IMG_S1_휴먼노트"
COL_AI_LABEL = "IMG_S1_AI라벨"
# -------------------------------------------------------------------------
#  품질 프리셋 정의
# -------------------------------------------------------------------------
@dataclass
class QualityConfig:
    name: str
    fg_ratio_min: float
    fg_ratio_max: float
    big_component_ratio: float
    edge_touch_threshold: int
    alpha_hard_cutoff: int

QUALITY_PRESETS: dict[str, QualityConfig] = {
    "공격적": QualityConfig(
        name="공격적",
        fg_ratio_min=0.01,
        fg_ratio_max=0.995,
        big_component_ratio=0.12,
        edge_touch_threshold=3,
        alpha_hard_cutoff=15,
    ),
    "균형": QualityConfig(
        name="균형",
        fg_ratio_min=0.03,
        fg_ratio_max=0.98,
        big_component_ratio=0.08,
        edge_touch_threshold=2,
        alpha_hard_cutoff=20,
    ),
    "보수적": QualityConfig(
        name="보수적",
        fg_ratio_min=0.05,
        fg_ratio_max=0.95,
        big_component_ratio=0.05,
        edge_touch_threshold=1,
        alpha_hard_cutoff=25,
    ),
}

PRESET_DESCRIPTIONS: dict[str, str] = {
    "공격적": "자동OK 비율↑ · 애매한 컷도 과감히 자동 통과",
    "균형":   "기본 추천값 · 자동OK / 수동확인 비율 균형",
    "보수적": "품질 우선 · 조금만 수상해도 수동확인으로 보냄",
}

CONFIG: QualityConfig = QUALITY_PRESETS["균형"]

def set_quality_config(cfg: QualityConfig):
    global CONFIG
    CONFIG = cfg

# 출력 사이즈 및 폴더명
OUTPUT_SIZE = (1000, 1000)
AUTO_OK_DIR = "_auto_ok"
NEED_MANUAL_DIR = "_need_manual"
ALPHA_DIR = "_alpha"


# -------------------------------------------------------------------------
#  모델 로딩 함수 (지연 로딩)
# -------------------------------------------------------------------------
def load_models_if_needed(log_func=print):
    """
    작업 시작 직전에 모델을 로딩합니다.
    이미 로딩되어 있다면 건너뜁니다.
    GPU(CUDA)가 사용 가능하다면 적극 활용합니다.
    """
    global CARVEKIT_IF, REMBG_SESSION, DEVICE

    # 1. CarveKit 로딩
    if CARVEKIT_IF is None:
        log_func(f"[System] CarveKit 모델 로딩 중... (Device: {DEVICE})")
        
        params = inspect.signature(HiInterface).parameters
        kwargs: Dict[str, Any] = {}

        if "object_type" in params:
            kwargs["object_type"] = "object"
        if "segm_model" in params:
            kwargs["segm_model"] = "tracer_b7"
        if "matting_model" in params:
            kwargs["matting_model"] = "fba"

        # GPU 설정
        if "device" in params:
            kwargs["device"] = DEVICE
        else:
            if "segm_device" in params:
                kwargs["segm_device"] = DEVICE
            if "matting_device" in params:
                kwargs["matting_device"] = DEVICE

        if "batch_size" in params:
            kwargs["batch_size"] = 1
        
        try:
            CARVEKIT_IF = HiInterface(**kwargs)
        except Exception as e:
            log_func(f"[Error] CarveKit 로딩 실패: {e}")
            raise e

    # 2. rembg 로딩
    if REMBG_SESSION is None:
        log_func(f"[System] rembg 모델 로딩 중... (Device: {DEVICE})")
        # ONNX Runtime Providers 설정
        providers = ["CUDAExecutionProvider", "CPUExecutionProvider"] if DEVICE == "cuda" else ["CPUExecutionProvider"]
        try:
            REMBG_SESSION = new_session("birefnet-general", providers=providers)
        except Exception as e:
            log_func(f"[Error] rembg 로딩 실패: {e}")
            raise e


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

#기존껏
# def render_to_1000x1000_rgb(img_rgba: Image.Image) -> Image.Image:
#     target_w, target_h = OUTPUT_SIZE
#     ratio_target = 0.85

#     if img_rgba.mode != "RGBA":
#         img_rgba = img_rgba.convert("RGBA")

#     a = img_rgba.split()[-1]
#     alpha = np.array(a, dtype=np.uint8)
#     h, w = alpha.shape
#     cfg = CONFIG

#     bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
#     fg_pixels = int(bin_mask.sum())

#     use_ratio = False
#     scale_obj = 1.0

#     if fg_pixels > 0:
#         ys, xs = np.where(bin_mask > 0)
#         y_min, y_max = ys.min(), ys.max()
#         x_min, x_max = xs.min(), xs.max()

#         if (x_min > 0 and y_min > 0 and x_max < w - 1 and y_max < h - 1):
#             bbox_w = x_max - x_min + 1
#             bbox_h = y_max - y_min + 1
#             obj_max = max(bbox_w, bbox_h)
#             if obj_max > 0:
#                 target_side = int(min(target_w, target_h) * ratio_target)
#                 scale_obj = target_side / obj_max
#                 use_ratio = True

#     src_w, src_h = img_rgba.size
#     scale_fit_canvas = min(target_w / src_w, target_h / src_h)

#     if use_ratio:
#         scale = min(scale_obj, scale_fit_canvas)
#     else:
#         scale = scale_fit_canvas

#     if scale <= 0: scale = scale_fit_canvas

#     new_w = max(1, int(round(src_w * scale)))
#     new_h = max(1, int(round(src_h * scale)))

#     resized = img_rgba.resize((new_w, new_h), Image.LANCZOS)
#     canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 255))
#     offset_x = (target_w - new_w) // 2
#     offset_y = (target_h - new_h) // 2
#     canvas.paste(resized, (offset_x, offset_y), resized)

#     return canvas.convert("RGB")

# 면닿은애들 보정 v1
# def render_to_1000x1000_rgb(img_rgba: Image.Image) -> Image.Image:
#     """
#     - 기본: 긴 변 기준으로 1000x1000 안에 꽉 차게 중앙 배치
#     - 전경이 네 면 모두에서 떨어져 있으면: 1:1 안에서 대략 85%까지 확대 후 중앙 배치
#     - 전경이 '정확히 한 면'에만 닿아 있고, 몇 가지 조건을 만족하면:
#         → 그 면을 기준으로 앵커 고정 + 85%까지 확대
#         (예: 왼쪽에만 닿아 있으면 왼쪽은 붙이고, 세로만 중앙 정렬)
#     """
#     target_w, target_h = OUTPUT_SIZE
#     ratio_target = 0.85

#     if img_rgba.mode != "RGBA":
#         img_rgba = img_rgba.convert("RGBA")

#     a = img_rgba.split()[-1]
#     alpha = np.array(a, dtype=np.uint8)
#     h, w = alpha.shape
#     cfg = CONFIG

#     bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
#     fg_pixels = int(bin_mask.sum())

#     use_center_ratio = False   # 기존 85% 중앙 정렬
#     use_anchor_ratio = False   # 새로 추가: 한쪽 면 앵커 + 85%
#     anchor_side = None         # "left" / "right" / "top" / "bottom"
#     scale_obj = 1.0

#     if fg_pixels > 0:
#         ys, xs = np.where(bin_mask > 0)
#         y_min, y_max = ys.min(), ys.max()
#         x_min, x_max = xs.min(), xs.max()

#         # 어떤 면에 닿았는지 체크
#         touch_left   = (x_min == 0)
#         touch_right  = (x_max == w - 1)
#         touch_top    = (y_min == 0)
#         touch_bottom = (y_max == h - 1)
#         touches = int(touch_left) + int(touch_right) + int(touch_top) + int(touch_bottom)

#         bbox_w = x_max - x_min + 1
#         bbox_h = y_max - y_min + 1
#         obj_max = max(bbox_w, bbox_h) if (bbox_w > 0 and bbox_h > 0) else 0

#         target_side = int(min(target_w, target_h) * ratio_target)

#         # --- 1) 네 면 모두에서 떨어져 있는 경우: 기존 85% 중앙 정렬 ---
#         if touches == 0 and obj_max > 0:
#             scale_obj = target_side / obj_max
#             use_center_ratio = True

#         # --- 2) 정확히 한 면에만 닿아 있고, "확대"해도 안전해 보이는 경우만 앵커+85% ---
#         elif touches == 1 and obj_max > 0:
#             # 2-1) 이미 target_side보다 큰 경우는 굳이 줄이지 않음 → 기존 로직 유지
#             if obj_max < target_side:
#                 # 2-2) 너무 길쭉한(극단적인) 비율은 위험하니 제외
#                 aspect = min(bbox_w, bbox_h) / max(bbox_w, bbox_h)
#                 if aspect >= 0.35:  # 0.0~1.0, 값이 작을수록 길쭉함
#                     # 2-3) 반대쪽 여백이 최소한 어느 정도 있어야 함
#                     margin_min_x = max(5, int(w * 0.05))
#                     margin_min_y = max(5, int(h * 0.05))

#                     # 각 경우별로 "반대쪽 여백" 체크
#                     if touch_left:
#                         free_right = (w - 1) - x_max
#                         if free_right >= margin_min_x:
#                             anchor_side = "left"
#                     elif touch_right:
#                         free_left = x_min
#                         if free_left >= margin_min_x:
#                             anchor_side = "right"
#                     elif touch_top:
#                         free_bottom = (h - 1) - y_max
#                         if free_bottom >= margin_min_y:
#                             anchor_side = "top"
#                     elif touch_bottom:
#                         free_top = y_min
#                         if free_top >= margin_min_y:
#                             anchor_side = "bottom"

#                     if anchor_side is not None:
#                         scale_obj = target_side / obj_max
#                         use_anchor_ratio = True

#     # --- 실제 스케일 계산 (캔버스를 넘지 않도록 안전장치) ---
#     src_w, src_h = img_rgba.size
#     scale_fit_canvas = min(target_w / src_w, target_h / src_h)

#     if use_center_ratio or use_anchor_ratio:
#         scale = min(scale_obj, scale_fit_canvas)
#     else:
#         # 예전처럼: 그냥 캔버스에 꽉 차게만 맞추기
#         scale = scale_fit_canvas

#     if scale <= 0:
#         scale = scale_fit_canvas

#     new_w = max(1, int(round(src_w * scale)))
#     new_h = max(1, int(round(src_h * scale)))

#     resized = img_rgba.resize((new_w, new_h), Image.LANCZOS)
#     canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 255))

#     # --- 위치(offset) 결정 ---
#     if use_anchor_ratio and anchor_side is not None:
#         # 한쪽 면은 붙이고, 반대 축만 중앙 정렬
#         if anchor_side == "left":
#             offset_x = 0
#             offset_y = (target_h - new_h) // 2
#         elif anchor_side == "right":
#             offset_x = target_w - new_w
#             offset_y = (target_h - new_h) // 2
#         elif anchor_side == "top":
#             offset_x = (target_w - new_w) // 2
#             offset_y = 0
#         elif anchor_side == "bottom":
#             offset_x = (target_w - new_w) // 2
#             offset_y = target_h - new_h
#         else:
#             # 혹시라도 이상한 값이면 안전하게 중앙 정렬
#             offset_x = (target_w - new_w) // 2
#             offset_y = (target_h - new_h) // 2
#     else:
#         # 기존처럼 중앙 정렬
#         offset_x = (target_w - new_w) // 2
#         offset_y = (target_h - new_h) // 2

#     canvas.paste(resized, (offset_x, offset_y), resized)
#     return canvas.convert("RGB")


# 면닿은애들 보정 v2
def render_to_1000x1000_rgb(img_rgba: Image.Image) -> Image.Image:
    """
    알파 포함 이미지를 1000x1000 흰 배경 RGB로 렌더.

    - 객체가 네 변과 모두 떨어져 있으면:
        → 객체 bbox가 정사각형 한 변의 약 85%가 되도록 확대 + 중앙 정렬
    - 객체가 '정확히 한 면'에만 닿아 있고, 몇 가지 안전 조건을 만족하면:
        → 그 면을 앵커로 고정(붙인 상태 유지) + 나머지 축만 중앙 정렬
        → bbox가 85%보다 작으면 그때만 85%까지 확대
    - 그 외(여러 면에 닿았거나, 너무 길쭉하거나, 이미 충분히 큰 경우):
        → 예전처럼 "캔버스에 꽉 차게"만 맞춘다.
    """
    target_w, target_h = OUTPUT_SIZE
    ratio_target = 0.85

    if img_rgba.mode != "RGBA":
        img_rgba = img_rgba.convert("RGBA")

    # 알파 마스크 분석
    a = img_rgba.split()[-1]
    alpha = np.array(a, dtype=np.uint8)
    h, w = alpha.shape
    cfg = CONFIG

    bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
    fg_pixels = int(bin_mask.sum())

    use_center_ratio = False   # 중앙 85% 모드
    use_anchor_ratio = False   # 한쪽 면 앵커 모드
    anchor_side = None         # "left" / "right" / "top" / "bottom"
    scale_obj = 1.0            # 객체 기준 스케일 (기본 1배)

    if fg_pixels > 0:
        ys, xs = np.where(bin_mask > 0)
        y_min, y_max = ys.min(), ys.max()
        x_min, x_max = xs.min(), xs.max()

        # 어떤 변에 닿았는지
        touch_left   = (x_min == 0)
        touch_right  = (x_max == w - 1)
        touch_top    = (y_min == 0)
        touch_bottom = (y_max == h - 1)
        touches = int(touch_left) + int(touch_right) + int(touch_top) + int(touch_bottom)

        bbox_w = x_max - x_min + 1
        bbox_h = y_max - y_min + 1
        obj_max = max(bbox_w, bbox_h) if (bbox_w > 0 and bbox_h > 0) else 0

        target_side = int(min(target_w, target_h) * ratio_target)

        # 1) 네 변 모두에서 떨어져 있는 경우 → 기존 중앙 85% 확대
        if touches == 0 and obj_max > 0:
            scale_obj = target_side / obj_max
            use_center_ratio = True

        # 2) 정확히 한 면에만 닿아 있는 경우 → 조건부 앵커 모드
        elif touches == 1 and obj_max > 0:
            # 너무 길쭉한 비율은 위험 → 제외 (조금 완화해서 0.3으로 둬도 됨)
            aspect = min(bbox_w, bbox_h) / max(bbox_w, bbox_h)
            if aspect >= 0.35:
                # 반대쪽 여백이 최소 5% 이상 있어야만 앵커 사용
                margin_min_x = max(5, int(w * 0.05))
                margin_min_y = max(5, int(h * 0.05))

                if touch_left:
                    free_right = (w - 1) - x_max
                    if free_right >= margin_min_x:
                        anchor_side = "left"
                elif touch_right:
                    free_left = x_min
                    if free_left >= margin_min_x:
                        anchor_side = "right"
                elif touch_top:
                    free_bottom = (h - 1) - y_max
                    if free_bottom >= margin_min_y:
                        anchor_side = "top"
                elif touch_bottom:
                    free_top = y_min
                    if free_top >= margin_min_y:
                        anchor_side = "bottom"

                if anchor_side is not None:
                    use_anchor_ratio = True
                    # 여기서가 핵심 변화:
                    # ➜ 앵커는 "확대 필요 여부"와 상관없이 켜고,
                    #    확대는 bbox가 target_side보다 작을 때만 한다.
                    if obj_max < target_side:
                        scale_obj = target_side / obj_max
                    else:
                        scale_obj = 1.0  # 이미 충분히 크면 1배 유지
        # 🔽🔽🔽 여기부터 추가 블록 🔽🔽🔽
        # 3) 보너스 규칙: 아직 어떤 모드도 안 켜졌고,
        #    거의 정사각형(또는 원형) + 적당한 크기의 객체라면
        #    닿아 있어도 중앙 정렬(필요하면 85% 확대) 허용
        if (not use_center_ratio) and (not use_anchor_ratio) and obj_max > 0:
            area_ratio = fg_pixels / (w * h)  # 전체 이미지 중 전경 비율
            aspect_square = min(bbox_w, bbox_h) / max(bbox_w, bbox_h)

            # 예시 조건:
            # - 전경이 이미지의 20~75% 정도를 차지
            # - 가로세로 비가 0.65 이상 (꽤 정사각형 / 원형 느낌)
            if 0.20 <= area_ratio <= 0.75 and aspect_square >= 0.6:
                use_center_ratio = True
                if obj_max < target_side:
                    scale_obj = target_side / obj_max
                else:
                    scale_obj = 1.0
        # 🔼🔼🔼 추가 끝 🔼🔼🔼


    # 실제 스케일 계산 (캔버스를 넘지 않도록 제한)
    src_w, src_h = img_rgba.size
    scale_fit_canvas = min(target_w / src_w, target_h / src_h)

    if use_center_ratio or use_anchor_ratio:
        # scale_obj는 (1배 이상) 확대용 or 1.0
        scale = min(scale_obj, scale_fit_canvas)
    else:
        # 예전처럼: 그냥 캔버스에 꽉 차게
        scale = scale_fit_canvas

    if scale <= 0:
        scale = scale_fit_canvas

    new_w = max(1, int(round(src_w * scale)))
    new_h = max(1, int(round(src_h * scale)))

    resized = img_rgba.resize((new_w, new_h), Image.LANCZOS)
    canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 255))

    # 위치 결정
    if use_anchor_ratio and anchor_side is not None:
        # 한쪽 면은 붙이고, 나머지 축만 중앙 정렬
        if anchor_side == "left":
            offset_x = 0
            offset_y = (target_h - new_h) // 2
        elif anchor_side == "right":
            offset_x = target_w - new_w
            offset_y = (target_h - new_h) // 2
        elif anchor_side == "top":
            offset_x = (target_w - new_w) // 2
            offset_y = 0
        elif anchor_side == "bottom":
            offset_x = (target_w - new_w) // 2
            offset_y = target_h - new_h
        else:
            offset_x = (target_w - new_w) // 2
            offset_y = (target_h - new_h) // 2
    else:
        # 기존처럼 가운데 정렬
        offset_x = (target_w - new_w) // 2
        offset_y = (target_h - new_h) // 2

    canvas.paste(resized, (offset_x, offset_y), resized)
    return canvas.convert("RGB")



def open_folder(path: str):
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception as e:
        print(f"[WARN] 폴더 열기 실패: {e}")


# -------------------------------------------------------------------------
#  품질 분석 및 크롭
# -------------------------------------------------------------------------
def analyze_mask(alpha: np.ndarray) -> bool:
    cfg = CONFIG
    h, w = alpha.shape
    bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)

    total_pixels = h * w
    fg_pixels = int(bin_mask.sum())

    if fg_pixels == 0: return True

    fg_ratio = fg_pixels / total_pixels
    if fg_ratio < cfg.fg_ratio_min or fg_ratio > cfg.fg_ratio_max:
        return True

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

    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(bin_mask)
    if num_labels > 1:
        areas = stats[1:, cv2.CC_STAT_AREA]
        large_areas = [a for a in areas if a > (total_pixels * cfg.big_component_ratio)]
        if len(large_areas) >= 2:
            return True

    return False

def center_crop_if_safe(img_rgba: Image.Image, alpha: np.ndarray, margin: int = 5):
    cfg = CONFIG
    h, w = alpha.shape
    bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
    if bin_mask.sum() == 0:
        return img_rgba, alpha

    ys, xs = np.where(bin_mask > 0)
    y_min, y_max = ys.min(), ys.max()
    x_min, x_max = xs.min(), xs.max()

    if (x_min <= 0 or y_min <= 0 or x_max >= w - 1 or y_max >= h - 1):
        return img_rgba, alpha

    x_min_c = max(0, x_min - margin)
    y_min_c = max(0, y_min - margin)
    x_max_c = min(w - 1, x_max + margin)
    y_max_c = min(h - 1, y_max + margin)

    img_cropped = img_rgba.crop((x_min_c, y_min_c, x_max_c + 1, y_max_c + 1))
    alpha_cropped = alpha[y_min_c : y_max_c + 1, x_min_c : x_max_c + 1].copy()

    return img_cropped, alpha_cropped


# -------------------------------------------------------------------------
#  CarveKit / rembg 개별 처리
# -------------------------------------------------------------------------
def remove_bg_carvekit(input_path: str):
    if CARVEKIT_IF is None:
        raise RuntimeError("CarveKit 모델이 로드되지 않았습니다.")
    
    img = Image.open(input_path).convert("RGB")
    result_list = CARVEKIT_IF([img])
    if not result_list:
        raise RuntimeError("CarveKit 결과가 비어 있습니다.")
    out = result_list[0].convert("RGBA")
    alpha = np.array(out.split()[-1], dtype=np.uint8)
    return out, alpha

def remove_bg_rembg(input_path: str):
    if REMBG_SESSION is None:
        raise RuntimeError("rembg 모델이 로드되지 않았습니다.")

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
    return Image.merge("RGBA", (r, g, b, final_a)), final_alpha


# -------------------------------------------------------------------------
#  한 장 처리 (하이브리드)
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
    if log is None: log = print
    base_name, _ = os.path.splitext(os.path.basename(input_path))
    output_name = f"{base_name}.jpg"

    # 1차: CarveKit
    try:
        img_rgba_ck, alpha_ck = remove_bg_carvekit(input_path)
        suspicious_ck = analyze_mask(alpha_ck)
    except Exception as e:
        # log(f"[CarveKit ERROR] {e}") # 로그가 너무 많으면 생략 가능
        img_rgba_ck, alpha_ck = None, None
        suspicious_ck = True

    if not suspicious_ck and img_rgba_ck is not None:
        final_rgba = img_rgba_ck
        final_alpha = alpha_ck
        method = "CarveKit"
        suspicious_final = False
    else:
        # 2차: rembg
        try:
            img_rgba_rm, alpha_rm = remove_bg_rembg(input_path)
            suspicious_rm = analyze_mask(alpha_rm)
            final_rgba = img_rgba_rm
            final_alpha = alpha_rm
            method = "rembg"
            suspicious_final = suspicious_rm
        except Exception as e:
            # log(f"[rembg ERROR] {e}")
            if img_rgba_ck is not None:
                final_rgba = img_rgba_ck
                final_alpha = alpha_ck
                method = "CarveKit(Fallback)"
            else:
                raise RuntimeError("모든 배경제거 시도 실패")
            suspicious_final = True

    result_flag = "need_manual" if suspicious_final else "auto_ok"
    result_category = "수동확인" if suspicious_final else "자동OK"
    subdir = NEED_MANUAL_DIR if suspicious_final else AUTO_OK_DIR

    final_rgba, final_alpha = center_crop_if_safe(final_rgba, final_alpha)

    target_root = os.path.join(output_root, subdir, rel_root)
    os.makedirs(target_root, exist_ok=True)
    output_path = os.path.join(target_root, output_name)

    output_img = render_to_1000x1000_rgb(final_rgba)
    output_img.save(output_path, "JPEG", quality=90, optimize=True)

    alpha_root = os.path.join(output_root, ALPHA_DIR, rel_root)
    os.makedirs(alpha_root, exist_ok=True)
    alpha_path = os.path.join(alpha_root, f"{base_name}.png")
    Image.fromarray(final_alpha, mode="L").save(alpha_path)


    log(f"[{method}] {base_name} -> {result_category}")

    # 매핑 정보 구성 (엑셀 업데이트용 + 필요시 다른 저장에도 사용 가능)
    input_abs = os.path.abspath(input_path)
    output_abs = os.path.abspath(output_path)
    mask_abs = os.path.abspath(alpha_path)
    
    # Windows 경로 호환
    input_rel = os.path.basename(input_path)
    output_rel = os.path.relpath(output_path, output_root).replace("\\", "/")
    mask_rel = os.path.relpath(alpha_path, output_root).replace("\\", "/")

    entry: Dict[str, Any] = {
        "input_abs": input_abs,
        "input_rel": input_rel,
        "output_abs": output_abs,
        "output_rel": output_rel,
        "mask_abs": mask_abs,
        "mask_rel": mask_rel,
        "result_category": result_category,
        "result_flag": result_flag,
        "method": method,
        "preset": preset_name,
        "human_label": None,
        "human_notes": "",
        "ai_label": None,
        "ai_score": None,
        "ai_model": None,
    }
    if extra_meta:
        entry.update(extra_meta)

    if mapping_collector is not None:
        mapping_collector.append(entry)

    # 엑셀 업데이트를 위해 entry 리턴
    return entry


# -------------------------------------------------------------------------
#  ToolTip Class (새로 추가됨)
# -------------------------------------------------------------------------
class CreateToolTip(object):
    """
    위젯에 마우스를 올리면 툴팁을 띄워주는 클래스
    """
    def __init__(self, widget, text='widget info'):
        self.waittime = 500     # miliseconds
        self.wraplength = 300   # pixels
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
        self.tw.wm_overrideredirect(True)
        self.tw.wm_geometry("+%d+%d" % (x, y))
        label = tk.Label(self.tw, text=self.text, justify='left',
                       background="#ffffe0", relief='solid', borderwidth=1,
                       wraplength = self.wraplength, font=("Malgun Gothic", 9))
        label.pack(ipadx=1)

    def hidetip(self):
        tw = self.tw
        self.tw= None
        if tw:
            tw.destroy()
# -------------------------------------------------------------------------
#  GUI App
# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
#  GUI App (UI 로직 전면 수정)
# -------------------------------------------------------------------------
class HybridBGApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(f"AI 배경제거 자동화 v2.0 - Device: {DEVICE}")
        self.root.geometry("950x800")
        
        # 기본 스타일 설정
        self.setup_styles()

        self.excel_path_var = tk.StringVar()
        self.output_path_var = tk.StringVar()
        self.preset_var = tk.StringVar(value="균형")
        self.auto_output_var = tk.BooleanVar(value=True)

        self.ui_queue: "queue.Queue[tuple]" = queue.Queue()
        self.job_thread: Optional[threading.Thread] = None
        self.job_running = False
        self.stop_requested = False 

        self.df: Optional[pd.DataFrame] = None
        self.excel_path: Optional[str] = None
        self.mapping_rows: List[Dict[str, Any]] = []

        self._build_ui()
        self.root.after(100, self.poll_queue)
        self.apply_preset("균형")

    def setup_styles(self):
        # 폰트 설정
        self.default_font = tkfont.Font(family="Malgun Gothic", size=10)
        self.bold_font = tkfont.Font(family="Malgun Gothic", size=10, weight="bold")
        self.header_font = tkfont.Font(family="Malgun Gothic", size=16, weight="bold")
        self.mono_font = tkfont.Font(family="Consolas", size=9)

        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        # 공통 배경색
        bg_color = "#f5f5f5"
        self.root.configure(bg=bg_color)
        
        # Frame 스타일
        style.configure("Main.TFrame", background=bg_color)
        style.configure("Card.TFrame", background="white", relief="solid", borderwidth=1)
        style.configure("TLabelframe", background="white", font=self.bold_font)
        style.configure("TLabelframe.Label", background="white", font=self.bold_font, foreground="#333333")

        # Label 스타일
        style.configure("TLabel", background="white", font=self.default_font, foreground="#333333")
        style.configure("Header.TLabel", background=bg_color, font=self.header_font, foreground="#2c3e50")
        style.configure("Sub.TLabel", background=bg_color, font=self.default_font, foreground="#7f8c8d")
        
        # Button 스타일 (ttk 버튼은 색상 커스텀이 제한적이므로 기본값 유지하되 폰트만 설정)
        style.configure("TButton", font=self.default_font, padding=6)
        
        # Progressbar
        style.configure("Horizontal.TProgressbar", thickness=20)


    def _build_ui(self):
        # 메인 컨테이너 (여백 확보)
        main_container = ttk.Frame(self.root, style="Main.TFrame")
        main_container.pack(fill="both", expand=True, padx=20, pady=20)

        # 1. 헤더 섹션
        header_frame = ttk.Frame(main_container, style="Main.TFrame")
        header_frame.pack(fill="x", pady=(0, 15))
        
        title_lbl = ttk.Label(header_frame, text="SHOPPING MALL IMAGE PROCESSOR", style="Header.TLabel")
        title_lbl.pack(anchor="w")
        
        desc_lbl = ttk.Label(header_frame, text="Stage1 썸네일 맵핑 엑셀을 기반으로 배경을 제거하고 결과를 자동 기록합니다.", style="Sub.TLabel")
        desc_lbl.pack(anchor="w", pady=(5, 0))

        # 2. 설정 카드 (입력/출력/옵션)
        settings_frame = ttk.LabelFrame(main_container, text=" 작업 설정 ", style="TLabelframe")
        settings_frame.pack(fill="x", pady=10, ipady=5)

        # 그리드 설정
        settings_frame.columnconfigure(1, weight=1)

        # [입력 엑셀]
        lbl_excel = ttk.Label(settings_frame, text="입력 엑셀:")
        lbl_excel.grid(row=0, column=0, padx=15, pady=10, sticky="e")
        
        entry_excel = ttk.Entry(settings_frame, textvariable=self.excel_path_var, font=self.default_font)
        entry_excel.grid(row=0, column=1, padx=5, pady=10, sticky="ew")
        CreateToolTip(entry_excel, "배경 제거를 수행할 이미지 경로가 담긴 엑셀 파일을 선택하세요.")
        
        btn_excel = ttk.Button(settings_frame, text="파일 찾기", command=self.select_excel)
        btn_excel.grid(row=0, column=2, padx=15, pady=10)

        # [출력 폴더]
        lbl_out = ttk.Label(settings_frame, text="출력 경로:")
        lbl_out.grid(row=1, column=0, padx=15, pady=10, sticky="e")
        
        entry_out = ttk.Entry(settings_frame, textvariable=self.output_path_var, font=self.default_font)
        entry_out.grid(row=1, column=1, padx=5, pady=10, sticky="ew")
        
        btn_out = ttk.Button(settings_frame, text="폴더 변경", command=self.select_output)
        btn_out.grid(row=1, column=2, padx=15, pady=10)
        
        chk_auto = ttk.Checkbutton(settings_frame, text="자동 생성 (엑셀위치 기준)", variable=self.auto_output_var, style="TCheckbutton")
        chk_auto.grid(row=2, column=1, padx=5, sticky="w")
        CreateToolTip(chk_auto, "체크 시 엑셀 파일이 있는 폴더에 '_bg' 폴더를 자동으로 생성합니다.")

        # 구분선
        ttk.Separator(settings_frame, orient="horizontal").grid(row=3, column=0, columnspan=3, sticky="ew", padx=10, pady=15)

        # [옵션 & 프리셋]
        lbl_preset = ttk.Label(settings_frame, text="품질 옵션:")
        lbl_preset.grid(row=4, column=0, padx=15, pady=10, sticky="e")
        
        preset_box_frame = ttk.Frame(settings_frame, style="Main.TFrame") # 배경 흰색 유지를 위해
        preset_box_frame.grid(row=4, column=1, sticky="w", padx=5)
        
        self.preset_combo = ttk.Combobox(preset_box_frame, textvariable=self.preset_var, values=list(QUALITY_PRESETS.keys()), state="readonly", width=12, font=self.default_font)
        self.preset_combo.pack(side="left")
        self.preset_combo.bind("<<ComboboxSelected>>", self.on_preset_changed)
        CreateToolTip(self.preset_combo, "배경 제거 민감도를 설정합니다.\n- 공격적: 많이 지움\n- 보수적: 안전하게 남김")
        
        self.preset_desc_label = ttk.Label(preset_box_frame, text="", foreground="#666666", font=self.default_font)
        self.preset_desc_label.pack(side="left", padx=10)


        # 3. 버튼 영역 (Start / Stop)
        # 중요 버튼은 ttk보다 tk.Button이 색상 커스텀에 유리함
        btn_frame = ttk.Frame(main_container, style="Main.TFrame")
        btn_frame.pack(fill="x", pady=15)

        self.start_button = tk.Button(btn_frame, text="▶ 작업 시작", command=self.start_processing, 
                                      bg="#2ecc71", fg="white", font=("Malgun Gothic", 11, "bold"), 
                                      relief="flat", cursor="hand2", height=2, width=15) # width를 여기로 이동
        # self.start_button.pack(side="right", padx=5, fill="x", expand=False, width=15) # ERROR 였던 부분
        self.start_button.pack(side="right", padx=5, fill="x", expand=False)
        CreateToolTip(self.start_button, "설정된 엑셀 파일을 읽어 배경 제거 작업을 시작합니다.")

        self.stop_button = tk.Button(btn_frame, text="■ 작업 중지", command=self.stop_processing, 
                                     bg="#e74c3c", fg="white", font=("Malgun Gothic", 11, "bold"), 
                                     relief="flat", cursor="hand2", height=2, state="disabled", width=15) # width를 여기로 이동
        self.stop_button.pack(side="right", padx=5, fill="x", expand=False)
        CreateToolTip(self.stop_button, "현재 진행 중인 작업을 안전하게 중단합니다.")


        # 4. 상태 및 프로그레스 바
        status_frame = ttk.Frame(main_container, style="Main.TFrame")
        status_frame.pack(fill="x", pady=5)

        # 정보 라벨들을 좌우로 배치
        info_frame = ttk.Frame(status_frame, style="Main.TFrame")
        info_frame.pack(fill="x", pady=(0, 5))        
                
        self.status_label = ttk.Label(info_frame, text="대기 중...", font=("Malgun Gothic", 10, "bold"), background="#f5f5f5")
        self.status_label.pack(side="left")
        
        self.time_label = ttk.Label(info_frame, text="-", font=("Malgun Gothic", 9), foreground="#7f8c8d", background="#f5f5f5")
        self.time_label.pack(side="right")

        self.progress_bar = ttk.Progressbar(status_frame, maximum=100, style="Horizontal.TProgressbar")
        self.progress_bar.pack(fill="x")


        # 5. 로그 영역
        log_frame = ttk.LabelFrame(main_container, text=" 처리 로그 ", style="TLabelframe")
        log_frame.pack(fill="both", expand=True, pady=(10, 0))
        
        self.log_text = scrolledtext.ScrolledText(log_frame, state="disabled", height=10, 
                                                  font=self.mono_font, bg="#fdfdfd", bd=0)
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)
        
        # 초기 로그
        self.log(f"System Ready. Device: '{DEVICE}'")
        if DEVICE == 'cuda':
            self.log(">>> NVIDIA GPU 가속이 활성화되었습니다.")
        else:
            self.log(">>> CPU 모드로 동작합니다. (속도가 느릴 수 있습니다)")

    def log(self, msg):
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def apply_preset(self, name):
        cfg = QUALITY_PRESETS.get(name)
        if cfg:
            set_quality_config(cfg)
            self.preset_desc_label.config(text=PRESET_DESCRIPTIONS.get(name, ""))

    def on_preset_changed(self, event):
        self.apply_preset(self.preset_var.get())

    def select_excel(self):
        path = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx;*.xls"), ("All", "*.*")])
        if path: self.excel_path_var.set(path)

    def select_output(self):
        path = filedialog.askdirectory()
        if path:
            self.output_path_var.set(path)
            self.auto_output_var.set(False)

    def start_processing(self):
        if self.job_running: return
        
        excel_path = self.excel_path_var.get().strip()
        if not excel_path or not os.path.exists(excel_path):
            messagebox.showerror("오류", "엑셀 파일을 확인해주세요.")
            return

        try:
            df = pd.read_excel(excel_path, dtype=str)
        except Exception as e:
            messagebox.showerror("오류", f"엑셀 읽기 실패: {e}")
            return

        if THUMB_COL_NAME not in df.columns:
            messagebox.showerror("오류", f"'{THUMB_COL_NAME}' 컬럼이 없습니다.")
            return

        # 엑셀 DataFrame / 경로를 인스턴스에 보관
        self.df = df
        self.excel_path = excel_path

        # 결과용 컬럼이 없으면 생성 (빈 문자열로 초기화)
        for col in [COL_IMG_OUT, COL_HUMAN_LABEL, COL_HUMAN_NOTES, COL_AI_LABEL]:
            if col not in df.columns:
                df[col] = ""


        excel_dir = os.path.dirname(excel_path)
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

            # 절대/상대 경로 처리
            if os.path.isabs(s):
                full_path = s
            else:
                full_path = os.path.normpath(os.path.join(excel_dir, s))

            if not os.path.exists(full_path):
                skipped_missing_file += 1
                # 어떤 행이 빠졌는지 로그 남기기
                self.ui_queue.put(
                    ("log", f"[SKIP] 행 {idx+1}: 이미지 파일을 찾을 수 없습니다: {full_path}")
                )
                continue

            meta = {"excel_row_index": idx, "excel_file": os.path.abspath(excel_path)}
            for k, v in row.items():
                if pd.isna(v):
                    vv = None
                else:
                    vv = str(v)
                meta[f"excel_{k}"] = vv

            items.append({"image_path": full_path, "meta": meta})

        if not items:
            msg = "처리할 이미지가 없습니다.\n"
            if skipped_no_path:
                msg += f"- 썸네일경로 비어 있음: {skipped_no_path}행\n"
            if skipped_missing_file:
                msg += f"- 이미지 파일 없음: {skipped_missing_file}행\n"
            messagebox.showwarning("주의", msg)
            return

        total = len(items)
        # 시작 전에 한 번 로그로 요약
        self.log(
            f"유효 썸네일 행 수: {total}개 / "
            f"썸네일경로 비어 있음: {skipped_no_path}행 / "
            f"이미지 없음: {skipped_missing_file}행"
        )


        # 출력 경로
        if self.auto_output_var.get() or not self.output_path_var.get():
            base = os.path.splitext(os.path.basename(excel_path))[0]
            out_root = os.path.join(excel_dir, base + "_bg")
        else:
            out_root = self.output_path_var.get()

        self.job_running = True
        self.stop_requested = False
        self.start_button.config(state="disabled")
        self.stop_button.config(state="normal") # 중단 버튼 활성화
        self.mapping_rows = []
        
        preset_name = self.preset_var.get()
        
        # 스레드 시작
        t = threading.Thread(
            target=self.worker,
            args=(out_root, items, preset_name),
            daemon=True
        )
        self.job_thread = t
        t.start()

    def stop_processing(self):
        if self.job_running:
            if messagebox.askyesno("확인", "작업을 중단하시겠습니까?"):
                self.stop_requested = True
                self.log("!!! 작업 중단 요청됨 (현재 이미지 완료 후 멈춤) !!!")

    def worker(self, out_root, items, preset_name):
        # 1. 모델 로딩
        try:
            load_models_if_needed(log_func=lambda m: self.ui_queue.put(("log", m)))
        except Exception as e:
            self.ui_queue.put(("log", f"[FATAL] 모델 로딩 실패: {e}"))
            self.ui_queue.put(("done", out_root, False))
            return

        start_ts = time.time()
        total = len(items)
        os.makedirs(out_root, exist_ok=True)

        self.ui_queue.put(("init", total, out_root, start_ts))

        stopped_by_user = False  # ← 추가

        for idx, item in enumerate(items, start=1):
            if self.stop_requested:
                stopped_by_user = True
                self.ui_queue.put(("log", ">>> 사용자에 의해 작업이 중단되었습니다."))
                break

            if idx % 50 == 0:
                gc.collect()

            input_path = item["image_path"]
            try:
                entry = process_one_image(
                    input_path,
                    out_root,
                    log=lambda m: self.ui_queue.put(("log", m)),
                    preset_name=preset_name,
                    mapping_collector=None,          # 더 이상 내부 리스트는 안 씀
                    extra_meta=item["meta"]
                )

                # ---- 여기서 엑셀에 결과 경로 기록 ----
                if entry is not None and self.df is not None:
                    row_idx = entry.get("excel_row_index")
                    out_abs = entry.get("output_abs")
                    if row_idx is not None and out_abs:
                        try:
                            self.df.at[row_idx, COL_IMG_OUT] = out_abs
                        except Exception as e:
                            self.ui_queue.put(("log", f"[WARN] 엑셀 업데이트 실패 (행 {row_idx}): {e}"))
                # ---------------------------------------

            except Exception as e:
                self.ui_queue.put(("log", f"[SKIP] {os.path.basename(input_path)} 오류: {e}"))

            elapsed = time.time() - start_ts
            avg = elapsed / idx
            remain = avg * (total - idx)
            self.ui_queue.put(("progress", idx, total, elapsed, remain))


        # 더 이상 별도 매핑 파일(bg_mapping.json/csv)은 저장하지 않음
        # 대신 엑셀 파일에 결과 컬럼을 반영
        if self.df is not None and self.excel_path:
            try:
                self.df.to_excel(self.excel_path, index=False)
                self.ui_queue.put(("log", f"[저장] 엑셀 업데이트 완료: {self.excel_path}"))
            except Exception as e:
                self.ui_queue.put(("log", f"[경고] 엑셀 저장 실패: {e}"))

        # 중단 여부에 따라 completed 플래그 변경
        self.ui_queue.put(("done", out_root, not stopped_by_user))



    def save_mapping(self, out_root):
        json_path = os.path.join(out_root, "bg_mapping.json")
        csv_path = os.path.join(out_root, "bg_mapping.csv")
        
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(self.mapping_rows, f, ensure_ascii=False, indent=2)

            keys = set().union(*(d.keys() for d in self.mapping_rows))
            # 순서 정렬
            priority = ["input_abs", "result_category", "method"]
            fieldnames = [k for k in priority if k in keys] + sorted([k for k in keys if k not in priority])
            
            with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.mapping_rows)
                
            self.ui_queue.put(("log", f"[저장] 매핑 파일 저장 완료 ({len(self.mapping_rows)}건)"))
        except Exception as e:
            self.ui_queue.put(("log", f"[오류] 매핑 저장 실패: {e}"))

    def poll_queue(self):
        try:
            while True:
                msg = self.ui_queue.get_nowait()
                kind = msg[0]
                
                if kind == "log":
                    self.log(msg[1])
                elif kind == "init":
                    total = msg[1]
                    path = msg[2]
                    start_ts = msg[3]

                    start_dt = datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
                    self.status_label.config(text=f"작업 시작: 총 {total}개")
                    self.time_label.config(text=f"시작 시간: {start_dt}")
                    self.log(f"[INFO] 시작 시간: {start_dt}")

                elif kind == "progress":
                    idx, total, elap, rem = msg[1:]
                    pct = (idx / total) * 100

                    now = time.time()
                    eta = now + rem
                    eta_str = datetime.fromtimestamp(eta).strftime("%Y-%m-%d %H:%M:%S")

                    self.progress_bar["value"] = pct
                    self.status_label.config(text=f"진행: {idx}/{total} ({pct:.1f}%)")

                    self.time_label.config(
                        text=f"경과: {format_time(elap)} / 남은: {format_time(rem)} / 종료예상: {eta_str}"
                    )


                elif kind == "done":
                    out_root, completed = msg[1], msg[2]
                    self.job_running = False
                    self.start_button.config(state="normal")
                    self.stop_button.config(state="disabled") # 중단 버튼 비활성화
                    
                    if completed:
                        if messagebox.askyesno("완료", "작업이 끝났습니다. 폴더를 여시겠습니까?"):
                            open_folder(out_root)
                    else:
                        messagebox.showwarning("중단", "작업이 중단되었거나 오류가 발생했습니다.")
                        
        except queue.Empty:
            pass
        self.root.after(100, self.poll_queue)

def run_gui():
    root = tk.Tk()
    app = HybridBGApp(root)
    root.mainloop()

if __name__ == "__main__":
    run_gui()