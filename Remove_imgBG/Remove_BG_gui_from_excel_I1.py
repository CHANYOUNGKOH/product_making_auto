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
from typing import Any, Dict, List, Optional, Callable
import re
import numpy as np
import cv2
from PIL import Image
import pandas as pd
import torch  # GPU 체크용
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# 프로세스 우선순위 조정 (Windows)
try:
    if sys.platform == "win32":
        import win32api
        import win32process
        import win32con
        # 프로세스 우선순위를 낮춤 (다른 작업에 영향을 최소화)
        current_process = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, os.getpid())
        win32process.SetPriorityClass(current_process, win32process.BELOW_NORMAL_PRIORITY_CLASS)
        win32api.CloseHandle(current_process)
except ImportError:
    # win32api가 없으면 무시 (선택적 의존성)
    pass
except Exception:
    # 우선순위 설정 실패는 무시
    pass

# --- CarveKit ------------------------------------------------------------
from carvekit.api.high import HiInterface

# --- rembg ---------------------------------------------------------------
from rembg import remove, new_session

# Tkinter GUI
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from tkinter import font as tkfont

def get_root_filename(filename):
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T3_I1.xlsx -> 아디다스.xlsx
    예: 나이키_T0_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T0_I0_T1_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
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
    suffixes = ["_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_with_images", "_bg"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext

def get_next_version_path(current_path, task_type='image'):
    """
    현재 파일명을 분석해서 다음 단계 파일명 생성 (I 버전 증가)
    예: Item_T0_I0.xlsx -> (image작업) -> Item_T0_I1.xlsx
    """
    dir_name = os.path.dirname(current_path)
    base_name = os.path.basename(current_path)
    name_only, ext = os.path.splitext(base_name)

    # 현재 버전 파싱
    pattern = r"_T(\d+)_I(\d+)$"
    match = re.search(pattern, name_only)

    if match:
        current_t = int(match.group(1))
        current_i = int(match.group(2))
        original_name = name_only[:match.start()]
    else:
        # 버전 정보가 없으면 초기 상태 T0_I0 가정
        current_t = 0
        current_i = 0
        original_name = name_only

    # 버전 업 (이미지 작업이므로 I 증가)
    if task_type == 'image':
        new_t = current_t
        new_i = current_i + 1
    elif task_type == 'text':
        new_t = current_t + 1
        new_i = current_i
    else:
        return current_path

    new_filename = f"{original_name}_T{new_t}_I{new_i}{ext}"
    return os.path.join(dir_name, new_filename)

def get_i1_output_path(input_path: str) -> str:
    """
    입력 파일명을 분석해서 항상 I1 버전으로 출력 파일명 생성
    예: Item_T0_I0.xlsx -> Item_T0_I1.xlsx
        Item_T1_I1.xlsx -> Item_T1_I1.xlsx (이미 I1이면 그대로)
    """
    dir_name = os.path.dirname(input_path)
    base_name = os.path.basename(input_path)
    name_only, ext = os.path.splitext(base_name)

    # 현재 버전 파싱
    pattern = r"_T(\d+)_I(\d+)$"
    match = re.search(pattern, name_only)

    if match:
        current_t = int(match.group(1))
        original_name = name_only[:match.start()]
    else:
        # 버전 정보가 없으면 T0 가정
        current_t = 0
        original_name = name_only

    # 항상 I1로 고정
    new_filename = f"{original_name}_T{current_t}_I1{ext}"
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

# -------------------------------------------------------------------------
#  전역 변수 (모델 지연 로딩을 위해 None으로 초기화)
# -------------------------------------------------------------------------
CARVEKIT_IF = None
REMBG_SESSION = None

# GPU 감지 및 진단
def detect_device():
    """GPU 사용 가능 여부를 확인하고 상세 정보를 반환"""
    device = "cpu"
    info = []
    
    # PyTorch CUDA 지원 여부 확인
    if not torch.cuda.is_available():
        info.append("❌ PyTorch가 CUDA를 사용할 수 없습니다.")
        
        # 상세 원인 파악
        if not hasattr(torch.version, 'cuda') or torch.version.cuda is None:
            info.append("   → PyTorch가 CUDA 버전으로 빌드되지 않았습니다.")
            info.append("   → 해결: CUDA 지원 PyTorch를 설치하세요.")
            info.append(f"   → 예: pip install torch torchvision --index-url https://download.pytorch.org/whl/cu121")
        else:
            info.append(f"   → PyTorch CUDA 버전: {torch.version.cuda}")
            info.append("   → CUDA 드라이버와 호환되지 않을 수 있습니다.")
        
        # CUDA 드라이버 확인
        try:
            import subprocess
            result = subprocess.run(['nvidia-smi'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                info.append("   → NVIDIA GPU 드라이버는 설치되어 있습니다.")
                # CUDA 버전 추출
                for line in result.stdout.split('\n'):
                    if 'CUDA Version:' in line:
                        cuda_ver = line.split('CUDA Version:')[1].strip().split()[0]
                        info.append(f"   → 드라이버 CUDA 버전: {cuda_ver}")
            else:
                info.append("   → nvidia-smi 명령을 실행할 수 없습니다.")
        except FileNotFoundError:
            info.append("   → nvidia-smi를 찾을 수 없습니다 (NVIDIA 드라이버 미설치 가능).")
        except Exception as e:
            info.append(f"   → nvidia-smi 확인 중 오류: {e}")
    else:
        device = "cuda"
        info.append("✅ PyTorch CUDA 사용 가능")
        info.append(f"   → PyTorch CUDA 버전: {torch.version.cuda}")
        info.append(f"   → GPU 개수: {torch.cuda.device_count()}")
        for i in range(torch.cuda.device_count()):
            info.append(f"   → GPU {i}: {torch.cuda.get_device_name(i)}")
            props = torch.cuda.get_device_properties(i)
            info.append(f"      메모리: {props.total_memory / 1024**3:.1f} GB")
    
    return device, info

DEVICE, DEVICE_INFO = detect_device()

# -------------------------------------------------------------------------
#  상수: 엑셀에서 사용할 썸네일 경로 컬럼명
# -------------------------------------------------------------------------
THUMB_COL_NAME = "썸네일경로"
# 엑셀에 쓸 결과 컬럼명들
COL_IMG_OUT = "IMG_S1_누끼"
COL_IMG_OUT_PNG = "IMG_S1_누끼_png"  # PNG 경로 컬럼
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
PNG_DIR = "_png"  # PNG 출력용 폴더

# 타임아웃 설정 (초) - 모든 이미지가 처리되도록 충분한 시간 확보
TIMEOUT_CARVEKIT_BASE = 60  # CarveKit 기본 타임아웃 (충분한 시간 확보)
TIMEOUT_REMBG_BASE = 60     # rembg 기본 타임아웃 (충분한 시간 확보)
TIMEOUT_CARVEKIT_MAX = 120  # CarveKit 최대 타임아웃 (재시도 시 증가)
TIMEOUT_REMBG_MAX = 120     # rembg 최대 타임아웃 (재시도 시 증가)
MAX_RETRIES = 1             # 최대 재시도 횟수 (타임아웃 시간이 길어서 1회로 감소)
MODEL_RELOAD_INTERVAL = 30  # N장마다 모델 재로딩 (GPU 메모리 누적 방지, 안정성과 성능의 균형)

# 타임아웃 발생 시 모델 재로딩 플래그 (전역 변수)
MODEL_RELOAD_NEEDED = False  # 타임아웃 발생 시 True로 설정되어 worker에서 모델 재로딩

# GPU 메모리 관리 설정 (더 적극적인 정리)
GPU_MEMORY_WARNING_THRESHOLD = 0.70  # GPU 메모리 사용률 70% 이상 시 경고 및 정리
GPU_MEMORY_CRITICAL_THRESHOLD = 0.85  # GPU 메모리 사용률 85% 이상 시 강제 정리
GPU_MEMORY_FORCE_CPU_THRESHOLD = 0.90  # GPU 메모리 사용률 90% 이상 시 CPU 모드 전환 고려
CLEAR_CACHE_INTERVAL = 10  # N장마다 1회 GPU 캐시 정리 (성능과 안정성 균형)

# CPU 사용률 제한 설정 제거 (잘 작동하는 버전에는 없음)

# 성능 최적화 설정
MAX_INPUT_SIZE = 2000  # 배경 제거 전 최대 이미지 크기 (큰 이미지 사전 리사이즈)

# 병렬 처리 설정
# 주의: GPU 모델(CarveKit, rembg)은 thread-safe하지 않아 병렬 처리 시 충돌 발생 가능
# GPU 메모리 과부하 방지를 위해 기본적으로 비활성화 (순차 처리 권장)
MAX_WORKERS = 1  # 동시 처리 스레드 수 (1=순차 처리, GPU 안정성 우선)
PROGRESS_SAVE_INTERVAL = 10  # N개마다 진행 상황 저장
EXCEL_SAVE_INTERVAL = 5  # N개마다 엑셀 파일 저장 (더 자주 저장하여 중단 시 손실 방지)

# 디버그 로그 설정
DEBUG_LOG_ENABLED = True
DEBUG_LOG_FILE = None  # 작업 시작 시 설정됨

# 진행 상황 파일명
PROGRESS_FILE_NAME = "_bg_remove_progress.json"


# -------------------------------------------------------------------------
#  GPU 메모리 관리 유틸
# -------------------------------------------------------------------------
def clear_gpu_cache(aggressive=False, skip_sync=False):
    """
    GPU 메모리 캐시 정리 (최적화: 빠른 정리)
    
    Args:
        aggressive: True일 경우 더 적극적으로 정리 (타임아웃 발생 시 사용)
        skip_sync: True일 경우 synchronize() 호출 생략 (타임아웃 발생 시 백그라운드 스레드 대기 방지)
    """
    if torch.cuda.is_available():
        try:
            # Python GC 먼저 실행 (참조 해제)
            import gc
            gc.collect()
            
            # PyTorch CUDA 캐시 정리
            torch.cuda.empty_cache()
            if not skip_sync:
                torch.cuda.synchronize()  # 타임아웃 발생 시에는 skip
            
            if aggressive:
                # 적극적 정리: 빠르게 3번 반복 (타임아웃 발생 시 더 적극적으로)
                for _ in range(3):
                    torch.cuda.empty_cache()
                    if not skip_sync:
                        torch.cuda.synchronize()  # 타임아웃 발생 시에는 skip
                    # 대기 시간 제거 (즉시 다음 정리 실행)
                
                # 추가 정리: Python GC 한 번 더 실행
                gc.collect()
        except Exception:
            pass

def check_gpu_memory():
    """GPU 메모리 사용량 확인 (MB 단위)"""
    if not torch.cuda.is_available():
        return None, None, None
    try:
        allocated = torch.cuda.memory_allocated() / 1024**2  # MB
        reserved = torch.cuda.memory_reserved() / 1024**2   # MB
        total = torch.cuda.get_device_properties(0).total_memory / 1024**2  # MB
        return allocated, reserved, total
    except Exception:
        return None, None, None

def check_gpu_memory_usage():
    """GPU 메모리 사용률 확인 (0.0 ~ 1.0)"""
    allocated, reserved, total = check_gpu_memory()
    if total is None or total == 0:
        return None
    return reserved / total if reserved else 0.0

# -------------------------------------------------------------------------
#  모델 로딩 함수 (지연 로딩 + GPU 메모리 관리)
#  [개선] OOM 방지를 위해 한 번에 하나의 모델만 GPU에 로드
# -------------------------------------------------------------------------
def unload_other_model(model_to_keep: str, log_func=print):
    """
    지정된 모델을 제외한 다른 모델을 해제하여 GPU 메모리 확보
    
    Args:
        model_to_keep: 유지할 모델 ("carvekit" 또는 "rembg")
    """
    global CARVEKIT_IF, REMBG_SESSION
    
    if model_to_keep == "carvekit":
        # rembg 해제
        if REMBG_SESSION is not None:
            log_func(f"[System] rembg 모델 해제 중 (CarveKit 사용을 위해)...")
            try:
                del REMBG_SESSION
                REMBG_SESSION = None
                clear_gpu_cache(aggressive=True)
                gc.collect()
                time.sleep(0.3)
                log_func(f"[System] rembg 모델 해제 완료")
            except Exception as e:
                log_func(f"[WARN] rembg 해제 중 오류: {e}")
    elif model_to_keep == "rembg":
        # CarveKit 해제
        if CARVEKIT_IF is not None:
            log_func(f"[System] CarveKit 모델 해제 중 (rembg 사용을 위해)...")
            try:
                del CARVEKIT_IF
                CARVEKIT_IF = None
                clear_gpu_cache(aggressive=True)
                gc.collect()
                time.sleep(0.3)
                log_func(f"[System] CarveKit 모델 해제 완료")
            except Exception as e:
                log_func(f"[WARN] CarveKit 해제 중 오류: {e}")

def load_carvekit_if_needed(log_func=print, force_cpu=False):
    """CarveKit 모델만 로드 (rembg는 해제) - 안정성 우선"""
    global CARVEKIT_IF, REMBG_SESSION, DEVICE
    
    # CarveKit이 이미 로드되어 있으면 재사용 (다른 모델만 해제)
    if CARVEKIT_IF is not None:
        # rembg만 해제 (이미 CarveKit이 로드되어 있으므로)
        if REMBG_SESSION is not None:
            unload_other_model("carvekit", log_func)
        return  # 이미 로드되어 있으면 재사용
    
    # CarveKit이 없으면 rembg 해제 후 로딩
    # rembg 해제 (GPU 메모리 확보, OOM 방지)
    unload_other_model("carvekit", log_func)
    
    # GPU 메모리 정리
    clear_gpu_cache(aggressive=True)
    gc.collect()
    time.sleep(0.5)
    
    # GPU 메모리 사용률 확인
    if DEVICE == "cuda" and not force_cpu:
        allocated, reserved, total = check_gpu_memory()
        if allocated is not None and total is not None:
            usage = check_gpu_memory_usage()
            if usage is not None:
                log_func(f"[System] GPU 메모리 상태: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체 ({usage*100:.1f}% 사용)")
                if usage >= GPU_MEMORY_FORCE_CPU_THRESHOLD:
                    log_func(f"[WARN] GPU 메모리 사용률이 {usage*100:.1f}%로 높습니다. CPU 모드로 전환합니다.")
                    force_cpu = True
                elif usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                    log_func(f"[WARN] GPU 메모리 사용률이 {usage*100:.1f}%로 높습니다. 추가 정리를 수행합니다.")
                    clear_gpu_cache(aggressive=True)
                    gc.collect()
                    time.sleep(0.3)
    
    use_device = "cpu" if force_cpu else DEVICE
    if force_cpu and DEVICE == "cuda":
        log_func(f"[System] GPU 메모리 부족으로 CPU 모드로 전환합니다.")

    # CarveKit 로딩
    if CARVEKIT_IF is None:
        log_func(f"[System] CarveKit 모델 로딩 중... (Device: {use_device})")
        
        params = inspect.signature(HiInterface).parameters
        kwargs: Dict[str, Any] = {}

        if "object_type" in params:
            kwargs["object_type"] = "object"
        if "segm_model" in params:
            kwargs["segm_model"] = "tracer_b7"
        if "matting_model" in params:
            kwargs["matting_model"] = "fba"

        if "device" in params:
            kwargs["device"] = use_device
        else:
            if "segm_device" in params:
                kwargs["segm_device"] = use_device
            if "matting_device" in params:
                kwargs["matting_device"] = use_device

        if "batch_size" in params:
            kwargs["batch_size"] = 1
        
        try:
            CARVEKIT_IF = HiInterface(**kwargs)
            if use_device == "cuda":
                allocated, reserved, total = check_gpu_memory()
                if allocated is not None:
                    log_func(f"[System] CarveKit 로딩 완료 | GPU 메모리: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체")
        except RuntimeError as e:
            if "out of memory" in str(e).lower() or "cuda" in str(e).lower():
                log_func(f"[WARN] GPU 메모리 부족 감지. CPU 모드로 재시도합니다...")
                clear_gpu_cache()
                if not force_cpu:
                    return load_carvekit_if_needed(log_func, force_cpu=True)
            log_func(f"[Error] CarveKit 로딩 실패: {e}")
            raise e
        except Exception as e:
            log_func(f"[Error] CarveKit 로딩 실패: {e}")
            raise e

def load_rembg_if_needed(log_func=print, force_cpu=False):
    """rembg 모델만 로드 (CarveKit은 해제) - 안정성 우선"""
    global CARVEKIT_IF, REMBG_SESSION, DEVICE
    
    # rembg가 이미 로드되어 있으면 재사용 (다른 모델만 해제)
    if REMBG_SESSION is not None:
        # CarveKit만 해제 (이미 rembg가 로드되어 있으므로)
        if CARVEKIT_IF is not None:
            unload_other_model("rembg", log_func)
        return  # 이미 로드되어 있으면 재사용
    
    # rembg가 없으면 CarveKit 해제 후 로딩
    # CarveKit 해제 (GPU 메모리 확보, OOM 방지)
    unload_other_model("rembg", log_func)
    
    # GPU 메모리 정리
    clear_gpu_cache(aggressive=True)
    gc.collect()
    time.sleep(0.5)
    
    # GPU 메모리 사용률 확인
    if DEVICE == "cuda" and not force_cpu:
        allocated, reserved, total = check_gpu_memory()
        if allocated is not None and total is not None:
            usage = check_gpu_memory_usage()
            if usage is not None:
                log_func(f"[System] GPU 메모리 상태: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체 ({usage*100:.1f}% 사용)")
                if usage >= GPU_MEMORY_FORCE_CPU_THRESHOLD:
                    log_func(f"[WARN] GPU 메모리 사용률이 {usage*100:.1f}%로 높습니다. CPU 모드로 전환합니다.")
                    force_cpu = True
                elif usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                    log_func(f"[WARN] GPU 메모리 사용률이 {usage*100:.1f}%로 높습니다. 추가 정리를 수행합니다.")
                    clear_gpu_cache(aggressive=True)
                    gc.collect()
                    time.sleep(0.3)
    
    use_device = "cpu" if force_cpu else DEVICE
    if force_cpu and DEVICE == "cuda":
        log_func(f"[System] GPU 메모리 부족으로 CPU 모드로 전환합니다.")

    # rembg 로딩
    if REMBG_SESSION is None:
        log_func(f"[System] rembg 모델 로딩 중... (Device: {use_device})")
        if use_device == "cuda":
            providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
        else:
            providers = ["CPUExecutionProvider"]
        try:
            REMBG_SESSION = new_session("birefnet-general", providers=providers)
            if use_device == "cuda":
                allocated, reserved, total = check_gpu_memory()
                if allocated is not None:
                    log_func(f"[System] rembg 로딩 완료 | GPU 메모리: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체")
        except Exception as e:
            error_str = str(e).lower()
            if "cublas" in error_str or "cuda" in error_str or "out of memory" in error_str:
                log_func(f"[WARN] GPU 오류 감지. CPU 모드로 재시도합니다...")
                clear_gpu_cache()
                if not force_cpu:
                    return load_rembg_if_needed(log_func, force_cpu=True)
            log_func(f"[Error] rembg 로딩 실패: {e}")
            raise e

# 하위 호환성을 위한 함수
def load_models_if_needed(log_func=print, force_cpu=False):
    """
    [주의] 하위 호환성을 위해 유지되지만, 
    OOM 방지를 위해 load_carvekit_if_needed() 또는 load_rembg_if_needed() 사용 권장
    """
    # 기본적으로 CarveKit만 로드 (rembg는 필요할 때 로드)
    load_carvekit_if_needed(log_func, force_cpu)

def load_models_if_needed_old(log_func=print, force_cpu=False):
    """
    [구버전] 두 모델을 모두 로드하는 함수 (OOM 위험)
    하위 호환성을 위해 유지
    """
    global CARVEKIT_IF, REMBG_SESSION, DEVICE
    
    # GPU 메모리 정리 (적극적으로)
    clear_gpu_cache(aggressive=True)
    gc.collect()
    time.sleep(0.5)  # 정리 후 안정화 대기
    
    # GPU 메모리 사용률 확인 (로딩 전)
    if DEVICE == "cuda" and not force_cpu:
        allocated, reserved, total = check_gpu_memory()
        if allocated is not None and total is not None:
            usage = check_gpu_memory_usage()
            if usage is not None:
                log_func(f"[System] GPU 메모리 상태: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체 ({usage*100:.1f}% 사용)")
                # 메모리 사용률이 너무 높으면 CPU 모드로 전환
                if usage >= GPU_MEMORY_FORCE_CPU_THRESHOLD:
                    log_func(f"[WARN] GPU 메모리 사용률이 {usage*100:.1f}%로 높습니다. CPU 모드로 전환합니다.")
                    force_cpu = True
                elif usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                    log_func(f"[WARN] GPU 메모리 사용률이 {usage*100:.1f}%로 높습니다. 추가 정리를 수행합니다.")
                    clear_gpu_cache(aggressive=True)
                    gc.collect()
                    time.sleep(0.3)
    
    # 사용할 디바이스 결정
    use_device = "cpu" if force_cpu else DEVICE
    if force_cpu and DEVICE == "cuda":
        log_func(f"[System] GPU 메모리 부족으로 CPU 모드로 전환합니다.")

    # 1. CarveKit 로딩
    if CARVEKIT_IF is None:
        log_func(f"[System] CarveKit 모델 로딩 중... (Device: {use_device})")
        
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
            kwargs["device"] = use_device
        else:
            if "segm_device" in params:
                kwargs["segm_device"] = use_device
            if "matting_device" in params:
                kwargs["matting_device"] = use_device

        if "batch_size" in params:
            kwargs["batch_size"] = 1
        
        try:
            CARVEKIT_IF = HiInterface(**kwargs)
            # GPU 메모리 사용량 로그
            if use_device == "cuda":
                allocated, reserved, total = check_gpu_memory()
                if allocated is not None:
                    log_func(f"[System] CarveKit 로딩 완료 | GPU 메모리: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체")
        except RuntimeError as e:
            if "out of memory" in str(e).lower() or "cuda" in str(e).lower():
                log_func(f"[WARN] GPU 메모리 부족 감지. CPU 모드로 재시도합니다...")
                clear_gpu_cache()
                # CPU로 재시도
                if not force_cpu:
                    return load_models_if_needed(log_func, force_cpu=True)
            log_func(f"[Error] CarveKit 로딩 실패: {e}")
            raise e
        except Exception as e:
            log_func(f"[Error] CarveKit 로딩 실패: {e}")
            raise e

    # 2. rembg 로딩
    if REMBG_SESSION is None:
        log_func(f"[System] rembg 모델 로딩 중... (Device: {use_device})")
        # ONNX Runtime Providers 설정
        if use_device == "cuda":
            providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
        else:
            providers = ["CPUExecutionProvider"]
        try:
            REMBG_SESSION = new_session("birefnet-general", providers=providers)
            # GPU 메모리 사용량 로그
            if use_device == "cuda":
                allocated, reserved, total = check_gpu_memory()
                if allocated is not None:
                    log_func(f"[System] rembg 로딩 완료 | GPU 메모리: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체")
        except Exception as e:
            error_str = str(e).lower()
            if "cublas" in error_str or "cuda" in error_str or "out of memory" in error_str:
                log_func(f"[WARN] GPU 오류 감지. CPU 모드로 재시도합니다...")
                clear_gpu_cache()
                # CPU로 재시도
                if not force_cpu:
                    return load_models_if_needed(log_func, force_cpu=True)
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

# -------------------------------------------------------------------------
#  디버그 로그 유틸
# -------------------------------------------------------------------------
def debug_log(message: str, level: str = "INFO"):
    """
    상세 디버그 로그를 파일에 기록
    level: INFO, WARN, ERROR, DEBUG
    """
    global DEBUG_LOG_FILE
    if not DEBUG_LOG_ENABLED:
        return
    
    if not DEBUG_LOG_FILE:
        # 로그 파일이 초기화되지 않았으면 콘솔에만 출력
        print(f"[DEBUG_LOG] {message}")
        return
    
    try:
        # 메시지가 문자열이 아니면 변환
        if not isinstance(message, str):
            message = str(message)
        
        # 한글 등 유니코드 문자가 포함된 경우 안전하게 인코딩
        # Windows에서 파일 읽기 시 인코딩 문제 방지를 위해 UTF-8로 명시적 인코딩
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_line = f"[{timestamp}] [{level}] {message}\n"
        
        # UTF-8 인코딩으로 파일에 쓰기 (errors='replace'로 인코딩 불가능한 문자 처리)
        with open(DEBUG_LOG_FILE, "a", encoding="utf-8", errors='replace', newline='\n') as f:
            f.write(log_line)
            f.flush()  # 즉시 디스크에 쓰기
    except UnicodeEncodeError as e:
        # 인코딩 오류 발생 시 안전하게 처리
        try:
            # 문제가 있는 문자를 제거하거나 대체
            safe_message = message.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            log_line = f"[{timestamp}] [{level}] {safe_message}\n"
            with open(DEBUG_LOG_FILE, "a", encoding="utf-8", errors='replace', newline='\n') as f:
                f.write(log_line)
                f.flush()
        except Exception as e2:
            # 로그 기록 실패는 콘솔에만 출력 (무한 루프 방지)
            print(f"[WARN] 디버그 로그 기록 실패 (인코딩 오류): {e2}")
            print(f"[WARN] 원본 메시지: {message[:100]}...")  # 처음 100자만 출력
    except Exception as e:
        # 로그 기록 실패는 콘솔에만 출력 (무한 루프 방지)
        print(f"[WARN] 디버그 로그 기록 실패: {e}")
        print(f"[WARN] 원본 메시지: {message[:100] if len(str(message)) > 100 else message}")  # 처음 100자만 출력

def init_debug_log(output_root: str):
    """디버그 로그 파일 초기화"""
    global DEBUG_LOG_FILE
    if not DEBUG_LOG_ENABLED:
        print("[DEBUG] 디버그 로그가 비활성화되어 있습니다.")
        return
    
    if not output_root:
        print("[WARN] 출력 루트가 설정되지 않아 디버그 로그를 생성할 수 없습니다.")
        return
    
    try:
        # 출력 폴더가 없으면 생성
        os.makedirs(output_root, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"bg_remove_debug_{timestamp}.log"
        DEBUG_LOG_FILE = os.path.join(output_root, log_filename)
        
        # 로그 파일 헤더 작성 (UTF-8 BOM 없이, Windows에서도 올바르게 읽히도록)
        with open(DEBUG_LOG_FILE, "w", encoding="utf-8", errors='replace', newline='\n') as f:
            # UTF-8 인코딩 정보를 헤더에 명시 (Windows 메모장 등에서 올바르게 인식하도록)
            f.write("# -*- coding: utf-8 -*-\n")
            f.write("# 이 파일은 UTF-8 인코딩으로 작성되었습니다.\n")
            f.write("# This file is encoded in UTF-8.\n")
            f.write("=" * 50 + "\n")
            f.write(f"=== 배경 제거 디버그 로그 시작 ===\n")
            f.write(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"출력 폴더: {output_root}\n")
            f.write(f"Device: {DEVICE}\n")
            f.write(f"\n=== GPU 진단 정보 ===\n")
            for info_line in DEVICE_INFO:
                f.write(f"{info_line}\n")
            f.write(f"===================\n\n")
            f.write(f"타임아웃 설정: CarveKit={TIMEOUT_CARVEKIT_BASE}s (최대 {TIMEOUT_CARVEKIT_MAX}s), rembg={TIMEOUT_REMBG_BASE}s (최대 {TIMEOUT_REMBG_MAX}s)\n")
            f.write(f"모델 재로딩 간격: {MODEL_RELOAD_INTERVAL}장마다\n")
            f.write(f"최대 입력 크기: {MAX_INPUT_SIZE}px\n")
            f.write(f"최대 재시도: {MAX_RETRIES}회\n")
            f.write(f"GPU 메모리 경고 임계값: {GPU_MEMORY_WARNING_THRESHOLD*100:.0f}%\n")
            f.write(f"GPU 메모리 위험 임계값: {GPU_MEMORY_CRITICAL_THRESHOLD*100:.0f}%\n")
            f.write(f"GPU 메모리 CPU 전환 임계값: {GPU_MEMORY_FORCE_CPU_THRESHOLD*100:.0f}%\n")
            f.write("=" * 50 + "\n\n")
            f.flush()  # 즉시 디스크에 쓰기
        
        print(f"[INFO] 디버그 로그 파일 생성: {DEBUG_LOG_FILE}")
        debug_log(f"디버그 로그 파일 초기화 완료: {log_filename}", "INFO")
    except Exception as e:
        error_msg = f"[ERROR] 디버그 로그 초기화 실패: {e}"
        print(error_msg)
        import traceback
        print(traceback.format_exc())
        DEBUG_LOG_FILE = None

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
def render_to_1000x1000_rgba(img_rgba: Image.Image) -> Image.Image:
    """
    알파 포함 이미지를 1000x1000 RGBA로 렌더 (PNG용).
    render_to_1000x1000_rgb와 동일한 로직이지만 RGBA를 유지하고 투명 배경을 사용합니다.
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

    use_center_ratio = False
    use_anchor_ratio = False
    anchor_side = None
    scale_obj = 1.0

    if fg_pixels > 0:
        ys, xs = np.where(bin_mask > 0)
        y_min, y_max = ys.min(), ys.max()
        x_min, x_max = xs.min(), xs.max()

        touch_left   = (x_min == 0)
        touch_right  = (x_max == w - 1)
        touch_top    = (y_min == 0)
        touch_bottom = (y_max == h - 1)
        touches = int(touch_left) + int(touch_right) + int(touch_top) + int(touch_bottom)

        bbox_w = x_max - x_min + 1
        bbox_h = y_max - y_min + 1
        obj_max = max(bbox_w, bbox_h) if (bbox_w > 0 and bbox_h > 0) else 0

        target_side = int(min(target_w, target_h) * ratio_target)

        if touches == 0 and obj_max > 0:
            scale_obj = target_side / obj_max
            use_center_ratio = True

        elif touches == 1 and obj_max > 0:
            aspect = min(bbox_w, bbox_h) / max(bbox_w, bbox_h)
            if aspect >= 0.35:
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
                    if obj_max < target_side:
                        scale_obj = target_side / obj_max
                    else:
                        scale_obj = 1.0

        if (not use_center_ratio) and (not use_anchor_ratio) and obj_max > 0:
            area_ratio = fg_pixels / (w * h)
            aspect_square = min(bbox_w, bbox_h) / max(bbox_w, bbox_h)

            if 0.20 <= area_ratio <= 0.75 and aspect_square >= 0.6:
                use_center_ratio = True
                if obj_max < target_side:
                    scale_obj = target_side / obj_max
                else:
                    scale_obj = 1.0

    src_w, src_h = img_rgba.size
    scale_fit_canvas = min(target_w / src_w, target_h / src_h)

    if use_center_ratio or use_anchor_ratio:
        scale = min(scale_obj, scale_fit_canvas)
    else:
        scale = scale_fit_canvas

    if scale <= 0:
        scale = scale_fit_canvas

    new_w = max(1, int(round(src_w * scale)))
    new_h = max(1, int(round(src_h * scale)))

    resized = img_rgba.resize((new_w, new_h), Image.LANCZOS)
    canvas = Image.new("RGBA", (target_w, target_h), (255, 255, 255, 0))  # 투명 배경

    if use_anchor_ratio and anchor_side is not None:
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
        offset_x = (target_w - new_w) // 2
        offset_y = (target_h - new_h) // 2

    canvas.paste(resized, (offset_x, offset_y), resized)
    return canvas


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
def analyze_mask_lightweight(alpha: np.ndarray) -> bool:
    """
    1차 품질 검사 (경량 버전)
    전경 비율과 기본 touches만 확인 - 빠른 필터링
    Returns: True if suspicious (수동확인 필요), False if OK
    """
    cfg = CONFIG
    h, w = alpha.shape
    total_pixels = h * w
    
    # 큰 이미지의 경우 샘플링
    if total_pixels > 4_000_000:  # 2000x2000 이상
        sample_factor = int(np.sqrt(total_pixels / 1_000_000))
        alpha_sampled = alpha[::sample_factor, ::sample_factor]
        bin_mask = (alpha_sampled > cfg.alpha_hard_cutoff).astype(np.uint8)
        sampled_pixels = bin_mask.shape[0] * bin_mask.shape[1]
        fg_pixels = int(bin_mask.sum())
        
        if fg_pixels == 0:
            return True
        
        fg_ratio = fg_pixels / sampled_pixels
        if fg_ratio < cfg.fg_ratio_min or fg_ratio > cfg.fg_ratio_max:
            return True
        
        # 샘플링된 마스크에서 bbox 계산
        ys, xs = np.where(bin_mask > 0)
        if len(ys) == 0:
            return True
        y_min, y_max = ys.min() * sample_factor, ys.max() * sample_factor
        x_min, x_max = xs.min() * sample_factor, xs.max() * sample_factor
        
        # 원본 크기 기준으로 터치 여부 확인
        touch_left = x_min <= sample_factor
        touch_right = x_max >= (w - sample_factor)
        touch_top = y_min <= sample_factor
        touch_bottom = y_max >= (h - sample_factor)
        touches = sum([touch_left, touch_right, touch_top, touch_bottom])
        
        if touches >= cfg.edge_touch_threshold:
            return True
    else:
        # 작은 이미지
        bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
        fg_pixels = int(bin_mask.sum())
        
        if fg_pixels == 0:
            return True
        
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
    
    # 1차 검사 통과 - 2차 검사 필요
    return None

def analyze_mask_heavy(alpha: np.ndarray) -> bool:
    """
    2차 품질 검사 (무거운 버전)
    connectedComponents 분석 - 1차 검사 통과 시에만 실행
    Returns: True if suspicious (수동확인 필요), False if OK
    """
    cfg = CONFIG
    h, w = alpha.shape
    total_pixels = h * w
    
    # 큰 이미지의 경우 샘플링
    if total_pixels > 4_000_000:  # 2000x2000 이상
        sample_factor = int(np.sqrt(total_pixels / 1_000_000))
        alpha_sampled = alpha[::sample_factor, ::sample_factor]
        bin_mask = (alpha_sampled > cfg.alpha_hard_cutoff).astype(np.uint8)
        sampled_pixels = bin_mask.shape[0] * bin_mask.shape[1]
        
        # connectedComponents는 샘플링된 이미지에서 수행
        num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(bin_mask)
        if num_labels > 1:
            areas = stats[1:, cv2.CC_STAT_AREA]
            large_areas = [a for a in areas if a > (sampled_pixels * cfg.big_component_ratio)]
            if len(large_areas) >= 2:
                return True
    else:
        # 작은 이미지
        bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
        
        num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(bin_mask)
        if num_labels > 1:
            areas = stats[1:, cv2.CC_STAT_AREA]
            large_areas = [a for a in areas if a > (total_pixels * cfg.big_component_ratio)]
            if len(large_areas) >= 2:
                return True

    return False

def analyze_mask(alpha: np.ndarray) -> bool:
    """
    마스크 품질 분석 (2단계 최적화 버전)
    1차: 경량 검사 (전경 비율, touches)
    2차: 무거운 검사 (connectedComponents) - 1차 통과 시에만
    """
    # 1차 경량 검사
    result_light = analyze_mask_lightweight(alpha)
    
    # 1차에서 이미 실패 판정이면 바로 반환
    if result_light is True:
        return True
    
    # 1차 통과 시에만 2차 검사 수행
    if result_light is None:
        return analyze_mask_heavy(alpha)
    
    # 1차에서 OK 판정 (이론적으로는 발생하지 않지만 안전장치)
    return False

def postprocess_alpha_simple(alpha: np.ndarray) -> np.ndarray:
    """
    알파 마스크 간단한 후처리 (노이즈 제거 + 최대 컴포넌트 유지)
    CarveKit 실패 시 rembg 호출 전에 사용하여 fallback 폭증 방지
    """
    cfg = CONFIG
    bin_mask = (alpha > cfg.alpha_hard_cutoff).astype(np.uint8)
    
    if bin_mask.sum() == 0:
        return alpha
    
    # connectedComponents로 최대 컴포넌트만 유지
    num_labels, labels = cv2.connectedComponents(bin_mask)
    if num_labels <= 1:
        # 단일 컴포넌트면 그대로 반환
        return alpha
    
    # 각 라벨의 면적 계산
    areas = np.bincount(labels.flatten())
    areas[0] = 0  # 배경 제외
    largest_label = areas.argmax()
    
    # 최대 컴포넌트만 유지
    keep_mask = (labels == largest_label).astype(np.uint8)
    
    # 원본 알파에 적용
    processed_alpha = alpha * keep_mask
    processed_alpha[processed_alpha < cfg.alpha_hard_cutoff] = 0
    
    return processed_alpha

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
#  타임아웃 래퍼 함수 (개선된 스레드 기반)
# -------------------------------------------------------------------------
# 주의: 프로세스 기반 타임아웃은 GPU 모델(CARVEKIT_IF, REMBG_SESSION)을
# 프로세스 간에 공유할 수 없어서 각 프로세스마다 모델을 다시 로드해야 합니다.
# 이는 오히려 성능 저하를 일으킬 수 있으므로, 스레드 기반을 유지하되
# GPU 메모리 관리와 단계별 시간 로그를 강화하여 멈춤 문제를 완화합니다.

class TimeoutError(Exception):
    """타임아웃 발생 시 사용하는 예외"""
    pass

def timeout_wrapper(timeout_seconds: float):
    """
    함수 실행에 타임아웃을 설정하는 데코레이터 (스레드 기반, 최적화)
    타임아웃 발생 시 즉시 다음으로 넘어가도록 최적화
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = [None]
            exception = [None]
            completed = [False]  # 완료 플래그
            timeout_occurred = [False]  # 타임아웃 발생 플래그
            start_time = time.time()
            
            def target():
                try:
                    # 타임아웃이 발생했으면 즉시 종료
                    if timeout_occurred[0]:
                        return
                    result[0] = func(*args, **kwargs)
                    completed[0] = True
                except Exception as e:
                    # 타임아웃이 발생했으면 예외 무시
                    if not timeout_occurred[0]:
                        exception[0] = e
                        completed[0] = True
            
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            thread.join(timeout=timeout_seconds)
            
            elapsed = time.time() - start_time
            
            if thread.is_alive():
                # 타임아웃 발생 - 즉시 다음으로 넘어감
                timeout_occurred[0] = True
                debug_log(f"[타임아웃] {func.__name__} 실행이 {timeout_seconds}초를 초과했습니다. (경과: {elapsed:.3f}s)", "ERROR")
                
                # 타임아웃 발생 시 즉시 GPU 메모리 정리 (백그라운드 스레드 무시, synchronize() 생략)
                debug_log(f"[타임아웃] GPU 메모리 즉시 정리 시작 (synchronize 생략)...", "WARN")
                clear_gpu_cache(aggressive=True, skip_sync=True)  # synchronize() 생략하여 백그라운드 스레드 대기 방지
                # 추가 정리: Python GC 실행 (여러 번 실행하여 더 적극적으로 정리)
                import gc
                for _ in range(5):  # 더 적극적으로 정리
                    gc.collect()
                
                # 타임아웃 발생 시 백그라운드 스레드가 GPU 리소스를 해제할 시간을 충분히 확보
                # 백그라운드 스레드가 완료되거나 최소한 GPU 리소스를 해제할 시간을 줌
                debug_log(f"[타임아웃] 백그라운드 스레드 GPU 리소스 해제 대기 중...", "WARN")
                time.sleep(3.0)  # 3초 대기 (안정성 우선: 백그라운드 스레드가 GPU 리소스를 해제할 시간 확보)
                
                # 추가 GPU 메모리 정리 (백그라운드 스레드가 일부 리소스를 해제했을 수 있음)
                clear_gpu_cache(aggressive=True, skip_sync=True)
                for _ in range(3):  # 추가 GC
                    gc.collect()
                
                # 최종 GPU 메모리 확인
                allocated, reserved, total = check_gpu_memory()
                if allocated is not None:
                    usage = check_gpu_memory_usage()
                    if usage is not None:
                        debug_log(f"[타임아웃] 정리 후 GPU 메모리: {allocated:.1f}MB/{total:.1f}MB ({usage*100:.1f}%)", "WARN")
                        if usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                            debug_log(f"[타임아웃] ⚠️ GPU 메모리 여전히 위험 - 모델 재로딩 필요", "ERROR")
                            # 타임아웃 발생 시 모델 재로딩 플래그 설정 (worker에서 처리)
                            global MODEL_RELOAD_NEEDED
                            MODEL_RELOAD_NEEDED = True
                            debug_log(f"[타임아웃] 모델 재로딩 플래그 설정됨", "WARN")
                
                # 타임아웃 예외 발생 (백그라운드 스레드는 무시하고 즉시 다음으로)
                debug_log(f"[타임아웃] 백그라운드 스레드는 무시하고 즉시 다음 이미지로 진행합니다.", "WARN")
                raise TimeoutError(f"{func.__name__} 실행이 {timeout_seconds}초를 초과했습니다.")
            
            if exception[0]:
                debug_log(f"[오류] {func.__name__} 실행 중 오류 발생 (경과: {elapsed:.3f}s): {exception[0]}", "ERROR")
                raise exception[0]
            
            debug_log(f"[완료] {func.__name__} 실행 완료 (경과: {elapsed:.3f}s)", "DEBUG")
            return result[0]
        return wrapper
    return decorator

# -------------------------------------------------------------------------
#  이미지 사전 리사이즈 (성능 최적화)
# -------------------------------------------------------------------------
def preprocess_image_for_bg_removal(input_path: str) -> tuple:
    """
    큰 이미지를 사전 리사이즈하여 배경 제거 성능 향상
    Returns: (resized_image, scale_factor, temp_path)
    """
    start_time = time.time()
    img = None
    try:
        # 파일 존재 확인
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"이미지 파일을 찾을 수 없습니다: {input_path}")
        
        img = Image.open(input_path)
        original_size = img.size
        max_dim = max(original_size)
        
        debug_log(f"이미지 로드: {os.path.basename(input_path)} | 크기: {original_size} | 최대: {max_dim}px", "DEBUG")
        
        # MAX_INPUT_SIZE보다 큰 이미지만 리사이즈
        if max_dim <= MAX_INPUT_SIZE:
            elapsed = time.time() - start_time
            debug_log(f"리사이즈 불필요 (크기: {max_dim}px <= {MAX_INPUT_SIZE}px) | 소요: {elapsed:.3f}s", "DEBUG")
            result = img.convert("RGB")
            img.close()  # 리소스 명시적 해제
            return result, 1.0, None
        
        # 비율 유지하며 리사이즈
        scale = MAX_INPUT_SIZE / max_dim
        new_w = int(original_size[0] * scale)
        new_h = int(original_size[1] * scale)
        
        debug_log(f"리사이즈 시작: {original_size} -> ({new_w}, {new_h}) | 스케일: {scale:.3f}", "DEBUG")
        resized = img.resize((new_w, new_h), Image.LANCZOS).convert("RGB")
        img.close()  # 원본 이미지 리소스 해제
        elapsed = time.time() - start_time
        debug_log(f"리사이즈 완료 | 소요: {elapsed:.3f}s", "DEBUG")
        return resized, scale, None
    except Exception as e:
        if img is not None:
            try:
                img.close()  # 예외 발생 시에도 리소스 해제
            except:
                pass
        elapsed = time.time() - start_time
        debug_log(f"리사이즈 실패: {e} | 소요: {elapsed:.3f}s", "ERROR")
        import traceback
        debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
        raise

def upscale_alpha_to_original(alpha: np.ndarray, original_size: tuple[int, int], scale: float) -> np.ndarray:
    """리사이즈된 알파를 원본 크기로 복원"""
    if scale == 1.0:
        return alpha
    
    from PIL import Image as PILImage
    alpha_img = PILImage.fromarray(alpha, mode="L")
    upscaled = alpha_img.resize(original_size, PILImage.LANCZOS)
    return np.array(upscaled, dtype=np.uint8)

# -------------------------------------------------------------------------
#  CarveKit / rembg 개별 처리 (타임아웃 적용 + 성능 최적화)
# -------------------------------------------------------------------------
@timeout_wrapper(TIMEOUT_CARVEKIT_BASE)
def remove_bg_carvekit(input_path: str):
    start_time = time.time()
    base_name = os.path.basename(input_path)
    debug_log(f"[CarveKit] 시작: {base_name}", "INFO")
    
    try:
        # CarveKit 모델 로드 (rembg는 해제됨)
        if CARVEKIT_IF is None:
            load_carvekit_if_needed(lambda msg: debug_log(msg, "INFO"))
        if CARVEKIT_IF is None:
            raise RuntimeError("CarveKit 모델이 로드되지 않았습니다.")
        
        # GPU 메모리 정리는 조건부로만 실행 (여기서는 제거)
        
        # 큰 이미지 사전 리사이즈
        preprocess_start = time.time()
        img, scale, _ = preprocess_image_for_bg_removal(input_path)
        # 원본 크기 확인 (리소스 관리 개선)
        with Image.open(input_path) as original_img:
            original_size = original_img.size
        preprocess_elapsed = time.time() - preprocess_start
        debug_log(f"[CarveKit] 전처리 완료 | 소요: {preprocess_elapsed:.3f}s", "DEBUG")
        
        # GPU 메모리 사용량 확인 (로깅만, 정리는 worker에서 담당)
        allocated, reserved, total = check_gpu_memory()
        if allocated is not None:
            usage = check_gpu_memory_usage()
            debug_log(f"[CarveKit] GPU 메모리 (처리 전): {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체", "DEBUG")
            if usage is not None:
                if usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                    debug_log(f"[CarveKit] ⚠️ GPU 메모리 위험 ({usage*100:.1f}%) - worker에서 정리 예정", "WARN")
                elif usage >= GPU_MEMORY_WARNING_THRESHOLD:
                    debug_log(f"[CarveKit] ⚠️ GPU 메모리 경고 ({usage*100:.1f}%)", "WARN")
        
        # CarveKit 처리
        carvekit_start = time.time()
        debug_log(f"[CarveKit] API 호출 시작...", "DEBUG")
        result_list = CARVEKIT_IF([img])
        carvekit_elapsed = time.time() - carvekit_start
        debug_log(f"[CarveKit] API 호출 완료 | 소요: {carvekit_elapsed:.3f}s", "DEBUG")
        
        # API 호출 직후 GPU 메모리 정리 (다른 작업을 위한 리소스 확보)
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        if not result_list:
            raise RuntimeError("CarveKit 결과가 비어 있습니다.")
        
        out = result_list[0].convert("RGBA")
        alpha = np.array(out.split()[-1], dtype=np.uint8)
        debug_log(f"[CarveKit] 결과 파싱 완료 | 알파 크기: {alpha.shape}", "DEBUG")
        
        # result_list는 더 이상 필요 없으므로 즉시 해제 (GPU 메모리 해제)
        del result_list
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        # 원본 크기로 복원
        if scale < 1.0:
            upscale_start = time.time()
            debug_log(f"[CarveKit] 알파 업스케일 시작: {alpha.shape} -> {original_size}", "DEBUG")
            alpha = upscale_alpha_to_original(alpha, original_size, scale)
            # 알파를 원본 크기로 복원했으므로 이미지도 복원 필요
            with Image.open(input_path) as original_img:
                out = original_img.convert("RGBA")
                # 알파 채널만 교체
                r, g, b, _ = out.split()
                out = Image.merge("RGBA", (r, g, b, Image.fromarray(alpha, mode="L")))
            upscale_elapsed = time.time() - upscale_start
            debug_log(f"[CarveKit] 알파 업스케일 완료 | 소요: {upscale_elapsed:.3f}s", "DEBUG")
        
        total_elapsed = time.time() - start_time
        debug_log(f"[CarveKit] 완료: {base_name} | 총 소요: {total_elapsed:.3f}s", "INFO")
        return out, alpha
    except TimeoutError as e:
        elapsed = time.time() - start_time
        debug_log(f"[CarveKit] 타임아웃: {base_name} | 소요: {elapsed:.3f}s | 오류: {e}", "ERROR")
        # 타임아웃 시 적극적인 메모리 정리는 timeout_wrapper에서 처리됨
        raise
    except RuntimeError as e:
        elapsed = time.time() - start_time
        error_str = str(e).lower()
        # CUDA 메모리 부족 오류 감지 (OOM 발생 시에만 정리 - 진짜 위기 상황)
        if "out of memory" in error_str or ("cuda" in error_str and "error" in error_str):
            clear_gpu_cache()  # OOM 발생 시에만 메모리 정리
            debug_log(f"[CarveKit] GPU 메모리 부족: {base_name} | 소요: {elapsed:.3f}s", "ERROR")
            # 특별한 예외로 변환하여 상위에서 CPU 폴백 처리 가능하도록
            raise RuntimeError(f"GPU 메모리 부족: {e}")
        # 일반 RuntimeError는 worker에서 정리하므로 여기서는 정리하지 않음
        debug_log(f"[CarveKit] 실패: {base_name} | 소요: {elapsed:.3f}s | 오류: {e}", "ERROR")
        import traceback
        debug_log(f"[CarveKit] 스택 트레이스:\n{traceback.format_exc()}", "ERROR")
        raise
    except Exception as e:
        elapsed = time.time() - start_time
        # 일반 Exception은 worker에서 정리하므로 여기서는 정리하지 않음
        debug_log(f"[CarveKit] 실패: {base_name} | 소요: {elapsed:.3f}s | 오류: {e}", "ERROR")
        import traceback
        debug_log(f"[CarveKit] 스택 트레이스:\n{traceback.format_exc()}", "ERROR")
        raise

@timeout_wrapper(TIMEOUT_REMBG_BASE)
def remove_bg_rembg(input_path: str):
    start_time = time.time()
    base_name = os.path.basename(input_path)
    debug_log(f"[rembg] 시작: {base_name}", "INFO")
    
    try:
        # rembg 모델 로드 (CarveKit은 해제됨)
        if REMBG_SESSION is None:
            load_rembg_if_needed(lambda msg: debug_log(msg, "INFO"))
        if REMBG_SESSION is None:
            raise RuntimeError("rembg 모델이 로드되지 않았습니다.")

        cfg = CONFIG
        # 원본 크기 확인 (리소스 관리 개선)
        with Image.open(input_path) as original_img:
            original_size = original_img.size
        debug_log(f"[rembg] 원본 크기: {original_size}", "DEBUG")
        
        # 큰 이미지 사전 리사이즈
        preprocess_start = time.time()
        img, scale, _ = preprocess_image_for_bg_removal(input_path)
        preprocess_elapsed = time.time() - preprocess_start
        debug_log(f"[rembg] 전처리 완료 | 소요: {preprocess_elapsed:.3f}s", "DEBUG")
        
        # PIL Image를 bytes로 변환
        convert_start = time.time()
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="JPEG", quality=95)
        img_bytes.seek(0)
        data = img_bytes.read()
        convert_elapsed = time.time() - convert_start
        debug_log(f"[rembg] 이미지 변환 완료 | 크기: {len(data)} bytes | 소요: {convert_elapsed:.3f}s", "DEBUG")

        # GPU 메모리 사용량 확인 (로깅만, 정리는 worker에서 담당)
        allocated, reserved, total = check_gpu_memory()
        if allocated is not None:
            usage = check_gpu_memory_usage()
            debug_log(f"[rembg] GPU 메모리 (처리 전): {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체", "DEBUG")
            if usage is not None:
                if usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                    debug_log(f"[rembg] ⚠️ GPU 메모리 위험 ({usage*100:.1f}%) - worker에서 정리 예정", "WARN")
                elif usage >= GPU_MEMORY_WARNING_THRESHOLD:
                    debug_log(f"[rembg] ⚠️ GPU 메모리 경고 ({usage*100:.1f}%)", "WARN")
        
        # rembg API 호출
        rembg_start = time.time()
        debug_log(f"[rembg] API 호출 시작...", "DEBUG")
        result_bytes = remove(
            data,
            session=REMBG_SESSION,
            alpha_matting=True,
            alpha_matting_foreground_threshold=240,
            alpha_matting_background_threshold=10,
            alpha_matting_erode_size=5,
            alpha_matting_base_size=min(1500, int(MAX_INPUT_SIZE * 0.75)),  # 리사이즈된 크기에 맞춤
            force_return_bytes=True,
        )
        rembg_elapsed = time.time() - rembg_start
        debug_log(f"[rembg] API 호출 완료 | 소요: {rembg_elapsed:.3f}s", "DEBUG")
        
        # API 호출 직후 GPU 메모리 정리 (다른 작업을 위한 리소스 확보)
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        # BytesIO에서 이미지 로드 (리소스 관리 개선)
        img_bytes_io = io.BytesIO(result_bytes)
        img_result = Image.open(img_bytes_io).convert("RGBA")
        r, g, b, a = img_result.split()
        alpha_raw = np.array(a, dtype=np.uint8)
        debug_log(f"[rembg] 결과 파싱 완료 | 알파 크기: {alpha_raw.shape}", "DEBUG")

        # result_bytes와 img는 더 이상 필요 없으므로 즉시 해제 (GPU 메모리 해제)
        del result_bytes, data, img_bytes, img, img_bytes_io
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        # 원본 크기로 복원
        if scale < 1.0:
            upscale_start = time.time()
            debug_log(f"[rembg] 알파 업스케일 시작: {alpha_raw.shape} -> {original_size}", "DEBUG")
            alpha_raw = upscale_alpha_to_original(alpha_raw, original_size, scale)
            # 원본 이미지로 복원 (리소스 관리 개선)
            with Image.open(input_path) as original_img:
                img_original = original_img.convert("RGBA")
                r, g, b, _ = img_original.split()
                img_result = Image.merge("RGBA", (r, g, b, Image.fromarray(alpha_raw, mode="L")))
            upscale_elapsed = time.time() - upscale_start
            debug_log(f"[rembg] 알파 업스케일 완료 | 소요: {upscale_elapsed:.3f}s", "DEBUG")

        # connectedComponents 최적화: 큰 이미지의 경우 샘플링
        h, w = alpha_raw.shape
        cc_start = time.time()
        if h * w > 4_000_000:  # 2000x2000 이상
            debug_log(f"[rembg] 큰 이미지 샘플링 모드 (크기: {h}x{w} = {h*w}px)", "DEBUG")
            # 샘플링하여 처리
            sample_factor = int(np.sqrt((h * w) / 1_000_000))  # 약 100만 픽셀로 샘플링
            debug_log(f"[rembg] 샘플링 팩터: {sample_factor}", "DEBUG")
            alpha_sampled = alpha_raw[::sample_factor, ::sample_factor]
            soft_mask_sampled = (alpha_sampled > 5).astype(np.uint8)
            num_labels, labels_sampled = cv2.connectedComponents(soft_mask_sampled)
            debug_log(f"[rembg] 샘플링된 connectedComponents 완료 | 라벨 수: {num_labels}", "DEBUG")
            
            if num_labels <= 1:
                keep_mask = (alpha_raw > 5).astype(np.uint8)
            else:
                # 샘플링된 결과를 원본 크기로 확대
                labels_sampled_img = Image.fromarray(labels_sampled.astype(np.uint16), mode="I")
                labels_upscaled = np.array(labels_sampled_img.resize((w, h), Image.NEAREST), dtype=np.uint16)
                areas = np.bincount(labels_upscaled.flatten())
                areas[0] = 0
                largest_label = areas.argmax()
                keep_mask = (labels_upscaled == largest_label).astype(np.uint8)
        else:
            debug_log(f"[rembg] 일반 모드 (크기: {h}x{w} = {h*w}px)", "DEBUG")
            # 작은 이미지는 기존 방식
            soft_mask = (alpha_raw > 5).astype(np.uint8)
            num_labels, labels = cv2.connectedComponents(soft_mask)
            debug_log(f"[rembg] connectedComponents 완료 | 라벨 수: {num_labels}", "DEBUG")
            if num_labels <= 1:
                keep_mask = soft_mask
            else:
                areas = np.bincount(labels.flatten())
                areas[0] = 0
                largest_label = areas.argmax()
                keep_mask = (labels == largest_label).astype(np.uint8)
        
        cc_elapsed = time.time() - cc_start
        debug_log(f"[rembg] connectedComponents 처리 완료 | 소요: {cc_elapsed:.3f}s", "DEBUG")

        final_alpha = alpha_raw * keep_mask
        final_alpha[final_alpha < cfg.alpha_hard_cutoff] = 0
        final_a = Image.fromarray(final_alpha, mode="L")
        
        # 중간 변수들 해제 (GPU 메모리 해제)
        del alpha_raw, keep_mask
        if 'soft_mask' in locals():
            del soft_mask
        if 'labels' in locals():
            del labels
        if 'labels_sampled' in locals():
            del labels_sampled, labels_upscaled
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        total_elapsed = time.time() - start_time
        debug_log(f"[rembg] 완료: {base_name} | 총 소요: {total_elapsed:.3f}s", "INFO")
        return Image.merge("RGBA", (r, g, b, final_a)), final_alpha
    except TimeoutError as e:
        elapsed = time.time() - start_time
        debug_log(f"[rembg] 타임아웃: {base_name} | 소요: {elapsed:.3f}s | 오류: {e}", "ERROR")
        # 타임아웃 시 적극적인 메모리 정리는 timeout_wrapper에서 처리됨
        raise
    except Exception as e:
        elapsed = time.time() - start_time
        error_str = str(e).lower()
        # CUDA/CUBLAS/OOM 오류 감지 (진짜 위기 상황에서만 정리)
        if "cublas" in error_str or ("cuda" in error_str and "error" in error_str) or "out of memory" in error_str:
            clear_gpu_cache()  # GPU 오류 발생 시에만 메모리 정리
            debug_log(f"[rembg] GPU 오류: {base_name} | 소요: {elapsed:.3f}s", "ERROR")
            # 특별한 예외로 변환하여 상위에서 CPU 폴백 처리 가능하도록
            raise RuntimeError(f"GPU 오류: {e}")
        # 일반 Exception은 worker에서 정리하므로 여기서는 정리하지 않음
        debug_log(f"[rembg] 실패: {base_name} | 소요: {elapsed:.3f}s | 오류: {e}", "ERROR")
        import traceback
        debug_log(f"[rembg] 스택 트레이스:\n{traceback.format_exc()}", "ERROR")
        raise


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
    
    process_start = time.time()
    debug_log(f"=== 이미지 처리 시작: {base_name} ===", "INFO")
    debug_log(f"입력 경로: {input_path}", "DEBUG")
    debug_log(f"출력 루트: {output_root}", "DEBUG")

    # 1차: CarveKit (재시도 로직 포함)
    # [OOM 방지] CarveKit 사용 전에 CarveKit만 로드 (rembg는 해제하여 GPU 메모리 확보)
    load_carvekit_if_needed(lambda msg: debug_log(msg, "INFO"))
    
    img_rgba_ck, alpha_ck = None, None
    suspicious_ck = True
    carvekit_error = None
    
    debug_log(f"[CarveKit] 시도 시작 (최대 {MAX_RETRIES + 1}회)", "INFO")
    for attempt in range(MAX_RETRIES + 1):
        try:
            debug_log(f"[CarveKit] 시도 {attempt + 1}/{MAX_RETRIES + 1}", "DEBUG")
            img_rgba_ck, alpha_ck = remove_bg_carvekit(input_path)
            
            analyze_start = time.time()
            # 1차 경량 검사
            light_start = time.time()
            suspicious_ck_light = analyze_mask_lightweight(alpha_ck)
            light_elapsed = time.time() - light_start
            debug_log(f"[CarveKit] 1차 경량 검사 완료 | 결과: {suspicious_ck_light} | 소요: {light_elapsed:.3f}s", "DEBUG")
            
            # 1차 통과 시에만 2차 검사
            if suspicious_ck_light is None:
                heavy_start = time.time()
                suspicious_ck = analyze_mask_heavy(alpha_ck)
                heavy_elapsed = time.time() - heavy_start
                debug_log(f"[CarveKit] 2차 무거운 검사 완료 | 수동확인 필요: {suspicious_ck} | 소요: {heavy_elapsed:.3f}s", "DEBUG")
            else:
                suspicious_ck = suspicious_ck_light
            
            analyze_elapsed = time.time() - analyze_start
            debug_log(f"[CarveKit] 품질 분석 완료 (총) | 수동확인 필요: {suspicious_ck} | 총 소요: {analyze_elapsed:.3f}s", "DEBUG")
            
            # CarveKit 처리 완료 후 즉시 GPU 메모리 정리 (임시 텐서/버퍼 해제)
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            debug_log(f"[CarveKit] GPU 메모리 정리 완료 (임시 데이터 해제)", "DEBUG")
            
            carvekit_error = None
            break
        except RuntimeError as e:
            error_str = str(e).lower()
            # GPU 메모리 부족 오류인 경우 (OOM 발생 시에만 clear_gpu_cache 실행 - 진짜 위기 상황)
            if "gpu 메모리 부족" in error_str or "out of memory" in error_str:
                carvekit_error = f"GPU 메모리 부족: {e}"
                debug_log(f"[CarveKit] GPU 메모리 부족 (시도 {attempt + 1})", "WARN")
                # OOM 발생 시에만 GPU 캐시 정리 (clear_gpu_cache 내부에서 empty_cache 호출하므로 중복 제거)
                clear_gpu_cache()
                log(f"[CarveKit] GPU 메모리 부족: {base_name} - rembg로 전환합니다")
                # CarveKit 실패로 처리하고 rembg로 넘어감
                break
            else:
                carvekit_error = str(e)
                debug_log(f"[CarveKit] 오류 발생 (시도 {attempt + 1}): {e}", "WARN")
                import traceback
                debug_log(f"[CarveKit] 스택 트레이스:\n{traceback.format_exc()}", "ERROR")
                if attempt < MAX_RETRIES:
                    log(f"[CarveKit] 재시도 {attempt + 1}/{MAX_RETRIES}: {base_name} - {e}")
                    time.sleep(1)
                else:
                    log(f"[CarveKit] 최종 실패: {base_name} - {e}")
        except TimeoutError as e:
            carvekit_error = f"타임아웃: {e}"
            debug_log(f"[CarveKit] 타임아웃 발생 (시도 {attempt + 1}): {e}", "WARN")
            # 타임아웃 발생 시 GPU 메모리 정리 후 재시도
            log(f"[CarveKit] 타임아웃 발생: {base_name} - GPU 메모리 정리 후 재시도")
            # GPU 메모리 적극적 정리 (더 적극적으로)
            for _ in range(5):
                gc.collect()
            clear_gpu_cache(aggressive=True, skip_sync=True)
            time.sleep(2)  # 더 긴 대기 (백그라운드 스레드가 GPU 리소스를 해제할 시간 확보)
            
            # 추가 정리 (백그라운드 스레드가 일부 리소스를 해제했을 수 있음)
            clear_gpu_cache(aggressive=True, skip_sync=True)
            gc.collect()
            
            if attempt < MAX_RETRIES:
                # 재시도
                log(f"[CarveKit] 재시도 {attempt + 1}/{MAX_RETRIES}: {base_name} - 타임아웃 후 재시도")
                continue
            else:
                log(f"[CarveKit] 최종 타임아웃: {base_name} - rembg로 전환합니다")
                break  # 최대 재시도 후 rembg로 넘어감
        except Exception as e:
            carvekit_error = str(e)
            debug_log(f"[CarveKit] 오류 발생 (시도 {attempt + 1}): {e}", "WARN")
            import traceback
            debug_log(f"[CarveKit] 스택 트레이스:\n{traceback.format_exc()}", "ERROR")
            if attempt < MAX_RETRIES:
                log(f"[CarveKit] 재시도 {attempt + 1}/{MAX_RETRIES}: {base_name} - {e}")
                time.sleep(1)
            else:
                log(f"[CarveKit] 최종 실패: {base_name} - {e}")
    
    if not suspicious_ck and img_rgba_ck is not None:
        final_rgba = img_rgba_ck
        final_alpha = alpha_ck
        method = "CarveKit"
        suspicious_final = False
        debug_log(f"[결정] CarveKit 사용 (품질 OK)", "INFO")
    else:
        # CarveKit 실패/애매 시: 알파 후처리 후 rembg 호출 (fallback 폭증 방지)
        if img_rgba_ck is not None and alpha_ck is not None:
            postprocess_start = time.time()
            debug_log(f"[CarveKit] 품질 불충분 → 알파 후처리 후 rembg 시도", "INFO")
            # 알파 후처리 (노이즈 제거 + 최대 컴포넌트 유지)
            alpha_ck_processed = postprocess_alpha_simple(alpha_ck)
            # 후처리된 알파로 품질 재검사
            suspicious_ck_post = analyze_mask_lightweight(alpha_ck_processed)
            postprocess_elapsed = time.time() - postprocess_start
            debug_log(f"[CarveKit] 알파 후처리 완료 | 재검사 결과: {suspicious_ck_post} | 소요: {postprocess_elapsed:.3f}s", "DEBUG")
            
            # 후처리 후에도 품질이 OK면 CarveKit 결과 사용
            if suspicious_ck_post is False:
                final_rgba = img_rgba_ck
                final_alpha = alpha_ck_processed
                method = "CarveKit(후처리)"
                suspicious_final = False
                debug_log(f"[결정] CarveKit 후처리 결과 사용 (품질 OK)", "INFO")
            else:
                debug_log(f"[CarveKit] 후처리 후에도 품질 불충분 → rembg 시도", "INFO")
        else:
            debug_log(f"[CarveKit] 실패 → rembg 시도", "INFO")
        # 2차: rembg (재시도 로직 포함)
        # [OOM 방지] rembg 사용 전에 rembg만 로드 (CarveKit은 해제하여 GPU 메모리 확보)
        load_rembg_if_needed(lambda msg: debug_log(msg, "INFO"))
        
        img_rgba_rm, alpha_rm = None, None
        suspicious_rm = True
        rembg_error = None
        
        debug_log(f"[rembg] 시도 시작 (최대 {MAX_RETRIES + 1}회)", "INFO")
        for attempt in range(MAX_RETRIES + 1):
            try:
                debug_log(f"[rembg] 시도 {attempt + 1}/{MAX_RETRIES + 1}", "DEBUG")
                img_rgba_rm, alpha_rm = remove_bg_rembg(input_path)
                
                analyze_start = time.time()
                # 1차 경량 검사
                light_start = time.time()
                suspicious_rm_light = analyze_mask_lightweight(alpha_rm)
                light_elapsed = time.time() - light_start
                debug_log(f"[rembg] 1차 경량 검사 완료 | 결과: {suspicious_rm_light} | 소요: {light_elapsed:.3f}s", "DEBUG")
                
                # 1차 통과 시에만 2차 검사
                if suspicious_rm_light is None:
                    heavy_start = time.time()
                    suspicious_rm = analyze_mask_heavy(alpha_rm)
                    heavy_elapsed = time.time() - heavy_start
                    debug_log(f"[rembg] 2차 무거운 검사 완료 | 수동확인 필요: {suspicious_rm} | 소요: {heavy_elapsed:.3f}s", "DEBUG")
                else:
                    suspicious_rm = suspicious_rm_light
                
                analyze_elapsed = time.time() - analyze_start
                debug_log(f"[rembg] 품질 분석 완료 (총) | 수동확인 필요: {suspicious_rm} | 총 소요: {analyze_elapsed:.3f}s", "DEBUG")
                
                # rembg 처리 완료 후 즉시 GPU 메모리 정리 (임시 텐서/버퍼 해제)
                gc.collect()
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                debug_log(f"[rembg] GPU 메모리 정리 완료 (임시 데이터 해제)", "DEBUG")
                
                rembg_error = None
                break
            except RuntimeError as e:
                error_str = str(e).lower()
                # GPU 오류인 경우 (OOM 포함, OOM 발생 시에만 clear_gpu_cache 실행 - 진짜 위기 상황)
                if "gpu 오류" in error_str or "cublas" in error_str or "cuda" in error_str or "out of memory" in error_str:
                    rembg_error = f"GPU 오류: {e}"
                    debug_log(f"[rembg] GPU 오류 (시도 {attempt + 1})", "WARN")
                    # OOM 발생 시에만 GPU 캐시 정리 (clear_gpu_cache 내부에서 empty_cache 호출하므로 중복 제거)
                    if "out of memory" in error_str:
                        clear_gpu_cache()
                    # GPU 오류는 재시도해도 의미 없으므로 바로 실패 처리
                    log(f"[rembg] GPU 오류: {base_name} - CPU 모드로 재시도하거나 스킵합니다")
                    if attempt < MAX_RETRIES:
                        # 모델을 CPU로 재로딩 시도
                        try:
                            log(f"[rembg] CPU 모드로 모델 재로딩 시도...")
                            # rembg만 CPU 모드로 재로딩 (CarveKit은 이미 해제됨)
                            load_rembg_if_needed(log_func=log, force_cpu=True)
                            time.sleep(2)  # 모델 재로딩 대기
                            continue  # 재시도
                        except Exception as cpu_retry_err:
                            debug_log(f"CPU 모드 재로딩 실패: {cpu_retry_err}", "WARN")
                    break
                else:
                    rembg_error = str(e)
                    debug_log(f"[rembg] 오류 발생 (시도 {attempt + 1}): {e}", "WARN")
                    import traceback
                    debug_log(f"[rembg] 스택 트레이스:\n{traceback.format_exc()}", "ERROR")
                    if attempt < MAX_RETRIES:
                        log(f"[rembg] 재시도 {attempt + 1}/{MAX_RETRIES}: {base_name} - {e}")
                        time.sleep(1)
                    else:
                        log(f"[rembg] 최종 실패: {base_name} - {e}")
            except TimeoutError as e:
                rembg_error = f"타임아웃: {e}"
                debug_log(f"[rembg] 타임아웃 발생 (시도 {attempt + 1}): {e}", "WARN")
                # 타임아웃 발생 시 GPU 메모리 정리 후 재시도
                log(f"[rembg] 타임아웃 발생: {base_name} - GPU 메모리 정리 후 재시도")
                # GPU 메모리 적극적 정리 (더 적극적으로)
                for _ in range(5):
                    gc.collect()
                clear_gpu_cache(aggressive=True, skip_sync=True)
                time.sleep(2)  # 더 긴 대기 (백그라운드 스레드가 GPU 리소스를 해제할 시간 확보)
                
                # 추가 정리 (백그라운드 스레드가 일부 리소스를 해제했을 수 있음)
                clear_gpu_cache(aggressive=True, skip_sync=True)
                gc.collect()
                
                if attempt < MAX_RETRIES:
                    # 재시도 시 타임아웃 시간 증가 (동적 조정)
                    log(f"[rembg] 재시도 {attempt + 1}/{MAX_RETRIES}: {base_name} - 타임아웃 시간 증가하여 재시도")
                    # 타임아웃 래퍼를 동적으로 변경할 수 없으므로, 재시도는 기본 타임아웃으로 진행
                    # 대신 GPU 메모리 정리 후 재시도
                    continue
                else:
                    log(f"[rembg] 최종 타임아웃: {base_name} - CarveKit 폴백 사용")
                    break  # 최대 재시도 후 폴백
            except Exception as e:
                rembg_error = str(e)
                debug_log(f"[rembg] 오류 발생 (시도 {attempt + 1}): {e}", "WARN")
                import traceback
                debug_log(f"[rembg] 스택 트레이스:\n{traceback.format_exc()}", "ERROR")
                if attempt < MAX_RETRIES:
                    log(f"[rembg] 재시도 {attempt + 1}/{MAX_RETRIES}: {base_name} - {e}")
                    time.sleep(1)
                else:
                    log(f"[rembg] 최종 실패: {base_name} - {e}")
        
        if img_rgba_rm is not None:
            final_rgba = img_rgba_rm
            final_alpha = alpha_rm
            method = "rembg"
            suspicious_final = suspicious_rm
            debug_log(f"[결정] rembg 사용 (품질: {'수동확인' if suspicious_rm else 'OK'})", "INFO")
        elif img_rgba_ck is not None:
            # CarveKit 결과를 폴백으로 사용
            final_rgba = img_rgba_ck
            final_alpha = alpha_ck
            method = "CarveKit(Fallback)"
            suspicious_final = True
            debug_log(f"[결정] CarveKit 폴백 사용 (rembg 실패)", "WARN")
        else:
            # 모든 시도 실패
            error_msg = f"모든 배경제거 시도 실패"
            if carvekit_error:
                error_msg += f" (CarveKit: {carvekit_error})"
            if rembg_error:
                error_msg += f" (rembg: {rembg_error})"
            debug_log(f"[결정] 모든 방법 실패: {error_msg}", "ERROR")
            raise RuntimeError(error_msg)

    result_flag = "need_manual" if suspicious_final else "auto_ok"
    result_category = "수동확인" if suspicious_final else "자동OK"
    subdir = NEED_MANUAL_DIR if suspicious_final else AUTO_OK_DIR
    
    debug_log(f"결과 분류: {result_category} | 폴더: {subdir}", "INFO")

    # 크롭 처리
    crop_start = time.time()
    final_rgba, final_alpha = center_crop_if_safe(final_rgba, final_alpha)
    crop_elapsed = time.time() - crop_start
    debug_log(f"크롭 처리 완료 | 소요: {crop_elapsed:.3f}s", "DEBUG")

    # JPG 출력 (기존)
    save_start = time.time()
    target_root = os.path.join(output_root, subdir, rel_root)
    os.makedirs(target_root, exist_ok=True)
    output_path = os.path.join(target_root, output_name)
    
    render_start = time.time()
    output_img = render_to_1000x1000_rgb(final_rgba)
    render_elapsed = time.time() - render_start
    debug_log(f"JPG 렌더링 완료 | 소요: {render_elapsed:.3f}s", "DEBUG")
    
    output_img.save(output_path, "JPEG", quality=90, optimize=True)
    save_elapsed = time.time() - save_start
    debug_log(f"JPG 저장 완료: {output_path} | 소요: {save_elapsed:.3f}s", "DEBUG")

    # PNG 출력 (추가) - JPG와 동일한 렌더링 로직 사용하되 RGBA 유지
    png_start = time.time()
    png_root = os.path.join(output_root, PNG_DIR, rel_root)
    os.makedirs(png_root, exist_ok=True)
    png_output_name = f"{base_name}.png"
    png_output_path = os.path.join(png_root, png_output_name)
    
    # render_to_1000x1000_rgb와 동일한 로직이지만 RGBA 유지
    png_render_start = time.time()
    png_output_img = render_to_1000x1000_rgba(final_rgba)
    png_render_elapsed = time.time() - png_render_start
    debug_log(f"PNG 렌더링 완료 | 소요: {png_render_elapsed:.3f}s", "DEBUG")
    
    png_output_img.save(png_output_path, "PNG", optimize=True)
    png_elapsed = time.time() - png_start
    debug_log(f"PNG 저장 완료: {png_output_path} | 소요: {png_elapsed:.3f}s", "DEBUG")

    # 알파 마스크 저장
    alpha_start = time.time()
    alpha_root = os.path.join(output_root, ALPHA_DIR, rel_root)
    os.makedirs(alpha_root, exist_ok=True)
    alpha_path = os.path.join(alpha_root, f"{base_name}.png")
    Image.fromarray(final_alpha, mode="L").save(alpha_path)
    alpha_elapsed = time.time() - alpha_start
    debug_log(f"알파 마스크 저장 완료: {alpha_path} | 소요: {alpha_elapsed:.3f}s", "DEBUG")

    total_elapsed = time.time() - process_start
    log(f"[{method}] {base_name} -> {result_category}")
    debug_log(f"=== 이미지 처리 완료: {base_name} | 총 소요: {total_elapsed:.3f}s ===", "INFO")

    # 이미지 처리 완료 후 최종 GPU 메모리 정리 (모든 임시 데이터 해제)
    # 모델 자체는 메모리에 상주하지만, 처리 중 생성된 임시 텐서/버퍼는 즉시 해제
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        # 메모리 사용량 로깅 (디버그용)
        allocated, reserved, total = check_gpu_memory()
        if allocated is not None:
            usage = check_gpu_memory_usage()
            if usage is not None:
                debug_log(f"[최종 정리] GPU 메모리: {allocated:.1f}MB 할당 / {reserved:.1f}MB 예약 / {total:.1f}MB 전체 ({usage*100:.1f}% 사용)", "DEBUG")
    
    # CPU 양보는 주기적으로만 (성능 우선)

    # 매핑 정보 구성 (엑셀 업데이트용 + 필요시 다른 저장에도 사용 가능)
    input_abs = os.path.abspath(input_path)
    output_abs = os.path.abspath(output_path)
    png_output_abs = os.path.abspath(png_output_path)
    mask_abs = os.path.abspath(alpha_path)
    
    # Windows 경로 호환
    input_rel = os.path.basename(input_path)
    output_rel = os.path.relpath(output_path, output_root).replace("\\", "/")
    png_output_rel = os.path.relpath(png_output_path, output_root).replace("\\", "/")
    mask_rel = os.path.relpath(alpha_path, output_root).replace("\\", "/")

    entry: Dict[str, Any] = {
        "input_abs": input_abs,
        "input_rel": input_rel,
        "output_abs": output_abs,
        "output_rel": output_rel,
        "png_output_abs": png_output_abs,
        "png_output_rel": png_output_rel,
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
        
        # 프로세스 우선순위 낮추기 (다른 작업에 영향 최소화)
        try:
            if sys.platform == "win32":
                try:
                    import win32api
                    import win32process
                    import win32con
                    current_process = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, os.getpid())
                    win32process.SetPriorityClass(current_process, win32process.BELOW_NORMAL_PRIORITY_CLASS)
                    win32api.CloseHandle(current_process)
                except (ImportError, Exception):
                    pass  # win32api 없거나 설정 실패해도 무시
        except Exception:
            pass
        
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
        
        # 진행 상황 관리
        self.progress_file_path: Optional[str] = None
        self.processed_indices: set = set()  # 처리 완료된 행 인덱스
        self.df_lock = Lock()  # DataFrame 동시 접근 방지

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
            self.log("")
            self.log("=== GPU 사용 불가 원인 진단 ===")
            for info_line in DEVICE_INFO:
                self.log(info_line)
            self.log("")
            self.log("GPU를 사용하려면:")
            self.log("1. CUDA 지원 PyTorch 설치:")
            self.log("   pip uninstall torch torchvision")
            self.log("   pip install torch torchvision --index-url https://download.pytorch.org/whl/cu121")
            self.log("2. 또는 CUDA 11.8용:")
            self.log("   pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118")
            self.log("3. 설치 후 프로그램을 재시작하세요.")
            self.log("================================")

    def log(self, msg):
        """로그 메시지 추가 (메모리 누수 방지를 위해 로그 길이 제한)"""
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, msg + "\n")
        
        # 로그가 너무 길어지면 오래된 로그 삭제 (메모리 누수 방지)
        # 약 10000줄 이상이면 처음 5000줄 삭제
        line_count = int(self.log_text.index('end-1c').split('.')[0])
        if line_count > 10000:
            self.log_text.delete('1.0', '5000.0')
            self.log_text.insert('1.0', "[로그 정리됨 - 오래된 로그 삭제]\n")
        
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
        path = filedialog.askopenfilename(
            title="배경제거 엑셀 선택 (I0 또는 I1 버전 가능)",
            filetypes=[("Excel", "*.xlsx;*.xls"), ("All", "*.*")]
        )
        if path:
            # I0 또는 I1 포함 여부 검증
            base_name = os.path.basename(path)
            if not re.search(r"_I[01]", base_name, re.IGNORECASE):
                messagebox.showerror(
                    "오류", 
                    f"이 도구는 I0 또는 I1 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                    f"선택한 파일: {base_name}\n"
                    f"파일명에 '_I0' 또는 '_I1' 패턴이 포함되어 있어야 합니다."
                )
                return
            self.excel_path_var.set(path)

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

        # I0 또는 I1 포함 여부 검증
        base_name = os.path.basename(excel_path)
        is_i0 = re.search(r"_I0", base_name, re.IGNORECASE)
        is_i1 = re.search(r"_I1", base_name, re.IGNORECASE)
        
        if not is_i0 and not is_i1:
            messagebox.showerror(
                "오류", 
                f"이 도구는 I0 또는 I1 버전의 엑셀 파일만 처리할 수 있습니다.\n\n"
                f"선택한 파일: {base_name}\n"
                f"파일명에 '_I0' 또는 '_I1' 패턴이 포함되어 있어야 합니다.\n\n"
                f"I1 파일을 선택하면 빈 셀부터 이어서 진행합니다."
            )
            return
        
        # I1 파일인 경우 경고 메시지
        if is_i1:
            result = messagebox.askyesno(
                "확인",
                f"I1 버전 파일을 선택하셨습니다.\n\n"
                f"이 파일은 이미 처리된 항목이 포함되어 있을 수 있습니다.\n"
                f"빈 셀부터 이어서 진행하시겠습니까?\n\n"
                f"예: 빈 셀부터 이어서 진행\n"
                f"아니오: 취소"
            )
            if not result:
                return

        try:
            # 엑셀 파일이 다른 프로세스에서 열려있는지 확인
            if not os.access(excel_path, os.W_OK):
                messagebox.showerror("오류", f"엑셀 파일이 다른 프로그램에서 열려있거나 쓰기 권한이 없습니다.\n\n파일을 닫고 다시 시도해주세요.")
                return
            
            df = pd.read_excel(excel_path, dtype=str, engine='openpyxl')
            
            # 빈 데이터프레임 확인
            if df.empty:
                messagebox.showerror("오류", "엑셀 파일이 비어있습니다.")
                return
                
        except FileNotFoundError:
            messagebox.showerror("오류", f"엑셀 파일을 찾을 수 없습니다:\n{excel_path}")
            return
        except PermissionError:
            messagebox.showerror("오류", f"엑셀 파일에 접근할 수 없습니다. 파일이 다른 프로그램에서 열려있을 수 있습니다.\n\n파일을 닫고 다시 시도해주세요.")
            return
        except Exception as e:
            error_msg = str(e)
            if "No module named 'openpyxl'" in error_msg:
                messagebox.showerror("오류", f"엑셀 읽기 실패: openpyxl 모듈이 설치되지 않았습니다.\n\n다음 명령어로 설치해주세요:\npip install openpyxl")
            else:
                messagebox.showerror("오류", f"엑셀 읽기 실패: {error_msg}")
            return

        if THUMB_COL_NAME not in df.columns:
            messagebox.showerror("오류", f"'{THUMB_COL_NAME}' 컬럼이 없습니다.")
            return

        # 엑셀 DataFrame / 경로를 인스턴스에 보관
        self.df = df
        self.excel_path = excel_path

        # [NEW] I0이면 I1로, I1이면 그대로 사용
        base_name = os.path.basename(excel_path)
        if re.search(r"_I1", base_name, re.IGNORECASE):
            # I1 파일이면 그대로 사용 (이어서 진행)
            self.target_excel_path = excel_path
            self.log("[정보] I1 파일 선택: 빈 셀부터 이어서 진행합니다.")
        else:
            # I0 파일이면 I1 버전으로 출력 파일명 생성
            self.target_excel_path = get_i1_output_path(excel_path)
        
        # 진행 상황 파일 경로 설정
        excel_dir = os.path.dirname(excel_path)
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        self.progress_file_path = os.path.join(excel_dir, base_name + PROGRESS_FILE_NAME)
        
        # 진행 상황 복원 (이전 작업 이어서 진행)
        # 방법 1: 진행 상황 파일에서 복원
        self.processed_indices = self.load_progress()
        
        # 방법 2: 엑셀 파일에서 빈 셀 확인하여 복원 (더 정확함)
        # I1 파일이 있으면 읽어서 이미 처리된 항목 확인
        if os.path.exists(self.target_excel_path):
            try:
                df_i1 = pd.read_excel(self.target_excel_path, dtype=str, engine='openpyxl')
                if COL_IMG_OUT in df_i1.columns:
                    # 빈 셀이 아닌 행 인덱스 수집
                    for idx, row in df_i1.iterrows():
                        img_out = row.get(COL_IMG_OUT, "")
                        if pd.notna(img_out) and str(img_out).strip() and not str(img_out).strip().startswith("[타임아웃]") and not str(img_out).strip().startswith("[오류]"):
                            self.processed_indices.add(idx)
                    self.log(f"[복원] I1 파일에서 {len(self.processed_indices)}개 이미지 이미 처리됨 확인")
            except Exception as e:
                self.log(f"[경고] I1 파일 읽기 실패: {e}")
        
        if self.processed_indices:
            skipped_count = len(self.processed_indices)
            self.log(f"[복원] 총 {skipped_count}개 이미지 이미 처리됨 (스킵)")
        
        # [NEW] 런처 현황판 업데이트 (시작 알림) - img 상태만 I1 (진행중)로 업데이트 (text 상태는 변경하지 않음)
        try:
            root_name = get_root_filename(excel_path)
            JobManager.update_status(root_name, img_msg="I1 (진행중)")
        except: pass

        # 결과용 컬럼이 없으면 생성 (빈 문자열로 초기화)
        for col in [COL_IMG_OUT, COL_IMG_OUT_PNG, COL_HUMAN_LABEL, COL_HUMAN_NOTES, COL_AI_LABEL]:
            if col not in df.columns:
                df[col] = ""


        excel_dir = os.path.dirname(excel_path)
        items = []
        skipped_no_path = 0
        skipped_missing_file = 0
        skipped_invalid_path = 0

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
            try:
                if os.path.isabs(s):
                    full_path = os.path.normpath(s)
                else:
                    full_path = os.path.normpath(os.path.join(excel_dir, s))
                
                # 경로 검증 (보안 및 안정성)
                if not os.path.exists(full_path):
                    skipped_missing_file += 1
                    self.ui_queue.put(
                        ("log", f"[SKIP] 행 {idx+1}: 이미지 파일을 찾을 수 없습니다: {os.path.basename(full_path)}")
                    )
                    continue
                
                # 파일 확장자 검증
                if not os.path.isfile(full_path):
                    skipped_invalid_path += 1
                    self.ui_queue.put(
                        ("log", f"[SKIP] 행 {idx+1}: 경로가 파일이 아닙니다: {os.path.basename(full_path)}")
                    )
                    continue
                    
            except (OSError, ValueError) as path_err:
                skipped_invalid_path += 1
                self.ui_queue.put(
                    ("log", f"[SKIP] 행 {idx+1}: 잘못된 경로 형식: {str(path_err)[:50]}")
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
            if skipped_invalid_path:
                msg += f"- 잘못된 경로: {skipped_invalid_path}행\n"
            messagebox.showwarning("주의", msg)
            return

        total = len(items)
        # 시작 전에 한 번 로그로 요약
        self.log(
            f"유효 썸네일 행 수: {total}개 / "
            f"썸네일경로 비어 있음: {skipped_no_path}행 / "
            f"이미지 없음: {skipped_missing_file}행 / "
            f"잘못된 경로: {skipped_invalid_path}행"
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
            if messagebox.askyesno("확인", "작업을 중단하시겠습니까?\n\n진행 상황은 저장되며, 다음에 이어서 진행할 수 있습니다."):
                self.stop_requested = True
                self.log("!!! 작업 중단 요청됨 (진행 상황 저장 중...) !!!")
                # 중단 요청 시 즉시 진행 상황 저장 시도 (엑셀은 I1로 저장하지 않음)
                try:
                    if hasattr(self, 'processed_indices') and hasattr(self, 'progress_file_path'):
                        self.save_progress(self.processed_indices)
                        self.log("[저장] 중단 시 진행 상황 저장 완료 (원본 I0 파일은 유지됩니다)")
                except Exception as e:
                    self.log(f"[경고] 중단 시 진행 상황 저장 실패: {e}")
    
    def load_progress(self) -> set:
        """진행 상황 파일에서 처리 완료된 인덱스 로드"""
        if not self.progress_file_path or not os.path.exists(self.progress_file_path):
            return set()
        
        try:
            with open(self.progress_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 엑셀 파일 경로가 일치하는지 확인
                if data.get('excel_path') == os.path.abspath(self.excel_path):
                    return set(data.get('processed_indices', []))
        except Exception as e:
            self.log(f"[경고] 진행 상황 파일 읽기 실패: {e}")
        return set()
    
    def save_progress(self, processed_indices: set):
        """진행 상황 파일에 처리 완료된 인덱스 저장"""
        if not self.progress_file_path:
            return
        
        try:
            # 디렉토리가 없으면 생성
            progress_dir = os.path.dirname(self.progress_file_path)
            if progress_dir and not os.path.exists(progress_dir):
                os.makedirs(progress_dir, exist_ok=True)
            
            data = {
                'excel_path': os.path.abspath(self.excel_path) if self.excel_path else '',
                'target_excel_path': os.path.abspath(self.target_excel_path) if hasattr(self, 'target_excel_path') and self.target_excel_path else '',
                'processed_indices': sorted(list(processed_indices)),
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_processed': len(processed_indices)
            }
            # 임시 파일에 먼저 쓰고, 성공 시 원본 파일로 교체 (원자적 쓰기)
            temp_file = self.progress_file_path + '.tmp'
            with open(temp_file, 'w', encoding='utf-8', errors='replace') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # 임시 파일을 원본 파일로 교체 (Windows에서도 안전하게)
            if os.path.exists(self.progress_file_path):
                os.remove(self.progress_file_path)
            os.rename(temp_file, self.progress_file_path)
            
        except Exception as e:
            self.log(f"[경고] 진행 상황 파일 저장 실패: {e}")
            debug_log(f"진행 상황 저장 실패: {e}", "ERROR")
            import traceback
            debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
    
    def clear_progress(self):
        """진행 상황 파일 삭제 (작업 완료 시)"""
        if self.progress_file_path and os.path.exists(self.progress_file_path):
            try:
                os.remove(self.progress_file_path)
            except Exception as e:
                self.log(f"[경고] 진행 상황 파일 삭제 실패: {e}")

    def worker(self, out_root, items, preset_name):
        """작업 실행 함수 (예외 발생 시에도 리소스 정리 보장)"""
        global CARVEKIT_IF, REMBG_SESSION, MODEL_RELOAD_NEEDED  # 전역 변수 사용을 위해 함수 시작 부분에 선언
        stopped_by_user = False
        processed_count = 0
        completed_count = 0
        start_ts = None
        
        # 작업 스레드의 우선순위 낮추기 (다른 작업에 영향 최소화)
        try:
            if sys.platform == "win32":
                try:
                    import win32api
                    import win32process
                    import win32con
                    # 현재 스레드 ID 가져오기
                    thread_handle = win32api.OpenThread(win32con.THREAD_SET_INFORMATION, False, threading.get_native_id())
                    if thread_handle:
                        win32process.SetThreadPriority(thread_handle, win32process.THREAD_PRIORITY_BELOW_NORMAL)
                        win32api.CloseHandle(thread_handle)
                except (ImportError, Exception):
                    pass  # 실패해도 계속 진행
        except Exception:
            pass
        
        try:
            # 디버그 로그 초기화 (가장 먼저 실행)
            print(f"[DEBUG] worker 시작 - 출력 루트: {out_root}")
            print(f"[DEBUG] 디버그 로그 활성화: {DEBUG_LOG_ENABLED}")
            
            init_debug_log(out_root)
            
            if DEBUG_LOG_FILE:
                debug_log(f"작업 시작: 총 {len(items)}개 이미지", "INFO")
                debug_log(f"출력 루트: {out_root}", "INFO")
                debug_log(f"프리셋: {preset_name}", "INFO")
                self.ui_queue.put(("log", f"[시스템] 디버그 로그 파일: {os.path.basename(DEBUG_LOG_FILE)}"))
            else:
                self.ui_queue.put(("log", "[경고] 디버그 로그 파일을 생성할 수 없습니다."))
            
            # 1. 모델 로딩
            try:
                model_load_start = time.time()
                debug_log("모델 로딩 시작...", "INFO")
                load_models_if_needed(log_func=lambda m: self.ui_queue.put(("log", m)))
                model_load_elapsed = time.time() - model_load_start
                debug_log(f"모델 로딩 완료 | 소요: {model_load_elapsed:.3f}s", "INFO")
            except Exception as e:
                error_msg = f"[FATAL] 모델 로딩 실패: {e}"
                self.ui_queue.put(("log", error_msg))
                debug_log(error_msg, "ERROR")
                import traceback
                debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
                self.ui_queue.put(("done", out_root, False))
                return

            start_ts = time.time()
            total = len(items)
            os.makedirs(out_root, exist_ok=True)

            # 이미 처리된 항목 필터링
            items_to_process = []
            for idx, item in enumerate(items):
                row_idx = item["meta"].get("excel_row_index")
                if row_idx not in self.processed_indices:
                    items_to_process.append((idx, item))
            
            total_to_process = len(items_to_process)
            if total_to_process == 0:
                self.ui_queue.put(("log", "[완료] 모든 이미지가 이미 처리되었습니다."))
                self.ui_queue.put(("done", out_root, True))
                return

            self.ui_queue.put(("init", total, out_root, start_ts))
            debug_log(f"작업 초기화 완료 | 시작 시간: {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')} | 처리 대상: {total_to_process}개 (전체: {total}개)", "INFO")

            processed_count = 0
            completed_count = len(self.processed_indices)
            
            # 병렬 처리 함수
            def process_single_item(item_data):
                """단일 이미지 처리 함수 (병렬 처리용)"""
                original_idx, item = item_data
                input_path = item["image_path"]
                row_idx = item["meta"].get("excel_row_index")
                
                # 이미 처리된 항목은 스킵
                if row_idx in self.processed_indices:
                    return None, original_idx, "skipped"
                
                # 중단 요청 확인
                if self.stop_requested:
                    return None, original_idx, "stopped"
                
                # 이미지 처리 전 GPU 메모리 확인 및 정리
                allocated_before, reserved_before, total_gpu = check_gpu_memory()
                if allocated_before is not None:
                    usage_before = check_gpu_memory_usage()
                    if usage_before is not None and usage_before >= GPU_MEMORY_CRITICAL_THRESHOLD:
                        # 위험 수준이면 처리 전에 미리 정리
                        debug_log(f"처리 전 GPU 메모리 정리 (사용률: {usage_before*100:.1f}%)", "WARN")
                        gc.collect()
                        clear_gpu_cache(aggressive=True)
                        time.sleep(0.2)  # 정리 후 잠시 대기
                
                try:
                    entry = process_one_image(
                        input_path,
                        out_root,
                        log=lambda m: self.ui_queue.put(("log", m)),
                        preset_name=preset_name,
                        mapping_collector=None,
                        extra_meta=item["meta"]
                    )
                    
                    # 처리 후 GPU 메모리 정리 (주기적으로만, 성능 우선)
                    if processed_count % CLEAR_CACHE_INTERVAL == 0:
                        gc.collect()
                        clear_gpu_cache(aggressive=False)  # 기본 정리로 충분
                    elif allocated_before is not None:
                        allocated_after, reserved_after, _ = check_gpu_memory()
                        if allocated_after is not None:
                            # 메모리가 크게 증가했으면 추가 정리
                            if allocated_after > allocated_before * 1.2:  # 20% 이상 증가
                                debug_log(f"처리 후 GPU 메모리 증가 감지 - 추가 정리 (전: {allocated_before:.1f}MB, 후: {allocated_after:.1f}MB)", "DEBUG")
                                gc.collect()
                                clear_gpu_cache(aggressive=True)  # 더 적극적으로 정리
                    
                    # 엑셀 업데이트 (락 사용)
                    if entry is not None and self.df is not None:
                        row_idx = entry.get("excel_row_index")
                        out_abs = entry.get("output_abs")
                        png_out_abs = entry.get("png_output_abs")
                        if row_idx is not None:
                            try:
                                with self.df_lock:
                                    if out_abs:
                                        self.df.at[row_idx, COL_IMG_OUT] = out_abs
                                    if png_out_abs:
                                        self.df.at[row_idx, COL_IMG_OUT_PNG] = png_out_abs
                            except Exception as e:
                                self.ui_queue.put(("log", f"[WARN] 엑셀 업데이트 실패 (행 {row_idx}): {e}"))
                    
                    return entry, original_idx, "success"
                    
                except TimeoutError as e:
                    # 타임아웃은 process_one_image 내부에서 재시도하므로 여기까지 오면 모든 시도 실패
                    error_msg = f"[실패] {os.path.basename(input_path)} 모든 시도 타임아웃: {e}"
                    self.ui_queue.put(("log", error_msg))
                    debug_log(f"모든 시도 타임아웃: {os.path.basename(input_path)} | 오류: {e}", "ERROR")
                    
                    # 엑셀에 실패 기록
                    if self.df is not None:
                        try:
                            with self.df_lock:
                                if row_idx is not None:
                                    self.df.at[row_idx, COL_IMG_OUT] = f"[타임아웃] 모든 시도 실패"
                        except Exception as excel_err:
                            debug_log(f"엑셀 업데이트 실패: {excel_err}", "WARN")
                    
                    return None, original_idx, "timeout"
                except Exception as e:
                    error_msg = str(e)
                    log_msg = f"[SKIP] {os.path.basename(input_path)} 오류: {error_msg}"
                    self.ui_queue.put(("log", log_msg))
                    debug_log(f"처리 실패: {os.path.basename(input_path)} | 오류: {error_msg}", "ERROR")
                    import traceback
                    debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
                    
                    # 엑셀에 실패 기록
                    if self.df is not None:
                        try:
                            with self.df_lock:
                                if row_idx is not None:
                                    self.df.at[row_idx, COL_IMG_OUT] = f"[오류] {error_msg[:50]}"
                        except Exception as excel_err:
                            debug_log(f"엑셀 업데이트 실패: {excel_err}", "WARN")
                    
                    return None, original_idx, "error"
            
            # 병렬 처리 실행 (GPU 안정성 우선으로 기본 비활성화)
            # GPU 모델(CarveKit, rembg)은 thread-safe하지 않아 병렬 처리 시 충돌 및 메모리 누수 발생
            # GPU 메모리 과부하 방지를 위해 순차 처리 권장
            use_parallel = False  # GPU 안정성을 위해 병렬 처리 비활성화
            
            if use_parallel:
                self.ui_queue.put(("log", f"[시스템] 병렬 처리 모드 (최대 {MAX_WORKERS}개 동시 처리)"))
                debug_log(f"병렬 처리 시작: {total_to_process}개 이미지 | 최대 {MAX_WORKERS}개 동시 처리", "INFO")
                
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_item = {executor.submit(process_single_item, item_data): item_data 
                                     for item_data in items_to_process}
                    
                    for future in as_completed(future_to_item):
                        if self.stop_requested:
                            stopped_by_user = True
                            stop_msg = ">>> 사용자에 의해 작업이 중단되었습니다."
                            self.ui_queue.put(("log", stop_msg))
                            debug_log(stop_msg, "WARN")
                            # 남은 작업 취소
                            for f in future_to_item:
                                f.cancel()
                            break
                        
                        try:
                            entry, original_idx, status = future.result()
                            item = items[original_idx]
                            row_idx = item["meta"].get("excel_row_index")
                            
                            if status == "success" and entry is not None:
                                processed_count += 1
                                completed_count += 1
                                self.processed_indices.add(row_idx)
                                
                            # 주기적으로 진행 상황 저장
                            if processed_count % PROGRESS_SAVE_INTERVAL == 0:
                                self.save_progress(self.processed_indices)
                                # 주기적으로 엑셀 중간 저장 (실제 파일로 저장하여 중단 시에도 복구 가능)
                                if processed_count % EXCEL_SAVE_INTERVAL == 0 and not self.stop_requested:
                                    try:
                                        excel_dir = os.path.dirname(self.target_excel_path)
                                        if excel_dir and not os.path.exists(excel_dir):
                                            os.makedirs(excel_dir, exist_ok=True)
                                        
                                        # 실제 I1 파일로 저장 (중단 시에도 복구 가능하도록)
                                        with self.df_lock:
                                            self.df.to_excel(self.target_excel_path, index=False, engine='openpyxl')
                                        
                                        debug_log(f"주기적 엑셀 중간 저장 완료: {processed_count}개 처리됨", "INFO")
                                        self.ui_queue.put(("log", f"[저장] 엑셀 중간 저장 완료: {processed_count}개 처리됨"))
                                    except Exception as e:
                                        debug_log(f"주기적 엑셀 중간 저장 실패: {e}", "WARN")
                                        self.ui_queue.put(("log", f"[경고] 엑셀 중간 저장 실패: {e}"))
                            elif status == "skipped":
                                completed_count += 1
                            elif status in ["timeout", "error"]:
                                processed_count += 1  # 시도는 했으므로 카운트
                                if row_idx is not None:
                                    self.processed_indices.add(row_idx)  # 실패해도 인덱스는 기록
                            
                            # 진행 상황 업데이트
                            elapsed = time.time() - start_ts
                            current_total = completed_count
                            if current_total > 0:
                                avg = elapsed / current_total
                                remain = avg * (total - current_total)
                                self.ui_queue.put(("progress", current_total, total, elapsed, remain))
                            
                            # GPU 메모리 주기적 정리 (병렬 처리 모드에서는 더 신중하게)
                            should_clear_gpu = False
                            allocated, reserved, total_gpu = check_gpu_memory()
                            if allocated is not None:
                                usage = check_gpu_memory_usage()
                                if usage is not None:
                                    # 주기적 정리
                                    if processed_count % CLEAR_CACHE_INTERVAL == 0:
                                        should_clear_gpu = True
                                        debug_log(f"주기적 GPU 메모리 정리 (처리된 이미지: {processed_count}개) | GPU: {allocated:.1f}MB/{total_gpu:.1f}MB ({usage*100:.1f}%)", "DEBUG")
                                    # 경고 수준 이상이면 즉시 정리
                                    elif usage >= GPU_MEMORY_WARNING_THRESHOLD:
                                        should_clear_gpu = True
                                        debug_log(f"⚠️ GPU 메모리 경고 ({usage*100:.1f}%) - 즉시 정리 | GPU: {allocated:.1f}MB/{total_gpu:.1f}MB", "WARN")
                                    # 위험 수준이면 적극적 정리
                                    if usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                                        should_clear_gpu = True
                                        debug_log(f"⚠️ GPU 메모리 위험 ({usage*100:.1f}%) - 강제 정리 | GPU: {allocated:.1f}MB/{total_gpu:.1f}MB", "ERROR")
                            
                            if should_clear_gpu:
                                gc.collect()
                                clear_gpu_cache(aggressive=(allocated is not None and usage is not None and usage >= GPU_MEMORY_CRITICAL_THRESHOLD))
                            
                            # 타임아웃 발생 시 모델 재로딩 (백그라운드 스레드 충돌 방지)
                            if MODEL_RELOAD_NEEDED:
                                debug_log(f"[긴급] 타임아웃 발생으로 인한 모델 재로딩 시작...", "WARN")
                                self.ui_queue.put(("log", "[긴급] 타임아웃 발생 - 모델 재로딩 중..."))
                                try:
                                    # 모델 재로딩 전 GPU 메모리 완전 정리
                                    for _ in range(5):
                                        gc.collect()
                                    clear_gpu_cache(aggressive=True, skip_sync=True)
                                    time.sleep(3.0)  # 백그라운드 스레드가 완전히 종료될 시간 확보
                                    
                                    # 모델 재로딩 (global은 함수 시작 부분에 이미 선언됨)
                                    CARVEKIT_IF = None
                                    REMBG_SESSION = None
                                    load_models_if_needed(lambda msg: debug_log(msg, "INFO"), force_cpu=False)
                                    
                                    MODEL_RELOAD_NEEDED = False
                                    debug_log(f"[긴급] 모델 재로딩 완료", "INFO")
                                    self.ui_queue.put(("log", "[긴급] 모델 재로딩 완료 - 계속 진행합니다."))
                                except Exception as reload_err:
                                    debug_log(f"[긴급] 모델 재로딩 실패: {reload_err}", "ERROR")
                                    self.ui_queue.put(("log", f"[경고] 모델 재로딩 실패: {reload_err}"))
                            
                            # 주기적으로 모델 재로딩 (병렬 처리 모드)
                            if processed_count > 0 and processed_count % MODEL_RELOAD_INTERVAL == 0:
                                try:
                                    debug_log(f"주기적 모델 재로딩 시작 (처리된 이미지: {processed_count}개)", "INFO")
                                    # 기존 모델 해제 (global은 함수 시작 부분에 선언됨)
                                    CARVEKIT_IF = None
                                    REMBG_SESSION = None
                                    for _ in range(3):
                                        gc.collect()
                                    clear_gpu_cache(aggressive=True)
                                    time.sleep(2)  # 더 긴 대기
                                    load_models_if_needed(log_func=lambda m: self.ui_queue.put(("log", m)))
                                    debug_log(f"주기적 모델 재로딩 완료", "INFO")
                                except Exception as reload_err:
                                    debug_log(f"주기적 모델 재로딩 실패: {reload_err}", "WARN")
                        
                        except Exception as e:
                            debug_log(f"병렬 처리 중 오류: {e}", "ERROR")
                            import traceback
                            debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
            else:
                # 순차 처리 (기존 방식, 안정적)
                self.ui_queue.put(("log", "[시스템] 순차 처리 모드"))
                debug_log(f"순차 처리 시작: {total_to_process}개 이미지", "INFO")
            
            for original_idx, item in items_to_process:
                if self.stop_requested:
                    stopped_by_user = True
                    stop_msg = ">>> 사용자에 의해 작업이 중단되었습니다."
                    self.ui_queue.put(("log", stop_msg))
                    debug_log(stop_msg, "WARN")
                    break
                
                entry, _, status = process_single_item((original_idx, item))
                row_idx = item["meta"].get("excel_row_index")
                
                if status == "success" and entry is not None:
                    processed_count += 1
                    completed_count += 1
                    self.processed_indices.add(row_idx)
                elif status == "skipped":
                    completed_count += 1
                elif status in ["timeout", "error"]:
                    processed_count += 1
                    if row_idx is not None:
                        self.processed_indices.add(row_idx)
                
                # 주기적으로 진행 상황 저장
                if processed_count % PROGRESS_SAVE_INTERVAL == 0:
                    self.save_progress(self.processed_indices)
                    # 주기적으로 엑셀 중간 저장 (실제 파일로 저장하여 중단 시에도 복구 가능)
                    if processed_count % EXCEL_SAVE_INTERVAL == 0 and not self.stop_requested:
                        try:
                            excel_dir = os.path.dirname(self.target_excel_path)
                            if excel_dir and not os.path.exists(excel_dir):
                                os.makedirs(excel_dir, exist_ok=True)
                            
                            # 실제 I1 파일로 저장 (중단 시에도 복구 가능하도록)
                            with self.df_lock:
                                self.df.to_excel(self.target_excel_path, index=False, engine='openpyxl')
                            
                            debug_log(f"주기적 엑셀 중간 저장 완료: {processed_count}개 처리됨", "INFO")
                            self.ui_queue.put(("log", f"[저장] 엑셀 중간 저장 완료: {processed_count}개 처리됨"))
                        except Exception as e:
                            debug_log(f"주기적 엑셀 중간 저장 실패: {e}", "WARN")
                            self.ui_queue.put(("log", f"[경고] 엑셀 중간 저장 실패: {e}"))
                
                # 진행 상황 업데이트
                elapsed = time.time() - start_ts
                if completed_count > 0:
                    avg = elapsed / completed_count
                    remain = avg * (total - completed_count)
                    self.ui_queue.put(("progress", completed_count, total, elapsed, remain))
                
                # GPU 메모리 정리 (주기적으로만, 성능 우선)
                allocated, reserved, total_gpu = check_gpu_memory()
                usage = None
                if allocated is not None:
                    usage = check_gpu_memory_usage()
                
                # 주기적으로만 GPU 메모리 정리
                if processed_count % CLEAR_CACHE_INTERVAL == 0:
                    gc.collect()  # Python GC 먼저 실행
                    aggressive_mode = (usage is not None and usage >= GPU_MEMORY_CRITICAL_THRESHOLD)
                    clear_gpu_cache(aggressive=aggressive_mode)
                    
                    # 로깅
                    if allocated is not None and usage is not None:
                        debug_log(f"GPU 메모리 상태 (처리된 이미지: {processed_count}개) | GPU: {allocated:.1f}MB/{total_gpu:.1f}MB ({usage*100:.1f}%)", "DEBUG")
                
                # 위험 수준 체크 및 경고 (즉시 정리)
                if usage is not None:
                    if usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                        debug_log(f"⚠️ GPU 메모리 위험 ({usage*100:.1f}%) - 강제 정리 | GPU: {allocated:.1f}MB/{total_gpu:.1f}MB", "ERROR")
                        gc.collect()
                        clear_gpu_cache(aggressive=True)
                        # 매우 위험하면 CPU 모드 전환 고려
                        if usage >= GPU_MEMORY_FORCE_CPU_THRESHOLD:
                            self.ui_queue.put(("log", f"[경고] GPU 메모리 부족 ({usage*100:.1f}%) - CPU 모드 전환 권장"))
                            debug_log(f"⚠️ GPU 메모리 매우 위험 ({usage*100:.1f}%) - CPU 모드 전환 고려", "ERROR")
                
                # 타임아웃 발생 시 모델 재로딩 (백그라운드 스레드 충돌 방지)
                if MODEL_RELOAD_NEEDED:
                    debug_log(f"[긴급] 타임아웃 발생으로 인한 모델 재로딩 시작...", "WARN")
                    self.ui_queue.put(("log", "[긴급] 타임아웃 발생 - 모델 재로딩 중..."))
                    try:
                        # 모델 재로딩 전 GPU 메모리 완전 정리
                        for _ in range(5):
                            gc.collect()
                        clear_gpu_cache(aggressive=True, skip_sync=True)
                        time.sleep(3.0)  # 백그라운드 스레드가 완전히 종료될 시간 확보
                        
                        # 모델 재로딩 (global은 함수 시작 부분에 이미 선언됨)
                        CARVEKIT_IF = None
                        REMBG_SESSION = None
                        load_models_if_needed(lambda msg: debug_log(msg, "INFO"), force_cpu=False)
                        
                        MODEL_RELOAD_NEEDED = False
                        debug_log(f"[긴급] 모델 재로딩 완료", "INFO")
                        self.ui_queue.put(("log", "[긴급] 모델 재로딩 완료 - 계속 진행합니다."))
                    except Exception as reload_err:
                        debug_log(f"[긴급] 모델 재로딩 실패: {reload_err}", "ERROR")
                        self.ui_queue.put(("log", f"[경고] 모델 재로딩 실패: {reload_err}"))
                
                # 주기적으로 모델 재로딩 (GPU 메모리 누적 방지)
                if processed_count > 0 and processed_count % MODEL_RELOAD_INTERVAL == 0:
                    try:
                        debug_log(f"주기적 모델 재로딩 시작 (처리된 이미지: {processed_count}개)", "INFO")
                        # 기존 모델 해제 (global은 함수 시작 부분에 선언됨)
                        CARVEKIT_IF = None
                        REMBG_SESSION = None
                        # GPU 메모리 강제 정리 (더 적극적으로)
                        for _ in range(3):
                            gc.collect()
                        clear_gpu_cache(aggressive=True)
                        time.sleep(2)  # 더 긴 대기
                        # 모델 재로딩
                        load_models_if_needed(log_func=lambda m: self.ui_queue.put(("log", m)))
                        debug_log(f"주기적 모델 재로딩 완료", "INFO")
                    except Exception as reload_err:
                        debug_log(f"주기적 모델 재로딩 실패: {reload_err}", "WARN")
                        # 재로딩 실패해도 계속 진행
                
                # 타임아웃 발생 빈도가 높으면 모델 재로딩 (타임아웃으로 인한 GPU 메모리 누적 방지)
                # 타임아웃 발생 횟수를 추적하는 로직은 복잡하므로, GPU 메모리 사용률이 높으면 모델 재로딩
                if allocated is not None:
                    usage = check_gpu_memory_usage()
                    if usage is not None and usage >= GPU_MEMORY_CRITICAL_THRESHOLD:
                        # GPU 메모리가 위험 수준이면 모델 재로딩 고려
                        if processed_count > 0 and processed_count % 10 == 0:  # 10장마다 체크
                            try:
                                debug_log(f"GPU 메모리 위험으로 인한 모델 재로딩 시작 (사용률: {usage*100:.1f}%)", "WARN")
                                CARVEKIT_IF = None
                                REMBG_SESSION = None
                                for _ in range(5):
                                    gc.collect()
                                clear_gpu_cache(aggressive=True)
                                time.sleep(2)
                                load_models_if_needed(log_func=lambda m: self.ui_queue.put(("log", m)))
                                debug_log(f"GPU 메모리 위험으로 인한 모델 재로딩 완료", "INFO")
                            except Exception as reload_err:
                                debug_log(f"긴급 모델 재로딩 실패: {reload_err}", "WARN")

        except Exception as worker_error:
            # 예상치 못한 예외 발생 시 처리
            error_msg = f"[FATAL] 작업 중 예상치 못한 오류 발생: {worker_error}"
            self.ui_queue.put(("log", error_msg))
            debug_log(error_msg, "ERROR")
            import traceback
            debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
            stopped_by_user = True  # 예외 발생 시 중단으로 처리
        
        finally:
            # 예외 발생 여부와 관계없이 항상 실행 (리소스 정리 보장)
            try:
                # 최종 진행 상황 저장 (예외 발생 시에도 보존)
                if hasattr(self, 'processed_indices'):
                    self.save_progress(self.processed_indices)
                
                # GPU 메모리 최종 정리
                gc.collect()
                if torch.cuda.is_available():
                    clear_gpu_cache(aggressive=True)
                    debug_log("작업 종료: GPU 메모리 최종 정리 완료", "INFO")
                
                # [MODIFIED] 엑셀 저장 로직 변경
                # 중간 저장은 이미 주기적으로 I1 파일로 저장되고 있으므로, 
                # 완료 시에는 최종 저장만 수행하고, 중단 시에는 이미 저장된 I1 파일을 유지
                if self.df is not None:
                    if not stopped_by_user:
                        # 작업이 성공적으로 완료된 경우 최종 저장 (이미 중간 저장으로 I1 파일이 있음)
                        try:
                            excel_save_start = time.time()
                            debug_log(f"엑셀 최종 저장 시작: {self.target_excel_path}", "INFO")
                            
                            # 디렉토리가 없으면 생성
                            excel_dir = os.path.dirname(self.target_excel_path)
                            if excel_dir and not os.path.exists(excel_dir):
                                os.makedirs(excel_dir, exist_ok=True)
                            
                            # IMG_S1_누끼가 있는 행과 없는 행 분리
                            df_with_img = None
                            df_no_img = None
                            no_img_path = None
                            
                            with self.df_lock:
                                if COL_IMG_OUT in self.df.columns:
                                    # IMG_S1_누끼가 비어있거나 None인 행 찾기
                                    df_with_img = self.df[self.df[COL_IMG_OUT].notna() & (self.df[COL_IMG_OUT] != '') & (~self.df[COL_IMG_OUT].astype(str).str.startswith('[타임아웃]')) & (~self.df[COL_IMG_OUT].astype(str).str.startswith('[오류]'))].copy()
                                    df_no_img = self.df[(self.df[COL_IMG_OUT].isna()) | (self.df[COL_IMG_OUT] == '') | (self.df[COL_IMG_OUT].astype(str).str.startswith('[타임아웃]')) | (self.df[COL_IMG_OUT].astype(str).str.startswith('[오류]'))].copy()
                                else:
                                    # 컬럼이 없으면 모든 행이 이미지 없음으로 처리
                                    df_with_img = pd.DataFrame()
                                    df_no_img = self.df.copy()
                                
                                # IMG_S1_누끼가 없는 행들을 I1(실패) 버전으로 별도 파일 저장
                                if len(df_no_img) > 0:
                                    base_dir = os.path.dirname(self.target_excel_path)
                                    base_name, ext = os.path.splitext(os.path.basename(self.target_excel_path))
                                    
                                    # 현재 파일명에서 버전 정보 추출 (예: _T0_I1)
                                    # I1(실패) 버전으로 변경
                                    name_only_clean = re.sub(r"\([^)]*\)", "", base_name)  # 기존 괄호 제거
                                    all_matches = list(re.finditer(r"_([Tt])(\d+)_([Ii])(\d+)", name_only_clean, re.IGNORECASE))
                                    
                                    if all_matches:
                                        # 마지막 버전 패턴 사용
                                        match = all_matches[-1]
                                        original_name = name_only_clean[: match.start()].rstrip("_")
                                        current_t = int(match.group(2))
                                        current_i = int(match.group(4))
                                        # I1(실패) 버전으로 생성
                                        new_filename = f"{original_name}_T{current_t}_I{current_i}(실패){ext}"
                                    else:
                                        # 버전 패턴이 없으면 기본적으로 I1(실패)로 생성
                                        new_filename = f"{base_name}(실패){ext}"
                                    
                                    no_img_path = os.path.join(base_dir, new_filename)
                                    df_no_img.to_excel(no_img_path, index=False, engine='openpyxl')
                                    
                                    self.ui_queue.put(("log", f" - I1(실패) 분리 파일: {os.path.basename(no_img_path)} ({len(df_no_img)}개 행)"))
                                    self.ui_queue.put(("log", f"   ※ 이 파일은 배경 제거 작업에 실패한 항목입니다."))
                                    debug_log(f"I1(실패) 분리 파일 생성: {no_img_path} ({len(df_no_img)}개 행)", "INFO")
                                    
                                    # 분리된 파일의 런처 상태 업데이트
                                    try:
                                        no_img_root_name = get_root_filename(no_img_path)
                                        JobManager.update_status(no_img_root_name, img_msg="I1(실패)")
                                        debug_log(f"런처 현황판 업데이트: {no_img_root_name} -> I1(실패)", "INFO")
                                    except Exception as e:
                                        debug_log(f"런처 업데이트 실패: {e}", "WARN")
                                
                                # IMG_S1_누끼가 있는 행들만 저장
                                if len(df_with_img) > 0:
                                    self.df = df_with_img
                                else:
                                    self.ui_queue.put(("log", "⚠️ IMG_S1_누끼가 있는 행이 없습니다."))
                                    debug_log("⚠️ IMG_S1_누끼가 있는 행이 없습니다.", "WARN")
                            
                            # 최종 저장 (중간 저장으로 이미 I1 파일이 있으므로 덮어쓰기)
                            with self.df_lock:
                                self.df.to_excel(self.target_excel_path, index=False, engine='openpyxl')
                            
                            excel_save_elapsed = time.time() - excel_save_start
                            save_msg = f"[저장] 엑셀 최종 저장 완료: {os.path.basename(self.target_excel_path)}"
                            self.ui_queue.put(("log", save_msg))
                            debug_log(f"엑셀 최종 저장 완료 | 소요: {excel_save_elapsed:.3f}s", "INFO")
                            
                            # [NEW] 런처 현황판 업데이트 (완료 알림) - img 상태만 I1(완료)로 업데이트 (text 상태는 변경하지 않음)
                            try:
                                root_name = get_root_filename(self.excel_path)
                                JobManager.update_status(root_name, img_msg="I1(완료)")
                                debug_log(f"런처 현황판 업데이트: {root_name} -> I1(완료)", "INFO")
                                # 작업 완료 시 진행 상황 파일 삭제
                                self.clear_progress()
                            except Exception as launcher_err:
                                debug_log(f"런처 업데이트 실패: {launcher_err}", "WARN")
                                
                        except Exception as e:
                            error_msg = f"[경고] 엑셀 최종 저장 실패: {e}"
                            self.ui_queue.put(("log", error_msg))
                            debug_log(error_msg, "ERROR")
                            import traceback
                            debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
                    else:
                        # 중단된 경우: 이미 중간 저장으로 I1 파일이 저장되어 있음
                        # 다음에 I1 파일을 읽어서 빈 셀부터 이어서 진행 가능
                        self.ui_queue.put(("log", f"[정보] 작업이 중단되었습니다. I1 파일에 진행 상황이 저장되었습니다. 다음에 I1 파일을 불러와서 빈 셀부터 이어서 진행할 수 있습니다."))
                        debug_log("작업 중단: I1 파일에 진행 상황 저장됨, 다음에 빈 셀부터 재개 가능", "INFO")
                
                # 작업 완료 로그
                if start_ts is not None:
                    total_elapsed = time.time() - start_ts
                    if stopped_by_user:
                        debug_log(f"작업 중단됨 (사용자 요청) | 총 소요: {total_elapsed:.3f}s", "WARN")
                    else:
                        debug_log(f"작업 완료 | 총 소요: {total_elapsed:.3f}s | 처리된 이미지: {processed_count}개", "INFO")
                    
                    if DEBUG_LOG_FILE:
                        try:
                            with open(DEBUG_LOG_FILE, "a", encoding="utf-8", errors='replace', newline='\n') as f:
                                f.write(f"\n{'='*50}\n")
                                f.write(f"작업 종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                                f.write(f"총 소요 시간: {format_time(total_elapsed)}\n")
                                if stopped_by_user:
                                    f.write(f"작업 상태: 사용자에 의해 중단됨\n")
                                else:
                                    f.write(f"작업 상태: 정상 완료\n")
                                f.write(f"처리된 이미지: {processed_count}개 / 전체: {len(items)}개\n")
                                f.write(f"{'='*50}\n")
                                f.flush()  # 즉시 디스크에 쓰기
                        except Exception as e:
                            debug_log(f"작업 종료 로그 기록 실패: {e}", "WARN")
                
                # 중단 여부에 따라 completed 플래그 변경
                self.ui_queue.put(("done", out_root, not stopped_by_user))
                
            except Exception as finally_error:
                # finally 블록 내에서도 예외가 발생할 수 있으므로 처리
                error_msg = f"[FATAL] 리소스 정리 중 오류 발생: {finally_error}"
                self.ui_queue.put(("log", error_msg))
                debug_log(error_msg, "ERROR")
                import traceback
                debug_log(f"스택 트레이스:\n{traceback.format_exc()}", "ERROR")
                # 최소한 완료 신호는 보내기
                self.ui_queue.put(("done", out_root, False))



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