"""
IMG_mixing.py

Stage 4-2: 배경 합성 (ComfyUI)
- 기능: I4 엑셀 파일에서 전경(누끼)과 배경 이미지를 읽어 ComfyUI로 합성
- 입력: I4 파일만 허용
- 출력: 합성된 이미지를 엑셀에 매핑
"""

import os
import json
import re
import time
import threading
import queue
import uuid
import websocket
import socket
import subprocess
import traceback
import zipfile
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from pathlib import Path

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

try:
    from PIL import Image, ImageTk
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# ========================================================
# 디버그 로그 시스템
# ========================================================
DEBUG_LOG_ENABLED = True
DEBUG_LOG_FILE = None  # 작업 시작 시 설정됨

def debug_log(message: str, level: str = "INFO"):
    """
    디버그 로그 기록
    level: INFO, WARN, ERROR, DEBUG
    """
    global DEBUG_LOG_FILE
    if not DEBUG_LOG_ENABLED:
        return
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_entry = f"[{timestamp}] [{level}] {message}\n"
    
    if not DEBUG_LOG_FILE:
        # 파일이 초기화되지 않았으면 콘솔에만 출력
        print(f"[DEBUG_LOG] {message}")
        return
    
    try:
        with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except Exception as e:
        print(f"[DEBUG_LOG] 로그 파일 쓰기 실패: {e}")

def init_debug_log(output_root: str):
    """
    디버그 로그 파일 초기화
    output_root: 로그 파일을 저장할 디렉토리
    """
    global DEBUG_LOG_FILE
    if not DEBUG_LOG_ENABLED:
        return
    
    try:
        # 출력 디렉토리 생성
        os.makedirs(output_root, exist_ok=True)
        
        # 로그 파일명 생성 (타임스탬프 포함)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"img_mixing_debug_{timestamp}.log"
        DEBUG_LOG_FILE = os.path.join(output_root, log_filename)
        
        # 로그 파일 초기화
        with open(DEBUG_LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"=== 배경 합성 디버그 로그 ===\n")
            f.write(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"로그 파일: {log_filename}\n")
            f.write("=" * 50 + "\n\n")
        
        print(f"[INFO] 디버그 로그 파일 생성: {DEBUG_LOG_FILE}")
        debug_log(f"디버그 로그 파일 초기화 완료: {log_filename}", "INFO")
    except Exception as e:
        print(f"[ERROR] 디버그 로그 파일 생성 실패: {e}")
        DEBUG_LOG_FILE = None

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _t*_i*, _T*(완)_I* 포함) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 나이키_T0_I0.xlsx -> 나이키.xlsx
    예: 아디다스_T3_I5.xlsx -> 아디다스.xlsx
    예: 나이키_T0_I0(업완).xlsx -> 나이키.xlsx
    예: 나이키_T0_I0_T1_I1.xlsx -> 나이키.xlsx (여러 버전 패턴 제거)
    예: 상품_T4(완)_I4.xlsx -> 상품.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)
    
    # 1. 버전 패턴 (_T숫자(완)?_I숫자 또는 _t숫자(완)?_i숫자) 반복 제거 (대소문자 구분 없음)
    # 패턴이 여러 번 나올 수 있으므로 반복 제거, (완) 부분도 함께 제거
    while True:
        new_base = re.sub(r"_[Tt]\d+(\(완\))?_[Ii]\d+", "", base, flags=re.IGNORECASE)
        if new_base == base:
            break
        base = new_base
    
    # 2. 괄호 안의 텍스트 제거 (예: (업완), (완료) 등) - 버전 패턴의 (완)은 이미 제거됨
    base = re.sub(r"\([^)]*\)", "", base)
    
    # 3. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_bg_prompt_done", "_bg_prompt_batch_done", "_bg_generation_done", "_bg_mixing_done", "_stage1_mapping", "_stage1_img_mapping", "_stage2_analysis", "_stage3_done", "_stage4_2_done", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")
    
    # 4. 끝에 남은 언더스코어 제거
    base = base.rstrip("_")
        
    return base + ext

def get_i5_output_path(input_path: str) -> str:
    """
    입력 파일명을 분석해서 I5로 고정된 출력 파일명을 생성합니다.
    입력: I4 파일 (예: 상품_T3_I4.xlsx, 상품_T4_I4.xlsx, 상품_T4(완)_I4.xlsx)
    출력: 항상 I5 (예: 상품_T3_I5.xlsx, 상품_T4_I5.xlsx, 상품_T4(완)_I5.xlsx)
    """
    dir_name = os.path.dirname(input_path)
    base_name = os.path.basename(input_path)
    name_only, ext = os.path.splitext(base_name)

    # _T4(완)_I4 또는 _T4_I4 패턴 매칭
    pattern = r"_T(\d+)(\(완\))?_I(\d+)$"
    match = re.search(pattern, name_only, re.IGNORECASE)

    if match:
        current_t = int(match.group(1))
        t_suffix = match.group(2) or ""  # (완) 부분이 있으면 유지
        original_name = name_only[: match.start()]
        # 항상 I5로 고정, T 부분은 그대로 유지 (예: _T4(완)_I5)
        new_filename = f"{original_name}_T{current_t}{t_suffix}_I5{ext}"
    else:
        # 버전 정보가 없으면 T 버전 추출 시도 (괄호 포함 가능)
        t_match = re.search(r"_T(\d+)(\(완\))?", name_only, re.IGNORECASE)
        if t_match:
            current_t = int(t_match.group(1))
            t_suffix = t_match.group(2) or ""
            original_name = name_only[: t_match.start()]
            new_filename = f"{original_name}_T{current_t}{t_suffix}_I5{ext}"
        else:
            current_t = 0
            original_name = name_only
            new_filename = f"{original_name}_T{current_t}_I5{ext}"
    
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
    def update_status(cls, filename, text_msg=None, img_msg=None, img_s3_1_msg=None, img_s3_2_msg=None, img_s4_1_msg=None, img_s4_2_msg=None, img_s5_1_msg=None, img_s5_2_msg=None):
        """
        작업 상태를 업데이트합니다.
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
                "image_s3_1_status": "-",
                "image_s3_1_time": "-",
                "image_s3_2_status": "-",
                "image_s3_2_time": "-",
                "image_s4_1_status": "-",
                "image_s4_1_time": "-",
                "image_s4_2_status": "-",
                "image_s4_2_time": "-",
                "image_s5_1_status": "-",
                "image_s5_1_time": "-",
                "image_s5_2_status": "-",
                "image_s5_2_time": "-",
                "memo": "",
            }

        if text_msg:
            data[filename]["text_status"] = text_msg
            data[filename]["text_time"] = now
        
        # img_msg가 있으면 우선적으로 사용 (하위 호환성)
        if img_msg:
            data[filename]["image_status"] = img_msg
            data[filename]["image_time"] = now
        
        # 가장 최근 단계만 표시하도록 통합 업데이트 (우선순위: I5 > I4 > I3)
        def update_image_status_from_stages():
            """가장 최근 단계만 표시하도록 image_status 업데이트"""
            if img_msg:
                return  # img_msg가 있으면 그대로 사용
            
            parts = []
            img_s5_1 = data[filename].get("image_s5_1_status", "-")
            img_s5_2 = data[filename].get("image_s5_2_status", "-")
            img_s4_1 = data[filename].get("image_s4_1_status", "-")
            img_s4_2 = data[filename].get("image_s4_2_status", "-")
            img_s3_1 = data[filename].get("image_s3_1_status", "-")
            img_s3_2 = data[filename].get("image_s3_2_status", "-")
            
            if img_s5_1 != "-" or img_s5_2 != "-":
                # I5 단계 표시
                if img_s5_1 != "-":
                    parts.append(img_s5_1)
                if img_s5_2 != "-":
                    parts.append(img_s5_2)
                i_time = (data[filename].get("image_s5_2_time") or 
                         data[filename].get("image_s5_1_time") or 
                         data[filename].get("image_time", now))
            elif img_s4_1 != "-" or img_s4_2 != "-":
                # I4 단계 표시
                if img_s4_1 != "-":
                    parts.append(img_s4_1)
                if img_s4_2 != "-":
                    parts.append(img_s4_2)
                i_time = (data[filename].get("image_s4_2_time") or 
                         data[filename].get("image_s4_1_time") or 
                         data[filename].get("image_time", now))
            elif img_s3_1 != "-" or img_s3_2 != "-":
                # I3 단계 표시
                if img_s3_1 != "-":
                    parts.append(img_s3_1)
                if img_s3_2 != "-":
                    parts.append(img_s3_2)
                i_time = (data[filename].get("image_s3_2_time") or 
                         data[filename].get("image_s3_1_time") or 
                         data[filename].get("image_time", now))
            
            if parts:
                data[filename]["image_status"] = " / ".join(parts)
                data[filename]["image_time"] = i_time
        
        if img_s3_1_msg:
            data[filename]["image_s3_1_status"] = img_s3_1_msg
            data[filename]["image_s3_1_time"] = now
            update_image_status_from_stages()
        
        if img_s3_2_msg:
            data[filename]["image_s3_2_status"] = img_s3_2_msg
            data[filename]["image_s3_2_time"] = now
            update_image_status_from_stages()
        
        if img_s4_1_msg:
            data[filename]["image_s4_1_status"] = img_s4_1_msg
            data[filename]["image_s4_1_time"] = now
            update_image_status_from_stages()
        
        if img_s4_2_msg:
            data[filename]["image_s4_2_status"] = img_s4_2_msg
            data[filename]["image_s4_2_time"] = now
            update_image_status_from_stages()
        
        if img_s5_1_msg:
            data[filename]["image_s5_1_status"] = img_s5_1_msg
            data[filename]["image_s5_1_time"] = now
            update_image_status_from_stages()
        
        if img_s5_2_msg:
            data[filename]["image_s5_2_status"] = img_s5_2_msg
            data[filename]["image_s5_2_time"] = now
            update_image_status_from_stages()

        data[filename]["last_update"] = now

        try:
            with open(db_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[JobManager Error] {e}")

def safe_save_excel(df: pd.DataFrame, path: str) -> bool:
    """엑셀 파일 저장 (재시도 포함)"""
    # 엑셀 파일 확장자에 따라 엔진 지정
    _, ext = os.path.splitext(path.lower())
    engine = 'openpyxl' if ext == '.xlsx' else 'xlwt' if ext == '.xls' else 'openpyxl'
    
    while True:
        try:
            df.to_excel(path, index=False, engine=engine)
            return True
        except PermissionError:
            if not messagebox.askretrycancel("저장 실패", f"엑셀 파일이 열려있습니다!\n[{os.path.basename(path)}]\n\n파일을 닫고 '다시 시도'를 눌러주세요."):
                return False
        except Exception as e:
            messagebox.showerror("오류", f"저장 중 알 수 없는 오류: {e}")
            return False

# ========================================================
# ComfyUI API 클라이언트
# ========================================================
def find_node_by_class_type(workflow: Dict[str, Any], class_type: str) -> Optional[str]:
    """워크플로우에서 클래스 타입으로 노드 ID 찾기"""
    for node_id, node_data in workflow.items():
        if isinstance(node_data, dict) and node_data.get("class_type") == class_type:
            return node_id
    return None

def check_server_port(host: str, port: int, timeout: float = 2.0) -> bool:
    """서버 포트 확인"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False

def find_comfyui_paths(server_address: str, workflow_path: Optional[str] = None, bat_path: Optional[str] = None, excel_path: Optional[str] = None, log_func=None) -> Dict[str, Optional[str]]:
    """
    ComfyUI 서버에서 input/output 폴더 경로를 조회합니다.
    excel_path: 엑셀 파일 경로 (IMG_S4_BG_생성경로 컬럼에서 경로 추출용)
    """
    def log(msg):
        if log_func:
            log_func(msg)
    
    paths = {"input": None, "output": None}
    
    # 방법 0: 엑셀 파일의 IMG_S4_BG_생성경로 컬럼에서 경로 추출 (가장 정확)
    if excel_path and os.path.exists(excel_path):
        try:
            log(f"[경로 탐색] 엑셀 파일에서 경로 추출 시도: {os.path.basename(excel_path)}")
            # 엑셀 파일 확장자에 따라 엔진 지정
            _, ext = os.path.splitext(excel_path.lower())
            if ext == '.xlsx':
                df_temp = pd.read_excel(excel_path, engine='openpyxl')
            elif ext == '.xls':
                df_temp = pd.read_excel(excel_path, engine='xlrd')
            else:
                # 기본값으로 openpyxl 시도
                try:
                    df_temp = pd.read_excel(excel_path, engine='openpyxl')
                except:
                    df_temp = pd.read_excel(excel_path, engine='xlrd')
            if "IMG_S4_BG_생성경로" in df_temp.columns:
                # 비어있지 않은 경로 찾기
                for idx, row in df_temp.iterrows():
                    bg_path = str(row.get("IMG_S4_BG_생성경로", "")).strip()
                    if bg_path and bg_path != "nan" and os.path.exists(bg_path):
                        # output 폴더 경로 추출
                        output_dir = os.path.dirname(os.path.abspath(bg_path))
                        # input 폴더 경로 추론 (output과 같은 레벨)
                        input_dir = os.path.join(os.path.dirname(output_dir), "input")
                        # output 폴더가 실제로 output인지 확인
                        if "output" in output_dir.lower() and os.path.exists(output_dir):
                            paths["output"] = output_dir
                            log(f"[경로 탐색] ✅ output 폴더 발견 (엑셀 경로): {paths['output']}")
                            if os.path.exists(input_dir):
                                paths["input"] = input_dir
                                log(f"[경로 탐색] ✅ input 폴더 발견 (엑셀 경로): {paths['input']}")
                            else:
                                # input 폴더가 없으면 output과 같은 디렉토리에 있을 수도 있음
                                alt_input_dir = os.path.join(output_dir, "..", "input")
                                alt_input_dir = os.path.abspath(alt_input_dir)
                                if os.path.exists(alt_input_dir):
                                    paths["input"] = alt_input_dir
                                    log(f"[경로 탐색] ✅ input 폴더 발견 (엑셀 경로, 대체): {paths['input']}")
                            break
                if not paths["input"] or not paths["output"]:
                    log(f"[경로 탐색] 엑셀 파일에서 유효한 경로를 찾지 못했습니다.")
        except Exception as e:
            log(f"[경로 탐색] 엑셀 파일에서 경로 추출 실패: {e}")
            import traceback
            log(f"[경로 탐색] 엑셀 경로 추출 오류 상세: {traceback.format_exc()}")
    
    # 이미 경로를 찾았으면 바로 반환
    if paths["input"] and paths["output"]:
        log(f"[경로 탐색] 엑셀 파일에서 경로를 찾아 탐색 완료!")
        return paths
    
    # bat 파일 경로 확인 및 추론
    bat_dir = None
    if bat_path:
        if os.path.exists(bat_path):
            log(f"[경로 탐색] bat 파일 확인됨: {bat_path}")
            bat_dir = os.path.dirname(os.path.abspath(bat_path))
        else:
            log(f"[경로 탐색] 경고: bat 파일이 존재하지 않습니다: {bat_path}")
            # bat 파일이 없어도 경로에서 추론 시도
            bat_dir = os.path.dirname(os.path.abspath(bat_path))
            log(f"[경로 탐색] bat 파일 경로에서 디렉토리 추론: {bat_dir}")
    else:
        log(f"[경로 탐색] bat 파일이 설정되지 않았습니다.")
    
    # 방법 1: bat 파일 경로 기준 (가장 정확)
    if bat_dir:
        log(f"[경로 탐색] bat 파일 기준 디렉토리: {bat_dir}")
        
        # bat 파일이 있는 디렉토리가 ComfyUI 루트일 가능성이 높음
        # 여러 가능한 경로 시도
        test_paths = [
            (os.path.join(bat_dir, "output"), os.path.join(bat_dir, "input")),  # bat_dir/output, bat_dir/input
            (os.path.join(bat_dir, "ComfyUI", "output"), os.path.join(bat_dir, "ComfyUI", "input")),  # bat_dir/ComfyUI/output
            (os.path.join(bat_dir, "..", "output"), os.path.join(bat_dir, "..", "input")),  # 상위 디렉토리
            (os.path.join(bat_dir, "..", "ComfyUI", "output"), os.path.join(bat_dir, "..", "ComfyUI", "input")),  # 상위/ComfyUI
        ]
        
        # bat_dir의 상위 디렉토리들도 시도
        parent_dirs = []
        current_dir = bat_dir
        for _ in range(3):  # 최대 3단계 상위로
            parent_dir = os.path.dirname(current_dir)
            if parent_dir != current_dir:
                parent_dirs.append(parent_dir)
                current_dir = parent_dir
            else:
                break
        
        for parent_dir in parent_dirs:
            test_paths.extend([
                (os.path.join(parent_dir, "ComfyUI", "output"), os.path.join(parent_dir, "ComfyUI", "input")),
                (os.path.join(parent_dir, "output"), os.path.join(parent_dir, "input")),
            ])
        
        log(f"[경로 탐색] bat 기준 경로 {len(test_paths)}개 시도 중...")
        for test_output, test_input in test_paths:
            test_output = os.path.abspath(test_output)
            test_input = os.path.abspath(test_input)
            
            if os.path.exists(test_output) and not paths["output"]:
                paths["output"] = test_output
                log(f"[경로 탐색] output 폴더 발견 (bat 기준): {paths['output']}")
            if os.path.exists(test_input) and not paths["input"]:
                paths["input"] = test_input
                log(f"[경로 탐색] input 폴더 발견 (bat 기준): {paths['input']}")
            
            if paths["input"] and paths["output"]:
                break
    
    # 방법 2: ComfyUI API로 경로 조회 시도
    if not paths["input"] or not paths["output"]:
        try:
            import requests
            log(f"[경로 탐색] API로 경로 조회 시도: http://{server_address}/system_stats")
            response = requests.get(f"http://{server_address}/system_stats", timeout=5)
            log(f"[경로 탐색] API 응답 상태 코드: {response.status_code}")
            if response.status_code == 200:
                stats = response.json()
                log(f"[경로 탐색] API 응답 내용: {json.dumps(stats, ensure_ascii=False)[:500]}...")
                
                # argv에서 실행 경로 추론 시도
                if "system" in stats and "argv" in stats["system"]:
                    argv = stats["system"]["argv"]
                    if argv and len(argv) > 0:
                        main_py_path = argv[0]  # "ComfyUI\\main.py"
                        log(f"[경로 탐색] API에서 실행 경로 추론: {main_py_path}")
                        # main.py의 상대 경로에서 ComfyUI 루트 추론
                        # "ComfyUI\\main.py" -> "ComfyUI" 폴더가 루트
                        if "ComfyUI" in main_py_path or "comfyui" in main_py_path.lower():
                            # bat_dir에서 ComfyUI 폴더 찾기
                            if bat_dir:
                                comfyui_root_candidates = [
                                    os.path.join(bat_dir, "ComfyUI"),
                                    os.path.join(os.path.dirname(bat_dir), "ComfyUI"),
                                    bat_dir,  # bat_dir 자체가 ComfyUI 루트일 수도 있음
                                ]
                                for candidate in comfyui_root_candidates:
                                    candidate_abs = os.path.abspath(candidate)
                                    test_input_api = os.path.join(candidate_abs, "input")
                                    test_output_api = os.path.join(candidate_abs, "output")
                                    if os.path.exists(test_input_api) and not paths["input"]:
                                        paths["input"] = test_input_api
                                        log(f"[경로 탐색] ✅ input 폴더 발견 (API argv 추론): {paths['input']}")
                                    if os.path.exists(test_output_api) and not paths["output"]:
                                        paths["output"] = test_output_api
                                        log(f"[경로 탐색] ✅ output 폴더 발견 (API argv 추론): {paths['output']}")
                                    if paths["input"] and paths["output"]:
                                        break
                
                if "paths" in stats:
                    if not paths["input"]:
                        paths["input"] = stats["paths"].get("input")
                        if paths["input"]:
                            log(f"[경로 탐색] input 폴더 발견 (API): {paths['input']}")
                        else:
                            log(f"[경로 탐색] API 응답에 input 경로가 없습니다.")
                    if not paths["output"]:
                        paths["output"] = stats["paths"].get("output")
                        if paths["output"]:
                            log(f"[경로 탐색] output 폴더 발견 (API): {paths['output']}")
                        else:
                            log(f"[경로 탐색] API 응답에 output 경로가 없습니다.")
                else:
                    log(f"[경로 탐색] API 응답에 'paths' 키가 없습니다.")
            else:
                log(f"[경로 탐색] API 조회 실패: HTTP {response.status_code}")
        except Exception as e:
            log(f"[경로 탐색] API 조회 실패: {e}")
            import traceback
            log(f"[경로 탐색] API 조회 오류 상세: {traceback.format_exc()}")
    
    # 방법 3: 일반적인 경로 시도
    if not paths["input"] or not paths["output"]:
        log(f"[경로 탐색] 일반적인 경로 시도 시작...")
        potential_dirs = []
        
        # bat 파일 디렉토리 기준 (파일이 없어도 경로 사용)
        if bat_path:
            bat_dir_from_path = os.path.dirname(os.path.abspath(bat_path))
            potential_dirs.append(bat_dir_from_path)
            log(f"[경로 탐색] bat 파일 디렉토리 추가: {bat_dir_from_path}")
            
            # bat_dir의 상위 디렉토리들도 추가
            current_dir = bat_dir_from_path
            for _ in range(3):  # 최대 3단계 상위로
                parent_dir = os.path.dirname(current_dir)
                if parent_dir != current_dir:
                    potential_dirs.append(parent_dir)
                    potential_dirs.append(os.path.join(parent_dir, "ComfyUI"))
                    current_dir = parent_dir
                else:
                    break
        
        # 워크플로우 파일 위치 기준
        if workflow_path:
            workflow_dir = os.path.dirname(os.path.abspath(workflow_path))
            potential_dirs.extend([
                workflow_dir,
                os.path.join(workflow_dir, ".."),
                os.path.join(workflow_dir, "..", ".."),
            ])
            log(f"[경로 탐색] 워크플로우 디렉토리 추가: {workflow_dir}")
        
        # 일반적인 설치 경로
        common_paths = [
            os.path.join(os.path.expanduser("~"), "ComfyUI"),
            os.path.join("C:", "ComfyUI"),
            os.path.join("C:", "ComfyUI_windows_portable_nvidia", "ComfyUI_windows_portable", "ComfyUI"),
            os.path.join("C:", "ComfyUI_windows_portable_nvidia", "ComfyUI_windows_portable"),
            os.path.join("C:", "ComfyUI_Portable", "ComfyUI_windows_portable", "ComfyUI"),
            os.path.join("C:", "ComfyUI_Portable", "ComfyUI_windows_portable"),
        ]
        
        # bat 파일 경로에서 추론한 경로도 추가
        if bat_path:
            bat_dir_from_path = os.path.dirname(os.path.abspath(bat_path))
            # ComfyUI_windows_portable 같은 패턴에서 ComfyUI 폴더 찾기
            if "ComfyUI_windows_portable" in bat_dir_from_path:
                comfyui_base = bat_dir_from_path.replace("ComfyUI_windows_portable", "").rstrip(os.sep)
                if comfyui_base:
                    common_paths.append(os.path.join(comfyui_base, "ComfyUI_windows_portable", "ComfyUI"))
                    common_paths.append(os.path.join(comfyui_base, "ComfyUI_windows_portable"))
        
        potential_dirs.extend(common_paths)
        log(f"[경로 탐색] 일반적인 경로 {len(common_paths)}개 추가")
        
        log(f"[경로 탐색] 총 {len(potential_dirs)}개 경로 시도 중...")
        for idx, base_dir in enumerate(potential_dirs, 1):
            base_dir_abs = os.path.abspath(base_dir)
            test_input = os.path.join(base_dir_abs, "input")
            test_output = os.path.join(base_dir_abs, "output")
            
            # 각 경로 시도 로그 (처음 10개만 상세 로그)
            if idx <= 10:
                log(f"[경로 탐색] [{idx}/{len(potential_dirs)}] 시도: {base_dir_abs}")
                log(f"  -> input: {test_input} (존재: {os.path.exists(test_input)})")
                log(f"  -> output: {test_output} (존재: {os.path.exists(test_output)})")
            
            if os.path.exists(test_output) and not paths["output"]:
                paths["output"] = test_output
                log(f"[경로 탐색] ✅ output 폴더 발견 (일반 경로): {paths['output']}")
            if os.path.exists(test_input) and not paths["input"]:
                paths["input"] = test_input
                log(f"[경로 탐색] ✅ input 폴더 발견 (일반 경로): {paths['input']}")
            
            if paths["input"] and paths["output"]:
                log(f"[경로 탐색] 경로 탐색 완료!")
                break
    
    if not paths["output"]:
        log(f"[경로 탐색] 경고: output 폴더를 찾을 수 없습니다.")
    if not paths["input"]:
        log(f"[경로 탐색] 경고: input 폴더를 찾을 수 없습니다.")
    
    return paths

class ComfyUIClient:
    def __init__(self, server_address: str = "127.0.0.1:8188", log_func=None):
        self.server_address = server_address
        self.ws = None
        self.client_id = str(uuid.uuid4())
        self.log_func = log_func or print
        # 배치 처리를 위한 완료 추적
        self.completed_prompts = {}  # {prompt_id: completion_data}
        self.completion_lock = threading.Lock()  # 스레드 안전성
        # 배치 처리를 위한 지속적인 WebSocket 메시지 수신 스레드
        self.receive_thread = None
        self.receive_thread_stop = threading.Event()

    def log(self, msg: str):
        if self.log_func:
            self.log_func(msg)
        else:
            print(msg)

    def connect(self):
        """WebSocket 연결"""
        ws_url = f"ws://{self.server_address}/ws?clientId={self.client_id}"
        try:
            # 기존 연결이 있으면 종료
            if self.ws:
                try:
                    self.ws.close()
                except:
                    pass
                self.ws = None
            
            # 기존 수신 스레드가 있으면 종료
            if self.receive_thread and self.receive_thread.is_alive():
                self.receive_thread_stop.set()
                self.receive_thread.join(timeout=1.0)
                self.receive_thread = None
                self.receive_thread_stop.clear()
            
            self.ws = websocket.WebSocket()
            self.ws.connect(ws_url, timeout=10)
            self.log(f"[ComfyUI] 연결 성공: {ws_url}")
            
            # 배치 처리를 위한 지속적인 메시지 수신 스레드 시작
            self._start_receive_thread()
            
            return True
        except Exception as e:
            self.log(f"[ComfyUI] 연결 실패: {e}")
            self.ws = None
            return False

    def _start_receive_thread(self):
        """배치 처리를 위한 지속적인 WebSocket 메시지 수신 스레드 시작"""
        if self.receive_thread and self.receive_thread.is_alive():
            return  # 이미 실행 중
        
        self.receive_thread_stop.clear()
        
        def receive_loop():
            reconnect_attempted = False
            while not self.receive_thread_stop.is_set():
                if not self.ws:
                    if not reconnect_attempted:
                        reconnect_attempted = True
                        self.log(f"[ComfyUI] WebSocket 연결 끊김, 재연결 시도...")
                        try:
                            # 재연결 (수신 스레드는 다시 시작하지 않음)
                            ws_url = f"ws://{self.server_address}/ws?clientId={self.client_id}"
                            self.ws = websocket.WebSocket()
                            self.ws.connect(ws_url, timeout=10)
                            self.log(f"[ComfyUI] 재연결 성공")
                            reconnect_attempted = False
                        except Exception as e:
                            self.log(f"[ComfyUI] 재연결 실패: {e}")
                            time.sleep(1.0)
                            continue
                    else:
                        time.sleep(0.5)
                        continue
                
                try:
                    message = self.ws.recv()
                    self._handle_websocket_message(message)
                except websocket.WebSocketTimeoutException:
                    continue
                except websocket.WebSocketConnectionClosedException:
                    if self.receive_thread_stop.is_set():
                        break
                    if not reconnect_attempted:
                        reconnect_attempted = True
                        self.log(f"[ComfyUI] WebSocket 연결 종료됨, 재연결 시도...")
                        try:
                            # 재연결 (수신 스레드는 다시 시작하지 않음)
                            ws_url = f"ws://{self.server_address}/ws?clientId={self.client_id}"
                            self.ws = websocket.WebSocket()
                            self.ws.connect(ws_url, timeout=10)
                            self.log(f"[ComfyUI] 재연결 성공")
                            reconnect_attempted = False
                        except Exception as e:
                            self.log(f"[ComfyUI] 재연결 실패: {e}")
                            time.sleep(1.0)
                            continue
                    else:
                        break
                except AttributeError as e:
                    if "'NoneType' object has no attribute 'recv'" in str(e):
                        break
                except Exception as e:
                    if "Expecting value" not in str(e) and "WinError 10038" not in str(e) and "'NoneType' object has no attribute 'recv'" not in str(e):
                        self.log(f"[ComfyUI] WebSocket 수신 오류: {e}")
                    time.sleep(0.1)
        
        self.receive_thread = threading.Thread(target=receive_loop, daemon=True)
        self.receive_thread.start()

    def _handle_websocket_message(self, message: str):
        """WebSocket 메시지 처리 (배치 처리용)"""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "executing":
                exec_data = data.get("data", {})
                if exec_data.get("node") is None:
                    # 실행 완료
                    msg_prompt_id = exec_data.get("prompt_id")
                    if msg_prompt_id:
                        done_data = {"type": "done", "data": data}
                        with self.completion_lock:
                            if msg_prompt_id not in self.completed_prompts:
                                self.completed_prompts[msg_prompt_id] = done_data
            elif msg_type == "executed":
                exec_data = data.get("data", {})
                msg_prompt_id = exec_data.get("prompt_id")
                if exec_data.get("output") and "images" in exec_data["output"]:
                    # 배치 처리를 위한 이미지 정보 저장
                    if msg_prompt_id:
                        with self.completion_lock:
                            if msg_prompt_id in self.completed_prompts:
                                if "output_images" not in self.completed_prompts[msg_prompt_id]:
                                    self.completed_prompts[msg_prompt_id]["output_images"] = []
                                for img_info in exec_data["output"]["images"]:
                                    self.completed_prompts[msg_prompt_id]["output_images"].append(img_info)
        except Exception as e:
            self.log(f"[ComfyUI] 메시지 파싱 오류: {e}")

    def disconnect(self):
        """WebSocket 연결 종료"""
        # 수신 스레드 종료
        if self.receive_thread and self.receive_thread.is_alive():
            self.receive_thread_stop.set()
            self.receive_thread.join(timeout=1.0)
            self.receive_thread = None
            self.receive_thread_stop.clear()
        
        if self.ws:
            try:
                self.ws.close()
                self.log("[ComfyUI] 연결 종료")
            except:
                pass
            self.ws = None

    def queue_prompt(self, workflow: Dict[str, Any]) -> Optional[str]:
        """워크플로우를 큐에 제출하고 prompt_id 반환"""
        # WebSocket 연결 확인 및 재연결
        if not self.ws:
            if not self.connect():
                return None
        else:
            # WebSocket이 살아있는지 확인 (간단한 상태 확인)
            try:
                # 연결 상태 확인을 위해 간단히 체크
                if hasattr(self.ws, 'sock') and self.ws.sock is None:
                    self.log(f"[ComfyUI] WebSocket 연결 끊김 감지, 재연결 시도...")
                    if not self.connect():
                        return None
            except:
                # 확인 실패 시 재연결 시도
                self.log(f"[ComfyUI] WebSocket 상태 확인 실패, 재연결 시도...")
                if not self.connect():
                    return None
        
        try:
            import requests
            prompt_id = str(uuid.uuid4())
            data = {
                "prompt": workflow,
                "client_id": self.client_id
            }
            
            self.log(f"[ComfyUI] 워크플로우 제출 중...")
            response = requests.post(
                f"http://{self.server_address}/prompt",
                json=data,
                timeout=30
            )
            # 응답 상태 확인 전에 응답 본문 확인 (400 오류 시 상세 정보)
            if response.status_code != 200:
                error_body = response.text
                debug_log(f"[ComfyUI] HTTP {response.status_code} 오류 응답: {error_body}", "ERROR")
                self.log(f"[ComfyUI] HTTP {response.status_code} 오류: {error_body[:500]}")
            
            response.raise_for_status()
            result = response.json()
            prompt_id = result.get("prompt_id", prompt_id)
            self.log(f"[ComfyUI] 워크플로우 제출 성공: prompt_id={prompt_id}")
            return prompt_id
        except Exception as e:
            self.log(f"[ComfyUI] 워크플로우 제출 실패: {e}")
            import traceback
            error_trace = traceback.format_exc()
            self.log(f"[ComfyUI] 상세 오류: {error_trace}")
            debug_log(f"[ComfyUI] 워크플로우 제출 실패 상세:\n{error_trace}", "ERROR")
            
            # HTTP 응답 본문 확인 (400 오류의 상세 내용)
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_body = e.response.text
                    debug_log(f"[ComfyUI] 서버 오류 응답: {error_body}", "ERROR")
                    self.log(f"[ComfyUI] 서버 오류 응답: {error_body}")
            except:
                pass
            
            # 워크플로우 구조 디버깅
            try:
                workflow_str = json.dumps(workflow, indent=2, ensure_ascii=False)
                debug_log(f"[ComfyUI] 제출 시도한 워크플로우 구조 (처음 1000자):\n{workflow_str[:1000]}...", "DEBUG")
            except:
                pass
            return None

    def wait_for_completion(self, prompt_id: str, timeout: int = 300) -> Optional[Dict[str, Any]]:
        """워크플로우 완료 대기 및 결과 반환"""
        if not self.ws:
            return None
        
        start_time = time.time()
        result_queue = queue.Queue()
        output_images = []  # 완료 시 이미지 정보 저장
        debug_log(f"워크플로우 완료 대기 시작: prompt_id={prompt_id}, timeout={timeout}초", "DEBUG")
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                msg_type = data.get("type")
                
                if msg_type == "execution_cached":
                    result_queue.put({"type": "cached", "data": data})
                elif msg_type == "executing":
                    exec_data = data.get("data", {})
                    if exec_data.get("node") is None:
                        # 실행 완료
                        self.log(f"[ComfyUI] 실행 완료 신호 수신")
                        # 배치 처리를 위한 완료 추적
                        # prompt_id는 exec_data에서 추출하거나 클로저에서 가져옴
                        msg_prompt_id = exec_data.get("prompt_id") or prompt_id
                        if msg_prompt_id:
                            done_data = {"type": "done", "data": data}
                            with self.completion_lock:
                                if msg_prompt_id not in self.completed_prompts:
                                    self.completed_prompts[msg_prompt_id] = done_data
                        result_queue.put({"type": "done", "data": data})
                elif msg_type == "progress":
                    progress = data.get("data", {}).get("value", 0)
                    self.log(f"[ComfyUI] 진행률: {progress}%")
                elif msg_type == "executed":
                    # 완료된 노드의 출력 이미지 정보 추출
                    exec_data = data.get("data", {})
                    if exec_data.get("output") and "images" in exec_data["output"]:
                        # 배치 처리를 위한 이미지 정보 저장
                        # prompt_id는 exec_data에서 추출하거나 클로저에서 가져옴
                        msg_prompt_id = exec_data.get("prompt_id") or prompt_id
                        if msg_prompt_id:
                            with self.completion_lock:
                                if msg_prompt_id in self.completed_prompts:
                                    if "output_images" not in self.completed_prompts[msg_prompt_id]:
                                        self.completed_prompts[msg_prompt_id]["output_images"] = []
                                    for img_info in exec_data["output"]["images"]:
                                        self.completed_prompts[msg_prompt_id]["output_images"].append(img_info)
                        for img_info in exec_data["output"]["images"]:
                            output_images.append(img_info)
                            self.log(f"[ComfyUI] 출력 이미지 발견: {img_info.get('filename', 'unknown')}")
            except Exception as e:
                self.log(f"[ComfyUI] 메시지 파싱 오류: {e}")
        
        # WebSocket 메시지 수신 스레드
        reconnect_attempted = False  # 재연결 시도 플래그
        thread_stop_flag = threading.Event()  # 스레드 종료 플래그
        
        def receive_thread():
            nonlocal reconnect_attempted
            while not thread_stop_flag.is_set():
                # WebSocket이 None이면 종료
                if not self.ws:
                    break
                
                try:
                    message = self.ws.recv()
                    on_message(self.ws, message)
                except websocket.WebSocketTimeoutException:
                    # 타임아웃은 정상 (계속 대기)
                    continue
                except websocket.WebSocketConnectionClosedException:
                    if thread_stop_flag.is_set():
                        break
                    if not reconnect_attempted:
                        reconnect_attempted = True
                        self.log(f"[ComfyUI] WebSocket 연결 종료됨, 재연결 시도...")
                        try:
                            if not self.connect():
                                self.log(f"[ComfyUI] WebSocket 재연결 실패")
                                break
                        except:
                            break
                    else:
                        # 이미 재연결 시도했으면 종료
                        break
                except AttributeError as e:
                    # 'NoneType' object has no attribute 'recv' 오류
                    if "'NoneType' object has no attribute 'recv'" in str(e):
                        # WebSocket이 None이 된 경우 종료
                        break
                    else:
                        if "Expecting value" not in str(e) and "WinError 10038" not in str(e):
                            self.log(f"[ComfyUI] WebSocket 수신 오류: {e}")
                except Exception as e:
                    # JSON 파싱 오류 등은 무시하고 계속 진행
                    if "'NoneType' object has no attribute 'recv'" in str(e):
                        # WebSocket이 None이 된 경우 종료
                        break
                    if "Expecting value" not in str(e) and "WinError 10038" not in str(e) and "'NoneType' object has no attribute 'recv'" not in str(e):
                        self.log(f"[ComfyUI] WebSocket 수신 오류: {e}")
                    # 연결이 끊어진 경우에만 재연결 시도
                    if not thread_stop_flag.is_set() and not reconnect_attempted and (not self.ws or (hasattr(self.ws, 'sock') and self.ws.sock is None)):
                        reconnect_attempted = True
                        try:
                            if not self.connect():
                                self.log(f"[ComfyUI] WebSocket 재연결 실패")
                                break
                        except:
                            break
                    elif thread_stop_flag.is_set():
                        break
        
        thread = threading.Thread(target=receive_thread, daemon=True)
        thread.start()
        
        # 완료 대기
        done_received = False
        last_progress_time = time.time()
        progress_warning_sent = False  # 경고 메시지 중복 방지
        
        try:
            while time.time() - start_time < timeout:
                try:
                    result = result_queue.get(timeout=0.3)  # 타임아웃을 0.3초로 더 줄여서 빠르게 반응
                    if result.get("type") == "done":
                        done_received = True
                        self.log(f"[ComfyUI] 워크플로우 완료: prompt_id={prompt_id}")
                        # 완료 데이터에 이미지 정보 추가
                        done_data = result.get("data", {})
                        if output_images:
                            done_data["output_images"] = output_images
                        # 배치 처리를 위한 완료 추적
                        with self.completion_lock:
                            self.completed_prompts[prompt_id] = done_data
                        # 완료 신호를 받았으면 바로 반환 (추가 대기 없음)
                        return done_data
                    elif result.get("type") == "progress":
                        progress = result.get("data", {}).get("value", 0)
                        last_progress_time = time.time()
                        progress_warning_sent = False  # 진행률 업데이트 시 경고 플래그 리셋
                        self.log(f"[ComfyUI] 진행률: {progress}%")
                except queue.Empty:
                    # 진행률이 멈춘 경우 (30초 이상 진행률 업데이트 없음) 체크
                    # 경고 메시지는 한 번만 출력
                    if not progress_warning_sent and time.time() - last_progress_time > 30:
                        self.log(f"[ComfyUI] 경고: 진행률 업데이트가 30초 이상 없습니다. (계속 대기 중...)")
                        progress_warning_sent = True
                    continue
            
            # 타임아웃 발생
            if done_received:
                # 완료 신호는 받았지만 타임아웃 발생 (이미지 정보는 있을 수 있음)
                self.log(f"[ComfyUI] 완료 신호 수신 후 타임아웃: prompt_id={prompt_id}")
                done_data = {"output_images": output_images} if output_images else {}
                return done_data
            else:
                self.log(f"[ComfyUI] 타임아웃: prompt_id={prompt_id}")
                return None
        finally:
            # 작업 완료 후 스레드 종료 신호
            thread_stop_flag.set()
            # 스레드가 종료될 때까지 잠시 대기 (최대 1초)
            thread.join(timeout=1.0)

    def get_output_images(self, prompt_id: str, completion_data: Optional[Dict] = None) -> list:
        """생성된 이미지 경로 목록 반환"""
        images = []
        
        # 1. 완료 데이터에서 이미지 정보 추출 (우선순위)
        if completion_data and "output_images" in completion_data:
            for img_info in completion_data["output_images"]:
                filename = img_info.get("filename") or img_info.get("name")
                if filename:
                    images.append(filename)
                    self.log(f"[ComfyUI] 완료 데이터에서 이미지 발견: {filename}")
        
        # 2. History API로 조회
        if not images:
            try:
                import requests
                self.log(f"[ComfyUI] History API 조회 시도: /history/{prompt_id}")
                response = requests.get(f"http://{self.server_address}/history/{prompt_id}", timeout=10)
                response.raise_for_status()
                history = response.json()
                
                self.log(f"[ComfyUI] History 응답: {json.dumps(history, indent=2)[:500]}...")
                
                for prompt_id_key, prompt_data in history.items():
                    if prompt_data.get("outputs"):
                        for node_id, node_output in prompt_data["outputs"].items():
                            if "images" in node_output:
                                for img_info in node_output["images"]:
                                    filename = img_info.get("filename") or img_info.get("name")
                                    if filename:
                                        images.append(filename)
                                        self.log(f"[ComfyUI] History에서 이미지 발견: {filename} (노드: {node_id})")
            except Exception as e:
                self.log(f"[ComfyUI] History API 조회 실패: {e}")
                import traceback
                self.log(f"[ComfyUI] 상세 오류: {traceback.format_exc()}")
        
        if not images:
            self.log(f"[ComfyUI] 경고: 이미지를 찾을 수 없습니다 (prompt_id={prompt_id})")
        
        return images
    
    def check_completion(self, prompt_id: str) -> bool:
        """비블로킹 방식으로 완료 여부 확인 (배치 처리용)"""
        with self.completion_lock:
            return prompt_id in self.completed_prompts
    
    def get_completion_data(self, prompt_id: str) -> Optional[Dict[str, Any]]:
        """완료된 워크플로우의 결과 데이터 반환 (배치 처리용)"""
        with self.completion_lock:
            return self.completed_prompts.get(prompt_id)
    
    def clear_completion(self, prompt_id: str):
        """완료 추적 데이터 정리 (메모리 관리용)"""
        with self.completion_lock:
            self.completed_prompts.pop(prompt_id, None)

# ========================================================
# GUI Class
# ========================================================
class IMGMixingGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 4-2: 배경 합성 (ComfyUI)")
        self.geometry("1000x850")

        self.input_file_path = tk.StringVar()
        self.comfyui_server_var = tk.StringVar(value="127.0.0.1:8188")
        self.workflow_path_var = tk.StringVar()
        self.comfyui_bat_path_var = tk.StringVar()
        self.skip_filled_var = tk.BooleanVar(value=True)
        self.auto_start_server_var = tk.BooleanVar(value=True)  # 서버 자동 시작 옵션 (기본값: True)
        self.batch_size_var = tk.IntVar(value=3)  # 배치 크기 (기본값: 3개)
        self.auto_save_interval_var = tk.IntVar(value=10)  # 중간 저장 간격 (기본값: 10개마다)

        self.is_running = False
        self.stop_requested = False

        self.stat_progress = tk.StringVar(value="0.0%")
        self.stat_count = tk.StringVar(value="0 / 0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_fail = tk.StringVar(value="0")
        self.stat_time = tk.StringVar(value="00:00:00")
        self.stat_start_time = tk.StringVar(value="-")
        self.stat_estimated_end = tk.StringVar(value="-")
        self.status_msg = tk.StringVar(value="파일을 선택하고 작업을 시작하세요.")
        
        # 경과 시간 계산용 변수
        self.time_timer_id = None
        
        # 이미지 미리보기 관련 변수
        self.preview_window = None
        self.current_nukki_image_path = None
        self.current_mix_image_path = None
        self.current_product_code = None
        self.current_product_name = None
        self.preview_nukki_photo = None
        self.preview_mix_photo = None
        
        # 완료된 항목 목록 (이전/다음 이동용)
        self.completed_items = []  # [{"nukki_path": ..., "mix_path": ..., "code": ..., "name": ..., "idx": ...}, ...]
        self.current_preview_index = -1  # 현재 미리보기 중인 항목 인덱스

        self._configure_styles()
        self._init_ui()
        self._load_config()

        # 기본 워크플로우 경로
        if not self.workflow_path_var.get():
            default_workflow = os.path.join(os.path.dirname(__file__), "배경합성_251209ver.json")
            if os.path.exists(default_workflow):
                self.workflow_path_var.set(default_workflow)

    def _configure_styles(self):
        """스타일 설정"""
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
        style.configure("Action.TButton", font=("맑은 고딕", 11, "bold"), padding=5)
        style.configure("Stop.TButton", font=("맑은 고딕", 11, "bold"), foreground="red", padding=5)

    def _init_ui(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)

        # 파일 선택
        frame_file = ttk.LabelFrame(main_frame, text="작업 대상 파일 (I4)", padding=15)
        frame_file.pack(fill='x', pady=(0, 10))
        
        rf = ttk.Frame(frame_file)
        rf.pack(fill='x')
        ttk.Entry(rf, textvariable=self.input_file_path).pack(side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(rf, text="📂 파일 선택", command=self._select_file).pack(side='right')

        # ComfyUI 설정
        frame_comfy = ttk.LabelFrame(main_frame, text="ComfyUI 설정", padding=15)
        frame_comfy.pack(fill='x', pady=(0, 10))

        r1 = ttk.Frame(frame_comfy)
        r1.pack(fill='x', pady=2)
        ttk.Label(r1, text="서버 주소:", width=12).pack(side='left')
        ttk.Entry(r1, textvariable=self.comfyui_server_var, width=30).pack(side='left', padx=5)
        ttk.Label(r1, text="(예: 127.0.0.1:8188)").pack(side='left', padx=5)
        
        r2 = ttk.Frame(frame_comfy)
        r2.pack(fill='x', pady=5)
        ttk.Label(r2, text="워크플로우 JSON:", width=12).pack(side='left')
        ttk.Entry(r2, textvariable=self.workflow_path_var, width=50).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(r2, text="📂 찾기", command=self._select_workflow).pack(side='right')
        
        r3 = ttk.Frame(frame_comfy)
        r3.pack(fill='x', pady=5)
        ttk.Label(r3, text="ComfyUI bat 파일:", width=12).pack(side='left')
        ttk.Entry(r3, textvariable=self.comfyui_bat_path_var, width=50).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(r3, text="📂 찾기", command=self._select_bat_file).pack(side='right')
        
        r4 = ttk.Frame(frame_comfy)
        r4.pack(fill='x', pady=5)
        ttk.Checkbutton(
            r4,
            text="서버가 꺼져있으면 자동으로 실행 (bat 파일 필요)",
            variable=self.auto_start_server_var
        ).pack(side='left')
        ttk.Button(r4, text="🔍 서버 연결 확인", command=self._check_server_connection).pack(side='right', padx=5)

        # 옵션
        frame_opt = ttk.LabelFrame(main_frame, text="옵션", padding=15)
        frame_opt.pack(fill='x', pady=(0, 10))

        ttk.Checkbutton(frame_opt, text="이미 처리된 행 건너뛰기 (IMG_S4_mix_생성경로가 있는 행)", variable=self.skip_filled_var).pack(anchor='w')
        
        # 배치 크기 설정
        batch_frame = ttk.Frame(frame_opt)
        batch_frame.pack(anchor='w', pady=(5,0))
        ttk.Label(batch_frame, text="배치 크기:").pack(side='left', padx=(0,5))
        batch_spinbox = ttk.Spinbox(batch_frame, from_=1, to=10, width=5, textvariable=self.batch_size_var)
        batch_spinbox.pack(side='left', padx=(0,5))
        ttk.Label(batch_frame, text="개씩 동시 처리 (권장: 3-5개)", font=("맑은 고딕", 9), foreground="#666").pack(side='left')

        # 중간 저장 간격 설정
        save_interval_frame = ttk.Frame(frame_opt)
        save_interval_frame.pack(anchor='w', pady=(5,0))
        ttk.Label(save_interval_frame, text="중간 저장:").pack(side='left', padx=(0,5))
        save_interval_spinbox = ttk.Spinbox(save_interval_frame, from_=0, to=100, width=5, textvariable=self.auto_save_interval_var)
        save_interval_spinbox.pack(side='left', padx=(0,5))
        ttk.Label(save_interval_frame, text="개마다 엑셀 자동 저장 (재부팅 시 복구용, 0=비활성화)", font=("맑은 고딕", 9), foreground="#666").pack(side='left')

        # 작업 제어 버튼 (옵션과 진행 상황 사이)
        frame_btn = ttk.LabelFrame(main_frame, text="작업 제어", padding=15)
        frame_btn.pack(fill='x', pady=(0, 10))

        btn_frame = ttk.Frame(frame_btn)
        btn_frame.pack(fill='x')

        self.btn_start = ttk.Button(btn_frame, text="▶ 작업 시작", command=self._start_thread, style="Action.TButton", width=18)
        self.btn_start.pack(side='left', padx=5)

        self.btn_stop = ttk.Button(btn_frame, text="⏹ 중단 (Safe Stop)", command=self._request_stop, state='disabled', style="Stop.TButton", width=18)
        self.btn_stop.pack(side='left', padx=5)

        self.btn_preview = ttk.Button(btn_frame, text="🖼️ 이미지 미리보기", command=self._show_preview_window, width=18)
        self.btn_preview.pack(side='left', padx=5)

        ttk.Label(btn_frame, textvariable=self.status_msg, font=("맑은 고딕", 10), foreground="#555").pack(side='left', padx=20)

        # 대시보드 (진행 상황)
        dash_frame = ttk.LabelFrame(main_frame, text="실시간 현황 (Dashboard)", padding=15)
        dash_frame.pack(fill='x', pady=(0, 10))

        # 1행: 진행률
        d1 = ttk.Frame(dash_frame)
        d1.pack(fill='x', pady=5)
        ttk.Label(d1, text="진행률:", width=10, font=("맑은 고딕", 10, "bold")).pack(side='left')
        self.pb = ttk.Progressbar(d1, maximum=100, mode='determinate')
        self.pb.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Label(d1, textvariable=self.stat_progress, font=("맑은 고딕", 11, "bold"), foreground="#0052cc", width=8).pack(side='right')

        # 2행: 통계
        d2 = ttk.Frame(dash_frame)
        d2.pack(fill='x', pady=5)
        ttk.Label(d2, text="처리 건수:", width=10).pack(side='left')
        ttk.Label(d2, textvariable=self.stat_count, width=15, foreground="blue", font=("맑은 고딕", 10, "bold")).pack(side='left')
        
        ttk.Label(d2, text="성공/실패:", width=10).pack(side='left', padx=(20, 0))
        lbl_succ = ttk.Label(d2, textvariable=self.stat_success, foreground="green", font=("맑은 고딕", 10, "bold"))
        lbl_succ.pack(side='left')
        ttk.Label(d2, text=" / ").pack(side='left')
        lbl_fail = ttk.Label(d2, textvariable=self.stat_fail, foreground="red", font=("맑은 고딕", 10, "bold"))
        lbl_fail.pack(side='left')

        # 3행: 시간 정보
        d3 = ttk.Frame(dash_frame)
        d3.pack(fill='x', pady=5)
        ttk.Label(d3, text="경과 시간:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_time, width=12, font=("맑은 고딕", 10, "bold")).pack(side='left')
        
        ttk.Label(d3, text="시작 시간:", width=10).pack(side='left', padx=(20, 0))
        ttk.Label(d3, textvariable=self.stat_start_time, width=12, font=("맑은 고딕", 9)).pack(side='left')
        
        # 4행: 예상 종료 시간
        d4 = ttk.Frame(dash_frame)
        d4.pack(fill='x', pady=5)
        ttk.Label(d4, text="예상 종료:", width=10).pack(side='left')
        ttk.Label(d4, textvariable=self.stat_estimated_end, width=20, font=("맑은 고딕", 9, "bold"), foreground="#007bff").pack(side='left')

        # 로그
        self.log_frame = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        self.log_frame.pack(fill='both', expand=True)
        self.log_widget = ScrolledText(self.log_frame, height=10, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    def _select_file(self):
        path = filedialog.askopenfilename(
            title="엑셀 파일 선택 (I5 버전만 가능)",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if path:
            base_name = os.path.basename(path)
            
            # I5 파일만 허용
            i_match = re.search(r"_I(\d+)", base_name, re.IGNORECASE)
            
            if not i_match:
                messagebox.showerror(
                    "오류",
                    f"파일명에 버전 정보(_T*_I*)가 없습니다.\n\n"
                    f"선택한 파일: {base_name}\n\n"
                    f"I5 버전 파일을 선택해주세요.\n"
                    f"(예: 상품_T3_I5.xlsx, 상품_T4_I5.xlsx)"
                )
                return
            
            i_version = int(i_match.group(1))
            if i_version != 5:
                messagebox.showerror(
                    "오류",
                    f"입력 파일은 I5 버전만 허용됩니다.\n\n"
                    f"선택한 파일: {base_name}\n"
                    f"현재 버전: I{i_version}\n\n"
                    f"I5 배경 생성이 완료된 파일을 선택해주세요.\n"
                    f"(예: 상품_T3_I5.xlsx, 상품_T4_I5.xlsx)"
                )
                return
            
            self.input_file_path.set(path)
            self._log(f"파일 선택됨: {base_name} (I5)")
            self.status_msg.set(f"준비 완료. 저장 시 {base_name}로 저장됩니다.")

    def _select_workflow(self):
        path = filedialog.askopenfilename(
            title="워크플로우 JSON 선택",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if path:
            self.workflow_path_var.set(path)
            self._save_config()

    def _select_bat_file(self):
        path = filedialog.askopenfilename(
            title="ComfyUI Bat 파일 선택",
            filetypes=[("Bat files", "*.bat"), ("All files", "*.*")]
        )
        if path:
            self.comfyui_bat_path_var.set(path)
            self._save_config()
            # --lowvram 옵션 확인 및 안내
            self._check_lowvram_option(path)
    
    def _check_server_connection(self):
        """ComfyUI 서버 연결 상태 확인"""
        server_address = self.comfyui_server_var.get().strip()
        if not server_address:
            messagebox.showwarning("오류", "서버 주소를 입력해주세요.")
            return
        
        try:
            host, port = server_address.split(":")
            port = int(port)
        except:
            messagebox.showerror("오류", "서버 주소 형식이 올바르지 않습니다.\n예: 127.0.0.1:8188")
            return
        
        # 소켓으로 포트 연결 확인
        is_connected = check_server_port(host, port)
        
        if is_connected:
            messagebox.showinfo("연결 확인", f"✅ ComfyUI 서버에 연결되었습니다!\n\n서버: {server_address}")
        else:
            msg = f"❌ ComfyUI 서버에 연결할 수 없습니다.\n\n서버: {server_address}\n\n"
            if self.comfyui_bat_path_var.get() and os.path.exists(self.comfyui_bat_path_var.get()):
                if self.auto_start_server_var.get():
                    msg += "자동 시작 옵션이 활성화되어 있습니다.\n작업 시작 시 자동으로 서버를 실행합니다."
                else:
                    msg += "bat 파일이 설정되어 있습니다.\n'서버 자동 시작' 옵션을 활성화하거나\n수동으로 bat 파일을 실행해주세요."
            else:
                msg += "ComfyUI bat 파일을 설정해주세요."
            messagebox.showwarning("연결 실패", msg)
    
    def _check_lowvram_option(self, bat_path: str):
        """bat 파일에 --lowvram 옵션이 있는지 확인하고 없으면 안내"""
        if not bat_path or not os.path.exists(bat_path):
            return
        
        try:
            with open(bat_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # --lowvram 옵션이 있는지 확인
            if '--lowvram' in content:
                self._log("[ComfyUI] ✅ --lowvram 옵션이 이미 설정되어 있습니다.")
                return
            
            # main.py 실행 라인 찾기
            if 'main.py' in content or 'ComfyUI' in content:
                # 사용자에게 안내
                msg = (
                    "💡 성능 개선 팁\n\n"
                    "현재 bat 파일에 '--lowvram' 옵션이 없습니다.\n"
                    "이 옵션을 추가하면 VRAM 관리가 개선되어 로딩 시간이 단축될 수 있습니다.\n\n"
                    "bat 파일을 자동으로 수정하시겠습니까?\n"
                    "(원본 파일은 백업됩니다)"
                )
                
                if messagebox.askyesno("--lowvram 옵션 추가", msg):
                    self._add_lowvram_option(bat_path)
        except Exception as e:
            self._log(f"[ComfyUI] bat 파일 확인 중 오류: {e}")
    
    def _add_lowvram_option(self, bat_path: str):
        """bat 파일에 --lowvram 옵션 추가"""
        try:
            # 백업 파일 생성
            backup_path = bat_path + ".backup"
            import shutil
            shutil.copy2(bat_path, backup_path)
            self._log(f"[ComfyUI] 백업 파일 생성: {os.path.basename(backup_path)}")
            
            # 파일 읽기
            with open(bat_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            # main.py 실행 라인 찾아서 --lowvram 추가
            modified = False
            for i, line in enumerate(lines):
                # main.py가 포함된 라인 찾기
                if 'main.py' in line and '--lowvram' not in line:
                    # 이미 다른 옵션이 있으면 그 뒤에 추가
                    if line.strip().endswith('"') or line.strip().endswith("'"):
                        # 따옴표로 끝나는 경우
                        lines[i] = line.rstrip().rstrip('"').rstrip("'").rstrip() + ' --lowvram"\n'
                    else:
                        # 일반적인 경우
                        lines[i] = line.rstrip() + ' --lowvram\n'
                    modified = True
                    self._log(f"[ComfyUI] --lowvram 옵션 추가됨: {line.strip()[:50]}...")
                    break
            
            if modified:
                # 파일 쓰기
                with open(bat_path, 'w', encoding='utf-8') as f:
                    f.writelines(lines)
                self._log(f"[ComfyUI] ✅ bat 파일 수정 완료: {os.path.basename(bat_path)}")
                messagebox.showinfo(
                    "수정 완료",
                    f"✅ --lowvram 옵션이 추가되었습니다.\n\n"
                    f"원본 파일: {os.path.basename(backup_path)}\n"
                    f"수정된 파일: {os.path.basename(bat_path)}"
                )
            else:
                self._log(f"[ComfyUI] ⚠️ main.py 실행 라인을 찾을 수 없어 수정하지 못했습니다.")
                messagebox.showwarning(
                    "수정 실패",
                    "bat 파일에서 main.py 실행 라인을 찾을 수 없습니다.\n"
                    "수동으로 편집해주세요:\n\n"
                    "python ... main.py 뒤에 --lowvram 추가"
                )
        except Exception as e:
            self._log(f"[ComfyUI] bat 파일 수정 중 오류: {e}")
            import traceback
            self._log(f"[ComfyUI] 상세 오류: {traceback.format_exc()}")
            messagebox.showerror("오류", f"bat 파일 수정 중 오류가 발생했습니다:\n{e}")

    def _load_config(self):
        config_file = os.path.join(os.path.dirname(__file__), "bg_mixing_config.json")
        if os.path.exists(config_file):
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    config = json.load(f)
                    if "workflow_path" in config:
                        self.workflow_path_var.set(config["workflow_path"])
                    if "comfyui_bat_path" in config:
                        self.comfyui_bat_path_var.set(config["comfyui_bat_path"])
                    if "auto_save_interval" in config:
                        self.auto_save_interval_var.set(config["auto_save_interval"])
            except Exception:
                pass

    def _save_config(self):
        config_file = os.path.join(os.path.dirname(__file__), "bg_mixing_config.json")
        try:
            config = {
                "workflow_path": self.workflow_path_var.get(),
                "comfyui_bat_path": self.comfyui_bat_path_var.get(),
                "auto_save_interval": self.auto_save_interval_var.get()
            }
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _log(self, msg: str):
        """로그 출력"""
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')

    def _show_preview_window(self):
        """이미지 미리보기 팝업 창 표시"""
        if self.preview_window is None or not self.preview_window.winfo_exists():
            # 새 팝업 창 생성
            self.preview_window = tk.Toplevel(self)
            self.preview_window.title("🖼️ 이미지 미리보기")
            # 누끼/합성 이미지 좌우 1:1 비율 + 하단 버튼 공간 확보
            # 가로:세로 = 약 1.6:1 비율 (예: 1000x620)
            self.preview_window.geometry("1000x620")
            self.preview_window.resizable(True, True)
            
            # 창 닫기 이벤트 처리
            self.preview_window.protocol("WM_DELETE_WINDOW", self._close_preview_window)
            
            # 메인 프레임
            main_preview_frame = ttk.Frame(self.preview_window, padding=10)
            main_preview_frame.pack(fill='both', expand=True)
            
            # 상품 정보 표시 영역
            info_frame = ttk.Frame(main_preview_frame)
            info_frame.pack(fill='x', pady=(0, 10))
            
            self.preview_product_code_label = ttk.Label(
                info_frame, 
                text="상품코드: -", 
                font=("맑은 고딕", 11, "bold"),
                foreground="#333"
            )
            self.preview_product_code_label.pack(side='left', padx=10)
            
            self.preview_product_name_label = ttk.Label(
                info_frame, 
                text="원본상품명: -", 
                font=("맑은 고딕", 10),
                foreground="#666"
            )
            self.preview_product_name_label.pack(side='left', padx=10)
            
            # 이미지 표시 영역 (좌우 1:1 비율)
            preview_content = ttk.Frame(main_preview_frame)
            preview_content.pack(fill='both', expand=True, padx=5, pady=5)
            
            # 왼쪽: 누끼 이미지
            left_frame = ttk.LabelFrame(preview_content, text="누끼 이미지 (IMG_S1_누끼)", padding=5)
            left_frame.pack(side='left', fill='both', expand=True, padx=(0, 5))
            
            self.preview_nukki_label = ttk.Label(left_frame, text="이미지 없음", anchor='center')
            self.preview_nukki_label.pack(fill='both', expand=True)
            
            # 오른쪽: 합성된 이미지
            right_frame = ttk.LabelFrame(preview_content, text="합성된 이미지 (IMG_S4_mix_생성경로)", padding=5)
            right_frame.pack(side='right', fill='both', expand=True, padx=(5, 0))
            
            self.preview_mix_label = ttk.Label(right_frame, text="이미지 없음", anchor='center')
            self.preview_mix_label.pack(fill='both', expand=True)
            
            # 네비게이션 버튼 (이전/다음)
            nav_frame = ttk.Frame(main_preview_frame)
            nav_frame.pack(fill='x', pady=(5, 0))
            
            self.btn_prev = ttk.Button(nav_frame, text="◀ 이전", command=self._show_previous_item, state='disabled')
            self.btn_prev.pack(side='left', padx=5)
            
            self.preview_index_label = ttk.Label(nav_frame, text="0 / 0", font=("맑은 고딕", 9))
            self.preview_index_label.pack(side='left', padx=10)
            
            self.btn_next = ttk.Button(nav_frame, text="다음 ▶", command=self._show_next_item, state='disabled')
            self.btn_next.pack(side='left', padx=5)
            
            # 닫기 버튼
            btn_close = ttk.Button(nav_frame, text="닫기", command=self._close_preview_window)
            btn_close.pack(side='right', padx=5)
            
            # 창 크기 변경 이벤트 바인딩 (이미지 크기 조정)
            self.preview_window.bind('<Configure>', lambda e: self._on_preview_window_resize())
            
            # 키보드 이벤트 바인딩 (좌우 화살표 키로 이전/다음 이동)
            self.preview_window.bind('<Left>', lambda e: self._show_previous_item())
            self.preview_window.bind('<Right>', lambda e: self._show_next_item())
            # 포커스를 받을 수 있도록 설정
            self.preview_window.focus_set()
            
            # 키보드 단축키 안내 레이블 추가
            keyboard_hint = ttk.Label(
                nav_frame, 
                text="💡 좌우 화살표 키(← →)로 이동 가능", 
                font=("맑은 고딕", 8),
                foreground="#666"
            )
            keyboard_hint.pack(side='left', padx=(20, 0))
            
            # 창이 완전히 렌더링된 후 네비게이션 버튼 상태 업데이트
            self.preview_window.after(100, self._update_navigation_buttons)
            
            # 현재 이미지가 있으면 표시
            if self.current_nukki_image_path or self.current_mix_image_path:
                # 창이 완전히 렌더링된 후 이미지 표시
                self.preview_window.after(100, lambda: self._update_preview_images(
                    nukki_path=self.current_nukki_image_path,
                    mix_path=self.current_mix_image_path,
                    product_code=self.current_product_code,
                    product_name=self.current_product_name
                ))
        else:
            # 이미 열려있으면 포커스 및 네비게이션 버튼 업데이트
            self.preview_window.lift()
            self.preview_window.focus()
            self.preview_window.focus_set()
            self._update_navigation_buttons()
    
    def _close_preview_window(self):
        """미리보기 창 닫기"""
        if self.preview_window:
            self.preview_window.destroy()
            self.preview_window = None
    
    def _on_preview_window_resize(self):
        """팝업 창 크기 변경 시 이미지 재조정"""
        if self.preview_window and self.preview_window.winfo_exists():
            # 현재 이미지 경로가 있으면 다시 표시
            if self.current_nukki_image_path or self.current_mix_image_path:
                self._update_preview_images(
                    nukki_path=self.current_nukki_image_path,
                    mix_path=self.current_mix_image_path,
                    product_code=self.current_product_code,
                    product_name=self.current_product_name
                )
    
    def _update_preview_images(self, nukki_path: Optional[str] = None, mix_path: Optional[str] = None, 
                              product_code: Optional[str] = None, product_name: Optional[str] = None):
        """미리보기 이미지 업데이트"""
        if not PIL_AVAILABLE:
            return
        
        # 현재 이미지 경로 및 상품 정보 저장
        if nukki_path:
            self.current_nukki_image_path = nukki_path
        if mix_path:
            self.current_mix_image_path = mix_path
        if product_code:
            self.current_product_code = product_code
        if product_name:
            self.current_product_name = product_name
        
        # 팝업 창이 열려있지 않으면 업데이트하지 않음
        if self.preview_window is None or not self.preview_window.winfo_exists():
            return
        
        def update_ui():
            try:
                # 상품 정보 업데이트
                if hasattr(self, 'preview_product_code_label'):
                    code_text = f"상품코드: {product_code if product_code else '-'}"
                    self.preview_product_code_label.config(text=code_text)
                
                if hasattr(self, 'preview_product_name_label'):
                    name_text = f"원본상품명: {product_name if product_name else '-'}"
                    # 긴 상품명은 잘라서 표시
                    if product_name and len(product_name) > 50:
                        name_text = f"원본상품명: {product_name[:47]}..."
                    self.preview_product_name_label.config(text=name_text)
                
                # 누끼 이미지 업데이트
                if nukki_path and os.path.exists(nukki_path):
                    img = Image.open(nukki_path)
                    # 팝업 창 크기에 맞춰 리사이즈 (1:1 비율 유지)
                    # 창 크기의 약 45% 크기로 설정 (양쪽 여백 고려)
                    window_width = self.preview_window.winfo_width()
                    window_height = self.preview_window.winfo_height()
                    if window_width > 1 and window_height > 1:
                        max_size = min(window_width // 2 - 50, window_height - 100)
                    else:
                        max_size = 350  # 기본값
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                    self.preview_nukki_photo = ImageTk.PhotoImage(img)
                    self.preview_nukki_label.config(image=self.preview_nukki_photo, text="")
                elif nukki_path:
                    self.preview_nukki_label.config(image="", text=f"파일 없음:\n{os.path.basename(nukki_path)}")
                else:
                    self.preview_nukki_label.config(image="", text="이미지 없음")
                
                # 합성 이미지 업데이트
                if mix_path and os.path.exists(mix_path):
                    img = Image.open(mix_path)
                    # 팝업 창 크기에 맞춰 리사이즈 (1:1 비율 유지)
                    window_width = self.preview_window.winfo_width()
                    window_height = self.preview_window.winfo_height()
                    if window_width > 1 and window_height > 1:
                        max_size = min(window_width // 2 - 50, window_height - 100)
                    else:
                        max_size = 350  # 기본값
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                    self.preview_mix_photo = ImageTk.PhotoImage(img)
                    self.preview_mix_label.config(image=self.preview_mix_photo, text="")
                elif mix_path:
                    self.preview_mix_label.config(image="", text=f"파일 없음:\n{os.path.basename(mix_path)}")
                else:
                    self.preview_mix_label.config(image="", text="이미지 없음")
                
                # 네비게이션 버튼 상태 업데이트
                self._update_navigation_buttons()
            except Exception as e:
                self._log(f"[미리보기] 이미지 로드 오류: {e}")
        
        # UI 스레드에서 실행
        self.after(0, update_ui)
    
    def _update_navigation_buttons(self):
        """네비게이션 버튼 상태 업데이트"""
        # 미리보기 창이 없거나 파괴되었으면 업데이트하지 않음
        if self.preview_window is None or not self.preview_window.winfo_exists():
            return
        
        # 위젯이 존재하는지 확인
        if not hasattr(self, 'btn_prev') or not hasattr(self, 'btn_next'):
            return
        
        # 위젯이 실제로 존재하는지 확인 (파괴되었을 수 있음)
        try:
            # 위젯 존재 여부 확인
            if not self.btn_prev.winfo_exists() or not self.btn_next.winfo_exists():
                return
        except (tk.TclError, AttributeError):
            # 위젯이 파괴되었거나 접근할 수 없음
            return
        
        total = len(self.completed_items)
        try:
            if total == 0:
                self.btn_prev.config(state='disabled')
                self.btn_next.config(state='disabled')
                if hasattr(self, 'preview_index_label') and self.preview_index_label.winfo_exists():
                    self.preview_index_label.config(text="0 / 0")
            else:
                # 이전 버튼
                if self.current_preview_index > 0:
                    self.btn_prev.config(state='normal')
                else:
                    self.btn_prev.config(state='disabled')
                
                # 다음 버튼
                if self.current_preview_index < total - 1:
                    self.btn_next.config(state='normal')
                else:
                    self.btn_next.config(state='disabled')
                
                # 인덱스 레이블 업데이트
                if hasattr(self, 'preview_index_label') and self.preview_index_label.winfo_exists():
                    self.preview_index_label.config(text=f"{self.current_preview_index + 1} / {total}")
        except (tk.TclError, AttributeError) as e:
            # 위젯 접근 중 오류 발생 (창이 닫혔을 수 있음)
            debug_log(f"네비게이션 버튼 업데이트 중 오류 (무시됨): {e}", "WARN")
            return
    
    def _show_previous_item(self):
        """이전 완료된 항목 표시"""
        # 미리보기 창이 없으면 동작하지 않음
        if self.preview_window is None or not self.preview_window.winfo_exists():
            return
        
        if self.current_preview_index > 0 and self.completed_items:
            self.current_preview_index -= 1
            item = self.completed_items[self.current_preview_index]
            self._update_preview_images(
                nukki_path=item['nukki_path'],
                mix_path=item['mix_path'],
                product_code=item.get('code', ''),
                product_name=item.get('name', '')
            )
            self._update_navigation_buttons()
    
    def _show_next_item(self):
        """다음 완료된 항목 표시"""
        # 미리보기 창이 없으면 동작하지 않음
        if self.preview_window is None or not self.preview_window.winfo_exists():
            return
        
        if self.current_preview_index < len(self.completed_items) - 1:
            self.current_preview_index += 1
            item = self.completed_items[self.current_preview_index]
            self._update_preview_images(
                nukki_path=item['nukki_path'],
                mix_path=item['mix_path'],
                product_code=item.get('code', ''),
                product_name=item.get('name', '')
            )
            self._update_navigation_buttons()

    def _start_thread(self):
        if self.is_running:
            return
        
        if not self.input_file_path.get():
            messagebox.showwarning("오류", "파일을 선택해주세요.")
            return
        
        if not self.workflow_path_var.get() or not os.path.exists(self.workflow_path_var.get()):
            messagebox.showwarning("오류", "ComfyUI 워크플로우 JSON 파일을 선택해주세요.")
            return
        
        self.is_running = True
        self.stop_requested = False
        self.btn_start.config(state='disabled')
        self.btn_stop.config(state='normal')
        
        # 시작 시간 및 예상 종료 시간 초기화
        self.stat_start_time.set("-")
        self.stat_estimated_end.set("-")
        
        threading.Thread(target=self._run_process, daemon=True).start()

    def _start_time_timer(self, start_time):
        """경과 시간 실시간 업데이트 타이머 시작"""
        def update_time():
            if not self.is_running:
                return
            
            elapsed = time.time() - start_time
            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            seconds = int(elapsed % 60)
            self.stat_time.set(f"{hours:02d}:{minutes:02d}:{seconds:02d}")
            
            # 1초마다 업데이트
            self.time_timer_id = self.after(1000, update_time)
        
        # 첫 업데이트 즉시 실행
        update_time()
    
    def _stop_time_timer(self):
        """경과 시간 타이머 중지"""
        if self.time_timer_id:
            self.after_cancel(self.time_timer_id)
            self.time_timer_id = None

    def _request_stop(self):
        self.stop_requested = True
        self._log("⛔ 중단 요청됨...")

    def _start_comfyui_server(self):
        bat_path = self.comfyui_bat_path_var.get()
        if not bat_path:
            return False
        
        # bat 파일이 존재하면 그대로 사용
        if os.path.exists(bat_path):
            try:
                bat_dir = os.path.dirname(os.path.abspath(bat_path))
                process = subprocess.Popen(
                    ["cmd.exe", "/c", "start", "/D", bat_dir, os.path.basename(bat_path)],
                    cwd=bat_dir,
                    shell=False
                )
                self._log(f"[ComfyUI] 서버 시작 시도 (PID: {process.pid})")
                return True
            except Exception as e:
                self._log(f"[ComfyUI] 서버 시작 실패: {e}")
                return False
        
        # bat 파일이 없어도 경로에서 추론해서 시도
        bat_dir = os.path.dirname(os.path.abspath(bat_path))
        self._log(f"[ComfyUI] bat 파일이 없지만 경로에서 추론 시도: {bat_dir}")
        
        # 일반적인 bat 파일 이름들 시도
        common_bat_names = [
            "run_nvidia_gpu.bat",
            "run_cpu.bat",
            "run.bat",
            "start.bat",
            "ComfyUI.bat",
        ]
        
        for bat_name in common_bat_names:
            test_bat_path = os.path.join(bat_dir, bat_name)
            if os.path.exists(test_bat_path):
                try:
                    process = subprocess.Popen(
                        ["cmd.exe", "/c", "start", "/D", bat_dir, bat_name],
                        cwd=bat_dir,
                        shell=False
                    )
                    self._log(f"[ComfyUI] 서버 시작 시도 (추론된 bat 파일: {bat_name}, PID: {process.pid})")
                    return True
                except Exception as e:
                    self._log(f"[ComfyUI] 서버 시작 실패 ({bat_name}): {e}")
                    continue
        
        # bat 파일을 찾지 못했지만, 경로가 설정되어 있으면 그 디렉토리에서 python으로 직접 시작 시도
        # ComfyUI 폴더 찾기
        comfyui_dirs = [
            os.path.join(bat_dir, "ComfyUI"),
            bat_dir,
            os.path.dirname(bat_dir),
        ]
        
        for comfyui_dir in comfyui_dirs:
            main_py = os.path.join(comfyui_dir, "main.py")
            if os.path.exists(main_py):
                try:
                    # python으로 직접 실행
                    self._log(f"[ComfyUI] Python으로 직접 시작 시도: {main_py}")
                    process = subprocess.Popen(
                        ["python", "main.py", "--lowvram"],
                        cwd=comfyui_dir,
                        shell=True,
                        creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                    )
                    self._log(f"[ComfyUI] 서버 시작 시도 (Python 직접 실행, PID: {process.pid})")
                    return True
                except Exception as e:
                    self._log(f"[ComfyUI] Python 직접 실행 실패: {e}")
                    continue
        
        self._log(f"[ComfyUI] 서버 시작 실패: bat 파일을 찾을 수 없습니다.")
        return False

    def _wait_for_server(self, host: str, port: int, max_wait: int = 60):
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if self.stop_requested:
                return False
            if check_server_port(host, port, timeout=1.0):
                return True
            time.sleep(0.5)
        return False

    def _find_i4_file(self, input_path):
        """
        I5 파일 경로에서 I4 파일을 찾습니다.
        예: 신년DB오너클랜_3_T4(완)_I5.xlsx -> 신년DB오너클랜_3_T4(완)_I4.xlsx
        Returns: I4 파일 경로 또는 None
        """
        try:
            dir_name = os.path.dirname(input_path)
            base_name = os.path.basename(input_path)
            name_only, ext = os.path.splitext(base_name)
            
            # I5 -> I4 변환
            i4_name = re.sub(r'_I5(\([^)]+\))?$', r'_I4\1', name_only, flags=re.IGNORECASE)
            if i4_name == name_only:
                # 패턴이 없으면 다른 방법 시도
                i4_name = re.sub(r'_I(\d+)(\([^)]+\))?$', r'_I4\2', name_only, flags=re.IGNORECASE)
            
            i4_path = os.path.join(dir_name, i4_name + ext)
            
            if os.path.exists(i4_path):
                return i4_path
            
            # 같은 디렉토리에서 I4 패턴 파일 찾기
            if os.path.exists(dir_name):
                root_name = get_root_filename(input_path)
                for f in os.listdir(dir_name):
                    if f.endswith('.xlsx') or f.endswith('.xls'):
                        f_root = get_root_filename(f)
                        if f_root == root_name:
                            # I4 패턴 확인
                            if re.search(r'_I4(\([^)]+\))?\.(xlsx|xls)$', f, re.IGNORECASE):
                                i4_path = os.path.join(dir_name, f)
                                if os.path.exists(i4_path):
                                    return i4_path
        except Exception as e:
            debug_log(f"I4 파일 찾기 실패: {e}", "WARN")
        return None
    
    def _recover_from_output_directory(self, df, comfyui_output_dir, excel_path=None):
        """
        ComfyUI output 디렉토리에서 직접 합성 이미지 파일을 스캔하여 엑셀에 복구합니다.
        디버그 로그가 없어도 output 디렉토리의 실제 파일을 확인하여 복구할 수 있습니다.
        excel_path가 제공되면 파일 생성 시간과 엑셀 수정 시간을 비교하여 검증합니다.
        Returns: 복구된 항목 수
        """
        recovered_count = 0
        
        try:
            if not comfyui_output_dir or not os.path.exists(comfyui_output_dir):
                return 0
            
            self._log(f"📂 Output 디렉토리에서 합성 이미지 파일 스캔 중: {comfyui_output_dir}")
            debug_log(f"Output 디렉토리 스캔 시작: {comfyui_output_dir}", "INFO")
            
            # 엑셀 파일 수정 시간 가져오기 (검증용)
            excel_mtime = None
            if excel_path and os.path.exists(excel_path):
                excel_mtime = os.path.getmtime(excel_path)
                # 엑셀 파일 수정 시간 기준으로 7일 이내 생성된 파일만 복구 (다른 작업 파일 제외, 재부팅 후에도 복구 가능)
                time_window = 7 * 24 * 60 * 60  # 7일 (초)
            
            # output 디렉토리의 모든 파일 목록
            try:
                output_files = os.listdir(comfyui_output_dir)
            except Exception as e:
                self._log(f"⚠️ output 디렉토리 읽기 실패: {e}")
                return 0
            
            # comp_{상품코드}_row{N}_ 또는 comp_row{N}_ 패턴으로 시작하는 파일 찾기
            comp_files = {}
            for filename in output_files:
                if filename.endswith('.png') and filename.startswith('comp'):
                    # 새 패턴: comp_{상품코드}_row{N}_ 또는 기존 패턴: comp_row{N}_
                    match_new = re.match(r'comp_([^_]+)_row(\d+)_', filename)
                    match_old = re.match(r'comp_row(\d+)_', filename)

                    if match_new:
                        file_product_code = match_new.group(1)
                        row_num = int(match_new.group(2))
                    elif match_old:
                        file_product_code = None  # 기존 패턴은 상품코드 없음
                        row_num = int(match_old.group(1))
                    else:
                        continue

                    row_idx = row_num - 1

                    # 엑셀 범위 체크 (먼저 필터링)
                    if row_idx < 0 or row_idx >= len(df):
                        continue

                    file_path = os.path.join(comfyui_output_dir, filename)
                    file_mtime = os.path.getmtime(file_path)

                    # 엑셀 파일 수정 시간과 비교하여 검증 (7일 이내 파일만)
                    if excel_mtime:
                        time_diff = abs(file_mtime - excel_mtime)
                        # 파일이 엑셀 수정 시간보다 너무 오래 전이거나 미래면 스킵 (다른 작업 파일일 가능성)
                        if file_mtime < excel_mtime - time_window or file_mtime > excel_mtime + time_window:
                            continue

                    if row_idx not in comp_files:
                        comp_files[row_idx] = []
                    comp_files[row_idx].append({
                        'filename': filename,
                        'path': file_path,
                        'mtime': file_mtime,
                        'product_code': file_product_code  # 상품코드 저장
                    })
            
            if not comp_files:
                self._log(f"📂 Output 디렉토리에서 합성 이미지 파일을 찾을 수 없습니다.")
                return 0
            
            self._log(f"📂 Output 디렉토리에서 {len(comp_files)}건의 합성 이미지 파일을 발견했습니다.")
            debug_log(f"Output 디렉토리에서 발견된 파일: {len(comp_files)}건", "INFO")
            
            # 엑셀에 복구
            for row_idx, files in comp_files.items():
                # 이미 경로가 있으면 스킵
                existing_path = str(df.at[row_idx, "IMG_S4_mix_생성경로"]).strip()
                if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                    continue

                # 엑셀의 해당 row 상품코드 가져오기
                excel_product_code = str(df.at[row_idx, "상품코드"]).strip() if "상품코드" in df.columns else ""
                safe_excel_code = re.sub(r'[\\/*?:"<>|]', '', excel_product_code)[:30] if excel_product_code else ""

                # 상품코드로 필터링: 엑셀 상품코드와 파일 상품코드가 일치하는 것만
                matching_files = []
                for f in files:
                    if safe_excel_code:
                        # 엑셀에 상품코드가 있으면, 파일도 같은 상품코드여야 함
                        if f['product_code'] == safe_excel_code:
                            matching_files.append(f)
                    else:
                        # 엑셀에 상품코드가 없으면, 파일도 상품코드 없는 기존 패턴만
                        if f['product_code'] is None:
                            matching_files.append(f)

                if not matching_files:
                    continue

                # 가장 최근 파일 선택 (타임스탬프 기준)
                matching_files.sort(key=lambda x: x['mtime'], reverse=True)
                file_path = matching_files[0]['path']

                if os.path.exists(file_path):
                    df.at[row_idx, "IMG_S4_mix_생성경로"] = file_path
                    recovered_count += 1
                    if recovered_count <= 10:  # 처음 10개만 상세 로그
                        debug_log(f"복구 (output 스캔): row {row_idx+1} (상품코드: {excel_product_code}) -> {matching_files[0]['filename']}", "INFO")
            
            if recovered_count > 10:
                debug_log(f"복구 (output 스캔): 추가로 {recovered_count - 10}건 복구됨", "INFO")
            
        except Exception as e:
            self._log(f"⚠️ Output 디렉토리 스캔 중 오류: {e}")
            debug_log(f"Output 디렉토리 스캔 오류: {e}", "ERROR")
            import traceback
            debug_log(traceback.format_exc(), "ERROR")
        
        return recovered_count
    
    def _recover_from_debug_log(self, df, log_dir, comfyui_output_dir, excel_path):
        """
        이전 디버그 로그 파일에서 처리 완료된 항목을 복구합니다.
        comfyui_output_dir이 None이면 로그만 파싱하고 파일 찾기는 나중에 수행합니다.
        Returns: 복구된 항목 수
        """
        recovered_count = 0
        
        try:
            # 현재 디렉토리에서 이전 디버그 로그 파일 찾기 (.log 및 .txt 모두 지원)
            log_files = []
            if os.path.exists(log_dir):
                for f in os.listdir(log_dir):
                    if f.startswith("img_mixing_debug_") and (f.endswith(".log") or f.endswith(".txt")):
                        log_path = os.path.join(log_dir, f)
                        # 현재 생성되는 로그 파일 제외 (아직 초기화 단계이므로)
                        if os.path.getmtime(log_path) < time.time() - 5:  # 5초 이전 파일만
                            log_files.append((log_path, os.path.getmtime(log_path)))
            
            # 수정 시간 기준으로 정렬 (최신순)
            log_files.sort(key=lambda x: x[1], reverse=True)
            
            if not log_files:
                return 0
            
            # 처리 완료된 항목 추출 (모든 로그 파일에서 누적 수집)
            # row_idx → 파일명 리스트 매핑 (같은 row가 여러 로그에 있을 수 있음)
            completed_rows = {}  # {row_idx: [{'filename': str, 'log_file': str, 'log_time': float}, ...]}
            
            log_count = 0
            total_completed_entries = 0
            
            for log_path, log_mtime in log_files:
                try:
                    log_count += 1
                    file_completed_count = 0
                    with open(log_path, "r", encoding="utf-8") as f:
                        for line in f:
                            # [배치] [N/M] ✅ 처리 완료: comp_{상품코드}_row{N}_...png 또는 comp_row{N}_...png 패턴 찾기
                            match = re.search(r'\[배치\]\s*\[\d+/\d+\]\s*✅\s*처리\s*완료:\s*(comp_(?:[^_]+_)?row(\d+)_[^\s]+\.png)', line)
                            if match:
                                filename = match.group(1)
                                row_num = int(match.group(2))  # row{N}에서 N 추출
                                row_idx = row_num - 1  # 엑셀 인덱스는 0부터 시작
                                
                                # 모든 로그 파일에서 누적 수집 (중복 허용)
                                if row_idx not in completed_rows:
                                    completed_rows[row_idx] = []
                                completed_rows[row_idx].append({
                                    'filename': filename,
                                    'log_file': os.path.basename(log_path),
                                    'log_time': log_mtime  # 로그 파일 수정 시간 (최신 우선 정렬용)
                                })
                                file_completed_count += 1
                                total_completed_entries += 1
                    
                    if file_completed_count > 0:
                        debug_log(f"로그 파일 ({os.path.basename(log_path)}): {file_completed_count}건의 처리 완료 항목 발견", "INFO")
                except Exception as e:
                    debug_log(f"로그 파일 읽기 실패 ({os.path.basename(log_path)}): {e}", "WARN")
                    continue
            
            if not completed_rows:
                if log_count > 0:
                    self._log(f"📋 {log_count}개 로그 파일 확인 완료 (처리 완료 항목 없음)")
                return 0
            
            # 각 row_idx별로 최신 로그 파일의 파일명 우선 사용 (같은 row가 여러 로그에 있으면 최신 것 사용)
            completed_rows_final = {}  # {row_idx: {'filename': str, 'log_file': str}}
            for row_idx, entries in completed_rows.items():
                # 로그 파일 시간 기준으로 정렬 (최신순)
                entries.sort(key=lambda x: x['log_time'], reverse=True)
                # 가장 최신 로그의 파일명 사용
                completed_rows_final[row_idx] = {
                    'filename': entries[0]['filename'],
                    'log_file': entries[0]['log_file'],
                    'found_in_logs': len(entries)  # 여러 로그에서 발견된 경우 기록
                }
            
            self._log(f"📋 {log_count}개 로그 파일 확인 완료: 총 {len(completed_rows_final)}건의 처리 완료 항목 발견 (누적 {total_completed_entries}건)")
            debug_log(f"이전 로그에서 발견된 처리 완료 항목: {len(completed_rows_final)}건 (총 {total_completed_entries}건)", "INFO")
            
            # 여러 로그에서 발견된 항목이 있으면 로그
            multi_log_items = {k: v for k, v in completed_rows_final.items() if v.get('found_in_logs', 1) > 1}
            if multi_log_items:
                self._log(f"   (참고: {len(multi_log_items)}건은 여러 로그 파일에서 발견됨, 최신 로그 기준 사용)")
                debug_log(f"여러 로그에서 발견된 항목: {len(multi_log_items)}건", "INFO")
            
            # ComfyUI output 디렉토리가 제공되지 않은 경우, 엑셀 파일에서 경로 추출 시도
            if not comfyui_output_dir:
                # 나중에 ComfyUI 경로를 찾은 후 다시 시도하도록 None 반환
                # 대신 로그만 확인하고 경로 복구는 나중에 수행
                return 0
            
            # 파일 시스템에서 실제 파일 찾기 및 엑셀에 복구
            if not os.path.exists(comfyui_output_dir):
                self._log(f"⚠️ ComfyUI output 디렉토리를 찾을 수 없습니다: {comfyui_output_dir}")
                return 0
            
            # output 디렉토리의 모든 파일 목록
            try:
                output_files = os.listdir(comfyui_output_dir)
            except Exception as e:
                self._log(f"⚠️ output 디렉토리 읽기 실패: {e}")
                return 0
            
            # 엑셀 파일 수정 시간 가져오기 (검증용)
            excel_mtime = None
            time_window = 7 * 24 * 60 * 60  # 7일 (초) - 재부팅 후에도 복구 가능하도록 확장
            if excel_path and os.path.exists(excel_path):
                excel_mtime = os.path.getmtime(excel_path)
            
            for row_idx, info in completed_rows_final.items():
                # 엑셀 범위 체크
                if row_idx < 0 or row_idx >= len(df):
                    continue
                
                # 이미 경로가 있으면 스킵
                existing_path = str(df.at[row_idx, "IMG_S4_mix_생성경로"]).strip()
                if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                    continue
                
                filename = info['filename']
                # 파일명으로 파일 찾기 (정확한 매칭)
                if filename in output_files:
                    file_path = os.path.join(comfyui_output_dir, filename)
                    if os.path.exists(file_path):
                        # 파일 생성 시간 검증 (엑셀 수정 시간과 비교)
                        if excel_mtime:
                            file_mtime = os.path.getmtime(file_path)
                            time_diff = abs(file_mtime - excel_mtime)
                            if file_mtime < excel_mtime - time_window or file_mtime > excel_mtime + time_window:
                                # 엑셀 파일 수정 시간과 너무 차이나면 다른 작업 파일일 가능성
                                debug_log(f"복구 스킵 (시간 검증 실패): row {row_idx+1} -> {filename} (시간차: {time_diff/3600:.1f}시간)", "WARN")
                                continue
                        
                        df.at[row_idx, "IMG_S4_mix_생성경로"] = file_path
                        recovered_count += 1
                        # 상품코드 정보도 로그에 추가 (검증용)
                        product_code = str(df.at[row_idx, "상품코드"]).strip() if "상품코드" in df.columns else "N/A"
                        debug_log(f"복구 (로그): row {row_idx+1} (상품코드: {product_code}) -> {filename}", "INFO")
                else:
                    # 정확한 매칭 실패 시 부분 매칭 시도
                    row_num = row_idx + 1
                    # 상품코드로 먼저 매칭 시도, 없으면 row 번호로 매칭
                    product_code_for_match = str(df.at[row_idx, "상품코드"]).strip() if "상품코드" in df.columns else ""
                    safe_code = re.sub(r'[\\/*?:"<>|]', '', product_code_for_match)[:30] if product_code_for_match else ""

                    # 상품코드가 있으면 새 패턴만 매칭 (다른 엑셀 파일과 충돌 방지)
                    # 상품코드가 없으면 기존 패턴으로 매칭 (하위 호환)
                    if safe_code:
                        prefix_new = f"comp_{safe_code}_row{row_num}_"
                        matching_files = [f for f in output_files if f.startswith(prefix_new)]
                    else:
                        prefix_old = f"comp_row{row_num}_"
                        matching_files = [f for f in output_files if f.startswith(prefix_old)]
                    if matching_files:
                        # 가장 최근 파일 선택 (타임스탬프 기준) + 시간 검증
                        files_with_time = []
                        for f in matching_files:
                            f_path = os.path.join(comfyui_output_dir, f)
                            file_mtime = os.path.getmtime(f_path)
                            # 파일 생성 시간 검증
                            if excel_mtime:
                                time_diff = abs(file_mtime - excel_mtime)
                                if file_mtime < excel_mtime - time_window or file_mtime > excel_mtime + time_window:
                                    continue  # 다른 작업 파일이므로 스킵
                            files_with_time.append((f, file_mtime))
                        
                        if files_with_time:
                            files_with_time.sort(key=lambda x: x[1], reverse=True)
                            file_path = os.path.join(comfyui_output_dir, files_with_time[0][0])
                            if os.path.exists(file_path):
                                df.at[row_idx, "IMG_S4_mix_생성경로"] = file_path
                                recovered_count += 1
                                # 상품코드 정보도 로그에 추가 (검증용)
                                product_code = str(df.at[row_idx, "상품코드"]).strip() if "상품코드" in df.columns else "N/A"
                                debug_log(f"복구 (로그, 부분매칭): row {row_idx+1} (상품코드: {product_code}) -> {files_with_time[0][0]}", "INFO")
                            debug_log(f"복구 (부분매칭): row {row_idx+1} -> {files_with_time[0][0]}", "INFO")
            
        except Exception as e:
            self._log(f"⚠️ 이전 로그 복구 중 오류: {e}")
            debug_log(f"이전 로그 복구 오류: {e}", "ERROR")
            import traceback
            debug_log(traceback.format_exc(), "ERROR")
        
        return recovered_count

    def _run_process(self):
        input_path = self.input_file_path.get()
        workflow_path = self.workflow_path_var.get()
        server_address = self.comfyui_server_var.get().strip()

        # 디버그 로그 초기화
        output_dir = os.path.dirname(os.path.abspath(input_path))
        init_debug_log(output_dir)
        debug_log(f"작업 시작: {os.path.basename(input_path)}", "INFO")
        debug_log(f"워크플로우: {os.path.basename(workflow_path)}", "INFO")
        debug_log(f"서버 주소: {server_address}", "INFO")

        # 완료된 항목 목록 초기화
        self.completed_items = []
        self.current_preview_index = -1

        # 메인 런처 현황판 업데이트 (I4-2: 배경 합성 진행중) - img 상태만 업데이트 (text 상태는 변경하지 않음)
        try:
            root_name = get_root_filename(input_path)
            JobManager.update_status(root_name, img_s4_2_msg="I4-2 (진행중)")
            self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-2 (진행중)")
            debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-2 (진행중)", "INFO")
        except Exception as e:
            debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")

        start_time = time.time()
        start_datetime = datetime.now()
        start_time_str = start_datetime.strftime("%H:%M:%S")
        self.after(0, lambda: self.stat_start_time.set(start_time_str))
        
        # 경과 시간 실시간 업데이트를 위한 타이머 시작
        self._start_time_timer(start_time)
        
        stats = {"total": 0, "success": 0, "fail": 0, "skip": 0}

        try:
            # 워크플로우 로드
            debug_log(f"워크플로우 JSON 로드 시작: {workflow_path}", "DEBUG")
            with open(workflow_path, "r", encoding="utf-8") as f:
                base_workflow = json.load(f)
            self._log(f"워크플로우 로드 완료: {os.path.basename(workflow_path)}")
            debug_log(f"워크플로우 로드 완료: {len(base_workflow)}개 노드", "INFO")

            # 엑셀 로드 (중단 후 재시작 시 저장된 파일 자동 사용)
            # IMG_mixing.py는 I5 파일을 입력으로 받고 같은 파일에 저장하므로,
            # 재시작 시 같은 파일을 읽으면 자동으로 진행된 항목이 반영됨
            self._log(f"엑셀 로드 중... {os.path.basename(input_path)}")
            debug_log(f"엑셀 파일 로드 시작: {input_path}", "DEBUG")
            
            # 파일이 존재하는지 확인
            if not os.path.exists(input_path):
                raise Exception(f"입력 파일을 찾을 수 없습니다: {input_path}")
            
            # 파일 크기 확인 (0바이트이면 손상 가능성)
            file_size = os.path.getsize(input_path)
            if file_size == 0:
                raise Exception(f"입력 파일이 비어있습니다 (0바이트). 파일이 손상되었을 수 있습니다: {input_path}")
            
            # 파일 수정 시간 확인 (중단 후 저장된 파일인지 확인)
            file_mtime = os.path.getmtime(input_path)
            file_mtime_str = datetime.fromtimestamp(file_mtime).strftime("%Y-%m-%d %H:%M:%S")
            self._log(f"📄 파일 수정 시간: {file_mtime_str}, 크기: {file_size:,} bytes")
            debug_log(f"파일 수정 시간: {file_mtime_str}, 크기: {file_size:,} bytes", "INFO")
            
            # 엑셀 파일 읽기 (에러 처리 강화)
            _, ext = os.path.splitext(input_path.lower())
            df = None
            read_error = None
            
            try:
                if ext == '.xlsx':
                    df = pd.read_excel(input_path, engine='openpyxl')
                elif ext == '.xls':
                    df = pd.read_excel(input_path, engine='xlrd')
                else:
                    # 기본값으로 openpyxl 시도
                    try:
                        df = pd.read_excel(input_path, engine='openpyxl')
                    except Exception as e1:
                        try:
                            df = pd.read_excel(input_path, engine='xlrd')
                        except Exception as e2:
                            raise Exception(f"엑셀 파일 읽기 실패 (openpyxl: {e1}, xlrd: {e2})")
            except zipfile.BadZipFile as e:
                # I5 파일이 손상되었으면 I4 파일 찾기 시도
                i4_path = self._find_i4_file(input_path)
                if i4_path and os.path.exists(i4_path):
                    self._log(f"⚠️ I5 파일이 손상되어 I4 파일을 사용합니다: {os.path.basename(i4_path)}")
                    debug_log(f"I5 파일 손상 (BadZipFile), I4 파일 사용: {i4_path}", "WARN")
                    input_path = i4_path
                    # I4 파일 다시 읽기
                    try:
                        if ext == '.xlsx':
                            df = pd.read_excel(i4_path, engine='openpyxl')
                        elif ext == '.xls':
                            df = pd.read_excel(i4_path, engine='xlrd')
                        else:
                            df = pd.read_excel(i4_path, engine='openpyxl')
                        debug_log(f"I4 파일 로드 완료: {len(df)}행, {len(df.columns)}컬럼", "INFO")
                        self._log(f"✅ I4 파일 로드 성공: {len(df)}행")
                    except Exception as e2:
                        raise Exception(
                            f"엑셀 파일이 손상되었습니다.\n\n"
                            f"손상된 파일: {os.path.basename(input_path)}\n"
                            f"대체 파일(I4) 읽기 실패: {os.path.basename(i4_path)}\n"
                            f"오류: {str(e2)}\n\n"
                            f"해결 방법:\n"
                            f"1. Excel에서 I4 파일을 열어서 확인하세요.\n"
                            f"2. I4 파일도 손상되었을 수 있으니 백업 파일을 확인하세요."
                        )
                else:
                    raise Exception(
                        f"엑셀 파일이 손상되었거나 파일 형식이 올바르지 않습니다.\n\n"
                        f"파일: {os.path.basename(input_path)}\n"
                        f"크기: {file_size:,} bytes\n\n"
                        f"해결 방법:\n"
                        f"1. I4 파일을 수동으로 선택하세요.\n"
                        f"2. 파일이 다른 프로그램에서 열려있는지 확인하세요.\n"
                        f"3. 파일을 Excel에서 열어서 다시 저장해보세요.\n"
                        f"4. 파일이 손상되었을 수 있으니 백업 파일을 확인하세요."
                    )
            except Exception as e:
                read_error = e
                # 파일이 사용 중일 수 있으므로 잠시 대기 후 재시도
                self._log(f"⚠️ 엑셀 파일 읽기 실패, 재시도 중... ({str(e)[:50]})")
                time.sleep(1)
                try:
                    if ext == '.xlsx':
                        df = pd.read_excel(input_path, engine='openpyxl')
                    elif ext == '.xls':
                        df = pd.read_excel(input_path, engine='xlrd')
                    else:
                        df = pd.read_excel(input_path, engine='openpyxl')
                except Exception as e2:
                    raise Exception(
                        f"엑셀 파일을 읽을 수 없습니다.\n\n"
                        f"파일: {os.path.basename(input_path)}\n"
                        f"오류: {str(e2)}\n\n"
                        f"해결 방법:\n"
                        f"1. 파일이 다른 프로그램(Excel 등)에서 열려있는지 확인하고 닫으세요.\n"
                        f"2. 파일을 Excel에서 열어서 정상적으로 저장되는지 확인하세요.\n"
                        f"3. 파일이 손상되었을 수 있으니 백업 파일을 확인하세요."
                    )
            
            if df is None or len(df) == 0:
                # I5 파일이 손상되었거나 비어있으면 I4 파일 찾기 시도
                i4_path = self._find_i4_file(input_path)
                if i4_path and os.path.exists(i4_path):
                    self._log(f"⚠️ I5 파일이 손상되어 I4 파일을 사용합니다: {os.path.basename(i4_path)}")
                    debug_log(f"I5 파일 손상/비어있음, I4 파일 사용: {i4_path}", "WARN")
                    input_path = i4_path
                    # I4 파일 다시 읽기
                    _, ext = os.path.splitext(i4_path.lower())
                    if ext == '.xlsx':
                        df = pd.read_excel(i4_path, engine='openpyxl')
                    elif ext == '.xls':
                        df = pd.read_excel(i4_path, engine='xlrd')
                    else:
                        df = pd.read_excel(i4_path, engine='openpyxl')
                    debug_log(f"I4 파일 로드 완료: {len(df)}행, {len(df.columns)}컬럼", "INFO")
                else:
                    raise Exception("엑셀 파일을 읽을 수 없습니다. I4 파일도 찾을 수 없습니다.")
            
            debug_log(f"엑셀 파일 로드 완료: {len(df)}행, {len(df.columns)}컬럼", "INFO")
            
            # 이미 처리된 항목 수 확인
            processed_count = 0
            if "IMG_S4_mix_생성경로" in df.columns:
                for idx, row in df.iterrows():
                    existing_path = str(row.get("IMG_S4_mix_생성경로", "")).strip()
                    if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                        processed_count += 1
            
            if processed_count > 0:
                self._log(f"⚠️ 이미 처리된 항목 {processed_count}건을 감지했습니다. 진행된 작업을 이어서 진행합니다.")
                debug_log(f"이미 처리된 항목: {processed_count}건", "INFO")
            
            if "IMG_S4_mix_생성경로" not in df.columns:
                df["IMG_S4_mix_생성경로"] = ""
            
            # I5 파일이 손상되었거나 없을 경우, I4 파일을 찾아서 사용
            if df is None or len(df) == 0:
                # I4 파일 찾기 시도
                i4_path = self._find_i4_file(input_path)
                if i4_path and os.path.exists(i4_path):
                    self._log(f"⚠️ I5 파일이 손상되어 I4 파일을 사용합니다: {os.path.basename(i4_path)}")
                    debug_log(f"I5 파일 손상, I4 파일 사용: {i4_path}", "WARN")
                    input_path = i4_path
                    # I4 파일 다시 읽기
                    _, ext = os.path.splitext(i4_path.lower())
                    if ext == '.xlsx':
                        df = pd.read_excel(i4_path, engine='openpyxl')
                    elif ext == '.xls':
                        df = pd.read_excel(i4_path, engine='xlrd')
                    else:
                        df = pd.read_excel(i4_path, engine='openpyxl')
                    debug_log(f"I4 파일 로드 완료: {len(df)}행, {len(df.columns)}컬럼", "INFO")

            # 서버 연결 확인
            try:
                host, port_str = server_address.split(":")
                port = int(port_str)
            except:
                raise Exception("서버 주소 형식이 올바르지 않습니다. (예: 127.0.0.1:8188)")

            if not check_server_port(host, port):
                if self.auto_start_server_var.get() and self.comfyui_bat_path_var.get():
                    self._log("[ComfyUI] 서버 자동 시작 시도...")
                    if self._start_comfyui_server():
                        if not self._wait_for_server(host, port, max_wait=60):
                            raise Exception("ComfyUI 서버를 시작했지만 연결할 수 없습니다.")
                    else:
                        # 서버 시작 실패, 잠시 대기 후 다시 확인 (서버가 이미 실행 중일 수 있음)
                        self._log("[경고] 서버 시작 시도 실패. 서버가 이미 실행 중인지 확인합니다...")
                        time.sleep(2)
                        if not check_server_port(host, port):
                            raise Exception(
                                "ComfyUI 서버에 연결할 수 없습니다.\n\n"
                                "해결 방법:\n"
                                "1. ComfyUI 서버가 실행 중인지 확인하세요.\n"
                                "2. 올바른 bat 파일 경로를 설정하세요.\n"
                                "3. 또는 서버를 수동으로 시작한 후 다시 시도하세요."
                            )
                else:
                    raise Exception(
                        "ComfyUI 서버에 연결할 수 없습니다.\n\n"
                        "해결 방법:\n"
                        "1. ComfyUI 서버가 실행 중인지 확인하세요.\n"
                        "2. '서버 자동 시작' 옵션을 활성화하고 bat 파일 경로를 설정하세요.\n"
                        "3. 또는 서버를 수동으로 시작한 후 다시 시도하세요."
                    )

            if self.stop_requested:
                # 메인 런처 현황판 업데이트 (중단)
                try:
                    root_name = get_root_filename(input_path)
                    JobManager.update_status(root_name, img_s4_2_msg="I4-2 (중단)")
                    self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-2 (중단)")
                    debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-2 (중단)", "INFO")
                except Exception as e:
                    debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
                return

            # ComfyUI 클라이언트 초기화
            client = ComfyUIClient(server_address=server_address, log_func=self._log)
            if not client.connect():
                raise Exception("ComfyUI 서버에 연결할 수 없습니다.")

            # ComfyUI 경로 찾기 (복구 전에 경로 확인)
            bat_path = self.comfyui_bat_path_var.get() if self.comfyui_bat_path_var.get() else None
            comfyui_paths = find_comfyui_paths(server_address, workflow_path=workflow_path, bat_path=bat_path, excel_path=input_path, log_func=self._log)
            comfyui_input_dir = comfyui_paths.get("input")
            comfyui_output_dir = comfyui_paths.get("output")
            
            # ComfyUI 경로를 찾은 후, 이전 디버그 로그 및 output 디렉토리에서 처리 완료된 항목 복구 (처리 대상 필터링 전에 실행)
            total_recovered = 0
            
            if comfyui_output_dir and os.path.exists(comfyui_output_dir):
                # output_dir은 log_dir과 동일하게 설정 (디버그 로그 위치)
                log_dir = os.path.dirname(input_path)  # 엑셀 파일이 있는 디렉토리
                
                # 1. 디버그 로그에서 복구
                recovered_from_log = self._recover_from_debug_log(df, log_dir, comfyui_output_dir, input_path)
                if recovered_from_log > 0:
                    self._log(f"✅ 이전 디버그 로그에서 {recovered_from_log}건의 처리 완료 항목을 복구했습니다.")
                    debug_log(f"이전 로그에서 복구된 항목: {recovered_from_log}건", "INFO")
                    total_recovered += recovered_from_log
                
                # 2. Output 디렉토리에서 직접 스캔하여 복구 (디버그 로그가 없어도 복구 가능)
                recovered_from_output = self._recover_from_output_directory(df, comfyui_output_dir, input_path)
                if recovered_from_output > 0:
                    self._log(f"✅ Output 디렉토리 스캔으로 {recovered_from_output}건의 합성 이미지 파일을 복구했습니다.")
                    debug_log(f"Output 디렉토리 스캔으로 복구된 항목: {recovered_from_output}건", "INFO")
                    total_recovered += recovered_from_output
                
                # 복구된 항목 저장 (I5 형식으로 저장)
                if total_recovered > 0:
                    processed_count += total_recovered
                    try:
                        recovery_output_path = get_i5_output_path(input_path)
                        if safe_save_excel(df, recovery_output_path):
                            self._log(f"💾 {total_recovered}건의 복구된 항목을 I5 파일에 저장했습니다: {os.path.basename(recovery_output_path)}")
                            debug_log(f"복구된 항목 저장 완료: {recovery_output_path} (총 {total_recovered}건)", "INFO")
                            # 복구 후 처리된 항목 수 재계산
                            processed_count = 0
                            if "IMG_S4_mix_생성경로" in df.columns:
                                for idx, row in df.iterrows():
                                    existing_path = str(row.get("IMG_S4_mix_생성경로", "")).strip()
                                    if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                                        processed_count += 1
                            if processed_count > 0:
                                self._log(f"⚠️ 복구 후 이미 처리된 항목 {processed_count}건을 감지했습니다.")
                                debug_log(f"복구 후 처리된 항목: {processed_count}건", "INFO")
                        else:
                            self._log(f"⚠️ 복구된 항목 저장 실패 (사용자가 취소)")
                            debug_log(f"복구된 항목 저장 실패 (사용자가 취소)", "WARN")
                    except Exception as e:
                        self._log(f"⚠️ 복구된 항목 저장 실패: {e}")
                        debug_log(f"복구된 항목 저장 실패: {e}", "WARN")

            # 처리할 행 필터링 (복구 후 업데이트된 df 사용)
            items = []
            for idx, row in df.iterrows():
                if self.skip_filled_var.get():
                    existing_path = str(row.get("IMG_S4_mix_생성경로", "")).strip()
                    if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                        stats["skip"] += 1
                        continue

                # IMG_S1_누끼_png (전경)와 IMG_S4_BG_생성경로 (배경) 읽기
                fg_path = str(row.get("IMG_S1_누끼_png", "")).strip()
                if not fg_path or fg_path == "nan":
                    # 폴백: IMG_S1_누끼 컬럼도 시도
                    fg_path = str(row.get("IMG_S1_누끼", "")).strip()
                bg_path = str(row.get("IMG_S4_BG_생성경로", "")).strip()

                if not fg_path or fg_path == "nan" or not os.path.exists(fg_path):
                    stats["skip"] += 1
                    continue
                if not bg_path or bg_path == "nan" or not os.path.exists(bg_path):
                    stats["skip"] += 1
                    continue

                # 상품코드와 원본상품명 추출
                product_code = str(row.get("상품코드", row.get("코드", ""))).strip()
                product_name = str(row.get("원본상품명", row.get("상품명", ""))).strip()
                
                # 미리보기용 누끼 이미지 경로 (IMG_S1_누끼 컬럼)
                nukki_preview_path = str(row.get("IMG_S1_누끼", "")).strip()
                if not nukki_preview_path or nukki_preview_path == "nan":
                    nukki_preview_path = fg_path  # 폴백

                items.append({
                    "idx": idx, 
                    "fg_path": fg_path,  # IMG_S1_누끼_png (워크플로우용)
                    "bg_path": bg_path,  # IMG_S4_BG_생성경로 (워크플로우용)
                    "nukki_preview_path": nukki_preview_path,  # IMG_S1_누끼 (미리보기용)
                    "product_code": product_code if product_code and product_code != "nan" else "",
                    "product_name": product_name if product_name and product_name != "nan" else ""
                })

            stats["total"] = len(items)
            self._log(f"처리 대상: {stats['total']}건 (스킵: {stats['skip']}건)")

            if stats["total"] == 0:
                self._log("=" * 60)
                self._log("⚠️ 처리할 항목이 없어 작업을 종료합니다.")
                self._log(f"   - 전체 행 수: {len(df)}건")
                self._log(f"   - 이미 처리됨: {stats['skip']}건")
                self._log(f"   - 처리할 항목: 0건")
                self._log("=" * 60)
                debug_log("처리할 항목이 없어 작업을 종료합니다.", "WARN")
                
                # 완료 처리를 위해 I5 파일로 저장 (다음 작업에서 사용 가능하도록)
                output_path = get_i5_output_path(input_path)
                self._log(f"완료 처리를 위해 I5 파일로 저장 중... (출력 파일: {os.path.basename(output_path)})")
                debug_log(f"완료 처리: I5 파일로 저장 시작: {output_path}", "INFO")
                if safe_save_excel(df, output_path):
                    self._log(f"✅ I5 파일 저장 완료: {os.path.basename(output_path)}")
                    debug_log(f"I5 파일 저장 완료: {output_path}", "INFO")
                else:
                    self._log("⚠️ I5 파일 저장 실패 (사용자가 취소)")
                    debug_log("I5 파일 저장 실패 (사용자가 취소)", "WARN")
                
                client.disconnect()
                # 메인 런처 현황판 업데이트 (처리할 항목 없음) - img 상태만 I4-2(합성완료)로 업데이트 (text 상태는 변경하지 않음)
                try:
                    root_name = get_root_filename(input_path)
                    JobManager.update_status(root_name, img_s4_2_msg="I4-2(합성완료)")
                    self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-2(합성완료)")
                    debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-2(합성완료)", "INFO")
                except Exception as e:
                    debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
                
                # 사용자에게 명확한 알림 팝업 표시
                def show_completion_message():
                    messagebox.showinfo(
                        "작업 완료",
                        f"처리할 항목이 없어 작업을 종료했습니다.\n\n"
                        f"• 전체 행 수: {len(df)}건\n"
                        f"• 이미 처리됨: {stats['skip']}건\n"
                        f"• 처리할 항목: 0건\n\n"
                        f"모든 항목이 이미 처리되었거나 처리 불가능한 상태입니다.\n\n"
                        f"완료 처리를 위해 I5 파일로 저장되었습니다.",
                        parent=self
                    )
                    self._on_process_complete()
                
                self.after(0, show_completion_message)
                return

            # 배치 처리 설정
            batch_size = max(1, min(10, self.batch_size_var.get()))  # 1-10 사이로 제한
            self._log(f"[배치 처리] 배치 크기: {batch_size}개씩 동시 처리")
            debug_log(f"배치 처리 모드 활성화: 배치 크기={batch_size}", "INFO")
            
            # 배치 처리를 위한 변수
            active_prompts = {}  # {prompt_id: {'item': item, 'unique_prefix': prefix, 'start_time': time, 'comfyui_output_dir': dir}}
            completed_count = 0
            item_index = 0

            # 중간 저장 설정
            auto_save_interval = max(0, self.auto_save_interval_var.get())  # 0이면 비활성화
            last_save_count = 0  # 마지막 중간 저장 시점의 완료 개수
            if auto_save_interval > 0:
                self._log(f"[중간 저장] {auto_save_interval}개마다 엑셀 자동 저장 활성화")
                debug_log(f"중간 저장 기능 활성화: {auto_save_interval}개마다 저장", "INFO")

            # 각 행 처리 (배치 처리 방식)
            while item_index < len(items) or active_prompts:
                if self.stop_requested:
                    self._log("⛔ 사용자 중단 요청으로 작업을 중단합니다.")
                    self._log(f"⛔ 처리 완료된 항목: {stats['success']}건, 실패: {stats['fail']}건")
                    debug_log("사용자 중단 요청으로 작업 중단", "WARN")
                    debug_log(f"중단 시점 처리 현황: 성공={stats['success']}, 실패={stats['fail']}, 스킵={stats['skip']}", "INFO")
                    
                    # 중단 시 즉시 저장 (진행된 항목 보존)
                    self._log("⛔ 중단 전 처리된 데이터 저장 중...")
                    debug_log("중단 시 즉시 저장 시도", "INFO")
                    if safe_save_excel(df, input_path):
                        self._log(f"✅ 중단 전 엑셀 저장 완료: {os.path.basename(input_path)}")
                        debug_log(f"중단 전 엑셀 저장 완료: {input_path}", "INFO")
                    else:
                        self._log("⚠️ 중단 전 엑셀 저장 실패 (사용자가 취소)")
                        debug_log("중단 전 엑셀 저장 실패 (사용자가 취소)", "WARN")
                    
                    # 메인 런처 현황판 업데이트 (중단)
                    try:
                        root_name = get_root_filename(input_path)
                        JobManager.update_status(root_name, img_s4_2_msg="I4-2 (중단)")
                        self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-2 (중단)")
                        debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-2 (중단)", "INFO")
                    except Exception as e:
                        debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
                    break

                # 1단계: 큐에 여유가 있으면 새 항목 제출
                while len(active_prompts) < batch_size and item_index < len(items) and not self.stop_requested:
                    item = items[item_index]
                    item_num = item_index + 1
                    item_start_time = time.time()
                    
                    debug_log(f"[배치] [{item_num}/{stats['total']}] 워크플로우 준비 시작: FG={os.path.basename(item['fg_path'])}, BG={os.path.basename(item['bg_path'])}", "INFO")
                    
                    try:
                        # 워크플로우 복사 및 수정
                        workflow = json.loads(json.dumps(base_workflow))
                        
                        # 노드 찾기
                        fg_load_node = None
                        bg_load_node = None
                        save_node = find_node_by_class_type(workflow, "SaveImage")
                        
                        # LoadImage 노드 찾기 (FG, BG를 _meta.title로 구분)
                        for node_id, node_data in workflow.items():
                            if isinstance(node_data, dict) and node_data.get("class_type") == "LoadImage":
                                meta = node_data.get("_meta", {})
                                title = meta.get("title", "")
                                if "FG" in title or "foreground" in title.lower() or "전경" in title:
                                    fg_load_node = node_id
                                elif "BG" in title or "background" in title.lower() or "배경" in title:
                                    bg_load_node = node_id
                        
                        # _meta.title로 찾지 못한 경우 순서로 판단
                        if not fg_load_node or not bg_load_node:
                            load_nodes = []
                            for node_id, node_data in workflow.items():
                                if isinstance(node_data, dict) and node_data.get("class_type") == "LoadImage":
                                    load_nodes.append(node_id)
                            if len(load_nodes) >= 2:
                                if not fg_load_node:
                                    fg_load_node = load_nodes[0]
                                if not bg_load_node:
                                    bg_load_node = load_nodes[1]
                            elif len(load_nodes) == 1:
                                if not fg_load_node:
                                    fg_load_node = load_nodes[0]
                        
                        if not fg_load_node:
                            raise Exception("전경 이미지 노드(LoadImage FG)를 찾을 수 없습니다.")
                        if not bg_load_node:
                            raise Exception("배경 이미지 노드(LoadImage BG)를 찾을 수 없습니다.")
                        
                        # 유니크 prefix 생성 (상품코드 포함)
                        # 상품코드에서 파일명으로 사용 불가능한 문자 제거
                        safe_product_code = re.sub(r'[\\/*?:"<>|]', '', item.get('product_code', '') or '')
                        safe_product_code = safe_product_code[:30]  # 길이 제한
                        if safe_product_code:
                            unique_prefix = f"comp_{safe_product_code}_row{item['idx']+1}_{int(time.time()*1000)}_"
                        else:
                            unique_prefix = f"comp_row{item['idx']+1}_{int(time.time()*1000)}_"
                        
                        # FG/BG 이미지 복사
                        if not comfyui_input_dir:
                            raise Exception("ComfyUI input 폴더를 찾을 수 없습니다.")
                        
                        import shutil
                        
                        # FG 이미지 복사
                        fg_filename = f"fg_row{item['idx']+1}_{os.path.basename(item['fg_path'])}"
                        comfyui_fg_path = os.path.join(comfyui_input_dir, fg_filename)
                        if not os.path.exists(item['fg_path']):
                            raise Exception(f"전경 이미지 파일이 존재하지 않습니다: {item['fg_path']}")
                        shutil.copy2(item['fg_path'], comfyui_fg_path)
                        workflow[fg_load_node]["inputs"]["image"] = fg_filename
                        
                        # BG 이미지 복사
                        bg_filename = f"bg_row{item['idx']+1}_{os.path.basename(item['bg_path'])}"
                        comfyui_bg_path = os.path.join(comfyui_input_dir, bg_filename)
                        if not os.path.exists(item['bg_path']):
                            raise Exception(f"배경 이미지 파일이 존재하지 않습니다: {item['bg_path']}")
                        shutil.copy2(item['bg_path'], comfyui_bg_path)
                        workflow[bg_load_node]["inputs"]["image"] = bg_filename
                        
                        # SaveImage prefix 설정
                        if save_node:
                            workflow[save_node]["inputs"]["filename_prefix"] = unique_prefix
                        
                        # 워크플로우 제출
                        prompt_id = client.queue_prompt(workflow)
                        if not prompt_id:
                            raise Exception("워크플로우 제출 실패")
                        
                        # 활성 프롬프트에 추가
                        active_prompts[prompt_id] = {
                            'item': item,
                            'item_num': item_num,
                            'unique_prefix': unique_prefix,
                            'start_time': item_start_time,
                            'comfyui_output_dir': comfyui_output_dir,
                        }
                        
                        self._log(f"[배치] [{item_num}/{stats['total']}] 워크플로우 제출 완료: prompt_id={prompt_id} (대기중: {len(active_prompts)}개)")
                        debug_log(f"[배치] [{item_num}/{stats['total']}] 워크플로우 제출: prompt_id={prompt_id}", "INFO")
                        item_index += 1
                        
                    except Exception as e:
                        stats["fail"] += 1
                        item_elapsed = time.time() - item_start_time
                        self._log(f"[배치] [{item_num}/{stats['total']}] ❌ 제출 실패: {e}")
                        debug_log(f"[배치] [{item_num}/{stats['total']}] ❌ 제출 실패: {e} (소요 시간: {item_elapsed:.2f}초)", "ERROR")
                        item_index += 1
                        continue
                
                # 2단계: 완료된 항목 확인 및 처리
                completed_prompt_ids = []
                for prompt_id in list(active_prompts.keys()):
                    try:
                        if client.check_completion(prompt_id):
                            completed_prompt_ids.append(prompt_id)
                    except:
                        pass
                
                # 완료된 항목 처리
                for prompt_id in completed_prompt_ids:
                    prompt_data = active_prompts.pop(prompt_id)
                    item = prompt_data['item']
                    item_num = prompt_data['item_num']
                    unique_prefix = prompt_data['unique_prefix']
                    item_start_time = prompt_data['start_time']
                    comfyui_output_dir = prompt_data['comfyui_output_dir']
                    
                    try:
                        # 완료 데이터 가져오기 (폴백: wait_for_completion 사용)
                        completion_data = client.get_completion_data(prompt_id)
                        if not completion_data:
                            # 폴백: 짧은 타임아웃으로 완료 대기 (중단 체크를 위해 2초로 제한)
                            self._log(f"[배치] [{item_num}/{stats['total']}] 완료 데이터 확인 중...")
                            completion_data = client.wait_for_completion(prompt_id, timeout=2)
                        
                        # 진행률 업데이트
                        completed_count += 1
                        progress = (completed_count / stats["total"]) * 100
                        self.after(0, lambda p=progress: self.pb.config(value=p))
                        self.after(0, lambda p=progress: self.stat_progress.set(f"{p:.1f}%"))
                        self.after(0, lambda c=f"{completed_count}/{stats['total']}": self.stat_count.set(c))
                        
                        elapsed = time.time() - start_time
                        if completed_count > 0 and elapsed > 0:
                            avg_time_per_item = elapsed / completed_count
                            remaining_items = stats["total"] - completed_count
                            estimated_remaining_seconds = avg_time_per_item * remaining_items
                            estimated_end_datetime = datetime.now() + timedelta(seconds=int(estimated_remaining_seconds))
                            estimated_end_str = estimated_end_datetime.strftime("%H:%M:%S")
                            self.after(0, lambda e=estimated_end_str: self.stat_estimated_end.set(e))
                        
                        # 생성된 이미지 가져오기
                        images = client.get_output_images(prompt_id, completion_data=completion_data)
                        mix_image_path = None
                        
                        # 방법 1: API에서 받은 이미지 파일명 사용
                        if images and comfyui_output_dir:
                            for img_filename in images:
                                if img_filename.startswith(unique_prefix):
                                    img_path = os.path.join(comfyui_output_dir, img_filename)
                                    if os.path.exists(img_path):
                                        mix_image_path = img_path
                                        break
                        
                        # 방법 2: output 폴더에서 직접 검색
                        if not mix_image_path and comfyui_output_dir:
                            try:
                                all_files = os.listdir(comfyui_output_dir)
                                matching_files = [f for f in all_files if f.startswith(unique_prefix)]
                                if matching_files:
                                    files_with_time = [(f, os.path.getmtime(os.path.join(comfyui_output_dir, f))) for f in matching_files]
                                    files_with_time.sort(key=lambda x: x[1], reverse=True)
                                    mix_image_path = os.path.join(comfyui_output_dir, files_with_time[0][0])
                            except Exception as e:
                                self._log(f"[배치] output 폴더 검색 오류: {e}")
                        
                        # 결과 처리
                        if mix_image_path and os.path.exists(mix_image_path):
                            df.at[item['idx'], "IMG_S4_mix_생성경로"] = mix_image_path
                            stats["success"] += 1
                            item_elapsed = time.time() - item_start_time
                            self._log(f"[배치] [{item_num}/{stats['total']}] ✅ 완료: {os.path.basename(mix_image_path)} (소요: {item_elapsed:.1f}초)")
                            debug_log(f"[배치] [{item_num}/{stats['total']}] ✅ 처리 완료: {os.path.basename(mix_image_path)} (소요 시간: {item_elapsed:.2f}초)", "INFO")
                            
                            # 완료된 항목 목록에 추가
                            completed_nukki = item.get('nukki_preview_path', item['fg_path'])
                            completed_item = {
                                'nukki_path': completed_nukki,
                                'mix_path': mix_image_path,
                                'code': item.get('product_code', ''),
                                'name': item.get('product_name', ''),
                                'idx': item['idx']
                            }
                            self.completed_items.append(completed_item)
                            
                            # 미리보기 업데이트
                            self._update_preview_images(
                                nukki_path=completed_nukki,
                                mix_path=mix_image_path,
                                product_code=item.get('product_code', ''),
                                product_name=item.get('product_name', '')
                            )
                            self.current_preview_index = len(self.completed_items) - 1
                            self._update_navigation_buttons()
                            
                            self.after(0, lambda s=stats["success"]: self.stat_success.set(str(s)))
                        else:
                            raise Exception(f"생성된 이미지 파일을 찾을 수 없습니다. (prefix: {unique_prefix})")
                        
                        # 완료 추적 데이터 정리
                        client.clear_completion(prompt_id)
                        
                    except Exception as e:
                        stats["fail"] += 1
                        item_elapsed = time.time() - item_start_time
                        self._log(f"[배치] [{item_num}/{stats['total']}] ❌ 실패: {e}")
                        debug_log(f"[배치] [{item_num}/{stats['total']}] ❌ 처리 실패: {e} (소요 시간: {item_elapsed:.2f}초)", "ERROR")
                        import traceback
                        error_trace = traceback.format_exc()
                        self._log(error_trace)
                        debug_log(f"[배치] [{item_num}/{stats['total']}] 오류 상세:\n{error_trace}", "ERROR")
                        self.after(0, lambda f=stats["fail"]: self.stat_fail.set(str(f)))
                        # 완료 추적 데이터 정리
                        client.clear_completion(prompt_id)

                # 중간 저장 체크 (N개마다 자동 저장)
                if auto_save_interval > 0 and completed_count > 0:
                    items_since_last_save = completed_count - last_save_count
                    if items_since_last_save >= auto_save_interval:
                        try:
                            self._log(f"[중간 저장] {completed_count}건 완료, 엑셀 저장 중...")
                            debug_log(f"중간 저장 시작: {completed_count}건 완료 시점", "INFO")
                            if safe_save_excel(df, input_path):
                                last_save_count = completed_count
                                self._log(f"[중간 저장] ✅ 엑셀 저장 완료 ({completed_count}건까지)")
                                debug_log(f"중간 저장 완료: {input_path} ({completed_count}건)", "INFO")
                            else:
                                self._log(f"[중간 저장] ⚠️ 엑셀 저장 실패")
                                debug_log(f"중간 저장 실패 (사용자 취소 또는 파일 잠금)", "WARN")
                        except Exception as save_err:
                            self._log(f"[중간 저장] ⚠️ 저장 중 오류: {save_err}")
                            debug_log(f"중간 저장 오류: {save_err}", "ERROR")

                # 3단계: 완료되지 않은 항목이 있으면 잠시 대기 (CPU 부하 감소)
                if active_prompts and not self.stop_requested:
                    time.sleep(0.1)  # 0.1초 대기 후 다시 확인 (중단 반응 속도 개선)

            # 엑셀 저장 (중단 요청 시에도 처리된 항목까지 저장)
            if self.stop_requested:
                self._log("⛔ 중단 요청으로 작업이 중단되었습니다.")
                self._log(f"⛔ 처리 완료된 항목까지 엑셀에 저장합니다... (성공: {stats['success']}건)")
                debug_log("중단 요청으로 작업 중단, 처리된 항목까지 엑셀 저장 시도", "WARN")
                # 메인 런처 현황판 업데이트 (중단)
                try:
                    root_name = get_root_filename(input_path)
                    JobManager.update_status(root_name, img_s4_2_msg="I4-2 (중단)")
                    self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-2 (중단)")
                    debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-2 (중단)", "INFO")
                except Exception as e:
                    debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
            
            self._log("엑셀 저장 중...")
            debug_log("엑셀 파일 저장 시작", "DEBUG")
            if safe_save_excel(df, input_path):
                if self.stop_requested:
                    self._log("✅ 엑셀 저장 완료 (처리된 항목까지 저장됨)")
                    debug_log(f"엑셀 파일 저장 완료 (중단 요청): {input_path}", "INFO")
                else:
                    self._log("엑셀 저장 완료")
                    debug_log(f"엑셀 파일 저장 완료: {input_path}", "INFO")
            else:
                self._log("엑셀 저장 실패 (사용자가 취소)")
                debug_log("엑셀 파일 저장 실패 (사용자가 취소)", "WARN")

            # ComfyUI 연결 종료
            debug_log("ComfyUI 연결 종료", "DEBUG")
            client.disconnect()

            # 메인 런처 현황판 업데이트 - img 상태만 I4-2(합성완료)로 업데이트 (text 상태는 변경하지 않음)
            try:
                root_name = get_root_filename(input_path)
                JobManager.update_status(root_name, img_s4_2_msg="I4-2(합성완료)")
                self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-2(합성완료)")
                debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-2(합성완료)", "INFO")
            except Exception as e:
                self._log(f"[ERROR] 런처 현황판 업데이트 실패: {e}")
                debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")

            # 완료 메시지
            elapsed_total = time.time() - start_time
            if self.stop_requested:
                self._log(f"=== 작업 중단 ===")
                self._log(f"총 소요 시간: {elapsed_total:.1f}초")
                self._log(f"처리 완료: {stats['success']}건, 실패: {stats['fail']}건, 스킵: {stats['skip']}건")
                self._log(f"남은 항목: {stats['total'] - stats['success'] - stats['fail'] - stats['skip']}건")
                debug_log(f"=== 작업 중단 ===", "WARN")
                debug_log(f"총 소요 시간: {elapsed_total:.1f}초", "INFO")
                debug_log(f"처리 완료: 성공={stats['success']}, 실패={stats['fail']}, 스킵={stats['skip']}", "INFO")
                debug_log(f"디버그 로그 파일: {DEBUG_LOG_FILE}", "INFO")
                
                self.after(0, lambda: messagebox.showinfo(
                    "작업 중단",
                    f"작업이 중단되었습니다.\n\n"
                    f"처리 완료: {stats['success']}건\n"
                    f"실패: {stats['fail']}건\n"
                    f"스킵: {stats['skip']}건\n\n"
                    f"처리된 항목까지 엑셀에 저장되었습니다.\n\n"
                    f"총 소요 시간: {elapsed_total:.1f}초"
                ))
            else:
                self._log(f"=== 작업 완료 ===")
                self._log(f"총 소요 시간: {elapsed_total:.1f}초")
                self._log(f"성공: {stats['success']}건, 실패: {stats['fail']}건, 스킵: {stats['skip']}건")
                debug_log(f"=== 작업 완료 ===", "INFO")
                debug_log(f"총 소요 시간: {elapsed_total:.1f}초", "INFO")
                debug_log(f"성공: {stats['success']}건, 실패: {stats['fail']}건, 스킵: {stats['skip']}건", "INFO")
                debug_log(f"디버그 로그 파일: {DEBUG_LOG_FILE}", "INFO")

                self.after(0, lambda: messagebox.showinfo(
                    "완료",
                    f"작업이 완료되었습니다.\n\n"
                    f"성공: {stats['success']}건\n"
                    f"실패: {stats['fail']}건\n"
                    f"스킵: {stats['skip']}건\n\n"
                    f"총 소요 시간: {elapsed_total:.1f}초"
                ))

        except Exception as e:
            self._log(f"❌ 오류 발생: {e}")
            debug_log(f"작업 중 치명적 오류 발생: {e}", "ERROR")
            import traceback
            error_trace = traceback.format_exc()
            self._log(error_trace)
            debug_log(f"오류 상세:\n{error_trace}", "ERROR")
            # 메인 런처 현황판 업데이트 (오류)
            try:
                if 'input_path' in locals():
                    root_name = get_root_filename(input_path)
                    JobManager.update_status(root_name, img_s4_2_msg="I4-2 (오류)")
                    self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-2 (오류)")
                    debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-2 (오류)", "INFO")
            except Exception as update_error:
                debug_log(f"메인 런처 현황판 업데이트 실패: {update_error}", "ERROR")
            self.after(0, lambda msg=str(e): messagebox.showerror("오류", msg))
        finally:
            debug_log("작업 종료", "INFO")
            if DEBUG_LOG_FILE:
                debug_log(f"디버그 로그 파일 위치: {DEBUG_LOG_FILE}", "INFO")
            self.after(0, self._on_process_complete)

    def _on_process_complete(self):
        self.is_running = False
        self._stop_time_timer()  # 타이머 중지
        self.btn_start.config(state='normal')
        self.btn_stop.config(state='disabled')
        self.status_msg.set("작업이 완료되었습니다.")
        # 예상 종료 시간을 "완료"로 표시
        self.stat_estimated_end.set("완료")

if __name__ == "__main__":
    app = IMGMixingGUI()
    app.mainloop()

