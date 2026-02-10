"""
Bg_Generation.py

Stage 4: 배경 생성 (ComfyUI)
- 기능: I4 엑셀 파일에서 이미지와 프롬프트를 읽어 ComfyUI로 배경 생성
- 입력: I4 파일만 허용
- 출력: 생성된 배경 이미지를 엑셀에 매핑
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
        log_filename = f"bg_generation_debug_{timestamp}.log"
        DEBUG_LOG_FILE = os.path.join(output_root, log_filename)
        
        # 로그 파일 초기화
        with open(DEBUG_LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"=== 배경 생성 디버그 로그 ===\n")
            f.write(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"로그 파일: {log_filename}\n")
            f.write("=" * 50 + "\n\n")
        
        print(f"[INFO] 디버그 로그 파일 생성: {DEBUG_LOG_FILE}")
        debug_log(f"디버그 로그 파일 초기화 완료: {log_filename}", "INFO")
    except Exception as e:
        print(f"[ERROR] 디버그 로그 파일 생성 실패: {e}")
        DEBUG_LOG_FILE = None

try:
    from PIL import Image, ImageTk
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# ========================================================
# 메인 런처 연동용 JobManager & 파일명 유틸
# ========================================================
def get_root_filename(filename: str) -> str:
    """
    파일명에서 버전 정보(_T*_I* 또는 _T*(완)_I*) 및 기타 꼬리표를 떼고 원본명(Key)만 추출
    예: 상품_T3_I4.xlsx -> 상품.xlsx
    예: 상품_T4(완)_I4.xlsx -> 상품.xlsx
    """
    name = os.path.basename(filename)
    base, ext = os.path.splitext(name)

    # 1. 버전 패턴 (_T숫자_I숫자 또는 _T숫자(완)_I숫자) 제거
    base = re.sub(r"_T\d+(\(완\))?_I\d+$", "", base)

    # 2. 기타 구형 꼬리표 제거 (호환성 유지)
    suffixes = ["_bg_generation_done", "_stage1_mapping", "_stage1_img_mapping", "_with_images"]
    for s in suffixes:
        base = base.replace(s, "")

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
    """엑셀 파일이 열려 있어 저장이 안 될 때 재시도를 유도하는 함수"""
    # 엑셀 파일 확장자에 따라 엔진 지정
    _, ext = os.path.splitext(path.lower())
    engine = 'openpyxl' if ext == '.xlsx' else 'xlwt' if ext == '.xls' else 'openpyxl'
    
    while True:
        try:
            df.to_excel(path, index=False, engine=engine)
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


# ========================================================
# ComfyUI API 클라이언트
# ========================================================
def convert_workflow_to_api_format(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """
    ComfyUI 워크플로우 JSON을 API 제출 형식으로 변환합니다.
    nodes 배열을 딕셔너리 형태로 변환하고, class_type 필드를 추가합니다.
    links 정보를 inputs에 반영합니다.
    """
    if "nodes" in workflow:
        # nodes 배열이 있는 경우 (JSON 파일 형식)
        api_workflow = {}
        nodes_list = workflow["nodes"]
        links_list = workflow.get("links", [])
        
        if not isinstance(nodes_list, list):
            error_msg = f"워크플로우 'nodes'가 배열이 아닙니다 (타입: {type(nodes_list)})"
            debug_log(error_msg, "ERROR")
            raise Exception(error_msg)
        
        # 원본 노드 정보 저장 (links 처리 시 outputs 정보 필요)
        original_nodes = {}
        
        # 먼저 모든 노드를 변환
        for idx, node in enumerate(nodes_list):
            if not isinstance(node, dict):
                debug_log(f"경고: 노드[{idx}]가 딕셔너리가 아닙니다 (타입: {type(node)})", "WARN")
                continue
                
            node_id = str(node.get("id", ""))
            if node_id:
                # 원본 노드 정보 저장 (outputs 정보 포함)
                original_nodes[node_id] = node
                
                # ComfyUI API 형식: class_type과 inputs만 필요 (UI 필드 제거)
                api_node = {}
                
                # type을 class_type으로 변환
                if "type" in node:
                    api_node["class_type"] = node["type"]
                elif "class_type" in node:
                    api_node["class_type"] = node["class_type"]
                else:
                    debug_log(f"경고: 노드 {node_id}에 class_type이 없습니다.", "WARN")
                    continue
                
                # inputs 초기화
                api_node["inputs"] = {}
                
                # widgets_values가 있으면 inputs에 반영 (LoadImage 등)
                if "widgets_values" in node and isinstance(node["widgets_values"], list):
                    widgets = node["widgets_values"]
                    # LoadImage의 경우: [filename, image_type]
                    if api_node.get("class_type") == "LoadImage" and len(widgets) >= 1:
                        api_node["inputs"]["image"] = widgets[0]
                        debug_log(f"노드 {node_id}: widgets_values에서 image 설정: {widgets[0]}", "DEBUG")
                
                api_workflow[node_id] = api_node
                debug_log(f"노드 변환: ID={node_id}, class_type={api_node.get('class_type', 'N/A')}", "DEBUG")
        
        # links 정보를 inputs에 반영
        # links 형식: [link_id, source_node_id, source_slot, dest_node_id, dest_slot, type]
        for link in links_list:
            if not isinstance(link, list) or len(link) < 6:
                continue
            
            link_id, src_node_id, src_slot, dest_node_id, dest_slot, link_type = link[0], link[1], link[2], link[3], link[4], link[5]
            dest_node_id_str = str(dest_node_id)
            src_node_id_str = str(src_node_id)
            
            if dest_node_id_str in api_workflow:
                dest_node = api_workflow[dest_node_id_str]
                if "inputs" not in dest_node:
                    dest_node["inputs"] = {}
                
                # 목적지 노드의 입력 이름 찾기 (dest_slot에 해당하는 입력)
                input_name = None
                if dest_node_id_str in original_nodes:
                    dest_node_original = original_nodes[dest_node_id_str]
                    inputs_original = dest_node_original.get("inputs", [])
                    if isinstance(inputs_original, list) and dest_slot < len(inputs_original):
                        input_info = inputs_original[dest_slot]
                        if isinstance(input_info, dict):
                            input_name = input_info.get("name")
                        elif isinstance(input_info, str):
                            input_name = input_info
                    
                    # inputs가 리스트가 아니거나 dest_slot을 찾을 수 없으면, class_type에 따라 기본 입력 이름 사용
                    if not input_name:
                        class_type = dest_node.get("class_type", "")
                        # SaveImage는 "images" 입력을 사용
                        if class_type == "SaveImage":
                            input_name = "images"
                        # 다른 노드들은 일반적으로 출력 타입 이름을 사용 (예: IMAGE, LATENT, CONDITIONING 등)
                        else:
                            # 소스 노드의 출력 타입을 확인
                            if src_node_id_str in original_nodes:
                                src_node_original = original_nodes[src_node_id_str]
                                outputs = src_node_original.get("outputs", [])
                                if isinstance(outputs, list) and src_slot < len(outputs):
                                    output_info = outputs[src_slot]
                                    if isinstance(output_info, dict):
                                        output_type = output_info.get("type", "")
                                        # 타입을 입력 이름으로 변환 (예: IMAGE -> IMAGE, LATENT -> samples 등)
                                        if output_type == "IMAGE":
                                            input_name = "IMAGE"
                                        elif output_type == "LATENT":
                                            input_name = "samples"
                                        elif output_type == "CONDITIONING":
                                            input_name = "conditioning"
                                        elif output_type == "MODEL":
                                            input_name = "model"
                                        elif output_type == "VAE":
                                            input_name = "vae"
                                        elif output_type == "CLIP":
                                            input_name = "clip"
                                        elif output_type == "IPADAPTER":
                                            input_name = "ipadapter"
                                        elif output_type == "CLIP_VISION":
                                            input_name = "clip_vision"
                                        else:
                                            # 기본값: 타입 이름을 소문자로 변환
                                            input_name = output_type.lower() if output_type else "input"
                
                if input_name:
                    # inputs에 연결 정보 추가: [source_node_id, source_slot]
                    dest_node["inputs"][input_name] = [src_node_id_str, src_slot]
                    debug_log(f"링크 추가: 노드 {dest_node_id_str}.inputs['{input_name}'] = [{src_node_id_str}, {src_slot}]", "DEBUG")
                else:
                    debug_log(f"경고: 노드 {dest_node_id_str}의 입력 이름을 찾을 수 없습니다 (dest_slot={dest_slot})", "WARN")
        
        debug_log(f"워크플로우 변환 완료: {len(api_workflow)}개 노드 (nodes 배열 -> 딕셔너리)", "DEBUG")
        debug_log(f"변환된 노드 ID 목록: {list(api_workflow.keys())}", "DEBUG")
        return api_workflow
    else:
        # 이미 딕셔너리 형태인 경우 (이미 변환됨)
        debug_log("워크플로우가 이미 API 형식입니다 (변환 불필요)", "DEBUG")
        if isinstance(workflow, dict):
            debug_log(f"기존 워크플로우 노드 ID 목록: {list(workflow.keys())[:10]}...", "DEBUG")
        return workflow


def find_node_by_class_type(workflow: Dict[str, Any], class_type: str) -> Optional[str]:
    """
    워크플로우에서 클래스 타입으로 노드 ID를 찾습니다.
    예: find_node_by_class_type(workflow, "LoadImage") -> "8"
    """
    for node_id, node_data in workflow.items():
        if isinstance(node_data, dict) and node_data.get("class_type") == class_type:
            return node_id
    return None


def check_server_port(host: str, port: int, timeout: float = 2.0) -> bool:
    """
    서버 포트가 열려있는지 확인합니다.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def find_comfyui_paths(server_address: str, workflow_path: Optional[str] = None, bat_path: Optional[str] = None, log_func=None) -> Dict[str, Optional[str]]:
    """
    ComfyUI 서버에서 input/output 폴더 경로를 조회합니다.
    """
    def log(msg):
        if log_func:
            log_func(msg)
    
    paths = {"input": None, "output": None}
    
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
        """로그 출력"""
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
class BGGenerationGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Stage 4: 배경 생성 (ComfyUI)")
        self.geometry("1000x850")
        
        # 스타일 설정
        self._configure_styles()

        # --- 변수 초기화 ---
        self.input_file_path = tk.StringVar()
        self.output_file_path = ""
        self.comfyui_server_var = tk.StringVar(value="127.0.0.1:8188")
        self.workflow_path_var = tk.StringVar()
        self.comfyui_bat_path_var = tk.StringVar()  # ComfyUI 실행 bat 파일 경로

        # 옵션 변수
        self.skip_filled_var = tk.BooleanVar(value=True)
        self.auto_start_server_var = tk.BooleanVar(value=True)  # 서버 자동 시작 옵션
        self.batch_size_var = tk.IntVar(value=1)  # 배치 크기 (기본값: 1개, 메모리 부족 시 1개 권장)

        # 상태 및 통계 변수
        self.is_running = False
        self.stop_requested = False
        self.time_timer_id = None  # 경과 시간 타이머 ID
        
        self.stat_progress = tk.StringVar(value="0.0%")
        self.stat_count = tk.StringVar(value="0 / 0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_fail = tk.StringVar(value="0")
        self.stat_time = tk.StringVar(value="00:00:00")
        self.stat_start_time = tk.StringVar(value="-")
        self.stat_estimated_end = tk.StringVar(value="-")
        self.status_msg = tk.StringVar(value="파일을 선택하고 작업을 시작하세요.")
        
        # 경과 시간 계산용 변수
        self.process_start_time = None
        
        # 이미지 미리보기 관련 변수
        self.preview_window = None
        self.current_nukki_image_path = None
        self.current_bg_image_path = None
        self.current_product_code = None
        self.current_product_name = None
        self.preview_nukki_photo = None
        self.preview_bg_photo = None
        
        # 완료된 항목 목록 (이전/다음 이동용)
        self.completed_items = []  # [{"nukki_path": ..., "bg_path": ..., "code": ..., "name": ..., "idx": ...}, ...]
        self.current_preview_index = -1  # 현재 미리보기 중인 항목 인덱스

        # UI 구성
        self._init_ui()
        
        # 설정 파일 경로
        self.config_file = os.path.join(os.path.dirname(__file__), "bg_generation_config.json")
        
        # 저장된 설정 로드
        self._load_config()
        
        # 워크플로우 JSON 경로 설정 (기본값)
        if not self.workflow_path_var.get():
            default_workflow = os.path.join(os.path.dirname(__file__), "배경생성 _경량ver.1.json")
            if os.path.exists(default_workflow):
                self.workflow_path_var.set(default_workflow)
            else:
                # 대체 경로 시도
                alt_workflow = os.path.join(os.path.dirname(__file__), "배경생성_251209ver.json")
                if os.path.exists(alt_workflow):
                    self.workflow_path_var.set(alt_workflow)

    def _configure_styles(self):
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

        # 1. 상단: ComfyUI 서버 설정
        frame_top = ttk.LabelFrame(main_frame, text="ComfyUI 서버 설정", padding=15)
        frame_top.pack(fill='x', pady=(0, 10))

        r1 = ttk.Frame(frame_top)
        r1.pack(fill='x', pady=2)
        ttk.Label(r1, text="서버 주소:", width=12).pack(side='left')
        ttk.Entry(r1, textvariable=self.comfyui_server_var, width=30).pack(side='left', padx=5)
        ttk.Label(r1, text="(예: 127.0.0.1:8188)").pack(side='left', padx=5)
        
        r2 = ttk.Frame(frame_top)
        r2.pack(fill='x', pady=5)
        ttk.Label(r2, text="워크플로우 JSON:", width=12).pack(side='left')
        ttk.Entry(r2, textvariable=self.workflow_path_var, width=50).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(r2, text="📂 찾기", command=self._select_workflow).pack(side='right')
        
        r3 = ttk.Frame(frame_top)
        r3.pack(fill='x', pady=5)
        ttk.Label(r3, text="ComfyUI bat 파일:", width=12).pack(side='left')
        ttk.Entry(r3, textvariable=self.comfyui_bat_path_var, width=50).pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(r3, text="📂 찾기", command=self._select_bat_file).pack(side='right')
        
        r4 = ttk.Frame(frame_top)
        r4.pack(fill='x', pady=5)
        ttk.Checkbutton(
            r4,
            text="서버가 꺼져있으면 자동으로 실행 (bat 파일 필요)",
            variable=self.auto_start_server_var
        ).pack(side='left')
        ttk.Button(r4, text="🔍 서버 연결 확인", command=self._check_server_connection).pack(side='right', padx=5)

        # 2. 파일 선택 & 옵션
        frame_file = ttk.LabelFrame(main_frame, text="작업 대상 파일 (I4)", padding=15)
        frame_file.pack(fill='x', pady=(0, 10))
        
        rf = ttk.Frame(frame_file)
        rf.pack(fill='x')
        ttk.Entry(rf, textvariable=self.input_file_path).pack(side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(rf, text="📂 파일 선택", command=self._select_file).pack(side='right')
        
        # 건너뛰기 체크박스
        ttk.Checkbutton(
            frame_file, 
            text="이미 결과(IMG_S4_BG_생성경로)가 있는 행은 건너뛰기", 
            variable=self.skip_filled_var
        ).pack(anchor='w', pady=(5,0))
        
        # 배치 크기 설정
        batch_frame = ttk.Frame(frame_file)
        batch_frame.pack(anchor='w', pady=(5,0))
        ttk.Label(batch_frame, text="배치 크기:").pack(side='left', padx=(0,5))
        batch_spinbox = ttk.Spinbox(batch_frame, from_=1, to=10, width=5, textvariable=self.batch_size_var)
        batch_spinbox.pack(side='left', padx=(0,5))
        ttk.Label(batch_frame, text="개씩 동시 처리 (메모리 부족 시 1개 권장)", font=("맑은 고딕", 9), foreground="#666").pack(side='left')

        # 3. 대시보드 (Dashboard)
        dash_frame = ttk.LabelFrame(main_frame, text="실시간 현황 (Dashboard)", padding=15)
        dash_frame.pack(fill='x', pady=(0, 10))

        # 1행: 진행률
        d1 = ttk.Frame(dash_frame)
        d1.pack(fill='x', pady=5)
        ttk.Label(d1, text="진행률:", style="Header.TLabel", width=10).pack(side='left')
        self.pb = ttk.Progressbar(d1, maximum=100, mode='determinate')
        self.pb.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Label(d1, textvariable=self.stat_progress, style="Stat.TLabel", width=8).pack(side='right')

        # 2행: 통계
        d2 = ttk.Frame(dash_frame)
        d2.pack(fill='x', pady=5)
        ttk.Label(d2, text="처리 건수:", width=10).pack(side='left')
        ttk.Label(d2, textvariable=self.stat_count, width=15, foreground="blue", font=("맑은 고딕", 10, "bold")).pack(side='left')
        
        ttk.Label(d2, text="성공/실패:", width=10).pack(side='left')
        lbl_succ = ttk.Label(d2, textvariable=self.stat_success, foreground="green", font=("맑은 고딕", 10, "bold"))
        lbl_succ.pack(side='left')
        ttk.Label(d2, text=" / ").pack(side='left')
        lbl_fail = ttk.Label(d2, textvariable=self.stat_fail, foreground="red", font=("맑은 고딕", 10, "bold"))
        lbl_fail.pack(side='left')

        # 3행: 시간 정보
        d3 = ttk.Frame(dash_frame)
        d3.pack(fill='x', pady=5)
        ttk.Label(d3, text="경과 시간:", width=10).pack(side='left')
        ttk.Label(d3, textvariable=self.stat_time, width=12).pack(side='left')
        
        ttk.Label(d3, text="시작 시간:", width=10).pack(side='left', padx=(20, 0))
        ttk.Label(d3, textvariable=self.stat_start_time, width=12, font=("맑은 고딕", 9)).pack(side='left')
        
        # 4행: 예상 종료 시간
        d4 = ttk.Frame(dash_frame)
        d4.pack(fill='x', pady=5)
        ttk.Label(d4, text="예상 종료:", width=10).pack(side='left')
        ttk.Label(d4, textvariable=self.stat_estimated_end, width=20, font=("맑은 고딕", 9, "bold"), foreground="#007bff").pack(side='left')

        # 4. 액션 버튼
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill='x', pady=(0, 10))
        
        self.btn_start = ttk.Button(btn_frame, text="▶ 작업 시작", style="Action.TButton", command=self._start_thread)
        self.btn_start.pack(side='left', fill='x', expand=True, padx=(0, 5))
        
        self.btn_stop = ttk.Button(btn_frame, text="⏹ 중단 (Safe Stop)", style="Stop.TButton", command=self._request_stop, state='disabled')
        self.btn_stop.pack(side='right', fill='x', expand=True, padx=(5, 0))

        ttk.Label(main_frame, textvariable=self.status_msg, foreground="#555", anchor='center').pack(fill='x', pady=(0, 5))

        # 5. 이미지 미리보기 버튼
        preview_btn_frame = ttk.Frame(main_frame)
        preview_btn_frame.pack(fill='x', pady=(0, 10))
        self.btn_preview = ttk.Button(preview_btn_frame, text="🖼️ 이미지 미리보기", command=self._show_preview_window)
        self.btn_preview.pack(side='left', padx=5)

        # 6. 로그창
        self.log_frame = ttk.LabelFrame(main_frame, text="상세 로그", padding=10)
        self.log_frame.pack(fill='both', expand=True)
        self.log_widget = ScrolledText(self.log_frame, height=10, state='disabled', font=("Consolas", 9))
        self.log_widget.pack(fill='both', expand=True)

    def _show_preview_window(self):
        """이미지 미리보기 팝업 창 표시"""
        if self.preview_window is None or not self.preview_window.winfo_exists():
            # 새 팝업 창 생성
            self.preview_window = tk.Toplevel(self)
            self.preview_window.title("🖼️ 이미지 미리보기")
            # 가로:세로 = 2:1 비율 (예: 800x500) - 네비게이션 버튼이 보이도록 세로 크기 증가
            self.preview_window.geometry("800x500")
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
            left_frame = ttk.LabelFrame(preview_content, text="누끼 이미지", padding=5)
            left_frame.pack(side='left', fill='both', expand=True, padx=(0, 5))
            
            self.preview_nukki_label = ttk.Label(left_frame, text="이미지 없음", anchor='center')
            self.preview_nukki_label.pack(fill='both', expand=True)
            
            # 오른쪽: 생성된 배경 이미지
            right_frame = ttk.LabelFrame(preview_content, text="생성된 배경", padding=5)
            right_frame.pack(side='right', fill='both', expand=True, padx=(5, 0))
            
            self.preview_bg_label = ttk.Label(right_frame, text="이미지 없음", anchor='center')
            self.preview_bg_label.pack(fill='both', expand=True)
            
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
            
            # 창 크기 변경 이벤트 바인딩 (이미지 크기 조정)
            self.preview_window.bind('<Configure>', lambda e: self._on_preview_window_resize())
            
            # 창이 완전히 렌더링된 후 네비게이션 버튼 상태 업데이트
            self.preview_window.after(100, self._update_navigation_buttons)
            
            # 현재 이미지가 있으면 표시
            if self.current_nukki_image_path or self.current_bg_image_path:
                # 창이 완전히 렌더링된 후 이미지 표시
                self.preview_window.after(100, lambda: self._update_preview_images(
                    nukki_path=self.current_nukki_image_path,
                    bg_path=self.current_bg_image_path,
                    product_code=self.current_product_code,
                    product_name=self.current_product_name
                ))
        else:
            # 이미 열려있으면 포커스 및 네비게이션 버튼 업데이트
            self.preview_window.lift()
            self.preview_window.focus()
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
            if self.current_nukki_image_path or self.current_bg_image_path:
                self._update_preview_images(
                    nukki_path=self.current_nukki_image_path,
                    bg_path=self.current_bg_image_path,
                    product_code=self.current_product_code,
                    product_name=self.current_product_name
                )
    
    def _update_preview_images(self, nukki_path: Optional[str] = None, bg_path: Optional[str] = None, 
                              product_code: Optional[str] = None, product_name: Optional[str] = None):
        """미리보기 이미지 업데이트"""
        if not PIL_AVAILABLE:
            return
        
        # 현재 이미지 경로 및 상품 정보 저장
        if nukki_path:
            self.current_nukki_image_path = nukki_path
        if bg_path:
            self.current_bg_image_path = bg_path
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
                
                # 배경 이미지 업데이트
                if bg_path and os.path.exists(bg_path):
                    img = Image.open(bg_path)
                    # 팝업 창 크기에 맞춰 리사이즈 (1:1 비율 유지)
                    window_width = self.preview_window.winfo_width()
                    window_height = self.preview_window.winfo_height()
                    if window_width > 1 and window_height > 1:
                        max_size = min(window_width // 2 - 50, window_height - 100)
                    else:
                        max_size = 350  # 기본값
                    img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
                    self.preview_bg_photo = ImageTk.PhotoImage(img)
                    self.preview_bg_label.config(image=self.preview_bg_photo, text="")
                elif bg_path:
                    self.preview_bg_label.config(image="", text=f"파일 없음:\n{os.path.basename(bg_path)}")
                else:
                    self.preview_bg_label.config(image="", text="이미지 없음")
                
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
                bg_path=item['bg_path'],
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
                bg_path=item['bg_path'],
                product_code=item.get('code', ''),
                product_name=item.get('name', '')
            )
            self._update_navigation_buttons()

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
    
    def _select_bat_file(self):
        """ComfyUI 실행 bat 파일 선택"""
        p = filedialog.askopenfilename(
            filetypes=[("Batch Files", "*.bat"), ("All Files", "*.*")],
            title="ComfyUI 실행 bat 파일 선택"
        )
        if p:
            self.comfyui_bat_path_var.set(p)
            self._log(f"ComfyUI bat 파일 선택: {os.path.basename(p)}")
            self._save_config()  # 설정 저장
            # --lowvram 옵션 확인 및 안내
            self._check_lowvram_option(p)
    
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
    
    def _start_comfyui_server(self) -> bool:
        """ComfyUI 서버를 bat 파일로 시작"""
        bat_path = self.comfyui_bat_path_var.get()
        
        if not bat_path:
            self._log("[경고] ComfyUI bat 파일 경로가 설정되지 않았습니다.")
            return False
        
        # bat 파일이 존재하면 그대로 사용
        if os.path.exists(bat_path):
            try:
                # 절대 경로로 변환
                bat_path = os.path.abspath(bat_path)
                bat_dir = os.path.dirname(bat_path)
                bat_filename = os.path.basename(bat_path)
                
                self._log(f"[ComfyUI] 서버 시작 시도: {bat_filename}")
                self._log(f"[ComfyUI] 작업 디렉토리: {bat_dir}")
                
                # 새 창에서 bat 파일 실행 (서버가 계속 실행되어야 하므로)
                # Windows에서는 cmd.exe를 통해 실행하여 더 안정적으로 동작
                if os.name == 'nt':
                    # cmd.exe를 통해 bat 파일 실행 (작업 디렉토리 설정)
                    process = subprocess.Popen(
                        ['cmd.exe', '/c', 'start', '/D', bat_dir, bat_filename],
                        shell=False,
                        cwd=bat_dir,
                        creationflags=subprocess.CREATE_NEW_CONSOLE
                    )
                else:
                    # Linux/Mac에서는 직접 실행
                    process = subprocess.Popen(
                        [bat_path],
                        shell=True,
                        cwd=bat_dir
                    )
                
                self._log(f"[ComfyUI] 서버 시작 명령 실행됨 (PID: {process.pid if process else 'N/A'})")
                self._log("[ComfyUI] 서버가 준비될 때까지 대기 중...")
                return True
            except Exception as e:
                self._log(f"[ComfyUI] 서버 시작 실패: {e}")
                import traceback
                self._log(f"[ComfyUI] 상세 오류: {traceback.format_exc()}")
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
                    self._log(f"[ComfyUI] 서버 시작 시도 (추론된 bat 파일: {bat_name})")
                    if os.name == 'nt':
                        process = subprocess.Popen(
                            ['cmd.exe', '/c', 'start', '/D', bat_dir, bat_name],
                            shell=False,
                            cwd=bat_dir,
                            creationflags=subprocess.CREATE_NEW_CONSOLE
                        )
                    else:
                        process = subprocess.Popen(
                            [test_bat_path],
                            shell=True,
                            cwd=bat_dir
                        )
                    self._log(f"[ComfyUI] 서버 시작 명령 실행됨 (추론된 bat 파일: {bat_name}, PID: {process.pid if process else 'N/A'})")
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
                    if os.name == 'nt':
                        process = subprocess.Popen(
                            ['python', 'main.py', '--lowvram'],
                            cwd=comfyui_dir,
                            shell=True,
                            creationflags=subprocess.CREATE_NEW_CONSOLE
                        )
                    else:
                        process = subprocess.Popen(
                            ['python', 'main.py', '--lowvram'],
                            cwd=comfyui_dir,
                            shell=True
                        )
                    self._log(f"[ComfyUI] 서버 시작 시도 (Python 직접 실행, PID: {process.pid if process else 'N/A'})")
                    return True
                except Exception as e:
                    self._log(f"[ComfyUI] Python 직접 실행 실패: {e}")
                    continue
        
        self._log(f"[ComfyUI] 서버 시작 실패: bat 파일을 찾을 수 없습니다.")
        return False

    def _recover_from_debug_log(self, df, log_dir, comfyui_output_dir, excel_path):
        """
        이전 디버그 로그 파일에서 처리 완료된 항목을 복구합니다.
        comfyui_output_dir이 None이면 로그만 파싱하고 파일 찾기는 나중에 수행합니다.
        Returns: 복구된 항목 수
        """
        recovered_count = 0

        try:
            # 현재 디렉토리에서 이전 디버그 로그 파일 찾기
            log_files = []
            if os.path.exists(log_dir):
                for f in os.listdir(log_dir):
                    if f.startswith("bg_generation_debug_") and f.endswith(".log"):
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
                            # [배치] [N/M] ✅ 처리 완료: BG_{상품코드}_row{N}_...png 또는 BG_row{N}_...png 패턴 찾기
                            match = re.search(r'\[배치\]\s*\[\d+/\d+\]\s*✅\s*처리\s*완료:\s*(BG_(?:[^_]+_)?row(\d+)_[^\s]+\.png)', line)
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

            # ComfyUI output 디렉토리가 제공되지 않은 경우, 나중에 다시 시도하도록 0 반환
            if not comfyui_output_dir:
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
                existing_path = str(df.at[row_idx, "IMG_S4_BG_생성경로"]).strip()
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

                        df.at[row_idx, "IMG_S4_BG_생성경로"] = file_path
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
                        prefix_new = f"BG_{safe_code}_row{row_num}_"
                        matching_files = [f for f in output_files if f.startswith(prefix_new)]
                    else:
                        prefix_old = f"BG_row{row_num}_"
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
                                df.at[row_idx, "IMG_S4_BG_생성경로"] = file_path
                                recovered_count += 1
                                # 상품코드 정보도 로그에 추가 (검증용)
                                product_code = str(df.at[row_idx, "상품코드"]).strip() if "상품코드" in df.columns else "N/A"
                                debug_log(f"복구 (로그, 부분매칭): row {row_idx+1} (상품코드: {product_code}) -> {files_with_time[0][0]}", "INFO")

        except Exception as e:
            self._log(f"⚠️ 이전 로그 복구 중 오류: {e}")
            debug_log(f"이전 로그 복구 오류: {e}", "ERROR")
            import traceback
            debug_log(traceback.format_exc(), "ERROR")

        return recovered_count

    def _recover_bg_from_output_directory(self, df, comfyui_output_dir, excel_path):
        """
        ComfyUI output 디렉토리에서 직접 배경 이미지 파일을 스캔하여 엑셀에 복구합니다.
        파일명 패턴: BG_row{N}_...png
        excel_path가 제공되면 파일 생성 시간과 엑셀 수정 시간을 비교하여 검증합니다.
        Returns: 복구된 항목 수
        """
        recovered_count = 0
        
        try:
            if not comfyui_output_dir or not os.path.exists(comfyui_output_dir):
                return 0
            
            self._log(f"📂 Output 디렉토리에서 배경 이미지 파일 스캔 중: {comfyui_output_dir}")
            debug_log(f"Output 디렉토리 스캔 시작: {comfyui_output_dir}", "INFO")
            
            # 엑셀 파일 수정 시간 가져오기 (검증용)
            excel_mtime = None
            if excel_path and os.path.exists(excel_path):
                excel_mtime = os.path.getmtime(excel_path)
                # 엑셀 파일 수정 시간 기준으로 7일 이내 생성된 파일만 복구 (재부팅 후에도 복구 가능하도록 확장)
                time_window = 7 * 24 * 60 * 60  # 7일 (초)
            
            # output 디렉토리의 모든 파일 목록
            try:
                output_files = os.listdir(comfyui_output_dir)
            except Exception as e:
                self._log(f"⚠️ output 디렉토리 읽기 실패: {e}")
                return 0
            
            # BG_row{N}_ 또는 BG_{상품코드}_row{N}_ 패턴으로 시작하는 파일 찾기
            bg_files = {}
            for filename in output_files:
                if filename.endswith('.png') and filename.startswith('BG_'):
                    # BG_{상품코드}_row{N}_ 또는 BG_row{N}_ 패턴 추출
                    # 새 패턴: BG_{상품코드}_row{N}_
                    match_new = re.match(r'BG_([^_]+)_row(\d+)_', filename)
                    # 기존 패턴: BG_row{N}_
                    match_old = re.match(r'BG_row(\d+)_', filename)

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

                    # 엑셀 파일 수정 시간과 비교하여 검증 (24시간 이내 파일만)
                    if excel_mtime:
                        time_diff = abs(file_mtime - excel_mtime)
                        # 파일이 엑셀 수정 시간보다 너무 오래 전이거나 미래면 스킵 (다른 작업 파일일 가능성)
                        if file_mtime < excel_mtime - time_window or file_mtime > excel_mtime + time_window:
                            continue

                    if row_idx not in bg_files:
                        bg_files[row_idx] = []
                    bg_files[row_idx].append({
                        'filename': filename,
                        'path': file_path,
                        'mtime': file_mtime,
                        'product_code': file_product_code  # 상품코드 저장
                    })
            
            if not bg_files:
                self._log(f"📂 Output 디렉토리에서 배경 이미지 파일을 찾을 수 없습니다.")
                return 0
            
            self._log(f"📂 Output 디렉토리에서 {len(bg_files)}건의 배경 이미지 파일을 발견했습니다.")
            debug_log(f"Output 디렉토리에서 발견된 파일: {len(bg_files)}건", "INFO")
            
            # 엑셀에 복구
            for row_idx, files in bg_files.items():
                # 이미 경로가 있으면 스킵
                existing_path = str(df.at[row_idx, "IMG_S4_BG_생성경로"]).strip()
                if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                    continue

                # 엑셀의 해당 row 상품코드 가져오기
                excel_product_code = str(df.at[row_idx, "상품코드"]).strip() if "상품코드" in df.columns else ""
                safe_excel_code = re.sub(r'[\\/*?:"<>|]', '', excel_product_code)[:30] if excel_product_code else ""

                # 상품코드로 필터링: 엑셀 상품코드와 파일 상품코드가 일치하는 것만
                # 엑셀에 상품코드가 있으면 -> 파일도 같은 상품코드여야 함
                # 엑셀에 상품코드가 없으면 -> 파일도 상품코드 없는 기존 패턴만
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
                    df.at[row_idx, "IMG_S4_BG_생성경로"] = file_path
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

    def _wait_for_server(self, host: str, port: int, max_wait: int = 60, check_interval: int = 2) -> bool:
        """서버가 시작될 때까지 대기 (중단 요청 확인 포함)"""
        start_time = time.time()
        while time.time() - start_time < max_wait:
            # 사용자 중단 요청 확인
            if self.stop_requested:
                self._log("[ComfyUI] 사용자 중단 요청으로 서버 대기 중단")
                return False
            
            if check_server_port(host, port):
                self._log(f"[ComfyUI] 서버 연결 성공! (대기 시간: {int(time.time() - start_time)}초)")
                return True
            
            # 중단 요청 확인 후 sleep
            elapsed = int(time.time() - start_time)
            self._log(f"[ComfyUI] 서버 대기 중... ({elapsed}초)")
            
            # check_interval 동안 중단 요청을 확인하면서 대기
            sleep_start = time.time()
            while time.time() - sleep_start < check_interval:
                if self.stop_requested:
                    self._log("[ComfyUI] 사용자 중단 요청으로 서버 대기 중단")
                    return False
                time.sleep(0.5)  # 0.5초마다 중단 요청 확인
        
        self._log(f"[ComfyUI] 서버 시작 타임아웃 ({max_wait}초)")
        return False

    # --- UI 이벤트 핸들러 ---
    def _load_config(self):
        """저장된 설정 로드"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if 'workflow_path' in config and os.path.exists(config['workflow_path']):
                        self.workflow_path_var.set(config['workflow_path'])
                    if 'bat_path' in config and os.path.exists(config['bat_path']):
                        self.comfyui_bat_path_var.set(config['bat_path'])
            except Exception as e:
                self._log(f"[설정] 설정 파일 로드 실패: {e}")
    
    def _save_config(self):
        """현재 설정 저장"""
        try:
            config = {
                'workflow_path': self.workflow_path_var.get(),
                'bat_path': self.comfyui_bat_path_var.get()
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._log(f"[설정] 설정 파일 저장 실패: {e}")
    
    def _select_workflow(self):
        p = filedialog.askopenfilename(
            filetypes=[("JSON Files", "*.json")],
            title="ComfyUI 워크플로우 JSON 선택"
        )
        if p:
            self.workflow_path_var.set(p)
            self._log(f"워크플로우 선택: {os.path.basename(p)}")
            self._save_config()  # 설정 저장

    def _select_file(self):
        p = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx;*.xls")])
        if p:
            base_name = os.path.basename(p)
            i_match = re.search(r"_I(\d+)", base_name)
            
            if i_match:
                i_version = int(i_match.group(1))
                if i_version != 4:
                    # I4만 허용
                    messagebox.showwarning(
                        "파일 버전 오류",
                        f"입력 파일은 I4 단계만 허용됩니다.\n\n"
                        f"선택한 파일: {base_name}\n"
                        f"현재 버전: I{i_version}\n\n"
                        f"I4 배경 프롬프트 생성이 완료된 파일을 선택해주세요."
                    )
                    return
            else:
                # 버전 정보가 없는 파일 거부
                messagebox.showwarning(
                    "파일 버전 오류",
                    f"파일명에 버전 정보(_T*_I*)가 없습니다.\n\n"
                    f"선택한 파일: {base_name}\n\n"
                    f"I4 단계 파일을 선택해주세요."
                )
                return
            
            # 엑셀 파일 검증
            try:
                df_check = pd.read_excel(p)
                
                # 필수 컬럼 체크
                required_cols = ["IMG_S1_누끼", "bg_positive_en", "bg_negative_en"]
                missing_cols = [col for col in required_cols if col not in df_check.columns]
                
                if missing_cols:
                    messagebox.showerror(
                        "필수 컬럼 누락",
                        f"'{base_name}' 파일에 다음 컬럼이 없습니다:\n\n"
                        f"{', '.join(missing_cols)}\n\n"
                        f"I4 배경 프롬프트 생성 작업을 먼저 완료해주세요."
                    )
                    return
                
                # bg_positive_en 또는 bg_negative_en이 비어있는 행 체크
                empty_positive = df_check["bg_positive_en"].isna() | (df_check["bg_positive_en"].astype(str).str.strip() == "")
                empty_negative = df_check["bg_negative_en"].isna() | (df_check["bg_negative_en"].astype(str).str.strip() == "")
                empty_rows = empty_positive | empty_negative
                
                if empty_rows.any():
                    empty_count = empty_rows.sum()
                    if messagebox.askyesno(
                        "경고",
                        f"'{base_name}' 파일에 'bg_positive_en' 또는 'bg_negative_en'이 비어있는 행이 {empty_count}개 있습니다.\n\n"
                        f"이 행들은 처리되지 않습니다.\n\n"
                        f"계속 진행하시겠습니까?"
                    ) == False:
                        return
                
                # 검증 통과
                self.input_file_path.set(p)
                self._log(f"파일 선택됨: {base_name} (I4)")
            except Exception as e:
                messagebox.showerror("파일 읽기 오류", f"엑셀 파일을 읽는 중 오류가 발생했습니다:\n{e}")
                return

    def _log(self, msg: str):
        """로그 출력"""
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_widget.config(state='normal')
        self.log_widget.insert('end', f"[{ts}] {msg}\n")
        self.log_widget.see('end')
        self.log_widget.config(state='disabled')

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
        
        t = threading.Thread(target=self._run_process, daemon=True)
        t.start()

    def _request_stop(self):
        self.stop_requested = True
        self._log("⛔ 중단 요청됨. 현재 작업 완료 후 중단됩니다...")

    def _run_process(self):
        """메인 처리 로직"""
        input_path = self.input_file_path.get()
        workflow_path = self.workflow_path_var.get()
        
        # 메인 런처 현황판 업데이트 (I4-1: 배경 생성 진행중)
        try:
            root_name = get_root_filename(input_path)
            JobManager.update_status(root_name, img_s4_1_msg="I4-1 (진행중)")
            self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-1 (진행중)")
            debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-1 (진행중)", "INFO")
        except Exception as e:
            debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
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
        
        start_time = time.time()
        start_datetime = datetime.now()
        start_time_str = start_datetime.strftime("%H:%M:%S")
        self.after(0, lambda: self.stat_start_time.set(start_time_str))
        
        # 경과 시간 실시간 업데이트를 위한 타이머 시작
        self._start_time_timer(start_time)
        
        stats = {
            "total": 0,
            "success": 0,
            "fail": 0,
            "skip": 0
        }
        
        try:
            # 워크플로우 JSON 로드
            debug_log(f"워크플로우 JSON 로드 시작: {workflow_path}", "DEBUG")
            with open(workflow_path, "r", encoding="utf-8") as f:
                workflow_raw = json.load(f)
            
            # ComfyUI API 형식으로 변환 (nodes 배열 -> 딕셔너리)
            # Bg_Generation.py와 동일하게 처리: 이미 딕셔너리면 그대로 사용, nodes 배열이면 변환
            if isinstance(workflow_raw, dict) and "nodes" not in workflow_raw:
                # 이미 API 형식 (딕셔너리)
                base_workflow = workflow_raw
                debug_log("워크플로우가 이미 API 형식입니다 (변환 불필요)", "DEBUG")
            else:
                # nodes 배열 형식이므로 변환 필요
                base_workflow = convert_workflow_to_api_format(workflow_raw)
            
            # 변환 결과 확인
            if not isinstance(base_workflow, dict):
                error_msg = f"워크플로우 변환 실패: 딕셔너리가 아닙니다 (타입: {type(base_workflow)})"
                self._log(f"❌ {error_msg}")
                debug_log(error_msg, "ERROR")
                raise Exception(error_msg)
            
            self._log(f"워크플로우 로드 완료: {os.path.basename(workflow_path)} ({len(base_workflow)}개 노드)")
            debug_log(f"워크플로우 로드 완료: {len(base_workflow)}개 노드", "INFO")
            debug_log(f"워크플로우 노드 ID 목록: {list(base_workflow.keys())[:10]}...", "DEBUG")
            
            # 엑셀 파일 로드 (중단 후 재시작 시 I5 파일이 있으면 자동으로 사용)
            i5_output_path = get_i5_output_path(input_path)
            actual_input_path = input_path
            
            # I5 파일이 존재하고 원본 I4 파일보다 최신이면 I5 파일 사용
            if os.path.exists(i5_output_path):
                i5_mtime = os.path.getmtime(i5_output_path)
                i4_mtime = os.path.getmtime(input_path) if os.path.exists(input_path) else 0
                if i5_mtime > i4_mtime:
                    actual_input_path = i5_output_path
                    self._log(f"⚠️ 중단 후 저장된 파일을 감지했습니다. 진행된 작업을 이어서 진행합니다.")
                    self._log(f"   원본 파일: {os.path.basename(input_path)}")
                    self._log(f"   이어서 진행: {os.path.basename(i5_output_path)}")
                    debug_log(f"중단 후 저장된 파일 사용: {i5_output_path}", "INFO")
            
            self._log(f"엑셀 로드 중... {os.path.basename(actual_input_path)}")
            debug_log(f"엑셀 파일 로드 시작: {actual_input_path}", "DEBUG")
            
            # 엑셀 파일 확장자에 따라 엔진 지정
            _, ext = os.path.splitext(actual_input_path.lower())
            try:
                if ext == '.xlsx':
                    df = pd.read_excel(actual_input_path, engine='openpyxl')
                elif ext == '.xls':
                    df = pd.read_excel(actual_input_path, engine='xlrd')
                else:
                    df = pd.read_excel(actual_input_path, engine='openpyxl')
            except (zipfile.BadZipFile, Exception) as e:
                # I5 파일이 손상된 경우 원본 I4 파일로 폴백
                if actual_input_path != input_path and actual_input_path == i5_output_path:
                    self._log(f"⚠️ I5 파일이 손상되어 읽을 수 없습니다: {os.path.basename(actual_input_path)}")
                    self._log(f"   원본 I4 파일로 폴백합니다: {os.path.basename(input_path)}")
                    debug_log(f"I5 파일 손상으로 인한 폴백: {actual_input_path} -> {input_path}", "WARN")
                    actual_input_path = input_path
                    # 원본 파일로 다시 시도
                    _, ext = os.path.splitext(actual_input_path.lower())
                    if ext == '.xlsx':
                        df = pd.read_excel(actual_input_path, engine='openpyxl')
                    elif ext == '.xls':
                        df = pd.read_excel(actual_input_path, engine='xlrd')
                    else:
                        df = pd.read_excel(actual_input_path, engine='openpyxl')
                else:
                    # 원본 파일도 읽을 수 없으면 예외를 다시 발생
                    raise
            
            debug_log(f"엑셀 파일 로드 완료: {len(df)}행, {len(df.columns)}컬럼", "INFO")
            
            # 실제 사용할 입력 경로 업데이트 (저장 시에도 동일한 파일에 저장하도록)
            input_path = actual_input_path
            
            # 결과 컬럼 추가
            if "IMG_S4_BG_생성경로" not in df.columns:
                df["IMG_S4_BG_생성경로"] = ""
            
            # 이미 처리된 항목 수 확인
            processed_count = 0
            if "IMG_S4_BG_생성경로" in df.columns:
                for idx, row in df.iterrows():
                    existing_path = str(row.get("IMG_S4_BG_생성경로", "")).strip()
                    if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                        processed_count += 1
            
            if processed_count > 0:
                self._log(f"⚠️ 이미 처리된 항목 {processed_count}건을 감지했습니다.")
                debug_log(f"이미 처리된 항목: {processed_count}건", "INFO")
            
            # 실제 사용할 입력 경로 업데이트 (저장 시에도 동일한 파일에 저장하도록)
            input_path = actual_input_path
            
            # 서버 연결 확인 및 자동 시작
            try:
                host, port_str = server_address.split(":")
                port = int(port_str)
            except:
                raise Exception("서버 주소 형식이 올바르지 않습니다. (예: 127.0.0.1:8188)")
            
            # 서버 연결 확인
            if not check_server_port(host, port):
                self._log("[ComfyUI] 서버가 실행되지 않았습니다.")
                
                # 사용자 중단 요청 확인
                if self.stop_requested:
                    self._log("[ComfyUI] 사용자 중단 요청으로 작업 중단")
                    return
                
                # 서버가 꺼져있으면 자동으로 서버 자동 시작 옵션 활성화
                if not self.auto_start_server_var.get() and self.comfyui_bat_path_var.get():
                    self.auto_start_server_var.set(True)
                    self._log("[ComfyUI] 서버가 꺼져있어 '서버 자동 시작' 옵션을 자동으로 활성화했습니다.")
                
                # 자동 시작 옵션이 활성화되어 있고 bat 파일이 설정되어 있으면 실행
                if self.auto_start_server_var.get() and self.comfyui_bat_path_var.get():
                    self._log("[ComfyUI] 서버 자동 시작 시도...")
                    if self._start_comfyui_server():
                        # 사용자 중단 요청 확인
                        if self.stop_requested:
                            self._log("[ComfyUI] 사용자 중단 요청으로 작업 중단")
                            return
                        
                        # 서버가 시작될 때까지 대기
                        if not self._wait_for_server(host, port, max_wait=60):
                            # 중단 요청으로 인한 실패인지 확인
                            if self.stop_requested:
                                self._log("[ComfyUI] 사용자 중단 요청으로 작업 중단")
                                return
                            
                            raise Exception(
                                "ComfyUI 서버를 시작했지만 연결할 수 없습니다.\n"
                                "서버가 정상적으로 시작되었는지 확인하세요.\n"
                                "수동으로 bat 파일을 실행한 후 다시 시도해주세요."
                            )
                    else:
                        # 서버 시작 실패, 잠시 대기 후 다시 확인 (서버가 이미 실행 중일 수 있음)
                        self._log("[경고] 서버 시작 시도 실패. 서버가 이미 실행 중인지 확인합니다...")
                        time.sleep(2)
                        if not check_server_port(host, port):
                            raise Exception(
                                "ComfyUI 서버에 연결할 수 없습니다.\n\n"
                                f"설정된 bat 파일 경로: {self.comfyui_bat_path_var.get()}\n\n"
                                "해결 방법:\n"
                                "1. ComfyUI 서버가 실행 중인지 확인하세요.\n"
                                "2. 올바른 bat 파일 경로를 설정하세요.\n"
                                "3. 또는 서버를 수동으로 시작한 후 다시 시도하세요."
                            )
                else:
                    raise Exception(
                        "ComfyUI 서버에 연결할 수 없습니다.\n\n"
                        "해결 방법:\n"
                        "1. ComfyUI bat 파일을 수동으로 실행하세요.\n"
                        "2. 또는 '서버 자동 시작' 옵션을 활성화하고\n"
                        "   ComfyUI bat 파일 경로를 설정한 후 다시 시도하세요."
                    )
            
            # 사용자 중단 요청 확인
            if self.stop_requested:
                self._log("[ComfyUI] 사용자 중단 요청으로 작업 중단")
                return
            
            # ComfyUI 클라이언트 초기화
            client = ComfyUIClient(server_address=server_address, log_func=self._log)
            if not client.connect():
                raise Exception("ComfyUI 서버에 연결할 수 없습니다. 서버가 실행 중인지 확인하세요.")
            
            # ComfyUI 경로 찾기 (복구 전에 경로 확인)
            bat_path = self.comfyui_bat_path_var.get() if hasattr(self, 'comfyui_bat_path_var') and self.comfyui_bat_path_var.get() else None
            comfyui_paths = find_comfyui_paths(server_address, workflow_path=workflow_path, bat_path=bat_path, log_func=self._log)
            comfyui_input_dir = comfyui_paths.get("input")
            comfyui_output_dir = comfyui_paths.get("output")

            # ComfyUI 경로를 찾은 후, 이전 디버그 로그에서 처리 완료된 항목 복구 (처리 대상 필터링 전에 실행)
            log_dir = os.path.dirname(os.path.abspath(input_path))
            if comfyui_output_dir and os.path.exists(comfyui_output_dir):
                recovered_from_log = self._recover_from_debug_log(df, log_dir, comfyui_output_dir, input_path)
                if recovered_from_log > 0:
                    self._log(f"✅ 이전 디버그 로그에서 {recovered_from_log}건의 처리 완료 항목을 복구했습니다.")
                    debug_log(f"이전 로그에서 복구된 항목: {recovered_from_log}건", "INFO")
                    processed_count += recovered_from_log
                    # 복구된 항목 저장 (I5 형식으로 저장)
                    try:
                        recovery_output_path = get_i5_output_path(input_path)
                        if safe_save_excel(df, recovery_output_path):
                            self._log(f"💾 {recovered_from_log}건의 복구된 항목을 I5 파일에 저장했습니다: {os.path.basename(recovery_output_path)}")
                            debug_log(f"복구된 항목 저장 완료 (로그 기반): {recovery_output_path} (총 {recovered_from_log}건)", "INFO")
                        else:
                            self._log(f"⚠️ 복구된 항목 저장 실패 (사용자가 취소)")
                            debug_log(f"복구된 항목 저장 실패 (사용자가 취소)", "WARN")
                        # 복구 후 처리된 항목 수 재계산
                        processed_count = 0
                        if "IMG_S4_BG_생성경로" in df.columns:
                            for idx, row in df.iterrows():
                                existing_path = str(row.get("IMG_S4_BG_생성경로", "")).strip()
                                if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                                    processed_count += 1
                        if processed_count > 0:
                            self._log(f"⚠️ 복구 후 이미 처리된 항목 {processed_count}건을 감지했습니다.")
                            debug_log(f"복구 후 처리된 항목: {processed_count}건", "INFO")
                    except Exception as e:
                        self._log(f"⚠️ 복구된 항목 저장 실패: {e}")
                        debug_log(f"복구된 항목 저장 실패: {e}", "WARN")

            # Output 디렉토리에서 실제 배경 이미지 파일을 찾아서 엑셀에 복구 (처리 대상 필터링 전에 실행)
            if comfyui_output_dir and os.path.exists(comfyui_output_dir):
                recovered_count = self._recover_bg_from_output_directory(df, comfyui_output_dir, input_path)
                if recovered_count > 0:
                    self._log(f"✅ Output 디렉토리에서 {recovered_count}건의 배경 이미지 파일을 복구했습니다.")
                    debug_log(f"Output 디렉토리에서 복구된 항목: {recovered_count}건", "INFO")
                    processed_count += recovered_count
                    # 복구된 항목 저장 (I5 형식으로 저장)
                    try:
                        recovery_output_path = get_i5_output_path(input_path)
                        if safe_save_excel(df, recovery_output_path):
                            self._log(f"💾 {recovered_count}건의 복구된 항목을 I5 파일에 저장했습니다: {os.path.basename(recovery_output_path)}")
                            debug_log(f"복구된 항목 저장 완료: {recovery_output_path} (총 {recovered_count}건)", "INFO")
                        else:
                            self._log(f"⚠️ 복구된 항목 저장 실패 (사용자가 취소)")
                            debug_log(f"복구된 항목 저장 실패 (사용자가 취소)", "WARN")
                        # 복구 후 처리된 항목 수 재계산
                        processed_count = 0
                        if "IMG_S4_BG_생성경로" in df.columns:
                            for idx, row in df.iterrows():
                                existing_path = str(row.get("IMG_S4_BG_생성경로", "")).strip()
                                if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                                    processed_count += 1
                        if processed_count > 0:
                            self._log(f"⚠️ 복구 후 이미 처리된 항목 {processed_count}건을 감지했습니다.")
                            debug_log(f"복구 후 처리된 항목: {processed_count}건", "INFO")
                    except Exception as e:
                        self._log(f"⚠️ 복구된 항목 저장 실패: {e}")
                        debug_log(f"복구된 항목 저장 실패: {e}", "WARN")
            
            # 처리할 행 필터링 (복구 후 업데이트된 df 사용)
            items = []
            for idx, row in df.iterrows():
                # 건너뛰기 체크
                if self.skip_filled_var.get():
                    existing_path = str(row.get("IMG_S4_BG_생성경로", "")).strip()
                    if existing_path and existing_path != "nan" and os.path.exists(existing_path):
                        stats["skip"] += 1
                        continue
                
                # 필수 컬럼 체크
                img_path = str(row.get("IMG_S1_누끼", "")).strip()
                positive_prompt = str(row.get("bg_positive_en", "")).strip()
                negative_prompt = str(row.get("bg_negative_en", "")).strip()
                
                # bg_positive_en 또는 bg_negative_en이 비어있으면 건너뛰기
                if not positive_prompt or positive_prompt == "nan" or not negative_prompt or negative_prompt == "nan":
                    self._log(f"[Row {idx+1}] bg_positive_en 또는 bg_negative_en이 비어있어 건너뜁니다.")
                    stats["skip"] += 1
                    continue
                
                # 이미지 파일 존재 확인
                if not img_path or img_path == "nan" or not os.path.exists(img_path):
                    self._log(f"[Row {idx+1}] 이미지 파일을 찾을 수 없습니다: {img_path}")
                    stats["skip"] += 1
                    continue
                
                # 상품코드와 원본상품명 추출 (컬럼명은 일반적인 패턴 시도)
                product_code = str(row.get("상품코드", row.get("코드", ""))).strip()
                product_name = str(row.get("원본상품명", row.get("상품명", ""))).strip()
                
                items.append({
                    "idx": idx,
                    "img_path": img_path,
                    "positive_prompt": positive_prompt,
                    "negative_prompt": negative_prompt,
                    "product_code": product_code if product_code and product_code != "nan" else "",
                    "product_name": product_name if product_name and product_name != "nan" else ""
                })
            
            stats["total"] = len(items)
            self._log(f"처리 대상: {stats['total']}건 (스킵: {stats['skip']}건)")
            debug_log(f"처리 대상: {stats['total']}건 (스킵: {stats['skip']}건)", "INFO")
            
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
                
                # 메인 런처 현황판 업데이트 (완료)
                try:
                    root_name = get_root_filename(input_path)
                    JobManager.update_status(root_name, img_s4_1_msg="I4-1(완료)")
                    self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-1(완료)")
                    debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-1(완료)", "INFO")
                except Exception as e:
                    debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
                
                client.disconnect()
                
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
            active_prompts = {}  # {prompt_id: {'item': item, 'item_num': item_num, 'unique_prefix': prefix, 'start_time': time, 'comfyui_output_dir': dir}}
            completed_count = 0
            item_index = 0
            
            # 각 행 처리 (배치 처리 방식)
            while item_index < len(items) or active_prompts:
                if self.stop_requested:
                    self._log("⛔ 사용자 중단 요청으로 작업을 중단합니다.")
                    # 중단 시에도 처리된 경로는 저장 (I5 형식으로)
                    output_path = get_i5_output_path(input_path)
                    self._log(f"중단 전 처리된 데이터 저장 중... (출력 파일: {os.path.basename(output_path)})")
                    if safe_save_excel(df, output_path):
                        self._log(f"중단 전 엑셀 저장 완료: {os.path.basename(output_path)}")
                    # 메인 런처 현황판 업데이트 (중단)
                    try:
                        root_name = get_root_filename(input_path)
                        JobManager.update_status(root_name, img_s4_1_msg="I4-1 (중단)")
                        self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-1 (중단)")
                        debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-1 (중단)", "INFO")
                    except Exception as e:
                        debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
                    break
                
                # 1단계: 큐에 여유가 있으면 새 항목 제출
                while len(active_prompts) < batch_size and item_index < len(items) and not self.stop_requested:
                    item = items[item_index]
                    item_num = item_index + 1
                    item_start_time = time.time()
                    
                    debug_log(f"[배치] [{item_num}/{stats['total']}] 워크플로우 준비 시작: {os.path.basename(item['img_path'])}", "INFO")
                    
                    try:
                        # 워크플로우 복사 및 수정
                        workflow = json.loads(json.dumps(base_workflow))  # Deep copy
                        
                        # 유니크한 파일명 prefix 생성 (상품코드 포함)
                        # 상품코드에서 파일명으로 사용 불가능한 문자 제거
                        safe_product_code = re.sub(r'[\\/*?:"<>|]', '', item.get('product_code', '') or '')
                        safe_product_code = safe_product_code[:30]  # 길이 제한
                        if safe_product_code:
                            unique_prefix = f"BG_{safe_product_code}_row{item['idx']+1}_{int(time.time()*1000)}_"
                        else:
                            unique_prefix = f"BG_row{item['idx']+1}_{int(time.time()*1000)}_"
                        
                        # 노드 찾기
                        load_image_node_id = find_node_by_class_type(workflow, "LoadImage")
                        positive_prompt_node_id = find_node_by_class_type(workflow, "CLIPTextEncode")
                        negative_prompt_node_id = None
                        save_image_node_id = find_node_by_class_type(workflow, "SaveImage")
                        ksampler_node_id = find_node_by_class_type(workflow, "KSampler")
                        
                        # 부정 프롬프트 노드 찾기
                        clip_nodes = []
                        for node_id, node_data in workflow.items():
                            if isinstance(node_data, dict) and node_data.get("class_type") == "CLIPTextEncode":
                                clip_nodes.append(node_id)
                        if len(clip_nodes) >= 2:
                            positive_prompt_node_id = clip_nodes[0]
                            negative_prompt_node_id = clip_nodes[1]
                        elif len(clip_nodes) == 1:
                            positive_prompt_node_id = clip_nodes[0]
                        
                        # LoadImage 노드 설정
                        if load_image_node_id:
                            img_abs_path = item['img_path']
                            
                            if not comfyui_input_dir:
                                raise Exception("ComfyUI input 폴더를 찾을 수 없습니다.")
                            
                            import shutil
                            img_ext = os.path.splitext(os.path.basename(img_abs_path))[1]
                            unique_img_filename = f"row{item['idx']+1}_{os.path.basename(img_abs_path)}"
                            comfyui_img_path = os.path.join(comfyui_input_dir, unique_img_filename)
                            shutil.copy2(img_abs_path, comfyui_img_path)
                            workflow[load_image_node_id]["inputs"]["image"] = unique_img_filename
                        else:
                            raise Exception("LoadImage 노드를 찾을 수 없습니다.")
                        
                        # 프롬프트 설정
                        if positive_prompt_node_id:
                            workflow[positive_prompt_node_id]["inputs"]["text"] = item['positive_prompt']
                        else:
                            raise Exception("긍정 프롬프트 노드를 찾을 수 없습니다.")
                        
                        if negative_prompt_node_id:
                            workflow[negative_prompt_node_id]["inputs"]["text"] = item['negative_prompt']
                        
                        # SaveImage prefix 설정
                        if save_image_node_id:
                            workflow[save_image_node_id]["inputs"]["filename_prefix"] = unique_prefix
                            
                            # #region agent log - 가설 C: SaveImage prefix 설정값 수집
                            try:
                                import json as json_lib
                                debug_log_path = r"c:\Users\kohaz\Desktop\Python\.cursor\debug.log"
                                session_id = f"debug-session-{int(time.time())}"
                                with open(debug_log_path, "a", encoding="utf-8") as f:
                                    log_entry = json_lib.dumps({
                                        "sessionId": session_id,
                                        "runId": "run1",
                                        "hypothesisId": "C",
                                        "location": f"Bg_Generation_V2.py:{2604}",
                                        "message": "SaveImage filename_prefix 설정",
                                        "data": {
                                            "save_image_node_id": save_image_node_id,
                                            "unique_prefix": unique_prefix,
                                            "workflow_prefix": workflow[save_image_node_id]["inputs"].get("filename_prefix")
                                        },
                                        "timestamp": int(time.time() * 1000)
                                    }, ensure_ascii=False) + "\n"
                                    f.write(log_entry)
                            except Exception:
                                pass
                            # #endregion
                        
                        # KSampler 시드 설정
                        if ksampler_node_id:
                            import random
                            workflow[ksampler_node_id]["inputs"]["seed"] = random.randint(0, 2**32 - 1)
                        
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
                        error_msg = str(e)
                        
                        # 서버 연결 오류 감지
                        is_connection_error = ("Connection" in error_msg and ("refused" in error_msg.lower() or "10061" in error_msg)) or \
                                            "연결을 거부" in error_msg or "연결하지 못했습니다" in error_msg
                        
                        if is_connection_error:
                            self._log(f"[배치] [{item_num}/{stats['total']}] ❌ ComfyUI 서버 연결 오류 발생")
                            self._log(f"    오류: {error_msg[:200]}")
                            self._log(f"    💡 해결 방법:")
                            self._log(f"    1. ComfyUI 서버가 실행 중인지 확인하세요")
                            self._log(f"    2. 서버가 크래시했을 수 있으므로 재시작이 필요합니다")
                            self._log(f"    3. 메모리 부족으로 인한 크래시일 수 있으므로 배치 크기를 1로 줄이세요")
                            debug_log(f"[배치] [{item_num}/{stats['total']}] 서버 연결 오류: {error_msg[:200]}", "ERROR")
                            # 서버 연결 오류 발생 시 작업 중단 제안
                            if item_num == 1 or (item_num > 1 and item_num % 5 == 0):
                                self._log(f"    ⚠️ 서버 연결이 계속 실패합니다. 작업을 중단하고 서버를 재시작한 후 다시 시도하세요.")
                        else:
                            self._log(f"[배치] [{item_num}/{stats['total']}] ❌ 제출 실패: {e}")
                            debug_log(f"[배치] [{item_num}/{stats['total']}] ❌ 제출 실패: {e} (소요 시간: {item_elapsed:.2f}초)", "ERROR")
                        
                        item_index += 1
                        continue
                
                # 2단계: 완료된 항목 확인 및 처리
                completed_prompt_ids = []
                for prompt_id in list(active_prompts.keys()):
                    try:
                        # WebSocket 메시지를 확인하여 완료 여부 체크
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
                        
                        # #region agent log - 가설 A, D: completion_data 및 images 수집
                        try:
                            import json as json_lib
                            debug_log_path = r"c:\Users\kohaz\Desktop\Python\.cursor\debug.log"
                            session_id = f"debug-session-{int(time.time())}"
                            with open(debug_log_path, "a", encoding="utf-8") as f:
                                log_entry = json_lib.dumps({
                                    "sessionId": session_id,
                                    "runId": "run1",
                                    "hypothesisId": "A",
                                    "location": f"Bg_Generation_V2.py:{2677}",
                                    "message": "completion_data 확인",
                                    "data": {
                                        "prompt_id": prompt_id,
                                        "item_num": item_num,
                                        "has_completion_data": completion_data is not None,
                                        "completion_data_keys": list(completion_data.keys()) if completion_data else [],
                                        "has_output_images": completion_data.get("output_images") if completion_data else None
                                    },
                                    "timestamp": int(time.time() * 1000)
                                }, ensure_ascii=False) + "\n"
                                f.write(log_entry)
                        except Exception:
                            pass
                        # #endregion
                        
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
                        
                        # #region agent log - 가설 D: images 리스트 수집
                        try:
                            import json as json_lib
                            debug_log_path = r"c:\Users\kohaz\Desktop\Python\.cursor\debug.log"
                            session_id = f"debug-session-{int(time.time())}"
                            with open(debug_log_path, "a", encoding="utf-8") as f:
                                log_entry = json_lib.dumps({
                                    "sessionId": session_id,
                                    "runId": "run1",
                                    "hypothesisId": "D",
                                    "location": f"Bg_Generation_V2.py:{2700}",
                                    "message": "get_output_images 반환값 확인",
                                    "data": {
                                        "prompt_id": prompt_id,
                                        "item_num": item_num,
                                        "images_count": len(images) if images else 0,
                                        "images": images[:10] if images else [],  # 처음 10개만
                                        "unique_prefix": unique_prefix
                                    },
                                    "timestamp": int(time.time() * 1000)
                                }, ensure_ascii=False) + "\n"
                                f.write(log_entry)
                        except Exception:
                            pass
                        # #endregion
                        
                        # 이미지 파일 찾기 (재시도 로직 포함)
                        bg_image_path = None
                        max_retry_attempts = 5  # 최대 5번 재시도
                        retry_delay = 1.0  # 1초 대기
                        
                        for attempt in range(max_retry_attempts):
                            # 방법 1: API에서 받은 이미지 파일명 사용
                            if images:
                                for img_filename in images:
                                    # prefix로 시작하는 파일 또는 최근 생성된 파일
                                    if comfyui_output_dir and os.path.exists(comfyui_output_dir):
                                        # prefix로 시작하는 파일 찾기
                                        if img_filename.startswith(unique_prefix):
                                            img_path = os.path.join(comfyui_output_dir, img_filename)
                                            if os.path.exists(img_path):
                                                bg_image_path = img_path
                                                break
                                        
                                        # prefix가 없으면 파일명 전체로 검색
                                        if not bg_image_path and img_filename:
                                            img_path = os.path.join(comfyui_output_dir, img_filename)
                                            if os.path.exists(img_path):
                                                # 생성 시간 확인 (완료 시간 이후 생성된 파일인지)
                                                file_mtime = os.path.getmtime(img_path)
                                                if file_mtime >= item_start_time - 5:  # 5초 오차 허용
                                                    bg_image_path = img_path
                                                    break
                            
                            # 방법 2: output 폴더에서 직접 검색 (prefix로 시작하는 파일)
                            if not bg_image_path and comfyui_output_dir and os.path.exists(comfyui_output_dir):
                                try:
                                    all_files = os.listdir(comfyui_output_dir)
                                    matching_files = [f for f in all_files if f.startswith(unique_prefix)]
                                    
                                    # #region agent log - 가설 E: output 디렉토리 파일 목록 수집
                                    try:
                                        import json as json_lib
                                        debug_log_path = r"c:\Users\kohaz\Desktop\Python\.cursor\debug.log"
                                        session_id = f"debug-session-{int(time.time())}"
                                        with open(debug_log_path, "a", encoding="utf-8") as f:
                                            log_entry = json_lib.dumps({
                                                "sessionId": session_id,
                                                "runId": "run1",
                                                "hypothesisId": "E",
                                                "location": f"Bg_Generation_V2.py:{2731}",
                                                "message": "output 디렉토리 파일 검색",
                                                "data": {
                                                    "item_num": item_num,
                                                    "attempt": attempt,
                                                    "comfyui_output_dir": comfyui_output_dir,
                                                    "unique_prefix": unique_prefix,
                                                    "total_files_count": len(all_files),
                                                    "matching_files_count": len(matching_files),
                                                    "matching_files": matching_files[:10],  # 처음 10개만
                                                    "recent_files": sorted(all_files, key=lambda f: os.path.getmtime(os.path.join(comfyui_output_dir, f)), reverse=True)[:10] if all_files else []  # 최근 10개 파일
                                                },
                                                "timestamp": int(time.time() * 1000)
                                            }, ensure_ascii=False) + "\n"
                                            f.write(log_entry)
                                    except Exception:
                                        pass
                                    # #endregion
                                    
                                    if matching_files:
                                        files_with_time = [(f, os.path.getmtime(os.path.join(comfyui_output_dir, f))) for f in matching_files]
                                        files_with_time.sort(key=lambda x: x[1], reverse=True)
                                        # 가장 최근 파일 선택
                                        candidate_path = os.path.join(comfyui_output_dir, files_with_time[0][0])
                                        if os.path.exists(candidate_path):
                                            bg_image_path = candidate_path
                                except Exception as e:
                                    if attempt == 0:  # 첫 시도에서만 로그 출력
                                        self._log(f"[배치] output 폴더 검색 오류: {e}")
                            
                            # 파일을 찾았으면 반복 종료
                            if bg_image_path and os.path.exists(bg_image_path):
                                break
                            
                            # 마지막 시도가 아니면 대기
                            if attempt < max_retry_attempts - 1:
                                time.sleep(retry_delay)
                                # 재시도 시 이미지 목록 다시 가져오기
                                try:
                                    images = client.get_output_images(prompt_id, completion_data=completion_data)
                                except:
                                    pass  # 재시도 중 오류는 무시
                        
                        # 결과 처리
                        if bg_image_path and os.path.exists(bg_image_path):
                            df.at[item['idx'], "IMG_S4_BG_생성경로"] = bg_image_path
                            stats["success"] += 1
                            item_elapsed = time.time() - item_start_time
                            self._log(f"[배치] [{item_num}/{stats['total']}] ✅ 완료: {os.path.basename(bg_image_path)} (소요: {item_elapsed:.1f}초)")
                            debug_log(f"[배치] [{item_num}/{stats['total']}] ✅ 처리 완료: {os.path.basename(bg_image_path)} (소요 시간: {item_elapsed:.2f}초)", "INFO")
                            
                            # 완료된 항목 목록에 추가
                            completed_item = {
                                'nukki_path': item['img_path'],
                                'bg_path': bg_image_path,
                                'code': item.get('product_code', ''),
                                'name': item.get('product_name', ''),
                                'idx': item['idx']
                            }
                            self.completed_items.append(completed_item)
                            
                            # 미리보기 업데이트
                            self._update_preview_images(
                                nukki_path=item['img_path'],
                                bg_path=bg_image_path,
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
                        error_msg = str(e)
                        
                        # 메모리 부족 오류 감지
                        is_memory_error = ("페이징 파일" in error_msg or 
                                         "paging file" in error_msg.lower() or
                                         "1455" in error_msg or
                                         "memory" in error_msg.lower() and ("insufficient" in error_msg.lower() or "low" in error_msg.lower()))
                        
                        # 이미지 파일을 찾을 수 없는 오류 감지
                        is_image_not_found = "생성된 이미지 파일을 찾을 수 없습니다" in error_msg
                        
                        if is_memory_error:
                            self._log(f"[배치] [{item_num}/{stats['total']}] ❌ 메모리 부족 오류 발생")
                            self._log(f"    오류: {error_msg}")
                            self._log(f"    💡 해결 방법:")
                            self._log(f"    1. 배치 크기를 1로 줄이세요 (현재: {batch_size}개)")
                            self._log(f"    2. Windows 페이징 파일 크기를 늘리세요")
                            self._log(f"    3. 다른 프로그램을 종료하여 메모리를 확보하세요")
                            debug_log(f"[배치] [{item_num}/{stats['total']}] 메모리 부족 오류: {error_msg}", "ERROR")
                        elif is_image_not_found:
                            self._log(f"[배치] [{item_num}/{stats['total']}] ❌ 이미지 파일을 찾을 수 없습니다")
                            self._log(f"    prefix: {unique_prefix}")
                            self._log(f"    💡 원인 및 해결 방법:")
                            self._log(f"    1. ComfyUI 서버에서 이미지 생성이 실패했을 수 있습니다")
                            self._log(f"    2. ComfyUI 서버 로그를 확인하세요")
                            self._log(f"    3. 서버가 메모리 부족으로 크래시했을 수 있습니다")
                            self._log(f"    4. 배치 크기를 1로 줄이고 서버를 재시작한 후 다시 시도하세요")
                            debug_log(f"[배치] [{item_num}/{stats['total']}] 이미지 파일 찾기 실패: prefix={unique_prefix}", "ERROR")
                        else:
                            self._log(f"[배치] [{item_num}/{stats['total']}] ❌ 실패: {e}")
                            debug_log(f"[배치] [{item_num}/{stats['total']}] ❌ 처리 실패: {e} (소요 시간: {item_elapsed:.2f}초)", "ERROR")
                        
                        import traceback
                        error_trace = traceback.format_exc()
                        if not is_memory_error:  # 메모리 오류가 아닐 때만 전체 트레이스 출력
                            self._log(error_trace)
                        debug_log(f"[배치] [{item_num}/{stats['total']}] 오류 상세:\n{error_trace}", "ERROR")
                        self.after(0, lambda f=stats["fail"]: self.stat_fail.set(str(f)))
                        # 완료 추적 데이터 정리
                        client.clear_completion(prompt_id)
                
                # 3단계: 완료되지 않은 항목이 있으면 잠시 대기 (CPU 부하 감소)
                if active_prompts and not self.stop_requested:
                    time.sleep(0.1)  # 0.1초 대기 후 다시 확인 (중단 반응 속도 개선)
            
            # 엑셀 저장 (I5 형식으로 저장)
            output_path = get_i5_output_path(input_path)
            self._log(f"엑셀 저장 중... (출력 파일: {os.path.basename(output_path)})")
            debug_log(f"엑셀 파일 저장 시작: {output_path}", "DEBUG")
            if safe_save_excel(df, output_path):
                self._log(f"엑셀 저장 완료: {os.path.basename(output_path)}")
                debug_log(f"엑셀 파일 저장 완료: {output_path}", "INFO")
            else:
                self._log("엑셀 저장 실패 (사용자가 취소)")
                debug_log("엑셀 파일 저장 실패 (사용자가 취소)", "WARN")
            
            # ComfyUI 연결 종료
            debug_log("ComfyUI 연결 종료", "DEBUG")
            client.disconnect()
            
            # 메인 런처 현황판 업데이트 (I4-1: 배경 생성 완료) - img 상태만 업데이트 (text 상태는 변경하지 않음)
            try:
                root_name = get_root_filename(input_path)
                JobManager.update_status(root_name, img_s4_1_msg="I4-1(배경생성완료)")
                self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-1(배경생성완료)")
                debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-1(배경생성완료)", "INFO")
            except Exception as e:
                debug_log(f"메인 런처 현황판 업데이트 실패: {e}", "ERROR")
            
            # 완료 메시지
            elapsed_total = time.time() - start_time
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
                    JobManager.update_status(root_name, img_s4_1_msg="I4-1 (오류)")
                    self._log(f"[INFO] 런처 현황판 업데이트: {root_name} -> I4-1 (오류)")
                    debug_log(f"메인 런처 현황판 업데이트: {root_name} -> I4-1 (오류)", "INFO")
            except Exception as update_error:
                debug_log(f"메인 런처 현황판 업데이트 실패: {update_error}", "ERROR")
            self.after(0, lambda: messagebox.showerror("오류", str(e)))
        finally:
            # 중단 또는 오류 발생 시에도 처리된 경로는 저장
            try:
                if 'df' in locals() and 'input_path' in locals():
                    if self.stop_requested or stats.get("success", 0) > 0 or stats.get("fail", 0) > 0:
                        # I5 형식으로 저장
                        output_path = get_i5_output_path(input_path)
                        self._log(f"처리된 데이터 저장 중... (출력 파일: {os.path.basename(output_path)})")
                        if safe_save_excel(df, output_path):
                            self._log(f"엑셀 저장 완료 (중단/오류 시에도 저장됨): {os.path.basename(output_path)}")
                        else:
                            self._log("엑셀 저장 실패 (사용자가 취소)")
            except Exception as save_error:
                self._log(f"엑셀 저장 중 오류: {save_error}")
            
            # ComfyUI 연결 종료 (정상 종료 시에만 실행됨, 중단 시에는 이미 종료되었을 수 있음)
            try:
                if 'client' in locals():
                    client.disconnect()
            except:
                pass
            
            debug_log("작업 종료", "INFO")
            if DEBUG_LOG_FILE:
                debug_log(f"디버그 로그 파일 위치: {DEBUG_LOG_FILE}", "INFO")
            
            # 중단 시 상태 메시지 업데이트
            if self.stop_requested:
                self.after(0, lambda: self.status_msg.set("작업이 중단되었습니다. (처리된 데이터는 저장되었습니다)"))
            
            self.after(0, self._on_process_complete)

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
    
    def _on_process_complete(self):
        """처리 완료 후 UI 상태 복원"""
        self.is_running = False
        self._stop_time_timer()  # 타이머 중지
        self.btn_start.config(state='normal')
        self.btn_stop.config(state='disabled')
        self.status_msg.set("작업이 완료되었습니다.")
        # 예상 종료 시간을 "완료"로 표시
        self.stat_estimated_end.set("완료")


if __name__ == "__main__":
    app = BGGenerationGUI()
    app.mainloop()

